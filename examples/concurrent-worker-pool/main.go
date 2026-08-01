// Command workerpool demonstrates a bounded concurrent worker pool built on
// errgroup with proper context cancellation.
//
// The shape of the problem: process N independent jobs with at most C running
// at once, stop early if the caller cancels, collect results and errors without
// data races, and make sure no goroutine outlives the function that started it.
//
// Key points this example makes concrete:
//
//   - errgroup.WithContext derives a context that is cancelled as soon as the
//     first worker returns a non-nil error, so siblings stop promptly
//   - g.SetLimit(n) bounds concurrency without a hand-rolled semaphore channel
//   - g.Wait() returns only the FIRST error; use errors.Join or a collector if
//     you need all of them
//   - workers must select on ctx.Done() at every blocking point, otherwise
//     cancellation is advisory and Wait() blocks until the slowest job finishes
//   - results go through a channel or a mutex-guarded slice, never a bare
//     shared map
//
// Run:
//
//	go run ./examples/concurrent-worker-pool
//	go run ./examples/concurrent-worker-pool -jobs 40 -workers 8 -fail-rate 0.2
//	go run ./examples/concurrent-worker-pool -mode collect-all
//	go run ./examples/concurrent-worker-pool -timeout 300ms
//
// Always verify this kind of code with the race detector:
//
//	go test -race ./...
//	go run -race ./examples/concurrent-worker-pool
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

// Job is a unit of work handed to a worker.
type Job struct {
	ID  int
	URL string
}

// Result is what a worker produces.
type Result struct {
	JobID    int
	Bytes    int
	Duration time.Duration
}

var errTransient = errors.New("transient failure")

func main() {
	var (
		jobCount = flag.Int("jobs", 20, "number of jobs to process")
		workers  = flag.Int("workers", 4, "maximum concurrent workers")
		failRate = flag.Float64("fail-rate", 0.1, "probability a job fails (0..1)")
		timeout  = flag.Duration("timeout", 10*time.Second, "overall deadline")
		mode     = flag.String("mode", "fail-fast",
			"fail-fast (stop on first error) or collect-all (run everything)")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Signals cancel the root context; the deadline bounds total runtime.
	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	jobs := make([]Job, *jobCount)
	for i := range jobs {
		jobs[i] = Job{ID: i, URL: fmt.Sprintf("https://example.internal/item/%d", i)}
	}

	start := time.Now()

	var (
		results []Result
		err     error
	)

	switch *mode {
	case "fail-fast":
		results, err = processFailFast(ctx, logger, jobs, *workers, *failRate)
	case "collect-all":
		results, err = processCollectAll(ctx, logger, jobs, *workers, *failRate)
	default:
		fmt.Fprintf(os.Stderr, "error: unknown -mode %q\n", *mode)
		os.Exit(2)
	}

	elapsed := time.Since(start)

	// Results are sorted because completion order is nondeterministic; without
	// this the output is different on every run and useless for diffing.
	sort.Slice(results, func(i, j int) bool { return results[i].JobID < results[j].JobID })

	var totalBytes int
	for _, r := range results {
		totalBytes += r.Bytes
	}

	logger.Info("finished",
		"mode", *mode,
		"succeeded", len(results),
		"of", len(jobs),
		"bytes", totalBytes,
		"elapsed", elapsed.Round(time.Millisecond),
	)

	if err != nil {
		logger.Error("completed with errors", "err", err)
		os.Exit(1)
	}
}

// processFailFast runs jobs with bounded concurrency and aborts the whole batch
// as soon as any job fails.
//
// This is the right default for a pipeline where a single failure invalidates
// the run: there is no point spending another two minutes on work you are going
// to throw away.
func processFailFast(ctx context.Context, logger *slog.Logger, jobs []Job,
	workers int, failRate float64) ([]Result, error) {

	// WithContext gives back a context that is cancelled when the first
	// goroutine returns an error OR when Wait returns. Workers must select on
	// it for the cancellation to mean anything.
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)

	var (
		mu      sync.Mutex
		results []Result
	)

	for _, job := range jobs {
		// Go 1.22+ gives each iteration its own `job` variable, so the old
		// `job := job` shadowing dance is no longer required. It is still
		// needed on Go 1.21 and earlier.

		// g.Go blocks here once `workers` goroutines are already running,
		// which is what bounds concurrency. It does not start a goroutine
		// per job up front.
		g.Go(func() error {
			res, err := doWork(ctx, job, failRate)
			if err != nil {
				// Wrapping preserves the sentinel for errors.Is at the top.
				return fmt.Errorf("job %d: %w", job.ID, err)
			}

			mu.Lock()
			results = append(results, res)
			mu.Unlock()

			logger.Debug("job done", "id", job.ID, "bytes", res.Bytes)
			return nil
		})
	}

	// Wait blocks until every started goroutine returns, then yields the first
	// non-nil error. Even on failure the successful results are usable.
	if err := g.Wait(); err != nil {
		return results, err
	}
	return results, nil
}

// processCollectAll runs every job to completion regardless of failures and
// reports all errors together.
//
// Use this when jobs are independent and you want a full report, e.g. a
// validation pass over 500 config files: stopping at the first bad file means
// five more round trips to fix them one at a time.
func processCollectAll(ctx context.Context, logger *slog.Logger, jobs []Job,
	workers int, failRate float64) ([]Result, error) {

	// Note: NOT errgroup.WithContext here. We do not want the first error to
	// cancel the siblings. Cancellation still comes from the caller's ctx.
	var g errgroup.Group
	g.SetLimit(workers)

	var (
		mu      sync.Mutex
		results []Result
		errs    []error
	)

	for _, job := range jobs {
		g.Go(func() error {
			res, err := doWork(ctx, job, failRate)

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				errs = append(errs, fmt.Errorf("job %d: %w", job.ID, err))
				return nil // swallow here; aggregated below
			}
			results = append(results, res)
			logger.Debug("job done", "id", job.ID, "bytes", res.Bytes)
			return nil
		})
	}

	_ = g.Wait() // no worker returns an error in this mode

	// errors.Join (Go 1.20+) builds a multi-error that errors.Is/As can still
	// inspect, and whose Error() lists every message on its own line.
	return results, errors.Join(errs...)
}

// doWork simulates a network call. The important part is not the fake latency
// but the shape: every blocking operation is selected against ctx.Done(), so
// cancellation takes effect immediately instead of after the work finishes.
func doWork(ctx context.Context, job Job, failRate float64) (Result, error) {
	start := time.Now()

	// Cheap pre-check: if the context is already dead, do not even start.
	// Without this, a cancelled batch still kicks off every queued job.
	if err := ctx.Err(); err != nil {
		return Result{}, fmt.Errorf("not started: %w", err)
	}

	work := time.Duration(50+rand.Intn(250)) * time.Millisecond

	select {
	case <-ctx.Done():
		// The canonical mistake is to omit this case and just sleep. The
		// goroutine then ignores cancellation entirely, and g.Wait() blocks
		// until the slowest job finishes no matter what the caller asked for.
		return Result{}, fmt.Errorf("cancelled after %s: %w",
			time.Since(start).Round(time.Millisecond), ctx.Err())

	case <-time.After(work):
		// time.After leaks its timer until it fires. For sub-second waits in a
		// short-lived goroutine that is fine; in a hot loop use a
		// time.NewTimer and defer timer.Stop().
	}

	if rand.Float64() < failRate {
		return Result{}, fmt.Errorf("fetching %s: %w", job.URL, errTransient)
	}

	return Result{
		JobID:    job.ID,
		Bytes:    1024 + rand.Intn(8192),
		Duration: time.Since(start),
	}, nil
}
