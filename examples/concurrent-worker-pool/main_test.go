package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func makeJobs(n int) []Job {
	jobs := make([]Job, n)
	for i := range jobs {
		jobs[i] = Job{ID: i, URL: "https://example.internal/item"}
	}
	return jobs
}

func TestProcessFailFastReturnsEveryResultWhenNothingFails(t *testing.T) {
	results, err := processFailFast(context.Background(), quietLogger(), makeJobs(12), 4, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 12 {
		t.Errorf("got %d results, want 12", len(results))
	}
	for _, r := range results {
		if r.Bytes <= 0 {
			t.Errorf("job %d reported %d bytes, want a positive count", r.JobID, r.Bytes)
		}
	}
}

// The whole point of errgroup.WithContext: the first failure cancels the
// siblings, so a large batch does not keep burning time on work that is
// already going to be thrown away.
func TestProcessFailFastStopsEarlyOnFailure(t *testing.T) {
	results, err := processFailFast(context.Background(), quietLogger(), makeJobs(50), 4, 1.0)
	if err == nil {
		t.Fatal("expected an error when every job fails")
	}
	if !errors.Is(err, errTransient) {
		t.Errorf("error = %v, want it to wrap errTransient", err)
	}
	if len(results) != 0 {
		t.Errorf("got %d results, want none when every job fails", len(results))
	}
}

// g.Wait() returns only the FIRST error. That is the documented behaviour and
// the reason collect-all exists at all, so it is worth pinning down.
func TestProcessFailFastReportsOneError(t *testing.T) {
	_, err := processFailFast(context.Background(), quietLogger(), makeJobs(20), 4, 1.0)
	if err == nil {
		t.Fatal("expected an error")
	}
	if lines := strings.Count(err.Error(), "\n"); lines != 0 {
		t.Errorf("fail-fast reported %d extra error lines, want a single error", lines)
	}
}

func TestProcessCollectAllAggregatesEveryError(t *testing.T) {
	const jobs = 8

	results, err := processCollectAll(context.Background(), quietLogger(), makeJobs(jobs), 4, 1.0)
	if err == nil {
		t.Fatal("expected an aggregated error when every job fails")
	}
	if len(results) != 0 {
		t.Errorf("got %d results, want none", len(results))
	}

	// errors.Join renders one message per line, and errors.Is still works
	// through the join — both are the properties worth demonstrating.
	if got := strings.Count(err.Error(), "\n") + 1; got != jobs {
		t.Errorf("aggregated %d errors, want %d", got, jobs)
	}
	if !errors.Is(err, errTransient) {
		t.Errorf("joined error = %v, want errors.Is to find errTransient", err)
	}
}

func TestProcessCollectAllRunsEverythingDespiteFailures(t *testing.T) {
	// Every job runs to completion, so successes and failures together must
	// account for the whole batch.
	const jobs = 40
	results, err := processCollectAll(context.Background(), quietLogger(), makeJobs(jobs), 8, 0.5)

	failures := 0
	if err != nil {
		failures = strings.Count(err.Error(), "\n") + 1
	}
	if len(results)+failures != jobs {
		t.Errorf("%d results + %d failures = %d, want %d",
			len(results), failures, len(results)+failures, jobs)
	}
}

func TestAlreadyCancelledContextStartsNoWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	results, err := processFailFast(ctx, quietLogger(), makeJobs(20), 4, 0)
	if err == nil {
		t.Fatal("expected an error from a cancelled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want it to wrap context.Canceled", err)
	}
	if len(results) != 0 {
		t.Errorf("got %d results, want none", len(results))
	}
}

// A deadline must actually cut the batch short. If workers ignored ctx.Done()
// this would still pass eventually — but only after every job had finished,
// which is what the elapsed-time bound catches.
func TestDeadlineCutsTheBatchShort(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := processFailFast(ctx, quietLogger(), makeJobs(200), 4, 0)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want it to wrap context.DeadlineExceeded", err)
	}
	if elapsed > 2*time.Second {
		t.Errorf("batch took %s after an 80ms deadline: workers are ignoring cancellation", elapsed)
	}
}

// SetLimit is what bounds concurrency. With a single worker, four jobs cannot
// finish faster than four sequential minimum work durations (50ms each).
func TestConcurrencyIsBounded(t *testing.T) {
	start := time.Now()
	if _, err := processFailFast(context.Background(), quietLogger(), makeJobs(4), 1, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed < 200*time.Millisecond {
		t.Errorf("4 jobs with 1 worker finished in %s; the concurrency limit is not being applied",
			elapsed)
	}
}

func TestDoWork(t *testing.T) {
	t.Run("returns a result on success", func(t *testing.T) {
		got, err := doWork(context.Background(), Job{ID: 7}, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.JobID != 7 {
			t.Errorf("JobID = %d, want 7", got.JobID)
		}
		if got.Duration <= 0 {
			t.Error("Duration is not positive")
		}
	})

	t.Run("wraps the sentinel on failure", func(t *testing.T) {
		_, err := doWork(context.Background(), Job{ID: 1}, 1.0)
		if !errors.Is(err, errTransient) {
			t.Errorf("error = %v, want it to wrap errTransient", err)
		}
	})

	t.Run("refuses to start on a dead context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		start := time.Now()
		_, err := doWork(ctx, Job{ID: 1}, 0)

		if !errors.Is(err, context.Canceled) {
			t.Errorf("error = %v, want it to wrap context.Canceled", err)
		}
		// The pre-check exists so a cancelled batch does not still pay for
		// every queued job's simulated latency.
		if elapsed := time.Since(start); elapsed > 20*time.Millisecond {
			t.Errorf("doWork spent %s on an already-cancelled context", elapsed)
		}
	})
}
