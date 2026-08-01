// Command queue-exporter is a minimal custom Prometheus exporter.
//
// It demonstrates the pattern that matters for exporters: implementing
// prometheus.Collector so metrics are scraped from the source on demand,
// instead of maintaining background goroutines that mutate Gauges on a timer.
//
// Why the Collector interface rather than package-level Gauges:
//
//   - values are always as fresh as the scrape, with no separate poll interval
//     to tune or to drift out of sync
//   - a failing backend is visible as an "up"-style metric plus a scrape error,
//     rather than silently stale values that look healthy
//   - no goroutine leak and no locking around a shared metrics struct
//
// The exception is genuinely event-driven data (request counters, latency
// histograms). Those must stay as normal instrumented Counters/Histograms,
// because a Collector cannot observe events that already happened.
//
// Run:
//
//	go run ./examples/prometheus-exporter
//	curl -s localhost:9101/metrics | grep queue_
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const namespace = "queue"

// queueStats is what the exporter scrapes from. In a real exporter this is an
// HTTP/AMQP/SQL client talking to the system being monitored.
type queueStats struct {
	Name      string
	Depth     float64
	Consumers float64
	OldestAge time.Duration
}

// backend is the thing being monitored. Keeping it an interface makes the
// collector testable with a fake that returns fixed data or a forced error.
type backend interface {
	Stats(ctx context.Context) ([]queueStats, error)
}

// queueCollector implements prometheus.Collector.
type queueCollector struct {
	backend backend
	timeout time.Duration
	logger  *slog.Logger

	// Descriptors are built once. Creating them per-scrape is wasteful and
	// makes it easy to accidentally emit inconsistent help text.
	depth      *prometheus.Desc
	consumers  *prometheus.Desc
	oldestAge  *prometheus.Desc
	up         *prometheus.Desc
	scrapeTime *prometheus.Desc
}

func newQueueCollector(b backend, timeout time.Duration,
	logger *slog.Logger) *queueCollector {

	labels := []string{"queue"}

	return &queueCollector{
		backend: b,
		timeout: timeout,
		logger:  logger,

		depth: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "depth_messages"),
			"Number of messages currently in the queue.",
			labels, nil,
		),
		consumers: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "consumers"),
			"Number of consumers attached to the queue.",
			labels, nil,
		),
		oldestAge: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "oldest_message_age_seconds"),
			"Age of the oldest message in the queue.",
			labels, nil,
		),
		// An "up"-style metric is mandatory for a custom exporter. Without it a
		// broken backend is indistinguishable from an empty one: both produce
		// no queue_depth_messages series at all.
		up: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "up"),
			"Whether the last scrape of the queue backend succeeded (1) or not (0).",
			nil, nil,
		),
		scrapeTime: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "scrape_duration_seconds"),
			"Duration of the last scrape of the queue backend.",
			nil, nil,
		),
	}
}

// Describe sends the descriptors of every metric this collector can produce.
//
// Sending nothing here would make it an "unchecked" collector, which disables
// the registry's duplicate-metric detection. Describing properly means the
// registry fails loudly at registration time instead of at scrape time.
func (c *queueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.depth
	ch <- c.consumers
	ch <- c.oldestAge
	ch <- c.up
	ch <- c.scrapeTime
}

// Collect is called on every scrape, and may be called concurrently.
// It must not block indefinitely: always bound the backend call with a
// context timeout shorter than Prometheus' scrape_timeout.
func (c *queueCollector) Collect(ch chan<- prometheus.Metric) {
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	stats, err := c.backend.Stats(ctx)
	duration := time.Since(start)

	ch <- prometheus.MustNewConstMetric(
		c.scrapeTime, prometheus.GaugeValue, duration.Seconds())

	if err != nil {
		// Report the failure as data, then return. Do not panic and do not
		// emit stale values: a gap is honest, a stale value is a lie.
		c.logger.Error("scrape failed", "err", err, "duration", duration)
		ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 0)
		return
	}

	ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 1)

	for _, s := range stats {
		// NewConstMetric builds a metric without any stored state. The label
		// values must line up positionally with the label names in the Desc.
		ch <- prometheus.MustNewConstMetric(
			c.depth, prometheus.GaugeValue, s.Depth, s.Name)
		ch <- prometheus.MustNewConstMetric(
			c.consumers, prometheus.GaugeValue, s.Consumers, s.Name)
		ch <- prometheus.MustNewConstMetric(
			c.oldestAge, prometheus.GaugeValue, s.OldestAge.Seconds(), s.Name)
	}
}

// fakeBackend stands in for a real queue system so the example runs standalone.
type fakeBackend struct {
	failEvery int
	calls     int
}

var errBackendUnavailable = errors.New("queue backend unavailable")

func (f *fakeBackend) Stats(ctx context.Context) ([]queueStats, error) {
	f.calls++

	// Respect cancellation even in a fake: it is the behaviour every real
	// backend call must have.
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("collecting stats: %w", ctx.Err())
	case <-time.After(10 * time.Millisecond):
	}

	if f.failEvery > 0 && f.calls%f.failEvery == 0 {
		return nil, errBackendUnavailable
	}

	return []queueStats{
		{
			Name:      "orders",
			Depth:     float64(rand.Intn(500)),
			Consumers: 4,
			OldestAge: time.Duration(rand.Intn(120)) * time.Second,
		},
		{
			Name:      "notifications",
			Depth:     float64(rand.Intn(50)),
			Consumers: 2,
			OldestAge: time.Duration(rand.Intn(30)) * time.Second,
		},
	}, nil
}

func main() {
	var (
		listenAddr    = flag.String("listen", ":9101", "metrics listen address")
		metricsPath   = flag.String("path", "/metrics", "metrics endpoint path")
		scrapeTimeout = flag.Duration("scrape-timeout", 5*time.Second,
			"timeout for one backend scrape; keep below Prometheus scrape_timeout")
	)
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	// A private registry, not the default one. prometheus.MustRegister on the
	// global registry makes tests order-dependent and drags in whatever any
	// transitively imported library decided to register.
	reg := prometheus.NewRegistry()

	// Opt in explicitly to the Go runtime and process collectors.
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	collector := newQueueCollector(&fakeBackend{failEvery: 7}, *scrapeTimeout, logger)
	if err := reg.Register(collector); err != nil {
		logger.Error("registering collector", "err", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.Handle(*metricsPath, promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		ErrorLog:            slog.NewLogLogger(logger.Handler(), slog.LevelError),
		ErrorHandling:       promhttp.ContinueOnError,
		MaxRequestsInFlight: 10,
		// Prometheus scrapes are serialised per target anyway, but a shared
		// exporter can be scraped by several servers at once.
		EnableOpenMetrics: true,
	}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})

	srv := &http.Server{
		Addr:              *listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("exporter listening", "addr", *listenAddr, "path", *metricsPath)
		if err := srv.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			logger.Error("server failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()

	// Graceful shutdown with its own timeout: the parent context is already
	// cancelled, so deriving from it would abort immediately.
	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "err", err)
		os.Exit(1)
	}
	logger.Info("stopped cleanly")
}
