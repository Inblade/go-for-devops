package main

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// stubBackend returns fixed data, or an error, or blocks until its context is
// cancelled — the three cases a collector has to survive.
type stubBackend struct {
	stats []queueStats
	err   error
	block bool
	calls int
}

func (s *stubBackend) Stats(ctx context.Context) ([]queueStats, error) {
	s.calls++
	if s.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return s.stats, s.err
}

func TestCollectorReportsQueueMetrics(t *testing.T) {
	backend := &stubBackend{stats: []queueStats{
		{Name: "orders", Depth: 42, Consumers: 3, OldestAge: 90 * time.Second},
		{Name: "emails", Depth: 0, Consumers: 1, OldestAge: 0},
	}}
	collector := newQueueCollector(backend, time.Second, quietLogger())

	expected := strings.NewReader(`
# HELP queue_consumers Number of consumers attached to the queue.
# TYPE queue_consumers gauge
queue_consumers{queue="emails"} 1
queue_consumers{queue="orders"} 3
# HELP queue_depth_messages Number of messages currently in the queue.
# TYPE queue_depth_messages gauge
queue_depth_messages{queue="emails"} 0
queue_depth_messages{queue="orders"} 42
# HELP queue_oldest_message_age_seconds Age of the oldest message in the queue.
# TYPE queue_oldest_message_age_seconds gauge
queue_oldest_message_age_seconds{queue="emails"} 0
queue_oldest_message_age_seconds{queue="orders"} 90
# HELP queue_up Whether the last scrape of the queue backend succeeded (1) or not (0).
# TYPE queue_up gauge
queue_up 1
`)

	if err := testutil.CollectAndCompare(collector, expected,
		"queue_depth_messages", "queue_consumers", "queue_oldest_message_age_seconds", "queue_up",
	); err != nil {
		t.Fatal(err)
	}
}

// The point of the up metric: a failed scrape must be visible as data. Without
// it a broken backend looks exactly like an empty one.
func TestCollectorReportsUpZeroOnFailure(t *testing.T) {
	backend := &stubBackend{err: errBackendUnavailable}
	collector := newQueueCollector(backend, time.Second, quietLogger())

	expected := strings.NewReader(`
# HELP queue_up Whether the last scrape of the queue backend succeeded (1) or not (0).
# TYPE queue_up gauge
queue_up 0
`)

	if err := testutil.CollectAndCompare(collector, expected, "queue_up"); err != nil {
		t.Fatal(err)
	}
}

// A failed scrape must emit no queue series at all. Emitting the previous
// values would be worse than a gap: a stale value is a lie an alert believes.
func TestFailedScrapeEmitsNoStaleSeries(t *testing.T) {
	backend := &stubBackend{err: errBackendUnavailable}
	collector := newQueueCollector(backend, time.Second, quietLogger())

	for _, metric := range []string{
		"queue_depth_messages", "queue_consumers", "queue_oldest_message_age_seconds",
	} {
		if got := testutil.CollectAndCount(collector, metric); got != 0 {
			t.Errorf("%s produced %d series after a failed scrape, want 0", metric, got)
		}
	}
}

// The scrape-duration metric must be emitted on both paths, otherwise a
// dashboard of scrape cost silently loses exactly the interesting samples.
func TestScrapeDurationIsAlwaysEmitted(t *testing.T) {
	tests := map[string]*stubBackend{
		"success": {stats: []queueStats{{Name: "orders", Depth: 1}}},
		"failure": {err: errBackendUnavailable},
	}

	for name, backend := range tests {
		t.Run(name, func(t *testing.T) {
			collector := newQueueCollector(backend, time.Second, quietLogger())
			if got := testutil.CollectAndCount(collector, "queue_scrape_duration_seconds"); got != 1 {
				t.Errorf("got %d scrape-duration series, want 1", got)
			}
		})
	}
}

// A backend that hangs must not hang the scrape: Collect bounds it with a
// timeout, and Prometheus gets an up=0 instead of a stuck connection.
func TestSlowBackendIsBoundedByTheTimeout(t *testing.T) {
	backend := &stubBackend{block: true}
	collector := newQueueCollector(backend, 50*time.Millisecond, quietLogger())

	done := make(chan int, 1)
	go func() { done <- testutil.CollectAndCount(collector, "queue_up") }()

	select {
	case count := <-done:
		if count != 1 {
			t.Errorf("got %d up series, want 1", count)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Collect did not return: the backend call is not bounded by a timeout")
	}
}

// Describe must announce every metric the collector can emit; a collector that
// describes nothing is "unchecked" and the registry stops detecting duplicates.
func TestDescribeAnnouncesEveryMetric(t *testing.T) {
	collector := newQueueCollector(&stubBackend{}, time.Second, quietLogger())

	ch := make(chan *prometheus.Desc, 16)
	collector.Describe(ch)
	close(ch)

	seen := map[string]bool{}
	for desc := range ch {
		seen[desc.String()] = true
	}
	if len(seen) != 5 {
		t.Errorf("Describe sent %d descriptors, want 5", len(seen))
	}
}

func TestCollectorRegistersWithAPedanticRegistry(t *testing.T) {
	registry := prometheus.NewPedanticRegistry()
	collector := newQueueCollector(&stubBackend{}, time.Second, quietLogger())

	if err := registry.Register(collector); err != nil {
		t.Fatalf("registration rejected: %v", err)
	}
}

func TestEmptyBackendStillReportsUp(t *testing.T) {
	collector := newQueueCollector(&stubBackend{stats: nil}, time.Second, quietLogger())

	expected := strings.NewReader(`
# HELP queue_up Whether the last scrape of the queue backend succeeded (1) or not (0).
# TYPE queue_up gauge
queue_up 1
`)
	if err := testutil.CollectAndCompare(collector, expected, "queue_up"); err != nil {
		t.Fatal(err)
	}
}

func TestFakeBackendFailsOnSchedule(t *testing.T) {
	backend := &fakeBackend{failEvery: 3}

	var failures int
	for i := 0; i < 9; i++ {
		if _, err := backend.Stats(context.Background()); err != nil {
			failures++
		}
	}
	if failures != 3 {
		t.Errorf("got %d failures in 9 calls with failEvery=3, want 3", failures)
	}
}
