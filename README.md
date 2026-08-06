# Go for DevOps

[![ci](https://github.com/Inblade/go-for-devops/actions/workflows/ci.yml/badge.svg)](https://github.com/Inblade/go-for-devops/actions/workflows/ci.yml)
[![Go 1.25](https://img.shields.io/badge/go-1.25-00ADD8?logo=go)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Small, idiomatic Go programs covering the patterns that come up repeatedly when
building infrastructure tooling: CLIs, Kubernetes clients, Prometheus exporters,
and bounded concurrent workers.

These are personal study notes and reference implementations, written from
hands-on experience running Go services and operational tooling in production.
Each example is deliberately small enough to read in one sitting, compiles, runs
standalone with no external infrastructure, and is commented to explain the
*reasoning* — particularly around the failure modes that only show up under
load or during a deploy: ignored cancellation, unbounded goroutines, errors that
lose their cause, and processes that drop in-flight work on SIGTERM.

All original material. No employer-specific content.

## Repository structure

```
go-for-devops/
├── examples/
│   ├── cli-with-cobra/
│   │   └── main.go              # Cobra CLI: RunE, persistent flags, testable output
│   ├── k8s-client-go/
│   │   └── main.go              # list pods: config loading, pagination, typed errors
│   ├── prometheus-exporter/
│   │   └── main.go              # custom Collector, up-metric, graceful shutdown
│   └── concurrent-worker-pool/
│       └── main.go              # errgroup + context, fail-fast vs collect-all
├── docs/
│   └── patterns.md              # context, errors, shutdown, concurrency, testing
├── go.mod
├── go.sum
├── LICENSE
└── README.md
```

## Requirements

Go 1.24 or newer (the module targets Go 1.25). Everything else is fetched by
`go mod download`.

## Building and running

```bash
go build ./...        # builds every example
go vet ./...
gofmt -l .            # should print nothing
```

Each example runs without any external dependency — no cluster and no Prometheus
server required.

### CLI with Cobra

```bash
go run ./examples/cli-with-cobra status --env staging
go run ./examples/cli-with-cobra rollout --env prod --revision 42 --dry-run
go run ./examples/cli-with-cobra status --env qa      # clean error, exit 1
```

Demonstrates `RunE` instead of `Run` (so errors propagate rather than calling
`os.Exit` deep in the command tree), `SilenceUsage`/`SilenceErrors` so a runtime
failure does not bury the message under the help text, an options struct passed
explicitly instead of package-level globals, and writing to `cmd.OutOrStdout()`
so commands can be tested against a `bytes.Buffer`.

### Kubernetes client-go

```bash
go run ./examples/k8s-client-go --namespace kube-system
go run ./examples/k8s-client-go --all-namespaces --selector app=nginx
go run ./examples/k8s-client-go --kubeconfig ~/.kube/config --context staging
```

Covers the config resolution order a well-behaved tool should follow (explicit
`--kubeconfig`, then in-cluster service account, then `$KUBECONFIG`/
`~/.kube/config`), raising `QPS`/`Burst` off the client-go defaults of 5/10
that throttle hard against any real cluster, server-side pagination via
`Limit`/`Continue`, per-call context timeouts, and classifying failures with
`apierrors.IsForbidden`/`IsNotFound` rather than matching error strings.

Without a reachable cluster it exits 1 with a clear message rather than
panicking.

### Prometheus exporter

```bash
go run ./examples/prometheus-exporter
curl -s localhost:9101/metrics | grep queue_
```

A custom `prometheus.Collector` that scrapes its backend on demand, so values
are always as fresh as the scrape and there is no poll interval to keep in sync.
It exports an `up`-style metric and a scrape-duration metric — without those, a
broken backend is indistinguishable from an empty one, since both produce no
series at all. Uses a private registry rather than the global default, bounds
the backend call with a context timeout, and shuts down gracefully on SIGTERM.

The bundled fake backend fails every 7th scrape so you can watch `queue_up` flip
to 0 and back.

### Concurrent worker pool

```bash
go run ./examples/concurrent-worker-pool -jobs 40 -workers 8 -fail-rate 0.2
go run ./examples/concurrent-worker-pool -mode collect-all
go run ./examples/concurrent-worker-pool -jobs 40 -workers 2 -timeout 300ms
go run -race ./examples/concurrent-worker-pool
```

Contrasts the two batch strategies directly: **fail-fast** via
`errgroup.WithContext`, where the first error cancels every sibling, and
**collect-all** via a plain `errgroup.Group` plus `errors.Join`, which runs
everything and reports all failures at once. Also shows `g.SetLimit` as
backpressure (`g.Go` blocks at the ceiling rather than spawning a goroutine per
job), and workers that select on `ctx.Done()` at every blocking point — without
which cancellation is merely advisory and `Wait()` blocks until the slowest job
finishes anyway.

Verified clean under `-race`.

## Tests

Every example has a test suite, because an example that claims a behaviour and
does not demonstrate it is just a comment.

```bash
go test -race ./...
```

What the tests actually pin down:

- **Worker pool** — that fail-fast really does cancel its siblings on the first
  error (the batch stops early instead of running all 50 jobs), that
  collect-all aggregates every error and `errors.Is` still reaches through the
  join, and that a deadline cuts the batch short rather than being honoured
  only after the last job finishes. Concurrency bounding is asserted from the
  outside: four jobs through one worker cannot beat four sequential minimum
  durations. All of it runs under `-race`.
- **Exporter** — a fake backend drives `testutil.CollectAndCompare` against
  exact expected exposition text. A failed scrape must emit `queue_up 0` *and*
  no queue series at all: a stale value is worse than a gap, because an alert
  believes it. A hanging backend must not hang the scrape.
- **Kubernetes client** — `client-go`'s fake clientset covers namespace and
  label filtering, and a reactor serves three pages so the continue-token loop
  is proven to follow pagination rather than returning page one. Every typed
  error path is checked for the message it produces, since "forbidden" without
  "check RBAC" costs someone ten minutes.
- **CLI** — the command tree is executed end to end with a buffer: persistent
  flags reaching subcommands, validation running before the command body,
  `NoArgs` rejecting positionals, and dry-run describing the plan instead of
  performing it.

Writing the CLI tests turned up a wrong claim in this repo's own comments:
`SilenceUsage` suppresses usage output for *every* error, including flag
parsing and unknown commands — not just errors returned from `RunE`. The
example now sets a `FlagErrorFunc` so a mistyped flag still gets its usage
text, and the comment says what actually happens.

## The notes

[`docs/patterns.md`](docs/patterns.md) is the written companion: context
propagation and the rules about cancellation being cooperative, error wrapping
with `%w` plus sentinels and custom types, graceful shutdown (including why
readiness must fail *before* you stop listening, and why `Shutdown` needs a
fresh context), bounded concurrency, and testing with consumer-defined
interfaces and injected clocks.

## Scope and non-goals

These are reference implementations, not libraries — copy the patterns, do not
import the packages. There is no attempt at a full CLI framework, a complete
exporter for any real system, or production-ready controller scaffolding
(use `controller-runtime` / `kubebuilder` for that).

The fake backends exist so the examples run anywhere; the point is the
surrounding structure, not the simulated data.

## License

MIT — see [LICENSE](LICENSE). Copyright (c) 2026 Danylo Kochetov.
