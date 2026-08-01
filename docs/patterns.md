# Go Patterns for Infrastructure Tooling

Notes on the handful of patterns that carry most of the weight in operational Go
— CLIs, exporters, controllers, and batch jobs. The emphasis is on the things
that cause production incidents rather than on language tutorials: leaked
goroutines, ignored cancellation, errors that lose their cause, and processes
that drop in-flight work when Kubernetes sends SIGTERM.

The four examples in `examples/` are the executable form of everything here.

---

## 1. Context propagation

### The rules

1. `context.Context` is the **first parameter**, always named `ctx`.
2. Never store a context in a struct field. It has request scope; structs
   usually do not. (The rare exception is a struct that *is* a request, and even
   then it is better to pass it.)
3. Never pass `nil` — use `context.TODO()` if you genuinely do not have one yet.
4. Only the function that creates a cancellable context calls `cancel`, and it
   does so with `defer`. Failing to call `cancel` leaks the timer and the
   goroutine tracking it until the parent is cancelled.

```go
func fetch(ctx context.Context, url string) ([]byte, error) {
    ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
    defer cancel() // required even on the success path

    req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
    if err != nil {
        return nil, fmt.Errorf("building request for %s: %w", url, err)
    }

    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return nil, fmt.Errorf("fetching %s: %w", url, err)
    }
    defer resp.Body.Close()

    return io.ReadAll(resp.Body)
}
```

`http.NewRequestWithContext` is the point people miss. Without it the context is
decorative: the request keeps running after cancellation, and a stuck upstream
holds the goroutine forever.

### Cancellation is cooperative

A context does not stop anything by itself. It closes a channel; your code has
to notice. Every blocking operation needs a `select` including `ctx.Done()`:

```go
select {
case <-ctx.Done():
    return ctx.Err()
case res := <-work:
    return handle(res)
}
```

A worker that just calls `time.Sleep` or blocks on an unbounded channel receive
ignores cancellation completely. The symptom is that shutdown "hangs" — really,
`Wait()` is blocking until the slowest task finishes on its own.

### Deadline vs timeout vs cancel

| Constructor | Use when |
|---|---|
| `context.WithTimeout(ctx, d)` | Bound the duration of one operation. |
| `context.WithDeadline(ctx, t)` | You have an absolute deadline (e.g. propagated from an inbound request). |
| `context.WithCancel(ctx)` | You cancel on some event rather than on time. |
| `context.WithoutCancel(ctx)` | Go 1.21+. Keep values but drop cancellation — for cleanup that must outlive the request. |
| `context.AfterFunc(ctx, f)` | Go 1.21+. Run `f` when ctx is done, without a goroutine of your own. |

Timeouts compose by shortening: a 5s child of a 2s parent expires at 2s. That is
what you want, and it is why per-call timeouts should be generous relative to
the top-level budget rather than the other way round.

### Shutdown needs a fresh context

The single most common shutdown bug:

```go
<-ctx.Done()                                  // ctx is now cancelled
srv.Shutdown(ctx)                             // returns instantly, drops connections

shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
srv.Shutdown(shutdownCtx)                     // correct: a fresh budget to drain
```

### Context values

Use them only for request-scoped metadata that crosses API boundaries — trace
IDs, request IDs, authenticated identity. Never for optional configuration or
dependencies; those are function parameters or struct fields.

Always use an unexported key type so no other package can collide with you:

```go
type ctxKey struct{}
var requestIDKey ctxKey

func WithRequestID(ctx context.Context, id string) context.Context {
    return context.WithValue(ctx, requestIDKey, id)
}

func RequestID(ctx context.Context) (string, bool) {
    id, ok := ctx.Value(requestIDKey).(string)
    return id, ok
}
```

A bare `context.WithValue(ctx, "id", v)` with a string key is a latent bug: any
package doing the same thing overwrites you silently.

---

## 2. Structured errors

### Wrap with `%w`, add context at every layer

```go
if err != nil {
    return fmt.Errorf("reading config %s: %w", path, err)
}
```

Each layer adds *what it was doing*, never repeats what the inner error already
says. The goal is a final message that reads as a chain:

```
loading cluster state: reading config /etc/app/config.yaml: open /etc/app/config.yaml: no such file or directory
```

Do not prefix with "failed to" or "error" — the fact that it is an error is
already established, and it produces `failed to X: failed to Y: failed to Z`.

Use `%w` when callers may want to inspect the cause; use `%v` to deliberately
*hide* the cause and keep it out of your API contract.

### Sentinel errors for expected conditions

```go
var ErrNotFound = errors.New("not found")

func Get(ctx context.Context, key string) (Value, error) {
    // ...
    return Value{}, fmt.Errorf("key %q: %w", key, ErrNotFound)
}

// caller
if errors.Is(err, ErrNotFound) {
    return defaultValue, nil
}
```

`errors.Is` walks the whole `%w` chain, so wrapping does not break the check.
Never compare with `==` on a wrapped error, and never match on `err.Error()`
substrings — that is a string-typed API that breaks on any message change.

### Custom error types when callers need data

```go
type ValidationError struct {
    Field  string
    Value  any
    Reason string
}

func (e *ValidationError) Error() string {
    return fmt.Sprintf("invalid %s=%v: %s", e.Field, e.Value, e.Reason)
}

// caller
var verr *ValidationError
if errors.As(err, &verr) {
    log.Error("validation failed", "field", verr.Field)
}
```

Rule of thumb: sentinel for "did this specific thing happen", custom type for
"I need details out of the failure".

Note the pointer receiver, and that `errors.As` takes a pointer to the target.
If you implement `Error()` on a value receiver but return `&T{}`, the type
assertions get confusing fast — pick pointer receivers and stay consistent.

### Aggregating errors

```go
var errs []error
for _, f := range files {
    if err := validate(f); err != nil {
        errs = append(errs, fmt.Errorf("%s: %w", f, err))
    }
}
return errors.Join(errs...)   // nil if errs is empty
```

`errors.Join` (Go 1.20+) keeps every error inspectable by `errors.Is`/`As` and
prints one per line. Use it whenever stopping at the first failure would force
the user into a fix-rerun-fix loop. `examples/concurrent-worker-pool` shows both
strategies side by side.

### Domain-specific classification

Prefer typed predicates over string matching. client-go is the model:

```go
switch {
case apierrors.IsNotFound(err):   // ...
case apierrors.IsForbidden(err):  // ...
case apierrors.IsConflict(err):   // retry with a fresh read
}
```

### Where to handle

Handle an error **once**. Either log it or return it, not both — double-logging
produces the same failure at five stack levels and makes the real origin
invisible. Only `main` (or the top of a request handler) both logs and decides
the exit status.

---

## 3. Graceful shutdown

Kubernetes sends SIGTERM, waits `terminationGracePeriodSeconds` (default 30),
then SIGKILL. A process that ignores SIGTERM drops in-flight work every deploy.

```go
func main() {
    ctx, stop := signal.NotifyContext(context.Background(),
        os.Interrupt, syscall.SIGTERM)
    defer stop()

    srv := &http.Server{
        Addr:              ":8080",
        Handler:           mux,
        ReadHeaderTimeout: 5 * time.Second, // mitigates Slowloris
    }

    go func() {
        if err := srv.ListenAndServe(); err != nil &&
            !errors.Is(err, http.ErrServerClosed) {
            log.Error("server failed", "err", err)
            stop()
        }
    }()

    <-ctx.Done()
    log.Info("shutdown signal received")

    shutdownCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
    defer cancel()

    if err := srv.Shutdown(shutdownCtx); err != nil {
        log.Error("graceful shutdown failed, forcing close", "err", err)
        _ = srv.Close()
    }
}
```

`signal.NotifyContext` (Go 1.16+) replaces the old `make(chan os.Signal, 1)` +
`signal.Notify` dance. Note the details:

- `http.ErrServerClosed` is the *normal* return from `ListenAndServe` after
  `Shutdown`. Treating it as a failure produces a spurious error on every clean
  exit.
- The shutdown budget must be **shorter** than
  `terminationGracePeriodSeconds`, or SIGKILL arrives mid-drain.
- `Shutdown` waits for active requests but does **not** close hijacked or
  WebSocket connections. Track those yourself.

### Sequencing matters

Real shutdown order for a service behind a load balancer:

1. Flip the readiness probe to failing.
2. **Wait a few seconds.** Endpoint removal propagates asynchronously to every
   kube-proxy/ingress; shutting down immediately still black-holes traffic. This
   sleep is the single most commonly missed step in "graceful" shutdown.
3. Stop accepting new connections (`srv.Shutdown`).
4. Drain in-flight work; flush buffered telemetry and logs.
5. Close databases, queues, and other clients — last, in reverse dependency
   order.

---

## 4. Concurrency

### errgroup over raw WaitGroup

`sync.WaitGroup` gives you no error propagation and no cancellation. Reach for
`golang.org/x/sync/errgroup`:

```go
g, ctx := errgroup.WithContext(ctx)
g.SetLimit(8)                 // bounded concurrency, no hand-rolled semaphore

for _, item := range items {
    g.Go(func() error {
        return process(ctx, item)   // must honour ctx
    })
}

if err := g.Wait(); err != nil {
    return fmt.Errorf("processing batch: %w", err)
}
```

Things to internalise:

- `errgroup.WithContext` cancels `ctx` when the **first** goroutine returns a
  non-nil error. Siblings only stop if they respect `ctx`.
- `g.Wait()` returns only the first error. Use `errors.Join` with a plain
  `errgroup.Group` when you need them all.
- `g.Go` **blocks** once the `SetLimit` ceiling is reached. That is the
  backpressure mechanism — it is not spawning a goroutine per item up front.
- Unbounded goroutine creation over a large input is a real outage cause: 100k
  concurrent HTTP calls will exhaust file descriptors and flatten the upstream.
  Always set a limit.

### Loop variables

Go 1.22 changed `for` loops so each iteration gets a fresh variable, and the old
`item := item` shadowing line is no longer needed. On **Go 1.21 and earlier it is
mandatory** — without it every goroutine sees the final value. Check the `go`
directive in `go.mod`, since that is what selects the semantics.

### Sharing results

Either a mutex-guarded slice or a channel; never a bare shared map (concurrent
map writes panic outright):

```go
var (
    mu      sync.Mutex
    results []Result
)
// inside the goroutine
mu.Lock()
results = append(results, r)
mu.Unlock()
```

Sort afterwards if determinism matters — completion order is arbitrary.

### Verify with the race detector

```bash
go test -race ./...
go build -race -o app . && ./app
```

It only reports races it actually observes, so run it against realistic
workloads. It is roughly 10x slower and uses far more memory — worth running in
CI, not in production.

### Timers

`time.After` allocates a timer that is not collected until it fires. Fine for a
short-lived select; a leak in a hot loop. Use an explicit timer there:

```go
timer := time.NewTimer(d)
defer timer.Stop()
select {
case <-ctx.Done():
    return ctx.Err()
case <-timer.C:
}
```

(Go 1.23 made unreferenced timers eligible for collection immediately, which
softens this, but the explicit form is still clearer and works on every version.)

---

## 5. Testing with interfaces

### Define interfaces at the consumer

The Go idiom is the opposite of Java's: the **package that uses** a dependency
declares the interface, and it declares the smallest one that does the job.

```go
// in the consuming package
type PodLister interface {
    List(ctx context.Context, ns string) ([]corev1.Pod, error)
}

type Reconciler struct {
    pods PodLister   // depends on the interface, not the concrete client
}
```

Small interfaces are trivial to fake. A fake for a 3-method interface is 20
lines; a fake for `kubernetes.Interface` is not something you write by hand.

### Hand-written fakes beat mock frameworks

```go
type fakePodLister struct {
    pods []corev1.Pod
    err  error
    calls int
}

func (f *fakePodLister) List(ctx context.Context, ns string) ([]corev1.Pod, error) {
    f.calls++
    if f.err != nil {
        return nil, f.err
    }
    return f.pods, nil
}
```

They are explicit, they compile, and they do not depend on codegen staying in
sync. Reserve generated mocks for genuinely large interfaces.

For Kubernetes specifically, use the upstream fakes rather than rolling your own:

```go
import "k8s.io/client-go/kubernetes/fake"

client := fake.NewSimpleClientset(&corev1.Pod{
    ObjectMeta: metav1.ObjectMeta{Name: "web-0", Namespace: "default"},
})
```

`fake.NewSimpleClientset` implements the whole `kubernetes.Interface` against an
in-memory object tracker, and supports reactors for injecting errors.

### Inject the seams: clock, randomness, I/O

Anything non-deterministic must be replaceable, or the test is flaky by
construction:

```go
type Service struct {
    now  func() time.Time   // time.Now in production, fixed in tests
    out  io.Writer          // os.Stdout in production, bytes.Buffer in tests
}
```

Writing to an `io.Writer` rather than `os.Stdout` is why the Cobra example's
`runStatus` takes a writer — the command becomes assertable without capturing
process output.

### Table-driven tests

```go
func TestPodStatus(t *testing.T) {
    tests := []struct {
        name string
        pod  corev1.Pod
        want string
    }{
        {name: "running", pod: runningPod(), want: "Running"},
        {name: "crashloop", pod: crashLoopPod(), want: "CrashLoopBackOff"},
        {name: "terminating", pod: terminatingPod(), want: "Terminating"},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            t.Parallel()
            if got := podStatus(&tt.pod); got != tt.want {
                t.Errorf("podStatus() = %q, want %q", got, tt.want)
            }
        })
    }
}
```

Use `t.Run` subtests so a failure names the case, and `t.Cleanup` instead of
`defer` for teardown that must run even when a helper calls `t.Fatal`.

### For HTTP, use httptest

```go
srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    w.WriteHeader(http.StatusServiceUnavailable)
}))
defer srv.Close()

_, err := fetch(context.Background(), srv.URL)
```

Real sockets, real client behaviour, no network dependency, no mocking of
`http.Client` internals.

---

## 6. Practical checklist

- [ ] `ctx` is the first parameter of every blocking function
- [ ] Every `context.With*` has a matching `defer cancel()`
- [ ] Every blocking operation selects on `ctx.Done()`
- [ ] `http.NewRequestWithContext`, never `http.NewRequest`
- [ ] Errors wrapped with `%w`, each layer adding what it was doing
- [ ] `errors.Is`/`errors.As` for classification, never string matching
- [ ] Errors handled once: logged or returned, not both
- [ ] SIGTERM handled; shutdown uses a **fresh** context; readiness flipped first
- [ ] Concurrency bounded (`SetLimit`), never unbounded over user input
- [ ] `go vet ./...` and `go test -race ./...` clean in CI
- [ ] Small consumer-defined interfaces at the seams
- [ ] Clock, randomness and I/O injected rather than called directly
