// Command podlist lists pods using client-go, with the config loading and
// context handling that production controllers and operators actually need.
//
// The parts worth copying:
//
//   - config resolution order: explicit --kubeconfig, then in-cluster, then
//     the standard client-go loading rules (KUBECONFIG, ~/.kube/config), with
//     kubectl-compatible --context override
//   - QPS/Burst raised off the client-go defaults (5/10), which throttle hard
//     against any real cluster
//   - a timeout on every API call via context
//   - server-side pagination so a namespace with 20k pods does not produce one
//     enormous response
//   - errors classified with k8s.io/apimachinery/pkg/api/errors instead of
//     string matching
//
// Run:
//
//	go run ./examples/k8s-client-go --namespace kube-system
//	go run ./examples/k8s-client-go --all-namespaces --selector app=nginx
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"text/tabwriter"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type config struct {
	kubeconfig    string
	kubecontext   string
	namespace     string
	allNamespaces bool
	selector      string
	timeout       time.Duration
	pageSize      int64
}

func main() {
	cfg := parseFlags()

	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() *config {
	cfg := &config{}

	flag.StringVar(&cfg.kubeconfig, "kubeconfig", "",
		"path to kubeconfig (default: in-cluster, then $KUBECONFIG, then ~/.kube/config)")
	flag.StringVar(&cfg.kubecontext, "context", "",
		"kubeconfig context to use (default: current-context)")
	flag.StringVar(&cfg.namespace, "namespace", "",
		"namespace to list (default: the namespace from the kubeconfig context)")
	flag.BoolVar(&cfg.allNamespaces, "all-namespaces", false,
		"list pods across all namespaces")
	flag.StringVar(&cfg.selector, "selector", "",
		"label selector, e.g. app=nginx,tier!=frontend")
	flag.DurationVar(&cfg.timeout, "timeout", 30*time.Second,
		"timeout for the API request")
	flag.Int64Var(&cfg.pageSize, "page-size", 500,
		"server-side pagination chunk size")

	flag.Parse()
	return cfg
}

func run(ctx context.Context, cfg *config) error {
	restCfg, defaultNS, err := buildRESTConfig(cfg)
	if err != nil {
		return fmt.Errorf("loading kubernetes configuration: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return fmt.Errorf("building clientset: %w", err)
	}

	ns := cfg.namespace
	if ns == "" {
		ns = defaultNS
	}
	if cfg.allNamespaces {
		ns = metav1.NamespaceAll
	}

	ctx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()

	pods, err := listPods(ctx, clientset, ns, cfg.selector, cfg.pageSize)
	if err != nil {
		return err
	}

	printPods(os.Stdout, pods, cfg.allNamespaces)
	return nil
}

// buildRESTConfig resolves the client configuration in the order a well-behaved
// tool should: an explicit path wins, then in-cluster service account
// credentials, then the standard client-go loading rules.
//
// It also returns the namespace implied by the kubeconfig context, so the tool
// defaults to the same namespace kubectl would use.
func buildRESTConfig(cfg *config) (*rest.Config, string, error) {
	var (
		restCfg   *rest.Config
		defaultNS = metav1.NamespaceDefault
		err       error
	)

	if cfg.kubeconfig == "" && cfg.kubecontext == "" {
		// rest.InClusterConfig reads the projected service account token and
		// the CA bundle mounted at /var/run/secrets/kubernetes.io/serviceaccount.
		// It returns ErrNotInCluster when those are absent, which is the signal
		// to fall through to kubeconfig loading.
		if restCfg, err = rest.InClusterConfig(); err == nil {
			// In-cluster, the namespace comes from the mounted token directory.
			if ns, nsErr := os.ReadFile(
				"/var/run/secrets/kubernetes.io/serviceaccount/namespace",
			); nsErr == nil && len(ns) > 0 {
				defaultNS = string(ns)
			}
			tuneRateLimits(restCfg)
			return restCfg, defaultNS, nil
		} else if err != rest.ErrNotInCluster {
			return nil, "", fmt.Errorf("in-cluster config: %w", err)
		}
	}

	// Standard loading rules: --kubeconfig, then $KUBECONFIG (colon-separated,
	// merged), then ~/.kube/config.
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if cfg.kubeconfig != "" {
		loadingRules.ExplicitPath = cfg.kubeconfig
	}

	overrides := &clientcmd.ConfigOverrides{}
	if cfg.kubecontext != "" {
		overrides.CurrentContext = cfg.kubecontext
	}

	clientConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadingRules, overrides)

	if defaultNS, _, err = clientConfig.Namespace(); err != nil {
		defaultNS = metav1.NamespaceDefault
	}

	if restCfg, err = clientConfig.ClientConfig(); err != nil {
		return nil, "", err
	}

	tuneRateLimits(restCfg)
	return restCfg, defaultNS, nil
}

// tuneRateLimits raises the client-side throttle. client-go defaults to QPS 5
// and Burst 10, which is far too low for anything that lists more than a
// handful of objects; the symptom is multi-second stalls with
// "Waited for ..." warnings rather than an outright error.
//
// Raise this deliberately: the API server has its own priority and fairness
// (APF) protections, but a badly behaved controller can still cause damage.
func tuneRateLimits(cfg *rest.Config) {
	cfg.QPS = 50
	cfg.Burst = 100
	cfg.UserAgent = "podlist/1.0 (go-for-devops example)"
}

// listPods pages through the pod list rather than requesting everything at
// once. Without Limit/Continue, a large namespace produces a single huge
// response that can exhaust the API server's memory budget for the request.
func listPods(ctx context.Context, client kubernetes.Interface,
	namespace, selector string, pageSize int64) ([]corev1.Pod, error) {

	var (
		all           []corev1.Pod
		continueToken string
	)

	for {
		opts := metav1.ListOptions{
			LabelSelector: selector,
			Limit:         pageSize,
			Continue:      continueToken,
		}

		list, err := client.CoreV1().Pods(namespace).List(ctx, opts)
		if err != nil {
			// Classify with the typed helpers, never by matching on the
			// error string.
			switch {
			case apierrors.IsForbidden(err):
				return nil, fmt.Errorf(
					"forbidden listing pods in %q (check RBAC for this service account): %w",
					namespace, err)
			case apierrors.IsNotFound(err):
				return nil, fmt.Errorf("namespace %q not found: %w", namespace, err)
			case apierrors.IsTimeout(err), apierrors.IsServerTimeout(err):
				return nil, fmt.Errorf("api server timeout: %w", err)
			case apierrors.IsResourceExpired(err):
				// The continue token aged out mid-pagination; a real tool
				// would restart the listing here.
				return nil, fmt.Errorf("pagination token expired, retry the list: %w", err)
			default:
				return nil, fmt.Errorf("listing pods in %q: %w", namespace, err)
			}
		}

		all = append(all, list.Items...)

		continueToken = list.Continue
		if continueToken == "" {
			break
		}
	}

	return all, nil
}

func printPods(w io.Writer, pods []corev1.Pod, showNamespace bool) {
	tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
	defer tw.Flush()

	if showNamespace {
		fmt.Fprintln(tw, "NAMESPACE\tNAME\tREADY\tSTATUS\tNODE\tAGE")
	} else {
		fmt.Fprintln(tw, "NAME\tREADY\tSTATUS\tNODE\tAGE")
	}

	for i := range pods {
		p := &pods[i]
		ready, total := readyContainers(p)
		age := time.Since(p.CreationTimestamp.Time).Truncate(time.Second)

		if showNamespace {
			fmt.Fprintf(tw, "%s\t%s\t%d/%d\t%s\t%s\t%s\n",
				p.Namespace, p.Name, ready, total, podStatus(p),
				p.Spec.NodeName, age)
		} else {
			fmt.Fprintf(tw, "%s\t%d/%d\t%s\t%s\t%s\n",
				p.Name, ready, total, podStatus(p), p.Spec.NodeName, age)
		}
	}

	fmt.Fprintf(tw, "\n%d pod(s)\n", len(pods))
}

func readyContainers(p *corev1.Pod) (ready, total int) {
	total = len(p.Status.ContainerStatuses)
	for _, cs := range p.Status.ContainerStatuses {
		if cs.Ready {
			ready++
		}
	}
	return ready, total
}

// podStatus approximates the column kubectl shows. Pod.Status.Phase alone is
// not enough: a CrashLoopBackOff pod is still phase Running, and a terminating
// pod keeps its old phase until it is gone.
func podStatus(p *corev1.Pod) string {
	if p.DeletionTimestamp != nil {
		return "Terminating"
	}

	for _, cs := range p.Status.ContainerStatuses {
		switch {
		case cs.State.Waiting != nil && cs.State.Waiting.Reason != "":
			return cs.State.Waiting.Reason // ImagePullBackOff, CrashLoopBackOff, ...
		case cs.State.Terminated != nil && cs.State.Terminated.Reason != "":
			return cs.State.Terminated.Reason // OOMKilled, Error, Completed
		}
	}

	if p.Status.Reason != "" {
		return p.Status.Reason // Evicted, NodeAffinity, ...
	}

	return string(p.Status.Phase)
}
