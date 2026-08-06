package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func pod(name, namespace string, opts ...func(*corev1.Pod)) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         namespace,
			CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Hour)),
		},
		Spec:   corev1.PodSpec{NodeName: "node-1"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func withLabels(labels map[string]string) func(*corev1.Pod) {
	return func(p *corev1.Pod) { p.Labels = labels }
}

func withContainers(statuses ...corev1.ContainerStatus) func(*corev1.Pod) {
	return func(p *corev1.Pod) { p.Status.ContainerStatuses = statuses }
}

func ready(name string, isReady bool) corev1.ContainerStatus {
	return corev1.ContainerStatus{Name: name, Ready: isReady}
}

func waiting(reason string) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}},
	}
}

func terminated(reason string) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		State: corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{Reason: reason},
		},
	}
}

func TestListPodsReturnsPodsInTheNamespace(t *testing.T) {
	client := fake.NewSimpleClientset(
		pod("web-1", "default"),
		pod("web-2", "default"),
		pod("other", "kube-system"),
	)

	pods, err := listPods(context.Background(), client, "default", "", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pods) != 2 {
		t.Fatalf("got %d pods, want 2", len(pods))
	}
	for _, p := range pods {
		if p.Namespace != "default" {
			t.Errorf("pod %s came from namespace %s", p.Name, p.Namespace)
		}
	}
}

func TestListPodsAppliesTheLabelSelector(t *testing.T) {
	client := fake.NewSimpleClientset(
		pod("api-1", "default", withLabels(map[string]string{"app": "api"})),
		pod("web-1", "default", withLabels(map[string]string{"app": "web"})),
	)

	pods, err := listPods(context.Background(), client, "default", "app=api", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pods) != 1 || pods[0].Name != "api-1" {
		t.Fatalf("got %v, want just api-1", podNames(pods))
	}
}

func TestListPodsAcrossAllNamespaces(t *testing.T) {
	client := fake.NewSimpleClientset(
		pod("web-1", "default"),
		pod("kube-dns", "kube-system"),
	)

	pods, err := listPods(context.Background(), client, metav1.NamespaceAll, "", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pods) != 2 {
		t.Errorf("got %d pods across all namespaces, want 2", len(pods))
	}
}

// The pagination loop is the reason this function is not a one-liner, so it is
// worth proving it follows a continue token instead of returning page one.
func TestListPodsFollowsTheContinueToken(t *testing.T) {
	client := fake.NewSimpleClientset()

	// The fake client does not implement Limit/Continue, so the pages are
	// served by hand: two pages carrying a continue token, then a final one
	// without. A function that ignored the token would stop after page one.
	calls := 0
	client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		call := calls
		calls++

		switch call {
		case 0:
			return true, &corev1.PodList{
				ListMeta: metav1.ListMeta{Continue: "page-2"},
				Items:    []corev1.Pod{*pod("web-1", "default")},
			}, nil
		case 1:
			return true, &corev1.PodList{
				ListMeta: metav1.ListMeta{Continue: "page-3"},
				Items:    []corev1.Pod{*pod("web-2", "default")},
			}, nil
		default:
			return true, &corev1.PodList{
				Items: []corev1.Pod{*pod("web-3", "default")},
			}, nil
		}
	})

	pods, err := listPods(context.Background(), client, "default", "", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pods) != 3 {
		t.Fatalf("got %d pods (%v), want all 3 pages", len(pods), podNames(pods))
	}
	if calls != 3 {
		t.Errorf("made %d list calls, want 3", calls)
	}
}

// Errors must be classified with the typed helpers, and the message has to say
// what to do about it — "forbidden" without "check RBAC" wastes ten minutes.
func TestListPodsClassifiesAPIErrors(t *testing.T) {
	resource := schema.GroupResource{Resource: "pods"}

	tests := []struct {
		name        string
		apiErr      error
		wantMessage string
	}{
		{
			name:        "forbidden points at RBAC",
			apiErr:      apierrors.NewForbidden(resource, "", errors.New("nope")),
			wantMessage: "check RBAC",
		},
		{
			name:        "not found names the namespace",
			apiErr:      apierrors.NewNotFound(schema.GroupResource{Resource: "namespaces"}, "ghost"),
			wantMessage: `namespace "ghost" not found`,
		},
		{
			name:        "server timeout is reported as such",
			apiErr:      apierrors.NewServerTimeout(resource, "list", 1),
			wantMessage: "api server timeout",
		},
		{
			name:        "expired token says to retry",
			apiErr:      apierrors.NewResourceExpired("continue token expired"),
			wantMessage: "retry the list",
		},
		{
			name:        "anything else is wrapped with context",
			apiErr:      errors.New("connection refused"),
			wantMessage: "listing pods",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			client.PrependReactor("list", "pods",
				func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, tt.apiErr
				})

			namespace := "default"
			if strings.Contains(tt.wantMessage, "ghost") {
				namespace = "ghost"
			}

			_, err := listPods(context.Background(), client, namespace, "", 100)
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tt.wantMessage) {
				t.Errorf("error = %q, want it to mention %q", err, tt.wantMessage)
			}
			if !errors.Is(err, tt.apiErr) {
				t.Errorf("error does not wrap the original API error")
			}
		})
	}
}

func TestPodStatus(t *testing.T) {
	tests := []struct {
		name string
		pod  *corev1.Pod
		want string
	}{
		{
			name: "running pod reports its phase",
			pod:  pod("web", "default"),
			want: "Running",
		},
		{
			// A pod being deleted keeps its old phase until it is gone, which
			// is why phase alone is not enough for this column.
			name: "deleted pod is Terminating",
			pod: pod("web", "default", func(p *corev1.Pod) {
				now := metav1.Now()
				p.DeletionTimestamp = &now
			}),
			want: "Terminating",
		},
		{
			// CrashLoopBackOff is still phase Running.
			name: "waiting reason wins over phase",
			pod:  pod("web", "default", withContainers(waiting("CrashLoopBackOff"))),
			want: "CrashLoopBackOff",
		},
		{
			name: "image pull failure is surfaced",
			pod:  pod("web", "default", withContainers(waiting("ImagePullBackOff"))),
			want: "ImagePullBackOff",
		},
		{
			name: "terminated reason is surfaced",
			pod:  pod("web", "default", withContainers(terminated("OOMKilled"))),
			want: "OOMKilled",
		},
		{
			name: "pod-level reason is used when containers say nothing",
			pod: pod("web", "default", func(p *corev1.Pod) {
				p.Status.Reason = "Evicted"
			}),
			want: "Evicted",
		},
		{
			name: "the first informative container wins",
			pod: pod("web", "default", withContainers(
				ready("sidecar", true),
				waiting("CrashLoopBackOff"),
			)),
			want: "CrashLoopBackOff",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := podStatus(tt.pod); got != tt.want {
				t.Errorf("podStatus() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestReadyContainers(t *testing.T) {
	tests := []struct {
		name             string
		pod              *corev1.Pod
		wantReady, total int
	}{
		{
			name: "all ready",
			pod: pod("web", "default", withContainers(
				ready("app", true), ready("sidecar", true))),
			wantReady: 2, total: 2,
		},
		{
			name: "partially ready",
			pod: pod("web", "default", withContainers(
				ready("app", true), ready("sidecar", false))),
			wantReady: 1, total: 2,
		},
		{
			name:      "no container statuses yet",
			pod:       pod("web", "default"),
			wantReady: 0, total: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotReady, gotTotal := readyContainers(tt.pod)
			if gotReady != tt.wantReady || gotTotal != tt.total {
				t.Errorf("readyContainers() = %d/%d, want %d/%d",
					gotReady, gotTotal, tt.wantReady, tt.total)
			}
		})
	}
}

func TestPrintPods(t *testing.T) {
	pods := []corev1.Pod{
		*pod("web-1", "default", withContainers(ready("app", true))),
		*pod("api-1", "backend", withContainers(ready("app", true), ready("sidecar", false))),
	}

	t.Run("without the namespace column", func(t *testing.T) {
		var buf bytes.Buffer
		printPods(&buf, pods, false)
		out := buf.String()

		if strings.Contains(out, "NAMESPACE") {
			t.Error("namespace column present when it was not requested")
		}
		for _, want := range []string{"NAME", "READY", "web-1", "1/1", "api-1", "1/2", "2 pod(s)"} {
			if !strings.Contains(out, want) {
				t.Errorf("output is missing %q:\n%s", want, out)
			}
		}
	})

	t.Run("with the namespace column", func(t *testing.T) {
		var buf bytes.Buffer
		printPods(&buf, pods, true)
		out := buf.String()

		for _, want := range []string{"NAMESPACE", "default", "backend"} {
			if !strings.Contains(out, want) {
				t.Errorf("output is missing %q:\n%s", want, out)
			}
		}
	})

	t.Run("an empty list still prints a header and a count", func(t *testing.T) {
		var buf bytes.Buffer
		printPods(&buf, nil, false)
		out := buf.String()

		if !strings.Contains(out, "NAME") || !strings.Contains(out, "0 pod(s)") {
			t.Errorf("unexpected output for an empty list:\n%s", out)
		}
	})
}

func podNames(pods []corev1.Pod) []string {
	names := make([]string, 0, len(pods))
	for _, p := range pods {
		names = append(names, p.Name)
	}
	return names
}
