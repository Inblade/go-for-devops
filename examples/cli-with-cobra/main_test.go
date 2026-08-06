package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// execute runs the whole command tree the way main() does, capturing output.
func execute(t *testing.T, args ...string) (string, error) {
	t.Helper()

	var buf bytes.Buffer
	root := newRootCmd()
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetArgs(args)

	err := root.ExecuteContext(context.Background())
	return buf.String(), err
}

func TestStatusCommand(t *testing.T) {
	out, err := execute(t, "status")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "env=dev") {
		t.Errorf("output = %q, want the default environment", out)
	}
}

func TestPersistentFlagsReachTheSubcommand(t *testing.T) {
	out, err := execute(t, "status", "--env", "prod")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "env=prod") {
		t.Errorf("output = %q, want env=prod", out)
	}
}

// PersistentPreRunE is where validation belongs: it runs before every
// subcommand, so no subcommand can forget to check.
func TestUnknownEnvironmentIsRejectedBeforeTheCommandRuns(t *testing.T) {
	for _, args := range [][]string{
		{"status", "--env", "production"},
		{"rollout", "--revision", "5", "--env", "production"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			out, err := execute(t, args...)
			if err == nil {
				t.Fatal("expected an error for an invalid environment")
			}
			if !errors.Is(err, errUnknownEnv) {
				t.Errorf("error = %v, want it to wrap errUnknownEnv", err)
			}
			if strings.Contains(out, "status=healthy") || strings.Contains(out, "validating") {
				t.Errorf("the command ran despite failing validation: %q", out)
			}
		})
	}
}

// SilenceUsage exists so a runtime error does not bury its own message under
// a screen of usage text.
func TestRuntimeErrorsDoNotPrintUsage(t *testing.T) {
	out, err := execute(t, "status", "--env", "nope")
	if err == nil {
		t.Fatal("expected an error")
	}
	if strings.Contains(out, "Usage:") {
		t.Errorf("usage text was printed for a runtime error:\n%s", out)
	}
}

// A missing required flag reports without usage — the message names the flag,
// which is enough. This is validated after parsing, so the FlagErrorFunc hook
// does not see it.
func TestMissingRequiredFlagNamesTheFlag(t *testing.T) {
	_, err := execute(t, "rollout")
	if err == nil {
		t.Fatal("expected an error when --revision is missing")
	}
	if !strings.Contains(err.Error(), "revision") {
		t.Errorf("error = %v, want it to name the missing flag", err)
	}
}

// A mistyped flag is a genuine usage error, and SilenceUsage would otherwise
// swallow the usage text for it too. That is what SetFlagErrorFunc restores.
func TestUnknownFlagStillPrintsUsage(t *testing.T) {
	out, err := execute(t, "status", "--nosuchflag")
	if err == nil {
		t.Fatal("expected an error for an unknown flag")
	}
	if !strings.Contains(out, "Usage:") {
		t.Errorf("no usage text for a flag-parsing error:\n%s", out)
	}
}

func TestRolloutRejectsNonPositiveRevisions(t *testing.T) {
	for _, revision := range []string{"0", "-3"} {
		t.Run("revision="+revision, func(t *testing.T) {
			_, err := execute(t, "rollout", "--revision", revision)
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), "positive integer") {
				t.Errorf("error = %v, want it to explain the constraint", err)
			}
		})
	}
}

func TestRolloutDryRunDescribesTheePlanWithoutRunningIt(t *testing.T) {
	out, err := execute(t, "rollout", "--revision", "7", "--dry-run", "--env", "staging")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, want := range []string{"dry-run", "revision 7", "staging", "validating manifests"} {
		if !strings.Contains(out, want) {
			t.Errorf("output is missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "validating manifests...") {
		t.Error("dry-run performed the step instead of describing it")
	}
}

func TestSubcommandsRejectPositionalArguments(t *testing.T) {
	for _, args := range [][]string{
		{"status", "extra"},
		{"rollout", "--revision", "1", "extra"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			if _, err := execute(t, args...); err == nil {
				t.Fatal("expected cobra.NoArgs to reject a positional argument")
			}
		})
	}
}

func TestVerboseAddsDetail(t *testing.T) {
	plain, err := execute(t, "status")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	verbose, err := execute(t, "status", "-v")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(verbose) <= len(plain) {
		t.Errorf("verbose output (%d bytes) is not longer than plain (%d bytes)",
			len(verbose), len(plain))
	}
}

func TestRunStatusHonoursCancellationInWatchMode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	var buf bytes.Buffer
	err := runStatus(ctx, &buf, &options{env: "dev"}, true)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want it to wrap context.DeadlineExceeded", err)
	}
	if buf.Len() == 0 {
		t.Error("watch mode printed nothing before being cancelled")
	}
}

func TestRunRolloutStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var buf bytes.Buffer
	err := runRollout(ctx, &buf, &options{env: "prod"}, 3, false)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want it to wrap context.Canceled", err)
	}
	if strings.Contains(buf.String(), "pushing revision") {
		t.Error("rollout continued past the cancellation point")
	}
}

// Every command must be reachable and documented; a subcommand with no Short
// shows up as a blank line in --help.
func TestEveryCommandIsDocumented(t *testing.T) {
	root := newRootCmd()

	if len(root.Commands()) != 2 {
		t.Fatalf("root has %d subcommands, want 2", len(root.Commands()))
	}
	for _, cmd := range root.Commands() {
		if cmd.Short == "" {
			t.Errorf("subcommand %q has no Short description", cmd.Name())
		}
		if cmd.Args == nil {
			t.Errorf("subcommand %q does not declare an Args policy", cmd.Name())
		}
	}
}

func TestHelpIsAvailableAndExitsCleanly(t *testing.T) {
	for _, args := range [][]string{{"--help"}, {"status", "--help"}, {"rollout", "--help"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			out, err := execute(t, args...)
			if err != nil {
				t.Fatalf("--help returned an error: %v", err)
			}
			if !strings.Contains(out, "Usage:") {
				t.Errorf("help output has no usage section:\n%s", out)
			}
		})
	}
}
