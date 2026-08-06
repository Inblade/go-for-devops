// Command deployctl is a small example of a Cobra-based infrastructure CLI.
//
// It demonstrates the structure worth copying for real internal tooling:
//
//   - persistent flags bound on the root command
//   - a config struct threaded through instead of package-level globals
//   - RunE (not Run) so errors propagate rather than calling os.Exit deep in
//     the tree
//   - SilenceUsage/SilenceErrors so a runtime failure does not dump the help
//     text over the actual error message
//   - context cancellation wired to SIGINT/SIGTERM
//   - output written to cmd.OutOrStdout() so the commands are testable
//
// Build and run:
//
//	go run ./examples/cli-with-cobra status --env staging
//	go run ./examples/cli-with-cobra rollout --env prod --revision 42 --dry-run
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

// options holds everything the subcommands need. Passing this explicitly beats
// package-level globals: the commands stay testable and there is no hidden
// initialisation order to reason about.
type options struct {
	env     string
	timeout time.Duration
	verbose bool
}

// errUnknownEnv is a sentinel so callers (and tests) can match on it with
// errors.Is rather than comparing strings.
var errUnknownEnv = errors.New("unknown environment")

var validEnvs = map[string]bool{
	"dev":     true,
	"staging": true,
	"prod":    true,
}

func main() {
	// Translate signals into context cancellation once, at the top level.
	// Everything below only has to respect ctx.Done().
	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := newRootCmd().ExecuteContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	opts := &options{}

	root := &cobra.Command{
		Use:   "deployctl",
		Short: "Example deployment control CLI",
		Long: "deployctl is a worked example of a Cobra CLI structured the way " +
			"internal infrastructure tooling should be: explicit configuration, " +
			"errors returned rather than fatally logged, and context honoured " +
			"throughout.",
		// Without these, Cobra prints the full usage text after any RunE error,
		// which buries the real message under a screen of flags.
		//
		// Note that SilenceUsage suppresses usage for *every* error, including
		// flag-parsing failures and unknown commands — it is not limited to
		// errors returned from RunE. That is usually not what you want: a
		// mistyped flag is a usage error and the caller deserves the usage
		// text. SetFlagErrorFunc below restores it for that case.
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			if !validEnvs[opts.env] {
				return fmt.Errorf("%w: %q (want one of dev, staging, prod)",
					errUnknownEnv, opts.env)
			}
			return nil
		},
	}

	// A flag the caller got wrong is a usage error, so print usage for it even
	// though SilenceUsage is set. This hook fires for parsing failures only
	// (unknown flag, bad value); "required flag not set" is validated later
	// and still reports without usage, which is fine — that message already
	// names the flag.
	root.SetFlagErrorFunc(func(cmd *cobra.Command, err error) error {
		cmd.Println(cmd.UsageString())
		return err
	})

	root.PersistentFlags().StringVar(&opts.env, "env", "dev",
		"target environment (dev|staging|prod)")
	root.PersistentFlags().DurationVar(&opts.timeout, "timeout", 30*time.Second,
		"overall timeout for the operation")
	root.PersistentFlags().BoolVarP(&opts.verbose, "verbose", "v", false,
		"verbose output")

	root.AddCommand(newStatusCmd(opts), newRolloutCmd(opts))
	return root
}

func newStatusCmd(opts *options) *cobra.Command {
	var watch bool

	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show the current deployment status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), opts.timeout)
			defer cancel()
			return runStatus(ctx, cmd.OutOrStdout(), opts, watch)
		},
	}

	cmd.Flags().BoolVar(&watch, "watch", false, "poll until interrupted")
	return cmd
}

func newRolloutCmd(opts *options) *cobra.Command {
	var (
		revision int
		dryRun   bool
	)

	cmd := &cobra.Command{
		Use:   "rollout",
		Short: "Roll out a revision to the target environment",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if revision <= 0 {
				return fmt.Errorf("--revision must be a positive integer, got %d",
					revision)
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), opts.timeout)
			defer cancel()
			return runRollout(ctx, cmd.OutOrStdout(), opts, revision, dryRun)
		},
	}

	cmd.Flags().IntVar(&revision, "revision", 0, "revision number to deploy")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false,
		"print the plan without applying it")
	// Cobra enforces this before RunE, producing a proper usage error.
	_ = cmd.MarkFlagRequired("revision")

	return cmd
}

// runStatus is the actual behaviour, separated from the Cobra plumbing so it
// can be unit-tested by passing a bytes.Buffer as w.
func runStatus(ctx context.Context, w io.Writer, opts *options, watch bool) error {
	for {
		fmt.Fprintf(w, "env=%s status=healthy replicas=3/3\n", opts.env)
		if opts.verbose {
			fmt.Fprintf(w, "  last-transition=%s\n",
				time.Now().UTC().Format(time.RFC3339))
		}
		if !watch {
			return nil
		}

		select {
		case <-ctx.Done():
			// ctx.Err() is wrapped so callers can still use errors.Is against
			// context.Canceled / context.DeadlineExceeded.
			return fmt.Errorf("watch aborted: %w", ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}

func runRollout(ctx context.Context, w io.Writer, opts *options,
	revision int, dryRun bool) error {

	steps := []string{
		"validating manifests",
		"pushing revision",
		"waiting for readiness",
	}

	if dryRun {
		fmt.Fprintf(w, "dry-run: would deploy revision %d to %s\n",
			revision, opts.env)
		for _, s := range steps {
			fmt.Fprintf(w, "  - %s\n", s)
		}
		return nil
	}

	for _, s := range steps {
		// Check for cancellation between units of work. A long-running step
		// should also take ctx and honour it internally.
		select {
		case <-ctx.Done():
			return fmt.Errorf("rollout aborted during %q: %w", s, ctx.Err())
		default:
		}

		fmt.Fprintf(w, "%s...\n", s)
		time.Sleep(200 * time.Millisecond) // stand-in for real work
	}

	fmt.Fprintf(w, "revision %d deployed to %s\n", revision, opts.env)
	return nil
}
