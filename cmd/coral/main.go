// Command coral is the entrypoint for the yaop coral binary.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yaop-labs/coral/internal/app"
	"github.com/yaop-labs/coral/internal/buildinfo"
	"github.com/yaop-labs/coral/internal/config"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "coral:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("coral", flag.ContinueOnError)
	flags.SetOutput(stdout)
	configPath := flags.String("config", "", "path to YAML config (required)")
	showVersion := flags.Bool("version", false, "print version and exit")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}

	build := buildinfo.Current()
	if *showVersion {
		_, err := fmt.Fprintln(stdout, build.String())
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	logger.Info("coral build",
		"version", build.Version,
		"revision", build.Revision,
		"modified", build.Modified,
		"go_version", build.GoVersion,
	)

	if *configPath == "" {
		return fmt.Errorf("--config is required; refusing to start an unconfigured coral")
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	logger.Info("loaded config", "path", *configPath)

	a, err := app.New(cfg, logger)
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := a.Start(ctx); err != nil {
		return err
	}
	logger.Info("coral started")

	<-ctx.Done()
	logger.Info("shutdown signal received")

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer stopCancel()
	if err := a.Shutdown(stopCtx); err != nil {
		logger.Error("shutdown error", "err", err)
		return err
	}
	logger.Info("coral stopped")
	return nil
}
