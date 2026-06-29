// Command coral is the entrypoint for the yaop coral binary.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yaop-labs/coral/internal/app"
	"github.com/yaop-labs/coral/internal/config"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "coral:", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "", "path to YAML config (required)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

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
