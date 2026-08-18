// Command dashboard is a standalone liveness probe for the UI.
//
// The dashboard itself is served by api-server at GET /, with its snapshot and
// SSE endpoints at /state and /events. It lives there because it reads exactly
// the state the API already reads, and a second process would duplicate that
// wiring to render the same view. This binary remains as a way to confirm the
// configured Redis is reachable from wherever the UI is expected to run.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"mini-k8s/internal/config"
	"mini-k8s/internal/logging"
	"mini-k8s/internal/redisclient"
)

func main() {
	ctx := context.Background()

	// Load configuration.
	cfg := config.Load()

	// Connect to Redis — fail fast if unreachable.
	client, err := redisclient.New(cfg.RedisAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dashboard: failed to connect to Redis: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	// Create structured logger.
	logger := logging.New("dashboard", client)

	// Emit component_started event.
	logger.Info(ctx, "component_started",
		fmt.Sprintf("dashboard started, port=%s", cfg.Port),
	)

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info(ctx, "component_stopped", "dashboard shutting down")
}
