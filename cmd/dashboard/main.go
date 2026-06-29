// Command dashboard is the UI entrypoint for mini-k8s.
// In Phase 1 it loads config, connects to Redis, emits a component_started
// log event, and idles. Phase 10 adds the static assets, SSE endpoint,
// and live cluster view.
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
