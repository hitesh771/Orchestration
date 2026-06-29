// Command api-server is the control-plane entrypoint for mini-k8s.
// In Phase 1 it loads config, connects to Redis, emits a component_started
// log event, and idles. Later phases add the REST API, scheduler, and controllers.
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
	"mini-k8s/internal/schema"
)

func main() {
	ctx := context.Background()

	// Load configuration.
	cfg := config.Load()

	// Connect to Redis — fail fast if unreachable.
	client, err := redisclient.New(cfg.RedisAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "api-server: failed to connect to Redis: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	// Create structured logger.
	logger := logging.New("api-server", client)

	// Smoke test: round-trip a test value through redisclient.
	if err := client.SetKey(ctx, schema.TestKey(), "api-server-smoke", 0); err != nil {
		logger.Error(ctx, "smoke_test_failed", fmt.Sprintf("failed to set test key: %v", err))
	} else {
		val, getErr := client.GetKey(ctx, schema.TestKey())
		if getErr != nil {
			logger.Error(ctx, "smoke_test_failed", fmt.Sprintf("failed to get test key: %v", getErr))
		} else {
			logger.Info(ctx, "smoke_test_passed", fmt.Sprintf("test key round-trip: %s", val))
		}
	}

	// Emit component_started event.
	logger.Info(ctx, "component_started", fmt.Sprintf("api-server started, listening on port %s", cfg.Port))

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info(ctx, "component_stopped", "api-server shutting down")
}
