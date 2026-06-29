// Command node-agent is the data-plane entrypoint for mini-k8s.
// In Phase 1 it loads config, connects to Redis, emits a component_started
// log event, and idles. Later phases add node registration, process
// supervision, health probes, and telemetry.
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
		fmt.Fprintf(os.Stderr, "node-agent: failed to connect to Redis: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	// Create structured logger.
	logger := logging.New("node-agent", client)

	// Emit component_started event.
	logger.Info(ctx, "component_started",
		fmt.Sprintf("node-agent started, node_id=%s, port=%s", cfg.NodeID, cfg.Port),
		logging.NodeID(cfg.NodeID),
	)

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info(ctx, "component_stopped", "node-agent shutting down",
		logging.NodeID(cfg.NodeID),
	)
}
