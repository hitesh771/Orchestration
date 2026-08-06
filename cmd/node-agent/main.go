// Command node-agent is the data-plane entrypoint for mini-k8s.
//
// It registers this node's capacity, then continuously refreshes a liveness
// lease so the control plane can detect node death purely from the lease's
// expiry. Later phases add process supervision, health probes, and telemetry.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mini-k8s/internal/config"
	"mini-k8s/internal/logging"
	"mini-k8s/internal/node"
	"mini-k8s/internal/redisclient"
	"mini-k8s/internal/supervisor"
)

// shutdownGrace bounds the clean-shutdown work so a wedged Redis cannot hang
// the agent on exit.
const shutdownGrace = 5 * time.Second

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "node-agent: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := config.Load()

	// Connect to Redis — fail fast rather than running blind.
	client, err := redisclient.New(cfg.RedisAddr)
	if err != nil {
		return fmt.Errorf("connect to redis: %w", err)
	}
	defer client.Close()

	logger := logging.New("node-agent", client)

	// Install the signal handler before any long-running work so a signal
	// arriving during registration still triggers a clean shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	registrar, err := node.NewRegistrar(client, logger, cfg)
	if err != nil {
		return fmt.Errorf("configure node registrar: %w", err)
	}

	logger.Info(ctx, "component_started",
		fmt.Sprintf("node-agent starting, node_id=%s redis=%s", cfg.NodeID, cfg.RedisAddr),
		logging.NodeID(cfg.NodeID),
	)

	if err := registrar.Register(ctx); err != nil {
		return fmt.Errorf("register node: %w", err)
	}

	// Publish the first lease immediately so the node is visible to the
	// scheduler without waiting a full heartbeat interval.
	if err := registrar.Heartbeat(ctx); err != nil {
		return fmt.Errorf("publish initial lease: %w", err)
	}

	go registrar.RunHeartbeatLoop(ctx)

	sup := supervisor.NewSupervisor(client, logger, cfg.NodeID)

	// Reconcile before consuming any commands. A queued start command for a pod
	// that is already running would otherwise spawn a second process for it,
	// which is exactly the duplicate the identity checks exist to prevent.
	if _, err := sup.ReconcileOnStartup(ctx); err != nil {
		return fmt.Errorf("reconcile pods on startup: %w", err)
	}

	go sup.RunCommandConsumer(ctx)

	logger.Info(ctx, "node_ready",
		fmt.Sprintf("lease active, refreshing every %s with a %s TTL", cfg.HeartbeatInterval, cfg.LeaseTTL),
		logging.NodeID(cfg.NodeID),
	)

	<-ctx.Done()

	// The signal already cancelled ctx, so shutdown work needs a fresh one.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()

	if err := registrar.Deregister(shutdownCtx); err != nil {
		// A failed lease drop is not fatal: the lease expires on its own,
		// which reaches the same outcome a moment later.
		logger.Warn(shutdownCtx, "deregister_failed",
			fmt.Sprintf("could not drop lease, it will expire within %s: %v", cfg.LeaseTTL, err),
			logging.NodeID(cfg.NodeID),
		)
	}

	// Workload processes are deliberately left running. A restarting agent
	// re-adopts them, so killing them here would turn every agent restart into
	// an outage for workloads that are perfectly healthy. If the node is really
	// going away, its lease lapses and the control plane reschedules the pods.
	logger.Info(shutdownCtx, "component_stopped",
		"node-agent stopped, supervised processes left running for re-adoption",
		logging.NodeID(cfg.NodeID),
	)
	return nil
}
