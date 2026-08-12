// Command api-server is the control-plane entrypoint for mini-k8s.
//
// It serves the REST API used to declare deployments and inspect cluster
// state. Later phases add the reconciliation controllers, the autoscaler, and
// the dashboard's SSE stream to this same process.
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mini-k8s/internal/api"
	"mini-k8s/internal/config"
	"mini-k8s/internal/controller"
	"mini-k8s/internal/logging"
	"mini-k8s/internal/redisclient"
)

const (
	// readHeaderTimeout bounds how long a client may take to send headers,
	// so an idle or slow connection cannot occupy a handler indefinitely.
	readHeaderTimeout = 10 * time.Second
	shutdownGrace     = 10 * time.Second
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "api-server: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := config.Load()

	client, err := redisclient.New(cfg.RedisAddr)
	if err != nil {
		return fmt.Errorf("connect to redis: %w", err)
	}
	defer client.Close()

	logger := logging.New("api-server", client)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Bound to loopback by default. The API is unauthenticated and a
	// deployment's exec_path is executed on worker nodes, so exposing this
	// port is equivalent to granting remote code execution on the cluster.
	// Overriding BIND_ADDR is an explicit decision, not the default.
	bindAddr := os.Getenv("BIND_ADDR")
	if bindAddr == "" {
		bindAddr = "127.0.0.1"
	}
	addr := net.JoinHostPort(bindAddr, cfg.Port)

	srv := &http.Server{
		Addr:              addr,
		Handler:           api.NewServer(client, logger).Routes(),
		ReadHeaderTimeout: readHeaderTimeout,
	}

	logger.Info(ctx, "component_started",
		fmt.Sprintf("api-server listening on %s, redis=%s, reconcile every %s",
			addr, cfg.RedisAddr, cfg.ReconcileInterval))

	if bindAddr != "127.0.0.1" && bindAddr != "localhost" {
		logger.Warn(ctx, "api_exposed_beyond_loopback",
			fmt.Sprintf("bound to %s: this API is unauthenticated and runs exec_path on workers", bindAddr))
	}

	// Reconciliation runs in this process rather than a separate binary: it
	// shares nothing with the HTTP handlers except Redis, and one fewer process
	// is one fewer thing to supervise.
	go controller.NewReplicaController(client, logger, cfg.ReconcileInterval).Run(ctx)
	go controller.NewHealthController(client, logger, cfg.ReconcileInterval).Run(ctx)

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("serve http: %w", err)
		}
		return nil
	case <-ctx.Done():
	}

	// The signal already cancelled ctx, so draining needs a fresh one.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown http server: %w", err)
	}

	logger.Info(shutdownCtx, "component_stopped", "api-server stopped")
	return nil
}
