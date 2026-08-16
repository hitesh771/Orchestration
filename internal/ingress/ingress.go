// Package ingress keeps an Nginx upstream configuration in step with the pods
// that are actually running.
//
// The controller is dual-mode like the others: it reacts to deployment events
// for latency and re-derives the whole configuration on a timer for
// correctness. Nothing is computed incrementally from event payloads. An
// upstream list rebuilt from scratch cannot drift, whereas one patched per event
// is wrong forever the first time an event is missed — and Redis Pub/Sub gives
// no delivery guarantee.
package ingress

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"mini-k8s/internal/events"
	"mini-k8s/internal/logging"
	"mini-k8s/internal/redisclient"
	"mini-k8s/internal/schema"
)

// Config tunes the ingress controller.
type Config struct {
	// ConfigPath is the file the generated upstream blocks are written to.
	ConfigPath string
	// ReloadCommand is argv for the reload; empty means "nginx -s reload".
	ReloadCommand []string
	// Debounce is the window over which bursts of changes are collapsed.
	Debounce time.Duration
	// ResyncInterval is how often the configuration is rebuilt regardless of
	// events, so a missed event costs one interval rather than being permanent.
	ResyncInterval time.Duration
}

// reloadTimeout bounds the reload command. A reload that hangs would stall
// every later change behind it, so it is abandoned rather than waited on.
const reloadTimeout = 10 * time.Second

// Controller regenerates and reloads the ingress configuration.
type Controller struct {
	client *redisclient.Client
	logger *logging.Logger
	cfg    Config

	// lastWritten is the last configuration successfully written. A rebuild
	// that produces identical output skips the reload: a reload drops
	// in-flight connection handling for no benefit if nothing changed.
	lastWritten string
}

// New builds a controller, filling in the spec's defaults.
func New(client *redisclient.Client, logger *logging.Logger, cfg Config) *Controller {
	if cfg.Debounce <= 0 {
		cfg.Debounce = 2 * time.Second
	}
	if cfg.ResyncInterval <= 0 {
		cfg.ResyncInterval = 30 * time.Second
	}
	if cfg.ConfigPath == "" {
		cfg.ConfigPath = "/tmp/mini-k8s-upstreams.conf"
	}
	if len(cfg.ReloadCommand) == 0 {
		cfg.ReloadCommand = []string{"nginx", "-s", "reload"}
	}
	return &Controller{client: client, logger: logger, cfg: cfg}
}

// Run watches for changes and keeps the configuration current.
//
// Events do not trigger a rebuild directly; they arm a debounce timer. A scale
// from 2 to 10 replicas publishes a burst of changes within a second, and
// rebuilding per event would mean ten rewrites and ten reloads to reach one
// final state. The timer collapses that into a single reload once the burst
// settles.
func (c *Controller) Run(ctx context.Context) {
	if err := c.Sync(ctx); err != nil && ctx.Err() == nil {
		c.logger.Error(ctx, "ingress_sync_failed", err.Error())
	}

	changes := make(chan struct{}, 1)
	go c.watch(ctx, changes)

	resync := time.NewTicker(c.cfg.ResyncInterval)
	defer resync.Stop()

	// A nil channel blocks forever in a select, which is how the debounce timer
	// stays disarmed until a change actually arrives.
	var debounce <-chan time.Time

	for {
		select {
		case <-ctx.Done():
			return
		case <-changes:
			// Restarting the timer on each change is deliberate: the window
			// should close once the burst goes quiet, not at a fixed offset
			// from whichever event happened to arrive first.
			debounce = time.After(c.cfg.Debounce)
		case <-debounce:
			debounce = nil
			if err := c.Sync(ctx); err != nil && ctx.Err() == nil {
				c.logger.Error(ctx, "ingress_sync_failed", err.Error())
			}
		case <-resync.C:
			if err := c.Sync(ctx); err != nil && ctx.Err() == nil {
				c.logger.Error(ctx, "ingress_sync_failed", err.Error())
			}
		}
	}
}

// watch signals the debounce loop whenever a deployment event arrives.
func (c *Controller) watch(ctx context.Context, changes chan<- struct{}) {
	for ctx.Err() == nil {
		sub, err := c.client.Subscribe(ctx, schema.DeploymentEventsChannel())
		if err != nil {
			c.logger.Error(ctx, "ingress_subscribe_failed", err.Error())
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		ch := sub.Channel()
	drain:
		for {
			select {
			case <-ctx.Done():
				_ = sub.Close()
				return
			case msg, ok := <-ch:
				if !ok {
					break drain
				}
				if _, err := events.Decode(msg.Payload); err != nil {
					continue
				}
				// The payload is only a hint that something changed; the
				// rebuild reads the real state. A full buffer already means a
				// rebuild is pending, so dropping the signal loses nothing.
				select {
				case changes <- struct{}{}:
				default:
				}
			}
		}
		_ = sub.Close()
	}
}

// Sync rebuilds the configuration from current state and reloads if it changed.
func (c *Controller) Sync(ctx context.Context) error {
	backends, err := c.backends(ctx)
	if err != nil {
		return err
	}

	rendered := Render(backends)
	if rendered == c.lastWritten {
		return nil
	}

	if err := c.write(rendered); err != nil {
		return err
	}
	c.lastWritten = rendered

	total := 0
	for _, ports := range backends {
		total += len(ports)
	}
	c.logger.Info(ctx, "ingress_config_written",
		fmt.Sprintf("%d deployment(s), %d backend(s) at %s",
			len(backends), total, c.cfg.ConfigPath))

	return c.reload(ctx)
}

// backends maps each deployment to the host ports of its serving pods.
//
// Only Running pods with a port are included. A Pending or CrashLoopBackOff pod
// has nothing listening, and routing traffic to it would turn a scaling event
// into a burst of connection errors.
func (c *Controller) backends(ctx context.Context) (map[string][]int, error) {
	keys, err := c.client.ScanKeys(ctx, schema.PodKeyPattern())
	if err != nil {
		return nil, fmt.Errorf("scan pods: %w", err)
	}

	backends := make(map[string][]int)
	for _, key := range keys {
		fields, err := c.client.HashGet(ctx, key)
		if err != nil || len(fields) == 0 {
			continue
		}
		pod, err := schema.MapToPod(fields)
		if err != nil || pod.Deployment == "" {
			continue
		}
		if pod.Status != schema.StatusRunning || pod.HostPort <= 0 {
			continue
		}
		backends[pod.Deployment] = append(backends[pod.Deployment], pod.HostPort)
	}

	// Sorting keeps the output stable. Key-scan order is arbitrary, so an
	// unsorted render would differ between runs over identical state and
	// trigger a reload every resync.
	for name := range backends {
		sort.Ints(backends[name])
	}
	return backends, nil
}

// Render produces the upstream and server blocks for the given backends.
//
// Pods share their node's network namespace, so a backend is a host port on
// loopback rather than a pod IP as it would be in real Kubernetes.
func Render(backends map[string][]int) string {
	names := make([]string, 0, len(backends))
	for name := range backends {
		names = append(names, name)
	}
	sort.Strings(names)

	var b strings.Builder
	b.WriteString("# Generated by mini-k8s. Do not edit: rewritten on every change.\n")

	for _, name := range names {
		ports := backends[name]
		if len(ports) == 0 {
			// An upstream with no servers is a config error in Nginx, so a
			// deployment with nothing running is omitted entirely.
			continue
		}
		upstream := upstreamName(name)
		fmt.Fprintf(&b, "\nupstream %s {\n", upstream)
		for _, port := range ports {
			fmt.Fprintf(&b, "    server 127.0.0.1:%d max_fails=2 fail_timeout=5s;\n", port)
		}
		b.WriteString("}\n")
	}

	for _, name := range names {
		if len(backends[name]) == 0 {
			continue
		}
		fmt.Fprintf(&b, "\nlocation /%s/ {\n", name)
		fmt.Fprintf(&b, "    proxy_pass http://%s/;\n", upstreamName(name))
		b.WriteString("    proxy_set_header Host $host;\n")
		b.WriteString("    proxy_next_upstream error timeout http_502 http_503;\n")
		b.WriteString("}\n")
	}
	return b.String()
}

// upstreamName derives an Nginx-safe upstream name from a deployment name.
func upstreamName(deployment string) string {
	return "minik8s_" + strings.ReplaceAll(deployment, "-", "_")
}

// write replaces the configuration file atomically.
//
// Nginx may read the file at any moment, including during a reload triggered by
// something else. A truncate-then-write would expose a partial config; a rename
// over the old file never does.
func (c *Controller) write(content string) error {
	dir := filepath.Dir(c.cfg.ConfigPath)
	tmp, err := os.CreateTemp(dir, ".upstreams-*.conf")
	if err != nil {
		return fmt.Errorf("create temp config in %s: %w", dir, err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op once the rename succeeds

	if _, err := tmp.WriteString(content); err != nil {
		tmp.Close()
		return fmt.Errorf("write temp config: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp config: %w", err)
	}
	if err := os.Chmod(tmpName, 0o644); err != nil {
		return fmt.Errorf("chmod temp config: %w", err)
	}
	if err := os.Rename(tmpName, c.cfg.ConfigPath); err != nil {
		return fmt.Errorf("install config at %s: %w", c.cfg.ConfigPath, err)
	}
	return nil
}

// reload asks Nginx to pick up the new configuration.
//
// A missing Nginx is reported once per sync and is not an error: the generated
// configuration is still correct and on disk, so a cluster running without an
// ingress router keeps working for direct-to-port clients. Failing the sync
// instead would make every later change retry a command that will never exist.
func (c *Controller) reload(ctx context.Context) error {
	bin := c.cfg.ReloadCommand[0]
	if _, err := exec.LookPath(bin); err != nil {
		c.logger.Warn(ctx, "ingress_reload_unavailable",
			fmt.Sprintf("%s not found: config written but not reloaded", bin))
		return nil
	}

	cmdCtx, cancel := context.WithTimeout(ctx, reloadTimeout)
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, bin, c.cfg.ReloadCommand[1:]...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		if errors.Is(cmdCtx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("reload timed out after %s", reloadTimeout)
		}
		return fmt.Errorf("reload failed: %w: %s", err, strings.TrimSpace(string(out)))
	}

	c.logger.Info(ctx, "ingress_reloaded", "nginx picked up the new upstreams")
	return nil
}
