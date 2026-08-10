package supervisor

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"mini-k8s/internal/logging"
	"mini-k8s/internal/schema"
)

const (
	// probeTimeout bounds a single liveness probe. A probe that hangs would
	// stall the whole health loop, so an unanswered connect counts as a failure
	// rather than as a reason to wait.
	probeTimeout = 1 * time.Second
	// probeHost is the address probes dial. Pods share the node's network
	// namespace, so a local connect is the whole check.
	probeHost = "127.0.0.1"
)

// HealthConfig tunes the self-healing loop.
type HealthConfig struct {
	Interval         time.Duration // how often each local pod is probed
	FailureThreshold int           // consecutive failures before declaring a pod dead
	BackoffBase      time.Duration // first restart delay
	BackoffMax       time.Duration // cap on the restart delay
}

// RunHealthChecker probes this node's pods and restarts the ones that die.
//
// Both halves of self-healing live here rather than in the control plane,
// because only the node holding a process can see whether that process is
// actually serving and only it can restart it in place. The control plane's job
// is replica count, not process liveness.
func (s *Supervisor) RunHealthChecker(ctx context.Context, cfg HealthConfig) {
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Second
	}
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 3
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = time.Second
	}
	if cfg.BackoffMax < cfg.BackoffBase {
		cfg.BackoffMax = 30 * time.Second
	}

	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.CheckOnce(ctx, cfg); err != nil && ctx.Err() == nil {
				s.logger.Error(ctx, "health_sweep_failed", err.Error(), logging.NodeID(s.nodeID))
			}
		}
	}
}

// CheckOnce probes every pod bound to this node exactly once.
func (s *Supervisor) CheckOnce(ctx context.Context, cfg HealthConfig) error {
	keys, err := s.client.ScanKeys(ctx, schema.PodKeyPattern())
	if err != nil {
		return fmt.Errorf("scan pods: %w", err)
	}

	for _, key := range keys {
		fields, err := s.client.HashGet(ctx, key)
		if err != nil || len(fields) == 0 {
			continue
		}
		pod, err := schema.MapToPod(fields)
		if err != nil || pod.NodeID != s.nodeID {
			continue
		}
		// Evicted pods belong to the control plane's cleanup path, not here.
		if pod.Status == schema.StatusEvicted {
			continue
		}
		if err := s.checkPod(ctx, pod, cfg); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			s.logger.Warn(ctx, "pod_health_check_failed", err.Error(),
				logging.DeploymentID(pod.Deployment), logging.PodID(pod.PodID))
		}
	}
	return nil
}

// checkPod probes one pod and advances its health state.
func (s *Supervisor) checkPod(ctx context.Context, pod *schema.PodSpec, cfg HealthConfig) error {
	podKey := schema.PodKey(pod.Deployment, pod.PodID)

	// A pod waiting out its backoff is not probed: it has no process to answer.
	if pod.Status == schema.StatusCrashLoopBackOff || pod.Status == schema.StatusFailed {
		return s.restartIfBackoffElapsed(ctx, pod, cfg)
	}

	if s.podHealthy(pod) {
		// Reset on success. A pod that failed twice and then recovered is not
		// one failure away from a restart; the threshold counts consecutive
		// failures, so a success has to clear the count.
		if pod.ConsecutiveFailures == 0 {
			return nil
		}
		return s.client.HashSet(ctx, podKey, map[string]interface{}{
			schema.FieldPodConsecFailures: 0,
			schema.FieldPodLastHealthOKAt: nowUnix(),
		})
	}

	failures := pod.ConsecutiveFailures + 1
	if failures < cfg.FailureThreshold {
		// Below the threshold on purpose: a single missed probe during a GC
		// pause or a brief overload is not evidence a process is dead, and
		// restarting on it would turn a hiccup into an outage.
		s.logger.Warn(ctx, "pod_probe_failed",
			fmt.Sprintf("failure %d of %d", failures, cfg.FailureThreshold),
			logging.DeploymentID(pod.Deployment), logging.PodID(pod.PodID))
		return s.client.HashSet(ctx, podKey, map[string]interface{}{
			schema.FieldPodConsecFailures: failures,
		})
	}

	delay := backoffFor(pod.RestartCount, cfg)
	s.logger.Warn(ctx, "pod_declared_failed",
		fmt.Sprintf("%d consecutive probe failures, restarting in %s", failures, delay),
		logging.DeploymentID(pod.Deployment), logging.PodID(pod.PodID))

	return s.client.HashSet(ctx, podKey, map[string]interface{}{
		schema.FieldPodStatus:         schema.StatusFailed,
		schema.FieldPodConsecFailures: failures,
		schema.FieldPodBackoffNextSec: int(delay.Seconds()),
		schema.FieldPodLastRestartAt:  nowUnix(),
	})
}

// restartIfBackoffElapsed restarts a failed pod once its delay has passed.
//
// The deadline is derived from timestamps in Redis rather than held in memory,
// so an agent that restarts mid-backoff resumes the same schedule instead of
// restarting the pod immediately and hiding the crash loop.
func (s *Supervisor) restartIfBackoffElapsed(ctx context.Context, pod *schema.PodSpec, cfg HealthConfig) error {
	last, err := strconv.ParseInt(pod.LastRestartAt, 10, 64)
	if err != nil {
		// No usable timestamp: treat the delay as elapsed rather than waiting
		// forever on a value that will never parse.
		last = 0
	}
	wait := time.Duration(pod.BackoffNextSeconds) * time.Second
	if wait <= 0 {
		wait = cfg.BackoffBase
	}
	if last > 0 && time.Since(time.Unix(last, 0)) < wait {
		return nil
	}

	podKey := schema.PodKey(pod.Deployment, pod.PodID)
	next := backoffFor(pod.RestartCount+1, cfg)

	// The counters advance before the spawn attempt. If the spawn fails, the
	// pod has still consumed an attempt, and recording it afterwards would let
	// a pod that fails to spawn retry at the base delay forever.
	if err := s.client.HashSet(ctx, podKey, map[string]interface{}{
		schema.FieldPodRestartCount:   pod.RestartCount + 1,
		schema.FieldPodBackoffNextSec: int(next.Seconds()),
		schema.FieldPodLastRestartAt:  nowUnix(),
		schema.FieldPodConsecFailures: 0,
		schema.FieldPodStatus:         schema.StatusCrashLoopBackOff,
	}); err != nil {
		return fmt.Errorf("record restart attempt for %s: %w", pod.PodID, err)
	}

	s.logger.Info(ctx, "pod_restarting",
		fmt.Sprintf("attempt %d, next delay %s", pod.RestartCount+1, next),
		logging.DeploymentID(pod.Deployment), logging.PodID(pod.PodID))

	// Spawn re-reads the record and sets the status itself, so a success lands
	// as Running and a failure lands as Failed.
	return s.Spawn(ctx, pod.Deployment, pod.PodID)
}

// podHealthy reports whether a pod is both running and answering on its port.
//
// The process check comes first and is authoritative: a dead process is dead
// regardless of what any port says, and checking identity rather than a bare PID
// keeps a recycled PID from reading as alive.
func (s *Supervisor) podHealthy(pod *schema.PodSpec) bool {
	if pod.PID == 0 || !IsSameProcess(pod.PID, pod.PIDStartTime) {
		return false
	}
	if pod.HostPort == 0 {
		// Nothing to probe. The process is alive and that is all this pod
		// exposes, so calling it unhealthy would restart a working pod forever.
		return true
	}
	return portAnswers(pod.HostPort)
}

// portAnswers reports whether something accepts a TCP connection on the port.
func portAnswers(port int) bool {
	conn, err := net.DialTimeout("tcp",
		net.JoinHostPort(probeHost, strconv.Itoa(port)), probeTimeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// backoffFor returns the delay before restart attempt n, doubling each time and
// capped so a pod that will never start does not drift into hour-long waits.
func backoffFor(restartCount int, cfg HealthConfig) time.Duration {
	if restartCount < 0 {
		restartCount = 0
	}
	delay := cfg.BackoffBase
	for i := 0; i < restartCount; i++ {
		delay *= 2
		if delay >= cfg.BackoffMax {
			return cfg.BackoffMax
		}
	}
	return delay
}

// nowUnix is the timestamp format every record in Redis uses.
func nowUnix() string {
	return strconv.FormatInt(time.Now().Unix(), 10)
}
