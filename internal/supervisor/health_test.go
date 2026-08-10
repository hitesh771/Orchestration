package supervisor

import (
	"context"
	"strconv"
	"testing"
	"time"

	"mini-k8s/internal/schema"
)

// testHealthConfig is deliberately fast: real intervals would make the suite
// wait on wall-clock time rather than on behavior.
func testHealthConfig() HealthConfig {
	return HealthConfig{
		Interval:         10 * time.Millisecond,
		FailureThreshold: 3,
		BackoffBase:      time.Second,
		BackoffMax:       30 * time.Second,
	}
}

// setPodFields patches a pod record directly, to put it in a state that would
// otherwise take many probe cycles to reach.
func (f *fixture) setPodFields(t *testing.T, podID string, fields map[string]interface{}) {
	t.Helper()
	if err := f.client.HashSet(context.Background(), schema.PodKey(f.deployment, podID), fields); err != nil {
		t.Fatalf("patch pod %s: %v", podID, err)
	}
}

// waitHealthy blocks until a freshly spawned pod is actually answering.
//
// Spawn returns as soon as the process exists, which is before the workload has
// bound its port. Probing in that window fails for a pod that is merely still
// starting, so a test about healthy pods has to wait for readiness first. In
// production the failure threshold absorbs the same window.
func (f *fixture) waitHealthy(t *testing.T, podID string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if f.sup.podHealthy(f.readPod(t, podID)) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("pod %s never became healthy", podID)
}

// deadPID returns a PID that is recorded but certainly not running, which is how
// a pod whose process died looks to the health checker.
func (f *fixture) writeDeadPod(t *testing.T, podID, status string) {
	t.Helper()
	f.createPod(t, podID)
	f.setPodFields(t, podID, map[string]interface{}{
		schema.FieldPodStatus:       status,
		schema.FieldPodPID:          4194303, // above the default pid_max
		schema.FieldPodPIDStartTime: 12345,
	})
}

func TestHealthCheckResetsFailureCountOnRecovery(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.createPod(t, "p1")
	if err := f.sup.Spawn(ctx, f.deployment, "p1"); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	f.waitHealthy(t, "p1")
	// Two earlier probes failed; a success has to clear them, or the pod would
	// sit one hiccup away from a restart for the rest of its life.
	f.setPodFields(t, "p1", map[string]interface{}{schema.FieldPodConsecFailures: 2})

	if err := f.sup.CheckOnce(ctx, testHealthConfig()); err != nil {
		t.Fatalf("CheckOnce: %v", err)
	}
	pod := f.readPod(t, "p1")
	if pod.ConsecutiveFailures != 0 {
		t.Errorf("consecutive_failures = %d, want 0 after a healthy probe", pod.ConsecutiveFailures)
	}
	if pod.Status != schema.StatusRunning {
		t.Errorf("status = %q, want Running", pod.Status)
	}
}

// A single failed probe is not evidence a process is dead. Restarting on it
// would turn a GC pause or a brief overload into an outage.
func TestHealthCheckDoesNotRestartBelowThreshold(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.writeDeadPod(t, "p1", schema.StatusRunning)

	if err := f.sup.CheckOnce(ctx, testHealthConfig()); err != nil {
		t.Fatalf("CheckOnce: %v", err)
	}
	pod := f.readPod(t, "p1")
	if pod.ConsecutiveFailures != 1 {
		t.Errorf("consecutive_failures = %d, want 1", pod.ConsecutiveFailures)
	}
	if pod.Status != schema.StatusRunning {
		t.Errorf("status = %q, want Running while still below threshold", pod.Status)
	}
}

func TestHealthCheckDeclaresFailedAtThreshold(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	cfg := testHealthConfig()
	f.writeDeadPod(t, "p1", schema.StatusRunning)
	f.setPodFields(t, "p1", map[string]interface{}{
		schema.FieldPodConsecFailures: cfg.FailureThreshold - 1,
	})

	if err := f.sup.CheckOnce(ctx, cfg); err != nil {
		t.Fatalf("CheckOnce: %v", err)
	}
	pod := f.readPod(t, "p1")
	if pod.Status != schema.StatusFailed {
		t.Fatalf("status = %q, want Failed at the threshold", pod.Status)
	}
	if pod.BackoffNextSeconds <= 0 {
		t.Errorf("backoff_next_seconds = %d, want a delay recorded", pod.BackoffNextSeconds)
	}
	if pod.LastRestartAt == "" {
		t.Error("last_restart_at should be set so the backoff deadline survives an agent restart")
	}
}

// The backoff deadline lives in Redis, so an agent that restarts mid-backoff
// resumes the same schedule instead of restarting immediately and hiding a
// crash loop.
func TestHealthCheckWaitsOutBackoffBeforeRestarting(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.writeDeadPod(t, "p1", schema.StatusFailed)
	f.setPodFields(t, "p1", map[string]interface{}{
		schema.FieldPodBackoffNextSec: 300,
		schema.FieldPodLastRestartAt:  strconv.FormatInt(time.Now().Unix(), 10),
	})

	if err := f.sup.CheckOnce(ctx, testHealthConfig()); err != nil {
		t.Fatalf("CheckOnce: %v", err)
	}
	pod := f.readPod(t, "p1")
	if pod.RestartCount != 0 {
		t.Errorf("restart_count = %d, want 0 while the backoff is unexpired", pod.RestartCount)
	}
	if pod.Status != schema.StatusFailed {
		t.Errorf("status = %q, want Failed while waiting", pod.Status)
	}
}

func TestHealthCheckRestartsOnceBackoffElapsed(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.writeDeadPod(t, "p1", schema.StatusFailed)
	f.setPodFields(t, "p1", map[string]interface{}{
		schema.FieldPodBackoffNextSec: 1,
		schema.FieldPodLastRestartAt:  strconv.FormatInt(time.Now().Unix()-60, 10),
	})

	if err := f.sup.CheckOnce(ctx, testHealthConfig()); err != nil {
		t.Fatalf("CheckOnce: %v", err)
	}
	pod := f.readPod(t, "p1")
	if pod.RestartCount != 1 {
		t.Errorf("restart_count = %d, want 1", pod.RestartCount)
	}
	if pod.Status != schema.StatusRunning {
		t.Fatalf("status = %q, want Running after a successful restart", pod.Status)
	}
	if !IsSameProcess(pod.PID, pod.PIDStartTime) {
		t.Error("restarted pod should be backed by a verifiable live process")
	}
}

// A pod that cannot start must not retry at the base delay forever: the attempt
// counter advances even when the spawn itself fails.
func TestHealthCheckAdvancesBackoffWhenRestartFails(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.writeDeadPod(t, "p1", schema.StatusFailed)
	f.setPodFields(t, "p1", map[string]interface{}{
		schema.FieldPodLastRestartAt: "1",
	})
	// An exec_path that cannot run makes every restart attempt fail.
	if err := f.client.HashSet(ctx, schema.DeploymentKey(f.deployment),
		map[string]interface{}{schema.FieldExecPath: "/nonexistent/binary"}); err != nil {
		t.Fatalf("patch deployment: %v", err)
	}

	_ = f.sup.CheckOnce(ctx, testHealthConfig())
	first := f.readPod(t, "p1")
	if first.RestartCount != 1 {
		t.Fatalf("restart_count = %d, want 1 even though the spawn failed", first.RestartCount)
	}

	f.setPodFields(t, "p1", map[string]interface{}{schema.FieldPodLastRestartAt: "1"})
	_ = f.sup.CheckOnce(ctx, testHealthConfig())
	second := f.readPod(t, "p1")
	if second.RestartCount != 2 {
		t.Fatalf("restart_count = %d, want 2", second.RestartCount)
	}
	if second.BackoffNextSeconds <= first.BackoffNextSeconds {
		t.Errorf("backoff did not grow: %d then %d", first.BackoffNextSeconds, second.BackoffNextSeconds)
	}
}

func TestHealthCheckIgnoresPodsOnOtherNodes(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.writeDeadPod(t, "p1", schema.StatusRunning)
	f.setPodFields(t, "p1", map[string]interface{}{schema.FieldPodNodeID: "some-other-node"})

	if err := f.sup.CheckOnce(ctx, testHealthConfig()); err != nil {
		t.Fatalf("CheckOnce: %v", err)
	}
	if pod := f.readPod(t, "p1"); pod.ConsecutiveFailures != 0 {
		t.Errorf("consecutive_failures = %d, want another node's pod left untouched", pod.ConsecutiveFailures)
	}
}

// Evicted pods belong to the control plane's cleanup path. Restarting one here
// would resurrect work whose capacity has already been handed to someone else.
func TestHealthCheckSkipsEvictedPods(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.writeDeadPod(t, "p1", schema.StatusEvicted)

	if err := f.sup.CheckOnce(ctx, testHealthConfig()); err != nil {
		t.Fatalf("CheckOnce: %v", err)
	}
	pod := f.readPod(t, "p1")
	if pod.Status != schema.StatusEvicted {
		t.Errorf("status = %q, want Evicted left alone", pod.Status)
	}
	if pod.RestartCount != 0 {
		t.Errorf("restart_count = %d, want an evicted pod never restarted", pod.RestartCount)
	}
}

func TestBackoffDoublesAndCaps(t *testing.T) {
	cfg := HealthConfig{BackoffBase: time.Second, BackoffMax: 8 * time.Second}
	want := []time.Duration{
		1 * time.Second, 2 * time.Second, 4 * time.Second,
		8 * time.Second, 8 * time.Second, 8 * time.Second,
	}
	for i, expected := range want {
		if got := backoffFor(i, cfg); got != expected {
			t.Errorf("backoffFor(%d) = %s, want %s", i, got, expected)
		}
	}
	// A negative count is nonsense but must not produce a zero delay, which
	// would busy-restart a broken pod.
	if got := backoffFor(-1, cfg); got != cfg.BackoffBase {
		t.Errorf("backoffFor(-1) = %s, want the base delay", got)
	}
}

// A pod with no port exposes nothing to probe, so process liveness is the whole
// check. Calling it unhealthy would restart a working pod forever.
func TestPodWithNoPortIsHealthyWhileItsProcessLives(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.createPod(t, "p1")
	if err := f.sup.Spawn(ctx, f.deployment, "p1"); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	pod := f.readPod(t, "p1")
	pod.HostPort = 0

	if !f.sup.podHealthy(pod) {
		t.Error("a live process with no port should count as healthy")
	}
}

// A recorded PID with no start time cannot be verified, and an unverifiable
// identity must not be trusted: the PID may since have been recycled.
func TestPodWithUnverifiableIdentityIsUnhealthy(t *testing.T) {
	f := newFixture(t)
	pod := &schema.PodSpec{PID: 1, PIDStartTime: 0}
	if f.sup.podHealthy(pod) {
		t.Error("a pod with no start-time marker should not be treated as healthy")
	}
}
