package supervisor

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"syscall"
	"testing"
	"time"

	"mini-k8s/internal/logging"
	"mini-k8s/internal/redisclient"
	"mini-k8s/internal/schema"
)

func testRedisAddr() string {
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		return addr
	}
	return "localhost:6379"
}

// fixture bundles a supervisor with the unique identifiers its test owns, so
// concurrent tests and leftover cluster state cannot collide.
type fixture struct {
	sup        *Supervisor
	client     *redisclient.Client
	nodeID     string
	deployment string
}

// newFixture builds a supervisor bound to a unique node, and registers a
// deployment whose exec_path is the compiled test workload.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	client, err := redisclient.New(testRedisAddr())
	if err != nil {
		t.Skipf("redis unavailable at %s: %v", testRedisAddr(), err)
	}

	stamp := time.Now().UnixNano()
	f := &fixture{
		client:     client,
		nodeID:     fmt.Sprintf("sup-node-%d", stamp),
		deployment: fmt.Sprintf("sup-dep-%d", stamp),
	}
	f.sup = NewSupervisor(client, logging.New("test", nil), f.nodeID)

	ctx := context.Background()
	dep := &schema.DeploymentSpec{
		Name: f.deployment, MinReplicas: 1, MaxReplicas: 4,
		DesiredReplicas: 1, TargetCPUPercent: 50,
		CPURequest: 100, MemRequest: 64,
		ExecPath: helperBinary, Port: 0, CreatedAt: "0",
	}
	if err := client.HashSet(ctx, schema.DeploymentKey(f.deployment), schema.DeploymentToMap(dep)); err != nil {
		t.Fatalf("write deployment: %v", err)
	}

	capacity := &schema.NodeCapacity{TotalCPU: 1000, TotalMem: 1024}
	if err := client.HashSet(ctx, schema.NodeCapacityKey(f.nodeID), schema.NodeCapacityToMap(capacity)); err != nil {
		t.Fatalf("write capacity: %v", err)
	}

	t.Cleanup(func() {
		ctx := context.Background()
		// Kill anything still running before dropping the records that point at it.
		if keys, err := client.ScanKeys(ctx, schema.DeploymentPodsPattern(f.deployment)); err == nil {
			for _, key := range keys {
				if fields, err := client.HashGet(ctx, key); err == nil {
					if pid, convErr := strconv.Atoi(fields[schema.FieldPodPID]); convErr == nil && pid > 0 {
						syscall.Kill(-pid, syscall.SIGKILL)
					}
				}
			}
			if len(keys) > 0 {
				client.DeleteKey(ctx, keys...)
			}
		}
		client.DeleteKey(ctx,
			schema.DeploymentKey(f.deployment),
			schema.NodeCapacityKey(f.nodeID),
			schema.NodeCommandsKey(f.nodeID))
		client.Close()
	})
	return f
}

// createPod writes a Pending pod bound to this fixture's node.
func (f *fixture) createPod(t *testing.T, podID string) {
	t.Helper()
	pod := &schema.PodSpec{
		Deployment: f.deployment, PodID: podID, Status: schema.StatusPending,
		NodeID: f.nodeID, CPURequest: 100, MemRequest: 64, CreatedAt: "0",
	}
	if err := f.client.HashSet(context.Background(), schema.PodKey(f.deployment, podID), schema.PodToMap(pod)); err != nil {
		t.Fatalf("create pod %s: %v", podID, err)
	}
}

// readPod reads a pod's current state.
func (f *fixture) readPod(t *testing.T, podID string) *schema.PodSpec {
	t.Helper()
	fields, err := f.client.HashGet(context.Background(), schema.PodKey(f.deployment, podID))
	if err != nil {
		t.Fatalf("read pod %s: %v", podID, err)
	}
	if len(fields) == 0 {
		return nil
	}
	pod, err := schema.MapToPod(fields)
	if err != nil {
		t.Fatalf("parse pod %s: %v", podID, err)
	}
	return pod
}

func TestSpawnStartsProcessAndRecordsDurableIdentity(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.createPod(t, "p1")

	if err := f.sup.Spawn(ctx, f.deployment, "p1"); err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	pod := f.readPod(t, "p1")
	if pod.Status != schema.StatusRunning {
		t.Errorf("status = %q, want %q", pod.Status, schema.StatusRunning)
	}
	if pod.PID <= 0 {
		t.Fatalf("pid = %d, want a real pid", pod.PID)
	}
	if !PIDAlive(pod.PID) {
		t.Errorf("pid %d should be alive after spawn", pod.PID)
	}
	// The start-time marker is what makes the identity durable; without it a
	// later reconcile could not distinguish this process from a PID reuse.
	if pod.PIDStartTime == 0 {
		t.Error("pid_start_time should be recorded so identity survives a restart")
	}
	if !IsSameProcess(pod.PID, pod.PIDStartTime) {
		t.Error("recorded identity should verify against the live process")
	}
	if pod.HostPort <= 0 {
		t.Errorf("host_port = %d, want an allocated port", pod.HostPort)
	}
}

// The duplicate this whole identity scheme exists to prevent.
func TestSpawnRefusesToDuplicateALiveProcess(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.createPod(t, "p1")

	if err := f.sup.Spawn(ctx, f.deployment, "p1"); err != nil {
		t.Fatalf("first Spawn: %v", err)
	}
	first := f.readPod(t, "p1")

	// A redelivered start command must not create a second process.
	if err := f.sup.Spawn(ctx, f.deployment, "p1"); err != nil {
		t.Fatalf("second Spawn: %v", err)
	}
	second := f.readPod(t, "p1")

	if second.PID != first.PID {
		t.Errorf("pid changed from %d to %d: a live pod was spawned twice", first.PID, second.PID)
	}
	if second.PIDStartTime != first.PIDStartTime {
		t.Error("start time changed, so a different process is now backing the pod")
	}
}

// A restart must keep the pod's address, since ingress upstreams point at it.
func TestSpawnReusesTheAssignedHostPortOnRestart(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.createPod(t, "p1")

	if err := f.sup.Spawn(ctx, f.deployment, "p1"); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	first := f.readPod(t, "p1")

	// Kill it the way a crash would, leaving the record behind.
	syscall.Kill(-first.PID, syscall.SIGKILL)
	waitGone(t, first.PID)

	if err := f.sup.Spawn(ctx, f.deployment, "p1"); err != nil {
		t.Fatalf("respawn: %v", err)
	}
	second := f.readPod(t, "p1")

	if second.HostPort != first.HostPort {
		t.Errorf("host port changed from %d to %d across a restart", first.HostPort, second.HostPort)
	}
	if second.PID == first.PID {
		t.Error("expected a new pid after the old process died")
	}
}

// Pods here share the node's network namespace, so replicas must not collide
// on one port.
func TestSpawnGivesEachPodOnANodeADistinctPort(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	ports := map[int]string{}
	for _, podID := range []string{"p1", "p2", "p3"} {
		f.createPod(t, podID)
		if err := f.sup.Spawn(ctx, f.deployment, podID); err != nil {
			t.Fatalf("Spawn %s: %v", podID, err)
		}
		pod := f.readPod(t, podID)
		if other, clash := ports[pod.HostPort]; clash {
			t.Fatalf("pods %s and %s both got port %d", other, podID, pod.HostPort)
		}
		ports[pod.HostPort] = podID
	}
	if len(ports) != 3 {
		t.Errorf("got %d distinct ports for 3 pods", len(ports))
	}
}

func TestSpawnMarksPodFailedWhenExecPathIsUnusable(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	// Point the deployment at something that cannot be executed.
	if err := f.client.HashSet(ctx, schema.DeploymentKey(f.deployment),
		map[string]interface{}{schema.FieldExecPath: "/nonexistent/definitely-not-here"}); err != nil {
		t.Fatalf("update exec path: %v", err)
	}
	f.createPod(t, "p1")

	if err := f.sup.Spawn(ctx, f.deployment, "p1"); err == nil {
		t.Fatal("spawning a missing binary should fail")
	}

	// The failure must be visible in state, not just in logs, or the pod would
	// sit in Pending with no explanation.
	pod := f.readPod(t, "p1")
	if pod.Status != schema.StatusFailed {
		t.Errorf("status = %q, want %q", pod.Status, schema.StatusFailed)
	}
}

// A command that waited in the queue while its pod was deleted must not create
// a process nothing owns.
func TestSpawnRefusesWhenPodOrDeploymentIsGone(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	if err := f.sup.Spawn(ctx, f.deployment, "never-created"); err == nil {
		t.Error("spawning a nonexistent pod should fail")
	}

	f.createPod(t, "p1")
	if err := f.client.DeleteKey(ctx, schema.DeploymentKey(f.deployment)); err != nil {
		t.Fatalf("delete deployment: %v", err)
	}
	if err := f.sup.Spawn(ctx, f.deployment, "p1"); err == nil {
		t.Error("spawning against a deleted deployment should fail")
	}
}

func TestStopTerminatesProcessAndClearsIdentity(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.createPod(t, "p1")

	if err := f.sup.Spawn(ctx, f.deployment, "p1"); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	pid := f.readPod(t, "p1").PID

	if err := f.sup.Stop(ctx, f.deployment, "p1"); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	waitGone(t, pid)

	// Identity must be cleared so the record never points at a PID that the
	// kernel may later recycle for an unrelated process.
	pod := f.readPod(t, "p1")
	if pod.PID != 0 || pod.PIDStartTime != 0 {
		t.Errorf("identity not cleared: pid=%d start=%d", pod.PID, pod.PIDStartTime)
	}
}

func TestStopAndRemoveReleasesCapacityAndDeletesPod(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	// Start from a node already accounting for this pod's reservation.
	if err := f.client.HashSet(ctx, schema.NodeCapacityKey(f.nodeID), map[string]interface{}{
		schema.FieldAllocatedCPU: 100, schema.FieldAllocatedMem: 64,
	}); err != nil {
		t.Fatalf("seed allocation: %v", err)
	}
	f.createPod(t, "p1")
	if err := f.sup.Spawn(ctx, f.deployment, "p1"); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	pid := f.readPod(t, "p1").PID

	if err := f.sup.StopAndRemove(ctx, f.deployment, "p1"); err != nil {
		t.Fatalf("StopAndRemove: %v", err)
	}
	waitGone(t, pid)

	if pod := f.readPod(t, "p1"); pod != nil {
		t.Error("pod record should be deleted")
	}

	fields, _ := f.client.HashGet(ctx, schema.NodeCapacityKey(f.nodeID))
	capacity, err := schema.MapToNodeCapacity(fields)
	if err != nil {
		t.Fatalf("parse capacity: %v", err)
	}
	if capacity.AllocatedCPU != 0 || capacity.AllocatedMem != 0 {
		t.Errorf("capacity not released: cpu=%d mem=%d, want 0/0",
			capacity.AllocatedCPU, capacity.AllocatedMem)
	}
}

func TestStopAndRemoveIsIdempotent(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.createPod(t, "p1")

	if err := f.sup.StopAndRemove(ctx, f.deployment, "p1"); err != nil {
		t.Fatalf("first StopAndRemove: %v", err)
	}
	// Removing an already-removed pod must not error, so a redelivered stop
	// command is harmless.
	if err := f.sup.StopAndRemove(ctx, f.deployment, "p1"); err != nil {
		t.Fatalf("second StopAndRemove: %v", err)
	}
}
