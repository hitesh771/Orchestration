package supervisor

import (
	"context"
	"syscall"
	"testing"
	"time"

	"mini-k8s/internal/schema"
)

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// The no-duplicates half of the restart guarantee: a pod whose process is
// verifiably alive must be adopted, never respawned.
func TestReconcileAdoptsLiveProcessWithoutRespawning(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.createPod(t, "p1")

	if err := f.sup.Spawn(ctx, f.deployment, "p1"); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	before := f.readPod(t, "p1")

	// Simulate the agent restarting: same Redis state, fresh reconcile.
	report, err := f.sup.ReconcileOnStartup(ctx)
	if err != nil {
		t.Fatalf("ReconcileOnStartup: %v", err)
	}

	if !contains(report.Adopted, "p1") {
		t.Errorf("pod should be adopted, report = %+v", report)
	}
	if contains(report.Restarted, "p1") {
		t.Error("a live pod must not be restarted")
	}

	after := f.readPod(t, "p1")
	if after.PID != before.PID {
		t.Errorf("pid changed %d -> %d: the agent spawned a duplicate", before.PID, after.PID)
	}
	if !PIDAlive(after.PID) {
		t.Errorf("adopted pid %d should still be running", after.PID)
	}
}

// A pod recorded as Running whose process died must be restarted.
func TestReconcileRestartsPodWhoseProcessDied(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.createPod(t, "p1")

	if err := f.sup.Spawn(ctx, f.deployment, "p1"); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	old := f.readPod(t, "p1")

	// Kill the process but leave the record pointing at it, which is what an
	// agent crash plus a workload crash looks like.
	syscall.Kill(-old.PID, syscall.SIGKILL)
	waitGone(t, old.PID)

	report, err := f.sup.ReconcileOnStartup(ctx)
	if err != nil {
		t.Fatalf("ReconcileOnStartup: %v", err)
	}
	if !contains(report.Restarted, "p1") {
		t.Errorf("dead pod should be restarted, report = %+v", report)
	}

	fresh := f.readPod(t, "p1")
	if fresh.PID == old.PID {
		t.Error("pid should be new after a restart")
	}
	if !IsSameProcess(fresh.PID, fresh.PIDStartTime) {
		t.Error("restarted pod should have verifiable identity")
	}
	if fresh.Status != schema.StatusRunning {
		t.Errorf("status = %q, want %q", fresh.Status, schema.StatusRunning)
	}
}

// A recorded PID that now belongs to an unrelated process must not be adopted.
// This is the PID-reuse case that a bare liveness check would get wrong.
func TestReconcileDoesNotAdoptAProcessWithMismatchedIdentity(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.createPod(t, "p1")

	// A live process that is NOT this pod's, recorded with a wrong start time.
	stranger := startHelper(t)
	if err := f.client.HashSet(ctx, schema.PodKey(f.deployment, "p1"), map[string]interface{}{
		schema.FieldPodPID:          stranger.Process.Pid,
		schema.FieldPodPIDStartTime: 999999999, // deliberately wrong
		schema.FieldPodStatus:       schema.StatusRunning,
	}); err != nil {
		t.Fatalf("seed mismatched identity: %v", err)
	}

	report, err := f.sup.ReconcileOnStartup(ctx)
	if err != nil {
		t.Fatalf("ReconcileOnStartup: %v", err)
	}
	if contains(report.Adopted, "p1") {
		t.Error("a process with a mismatched start time must not be adopted")
	}
	if !contains(report.Restarted, "p1") {
		t.Errorf("pod should be restarted instead, report = %+v", report)
	}

	// The unrelated process must be left alone.
	if !PIDAlive(stranger.Process.Pid) {
		t.Error("reconcile killed a process that did not belong to the cluster")
	}
}

// The no-orphans half: a process carrying this node's pod marker that no live
// pod claims must be killed, since nothing else would ever clean it up.
func TestReconcileKillsOrphanedProcess(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	// A process that looks like one of our pods, but with no pod record: the
	// agent died between spawning it and recording its PID.
	orphan := startHelper(t,
		PodIDEnv+"=ghost-pod",
		NodeIDEnv+"="+f.nodeID,
		DeploymentEnv+"="+f.deployment,
	)
	pid := orphan.Process.Pid
	waitForMarker(t, pid) // the marker must be visible before the sweep runs

	report, err := f.sup.ReconcileOnStartup(ctx)
	if err != nil {
		t.Fatalf("ReconcileOnStartup: %v", err)
	}
	if !contains(report.Orphaned, "ghost-pod") {
		t.Errorf("orphan should be reported, report = %+v", report)
	}
	waitGone(t, pid)
}

// An orphan belonging to a different node must be left alone, or two agents
// sharing a /proc would kill each other's processes.
func TestReconcileLeavesOtherNodesProcessesAlone(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	other := startHelper(t,
		PodIDEnv+"=their-pod",
		NodeIDEnv+"=some-other-node",
	)
	waitForMarker(t, other.Process.Pid)

	report, err := f.sup.ReconcileOnStartup(ctx)
	if err != nil {
		t.Fatalf("ReconcileOnStartup: %v", err)
	}
	if contains(report.Orphaned, "their-pod") {
		t.Error("another node's process must not be killed")
	}
	if !PIDAlive(other.Process.Pid) {
		t.Error("another node's process should still be running")
	}
}

// An evicted pod is no longer this node's responsibility.
func TestReconcileSkipsEvictedPods(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.createPod(t, "p1")

	if err := f.client.HashSet(ctx, schema.PodKey(f.deployment, "p1"),
		map[string]interface{}{schema.FieldPodStatus: schema.StatusEvicted}); err != nil {
		t.Fatalf("mark evicted: %v", err)
	}

	report, err := f.sup.ReconcileOnStartup(ctx)
	if err != nil {
		t.Fatalf("ReconcileOnStartup: %v", err)
	}
	if contains(report.Restarted, "p1") || contains(report.Adopted, "p1") {
		t.Errorf("an evicted pod should be ignored, report = %+v", report)
	}
}

// A pod bound elsewhere must not be started here.
func TestReconcileIgnoresPodsOnOtherNodes(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	pod := &schema.PodSpec{
		Deployment: f.deployment, PodID: "theirs", Status: schema.StatusPending,
		NodeID: "different-node", CPURequest: 100, MemRequest: 64, CreatedAt: "0",
	}
	if err := f.client.HashSet(ctx, schema.PodKey(f.deployment, "theirs"), schema.PodToMap(pod)); err != nil {
		t.Fatalf("write foreign pod: %v", err)
	}

	report, err := f.sup.ReconcileOnStartup(ctx)
	if err != nil {
		t.Fatalf("ReconcileOnStartup: %v", err)
	}
	if contains(report.Restarted, "theirs") || contains(report.Adopted, "theirs") {
		t.Errorf("another node's pod must not be touched, report = %+v", report)
	}
}

// The command consumer must act on queued work and exit on cancellation.
func TestCommandConsumerStartsAndStopsPods(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f.createPod(t, "p1")
	if err := SendCommand(ctx, f.client, f.nodeID, Command{
		Type: CommandStartPod, Deployment: f.deployment, PodID: "p1",
	}); err != nil {
		t.Fatalf("queue start: %v", err)
	}

	done := make(chan struct{})
	go func() {
		f.sup.RunCommandConsumer(ctx)
		close(done)
	}()

	// Wait for the pod to come up.
	var pid int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if pod := f.readPod(t, "p1"); pod != nil && pod.Status == schema.StatusRunning && pod.PID > 0 {
			pid = pod.PID
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if pid == 0 {
		t.Fatal("consumer did not start the pod")
	}

	if err := SendCommand(ctx, f.client, f.nodeID, Command{
		Type: CommandStopPod, Deployment: f.deployment, PodID: "p1",
	}); err != nil {
		t.Fatalf("queue stop: %v", err)
	}

	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f.readPod(t, "p1") == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if f.readPod(t, "p1") != nil {
		t.Error("consumer did not remove the pod")
	}
	waitGone(t, pid)

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not exit on context cancel")
	}
}

// A malformed envelope must be dropped, not retried forever, or it would block
// every command behind it.
func TestCommandConsumerDropsMalformedCommands(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := f.client.ListPush(ctx, schema.NodeCommandsKey(f.nodeID), "{not valid json"); err != nil {
		t.Fatalf("queue garbage: %v", err)
	}
	f.createPod(t, "p1")
	if err := SendCommand(ctx, f.client, f.nodeID, Command{
		Type: CommandStartPod, Deployment: f.deployment, PodID: "p1",
	}); err != nil {
		t.Fatalf("queue start: %v", err)
	}

	go f.sup.RunCommandConsumer(ctx)

	// The valid command behind the garbage must still be processed.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if pod := f.readPod(t, "p1"); pod != nil && pod.Status == schema.StatusRunning {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Error("a malformed command blocked the queue")
}
