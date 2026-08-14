package controller

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"mini-k8s/internal/events"
	"mini-k8s/internal/logging"
	"mini-k8s/internal/redisclient"
	"mini-k8s/internal/schema"
	"mini-k8s/internal/supervisor"
)

func testRedisAddr() string {
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		return addr
	}
	return "localhost:6379"
}

// newTestController returns a controller and a prefix that namespaces every
// node and deployment name, so concurrent tests and leftover state cannot
// interfere with each other.
func newTestController(t *testing.T) (*ReplicaController, *redisclient.Client, string) {
	t.Helper()
	client, err := redisclient.New(testRedisAddr())
	if err != nil {
		t.Skipf("redis unavailable at %s: %v", testRedisAddr(), err)
	}
	t.Cleanup(func() { client.Close() })
	prefix := fmt.Sprintf("ctrl-%d", time.Now().UnixNano())
	return NewReplicaController(client, logging.New("test", nil), 50*time.Millisecond), client, prefix
}

// addNode registers a node with a liveness lease. Passing ttl <= 0 registers it
// without a lease, which is how a dead-but-not-yet-evicted node looks.
func addNode(t *testing.T, client *redisclient.Client, nodeID string, cpu, mem int, ttl time.Duration) {
	t.Helper()
	ctx := context.Background()
	cap := &schema.NodeCapacity{TotalCPU: cpu, TotalMem: mem}
	if err := client.HashSet(ctx, schema.NodeCapacityKey(nodeID), schema.NodeCapacityToMap(cap)); err != nil {
		t.Fatalf("write capacity: %v", err)
	}
	if err := client.SetAdd(ctx, schema.NodeRegistryKey(), nodeID); err != nil {
		t.Fatalf("join registry: %v", err)
	}
	if ttl > 0 {
		if err := client.SetKey(ctx, schema.NodeStatusKey(nodeID), schema.NodeStatusReady, ttl); err != nil {
			t.Fatalf("write lease: %v", err)
		}
	}
	t.Cleanup(func() {
		client.SetRemove(ctx, schema.NodeRegistryKey(), nodeID)
		client.DeleteKey(ctx, schema.NodeCapacityKey(nodeID), schema.NodeStatusKey(nodeID),
			schema.NodeCommandsKey(nodeID))
	})
}

// addDeployment writes a deployment spec and cleans up it and its pods.
func addDeployment(t *testing.T, client *redisclient.Client, name string, desired int) *schema.DeploymentSpec {
	t.Helper()
	ctx := context.Background()
	dep := &schema.DeploymentSpec{
		Name: name, MinReplicas: 1, MaxReplicas: 10,
		CPURequest: 100, MemRequest: 128,
		DesiredReplicas: desired, TargetCPUPercent: 70,
		ExecPath: "/bin/true", Port: 9000,
		CreatedAt: fmt.Sprintf("%d", time.Now().Unix()),
	}
	if err := client.HashSet(ctx, schema.DeploymentKey(name), schema.DeploymentToMap(dep)); err != nil {
		t.Fatalf("write deployment: %v", err)
	}
	t.Cleanup(func() {
		keys, _ := client.ScanKeys(ctx, schema.DeploymentPodsPattern(name))
		if len(keys) > 0 {
			client.DeleteKey(ctx, keys...)
		}
		client.DeleteKey(ctx, schema.DeploymentKey(name))
	})
	return dep
}

func podsOf(t *testing.T, client *redisclient.Client, deployment string) []*schema.PodSpec {
	t.Helper()
	ctx := context.Background()
	keys, err := client.ScanKeys(ctx, schema.DeploymentPodsPattern(deployment))
	if err != nil {
		t.Fatalf("scan pods: %v", err)
	}
	var pods []*schema.PodSpec
	for _, k := range keys {
		fields, err := client.HashGet(ctx, k)
		if err != nil || len(fields) == 0 {
			continue
		}
		pod, err := schema.MapToPod(fields)
		if err != nil {
			t.Fatalf("parse pod %s: %v", k, err)
		}
		pods = append(pods, pod)
	}
	return pods
}

func TestReconcileCreatesMissingReplicas(t *testing.T) {
	c, client, prefix := newTestController(t)
	ctx := context.Background()
	addNode(t, client, prefix+"-node", 1000, 2048, 10*time.Second)
	dep := prefix + "-web"
	addDeployment(t, client, dep, 3)

	delta, err := c.ReconcileDeployment(ctx, dep)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if delta.Created != 3 || delta.Desired != 3 || delta.Observed != 0 {
		t.Fatalf("want created 3 from observed 0/desired 3, got %+v", delta)
	}
	if got := len(podsOf(t, client, dep)); got != 3 {
		t.Fatalf("want 3 pods, got %d", got)
	}
}

func TestReconcileIsIdempotentAtDesiredCount(t *testing.T) {
	c, client, prefix := newTestController(t)
	ctx := context.Background()
	addNode(t, client, prefix+"-node", 1000, 2048, 10*time.Second)
	dep := prefix + "-web"
	addDeployment(t, client, dep, 2)

	if _, err := c.ReconcileDeployment(ctx, dep); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	delta, err := c.ReconcileDeployment(ctx, dep)
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if delta.Created != 0 || delta.Removed != 0 {
		t.Fatalf("converged reconcile must be a no-op, got %+v", delta)
	}
	if got := len(podsOf(t, client, dep)); got != 2 {
		t.Fatalf("want 2 pods, got %d", got)
	}
}

// A live node owns its processes, so scale-down must go through that node's
// command queue rather than deleting records behind its back.
func TestScaleDownDispatchesStopCommandsToLiveNodes(t *testing.T) {
	c, client, prefix := newTestController(t)
	ctx := context.Background()
	node := prefix + "-node"
	addNode(t, client, node, 1000, 2048, 10*time.Second)
	dep := prefix + "-web"
	addDeployment(t, client, dep, 3)

	if _, err := c.ReconcileDeployment(ctx, dep); err != nil {
		t.Fatalf("scale up: %v", err)
	}
	if err := client.HashSet(ctx, schema.DeploymentKey(dep),
		map[string]interface{}{schema.FieldDesiredReplicas: 1}); err != nil {
		t.Fatalf("set desired: %v", err)
	}

	delta, err := c.ReconcileDeployment(ctx, dep)
	if err != nil {
		t.Fatalf("scale down: %v", err)
	}
	if delta.Removed != 2 {
		t.Fatalf("want 2 removals, got %+v", delta)
	}

	var stops int
	for {
		payload, err := client.BlockingPop(ctx, schema.NodeCommandsKey(node), time.Second)
		if err != nil {
			break
		}
		cmd, err := supervisor.DecodeCommand(payload)
		if err != nil {
			t.Fatalf("decode command: %v", err)
		}
		if cmd.Type == supervisor.CommandStopPod {
			stops++
		}
	}
	if stops != 2 {
		t.Fatalf("want 2 stop commands queued, got %d", stops)
	}
	// Records stay until the owning agent acts on them: deleting here would
	// leave the process running with nothing pointing at it.
	if got := len(podsOf(t, client, dep)); got != 3 {
		t.Fatalf("want records intact pending agent action, got %d", got)
	}
}

// A dead node will never consume a command, so its pods are removed directly or
// their reservations would be stranded forever.
func TestScaleDownForceRemovesPodsOnDeadNodes(t *testing.T) {
	c, client, prefix := newTestController(t)
	ctx := context.Background()
	node := prefix + "-node"
	addNode(t, client, node, 1000, 2048, 10*time.Second)
	dep := prefix + "-web"
	addDeployment(t, client, dep, 2)

	if _, err := c.ReconcileDeployment(ctx, dep); err != nil {
		t.Fatalf("scale up: %v", err)
	}
	// Drop the lease: registered, but no longer alive.
	if err := client.DeleteKey(ctx, schema.NodeStatusKey(node)); err != nil {
		t.Fatalf("drop lease: %v", err)
	}
	if err := client.HashSet(ctx, schema.DeploymentKey(dep),
		map[string]interface{}{schema.FieldDesiredReplicas: 0}); err != nil {
		t.Fatalf("set desired: %v", err)
	}

	if _, err := c.ReconcileDeployment(ctx, dep); err != nil {
		t.Fatalf("scale down: %v", err)
	}
	if got := len(podsOf(t, client, dep)); got != 0 {
		t.Fatalf("want pods removed, got %d", got)
	}
	fields, err := client.HashGet(ctx, schema.NodeCapacityKey(node))
	if err != nil {
		t.Fatalf("read capacity: %v", err)
	}
	cap, err := schema.MapToNodeCapacity(fields)
	if err != nil {
		t.Fatalf("parse capacity: %v", err)
	}
	if cap.AllocatedCPU != 0 || cap.AllocatedMem != 0 {
		t.Fatalf("want reservations released, got cpu=%d mem=%d", cap.AllocatedCPU, cap.AllocatedMem)
	}
}

// Evicted pods are records of work that no longer exists. Counting them would
// suppress the replacements the deployment needs.
func TestReconcileReplacesEvictedPods(t *testing.T) {
	c, client, prefix := newTestController(t)
	ctx := context.Background()
	addNode(t, client, prefix+"-node", 1000, 2048, 10*time.Second)
	dep := prefix + "-web"
	addDeployment(t, client, dep, 2)

	if _, err := c.ReconcileDeployment(ctx, dep); err != nil {
		t.Fatalf("scale up: %v", err)
	}
	pods := podsOf(t, client, dep)
	if err := client.HashSet(ctx, schema.PodKey(dep, pods[0].PodID),
		map[string]interface{}{schema.FieldPodStatus: schema.StatusEvicted}); err != nil {
		t.Fatalf("mark evicted: %v", err)
	}

	delta, err := c.ReconcileDeployment(ctx, dep)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if delta.Created != 1 {
		t.Fatalf("want 1 replacement for the evicted pod, got %+v", delta)
	}
	for _, pod := range podsOf(t, client, dep) {
		if pod.Status == schema.StatusEvicted {
			t.Fatal("evicted record should have been cleared")
		}
	}
}

// Scaling down should shed the pods that are not doing useful work first.
func TestScaleDownPrefersUnhealthyPods(t *testing.T) {
	c, client, prefix := newTestController(t)
	ctx := context.Background()
	node := prefix + "-node"
	addNode(t, client, node, 1000, 2048, 0) // dead, so removal is immediate
	dep := prefix + "-web"
	addDeployment(t, client, dep, 3)

	statuses := []string{schema.StatusRunning, schema.StatusFailed, schema.StatusRunning}
	for i, status := range statuses {
		podID := fmt.Sprintf("pod%d", i)
		pod := &schema.PodSpec{
			Deployment: dep, PodID: podID, Status: status, NodeID: node,
			CPURequest: 100, MemRequest: 128,
			CreatedAt: fmt.Sprintf("%d", time.Now().Unix()),
		}
		if err := client.HashSet(ctx, schema.PodKey(dep, podID), schema.PodToMap(pod)); err != nil {
			t.Fatalf("write pod: %v", err)
		}
	}
	if err := client.HashSet(ctx, schema.DeploymentKey(dep),
		map[string]interface{}{schema.FieldDesiredReplicas: 2}); err != nil {
		t.Fatalf("set desired: %v", err)
	}

	if _, err := c.ReconcileDeployment(ctx, dep); err != nil {
		t.Fatalf("scale down: %v", err)
	}
	remaining := podsOf(t, client, dep)
	if len(remaining) != 2 {
		t.Fatalf("want 2 pods left, got %d", len(remaining))
	}
	for _, pod := range remaining {
		if pod.Status == schema.StatusFailed {
			t.Fatal("the Failed pod should have been removed before a Running one")
		}
	}
}

func TestReconcileAllReapsPodsOfDeletedDeployments(t *testing.T) {
	c, client, prefix := newTestController(t)
	ctx := context.Background()
	node := prefix + "-node"
	addNode(t, client, node, 1000, 2048, 0) // dead node: removal is immediate
	dep := prefix + "-gone"

	pod := &schema.PodSpec{
		Deployment: dep, PodID: "orphan", Status: schema.StatusRunning, NodeID: node,
		CPURequest: 100, MemRequest: 128, CreatedAt: "1",
	}
	if err := client.HashSet(ctx, schema.PodKey(dep, "orphan"), schema.PodToMap(pod)); err != nil {
		t.Fatalf("write pod: %v", err)
	}
	t.Cleanup(func() { client.DeleteKey(ctx, schema.PodKey(dep, "orphan")) })

	if _, err := c.ReconcileAll(ctx); err != nil {
		t.Fatalf("reconcile all: %v", err)
	}
	if got := len(podsOf(t, client, dep)); got != 0 {
		t.Fatalf("want orphaned pod reaped, got %d", got)
	}
}

// The sweep, not the event stream, is what makes reconciliation correct: state
// that drifted with no event published must still converge.
func TestSweeperConvergesWithoutAnyEvent(t *testing.T) {
	c, client, prefix := newTestController(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addNode(t, client, prefix+"-node", 1000, 2048, 10*time.Second)
	dep := prefix + "-web"
	addDeployment(t, client, dep, 2)

	go c.RunSweeper(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(podsOf(t, client, dep)) == 2 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("sweeper did not converge, got %d pods", len(podsOf(t, client, dep)))
}

func TestSubscriberReactsToDeploymentEvents(t *testing.T) {
	c, client, prefix := newTestController(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addNode(t, client, prefix+"-node", 1000, 2048, 10*time.Second)
	dep := prefix + "-web"
	addDeployment(t, client, dep, 2)

	go c.RunSubscriber(ctx)
	time.Sleep(200 * time.Millisecond) // let the subscription establish

	if err := events.Publish(ctx, client, events.DeploymentEvent{
		Event: events.EventUpdate, Deployment: dep, Desired: 2,
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(podsOf(t, client, dep)) == 2 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("subscriber did not react, got %d pods", len(podsOf(t, client, dep)))
}

func TestReconcileRejectsEmptyDeploymentName(t *testing.T) {
	c, _, _ := newTestController(t)
	if _, err := c.ReconcileDeployment(context.Background(), ""); err == nil {
		t.Fatal("want an error for an empty deployment name")
	}
}
