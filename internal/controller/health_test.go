package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"mini-k8s/internal/logging"
	"mini-k8s/internal/redisclient"
	"mini-k8s/internal/schema"
)

func newTestHealthController(t *testing.T) (*HealthController, *redisclient.Client, string) {
	t.Helper()
	client, err := redisclient.New(testRedisAddr())
	if err != nil {
		t.Skipf("redis unavailable at %s: %v", testRedisAddr(), err)
	}
	t.Cleanup(func() { client.Close() })
	prefix := fmt.Sprintf("health-%d", time.Now().UnixNano())
	return NewHealthController(client, logging.New("test", nil), 50*time.Millisecond), client, prefix
}

// writePod writes a pod bound to a node and cleans it up.
func writePod(t *testing.T, client *redisclient.Client, deployment, podID, nodeID, status string) {
	t.Helper()
	ctx := context.Background()
	pod := &schema.PodSpec{
		Deployment: deployment, PodID: podID, Status: status, NodeID: nodeID,
		CPURequest: 100, MemRequest: 128, CreatedAt: "1",
	}
	if err := client.HashSet(ctx, schema.PodKey(deployment, podID), schema.PodToMap(pod)); err != nil {
		t.Fatalf("write pod: %v", err)
	}
	t.Cleanup(func() { client.DeleteKey(ctx, schema.PodKey(deployment, podID)) })
}

func podStatus(t *testing.T, client *redisclient.Client, deployment, podID string) string {
	t.Helper()
	status, err := client.HashGetField(context.Background(), schema.PodKey(deployment, podID), schema.FieldPodStatus)
	if err != nil {
		t.Fatalf("read status: %v", err)
	}
	return status
}

func TestEvictNodeMarksPodsEvictedAndReleasesCapacity(t *testing.T) {
	h, client, prefix := newTestHealthController(t)
	ctx := context.Background()
	node := prefix + "-node"
	dep := prefix + "-web"

	// Registered with allocations but no lease: dead, not yet evicted.
	addNode(t, client, node, 1000, 2048, 0)
	if err := client.HashSet(ctx, schema.NodeCapacityKey(node), map[string]interface{}{
		schema.FieldAllocatedCPU: 200, schema.FieldAllocatedMem: 256,
	}); err != nil {
		t.Fatalf("set allocations: %v", err)
	}
	writePod(t, client, dep, "p1", node, schema.StatusRunning)
	writePod(t, client, dep, "p2", node, schema.StatusRunning)

	if err := h.EvictNode(ctx, node); err != nil {
		t.Fatalf("evict: %v", err)
	}

	for _, id := range []string{"p1", "p2"} {
		if got := podStatus(t, client, dep, id); got != schema.StatusEvicted {
			t.Fatalf("pod %s: want Evicted, got %q", id, got)
		}
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
		t.Fatalf("want allocations zeroed, got cpu=%d mem=%d", cap.AllocatedCPU, cap.AllocatedMem)
	}

	members, err := client.SetMembers(ctx, schema.NodeRegistryKey())
	if err != nil {
		t.Fatalf("read registry: %v", err)
	}
	for _, m := range members {
		if m == node {
			t.Fatal("evicted node should have left the registry")
		}
	}
}

// A node that renewed its lease between detection and eviction is alive again.
func TestEvictNodeSkipsNodeThatCameBack(t *testing.T) {
	h, client, prefix := newTestHealthController(t)
	ctx := context.Background()
	node := prefix + "-node"
	dep := prefix + "-web"
	addNode(t, client, node, 1000, 2048, 10*time.Second) // holds a lease
	writePod(t, client, dep, "p1", node, schema.StatusRunning)

	if err := h.EvictNode(ctx, node); err != nil {
		t.Fatalf("evict: %v", err)
	}
	if got := podStatus(t, client, dep, "p1"); got != schema.StatusRunning {
		t.Fatalf("want a live node's pod untouched, got %q", got)
	}
}

func TestEvictNodeLeavesOtherNodesPodsAlone(t *testing.T) {
	h, client, prefix := newTestHealthController(t)
	ctx := context.Background()
	dead, live := prefix+"-dead", prefix+"-live"
	dep := prefix + "-web"
	addNode(t, client, dead, 1000, 2048, 0)
	addNode(t, client, live, 1000, 2048, 10*time.Second)
	writePod(t, client, dep, "onDead", dead, schema.StatusRunning)
	writePod(t, client, dep, "onLive", live, schema.StatusRunning)

	if err := h.EvictNode(ctx, dead); err != nil {
		t.Fatalf("evict: %v", err)
	}
	if got := podStatus(t, client, dep, "onDead"); got != schema.StatusEvicted {
		t.Fatalf("dead node's pod: want Evicted, got %q", got)
	}
	if got := podStatus(t, client, dep, "onLive"); got != schema.StatusRunning {
		t.Fatalf("live node's pod: want Running, got %q", got)
	}
}

// The sweep is the correctness path: it must evict a lease-less node even when
// no expiry notification was ever delivered.
func TestSweepEvictsNodesWithNoLease(t *testing.T) {
	h, client, prefix := newTestHealthController(t)
	ctx := context.Background()
	node := prefix + "-node"
	dep := prefix + "-web"
	addNode(t, client, node, 1000, 2048, 0)
	writePod(t, client, dep, "p1", node, schema.StatusRunning)

	if err := h.SweepOnce(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if got := podStatus(t, client, dep, "p1"); got != schema.StatusEvicted {
		t.Fatalf("want Evicted after sweep, got %q", got)
	}
}

// Eviction is idempotent: two controllers racing on the same dead node, or a
// sweep following an expiry notification, must not double-count anything.
func TestEvictNodeIsIdempotent(t *testing.T) {
	h, client, prefix := newTestHealthController(t)
	ctx := context.Background()
	node := prefix + "-node"
	dep := prefix + "-web"
	addNode(t, client, node, 1000, 2048, 0)
	writePod(t, client, dep, "p1", node, schema.StatusRunning)

	for i := 0; i < 3; i++ {
		if err := h.EvictNode(ctx, node); err != nil {
			t.Fatalf("evict %d: %v", i, err)
		}
	}
	if got := podStatus(t, client, dep, "p1"); got != schema.StatusEvicted {
		t.Fatalf("want Evicted, got %q", got)
	}
}

func TestNodeIDFromStatusKey(t *testing.T) {
	cases := []struct {
		key    string
		want   string
		wantOK bool
	}{
		{"node:node-a:status", "node-a", true},
		{"pod:web:abc", "", false},
		{"node::status", "", false},
		{"node:a:b:status", "", false}, // ambiguous, refuse rather than guess
		{"deployment:web", "", false},
		{"", "", false},
	}
	for _, c := range cases {
		got, ok := nodeIDFromStatusKey(c.key)
		if got != c.want || ok != c.wantOK {
			t.Errorf("%q: got (%q,%v), want (%q,%v)", c.key, got, ok, c.want, c.wantOK)
		}
	}
}

// Eviction hands replacement to the replica controller rather than doing it, so
// the two are not both responsible for the same decision.
func TestEvictedPodsAreReplacedByTheReplicaController(t *testing.T) {
	h, client, prefix := newTestHealthController(t)
	c := NewReplicaController(client, logging.New("test", nil), 50*time.Millisecond)
	ctx := context.Background()

	dead, live := prefix+"-dead", prefix+"-live"
	addNode(t, client, dead, 1000, 2048, 0)
	addNode(t, client, live, 1000, 2048, 10*time.Second)
	dep := prefix + "-web"
	addDeployment(t, client, dep, 2)
	writePod(t, client, dep, "p1", dead, schema.StatusRunning)
	writePod(t, client, dep, "p2", dead, schema.StatusRunning)

	if err := h.EvictNode(ctx, dead); err != nil {
		t.Fatalf("evict: %v", err)
	}
	delta, err := c.ReconcileDeployment(ctx, dep)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if delta.Created != 2 {
		t.Fatalf("want 2 replacements on the surviving node, got %+v", delta)
	}
	for _, pod := range podsOf(t, client, dep) {
		if pod.NodeID != live {
			t.Fatalf("replacement landed on %q, want the live node", pod.NodeID)
		}
	}
}
