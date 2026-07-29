package node

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"mini-k8s/internal/config"
	"mini-k8s/internal/logging"
	"mini-k8s/internal/redisclient"
	"mini-k8s/internal/schema"
)

// testRedisAddr is where tests expect a live Redis. Tests skip when it is
// unreachable so the suite stays runnable without infrastructure.
func testRedisAddr() string {
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		return addr
	}
	return "localhost:6379"
}

// newTestRegistrar builds a Registrar bound to a node ID unique to this test.
// Because every capacity/lease key and every allocation lookup is namespaced
// by node ID, tests cannot see each other's state.
func newTestRegistrar(t *testing.T, totalCPU, totalMem int) (*Registrar, *redisclient.Client, string) {
	t.Helper()

	client, err := redisclient.New(testRedisAddr())
	if err != nil {
		t.Skipf("redis unavailable at %s: %v", testRedisAddr(), err)
	}

	nodeID := fmt.Sprintf("test-%s-%d", t.Name(), time.Now().UnixNano())
	cfg := &config.Config{
		NodeID:            nodeID,
		TotalCPU:          totalCPU,
		TotalMem:          totalMem,
		HeartbeatInterval: 100 * time.Millisecond,
		LeaseTTL:          2 * time.Second,
	}

	// A logger with a nil client keeps test output off the shared log stream.
	r, err := NewRegistrar(client, logging.New("test", nil), cfg)
	if err != nil {
		t.Fatalf("NewRegistrar: %v", err)
	}

	t.Cleanup(func() {
		ctx := context.Background()
		client.DeleteKey(ctx, schema.NodeCapacityKey(nodeID), schema.NodeStatusKey(nodeID))
		client.SetRemove(ctx, schema.NodeRegistryKey(), nodeID)
		client.Close()
	})

	return r, client, nodeID
}

// writePod creates a pod hash bound to nodeID and registers its cleanup.
func writePod(t *testing.T, client *redisclient.Client, nodeID, deployment, podID, status string, cpu, mem int) {
	t.Helper()
	ctx := context.Background()
	pod := &schema.PodSpec{
		Deployment: deployment,
		PodID:      podID,
		Status:     status,
		NodeID:     nodeID,
		CPURequest: cpu,
		MemRequest: mem,
		CreatedAt:  "0",
	}
	if err := client.HashSet(ctx, schema.PodKey(deployment, podID), schema.PodToMap(pod)); err != nil {
		t.Fatalf("write pod %s/%s: %v", deployment, podID, err)
	}
	t.Cleanup(func() { client.DeleteKey(context.Background(), schema.PodKey(deployment, podID)) })
}

func TestNewRegistrarRejectsHeartbeatTooCloseToLeaseTTL(t *testing.T) {
	client, err := redisclient.New(testRedisAddr())
	if err != nil {
		t.Skipf("redis unavailable: %v", err)
	}
	defer client.Close()
	logger := logging.New("test", nil)

	cases := []struct {
		name      string
		heartbeat time.Duration
		ttl       time.Duration
	}{
		{"interval equals ttl", 10 * time.Second, 10 * time.Second},
		{"interval exceeds ttl", 15 * time.Second, 10 * time.Second},
		{"interval is exactly half the ttl", 5 * time.Second, 10 * time.Second},
		{"zero interval", 0, 10 * time.Second},
		{"zero ttl", 3 * time.Second, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{
				NodeID: "n", TotalCPU: 1000, TotalMem: 1024,
				HeartbeatInterval: tc.heartbeat, LeaseTTL: tc.ttl,
			}
			if _, err := NewRegistrar(client, logger, cfg); err == nil {
				t.Fatalf("expected rejection for heartbeat=%s ttl=%s, got none", tc.heartbeat, tc.ttl)
			}
		})
	}

	// The documented default (3s refresh, 10s lease) must be accepted.
	cfg := &config.Config{
		NodeID: "n", TotalCPU: 1000, TotalMem: 1024,
		HeartbeatInterval: 3 * time.Second, LeaseTTL: 10 * time.Second,
	}
	if _, err := NewRegistrar(client, logger, cfg); err != nil {
		t.Fatalf("default timing should be valid, got: %v", err)
	}
}

func TestNewRegistrarRejectsNonPositiveCapacity(t *testing.T) {
	client, err := redisclient.New(testRedisAddr())
	if err != nil {
		t.Skipf("redis unavailable: %v", err)
	}
	defer client.Close()

	for _, tc := range []struct{ cpu, mem int }{{0, 1024}, {1000, 0}, {-1, -1}} {
		cfg := &config.Config{
			NodeID: "n", TotalCPU: tc.cpu, TotalMem: tc.mem,
			HeartbeatInterval: 3 * time.Second, LeaseTTL: 10 * time.Second,
		}
		if _, err := NewRegistrar(client, logging.New("test", nil), cfg); err == nil {
			t.Errorf("expected rejection for cpu=%d mem=%d", tc.cpu, tc.mem)
		}
	}
}

func TestRegisterJoinsRegistryAndWritesCapacity(t *testing.T) {
	r, client, nodeID := newTestRegistrar(t, 1000, 1024)
	ctx := context.Background()

	if err := r.Register(ctx); err != nil {
		t.Fatalf("Register: %v", err)
	}

	members, err := client.SetMembers(ctx, schema.NodeRegistryKey())
	if err != nil {
		t.Fatalf("read registry: %v", err)
	}
	if !contains(members, nodeID) {
		t.Errorf("node %s missing from registry", nodeID)
	}

	fields, err := client.HashGet(ctx, schema.NodeCapacityKey(nodeID))
	if err != nil {
		t.Fatalf("read capacity: %v", err)
	}
	cap, err := schema.MapToNodeCapacity(fields)
	if err != nil {
		t.Fatalf("parse capacity: %v", err)
	}
	if cap.TotalCPU != 1000 || cap.TotalMem != 1024 {
		t.Errorf("totals = cpu %d mem %d, want cpu 1000 mem 1024", cap.TotalCPU, cap.TotalMem)
	}
	// A fresh node with no pods must start with nothing allocated.
	if cap.AllocatedCPU != 0 || cap.AllocatedMem != 0 {
		t.Errorf("fresh node allocations = cpu %d mem %d, want 0/0", cap.AllocatedCPU, cap.AllocatedMem)
	}
}

func TestRegisterIsIdempotent(t *testing.T) {
	r, client, nodeID := newTestRegistrar(t, 1000, 1024)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := r.Register(ctx); err != nil {
			t.Fatalf("Register call %d: %v", i+1, err)
		}
	}

	members, err := client.SetMembers(ctx, schema.NodeRegistryKey())
	if err != nil {
		t.Fatalf("read registry: %v", err)
	}
	n := 0
	for _, m := range members {
		if m == nodeID {
			n++
		}
	}
	if n != 1 {
		t.Errorf("node appears %d times in registry, want exactly 1", n)
	}
}

// The statelessness guarantee: a restarting agent must recover the capacity
// its live pods are still holding instead of resetting allocations to zero.
func TestRegisterRebuildsAllocationsFromLivePods(t *testing.T) {
	r, client, nodeID := newTestRegistrar(t, 1000, 1024)
	ctx := context.Background()
	dep := nodeID + "-dep"

	writePod(t, client, nodeID, dep, "pod-a", schema.StatusRunning, 200, 128)
	writePod(t, client, nodeID, dep, "pod-b", schema.StatusPending, 150, 64)

	if err := r.Register(ctx); err != nil {
		t.Fatalf("Register: %v", err)
	}

	fields, _ := client.HashGet(ctx, schema.NodeCapacityKey(nodeID))
	cap, err := schema.MapToNodeCapacity(fields)
	if err != nil {
		t.Fatalf("parse capacity: %v", err)
	}
	if cap.AllocatedCPU != 350 {
		t.Errorf("allocated cpu = %d, want 350 (200 running + 150 pending)", cap.AllocatedCPU)
	}
	if cap.AllocatedMem != 192 {
		t.Errorf("allocated mem = %d, want 192 (128 + 64)", cap.AllocatedMem)
	}
}

func TestReconcileAllocationsIgnoresOtherNodesAndEvictedPods(t *testing.T) {
	r, client, nodeID := newTestRegistrar(t, 1000, 1024)
	ctx := context.Background()
	dep := nodeID + "-dep"

	writePod(t, client, nodeID, dep, "mine-running", schema.StatusRunning, 100, 32)
	// Failed and CrashLoopBackOff pods are still bound here awaiting restart,
	// so their reservations must be preserved.
	writePod(t, client, nodeID, dep, "mine-failed", schema.StatusFailed, 100, 32)
	writePod(t, client, nodeID, dep, "mine-backoff", schema.StatusCrashLoopBackOff, 100, 32)
	// Evicted pods have had their capacity released already.
	writePod(t, client, nodeID, dep, "mine-evicted", schema.StatusEvicted, 500, 500)
	// A pod on a different node must never count against this one.
	writePod(t, client, "some-other-node", dep, "theirs", schema.StatusRunning, 900, 900)

	cpu, mem, err := r.ReconcileAllocations(ctx)
	if err != nil {
		t.Fatalf("ReconcileAllocations: %v", err)
	}
	if cpu != 300 {
		t.Errorf("cpu = %d, want 300 (running+failed+backoff, excluding evicted and other nodes)", cpu)
	}
	if mem != 96 {
		t.Errorf("mem = %d, want 96", mem)
	}
}

func TestHeartbeatSetsLeaseWithTTLAndRefreshes(t *testing.T) {
	r, client, nodeID := newTestRegistrar(t, 1000, 1024)
	ctx := context.Background()

	if err := r.Heartbeat(ctx); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	val, err := client.GetKey(ctx, schema.NodeStatusKey(nodeID))
	if err != nil {
		t.Fatalf("read lease: %v", err)
	}
	if val != schema.NodeStatusReady {
		t.Errorf("lease value = %q, want %q", val, schema.NodeStatusReady)
	}

	// Let the TTL decay, then confirm a refresh pushes it back up rather
	// than letting it run down.
	time.Sleep(600 * time.Millisecond)
	if err := r.Heartbeat(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	ok, err := client.Exists(ctx, schema.NodeStatusKey(nodeID))
	if err != nil || !ok {
		t.Fatalf("lease should exist after refresh (exists=%v err=%v)", ok, err)
	}
}

// Lease expiry is the only node-death signal, so it must actually fire when
// the agent stops refreshing.
func TestLeaseExpiresWhenHeartbeatStops(t *testing.T) {
	r, client, nodeID := newTestRegistrar(t, 1000, 1024)
	ctx := context.Background()
	r.leaseTTL = 1 * time.Second

	if err := r.Heartbeat(ctx); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if ok, _ := client.Exists(ctx, schema.NodeStatusKey(nodeID)); !ok {
		t.Fatal("lease should exist immediately after heartbeat")
	}

	time.Sleep(1500 * time.Millisecond)

	ok, err := client.Exists(ctx, schema.NodeStatusKey(nodeID))
	if err != nil {
		t.Fatalf("check lease: %v", err)
	}
	if ok {
		t.Error("lease still present after TTL elapsed with no refresh")
	}
}

func TestRunHeartbeatLoopKeepsLeaseAliveAndStopsOnContextCancel(t *testing.T) {
	r, client, nodeID := newTestRegistrar(t, 1000, 1024)
	r.heartbeatInterval = 100 * time.Millisecond
	r.leaseTTL = 400 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.RunHeartbeatLoop(ctx)
		close(done)
	}()

	// Across several TTL windows the lease must never lapse while the loop runs.
	for i := 0; i < 8; i++ {
		time.Sleep(150 * time.Millisecond)
		ok, err := client.Exists(context.Background(), schema.NodeStatusKey(nodeID))
		if err != nil {
			t.Fatalf("check lease: %v", err)
		}
		if !ok {
			t.Fatalf("lease lapsed while heartbeat loop was running (check %d)", i+1)
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeat loop did not exit on context cancel")
	}
}

// A clean shutdown drops the lease immediately but stays in the registry, so
// the Health Controller still evicts and reschedules the node's pods.
func TestDeregisterDropsLeaseButKeepsRegistryMembership(t *testing.T) {
	r, client, nodeID := newTestRegistrar(t, 1000, 1024)
	ctx := context.Background()

	if err := r.Register(ctx); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := r.Heartbeat(ctx); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if err := r.Deregister(ctx); err != nil {
		t.Fatalf("Deregister: %v", err)
	}

	ok, err := client.Exists(ctx, schema.NodeStatusKey(nodeID))
	if err != nil {
		t.Fatalf("check lease: %v", err)
	}
	if ok {
		t.Error("lease should be gone right after deregister")
	}

	members, _ := client.SetMembers(ctx, schema.NodeRegistryKey())
	if !contains(members, nodeID) {
		t.Error("node should remain in registry so its pods are evicted and rescheduled")
	}
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
