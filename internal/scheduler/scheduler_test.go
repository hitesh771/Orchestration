package scheduler

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
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

// newTestScheduler returns a Scheduler and a unique prefix. Every node and
// deployment name is namespaced by that prefix so concurrent tests, and any
// leftover cluster state, cannot interfere.
func newTestScheduler(t *testing.T) (*Scheduler, *redisclient.Client, string) {
	t.Helper()
	client, err := redisclient.New(testRedisAddr())
	if err != nil {
		t.Skipf("redis unavailable at %s: %v", testRedisAddr(), err)
	}
	prefix := fmt.Sprintf("sched-%d", time.Now().UnixNano())
	t.Cleanup(func() { client.Close() })
	return New(client, logging.New("test", nil)), client, prefix
}

// addNode registers a live node (capacity hash + lease) and cleans it up.
func addNode(t *testing.T, client *redisclient.Client, nodeID string, totalCPU, totalMem, allocCPU, allocMem int) {
	t.Helper()
	ctx := context.Background()
	capacity := &schema.NodeCapacity{
		TotalCPU: totalCPU, TotalMem: totalMem,
		AllocatedCPU: allocCPU, AllocatedMem: allocMem,
	}
	if err := client.HashSet(ctx, schema.NodeCapacityKey(nodeID), schema.NodeCapacityToMap(capacity)); err != nil {
		t.Fatalf("write capacity for %s: %v", nodeID, err)
	}
	if err := client.SetKey(ctx, schema.NodeStatusKey(nodeID), schema.NodeStatusReady, time.Minute); err != nil {
		t.Fatalf("write lease for %s: %v", nodeID, err)
	}
	if err := client.SetAdd(ctx, schema.NodeRegistryKey(), nodeID); err != nil {
		t.Fatalf("register %s: %v", nodeID, err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		client.DeleteKey(ctx, schema.NodeCapacityKey(nodeID), schema.NodeStatusKey(nodeID))
		client.SetRemove(ctx, schema.NodeRegistryKey(), nodeID)
	})
}

func readCapacity(t *testing.T, client *redisclient.Client, nodeID string) *schema.NodeCapacity {
	t.Helper()
	fields, err := client.HashGet(context.Background(), schema.NodeCapacityKey(nodeID))
	if err != nil {
		t.Fatalf("read capacity for %s: %v", nodeID, err)
	}
	capacity, err := schema.MapToNodeCapacity(fields)
	if err != nil {
		t.Fatalf("parse capacity for %s: %v", nodeID, err)
	}
	return capacity
}

func cleanupPods(t *testing.T, client *redisclient.Client, deployment string) {
	t.Cleanup(func() {
		ctx := context.Background()
		keys, err := client.ScanKeys(ctx, schema.DeploymentPodsPattern(deployment))
		if err == nil && len(keys) > 0 {
			client.DeleteKey(ctx, keys...)
		}
	})
}

func TestReserveSucceedsWithinHeadroomAndIncrementsExactly(t *testing.T) {
	s, client, prefix := newTestScheduler(t)
	node := prefix + "-n1"
	addNode(t, client, node, 1000, 1024, 0, 0)

	ok, err := s.Reserve(context.Background(), node, 200, 128)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if !ok {
		t.Fatal("reservation should succeed on an empty node")
	}

	capacity := readCapacity(t, client, node)
	if capacity.AllocatedCPU != 200 || capacity.AllocatedMem != 128 {
		t.Errorf("allocated = cpu %d mem %d, want cpu 200 mem 128",
			capacity.AllocatedCPU, capacity.AllocatedMem)
	}
}

// A request must fit in BOTH dimensions; plenty of CPU cannot compensate for
// exhausted memory.
func TestReserveRefusesWhenEitherDimensionIsShort(t *testing.T) {
	s, client, prefix := newTestScheduler(t)
	ctx := context.Background()

	cases := []struct {
		name               string
		totalCPU, totalMem int
		allocCPU, allocMem int
		cpuReq, memReq     int
	}{
		{"cpu exhausted", 1000, 1024, 900, 0, 200, 128},
		{"memory exhausted", 1000, 1024, 0, 1000, 200, 128},
		{"both exhausted", 1000, 1024, 950, 1000, 200, 128},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node := fmt.Sprintf("%s-refuse-%d", prefix, i)
			addNode(t, client, node, tc.totalCPU, tc.totalMem, tc.allocCPU, tc.allocMem)

			ok, err := s.Reserve(ctx, node, tc.cpuReq, tc.memReq)
			if err != nil {
				t.Fatalf("Reserve: %v", err)
			}
			if ok {
				t.Error("reservation should have been refused")
			}
			// A refused reservation must leave the node untouched.
			capacity := readCapacity(t, client, node)
			if capacity.AllocatedCPU != tc.allocCPU || capacity.AllocatedMem != tc.allocMem {
				t.Errorf("refused reservation mutated node: allocated = cpu %d mem %d, want cpu %d mem %d",
					capacity.AllocatedCPU, capacity.AllocatedMem, tc.allocCPU, tc.allocMem)
			}
		})
	}
}

// An exact fit must be accepted: headroom equal to the request is enough.
func TestReserveAcceptsExactFit(t *testing.T) {
	s, client, prefix := newTestScheduler(t)
	node := prefix + "-exact"
	addNode(t, client, node, 1000, 1024, 800, 896)

	ok, err := s.Reserve(context.Background(), node, 200, 128)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if !ok {
		t.Fatal("exact fit should be accepted")
	}
	capacity := readCapacity(t, client, node)
	if capacity.AllocatedCPU != capacity.TotalCPU || capacity.AllocatedMem != capacity.TotalMem {
		t.Errorf("node should be exactly full: cpu %d/%d mem %d/%d",
			capacity.AllocatedCPU, capacity.TotalCPU, capacity.AllocatedMem, capacity.TotalMem)
	}
}

func TestReserveOnUnknownNodeFailsWithoutError(t *testing.T) {
	s, _, prefix := newTestScheduler(t)
	ok, err := s.Reserve(context.Background(), prefix+"-ghost", 100, 100)
	if err != nil {
		t.Fatalf("a vanished node should not be an error: %v", err)
	}
	if ok {
		t.Error("reservation on a nonexistent node should fail")
	}
}

// The core guarantee of Phase 3: concurrent reservations can never
// oversubscribe a node. 40 goroutines race for 5 slots; exactly 5 may win.
func TestConcurrentReservationsNeverOversubscribe(t *testing.T) {
	s, client, prefix := newTestScheduler(t)
	node := prefix + "-race"
	const (
		totalCPU    = 1000
		totalMem    = 1000
		cpuReq      = 200 // 1000/200 = 5 slots
		memReq      = 200
		wantWinners = 5
		racers      = 40
	)
	addNode(t, client, node, totalCPU, totalMem, 0, 0)

	var granted int64
	var firstErr atomic.Value
	start := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // release all goroutines at once to maximize contention
			ok, err := s.Reserve(context.Background(), node, cpuReq, memReq)
			if err != nil {
				firstErr.CompareAndSwap(nil, err)
				return
			}
			if ok {
				atomic.AddInt64(&granted, 1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if err := firstErr.Load(); err != nil {
		t.Fatalf("reservation error during race: %v", err)
	}
	if got := atomic.LoadInt64(&granted); got != wantWinners {
		t.Errorf("%d reservations granted, want exactly %d", got, wantWinners)
	}

	capacity := readCapacity(t, client, node)
	if capacity.AllocatedCPU > capacity.TotalCPU {
		t.Errorf("OVERSUBSCRIBED cpu: allocated %d > total %d", capacity.AllocatedCPU, capacity.TotalCPU)
	}
	if capacity.AllocatedMem > capacity.TotalMem {
		t.Errorf("OVERSUBSCRIBED mem: allocated %d > total %d", capacity.AllocatedMem, capacity.TotalMem)
	}
	if capacity.AllocatedCPU != wantWinners*cpuReq {
		t.Errorf("allocated cpu = %d, want %d", capacity.AllocatedCPU, wantWinners*cpuReq)
	}
}

func TestReleaseReturnsCapacityAndClampsAtZero(t *testing.T) {
	s, client, prefix := newTestScheduler(t)
	ctx := context.Background()
	node := prefix + "-rel"
	addNode(t, client, node, 1000, 1024, 400, 256)

	if err := s.Release(ctx, node, 200, 128); err != nil {
		t.Fatalf("Release: %v", err)
	}
	capacity := readCapacity(t, client, node)
	if capacity.AllocatedCPU != 200 || capacity.AllocatedMem != 128 {
		t.Errorf("after release allocated = cpu %d mem %d, want cpu 200 mem 128",
			capacity.AllocatedCPU, capacity.AllocatedMem)
	}

	// Over-releasing (e.g. a retry racing an eviction that already reset the
	// node) must clamp at zero, never go negative and invent capacity.
	if err := s.Release(ctx, node, 9999, 9999); err != nil {
		t.Fatalf("over-release: %v", err)
	}
	capacity = readCapacity(t, client, node)
	if capacity.AllocatedCPU != 0 || capacity.AllocatedMem != 0 {
		t.Errorf("over-release should clamp to 0/0, got cpu %d mem %d",
			capacity.AllocatedCPU, capacity.AllocatedMem)
	}
}

// A dead node is still in the registry until eviction runs, so placement must
// filter on the lease rather than on registry membership.
func TestLiveNodesExcludesNodesWithoutLease(t *testing.T) {
	s, client, prefix := newTestScheduler(t)
	ctx := context.Background()
	alive, dead := prefix+"-alive", prefix+"-dead"

	addNode(t, client, alive, 1000, 1024, 0, 0)
	addNode(t, client, dead, 1000, 1024, 0, 0)
	// Simulate the dead node's lease expiring.
	if err := client.DeleteKey(ctx, schema.NodeStatusKey(dead)); err != nil {
		t.Fatalf("expire lease: %v", err)
	}

	live, err := s.LiveNodes(ctx)
	if err != nil {
		t.Fatalf("LiveNodes: %v", err)
	}
	if !containsStr(live, alive) {
		t.Errorf("live node %s missing from %v", alive, live)
	}
	if containsStr(live, dead) {
		t.Errorf("node %s has no lease and must not be schedulable", dead)
	}
}

// Best-fit means the tightest-fitting node wins, so large gaps stay free for
// pods with larger requests.
func TestSchedulerPrefersTightestFit(t *testing.T) {
	s, client, prefix := newTestScheduler(t)
	ctx := context.Background()
	roomy, snug := prefix+"-roomy", prefix+"-snug"

	// Both fit a 200m/128MB pod, but snug has far less headroom.
	addNode(t, client, roomy, 1000, 1024, 0, 0)
	addNode(t, client, snug, 1000, 1024, 700, 768)

	dep := &schema.DeploymentSpec{
		Name: prefix + "-dep", CPURequest: 200, MemRequest: 128,
		MinReplicas: 1, MaxReplicas: 5, DesiredReplicas: 1, CreatedAt: "0",
	}
	cleanupPods(t, client, dep.Name)

	podIDs, err := s.ScheduleReplicas(ctx, dep, 1)
	if err != nil {
		t.Fatalf("ScheduleReplicas: %v", err)
	}
	if len(podIDs) != 1 {
		t.Fatalf("scheduled %d pods, want 1", len(podIDs))
	}

	fields, _ := client.HashGet(ctx, schema.PodKey(dep.Name, podIDs[0]))
	if got := fields[schema.FieldPodNodeID]; got != snug {
		t.Errorf("pod placed on %s, want tightest-fit node %s", got, snug)
	}
}

func TestScheduleReplicasCreatesPendingPodsAndAccountsExactly(t *testing.T) {
	s, client, prefix := newTestScheduler(t)
	ctx := context.Background()
	node := prefix + "-n1"
	addNode(t, client, node, 1000, 1024, 0, 0)

	dep := &schema.DeploymentSpec{
		Name: prefix + "-dep", CPURequest: 200, MemRequest: 128,
		MinReplicas: 3, MaxReplicas: 5, DesiredReplicas: 3, CreatedAt: "0",
	}
	cleanupPods(t, client, dep.Name)

	podIDs, err := s.ScheduleReplicas(ctx, dep, 3)
	if err != nil {
		t.Fatalf("ScheduleReplicas: %v", err)
	}
	if len(podIDs) != 3 {
		t.Fatalf("scheduled %d pods, want 3", len(podIDs))
	}

	seen := map[string]bool{}
	for _, id := range podIDs {
		if seen[id] {
			t.Errorf("duplicate pod id %s", id)
		}
		seen[id] = true

		fields, err := client.HashGet(ctx, schema.PodKey(dep.Name, id))
		if err != nil || len(fields) == 0 {
			t.Fatalf("pod %s hash missing (err=%v)", id, err)
		}
		pod, err := schema.MapToPod(fields)
		if err != nil {
			t.Fatalf("parse pod %s: %v", id, err)
		}
		if pod.Status != schema.StatusPending {
			t.Errorf("pod %s status = %q, want %q", id, pod.Status, schema.StatusPending)
		}
		if pod.NodeID != node {
			t.Errorf("pod %s bound to %q, want %q", id, pod.NodeID, node)
		}
		if pod.CPURequest != 200 || pod.MemRequest != 128 {
			t.Errorf("pod %s requests = %dm/%dMB, want 200m/128MB", id, pod.CPURequest, pod.MemRequest)
		}
	}

	// Capacity must rise by exactly request × replicas — no drift.
	capacity := readCapacity(t, client, node)
	if capacity.AllocatedCPU != 600 || capacity.AllocatedMem != 384 {
		t.Errorf("allocated = cpu %d mem %d, want cpu 600 mem 384",
			capacity.AllocatedCPU, capacity.AllocatedMem)
	}
}

// A saturated cluster must degrade to partial placement, not error out and
// not overcommit.
func TestScheduleReplicasPlacesPartiallyWhenClusterSaturated(t *testing.T) {
	s, client, prefix := newTestScheduler(t)
	ctx := context.Background()
	node := prefix + "-small"
	// Room for exactly 2 pods of 400m.
	addNode(t, client, node, 800, 2048, 0, 0)

	dep := &schema.DeploymentSpec{
		Name: prefix + "-dep", CPURequest: 400, MemRequest: 64,
		MinReplicas: 1, MaxReplicas: 10, DesiredReplicas: 5, CreatedAt: "0",
	}
	cleanupPods(t, client, dep.Name)

	podIDs, err := s.ScheduleReplicas(ctx, dep, 5)
	if err != nil {
		t.Fatalf("saturation must not be an error: %v", err)
	}
	if len(podIDs) != 2 {
		t.Fatalf("placed %d pods, want 2 (the node's real capacity)", len(podIDs))
	}

	capacity := readCapacity(t, client, node)
	if capacity.AllocatedCPU != 800 {
		t.Errorf("allocated cpu = %d, want 800", capacity.AllocatedCPU)
	}
	if capacity.AllocatedCPU > capacity.TotalCPU {
		t.Errorf("overcommitted: %d > %d", capacity.AllocatedCPU, capacity.TotalCPU)
	}
}

func TestScheduleReplicasSpreadsAcrossNodesWhenOneFills(t *testing.T) {
	s, client, prefix := newTestScheduler(t)
	ctx := context.Background()
	n1, n2 := prefix+"-a", prefix+"-b"
	addNode(t, client, n1, 400, 1024, 0, 0) // fits 2
	addNode(t, client, n2, 400, 1024, 0, 0) // fits 2

	dep := &schema.DeploymentSpec{
		Name: prefix + "-dep", CPURequest: 200, MemRequest: 64,
		MinReplicas: 4, MaxReplicas: 8, DesiredReplicas: 4, CreatedAt: "0",
	}
	cleanupPods(t, client, dep.Name)

	podIDs, err := s.ScheduleReplicas(ctx, dep, 4)
	if err != nil {
		t.Fatalf("ScheduleReplicas: %v", err)
	}
	if len(podIDs) != 4 {
		t.Fatalf("scheduled %d pods, want 4", len(podIDs))
	}

	perNode := map[string]int{}
	for _, id := range podIDs {
		fields, _ := client.HashGet(ctx, schema.PodKey(dep.Name, id))
		perNode[fields[schema.FieldPodNodeID]]++
	}
	if perNode[n1] != 2 || perNode[n2] != 2 {
		t.Errorf("distribution = %v, want 2 on each of %s and %s", perNode, n1, n2)
	}
}

func TestScheduleReplicasWithNoLiveNodesPlacesNothing(t *testing.T) {
	s, client, prefix := newTestScheduler(t)
	dep := &schema.DeploymentSpec{
		Name: prefix + "-dep", CPURequest: 100, MemRequest: 64,
		MinReplicas: 1, MaxReplicas: 3, DesiredReplicas: 2, CreatedAt: "0",
	}
	cleanupPods(t, client, dep.Name)

	podIDs, err := s.ScheduleReplicas(context.Background(), dep, 2)
	if err != nil {
		t.Fatalf("an empty cluster must not be an error: %v", err)
	}
	if len(podIDs) != 0 {
		t.Errorf("placed %d pods with no live nodes, want 0", len(podIDs))
	}
}

func TestScheduleReplicasZeroOrNegativeCountIsNoop(t *testing.T) {
	s, client, prefix := newTestScheduler(t)
	node := prefix + "-n1"
	addNode(t, client, node, 1000, 1024, 0, 0)
	dep := &schema.DeploymentSpec{
		Name: prefix + "-dep", CPURequest: 100, MemRequest: 64, CreatedAt: "0",
	}

	for _, count := range []int{0, -3} {
		podIDs, err := s.ScheduleReplicas(context.Background(), dep, count)
		if err != nil || len(podIDs) != 0 {
			t.Errorf("count=%d: got %d pods err=%v, want 0 pods no error", count, len(podIDs), err)
		}
	}
	capacity := readCapacity(t, client, node)
	if capacity.AllocatedCPU != 0 {
		t.Errorf("no-op scheduling reserved %dm CPU, want 0", capacity.AllocatedCPU)
	}
}

func containsStr(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
