package api

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"mini-k8s/internal/redisclient"
	"mini-k8s/internal/schema"
)

func testRedisAddr() string {
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		return addr
	}
	return "localhost:6379"
}

// newTestStore returns a store and a unique deployment name so concurrent
// tests and leftover cluster state cannot collide.
func newTestStore(t *testing.T) (*DeploymentStore, *redisclient.Client, string) {
	t.Helper()
	client, err := redisclient.New(testRedisAddr())
	if err != nil {
		t.Skipf("redis unavailable at %s: %v", testRedisAddr(), err)
	}
	name := fmt.Sprintf("t-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		client.DeleteKey(context.Background(), schema.DeploymentKey(name))
		client.Close()
	})
	return NewDeploymentStore(client), client, name
}

func requestFor(name string) *DeploymentRequest {
	r := validRequest()
	r.Name = name
	return r
}

func TestApplyCreatesWithDesiredAtMinReplicas(t *testing.T) {
	store, _, name := newTestStore(t)
	req := requestFor(name)
	req.MinReplicas = intPtr(2)
	req.MaxReplicas = intPtr(6)

	dep, existed, err := store.Apply(context.Background(), req)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if existed {
		t.Error("a fresh deployment should not report as existing")
	}
	if dep.DesiredReplicas != 2 {
		t.Errorf("desired_replicas = %d, want 2 (min_replicas on create)", dep.DesiredReplicas)
	}
	if dep.CreatedAt == "" {
		t.Error("created_at should be stamped on create")
	}
}

func TestApplyRoundTripsEveryFieldThroughRedis(t *testing.T) {
	store, _, name := newTestStore(t)
	ctx := context.Background()

	if _, _, err := store.Apply(ctx, requestFor(name)); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	got, found, err := store.Get(ctx, name)
	if err != nil || !found {
		t.Fatalf("Get after Apply: found=%v err=%v", found, err)
	}
	if got.Name != name {
		t.Errorf("name = %q, want %q", got.Name, name)
	}
	if got.ExecPath != "/usr/local/bin/payment-api" {
		t.Errorf("exec_path = %q, did not survive the round trip", got.ExecPath)
	}
	if got.Port != 8080 {
		t.Errorf("port = %d, want 8080", got.Port)
	}
	if got.TargetCPUPercent != 50 {
		t.Errorf("target_cpu_percentage = %d, want 50", got.TargetCPUPercent)
	}
	if got.CPURequest != 150 || got.MemRequest != 128 {
		t.Errorf("requests = %dm/%dMB, want 150m/128MB", got.CPURequest, got.MemRequest)
	}
}

// The decision that matters: editing an unrelated field must not silently
// undo the autoscaler's work by collapsing a scaled-up deployment.
func TestApplyUpdatePreservesDesiredReplicasAndCreatedAt(t *testing.T) {
	store, _, name := newTestStore(t)
	ctx := context.Background()

	created, _, err := store.Apply(ctx, requestFor(name))
	if err != nil {
		t.Fatalf("initial Apply: %v", err)
	}

	// The autoscaler raises the count under load.
	if _, err := store.SetDesiredReplicas(ctx, name, 5); err != nil {
		t.Fatalf("SetDesiredReplicas: %v", err)
	}

	// The user then edits an unrelated field.
	req := requestFor(name)
	req.MemRequest = intPtr(256)
	updated, existed, err := store.Apply(ctx, req)
	if err != nil {
		t.Fatalf("update Apply: %v", err)
	}
	if !existed {
		t.Error("update should report the deployment as existing")
	}
	if updated.DesiredReplicas != 5 {
		t.Errorf("desired_replicas = %d, want 5 preserved: an unrelated edit must not "+
			"drop capacity that the autoscaler added under load", updated.DesiredReplicas)
	}
	if updated.CreatedAt != created.CreatedAt {
		t.Errorf("created_at changed on update (%q -> %q)", created.CreatedAt, updated.CreatedAt)
	}
	if updated.MemRequest != 256 {
		t.Errorf("mem_request = %d, want the updated 256", updated.MemRequest)
	}
}

// Preserving desired must still respect bounds that the edit tightened.
func TestApplyUpdateClampsDesiredIntoNewBounds(t *testing.T) {
	store, _, name := newTestStore(t)
	ctx := context.Background()

	if _, _, err := store.Apply(ctx, requestFor(name)); err != nil {
		t.Fatalf("initial Apply: %v", err)
	}
	if _, err := store.SetDesiredReplicas(ctx, name, 6); err != nil {
		t.Fatalf("SetDesiredReplicas: %v", err)
	}

	// Tighten max below the current desired count.
	req := requestFor(name)
	req.MinReplicas = intPtr(1)
	req.MaxReplicas = intPtr(3)
	updated, _, err := store.Apply(ctx, req)
	if err != nil {
		t.Fatalf("update Apply: %v", err)
	}
	if updated.DesiredReplicas != 3 {
		t.Errorf("desired_replicas = %d, want 3 (clamped to the new max)", updated.DesiredReplicas)
	}

	// Raising min above the current desired count must lift it.
	req2 := requestFor(name)
	req2.MinReplicas = intPtr(5)
	req2.MaxReplicas = intPtr(8)
	updated2, _, err := store.Apply(ctx, req2)
	if err != nil {
		t.Fatalf("second update: %v", err)
	}
	if updated2.DesiredReplicas != 5 {
		t.Errorf("desired_replicas = %d, want 5 (lifted to the new min)", updated2.DesiredReplicas)
	}
}

func TestGetReportsMissingDeploymentWithoutError(t *testing.T) {
	store, _, name := newTestStore(t)
	dep, found, err := store.Get(context.Background(), name+"-absent")
	if err != nil {
		t.Fatalf("a missing deployment is not an error: %v", err)
	}
	if found || dep != nil {
		t.Errorf("found=%v dep=%v, want not found", found, dep)
	}
}

func TestSetDesiredReplicasClampsToDeclaredBounds(t *testing.T) {
	store, _, name := newTestStore(t)
	ctx := context.Background()

	req := requestFor(name)
	req.MinReplicas = intPtr(2)
	req.MaxReplicas = intPtr(6)
	if _, _, err := store.Apply(ctx, req); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	for _, tc := range []struct{ requested, want int }{
		{0, 2},  // below min lifts to min
		{1, 2},  // still below min
		{4, 4},  // inside bounds is honoured
		{99, 6}, // above max clamps to max
		{6, 6},  // exactly max
	} {
		dep, err := store.SetDesiredReplicas(ctx, name, tc.requested)
		if err != nil {
			t.Fatalf("SetDesiredReplicas(%d): %v", tc.requested, err)
		}
		if dep.DesiredReplicas != tc.want {
			t.Errorf("requested %d -> desired %d, want %d", tc.requested, dep.DesiredReplicas, tc.want)
		}
	}
}

func TestSetDesiredReplicasOnMissingDeploymentErrors(t *testing.T) {
	store, _, name := newTestStore(t)
	if _, err := store.SetDesiredReplicas(context.Background(), name+"-absent", 3); err == nil {
		t.Error("scaling a nonexistent deployment should error")
	}
}

func TestListReturnsStoredDeploymentsSortedByName(t *testing.T) {
	store, client, name := newTestStore(t)
	ctx := context.Background()

	// Create two deployments whose names sort in a known order.
	a, b := name+"-aaa", name+"-bbb"
	for _, n := range []string{b, a} { // insert out of order on purpose
		if _, _, err := store.Apply(ctx, requestFor(n)); err != nil {
			t.Fatalf("Apply %s: %v", n, err)
		}
		defer client.DeleteKey(ctx, schema.DeploymentKey(n))
	}

	all, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	var idxA, idxB = -1, -1
	for i, d := range all {
		switch d.Name {
		case a:
			idxA = i
		case b:
			idxB = i
		}
	}
	if idxA < 0 || idxB < 0 {
		t.Fatalf("both deployments should be listed (idxA=%d idxB=%d)", idxA, idxB)
	}
	if idxA > idxB {
		t.Errorf("listing is not sorted by name: %s at %d came after %s at %d", a, idxA, b, idxB)
	}
}

// A single unreadable hash must not blank the whole listing.
func TestListSkipsMalformedDeployments(t *testing.T) {
	store, client, name := newTestStore(t)
	ctx := context.Background()

	if _, _, err := store.Apply(ctx, requestFor(name)); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	broken := name + "-broken"
	if err := client.HashSet(ctx, schema.DeploymentKey(broken),
		map[string]interface{}{schema.FieldDeploymentName: broken, schema.FieldMinReplicas: "not-a-number"}); err != nil {
		t.Fatalf("write malformed deployment: %v", err)
	}
	defer client.DeleteKey(ctx, schema.DeploymentKey(broken))

	all, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List should tolerate a malformed hash: %v", err)
	}

	var sawGood, sawBroken bool
	for _, d := range all {
		if d.Name == name {
			sawGood = true
		}
		if d.Name == broken {
			sawBroken = true
		}
	}
	if !sawGood {
		t.Error("the valid deployment should still be listed")
	}
	if sawBroken {
		t.Error("the malformed deployment should be skipped")
	}
}
