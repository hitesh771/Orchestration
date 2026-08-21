package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"mini-k8s/internal/redisclient"
	"mini-k8s/internal/schema"
)

// seedCluster writes one node, one deployment, and its pods, and cleans them up.
func seedCluster(t *testing.T, deployment string) *redisclient.Client {
	t.Helper()
	client, err := redisclient.New(testRedisAddr())
	if err != nil {
		t.Skipf("redis unavailable at %s: %v", testRedisAddr(), err)
	}
	ctx := context.Background()
	node := deployment + "-node"

	capacity := &schema.NodeCapacity{TotalCPU: 1000, TotalMem: 2048, AllocatedCPU: 200, AllocatedMem: 256}
	if err := client.HashSet(ctx, schema.NodeCapacityKey(node), schema.NodeCapacityToMap(capacity)); err != nil {
		t.Fatalf("write capacity: %v", err)
	}
	if err := client.SetAdd(ctx, schema.NodeRegistryKey(), node); err != nil {
		t.Fatalf("join registry: %v", err)
	}
	if err := client.SetKey(ctx, schema.NodeStatusKey(node), schema.NodeStatusReady, 30*time.Second); err != nil {
		t.Fatalf("write lease: %v", err)
	}

	dep := &schema.DeploymentSpec{
		Name: deployment, MinReplicas: 1, MaxReplicas: 5, DesiredReplicas: 2,
		CPURequest: 100, MemRequest: 128, TargetCPUPercent: 70,
		ExecPath: "/bin/true", Port: 9000, CreatedAt: "1",
	}
	if err := client.HashSet(ctx, schema.DeploymentKey(deployment), schema.DeploymentToMap(dep)); err != nil {
		t.Fatalf("write deployment: %v", err)
	}

	for i, status := range []string{schema.StatusRunning, schema.StatusPending} {
		podID := fmt.Sprintf("pod%d", i)
		pod := &schema.PodSpec{
			Deployment: deployment, PodID: podID, Status: status, NodeID: node,
			CPURequest: 100, MemRequest: 128, CreatedAt: "1", HostPort: 20000 + i,
		}
		if err := client.HashSet(ctx, schema.PodKey(deployment, podID), schema.PodToMap(pod)); err != nil {
			t.Fatalf("write pod: %v", err)
		}
	}

	t.Cleanup(func() {
		keys, _ := client.ScanKeys(ctx, schema.DeploymentPodsPattern(deployment))
		if len(keys) > 0 {
			client.DeleteKey(ctx, keys...)
		}
		client.SetRemove(ctx, schema.NodeRegistryKey(), node)
		client.DeleteKey(ctx, schema.NodeCapacityKey(node), schema.NodeStatusKey(node),
			schema.DeploymentKey(deployment))
		client.Close()
	})
	return client
}

func TestStateEndpointReportsClusterView(t *testing.T) {
	srv, name := newTestServer(t)
	seedCluster(t, name)

	resp, err := http.Get(srv.URL + "/state")
	if err != nil {
		t.Fatalf("GET /state: %v", err)
	}
	defer resp.Body.Close()

	var snapshot StateSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
		t.Fatalf("decode: %v", err)
	}

	var view *DeploymentView
	for i := range snapshot.Deployments {
		if snapshot.Deployments[i].Name == name {
			view = &snapshot.Deployments[i]
		}
	}
	if view == nil {
		t.Fatalf("deployment %s missing from the snapshot", name)
	}
	if view.DesiredReplicas != 2 {
		t.Errorf("desired = %d, want 2", view.DesiredReplicas)
	}
	// Only the Running pod counts as ready; the Pending one is not serving.
	if view.ReadyReplicas != 1 {
		t.Errorf("ready = %d, want 1 of the 2 pods", view.ReadyReplicas)
	}

	var podCount int
	for _, pod := range snapshot.Pods {
		if pod.Deployment == name {
			podCount++
		}
	}
	if podCount != 2 {
		t.Errorf("pods = %d, want 2", podCount)
	}

	var found bool
	for _, node := range snapshot.Nodes {
		if node.NodeID == name+"-node" {
			found = true
			if !node.Ready {
				t.Error("a node holding a lease should report ready")
			}
			if node.PodCount != 2 {
				t.Errorf("node pod_count = %d, want 2", node.PodCount)
			}
		}
	}
	if !found {
		t.Error("seeded node missing from the snapshot")
	}
}

// Key-scan order is arbitrary, so an unsorted view would reshuffle the tables
// between frames and make them unreadable.
func TestSnapshotOrderingIsStable(t *testing.T) {
	srv, name := newTestServer(t)
	seedCluster(t, name)

	var first string
	for i := 0; i < 3; i++ {
		resp, err := http.Get(srv.URL + "/state")
		if err != nil {
			t.Fatalf("GET /state: %v", err)
		}
		var snapshot StateSnapshot
		json.NewDecoder(resp.Body).Decode(&snapshot)
		resp.Body.Close()

		// Only this test's own pods are compared. The snapshot covers the whole
		// cluster, so pods belonging to other tests would come and go mid-run
		// and make the comparison about their lifecycle rather than ordering.
		var ids []string
		for _, pod := range snapshot.Pods {
			if pod.Deployment != name {
				continue
			}
			ids = append(ids, pod.Deployment+"/"+pod.PodID)
		}
		joined := strings.Join(ids, ",")
		if i == 0 {
			first = joined
			continue
		}
		if joined != first {
			t.Fatalf("pod ordering changed between snapshots:\n%s\n%s", first, joined)
		}
	}
}

func TestEventsEndpointStreamsSnapshots(t *testing.T) {
	srv, name := newTestServer(t)
	seedCluster(t, name)

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/events", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer resp.Body.Close()

	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("Content-Type = %q, want text/event-stream", got)
	}

	// The first frame must arrive without waiting for a tick, or a freshly
	// opened dashboard would sit blank for a full interval.
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var snapshot StateSnapshot
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &snapshot); err != nil {
			t.Fatalf("decode frame: %v", err)
		}
		for _, dep := range snapshot.Deployments {
			if dep.Name == name {
				return
			}
		}
		t.Fatalf("first frame did not include %s", name)
	}
	t.Fatal("no frame arrived")
}

func TestDashboardServesPageAtRoot(t *testing.T) {
	srv, _ := newTestServer(t)

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/html") {
		t.Errorf("Content-Type = %q, want text/html", got)
	}
	if resp.Header.Get("Content-Security-Policy") == "" {
		t.Error("want a CSP header: deployment names are user-supplied and reach this page")
	}
}

// "GET /" is ServeMux's catch-all, so an unknown path must still 404 rather than
// silently serving the dashboard.
func TestUnknownPathReturns404(t *testing.T) {
	srv, _ := newTestServer(t)

	resp, err := http.Get(srv.URL + "/no-such-path")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

// Stream ids are "ms-seq", so comparing them as strings would order "10-0"
// before "9-0" and silently drop log entries from the panel.
func TestStreamIDComparisonIsNumeric(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"10-0", "9-0", true},
		{"9-0", "10-0", false},
		{"100-5", "100-4", true},
		{"100-4", "100-5", false},
		{"100-0", "100-0", false},
		{"5", "4", true},
	}
	for _, c := range cases {
		if got := streamIDNewer(c.a, c.b); got != c.want {
			t.Errorf("streamIDNewer(%q, %q) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}

func TestObservedCPUAveragesRecentSamples(t *testing.T) {
	srv, name := newTestServer(t)
	client := seedCluster(t, name)
	ctx := context.Background()

	key := schema.TelemetryCPUKey(name, "pod0")
	for _, v := range []string{"40", "50", "60"} {
		if err := client.ListPush(ctx, key, v); err != nil {
			t.Fatalf("push sample: %v", err)
		}
	}
	t.Cleanup(func() { client.DeleteKey(ctx, key) })

	resp, err := http.Get(srv.URL + "/state")
	if err != nil {
		t.Fatalf("GET /state: %v", err)
	}
	defer resp.Body.Close()
	var snapshot StateSnapshot
	json.NewDecoder(resp.Body).Decode(&snapshot)

	for _, dep := range snapshot.Deployments {
		if dep.Name != name {
			continue
		}
		if dep.ObservedCPU != 50 {
			t.Fatalf("observed = %d%%, want the 50%% average of 40/50/60", dep.ObservedCPU)
		}
		return
	}
	t.Fatalf("deployment %s missing", name)
}
