package ingress

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

func newTestController(t *testing.T) (*Controller, *redisclient.Client, string) {
	t.Helper()
	client, err := redisclient.New(testRedisAddr())
	if err != nil {
		t.Skipf("redis unavailable at %s: %v", testRedisAddr(), err)
	}
	t.Cleanup(func() { client.Close() })
	prefix := fmt.Sprintf("ing-%d", time.Now().UnixNano())
	c := New(client, logging.New("test", nil), Config{
		ConfigPath: filepath.Join(t.TempDir(), "upstreams.conf"),
		// A command that certainly does not exist, so the reload path is
		// exercised without depending on a real Nginx.
		ReloadCommand: []string{"minik8s-no-such-reload-binary"},
		Debounce:      20 * time.Millisecond,
	})
	return c, client, prefix
}

func writePod(t *testing.T, client *redisclient.Client, deployment, podID, status string, port int) {
	t.Helper()
	ctx := context.Background()
	pod := &schema.PodSpec{
		Deployment: deployment, PodID: podID, Status: status, NodeID: "n1",
		CPURequest: 100, MemRequest: 128, CreatedAt: "1", HostPort: port,
	}
	if err := client.HashSet(ctx, schema.PodKey(deployment, podID), schema.PodToMap(pod)); err != nil {
		t.Fatalf("write pod: %v", err)
	}
	t.Cleanup(func() { client.DeleteKey(ctx, schema.PodKey(deployment, podID)) })
}

func TestSyncWritesUpstreamsForRunningPods(t *testing.T) {
	c, client, prefix := newTestController(t)
	dep := prefix + "-web"
	writePod(t, client, dep, "p1", schema.StatusRunning, 20001)
	writePod(t, client, dep, "p2", schema.StatusRunning, 20002)

	if err := c.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	data, err := os.ReadFile(c.cfg.ConfigPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	conf := string(data)
	for _, want := range []string{"127.0.0.1:20001", "127.0.0.1:20002", upstreamName(dep)} {
		if !strings.Contains(conf, want) {
			t.Errorf("config missing %q:\n%s", want, conf)
		}
	}
}

// Routing traffic to a pod that is not listening would turn a scaling event into
// a burst of connection errors.
func TestSyncExcludesPodsThatAreNotServing(t *testing.T) {
	c, client, prefix := newTestController(t)
	dep := prefix + "-web"
	writePod(t, client, dep, "up", schema.StatusRunning, 20001)
	writePod(t, client, dep, "pending", schema.StatusPending, 20002)
	writePod(t, client, dep, "crashing", schema.StatusCrashLoopBackOff, 20003)
	writePod(t, client, dep, "noport", schema.StatusRunning, 0)

	if err := c.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	data, _ := os.ReadFile(c.cfg.ConfigPath)
	conf := string(data)
	if !strings.Contains(conf, "20001") {
		t.Error("the serving pod should be routed to")
	}
	for _, unwanted := range []string{"20002", "20003"} {
		if strings.Contains(conf, unwanted) {
			t.Errorf("config routes to a non-serving pod (%s):\n%s", unwanted, conf)
		}
	}
}

// Key-scan order is arbitrary, so identical state must still render identically
// or every resync would trigger a needless reload.
func TestRenderIsStableAcrossRuns(t *testing.T) {
	backends := map[string][]int{"b": {2, 1}, "a": {4, 3}}
	first := Render(backends)
	for i := 0; i < 5; i++ {
		if got := Render(backends); got != first {
			t.Fatal("render is not deterministic over identical state")
		}
	}
	if strings.Index(first, "minik8s_a") > strings.Index(first, "minik8s_b") {
		t.Error("upstreams should be emitted in a stable sorted order")
	}
}

// An upstream with no servers is a config error in Nginx.
func TestRenderOmitsDeploymentsWithNoBackends(t *testing.T) {
	got := Render(map[string][]int{"empty": {}})
	if strings.Contains(got, "upstream") {
		t.Errorf("want no upstream block for a deployment with nothing running:\n%s", got)
	}
}

// A reload drops in-flight connection handling, so an unchanged config must not
// trigger one.
func TestSyncSkipsReloadWhenNothingChanged(t *testing.T) {
	c, client, prefix := newTestController(t)
	ctx := context.Background()
	dep := prefix + "-web"
	writePod(t, client, dep, "p1", schema.StatusRunning, 20001)

	if err := c.Sync(ctx); err != nil {
		t.Fatalf("first Sync: %v", err)
	}
	info, err := os.Stat(c.cfg.ConfigPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	first := info.ModTime()

	time.Sleep(20 * time.Millisecond)
	if err := c.Sync(ctx); err != nil {
		t.Fatalf("second Sync: %v", err)
	}
	info, _ = os.Stat(c.cfg.ConfigPath)
	if !info.ModTime().Equal(first) {
		t.Error("config was rewritten even though nothing changed")
	}
}

// A missing Nginx is not an error: the generated config is still correct and on
// disk, and failing would make every later change retry a command that will
// never exist.
func TestSyncSucceedsWithoutNginxInstalled(t *testing.T) {
	c, client, prefix := newTestController(t)
	writePod(t, client, prefix+"-web", "p1", schema.StatusRunning, 20001)
	if err := c.Sync(context.Background()); err != nil {
		t.Fatalf("Sync should tolerate a missing reload binary: %v", err)
	}
}

// Nginx may read the file at any moment, so a partial config must never be
// visible: the write goes through a rename.
func TestWriteReplacesConfigAtomically(t *testing.T) {
	c, _, _ := newTestController(t)
	if err := c.write("first"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := c.write("second"); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	data, _ := os.ReadFile(c.cfg.ConfigPath)
	if string(data) != "second" {
		t.Errorf("config = %q, want the newest content", data)
	}
	// The temp file used for the rename must not be left behind.
	entries, _ := os.ReadDir(filepath.Dir(c.cfg.ConfigPath))
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".upstreams-") {
			t.Errorf("temp file %s was left behind", e.Name())
		}
	}
}
