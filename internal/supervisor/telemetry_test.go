package supervisor

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"mini-k8s/internal/schema"
)

func TestProcessCPUTicksReadsALiveProcess(t *testing.T) {
	// This process is certainly alive and has certainly burned some CPU.
	ticks, err := processCPUTicks(os.Getpid())
	if err != nil {
		t.Fatalf("processCPUTicks: %v", err)
	}
	if ticks == 0 {
		t.Error("want non-zero cumulative CPU time for a running test binary")
	}
}

func TestProcessCPUTicksFailsForDeadProcess(t *testing.T) {
	if _, err := processCPUTicks(4194303); err == nil {
		t.Error("want an error for a pid that is not running")
	}
}

// CPU time in /proc is cumulative, so a percentage is only meaningful as a rate
// between two readings.
func TestCPUPercentIsARateNotATotal(t *testing.T) {
	base := time.Now()
	// 100 ticks over 1s is one full core: 1000 millicores. Against a 500m
	// request that is 200%.
	prev := cpuSample{ticks: 1000, at: base}
	cur := cpuSample{ticks: 1100, at: base.Add(time.Second)}
	if got := cpuPercentOfRequest(prev, cur, 500); got != 200 {
		t.Errorf("got %d%%, want 200%%", got)
	}
	// Half a core against the same request is 100%.
	cur = cpuSample{ticks: 1050, at: base.Add(time.Second)}
	if got := cpuPercentOfRequest(prev, cur, 500); got != 100 {
		t.Errorf("got %d%%, want 100%%", got)
	}
}

// Degenerate inputs must produce zero rather than a divide-by-zero or a wildly
// wrong number that would drive a scaling decision.
func TestCPUPercentHandlesDegenerateInputs(t *testing.T) {
	base := time.Now()
	cases := []struct {
		name       string
		prev, cur  cpuSample
		cpuRequest int
	}{
		{"no elapsed time", cpuSample{ticks: 10, at: base}, cpuSample{ticks: 20, at: base}, 100},
		{"no cpu request", cpuSample{ticks: 10, at: base}, cpuSample{ticks: 20, at: base.Add(time.Second)}, 0},
		{"counter went backwards", cpuSample{ticks: 20, at: base}, cpuSample{ticks: 10, at: base.Add(time.Second)}, 100},
	}
	for _, c := range cases {
		if got := cpuPercentOfRequest(c.prev, c.cur, c.cpuRequest); got != 0 {
			t.Errorf("%s: got %d%%, want 0%%", c.name, got)
		}
	}
}

// The first reading of a pod only establishes a baseline. Publishing it as a
// rate would report every long-running pod as permanently pegged.
func TestTelemetryFirstCollectionOnlyEstablishesABaseline(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.createPod(t, "p1")
	if err := f.sup.Spawn(ctx, f.deployment, "p1"); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	key := schema.TelemetryCPUKey(f.deployment, "p1")
	t.Cleanup(func() { f.client.DeleteKey(ctx, key) })

	last := make(map[string]cpuSample)
	if err := f.sup.collectOnce(ctx, last, time.Second); err != nil {
		t.Fatalf("collectOnce: %v", err)
	}
	samples, err := f.client.ListRange(ctx, key, 0, -1)
	if err != nil {
		t.Fatalf("read samples: %v", err)
	}
	if len(samples) != 0 {
		t.Fatalf("want no samples from the baseline pass, got %v", samples)
	}
	if len(last) != 1 {
		t.Fatalf("want a baseline recorded, got %d entries", len(last))
	}

	// The second pass has two readings and can publish a real rate.
	time.Sleep(50 * time.Millisecond)
	if err := f.sup.collectOnce(ctx, last, time.Second); err != nil {
		t.Fatalf("second collectOnce: %v", err)
	}
	samples, err = f.client.ListRange(ctx, key, 0, -1)
	if err != nil {
		t.Fatalf("read samples: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("want one published sample, got %v", samples)
	}
	if _, err := strconv.Atoi(samples[0]); err != nil {
		t.Errorf("sample %q is not a number: %v", samples[0], err)
	}
}

// The retained window is bounded, or the lists would grow without limit for
// data the autoscaler never reads.
func TestTelemetryTrimsToTheRetainedWindow(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.createPod(t, "p1")
	pod := f.readPod(t, "p1")
	key := schema.TelemetryCPUKey(f.deployment, "p1")
	t.Cleanup(func() { f.client.DeleteKey(ctx, key) })

	for i := 0; i < telemetrySamples+5; i++ {
		if err := f.sup.publishSample(ctx, pod, i, time.Second); err != nil {
			t.Fatalf("publishSample: %v", err)
		}
	}
	samples, err := f.client.ListRange(ctx, key, 0, -1)
	if err != nil {
		t.Fatalf("read samples: %v", err)
	}
	if len(samples) != telemetrySamples {
		t.Fatalf("want %d retained samples, got %d", telemetrySamples, len(samples))
	}
	// The tail is what survives: the autoscaler needs the newest samples.
	if samples[len(samples)-1] != strconv.Itoa(telemetrySamples+4) {
		t.Errorf("newest sample = %q, want the most recent value", samples[len(samples)-1])
	}
}

func TestTelemetryIgnoresPodsOnOtherNodes(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.createPod(t, "p1")
	if err := f.sup.Spawn(ctx, f.deployment, "p1"); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	f.setPodFields(t, "p1", map[string]interface{}{schema.FieldPodNodeID: "some-other-node"})
	key := schema.TelemetryCPUKey(f.deployment, "p1")
	t.Cleanup(func() { f.client.DeleteKey(ctx, key) })

	last := make(map[string]cpuSample)
	if err := f.sup.collectOnce(ctx, last, time.Second); err != nil {
		t.Fatalf("collectOnce: %v", err)
	}
	if len(last) != 0 {
		t.Fatalf("want another node's pod ignored, got %d baselines", len(last))
	}
}

// A pod that goes away must not leave its baseline behind, or the map grows for
// the life of the agent.
func TestTelemetryForgetsBaselinesOfVanishedPods(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	last := map[string]cpuSample{
		f.deployment + "/gone": {ticks: 1, at: time.Now()},
	}
	if err := f.sup.collectOnce(ctx, last, time.Second); err != nil {
		t.Fatalf("collectOnce: %v", err)
	}
	if len(last) != 0 {
		t.Fatalf("want the stale baseline dropped, got %v", last)
	}
}
