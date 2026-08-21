package controller

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"mini-k8s/internal/logging"
	"mini-k8s/internal/redisclient"
	"mini-k8s/internal/schema"
)

// newTestAutoscaler returns an autoscaler with cooldowns short enough that a
// test exercises the decision logic rather than waiting on wall-clock time.
func newTestAutoscaler(t *testing.T) (*Autoscaler, *redisclient.Client, string) {
	t.Helper()
	client, err := redisclient.New(testRedisAddr())
	if err != nil {
		t.Skipf("redis unavailable at %s: %v", testRedisAddr(), err)
	}
	t.Cleanup(func() { client.Close() })
	prefix := fmt.Sprintf("hpa-%d", time.Now().UnixNano())
	as := NewAutoscaler(client, logging.New("test", nil), AutoscaleConfig{
		Interval:          10 * time.Millisecond,
		ScaleUpCooldown:   time.Second,
		ScaleDownCooldown: time.Second,
	})
	return as, client, prefix
}

// writeSamples publishes CPU samples for one pod of a deployment.
func writeSamples(t *testing.T, client *redisclient.Client, deployment, podID string, percents ...int) {
	t.Helper()
	ctx := context.Background()
	key := schema.TelemetryCPUKey(deployment, podID)
	for _, p := range percents {
		if err := client.ListPush(ctx, key, strconv.Itoa(p)); err != nil {
			t.Fatalf("push sample: %v", err)
		}
	}
	t.Cleanup(func() { client.DeleteKey(ctx, key) })
}

func desiredOf(t *testing.T, client *redisclient.Client, deployment string) int {
	t.Helper()
	v, err := client.HashGetField(context.Background(), schema.DeploymentKey(deployment),
		schema.FieldDesiredReplicas)
	if err != nil {
		t.Fatalf("read desired: %v", err)
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("parse desired %q: %v", v, err)
	}
	return n
}

func TestAutoscaleScalesUpOnSustainedLoad(t *testing.T) {
	as, client, prefix := newTestAutoscaler(t)
	dep := prefix + "-web"
	addDeployment(t, client, dep, 2) // target 70%
	// 90% observed against a 70% target wants ceil(2*90/70) = 3.
	writeSamples(t, client, dep, "p1", 90, 90, 90)
	writeSamples(t, client, dep, "p2", 90, 90, 90)

	d, err := as.Evaluate(context.Background(), dep)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if d.Outcome != "scaled_up" || d.To != 3 {
		t.Fatalf("want scaled_up to 3, got %+v", d)
	}
	if got := desiredOf(t, client, dep); got != 3 {
		t.Fatalf("desired_replicas = %d, want 3", got)
	}
}

func TestAutoscaleScalesDownWhenIdle(t *testing.T) {
	as, client, prefix := newTestAutoscaler(t)
	dep := prefix + "-web"
	addDeployment(t, client, dep, 4)
	// 10% against 70% wants ceil(4*10/70) = 1.
	writeSamples(t, client, dep, "p1", 10, 10, 10)

	d, err := as.Evaluate(context.Background(), dep)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if d.Outcome != "scaled_down" || d.To != 1 {
		t.Fatalf("want scaled_down to 1, got %+v", d)
	}
}

// The average is what makes the signal usable: one spike in a three-sample
// window must not move the replica count.
func TestAutoscaleIgnoresASingleSpike(t *testing.T) {
	as, client, prefix := newTestAutoscaler(t)
	dep := prefix + "-web"
	addDeployment(t, client, dep, 2)
	// The last sample alone would want ceil(2*100/70) = 3 replicas. Averaged
	// over the window it reads as 40%, which is under the 70% target, so the
	// count holds. That gap is the whole point of the window.
	writeSamples(t, client, dep, "p1", 10, 10, 100)

	d, err := as.Evaluate(context.Background(), dep)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if d.Changed() {
		t.Fatalf("a lone spike should not move the replica count, got %+v", d)
	}
	if d.ObservedCPU != 40 {
		t.Fatalf("observed %d%%, want the 40%% average rather than the 100%% spike", d.ObservedCPU)
	}
}

// Only the retained window counts: an old spike that has aged out must not keep
// influencing decisions.
func TestAutoscaleAveragesOnlyTheRecentWindow(t *testing.T) {
	as, client, prefix := newTestAutoscaler(t)
	dep := prefix + "-web"
	addDeployment(t, client, dep, 2)
	// The leading 500 sits outside the three-sample window.
	writeSamples(t, client, dep, "p1", 500, 10, 10, 10)

	d, err := as.Evaluate(context.Background(), dep)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if d.ObservedCPU > 20 {
		t.Fatalf("observed %d%%: the aged-out spike still counted (%+v)", d.ObservedCPU, d)
	}
}

func TestAutoscaleRespectsScaleUpCooldown(t *testing.T) {
	as, client, prefix := newTestAutoscaler(t)
	ctx := context.Background()
	dep := prefix + "-web"
	addDeployment(t, client, dep, 2)
	writeSamples(t, client, dep, "p1", 200, 200, 200)

	if err := client.HashSet(ctx, schema.DeploymentKey(dep), map[string]interface{}{
		schema.FieldLastScaleUpAt: strconv.FormatInt(time.Now().Unix(), 10),
	}); err != nil {
		t.Fatalf("set last_scale_up_at: %v", err)
	}

	d, err := as.Evaluate(ctx, dep)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if d.Outcome != "up_cooldown" || d.Changed() {
		t.Fatalf("want the scale-up suppressed by cooldown, got %+v", d)
	}
}

// The anti-flapping guarantee: a deployment that just scaled up must not be
// scaled straight back down.
func TestAutoscaleRespectsScaleDownCooldown(t *testing.T) {
	as, client, prefix := newTestAutoscaler(t)
	ctx := context.Background()
	dep := prefix + "-web"
	addDeployment(t, client, dep, 4)
	writeSamples(t, client, dep, "p1", 1, 1, 1)

	if err := client.HashSet(ctx, schema.DeploymentKey(dep), map[string]interface{}{
		schema.FieldLastScaleDownAt: strconv.FormatInt(time.Now().Unix(), 10),
	}); err != nil {
		t.Fatalf("set last_scale_down_at: %v", err)
	}

	d, err := as.Evaluate(ctx, dep)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if d.Outcome != "down_cooldown" || d.Changed() {
		t.Fatalf("want the scale-down suppressed by cooldown, got %+v", d)
	}
	if got := desiredOf(t, client, dep); got != 4 {
		t.Fatalf("desired_replicas = %d, want 4 held through the cooldown", got)
	}
}

// A cooldown must not be permanent: once it expires the decision it deferred
// has to actually happen.
func TestAutoscaleScalesAfterCooldownExpires(t *testing.T) {
	as, client, prefix := newTestAutoscaler(t)
	ctx := context.Background()
	dep := prefix + "-web"
	addDeployment(t, client, dep, 4)
	writeSamples(t, client, dep, "p1", 1, 1, 1)

	// Cooldown is 1s in tests, so a 10s-old stamp is comfortably expired.
	if err := client.HashSet(ctx, schema.DeploymentKey(dep), map[string]interface{}{
		schema.FieldLastScaleDownAt: strconv.FormatInt(time.Now().Unix()-10, 10),
	}); err != nil {
		t.Fatalf("set last_scale_down_at: %v", err)
	}

	d, err := as.Evaluate(ctx, dep)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if d.Outcome != "scaled_down" {
		t.Fatalf("want the deferred scale-down to happen, got %+v", d)
	}
}

func TestAutoscaleClampsToDeclaredBounds(t *testing.T) {
	as, client, prefix := newTestAutoscaler(t)
	ctx := context.Background()
	dep := prefix + "-web"
	addDeployment(t, client, dep, 2) // min 1, max 10
	// 5000% would want 100 replicas; max_replicas is the ceiling.
	writeSamples(t, client, dep, "p1", 5000, 5000, 5000)

	d, err := as.Evaluate(ctx, dep)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if d.To != 10 {
		t.Fatalf("want clamping to max_replicas 10, got %+v", d)
	}

	// And the floor, from the other direction.
	if err := client.HashSet(ctx, schema.DeploymentKey(dep), map[string]interface{}{
		schema.FieldDesiredReplicas: 10,
		schema.FieldLastScaleDownAt: "0",
	}); err != nil {
		t.Fatalf("reset: %v", err)
	}
	writeSamples(t, client, dep, "p2", 0, 0, 0)
	if err := client.DeleteKey(ctx, schema.TelemetryCPUKey(dep, "p1")); err != nil {
		t.Fatalf("clear samples: %v", err)
	}
	d, err = as.Evaluate(ctx, dep)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if d.To < 1 {
		t.Fatalf("want clamping to min_replicas 1, got %+v", d)
	}
}

// With no telemetry the autoscaler has no signal. Treating that as zero load
// would scale every deployment to its floor the moment collection stalls.
func TestAutoscaleHoldsWhenThereIsNoTelemetry(t *testing.T) {
	as, client, prefix := newTestAutoscaler(t)
	dep := prefix + "-web"
	addDeployment(t, client, dep, 3)

	d, err := as.Evaluate(context.Background(), dep)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if d.Outcome != "no_telemetry" || d.Changed() {
		t.Fatalf("want the count held with no signal, got %+v", d)
	}
	if got := desiredOf(t, client, dep); got != 3 {
		t.Fatalf("desired_replicas = %d, want 3 untouched", got)
	}
}

// A pod that has not reported yet is skipped rather than counted as idle: a
// freshly started pod would otherwise dilute the average and mask the load that
// caused it to be started.
func TestAutoscaleSkipsPodsWithNoSamples(t *testing.T) {
	as, client, prefix := newTestAutoscaler(t)
	ctx := context.Background()
	dep := prefix + "-web"
	addDeployment(t, client, dep, 2)
	writeSamples(t, client, dep, "p1", 100, 100, 100)
	// p2 exists but has published nothing.
	if err := client.ListPush(ctx, schema.TelemetryCPUKey(dep, "p2"), ""); err != nil {
		t.Fatalf("seed empty: %v", err)
	}
	t.Cleanup(func() { client.DeleteKey(ctx, schema.TelemetryCPUKey(dep, "p2")) })

	d, err := as.Evaluate(ctx, dep)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if d.ObservedCPU != 100 {
		t.Fatalf("observed %d%%, want 100 with the silent pod excluded (%+v)", d.ObservedCPU, d)
	}
}

func TestAutoscaleIgnoresDeploymentsWithNoTarget(t *testing.T) {
	as, client, prefix := newTestAutoscaler(t)
	ctx := context.Background()
	dep := prefix + "-web"
	addDeployment(t, client, dep, 2)
	if err := client.HashSet(ctx, schema.DeploymentKey(dep), map[string]interface{}{
		schema.FieldTargetCPUPercent: 0,
	}); err != nil {
		t.Fatalf("clear target: %v", err)
	}
	writeSamples(t, client, dep, "p1", 100, 100, 100)

	d, err := as.Evaluate(ctx, dep)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if d.Outcome != "no_target" || d.Changed() {
		t.Fatalf("want no action without a target, got %+v", d)
	}
}

func TestAutoscaleHandlesMissingDeployment(t *testing.T) {
	as, _, prefix := newTestAutoscaler(t)
	d, err := as.Evaluate(context.Background(), prefix+"-nonexistent")
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if d.Outcome != "missing" {
		t.Fatalf("want the missing deployment reported, got %+v", d)
	}
}

// The flap the scale-down cooldown exists to prevent is a scale-up followed
// immediately by a tear-down, so a recent scale-up must block a scale-down just
// as a recent scale-down does.
func TestAutoscaleDoesNotTearDownRightAfterScalingUp(t *testing.T) {
	as, client, prefix := newTestAutoscaler(t)
	ctx := context.Background()
	dep := prefix + "-web"
	addDeployment(t, client, dep, 4)
	writeSamples(t, client, dep, "p1", 1, 1, 1) // idle: wants the floor

	// Scaled up a moment ago, and never scaled down.
	if err := client.HashSet(ctx, schema.DeploymentKey(dep), map[string]interface{}{
		schema.FieldLastScaleUpAt:   strconv.FormatInt(time.Now().Unix(), 10),
		schema.FieldLastScaleDownAt: "0",
	}); err != nil {
		t.Fatalf("set stamps: %v", err)
	}

	d, err := as.Evaluate(ctx, dep)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if d.Outcome != "down_cooldown" || d.Changed() {
		t.Fatalf("want the tear-down blocked by the recent scale-up, got %+v", d)
	}
	if got := desiredOf(t, client, dep); got != 4 {
		t.Fatalf("desired_replicas = %d, want 4 held", got)
	}
}
