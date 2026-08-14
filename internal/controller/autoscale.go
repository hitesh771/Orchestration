package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"mini-k8s/internal/events"
	"mini-k8s/internal/logging"
	"mini-k8s/internal/redisclient"
	"mini-k8s/internal/schema"
)

// autoscaleScript decides and applies one deployment's replica count.
//
// The whole decision is one script because it reads the telemetry lists, the
// current replica count, and the cooldown timestamps, then writes a new count
// derived from all three. Doing that in application code would leave a window
// where another actor changes desired_replicas between the read and the write,
// and the loser silently overwrites the winner. Here the read and the write
// cannot be separated.
//
// Cooldowns are enforced inside the script for the same reason: they exist to
// stop flapping, and a check that races the write it guards does not stop
// anything.
//
// KEYS[1] = deployment hash
// ARGV[1] = telemetry key prefix, ARGV[2] = now (unix seconds)
// ARGV[3] = scale-up cooldown seconds, ARGV[4] = scale-down cooldown seconds
// ARGV[5] = samples to average
//
// Returns {decision, oldReplicas, newReplicas, observedPercent}.
var autoscaleScript = `
local depKey = KEYS[1]
local prefix = ARGV[1]
local now = tonumber(ARGV[2])
local upCooldown = tonumber(ARGV[3])
local downCooldown = tonumber(ARGV[4])
local window = tonumber(ARGV[5])

if redis.call("EXISTS", depKey) == 0 then
  return {"missing", 0, 0, 0}
end

local vals = redis.call("HMGET", depKey,
  "desired_replicas", "min_replicas", "max_replicas",
  "target_cpu_percentage", "last_scale_up_at", "last_scale_down_at")

local desired = tonumber(vals[1]) or 0
local minR = tonumber(vals[2]) or 1
local maxR = tonumber(vals[3]) or 1
local target = tonumber(vals[4]) or 0
local lastUp = tonumber(vals[5]) or 0
local lastDown = tonumber(vals[6]) or 0

if target <= 0 then
  return {"no_target", desired, desired, 0}
end

-- Average the tail of every pod's samples. A pod with no samples yet is skipped
-- rather than counted as idle: a freshly started pod would otherwise drag the
-- average down and mask the very load that caused it to be started.
local cursor = "0"
local sum = 0
local count = 0
repeat
  local res = redis.call("SCAN", cursor, "MATCH", prefix, "COUNT", 100)
  cursor = res[1]
  for _, key in ipairs(res[2]) do
    local samples = redis.call("LRANGE", key, -window, -1)
    local podSum = 0
    local podCount = 0
    for _, v in ipairs(samples) do
      local n = tonumber(v)
      if n then
        podSum = podSum + n
        podCount = podCount + 1
      end
    end
    if podCount > 0 then
      sum = sum + (podSum / podCount)
      count = count + 1
    end
  end
until cursor == "0"

if count == 0 then
  return {"no_telemetry", desired, desired, 0}
end

local observed = math.floor(sum / count)

-- The standard ratio: replicas needed to bring observed utilization to target.
local wanted = math.ceil(desired * observed / target)
if wanted < minR then wanted = minR end
if wanted > maxR then wanted = maxR end

if wanted == desired then
  return {"steady", desired, desired, observed}
end

if wanted > desired then
  if now - lastUp < upCooldown then
    return {"up_cooldown", desired, desired, observed}
  end
  redis.call("HSET", depKey, "desired_replicas", wanted, "last_scale_up_at", now)
  return {"scaled_up", desired, wanted, observed}
end

if now - lastDown < downCooldown then
  return {"down_cooldown", desired, desired, observed}
end
redis.call("HSET", depKey, "desired_replicas", wanted, "last_scale_down_at", now)
return {"scaled_down", desired, wanted, observed}
`

// AutoscaleConfig tunes the autoscaler.
type AutoscaleConfig struct {
	Interval          time.Duration // how often every deployment is evaluated
	ScaleUpCooldown   time.Duration // minimum gap between scale-ups
	ScaleDownCooldown time.Duration // minimum gap between scale-downs
}

// Autoscaler adjusts desired replica counts from observed CPU utilization.
type Autoscaler struct {
	client *redisclient.Client
	logger *logging.Logger
	cfg    AutoscaleConfig
}

// NewAutoscaler builds an autoscaler, filling in the spec's defaults for any
// unset interval.
func NewAutoscaler(client *redisclient.Client, logger *logging.Logger, cfg AutoscaleConfig) *Autoscaler {
	if cfg.Interval <= 0 {
		cfg.Interval = 15 * time.Second
	}
	if cfg.ScaleUpCooldown <= 0 {
		cfg.ScaleUpCooldown = 30 * time.Second
	}
	if cfg.ScaleDownCooldown <= 0 {
		cfg.ScaleDownCooldown = 180 * time.Second
	}
	return &Autoscaler{client: client, logger: logger, cfg: cfg}
}

// Decision is the outcome of evaluating one deployment.
type Decision struct {
	Deployment  string
	Outcome     string // scaled_up, scaled_down, steady, up_cooldown, down_cooldown, no_telemetry, no_target
	From        int
	To          int
	ObservedCPU int
}

// Changed reports whether the decision altered the replica count.
func (d Decision) Changed() bool { return d.From != d.To }

// Run evaluates every deployment on a timer until ctx is cancelled.
func (a *Autoscaler) Run(ctx context.Context) {
	ticker := time.NewTicker(a.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := a.EvaluateAll(ctx); err != nil && ctx.Err() == nil {
				a.logger.Error(ctx, "autoscale_sweep_failed", err.Error())
			}
		}
	}
}

// EvaluateAll evaluates every deployment once.
func (a *Autoscaler) EvaluateAll(ctx context.Context) ([]Decision, error) {
	keys, err := a.client.ScanKeys(ctx, schema.DeploymentKeyPattern())
	if err != nil {
		return nil, fmt.Errorf("scan deployments: %w", err)
	}

	var decisions []Decision
	for _, key := range keys {
		name := strings.TrimPrefix(key, "deployment:")
		if name == "" || name == key {
			continue
		}
		decision, err := a.Evaluate(ctx, name)
		if err != nil {
			// One deployment's failure must not stop the others: they are
			// independent and still need evaluating.
			if ctx.Err() != nil {
				return decisions, ctx.Err()
			}
			a.logger.Error(ctx, "autoscale_failed", err.Error(), logging.DeploymentID(name))
			continue
		}
		decisions = append(decisions, decision)
	}
	return decisions, nil
}

// Evaluate applies the autoscale decision for one deployment.
func (a *Autoscaler) Evaluate(ctx context.Context, name string) (Decision, error) {
	res, err := a.client.EvalScript(ctx, autoscaleScript,
		[]string{schema.DeploymentKey(name)},
		telemetryPattern(name),
		time.Now().Unix(),
		int(a.cfg.ScaleUpCooldown.Seconds()),
		int(a.cfg.ScaleDownCooldown.Seconds()),
		telemetryWindow,
	)
	if err != nil {
		return Decision{}, fmt.Errorf("autoscale %s: %w", name, err)
	}

	decision, err := parseDecision(name, res)
	if err != nil {
		return Decision{}, err
	}

	if decision.Changed() {
		a.logger.Info(ctx, "deployment_autoscaled",
			fmt.Sprintf("observed %d%% CPU: %d -> %d replicas",
				decision.ObservedCPU, decision.From, decision.To),
			logging.DeploymentID(name))

		// Tell the replica controller now rather than leaving it to the next
		// sweep. The event is only a latency win: the sweep would converge
		// anyway, so a failed publish is not worth failing the decision over.
		if err := events.Publish(ctx, a.client, events.DeploymentEvent{
			Event: events.EventUpdate, Deployment: name, Desired: decision.To,
		}); err != nil {
			a.logger.Warn(ctx, "autoscale_event_publish_failed", err.Error(),
				logging.DeploymentID(name))
		}
	}
	return decision, nil
}

// telemetryWindow is how many samples per pod the average covers. The spec
// fixes it at three: enough to ignore a momentary spike, few enough to still
// react within a couple of collection intervals.
const telemetryWindow = 3

// telemetryPattern matches every telemetry list belonging to a deployment.
func telemetryPattern(deployment string) string {
	return fmt.Sprintf("telemetry:%s:*:cpu", deployment)
}

// parseDecision converts the script's reply into a Decision.
func parseDecision(name string, res interface{}) (Decision, error) {
	parts, ok := res.([]interface{})
	if !ok || len(parts) != 4 {
		return Decision{}, fmt.Errorf("autoscale %s: unexpected script reply %#v", name, res)
	}
	outcome, ok := parts[0].(string)
	if !ok {
		return Decision{}, fmt.Errorf("autoscale %s: unexpected outcome %#v", name, parts[0])
	}
	from, _ := parts[1].(int64)
	to, _ := parts[2].(int64)
	observed, _ := parts[3].(int64)

	return Decision{
		Deployment: name, Outcome: outcome,
		From: int(from), To: int(to), ObservedCPU: int(observed),
	}, nil
}
