package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"mini-k8s/internal/logging"
	"mini-k8s/internal/redisclient"
	"mini-k8s/internal/schema"
)

// expiredEventChannel is the keyspace notification channel Redis publishes to
// when a key expires. It requires `notify-keyspace-events Ex` on the server;
// without it this subscriber simply never fires and the sweep carries the load.
const expiredEventChannel = "__keyevent@0__:expired"

// evictScript atomically evicts every pod bound to a dead node.
//
// Eviction must be atomic because it spans several keys: each pod's status, the
// node's allocation counters, and the registry. A partial eviction is worse than
// none — pods marked Evicted while the node still holds their reservations would
// leave capacity permanently consumed by work that no longer exists, and two
// controllers racing on the same dead node would double-subtract.
//
// KEYS[1] = node capacity hash, KEYS[2] = node registry
// ARGV[1] = node id, ARGV[2] = pod key pattern, ARGV[3] = Evicted status
var evictScript = `
local capKey = KEYS[1]
local registry = KEYS[2]
local nodeID = ARGV[1]
local pattern = ARGV[2]
local evicted = ARGV[3]

local cursor = "0"
local count = 0
repeat
  local res = redis.call("SCAN", cursor, "MATCH", pattern, "COUNT", 100)
  cursor = res[1]
  for _, key in ipairs(res[2]) do
    if redis.call("HGET", key, "node_id") == nodeID then
      local status = redis.call("HGET", key, "status")
      if status ~= evicted then
        redis.call("HSET", key, "status", evicted)
        count = count + 1
      end
    end
  end
until cursor == "0"

-- Allocations go to zero rather than being decremented per pod. The node is
-- gone, so nothing it held is still reserved, and a fresh agent on the same
-- node id rebuilds the count from the pods that actually exist.
if redis.call("EXISTS", capKey) == 1 then
  redis.call("HSET", capKey, "allocated_cpu", 0, "allocated_mem", 0)
end
redis.call("SREM", registry, nodeID)

return count
`

// HealthController detects dead nodes and evicts their pods.
//
// Like replica reconciliation this runs in two modes. Keyspace expiry
// notifications make detection near-instant, and a sweep re-derives liveness
// from the registry regardless. The sweep is the correctness path: keyspace
// notifications are best-effort, are silently unavailable unless the server is
// configured for them, and are missed entirely while this process restarts. A
// node whose expiry notification was lost would otherwise stay in the registry
// forever, holding capacity and attracting placements that can never run.
type HealthController struct {
	client        *redisclient.Client
	logger        *logging.Logger
	sweepInterval time.Duration
}

// NewHealthController builds a controller.
func NewHealthController(client *redisclient.Client, logger *logging.Logger, sweepInterval time.Duration) *HealthController {
	if sweepInterval <= 0 {
		sweepInterval = DefaultSweepInterval
	}
	return &HealthController{client: client, logger: logger, sweepInterval: sweepInterval}
}

// Run starts both detection modes and blocks until ctx is cancelled.
func (h *HealthController) Run(ctx context.Context) {
	go h.RunExpirySubscriber(ctx)
	h.RunSweeper(ctx)
}

// RunSweeper periodically evicts registered nodes that hold no lease.
func (h *HealthController) RunSweeper(ctx context.Context) {
	ticker := time.NewTicker(h.sweepInterval)
	defer ticker.Stop()
	for {
		if err := h.SweepOnce(ctx); err != nil && ctx.Err() == nil {
			h.logger.Error(ctx, "node_sweep_failed", err.Error())
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// SweepOnce evicts every registered node whose liveness lease is absent.
func (h *HealthController) SweepOnce(ctx context.Context) error {
	registered, err := h.client.SetMembers(ctx, schema.NodeRegistryKey())
	if err != nil {
		return fmt.Errorf("read node registry: %w", err)
	}
	for _, nodeID := range registered {
		alive, err := h.client.Exists(ctx, schema.NodeStatusKey(nodeID))
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			h.logger.Warn(ctx, "lease_check_failed", err.Error(), logging.NodeID(nodeID))
			continue
		}
		if alive {
			continue
		}
		if err := h.EvictNode(ctx, nodeID); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			h.logger.Error(ctx, "evict_failed", err.Error(), logging.NodeID(nodeID))
		}
	}
	return nil
}

// RunExpirySubscriber evicts a node the moment its lease key expires.
func (h *HealthController) RunExpirySubscriber(ctx context.Context) {
	for ctx.Err() == nil {
		sub, err := h.client.Subscribe(ctx, expiredEventChannel)
		if err != nil {
			h.logger.Error(ctx, "expiry_subscribe_failed", err.Error())
			if !sleepCtx(ctx, time.Second) {
				return
			}
			continue
		}
		h.consumeExpiries(ctx, sub)
		_ = sub.Close()
	}
}

// consumeExpiries handles one subscription's messages until it ends.
func (h *HealthController) consumeExpiries(ctx context.Context, sub *redisclient.Subscription) {
	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			// The payload is the expired key. Every key in the database arrives
			// here, so anything that is not a node lease is not our concern.
			nodeID, ok := nodeIDFromStatusKey(msg.Payload)
			if !ok {
				continue
			}
			if err := h.EvictNode(ctx, nodeID); err != nil && ctx.Err() == nil {
				h.logger.Error(ctx, "evict_failed", err.Error(), logging.NodeID(nodeID))
			}
		}
	}
}

// EvictNode marks a dead node's pods Evicted, zeroes its allocations, and
// removes it from the registry, all in one atomic step.
//
// The pod records are deliberately left in place rather than deleted. The
// replica controller owns replacement, and it needs to see that those replicas
// are gone; deleting them here would make two controllers responsible for the
// same decision.
func (h *HealthController) EvictNode(ctx context.Context, nodeID string) error {
	if nodeID == "" {
		return nil
	}

	// A node that re-registered between detection and now is alive again.
	// Evicting it would kill a healthy node's accounting for no reason.
	alive, err := h.client.Exists(ctx, schema.NodeStatusKey(nodeID))
	if err != nil {
		return fmt.Errorf("check lease for %s: %w", nodeID, err)
	}
	if alive {
		return nil
	}

	res, err := h.client.EvalScript(ctx, evictScript,
		[]string{schema.NodeCapacityKey(nodeID), schema.NodeRegistryKey()},
		nodeID, schema.PodKeyPattern(), schema.StatusEvicted)
	if err != nil {
		return fmt.Errorf("evict %s: %w", nodeID, err)
	}

	evicted, _ := res.(int64)
	h.logger.Warn(ctx, "node_evicted",
		fmt.Sprintf("lease expired: marked %d pod(s) Evicted and released its capacity", evicted),
		logging.NodeID(nodeID))
	return nil
}

// nodeIDFromStatusKey extracts a node id from a lease key, reporting false for
// any other key.
func nodeIDFromStatusKey(key string) (string, bool) {
	const prefix = "node:"
	const suffix = ":status"
	if !strings.HasPrefix(key, prefix) || !strings.HasSuffix(key, suffix) {
		return "", false
	}
	id := strings.TrimSuffix(strings.TrimPrefix(key, prefix), suffix)
	if id == "" || strings.Contains(id, ":") {
		return "", false
	}
	return id, true
}
