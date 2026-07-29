// Package node implements a worker node's self-registration and liveness
// lease. It is used by the node-agent to announce capacity on startup and to
// continuously prove the node is alive.
//
// Liveness is derived purely from the presence of the lease key: no component
// ever sends a "node down" message. A live agent refreshes the lease well
// inside its TTL; a dead agent stops refreshing and the key expires, which is
// the only signal the control plane needs.
package node

import (
	"context"
	"fmt"
	"time"

	"mini-k8s/internal/config"
	"mini-k8s/internal/logging"
	"mini-k8s/internal/redisclient"
	"mini-k8s/internal/schema"
)

// Registrar owns this node's registry membership, capacity hash, and lease.
type Registrar struct {
	client *redisclient.Client
	logger *logging.Logger
	nodeID string

	totalCPU int // millicores this node offers
	totalMem int // MB this node offers

	heartbeatInterval time.Duration
	leaseTTL          time.Duration
}

// NewRegistrar builds a Registrar from configuration.
//
// It returns an error if the heartbeat interval is not comfortably shorter
// than the lease TTL. That misconfiguration would let a single delayed
// refresh expire the lease of a perfectly healthy node, causing the control
// plane to evict and reschedule its pods for no reason.
func NewRegistrar(client *redisclient.Client, logger *logging.Logger, cfg *config.Config) (*Registrar, error) {
	if cfg.HeartbeatInterval <= 0 || cfg.LeaseTTL <= 0 {
		return nil, fmt.Errorf("heartbeat interval and lease TTL must both be positive (got %s and %s)",
			cfg.HeartbeatInterval, cfg.LeaseTTL)
	}
	// Require the TTL to cover at least two missed refreshes.
	if cfg.HeartbeatInterval*2 >= cfg.LeaseTTL {
		return nil, fmt.Errorf(
			"heartbeat interval %s is too close to lease TTL %s: TTL must exceed twice the interval so one missed refresh cannot expire a live node",
			cfg.HeartbeatInterval, cfg.LeaseTTL)
	}
	if cfg.TotalCPU <= 0 || cfg.TotalMem <= 0 {
		return nil, fmt.Errorf("node must advertise positive capacity (got cpu=%dm mem=%dMB)",
			cfg.TotalCPU, cfg.TotalMem)
	}

	return &Registrar{
		client:            client,
		logger:            logger,
		nodeID:            cfg.NodeID,
		totalCPU:          cfg.TotalCPU,
		totalMem:          cfg.TotalMem,
		heartbeatInterval: cfg.HeartbeatInterval,
		leaseTTL:          cfg.LeaseTTL,
	}, nil
}

// Register adds this node to the registry and writes its capacity hash.
//
// It is idempotent, so an agent restart re-runs it safely. Allocations are
// never reset to zero: they are recomputed from the pods actually bound to
// this node, so restarting an agent cannot lose track of capacity that live
// pods are still holding.
func (r *Registrar) Register(ctx context.Context) error {
	allocatedCPU, allocatedMem, err := r.ReconcileAllocations(ctx)
	if err != nil {
		return fmt.Errorf("reconcile allocations for %s: %w", r.nodeID, err)
	}

	capacity := &schema.NodeCapacity{
		TotalCPU:     r.totalCPU,
		TotalMem:     r.totalMem,
		AllocatedCPU: allocatedCPU,
		AllocatedMem: allocatedMem,
	}
	if err := r.client.HashSet(ctx, schema.NodeCapacityKey(r.nodeID), schema.NodeCapacityToMap(capacity)); err != nil {
		return fmt.Errorf("write capacity hash for %s: %w", r.nodeID, err)
	}

	// Join the registry last: a node in the registry without a capacity hash
	// would be visible to the scheduler before its headroom is known.
	if err := r.client.SetAdd(ctx, schema.NodeRegistryKey(), r.nodeID); err != nil {
		return fmt.Errorf("join node registry as %s: %w", r.nodeID, err)
	}

	r.logger.Info(ctx, "node_registered",
		fmt.Sprintf("registered with cpu=%dm mem=%dMB, reclaimed allocations cpu=%dm mem=%dMB",
			r.totalCPU, r.totalMem, allocatedCPU, allocatedMem),
		logging.NodeID(r.nodeID),
	)
	return nil
}

// ReconcileAllocations recomputes this node's allocated CPU and memory from
// the pods actually bound to it, rather than trusting a stored counter.
//
// This is what makes the agent stateless: Redis holds the pods, so the true
// allocation is always derivable and a restart cannot drift from reality.
func (r *Registrar) ReconcileAllocations(ctx context.Context) (cpu int, mem int, err error) {
	podKeys, err := r.client.ScanKeys(ctx, schema.PodKeyPattern())
	if err != nil {
		return 0, 0, fmt.Errorf("scan pods: %w", err)
	}

	for _, key := range podKeys {
		fields, err := r.client.HashGet(ctx, key)
		if err != nil {
			return 0, 0, fmt.Errorf("read pod %s: %w", key, err)
		}
		// A pod deleted between the scan and this read returns no fields.
		if len(fields) == 0 {
			continue
		}
		if fields[schema.FieldPodNodeID] != r.nodeID {
			continue
		}
		if !schema.ConsumesCapacity(fields[schema.FieldPodStatus]) {
			continue
		}

		pod, err := schema.MapToPod(fields)
		if err != nil {
			// A malformed pod hash must not make the node unschedulable.
			r.logger.Warn(ctx, "pod_hash_malformed",
				fmt.Sprintf("skipping unreadable pod %s during allocation reconcile: %v", key, err),
				logging.NodeID(r.nodeID),
			)
			continue
		}
		cpu += pod.CPURequest
		mem += pod.MemRequest
	}
	return cpu, mem, nil
}

// Heartbeat refreshes the liveness lease, resetting its TTL.
func (r *Registrar) Heartbeat(ctx context.Context) error {
	return r.client.SetKey(ctx, schema.NodeStatusKey(r.nodeID), schema.NodeStatusReady, r.leaseTTL)
}

// RunHeartbeatLoop refreshes the lease on every interval until ctx is done.
//
// A failed refresh is logged and retried on the next tick rather than being
// fatal: a brief Redis blip should not tear down an agent that is supervising
// healthy pods. If Redis stays unreachable the lease simply expires, which is
// exactly the signal the control plane is watching for.
func (r *Registrar) RunHeartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(r.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.Heartbeat(ctx); err != nil {
				if ctx.Err() != nil {
					return // shutting down, not a real failure
				}
				r.logger.Warn(ctx, "heartbeat_failed",
					fmt.Sprintf("lease refresh failed, will retry in %s: %v", r.heartbeatInterval, err),
					logging.NodeID(r.nodeID),
				)
			}
		}
	}
}

// Deregister announces a clean departure and drops the lease immediately.
//
// The node stays in the registry on purpose. The Health Controller's sweep
// sees a registered node with no lease, evicts it, and reschedules its pods
// onto surviving nodes — the same recovery path as a crash, just reached
// without waiting out the TTL. Eviction is what removes it from the registry.
func (r *Registrar) Deregister(ctx context.Context) error {
	r.logger.Info(ctx, "node_deregistering", "clean shutdown, dropping liveness lease",
		logging.NodeID(r.nodeID),
	)
	if err := r.client.DeleteKey(ctx, schema.NodeStatusKey(r.nodeID)); err != nil {
		return fmt.Errorf("drop lease for %s: %w", r.nodeID, err)
	}
	return nil
}
