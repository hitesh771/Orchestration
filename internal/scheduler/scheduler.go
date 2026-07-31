// Package scheduler places pod replicas onto worker nodes using best-fit
// selection backed by race-free capacity reservation.
//
// Capacity is mutated only through Lua scripts that run atomically inside
// Redis. Two schedulers racing on the same node therefore cannot both observe
// the same headroom and both claim it, so a node's allocations can never
// exceed its totals.
package scheduler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"time"

	"mini-k8s/internal/logging"
	"mini-k8s/internal/redisclient"
	"mini-k8s/internal/schema"
)

// Reservation outcomes returned by the reserve script.
const (
	reserveOK         = 1  // capacity was reserved
	reserveNoHeadroom = 0  // node exists but cannot fit the request
	reserveNoSuchNode = -1 // node has no capacity hash
	reserveMalformed  = -2 // capacity hash is unreadable
)

// reserveCapacityScript atomically reserves CPU and memory on one node.
//
// Read, headroom check, and increment happen in a single indivisible step,
// which is what removes the read-then-write race. The script mutates nothing
// unless both dimensions fit.
var reserveCapacityScript = fmt.Sprintf(`
local key = KEYS[1]
local cpuReq = tonumber(ARGV[1])
local memReq = tonumber(ARGV[2])

if redis.call('EXISTS', key) == 0 then
  return %d
end

local totalCPU = tonumber(redis.call('HGET', key, '%s'))
local totalMem = tonumber(redis.call('HGET', key, '%s'))
local allocCPU = tonumber(redis.call('HGET', key, '%s'))
local allocMem = tonumber(redis.call('HGET', key, '%s'))

if totalCPU == nil or totalMem == nil or allocCPU == nil or allocMem == nil then
  return %d
end

if (totalCPU - allocCPU) < cpuReq or (totalMem - allocMem) < memReq then
  return %d
end

redis.call('HINCRBY', key, '%s', cpuReq)
redis.call('HINCRBY', key, '%s', memReq)
return %d
`,
	reserveNoSuchNode,
	schema.FieldTotalCPU, schema.FieldTotalMem,
	schema.FieldAllocatedCPU, schema.FieldAllocatedMem,
	reserveMalformed,
	reserveNoHeadroom,
	schema.FieldAllocatedCPU, schema.FieldAllocatedMem,
	reserveOK,
)

// releaseCapacityScript atomically returns CPU and memory to a node.
//
// Allocations are clamped at zero: a double release (for example a retry
// racing an eviction that already reset the node) must not drive the counter
// negative and hand out capacity the node does not have.
var releaseCapacityScript = fmt.Sprintf(`
local key = KEYS[1]
local cpuRel = tonumber(ARGV[1])
local memRel = tonumber(ARGV[2])

if redis.call('EXISTS', key) == 0 then
  return 0
end

local allocCPU = tonumber(redis.call('HGET', key, '%s')) or 0
local allocMem = tonumber(redis.call('HGET', key, '%s')) or 0

local newCPU = allocCPU - cpuRel
local newMem = allocMem - memRel
if newCPU < 0 then newCPU = 0 end
if newMem < 0 then newMem = 0 end

redis.call('HSET', key, '%s', newCPU, '%s', newMem)
return 1
`,
	schema.FieldAllocatedCPU, schema.FieldAllocatedMem,
	schema.FieldAllocatedCPU, schema.FieldAllocatedMem,
)

// Scheduler assigns pods to nodes.
type Scheduler struct {
	client *redisclient.Client
	logger *logging.Logger
}

// New builds a Scheduler.
func New(client *redisclient.Client, logger *logging.Logger) *Scheduler {
	return &Scheduler{client: client, logger: logger}
}

// candidate is a node considered for placement.
type candidate struct {
	nodeID   string
	capacity *schema.NodeCapacity
}

// LiveNodes returns the registered nodes whose liveness lease is present.
//
// A registered node with an expired lease is dead but not yet evicted, so
// placing work on it would strand that work until the next eviction sweep.
func (s *Scheduler) LiveNodes(ctx context.Context) ([]string, error) {
	registered, err := s.client.SetMembers(ctx, schema.NodeRegistryKey())
	if err != nil {
		return nil, fmt.Errorf("read node registry: %w", err)
	}

	live := make([]string, 0, len(registered))
	for _, nodeID := range registered {
		ok, err := s.client.Exists(ctx, schema.NodeStatusKey(nodeID))
		if err != nil {
			return nil, fmt.Errorf("check lease for %s: %w", nodeID, err)
		}
		if ok {
			live = append(live, nodeID)
		}
	}
	sort.Strings(live) // deterministic ordering for reproducible placement
	return live, nil
}

// bestFitOrder returns live nodes that can fit the request, ordered from
// tightest to loosest fit.
//
// Tight packing is deliberate: filling a nearly-full node first preserves
// large contiguous gaps on other nodes for pods with bigger requests. Scores
// are normalized per dimension because CPU is measured in millicores and
// memory in megabytes, so raw leftovers would let memory dominate the metric.
func (s *Scheduler) bestFitOrder(ctx context.Context, cpuReq, memReq int) ([]candidate, error) {
	liveNodes, err := s.LiveNodes(ctx)
	if err != nil {
		return nil, err
	}

	fits := make([]candidate, 0, len(liveNodes))
	for _, nodeID := range liveNodes {
		fields, err := s.client.HashGet(ctx, schema.NodeCapacityKey(nodeID))
		if err != nil {
			return nil, fmt.Errorf("read capacity for %s: %w", nodeID, err)
		}
		if len(fields) == 0 {
			continue // registered but capacity not published yet
		}
		capacity, err := schema.MapToNodeCapacity(fields)
		if err != nil {
			s.logger.Warn(ctx, "capacity_hash_malformed",
				fmt.Sprintf("skipping node %s during placement: %v", nodeID, err),
				logging.NodeID(nodeID),
			)
			continue
		}
		if capacity.TotalCPU-capacity.AllocatedCPU < cpuReq {
			continue
		}
		if capacity.TotalMem-capacity.AllocatedMem < memReq {
			continue
		}
		fits = append(fits, candidate{nodeID: nodeID, capacity: capacity})
	}

	score := func(c candidate) float64 {
		var cpuLeft, memLeft float64
		if c.capacity.TotalCPU > 0 {
			cpuLeft = float64(c.capacity.TotalCPU-c.capacity.AllocatedCPU-cpuReq) / float64(c.capacity.TotalCPU)
		}
		if c.capacity.TotalMem > 0 {
			memLeft = float64(c.capacity.TotalMem-c.capacity.AllocatedMem-memReq) / float64(c.capacity.TotalMem)
		}
		return cpuLeft + memLeft
	}

	sort.SliceStable(fits, func(i, j int) bool {
		si, sj := score(fits[i]), score(fits[j])
		if si != sj {
			return si < sj // least leftover first
		}
		return fits[i].nodeID < fits[j].nodeID // stable tiebreak
	})
	return fits, nil
}

// Reserve atomically claims capacity on a node. It reports whether the
// reservation succeeded; a node that cannot fit the request is not an error.
func (s *Scheduler) Reserve(ctx context.Context, nodeID string, cpuReq, memReq int) (bool, error) {
	raw, err := s.client.EvalScript(ctx, reserveCapacityScript,
		[]string{schema.NodeCapacityKey(nodeID)}, cpuReq, memReq)
	if err != nil {
		return false, fmt.Errorf("reserve capacity on %s: %w", nodeID, err)
	}

	code, ok := raw.(int64)
	if !ok {
		return false, fmt.Errorf("reserve capacity on %s: unexpected script result %T", nodeID, raw)
	}

	switch code {
	case reserveOK:
		return true, nil
	case reserveNoHeadroom:
		return false, nil
	case reserveNoSuchNode:
		return false, nil // node vanished mid-placement; try the next candidate
	case reserveMalformed:
		return false, fmt.Errorf("capacity hash for %s is malformed", nodeID)
	default:
		return false, fmt.Errorf("reserve capacity on %s: unknown result %d", nodeID, code)
	}
}

// Release atomically returns capacity to a node, clamping at zero.
func (s *Scheduler) Release(ctx context.Context, nodeID string, cpuRel, memRel int) error {
	_, err := s.client.EvalScript(ctx, releaseCapacityScript,
		[]string{schema.NodeCapacityKey(nodeID)}, cpuRel, memRel)
	if err != nil {
		return fmt.Errorf("release capacity on %s: %w", nodeID, err)
	}
	return nil
}

// ScheduleReplicas places count new replicas of a deployment.
//
// It returns the pod IDs actually created. Placing fewer than requested is a
// normal outcome when the cluster is saturated: the shortfall is logged as
// unschedulable and retried by the next reconciliation sweep, which is why a
// full cluster degrades rather than failing the caller.
func (s *Scheduler) ScheduleReplicas(ctx context.Context, dep *schema.DeploymentSpec, count int) ([]string, error) {
	if count <= 0 {
		return nil, nil
	}

	created := make([]string, 0, count)
	for i := 0; i < count; i++ {
		podID, err := s.scheduleOne(ctx, dep)
		if err != nil {
			return created, err
		}
		if podID == "" {
			// No node could fit this replica, so none will fit the rest.
			s.logger.Warn(ctx, "pods_unschedulable",
				fmt.Sprintf("placed %d of %d replicas; no node has %dm CPU and %dMB free",
					len(created), count, dep.CPURequest, dep.MemRequest),
				logging.DeploymentID(dep.Name),
			)
			break
		}
		created = append(created, podID)
	}
	return created, nil
}

// scheduleOne places a single replica, returning "" when nothing fits.
func (s *Scheduler) scheduleOne(ctx context.Context, dep *schema.DeploymentSpec) (string, error) {
	candidates, err := s.bestFitOrder(ctx, dep.CPURequest, dep.MemRequest)
	if err != nil {
		return "", err
	}

	for _, c := range candidates {
		// The pre-filter used a possibly stale read, so the atomic reserve is
		// the real gate. A node that filled up in between simply fails here
		// and we fall through to the next candidate.
		reserved, err := s.Reserve(ctx, c.nodeID, dep.CPURequest, dep.MemRequest)
		if err != nil {
			return "", err
		}
		if !reserved {
			continue
		}

		podID, err := newPodID()
		if err != nil {
			// Give the capacity back rather than leaking it.
			s.releaseAfterFailure(ctx, c.nodeID, dep)
			return "", fmt.Errorf("generate pod id: %w", err)
		}

		pod := &schema.PodSpec{
			Deployment: dep.Name,
			PodID:      podID,
			Status:     schema.StatusPending,
			NodeID:     c.nodeID,
			CPURequest: dep.CPURequest,
			MemRequest: dep.MemRequest,
			CreatedAt:  strconv.FormatInt(time.Now().Unix(), 10),
		}
		if err := s.client.HashSet(ctx, schema.PodKey(dep.Name, podID), schema.PodToMap(pod)); err != nil {
			// The reservation is already committed, so it must be undone or
			// the node would lose capacity to a pod that does not exist.
			s.releaseAfterFailure(ctx, c.nodeID, dep)
			return "", fmt.Errorf("write pod hash for %s/%s: %w", dep.Name, podID, err)
		}

		s.logger.Info(ctx, "pod_scheduled",
			fmt.Sprintf("placed pod on %s reserving %dm CPU and %dMB", c.nodeID, dep.CPURequest, dep.MemRequest),
			logging.DeploymentID(dep.Name), logging.PodID(podID), logging.NodeID(c.nodeID),
		)
		return podID, nil
	}
	return "", nil
}

// releaseAfterFailure undoes a committed reservation when the pod it was for
// could not be created.
func (s *Scheduler) releaseAfterFailure(ctx context.Context, nodeID string, dep *schema.DeploymentSpec) {
	if err := s.Release(ctx, nodeID, dep.CPURequest, dep.MemRequest); err != nil {
		// Logged rather than returned: the caller is already failing, and the
		// node's allocation is recomputed from real pods when its agent
		// restarts, so this cannot leak permanently.
		s.logger.Error(ctx, "capacity_release_failed",
			fmt.Sprintf("could not roll back reservation on %s: %v", nodeID, err),
			logging.DeploymentID(dep.Name), logging.NodeID(nodeID),
		)
	}
}

// newPodID returns a short random identifier for a pod.
func newPodID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
