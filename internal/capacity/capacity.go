// Package capacity is the single place node resource allocations are mutated.
//
// Every change runs as a Lua script inside Redis, so read, check, and write
// happen as one indivisible step. That is what lets concurrent schedulers and
// agents touch the same node without a read-then-write race letting a node be
// oversubscribed.
//
// Both the control plane (reserving during placement) and the data plane
// (releasing when a pod is removed) go through here, so the two cannot drift
// apart on how allocation arithmetic is done.
package capacity

import (
	"context"
	"fmt"

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

// reserveScript atomically reserves CPU and memory on one node.
//
// It mutates nothing unless the request fits in both dimensions, so a refused
// reservation leaves the node exactly as it was.
var reserveScript = fmt.Sprintf(`
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

// releaseScript atomically returns CPU and memory to a node.
//
// Allocations are clamped at zero. A double release — a scheduler rolling back
// while an eviction has already reset the node, for instance — must not drive
// the counter negative, which would advertise capacity the node does not have.
var releaseScript = fmt.Sprintf(`
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

// Reserve atomically claims capacity on a node, reporting whether it fit.
//
// A node that cannot fit the request is a normal scheduling outcome, not an
// error, and neither is a node that has disappeared: the caller simply tries
// the next candidate.
func Reserve(ctx context.Context, client *redisclient.Client, nodeID string, cpuReq, memReq int) (bool, error) {
	raw, err := client.EvalScript(ctx, reserveScript,
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
	case reserveNoHeadroom, reserveNoSuchNode:
		return false, nil
	case reserveMalformed:
		return false, fmt.Errorf("capacity hash for %s is malformed", nodeID)
	default:
		return false, fmt.Errorf("reserve capacity on %s: unknown result %d", nodeID, code)
	}
}

// Release atomically returns capacity to a node, clamping at zero.
func Release(ctx context.Context, client *redisclient.Client, nodeID string, cpuRel, memRel int) error {
	if _, err := client.EvalScript(ctx, releaseScript,
		[]string{schema.NodeCapacityKey(nodeID)}, cpuRel, memRel); err != nil {
		return fmt.Errorf("release capacity on %s: %w", nodeID, err)
	}
	return nil
}
