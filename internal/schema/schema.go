// Package schema is the single source of truth for every Redis key pattern,
// hash field name, and logical data shape used across the mini-k8s cluster.
// No other module may define raw Redis key strings — all key construction
// and field references flow through this package.
package schema

import (
	"fmt"
	"strconv"
)

// ─── Key builders ───────────────────────────────────────────────────────────

// LogStreamKey returns the key for the cluster-wide structured log stream.
func LogStreamKey() string { return "cluster:logstream" }

// TestKey returns a throwaway key used for smoke-test round-trips.
func TestKey() string { return "test:key" }

// NodeRegistryKey returns the key for the set of known node IDs.
func NodeRegistryKey() string { return "nodes:registry" }

// NodeCapacityKey returns the hash key holding a node's resource capacity.
func NodeCapacityKey(nodeID string) string {
	return fmt.Sprintf("node:%s:capacity", nodeID)
}

// NodeStatusKey returns the key for a node's liveness lease.
func NodeStatusKey(nodeID string) string {
	return fmt.Sprintf("node:%s:status", nodeID)
}

// NodeCommandsKey returns the list key for a node's pending command queue.
func NodeCommandsKey(nodeID string) string {
	return fmt.Sprintf("node:%s:commands", nodeID)
}

// DeploymentKey returns the hash key for a deployment spec.
func DeploymentKey(name string) string {
	return fmt.Sprintf("deployment:%s", name)
}

// PodKey returns the hash key for a specific pod within a deployment.
func PodKey(deployment, podID string) string {
	return fmt.Sprintf("pod:%s:%s", deployment, podID)
}

// PodKeyPattern returns the glob pattern matching every pod hash key.
// Used by SCAN-based sweeps that must enumerate pods without blocking Redis.
func PodKeyPattern() string { return "pod:*:*" }

// DeploymentPodsPattern returns the glob pattern matching every pod key
// belonging to a single deployment.
func DeploymentPodsPattern(deployment string) string {
	return fmt.Sprintf("pod:%s:*", deployment)
}

// DeploymentKeyPattern returns the glob pattern matching every deployment hash key.
func DeploymentKeyPattern() string { return "deployment:*" }

// DeploymentEventsChannel returns the Pub/Sub channel for deployment change notifications.
func DeploymentEventsChannel() string { return "deployment:events" }

// TelemetryCPUKey returns the list key for a pod's recent CPU samples.
func TelemetryCPUKey(deployment, podID string) string {
	return fmt.Sprintf("telemetry:%s:%s:cpu", deployment, podID)
}

// ─── Field-name constants ───────────────────────────────────────────────────
// Grouped by the hash type they belong to. Every hash read/write in the
// codebase must reference these constants, never bare strings.

// Deployment hash fields.
const (
	FieldDeploymentName   = "name"
	FieldMinReplicas      = "min_replicas"
	FieldMaxReplicas      = "max_replicas"
	FieldCPURequest       = "cpu_request"
	FieldMemRequest       = "mem_request"
	FieldDesiredReplicas  = "desired_replicas"
	FieldCreatedAt        = "created_at"
	FieldLastScaleUpAt    = "last_scale_up_at"
	FieldLastScaleDownAt  = "last_scale_down_at"
	FieldTargetCPUPercent = "target_cpu_percentage"
	FieldExecPath         = "exec_path"
	FieldPort             = "port"
)

// Pod hash fields.
const (
	FieldPodDeployment     = "deployment"
	FieldPodID             = "pod_id"
	FieldPodStatus         = "status"
	FieldPodNodeID         = "node_id"
	FieldPodCPURequest     = "cpu_request"
	FieldPodMemRequest     = "mem_request"
	FieldPodCreatedAt      = "created_at"
	FieldPodPID            = "pid"
	FieldPodStartedAt      = "started_at"
	FieldPodRestartCount   = "restart_count"
	FieldPodConsecFailures = "consecutive_failures"
	FieldPodLastHealthOKAt = "last_health_ok_at"
	FieldPodBackoffNextSec = "backoff_next_seconds"
	FieldPodLastRestartAt  = "last_restart_at"
	FieldPodPIDStartTime   = "pid_start_time"
	FieldPodHostPort       = "host_port"
)

// Node capacity hash fields.
const (
	FieldTotalCPU     = "total_cpu"
	FieldTotalMem     = "total_mem"
	FieldAllocatedCPU = "allocated_cpu"
	FieldAllocatedMem = "allocated_mem"
)

// ─── Status constants ───────────────────────────────────────────────────────

const (
	StatusPending          = "Pending"
	StatusRunning          = "Running"
	StatusFailed           = "Failed"
	StatusEvicted          = "Evicted"
	StatusCrashLoopBackOff = "CrashLoopBackOff"
	StatusUnschedulable    = "Unschedulable"
)

// ConsumesCapacity reports whether a pod in the given status still holds a
// reservation against its node's capacity.
//
// A pod consumes capacity from the moment the scheduler reserves it (Pending)
// until that reservation is explicitly released. Only Evicted releases it: the
// Health Controller resets a dead node's allocations when it evicts, so an
// Evicted pod no longer counts anywhere. Failed and CrashLoopBackOff pods are
// still bound to their node and awaiting restart, so their reservation stands.
func ConsumesCapacity(status string) bool {
	return status != StatusEvicted
}

// Node status value (written as the lease value).
const NodeStatusReady = "Ready"

// ─── Logical data shapes (structs) ─────────────────────────────────────────
// Used for (de)serialization between Go and Redis hashes.

// DeploymentSpec represents a deployment's configuration stored in Redis.
type DeploymentSpec struct {
	Name             string `json:"name"`
	MinReplicas      int    `json:"min_replicas"`
	MaxReplicas      int    `json:"max_replicas"`
	CPURequest       int    `json:"cpu_request"` // millicores
	MemRequest       int    `json:"mem_request"` // MB
	DesiredReplicas  int    `json:"desired_replicas"`
	TargetCPUPercent int    `json:"target_cpu_percentage"`
	ExecPath         string `json:"exec_path"`
	Port             int    `json:"port"`
	CreatedAt        string `json:"created_at"`
	LastScaleUpAt    string `json:"last_scale_up_at,omitempty"`
	LastScaleDownAt  string `json:"last_scale_down_at,omitempty"`
}

// PodSpec represents a pod's state stored in Redis.
type PodSpec struct {
	Deployment          string `json:"deployment"`
	PodID               string `json:"pod_id"`
	Status              string `json:"status"`
	NodeID              string `json:"node_id"`
	CPURequest          int    `json:"cpu_request"`
	MemRequest          int    `json:"mem_request"`
	CreatedAt           string `json:"created_at"`
	PID                 int    `json:"pid,omitempty"`
	StartedAt           string `json:"started_at,omitempty"`
	RestartCount        int    `json:"restart_count,omitempty"`
	ConsecutiveFailures int    `json:"consecutive_failures,omitempty"`
	LastHealthOKAt      string `json:"last_health_ok_at,omitempty"`
	BackoffNextSeconds  int    `json:"backoff_next_seconds,omitempty"`
	LastRestartAt       string `json:"last_restart_at,omitempty"`
	PIDStartTime        uint64 `json:"pid_start_time,omitempty"`
	HostPort            int    `json:"host_port,omitempty"`
}

// NodeCapacity represents a node's resource capacity stored in Redis.
type NodeCapacity struct {
	TotalCPU     int `json:"total_cpu"`     // millicores
	TotalMem     int `json:"total_mem"`     // MB
	AllocatedCPU int `json:"allocated_cpu"` // millicores
	AllocatedMem int `json:"allocated_mem"` // MB
}

// ─── Struct ↔ Redis hash conversion ─────────────────────────────────────────

// DeploymentToMap converts a DeploymentSpec to a Redis hash field map.
func DeploymentToMap(d *DeploymentSpec) map[string]interface{} {
	m := map[string]interface{}{
		FieldDeploymentName:   d.Name,
		FieldMinReplicas:      d.MinReplicas,
		FieldMaxReplicas:      d.MaxReplicas,
		FieldCPURequest:       d.CPURequest,
		FieldMemRequest:       d.MemRequest,
		FieldDesiredReplicas:  d.DesiredReplicas,
		FieldTargetCPUPercent: d.TargetCPUPercent,
		FieldExecPath:         d.ExecPath,
		FieldPort:             d.Port,
		FieldCreatedAt:        d.CreatedAt,
	}
	if d.LastScaleUpAt != "" {
		m[FieldLastScaleUpAt] = d.LastScaleUpAt
	}
	if d.LastScaleDownAt != "" {
		m[FieldLastScaleDownAt] = d.LastScaleDownAt
	}
	return m
}

// MapToDeployment converts a Redis hash field map to a DeploymentSpec.
func MapToDeployment(m map[string]string) (*DeploymentSpec, error) {
	d := &DeploymentSpec{
		Name:            m[FieldDeploymentName],
		ExecPath:        m[FieldExecPath],
		CreatedAt:       m[FieldCreatedAt],
		LastScaleUpAt:   m[FieldLastScaleUpAt],
		LastScaleDownAt: m[FieldLastScaleDownAt],
	}

	var err error
	if d.MinReplicas, err = atoi(m[FieldMinReplicas]); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", FieldMinReplicas, err)
	}
	if d.MaxReplicas, err = atoi(m[FieldMaxReplicas]); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", FieldMaxReplicas, err)
	}
	if d.CPURequest, err = atoi(m[FieldCPURequest]); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", FieldCPURequest, err)
	}
	if d.MemRequest, err = atoi(m[FieldMemRequest]); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", FieldMemRequest, err)
	}
	if d.DesiredReplicas, err = atoi(m[FieldDesiredReplicas]); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", FieldDesiredReplicas, err)
	}

	// Optional at the schema layer so hashes predating these fields stay
	// readable; the API validator is what requires them on write.
	d.TargetCPUPercent, _ = atoiDefault(m[FieldTargetCPUPercent], 0)
	d.Port, _ = atoiDefault(m[FieldPort], 0)
	return d, nil
}

// PodToMap converts a PodSpec to a Redis hash field map.
func PodToMap(p *PodSpec) map[string]interface{} {
	m := map[string]interface{}{
		FieldPodDeployment: p.Deployment,
		FieldPodID:         p.PodID,
		FieldPodStatus:     p.Status,
		FieldPodNodeID:     p.NodeID,
		FieldPodCPURequest: p.CPURequest,
		FieldPodMemRequest: p.MemRequest,
		FieldPodCreatedAt:  p.CreatedAt,
	}
	if p.PID != 0 {
		m[FieldPodPID] = p.PID
	}
	if p.StartedAt != "" {
		m[FieldPodStartedAt] = p.StartedAt
	}
	if p.RestartCount != 0 {
		m[FieldPodRestartCount] = p.RestartCount
	}
	if p.ConsecutiveFailures != 0 {
		m[FieldPodConsecFailures] = p.ConsecutiveFailures
	}
	if p.LastHealthOKAt != "" {
		m[FieldPodLastHealthOKAt] = p.LastHealthOKAt
	}
	if p.BackoffNextSeconds != 0 {
		m[FieldPodBackoffNextSec] = p.BackoffNextSeconds
	}
	if p.LastRestartAt != "" {
		m[FieldPodLastRestartAt] = p.LastRestartAt
	}
	if p.PIDStartTime != 0 {
		m[FieldPodPIDStartTime] = p.PIDStartTime
	}
	if p.HostPort != 0 {
		m[FieldPodHostPort] = p.HostPort
	}
	return m
}

// MapToPod converts a Redis hash field map to a PodSpec.
func MapToPod(m map[string]string) (*PodSpec, error) {
	p := &PodSpec{
		Deployment:     m[FieldPodDeployment],
		PodID:          m[FieldPodID],
		Status:         m[FieldPodStatus],
		NodeID:         m[FieldPodNodeID],
		CreatedAt:      m[FieldPodCreatedAt],
		StartedAt:      m[FieldPodStartedAt],
		LastHealthOKAt: m[FieldPodLastHealthOKAt],
		LastRestartAt:  m[FieldPodLastRestartAt],
	}

	var err error
	if p.CPURequest, err = atoi(m[FieldPodCPURequest]); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", FieldPodCPURequest, err)
	}
	if p.MemRequest, err = atoi(m[FieldPodMemRequest]); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", FieldPodMemRequest, err)
	}

	// Optional integer fields — default to 0 if absent.
	p.PID, _ = atoiDefault(m[FieldPodPID], 0)
	p.RestartCount, _ = atoiDefault(m[FieldPodRestartCount], 0)
	p.ConsecutiveFailures, _ = atoiDefault(m[FieldPodConsecFailures], 0)
	p.BackoffNextSeconds, _ = atoiDefault(m[FieldPodBackoffNextSec], 0)
	p.HostPort, _ = atoiDefault(m[FieldPodHostPort], 0)
	p.PIDStartTime = parseUint(m[FieldPodPIDStartTime])

	return p, nil
}

// NodeCapacityToMap converts a NodeCapacity to a Redis hash field map.
func NodeCapacityToMap(n *NodeCapacity) map[string]interface{} {
	return map[string]interface{}{
		FieldTotalCPU:     n.TotalCPU,
		FieldTotalMem:     n.TotalMem,
		FieldAllocatedCPU: n.AllocatedCPU,
		FieldAllocatedMem: n.AllocatedMem,
	}
}

// MapToNodeCapacity converts a Redis hash field map to a NodeCapacity.
func MapToNodeCapacity(m map[string]string) (*NodeCapacity, error) {
	n := &NodeCapacity{}
	var err error
	if n.TotalCPU, err = atoi(m[FieldTotalCPU]); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", FieldTotalCPU, err)
	}
	if n.TotalMem, err = atoi(m[FieldTotalMem]); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", FieldTotalMem, err)
	}
	if n.AllocatedCPU, err = atoi(m[FieldAllocatedCPU]); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", FieldAllocatedCPU, err)
	}
	if n.AllocatedMem, err = atoi(m[FieldAllocatedMem]); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", FieldAllocatedMem, err)
	}
	return n, nil
}

// ─── Helpers ────────────────────────────────────────────────────────────────

// parseUint reads an unsigned value, yielding 0 when absent or malformed.
// For the process start-time marker 0 means "unknown", which callers must
// treat as identity being unverifiable rather than as a match.
func parseUint(s string) uint64 {
	if s == "" {
		return 0
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// atoi converts a string to an int, returning an error on failure.
func atoi(s string) (int, error) {
	if s == "" {
		return 0, fmt.Errorf("empty string")
	}
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}

// atoiDefault converts a string to an int, returning the default on failure.
func atoiDefault(s string, def int) (int, error) {
	if s == "" {
		return def, nil
	}
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	if err != nil {
		return def, err
	}
	return n, nil
}
