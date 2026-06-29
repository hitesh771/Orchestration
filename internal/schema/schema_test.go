package schema

import (
	"fmt"
	"testing"
)

// ─── Key builder tests ──────────────────────────────────────────────────────

func TestLogStreamKey(t *testing.T) {
	if got := LogStreamKey(); got != "cluster:logstream" {
		t.Errorf("LogStreamKey() = %q, want %q", got, "cluster:logstream")
	}
}

func TestTestKey(t *testing.T) {
	if got := TestKey(); got != "test:key" {
		t.Errorf("TestKey() = %q, want %q", got, "test:key")
	}
}

func TestNodeRegistryKey(t *testing.T) {
	if got := NodeRegistryKey(); got != "nodes:registry" {
		t.Errorf("NodeRegistryKey() = %q, want %q", got, "nodes:registry")
	}
}

func TestNodeCapacityKey(t *testing.T) {
	got := NodeCapacityKey("node-1")
	want := "node:node-1:capacity"
	if got != want {
		t.Errorf("NodeCapacityKey(\"node-1\") = %q, want %q", got, want)
	}
}

func TestNodeStatusKey(t *testing.T) {
	got := NodeStatusKey("node-1")
	want := "node:node-1:status"
	if got != want {
		t.Errorf("NodeStatusKey(\"node-1\") = %q, want %q", got, want)
	}
}

func TestNodeCommandsKey(t *testing.T) {
	got := NodeCommandsKey("node-1")
	want := "node:node-1:commands"
	if got != want {
		t.Errorf("NodeCommandsKey(\"node-1\") = %q, want %q", got, want)
	}
}

func TestDeploymentKey(t *testing.T) {
	got := DeploymentKey("web-app")
	want := "deployment:web-app"
	if got != want {
		t.Errorf("DeploymentKey(\"web-app\") = %q, want %q", got, want)
	}
}

func TestPodKey(t *testing.T) {
	got := PodKey("web-app", "abc-123")
	want := "pod:web-app:abc-123"
	if got != want {
		t.Errorf("PodKey(\"web-app\", \"abc-123\") = %q, want %q", got, want)
	}
}

func TestDeploymentEventsChannel(t *testing.T) {
	if got := DeploymentEventsChannel(); got != "deployment:events" {
		t.Errorf("DeploymentEventsChannel() = %q, want %q", got, "deployment:events")
	}
}

func TestTelemetryCPUKey(t *testing.T) {
	got := TelemetryCPUKey("web-app", "abc-123")
	want := "telemetry:web-app:abc-123:cpu"
	if got != want {
		t.Errorf("TelemetryCPUKey(\"web-app\", \"abc-123\") = %q, want %q", got, want)
	}
}

// ─── Struct ↔ map round-trip tests ──────────────────────────────────────────

func TestDeploymentRoundTrip(t *testing.T) {
	original := &DeploymentSpec{
		Name:            "web-app",
		MinReplicas:     2,
		MaxReplicas:     10,
		CPURequest:      100,
		MemRequest:      256,
		DesiredReplicas: 3,
		CreatedAt:       "2025-01-01T00:00:00Z",
	}

	m := DeploymentToMap(original)

	// Convert map[string]interface{} to map[string]string for MapToDeployment
	strMap := make(map[string]string)
	for k, v := range m {
		strMap[k] = fmt.Sprintf("%v", v)
	}

	got, err := MapToDeployment(strMap)
	if err != nil {
		t.Fatalf("MapToDeployment() error: %v", err)
	}

	if got.Name != original.Name {
		t.Errorf("Name = %q, want %q", got.Name, original.Name)
	}
	if got.MinReplicas != original.MinReplicas {
		t.Errorf("MinReplicas = %d, want %d", got.MinReplicas, original.MinReplicas)
	}
	if got.MaxReplicas != original.MaxReplicas {
		t.Errorf("MaxReplicas = %d, want %d", got.MaxReplicas, original.MaxReplicas)
	}
	if got.CPURequest != original.CPURequest {
		t.Errorf("CPURequest = %d, want %d", got.CPURequest, original.CPURequest)
	}
	if got.MemRequest != original.MemRequest {
		t.Errorf("MemRequest = %d, want %d", got.MemRequest, original.MemRequest)
	}
	if got.DesiredReplicas != original.DesiredReplicas {
		t.Errorf("DesiredReplicas = %d, want %d", got.DesiredReplicas, original.DesiredReplicas)
	}
	if got.CreatedAt != original.CreatedAt {
		t.Errorf("CreatedAt = %q, want %q", got.CreatedAt, original.CreatedAt)
	}
}

func TestPodRoundTrip(t *testing.T) {
	original := &PodSpec{
		Deployment:          "web-app",
		PodID:               "abc-123",
		Status:              StatusRunning,
		NodeID:              "node-1",
		CPURequest:          100,
		MemRequest:          256,
		CreatedAt:           "2025-01-01T00:00:00Z",
		PID:                 12345,
		StartedAt:           "2025-01-01T00:00:01Z",
		RestartCount:        2,
		ConsecutiveFailures: 1,
		LastHealthOKAt:      "2025-01-01T00:05:00Z",
		BackoffNextSeconds:  4,
		LastRestartAt:       "2025-01-01T00:04:00Z",
	}

	m := PodToMap(original)

	strMap := make(map[string]string)
	for k, v := range m {
		strMap[k] = fmt.Sprintf("%v", v)
	}

	got, err := MapToPod(strMap)
	if err != nil {
		t.Fatalf("MapToPod() error: %v", err)
	}

	if got.Deployment != original.Deployment {
		t.Errorf("Deployment = %q, want %q", got.Deployment, original.Deployment)
	}
	if got.PodID != original.PodID {
		t.Errorf("PodID = %q, want %q", got.PodID, original.PodID)
	}
	if got.Status != original.Status {
		t.Errorf("Status = %q, want %q", got.Status, original.Status)
	}
	if got.NodeID != original.NodeID {
		t.Errorf("NodeID = %q, want %q", got.NodeID, original.NodeID)
	}
	if got.CPURequest != original.CPURequest {
		t.Errorf("CPURequest = %d, want %d", got.CPURequest, original.CPURequest)
	}
	if got.MemRequest != original.MemRequest {
		t.Errorf("MemRequest = %d, want %d", got.MemRequest, original.MemRequest)
	}
	if got.PID != original.PID {
		t.Errorf("PID = %d, want %d", got.PID, original.PID)
	}
	if got.RestartCount != original.RestartCount {
		t.Errorf("RestartCount = %d, want %d", got.RestartCount, original.RestartCount)
	}
	if got.ConsecutiveFailures != original.ConsecutiveFailures {
		t.Errorf("ConsecutiveFailures = %d, want %d", got.ConsecutiveFailures, original.ConsecutiveFailures)
	}
	if got.BackoffNextSeconds != original.BackoffNextSeconds {
		t.Errorf("BackoffNextSeconds = %d, want %d", got.BackoffNextSeconds, original.BackoffNextSeconds)
	}
}

func TestNodeCapacityRoundTrip(t *testing.T) {
	original := &NodeCapacity{
		TotalCPU:     4000,
		TotalMem:     8192,
		AllocatedCPU: 1000,
		AllocatedMem: 2048,
	}

	m := NodeCapacityToMap(original)

	strMap := make(map[string]string)
	for k, v := range m {
		strMap[k] = fmt.Sprintf("%v", v)
	}

	got, err := MapToNodeCapacity(strMap)
	if err != nil {
		t.Fatalf("MapToNodeCapacity() error: %v", err)
	}

	if got.TotalCPU != original.TotalCPU {
		t.Errorf("TotalCPU = %d, want %d", got.TotalCPU, original.TotalCPU)
	}
	if got.TotalMem != original.TotalMem {
		t.Errorf("TotalMem = %d, want %d", got.TotalMem, original.TotalMem)
	}
	if got.AllocatedCPU != original.AllocatedCPU {
		t.Errorf("AllocatedCPU = %d, want %d", got.AllocatedCPU, original.AllocatedCPU)
	}
	if got.AllocatedMem != original.AllocatedMem {
		t.Errorf("AllocatedMem = %d, want %d", got.AllocatedMem, original.AllocatedMem)
	}
}

// ─── Edge case tests ────────────────────────────────────────────────────────

func TestMapToDeployment_MissingField(t *testing.T) {
	m := map[string]string{
		FieldDeploymentName: "test",
		// Missing all integer fields
	}
	_, err := MapToDeployment(m)
	if err == nil {
		t.Error("MapToDeployment() with missing fields should return error")
	}
}

func TestMapToPod_MissingRequired(t *testing.T) {
	m := map[string]string{
		FieldPodDeployment: "test",
		FieldPodID:         "abc",
		FieldPodStatus:     StatusPending,
		FieldPodNodeID:     "node-1",
		// Missing cpu_request and mem_request
	}
	_, err := MapToPod(m)
	if err == nil {
		t.Error("MapToPod() with missing required integer fields should return error")
	}
}

func TestMapToPod_OptionalFieldsDefault(t *testing.T) {
	m := map[string]string{
		FieldPodDeployment: "test",
		FieldPodID:         "abc",
		FieldPodStatus:     StatusPending,
		FieldPodNodeID:     "node-1",
		FieldPodCPURequest: "100",
		FieldPodMemRequest: "256",
		FieldPodCreatedAt:  "2025-01-01T00:00:00Z",
		// No optional fields
	}
	pod, err := MapToPod(m)
	if err != nil {
		t.Fatalf("MapToPod() unexpected error: %v", err)
	}
	if pod.PID != 0 {
		t.Errorf("PID should default to 0, got %d", pod.PID)
	}
	if pod.RestartCount != 0 {
		t.Errorf("RestartCount should default to 0, got %d", pod.RestartCount)
	}
	if pod.BackoffNextSeconds != 0 {
		t.Errorf("BackoffNextSeconds should default to 0, got %d", pod.BackoffNextSeconds)
	}
}
