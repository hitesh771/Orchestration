//go:build windows

package supervisor

import (
	"context"
	"time"

	"mini-k8s/internal/schema"
)

const (
	telemetrySamples = 3
)

// RunTelemetryCollector samples CPU telemetry for pods on this node.
//
// On Windows, /proc/[pid]/stat is unavailable, so CPU is always reported as
// 0% (a known limitation). Health-check liveness still works via TCP probes.
func (s *Supervisor) RunTelemetryCollector(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.collectTelemetry(ctx, interval)
		}
	}
}

func (s *Supervisor) collectTelemetry(ctx context.Context, interval time.Duration) {
	keys, err := s.client.ScanKeys(ctx, schema.PodKeyPattern())
	if err != nil {
		return
	}
	for _, key := range keys {
		fields, err := s.client.HashGet(ctx, key)
		if err != nil || len(fields) == 0 {
			continue
		}
		pod, err := schema.MapToPod(fields)
		if err != nil || pod.NodeID != s.nodeID {
			continue
		}
		if pod.Status != schema.StatusRunning || pod.PID <= 0 {
			continue
		}
		// CPU sampling not available on Windows — publish 0.
		cpuKey := schema.TelemetryCPUKey(pod.Deployment, pod.PodID)
		_ = s.client.ListPush(ctx, cpuKey, "0")
		_ = s.client.ListTrim(ctx, cpuKey, 0, int64(telemetrySamples-1))
		_ = s.client.Expire(ctx, cpuKey, interval*time.Duration(telemetrySamples+1))
	}
}

