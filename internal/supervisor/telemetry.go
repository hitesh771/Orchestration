package supervisor

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"mini-k8s/internal/logging"
	"mini-k8s/internal/schema"
)

const (
	// telemetrySamples is how many samples each pod's list retains. The
	// autoscaler averages the last three, so keeping more would age into the
	// average nothing reads, and keeping fewer would let one spike decide a
	// scaling action on its own.
	telemetrySamples = 3

	// utimeField and stimeField are the 1-based positions of user and system
	// CPU time in /proc/[pid]/stat, counted after the comm field.
	utimeField = 14
	stimeField = 15
)

// sampleTTLFactor sets how many collection intervals a telemetry list outlives
// its last write.
//
// Without an expiry, a pod's samples survive the pod. The autoscaler averages
// across every pod's list, so a departed pod's last readings keep voting: after
// a few rescheduling rounds the average is dominated by pods that no longer
// exist, and a genuinely loaded deployment reads as idle. Refreshing the TTL on
// every write means a list outlives its pod by a bounded margin and then
// disappears on its own, which no deletion path has to remember to do.
//
// The margin has to tolerate a missed collection or two, or a live pod's own
// samples would expire between writes and it would drop out of the average it
// belongs in.
const sampleTTLFactor = 4

// cpuSample is one pod's cumulative CPU time at a point in time.
type cpuSample struct {
	ticks uint64
	at    time.Time
}

// RunTelemetryCollector samples this node's pods and publishes CPU utilization.
//
// Utilization is expressed as a percentage of the pod's own CPU request rather
// than of the machine, because that is what the autoscaler's target compares
// against: a deployment asking for 100m and using 90m is at 90% of what it
// declared, regardless of how large the node is.
func (s *Supervisor) RunTelemetryCollector(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}

	// The first sample of a pod only establishes a baseline: CPU time in /proc
	// is cumulative, so a rate needs two readings. Publishing the raw total as
	// if it were a rate would report a long-running pod as permanently pegged.
	last := make(map[string]cpuSample)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.collectOnce(ctx, last, interval); err != nil && ctx.Err() == nil {
				s.logger.Warn(ctx, "telemetry_collection_failed", err.Error(),
					logging.NodeID(s.nodeID))
			}
		}
	}
}

// collectOnce publishes one sample per running pod on this node.
func (s *Supervisor) collectOnce(ctx context.Context, last map[string]cpuSample, interval time.Duration) error {
	keys, err := s.client.ScanKeys(ctx, schema.PodKeyPattern())
	if err != nil {
		return fmt.Errorf("scan pods: %w", err)
	}

	seen := make(map[string]bool, len(keys))
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
		// Verify identity before trusting the PID: sampling a recycled PID
		// would attribute an unrelated process's CPU time to this pod.
		if !IsSameProcess(pod.PID, pod.PIDStartTime) {
			continue
		}

		ticks, err := processCPUTicks(pod.PID)
		if err != nil {
			continue // exited between the identity check and the read
		}

		id := pod.Deployment + "/" + pod.PodID
		seen[id] = true
		now := time.Now()
		prev, ok := last[id]
		last[id] = cpuSample{ticks: ticks, at: now}
		if !ok {
			continue // baseline only
		}

		percent := cpuPercentOfRequest(prev, cpuSample{ticks: ticks, at: now}, pod.CPURequest)
		if err := s.publishSample(ctx, pod, percent, interval); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			s.logger.Warn(ctx, "telemetry_publish_failed", err.Error(),
				logging.DeploymentID(pod.Deployment), logging.PodID(pod.PodID))
		}
	}

	// Drop baselines for pods that are gone, or this map would grow for the
	// life of the agent.
	for id := range last {
		if !seen[id] {
			delete(last, id)
		}
	}
	return nil
}

// publishSample appends a sample, trims the list, and refreshes its expiry.
func (s *Supervisor) publishSample(ctx context.Context, pod *schema.PodSpec, percent int, interval time.Duration) error {
	key := schema.TelemetryCPUKey(pod.Deployment, pod.PodID)
	if err := s.client.ListPush(ctx, key, strconv.Itoa(percent)); err != nil {
		return fmt.Errorf("push sample: %w", err)
	}
	// Trim on every write rather than periodically: an untrimmed list has no
	// bound, and the autoscaler only ever reads the tail.
	if err := s.client.ListTrim(ctx, key, -telemetrySamples, -1); err != nil {
		return fmt.Errorf("trim samples: %w", err)
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if err := s.client.Expire(ctx, key, interval*sampleTTLFactor); err != nil {
		return fmt.Errorf("set sample expiry: %w", err)
	}
	return nil
}

// cpuPercentOfRequest converts two cumulative readings into a percentage of the
// pod's CPU request.
//
// A request of zero means the pod declared no CPU, so there is nothing to be a
// percentage of; reporting zero is the honest answer and avoids dividing by it.
func cpuPercentOfRequest(prev, cur cpuSample, cpuRequestMillicores int) int {
	elapsed := cur.at.Sub(prev.at).Seconds()
	if elapsed <= 0 || cpuRequestMillicores <= 0 || cur.ticks < prev.ticks {
		return 0
	}

	// Ticks are USER_HZ units; 100 is the value on every platform this runs on.
	const userHZ = 100.0
	cpuSeconds := float64(cur.ticks-prev.ticks) / userHZ
	usedMillicores := (cpuSeconds / elapsed) * 1000.0
	percent := int((usedMillicores / float64(cpuRequestMillicores)) * 100.0)
	if percent < 0 {
		return 0
	}
	return percent
}

// processCPUTicks reads a process's cumulative user+system CPU time.
//
// The fields cannot be located by splitting on whitespace, because comm may
// itself contain spaces and parentheses; everything is counted after the final
// ')' instead.
func processCPUTicks(pid int) (uint64, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, fmt.Errorf("read stat for %d: %w", pid, err)
	}

	close := strings.LastIndexByte(string(data), ')')
	if close < 0 || close+2 >= len(data) {
		return 0, fmt.Errorf("malformed stat for %d", pid)
	}
	fields := strings.Fields(string(data[close+2:]))

	// Fields after comm start at position 3, so field N sits at index N-3.
	uIdx, sIdx := utimeField-3, stimeField-3
	if sIdx >= len(fields) {
		return 0, fmt.Errorf("stat for %d has too few fields", pid)
	}
	utime, err := strconv.ParseUint(fields[uIdx], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse utime for %d: %w", pid, err)
	}
	stime, err := strconv.ParseUint(fields[sIdx], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse stime for %d: %w", pid, err)
	}
	return utime + stime, nil
}
