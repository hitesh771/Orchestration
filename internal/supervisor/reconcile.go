package supervisor

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"mini-k8s/internal/logging"
	"mini-k8s/internal/schema"
)

// ReconcileReport summarizes what startup reconciliation found.
type ReconcileReport struct {
	Adopted   []string // pods whose process was verified and re-adopted
	Restarted []string // pods whose process was gone and were respawned
	Orphaned  []string // stray processes killed because no pod claimed them
}

// ReconcileOnStartup aligns this node's recorded pods with the processes
// actually running, before the agent begins accepting new work.
//
// This is what makes an agent restart safe. Redis holds the authoritative pod
// list, so the agent rebuilds its view from Redis and compares it against
// reality:
//
//   - a pod whose recorded (pid, start_time) is alive and matching is adopted,
//     never respawned, which is what prevents duplicate processes;
//   - a pod whose process is gone is respawned;
//   - a process carrying this cluster's pod marker that no live pod claims is
//     killed, which is what prevents orphans.
//
// Running this before the command consumer starts matters: consuming a queued
// start command first could spawn a second process for a pod that is already
// running.
func (s *Supervisor) ReconcileOnStartup(ctx context.Context) (*ReconcileReport, error) {
	report := &ReconcileReport{}

	podKeys, err := s.client.ScanKeys(ctx, schema.PodKeyPattern())
	if err != nil {
		return nil, fmt.Errorf("scan pods: %w", err)
	}

	// Tracks (pid -> podID) for every process a live pod legitimately owns, so
	// the orphan scan can tell a supervised process from a stray one.
	owned := make(map[int]string)

	for _, key := range podKeys {
		fields, err := s.client.HashGet(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("read pod %s: %w", key, err)
		}
		if len(fields) == 0 {
			continue // deleted between scan and read
		}
		if fields[schema.FieldPodNodeID] != s.nodeID {
			continue
		}

		pod, err := schema.MapToPod(fields)
		if err != nil {
			s.logger.Warn(ctx, "pod_hash_malformed",
				fmt.Sprintf("skipping unreadable pod %s during startup reconcile: %v", key, err),
				logging.NodeID(s.nodeID))
			continue
		}

		// An evicted pod is no longer this node's responsibility.
		if pod.Status == schema.StatusEvicted {
			continue
		}

		if IsSameProcess(pod.PID, pod.PIDStartTime) {
			owned[pod.PID] = pod.PodID
			report.Adopted = append(report.Adopted, pod.PodID)

			// The record may still say Pending if the agent died between
			// spawning and the status write; the live process is the truth.
			if pod.Status != schema.StatusRunning {
				if err := s.client.HashSet(ctx, key, map[string]interface{}{
					schema.FieldPodStatus: schema.StatusRunning,
				}); err != nil {
					s.logger.Warn(ctx, "pod_status_write_failed",
						fmt.Sprintf("adopted pid %d but could not correct status: %v", pod.PID, err),
						logging.DeploymentID(pod.Deployment), logging.PodID(pod.PodID))
				}
			}

			s.logger.Info(ctx, "pod_adopted",
				fmt.Sprintf("re-adopted running pid %d, not spawning a duplicate", pod.PID),
				logging.DeploymentID(pod.Deployment), logging.PodID(pod.PodID), logging.NodeID(s.nodeID))
			continue
		}

		// The recorded process is gone (or its identity cannot be verified, so
		// it must not be trusted). Respawn.
		s.logger.Warn(ctx, "pod_process_missing",
			fmt.Sprintf("recorded pid %d is not this pod's process, restarting it", pod.PID),
			logging.DeploymentID(pod.Deployment), logging.PodID(pod.PodID), logging.NodeID(s.nodeID))

		if err := s.Spawn(ctx, pod.Deployment, pod.PodID); err != nil {
			// A pod that cannot restart is already marked Failed by Spawn; the
			// control plane reschedules it. Keep reconciling the others.
			s.logger.Error(ctx, "pod_restart_failed",
				fmt.Sprintf("could not restart pod during startup reconcile: %v", err),
				logging.DeploymentID(pod.Deployment), logging.PodID(pod.PodID), logging.NodeID(s.nodeID))
			continue
		}
		report.Restarted = append(report.Restarted, pod.PodID)

		// Re-read to learn the new PID so the orphan scan does not kill the
		// process that was just created.
		if refreshed, err := s.client.HashGet(ctx, key); err == nil {
			if newPID, convErr := strconv.Atoi(refreshed[schema.FieldPodPID]); convErr == nil && newPID > 0 {
				owned[newPID] = pod.PodID
			}
		}
	}

	orphans, err := s.killOrphans(ctx, owned)
	if err != nil {
		// Failing the orphan scan must not block an otherwise healthy startup.
		s.logger.Warn(ctx, "orphan_scan_failed",
			fmt.Sprintf("could not scan for stray processes: %v", err),
			logging.NodeID(s.nodeID))
	}
	report.Orphaned = orphans

	s.logger.Info(ctx, "startup_reconciled",
		fmt.Sprintf("adopted %d, restarted %d, killed %d orphaned processes",
			len(report.Adopted), len(report.Restarted), len(report.Orphaned)),
		logging.NodeID(s.nodeID))
	return report, nil
}

// killOrphans terminates processes that carry this node's pod marker but are
// not claimed by any live pod.
//
// These arise when the agent dies between spawning a process and durably
// recording its PID: Redis has no pointer to the process, so nothing would
// ever clean it up. The marker in the process environment is the only
// remaining link, which is why it is written at spawn time.
func (s *Supervisor) killOrphans(ctx context.Context, owned map[int]string) ([]string, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, fmt.Errorf("read /proc: %w", err)
	}

	var killed []string
	self := os.Getpid()

	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == self {
			continue // not a process directory, or the agent itself
		}
		if _, isOwned := owned[pid]; isOwned {
			continue
		}

		podID, nodeID, ok := readPodMarker(pid)
		if !ok || nodeID != s.nodeID {
			// Not one of ours: either an unrelated process, or a pod belonging
			// to a different node that happens to share this /proc.
			continue
		}

		s.logger.Warn(ctx, "orphan_process_killed",
			fmt.Sprintf("pid %d carries pod marker %s but no live pod claims it", pid, podID),
			logging.PodID(podID), logging.NodeID(s.nodeID))

		s.terminate(pid)
		killed = append(killed, podID)
	}
	return killed, nil
}

// readPodMarker reads the pod and node identity from a process environment.
//
// It reports false when the environment cannot be read or is not yet populated.
// Both are expected: processes owned by another user are unreadable, and a
// process sampled in the instant between fork and the completion of exec reads
// back an empty environ. Reporting false is the safe direction for the orphan
// sweep, which then leaves the process alone rather than killing something it
// cannot identify; a genuine orphan is caught by the next reconcile.
func readPodMarker(pid int) (podID, nodeID string, ok bool) {
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "environ"))
	if err != nil {
		return "", "", false
	}

	// Entries in /proc/[pid]/environ are NUL-separated.
	for _, entry := range bytes.Split(data, []byte{0}) {
		text := string(entry)
		switch {
		case strings.HasPrefix(text, PodIDEnv+"="):
			podID = strings.TrimPrefix(text, PodIDEnv+"=")
		case strings.HasPrefix(text, NodeIDEnv+"="):
			nodeID = strings.TrimPrefix(text, NodeIDEnv+"=")
		}
	}
	return podID, nodeID, podID != ""
}
