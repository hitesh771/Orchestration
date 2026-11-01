//go:build windows

package supervisor

import (
	"context"

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
// On Windows, /proc scanning is not available, so orphan detection is skipped.
// Pods are still adopted if their PID is verifiably alive.
func (s *Supervisor) ReconcileOnStartup(ctx context.Context) (ReconcileReport, error) {
	var report ReconcileReport

	keys, err := s.client.ScanKeys(ctx, schema.PodKeyPattern())
	if err != nil {
		return report, err
	}

	for _, key := range keys {
		fields, err := s.client.HashGet(ctx, key)
		if err != nil || len(fields) == 0 {
			continue
		}
		pod, err := schema.MapToPod(fields)
		if err != nil {
			s.logger.Warn(ctx, "pod_hash_malformed",
				"skipping unreadable pod during startup reconcile",
				logging.NodeID(s.nodeID))
			continue
		}

		if pod.Status == schema.StatusEvicted {
			continue
		}

		if IsSameProcess(pod.PID, pod.PIDStartTime) {
			report.Adopted = append(report.Adopted, pod.PodID)
			s.logger.Info(ctx, "pod_adopted",
				"re-adopted running process",
				logging.DeploymentID(pod.Deployment), logging.PodID(pod.PodID), logging.NodeID(s.nodeID))
			continue
		}

		// Process is gone — respawn.
		s.logger.Warn(ctx, "pod_process_missing",
			"recorded pid is not alive, restarting",
			logging.DeploymentID(pod.Deployment), logging.PodID(pod.PodID), logging.NodeID(s.nodeID))

		if err := s.Spawn(ctx, pod.Deployment, pod.PodID); err != nil {
			s.logger.Error(ctx, "pod_restart_failed", err.Error(),
				logging.DeploymentID(pod.Deployment), logging.PodID(pod.PodID), logging.NodeID(s.nodeID))
			continue
		}
		report.Restarted = append(report.Restarted, pod.PodID)
	}

	// Orphan scanning is not supported on Windows (no /proc).
	return report, nil
}

