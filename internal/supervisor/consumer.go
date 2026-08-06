package supervisor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"mini-k8s/internal/capacity"
	"mini-k8s/internal/logging"
	"mini-k8s/internal/schema"

	"github.com/redis/go-redis/v9"
)

// commandPopTimeout bounds each blocking read of the command queue.
//
// The wait is bounded rather than indefinite so the loop returns to the top
// periodically and notices context cancellation even when no commands arrive.
const commandPopTimeout = 2 * time.Second

// RunCommandConsumer processes this node's command queue until ctx is done.
//
// ReconcileOnStartup must have run first: consuming a queued start command
// before reconciling could spawn a second process for a pod that is already
// running.
func (s *Supervisor) RunCommandConsumer(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		payload, err := s.client.BlockingPop(ctx, schema.NodeCommandsKey(s.nodeID), commandPopTimeout)
		if err != nil {
			// An empty queue is the normal case, not a failure.
			if errors.Is(err, redis.Nil) {
				continue
			}
			if ctx.Err() != nil {
				return // shutting down
			}
			// A transient Redis error must not kill the consumer, or the node
			// would stop accepting work while still holding its lease.
			s.logger.Warn(ctx, "command_pop_failed",
				fmt.Sprintf("could not read command queue, retrying: %v", err),
				logging.NodeID(s.nodeID))
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		cmd, err := DecodeCommand(payload)
		if err != nil {
			// A malformed envelope is dropped: retrying it would block the
			// queue forever behind a command that can never succeed.
			s.logger.Error(ctx, "command_malformed", err.Error(), logging.NodeID(s.nodeID))
			continue
		}

		s.handleCommand(ctx, cmd)
	}
}

// handleCommand dispatches a single command.
func (s *Supervisor) handleCommand(ctx context.Context, cmd Command) {
	switch cmd.Type {
	case CommandStartPod:
		if err := s.Spawn(ctx, cmd.Deployment, cmd.PodID); err != nil {
			// Spawn already marked the pod Failed and logged the cause; the
			// control plane decides whether to reschedule.
			return
		}

	case CommandStopPod:
		if err := s.StopAndRemove(ctx, cmd.Deployment, cmd.PodID); err != nil {
			s.logger.Error(ctx, "pod_stop_failed", err.Error(),
				logging.DeploymentID(cmd.Deployment), logging.PodID(cmd.PodID), logging.NodeID(s.nodeID))
		}

	default:
		s.logger.Warn(ctx, "command_unknown",
			fmt.Sprintf("ignoring unrecognized command type %q", cmd.Type),
			logging.NodeID(s.nodeID))
	}
}

// StopAndRemove terminates a pod's process, releases its capacity, and deletes
// its record.
//
// The order matters. Capacity is released before the pod hash is deleted, so a
// crash in between leaves the pod present but over-counted rather than absent
// and under-counted: the agent's startup allocation rebuild recomputes from the
// pods that actually exist, which corrects the drift either way.
func (s *Supervisor) StopAndRemove(ctx context.Context, deployment, podID string) error {
	podKey := schema.PodKey(deployment, podID)

	fields, err := s.client.HashGet(ctx, podKey)
	if err != nil {
		return fmt.Errorf("read pod %s: %w", podID, err)
	}
	if len(fields) == 0 {
		return nil // already removed
	}
	pod, err := schema.MapToPod(fields)
	if err != nil {
		// Unreadable, but it must still be removed or it would occupy capacity
		// forever with no way to act on it.
		if delErr := s.client.DeleteKey(ctx, podKey); delErr != nil {
			return fmt.Errorf("delete malformed pod %s: %w", podID, delErr)
		}
		return nil
	}

	if err := s.Stop(ctx, deployment, podID); err != nil {
		return err
	}

	if schema.ConsumesCapacity(pod.Status) && pod.NodeID != "" {
		if err := s.releaseCapacity(ctx, pod); err != nil {
			return err
		}
	}

	if err := s.client.DeleteKey(ctx, podKey); err != nil {
		return fmt.Errorf("delete pod %s: %w", podID, err)
	}

	s.logger.Info(ctx, "pod_removed",
		fmt.Sprintf("released %dm CPU and %dMB back to the node",
			pod.CPURequest, pod.MemRequest),
		logging.DeploymentID(deployment), logging.PodID(podID), logging.NodeID(s.nodeID))
	return nil
}

// releaseCapacity returns a pod's reservation to its node.
func (s *Supervisor) releaseCapacity(ctx context.Context, pod *schema.PodSpec) error {
	return capacity.Release(ctx, s.client, pod.NodeID, pod.CPURequest, pod.MemRequest)
}
