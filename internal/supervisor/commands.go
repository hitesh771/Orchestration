package supervisor

import (
	"context"
	"encoding/json"
	"fmt"

	"mini-k8s/internal/redisclient"
	"mini-k8s/internal/schema"
)

// Command types delivered to a node's command queue.
const (
	// CommandStartPod asks the node to spawn a pod's workload process.
	CommandStartPod = "start-pod"
	// CommandStopPod asks the node to terminate a pod's process and remove it.
	CommandStopPod = "stop-pod"
)

// Command is the envelope pushed onto a node's command list.
//
// It carries only identifiers, never a copy of the spec. The agent re-reads
// the pod and deployment from Redis when it acts, so a command that sat in the
// queue while the spec changed cannot apply stale parameters.
type Command struct {
	Type       string `json:"type"`
	Deployment string `json:"deployment"`
	PodID      string `json:"pod_id"`
}

// EncodeCommand serializes a command for the queue.
func EncodeCommand(cmd Command) (string, error) {
	payload, err := json.Marshal(cmd)
	if err != nil {
		return "", fmt.Errorf("marshal command: %w", err)
	}
	return string(payload), nil
}

// DecodeCommand parses a command envelope, rejecting one that is unusable.
func DecodeCommand(payload string) (Command, error) {
	var cmd Command
	if err := json.Unmarshal([]byte(payload), &cmd); err != nil {
		return cmd, fmt.Errorf("decode command %q: %w", payload, err)
	}
	if cmd.Type == "" {
		return cmd, fmt.Errorf("command %q has no type", payload)
	}
	if cmd.Deployment == "" || cmd.PodID == "" {
		return cmd, fmt.Errorf("command %q does not identify a pod", payload)
	}
	return cmd, nil
}

// SendCommand queues a command for a node.
func SendCommand(ctx context.Context, client *redisclient.Client, nodeID string, cmd Command) error {
	payload, err := EncodeCommand(cmd)
	if err != nil {
		return err
	}
	if err := client.ListPush(ctx, schema.NodeCommandsKey(nodeID), payload); err != nil {
		return fmt.Errorf("queue %s command for %s: %w", cmd.Type, nodeID, err)
	}
	return nil
}
