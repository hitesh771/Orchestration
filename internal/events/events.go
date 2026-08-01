// Package events carries deployment change notifications over Redis Pub/Sub.
//
// Events are a latency optimization only. Every controller that consumes them
// also runs a periodic sweep that recomputes state from Redis, so a dropped
// event delays convergence but never breaks it.
package events

import (
	"context"
	"encoding/json"
	"fmt"

	"mini-k8s/internal/redisclient"
	"mini-k8s/internal/schema"
)

// Event types published on the deployment channel.
const (
	// EventUpdate signals that a deployment's spec or desired replica count
	// changed and reconciliation should run.
	EventUpdate = "UPDATE"
	// EventDeleted signals that a deployment was removed.
	EventDeleted = "DELETE"
)

// DeploymentEvent is the payload published on the deployment events channel.
type DeploymentEvent struct {
	Event      string `json:"event"`
	Deployment string `json:"deployment"`
	Desired    int    `json:"desired"`
}

// Publish broadcasts a deployment event.
//
// Callers treat a publish failure as non-fatal: the state write that preceded
// it is already durable in Redis, so the periodic sweep will still converge.
func Publish(ctx context.Context, client *redisclient.Client, ev DeploymentEvent) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal deployment event: %w", err)
	}
	if err := client.Publish(ctx, schema.DeploymentEventsChannel(), string(payload)); err != nil {
		return fmt.Errorf("publish deployment event: %w", err)
	}
	return nil
}

// Decode parses a raw Pub/Sub message payload into a DeploymentEvent.
func Decode(payload string) (DeploymentEvent, error) {
	var ev DeploymentEvent
	if err := json.Unmarshal([]byte(payload), &ev); err != nil {
		return ev, fmt.Errorf("decode deployment event %q: %w", payload, err)
	}
	if ev.Deployment == "" {
		return ev, fmt.Errorf("deployment event %q names no deployment", payload)
	}
	return ev, nil
}
