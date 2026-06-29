// Package logging provides a structured logger that emits each event both
// to stdout (JSON, one line per event) and to a Redis stream (cluster:logstream).
// This exists from Phase 1 so chaos debugging works for every later phase.
//
// If the Redis write fails (e.g., connection lost), the logger emits to stdout
// only and does NOT crash — logging must never block the system.
package logging

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"mini-k8s/internal/redisclient"
	"mini-k8s/internal/schema"
)

// KeyValue holds an optional identifier to attach to a log entry.
type KeyValue struct {
	Key   string
	Value string
}

// Logger is a structured logger bound to a specific component.
type Logger struct {
	component string
	client    *redisclient.Client
}

// LogEntry is the structured log format written to stdout and Redis.
type LogEntry struct {
	Timestamp    string `json:"timestamp"`
	Component    string `json:"component"`
	Level        string `json:"level"`
	EventType    string `json:"event_type"`
	DeploymentID string `json:"deployment_id,omitempty"`
	PodID        string `json:"pod_id,omitempty"`
	NodeID       string `json:"node_id,omitempty"`
	Message      string `json:"message"`
}

// New creates a logger for the given component, backed by the Redis client.
func New(component string, client *redisclient.Client) *Logger {
	return &Logger{
		component: component,
		client:    client,
	}
}

// Info logs an informational event.
func (l *Logger) Info(ctx context.Context, eventType, msg string, ids ...KeyValue) {
	l.log(ctx, "INFO", eventType, msg, ids)
}

// Warn logs a warning event.
func (l *Logger) Warn(ctx context.Context, eventType, msg string, ids ...KeyValue) {
	l.log(ctx, "WARN", eventType, msg, ids)
}

// Error logs an error event.
func (l *Logger) Error(ctx context.Context, eventType, msg string, ids ...KeyValue) {
	l.log(ctx, "ERROR", eventType, msg, ids)
}

// log writes a structured entry to both stdout and Redis stream.
func (l *Logger) log(ctx context.Context, level, eventType, msg string, ids []KeyValue) {
	entry := LogEntry{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Component: l.component,
		Level:     level,
		EventType: eventType,
		Message:   msg,
	}

	// Apply optional IDs.
	for _, kv := range ids {
		switch kv.Key {
		case "deployment_id":
			entry.DeploymentID = kv.Value
		case "pod_id":
			entry.PodID = kv.Value
		case "node_id":
			entry.NodeID = kv.Value
		}
	}

	// Write to stdout (JSON, one line).
	data, err := json.Marshal(entry)
	if err != nil {
		fmt.Fprintf(os.Stderr, "logging: marshal error: %v\n", err)
		return
	}
	fmt.Fprintln(os.Stdout, string(data))

	// Write to Redis stream (best-effort — never crash on failure).
	if l.client != nil {
		fields := map[string]interface{}{
			"timestamp":  entry.Timestamp,
			"component":  entry.Component,
			"level":      entry.Level,
			"event_type": entry.EventType,
			"message":    entry.Message,
		}
		if entry.DeploymentID != "" {
			fields["deployment_id"] = entry.DeploymentID
		}
		if entry.PodID != "" {
			fields["pod_id"] = entry.PodID
		}
		if entry.NodeID != "" {
			fields["node_id"] = entry.NodeID
		}

		streamErr := l.client.StreamAppend(ctx, schema.LogStreamKey(), fields)
		if streamErr != nil {
			// Log the Redis failure to stderr only — never crash.
			fmt.Fprintf(os.Stderr, "logging: redis stream write failed: %v\n", streamErr)
		}
	}
}

// ─── Convenience constructors for KeyValue IDs ──────────────────────────────

// DeploymentID creates a KeyValue for a deployment identifier.
func DeploymentID(id string) KeyValue {
	return KeyValue{Key: "deployment_id", Value: id}
}

// PodID creates a KeyValue for a pod identifier.
func PodID(id string) KeyValue {
	return KeyValue{Key: "pod_id", Value: id}
}

// NodeID creates a KeyValue for a node identifier.
func NodeID(id string) KeyValue {
	return KeyValue{Key: "node_id", Value: id}
}
