// Package config loads configuration from environment variables with sane
// defaults. Every tunable behavioral parameter (intervals, thresholds,
// cooldowns) is centralized here so nothing is hard-coded elsewhere.
package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds all configuration values for mini-k8s components.
type Config struct {
	// Identity
	NodeID string // unique identity for this node
	Port   string // listen port for HTTP server

	// Redis
	RedisAddr string // host:port of the Redis instance

	// Behavioral parameters — node heartbeat & lease
	HeartbeatInterval time.Duration // how often the node refreshes its lease
	LeaseTTL          time.Duration // how long the lease key lives without refresh

	// Behavioral parameters — reconciliation
	ReconcileInterval time.Duration // how often the level-triggered sweep runs

	// Behavioral parameters — pod health
	HealthCheckInterval time.Duration // how often to probe pod liveness
	FailureThreshold    int           // consecutive failures before declaring dead

	// Behavioral parameters — restart backoff
	BackoffBase time.Duration // initial restart delay
	BackoffMax  time.Duration // maximum restart delay (cap)

	// Behavioral parameters — autoscaling
	AutoscaleInterval time.Duration // how often the autoscaler evaluates deployments

	// Behavioral parameters — scaling cooldowns
	ScaleUpCooldown   time.Duration // minimum gap between scale-up actions
	ScaleDownCooldown time.Duration // minimum gap between scale-down actions

	// Behavioral parameters — ingress
	IngressDebounce time.Duration // collapse burst of changes into one reload

	// Node resources (used by node-agent for registration)
	TotalCPU int // total CPU in millicores this node offers
	TotalMem int // total memory in MB this node offers
}

// Load reads configuration from environment variables, falling back to
// sane defaults for any value not set.
func Load() *Config {
	return &Config{
		NodeID:              envOrDefault("NODE_ID", hostname()),
		Port:                envOrDefault("PORT", "8080"),
		RedisAddr:           envOrDefault("REDIS_ADDR", "localhost:6379"),
		HeartbeatInterval:   envDuration("HEARTBEAT_INTERVAL", 3*time.Second),
		LeaseTTL:            envDuration("LEASE_TTL", 10*time.Second),
		ReconcileInterval:   envDuration("RECONCILE_INTERVAL", 10*time.Second),
		HealthCheckInterval: envDuration("HEALTH_CHECK_INTERVAL", 5*time.Second),
		FailureThreshold:    envInt("FAILURE_THRESHOLD", 3),
		BackoffBase:         envDuration("BACKOFF_BASE", 1*time.Second),
		BackoffMax:          envDuration("BACKOFF_MAX", 30*time.Second),
		AutoscaleInterval:   envDuration("AUTOSCALE_INTERVAL", 15*time.Second),
		ScaleUpCooldown:     envDuration("SCALE_UP_COOLDOWN", 30*time.Second),
		ScaleDownCooldown:   envDuration("SCALE_DOWN_COOLDOWN", 180*time.Second),
		IngressDebounce:     envDuration("INGRESS_DEBOUNCE", 2*time.Second),
		TotalCPU:            envInt("TOTAL_CPU", 1000),
		TotalMem:            envInt("TOTAL_MEM", 1024),
	}
}

// ─── Helpers ────────────────────────────────────────────────────────────────

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}
