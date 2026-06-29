# Phase 1 — Foundation: State Client, Shared Schema, and Logging

Build the skeleton, shared data contracts, and observability that every later phase depends on. Nothing orchestrates yet, but every later component speaks the same language and is debuggable from the first line.

## User Review Required

> [!IMPORTANT]
> **Language choice: Go** — The manifest describes a systems project with Redis, process supervision, Lua scripts, and cgroup interaction. Go is the natural fit (same language as real Kubernetes). Please confirm or suggest an alternative.

> [!IMPORTANT]
> **Redis client library: `github.com/redis/go-redis/v9`** — The standard Go Redis client with full support for Streams, Pub/Sub, Lua scripting, and connection pooling. Please confirm.

## Proposed Changes

### Repository Scaffold

#### [NEW] `go.mod` / `go.sum`
Go module `mini-k8s` with dependency on `go-redis/v9`.

#### [NEW] Directory structure
```
mini-k8s/
├── cmd/
│   ├── api-server/main.go
│   ├── node-agent/main.go
│   └── dashboard/main.go
├── internal/
│   ├── schema/schema.go
│   ├── redisclient/client.go
│   ├── logging/logger.go
│   └── config/config.go
```

---

### Schema Module — `internal/schema`

#### [NEW] [schema.go](file:///home/hitesh/mini_kubernetes/internal/schema/schema.go)

Single source of truth for **every** Redis key pattern and field name. No raw key strings anywhere else.

**Contents:**
- **Key builder functions:** `LogStreamKey()`, `TestKey()`, `NodeRegistryKey()`, `NodeCapacityKey(nodeID)`, `NodeStatusKey(nodeID)`, `DeploymentKey(name)`, `PodKey(deployment, podID)`, `NodeCommandsKey(nodeID)`, `DeploymentEventsChannel()`, `TelemetryCPUKey(deployment, podID)`
- **Field name constants:** groups of `const` for each hash type:
  - `Deployment` fields: `Name`, `MinReplicas`, `MaxReplicas`, `CPURequest`, `MemRequest`, `DesiredReplicas`, `CreatedAt`
  - `Pod` fields: `Deployment`, `PodID`, `Status`, `NodeID`, `CPURequest`, `MemRequest`, `CreatedAt`, `PID`, `StartedAt`, `RestartCount`, `ConsecutiveFailures`, `LastHealthOKAt`, `BackoffNextSeconds`, `LastRestartAt`
  - `NodeCapacity` fields: `TotalCPU`, `TotalMem`, `AllocatedCPU`, `AllocatedMem`
- **Go structs:** `DeploymentSpec`, `PodSpec`, `NodeCapacity` — the logical shapes, used for (de)serialization
- **Status constants:** `StatusPending`, `StatusRunning`, `StatusFailed`, `StatusEvicted`, `StatusCrashLoopBackOff`

> [!NOTE]
> We define keys/fields for ALL phases now (they're just constants/structs — zero cost), so the schema module truly is the "one place" and later phases never need to modify it.

---

### Redis Client — `internal/redisclient`

#### [NEW] [client.go](file:///home/hitesh/mini_kubernetes/internal/redisclient/client.go)

Thin wrapper owning the connection pool. **Every** Redis interaction goes through this module.

**Exported API:**
- `New(addr string) (*Client, error)` — create pool, ping to verify reachability, fail fast if unreachable
- `Ping(ctx) error` — reachability check
- `Close() error`
- **Hash helpers:** `HashSet(ctx, key, fields map[string]string)`, `HashGet(ctx, key string) (map[string]string, error)`, `HashGetField(ctx, key, field string) (string, error)`
- **Set helpers:** `SetAdd(ctx, key, member)`, `SetMembers(ctx, key) ([]string, error)`, `SetRemove(ctx, key, member)`
- **List helpers:** `ListPush(ctx, key, value)`, `ListTrim(ctx, key, start, stop)`, `ListRange(ctx, key, start, stop)`, `BlockingPop(ctx, key, timeout) (string, error)`
- **Key helpers:** `SetKey(ctx, key, value, ttl)`, `GetKey(ctx, key) (string, error)`, `DeleteKey(ctx, key)`, `Exists(ctx, key) (bool, error)`, `Expire(ctx, key, ttl)`
- **Stream helpers:** `StreamAppend(ctx, key string, fields map[string]string) error`, `StreamRead(ctx, key, lastID string, count int64) ([]StreamEntry, error)`
- **Pub/Sub helpers:** `Publish(ctx, channel, message)`, `Subscribe(ctx, channel) (*Subscription, error)`
- **Lua scripting:** `EvalScript(ctx, script string, keys []string, args ...interface{}) (interface{}, error)`

Centralizes timeouts (5s default), retries (none for now — fail fast), and context propagation.

---

### Logging — `internal/logging`

#### [NEW] [logger.go](file:///home/hitesh/mini_kubernetes/internal/logging/logger.go)

Structured logger that emits each event **both** to stdout (JSON) and to the Redis stream `cluster:logstream`.

**Exported API:**
- `New(component string, client *redisclient.Client) *Logger`
- `Info(ctx, eventType, msg string, ids ...KeyValue)` 
- `Warn(ctx, eventType, msg string, ids ...KeyValue)`
- `Error(ctx, eventType, msg string, ids ...KeyValue)`
- `type KeyValue struct { Key, Value string }` — for optional deployment/pod/node IDs

**Each log entry carries:** `timestamp` (RFC3339Nano), `component`, `level`, `event_type`, optional `deployment_id`/`pod_id`/`node_id`, and `message`.

Stdout output is JSON, one line per event. Redis stream entry uses the same fields.

If the Redis write fails (e.g., connection lost), the logger logs to stdout only and does **not** crash — logging must never block the system.

---

### Config — `internal/config`

#### [NEW] [config.go](file:///home/hitesh/mini_kubernetes/internal/config/config.go)

Loads configuration from environment with sane defaults.

**Fields:**
| Env Var | Default | Description |
|---------|---------|-------------|
| `NODE_ID` | hostname | Identity of this node |
| `PORT` | `8080` | Listen port |
| `REDIS_ADDR` | `localhost:6379` | Redis address |
| `HEARTBEAT_INTERVAL` | `3s` | Node heartbeat interval |
| `LEASE_TTL` | `10s` | Node lease TTL |
| `HEALTH_CHECK_INTERVAL` | `5s` | Pod health probe interval |
| `FAILURE_THRESHOLD` | `3` | Consecutive failures before restart |
| `BACKOFF_BASE` | `1s` | Restart backoff base |
| `BACKOFF_MAX` | `30s` | Restart backoff cap |
| `SCALE_UP_COOLDOWN` | `30s` | Scaling cooldown (up) |
| `SCALE_DOWN_COOLDOWN` | `60s` | Scaling cooldown (down) |
| `INGRESS_DEBOUNCE` | `2s` | Nginx reload debounce window |

All future phases just read from this config — no hard-coded values.

---

### Entrypoint Stubs

#### [NEW] [cmd/api-server/main.go](file:///home/hitesh/mini_kubernetes/cmd/api-server/main.go)

- Load config
- Connect to Redis via `redisclient.New()` — fail fast if unreachable
- Create logger
- Emit `component_started` log event (component=`api-server`)
- Block (select{}) — idle until Phase 3 adds the HTTP server

#### [NEW] [cmd/node-agent/main.go](file:///home/hitesh/mini_kubernetes/cmd/node-agent/main.go)

- Same pattern: load config, connect, log `component_started` (component=`node-agent`), idle

#### [NEW] [cmd/dashboard/main.go](file:///home/hitesh/mini_kubernetes/cmd/dashboard/main.go)

- Same pattern: load config, connect, log `component_started` (component=`dashboard`), idle

---

## Verification Plan

### Automated Tests
1. `go build ./...` — all three binaries compile
2. `go vet ./...` — no issues
3. Unit test for schema key builders (confirm exact key strings)
4. Unit test for struct ↔ Redis hash round-trip serialization

### Manual Verification (requires Redis running)
```bash
# Start Redis
redis-server &

# Run each binary, confirm component_started appears in both stdout and Redis stream
go run ./cmd/api-server/
go run ./cmd/node-agent/
go run ./cmd/dashboard/

# Verify log stream
redis-cli XRANGE cluster:logstream - + COUNT 10

# Smoke test: set-then-get through redisclient
# (A small test program or the api-server stub can do this)
redis-cli GET test:key
```
