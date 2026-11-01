# Framework & Tools Reference

This document lists every tool, library, and technology used in **Mini-Kubernetes (Mini-K8s)** — a lightweight process-orchestration engine built in Go — with direct links to the exact documentation sections that apply to how each tool is used here.

---

## 1. Go (Programming Language)

> The entire codebase is written in Go. The same language real Kubernetes uses.

| Feature used | Where in this project | Learn it |
|---|---|---|
| **Modules & `go.mod`** | Dependency management (`go.mod`, `go.sum`) | [Go Modules Reference](https://go.dev/ref/mod) |
| **Build constraints / tags** | `//go:build !windows` / `//go:build windows` to select platform implementations | [Build Constraints](https://pkg.go.dev/cmd/go#hdr-Build_constraints) |
| **`context.Context`** | Every goroutine and Redis call is context-aware for cancellation and timeouts | [context package](https://pkg.go.dev/context) |
| **`os/exec`** | Spawning workload processes in the node agent (`spawn.go`) | [exec.Command](https://pkg.go.dev/os/exec#Command) |
| **`os/signal`** | Catching `SIGINT`/`SIGTERM` for graceful shutdown in all entrypoints | [signal.NotifyContext](https://pkg.go.dev/os/signal#NotifyContext) |
| **`net/http`** | REST API server, SSE dashboard, health endpoint (`internal/api/server.go`) | [net/http — ServeMux](https://pkg.go.dev/net/http#ServeMux) |
| **Server-Sent Events (SSE)** | Live dashboard streams cluster state over SSE (`internal/api/dashboard.go`) | [ResponseWriter.Flusher](https://pkg.go.dev/net/http#Flusher) |
| **Goroutines & `sync`** | Every controller loop (reconciler, health, autoscaler, heartbeat) runs as a goroutine | [Concurrency — A Tour of Go](https://go.dev/tour/concurrency/1) |
| **`encoding/json`** | REST request/response (de)serialisation and log output | [json.Marshal / Unmarshal](https://pkg.go.dev/encoding/json) |
| **`syscall` (Linux only)** | `syscall.Kill`, `syscall.SIGTERM/SIGKILL`, `SysProcAttr.Setpgid` — process group management on Linux | [syscall package](https://pkg.go.dev/syscall) |
| **`/proc` filesystem (Linux only)** | `ProcessStartTime` reads `/proc/[pid]/stat`; `readPodMarker` reads `/proc/[pid]/environ` for orphan detection | [proc(5) man page](https://man7.org/linux/man-pages/man5/proc.5.html) |
| **`net.Listen` / port probing** | `portFree()` probes TCP ports before assigning them to pods | [net.Listen](https://pkg.go.dev/net#Listen) |
| **`testing` package** | Unit and integration tests for every package | [testing package](https://pkg.go.dev/testing) |
| **Table-driven tests** | Used extensively across all `_test.go` files | [Table-Driven Tests](https://go.dev/wiki/TableDrivenTests) |

---

## 2. Redis

> Redis is the **single source of truth** for cluster state — replacing etcd in real Kubernetes.  
> Client library: [`github.com/redis/go-redis/v9`](https://github.com/redis/go-redis)

| Redis feature | How it's used | Learn it |
|---|---|---|
| **Hashes (`HSET`/`HGETALL`)** | Store deployment specs, pod specs, and node capacity as Redis hashes (`internal/schema/schema.go`) | [Redis Hashes](https://redis.io/docs/data-types/hashes/) |
| **Sets (`SADD`/`SMEMBERS`)** | Node registry — the set of all known node IDs (`schema.NodeRegistryKey`) | [Redis Sets](https://redis.io/docs/data-types/sets/) |
| **Keys with TTL (`SET`/`EXPIRE`)** | Node lease keys expire automatically; the health controller sweeps expired leases to detect dead nodes | [Redis Key Expiry](https://redis.io/docs/manual/keyspace-notifications/) |
| **Keyspace notifications** | Health controller subscribes to `__keyevent@0__:expired` to detect node lease expiry in real time (requires `--notify-keyspace-events Ex`) | [Keyspace Notifications](https://redis.io/docs/manual/keyspace-notifications/) |
| **Pub/Sub (`PUBLISH`/`SUBSCRIBE`)** | `DeploymentEventsChannel` broadcasts changes so replica controller reacts instantly; ingress controller syncs on the same channel | [Redis Pub/Sub](https://redis.io/docs/manual/pubsub/) |
| **Streams (`XADD`/`XRANGE`)** | Structured audit log — every event (pod started, autoscaled, evicted) is appended to `cluster:logstream` | [Redis Streams](https://redis.io/docs/data-types/streams/) |
| **Lists (`LPUSH`/`LTRIM`)** | Rolling window of CPU telemetry samples per pod (last 3 samples, used by autoscaler) | [Redis Lists](https://redis.io/docs/data-types/lists/) |
| **Blocking Pop (`BLPOP`)** | `NodeCommandsKey` queue — control plane enqueues `start`/`stop` commands; node agent blocks waiting for them | [BLPOP](https://redis.io/commands/blpop/) |
| **Lua scripting (`EVAL`)** | Atomic multi-key operations via `EvalScript` in `redisclient.Client` | [Redis Scripting with Lua](https://redis.io/docs/manual/programmability/eval-intro/) |
| **`SCAN` (pattern scan)** | `ScanKeys` iterates pod keys without blocking the server (`redisclient/client.go`) | [SCAN command](https://redis.io/commands/scan/) |
| **Connection pooling** | `go-redis/v9` manages a connection pool; `New()` pings on startup to fail fast | [go-redis Connection Options](https://redis.uptrace.dev/guide/go-redis.html#connecting-to-redis-server) |

---

## 3. go-redis/v9

> Go client for Redis. Used throughout `internal/redisclient/client.go`.

| Feature | Usage | Learn it |
|---|---|---|
| **`redis.NewClient`** | Creates the connection pool with address and dial timeout | [go-redis — Getting Started](https://redis.uptrace.dev/guide/go-redis.html) |
| **`client.Subscribe`** | Used by ingress controller and health controller to react to Pub/Sub events | [go-redis — PubSub](https://redis.uptrace.dev/guide/go-redis-pubsub.html) |
| **`client.XAdd` / `XRange`** | Appends and reads from Redis Streams (log stream) | [go-redis — Streams](https://redis.uptrace.dev/guide/go-redis-stream.html) |
| **Pipeline / `TxPipeline`** | Batching multiple commands in a single round-trip for atomic identity writes | [go-redis — Pipelines](https://redis.uptrace.dev/guide/go-redis-pipelines.html) |

---

## 4. Nginx

> Used as the **Ingress Router** — dynamically updated upstream config routes external traffic to pod host ports.

| Feature | Usage | Learn it |
|---|---|---|
| **`upstream` block** | Ingress controller generates an `upstream` block per deployment listing all healthy pod `127.0.0.1:<port>` entries | [nginx upstream module](https://nginx.org/en/docs/http/ngx_http_upstream_module.html) |
| **`nginx -s reload`** | Called by ingress controller after atomically swapping config files (debounced with a 2s window) | [nginx signals](https://nginx.org/en/docs/control.html) |
| **Temp-file atomic swap** | Controller writes to a temp file then `os.Rename` atomically replaces the live config — zero-downtime reload | [os.Rename](https://pkg.go.dev/os#Rename) |

> **Windows note:** Nginx is not available natively on Windows; the ingress controller logs a non-fatal error. All other components work fully.

---

## 5. Makefile

> Build, test, and run targets are defined in [`Makefile`](./Makefile).

| Target | What it does | Learn it |
|---|---|---|
| `make build` | Compiles all three binaries into `bin/` | [GNU Make — Rules](https://www.gnu.org/software/make/manual/make.html#Rule-Introduction) |
| `make test` | Runs `go test ./...` | [GNU Make — Phony Targets](https://www.gnu.org/software/make/manual/make.html#Phony-Targets) |
| `make redis` | Starts Redis with keyspace notifications enabled | [Redis server options](https://redis.io/docs/management/config/) |
| `make fmt` / `make vet` | Enforces formatting and catches common bugs | [go fmt](https://pkg.go.dev/cmd/gofmt) · [go vet](https://pkg.go.dev/cmd/vet) |

---

## 6. Shell Scripts (`scripts/`)

| Script | Purpose | Learn it |
|---|---|---|
| [`cluster.sh`](./scripts/cluster.sh) | Bootstraps a multi-node Incus cluster for integration testing | [Incus — Getting Started](https://linuxcontainers.org/incus/docs/main/tutorial/first_steps/) |
| [`chaos.sh`](./scripts/chaos.sh) | Injects failures: kills nodes, spams requests, drops Redis connections | [Chaos Engineering Principles](https://principlesofchaos.org/) |
| [`demo.sh`](./scripts/demo.sh) | End-to-end demo: deploy, scale, kill a node, watch healing | [Bash scripting guide](https://www.gnu.org/software/bash/manual/bash.html) |

---

## 7. Incus (Linux Container Runtime)

> Worker nodes run as **Incus containers** (LXD successor) on a `incusbr0` bridge network.  
> The node agent runs inside each container as a native process — no Docker overhead.

| Feature | Usage | Learn it |
|---|---|---|
| **`incus launch`** | Creates worker node VMs/containers | [incus launch](https://linuxcontainers.org/incus/docs/main/reference/api_extensions/#instance-create) |
| **`incusbr0` bridge** | All nodes share a virtual bridge; pods bind to the node's IP on ephemeral ports 20000–60000 | [Incus networking](https://linuxcontainers.org/incus/docs/main/explanation/networks/) |
| **No container-per-pod** | Unlike real K8s, pods are plain OS processes — process groups (`Setpgid`) contain child trees | [Linux process groups](https://man7.org/linux/man-pages/man2/setpgid.2.html) |

---

## 8. Architecture Patterns Used

| Pattern | Where | Learn it |
|---|---|---|
| **Control loop / Reconciler** | `ReplicaController`, `Autoscaler`, `HealthMonitor` — each runs `desired vs actual → act` loops | [Kubernetes Controllers](https://kubernetes.io/docs/concepts/architecture/controller/) |
| **Leader-less design** | No leader election; Redis TTL leases are the coordination primitive | [Lease-based coordination](https://redis.io/docs/manual/patterns/distributed-locks/) |
| **Reactive (event-driven)** | Pub/Sub replaces polling; controllers sleep until Redis publishes a change | [Redis Pub/Sub patterns](https://redis.io/docs/manual/pubsub/) |
| **Dual-mode health check** | Keyspace-notification fast path + periodic TTL sweep fallback (`health.go`) | [Dual-mode reliability](https://en.wikipedia.org/wiki/Defense_in_depth_(computing)) |
| **Exponential backoff** | CrashLoopBackOff — restart delay doubles on each failure, capped at 30s (`health.go`) | [Exponential Backoff](https://en.wikipedia.org/wiki/Exponential_backoff) |
| **PID identity (pid, start_time)** | Prevents PID-reuse from supervising the wrong process after a restart (`identity.go`, `process.go`) | [Linux PID reuse](https://man7.org/linux/man-pages/man5/proc.5.html) |
| **Atomic config swap** | Nginx config is written to a temp file then renamed — guarantees the router never reads a partial file | [Atomic file replace pattern](https://pkg.go.dev/os#Rename) |
| **Capacity bin-packing** | Scheduler fits pods using a least-remaining-capacity heuristic (`scheduler.go`) | [Bin packing](https://en.wikipedia.org/wiki/Bin_packing_problem) |

---

## 9. go.uber.org/atomic

> Used for lock-free counters in hot paths where a mutex would be overkill.

| Usage | Learn it |
|---|---|
| Atomic int64/bool values shared between goroutines without a mutex | [uber-go/atomic README](https://github.com/uber-go/atomic) |

---

## 10. cespare/xxhash

> Fast non-cryptographic hash used internally by go-redis for consistent hashing.

| Usage | Learn it |
|---|---|
| Indirect dependency via go-redis — used for cluster slot hashing | [xxhash README](https://github.com/cespare/xxhash) |

---

## Quick Reference: Package → Tool Map

```
internal/
├── schema/        → Go structs + Redis key naming conventions
├── redisclient/   → go-redis/v9 (Hashes, Sets, Lists, Streams, Pub/Sub, Lua)
├── logging/       → Go encoding/json + Redis Streams (XADD)
├── config/        → Go os.Getenv + time.Duration parsing
├── api/           → Go net/http (REST + SSE dashboard)
├── scheduler/     → Redis Hashes (node capacity) + bin-packing algorithm
├── controller/    → Redis Pub/Sub + control loops (replica, autoscale, health)
├── ingress/       → Redis Pub/Sub + Nginx upstream config generation
├── node/          → Redis Keys with TTL (lease heartbeat)
├── supervisor/    → os/exec + syscall (Linux) / tasklist (Windows)
│   ├── *_linux    → syscall.Kill, Setpgid, /proc/[pid]/stat
│   └── *_windows  → os.Process.Kill, tasklist, 0%-CPU stub
└── events/        → Redis Streams event types
```
