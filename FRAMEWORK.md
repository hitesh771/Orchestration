# Framework & Tools Reference

This document lists every tool, library, and technology used in **Mini-Kubernetes (Mini-K8s)** — a lightweight process-orchestration engine built in Go — with direct links to the exact documentation sections that apply to how each tool is used here. It also contains the **complete, ordered operational guide** to start, verify, deploy to, and shut down the cluster.

---

## Part 1: Tools & Technology Reference

### 1. Go (Programming Language)

> The entire codebase is written in Go. The same language real Kubernetes uses.

| Feature used | Where in this project | Learn it |
|---|---|---|
| **Modules & `go.mod`** | Dependency management (`go.mod`, `go.sum`) | [Go Modules Reference](https://go.dev/ref/mod) |
| **Build constraints / tags** | `//go:build !windows` / `//go:build windows` to select platform implementations | [Build Constraints](https://pkg.go.dev/cmd/go#hdr-Build_constraints) |
| **`context.Context`** | Every goroutine and Redis call is context-aware for cancellation and timeouts | [context package](https://pkg.go.dev/context) |
| **`os/exec`** | Spawning workload processes in the node agent (`spawn.go` / `spawn_windows.go`) | [exec.Command](https://pkg.go.dev/os/exec#Command) |
| **`os/signal`** | Catching `SIGINT`/`SIGTERM` for graceful shutdown in all entrypoints | [signal.NotifyContext](https://pkg.go.dev/os/signal#NotifyContext) |
| **`net/http`** | REST API server, SSE dashboard, health endpoint (`internal/api/server.go`) | [net/http — ServeMux](https://pkg.go.dev/net/http#ServeMux) |
| **Server-Sent Events (SSE)** | Live dashboard streams cluster state over SSE (`internal/api/dashboard.go`) | [ResponseWriter.Flusher](https://pkg.go.dev/net/http#Flusher) |
| **Goroutines & `sync`** | Every controller loop (reconciler, health, autoscaler, heartbeat) runs as a goroutine | [Concurrency — A Tour of Go](https://go.dev/tour/concurrency/1) |
| **`encoding/json`** | REST request/response (de)serialisation and log output | [json.Marshal / Unmarshal](https://pkg.go.dev/encoding/json) |
| **`syscall` (Linux only)** | `syscall.Kill`, `syscall.SIGTERM/SIGKILL`, `SysProcAttr.Setpgid` — process group management on Linux | [syscall package](https://man7.org/linux/man-pages/man2/kill.2.html) |
| **`/proc` filesystem (Linux only)** | `ProcessStartTime` reads `/proc/[pid]/stat`; `readPodMarker` reads `/proc/[pid]/environ` for orphan detection | [proc(5) man page](https://man7.org/linux/man-pages/man5/proc.5.html) |
| **`tasklist` (Windows fallback)** | Cross-platform fallback on Windows in `process_windows.go` to verify PID liveness | [tasklist command](https://learn.microsoft.com/en-us/windows-server/administration/windows-commands/tasklist) |
| **`net.Listen` / port probing** | `portFree()` probes TCP ports before assigning them to pods | [net.Listen](https://pkg.go.dev/net#Listen) |
| **`testing` package** | Unit and integration tests for every package | [testing package](https://pkg.go.dev/testing) |
| **Table-driven tests** | Used extensively across all `_test.go` files | [Table-Driven Tests](https://go.dev/wiki/TableDrivenTests) |

---

### 2. Redis

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

### 3. go-redis/v9

> Go client for Redis. Used throughout `internal/redisclient/client.go`.

| Feature | Usage | Learn it |
|---|---|---|
| **`redis.NewClient`** | Creates the connection pool with address and dial timeout | [go-redis — Getting Started](https://redis.uptrace.dev/guide/go-redis.html) |
| **`client.Subscribe`** | Used by ingress controller and health controller to react to Pub/Sub events | [go-redis — PubSub](https://redis.uptrace.dev/guide/go-redis-pubsub.html) |
| **`client.XAdd` / `XRange`** | Appends and reads from Redis Streams (log stream) | [go-redis — Streams](https://redis.uptrace.dev/guide/go-redis-stream.html) |
| **Pipeline / `TxPipeline`** | Batching multiple commands in a single round-trip for atomic identity writes | [go-redis — Pipelines](https://redis.uptrace.dev/guide/go-redis-pipelines.html) |

---

### 4. Nginx

> Used as the **Ingress Router** — dynamically updated upstream config routes external traffic to pod host ports.

| Feature | Usage | Learn it |
|---|---|---|
| **`upstream` block** | Ingress controller generates an `upstream` block per deployment listing all healthy pod `127.0.0.1:<port>` entries | [nginx upstream module](https://nginx.org/en/docs/http/ngx_http_upstream_module.html) |
| **`nginx -s reload`** | Called by ingress controller after atomically swapping config files (debounced with a 2s window) | [nginx signals](https://nginx.org/en/docs/control.html) |
| **Temp-file atomic swap** | Controller writes to a temp file (`os.TempDir()`), then `os.Rename` atomically replaces the live config — zero-downtime reload | [os.Rename](https://pkg.go.dev/os#Rename) |

---

### 5. Makefile & Shell Scripts

| Target / Script | Purpose | Learn it |
|---|---|---|
| [`Makefile`](./Makefile) | `build`, `test`, `vet`, `fmt`, and `redis` targets | [GNU Make Manual](https://www.gnu.org/software/make/manual/make.html) |
| [`scripts/cluster.sh`](./scripts/cluster.sh) | Bootstraps a multi-node Incus cluster for Linux integration testing | [Incus Documentation](https://linuxcontainers.org/incus/docs/main/) |
| [`scripts/chaos.sh`](./scripts/chaos.sh) | Injects node crashes, latency, and pod failure chaos | [Principles of Chaos](https://principlesofchaos.org/) |
| [`scripts/demo.sh`](./scripts/demo.sh) | Automated end-to-end demo script | [Bash Guide](https://www.gnu.org/software/bash/manual/) |

---

### 6. Architecture Patterns Used

| Pattern | Where | Learn it |
|---|---|---|
| **Autonomous Control loop** | `ReplicaController`, `Autoscaler`, `HealthMonitor` — constantly reconciling `desired` vs `actual` | [Kubernetes Controllers](https://kubernetes.io/docs/concepts/architecture/controller/) |
| **Leader-less design** | No central leader election; Redis TTL leases are the distributed lock / coordination primitive | [Lease Coordination](https://redis.io/docs/manual/patterns/distributed-locks/) |
| **Reactive event-driven** | Pub/Sub triggers reconcilers immediately on state changes; fallback sweeps every 10s | [Event-driven Architecture](https://en.wikipedia.org/wiki/Event-driven_architecture) |
| **Dual-mode health check** | Keyspace-notification fast path + periodic TTL sweep fallback (`health.go`) | [Defense in Depth](https://en.wikipedia.org/wiki/Defense_in_depth_(computing)) |
| **Exponential backoff** | `CrashLoopBackOff` — restart delay doubles on each failure, capped at 30s (`health.go`) | [Exponential Backoff](https://en.wikipedia.org/wiki/Exponential_backoff) |
| **PID identity (pid, start_time)** | Prevents PID-reuse from adopting an unrelated process after a restart (`identity.go`, `process.go`) | [Linux PID Reuse](https://man7.org/linux/man-pages/man5/proc.5.html) |
| **Capacity bin-packing** | Scheduler fits pods onto nodes using a least-remaining-capacity heuristic (`scheduler.go`) | [Bin Packing Problem](https://en.wikipedia.org/wiki/Bin_packing_problem) |

---

## Part 2: Installation Locations on This Device

| Tool | Executable Path | Directory |
|---|---|---|
| **Go Compiler** | `C:\Program Files\Go\bin\go.exe` | `C:\Program Files\Go\` |
| **Redis Server** | `C:\Users\LOQ\AppData\Local\Microsoft\WinGet\Packages\taizod1024.redis-windows-fork_Microsoft.Winget.Source_8wekyb3d8bbwe\redis-server.exe` | WinGet Local AppData Package |
| **Mini-K8s Binaries** | `D:\Antigravity\Orchestration\Orchestration\bin\` | Project build directory |

---

## Part 3: Ordered Operational Guide

Follow these steps in exact order to build, run, monitor, and cleanly shut down the cluster.

### Step 1: Build All Binaries

Open PowerShell in the repository root (`D:\Antigravity\Orchestration\Orchestration`):

```powershell
cd "D:\Antigravity\Orchestration\Orchestration"

# Build all cluster binaries and the sample webapp workload
go build -o bin/api-server.exe ./cmd/api-server
go build -o bin/node-agent.exe ./cmd/node-agent
go build -o bin/dashboard.exe  ./cmd/dashboard
go build -o bin/webapp.exe     ./examples/webapp
```

---

### Step 2: Start the Servers & Nodes in Order

Mini-K8s requires the state store (Redis) first, then the control plane (API Server), then the worker (Node Agent).

#### 1. Start Redis Server
In Terminal 1 (or as a background process):
```powershell
redis-server --port 6379 --notify-keyspace-events Ex --save ""
```
*`--notify-keyspace-events Ex` enables keyspace notifications so node lease expiries trigger immediate node eviction and rescheduling.*

#### 2. Start API Server (Control Plane)
In Terminal 2:
```powershell
cd "D:\Antigravity\Orchestration\Orchestration"
$env:REDIS_ADDR = "localhost:6379"
$env:PORT = "8080"
.\bin\api-server.exe
```
*Listens on `http://127.0.0.1:8080`, hosts the REST API, controller loops, and web dashboard.*

#### 3. Start Node Agent (Worker / Kubelet)
In Terminal 3:
```powershell
cd "D:\Antigravity\Orchestration\Orchestration"
$env:REDIS_ADDR = "localhost:6379"
.\bin\node-agent.exe
```
*Registers the host as an active worker node with CPU/memory capacity and heartbeats every 3s.*

---

### Step 3: Check & Verify the Cluster

#### 1. Check API Health
```powershell
Invoke-RestMethod -Uri "http://localhost:8080/healthz"
# Expected output: {"status":"ok"}
```

#### 2. Check Registered Worker Nodes
```powershell
Invoke-RestMethod -Uri "http://localhost:8080/nodes" | ConvertTo-Json -Depth 3
```
*Shows all registered nodes, their ready state, total capacity, and allocated resources.*

#### 3. Check Complete Cluster State (Nodes, Deployments, Pods)
```powershell
Invoke-RestMethod -Uri "http://localhost:8080/state" | ConvertTo-Json -Depth 4
```

#### 4. Open the Web Dashboard
Open your browser to:
```
http://localhost:8080/
```
*Displays live real-time tables for Nodes, Deployments, Pods, and an audit log stream (SSE).*

---

### Step 4: Deploy, Inspect, and Test a Workload Pod

#### 1. Deploy the Workload
Create a deployment spec targeting the built executable:

```powershell
$body = @{
    name                  = "webapp"
    exec_path             = "D:/Antigravity/Orchestration/Orchestration/bin/webapp.exe"
    min_replicas          = 1
    max_replicas          = 3
    target_cpu_percentage = 50
    cpu_request           = 100
    mem_request           = 64
    port                  = 8000
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8080/deployments" -Method Post -Body $body -ContentType "application/json"
```

#### 2. Inspect the Scheduled Pod
Query the cluster state to get the assigned host port:
```powershell
$state = Invoke-RestMethod -Uri "http://localhost:8080/state"
$state.pods | Select-Object pod_id, deployment, status, host_port
```

#### 3. Send HTTP Requests to the Pod
Query the pod directly on its allocated port (typically `20000`):
```powershell
Invoke-RestMethod -Uri "http://localhost:20000/"
# Returns: pod=<pod_id> served=1
```

---

### Step 5: Scale the Workload

#### Scale via REST API:
Increase replicas to 3:
```powershell
$scale = @{ desired_replicas = 3 } | ConvertTo-Json
Invoke-RestMethod -Uri "http://localhost:8080/deployments/webapp/scale" -Method Post -Body $scale -ContentType "application/json"
```

#### Scale via Web Dashboard:
Visit `http://localhost:8080/` and click the **`+`** or **`-`** buttons under the **Deployments** panel.

---

### Step 6: Ordered Shutdown (Close All Nodes, Servers, and Pods)

To cleanly stop the entire system without leaving orphan processes holding network ports, shut down components in reverse order:

#### 1. Stop Worker Workloads & Node Agent
In Terminal 3, press `Ctrl + C`, or run in PowerShell:
```powershell
Stop-Process -Name "node-agent" -Force -ErrorAction SilentlyContinue
Stop-Process -Name "webapp" -Force -ErrorAction SilentlyContinue
```

#### 2. Stop Control Plane (API Server)
In Terminal 2, press `Ctrl + C`, or run in PowerShell:
```powershell
Stop-Process -Name "api-server" -Force -ErrorAction SilentlyContinue
```

#### 3. Stop State Store (Redis Server)
In Terminal 1, press `Ctrl + C`, or run in PowerShell:
```powershell
Stop-Process -Name "redis-server" -Force -ErrorAction SilentlyContinue
```

#### Quick One-Liner to Stop Everything at Once:
```powershell
Stop-Process -Name "node-agent", "api-server", "redis-server", "webapp" -Force -ErrorAction SilentlyContinue
```

#### Verify Everything is Stopped:
```powershell
Get-Process redis-server, api-server, node-agent, webapp -ErrorAction SilentlyContinue
```
*(No output indicates all servers and nodes are fully closed and all ports are released).*
