# Mini-K8s (Nova) — Detailed Phase-Wise Build Manifest

**Project:** Mini-K8s — a bare-metal container orchestrator (Nova architecture)
**Organization:** By **phase and dependency**, not by day. No schedule timeboxes — a phase is "done" when its Definition of Done is met, and it may begin only when its prerequisite phases are complete.
**Detail level:** Each phase lists *every artifact created* — modules, files, Redis keys, routines, loops, atomic scripts, interfaces, config — and describes **what each piece must do**, in plain language. **No source code** appears anywhere; behavior, inputs, outputs, and side-effects are specified instead.

> **On "no time constraints":** this removes the *project schedule* (Day 0–10). It does **not** remove the system's own *behavioral timing* (heartbeat interval vs lease TTL, backoff curve, cooldown windows, debounce window) — those are functional requirements of what the code must do, so they remain, labeled as **behavioral parameters**.

> **What changed from the earlier build plan:** this version folds in the five gaps from the review — (1) **atomic capacity reservation** in the scheduler, (2) **restart-safe process identity** in the agent, (3) a **consecutive-failure threshold** on health checks, (4) **structured logging from Phase 1** instead of at the dashboard, and (5) an explicit **controller kill-and-restart statelessness check**.

---

## Foundations & conventions (apply to every phase)

**Repository layout (created in Phase 1, used throughout)**

```text
mini-k8s/
├── cmd/
│   ├── api-server/        # control-plane entrypoint: REST API, scheduler, controllers
│   ├── node-agent/        # data-plane entrypoint: supervision, telemetry, local heal
│   └── dashboard/         # static UI assets (served by api-server) + SSE client
├── internal/
│   ├── schema/            # single source of truth for every Redis key pattern & field name
│   ├── redisclient/       # the ONLY way any component touches Redis
│   ├── logging/           # structured logging to stdout + cluster:logstream
│   ├── config/            # env/config loading with defaults
│   ├── scheduler/         # best-fit placement + atomic reservation
│   ├── controllers/       # replica controller, health controller
│   ├── supervisor/        # process spawn, health probe, backoff (agent)
│   ├── telemetry/         # sampling + autoscaler evaluation
│   └── ingress/           # nginx upstream generation + debounced reload
├── deploy/
│   └── nginx/             # generated upstream config + base nginx config
└── scripts/               # chaos + load-gen runners (described, not implemented here)
```

**Plane mapping (Nova blueprint ↔ this build)**

| Nova plane | Component(s) here |
|---|---|
| Storage (`nova-store`) | Redis |
| Control (`nova-api`, `nova-scheduler`, `nova-controller`) | `api-server`: REST + scheduler + Replica Controller + Health Controller |
| Data (`nova-agent`, `nova-router`) | `node-agent`: supervision/telemetry; Ingress Controller: Nginx |

**Three global invariants (enforced and tested in every relevant phase)**

1. **Decoupling via state** — no component ever calls another component directly; all coordination happens through Redis reads/writes via `redisclient`.
2. **Stateless controllers/agents** — any process can be killed and restarted, rebuild its working memory from Redis, and resume with no lost or duplicated state.
3. **Level-triggered, not edge-triggered** — every controller compares desired vs current state on a periodic sweep; Pub/Sub events are a latency optimization, never the only path to correctness.

---

## Phase 1 — Foundation: state client, shared schema, and logging

**Goal:** Establish the skeleton, the shared data contracts, and observability that every later phase depends on. Nothing orchestrates yet, but every later component speaks the same language and is debuggable from the first line.

**Depends on:** nothing.

**Modules & files created**

| Artifact | What it must do |
|---|---|
| `cmd/api-server`, `cmd/node-agent`, `cmd/dashboard` (stubs) | Three buildable entrypoints that load config, connect to Redis, emit a "started" log event, and idle. |
| `internal/schema` | Define, as named constants in one place, every Redis key pattern and every hash field name, plus the logical shape (fields + types) of Deployment, Pod, and NodeCapacity. Every other module imports these — no raw key strings anywhere else. |
| `internal/redisclient` | A thin wrapper that owns the connection pool and exposes typed helpers: hash read/write, set add/members, list push/trim/blocking-pop, Pub/Sub publish/subscribe, stream append/read, and an "evaluate atomic script" helper. Centralizes timeouts, retries, and (de)serialization between Redis hashes and the schema structs. |
| `internal/logging` | A structured logger that emits each event both to stdout and to a Redis stream (`cluster:logstream`). Each event carries: timestamp, component, level, event-type, optional deployment/pod/node IDs, and a message. This exists from Phase 1 so chaos debugging works for every later phase. |
| `internal/config` | Load configuration from environment with sane defaults: `NODE_ID`, listen `PORT`, Redis address, and tunable behavioral parameters (intervals, thresholds, cooldowns) so nothing is hard-coded. |

**State / Redis schema introduced**

| Key pattern | Type | Fields / contents | Written by | Read by | TTL |
|---|---|---|---|---|---|
| `cluster:logstream` | Stream | structured log entries (ts, component, level, event, ids, msg) | all components | dashboard, operators | none (capped length) |
| `test:key` | String | throwaway smoke value | foundation smoke check | smoke check | none |

**Routines & what they do**

- *connect-and-verify* — open the Redis pool and confirm reachability (a ping); fail fast and loud if Redis is unavailable.
- *key-builders* — given IDs, produce the exact key strings from the schema constants (so callers never concatenate strings themselves).
- *struct (de)serialize* — convert a Deployment/Pod/NodeCapacity to and from a Redis hash, validating field presence and types.
- *log-event* — append a structured entry to stdout and `cluster:logstream` in one call.

**Control flow**

On startup, each binary loads config, connects to Redis, verifies reachability, and emits a `component_started` log event naming its component and identity. If Redis is unreachable, it logs the reason and exits non-zero rather than running blind.

**Edge cases & invariants**

- Redis unavailable at boot → fail fast with a clear log, never silently degrade.
- Schema is the *only* place key/field names are defined → guarantees every phase reads/writes the same shapes (supports invariant 1).

**Manual verification**

Start Redis and run each binary; confirm a `component_started` entry appears in the log stream for each. Perform a trivial set-then-get of `test:key` through `redisclient` and confirm the value round-trips. This proves the wiring and the shared contracts before anything depends on them.

**Phase Definition of Done:** all three binaries build, connect, and log to the stream; the schema module is the sole source for key/field names; a smoke value round-trips through the client.

---

## Phase 2 — Node registration & lease heartbeat

**Goal:** Nodes announce themselves and their capacity, and continuously prove they are alive via an expiring lease. Node death becomes observable before any controller exists to react.

**Depends on:** Phase 1.

**Modules & files created**

| Artifact | What it must do |
|---|---|
| node-agent registration routine | On startup, add this node's ID to the registry set and write its capacity hash. Idempotent on restart. |
| node-agent heartbeat loop | Periodically refresh the node's status key with a short TTL so the key's *presence* means "alive" and its *expiry* means "dead." |
| capacity reconstructor (statelessness) | On restart, recompute `allocated_cpu`/`allocated_mem` from the pods actually present on this node rather than resetting them to zero. |
| graceful-shutdown handler | On clean stop, log departure and let the lease lapse (or remove from registry). |

**State / Redis schema introduced**

| Key pattern | Type | Fields / contents | Written by | Read by | TTL |
|---|---|---|---|---|---|
| `nodes:registry` | Set | one member per node ID | node-agent | scheduler, health controller | none |
| `node:{node_id}:capacity` | Hash | `total_cpu`, `total_mem`, `allocated_cpu`, `allocated_mem` (CPU in millicores, mem in MB) | node-agent (init), scheduler (reserve), health controller (reset) | scheduler, dashboard | none |
| `node:{node_id}:status` | String | value "Ready" — the liveness **lease** | node-agent heartbeat | health controller, scheduler | short (lease window) |

**Behavioral parameters**

- **Heartbeat interval < lease TTL**, with margin (e.g., refresh roughly every third of the TTL) so a single missed refresh does not falsely expire a live node, but a stopped node expires within the TTL window.

**Routines & what they do**

- *register-node* — add to registry (idempotent) and write the capacity hash from configured totals; on restart, reconcile allocations from existing pods instead of zeroing.
- *heartbeat-tick* — refresh the status lease with its TTL on each interval.
- *deregister* — on graceful shutdown, announce departure and stop refreshing.

**Background loops / daemons**

- *heartbeat loop* — repeats the lease refresh forever while the agent runs.

**Edge cases & invariants**

- Crash vs graceful stop: both eventually expire the lease; graceful stop also logs intent.
- Restart must **not** clobber live capacity allocations (recompute from real pods) — this is the statelessness fix.
- Liveness is derived **only** from lease presence; no "node down" message is ever sent (invariant 1 + 3).

**Manual verification**

Start two agents on different ports; confirm both appear in the registry and each has a correctly initialized capacity hash. Watch one node's lease TTL and confirm it refreshes on the interval rather than decaying. Stop one agent and confirm its lease key disappears within the lease window — the node-death signal works with no controller present.

**Phase Definition of Done:** nodes self-register with accurate capacity; liveness is observable purely via lease expiry; restart preserves capacity accounting.

---

## Phase 3 — Deployment API & atomic best-fit scheduler

**Goal:** Accept and validate deployment specs over REST, and place replicas onto nodes using best-fit selection with **race-free, atomic capacity reservation** so a node can never be double-booked.

**Depends on:** Phase 2.

**Modules & files created**

| Artifact | What it must do |
|---|---|
| api-server HTTP layer | Expose REST endpoints; parse and validate request bodies; translate to Redis writes. |
| deployment validator/writer | Reject malformed specs; persist valid ones as a deployment hash with `desired_replicas` initialized to `min_replicas`. |
| `internal/scheduler` (best-fit) | Given a deployment and a replica count, choose suitable nodes and create pods on them. |
| atomic reservation script (Lua) | Reserve capacity on a chosen node in a single indivisible step (read → check headroom → increment), so concurrent scheduling cannot oversubscribe. |
| debug scheduler trigger | A manual endpoint/flag to run the scheduler once, before any controller loop exists. |

**State / Redis schema introduced**

| Key pattern | Type | Fields / contents | Written by | Read by | TTL |
|---|---|---|---|---|---|
| `deployment:{name}` | Hash | `name`, `min_replicas`, `max_replicas`, `cpu_request`, `mem_request`, `desired_replicas`, `created_at` | api-server | scheduler, controllers, autoscaler, dashboard | none |
| `pod:{deployment}:{uuid}` | Hash | `deployment`, `pod_id`, `status` (=`Pending`), `node_id`, `cpu_request`, `mem_request`, `created_at` (more fields added later) | scheduler | agent, controllers, dashboard | none |

**Interfaces / endpoints**

- `POST /deployments` — validate and store a deployment spec.
- `GET /deployments/{name}` — read a deployment back.
- `GET /healthz` — liveness of the api-server itself.

**Atomic (Lua) operations**

- *reserve-capacity* — atomically: read a node's capacity, verify `total − allocated ≥ request` for both CPU and memory, and if it fits, increment the allocations and report success; otherwise report failure without mutating anything. This removes the read-scan-decrement race.

**Routines & what they do**

- *validate-deployment-spec* — enforce required fields, types, and bounds (e.g., `min ≤ max`, positive requests).
- *best-fit-select* — from nodes that are **alive** (lease present) and have headroom, pick the node whose remaining capacity most tightly fits the request (tight packing), leaving large gaps free for larger future pods.
- *schedule-pod* — for each replica: pick a candidate, run *reserve-capacity*; on success create the pod hash as `Pending` bound to that node; on failure, try the next candidate.
- *handle-unschedulable* — if no node fits, leave the replica unplaced, log it as `Unschedulable`, and surface it for retry on the next sweep.

**Control flow**

A request arrives, is validated, and the deployment is stored. When scheduling is triggered, the scheduler filters to live nodes with headroom, scores them by tightest fit, and for each needed replica atomically reserves capacity and creates a `Pending` pod. Capacity is mutated **only** through the atomic script, so two scheduling passes (or a scheduler racing an agent) can never exceed a node's total.

**Edge cases & invariants**

- No node fits → `Unschedulable`, logged, retried later — never a crash, never a partial reservation.
- Duplicate `POST` for an existing name → treated as a spec/desired update (defined, idempotent), not a silent second deployment.
- `allocated` can never exceed `total` (guaranteed by the atomic check).

**Manual verification**

Submit a deployment and read it back to confirm the stored fields. Trigger the scheduler once and confirm pods were created `Pending` on a node that had headroom, and that the node's `allocated_cpu` rose by **exactly** the request times the replica count. Submit a malformed spec and confirm it is rejected. Attempt to oversubscribe a node and confirm graceful `Unschedulable` handling rather than negative capacity.

**Phase Definition of Done:** deterministic best-fit placement with exact, race-free capacity accounting and clean handling of unschedulable replicas.

---

## Phase 4 — Real process supervision & restart-safe identity

**Goal:** Actually run the workload process for each assigned pod, record its identity durably, and make supervision survive an agent restart with **no orphaned and no duplicated processes**.

**Depends on:** Phase 3.

**Modules & files created**

| Artifact | What it must do |
|---|---|
| agent command consumer | Receive "start this pod" instructions for this node and act on them. |
| process spawner | Launch the workload binary, place it in its own process group, and capture its PID. |
| supervisor registry (in-memory cache) | Track locally-running pods, always rebuildable from Redis. |
| startup reconciler (statelessness) | On agent boot, reconcile recorded pods against actually-alive processes: re-adopt living ones, flag dead ones for restart, never double-spawn. |

**State / Redis schema introduced**

| Key pattern | Type | Fields / contents | Written by | Read by | TTL |
|---|---|---|---|---|---|
| `node:{node_id}:commands` | List | pending command envelopes (e.g., start-pod{pod_id}) | scheduler/controllers | this node's agent (blocking pop) | none |
| `pod:{...}` (fields added) | Hash | adds `pid`, `started_at`; `status` transitions `Pending → Running` (and later `Failed`/`Evicted`) | agent | controllers, dashboard | none |

**Routines & what they do**

- *consume-commands* — block waiting for the node's command list; on a start instruction, spawn the pod.
- *spawn-process* — launch the workload in its own process group, capture the PID, and write `pid` + `started_at` to the pod hash **immediately** so identity is durable, then flip status to `Running`.
- *reconcile-on-startup* — read all pods bound to this node; for each, check whether the recorded PID is still a live process belonging to this pod; re-adopt the live ones, and mark the dead ones for restart. Never spawn a duplicate for a pod that is already running.
- *liveness-check-of-pid* — determine whether a recorded PID is still alive and is the same process (using start-time/marker to defend against PID reuse).

**Background loops / daemons**

- *command-consumer loop* — continuously processes start instructions for this node.

**Control flow**

When a pod is assigned, a start command lands on the node's command list; the agent pops it, spawns the process, records the PID durably, and marks the pod `Running`. If the agent restarts, *reconcile-on-startup* runs first: it compares Redis's record against reality, adopts survivors, and only restarts genuinely dead pods — guaranteeing no orphans and no doubles.

**Edge cases & invariants**

- Spawn failure → status `Failed`, logged, eligible for restart logic (Phase 6).
- Agent crash between spawn and PID write → reconciler detects a `Running` pod with no live PID and restarts it.
- PID reuse → disambiguated by start-time/marker so a recycled PID isn't mistaken for the pod.
- A pod's authoritative state lives in Redis; local memory is a rebuildable cache (invariant 2).

**Manual verification**

Schedule a real long-running HTTP workload; confirm an OS process exists, the pod shows `Running` with a recorded PID, and the process answers on its port directly. Then restart the agent and confirm it **re-adopts** the existing process instead of launching a duplicate, and that no pod is left orphaned.

**Phase Definition of Done:** real processes run and are reachable; identity is durable in Redis; agent restart produces neither orphans nor duplicates.

---

## Phase 5 — Dual-mode reconciliation (Replica Controller)

**Goal:** Keep the count of healthy actual replicas equal to `desired_replicas` for every deployment, through both a fast event path and a guaranteed periodic sweep — and survive a controller restart mid-reconcile.

**Depends on:** Phase 4.

**Modules & files created**

| Artifact | What it must do |
|---|---|
| Replica Controller (subscriber) | React to deployment events and reconcile the named deployment quickly. |
| Replica Controller (sweeper) | Independently and periodically reconcile **all** deployments regardless of events. |
| delta engine | Compute `desired − healthy-actual` and return the work needed to close it. |
| event publisher hook | Publish a deployment event whenever `desired_replicas` changes. |

**State / Redis schema introduced**

| Key pattern | Type | Fields / contents | Written by | Read by | TTL |
|---|---|---|---|---|---|
| `deployment:events` | Pub/Sub channel | messages `{event, deployment}` | api-server, autoscaler | replica controller, ingress | n/a |

**Routines & what they do**

- *compute-delta* — count healthy pods for a deployment and subtract from `desired_replicas` to get a signed delta.
- *scale-up* — for a positive delta, invoke the Phase 3 scheduler to create the missing replicas.
- *scale-down* — for a negative delta, select victim pods (e.g., newest first), issue stop commands, and release their capacity.
- *on-event* — recompute the delta for one named deployment (the fast path).
- *sweep* — iterate every deployment and recompute deltas (the correctness guarantee).

**Background loops / daemons**

- *event-subscriber loop* — handles events as they arrive.
- *sweep loop* — runs the same delta logic on a periodic interval, independent of events.

**Control flow**

Both paths call the **identical** delta engine, so correctness never depends on which path fired. The event path reduces latency; the sweep path guarantees the system self-corrects even if events are lost or the controller was offline. Because all working state is derived from Redis, a controller killed mid-reconcile simply resumes on restart.

**Edge cases & invariants**

- Lost event → the sweep still reconciles (the level-triggered proof).
- Duplicate events → harmless, because the delta is always recomputed from truth.
- Controller restart mid-reconcile → resumes from Redis with no lost or duplicated work (invariant 2).

**Manual verification**

Raise `desired_replicas` via an event and confirm new pods appear quickly. Then disable the publish step and change `desired_replicas` directly; confirm the periodic sweep still reconciles — proving level-triggered correctness. Finally, kill the controller mid-reconcile and restart it; confirm it converges to the correct replica count without creating extras.

**Phase Definition of Done:** replica count converges to desired under event loss **and** controller restart, via a single shared delta engine.

---

## Phase 6 — Pod-level self-healing (local, threshold-gated, persisted backoff)

**Goal:** The agent locally restarts an unhealthy pod with **no control-plane round trip**, using a consecutive-failure threshold to resist flapping and an exponential backoff whose state survives an agent restart.

**Depends on:** Phase 4 (supervision), Phase 5 (so reschedules coordinate).

**Modules & files created**

| Artifact | What it must do |
|---|---|
| per-pod health checker | Probe each supervised process (TCP/HTTP) for liveness. |
| failure evaluator (threshold) | Only declare a pod dead after N consecutive failed probes, to absorb transient blips. |
| restart manager | Kill any remnant, respawn, and increment restart bookkeeping. |
| backoff state machine | Compute and persist the next restart delay so the curve survives restarts. |

**State / Redis schema introduced**

| Key pattern | Type | Fields / contents | Written by | Read by | TTL |
|---|---|---|---|---|---|
| `pod:{...}` (fields added) | Hash | adds `restart_count`, `consecutive_failures`, `last_health_ok_at`, `backoff_next_seconds`, `last_restart_at` | agent | controllers, dashboard | none |

**Behavioral parameters**

- **Health probe interval**; **failure threshold N** (consecutive failures before "dead"); **backoff curve** doubling from a small base up to a capped maximum; optional **crash-loop cap** (mark `CrashLoopBackOff` after K rapid restarts).

**Routines & what they do**

- *health-check* — probe the pod's port; record success/failure.
- *evaluate-health* — increment `consecutive_failures` on a miss and reset it on a success; only trigger a restart when the threshold is crossed.
- *restart-pod* — terminate any remnant, respawn via the Phase 4 spawner, increment `restart_count`, stamp `last_restart_at`, and persist the next backoff delay.
- *backoff-schedule* — double the delay from the base toward the cap, reading and writing the persisted value so a mid-backoff restart doesn't reset the curve.

**Background loops / daemons**

- *supervision loop* — probes each local pod on the interval and gates restarts by the persisted backoff timer.

**Control flow**

The supervision loop probes pods; a single miss does nothing until the consecutive-failure threshold is reached, at which point the pod is restarted and its backoff delay grows. Because backoff and counters live in the pod hash, the healing behavior is correct even across an agent restart.

**Edge cases & invariants**

- Probe flakiness → absorbed by the threshold (no needless restarts).
- Persistent crashes → backoff caps; optional crash-loop state prevents tight respawn storms.
- Healing is purely local for speed and never involves the control plane (invariant 1); backoff state is in Redis (invariant 2).

**Manual verification**

Kill a supervised process and confirm it returns and `restart_count` increments. Kill it repeatedly and confirm the gaps grow exponentially rather than staying flat. Restart the agent mid-backoff and confirm the backoff delay was preserved (not reset). Briefly make a probe fail once (under the threshold) and confirm **no** restart occurs.

**Phase Definition of Done:** fast local heal, flap-resistant via threshold, exponential backoff that persists across agent restarts.

---

## Phase 7 — Node-level self-healing (Health Controller) + optional real cgroup limits

**Goal:** Detect node death via lease expiry, **atomically** evict that node's pods and free its capacity, and let reconciliation reschedule the lost replicas. Optionally enforce **real** cgroup v2 limits so resource caps are demonstrably enforced, not merely tracked.

**Depends on:** Phase 2 (leases), Phase 5 (reschedule), Phase 6.

**Modules & files created**

| Artifact | What it must do |
|---|---|
| Health Controller (expiry subscriber) | React to a node lease expiring by evicting that node. |
| Health Controller (sweep fallback) | Periodically scan the registry for nodes whose lease is gone and evict them, covering any missed notification. |
| atomic eviction script (Lua) | Mark a dead node's pods evicted and reset its capacity in one indivisible step. |
| (optional) cgroup writer | Before exec, create a per-pod cgroup v2 directory and write real CPU/memory limits. |

**Configuration artifacts**

- Enable Redis keyspace notifications for expired-key events so lease expiry can trigger the controller.
- (optional) A per-pod cgroup v2 directory under the node's cgroup hierarchy.

**State / Redis schema introduced**

| Key pattern | Type | Fields / contents | Written by | Read by | TTL |
|---|---|---|---|---|---|
| `pod:{...}` (status) | Hash | `status` may transition to `Evicted` | health controller | controllers, dashboard | none |
| `node:{node_id}:capacity` (reset) | Hash | allocations reset to 0 on eviction | health controller | scheduler, dashboard | none |

**Atomic (Lua) operations**

- *evict-node* — atomically: find all pods bound to the dead node, set each to `Evicted`, reset that node's `allocated_cpu`/`allocated_mem` to zero, and remove the node from the registry — so no partial/inconsistent state is ever visible.

**Routines & what they do**

- *on-node-expiry* — triggered by an expired lease; run *evict-node* for that node and log it.
- *sweep-dead-nodes* — periodically detect registry members whose lease key is gone and evict them (fallback for missed notifications — the level-triggered guarantee).
- *(optional) write-cgroup-limits* — before spawning a pod, create its cgroup directory and write CPU/memory caps derived from the request, so the limit is enforced by the kernel.

**Background loops / daemons**

- *keyspace-expiry subscriber* — fast path.
- *dead-node sweep* — fallback path.

**Control flow**

When a node stops, its lease expires; the controller evicts that node atomically and frees its capacity. The Replica Controller's next delta then sees the missing replicas and reschedules them onto surviving nodes. If the optional cgroup task is included, each pod runs under a real kernel-enforced limit that can be inspected directly.

**Edge cases & invariants**

- Missed expiry notification → the sweep still evicts (invariant 3).
- A node re-registers after a flap → its evicted pods stay evicted; fresh replicas are scheduled normally.
- cgroup write failure → logged and handled by a defined policy (fail the pod or proceed tracked-only).
- Eviction is atomic and idempotent; capacity accounting stays consistent.

**Manual verification**

Kill an entire agent (not just a child). Watch its lease expire, then confirm the eviction script fires (logged), its pods flip to `Evicted`, and its capacity resets. Confirm the surviving node receives the rescheduled replicas. If the optional task is included, read the pod's cgroup limit file directly and confirm the cap is real.

**Phase Definition of Done:** node death is detected, contained by atomic eviction, and recovered by reschedule; optional limits are real and demonstrable.

---

## Phase 8 — Telemetry & horizontal autoscaler

**Goal:** Collect recent per-pod CPU telemetry and scale a deployment by the spec formula, clamped to its min/max, via an atomic evaluation that can't race the reconciler.

**Depends on:** Phase 6 (real pods to measure), Phase 5 (to act on desired changes).

**Modules & files created**

| Artifact | What it must do |
|---|---|
| telemetry writer (agent) | Periodically record each pod's recent CPU usage, keeping only the most recent samples. |
| autoscaler evaluator | For each deployment, average recent samples, apply the scale formula, clamp, and update desired. |
| autoscale script (Lua) | Perform the read-average-formula-clamp-write-publish as one atomic step. |

**State / Redis schema introduced**

| Key pattern | Type | Fields / contents | Written by | Read by | TTL |
|---|---|---|---|---|---|
| `telemetry:{deployment}:{pod}:cpu` | List | recent CPU samples, trimmed to the last 10 | agent telemetry writer | autoscaler, dashboard | none |
| `deployment:{name}` (field) | Hash | `desired_replicas` updated by the autoscaler | autoscaler | controllers, dashboard | none |

**Behavioral parameters**

- **Sampling interval**; **averaging window** (last 3 samples); **target utilization** used in the formula; **bounds** `[min_replicas, max_replicas]`.

**Atomic (Lua) operations**

- *autoscale* — atomically: read the most recent samples across the deployment's pods, average them, compute desired replicas from the formula (scaling current replicas by the ratio of observed to target utilization, rounded up), clamp to `[min, max]`, write `desired_replicas`, and publish a deployment event.

**Routines & what they do**

- *sample-telemetry* — read each pod's CPU (real cgroup stats if the optional Phase 7 task exists, otherwise a synthetic generator) and push it with a trim to the last 10.
- *run-autoscaler* — invoke the atomic *autoscale* script per deployment.

**Background loops / daemons**

- *telemetry-sampling loop* (agent) and *autoscaler-evaluation loop* (control plane).

**Control flow**

Agents continuously record CPU samples; the autoscaler periodically averages the latest few, applies the formula, clamps to bounds, and updates `desired_replicas` atomically — after which the Replica Controller (Phase 5) closes the new delta. Doing the whole computation atomically prevents a half-updated desired value from being read mid-flight.

**Edge cases & invariants**

- Too few samples → no-op rather than acting on noise.
- Spikes → smoothed by averaging; a computed value above max is clamped, preventing overshoot.
- `desired_replicas` changes **only** through the atomic script and never leaves `[min, max]`.

**Manual verification**

Push several high CPU samples and run the autoscaler; confirm `desired_replicas` equals the value computed by hand from the formula. Push values that would exceed the maximum and confirm the result clamps to the max rather than overshooting.

**Phase Definition of Done:** telemetry-driven scaling that matches the spec formula exactly and always respects bounds.

---

## Phase 9 — Scaling cooldowns & ingress routing

**Goal:** Prevent scaling thrash with asymmetric cooldowns, and route external traffic to currently-running pods while collapsing bursts of change into a single Nginx reload.

**Depends on:** Phase 8 (scaling), Phase 5 (reconcile), Phase 4 (running pods with ports).

**Modules & files created**

| Artifact | What it must do |
|---|---|
| cooldown gate | Before acting on a scaling delta, ensure the relevant cooldown window has elapsed. |
| Ingress Controller daemon | Watch for deployment changes and keep Nginx upstreams in sync. |
| upstream config generator | Render the Nginx upstream block from the set of currently-running pods. |
| debounced reload | Collapse a burst of changes into exactly one config rewrite + reload. |

**State / Redis schema introduced**

| Key pattern | Type | Fields / contents | Written by | Read by | TTL |
|---|---|---|---|---|---|
| `deployment:{name}` (fields) | Hash | `last_scale_up_at`, `last_scale_down_at` timestamps | reconciler/autoscaler | cooldown gate | none |

**Configuration artifacts**

- A generated Nginx upstream config file plus a base config, and the reload mechanism that applies it.

**Behavioral parameters**

- **Scale-up cooldown** (short) and **scale-down cooldown** (long, to avoid removing capacity prematurely); **ingress debounce window** during which multiple events coalesce into one reload.

**Routines & what they do**

- *check-cooldown* — before scaling up or down, confirm the matching window since the last action has elapsed; otherwise defer.
- *record-scale-action* — stamp the appropriate timestamp after a scaling action.
- *build-upstreams* — enumerate running pods and their host:port and render the upstream block.
- *debounce-reload* — buffer events for the debounce window, then rewrite the config once and reload Nginx once.

**Background loops / daemons**

- *ingress subscriber* with a debounce timer; cooldown checks are inline gates rather than loops.

**Control flow**

When a scaling delta appears, the cooldown gate allows it only if the matching window has passed — up-scaling reacts quickly, down-scaling waits longer so capacity isn't yanked on a brief dip. Meanwhile, the Ingress Controller batches change events: a burst within the debounce window produces a single upstream rewrite and a single reload, and external requests flow through Nginx to a currently-running pod.

**Edge cases & invariants**

- Rapid events → exactly one reload per debounce window (verified by a reload counter/log).
- Brief load dip → no premature scale-down until the long window elapses.
- Pod set changing mid-debounce → the final state is used for the single rewrite.

**Manual verification**

Trigger a scale-up, then immediately attempt another and confirm it's suppressed until the up-cooldown elapses. Drop load and confirm pods are **not** removed until the down-cooldown passes, then are. Fire several rapid scaling events and confirm exactly one Nginx reload occurred. Send a request through Nginx and confirm it reaches a running pod.

**Phase Definition of Done:** thrash-free scaling via asymmetric cooldowns, and correct ingress routing with one reload per burst.

---

## Phase 10 — Dashboard & live updates (SSE)

**Goal:** Provide a live, auto-updating view of nodes, pods, deployments, and logs, with recent history rendered instantly on load. The UI is a pure consumer of Redis truth.

**Depends on:** Phase 1 (log stream) and enough state from prior phases to display.

**Modules & files created**

| Artifact | What it must do |
|---|---|
| dashboard page (HTML/JS) | Render a table/feed of nodes, pods, deployments, and a live log feed. |
| SSE endpoint (api-server) | Push incremental state diffs to connected browsers. |
| state snapshot builder | Assemble the current cluster state into a view model on connect. |
| log backfill | On connect, load the most recent log entries so the feed is never blank. |

**Interfaces / endpoints**

- `GET /` — serve the dashboard.
- `GET /state` — initial full snapshot.
- `GET /events` — SSE stream of state diffs.

**State / Redis schema**

- Reads existing keys (`nodes:registry`, `node:*`, `deployment:*`, `pod:*`) and `cluster:logstream`. No new keys.

**Routines & what they do**

- *build-snapshot* — gather current nodes, pods, and deployments into a single view model.
- *stream-diffs* — push state changes to the browser as they happen.
- *backfill-logs* — on connect, read the last ~20 log entries (newest first) so the feed shows history immediately.

**Control flow**

On load, the browser fetches a snapshot and a log backfill, then subscribes to the SSE stream for live diffs. Killing a pod, scaling a deployment, or killing a node is reflected without a manual refresh. The dashboard never writes cluster state — it only reads, preserving Redis as the single source of truth.

**Edge cases & invariants**

- Reconnect → re-snapshot and re-backfill.
- Large state → summarize/paginate.
- The dashboard is strictly a consumer; it never becomes a source of state (invariant 1).

**Manual verification**

Open the dashboard; from a separate terminal, kill a pod, scale a deployment, and kill a node, and confirm the view updates live without refreshing. Refresh the page mid-session and confirm the most recent events appear immediately rather than a blank feed.

**Phase Definition of Done:** a live, auto-updating, read-only view with instant history on load.

---

## Phase 11 — Load generation & chaos acceptance (proof-of-work)

**Goal:** Exercise the whole system under realistic load and chaos, and capture the artifacts that make every quantitative claim defensible.

**Depends on:** all prior phases.

**Modules & files created**

| Artifact | What it must do |
|---|---|
| load generator | Drive rate-ramped HTTP traffic against a deployment's ingress route. |
| chaos runner (3 scenarios) | Execute the three acceptance scenarios back-to-back and observe recovery. |
| artifact capture | Export the relevant logs/timestamps and record a screen capture of the run. |

**State / Redis schema**

- Reads telemetry and the log stream; optionally writes an acceptance-run marker. No new persistent schema.

**Behavioral parameters**

- **Load ramp profile** (e.g., increasing request rate over a range); the three scenarios run **consecutively** with timing captured.

**Routines & what they do**

- *ramp-load* — increase request rate over the profile against the ingress route.
- *scenario-1 (pod kill)* — kill a child process and observe local healing (Phase 6).
- *scenario-2 (node kill)* — kill a whole node and observe eviction + reschedule (Phases 7 + 5).
- *scenario-3 (load stress)* — ramp load to trigger scale-up (Phase 8), then confirm cooldown-respecting scale-down (Phase 9).
- *capture-artifacts* — export the relevant Redis logs/timestamps and save the recording.

**Control flow**

The three scenarios run one after another in a single continuous session, each demonstrating a distinct recovery behavior, while logs and timestamps are captured throughout. The output is a reproducible record proving the system heals pods, heals nodes, and autoscales with cooldowns.

**Edge cases & invariants**

- Run the scenarios truly back-to-back (or with clean state between) and capture timing.
- Every quantitative claim made later is backed by a captured artifact (invariant: defensibility).

**Manual verification**

Run all three scenarios in one session and confirm each recovers correctly; save the screen recording and the exported logs/timestamps as the proof-of-work bundle.

**Phase Definition of Done:** a reproducible chaos pass with captured artifacts covering pod heal, node heal, and autoscale-with-cooldown.

---

## Appendix A — Consolidated Redis schema (every key, one place)

| Key pattern | Type | Purpose | Introduced in |
|---|---|---|---|
| `cluster:logstream` | Stream | structured logs from all components | Phase 1 |
| `nodes:registry` | Set | the set of known node IDs | Phase 2 |
| `node:{node_id}:capacity` | Hash | total/allocated CPU & memory | Phase 2 |
| `node:{node_id}:status` | String (TTL) | liveness lease ("Ready") | Phase 2 |
| `deployment:{name}` | Hash | spec + `desired_replicas` + scale timestamps | Phase 3 / 9 |
| `pod:{deployment}:{uuid}` | Hash | pod spec, status, pid, restart/backoff state | Phase 3 / 4 / 6 |
| `node:{node_id}:commands` | List | per-node command queue (start-pod, etc.) | Phase 4 |
| `deployment:events` | Pub/Sub | deployment change notifications | Phase 5 |
| `telemetry:{deployment}:{pod}:cpu` | List | recent CPU samples (last 10) | Phase 8 |

## Appendix B — Cross-cutting modules (created in Phase 1, used everywhere)

| Module | Responsibility |
|---|---|
| `schema` | the only definition of key patterns, field names, and data shapes |
| `redisclient` | the only path to Redis; typed helpers + atomic-script evaluation |
| `logging` | structured events to stdout and `cluster:logstream` |
| `config` | env-driven configuration and behavioral parameters |

## Appendix C — Global invariants and where each is proven

| Invariant | Proven by |
|---|---|
| Decoupling via state | every component touches only Redis via `redisclient`; healing is local (Phase 6); dashboard is read-only (Phase 10) |
| Stateless components | capacity reconstruction on node restart (Phase 2); restart-safe process identity (Phase 4); persisted backoff (Phase 6); controller kill-and-restart test (Phase 5) |
| Level-triggered | reconciliation sweep independent of events (Phase 5); dead-node sweep fallback (Phase 7) |

## Appendix D — Phase dependency graph

```text
Phase 1 (foundation)
   └─> Phase 2 (nodes + lease)
          └─> Phase 3 (API + atomic scheduler)
                 └─> Phase 4 (supervision + restart-safe identity)
                        ├─> Phase 5 (reconciliation) ──────────────┐
                        └─> Phase 6 (pod self-heal) ───────────────┤
Phase 2 + Phase 5 + Phase 6 ─> Phase 7 (node self-heal + cgroups)  │
Phase 5 + Phase 6 ───────────> Phase 8 (telemetry + autoscaler)    │
Phase 8 + Phase 5 + Phase 4 ─> Phase 9 (cooldowns + ingress)       │
Phase 1 + state from above ──> Phase 10 (dashboard + SSE)          │
all phases ──────────────────> Phase 11 (load gen + chaos)  <──────┘
```
