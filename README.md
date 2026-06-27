Mini-Kubernetes: High-Performance Architecture, Specification, and Testing Document

This document defines the complete structural specifications, architectural layouts, and verification workflows for Mini-Kubernetes (Mini-K8s). Mini-K8s is a lightweight, low-latency, resilient container/process orchestration engine designed around two core operational patterns: Reactive Horizontal Pod Autoscaling (HPA) and State-Driven Auto-Healing.

The system relies entirely on a high-throughput, in-memory Redis database to manage cluster topology, using Redis Pub/Sub channels and Streams to replace traditional heavy polling and mirror the reactive, watch-based architecture of enterprise Kubernetes.

1. Architectural Blueprint & Data Flow

Complete System Topology

+-----------------------------------------------------------------------------------+
|                                 WEB DASHBOARD UI                                  |
|         [Cluster Canvas]     [Config Forms]     [Internal Traffic Gen]            |
+-----------------------------------------------------------------------------------+
                 |                                      ^
        (REST API Writes JSON)                          | (SSE / WebSocket)
                 |                                      |
                 v                                      |
+-----------------------------------------------------------------------------------+
|                            CONTROL PLANE (Host / VM)                              |
|                                                                                   |
|  +---------------------------+             +----------------------------------+   |
|  |     REST API Server       |             |           Redis Server           |   |
|  |  - Serves Static Web UI   |             |   - Holds In-Memory Topology     |   |
|  |  - Validates Config Post  | ----------> |   - Pub/Sub Channel Broadcast    |   |
|  |  - Handles Node Heartbeats|             |   - Redis Stream Event Logging   |   |
|  +---------------------------+             |   - Expirable TTL Lease Keys     |   |
|                                            +----------------------------------+   |
|                                                              |                    |
|                                                              | (Pub/Sub & Sweeps) |
|                                                              v                    |
|  +---------------------------+             +----------------------------------+   |
|  | Ingress Controller Daemon |             |        CONTROLLER MANAGER        |   |
|  |  - Debounces Sync Events  | <---------- |                                  |   |
|  |  - Updates Nginx Upstreams|  (Pub/Sub)  |  +----------------------------+  |   |
|  +---------------------------+             |  |     Replica Controller     |  |   |
|                 |                          |  |  (Reacts to desired state) |  |   |
|                 v (Reloads Router)         |  +----------------------------+  |   |
|  +---------------------------+             |  +----------------------------+  |   |
|  | Ingress Router (Nginx)    |             |  |     Autoscaler Engine      |  |   |
|  +---------------------------+             |  |  (Filters & scales metrics)|  |   |
|                                            |  +----------------------------+  |   |
|                                            |  +----------------------------+  |   |
|                                            |  |   Health & Node Monitor    |  |   |
|                                            |  |  (Dual-Mode Lease Sweep)   |  |   |
|                                            |  +----------------------------+  |   |
|                                            +----------------------------------+   |
+-----------------------------------------------------------------------------------+
                                       |
                (Asynchronous Commands | Unified Telemetry & Keep-Alive Pipelines)
                                       v
+-----------------------------------------------------------------------------------+
|                        VIRTUAL INCUS ROUTING BRIDGE (incusbr0)                    |
+-----------------------------------------------------------------------------------+
         |                                                 |
         v                                                 v
+-------------------------------+                 +-------------------------------+
|      WORKER NODE 1 (Incus)    |                 |      WORKER NODE 2 (Incus)    |
|                               |                 |                               |
|  +-------------------------+  |                 |  +-------------------------+  |
|  |       Node Agent        |  |                 |  |       Node Agent        |  |
|  |     (Mini-Kubelet)      |  |                 |  |     (Mini-Kubelet)      |  |
|  +-------------------------+  |                 |  +-------------------------+  |
|       |               ^       |                 |       |               ^       |
|       | (Supervises)  | (Poll)|                 |       | (Supervises)  | (Poll)|
|       v               |       |                 v               |       |
|  +-------------------------+  |                 +-------------------------+  |
|  |  Supervised OS Process  |  |                 |  Supervised OS Process  |  |
|  |  (No Docker Overhead)   |  |                 |  (No Docker Overhead)   |  |
|  +-------------------------+  |                 +-------------------------+  |
|    |      |      |            |                 |    |      |      |            |
|    v      v      v            |                 |    v      v      v            |
|  [Pod]  [Pod]  [Pod]          |                 |  [Pod]  [Pod]  [Pod]          |
+-------------------------------+                 +-------------------------------+


2. Structural Requirements Matrix

Functional Requirements (FR)

FR-1: Declarative Web UI Configuration

The cluster must expose a graphical HTML webpage to completely eliminate the requirement of static configurations or YAML manifests.

Worker Deployment Form: Users must be able to declare Workload Configuration blocks using input fields (Image/Exec Path, Target Port, Environment Variables, CPU Resource Request, Memory Resource Request).

Scaling Boundaries Form: Users must be configured to declare limits (min_replicas, max_replicas, target_cpu_percentage) and timing thresholds via HTML inputs.

Live Update Execution: All submissions must be validated by the API Server and directly written into Redis, automatically invoking reconciliation handlers.

FR-2: Dual-Mode Reconciliation Engine (Edge & Level-Triggered)

To ensure both rapid reaction and guaranteed eventual consistency, the system must utilize a dual-trigger mechanism:

Edge-Triggered Path (Pub/Sub): The Replica Controller must subscribe to Redis Pub/Sub event updates on the deployment:events channel. The moment an update is detected, the reconciliation loop must execute immediately to enforce state changes.

Level-Triggered Path (Periodic Sweep Fallback): Independent of Pub/Sub, the controller must execute a full state synchronization sweep every $30\text{s}$. This guarantees recovery and consistency even if a controller thread crashes, restarts, or misses transient network messages.

Reconciliation Loop Contract: The logic must dynamically calculate:

$$\Delta = \text{Desired Replicas} - \text{Actual Healthy Replicas}$$

If $\Delta > 0$, the Scheduler selects nodes to spin up workloads. If $\Delta < 0$, target pods are designated for deletion.

FR-3: High-Fidelity Autoscaling Engine (HPA)

Telemetry Collection: The Autoscaler Engine must regularly query telemetry metrics written to Redis by individual node agents.

Signal Filtering & Noise Control: Telemetry data must be managed using Redis Lists. The engine must extract the rolling average of the last three metric samples to prevent momentary spikes from causing resource thrashing.

Atomic Evaluation (Lua Script): The entire read-average-clamp-write sequence must run within an atomic Redis Lua Script (EVAL) per deployment. This prevents race conditions where dual concurrent evaluation cycles attempt to scale the same workload based on identical stale data.

Scale Evaluation: The HPA loop calculates the desired pod population using the target CPU formula:

$$\text{Desired Replicas} = \left\lceil \text{Current Replicas} \times \left( \frac{\text{Average CPU Metric}}{\text{Target CPU Metric}} \right) \right\rceil$$

Boundary Validation & Clamping: Calculated counts must be strictly clamped:

$$\text{min\_replicas} \le \text{Desired Replicas} \le \text{max\_replicas}$$

FR-4: Dual-Mode Self-Healing & Lease Recovery

Pod Level Recovery: The local Node Agent must run non-blocking TCP socket or HTTP endpoint checks directly against running child processes. If a process exits unexpectedly or consecutive checks fail, the Node Agent must execute a local restart.

Backoff Rate Limiting: Consecutive child process crash failures must trigger an exponential wait delay ($5\text{s} \rightarrow 10\text{s} \rightarrow 20\text{s} \rightarrow \dots$) up to $5\text{m}$ to prevent host-resource depletion.

Dual-Trigger Node Recovery:

Edge-Triggered Path: The Control Plane Health Controller must subscribe to Redis keyspace notifications (notify-keyspace-events Ex). Upon receiving an expiration signal for node:{node_id}:status, it schedules an eviction pipeline. Due to Redis's active expiration sampling interval, this event will fire within 1–2 seconds of expiration.

Level-Triggered Path (Periodic Sweep): To safeguard against transient connection drops where the subscription might miss an expiration signal, the controller executes a sweep every $30\text{s}$. It queries the explicit registry of nodes (nodes:registry) using non-blocking SCAN logic to verify the presence of status keys. Any missing leases trigger eviction.

Atomic Rescheduling Coordination: The node eviction, pod-state marking (Evicted), node capacity releasing, and scheduling triggers are executed inside an atomic Redis Lua script to prevent race conditions with ongoing Replica Controller sweeps.

FR-5: Real-time Telemetry Dashboard & Persistent Logging

Live Visual Canvas: The UI must display cluster topologies in real-time, showing Nodes, Pod status blocks (Running, Pending, Failed), live CPU utilization, and a scrolling historical event feed.

Native SSE Streaming Engine: The API server must stream state updates to the dashboard via a Server-Sent Events (SSE) connection, pushing incremental state updates.

Persistent Event Log Streams: System events and controller decisions must be committed to a capped Redis Stream (cluster:logstream). When a user accesses or refreshes the dashboard, the backend performs an XREVRANGE scan to retrieve and display the last 20 events instantly, preventing blank-slate state drops on page load.

Built-in Load Generator: The dashboard must include a configurable HTTP traffic generator capable of spawning concurrent, rate-ramped workloads to test autoscaling limits directly from the web panel.

FR-6: Dynamic Routing Engine with Reload Debouncing

Dynamic Ingress Controller: The host must run an Ingress Controller Daemon that subscribes directly to the deployment:events channel to watch for newly scaled or terminated Pod IP mappings.

Config Sync & Reload Debouncing: When scaling activities occur in rapid succession (e.g., during a spike scaling event), the Ingress Controller must debounce the reload execution. It buffers dynamic configurations over a $2\text{s}$ window, rewriting the Nginx upstream blocks and executing a single reload command (nginx -s reload) to handle the batch without thrashing connections.

Non-Functional Requirements (NFR)

NFR-1: Data Path and Plane Isolation

External traffic targeting application workloads must not degrade control plane performance. The telemetry and state routing pipelines must use separate communication paths (virtual interfaces, independent server threads, decoupled Redis connections) so that even under a surge of 1,000 Requests Per Second (RPS) (the physical capacity ceiling of the virtual testing network), the REST API and control plane loops run without latency spikes.

NFR-2: Flapping and Thrashing Mitigation

The system must enforce independent scaling timers:

Scale-Up Cooldown: $30\text{s}$. The system cannot trigger subsequent scale-up routines while recently created processes are completing cold boot or initialization.

Scale-Down Cooldown: $180\text{s}$. Prevents rapid container creation/deletion loops ("flapping") when traffic patterns are volatile.

NFR-3: Resource-Aware Scheduling & Protection Boundaries

Bounded Resource Allocation: The Control Plane Scheduler must enforce strict resource accounting. It retrieves the CPU and Memory requests declared in the deployment hash and maps them against available allocations in the node capacity hash.

Best-Fit Placement: A pod is only scheduled onto a node that possesses adequate unallocated resource headroom. If all nodes are saturated, the pod transitions to Pending. The scheduler must strictly refuse to overcommit physical boundaries beyond the defined limits.

NFR-4: Ephemeral Thread Robustness

The orchestration state must reside entirely inside the Redis server. If the Controller Manager processes or REST API threads crash, they must recover state instantly upon restart by reading the persistent structure of Redis, without disrupting running processes on the worker nodes.

3. High-Performance Redis Key-Value Schema

Redis keys are designed using structured namespaces to ensure extremely fast lookups and decoupled event notifications.

3.1 Workload Configurations (Hashes)

Holds the declarative deployment configuration, target scaling rules, and resource requests.

Key Format: deployment:{deployment_name}

Data Fields:

min_replicas: "2"
max_replicas: "10"
target_cpu_percentage: "70"
desired_replicas: "2"
exec_path: "/usr/local/bin/payment-api"
port: "80"
cpu_request: "200"        # Millicores (e.g., 200m = 20% of 1 core)
mem_request: "128"        # Megabytes


3.2 Live Node Registrations & Status

Node Registry Set: nodes:registry

Type: Redis Set containing registered Node IDs (e.g., ["node-1", "node-2", "node-3"]). This allows the fallback sweep to inspect nodes using O(Nodes) operations instead of blocking keyspace queries.

Lease Status Key: node:{node_id}:status

Type: String (Value: "Ready")

Lease Policy: Configured with an expiration window (EX 15). Node Agents refresh this key on a 5-second cadence.

3.3 Node Resource Capacity Tracking (Hashes)

Maintains current physical capacities and tracks allocated resources for scheduling constraints.

Key Format: node:{node_id}:capacity

Data Fields:

total_cpu: "1000"         # Max capacity in millicores (1 core = 1000m)
allocated_cpu: "400"      # Active allocation summation of scheduled pods
total_mem: "1024"         # Max memory capacity in MB
allocated_mem: "256"      # Active allocation summation of scheduled pods


3.4 Active Pod Metadata (Hashes)

Tracks the exact location, execution status, resource consumption, and historical data of active pod instances.

Key Format: pod:{deployment_name}:{pod_uuid}

Data Fields:

node_id: "node-2"
status: "Running"
ip_address: "10.0.5.12"
restart_count: "0"
last_heard: "171923456"
cpu_request: "200"
mem_request: "128"


3.5 Live Telemetry (Lists)

Maintains historical CPU readings to compute filtered moving averages.

Key Format: telemetry:{deployment_name}:{pod_uuid}:cpu

Structure: Redis List (LPUSH and LTRIM to keep the list size restricted to the 10 most recent values).

Telemetry Source: Node Agents read system statistics directly from cgroups (/sys/fs/cgroup/system.slice/) to completely bypass process metrics overhead.

3.6 Event Logging & Broadcasting

Channel deployment:events: Used to broadcast deployment target adjustments and pod IP additions.

Payload Structure: {"event": "UPDATE", "deployment": "payment-api", "desired": 5}

Capped Log Stream cluster:logstream: Persistent logging of orchestration activities.

Type: Redis Stream capped at 1000 items (XADD with MAXLEN ~ 1000).

Fields: time, level, component, msg

4. Emulated Network & Testing Topology

The testing architecture uses an Incus system container cluster running on a localized Linux host. This provides full process, network, and resource namespace isolation without the high overhead of virtual machines.

+-----------------------------------------------------------------------------------+
|                            DEVELOPMENT LINUX HOST MACHINE                         |
|                                                                                   |
|  +-----------------------------------------------------------------------------+  |
|  |                         VIRTUAL BRIDGE INTERFACE (incusbr0)                 |  |
|  |                         Network Block: 10.0.5.0/24                          |  |
|  +-----------------------------------------------------------------------------+  |
|         |                        |                    |                  |        |
|  (IP: 10.0.5.10)          (IP: 10.0.5.11)      (IP: 10.0.5.12)    (IP: 10.0.5.13) |
|         v                        v                    v                  v        |
|  +--------------+         +--------------+     +--------------+   +--------------+  |
|  |  incus-vm    |         |  incus-vm    |     |  incus-vm    |   |  incus-vm    |  |
|  |control-plane |         |    node-1    |     |    node-2    |   |    node-3    |  |
|  |              |         |              |     |              |   |              |  |
|  | - REST API   |         | - Node Agent |     | - Node Agent |   | - Node Agent |  |
|  | - Controllers|         | - Supervised |     | - Supervised |     - Supervised |  |
|  | - Redis DB   |         |   Processes  |     |   Processes  |   |   Processes  |  |
|  +--------------+         +--------------+     +--------------+   +--------------+  |
+-----------------------------------------------------------------------------------+


4.1 Cluster Allocation Specifications

Each simulated node is running Ubuntu 24.04 LTS inside a system container. By replacing nested Docker with supervised processes running directly in native namespaces, we free up roughly $150\text{MB}$ of idle RAM overhead per node, increasing overall stability.

Control Plane Node (control-plane):

CPU Capacity Limits: Restricted to 2 Cores.

Memory Space: Restricted to 2.0 GiB.

Ports Exposed: 8080 (Web UI/REST API Server), 6379 (Redis Instance).

Worker Node 1 (node-1):

CPU Capacity Limits: Restricted to 1 Core (1000m capacity).

Memory Space: Restricted to 1.0 GiB.

Worker Node 2 (node-2):

CPU Capacity Limits: Restricted to 1 Core (1000m capacity).

Memory Space: Restricted to 1.0 GiB.

Worker Node 3 (node-3):

CPU Capacity Limits: Restricted to 1 Core (1000m capacity).

Memory Space: Restricted to 1.0 GiB.

5. End-to-End Execution Trace

The trace below illustrates how the decoupled architecture handles a dynamic traffic surge, processes state updates, and isolates the Control Plane.

 [HTTP Rate-Ramped Traffic] ---> [Host Ingress Router (Nginx)] ---> Dynamic Routing to Process IPs
                                                                          |
+-------------------------------------------------------------------------+
|                                                                         v
|                                                            [Host App Processes Spike]
|                                                                    Avg CPU: 92%
|                                                                         |
|  Unified Telemetry & Keep-Alive Pipeline                                v
|  ======================================                    [Worker Node Agents]
|  1. Node agents read /sys/fs/cgroup/ statistics directly                |
|  2. Agent pipelines both Heartbeat EX and Telemetry LPUSH in 1 roundtrip|
|                                                                            v
|                                                            +------------------------+
|                                                            |    Redis Store         |
|                                                            |  - LPUSH & LTRIM List  |
|                                                            +------------------------+
|                                                                        |
|  Orchestration Pipeline (Decoupled Path)                               v
|  =======================================                  +------------------------+
|  3. Autoscaler executes on a 15-second timer               |  Autoscaler Controller |
|  4. Runs atomic EVAL Lua Script over telemetry lists       |  - Evaluates Target    |
|  5. Calculates desired target state                        |  - Updates State       |
|                                                            +------------------------+
|                                                                        |
|                                                                        v
|                                                            +------------------------+
|                                                            |  Redis Database        |
|                                                            |  - Updates desired: 4  |
|                                                            |  - Publishes Events    |
|                                                            +------------------------+
|                                                                        |
|  Reconciliation and Scaling Pipeline                                   v
|  ===================================                      +------------------------+
|  6. Replica Controller reacts instantly to Pub/Sub event    |   Replica Controller   |
|  7. Scheduler assigns the new process target to Node 3     |   - Matches capacity   |
|     by evaluating available resource allocations           |   - Sends Spin-Up Cmd  |
|  8. Node 3 Agent forks and supervises the new process      |                        |
|                                                            +------------------------+
|                                                                        |
|                                                                        v
|  9. Stream backfills dashboard logs using XREVRANGE ------> [Dashboard Live Updates]
+-----------------------------------------------------------------------------------+


6. Chaos Diagnostics & Operational Verification

Use these step-by-step diagnostic workflows inside your virtual testing environment to verify that both the Autoscaling and Auto-healing mechanics function correctly under stress.

6.1 Test Phase 1: Validating Pod-Level Healing

Objective: Verify that when a supervised process exits or fails, the local Node Agent heals the workload without involving the primary Control Plane.

Diagnostic Actions:

Access a shell on node-1:

incus exec node-1 -- bash


Locate and kill the target application process (e.g. Nginx/Go process):

ps aux | grep payment-api
kill -9 <pid>


Expected Result:

The Node Agent running on node-1 detects the child process death within $5\text{s}$ via its local process monitor.

The Node Agent automatically restarts the process binary with its declared parameters.

The restart_count field in the Redis pod metadata Hash increments by 1.

The change streams down to the Web UI SSE dashboard, showing a warning event and an updated restart count without any rescheduling latency.

6.2 Test Phase 2: Validating Cluster-Level Node Healing with Resource Verification

Objective: Verify that the Control Plane detects a node failure, evicts its active workloads, releases its allocated capacity from the tracking configurations, and schedules replicas onto surviving nodes with adequate free capacity.

Diagnostic Actions:

Shut down node-2 from your host shell to simulate a catastrophic hardware failure:

incus stop node-2


Monitor the active Redis state keys using the terminal:

redis-cli -h 10.0.5.10 -p 6379 KEYS "node:*:status"


Expected Result:

Within $15\text{s}$, the Redis key node:node-2:status expires. If the keyspace notification is missed, the level-triggered 30-second sweep detects the missing key by scanning nodes:registry.

The Control Plane Health Controller executes an atomic Lua evacuation script that marks all pods assigned to node-2 as Evicted and subtracts their resource allocations from node-2's capacity allocation tracking.

The Replica Controller schedules replacement workloads. The scheduler evaluates node resources and assigns replacement pods to node-1 or node-3 depending on available millicores and memory headroom.

The Web UI logs the events (retrieved dynamically via the persistent stream) and displays the updated topology canvas.

6.3 Test Phase 3: Stress-Testing HPA Autoscaling with Rate Ramping

Objective: Verify that the system dynamically scales pod counts up and down in response to metric spikes, while successfully mitigating flapping.

Diagnostic Actions:

Open the Web Dashboard, navigate to the Deployment panel, and register a deployment with:

min_replicas: 2, max_replicas: 6, target_cpu_percentage: 50

cpu_request: 150m (0.15 core)

On the Load Testing panel, select the deployment and configure an HTTP load that ramps up gradually over time (e.g., $10 \rightarrow 50 \rightarrow 200 \rightarrow 500\text{ RPS}$) to stay within your physical resource bounds.

Expected Result:

Telemetry values reported in Redis spike to 90%.

Within $15\text{s}$, the Autoscaler Engine computes the scale requirement via the Lua atomic evaluation loop:

$$\text{Desired Replicas} = \left\lceil 2 \times \left( \frac{90}{50} \right) \right\rceil = 4$$

The Autoscaler updates the desired state in Redis. The Replica Controller reacts instantly and schedules 2 new pods onto nodes containing at least 150m of unallocated CPU capacity.

Stop the load generator. Average metrics fall back down to 10%.

Cooldown Verification: The system must refuse to delete any pods for at least 180 seconds (the Scale-Down Cooldown window). Once this cooldown timer expires, the replica count must safely scale back down to 2, validating the system's anti-flapping guardrails.
