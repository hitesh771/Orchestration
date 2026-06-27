# Nova: Internal Architecture & Systems Blueprint

This document is the definitive engineering guide for Nova. It outlines our core design philosophy, component boundaries, and the strict data flows required to build a fault-tolerant, bare-metal orchestrator. 

If you are a contributor writing code for this system, this is your source of truth.

---

## 🧠 1. Core Design Philosophy

To ensure Nova remains highly performant and scalable, all code must adhere to these three principles:

1. **Absolute Decoupling via State:** No two components ever communicate directly with each other (e.g., the API never pings a Node Agent). All components communicate *exclusively* by reading from and writing to the central State Store.
2. **Stateless Controllers:** If a controller (API, Scheduler, Reconciler) crashes, it should be able to restart, read the State Store, and immediately resume its duties without losing any data. No component should store active state in local memory.
3. **Level-Triggered, Not Edge-Triggered:** Controllers do not just react to events (like a node crashing). They constantly poll the database in an infinite loop, comparing the "Desired State" against the "Current State," and act on the delta.

---

## 🧩 2. Component Boundaries

Nova is divided into three distinct execution planes. When writing code, ensure your logic stays strictly within its designated boundary.

### A. The Storage Plane (The Source of Truth)
* **Component:** `nova-store` (Redis / etcd)
* **Responsibility:** Holds the entire reality of the cluster.
* **Data Schemas:** Stores `Jobs` (Desired Workloads) and `Nodes` (Available Hardware). It does NOT store container images or application data.

### B. The Control Plane (The Brains)
These components can run anywhere and orchestrate the cluster.
* **`nova-api`:** The ingress gateway. Validates user JSON requests and writes `Pending` jobs to the store. 
* **`nova-scheduler`:** The matchmaker. Reads `Pending` jobs and reads all `Node` availability. Runs a scoring algorithm to find the best hardware fit and updates the job's `assigned_node`.
* **`nova-controller`:** The watchdog. Constantly checks for anomalies (e.g., a node hasn't updated its heartbeat in 15 seconds) and rewrites orphaned jobs back to `Pending`.

### C. The Data Plane (The Muscle)
These components run strictly on the Linux worker machines.
* **`nova-agent`:** The daemon. Watches the store for jobs assigned to its specific `node_id`. It interfaces directly with Linux OS primitives:
  * Pulls the container image.
  * Uses `cgroups` to enforce CPU and memory limits.
  * Spawns the process inside a Linux Namespace.
  * Reports real-time hardware telemetry back to the store.
* **`nova-router`:** The network bridge. Dynamically rewrites proxy (NGINX/Envoy) configs to route incoming traffic from the physical Node IP into the specific virtual container namespaces.

---

## 🔄 3. The Workload Lifecycle (Data Flow)

When a user requests a job, the system executes a strict, asynchronous chain of events. Do not bypass this flow.

1. **Intent (API):** * User POSTs a job requesting 4 CPU cores. 
   * `nova-api` writes: `{job_id: 123, status: "Pending", assigned_node: null}`.
2. **Allocation (Scheduler):** * `nova-scheduler` sees the Pending job. 
   * It scans the nodes, sees Node-A has 6 cores available. 
   * It writes: `{job_id: 123, status: "Pending", assigned_node: "Node-A"}`.
3. **Execution (Agent):** * `nova-agent` (running on Node-A) sees a job assigned to it. 
   * It configures `cgroups`, starts the container, and updates the database.
   * It writes: `{job_id: 123, status: "Running"}`. 
   * *Crucially, it also reduces Node-A's available CPU cores by 4 in the State Store.*
4. **Networking (Router):** * `nova-router` sees a new `Running` job.
   * It updates local `iptables` and proxy configs so external traffic can hit the new container.

---

## 🛠️ 4. Tech Stack & Implementation Details

* **Language:** C++ or Go (Required for high-concurrency controllers and low-level system calls).
* **OS Target:** Linux strictly (for `cgroups` and `namespaces` access).
* **Communication:** JSON over HTTP/REST (for API) and native client libraries for the State Store.

### Directory Structure & Code Ownership

When claiming a task, work within these designated modules:

```text
nova/
├── cmd/                  # Main entry points for all binaries
│   ├── api/              
│   ├── scheduler/        
│   └── agent/            
├── pkg/                  # Reusable internal libraries
│   ├── store/            # DB connection and CRUD wrappers (EVERYONE uses this)
│   ├── sys/              # Linux syscalls, cgroup management (Agent team)
│   ├── algo/             # Resource scoring logic (Scheduler team)
│   └── types/            # JSON Structs for Jobs/Nodes (Source of Truth)
