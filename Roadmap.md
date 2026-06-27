# Nova: Development Roadmap & Contributor Guide

Welcome to the Nova roadmap. This document outlines the strategic milestones, technical deliverables, and timeline for building our bare-metal distributed orchestrator. 

Our primary goal is to build a high-performance, fault-tolerant system that strictly enforces hardware limits via Linux kernel features (`cgroups`, namespaces) rather than relying on cloud provider abstractions.

---

## 🎯 High-Level Timeline

To maintain momentum and ensure a Minimum Viable Product (MVP) is operational for upcoming technical reviews, we are executing in four distinct phases:

* **Phase 1: The Control Plane (State & API)** — Establishing the single source of truth.
* **Phase 2: The Node Agent (Hardware Execution)** — Interfacing with the host OS.
* **Phase 3: The Intelligence (Scheduling & Reconciliation)** — Automating system fault tolerance.
* **Phase 4: The Network (Ingress Routing)** — Managing live data traffic.

---

## 🗺️ Detailed Milestones

### Phase 1: The Control Plane (State & API)
**Goal:** Define the data structures and build the entry point for user intent. The entire system relies on these schema definitions.

- [ ] **1.1 Database Schema Design**
  - Define the exact JSON schema for `Job` (Desired State).
  - Define the exact JSON schema for `Node` (Current State / Telemetry).
- [ ] **1.2 State Store Implementation**
  - Set up the centralized key-value store (Redis or etcd).
  - Write the data access layer (CRUD operations) to interface with the store.
- [ ] **1.3 API Gateway (`nova-api`)**
  - Implement a REST/gRPC server.
  - Create the `/api/v1/jobs` POST endpoint to accept workload requests.
  - Ensure the API validates resource limits (CPU/RAM) before writing to the State Store as `Pending`.

### Phase 2: The Node Agent (Hardware Execution)
**Goal:** Build the daemon that runs on the worker machines. This is where high-performance computing principles apply, ensuring workloads are strictly isolated.

- [ ] **2.1 Hardware Telemetry**
  - Write OS-level scripts to read host motherboard data (Total CPU cores, Total RAM).
  - Implement a heartbeat loop that writes this telemetry to the State Store every 5 seconds.
- [ ] **2.2 Workload Execution Engine**
  - Implement the logic to pull target container images (e.g., from Docker Hub).
  - Write the execution wrapper to start the container runtime.
- [ ] **2.3 Kernel Isolation (`cgroups`)**
  - Map the user's requested CPU/RAM limits to Linux `cgroups` commands.
  - Ensure the agent physically throttles the container upon startup to prevent node starvation.
  - Update the database state to `Running` upon successful execution.

### Phase 3: The Intelligence (Scheduling & Reconciliation)
**Goal:** Remove the human from the loop. Build the controllers that make Nova a true, self-healing orchestrator.

- [ ] **3.1 Resource-Aware Scheduler (`nova-scheduler`)**
  - Build a background process that polls for `Pending` jobs.
  - Implement the scoring algorithm: Compare required job limits against real-time node availability.
  - Assign the optimal `node_id` to the job in the database.
- [ ] **3.2 The Reconciliation Loop (`nova-controller`)**
  - Build the infinite loop (`while true`) that compares Desired State vs. Current State.
  - Implement crash detection: If a node's heartbeat goes stale, automatically rewrite its assigned jobs back to `Pending` for rescheduling.

### Phase 4: The Network (Ingress Routing)
**Goal:** Enable external traffic to reach the isolated workloads dynamically.

- [ ] **4.1 Dynamic Reverse Proxy**
  - Deploy a central edge proxy (NGINX/Envoy).
  - Build the `nova-router` controller that watches the database for new `Running` jobs.
- [ ] **4.2 Network Bridging (`iptables`)**
  - Automate the mapping of physical host IPs to virtual container namespace IPs.
  - Trigger hot-reloads of the proxy configuration without dropping active connections.

---

## 🤝 How to Contribute

We divide tasks based on these components to prevent merge conflicts and step on each other's toes. 

1. **Pick a Component:** Decide if you want to work on the Data layer (API/Store), the System layer (Agent/cgroups), or the Logic layer (Scheduler/Controller).
2. **Open an Issue:** Before writing code, open a GitHub issue detailing the specific function or struct you are building.
3. **Draft the Contract:** In the issue, write out the expected input/output (the API contract) so others can build against it while you work.
4. **Branch & PR:** Create a branch for your feature (`feature/api-validation`), build it, and submit a Pull Request.

**Note on Testing:** Because our components communicate entirely via the State Store, you do not need the whole cluster running to test your piece. You can manually inject a mock JSON payload into the database to test if your specific controller reacts correctly.
