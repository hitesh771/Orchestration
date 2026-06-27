# Nova: Bare-Metal Distributed Compute Orchestrator

[![Go Report Card](https://goreportcard.com/badge/github.com/yourusername/nova)](https://goreportcard.com/report/github.com/yourusername/nova)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Nova** is a custom, high-performance orchestration platform built from scratch to manage bare-metal compute resources. 

Moving beyond cloud provider abstractions, Nova is designed to interface directly with the host operating system. It handles decentralized hardware profiling, strictly enforces CPU/Memory limits via kernel features, dynamically routes live network traffic, and maintains system integrity through an automated reconciliation loop.

---

## 💻 Target Environment & Machine Specs

Nova is designed strictly for **Linux** environments. While you can develop the control plane on Windows/macOS, the Node Agent requires a Linux kernel to execute workloads. 

The system relies on three core OS-level technologies:
1. **`cgroups` (Control Groups):** To mathematically throttle CPU cores and RAM allocation per job.
2. **Linux Namespaces:** To isolate the workload's filesystem and process tree.
3. **`iptables` / Virtual Bridges:** To route physical network ingress into isolated container sockets.

---

## 🏗️ System Architecture & Visual Pipelines

Nova splits its operations into two highly decoupled paths: **The Control Pipeline** (managing state) and **The Ingress Pipeline** (managing live user traffic).

### 1. The Control Pipeline (Reconciliation Loop)
This asynchronous loop is the "brain" of Nova. No human intervention is required; the components constantly read from and write to the central State Store to ensure reality matches the user's desires.

```mermaid
graph TD
    User([User / Developer]) -->|POST /jobs| API[Nova API]
    API -->|Writes Desired State| DB[(State Store: Redis/etcd)]
    
    Controller[Nova Controller] <-->|Checks Desired vs Current| DB
    
    DB -->|Polls Pending Jobs| Sched[Nova Scheduler]
    Sched -->|Assigns Node ID| DB
    
    DB -->|Reads Assignment| Agent[Nova Node Agent]
    Agent -->|Enforces limits| OS[Linux cgroups & runtime]
    Agent -->|Writes Current State| DB
