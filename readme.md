<h1 align="center">interlink-provider-cloudvm</h1>

<p align="center">
  <!-- Implementation -->
  <!-- Language - Go -->
  <a href="https://go.dev/">
    <img src="https://img.shields.io/badge/language-Go-00ADD8?logo=go&amp;logoColor=white" alt="Written in Go">
  </a>
  <!-- Toolchain - mise -->
  <a href="https://mise.jdx.dev/">
    <img src="https://img.shields.io/badge/toolchain-mise-8B5CF6?logo=iterm2&amp;logoColor=white" alt="mise toolchain">
  </a>
  <br>
  <!-- Verification -->
  <!-- CI - GitHub Actions -->
  <a href="https://github.com/simonepri/interlink-provider-cloudvm/actions/workflows/ci.yml">
    <img src="https://img.shields.io/github/actions/workflow/status/simonepri/interlink-provider-cloudvm/ci.yml?branch=main&amp;label=CI&amp;logo=githubactions&amp;logoColor=white" alt="CI status">
  </a>
  <br>
  <!-- Distribution -->
  <!-- Release - Release Please -->
  <a href="https://github.com/googleapis/release-please">
    <img src="https://img.shields.io/badge/released_with-Release_Please-4285F4?logo=google&amp;logoColor=white" alt="Released with Release Please">
  </a>
  <!-- Packaging - Helm -->
  <a href="https://helm.sh/">
    <img src="https://img.shields.io/badge/packaging-Helm_OCI-0F1689?logo=helm&amp;logoColor=white" alt="Helm OCI chart">
  </a>
  <!-- Container - Docker -->
  <a href="https://www.docker.com/">
    <img src="https://img.shields.io/badge/container-Docker-2496ED?logo=docker&amp;logoColor=white" alt="Docker container image">
  </a>
  <!-- License - Apache 2.0 -->
  <a href="LICENSE">
    <img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="Apache 2.0 license">
  </a>
</p>

<p align="center">
  ⚡ Ultra-fast, multi-cloud Spot and On-Demand VM bursting for Kubernetes via CNCF InterLink.
</p>

---

## Overview

`interlink-provider-cloudvm` is an open-source provider plugin for **[CNCF InterLink](https://github.com/interlink-hq/interLink)**. It allows Kubernetes clusters to burst computational workloads (such as distributed Ray clusters, PyTorch training, or batch genomics pipelines) directly onto opportunistic cloud virtual machines (**AWS EC2 Fleet**, **GCP Compute Engine**) without running local node pools or waiting for traditional Cluster Autoscalers.

```mermaid
flowchart TD
    classDef k8s fill:#1E293B,stroke:#38BDF8,stroke-width:2px,color:#F8FAFC
    classDef daemon fill:#311042,stroke:#C084FC,stroke-width:2px,color:#F8FAFC
    classDef cloud fill:#0C4A6E,stroke:#38BDF8,stroke-width:2px,color:#F8FAFC
    classDef vm fill:#064E3B,stroke:#34D399,stroke-width:2px,color:#F8FAFC

    subgraph K8s["1. Kubernetes Workload Tier"]
        Workload["Workload\n(Kueue / Ray / PyTorch / Pod)"]:::k8s
        VK["InterLink Virtual Kubelet\n(virtual-node)"]:::k8s
        Workload -->|Schedules on virtual-node| VK
    end

    subgraph Provider["2. Provider Daemon Tier (:4040)"]
        Server["InterLink REST API Server"]:::daemon
        Solver["Spot & Fleet Solver"]:::daemon
        Reconciler["Bidirectional Reconciler"]:::daemon
        Server --> Solver
        Server <--> Reconciler
    end

    subgraph Infrastructure["3. Multi-Cloud Infrastructure Tier"]
        AWS["AWS EC2 Fleet\n(Spot / On-Demand)"]:::cloud
        GCP["GCP Compute Engine\n(Bulk Insert)"]:::cloud
        VMs["1:1 Ephemeral Workers\n(Fast-Boot Cloud VMs)"]:::vm
        AWS --> VMs
        GCP --> VMs
    end

    VK -->|REST POST /create| Server
    Solver --> AWS
    Solver --> GCP
```

---

## Core Subsystems

### 1. Spot Portfolio Solver & Weighted Capacity

When bidding for spot capacity across heterogeneous GPU instance families (`g4dn`, `g5`, `g6`, `p4d`, `p5`), default cloud fleet APIs treat each instance as `1 unit`. Without weighting, requesting 8 units from a fleet containing `g5.12xlarge` (4 GPUs each) would launch 8 instances with 32 GPUs total, multiplying cloud costs fourfold.

The solver calculates exact **Weighted Capacity** based on physical accelerator count:

$$\\sum (\\text{count}_i \\times \\text{weight}_i) = \\text{TargetCapacity}$$

```mermaid
flowchart LR
    classDef req fill:#1E293B,stroke:#38BDF8,stroke-width:2px,color:#F8FAFC
    classDef solver fill:#311042,stroke:#C084FC,stroke-width:2px,color:#F8FAFC
    classDef cloud fill:#0C4A6E,stroke:#38BDF8,stroke-width:2px,color:#F8FAFC

    Req["Pod Spec\n(Requested GPUs & Families)"]:::req --> Solver["Spot Portfolio Solver"]:::solver

    subgraph Weights["Calculated Weighted Capacity"]
        direction TB
        W1["g5.12xlarge / g6.12xlarge (Weight: 4.0)"]
        W2["p4d.24xlarge / p5.48xlarge (Weight: 8.0)"]
        W3["g5.xlarge / g6.xlarge (Weight: 1.0)"]
    end
    Solver --> Weights

    Weights --> AWS["AWS EC2 Fleet API\n(price-capacity-optimized)"]:::cloud
    Weights --> GCP["GCP Compute Engine\n(locationPolicy: ANY_SINGLE_ZONE)"]:::cloud
```

| Instance Family | Physical GPUs | Calculated Weight | Fulfillment for 8 Requested GPUs |
| :--- | :---: | :---: | :--- |
| `g5.xlarge` / `g6.xlarge` | 1 | **1.0** | Provisions 8 VMs (8 × 1 = 8 GPUs) |
| `g5.12xlarge` / `g6.12xlarge` | 4 | **4.0** | Provisions 2 VMs (2 × 4 = 8 GPUs) |
| `p4d.24xlarge` / `p5.48xlarge` | 8 | **8.0** | Provisions 1 VM (1 × 8 = 8 GPUs) |

#### Fleet Allocation Strategies

- **`price-capacity-optimized` (AWS)**: Analyzes historical spot interruption data to identify deep capacity pools, then selects the lowest-priced pool among them. This provides 70–90% cost savings with the lowest preemption frequency.
- **`locationPolicy: ANY_SINGLE_ZONE` (GCP)**: Dynamically selects whichever availability zone in a target region currently has sufficient spot inventory to fulfill the entire requested batch, avoiding inter-zone network egress latency and cross-zone traffic costs.

---

### 2. In-VM Ephemeral Lifecycle & Fast-Boot

Traditional Kubernetes nodes take 3–5 minutes to become ready: VM boot (~45s), Kubelet and CNI initialization (~45s), and pulling 10–15GB CUDA/PyTorch container images over the network (60–180s). During spot preemption, this delay halts distributed computation.

`interlink-provider-cloudvm` introduces **Native DLAMI Fast-Boot**:
- **DLAMI (Deep Learning Amazon Machine Image)**: Official cloud images with pre-installed NVIDIA drivers, CUDA toolkits, and PyTorch runtimes directly on disk.
- **~33-Second Cold Start**: By running natively within the pre-warmed DLAMI environment, the worker process launches directly without pulling heavy container layers, achieving cold-start-to-cluster readiness in ~33 seconds.
- **Container Mode**: Standard container execution (Docker / Containerd) remains available for custom images.

```mermaid
flowchart TD
    classDef init fill:#1E293B,stroke:#38BDF8,stroke-width:2px,color:#F8FAFC
    classDef runtime fill:#064E3B,stroke:#34D399,stroke-width:2px,color:#F8FAFC
    classDef watch fill:#451A03,stroke:#F59E0B,stroke-width:2px,color:#F8FAFC
    classDef exit fill:#4C0519,stroke:#F43F5E,stroke-width:2px,color:#F8FAFC

    Start["1. Cloud-Init Bootstrap"]:::init --> Mesh["Join Private Mesh\n(Tailscale / WireGuard)"]:::init
    Mesh --> Arm["Arm 300s Fail-Safe Watchdog"]:::watch
    
    Arm --> Mode{"Execution Mode"}
    Mode -->|"DLAMI Fast-Boot (~33s)"| Native["Native PyTorch / ML Worker"]:::runtime
    Mode -->|"Container Mode"| Docker["Docker / Containerd Image"]:::runtime

    Native --> Health["Asynchronous Healthcheck"]:::runtime
    Docker --> Health
    Health -->|Touches /tmp/workload-ready| Disarm["Disarms Fail-Safe Watchdog"]:::runtime

    subgraph Monitors["Active In-VM Daemons"]
        direction LR
        Preempt["Spot Interruption Monitor\n(IMDSv2 & Cloud Metadata)"]:::watch
        Console["Serial Console Streamer\n(/dev/console JSON logs)"]:::watch
    end
    Native --- Monitors

    Preempt -->|"Termination Warning"| Drain["Drain Workload & Flush"]:::exit
    Drain --> Terminate["Self-Destruct (poweroff)"]:::exit
    Native -->|"Task Completed"| Terminate
```

#### In-VM Watchdogs & Telemetry
- **2-Minute Preemption Monitor**: Continuously polls AWS IMDSv2 metadata (`/latest/meta-data/spot/instance-action`) and GCP metadata (`/computeMetadata/v1/instance/preempted`) to trigger graceful worker drains before eviction.
- **300s Fail-Safe Self-Destruct**: Shuts down the machine if workload initialization fails within 5 minutes, preventing budget leaks from orphaned instances.
- **Serial Console Telemetry**: Streams structured status heartbeats to `/dev/console`, allowing `kubectl logs` and status tracking without maintaining open SSH ports or credentials.

---

### 3. Bidirectional Reconciler (Garbage Collection)

An asynchronous reconciliation loop runs every 60 seconds to synchronize Kubernetes Pod state with live cloud provider inventory:

```mermaid
flowchart TD
    classDef rec fill:#311042,stroke:#C084FC,stroke-width:2px,color:#F8FAFC
    classDef k8s fill:#1E293B,stroke:#38BDF8,stroke-width:2px,color:#F8FAFC
    classDef cloud fill:#0C4A6E,stroke:#38BDF8,stroke-width:2px,color:#F8FAFC
    classDef act fill:#4C0519,stroke:#F43F5E,stroke-width:2px,color:#F8FAFC

    subgraph State["State Sources"]
        CloudInv["Live Cloud Inventory\n(AWS EC2 & GCP APIs)"]:::cloud
        K8sPods["Active Virtual Pods\n(InterLink API / K8s etcd)"]:::k8s
    end

    CloudInv --> Reconciler["Background Reconciler\n(Runs every 60s)"]:::rec
    K8sPods --> Reconciler

    Reconciler --> Check1{"Instance exists in Cloud,\nbut Pod vanished from K8s?"}
    Check1 -->|"Yes & Age > Grace TTL"| KillZombie["Terminate Zombie VM\n(Prevents budget leak)"]:::act

    Reconciler --> Check2{"Pod active in K8s,\nbut Instance terminated in Cloud?"}
    Check2 -->|"Yes (Preempted / Watchdog Exited)"| EvictPod["Mark Pod Failed / Evict\n(Triggers K8s Reschedule)"]:::act
```

---

## Usage

Workloads target the `cloudvm` virtual node using standard InterLink annotations:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: gpu-inference-worker
  annotations:
    cloudvm-job.vk.io/provider: "aws"
    cloudvm-job.vk.io/capacity-type: "spot"
    cloudvm-job.vk.io/ami-mode: "dlami"
    cloudvm-job.vk.io/instance-type: "g5.xlarge"
spec:
  nodeSelector:
    kubernetes.io/role: cloudvm
  tolerations:
    - key: virtual-node.interlink/no-schedule
      operator: Exists
      effect: NoSchedule
  containers:
    - name: worker
      resources:
        limits:
          nvidia.com/gpu: "1"
```

---

## Installation via Helm

```bash
helm upgrade --install interlink-provider-cloudvm \
  oci://ghcr.io/simonepri/charts/interlink-provider-cloudvm \
  --namespace interlink \
  --create-namespace \
  --set provider.type=aws \
  --set provider.awsRegion=us-east-1
```

---

## Development

```bash
# Install toolchain
mise install

# Run static checks and unit tests
mise run verify

# Build binary
mise run build
```

---

## License

[Apache 2.0](LICENSE) © 2026 Simone Primarosa
