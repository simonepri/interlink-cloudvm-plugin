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

## 1. High-Level Concept

`interlink-provider-cloudvm` is an open-source provider plugin for **[CNCF InterLink](https://github.com/interlink-hq/interLink)**. 

Its primary purpose is to solve the **Spot GPU bottleneck** in Kubernetes: allowing clusters to instantly tap into global, multi-region spot GPU fleets (e.g. NVIDIA A10G, L4, L40S, H100) with zero local node-pool overhead. It also seamlessly handles general-purpose CPU workloads (such as data preprocessing, ETL, and Ray data pipelines).

```mermaid
flowchart LR
    classDef k8s fill:#1E293B,stroke:#38BDF8,stroke-width:2px,color:#F8FAFC
    classDef plugin fill:#311042,stroke:#C084FC,stroke-width:2px,color:#F8FAFC
    classDef cloud fill:#064E3B,stroke:#34D399,stroke-width:2px,color:#F8FAFC

    A["1. Kubernetes Workload\n(Ray / PyTorch / Pod)"]:::k8s 
    -->|"Schedules on\nvirtual-node"| B["2. interlink-provider-cloudvm\n(CNCF InterLink Plugin)"]:::plugin
    -->|"Provisions 1:1 Cloud VM\n(AWS Fleet / GCP Bulk)"| C["3. Ephemeral Cloud VM\n(Runs workload & terminates)"]:::cloud
```

### The Problem It Solves

| | Traditional Kubernetes (Cluster Autoscaler / Karpenter) | `interlink-provider-cloudvm` |
| :--- | :--- | :--- |
| **Execution Model** | Provisions full nodes $\\to$ registers `kubelet` $\\to$ pulls images $\\to$ schedules pods | **1 Pod = 1 Dedicated Cloud VM** |
| **GPU Cold-Start** | 3–5 minutes (node bootstrap + pulling 15GB CUDA container images) | **~33 seconds** (native pre-baked DLAMI execution) |
| **Geographic Scope** | Single VPC / Single Region | **Cross-Region & Multi-Cloud Fleet Arbitrage** |
| **Idle Cost** | Keeps empty nodes warm; consolidation and scale-down delay | **$0** (VM terminates 2 seconds after the task process exits) |
| **Workload Scope** | Any Kubernetes workload | **Optimized for Spot GPUs**, fully supports CPU pipelines |

---

## 2. How It Works (Step-by-Step)

A workload progresses through three clean stages:

```mermaid
flowchart LR
    classDef stage fill:#1E293B,stroke:#38BDF8,stroke-width:2px,color:#F8FAFC

    S1["<b>1. Scheduling</b><br>Workload schedules onto<br>virtual-node via Kueue / Ray"]:::stage
    --> S2["<b>2. Bidding</b><br>Solver calculates weights &<br>launches Spot Fleet API"]:::stage
    --> S3["<b>3. Execution</b><br>VM boots in ~33s, runs task,<br>and terminates 2s after exit"]:::stage
```

### Core Engines

To deliver this, `interlink-provider-cloudvm` is built on three core internal engines:

1. **Spot Portfolio Solver**: Translates pod resource requirements into multi-family weighted fleet requests (`CreateFleet` / `bulkInsert`) to bid at the lowest spot price without overprovisioning.
2. **In-VM Ephemeral Bootstrap & Watchdogs**: A hardened cloud-init engine providing fast native boot (~33s), P2P WireGuard mesh networking, a 2-minute preemption watcher, and a self-destruct watchdog.
3. **Bidirectional Reconciler**: A background garbage collector that terminates orphan/zombie cloud VMs if pods vanish, and evicts pods from Kubernetes if cloud instances are preempted.

---

## 3. Subsystem Deep Dives

### A. Spot Portfolio Solver & Weighted Capacity

When bidding for spot capacity across diversified GPU instance families (`g4dn`, `g5`, `g6`, `g6e`, `p4d`, `p5`), cloud fleet APIs count **instances**, not **GPUs**. Without weighting, requesting 8 units from a fleet containing `g5.12xlarge` (4 GPUs each) would launch 8 VMs with 32 GPUs total, quadrupling costs.

The solver computes **Weighted Capacity** based on physical accelerator counts:

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

#### Multi-Cloud Placement Strategies
- **`price-capacity-optimized` (AWS EC2 Fleet)**: Filters for spot pools with the deepest available inventory (lowest preemption risk), then allocates the lowest-priced pool among them. Delivers 70–90% cost savings with industry-leading spot stability.
- **`locationPolicy: ANY_SINGLE_ZONE` (GCP Compute Engine)**: Automatically places distributed worker batches in whichever single availability zone has enough spot capacity to host the entire group, eliminating cross-zone egress latency and inter-zone networking fees.

---

### B. In-VM Ephemeral Lifecycle & Execution Modes

`interlink-provider-cloudvm` supports two runtime modes:

1. **Native DLAMI Fast-Boot (Optimized for GPU ML Workloads)**:
   - Utilizes the official **AWS Deep Learning AMI (DLAMI)**, which comes with NVIDIA drivers, CUDA, and PyTorch pre-installed on disk.
   - Bypasses pulling heavy 10–15GB container images over the network.
   - Reaches cluster-ready status in **~33 seconds**.
2. **Container Mode (Optimized for CPU & Custom Workloads)**:
   - Uses standard Docker/Containerd execution for CPU pipelines, data transformations, or custom application containers.
   - Ideal for Ray Data pipelines, ETL jobs, and batch CPU processing.

```mermaid
flowchart TD
    classDef init fill:#1E293B,stroke:#38BDF8,stroke-width:2px,color:#F8FAFC
    classDef runtime fill:#064E3B,stroke:#34D399,stroke-width:2px,color:#F8FAFC
    classDef watch fill:#451A03,stroke:#F59E0B,stroke-width:2px,color:#F8FAFC
    classDef exit fill:#4C0519,stroke:#F43F5E,stroke-width:2px,color:#F8FAFC

    Start["1. Cloud-Init Bootstrap"]:::init --> Mesh["Join Private Mesh\n(Tailscale / WireGuard)"]:::init
    Mesh --> Arm["Arm 300s Fail-Safe Watchdog"]:::watch
    
    Arm --> Mode{"Execution Mode"}
    Mode -->|"DLAMI Fast-Boot (~33s)\nGPU Accelerators"| Native["Native PyTorch / ML Worker"]:::runtime
    Mode -->|"Container Mode\nCPU Pipelines & Custom Images"| Docker["Docker / Containerd Image"]:::runtime

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

#### Watchdogs & Telemetry
- **2-Minute Preemption Monitor**: Continuously polls AWS IMDSv2 metadata (`/latest/meta-data/spot/instance-action`) and GCP metadata (`/computeMetadata/v1/instance/preempted`) to cleanly drain workers and checkpoint state prior to eviction.
- **300s Fail-Safe Self-Destruct**: Shuts down the machine if workload initialization fails within 5 minutes, preventing budget leaks from orphaned instances.
- **Serial Console Telemetry**: Emits structured heartbeats to `/dev/console` (read via cloud serial port APIs), enabling `kubectl logs` without open SSH ports or credentials.

---

### C. Bidirectional Reconciler (Garbage Collection)

An asynchronous background loop runs every 60 seconds to synchronize Kubernetes Pod state with live cloud inventory:

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

## 4. Running Node Daemons on Ephemeral VMs

In traditional Kubernetes, infrastructure tools run as cluster-wide **DaemonSets**. Because `interlink-provider-cloudvm` provisions pure 1:1 cloud instances without a local Kubelet, DaemonSets do not schedule on ephemeral VMs.

Instead, the provider uses a **Split Observability & Security Architecture**:

```mermaid
flowchart TD
    classDef vm fill:#064E3B,stroke:#34D399,stroke-width:2px,color:#F8FAFC
    classDef daemon fill:#132E27,stroke:#10B981,stroke-width:1px,color:#E2E8F0
    classDef mesh fill:#1E293B,stroke:#38BDF8,stroke-width:2px,color:#F8FAFC
    classDef central fill:#311042,stroke:#C084FC,stroke-width:2px,color:#F8FAFC

    subgraph VM["Ephemeral Cloud VM Worker"]
        Workload["Workload Process\n(Ray / PyTorch / Container)"]:::vm
        
        subgraph Agents["In-VM Daemons (Pre-baked AMI / Cloud-Init)"]
            direction TB
            OTEL["OTEL Host Agent\n(Host & GPU Metrics)"]:::daemon
            OBI["OBI (OpenTelemetry eBPF)\n(L4 Socket & Network Flows)"]:::daemon
            Parca["Parca Agent\n(Kernel eBPF Continuous Profiling)"]:::daemon
            Falco["Falco Modern eBPF\n(Runtime Security & Syscall Audit)"]:::daemon
            Dragonfly["Dragonfly dfdaemon\n(P2P Image & Artifact Proxy)"]:::daemon
        end
    end

    subgraph Mesh["Encrypted Private Mesh (Tailscale / WireGuard)"]
        direction LR
        M1["Secure P2P Overlay Net"]:::mesh
    end

    subgraph Cluster["Central Kubernetes Cluster Infrastructure"]
        direction TB
        C1["OTEL Collector Deployment\n(SigNoz / Prometheus / Datadog)"]:::central
        C2["Parca Server\n(Continuous CPU/Memory Flamegraphs)"]:::central
        C3["FalcoSidekick / SIEM\n(Security Alerts & Audit Log)"]:::central
        C4["Dragonfly Supernode\n(Peer-to-Peer Distribution Seed)"]:::central
    end

    Agents -->|WireGuard / Tailscale| Mesh
    Mesh --> Cluster
```

### How Standard Infrastructure Daemons Run:

| Tool / Daemon | Traditional K8s DaemonSet | In Ephemeral CloudVM | Telemetry & Data Path |
| :--- | :--- | :--- | :--- |
| **OpenTelemetry (OTEL)** | OTEL DaemonSet scraping pods | Injected via pod env vars or host agent (`OTEL_EXPORTER_OTLP_ENDPOINT`) | OTLP gRPC over private mesh $\\to$ central in-cluster OTEL Collector. |
| **OBI (OpenTelemetry eBPF)** | OBI DaemonSet (`preset: network`) | Runs as lightweight systemd service hooking kernel sockets | Exports `obi.network.flow.bytes` over mesh $\\to$ central OTEL Collector / SigNoz. |
| **Parca** | Parca DaemonSet with host eBPF | Runs as systemd service with eBPF enabled on VM kernel | Continuously pushes pprof profiles over mesh $\\to$ central Parca server. |
| **Falco** | Falco DaemonSet with kernel driver | Runs `falco --modern-bpf` with VM-level hypervisor isolation | Syscall violations forward via gRPC/HTTP $\\to$ central `falcosidekick`. |
| **Dragonfly** | Dragonfly Dfdaemon DaemonSet | Runs local `dfdaemon` proxy redirecting `/etc/docker/daemon.json` | P2P block distribution across global VMs $\\to$ avoids registry rate-limits. |

### Golden AMI vs. Dynamic Cloud-Init Injection

To maintain the **~33-second cold-start**, daemons can be packaged in two ways:
1. **Pre-baked Golden AMI (Recommended for Production)**: Tools (Parca, Falco, Dragonfly) are pre-installed into custom AMIs via Packer. The VM boots with all binaries and kernel eBPF probes already on disk—zero network downloads at runtime.
2. **Cloud-Init Systemd Hooks**: Lightweight binaries or systemd units are launched dynamically at boot through cloud-init user-data scripts.

### Why Ephemeral VMs Offer Stronger Security (Falco & eBPF)
On a multi-tenant Kubernetes worker node, a compromised container sharing the node kernel poses a risk to neighboring pods. With 1:1 ephemeral VMs:
- Each task runs inside its own **isolated hardware virtual machine** (AWS Nitro / GCP KVM).
- Falco and Parca monitor the dedicated kernel with zero interference from other tenants.
- When the task completes, the VM and its file systems are permanently deleted.

## 5. Usage Examples

### Example 1: Spot GPU ML Worker (Fast-Boot)

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: esm2-gpu-inference
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

### Example 2: Spot CPU Data Pipeline Worker (Container Mode)

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: ray-data-transform
  annotations:
    cloudvm-job.vk.io/provider: "aws"
    cloudvm-job.vk.io/capacity-type: "spot"
    cloudvm-job.vk.io/instance-type: "c6i.2xlarge"
spec:
  nodeSelector:
    kubernetes.io/role: cloudvm
  tolerations:
    - key: virtual-node.interlink/no-schedule
      operator: Exists
      effect: NoSchedule
  containers:
    - name: worker
      image: rayproject/ray:2.44.0
      env:
        - name: RAY_HEAD_ADDRESS
          value: "ray-head.default.svc.cluster.local:6379"
      resources:
        limits:
          cpu: "8"
          memory: "16Gi"
```

---

## 6. Installation via Helm

```bash
helm upgrade --install interlink-provider-cloudvm \
  oci://ghcr.io/simonepri/charts/interlink-provider-cloudvm \
  --namespace interlink \
  --create-namespace \
  --set provider.type=aws \
  --set provider.awsRegion=us-east-1
```

---

## 7. Development

```bash
# Install toolchain
mise install

# Run static verification gate and unit tests
mise run verify

# Build binary
mise run build
```

---

## License

[Apache 2.0](LICENSE) © 2026 Simone Primarosa
