<h1 align="center">interlink-provider-cloudvm</h1>

<p align="center">
  <!-- Implementation -->
  <a href="https://go.dev/">
    <img src="https://img.shields.io/badge/language-Go-00ADD8?logo=go&amp;logoColor=white" alt="Written in Go">
  </a>
  <!-- Toolchain - mise -->
  <a href="https://mise.jdx.dev/">
    <img src="https://img.shields.io/badge/toolchain-mise-8B5CF6?logo=iterm2&amp;logoColor=white" alt="mise toolchain">
  </a>
  <br>
  <!-- Verification -->
  <a href="https://github.com/simonepri/interlink-provider-cloudvm/actions/workflows/ci.yml">
    <img src="https://img.shields.io/github/actions/workflow/status/simonepri/interlink-provider-cloudvm/ci.yml?branch=main&amp;label=CI&amp;logo=githubactions&amp;logoColor=white" alt="CI status">
  </a>
  <br>
  <!-- Distribution -->
  <a href="https://github.com/googleapis/release-please">
    <img src="https://img.shields.io/badge/released_with-Release_Please-4285F4?logo=google&amp;logoColor=white" alt="Released with Release Please">
  </a>
  <a href="https://helm.sh/">
    <img src="https://img.shields.io/badge/packaging-Helm_OCI-0F1689?logo=helm&amp;logoColor=white" alt="Helm OCI chart">
  </a>
  <a href="https://www.docker.com/">
    <img src="https://img.shields.io/badge/container-Docker-2496ED?logo=docker&amp;logoColor=white" alt="Docker container image">
  </a>
  <a href="LICENSE">
    <img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="Apache 2.0 license">
  </a>
</p>

<p align="center">
  Multi-cloud Spot and On-Demand VM bursting for Kubernetes via CNCF InterLink.
</p>

---

## Overview

`interlink-provider-cloudvm` is a **[CNCF InterLink](https://github.com/interlink-hq/interLink)** provider plugin that lets a Kubernetes cluster burst pods directly onto ephemeral cloud VMs across regions and cloud providers.

When a single region runs out of spot GPUs (A10G, L4, L40S, H100), in-cluster autoscalers get stuck in `Pending` because they can only launch nodes inside the local cluster VPC. `interlink-provider-cloudvm` bypasses that limit by turning scheduled pods into standalone EC2 Fleet or GCP Compute Engine requests in any target region, connecting the VM back to the cluster over a private WireGuard mesh.

```mermaid
flowchart LR
    classDef k8s fill:#1E293B,stroke:#38BDF8,stroke-width:2px,color:#F8FAFC
    classDef plugin fill:#311042,stroke:#C084FC,stroke-width:2px,color:#F8FAFC
    classDef cloud fill:#064E3B,stroke:#34D399,stroke-width:2px,color:#F8FAFC

    A["1. Kubernetes Pods
(Ray / PyTorch / Batch)"]:::k8s 
    -->|"Schedules on
virtual-node"| B["2. interlink-provider-cloudvm
(Bin-Packer & Fleet Solver)"]:::plugin
    -->|"Slices warm VM or
launches new Spot VM"| C["3. Ephemeral Cloud VM
(1:1 or N:1 GPU/CPU Slicing)"]:::cloud
```

### In-Cluster Autoscaling vs. Virtual Bursting

Most setups run both side-by-side:
- **In-Cluster Nodes (Karpenter / Cluster Autoscaler)**: Run persistent services, Ray head nodes, Kueue controllers, databases, and low-latency workloads inside the cluster VPC.
- **Burst Virtual Nodes (`interlink-provider-cloudvm`)**: Run stateless GPU/CPU batch jobs across external regions and spot pools when local capacity is full or too expensive.

| Capability | In-Cluster Autoscaler (Karpenter) | `interlink-provider-cloudvm` |
| :--- | :---: | :---: |
| **Primary Role** | Local steady-state & stateful services | Multi-region & multi-cloud batch burst |
| **Geographic Scope** | ❌ Single VPC & region | ✅ Any region or cloud via WireGuard |
| **Spot Pool Access** | ❌ Local AZs only | ✅ Cross-region price & capacity arbitrage |
| **Node Bootstrap** | ⚠️ Kubelet join + CNI + DaemonSets | ✅ Boots straight into workload |
| **Cluster Overhead** | ⚠️ `Node` object in etcd + VPC IPs | ✅ Single static virtual node, 0 VPC IPs |
| **Packing & Isolation** | ⚠️ Shared host kernel across pods | ✅ 1:1 dedicated VM or N:1 GPU/CPU bin-packing |
| **Persistent Volumes (CSI)** | ✅ Native EBS / CSI mounts | ❌ S3 / GCS / FUSE mounts only |
| **Idle Cost** | Consolidation & scale-down cooldown | **`$0`** (VM powers off on process exit) |

---

## Architecture

```mermaid
flowchart LR
    classDef stage fill:#1E293B,stroke:#38BDF8,stroke-width:2px,color:#F8FAFC

    S1["<b>1. Schedule</b><br>Pod binds to virtual-node<br>via Kueue or Ray"]:::stage
    --> S2["<b>2. Bid</b><br>Solver picks candidate shapes<br>& calls EC2 Fleet / GCP API"]:::stage
    --> S3["<b>3. Run</b><br>VM joins mesh, runs container,<br>and terminates on exit"]:::stage
```

The provider consists of five main components:

1. **Multi-Pod Bin-Packer (`NodePool`)**: Tracks free CPU, memory, and discrete GPU/TPU device slots across active VMs using `k8s.io/apimachinery/pkg/api/resource`. If a 4-GPU pod lands on an 8-GPU spot VM (`g5.48xlarge` or `p4d.24xlarge`), a second 4-GPU pod packs straight into the remaining 4 GPUs (`[4,5,6,7]`) without booting another VM.
2. **Fleet Solver**: On a bin-pack miss, maps pod resource requests (`nvidia.com/gpu`, `google.com/tpu`, CPU, memory) to equivalent cloud instance types (`CreateFleet` on AWS, `bulkInsert` on GCP) with optional hourly price caps (`cloudvm.vk.io/max-price`).
3. **Backpressure Loop**: Returns capacity status (`CapacityExhausted`, `PriceCapExceeded`) to Kubernetes so job queues like Kueue can fall back to on-demand or another region.
4. **Egress Spend Governor**: Converts an hourly dollar budget (`cloudvm.vk.io/max-egress-rate: "$0.50"`) into a Linux `tc` Token Bucket Filter on `eth0`, throttling public internet egress while leaving private mesh traffic untouched.
5. **Reconciler & Cost Tagging**: Polls cloud APIs every 60s to clean up orphaned VMs, update pod states on spot preemption, and tag instances/EBS volumes for OpenCost billing reports.

---

## Core Subsystems

### 1. Spot Fleet Bidding & Multi-Pod Bin-Packing (N Pods to 1 VM)

When a pod schedules onto the virtual node, the provider first checks its `NodePool` for an active VM in the target region with enough free CPU, memory, and discrete accelerator slots (`NodeResourcesMostAllocated` scoring):

- **Bin-Pack Hit (< 1s)**: If a previous 4-GPU pod triggered an 8-GPU spot instance (for example, when 4-GPU spot pools were empty and an 8-GPU `g5.48xlarge` was allocated), the provider assigns the free GPU slots (`NVIDIA_VISIBLE_DEVICES=4,5,6,7`) and starts the second pod on the same warm VM immediately.
- **Bin-Pack Miss**: If no warm VM can fit the pod, the solver builds a list of compatible shapes and calls the cloud API for a `price-capacity-optimized` allocation (`TotalTargetCapacity = 1`).
- **Co-Tenant Lifecycle**: Deleting one pod on a shared VM releases its GPU/CPU slots while keeping the VM alive for remaining co-tenants. The VM only terminates when the last pod on it exits (or after the 60s idle grace window).

```mermaid
flowchart LR
    classDef req fill:#1E293B,stroke:#38BDF8,stroke-width:2px,color:#F8FAFC
    classDef solver fill:#311042,stroke:#C084FC,stroke-width:2px,color:#F8FAFC
    classDef cloud fill:#0C4A6E,stroke:#38BDF8,stroke-width:2px,color:#F8FAFC

    Req["Pod Spec
(1 GPU requested)"]:::req --> Solver["Spot Solver"]:::solver

    subgraph Portfolio["Candidate Shapes (Weight: 1.0)"]
        direction TB
        W1["g5.xlarge (1x A10G)"]
        W2["g6.xlarge (1x L4)"]
        W3["g4dn.xlarge (1x T4)"]
    end
    Solver --> Portfolio

    Portfolio --> AWS["AWS EC2 Fleet API
(TargetCapacity = 1)"]:::cloud
    Portfolio --> GCP["GCP Compute Engine
(bulkInsert)"]:::cloud
```

| Pod Request | Fleet Target | Candidate Shapes | Result |
| :--- | :---: | :--- | :--- |
| **1 GPU** | 1 VM | `g5.xlarge`, `g6.xlarge`, `g4dn.xlarge` | 1 VM with 1 GPU |
| **4 GPUs** | 1 VM | `g5.12xlarge`, `g5.24xlarge`, `g6.12xlarge` | 1 VM with 4 GPUs |
| **8 GPUs** | 1 VM | `p5.48xlarge`, `p4de.24xlarge`, `g5.48xlarge` | 1 VM with 8 GPUs |

> [!NOTE]
> Multi-node jobs (distributed PyTorch, Ray clusters) run as multiple Kubernetes pods. Each worker pod binds to the virtual node and gets its own VM.

- **Idempotency**: Every AWS `CreateFleet` call sets a deterministic `ClientToken` (`cvm-<uid>-<namespace>-<pod>`) with the Pod UID first. Network retries will not launch duplicate VMs.
- **Hardware Pinning**: If a workload requires a specific GPU architecture, set `cloudvm.vk.io/instance-type: "p4d.24xlarge"` on the pod.

---

### 2. Capacity Backpressure & Kueue Integration

When spot pools are empty or above the price cap, the provider fails fast so upstream controllers can react instead of hanging:

```mermaid
flowchart TD
    classDef cloud fill:#0C4A6E,stroke:#38BDF8,stroke-width:2px,color:#F8FAFC
    classDef driver fill:#311042,stroke:#C084FC,stroke-width:2px,color:#F8FAFC
    classDef k8s fill:#1E293B,stroke:#38BDF8,stroke-width:2px,color:#F8FAFC
    classDef queue fill:#064E3B,stroke:#34D399,stroke-width:2px,color:#F8FAFC

    Cloud["Cloud API
(InsufficientCapacity)"]:::cloud
    --> Driver["CloudVM (/create)
Returns error code"]:::driver
    --> VK["InterLink
Sets PodScheduled=False"]:::k8s
    --> Kueue["Kueue / Job Controller
Falls back to On-Demand or backs off"]:::queue
```

- `CapacityExhausted`: Requested spot pools have no capacity in the target region.
- `PriceCapExceeded`: Current spot price is higher than `cloudvm.vk.io/max-price`.

---

### 3. Egress Rate Limiting (Linux `tc`)

Cloud internet egress ($0.09/GB on AWS, $0.085/GB on GCP) can easily surprise teams running data-heavy jobs. Setting `cloudvm.vk.io/max-egress-rate: "$0.50"` caps public egress spend at $0.50/hr by configuring a Linux `tc` Token Bucket Filter at boot:

```mermaid
flowchart LR
    classDef user fill:#1E293B,stroke:#38BDF8,stroke-width:2px,color:#F8FAFC
    classDef calc fill:#311042,stroke:#C084FC,stroke-width:2px,color:#F8FAFC
    classDef tc fill:#064E3B,stroke:#34D399,stroke-width:2px,color:#F8FAFC

    Ann["Annotation:
max-egress-rate: $0.50"]:::user
    --> Calc["EgressRateToBandwidth
($0.50/hr / $0.09/GB) * 8000 / 3600"]:::calc
    --> TC["Linux tc TBF
Rate: 12mbit | Burst: 100mb"]:::tc
```

`tc` sets up a two-band priority queue (`prio bands 2`) on `eth0`:
- **Band 1 (Unthrottled)**: WireGuard (`UDP 51820`) and Tailscale (`UDP 41641`) mesh traffic bypasses the rate limit so cluster heartbeats and Ray GCS traffic are never throttled.
- **Band 2 (Throttled)**: All other outbound traffic goes through the Token Bucket Filter.

---

### 4. VM Boot Lifecycle & Watchdogs

The provider supports two boot modes:
1. **DLAMI Mode (`cloudvm.vk.io/ami-mode: "dlami"`)**: Boots an AWS Deep Learning AMI with NVIDIA drivers, CUDA, and PyTorch pre-installed on disk.
2. **Container Mode (Default)**: Pulls and runs the pod's container image via Docker/Containerd.

```mermaid
flowchart TD
    classDef init fill:#1E293B,stroke:#38BDF8,stroke-width:2px,color:#F8FAFC
    classDef runtime fill:#064E3B,stroke:#34D399,stroke-width:2px,color:#F8FAFC
    classDef watch fill:#451A03,stroke:#F59E0B,stroke-width:2px,color:#F8FAFC
    classDef exit fill:#4C0519,stroke:#F43F5E,stroke-width:2px,color:#F8FAFC

    Start["1. Cloud-Init"]:::init --> Mesh["Join WireGuard / Tailscale"]:::init
    Mesh --> Arm["Start 300s Boot Watchdog"]:::watch
    
    Arm --> Mode{"Boot Mode"}
    Mode -->|DLAMI| Native["Host ML Worker"]:::runtime
    Mode -->|Container| Docker["Docker Container"]:::runtime

    Native --> Health["Ready Check"]:::runtime
    Docker --> Health
    Health -->|Touches /tmp/workload-ready| Disarm["Stop Boot Watchdog"]:::runtime

    subgraph Monitors["Background Watchdogs"]
        direction LR
        Preempt["Spot Interruption Poller
(IMDSv2 / GCP Metadata)"]:::watch
        DeadMan["Process Watchdog
(Polls every 60s)"]:::watch
    end
    Native --- Monitors

    Preempt -->|Preemption Notice| Drain["SIGTERM & Flush"]:::exit
    Drain --> Terminate["poweroff"]:::exit
    Native -->|Process Exits| Terminate
```

- **Spot Interruption Poller**: Polls AWS IMDSv2 (`/latest/meta-data/spot/instance-action`, 120s warning) or GCP metadata (`/computeMetadata/v1/instance/preempted`, 30s warning) and sends `SIGTERM` to the container before shutting down.
- **300s Boot Watchdog**: Powers off the VM if the container fails to start within 5 minutes, preventing stuck VMs from accruing charges.
- **Process Exit Hook**: AWS VMs launch with `InstanceInitiatedShutdownBehavior=terminate`. As soon as the container exits and `cloud-init` runs `poweroff`, AWS terminates the instance and deletes its root EBS volume.

---

### 5. Reconciler & Zombie Cleanup

A background loop runs every 60 seconds in the provider to keep Kubernetes and cloud state in sync:

```mermaid
flowchart TD
    classDef rec fill:#311042,stroke:#C084FC,stroke-width:2px,color:#F8FAFC
    classDef k8s fill:#1E293B,stroke:#38BDF8,stroke-width:2px,color:#F8FAFC
    classDef cloud fill:#0C4A6E,stroke:#38BDF8,stroke-width:2px,color:#F8FAFC
    classDef act fill:#4C0519,stroke:#F43F5E,stroke-width:2px,color:#F8FAFC

    subgraph State["Sources"]
        CloudInv["Cloud API Inventory
(EC2 / GCE)"]:::cloud
        K8sPods["Active Pods
(InterLink State)"]:::k8s
    end

    CloudInv --> Reconciler["Reconciler (60s loop)"]:::rec
    K8sPods --> Reconciler

    Reconciler --> Check1{"VM running in cloud,
but Pod deleted in K8s?"}
    Check1 -->|"Age > 10m"| KillZombie["Terminate Orphaned VM"]:::act

    Reconciler --> Check2{"Pod active in K8s,
but VM gone in cloud?"}
    Check2 -->|Yes| EvictPod["Mark Pod Failed"]:::act
```

---

### 6. Spot Price Ticker (`cloudvm-ticker`)

To avoid hitting AWS/GCP API rate limits when running multiple provider instances, spot price polling can be separated into a standalone `cloudvm-ticker` deployment:

```mermaid
flowchart TD
    classDef central fill:#311042,stroke:#C084FC,stroke-width:2px,color:#F8FAFC
    classDef cell fill:#064E3B,stroke:#34D399,stroke-width:2px,color:#F8FAFC
    classDef otel fill:#1E293B,stroke:#38BDF8,stroke-width:2px,color:#F8FAFC

    subgraph Central["Management Cluster"]
        Ticker["cloudvm-ticker
Polls Spot Price History (10m)"]:::central
        Ticker --> HTTP["GET /prices"]:::central
        Ticker --> Metrics["GET /metrics
cloudvm_spot_price_usd_per_hour"]:::central
    end

    Metrics --> OTEL["Prometheus / Grafana"]:::otel

    subgraph Cells["Worker Clusters"]
        HTTP --> Cell1["Provider 1
(--ticker-url=http://...)"]:::cell
        HTTP --> Cell2["Provider 2
(--ticker-url=http://...)"]:::cell
    end
```

- **Embedded Mode (Default)**: The provider polls spot prices directly every `10m ± 2m`.
- **Central Ticker Mode**: Deploy the `cloudvm-ticker` Helm chart once and point providers at `--ticker-url=http://cloudvm-ticker.monitoring.svc:8080`.

---

### 7. Cost Allocation (OpenCost)

Because burst VMs do not run Kubelet or cAdvisor, cost attribution relies on cloud billing tags. Every instance and EBS volume is tagged on creation:
- `ManagedBy: interlink-provider-cloudvm`
- `ClusterTag: <cluster-name>`
- `PodName: <pod-name>`
- `Namespace: <pod-namespace>`
- `PodUID: <pod-uid>`
- `CostCenter: <cost-center>`
- `JobID: <job-id>`

OpenCost ingests these tags from the AWS Cost and Usage Report (CUR) or GCP billing export to map VM spend back to Kubernetes namespaces and jobs.

---

### 8. Replacing Node DaemonSets on Bare VMs

Standard Kubernetes DaemonSets do not run on InterLink virtual nodes. Instead, host-level tools are configured via `cloud-init` or pre-baked into the VM image:

| Kubernetes DaemonSet | CloudVM Equivalent | How It Runs | Purpose |
| :--- | :--- | :--- | :--- |
| **CSI FUSE Driver** (`csi-rclone`) | `rclone mount` / `s3fs` / `gcsfuse` | `cloud-init` service | Mounts S3/GCS buckets to `/mnt/data` before the container starts. |
| **Torch Compile Cache** | Pre-populated kernel cache in AMI | Local disk / S3 | Avoids 5-15 minute Triton/Inductor compilation on cold boot. |
| **Node Problem Detector** | NVML health check script | Host cron / systemd | Checks for GPU Xid/ECC errors and exits the VM if a card fails. |
| **DCGM Exporter** | Host OTEL / DCGM agent | Systemd service | Pushes GPU utilization and memory metrics over the WireGuard mesh. |

---

## Security & Isolation

1. **VM-Level Isolation**: Each job runs in a dedicated cloud VM (AWS Nitro / GCP KVM) with its own kernel and ephemeral root disk.
2. **IMDSv2 Hop Limit**: AWS instances enforce IMDSv2 with `HttpPutResponseHopLimit: 1`, blocking containers from stealing host IAM role credentials via `169.254.169.254`.
3. **No Inbound SSH**: VMs join the WireGuard/Tailscale mesh without `--ssh` enabled and with no inbound ports open on the cloud security group.
4. **Ephemeral Mesh Keys**: Mesh auth keys are scoped to short-lived tags.

---

## When NOT to Use This

| Workload Type | Why It Fits Poorly | Use This Instead |
| :--- | :--- | :--- |
| **Tightly Coupled Multi-Node Training** (e.g. 400-3200 Gbps EFA/RoCE NCCL all-reduce) | WireGuard overlays are built for control-plane traffic and Ray GCS coordination, not multi-hundred-gigabit RDMA tensor parallelism. | Same-AZ GPU placement groups with AWS EFA or GCP GPUDirect-TCPXO. |
| **Small Microservices** (e.g. 0.25 vCPU, 512MB RAM) | Launching a dedicated VM per pod wastes capacity and hits the 60-second cloud billing minimum. | Standard Kubernetes nodes managed by **Karpenter**. |
| **Multi-Container Pods with Shared Volumes** | Only the primary workload container runs on the VM; Kubernetes `ConfigMap` and `Secret` volume mounts are not projected into the VM filesystem. | In-cluster node pools, or pass config via environment variables and S3. |
| **Sub-Minute Tasks (< 60s)** | Cloud providers charge a 60-second minimum per VM launch, and VM boot takes ~30-60s. | Warm in-cluster pod pools or serverless functions. |

---

## Failure Modes

| Event | How It's Detected | What Happens | Recovery |
| :--- | :--- | :--- | :--- |
| **Spot Preemption** | In-VM poller checks IMDSv2 / GCP metadata | Sends `SIGTERM` to workload (120s on AWS, 30s on GCP), flushes logs, and powers off. | Reconciler marks pod `Failed (SpotInterruption)`; job controller re-queues. |
| **Spot Pool Empty** | Cloud API returns `InsufficientInstanceCapacity` | `/create` fails immediately and sets `PodScheduled=False (CapacityExhausted)`. | Kueue falls back to on-demand flavor or retries with backoff. |
| **Container Fails to Start** | 300s boot watchdog on the VM | Watchdog powers off the VM if `/tmp/workload-ready` is not created within 5m. | VM terminates; pod is marked `Failed`. |
| **Container Crashes** | 60s process liveness check | Script sees container exited and runs `poweroff`. | VM terminates; pod is marked `Failed`. |
| **Control Plane Disconnected** | Provider reconciler loop | If the pod no longer exists in Kubernetes after 10m, the provider terminates the VM via cloud API. | Prevents orphaned VMs from running if the cluster loses connectivity. |

---

## Usage Examples

### Example 1: Spot GPU Job (DLAMI Fast-Boot)

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: gpu-inference-worker
  annotations:
    cloudvm.vk.io/provider: "aws"
    cloudvm.vk.io/capacity-type: "spot"
    cloudvm.vk.io/ami-mode: "dlami"
    cloudvm.vk.io/instance-type: "g5.xlarge"
    cloudvm.vk.io/max-price: "2.50"
    cloudvm.vk.io/max-egress-rate: "$0.50"
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

### Example 2: Spot CPU Worker (Container Mode)

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: ray-data-worker
  annotations:
    cloudvm.vk.io/provider: "aws"
    cloudvm.vk.io/capacity-type: "spot"
    cloudvm.vk.io/instance-type: "c6i.2xlarge"
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

## Installation

### Provider Chart

```bash
helm upgrade --install interlink-provider-cloudvm \
  oci://ghcr.io/simonepri/charts/interlink-provider-cloudvm \
  --namespace interlink \
  --create-namespace \
  --set provider.type=aws \
  --set provider.awsRegion=us-east-1
```

### Spot Price Ticker Chart (Optional)

```bash
helm upgrade --install cloudvm-ticker \
  oci://ghcr.io/simonepri/charts/cloudvm-ticker \
  --namespace monitoring \
  --create-namespace \
  --set serviceMonitor.enabled=true
```

---

## Development

Requires [mise](https://mise.jdx.dev/) for tool management.

```bash
# Install Go, linters, and Helm
mise install

# Run all linters, security checks, and tests
mise run verify

# Run unit tests with race detector
mise run test

# Build binary to ./bin/interlink-provider-cloudvm
mise run build
```
