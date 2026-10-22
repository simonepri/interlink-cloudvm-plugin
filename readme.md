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
  <!-- Test cluster - Kind -->
  <a href="https://kind.sigs.k8s.io/">
    <img src="https://img.shields.io/badge/test_cluster-Kind-326CE5?logo=kubernetes&amp;logoColor=white" alt="Kind Kubernetes test cluster">
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
  ⚡ Ultra-fast, multi-cloud, cross-region Spot and On-Demand VM bursting for Kubernetes via CNCF InterLink.
</p>

---

## Architecture

`interlink-provider-cloudvm` bridges the **[CNCF InterLink](https://github.com/interlink-hq/interLink)** Virtual Kubelet provider directly to cloud provider fleet APIs (**AWS EC2 Fleet**, **GCP Compute Engine**):

```
                        Kubernetes Control Plane (Kueue / Ray / Pods)
                                              │
                                              ▼
                             InterLink Virtual Node (:4000)
                                              │ REST
                                              ▼
                     interlink-provider-cloudvm (:4040)
                                              │
                      ┌───────────────────────┴───────────────────────┐
                      ▼                                               ▼
             AWS EC2 Spot Fleet                             GCP Bulk Insert
          (Price-Capacity-Optimized)                     (ANY_SINGLE_ZONE Spot)
                      │                                               │
                      └───────────────────────┬───────────────────────┘
                                              ▼
                                 Ephemeral Cloud VMs (1:1)
                         • Nitro Serial Console Telemetry
                         • IMDSv2 Preemption Watchdog (2-min notice)
                         • Native DLAMI Fast-Boot (~30s) or Docker
                         • Automatic Self-Destruct Watchdog
```

---

## Features

- **Global Spot Arbitrage:** Dynamically bid across 16+ global AWS regions or GCP zones with diversified instance portfolios (`g4dn`, `g5`, `g6`, `g6e`, `p4d`, `p5`).
- **Exact Weighted Capacity:** Translates GPU requirements into weighted fleet overrides so multi-GPU instances (`g5.12xlarge`, `p5.48xlarge`) never cause over-allocation.
- **Fast Cold Starts (~33s):** Native AWS DLAMI mode bypasses pulling 15GB Docker container images over the internet.
- **Preemption & Self-Destruct Watchdogs:** In-VM systemd monitoring handles AWS IMDSv2 notices and terminates zombie instances automatically if disconnected.
- **Zero-Credential Log Streaming:** Heartbeats and logs stream over the AWS Nitro Serial Console directly into `kubectl logs`.

---

## Usage

Schedule any Kubernetes Pod onto the `cloudvm` virtual node using standard InterLink annotations:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: gpu-inference-worker
  annotations:
    cloudvm-job.vk.io/provider: "aws"
    cloudvm-job.vk.io/capacity-type: "spot"
    cloudvm-job.vk.io/regions: "us-east-1,us-east-2,us-west-2"
    cloudvm-job.vk.io/ami-mode: "dlami"
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
helm upgrade --install interlink-provider-cloudvm   oci://ghcr.io/simonepri/charts/interlink-provider-cloudvm   --namespace interlink   --create-namespace   --set provider.type=aws   --set provider.awsRegion=us-east-1
```

---

## Local Development

```bash
# Install toolchain
mise install

# Run unit tests
mise run test

# Build binary
mise run build
```

---

## License

[Apache 2.0](LICENSE) © 2026 Simone Pri
