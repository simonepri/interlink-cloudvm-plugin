# Changelog

## 0.1.0 (2026-10-22)


### Features

* add dead-man switch, client token idempotency, selective tc band filter, and operational boundaries ([645079f](https://github.com/simonepri/interlink-cloudvm-plugin/commit/645079fee2b8a7c3e0c442bead503d2235f6fa2f))
* add dollar-based egress spend governor and opencost finops integration ([4821098](https://github.com/simonepri/interlink-cloudvm-plugin/commit/482109887f0db7bd6c8098d31b740a36cdeddce2))
* add namespace-scoped instance keying, roadmap status table, and standalone ticker architecture ([3cee378](https://github.com/simonepri/interlink-cloudvm-plugin/commit/3cee3785f5f4393e5223f1db50c3e87b03f7ac70))
* add on-demand support, price capping policies, and egress rate limiting ([49c7ff1](https://github.com/simonepri/interlink-cloudvm-plugin/commit/49c7ff1437f505d92f03b2388eb76a03294efc4e))
* **auth:** add Kubernetes imagePullSecrets resolution and ephemeral registry credentials ([d4e5728](https://github.com/simonepri/interlink-cloudvm-plugin/commit/d4e57286676a675cc17f2cd3538d1af4ebccfa2a))
* **core:** integrate KMS encryption, SecretResolver, and resilient container lifecycle in server ([9a84fae](https://github.com/simonepri/interlink-cloudvm-plugin/commit/9a84fae9e5e1ddcaeb5e832d8924a400da5ca79c))
* fix end-to-end N:1 VM bin-packing capacity and co-tenant deletion, document in readme ([511b835](https://github.com/simonepri/interlink-cloudvm-plugin/commit/511b835c988415708360cec389d31fb2bf995d53))
* **gcp:** implement production-grade Google Compute Engine provider ([902d466](https://github.com/simonepri/interlink-cloudvm-plugin/commit/902d466e4eb3a0450b47553ff91f4a2533819e1d))
* **helm:** add configurable RBAC scoping and KMS environment configuration ([afa1a15](https://github.com/simonepri/interlink-cloudvm-plugin/commit/afa1a15d34afb51edb347256ae67768a8706d843))
* implement dynamic ec2:DescribeInstanceTypes hardware catalog and DescribeSpotPriceHistory ticker daemon ([58a5722](https://github.com/simonepri/interlink-cloudvm-plugin/commit/58a5722f38ae4c50ce4e3d669b55613a88aa94b4))
* implement multi-pod multi-dimensional resource bin-packing and discrete accelerator slicing ([1526bfb](https://github.com/simonepri/interlink-cloudvm-plugin/commit/1526bfbcfc05efaef22c116e228a35b54e949691))
* initial release of interlink-cloudvm-plugin ([e9a6e79](https://github.com/simonepri/interlink-cloudvm-plugin/commit/e9a6e79983902dd7ba2719dc2e6889428351cc56))
* persist runner tokens in private TokenStore, concurrent status querying, in-flight wait state, and idle node termination ([3df8c0b](https://github.com/simonepri/interlink-cloudvm-plugin/commit/3df8c0b2f4093dd94c0a3f1c17b6ea03d86df3db))
* **security:** add AWS KMS encryption for runner authentication tokens ([372b734](https://github.com/simonepri/interlink-cloudvm-plugin/commit/372b7348ea077b8db7b5c85e9ecef3db2796a1de))


### Bug Fixes

* adopt active VMs on startup throttle, consecutive miss pruning, and live state reason queries ([28f8c23](https://github.com/simonepri/interlink-cloudvm-plugin/commit/28f8c237ee324273dade4f193bc5e9d336fa9447))
* **ci:** optimize multi-arch Docker build and initialize release manifest ([31fad53](https://github.com/simonepri/interlink-cloudvm-plugin/commit/31fad53a2dbe2c8aa9ccebc54b409e366631a921))
* CSV-quote discrete GPU device lists, persist runner tokens, lock-free status querying, and prevent IMDS access ([9e7fa1d](https://github.com/simonepri/interlink-cloudvm-plugin/commit/9e7fa1de0ab75a0e3af0d14c0c28942e3ef120d8))
* detached launch context, completed store restart persistence, and securityContext support ([4347237](https://github.com/simonepri/interlink-cloudvm-plugin/commit/4347237a6a6c3684479d5050823c1f974681885a))
* flag adopted nodes with unknown co-tenants when runner listing fails ([8d12ce0](https://github.com/simonepri/interlink-cloudvm-plugin/commit/8d12ce0b27bd2e1e45ccb24b8378cff30c85b764))
* guard unknown co-tenants on recovery failure, require status evidence for adoption, and document architecture notes ([a734b82](https://github.com/simonepri/interlink-cloudvm-plugin/commit/a734b829f6b6e92f67b9b24042fcc5ebd7347f56))
* harden in-VM micro-runner, multi-pod idle watchdog, ticker routing, and node recovery cordoning ([620e1de](https://github.com/simonepri/interlink-cloudvm-plugin/commit/620e1ded20a42141b10a5dbdf20964f5ae335e00))
* harden shared VM reconciler check, atomic RegisterAndAllocate, and bin-packing table ([4361744](https://github.com/simonepri/interlink-cloudvm-plugin/commit/4361744f526a42afd2b5853de0f9489f148d5d5e))
* match CNCF InterLink API contract, add state recovery, multi-region caching, and chart serviceaccounts ([1dd75d8](https://github.com/simonepri/interlink-cloudvm-plugin/commit/1dd75d8eedae9cdafcde9c4cf64701b453048503))
* match CNCF InterLink status UID tag, add multi-region tracking, and index by pod UID ([90af2f8](https://github.com/simonepri/interlink-cloudvm-plugin/commit/90af2f834a647c7f40bcf516b7dba2c0f4a0a20d))
* match market type in bin-packing, per-region inventory reconciliation, and disk write dedup ([09490bf](https://github.com/simonepri/interlink-cloudvm-plugin/commit/09490bf2a3abda998ec500a8e414e89113a599a6))
* multi-region active instance inventory, persistent region store, warm node metadata, and launch grace period ([e617c23](https://github.com/simonepri/interlink-cloudvm-plugin/commit/e617c23340cecccf6c90ce25ce6b1436f6bab123))
* multi-region active instance inventory, protect completed co-tenants, and deduplicate active pod reconciliation ([eb11d60](https://github.com/simonepri/interlink-cloudvm-plugin/commit/eb11d60b5647ec6f73eae4fc928e5706a72d27ce))
* persist tombstones in chart, lock NodePool queries, and skip tombstoned recovery ([938272e](https://github.com/simonepri/interlink-cloudvm-plugin/commit/938272ee7cae8c06cff7459fc88bfc6dd601e6ef))
* persistent completed pod status, heartbeat tolerance, async container launch, and spot poller IMDS isolation ([01e2463](https://github.com/simonepri/interlink-cloudvm-plugin/commit/01e246344420975bcf029f35a5a82ebe516a22bd))
* preserve completed allocations, prevent co-tenant eviction, bridge container isolation, runner heartbeat, and create idempotency ([5169bd9](https://github.com/simonepri/interlink-cloudvm-plugin/commit/5169bd98cc8890f03336d7e36a7598bd8ea3fc9b))
* protect unknown co-tenants from idle collection and reconciler with bounded retry budget ([9661b5e](https://github.com/simonepri/interlink-cloudvm-plugin/commit/9661b5e540ed9a2d2ea1148fca8b8b71cada1939))
* reconciler spot interruption eviction, defensive instance cloning, and atomic fsync persistence ([5a85639](https://github.com/simonepri/interlink-cloudvm-plugin/commit/5a85639f6753fedbf201bfed8489fe0e56bf7fc4))
* reject root containers, isolate runner pod locks, preserve corrupt store files, and propagate initial container specs ([e8604ee](https://github.com/simonepri/interlink-cloudvm-plugin/commit/e8604eed4e30d71ecba3915cb9bda5da5b90ac04))
* remove stale watchdog phase 2, secure in-VM runner, and wire status telemetry ([5662941](https://github.com/simonepri/interlink-cloudvm-plugin/commit/56629416ad324db3ba04d7c4ce0a27210688135f))
* single-flight create deduplication, completed store persistence, and container isolation ([b0846fd](https://github.com/simonepri/interlink-cloudvm-plugin/commit/b0846fdeb2bb00955a08b6663c1306b5960a9729))
* token store corrupt file guard, limits-only container cgroup enforcement, and podUID recovery fallback ([5e8abe9](https://github.com/simonepri/interlink-cloudvm-plugin/commit/5e8abe92e9eb0521788ef9ca75a457557dd7d077))
* tombstone deleted pods, cordon adopted nodes, and restore test speed ([bc52fe6](https://github.com/simonepri/interlink-cloudvm-plugin/commit/bc52fe62d5a463d9323c69de22874f8029364333))
* UID-first client token, post-launch readiness watcher, and doc numbering polish ([5bea796](https://github.com/simonepri/interlink-cloudvm-plugin/commit/5bea79658f94a86cb1cb0ceb01a8295b432bb60d))
