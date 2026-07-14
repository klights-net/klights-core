# Changelog

All notable public release changes for `klights-core` are documented here.

This project uses GitHub Releases as the canonical public release page. The
release workflow extracts the matching version section from this file and
attaches distro packages to the GitHub Release.

## [0.9.13] - 2026-07-13

This release hardens **protobuf API compatibility** for clients such as
client-go, kubectl, and ArgoCD, completes **Kubernetes Quantity parsing**
fidelity, and restores two control-plane correctness paths: pod phase
transitions and leader-owned static volume reconciliation.

### What's new

- **Protobuf watch and response fidelity.** Watch events are now emitted as
  raw length-prefixed protobuf stream events carrying the watched
  group/version in the outer Kubernetes envelope, status responses preserve
  their protobuf fields, and metadata timestamps keep apimachinery precision
  across JSON and protobuf paths. This closes several client-go/protobuf watch
  framing and status-shape gaps.
- **Kubernetes Quantity parsing fidelity.** Quantities are parsed with bigint
  precision, whitespace is rejected, and exponent/rational scaling is reduced
  before multiplication, completing quantity and content-negotiation edge
  handling for resource requests and limits.
- **Canonical metadata creation timestamps.** Server-generated
  `creationTimestamp` fields are stamped at `metav1.Time` second precision and
  kept aligned across create paths and protobuf decode.
- **Pod phase correctness restored.** A `Never` restart policy combined with a
  nonzero exit code again transitions the pod to the `Failed` phase.
- **Leader-owned static volume reconciliation restored.** PersistentVolume and
  PersistentVolumeClaim reconciliation is now driven by injected leadership
  authority instead of a hard-coded boolean, so a `Pending` PVC created before
  its matching static PV binds once the PV appears. Followers and workers never
  originate PV/PVC writes, and reconciliation stays event-driven with no
  polling.

### Kubernetes compatibility status

- Targets Kubernetes v1.34.6 API compatibility.
- Protobuf watch event framing, status response shape, and metadata timestamp
  precision now match apimachinery semantics, improving client-go/protobuf and
  ArgoCD compatibility.
- Kubernetes `resource.Quantity` parsing now handles bigint precision,
  whitespace rejection, and exponent/rational scaling edge cases.

### Known beta limitations

Carried forward from 0.9.12; no listed limitation was fully resolved in this
release.

- HPA autoscaling control loop and metrics-backed autoscaling remain
  deferred; the metrics data source is available, but the autoscaler is not.
- Pod subresource coverage is incomplete: `pods/attach` is still not
  implemented, and the `pods/binding` subresource route is missing.
- Built-in OpenAPI schemas are incomplete. CRD OpenAPI publishing exists, but
  built-in kinds still expose stub schemas, so `kubectl explain` for built-in
  fields is limited.
- Scheduler behavior is not fully upstream-compatible. Known gaps include
  pod affinity/anti-affinity, topology spread constraints, PDB-aware
  preemption, preferred node-affinity scoring, hostPort conflict predicates,
  and some taint handling/default-priority behavior.
- PodSecurity admission is not implemented. Namespace labels such as
  `pod-security.kubernetes.io/enforce`, `audit`, and `warn` are not enforced.
- Some admission/defaulting behavior remains incomplete, including parts of
  ResourceQuota, LimitRange, DefaultStorageClass, Service family defaulting,
  Pod defaulting, ServiceAccount imagePullSecret propagation, and built-in
  field-selector validation.
- Watch and delete semantics still have known edge-case gaps, including
  selector-less `resourceVersion=0` watch behavior, pending-delete status
  codes, `DeleteCollection` dry-run handling, and some foreground/orphan
  deletion details, although protobuf watch event framing was hardened in
  this release.
- NetworkPolicy resources are stored but not yet enforced in the datapath.
- Aggregated API server support is passthrough-only; the kube-aggregator
  control plane is not implemented.
- API Priority and Fairness resources exist for CRUD/discovery, but request
  prioritization is not enforced.
- Structured audit logging is not yet implemented.
- Server-Side Apply (`application/apply-patch+yaml`) is implemented for built-in
  resources: `metadata.managedFields` is produced, cross-manager conflicts are
  reported (HTTP 409), `force=true` transfers ownership, and dropped fields are
  pruned. It is not yet complete: field ownership uses heuristic merge-key
  inference rather than a schema-driven engine, CRD apply does not produce
  `managedFields`, and protobuf responses omit `managedFields` (JSON-only).

### Binary packages

This release publishes:

- Static binaries: `klights-linux-x86_64-static`, `klights-linux-arm64-static`
- Ubuntu 24.04 (noble): `klights_0.9.13-1~noble_amd64.deb`, `klights_0.9.13-1~noble_arm64.deb`
- Ubuntu 26.04 (resolute): `klights_0.9.13-1~resolute_amd64.deb`, `klights_0.9.13-1~resolute_arm64.deb`
- RHEL 9: `klights-0.9.13-1.el9.x86_64.rpm`, `klights-0.9.13-1.el9.aarch64.rpm`
- RHEL 10: `klights-0.9.13-1.el10.x86_64.rpm`, `klights-0.9.13-1.el10.aarch64.rpm`
- RHEL runtime dependencies: `containerd-2.3.2-1.el9.x86_64.rpm`, `containerd-2.3.2-1.el9.aarch64.rpm`, `containerd-2.3.2-1.el10.x86_64.rpm`, `containerd-2.3.2-1.el10.aarch64.rpm`, `runc-1.5.0-1.el9.x86_64.rpm`, `runc-1.5.0-1.el9.aarch64.rpm`, `runc-1.5.0-1.el10.x86_64.rpm`, `runc-1.5.0-1.el10.aarch64.rpm`

Package repositories are published from the `package-repo` branch:

- APT: https://raw.githubusercontent.com/klights-net/klights-core/package-repo/apt
- RPM: https://raw.githubusercontent.com/klights-net/klights-core/package-repo/rpm
- Public key: https://raw.githubusercontent.com/klights-net/klights-core/package-repo/klights-archive-keyring.asc

## [0.9.12] - 2026-07-11

This release makes the **on-demand metrics API production-ready** and
completes the committed-apply resourceVersion model that underpins watch
correctness across leader changes and worker mirrors.

### What's new

- **On-demand metrics API is ready.** `metrics.k8s.io/v1beta1` node and pod
  metrics now aggregate across the cluster with on-demand cross-node fanout,
  and the endpoints require real runtime samples (no zero or placeholder
  values). `kubectl top nodes` and `kubectl top pods` return live usage in a
  multinode cluster.
- Resource versions are now assigned exactly once at committed Raft apply,
  after dedupe, watermark, and stale-status decisions. This gives deterministic
  watch ordering and eliminates reserved or placeholder resourceVersions.
- Positioned list-watch replay: durable watch history is paged by apply
  position, worker mirrors are authoritative for their node-local streams, and
  gRPC watch opens recover transparently across leader changes.
- Watch convergence hardening for selector and exact-name watches, retained
  delete and tombstone replay, and correct catch-up replay floor handling.
- Service reconciliation and service-route networking hardening: service
  status fields are preserved, nft service-route rewrites are split safely,
  routes are source-authoritative, ports are retained across partial endpoint
  sources, and routes refresh on inventory watches.
- Node lifecycle correctness: pods for deleted nodes are cleaned up, and
  `NodeLost` pod cleanup is enforced through the actor-owned finalization path.
- Reliability fixes: netfilter socket recovery after batch errors, projected
  service-account token volumes preserved across restart, outbox stream
  gap/duplicate unblocking, a typed outbox scheduling policy, and bounded
  foreground owner GC finalization.

### Kubernetes compatibility status

- Targets Kubernetes v1.34.6 API compatibility.
- On-demand `metrics.k8s.io/v1beta1` metrics are available cluster-wide.
- Watch and list `resourceVersion`, and the list-to-watch handoff, are now
  positioned by durable apply identity, improving convergence across leader
  elections and worker mirrors.

### Known beta limitations

Carried forward from 0.9.11, with the on-demand metrics API now ready and
removed from the list.

- HPA autoscaling control loop and metrics-backed autoscaling remain
  deferred; the metrics data source is now available, but the autoscaler is not.
- Pod subresource coverage is incomplete: `pods/attach` is still not
  implemented, and the `pods/binding` subresource route is missing.
- Built-in OpenAPI schemas are incomplete. CRD OpenAPI publishing exists, but
  built-in kinds still expose stub schemas, so `kubectl explain` for built-in
  fields is limited.
- Scheduler behavior is not fully upstream-compatible. Known gaps include
  pod affinity/anti-affinity, topology spread constraints, PDB-aware
  preemption, preferred node-affinity scoring, hostPort conflict predicates,
  and some taint handling/default-priority behavior.
- PodSecurity admission is not implemented. Namespace labels such as
  `pod-security.kubernetes.io/enforce`, `audit`, and `warn` are not enforced.
- Some admission/defaulting behavior remains incomplete, including parts of
  ResourceQuota, LimitRange, DefaultStorageClass, Service family defaulting,
  Pod defaulting, ServiceAccount imagePullSecret propagation, and built-in
  field-selector validation.
- Watch and delete semantics still have known edge-case gaps, including
  selector-less `resourceVersion=0` watch behavior, pending-delete status
  codes, `DeleteCollection` dry-run handling, and some foreground/orphan
  deletion details, although watch-replay convergence was substantially
  hardened in this release.
- NetworkPolicy resources are stored but not yet enforced in the datapath.
- Aggregated API server support is passthrough-only; the kube-aggregator
  control plane is not implemented.
- API Priority and Fairness resources exist for CRUD/discovery, but request
  prioritization is not enforced.
- Structured audit logging is not yet implemented.
- Server-Side Apply (`application/apply-patch+yaml`) is implemented for built-in
  resources: `metadata.managedFields` is produced, cross-manager conflicts are
  reported (HTTP 409), `force=true` transfers ownership, and dropped fields are
  pruned. It is not yet complete: field ownership uses heuristic merge-key
  inference rather than a schema-driven engine, CRD apply does not produce
  `managedFields`, and protobuf responses omit `managedFields` (JSON-only).

### Binary packages

This release publishes:

- Static binaries: `klights-linux-x86_64-static`, `klights-linux-arm64-static`
- Ubuntu 24.04 (noble): `klights_0.9.12-1~noble_amd64.deb`, `klights_0.9.12-1~noble_arm64.deb`
- Ubuntu 26.04 (resolute): `klights_0.9.12-1~resolute_amd64.deb`, `klights_0.9.12-1~resolute_arm64.deb`
- RHEL 9: `klights-0.9.12-1.el9.x86_64.rpm`, `klights-0.9.12-1.el9.aarch64.rpm`
- RHEL 10: `klights-0.9.12-1.el10.x86_64.rpm`, `klights-0.9.12-1.el10.aarch64.rpm`
- RHEL runtime dependencies: `containerd-2.3.2-1.el9.x86_64.rpm`, `containerd-2.3.2-1.el9.aarch64.rpm`, `containerd-2.3.2-1.el10.x86_64.rpm`, `containerd-2.3.2-1.el10.aarch64.rpm`, `runc-1.5.0-1.el9.x86_64.rpm`, `runc-1.5.0-1.el9.aarch64.rpm`, `runc-1.5.0-1.el10.x86_64.rpm`, `runc-1.5.0-1.el10.aarch64.rpm`

Package repositories are published from the `package-repo` branch:

- APT: https://raw.githubusercontent.com/klights-net/klights-core/package-repo/apt
- RPM: https://raw.githubusercontent.com/klights-net/klights-core/package-repo/rpm
- Public key: https://raw.githubusercontent.com/klights-net/klights-core/package-repo/klights-archive-keyring.asc

## [0.9.11] - 2026-07-05

This release introduces the `metrics.k8s.io` API surface, substantially
completes VXLAN dataplane removal, and restructures API mutations behind a
shared write-strategy boundary. It also fixes a watch-event GC backlog that
drove memory growth under sustained watch load.

### What's new

- `metrics.k8s.io/v1beta1` API surface implemented: discovery for NodeMetrics
  and PodMetrics (including aggregated discovery) plus read-only node and pod
  list/get endpoints. (On-demand cross-node aggregation and runtime-sampled
  values arrive in 0.9.12.)
- VXLAN dataplane removal substantially complete. Remaining references were
  reduced to a handful of dormant legacy schema/test compatibility references;
  WireGuard is the default encrypted pod dataplane.
- API mutation writes restructured behind a shared write-strategy boundary:
  pod, CRD, generated, status-subresource, and non-pod delete mutations now
  flow through centralized strategies with typed status-merge and
  side-effect dispatch policies.
- Watch-event GC drains its backlog across bounded batches each tick, capping
  memory growth under sustained watch load.
- Status correctness across stale Raft applies: PDB, CronJob, and persistent
  volume metadata/status are rebased on stale puts, scheduler bind status is
  preserved through Raft, and generated dry-run writes are honored.
- Health and readiness endpoints are served on Raft followers.
- Control-plane bootstrap join token is stored in a single field.

### Kubernetes compatibility status

- Targets Kubernetes v1.34.6 API compatibility.
- `metrics.k8s.io/v1beta1` discovery and list/get endpoints are available
  (local-node oriented; cluster-wide on-demand fanout arrives in 0.9.12).
- API mutation and status-apply paths now use a centralized, type-aware merge
  boundary.

### Known beta limitations

Carried forward from 0.9.10, with the `metrics.k8s.io` API surface now
implemented and VXLAN dataplane removal substantially complete.

- `metrics.k8s.io/v1beta1` API surface is implemented but is local-node
  oriented; on-demand cross-node aggregation and runtime-sampled values are
  incomplete (addressed in 0.9.12).
- HPA autoscaling control loop and metrics-backed autoscaling remain deferred.
- Pod subresource coverage is incomplete: `pods/attach` is still not
  implemented, and the `pods/binding` subresource route is missing.
- Built-in OpenAPI schemas are incomplete. CRD OpenAPI publishing exists, but
  built-in kinds still expose stub schemas, so `kubectl explain` for built-in
  fields is limited.
- Scheduler behavior is not fully upstream-compatible. Known gaps include
  pod affinity/anti-affinity, topology spread constraints, PDB-aware
  preemption, preferred node-affinity scoring, hostPort conflict predicates,
  and some taint handling/default-priority behavior.
- PodSecurity admission is not implemented. Namespace labels such as
  `pod-security.kubernetes.io/enforce`, `audit`, and `warn` are not enforced.
- Some admission/defaulting behavior remains incomplete, including parts of
  ResourceQuota, LimitRange, DefaultStorageClass, Service family defaulting,
  Pod defaulting, ServiceAccount imagePullSecret propagation, and built-in
  field-selector validation.
- Watch and delete semantics still have known edge-case gaps, including
  selector-less `resourceVersion=0` watch behavior, pending-delete status
  codes, `DeleteCollection` dry-run handling, and some foreground/orphan
  deletion details.
- NetworkPolicy resources are stored but not yet enforced in the datapath.
- Aggregated API server support is passthrough-only; the kube-aggregator
  control plane is not implemented.
- API Priority and Fairness resources exist for CRUD/discovery, but request
  prioritization is not enforced.
- Structured audit logging is not yet implemented.
- Server-Side Apply (`application/apply-patch+yaml`) is implemented for built-in
  resources: `metadata.managedFields` is produced, cross-manager conflicts are
  reported (HTTP 409), `force=true` transfers ownership, and dropped fields are
  pruned. It is not yet complete: field ownership uses heuristic merge-key
  inference rather than a schema-driven engine, CRD apply does not produce
  `managedFields`, and protobuf responses omit `managedFields` (JSON-only).

### Binary packages

This release publishes:

- Static binaries: `klights-linux-x86_64-static`, `klights-linux-arm64-static`
- Ubuntu 24.04 (noble): `klights_0.9.11-1~noble_amd64.deb`, `klights_0.9.11-1~noble_arm64.deb`
- Ubuntu 26.04 (resolute): `klights_0.9.11-1~resolute_amd64.deb`, `klights_0.9.11-1~resolute_arm64.deb`
- RHEL 9: `klights-0.9.11-1.el9.x86_64.rpm`, `klights-0.9.11-1.el9.aarch64.rpm`
- RHEL 10: `klights-0.9.11-1.el10.x86_64.rpm`, `klights-0.9.11-1.el10.aarch64.rpm`
- RHEL runtime dependencies: `containerd-2.3.2-1.el9.x86_64.rpm`, `containerd-2.3.2-1.el9.aarch64.rpm`, `containerd-2.3.2-1.el10.x86_64.rpm`, `containerd-2.3.2-1.el10.aarch64.rpm`, `runc-1.5.0-1.el9.x86_64.rpm`, `runc-1.5.0-1.el9.aarch64.rpm`, `runc-1.5.0-1.el10.x86_64.rpm`, `runc-1.5.0-1.el10.aarch64.rpm`

Package repositories are published from the `package-repo` branch:

- APT: https://raw.githubusercontent.com/klights-net/klights-core/package-repo/apt
- RPM: https://raw.githubusercontent.com/klights-net/klights-core/package-repo/rpm
- Public key: https://raw.githubusercontent.com/klights-net/klights-core/package-repo/klights-archive-keyring.asc

## [0.9.10] - 2026-06-28

First public beta release of `klights-core`.

This beta is intended for early testing of the klights control plane, embedded
kubelet/runtime integration, and package distribution flow. The project goal is
full Kubernetes API compatibility with Kubernetes v1.34.6, but this beta is not
yet a complete Kubernetes replacement and still has known conformance gaps.

### Added

- Public tag-triggered GitHub Actions release workflow.
- Static binary packaging for Ubuntu 24.04 (`noble`) and Ubuntu 26.04 (`resolute`).
- Static binary packaging for RHEL 9 (`el9`) and RHEL 10 (`el10`).
- GitHub Pages publication for APT and RPM package repository metadata.
- Systemd service packaging with default `RUST_LOG=info`.
- Internal public release checklist in `public-release.md`.

### Kubernetes compatibility status

- Targets Kubernetes v1.34.6 API compatibility.
- Supports the core local-development packaging path for Ubuntu/Debian and
  RHEL-compatible hosts.
- Returns Kubernetes-style not-implemented behavior for some incomplete API
  surfaces instead of silently pretending support is complete.

### Known beta limitations

- Metrics API (`metrics.k8s.io`) is not implemented. `kubectl top` and
  metrics-backed HPA behavior are not available in this beta.
- HPA API storage/discovery exists, but the autoscaling control loop and metrics
  source integration are deferred.
- Pod subresource coverage is incomplete: `pods/attach` is still not implemented,
  and the `pods/binding` subresource route is missing.
- Built-in OpenAPI schemas are incomplete. CRD OpenAPI publishing exists, but
  built-in kinds still expose stub schemas, so `kubectl explain` for built-in
  fields is limited.
- Scheduler behavior is not fully upstream-compatible. Known gaps include
  pod affinity/anti-affinity, topology spread constraints, PDB-aware preemption,
  preferred node-affinity scoring, hostPort conflict predicates, and some taint
  handling/default-priority behavior.
- PodSecurity admission is not implemented. Namespace labels such as
  `pod-security.kubernetes.io/enforce`, `audit`, and `warn` are not enforced.
- Some admission/defaulting behavior remains incomplete, including parts of
  ResourceQuota, LimitRange, DefaultStorageClass, Service family defaulting,
  Pod defaulting, ServiceAccount imagePullSecret propagation, and built-in
  field-selector validation.
- Watch and delete semantics still have known edge-case gaps, including
  selector-less `resourceVersion=0` watch behavior, pending-delete status codes,
  `DeleteCollection` dry-run handling, and some foreground/orphan deletion
  details.
- NetworkPolicy resources are stored but not yet enforced in the datapath.
- Aggregated API server support is passthrough-only; the kube-aggregator control
  plane is not implemented.
- API Priority and Fairness resources exist for CRUD/discovery, but request
  prioritization is not enforced.
- Structured audit logging is not yet implemented.
- Server-Side Apply (`application/apply-patch+yaml`) is implemented for built-in
  resources: `metadata.managedFields` is produced, cross-manager conflicts are
  reported (HTTP 409), `force=true` transfers ownership, and dropped fields are
  pruned. It is not yet complete: field ownership uses heuristic merge-key
  inference rather than a schema-driven engine, CRD apply does not produce
  `managedFields`, and protobuf responses omit `managedFields` (JSON-only).
- VXLAN removal is still in progress. WireGuard is the intended encrypted
  dataplane; remaining VXLAN references are legacy cleanup work.

### Binary packages

This release publishes:

- Static binaries: `klights-linux-x86_64-static`, `klights-linux-arm64-static`
- Ubuntu 24.04 (noble): `klights_0.9.10-1~noble_amd64.deb`, `klights_0.9.10-1~noble_arm64.deb`
- Ubuntu 26.04 (resolute): `klights_0.9.10-1~resolute_amd64.deb`, `klights_0.9.10-1~resolute_arm64.deb`
- RHEL 9: `klights-0.9.10-1.el9.x86_64.rpm`, `klights-0.9.10-1.el9.aarch64.rpm`
- RHEL 10: `klights-0.9.10-1.el10.x86_64.rpm`, `klights-0.9.10-1.el10.aarch64.rpm`
- RHEL runtime dependencies: `containerd-2.3.2-1.el9.x86_64.rpm`, `containerd-2.3.2-1.el9.aarch64.rpm`, `containerd-2.3.2-1.el10.x86_64.rpm`, `containerd-2.3.2-1.el10.aarch64.rpm`, `runc-1.5.0-1.el9.x86_64.rpm`, `runc-1.5.0-1.el9.aarch64.rpm`, `runc-1.5.0-1.el10.x86_64.rpm`, `runc-1.5.0-1.el10.aarch64.rpm`

Package repositories are published from the `package-repo` branch:

- APT: https://raw.githubusercontent.com/klights-net/klights-core/package-repo/apt
- RPM: https://raw.githubusercontent.com/klights-net/klights-core/package-repo/rpm
- Public key: https://raw.githubusercontent.com/klights-net/klights-core/package-repo/klights-archive-keyring.asc

### Notes

- GitHub Pages must be enabled with source set to GitHub Actions before the
  first public tag release.
- Optional repository signing uses GitHub encrypted secrets
  `PACKAGE_GPG_PRIVATE_KEY` and `PACKAGE_GPG_PASSPHRASE`.
