# Changelog

All notable public release changes for `klights-core` are documented here.

This project uses GitHub Releases as the canonical public release page. The
release workflow extracts the matching version section from this file and
attaches distro packages to the GitHub Release.

## [0.10.3] - 2026-08-16

This release strengthens multinode correctness, Kubernetes API compatibility,
Pod lifecycle reliability, and standalone package publication. It also
completes a major internal ownership refactor for maintainability and clearer
component boundaries.

### What's new

- **Multinode and Raft correctness.** Namespace, controller, CronJob, Service
  allocation, Node lifecycle, garbage-collection, and bootstrap mutations now
  consistently flow through committed Raft state. Snapshot delivery preserves
  live state, and visible worker outbox effects retain strict ordering and
  authority boundaries.
- **Pod and kubelet reliability.** Actor-owned Pod finalization is serialized
  and deadline-bounded, deferred deletion reminders survive leases, stale
  same-name replacements are rejected, and CRI startup, reconnect, sandbox
  event, and sandbox garbage-collection races are fenced. StatefulSet Pod
  finalization and unscheduled Pod CAS deletion now complete through replicated
  paths.
- **API, LIST, and watch compatibility.** Typed LIST continuation semantics,
  exact remaining-item counts, continuation TTL expiry, replicated
  `resourceVersion` advancement, cross-version Event lists, CRD replay, and
  positioned watch recovery are preserved across leader and datastore paths.
  No-op mutations no longer churn public resource versions.
- **Controller and admission behavior.** PodDisruptionBudget eviction admission
  is centralized, webhook calls use the service routing path, CronJob status is
  checked against live state, generated Pod name collisions are retried, and
  foreground/orphan garbage collection retries safely through Raft conflicts.
- **Networking and image distribution.** Routing watches remain live across
  Raft progress, Docker Hub image names are normalized, and image pulls can be
  routed through the registry proxy.
- **Maintainability.** A major internal refactor establishes focused crate
  ownership, private composition boundaries, and clearer datastore, kubelet,
  controller, networking, authentication, and replication interfaces.
- **Standalone releases.** `klights-core` release builds now create their own
  temporary root and no longer inherit the base repository's sccache wrapper,
  while base builds continue to use the shared sccache configuration.

### Kubernetes compatibility status

- Targets Kubernetes v1.34.6 API compatibility.
- Multinode writes consistently use committed Raft apply paths, including
  controller, lifecycle, allocation, and cleanup effects.
- JSON and protobuf LIST/watch behavior retains positioned resource-version
  and continuation semantics across failover and replay.

### Known beta limitations

The following limitations are carried forward from 0.9.14; this release does
not claim to resolve them:

- HPA autoscaling and metrics-backed control loops remain incomplete.
- Pod subresource coverage remains incomplete, including `pods/attach` and
  `pods/binding` compatibility gaps.
- Built-in OpenAPI schemas remain incomplete, limiting `kubectl explain` for
  built-in fields.
- Scheduler behavior is not fully upstream-compatible for affinity,
  anti-affinity, topology spread, PDB-aware preemption, preferred affinity
  scoring, hostPort predicates, and some taint/default-priority behavior.
- PodSecurity admission is not implemented.
- Some ResourceQuota, LimitRange, DefaultStorageClass, Service and Pod
  defaulting, ServiceAccount image-pull-secret propagation, and built-in field
  selector behavior remains incomplete.
- Some watch, delete, DeleteCollection, and foreground/orphan deletion edge
  semantics remain incomplete.
- NetworkPolicy resources are stored but are not yet enforced in the datapath.
- Aggregated API server support remains passthrough-only.
- API Priority and Fairness resources support CRUD/discovery, but request
  prioritization is not enforced.
- Server-Side Apply remains incomplete for schema-driven ownership, CRD
  managed fields, and protobuf `managedFields` responses.

## [0.9.14] - 2026-07-14

This release completes **HTTP content-negotiation (Accept) fidelity** for
clients such as client-go, kubectl, and ArgoCD, fixes a **Kubernetes
`resource.Quantity` overflow** misclassification affecting PVC and quota
accounting, and resolves two **watch and Node-status stability** issues: a
spurious 410 on absent exact-name watches and a self-triggering Node status
write loop.

### What's new

- **HTTP Accept content-negotiation fidelity.** Unary Accept negotiation now
  derives each server-supported representation's effective quality from its
  most-specific matching range, honoring `q=0` exclusions and wildcard
  specificity. For example, `Accept: application/json;q=0, */*;q=1` now selects
  protobuf when supported and returns HTTP 406 Not Acceptable when it is not,
  instead of ignoring the JSON exclusion or silently defaulting to JSON.
  Repeated headers, wildcard exclusions, fallback selection, malformed
  precision, and Service `/status` responses all share the same negotiation,
  so a 406 result propagates and a protobuf encode failure never silently falls
  back to JSON. This improves client-go / kubectl / ArgoCD content-negotiation
  compatibility.
- **Kubernetes `resource.Quantity` overflow fix.** Positive-exponent overflow
  is now classified against the exponent *after* canceling decimal scale
  against the denominator, instead of the raw decimal exponent. A valid
  quantity such as `0.` followed by 4,999 zeros and `1e5000` (which equals
  exactly one) is no longer misclassified as overflow (`i64::MAX`), fixing PVC
  capacity ordering, static-volume selection, and resource-quota accounting.
  Exponent cancellation is now constant-time via tracked decimal scales,
  avoiding repeated BigInt division; one shared Quantity parser serves PVC/PV
  matching and resource quota.
- **Watch stability for exact-name watches.** Exact-name collection watches
  stay open instead of returning a synthetic 410 when unrelated traffic
  advances the watch before its selected object exists. Scoped bookmarks stay
  anchored to the last delivered resource version.
- **Node status write-loop fix.** Timestamped Node condition transitions are
  treated as newer than legacy conditions without timestamps, and Raft status
  mutations are suppressed when typed status merging produces no persisted
  change — preventing self-triggering watch churn and PATCH starvation.

### Kubernetes compatibility status

- Targets Kubernetes v1.34.6 API compatibility.
- Unary HTTP `Accept` content negotiation now honors `q=0` exclusions and
  per-representation specificity, returning 406 Not Acceptable when no
  supported representation is acceptable.
- Kubernetes `resource.Quantity` parsing no longer misclassifies valid
  long-exponent quantities as overflow.

### Known beta limitations

Carried forward from 0.9.13; no listed limitation was fully resolved in this
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
  deletion details, although watch stability was further hardened in this
  release.
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
- Ubuntu 24.04 (noble): `klights_0.9.14-1~noble_amd64.deb`, `klights_0.9.14-1~noble_arm64.deb`
- Ubuntu 26.04 (resolute): `klights_0.9.14-1~resolute_amd64.deb`, `klights_0.9.14-1~resolute_arm64.deb`
- RHEL 9: `klights-0.9.14-1.el9.x86_64.rpm`, `klights-0.9.14-1.el9.aarch64.rpm`
- RHEL 10: `klights-0.9.14-1.el10.x86_64.rpm`, `klights-0.9.14-1.el10.aarch64.rpm`
- RHEL runtime dependencies: `containerd-2.3.2-1.el9.x86_64.rpm`, `containerd-2.3.2-1.el9.aarch64.rpm`, `containerd-2.3.2-1.el10.x86_64.rpm`, `containerd-2.3.2-1.el10.aarch64.rpm`, `runc-1.5.0-1.el9.x86_64.rpm`, `runc-1.5.0-1.el9.aarch64.rpm`, `runc-1.5.0-1.el10.x86_64.rpm`, `runc-1.5.0-1.el10.aarch64.rpm`

Package repositories are published from the `package-repo` branch:

- APT: https://raw.githubusercontent.com/klights-net/klights-core/package-repo/apt
- RPM: https://raw.githubusercontent.com/klights-net/klights-core/package-repo/rpm
- Public key: https://raw.githubusercontent.com/klights-net/klights-core/package-repo/klights-archive-keyring.asc

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
