# Changelog

All notable public release changes for `klights-core` are documented here.

This project uses GitHub Releases as the canonical public release page. The
release workflow extracts the matching version section from this file and
attaches distro packages to the GitHub Release.

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
- VXLAN removal is still in progress. WireGuard is the intended encrypted
  dataplane; remaining VXLAN references are legacy cleanup work.

### Notes

- GitHub Pages must be enabled with source set to GitHub Actions before the
  first public tag release.
- Optional repository signing uses GitHub encrypted secrets
  `PACKAGE_GPG_PRIVATE_KEY` and `PACKAGE_GPG_PASSPHRASE`.
