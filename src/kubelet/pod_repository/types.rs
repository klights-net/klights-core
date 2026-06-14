//! Value types passed across the `PodRepository` trait surface.
//!
//! These types live in their own module so trait declarations in `mod.rs`
//! stay focused on behavior and so consumer crates can re-import the
//! domain shapes without pulling in the implementation modules.

use serde_json::Value;

/// Standard post-sandbox / post-IP-assignment pod status update.
///
/// Built by lifecycle code after `RunPodSandbox` returns and the CNI shim
/// has assigned an IP. Carries the runtime view that's authoritative for a
/// just-started pod.
#[derive(Debug, Clone)]
pub struct PodStatusUpdate {
    pub phase: String,
    pub pod_ip: String,
    pub host_ip: String,
    pub container_statuses: Vec<Value>,
    pub init_container_statuses: Option<Vec<Value>>,
    pub qos_class: Option<String>,
}

/// Reduced status view used by the runtime-reconcile event handler.
///
/// Only `phase` and `containerStatuses` get overwritten — IPs, conditions,
/// and qosClass are preserved from the prior status because the runtime
/// reconciler does not own them.
#[derive(Debug, Clone)]
pub struct RuntimeReconcileStatus {
    pub phase: String,
    pub container_statuses: Vec<Value>,
}

/// Pod IP / host IP pair returned by `PodNetworkReader::read_pod_network_assignment`.
///
/// `host_network=true` pods get the same string in both fields. Otherwise
/// `pod_ip` comes from the `pod_network` row written by the klights CNI
/// shim during containerd `RunPodSandbox`.
#[derive(Debug, Clone)]
pub struct PodNetworkAssignment {
    pub pod_ip: String,
    pub host_ip: String,
}

/// Patch content type for `PodSubresourceWriter::patch_status_from_api`
/// and `PodApiWriter::api_patch_pod`.
///
/// Mirrors the four arms of `crate::api::apply_patch` (see
/// `src/api/helpers.rs`). Do not collapse to `crate::datastore::PatchKind`,
/// which only knows `Merge` and would regress patch semantics on Pod
/// `/status`.
#[derive(Debug, Clone, Copy)]
pub enum PodStatusPatchType {
    /// `application/json-patch+json` (RFC 6902)
    JsonPatch,
    /// `application/merge-patch+json` (RFC 7386 / 7396) and `application/json`
    MergePatch,
    /// `application/strategic-merge-patch+json`
    StrategicMerge,
    /// `application/apply-patch+yaml` (server-side apply)
    ApplyPatch,
}

/// Map an HTTP `Content-Type` header value to the strongly-typed
/// [`PodStatusPatchType`] enum used by `PodSubresourceWriter`.
///
/// Mirrors the dispatch in `crate::api::apply_patch`:
/// - `application/json-patch+json` -> `JsonPatch` (RFC 6902)
/// - `application/strategic-merge-patch+json` -> `StrategicMerge`
/// - `application/apply-patch+yaml` -> `ApplyPatch` (server-side apply)
/// - everything else (including `application/merge-patch+json`,
///   `application/json`, missing header) -> `MergePatch` (default)
pub fn content_type_to_patch_type(ct: Option<&str>) -> PodStatusPatchType {
    match ct {
        Some("application/json-patch+json") => PodStatusPatchType::JsonPatch,
        Some("application/strategic-merge-patch+json") => PodStatusPatchType::StrategicMerge,
        Some("application/apply-patch+yaml") => PodStatusPatchType::ApplyPatch,
        _ => PodStatusPatchType::MergePatch,
    }
}

/// Input to `PodApiWriter::api_create_pod`.
///
/// Mirrors today's `src/pod_create.rs::create_pod_through_pipeline`
/// signature so the migration in Task 11.B is a straight body move.
#[derive(Debug, Clone)]
pub struct PodApiCreateRequest {
    pub namespace: String,
    pub name: String,
    pub body: Value,
    pub dry_run: bool,
    pub run_admission: bool,
}

/// Output of `PodApiWriter::api_create_pod`.
///
/// `resource = None` only on the dry-run path; persisted creates always
/// return `Some(resource)`. `body` is the JSON shape returned to the HTTP
/// caller (post-normalization, with `resourceVersion` injected).
#[derive(Debug, Clone)]
pub struct PodApiCreateResult {
    pub resource: Option<crate::datastore::Resource>,
    pub body: Value,
}

/// Output of `PodApiWriter::api_update_pod` and `api_patch_pod`.
#[derive(Debug, Clone)]
pub enum PodApiUpdateOutcome {
    /// Persisted update; carries the post-write `Resource` (including new
    /// `resource_version`).
    Persisted(crate::datastore::Resource),
    /// Dry run — no DB write, returns the would-be body.
    DryRun(Value),
}

/// Output of `PodApiWriter::api_delete_pod`.
#[derive(Debug, Clone)]
pub enum PodApiDeleteOutcome {
    /// Graceful delete: `deletionTimestamp` and
    /// `deletionGracePeriodSeconds` were set; a UID-bound deferred cleanup
    /// reminder was enqueued to `PodWorkqueue`.
    GracefulSet(crate::datastore::Resource),
    /// Dry run — no DB write, returns the would-be body.
    DryRun(Value),
}
