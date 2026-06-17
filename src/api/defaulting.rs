//! Pure synchronous defaulting helpers used by the generic `create_inner`,
//! `update_inner`, `patch_inner`, and `delete_inner` handlers in
//! `generated_handlers.rs`.
//!
//! These functions take and mutate a `serde_json::Value` body without any
//! I/O, datastore access, or `AppState` plumbing — making them trivially
//! unit-testable in isolation. See `defaulting_tests.rs` for the test
//! coverage.
//!
//! Keep these functions side-effect-free: the only mutation should be
//! against the borrowed `Value` argument. Any logic requiring a database,
//! supervisor, network, or admission webhook belongs in
//! `helpers.rs`/`validation.rs` (async) or in a dedicated controller, not
//! here.

use crate::api::helpers::SPEC_BEARING_KINDS;
use crate::api::{apply_pod_container_defaults, compute_qos_class};
use serde_json::{Map, Value};

/// Inject namespace, name, UID, creationTimestamp, and generation defaults
/// into `body.metadata`. Mirrors the K8s API server's per-create metadata
/// stamping.
///
/// - `ns`: `Some(_)` for namespaced kinds, `None` for cluster-scoped.
/// - `resource_name`: the resolved resource name to stamp into
///   `metadata.name` (so a generated name from `metadata.generateName` is
///   reflected even when the request body has none).
///
/// UID is generated only when missing, null, or whitespace-only.
/// `creationTimestamp` and `generation` are added only when absent / null /
/// (for generation) zero.
pub fn inject_create_metadata(ns: Option<&str>, body: &mut Value, resource_name: &str) {
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    let Some(metadata) = obj.get_mut("metadata") else {
        return;
    };
    let Some(meta_obj) = metadata.as_object_mut() else {
        return;
    };

    if let Some(namespace) = ns {
        meta_obj.insert(
            "namespace".to_string(),
            Value::String(namespace.to_string()),
        );
    }
    meta_obj.insert("name".to_string(), Value::String(resource_name.to_string()));

    let uid_missing_or_empty = meta_obj
        .get("uid")
        .is_none_or(|v| v.is_null() || v.as_str().is_some_and(|s| s.trim().is_empty()));
    if uid_missing_or_empty {
        meta_obj.insert(
            "uid".to_string(),
            Value::String(uuid::Uuid::new_v4().to_string()),
        );
    }
    if meta_obj
        .get("creationTimestamp")
        .is_none_or(|v| v.is_null())
    {
        meta_obj.insert(
            "creationTimestamp".to_string(),
            Value::String(crate::utils::k8s_timestamp()),
        );
    }
    let r#gen = meta_obj
        .get("generation")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    if r#gen == 0 {
        meta_obj.insert("generation".to_string(), serde_json::json!(1));
    }
}

/// Apply Pod-specific create-time defaults: terminationGracePeriodSeconds,
/// container defaults, and a fresh status with phase=Pending and the
/// computed QoS class.
///
/// Idempotent: existing terminationGracePeriodSeconds is preserved, but
/// status is unconditionally written (matches the prior inline behavior in
/// `create_inner`).
pub fn apply_pod_create_defaults(body: &mut Value) {
    let qos_class = compute_qos_class(body);
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    let spec = obj.entry("spec").or_insert_with(|| serde_json::json!({}));
    if let Some(spec_obj) = spec.as_object_mut() {
        apply_pod_spec_create_defaults(spec_obj);
    }
    obj.insert(
        "status".to_string(),
        serde_json::json!({
            "phase": "Pending",
            "conditions": [
                {
                    "type": "Initialized",
                    "status": "True",
                    "lastTransitionTime": crate::utils::k8s_timestamp(),
                },
                {
                    "type": "Ready",
                    "status": "False",
                    "lastTransitionTime": crate::utils::k8s_timestamp(),
                },
                {
                    "type": "ContainersReady",
                    "status": "False",
                    "lastTransitionTime": crate::utils::k8s_timestamp(),
                },
                {
                    "type": "PodScheduled",
                    "status": "True",
                    "lastTransitionTime": crate::utils::k8s_timestamp(),
                }
            ],
            "containerStatuses": [],
            "qosClass": qos_class,
        }),
    );
}

/// Apply create-time defaults that live under `Pod.spec`.
pub fn apply_pod_spec_create_defaults(spec_obj: &mut Map<String, Value>) {
    if !spec_obj.contains_key("terminationGracePeriodSeconds") {
        spec_obj.insert(
            "terminationGracePeriodSeconds".to_string(),
            serde_json::json!(30),
        );
    }
    apply_pod_service_account_defaults(spec_obj);
    let dns_policy_missing_or_empty = spec_obj
        .get("dnsPolicy")
        .and_then(|v| v.as_str())
        .is_none_or(str::is_empty);
    if dns_policy_missing_or_empty {
        spec_obj.insert("dnsPolicy".to_string(), serde_json::json!("ClusterFirst"));
    }
    let scheduler_name_missing_or_empty = spec_obj
        .get("schedulerName")
        .and_then(|v| v.as_str())
        .is_none_or(str::is_empty);
    if scheduler_name_missing_or_empty {
        spec_obj.insert(
            "schedulerName".to_string(),
            serde_json::json!("default-scheduler"),
        );
    }
    apply_pod_container_defaults(spec_obj);
}

/// Default and mirror Pod ServiceAccount fields.
///
/// Kubernetes still carries the deprecated `spec.serviceAccount` alias in its
/// wire type. Keep it mirrored with `spec.serviceAccountName` so JSON and
/// protobuf paths agree.
pub fn apply_pod_service_account_defaults(spec_obj: &mut Map<String, Value>) {
    let service_account_name = spec_obj
        .get("serviceAccountName")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());
    let deprecated_service_account = spec_obj
        .get("serviceAccount")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());
    let chosen = service_account_name
        .or(deprecated_service_account)
        .unwrap_or("default")
        .to_string();

    spec_obj.insert(
        "serviceAccountName".to_string(),
        serde_json::json!(chosen.clone()),
    );
    spec_obj.insert("serviceAccount".to_string(), serde_json::json!(chosen));
}

/// Set `status.phase = "Pending"` on a freshly-created PVC when missing.
pub fn apply_pvc_create_defaults(body: &mut Value) {
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    let status = obj.entry("status").or_insert_with(|| serde_json::json!({}));
    let Some(status_obj) = status.as_object_mut() else {
        return;
    };
    let needs_phase = status_obj
        .get("phase")
        .is_none_or(|v| v.is_null() || v.as_str().is_some_and(str::is_empty));
    if needs_phase {
        status_obj.insert("phase".to_string(), serde_json::json!("Pending"));
    }
}

/// Set `status.phase` on a freshly-created PV when missing:
/// `"Bound"` if `spec.claimRef` is set, otherwise `"Available"`.
pub fn apply_pv_create_defaults(body: &mut Value) {
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    let spec_has_claim_ref = obj
        .get("spec")
        .and_then(|s| s.get("claimRef"))
        .is_some_and(|v| !v.is_null());
    let status = obj.entry("status").or_insert_with(|| serde_json::json!({}));
    let Some(status_obj) = status.as_object_mut() else {
        return;
    };
    let needs_phase = status_obj
        .get("phase")
        .is_none_or(|v| v.is_null() || v.as_str().is_some_and(str::is_empty));
    if needs_phase {
        let phase = if spec_has_claim_ref {
            "Bound"
        } else {
            "Available"
        };
        status_obj.insert("phase".to_string(), serde_json::json!(phase));
    }
}

/// Default `spec.replicas = 1` for the four workload kinds that have a
/// replicas field. No-op for other kinds and when replicas is already
/// explicitly set (including `replicas: 0`).
pub fn apply_workload_replicas_default(kind: &str, body: &mut Value) {
    if !matches!(
        kind,
        "Deployment" | "StatefulSet" | "ReplicaSet" | "ReplicationController"
    ) {
        return;
    }
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    let spec = obj.entry("spec").or_insert_with(|| serde_json::json!({}));
    let Some(spec_obj) = spec.as_object_mut() else {
        return;
    };
    if !spec_obj.contains_key("replicas") {
        spec_obj.insert("replicas".to_string(), serde_json::json!(1));
    }
}

/// For ReplicationController: when `spec.selector` is missing or empty,
/// copy it from `spec.template.metadata.labels`. Matches K8s's
/// "selector defaults to template labels" rule.
pub fn apply_replicationcontroller_selector_default(body: &mut Value) {
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    let spec = obj.entry("spec").or_insert_with(|| serde_json::json!({}));
    let Some(spec_obj) = spec.as_object_mut() else {
        return;
    };
    let selector_missing = spec_obj
        .get("selector")
        .is_none_or(|v| v.is_null() || v.as_object().is_some_and(|obj| obj.is_empty()));
    if !selector_missing {
        return;
    }
    if let Some(labels) = spec_obj
        .get("template")
        .and_then(|template| template.get("metadata"))
        .and_then(|metadata| metadata.get("labels"))
        .cloned()
    {
        spec_obj.insert("selector".to_string(), labels);
    }
}

/// Initialise ResourceQuota status at create time:
/// - `status.hard` mirrors `spec.hard`.
/// - `status.used` is a string-zero per `spec.hard` key.
///
/// No-op when `spec.hard` is missing — the quota controller will populate
/// status later. Matches the conformance-test expectation that
/// status.hard/used are visible immediately on create.
pub fn apply_resourcequota_create_status(body: &mut Value) {
    let Some(hard) = body.pointer("/spec/hard").cloned() else {
        return;
    };
    let used = if let Some(hard_obj) = hard.as_object() {
        let mut zeros = serde_json::Map::new();
        for key in hard_obj.keys() {
            zeros.insert(key.clone(), serde_json::json!("0"));
        }
        Value::Object(zeros)
    } else {
        serde_json::json!({})
    };
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    obj.insert(
        "status".to_string(),
        serde_json::json!({
            "hard": hard,
            "used": used,
        }),
    );
}

/// Increment `metadata.generation` by 1 when `body.spec` differs from
/// `current.spec`. No-op when spec is unchanged or the body has no metadata
/// object to carry the incremented generation.
pub fn increment_generation_for_spec_change(current: &Value, body: &mut Value) {
    if body.pointer("/spec") == current.pointer("/spec") {
        return;
    }
    let Some(meta) = body.pointer_mut("/metadata") else {
        return;
    };
    let Some(meta_obj) = meta.as_object_mut() else {
        return;
    };
    let current_gen = current
        .pointer("/metadata/generation")
        .and_then(|v| v.as_i64())
        .unwrap_or(1);
    meta_obj.insert("generation".to_string(), serde_json::json!(current_gen + 1));
}

/// Increment `metadata.generation` by 1 when `body.spec` differs from
/// `current.spec` AND `kind` is one of `SPEC_BEARING_KINDS`.
///
/// No-op for non-spec-bearing kinds and when spec is unchanged.
pub fn increment_generation_if_spec_changed(kind: &str, current: &Value, body: &mut Value) {
    if !SPEC_BEARING_KINDS.contains(&kind) {
        return;
    }
    increment_generation_for_spec_change(current, body);
}

/// Stamp `metadata.deletionTimestamp = now` and
/// `metadata.deletionGracePeriodSeconds = 0` on a resource being marked
/// for deletion.
///
/// Used in three places by `delete_inner`:
/// 1. dry-run delete responses,
/// 2. resources with finalizers (graceful delete writes back the marker
///    instead of hard-deleting),
/// 3. (any future caller).
///
/// Replaces existing values — callers should clone first if they need to
/// preserve the originals.
pub fn set_deletion_timestamp(body: &mut Value) {
    let Some(meta) = body.get_mut("metadata").and_then(|m| m.as_object_mut()) else {
        return;
    };
    meta.insert(
        "deletionTimestamp".to_string(),
        Value::String(crate::utils::k8s_timestamp()),
    );
    meta.insert(
        "deletionGracePeriodSeconds".to_string(),
        serde_json::json!(0),
    );
}

/// Pull `metadata.uid` out of a resource body as an owned `String`.
/// Returns the empty string when the path is missing or non-string —
/// matching the prior inline `.unwrap_or("").to_string()` shape so callers
/// don't need a separate "missing UID" branch.
#[cfg(test)]
pub fn extract_owner_uid(resource_data: &Value) -> String {
    resource_data
        .pointer("/metadata/uid")
        .and_then(|u| u.as_str())
        .unwrap_or("")
        .to_string()
}
