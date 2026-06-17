use crate::api::*;
use serde::Deserialize;
use serde_json::Value;
use std::cmp::Ordering;

pub fn ensure_namespace_status_phase_active(data: &mut Value) {
    let Some(obj) = data.as_object_mut() else {
        return;
    };

    let status = obj
        .entry("status".to_string())
        .or_insert_with(|| serde_json::json!({}));
    if !status.is_object() {
        *status = serde_json::json!({});
    }

    if let Some(status_obj) = status.as_object_mut() {
        let needs_default = match status_obj.get("phase") {
            None => true,
            Some(Value::Null) => true,
            Some(Value::String(s)) => s.trim().is_empty(),
            _ => false,
        };
        if needs_default {
            status_obj.insert("phase".to_string(), serde_json::json!("Active"));
        }
    }
}

/// P0-CORR-02: Helper to ensure `parent[key]` is a JSON array.
///
/// Replaces or initializes the field as an empty array `[]` when it is:
/// - missing
/// - `null`
/// - a non-array type (string, number, object)
///
/// Returns a mutable reference to the array for further manipulation.
///
/// This prevents panics when user input provides malformed JSON where
/// an array field is expected (e.g., `status.conditions` being a string).
///
/// # Panics
///
/// Will only panic if `parent` is not a JSON object (caller error).
pub fn ensure_array<'a>(
    parent: &'a mut serde_json::Value,
    key: &str,
) -> &'a mut Vec<serde_json::Value> {
    if !parent.get(key).is_some_and(serde_json::Value::is_array) {
        parent[key] = serde_json::json!([]);
    }
    parent[key]
        .as_array_mut()
        .expect("just initialized as array")
}

/// P0-CORR-02: Helper to ensure `parent[key]` is a JSON object.
///
/// Replaces or initializes the field as an empty object `{}` when it is:
/// - missing
/// - `null`
/// - a non-object type (string, number, array)
///
/// Returns a mutable reference to the object for further manipulation.
///
/// This prevents panics when user input provides malformed JSON where
/// an object field is expected (e.g., `status` being an array).
///
/// # Panics
///
/// Will only panic if `parent` is not a JSON object (caller error).
pub fn ensure_object<'a>(
    parent: &'a mut serde_json::Value,
    key: &str,
) -> &'a mut serde_json::Map<String, serde_json::Value> {
    if !parent.get(key).is_some_and(serde_json::Value::is_object) {
        parent[key] = serde_json::json!({});
    }
    parent[key]
        .as_object_mut()
        .expect("just initialized as object")
}

pub fn resource_has_finalizers(data: &Value, pointer: &str) -> bool {
    data.pointer(pointer)
        .and_then(|v| v.as_array())
        .is_some_and(|arr| !arr.is_empty())
}

pub fn set_namespace_terminating_status(
    namespace: &mut Value,
    with_content_failure_condition: bool,
) {
    if let Some(obj) = namespace.as_object_mut() {
        if let Some(metadata) = obj.get_mut("metadata").and_then(|m| m.as_object_mut())
            && (!metadata.contains_key("deletionTimestamp")
                || metadata["deletionTimestamp"].is_null())
        {
            metadata.insert(
                "deletionTimestamp".to_string(),
                serde_json::json!(crate::utils::k8s_timestamp()),
            );
        }

        let status = obj
            .entry("status".to_string())
            .or_insert_with(|| serde_json::json!({}));
        if let Some(status_obj) = status.as_object_mut() {
            status_obj.insert("phase".to_string(), serde_json::json!("Terminating"));
            if with_content_failure_condition {
                status_obj.insert(
                    "conditions".to_string(),
                    serde_json::json!([{
                        "type": "NamespaceDeletionContentFailure",
                        "status": "True",
                        "reason": "ContentDeletionFailed",
                        "message": "Namespace contains content with pending finalization",
                        "lastTransitionTime": crate::utils::k8s_timestamp(),
                    }]),
                );
            } else {
                status_obj.remove("conditions");
            }
        }
    }
}

pub async fn reconcile_namespace_termination(
    db: &dyn DatastoreBackend,
    namespace: &str,
    metrics: &crate::side_effects::SideEffectMetrics,
) -> Result<(), AppError> {
    reconcile_namespace_termination_inner(db, namespace, None, metrics).await
}

// `reconcile_namespace_termination_for_uid` (the legacy variant without an
// outcome return) was superseded by
// `reconcile_namespace_termination_for_uid_with_outcome`. Callers must use
// the outcome variant so they can decide whether to schedule a delayed
// retry — under churn the inner reconcile may legitimately return Ok while
// the namespace is still Terminating.

/// Outcome of an end-to-end termination reconcile. The workqueue uses this
/// to decide whether to schedule a delayed retry, since a single reconcile
/// pass under churn may legitimately return Ok while leaving the namespace
/// still Terminating (pods still draining, content pending finalization).
pub enum NamespaceTerminationOutcome {
    /// Namespace fully removed (or not present), or no deletionTimestamp.
    Finalized,
    /// Namespace still has the same UID and a deletionTimestamp — needs
    /// another reconcile pass.
    StillPending,
}

/// Reconcile namespace termination and report whether the namespace is
/// fully drained (Finalized) or still needs another pass (StillPending).
/// Centralizes the "still terminating?" check so the workqueue does not
/// have to perform its own forbidden DB query (the
/// tests/source_guard_tests.py restricts it to workqueue CRUD only).
pub async fn reconcile_namespace_termination_for_uid_with_outcome(
    db: &dyn DatastoreBackend,
    namespace: &str,
    expected_uid: &str,
    metrics: &crate::side_effects::SideEffectMetrics,
) -> Result<NamespaceTerminationOutcome, AppError> {
    let reconcile_result =
        reconcile_namespace_termination_inner(db, namespace, Some(expected_uid), metrics).await;

    // Whether the inner reconcile returned Ok or a transient Err, look at
    // the post-state to decide whether the workqueue should schedule a
    // delayed retry.
    let outcome = match db.get_namespace(namespace).await {
        Ok(Some(ns)) => {
            let same_uid = ns
                .data
                .pointer("/metadata/uid")
                .and_then(|v| v.as_str())
                .map(|u| u == expected_uid)
                .unwrap_or(false);
            let has_dt = ns
                .data
                .pointer("/metadata/deletionTimestamp")
                .and_then(|v| v.as_str())
                .is_some();
            if same_uid && has_dt {
                NamespaceTerminationOutcome::StillPending
            } else {
                NamespaceTerminationOutcome::Finalized
            }
        }
        Ok(None) => NamespaceTerminationOutcome::Finalized,
        // Conservative: if we can't read the namespace, assume work is
        // unfinished and let the caller schedule a retry.
        Err(_) => NamespaceTerminationOutcome::StillPending,
    };

    reconcile_result?;
    Ok(outcome)
}

async fn reconcile_namespace_termination_inner(
    db: &dyn DatastoreBackend,
    namespace: &str,
    expected_uid: Option<&str>,
    metrics: &crate::side_effects::SideEffectMetrics,
) -> Result<(), AppError> {
    // Termination reconcile is triggered from many call sites under load:
    // the API delete handler, every Pod actor finalize, and any deferred
    // workqueue entry that fires after the namespace was already deleted
    // by a prior reconcile. Each path reads the namespace fresh and then
    // CAS-updates it, so concurrent reconciles race on the same row and
    // produce `Conflict` (RV moved) or `NotFound` (someone else finished).
    // Both are benign — one of the racing reconciles already advanced the
    // termination — so we retry on Conflict and treat NotFound as success.
    for attempt in 0..5 {
        match reconcile_namespace_termination_once(db, namespace, expected_uid, metrics).await {
            Ok(()) => return Ok(()),
            Err(AppError::NotFound(_)) => return Ok(()),
            Err(AppError::Conflict(_)) if attempt < 4 => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

async fn reconcile_namespace_termination_once(
    db: &dyn DatastoreBackend,
    namespace: &str,
    expected_uid: Option<&str>,
    metrics: &crate::side_effects::SideEffectMetrics,
) -> Result<(), AppError> {
    let Some(current_ns) = db.get_namespace(namespace).await? else {
        return Ok(());
    };
    if let Some(expected_uid) = expected_uid {
        let current_uid = current_ns
            .data
            .pointer("/metadata/uid")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if current_uid != expected_uid {
            tracing::info!(
                namespace = %namespace,
                expected_uid = %expected_uid,
                current_uid = %current_uid,
                "namespace termination: stale queued work ignored"
            );
            return Ok(());
        }
    }

    let namespace_terminating = current_ns
        .data
        .pointer("/metadata/deletionTimestamp")
        .and_then(|v| v.as_str())
        .is_some();
    if !namespace_terminating {
        return Ok(());
    }

    let pods = db
        .list_namespace_resources_of_kind(namespace, "Pod")
        .await?;
    let mut pod_blockers = false;

    for resource in &pods {
        pod_blockers = true;
        let has_deletion_timestamp = resource
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some();

        if !has_deletion_timestamp {
            let mut pod: Value = (*resource.data).clone();
            if let Some(meta) = pod.get_mut("metadata").and_then(|m| m.as_object_mut()) {
                meta.insert(
                    "deletionTimestamp".to_string(),
                    serde_json::json!(crate::utils::k8s_timestamp()),
                );
                meta.insert(
                    "deletionGracePeriodSeconds".to_string(),
                    serde_json::json!(0),
                );
            }
            db.update_resource_with_preconditions(
                &resource.api_version.clone(),
                &resource.kind.clone(),
                Some(namespace),
                &resource.name.clone(),
                pod,
                crate::datastore::ResourcePreconditions::from_resource(resource),
            )
            .await?;
        }
    }

    let mut namespace_data: Value = (*current_ns.data).clone();
    set_namespace_terminating_status(&mut namespace_data, pod_blockers);
    let updated_ns = db
        .update_namespace(namespace, namespace_data, current_ns.resource_version)
        .await?;

    if pod_blockers {
        return Ok(());
    }

    let resources_after_pods = db
        .list_namespace_resources_excluding_kind(namespace, "Pod")
        .await?;
    for resource in &resources_after_pods {
        db.delete_resource(
            &resource.api_version.clone(),
            &resource.kind.clone(),
            Some(namespace),
            &resource.name.clone(),
        )
        .await?;
    }

    if db.count_namespace_resources(namespace).await? == 0
        && let Err(e) = db.delete_namespace(&updated_ns.name).await
    {
        // A concurrent reconcile may have already deleted the namespace;
        // surface NotFound so the outer retry loop returns Ok.
        let msg = e.to_string();
        if msg.contains("not found") || msg.contains("Not found") {
            return Err(AppError::NotFound(msg));
        }
        metrics
            .namespace_delete_failures_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::error!(
            namespace = %updated_ns.name,
            error = %e,
            "namespace termination: namespace delete failed"
        );
        return Err(AppError::Internal(format!(
            "namespace termination: namespace delete failed: {}",
            e
        )));
    }

    Ok(())
}

/// Process Secret stringData field, converting it to base64-encoded data field.
/// This implements K8s Secret behavior:
/// 1. If stringData is present, base64-encode each value and put into data field
/// 2. stringData values override data values for the same key
/// 3. Remove stringData field (it's write-only)
/// 4. If type is not set, default to "Opaque"
pub fn process_secret_stringdata(secret: &mut Value) {
    use base64::Engine;
    let engine = base64::engine::general_purpose::STANDARD;

    if let Some(obj) = secret.as_object_mut() {
        // Default type to "Opaque" if not set
        if !obj.contains_key("type") {
            obj.insert("type".to_string(), serde_json::json!("Opaque"));
        }

        // Check if stringData exists
        if let Some(string_data) = obj.remove("stringData")
            && let Some(string_data_obj) = string_data.as_object()
        {
            // Get or create data field
            let data = obj
                .entry("data".to_string())
                .or_insert_with(|| serde_json::json!({}));

            if let Some(data_obj) = data.as_object_mut() {
                // For each stringData entry, base64-encode and put into data
                for (key, value) in string_data_obj {
                    if let Some(plaintext) = value.as_str() {
                        let encoded = engine.encode(plaintext);
                        data_obj.insert(key.clone(), serde_json::json!(encoded));
                    }
                }
            }
        }
    }
}

/// Validate Secret data and stringData keys.
/// K8s rejects Secrets with empty string keys ("") in the data or stringData maps.
/// Returns Ok(()) if valid, Err(message) if invalid.
pub fn validate_secret_data(body: &Value) -> Result<(), String> {
    // Check data field
    if let Some(data) = body.get("data")
        && let Some(data_obj) = data.as_object()
    {
        for key in data_obj.keys() {
            if key.is_empty() {
                let name = body
                    .get("metadata")
                    .and_then(|m| m.get("name"))
                    .and_then(|n| n.as_str())
                    .unwrap_or("unknown");
                return Err(format!(
                    "Secret \"{}\" is invalid: data[]: Invalid value: \"\": a valid config key must consist of alphanumeric characters, '-', '_' or '.'",
                    name
                ));
            }
        }
    }

    // Check stringData field
    if let Some(string_data) = body.get("stringData")
        && let Some(string_data_obj) = string_data.as_object()
    {
        for key in string_data_obj.keys() {
            if key.is_empty() {
                let name = body
                    .get("metadata")
                    .and_then(|m| m.get("name"))
                    .and_then(|n| n.as_str())
                    .unwrap_or("unknown");
                return Err(format!(
                    "Secret \"{}\" is invalid: stringData[]: Invalid value: \"\": a valid config key must consist of alphanumeric characters, '-', '_' or '.'",
                    name
                ));
            }
        }
    }

    Ok(())
}

// Apply JSON Patch (RFC 6902) or JSON Merge Patch (RFC 7386)
/// Return the merge key for a known K8s array field path, or None if it should replace.
/// Path is the dot-joined field name within the resource (e.g. "status.conditions").
pub fn strategic_merge_key(field_path: &str) -> Option<&'static str> {
    match field_path {
        "metadata.ownerReferences" => Some("uid"),
        "status.conditions" => Some("type"),
        "spec.containers" | "spec.initContainers" | "spec.ephemeralContainers" => Some("name"),
        "spec.volumes" => Some("name"),
        "spec.imagePullSecrets" => Some("name"),
        "spec.hostAliases" => Some("ip"),
        "spec.tolerations" => Some("key"),
        // CRD versions array: merge by name so patching one version doesn't remove others
        "spec.versions" => Some("name"),
        // Service / EndpointSubset ports merge by port number.
        "spec.ports" => Some("port"),
        _ => nested_container_merge_key(field_path),
    }
}

/// Merge keys for arrays nested inside a container element
/// (`{spec,…}.{containers,initContainers,ephemeralContainers}.<field>`), so a
/// strategic patch of one container's ports/env/mounts merges element-by-element
/// instead of replacing the whole list. Works under any prefix, including
/// workload pod templates (`spec.template.spec.containers.ports`).
fn nested_container_merge_key(field_path: &str) -> Option<&'static str> {
    let (parent, last) = field_path.rsplit_once('.')?;
    let under_container = parent.ends_with("containers") || parent.ends_with("Containers");
    if !under_container {
        return None;
    }
    match last {
        "ports" => Some("containerPort"),
        "env" => Some("name"),
        "volumeMounts" => Some("mountPath"),
        "volumeDevices" => Some("devicePath"),
        _ => None,
    }
}

const SMP_PATCH: &str = "$patch";
const SMP_DELETE_PRIMITIVE: &str = "$deleteFromPrimitiveList/";
const SMP_SET_ORDER: &str = "$setElementOrder/";

/// Strip strategic-merge directive keys (`$patch`, `$deleteFromPrimitiveList/…`,
/// `$setElementOrder/…`) from a value being inserted wholesale (no existing
/// value to merge against), recursively. Keeps persisted objects clean.
fn strip_smp_directives(value: Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .filter(|(k, _)| {
                    k != SMP_PATCH
                        && !k.starts_with(SMP_DELETE_PRIMITIVE)
                        && !k.starts_with(SMP_SET_ORDER)
                })
                .map(|(k, v)| (k, strip_smp_directives(v)))
                .collect(),
        ),
        Value::Array(arr) => Value::Array(arr.into_iter().map(strip_smp_directives).collect()),
        other => other,
    }
}

fn smp_directive(map: &serde_json::Map<String, Value>) -> Option<&str> {
    map.get(SMP_PATCH).and_then(|v| v.as_str())
}

/// Strategic Merge Patch — like JSON merge patch but arrays with known merge keys
/// are merged element-by-element rather than replaced wholesale.
///
/// Takes `current` by value so the implementation can reuse the existing
/// allocation tree instead of cloning every intermediate map/array. Callers
/// that have a borrowed `&Value` should clone once at the top; the cost is
/// equivalent (or lower) to the per-recursion cloning the borrowed-only
/// version used to do.
pub fn strategic_merge(current: Value, patch: &Value, path: &str) -> Value {
    match (current, patch) {
        (Value::Object(mut cur_map), Value::Object(patch_map)) => {
            // `$patch: replace` — discard the current value, take the patch
            // object verbatim (minus the directive).
            if smp_directive(patch_map) == Some("replace") {
                return strip_smp_directives(Value::Object(patch_map.clone()));
            }

            for (key, patch_val) in patch_map {
                if key == SMP_PATCH || key.starts_with(SMP_SET_ORDER) {
                    continue; // handled separately
                }
                // `$deleteFromPrimitiveList/<field>: [v, …]` removes scalars.
                if let Some(field) = key.strip_prefix(SMP_DELETE_PRIMITIVE) {
                    if let (Some(list), Some(to_delete)) = (
                        cur_map.get_mut(field).and_then(|v| v.as_array_mut()),
                        patch_val.as_array(),
                    ) {
                        list.retain(|e| !to_delete.contains(e));
                    }
                    continue;
                }
                if patch_val.is_null() {
                    cur_map.remove(key);
                    continue;
                }
                // `{ "field": {"$patch": "delete"} }` deletes the key.
                if patch_val
                    .as_object()
                    .is_some_and(|m| smp_directive(m) == Some("delete"))
                {
                    cur_map.remove(key);
                    continue;
                }
                let child_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", path, key)
                };
                let merged = match cur_map.remove(key) {
                    Some(cur_val) => strategic_merge(cur_val, patch_val, &child_path),
                    None => strip_smp_directives(patch_val.clone()),
                };
                cur_map.insert(key.clone(), merged);
            }

            // `$setElementOrder/<field>: [...]` — reorder a merged list to follow
            // the requested order, appending any elements it does not mention.
            for (key, patch_val) in patch_map {
                if let Some(field) = key.strip_prefix(SMP_SET_ORDER) {
                    let child_path = if path.is_empty() {
                        field.to_string()
                    } else {
                        format!("{}.{}", path, field)
                    };
                    if let Some(list) = cur_map.get_mut(field).and_then(|v| v.as_array_mut()) {
                        apply_set_element_order(list, patch_val, &child_path);
                    }
                }
            }
            Value::Object(cur_map)
        }
        (Value::Array(cur_arr), Value::Array(patch_arr)) => {
            merge_strategic_array(cur_arr, patch_arr, path)
        }
        // For all other types, patch value replaces current.
        (_, patch_val) => strip_smp_directives(patch_val.clone()),
    }
}

/// Merge a patch array into a current array using the path's merge key, honoring
/// per-element `$patch: delete`/`replace` directives. Elements of the current
/// array not referenced by the patch are preserved.
fn merge_strategic_array(mut cur_arr: Vec<Value>, patch_arr: &[Value], path: &str) -> Value {
    let Some(merge_key) = strategic_merge_key(path) else {
        // No merge key — replace array (standard JSON merge patch behavior).
        return Value::Array(
            patch_arr
                .iter()
                .cloned()
                .map(strip_smp_directives)
                .collect(),
        );
    };

    let find_idx = |arr: &[Value], key_val: &Value| -> Option<usize> {
        arr.iter().position(|e| e.get(merge_key) == Some(key_val))
    };

    for patch_item in patch_arr {
        let directive = patch_item.as_object().and_then(smp_directive);
        let key_val = patch_item.get(merge_key).cloned();
        let idx = key_val.as_ref().and_then(|kv| find_idx(&cur_arr, kv));

        match directive {
            Some("delete") => {
                if let Some(i) = idx {
                    cur_arr.remove(i);
                }
            }
            Some("replace") => {
                let replacement = strip_smp_directives(patch_item.clone());
                match idx {
                    Some(i) => cur_arr[i] = replacement,
                    None => cur_arr.push(replacement),
                }
            }
            _ => match idx {
                Some(i) => {
                    let owned = std::mem::take(&mut cur_arr[i]);
                    cur_arr[i] = strategic_merge(owned, patch_item, path);
                }
                None => cur_arr.push(strip_smp_directives(patch_item.clone())),
            },
        }
    }
    Value::Array(cur_arr)
}

/// Reorder `list` so its elements follow `order`. For keyed lists `order` is a
/// list of `{<mergeKey>: v}` objects; for primitive lists it is the scalar
/// values. Elements absent from `order` keep their relative position at the end.
fn apply_set_element_order(list: &mut [Value], order: &Value, child_path: &str) {
    let Some(order) = order.as_array() else {
        return;
    };
    let rank_of = |elem: &Value| -> usize {
        match strategic_merge_key(child_path) {
            Some(merge_key) => {
                let key_val = elem.get(merge_key);
                order
                    .iter()
                    .position(|o| o.get(merge_key) == key_val)
                    .unwrap_or(usize::MAX)
            }
            None => order.iter().position(|o| o == elem).unwrap_or(usize::MAX),
        }
    };
    // Stable sort keeps unlisted elements (rank usize::MAX) in original order.
    list.sort_by_key(rank_of);
}

pub fn apply_patch(
    current: &Value,
    patch: &Value,
    content_type: Option<&str>,
) -> Result<Value, AppError> {
    match content_type {
        Some("application/json-patch+json") => {
            // JSON Patch (RFC 6902)
            let patch_ops = json_patch_crate::Patch::deserialize(patch)
                .map_err(|e| AppError::BadRequest(format!("Invalid JSON Patch: {}", e)))?;
            let mut doc = current.clone();
            json_patch_crate::patch(&mut doc, &patch_ops)
                .map_err(|e| AppError::BadRequest(format!("Patch failed: {}", e)))?;
            Ok(doc)
        }
        Some("application/merge-patch+json") | Some("application/json") | None => {
            // JSON Merge Patch (RFC 7386) - default
            let mut doc = current.clone();
            crate::json_patch::apply_merge_patch(&mut doc, patch)
                .map_err(|e| AppError::BadRequest(format!("Invalid JSON Merge Patch: {}", e)))?;
            Ok(doc)
        }
        Some("application/strategic-merge-patch+json") | Some("application/apply-patch+yaml") => {
            // Strategic Merge Patch - K8s specific
            // Arrays with known merge keys (status.conditions→type, spec.containers→name, etc.)
            // are merged element-by-element; everything else deep-merges like JSON merge patch.
            Ok(strategic_merge(current.clone(), patch, ""))
        }
        _ => Err(AppError::BadRequest(
            "Unsupported patch content type".to_string(),
        )),
    }
}

#[cfg(test)]
pub fn watch_event_from_type(event_type: &str, data: Value) -> WatchEvent {
    WatchEvent::from_type(event_type, data)
}

/// Workload resource kinds that carry a spec-version `metadata.generation`.
/// The API server increments generation when the spec field changes on
/// PUT/PATCH so controllers can use `status.observedGeneration` to detect
/// whether they have reconciled the latest spec.
pub const SPEC_BEARING_KINDS: &[&str] = &[
    "APIService",
    "CertificateSigningRequest",
    "CSIDriver",
    "CSINode",
    "DaemonSet",
    "Deployment",
    "FlowSchema",
    "HorizontalPodAutoscaler",
    "Ingress",
    "CronJob",
    "Job",
    "LimitRange",
    "MutatingWebhookConfiguration",
    "NetworkPolicy",
    "PersistentVolume",
    "PersistentVolumeClaim",
    "Pod",
    "PodDisruptionBudget",
    "PriorityLevelConfiguration",
    "ReplicaSet",
    "ReplicationController",
    "ResourceQuota",
    "Service",
    "StatefulSet",
    "ValidatingAdmissionPolicy",
    "ValidatingAdmissionPolicyBinding",
    "ValidatingWebhookConfiguration",
    "VolumeAttachment",
];

pub use crate::resource_semantics::preserve_status_subresource_on_main_update;

pub fn normalize_resource_for_storage(api_version: &str, kind: &str, body: &mut Value) {
    if api_version == "events.k8s.io/v1" && kind == "Event" {
        crate::utils::normalize_event_microtime_fields(body);
    }
    if api_version == "apps/v1" && kind == "Deployment" {
        apply_deployment_strategy_defaults(body);
    }
}

pub fn normalize_resource_for_read(api_version: &str, kind: &str, body: &mut Value) {
    if api_version == "v1" && kind == "Event" {
        normalize_events_v1_to_core_event_shape(body);
    }
}

pub fn normalize_events_v1_to_core_event_shape(body: &mut Value) {
    if body.get("apiVersion").and_then(|v| v.as_str()) != Some("events.k8s.io/v1") {
        return;
    }

    fn non_empty_str(value: Option<&Value>) -> Option<&str> {
        value.and_then(|v| v.as_str()).filter(|s| !s.is_empty())
    }

    if let Some(obj) = body.as_object_mut() {
        // Core/v1 event list/get paths expect involvedObject/source fields.
        if obj.get("involvedObject").is_none()
            && let Some(regarding) = obj.get("regarding").cloned()
        {
            obj.insert("involvedObject".to_string(), regarding);
        }

        if obj.get("source").is_none() {
            let mut source = serde_json::Map::new();
            if let Some(component) =
                non_empty_str(obj.get("deprecatedSource").and_then(|v| v.get("component")))
                    .or_else(|| non_empty_str(obj.get("reportingController")))
            {
                source.insert(
                    "component".to_string(),
                    Value::String(component.to_string()),
                );
            }
            if let Some(host) =
                non_empty_str(obj.get("deprecatedSource").and_then(|v| v.get("host")))
                    .or_else(|| non_empty_str(obj.get("reportingInstance")))
            {
                source.insert("host".to_string(), Value::String(host.to_string()));
            }
            if !source.is_empty() {
                obj.insert("source".to_string(), Value::Object(source));
            }
        }

        if obj.get("firstTimestamp").is_none()
            && let Some(v) = obj.get("deprecatedFirstTimestamp").cloned()
        {
            obj.insert("firstTimestamp".to_string(), v);
        }
        if obj.get("lastTimestamp").is_none()
            && let Some(v) = obj.get("deprecatedLastTimestamp").cloned()
        {
            obj.insert("lastTimestamp".to_string(), v);
        }

        obj.insert("apiVersion".to_string(), Value::String("v1".to_string()));
        obj.insert("kind".to_string(), Value::String("Event".to_string()));
    }
}

pub fn apply_deployment_strategy_defaults(body: &mut Value) {
    let Some(spec_obj) = body
        .get_mut("spec")
        .and_then(serde_json::Value::as_object_mut)
    else {
        return;
    };

    let strategy = spec_obj
        .entry("strategy".to_string())
        .or_insert_with(|| serde_json::json!({}));
    let Some(strategy_obj) = strategy.as_object_mut() else {
        return;
    };

    let strategy_type = strategy_obj
        .get("type")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .unwrap_or("RollingUpdate")
        .to_string();
    strategy_obj.insert("type".to_string(), Value::String(strategy_type.clone()));

    if strategy_type != "RollingUpdate" {
        return;
    }

    let rolling_update = strategy_obj
        .entry("rollingUpdate".to_string())
        .or_insert_with(|| serde_json::json!({}));
    let Some(rolling_update_obj) = rolling_update.as_object_mut() else {
        return;
    };

    rolling_update_obj
        .entry("maxUnavailable".to_string())
        .or_insert_with(|| Value::String("25%".to_string()));
    rolling_update_obj
        .entry("maxSurge".to_string())
        .or_insert_with(|| Value::String("25%".to_string()));
}

pub async fn apply_pod_runtimeclass_admission(
    db: &dyn DatastoreBackend,
    body: &mut Value,
) -> Result<(), AppError> {
    let Some(runtime_class_name) = body
        .get("spec")
        .and_then(|s| s.get("runtimeClassName"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
    else {
        return Ok(());
    };

    let rc_resource = db
        .get_resource("node.k8s.io/v1", "RuntimeClass", None, &runtime_class_name)
        .await?;
    let Some(rc_resource) = rc_resource else {
        return Err(AppError::Forbidden(format!(
            "Pod rejected: RuntimeClass \"{runtime_class_name}\" not found"
        )));
    };

    // RuntimeClass overhead is top-level `.overhead.podFixed` in node.k8s.io/v1.
    // Keep a legacy fallback for older test fixtures that used `.spec.overhead`.
    let overhead = rc_resource
        .data
        .pointer("/overhead/podFixed")
        .or_else(|| rc_resource.data.pointer("/spec/overhead/podFixed"));
    if let Some(overhead) = overhead
        && let Some(obj) = body.as_object_mut()
    {
        let spec = obj.entry("spec").or_insert_with(|| serde_json::json!({}));
        if let Some(spec_obj) = spec.as_object_mut() {
            spec_obj
                .entry("overhead")
                .or_insert_with(|| overhead.clone());
        }
    }

    Ok(())
}

pub async fn apply_limitrange_defaults_to_pod(
    db: &dyn DatastoreBackend,
    namespace: &str,
    pod: &mut Value,
) -> Result<(), AppError> {
    use serde_json::Map;
    use std::collections::HashSet;

    let ranges = db
        .list_resources(
            "v1",
            "LimitRange",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;

    let mut default_limits: Map<String, Value> = Map::new();
    let mut default_requests: Map<String, Value> = Map::new();

    for range in ranges.items {
        let Some(limit_items) = range
            .data
            .pointer("/spec/limits")
            .and_then(|v| v.as_array())
        else {
            continue;
        };
        for item in limit_items {
            if item.get("type").and_then(|t| t.as_str()) != Some("Container") {
                continue;
            }
            if let Some(default_obj) = item.get("default").and_then(|v| v.as_object()) {
                for (k, v) in default_obj {
                    default_limits.entry(k.clone()).or_insert_with(|| v.clone());
                }
            }
            if let Some(default_req_obj) = item.get("defaultRequest").and_then(|v| v.as_object()) {
                for (k, v) in default_req_obj {
                    default_requests
                        .entry(k.clone())
                        .or_insert_with(|| v.clone());
                }
            }
        }
    }

    if default_limits.is_empty() && default_requests.is_empty() {
        return Ok(());
    }

    let Some(spec_obj) = pod.get_mut("spec").and_then(|v| v.as_object_mut()) else {
        return Ok(());
    };

    for field in ["containers", "initContainers"] {
        let Some(containers) = spec_obj.get_mut(field).and_then(|v| v.as_array_mut()) else {
            continue;
        };
        for container in containers {
            let Some(container_obj) = container.as_object_mut() else {
                continue;
            };
            let resources = container_obj
                .entry("resources".to_string())
                .or_insert_with(|| serde_json::json!({}));
            let Some(resources_obj) = resources.as_object_mut() else {
                continue;
            };
            let explicit_limit_keys: HashSet<String> = resources_obj
                .get("limits")
                .and_then(|v| v.as_object())
                .map(|obj| obj.keys().cloned().collect())
                .unwrap_or_default();
            let explicit_request_keys: HashSet<String> = resources_obj
                .get("requests")
                .and_then(|v| v.as_object())
                .map(|obj| obj.keys().cloned().collect())
                .unwrap_or_default();

            let limits = resources_obj
                .entry("limits".to_string())
                .or_insert_with(|| serde_json::json!({}));
            if let Some(limits_obj) = limits.as_object_mut() {
                for (k, v) in &default_limits {
                    limits_obj.entry(k.clone()).or_insert_with(|| v.clone());
                }
            }
            let effective_limits = resources_obj.get("limits").cloned();

            let requests = resources_obj
                .entry("requests".to_string())
                .or_insert_with(|| serde_json::json!({}));
            if let Some(requests_obj) = requests.as_object_mut() {
                for (k, v) in &default_requests {
                    requests_obj.entry(k.clone()).or_insert_with(|| v.clone());
                }
                if let Some(limits_obj) = effective_limits.as_ref().and_then(|v| v.as_object()) {
                    // Kubernetes behavior: if a limit is explicitly set on a container
                    // and its matching request is omitted, default request to that limit.
                    for limit_key in &explicit_limit_keys {
                        if explicit_request_keys.contains(limit_key) {
                            continue;
                        }
                        if let Some(limit_val) = limits_obj.get(limit_key) {
                            requests_obj.insert(limit_key.clone(), limit_val.clone());
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

pub fn parse_limitrange_quantity(resource_key: &str, raw: &Value) -> Option<i64> {
    let quantity = raw.as_str()?;
    crate::controllers::resource_quota::parse_resource_quantity(resource_key, quantity)
}

pub fn parse_limitrange_ratio(raw: &Value) -> Option<f64> {
    raw.as_str()?.parse::<f64>().ok()
}

pub fn container_quantity(container: &Value, bucket: &str, resource_key: &str) -> Option<i64> {
    let raw = container
        .pointer(&format!("/resources/{bucket}/{resource_key}"))
        .and_then(|v| v.as_str())?;
    crate::controllers::resource_quota::parse_resource_quantity(resource_key, raw)
}

fn pod_effective_quantity(pod: &Value, bucket: &str, resource_key: &str) -> Option<i64> {
    let has_quantity = pod
        .pointer("/spec/containers")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .chain(
            pod.pointer("/spec/initContainers")
                .and_then(|v| v.as_array())
                .into_iter()
                .flatten(),
        )
        .any(|container| container_quantity(container, bucket, resource_key).is_some());
    if !has_quantity {
        return None;
    }
    Some(
        crate::controllers::resource_quota::calculate_pod_effective_resource_for_key(
            pod,
            bucket,
            resource_key,
        ),
    )
}

fn enforce_limitrange_pod_item(pod: &Value, item: &Value) -> Result<(), AppError> {
    let min = item.get("min").and_then(|v| v.as_object());
    let max = item.get("max").and_then(|v| v.as_object());

    if let Some(min_obj) = min {
        for (resource_key, min_raw) in min_obj {
            let Some(min_value) = parse_limitrange_quantity(resource_key, min_raw) else {
                continue;
            };
            for bucket in ["requests", "limits"] {
                if let Some(value) = pod_effective_quantity(pod, bucket, resource_key)
                    && value > 0
                    && value < min_value
                {
                    return Err(AppError::Forbidden(format!(
                        "minimum {bucket} for pod resource {resource_key} is {min_raw}"
                    )));
                }
            }
        }
    }

    if let Some(max_obj) = max {
        for (resource_key, max_raw) in max_obj {
            let Some(max_value) = parse_limitrange_quantity(resource_key, max_raw) else {
                continue;
            };
            for bucket in ["requests", "limits"] {
                if let Some(value) = pod_effective_quantity(pod, bucket, resource_key)
                    && value > max_value
                {
                    return Err(AppError::Forbidden(format!(
                        "maximum {bucket} for pod resource {resource_key} is {max_raw}"
                    )));
                }
            }
        }
    }

    Ok(())
}

pub async fn enforce_limitrange_constraints_for_pod(
    db: &dyn DatastoreBackend,
    namespace: &str,
    pod: &Value,
) -> Result<(), AppError> {
    let ranges = db
        .list_resources(
            "v1",
            "LimitRange",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;

    let Some(spec) = pod.get("spec").and_then(|v| v.as_object()) else {
        return Ok(());
    };

    let iter_containers = spec
        .get("containers")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .chain(
            spec.get("initContainers")
                .and_then(|v| v.as_array())
                .into_iter()
                .flatten(),
        );

    let containers: Vec<&Value> = iter_containers.collect();
    if containers.is_empty() {
        return Ok(());
    }

    for range in ranges.items {
        let Some(limit_items) = range
            .data
            .pointer("/spec/limits")
            .and_then(|v| v.as_array())
        else {
            continue;
        };

        for item in limit_items {
            match item.get("type").and_then(|t| t.as_str()) {
                Some("Pod") => {
                    enforce_limitrange_pod_item(pod, item)?;
                    continue;
                }
                Some("Container") => {}
                _ => continue,
            }

            let min = item.get("min").and_then(|v| v.as_object());
            let max = item.get("max").and_then(|v| v.as_object());
            let ratio = item.get("maxLimitRequestRatio").and_then(|v| v.as_object());

            for container in &containers {
                let container_name = container
                    .pointer("/name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");

                if let Some(min_obj) = min {
                    for (resource_key, min_raw) in min_obj {
                        let Some(min_value) = parse_limitrange_quantity(resource_key, min_raw)
                        else {
                            continue;
                        };
                        if let Some(request_value) =
                            container_quantity(container, "requests", resource_key)
                            && request_value < min_value
                        {
                            return Err(AppError::Forbidden(format!(
                                "minimum request for container {container_name} resource {resource_key} is {min_raw}, got {}",
                                container
                                    .pointer(&format!("/resources/requests/{resource_key}"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("0")
                            )));
                        }
                        if let Some(limit_value) =
                            container_quantity(container, "limits", resource_key)
                            && limit_value < min_value
                        {
                            return Err(AppError::Forbidden(format!(
                                "minimum limit for container {container_name} resource {resource_key} is {min_raw}, got {}",
                                container
                                    .pointer(&format!("/resources/limits/{resource_key}"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("0")
                            )));
                        }
                    }
                }

                if let Some(max_obj) = max {
                    for (resource_key, max_raw) in max_obj {
                        let Some(max_value) = parse_limitrange_quantity(resource_key, max_raw)
                        else {
                            continue;
                        };
                        if let Some(request_value) =
                            container_quantity(container, "requests", resource_key)
                            && request_value > max_value
                        {
                            return Err(AppError::Forbidden(format!(
                                "maximum request for container {container_name} resource {resource_key} is {max_raw}, got {}",
                                container
                                    .pointer(&format!("/resources/requests/{resource_key}"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("0")
                            )));
                        }
                        if let Some(limit_value) =
                            container_quantity(container, "limits", resource_key)
                            && limit_value > max_value
                        {
                            return Err(AppError::Forbidden(format!(
                                "maximum limit for container {container_name} resource {resource_key} is {max_raw}, got {}",
                                container
                                    .pointer(&format!("/resources/limits/{resource_key}"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("0")
                            )));
                        }
                    }
                }

                if let Some(ratio_obj) = ratio {
                    for (resource_key, ratio_raw) in ratio_obj {
                        let Some(max_ratio) = parse_limitrange_ratio(ratio_raw) else {
                            continue;
                        };
                        let request_value =
                            container_quantity(container, "requests", resource_key).unwrap_or(0);
                        let limit_value =
                            container_quantity(container, "limits", resource_key).unwrap_or(0);
                        if request_value > 0 && limit_value > 0 {
                            if limit_value < request_value {
                                return Err(AppError::Forbidden(format!(
                                    "limit must be greater than or equal to request for container {container_name} resource {resource_key}"
                                )));
                            }
                            let ratio_value = (limit_value as f64) / (request_value as f64);
                            if ratio_value > max_ratio {
                                return Err(AppError::Forbidden(format!(
                                    "maximum limit to request ratio per container for resource {resource_key} is {}, got {}",
                                    ratio_raw, ratio_value
                                )));
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

fn pvc_requested_storage(pvc: &Value) -> Option<i64> {
    let raw = pvc.pointer("/spec/resources/requests/storage")?;
    parse_limitrange_quantity("storage", raw)
}

pub async fn enforce_limitrange_constraints_for_pvc(
    db: &dyn DatastoreBackend,
    namespace: &str,
    pvc: &Value,
) -> Result<(), AppError> {
    let Some(storage) = pvc_requested_storage(pvc) else {
        return Ok(());
    };
    let ranges = db
        .list_resources(
            "v1",
            "LimitRange",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;

    for range in ranges.items {
        let Some(limit_items) = range
            .data
            .pointer("/spec/limits")
            .and_then(|v| v.as_array())
        else {
            continue;
        };
        for item in limit_items {
            if item.get("type").and_then(|t| t.as_str()) != Some("PersistentVolumeClaim") {
                continue;
            }
            if let Some(min_value) = item
                .pointer("/min/storage")
                .and_then(|raw| parse_limitrange_quantity("storage", raw))
                && storage < min_value
            {
                let min_raw = item
                    .pointer("/min/storage")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                return Err(AppError::Forbidden(format!(
                    "minimum storage request for PersistentVolumeClaim is {min_raw}"
                )));
            }
            if let Some(max_value) = item
                .pointer("/max/storage")
                .and_then(|raw| parse_limitrange_quantity("storage", raw))
                && storage > max_value
            {
                let max_raw = item
                    .pointer("/max/storage")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                return Err(AppError::Forbidden(format!(
                    "maximum storage request for PersistentVolumeClaim is {max_raw}"
                )));
            }
        }
    }

    Ok(())
}

pub async fn apply_default_storage_class_admission(
    db: &dyn DatastoreBackend,
    pvc: &mut Value,
) -> Result<(), AppError> {
    match pvc.pointer("/spec/storageClassName") {
        Some(Value::String(_)) => return Ok(()),
        Some(value) if !value.is_null() => return Ok(()),
        _ => {}
    }

    let storage_classes = db
        .list_resources(
            "storage.k8s.io/v1",
            "StorageClass",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;
    let Some(default_class) = storage_classes
        .items
        .iter()
        .filter(|resource| storage_class_is_default(&resource.data))
        .max_by(compare_storage_class_default_order)
    else {
        return Ok(());
    };
    let Some(default_name) = default_class
        .data
        .pointer("/metadata/name")
        .and_then(Value::as_str)
    else {
        return Ok(());
    };

    let Some(pvc_obj) = pvc.as_object_mut() else {
        return Ok(());
    };
    let spec = pvc_obj
        .entry("spec".to_string())
        .or_insert_with(|| serde_json::json!({}));
    if !spec.is_object() {
        return Ok(());
    }
    let Some(spec_obj) = spec.as_object_mut() else {
        return Ok(());
    };
    spec_obj.insert(
        "storageClassName".to_string(),
        Value::String(default_name.to_string()),
    );
    Ok(())
}

fn storage_class_is_default(storage_class: &Value) -> bool {
    const DEFAULT_SC_ANNOTATIONS: [&str; 2] = [
        "storageclass.kubernetes.io/is-default-class",
        "storageclass.beta.kubernetes.io/is-default-class",
    ];

    DEFAULT_SC_ANNOTATIONS.iter().any(|key| {
        storage_class
            .pointer("/metadata/annotations")
            .and_then(Value::as_object)
            .and_then(|annotations| annotations.get(*key))
            .and_then(Value::as_str)
            .is_some_and(|value| value.eq_ignore_ascii_case("true"))
    })
}

fn compare_storage_class_default_order(
    left: &&crate::datastore::Resource,
    right: &&crate::datastore::Resource,
) -> Ordering {
    storage_class_creation_timestamp(&left.data)
        .cmp(storage_class_creation_timestamp(&right.data))
        .then_with(|| left.resource_version.cmp(&right.resource_version))
        .then_with(|| left.name.cmp(&right.name))
}

fn storage_class_creation_timestamp(storage_class: &Value) -> &str {
    storage_class
        .pointer("/metadata/creationTimestamp")
        .and_then(Value::as_str)
        .unwrap_or("")
}

// Macro to generate generic handlers for namespaced resources
