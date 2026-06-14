// Status subresource handlers

use axum::{
    Json,
    body::Bytes,
    extract::{Path, State},
    http::HeaderMap,
};
use serde_json::Value;
use std::sync::Arc;

use crate::api::{AppError, AppState, LenientJson, apply_patch, inject_resource_version};
use crate::datastore::{PatchKind, ResourcePreconditions};

pub fn ensure_type_meta(
    obj: impl Into<std::sync::Arc<Value>>,
    api_version: &str,
    kind: &str,
) -> Value {
    let mut obj = std::sync::Arc::unwrap_or_clone(obj.into());
    if let Some(map) = obj.as_object_mut() {
        let api_version_missing_or_empty = map
            .get("apiVersion")
            .and_then(|v| v.as_str())
            .map(|s| s.is_empty())
            .unwrap_or(true);
        if api_version_missing_or_empty {
            map.insert(
                "apiVersion".to_string(),
                Value::String(api_version.to_string()),
            );
        }
        let kind_missing_or_empty = map
            .get("kind")
            .and_then(|v| v.as_str())
            .map(|s| s.is_empty())
            .unwrap_or(true);
        if kind_missing_or_empty {
            map.insert("kind".to_string(), Value::String(kind.to_string()));
        }
    }
    obj
}

/// Decode a request body that may be protobuf (k8s\x00 prefix) or JSON.
pub fn decode_patch_body(body: &Bytes) -> Result<Value, AppError> {
    if body.len() >= 4 && &body[..4] == b"k8s\x00" {
        crate::protobuf::decode_protobuf(&body[4..])
            .map_err(|e| AppError::BadRequest(format!("Failed to decode protobuf: {}", e)))
    } else {
        serde_json::from_slice(body)
            .map_err(|e| AppError::BadRequest(format!("Invalid JSON: {}", e)))
    }
}

async fn enqueue_post_status_reconcile(
    state: &AppState,
    api_version: &str,
    kind: &str,
    resource_data: &Value,
) {
    if !matches!(
        (api_version, kind),
        ("apps/v1", "Deployment")
            | ("apps/v1", "ReplicaSet")
            | ("apps/v1", "StatefulSet")
            | ("apps/v1", "DaemonSet")
            | ("batch/v1", "Job")
            | ("v1", "ReplicationController")
            | ("policy/v1", "PodDisruptionBudget")
    ) {
        return;
    }

    state.controller_dispatcher.enqueue(resource_data).await;
}

/// Generic status subresource update handler for namespaced resources.
///
/// K8s status subresource semantics:
/// - `PUT /apis/{group}/{version}/namespaces/{ns}/{resource}/{name}/status`
/// - `.status` is taken from the request body and replaces the existing status.
/// - `metadata.annotations` and `metadata.labels` from the body are merged into
///   metadata; all other top-level fields (including `.spec`) are preserved.
///
/// The status write goes through `update_status_only`, which uses
/// `json_set(data, '$.status', ?)` atomically inside SQLite — there is no
/// read-modify-write window, so a concurrent user `kubectl scale` (PATCH on
/// `.spec.replicas`) cannot be lost between the controller's read and the
/// status write.
pub async fn update_status_subresource(
    state: Arc<AppState>,
    api_version: String,
    kind: String,
    namespace: String,
    name: String,
    body: Value,
) -> Result<Json<Value>, AppError> {
    // Existence check — surface 404 before any write attempt.
    let existing_resource = state
        .db
        .get_resource(&api_version, &kind, Some(&namespace), &name)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("{} {}/{} not found", kind, namespace, name)))?;

    // Honor body's metadata.resourceVersion as the CAS guard when the caller
    // supplied one; otherwise skip CAS (K8s contract for unconditional updates).
    let expected_rv = body
        .pointer("/metadata/resourceVersion")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i64>().ok());

    // 1. Apply status atomically via json_set — preserves `.spec` against any
    //    concurrent user write.
    let body_status = body.get("status").cloned().unwrap_or(Value::Null);
    state
        .db
        .update_status_only_with_preconditions(
            &api_version,
            &kind,
            Some(&namespace),
            &name,
            body_status,
            ResourcePreconditions {
                uid: Some(existing_resource.uid.clone()),
                resource_version: expected_rv,
            },
        )
        .await?;

    // 2. If the body specifies annotation or label updates, apply them as a
    //    metadata-only merge patch. patch_resource_latest runs inside an
    //    Immediate transaction, so this second write is also race-safe against
    //    concurrent spec edits (untouched fields pass through verbatim).
    if let Some(metadata_patch) = build_status_metadata_patch(body.get("metadata")) {
        state
            .db
            .patch_resource_latest_with_preconditions(
                &api_version,
                &kind,
                Some(&namespace),
                &name,
                crate::datastore::ResourcePatchRequest::new(
                    PatchKind::Merge,
                    metadata_patch,
                    ResourcePreconditions {
                        uid: Some(existing_resource.uid.clone()),
                        resource_version: None,
                    },
                ),
            )
            .await?;
    }

    // ResourceQuota: do NOT reconcile immediately after a /status PATCH/PUT.
    // The K8s conformance test watches for the patched status value to appear
    // in the watch stream before expecting the controller to eventually reset
    // it. Immediate reconciliation would overwrite the patched value before
    // the watch can observe it. The periodic background reconciler handles
    // syncing Status.Hard = Spec.Hard.

    let final_resource = state
        .db
        .get_resource(&api_version, &kind, Some(&namespace), &name)
        .await?
        .ok_or_else(|| {
            AppError::NotFound(format!(
                "{} {}/{} disappeared after status update",
                kind, namespace, name
            ))
        })?;
    let with_type_meta = ensure_type_meta(final_resource.data.clone(), &api_version, &kind);
    let result =
        crate::api::inject_resource_version(with_type_meta, final_resource.resource_version);
    enqueue_post_status_reconcile(state.as_ref(), &api_version, &kind, &final_resource.data).await;
    Ok(Json(result))
}

/// Build a merge-patch body that touches only the metadata fields the K8s
/// status subresource is allowed to mutate (`annotations`, `labels`). Returns
/// `None` when the body has none — saves a redundant DB write.
fn build_status_metadata_patch(body_meta: Option<&Value>) -> Option<Value> {
    let body_meta = body_meta?.as_object()?;
    let mut patch_meta = serde_json::Map::new();
    if let Some(annotations) = body_meta.get("annotations") {
        patch_meta.insert("annotations".to_string(), annotations.clone());
    }
    if let Some(labels) = body_meta.get("labels") {
        patch_meta.insert("labels".to_string(), labels.clone());
    }
    if patch_meta.is_empty() {
        None
    } else {
        Some(serde_json::json!({"metadata": Value::Object(patch_meta)}))
    }
}

pub async fn patch_status_subresource(
    state: Arc<AppState>,
    api_version: String,
    kind: String,
    namespace: String,
    name: String,
    patch: Value,
    content_type: Option<&str>,
) -> Result<Json<Value>, AppError> {
    // GET to apply the patch against the latest snapshot — needed to compute
    // the post-patch status and metadata. The actual writes go through paths
    // that do NOT depend on this snapshot's resourceVersion for `.spec`
    // preservation: the status write uses json_set inside SQLite, and the
    // metadata write uses an Immediate-tx merge patch.
    let resource = state
        .db
        .get_resource(&api_version, &kind, Some(&namespace), &name)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("{} {}/{} not found", kind, namespace, name)))?;

    let patched = apply_patch(&resource.data, &patch, content_type)?;

    // 1. Status write — atomic, preserves spec.
    if let Some(new_status) = patched.get("status") {
        state
            .db
            .update_status_only_with_preconditions(
                &api_version,
                &kind,
                Some(&namespace),
                &name,
                new_status.clone(),
                ResourcePreconditions {
                    uid: Some(resource.uid.clone()),
                    resource_version: None,
                },
            )
            .await?;
    }

    // 2. Metadata write — annotations/labels only (status subresource may not
    //    overwrite uid/resourceVersion/creationTimestamp). Skip when nothing
    //    actually changed in the allowed fields.
    if let Some(metadata_patch) = build_status_metadata_patch_from_diff(
        resource.data.get("metadata"),
        patched.get("metadata"),
    ) {
        state
            .db
            .patch_resource_latest_with_preconditions(
                &api_version,
                &kind,
                Some(&namespace),
                &name,
                crate::datastore::ResourcePatchRequest::new(
                    PatchKind::Merge,
                    metadata_patch,
                    ResourcePreconditions {
                        uid: Some(resource.uid.clone()),
                        resource_version: None,
                    },
                ),
            )
            .await?;
    }

    // ResourceQuota: the /status PATCH may diverge Status.Hard from Spec.Hard.
    // Spawn an async reconcile so the controller re-syncs Status.Hard = Spec.Hard.
    if kind == "ResourceQuota" {
        let db = state.db.clone();
        let pod_repository = state.pod_repository.clone();
        let ns = namespace.clone();
        if let Err(err) = state
            .task_supervisor
            .spawn_async(
                crate::task_supervisor::TaskCategory::Others,
                "resourcequota_post_status_reconcile",
                async move {
                    if let Err(e) =
                        crate::controllers::resource_quota::reconcile_resource_quotas_for_namespace(
                            db.as_ref(),
                            pod_repository.as_ref(),
                            &ns,
                        )
                        .await
                    {
                        tracing::warn!(
                            "ResourceQuota post-status reconcile failed for {}: {}",
                            ns,
                            e
                        );
                    }
                },
            )
            .await
        {
            tracing::warn!(
                "Failed to spawn ResourceQuota post-status reconcile task for {}: {}",
                namespace,
                err
            );
        }
    }

    let final_resource = state
        .db
        .get_resource(&api_version, &kind, Some(&namespace), &name)
        .await?
        .ok_or_else(|| {
            AppError::NotFound(format!(
                "{} {}/{} disappeared after status patch",
                kind, namespace, name
            ))
        })?;
    let with_type_meta = ensure_type_meta(final_resource.data.clone(), &api_version, &kind);
    let result =
        crate::api::inject_resource_version(with_type_meta, final_resource.resource_version);
    enqueue_post_status_reconcile(state.as_ref(), &api_version, &kind, &final_resource.data).await;
    Ok(Json(result))
}

/// Compute a metadata merge-patch body limited to annotation/label deltas
/// between the pre-patch and post-patch metadata. Returns `None` when nothing
/// changed in either of those fields.
fn build_status_metadata_patch_from_diff(
    before: Option<&Value>,
    after: Option<&Value>,
) -> Option<Value> {
    let after_obj = after?.as_object()?;
    let mut patch_meta = serde_json::Map::new();
    let before_annotations = before.and_then(|m| m.get("annotations"));
    let after_annotations = after_obj.get("annotations");
    if after_annotations != before_annotations
        && let Some(v) = after_annotations
    {
        patch_meta.insert("annotations".to_string(), v.clone());
    }
    let before_labels = before.and_then(|m| m.get("labels"));
    let after_labels = after_obj.get("labels");
    if after_labels != before_labels
        && let Some(v) = after_labels
    {
        patch_meta.insert("labels".to_string(), v.clone());
    }
    if patch_meta.is_empty() {
        None
    } else {
        Some(serde_json::json!({"metadata": Value::Object(patch_meta)}))
    }
}

// Generic cluster-scoped status helpers moved for cross-module re-use.
/// Generic status GET handler for cluster-scoped resources.
pub async fn get_cluster_status_subresource(
    state: Arc<AppState>,
    api_version: String,
    kind: String,
    name: String,
) -> Result<Json<Value>, AppError> {
    let resource = state
        .db
        .get_resource(&api_version, &kind, None, &name)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("{} {} not found", kind, name)))?;
    let result = inject_resource_version(resource.data, resource.resource_version);
    Ok(Json(result))
}

/// Generic status PUT handler for cluster-scoped resources.
pub async fn update_cluster_status_subresource(
    state: Arc<AppState>,
    api_version: String,
    kind: String,
    name: String,
    body: Value,
) -> Result<Json<Value>, AppError> {
    let existing_resource = state
        .db
        .get_resource(&api_version, &kind, None, &name)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("{} {} not found", kind, name)))?;

    let expected_rv = body
        .pointer("/metadata/resourceVersion")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i64>().ok());

    let mut body_status = body.get("status").cloned().unwrap_or(Value::Null);
    if api_version == "v1" && kind == "Node" {
        preserve_node_extended_resources(existing_resource.data.get("status"), &mut body_status);
    }
    state
        .db
        .update_status_only_with_preconditions(
            &api_version,
            &kind,
            None,
            &name,
            body_status,
            ResourcePreconditions {
                uid: Some(existing_resource.uid.clone()),
                resource_version: expected_rv,
            },
        )
        .await?;

    if let Some(metadata_patch) = build_status_metadata_patch(body.get("metadata")) {
        state
            .db
            .patch_resource_latest_with_preconditions(
                &api_version,
                &kind,
                None,
                &name,
                crate::datastore::ResourcePatchRequest::new(
                    PatchKind::Merge,
                    metadata_patch,
                    ResourcePreconditions {
                        uid: Some(existing_resource.uid.clone()),
                        resource_version: None,
                    },
                ),
            )
            .await?;
    }

    let final_resource = state
        .db
        .get_resource(&api_version, &kind, None, &name)
        .await?
        .ok_or_else(|| {
            AppError::NotFound(format!("{} {} disappeared after status update", kind, name))
        })?;
    let result = inject_resource_version(final_resource.data, final_resource.resource_version);
    Ok(Json(result))
}

pub fn preserve_node_extended_resources(existing_status: Option<&Value>, new_status: &mut Value) {
    preserve_node_extended_resource_map(existing_status, new_status, "capacity");
    preserve_node_extended_resource_map(existing_status, new_status, "allocatable");
    preserve_node_daemon_endpoint(existing_status, new_status);
}

fn preserve_node_extended_resource_map(
    existing_status: Option<&Value>,
    new_status: &mut Value,
    field: &str,
) {
    let Some(existing_map) = existing_status
        .and_then(|s| s.get(field))
        .and_then(|v| v.as_object())
    else {
        return;
    };
    let Some(new_map) = new_status.get_mut(field).and_then(|v| v.as_object_mut()) else {
        return;
    };

    for (resource_name, quantity) in existing_map {
        if resource_name.contains('/') && !new_map.contains_key(resource_name) {
            new_map.insert(resource_name.clone(), quantity.clone());
        }
    }
}

fn preserve_node_daemon_endpoint(existing_status: Option<&Value>, new_status: &mut Value) {
    if new_status
        .pointer("/daemonEndpoints/kubeletEndpoint/Port")
        .and_then(|v| v.as_i64())
        .is_some()
    {
        if let Some(endpoint_obj) = new_status
            .pointer_mut("/daemonEndpoints/kubeletEndpoint")
            .and_then(|v| v.as_object_mut())
        {
            endpoint_obj.remove("port");
        }
        return;
    }

    let port = existing_status
        .and_then(|status| {
            status
                .pointer("/daemonEndpoints/kubeletEndpoint/Port")
                .or_else(|| status.pointer("/daemonEndpoints/kubeletEndpoint/port"))
        })
        .and_then(|v| v.as_i64())
        .unwrap_or(10250);

    let Some(status_obj) = new_status.as_object_mut() else {
        return;
    };
    let daemon_endpoints = status_obj
        .entry("daemonEndpoints".to_string())
        .or_insert_with(|| serde_json::json!({}));
    if !daemon_endpoints.is_object() {
        *daemon_endpoints = serde_json::json!({});
    }
    let Some(daemon_obj) = daemon_endpoints.as_object_mut() else {
        return;
    };
    let kubelet_endpoint = daemon_obj
        .entry("kubeletEndpoint".to_string())
        .or_insert_with(|| serde_json::json!({}));
    if !kubelet_endpoint.is_object() {
        *kubelet_endpoint = serde_json::json!({});
    }
    if let Some(endpoint_obj) = kubelet_endpoint.as_object_mut() {
        endpoint_obj.remove("port");
        endpoint_obj.insert("Port".to_string(), serde_json::json!(port));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn node_status_preserve_keeps_daemon_endpoint_kubelet_port() {
        let existing_status = json!({
            "daemonEndpoints": {
                "kubeletEndpoint": {
                    "Port": 10250
                }
            },
            "capacity": {
                "cpu": "8"
            },
            "allocatable": {
                "cpu": "8"
            }
        });
        let mut new_status = json!({
            "capacity": {
                "cpu": "8"
            },
            "allocatable": {
                "cpu": "8"
            },
            "conditions": [
                {"type": "Ready", "status": "True"}
            ]
        });

        preserve_node_extended_resources(Some(&existing_status), &mut new_status);

        assert_eq!(
            new_status.pointer("/daemonEndpoints/kubeletEndpoint/Port"),
            Some(&json!(10250)),
            "Node status updates must preserve kubelet endpoint Port for e2e debug dump"
        );
        assert!(
            new_status
                .pointer("/daemonEndpoints/kubeletEndpoint/port")
                .is_none(),
            "Node status updates must not emit non-Kubernetes lowercase daemon endpoint port"
        );
    }
}

/// Generic status PATCH handler for cluster-scoped resources.
pub async fn patch_cluster_status_subresource(
    state: Arc<AppState>,
    api_version: String,
    kind: String,
    name: String,
    patch: Value,
    content_type: Option<&str>,
) -> Result<Json<Value>, AppError> {
    let resource = state
        .db
        .get_resource(&api_version, &kind, None, &name)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("{} {} not found", kind, name)))?;

    let patched = apply_patch(&resource.data, &patch, content_type)?;

    if let Some(new_status) = patched.get("status") {
        state
            .db
            .update_status_only_with_preconditions(
                &api_version,
                &kind,
                None,
                &name,
                new_status.clone(),
                ResourcePreconditions {
                    uid: Some(resource.uid.clone()),
                    resource_version: None,
                },
            )
            .await?;
    }

    if let Some(metadata_patch) = build_status_metadata_patch_from_diff(
        resource.data.get("metadata"),
        patched.get("metadata"),
    ) {
        state
            .db
            .patch_resource_latest_with_preconditions(
                &api_version,
                &kind,
                None,
                &name,
                crate::datastore::ResourcePatchRequest::new(
                    PatchKind::Merge,
                    metadata_patch,
                    ResourcePreconditions {
                        uid: Some(resource.uid.clone()),
                        resource_version: None,
                    },
                ),
            )
            .await?;
    }

    let final_resource = state
        .db
        .get_resource(&api_version, &kind, None, &name)
        .await?
        .ok_or_else(|| {
            AppError::NotFound(format!("{} {} disappeared after status patch", kind, name))
        })?;
    let result = inject_resource_version(final_resource.data, final_resource.resource_version);
    Ok(Json(result))
}

// Wrapper functions for specific resource types' status subresources

#[macro_export]
macro_rules! namespaced_status_update_handler {
    ($fn_name:ident, $api_version:expr_2021, $kind:expr_2021) => {
        pub async fn $fn_name(
            State(state): State<Arc<AppState>>,
            Path((namespace, name)): Path<(String, String)>,
            LenientJson(body): LenientJson<Value>,
        ) -> Result<Json<Value>, AppError> {
            $crate::api_status::update_status_subresource(
                state,
                $api_version.to_string(),
                $kind.to_string(),
                namespace,
                name,
                body,
            )
            .await
        }
    };
}

#[macro_export]
macro_rules! namespaced_status_patch_handler {
    ($fn_name:ident, $api_version:expr_2021, $kind:expr_2021) => {
        pub async fn $fn_name(
            State(state): State<Arc<AppState>>,
            Path((namespace, name)): Path<(String, String)>,
            headers: HeaderMap,
            body: Bytes,
        ) -> Result<Json<Value>, AppError> {
            let content_type = headers.get("content-type").and_then(|h| h.to_str().ok());
            let patch: Value = $crate::api_status::decode_patch_body(&body)?;
            $crate::api_status::patch_status_subresource(
                state,
                $api_version.to_string(),
                $kind.to_string(),
                namespace,
                name,
                patch,
                content_type,
            )
            .await
        }
    };
}

#[macro_export]
macro_rules! namespaced_status_get_handler {
    ($fn_name:ident, $api_version:expr_2021, $kind:expr_2021) => {
        pub async fn $fn_name(
            State(state): State<Arc<AppState>>,
            Path((namespace, name)): Path<(String, String)>,
        ) -> Result<Json<Value>, AppError> {
            let resource = state
                .db
                .get_resource($api_version, $kind, Some(&namespace), &name)
                .await?
                .ok_or_else(|| AppError::NotFound(format!("{} {} not found", $kind, name)))?;

            let data =
                $crate::api::inject_resource_version(resource.data, resource.resource_version);
            Ok(Json(data))
        }
    };
}

#[macro_export]
macro_rules! cluster_status_get_handler {
    ($fn_name:ident, $api_version:expr_2021, $kind:expr_2021) => {
        pub async fn $fn_name(
            State(state): State<Arc<AppState>>,
            Path(name): Path<String>,
        ) -> Result<Json<Value>, AppError> {
            $crate::api_status::get_cluster_status_subresource(
                state,
                $api_version.to_string(),
                $kind.to_string(),
                name,
            )
            .await
        }
    };
}

#[macro_export]
macro_rules! cluster_status_update_handler {
    ($fn_name:ident, $api_version:expr_2021, $kind:expr_2021) => {
        pub async fn $fn_name(
            State(state): State<Arc<AppState>>,
            Path(name): Path<String>,
            LenientJson(body): LenientJson<Value>,
        ) -> Result<Json<Value>, AppError> {
            $crate::api_status::update_cluster_status_subresource(
                state,
                $api_version.to_string(),
                $kind.to_string(),
                name,
                body,
            )
            .await
        }
    };
}

#[macro_export]
macro_rules! cluster_status_patch_handler {
    ($fn_name:ident, $api_version:expr_2021, $kind:expr_2021) => {
        pub async fn $fn_name(
            State(state): State<Arc<AppState>>,
            Path(name): Path<String>,
            headers: HeaderMap,
            body: Bytes,
        ) -> Result<Json<Value>, AppError> {
            let content_type = headers.get("content-type").and_then(|h| h.to_str().ok());
            let patch: Value = $crate::api_status::decode_patch_body(&body)?;
            $crate::api_status::patch_cluster_status_subresource(
                state,
                $api_version.to_string(),
                $kind.to_string(),
                name,
                patch,
                content_type,
            )
            .await
        }
    };
}

namespaced_status_update_handler!(update_deployment_status, "apps/v1", "Deployment");
namespaced_status_update_handler!(update_replicaset_status, "apps/v1", "ReplicaSet");
namespaced_status_patch_handler!(patch_replicaset_status, "apps/v1", "ReplicaSet");

namespaced_status_update_handler!(update_statefulset_status, "apps/v1", "StatefulSet");
namespaced_status_update_handler!(update_daemonset_status, "apps/v1", "DaemonSet");
namespaced_status_patch_handler!(patch_deployment_status, "apps/v1", "Deployment");
namespaced_status_patch_handler!(patch_statefulset_status, "apps/v1", "StatefulSet");
namespaced_status_patch_handler!(patch_daemonset_status, "apps/v1", "DaemonSet");
namespaced_status_get_handler!(
    get_persistentvolumeclaim_status,
    "v1",
    "PersistentVolumeClaim"
);
namespaced_status_update_handler!(
    update_persistentvolumeclaim_status,
    "v1",
    "PersistentVolumeClaim"
);
namespaced_status_patch_handler!(
    patch_persistentvolumeclaim_status,
    "v1",
    "PersistentVolumeClaim"
);
namespaced_status_update_handler!(update_service_status, "v1", "Service");
namespaced_status_patch_handler!(patch_service_status, "v1", "Service");
namespaced_status_update_handler!(update_ingress_status, "networking.k8s.io/v1", "Ingress");
namespaced_status_patch_handler!(patch_ingress_status, "networking.k8s.io/v1", "Ingress");

pub async fn update_node_status(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    LenientJson(body): LenientJson<Value>,
) -> Result<Json<Value>, AppError> {
    crate::api_status::update_cluster_status_subresource(
        state,
        "v1".to_string(),
        "Node".to_string(),
        name,
        body,
    )
    .await
}
