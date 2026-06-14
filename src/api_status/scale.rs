use axum::{
    Json,
    body::Bytes,
    extract::{Path, State},
    http::HeaderMap,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use serde_json::Value;
use std::sync::Arc;

use crate::api::{AppError, AppState, LenientJson, apply_patch};
use crate::datastore::{PatchKind, Resource, ResourcePreconditions};

// Scale endpoints are split from helpers to keep each file manageable.
// Authorization for scale subresources is enforced by the global
// `authorize_request` middleware chokepoint (see src/auth/middleware.rs).

/// Build a Scale JSON response from a resource's current state.
fn build_scale_response(
    name: &str,
    namespace: &str,
    resource_version: i64,
    replicas: i64,
    status_replicas: i64,
    selector_str: String,
) -> Value {
    let scale = k8s_openapi::api::autoscaling::v1::Scale {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            resource_version: Some(resource_version.to_string()),
            ..Default::default()
        },
        spec: Some(k8s_openapi::api::autoscaling::v1::ScaleSpec {
            replicas: Some(replicas as i32),
        }),
        status: Some(k8s_openapi::api::autoscaling::v1::ScaleStatus {
            replicas: status_replicas as i32,
            selector: if selector_str.is_empty() {
                None
            } else {
                Some(selector_str)
            },
        }),
    };
    serde_json::to_value(scale).unwrap_or_default()
}

/// Extract and validate `spec.replicas` from a Scale request body.
/// Returns the validated `i32` replica count or an appropriate error.
fn extract_scale_replicas(body: &Value) -> Result<i32, AppError> {
    let replicas_value = body
        .pointer("/spec/replicas")
        .ok_or_else(|| AppError::BadRequest("spec.replicas is required".to_string()))?;

    // Reject non-integer values (strings, floats, null, etc.)
    let as_i64 = replicas_value
        .as_i64()
        .ok_or_else(|| AppError::BadRequest("spec.replicas must be an integer".to_string()))?;

    // Reject values outside i32 range
    i32::try_from(as_i64)
        .map_err(|_| AppError::BadRequest("spec.replicas must fit in a 32-bit integer".to_string()))
}

fn extract_scale_resource_version(body: &Value) -> Result<Option<i64>, AppError> {
    let Some(resource_version) = body
        .pointer("/metadata/resourceVersion")
        .and_then(|v| v.as_str())
    else {
        return Ok(None);
    };
    if resource_version.is_empty() {
        return Ok(None);
    }
    resource_version.parse::<i64>().map(Some).map_err(|_| {
        AppError::BadRequest("metadata.resourceVersion must be an integer string".to_string())
    })
}

/// Extract the selector string from a resource's spec.selector.
/// For apps/v1 resources (Deployment, StatefulSet, ReplicaSet), the selector
/// uses `matchLabels`. Returns empty string when no selector is found.
fn selector_string_from_match_labels(resource: &Resource) -> String {
    resource
        .data
        .pointer("/spec/selector")
        .and_then(|s| s.pointer("/matchLabels"))
        .and_then(|ml| ml.as_object())
        .map(|obj| {
            obj.iter()
                .map(|(k, v)| format!("{}={}", k, v.as_str().unwrap_or("")))
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default()
}

/// Extract the selector string from a ReplicationController's flat spec.selector.
fn selector_string_from_flat_selector(resource: &Resource) -> String {
    resource
        .data
        .pointer("/spec/selector")
        .and_then(|s| s.as_object())
        .map(|obj| {
            obj.iter()
                .map(|(k, v)| format!("{}={}", k, v.as_str().unwrap_or("")))
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default()
}

pub async fn get_replicaset_scale(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    let rs = state
        .db
        .get_resource("apps/v1", "ReplicaSet", Some(&namespace), &name)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("replicaset {} not found", name)))?;

    let replicas = rs
        .data
        .pointer("/spec/replicas")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let status_replicas = rs
        .data
        .pointer("/status/replicas")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let selector_str = selector_string_from_match_labels(&rs);

    Ok(Json(build_scale_response(
        &name,
        &namespace,
        rs.resource_version,
        replicas,
        status_replicas,
        selector_str,
    )))
}

pub async fn update_replicaset_scale(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    LenientJson(body): LenientJson<Value>,
) -> Result<Json<Value>, AppError> {
    update_apps_v1_scale(state, "ReplicaSet", namespace, name, body).await
}

/// Generic scale GET handler for apps/v1 resources (Deployment, StatefulSet, etc.)
async fn get_apps_v1_scale(
    state: Arc<AppState>,
    kind: &str,
    namespace: String,
    name: String,
) -> Result<Json<Value>, AppError> {
    let resource = state
        .db
        .get_resource("apps/v1", kind, Some(&namespace), &name)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("{} {} not found", kind.to_lowercase(), name)))?;

    let replicas = resource
        .data
        .pointer("/spec/replicas")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let status_replicas = resource
        .data
        .pointer("/status/replicas")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let selector_str = selector_string_from_match_labels(&resource);

    Ok(Json(build_scale_response(
        &name,
        &namespace,
        resource.resource_version,
        replicas,
        status_replicas,
        selector_str,
    )))
}

/// Generic scale PUT handler for apps/v1 resources
async fn update_apps_v1_scale(
    state: Arc<AppState>,
    kind: &str,
    namespace: String,
    name: String,
    body: Value,
) -> Result<Json<Value>, AppError> {
    let new_replicas = extract_scale_replicas(&body)?;
    let expected_resource_version = extract_scale_resource_version(&body)?;

    let resource = state
        .db
        .get_resource("apps/v1", kind, Some(&namespace), &name)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("{} {} not found", kind.to_lowercase(), name)))?;

    let updated = state
        .db
        .patch_resource_latest_with_preconditions(
            "apps/v1",
            kind,
            Some(&namespace),
            &name,
            crate::datastore::ResourcePatchRequest::new(
                PatchKind::Merge,
                serde_json::json!({"spec": {"replicas": new_replicas}}),
                ResourcePreconditions {
                    uid: Some(resource.uid),
                    resource_version: expected_resource_version,
                },
            ),
        )
        .await?
        .ok_or_else(|| AppError::NotFound(format!("{} {} not found", kind.to_lowercase(), name)))?;

    state.controller_dispatcher.enqueue(&updated.data).await;

    let status_replicas = updated
        .data
        .pointer("/status/replicas")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    Ok(Json(build_scale_response(
        &name,
        &namespace,
        updated.resource_version,
        new_replicas as i64,
        status_replicas,
        selector_string_from_match_labels(&updated),
    )))
}

async fn patch_apps_v1_scale_replicas_latest(
    state: Arc<AppState>,
    kind: &str,
    namespace: String,
    name: String,
    uid: String,
    replicas: i32,
) -> Result<Json<Value>, AppError> {
    let updated = state
        .db
        .patch_resource_latest_with_preconditions(
            "apps/v1",
            kind,
            Some(&namespace),
            &name,
            crate::datastore::ResourcePatchRequest::new(
                PatchKind::Merge,
                serde_json::json!({"spec": {"replicas": replicas}}),
                ResourcePreconditions::uid(uid),
            ),
        )
        .await?
        .ok_or_else(|| AppError::NotFound(format!("{} {} not found", kind.to_lowercase(), name)))?;

    state.controller_dispatcher.enqueue(&updated.data).await;

    let status_replicas = updated
        .data
        .pointer("/status/replicas")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let selector_str = selector_string_from_match_labels(&updated);

    Ok(Json(build_scale_response(
        &name,
        &namespace,
        updated.resource_version,
        replicas as i64,
        status_replicas,
        selector_str,
    )))
}

async fn patch_replicationcontroller_scale_replicas_latest(
    state: Arc<AppState>,
    namespace: String,
    name: String,
    uid: String,
    replicas: i32,
) -> Result<Json<Value>, AppError> {
    let updated = state
        .db
        .patch_resource_latest_with_preconditions(
            "v1",
            "ReplicationController",
            Some(&namespace),
            &name,
            crate::datastore::ResourcePatchRequest::new(
                PatchKind::Merge,
                serde_json::json!({"spec": {"replicas": replicas}}),
                ResourcePreconditions::uid(uid),
            ),
        )
        .await?
        .ok_or_else(|| AppError::NotFound(format!("replicationcontroller {} not found", name)))?;

    state.controller_dispatcher.enqueue(&updated.data).await;

    let status_replicas = updated
        .data
        .pointer("/status/replicas")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let selector_str = selector_string_from_flat_selector(&updated);

    Ok(Json(build_scale_response(
        &name,
        &namespace,
        updated.resource_version,
        replicas as i64,
        status_replicas,
        selector_str,
    )))
}

pub async fn get_deployment_scale(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    get_apps_v1_scale(state, "Deployment", namespace, name).await
}

pub async fn update_deployment_scale(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    LenientJson(body): LenientJson<Value>,
) -> Result<Json<Value>, AppError> {
    update_apps_v1_scale(state, "Deployment", namespace, name, body).await
}

pub async fn get_statefulset_scale(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    get_apps_v1_scale(state, "StatefulSet", namespace, name).await
}

pub async fn update_statefulset_scale(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    LenientJson(body): LenientJson<Value>,
) -> Result<Json<Value>, AppError> {
    update_apps_v1_scale(state, "StatefulSet", namespace, name, body).await
}

/// Generic PATCH /scale handler — fetches the current Scale projection,
/// applies the patch (json-patch / merge-patch / strategic-merge-patch), and
/// hands the resulting `spec.replicas` to the corresponding PUT path.
///
/// P0-E2E-20260423-04 regression: conformance scales workloads via PATCH;
/// previously the route only accepted GET/PUT and returned `405 method not
/// allowed`. apps/v1 path covers Deployment/ReplicaSet/StatefulSet.
async fn patch_apps_v1_scale(
    state: Arc<AppState>,
    kind: &str,
    namespace: String,
    name: String,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let patch = crate::api_status::decode_patch_body(&body)?;

    let resource = state
        .db
        .get_resource("apps/v1", kind, Some(&namespace), &name)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("{} {} not found", kind.to_lowercase(), name)))?;

    let replicas = resource
        .data
        .pointer("/spec/replicas")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let status_replicas = resource
        .data
        .pointer("/status/replicas")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let selector_str = selector_string_from_match_labels(&resource);
    let current_scale = build_scale_response(
        &name,
        &namespace,
        resource.resource_version,
        replicas,
        status_replicas,
        selector_str,
    );
    let patched = apply_patch(&current_scale, &patch, content_type.as_deref())?;
    let new_replicas = extract_scale_replicas(&patched)?;

    patch_apps_v1_scale_replicas_latest(state, kind, namespace, name, resource.uid, new_replicas)
        .await
}

pub async fn patch_deployment_scale(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    patch_apps_v1_scale(state, "Deployment", namespace, name, headers, body).await
}

pub async fn patch_statefulset_scale(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    patch_apps_v1_scale(state, "StatefulSet", namespace, name, headers, body).await
}

/// PATCH /replicasets/{name}/scale uses the same latest-spec patch semantics
/// as Deployment/StatefulSet PATCH /scale. Controller status writes may advance
/// the parent ReplicaSet resourceVersion between GET and PATCH; PATCH /scale is
/// UID-bound, not a stale full-object CAS.
pub async fn patch_replicaset_scale(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let patch = crate::api_status::decode_patch_body(&body)?;

    let resource = state
        .db
        .get_resource("apps/v1", "ReplicaSet", Some(&namespace), &name)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("replicaset {} not found", name)))?;
    let replicas = resource
        .data
        .pointer("/spec/replicas")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let status_replicas = resource
        .data
        .pointer("/status/replicas")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let selector_str = selector_string_from_match_labels(&resource);
    let current_scale = build_scale_response(
        &name,
        &namespace,
        resource.resource_version,
        replicas,
        status_replicas,
        selector_str,
    );
    let patched = apply_patch(&current_scale, &patch, content_type.as_deref())?;
    let new_replicas = extract_scale_replicas(&patched)?;

    patch_apps_v1_scale_replicas_latest(
        state,
        "ReplicaSet",
        namespace,
        name,
        resource.uid,
        new_replicas,
    )
    .await
}

pub async fn get_replicationcontroller_scale(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    let rc = state
        .db
        .get_resource("v1", "ReplicationController", Some(&namespace), &name)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("replicationcontroller {} not found", name)))?;

    let replicas = rc
        .data
        .pointer("/spec/replicas")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let status_replicas = rc
        .data
        .pointer("/status/replicas")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let selector_str = selector_string_from_flat_selector(&rc);

    Ok(Json(build_scale_response(
        &name,
        &namespace,
        rc.resource_version,
        replicas,
        status_replicas,
        selector_str,
    )))
}

pub async fn update_replicationcontroller_scale(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    LenientJson(body): LenientJson<Value>,
) -> Result<Json<Value>, AppError> {
    let new_replicas = extract_scale_replicas(&body)?;
    let expected_resource_version = extract_scale_resource_version(&body)?;

    let rc = state
        .db
        .get_resource("v1", "ReplicationController", Some(&namespace), &name)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("replicationcontroller {} not found", name)))?;

    let updated = state
        .db
        .patch_resource_latest_with_preconditions(
            "v1",
            "ReplicationController",
            Some(&namespace),
            &name,
            crate::datastore::ResourcePatchRequest::new(
                PatchKind::Merge,
                serde_json::json!({"spec": {"replicas": new_replicas}}),
                ResourcePreconditions {
                    uid: Some(rc.uid),
                    resource_version: expected_resource_version,
                },
            ),
        )
        .await?
        .ok_or_else(|| AppError::NotFound(format!("replicationcontroller {} not found", name)))?;

    state.controller_dispatcher.enqueue(&updated.data).await;

    let status_replicas = updated
        .data
        .pointer("/status/replicas")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    Ok(Json(build_scale_response(
        &name,
        &namespace,
        updated.resource_version,
        new_replicas as i64,
        status_replicas,
        selector_string_from_flat_selector(&updated),
    )))
}

pub async fn patch_replicationcontroller_scale(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let patch = crate::api_status::decode_patch_body(&body)?;

    let resource = state
        .db
        .get_resource("v1", "ReplicationController", Some(&namespace), &name)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("replicationcontroller {} not found", name)))?;
    let replicas = resource
        .data
        .pointer("/spec/replicas")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let status_replicas = resource
        .data
        .pointer("/status/replicas")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let selector_str = selector_string_from_flat_selector(&resource);
    let current_scale = build_scale_response(
        &name,
        &namespace,
        resource.resource_version,
        replicas,
        status_replicas,
        selector_str,
    );
    let patched = apply_patch(&current_scale, &patch, content_type.as_deref())?;
    let new_replicas = extract_scale_replicas(&patched)?;

    patch_replicationcontroller_scale_replicas_latest(
        state,
        namespace,
        name,
        resource.uid,
        new_replicas,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_scale_replicas_missing_spec_returns_400() {
        let body = json!({});
        let err = extract_scale_replicas(&body).unwrap_err();
        let msg = match err {
            AppError::BadRequest(msg) => msg,
            other => panic!("expected BadRequest, got {other:?}"),
        };
        assert!(
            msg.contains("spec.replicas is required"),
            "unexpected message: {msg}"
        );
    }

    #[test]
    fn extract_scale_replicas_string_value_returns_400() {
        let body = json!({"spec": {"replicas": "five"}});
        let err = extract_scale_replicas(&body).unwrap_err();
        let msg = match err {
            AppError::BadRequest(msg) => msg,
            other => panic!("expected BadRequest, got {other:?}"),
        };
        assert!(
            msg.contains("must be an integer"),
            "unexpected message: {msg}"
        );
    }

    #[test]
    fn extract_scale_replicas_i64_overflow_returns_400() {
        let body = json!({"spec": {"replicas": i64::MAX}});
        let err = extract_scale_replicas(&body).unwrap_err();
        let msg = match err {
            AppError::BadRequest(msg) => msg,
            other => panic!("expected BadRequest, got {other:?}"),
        };
        assert!(msg.contains("32-bit"), "unexpected message: {msg}");
    }

    #[test]
    fn extract_scale_replicas_float_value_returns_400() {
        let body = json!({"spec": {"replicas": 3.5}});
        let err = extract_scale_replicas(&body).unwrap_err();
        assert!(
            matches!(err, AppError::BadRequest(_)),
            "float replicas should be rejected"
        );
    }

    #[test]
    fn extract_scale_replicas_null_value_returns_400() {
        let body = json!({"spec": {"replicas": null}});
        let err = extract_scale_replicas(&body).unwrap_err();
        assert!(
            matches!(err, AppError::BadRequest(_)),
            "null replicas should be rejected"
        );
    }

    #[test]
    fn extract_scale_replicas_valid_i32_passes() {
        let body = json!({"spec": {"replicas": 5}});
        assert_eq!(extract_scale_replicas(&body).unwrap(), 5);
    }

    #[test]
    fn extract_scale_replicas_negative_value_passes() {
        let body = json!({"spec": {"replicas": -1}});
        assert_eq!(extract_scale_replicas(&body).unwrap(), -1);
    }

    #[test]
    fn extract_scale_replicas_zero_passes() {
        let body = json!({"spec": {"replicas": 0}});
        assert_eq!(extract_scale_replicas(&body).unwrap(), 0);
    }

    #[test]
    fn extract_scale_resource_version_empty_is_unconditional() {
        let body = json!({"metadata": {"resourceVersion": ""}});
        assert_eq!(extract_scale_resource_version(&body).unwrap(), None);
    }

    #[test]
    fn extract_scale_resource_version_missing_is_unconditional() {
        let body = json!({});
        assert_eq!(extract_scale_resource_version(&body).unwrap(), None);
    }

    #[test]
    fn extract_scale_resource_version_string_parses() {
        let body = json!({"metadata": {"resourceVersion": "42"}});
        assert_eq!(extract_scale_resource_version(&body).unwrap(), Some(42));
    }

    #[test]
    fn extract_scale_resource_version_invalid_string_returns_400() {
        let body = json!({"metadata": {"resourceVersion": "not-a-number"}});
        let err = extract_scale_resource_version(&body).unwrap_err();
        assert!(
            matches!(err, AppError::BadRequest(_)),
            "invalid resourceVersion should be rejected"
        );
    }

    #[test]
    fn build_scale_response_produces_valid_scale_json() {
        let scale = build_scale_response("my-deploy", "default", 42, 5, 3, "app=nginx".to_string());
        assert_eq!(scale["apiVersion"], "autoscaling/v1");
        assert_eq!(scale["kind"], "Scale");
        assert_eq!(scale["metadata"]["name"], "my-deploy");
        assert_eq!(scale["metadata"]["namespace"], "default");
        assert_eq!(scale["metadata"]["resourceVersion"], "42");
        assert_eq!(scale["spec"]["replicas"], 5);
        assert_eq!(scale["status"]["replicas"], 3);
        assert_eq!(scale["status"]["selector"], "app=nginx");
    }

    #[test]
    fn build_scale_response_omits_empty_selector() {
        let scale = build_scale_response("my-deploy", "default", 1, 1, 0, String::new());
        assert!(
            scale["status"]["selector"].is_null(),
            "empty selector should be null (omitted by serde)"
        );
    }

    #[tokio::test]
    async fn patch_statefulset_scale_does_not_conflict_with_concurrent_status_updates() {
        use axum::body::{Body, to_bytes};
        use axum::http::{Request, StatusCode};
        use std::sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        };
        use tower::ServiceExt;

        let (app, db) = crate::api::test_support::build_test_router_with_db().await;
        db.create_resource(
            "apps/v1",
            "StatefulSet",
            Some("default"),
            "scale-race",
            json!({
                "apiVersion": "apps/v1",
                "kind": "StatefulSet",
                "metadata": {
                    "name": "scale-race",
                    "namespace": "default",
                    "uid": "scale-race-uid"
                },
                "spec": {
                    "replicas": 1,
                    "serviceName": "scale-race",
                    "selector": {"matchLabels": {"app": "scale-race"}},
                    "template": {
                        "metadata": {"labels": {"app": "scale-race"}},
                        "spec": {"containers": [{"name": "web", "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"}]}
                    }
                },
                "status": {"replicas": 1, "readyReplicas": 1}
            }),
        )
        .await
        .unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let status_writes = Arc::new(AtomicUsize::new(0));
        let churn_db = db.clone();
        let churn_stop = stop.clone();
        let churn_count = status_writes.clone();
        let churn = tokio::spawn(async move {
            let mut replicas = 0_i64;
            while !churn_stop.load(Ordering::SeqCst) {
                let _ = churn_db
                    .update_status_only_with_preconditions(
                        "apps/v1",
                        "StatefulSet",
                        Some("default"),
                        "scale-race",
                        json!({"replicas": replicas, "readyReplicas": replicas}),
                        ResourcePreconditions::uid("scale-race-uid"),
                    )
                    .await;
                replicas = (replicas + 1) % 3;
                churn_count.fetch_add(1, Ordering::SeqCst);
            }
        });

        while status_writes.load(Ordering::SeqCst) < 5 {
            tokio::task::yield_now().await;
        }

        let mut conflict_at = None;
        for replicas in 2..122 {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PATCH")
                        .uri("/apis/apps/v1/namespaces/default/statefulsets/scale-race/scale")
                        .header("content-type", "application/merge-patch+json")
                        .body(Body::from(
                            json!({"spec": {"replicas": replicas}}).to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();

            if response.status() == StatusCode::CONFLICT {
                conflict_at = Some(replicas);
                break;
            }

            assert_eq!(
                response.status(),
                StatusCode::OK,
                "unexpected scale PATCH status at replicas={replicas}"
            );
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let scale: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(scale["spec"]["replicas"], replicas);
        }

        stop.store(true, Ordering::SeqCst);
        churn.await.unwrap();

        assert!(
            conflict_at.is_none(),
            "scale PATCH must not return 409 while controller status updates race; first conflict at replicas={:?}",
            conflict_at
        );
    }

    #[tokio::test]
    async fn patch_replicaset_scale_does_not_conflict_with_concurrent_status_updates() {
        use axum::body::{Body, to_bytes};
        use axum::http::{Request, StatusCode};
        use std::sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        };
        use tower::ServiceExt;

        let (app, db) = crate::api::test_support::build_test_router_with_db().await;
        db.create_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "scale-race-rs",
            json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "metadata": {
                    "name": "scale-race-rs",
                    "namespace": "default",
                    "uid": "scale-race-rs-uid"
                },
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "scale-race-rs"}},
                    "template": {
                        "metadata": {"labels": {"app": "scale-race-rs"}},
                        "spec": {"containers": [{"name": "web", "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"}]}
                    }
                },
                "status": {"replicas": 1, "readyReplicas": 1}
            }),
        )
        .await
        .unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let status_writes = Arc::new(AtomicUsize::new(0));
        let churn_db = db.clone();
        let churn_stop = stop.clone();
        let churn_count = status_writes.clone();
        let churn = tokio::spawn(async move {
            let mut replicas = 0_i64;
            while !churn_stop.load(Ordering::SeqCst) {
                let _ = churn_db
                    .update_status_only_with_preconditions(
                        "apps/v1",
                        "ReplicaSet",
                        Some("default"),
                        "scale-race-rs",
                        json!({"replicas": replicas, "readyReplicas": replicas}),
                        ResourcePreconditions::uid("scale-race-rs-uid"),
                    )
                    .await;
                replicas = (replicas + 1) % 3;
                churn_count.fetch_add(1, Ordering::SeqCst);
            }
        });

        while status_writes.load(Ordering::SeqCst) < 5 {
            tokio::task::yield_now().await;
        }

        let mut conflict_at = None;
        for replicas in 2..122 {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PATCH")
                        .uri("/apis/apps/v1/namespaces/default/replicasets/scale-race-rs/scale")
                        .header("content-type", "application/merge-patch+json")
                        .body(Body::from(
                            json!({"spec": {"replicas": replicas}}).to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();

            if response.status() == StatusCode::CONFLICT {
                conflict_at = Some(replicas);
                break;
            }

            assert_eq!(
                response.status(),
                StatusCode::OK,
                "unexpected ReplicaSet scale PATCH status at replicas={replicas}"
            );
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let scale: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(scale["spec"]["replicas"], replicas);
        }

        stop.store(true, Ordering::SeqCst);
        churn.await.unwrap();

        assert!(
            conflict_at.is_none(),
            "ReplicaSet scale PATCH must not return 409 while controller status updates race; first conflict at replicas={:?}",
            conflict_at
        );
    }

    #[tokio::test]
    async fn update_replicaset_scale_with_empty_resource_version_is_unconditional() {
        use axum::body::{Body, to_bytes};
        use axum::http::{Request, StatusCode};
        use std::sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        };
        use tower::ServiceExt;

        let (app, db) = crate::api::test_support::build_test_router_with_db().await;
        db.create_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "scale-put-race-rs",
            json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "metadata": {
                    "name": "scale-put-race-rs",
                    "namespace": "default",
                    "uid": "scale-put-race-rs-uid"
                },
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "scale-put-race-rs"}},
                    "template": {
                        "metadata": {"labels": {"app": "scale-put-race-rs"}},
                        "spec": {"containers": [{"name": "web", "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"}]}
                    }
                },
                "status": {"replicas": 1, "readyReplicas": 1}
            }),
        )
        .await
        .unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let status_writes = Arc::new(AtomicUsize::new(0));
        let churn_db = db.clone();
        let churn_stop = stop.clone();
        let churn_count = status_writes.clone();
        let churn = tokio::spawn(async move {
            let mut replicas = 0_i64;
            while !churn_stop.load(Ordering::SeqCst) {
                let _ = churn_db
                    .update_status_only_with_preconditions(
                        "apps/v1",
                        "ReplicaSet",
                        Some("default"),
                        "scale-put-race-rs",
                        json!({"replicas": replicas, "readyReplicas": replicas}),
                        ResourcePreconditions::uid("scale-put-race-rs-uid"),
                    )
                    .await;
                replicas = (replicas + 1) % 3;
                churn_count.fetch_add(1, Ordering::SeqCst);
            }
        });

        while status_writes.load(Ordering::SeqCst) < 5 {
            tokio::task::yield_now().await;
        }

        let mut conflict_at = None;
        for replicas in 2..122 {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri("/apis/apps/v1/namespaces/default/replicasets/scale-put-race-rs/scale")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            json!({
                                "apiVersion": "autoscaling/v1",
                                "kind": "Scale",
                                "metadata": {
                                    "name": "scale-put-race-rs",
                                    "namespace": "default",
                                    "resourceVersion": ""
                                },
                                "spec": {"replicas": replicas}
                            })
                            .to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();

            if response.status() == StatusCode::CONFLICT {
                conflict_at = Some(replicas);
                break;
            }

            assert_eq!(
                response.status(),
                StatusCode::OK,
                "unexpected ReplicaSet scale PUT status at replicas={replicas}"
            );
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let scale: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(scale["spec"]["replicas"], replicas);
        }

        stop.store(true, Ordering::SeqCst);
        churn.await.unwrap();

        assert!(
            conflict_at.is_none(),
            "ReplicaSet scale PUT with empty resourceVersion must be unconditional; first conflict at replicas={:?}",
            conflict_at
        );
    }

    #[tokio::test]
    async fn update_replicaset_scale_with_stale_resource_version_returns_conflict() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let (app, db) = crate::api::test_support::build_test_router_with_db().await;
        db.create_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "scale-put-stale-rs",
            json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "metadata": {
                    "name": "scale-put-stale-rs",
                    "namespace": "default",
                    "uid": "scale-put-stale-rs-uid"
                },
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "scale-put-stale-rs"}},
                    "template": {
                        "metadata": {"labels": {"app": "scale-put-stale-rs"}},
                        "spec": {"containers": [{"name": "web", "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"}]}
                    }
                },
                "status": {"replicas": 1, "readyReplicas": 1}
            }),
        )
        .await
        .unwrap();

        let initial = db
            .get_resource(
                "apps/v1",
                "ReplicaSet",
                Some("default"),
                "scale-put-stale-rs",
            )
            .await
            .unwrap()
            .unwrap();
        db.update_status_only_with_preconditions(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "scale-put-stale-rs",
            json!({"replicas": 1, "readyReplicas": 1, "observedGeneration": 1}),
            ResourcePreconditions::uid("scale-put-stale-rs-uid"),
        )
        .await
        .unwrap();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/apis/apps/v1/namespaces/default/replicasets/scale-put-stale-rs/scale")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "apiVersion": "autoscaling/v1",
                            "kind": "Scale",
                            "metadata": {
                                "name": "scale-put-stale-rs",
                                "namespace": "default",
                                "resourceVersion": initial.resource_version.to_string()
                            },
                            "spec": {"replicas": 2}
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::CONFLICT,
            "stale non-empty scale resourceVersion must remain a CAS precondition"
        );
    }

    #[tokio::test]
    async fn patch_replicationcontroller_scale_does_not_conflict_with_concurrent_status_updates() {
        use axum::body::{Body, to_bytes};
        use axum::http::{Request, StatusCode};
        use std::sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        };
        use tower::ServiceExt;

        let (app, db) = crate::api::test_support::build_test_router_with_db().await;
        db.create_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "scale-race-rc",
            json!({
                "apiVersion": "v1",
                "kind": "ReplicationController",
                "metadata": {
                    "name": "scale-race-rc",
                    "namespace": "default",
                    "uid": "scale-race-rc-uid"
                },
                "spec": {
                    "replicas": 1,
                    "selector": {"app": "scale-race-rc"},
                    "template": {
                        "metadata": {"labels": {"app": "scale-race-rc"}},
                        "spec": {"containers": [{"name": "web", "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"}]}
                    }
                },
                "status": {"replicas": 1, "readyReplicas": 1}
            }),
        )
        .await
        .unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let status_writes = Arc::new(AtomicUsize::new(0));
        let churn_db = db.clone();
        let churn_stop = stop.clone();
        let churn_count = status_writes.clone();
        let churn = tokio::spawn(async move {
            let mut replicas = 0_i64;
            while !churn_stop.load(Ordering::SeqCst) {
                let _ = churn_db
                    .update_status_only_with_preconditions(
                        "v1",
                        "ReplicationController",
                        Some("default"),
                        "scale-race-rc",
                        json!({"replicas": replicas, "readyReplicas": replicas}),
                        ResourcePreconditions::uid("scale-race-rc-uid"),
                    )
                    .await;
                replicas = (replicas + 1) % 3;
                churn_count.fetch_add(1, Ordering::SeqCst);
            }
        });

        while status_writes.load(Ordering::SeqCst) < 5 {
            tokio::task::yield_now().await;
        }

        let mut conflict_at = None;
        for replicas in 2..122 {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PATCH")
                        .uri(
                            "/api/v1/namespaces/default/replicationcontrollers/scale-race-rc/scale",
                        )
                        .header("content-type", "application/merge-patch+json")
                        .body(Body::from(
                            json!({"spec": {"replicas": replicas}}).to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();

            if response.status() == StatusCode::CONFLICT {
                conflict_at = Some(replicas);
                break;
            }

            assert_eq!(
                response.status(),
                StatusCode::OK,
                "unexpected ReplicationController scale PATCH status at replicas={replicas}"
            );
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let scale: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(scale["spec"]["replicas"], replicas);
        }

        stop.store(true, Ordering::SeqCst);
        churn.await.unwrap();

        assert!(
            conflict_at.is_none(),
            "ReplicationController scale PATCH must not return 409 while controller status updates race; first conflict at replicas={:?}",
            conflict_at
        );
    }
}
