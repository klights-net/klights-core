use axum::{
    Json,
    body::Bytes,
    extract::{Path, State},
    http::HeaderMap,
};
use serde_json::Value;
use std::sync::Arc;

use super::{
    DatastoreScaleMutationWriter, JsonScaleMutationResponder, ScaleMutationPipeline,
    ScaleMutationTarget, ScalePatchOperation, ScalePutOperation, ScaleSelectorStyle,
    build_scale_response,
};
use crate::{AppError, LenientJson, generic_command::GenericCommandState, generic_read};
use klights_cluster_core::Resource;

#[cfg(test)]
use super::{extract_scale_replicas, extract_scale_resource_version};

// Scale endpoints are split from helpers to keep each file manageable.
// Authorization for scale subresources is enforced by the global
// native `authorize_request` middleware chokepoint (see `crate::auth_http`).

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

pub async fn get_replicaset_scale<S: GenericCommandState + 'static>(
    State(state): State<Arc<S>>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    let rs = generic_read::get_resource(
        state.command_store().resource_query(),
        "apps/v1",
        "ReplicaSet",
        Some(&namespace),
        &name,
    )
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

pub async fn update_replicaset_scale<S: GenericCommandState + 'static>(
    State(state): State<Arc<S>>,
    Path((namespace, name)): Path<(String, String)>,
    LenientJson(body): LenientJson<Value>,
) -> Result<Json<Value>, AppError> {
    update_apps_v1_scale(state, "ReplicaSet", namespace, name, body).await
}

/// Generic scale GET handler for apps/v1 resources (Deployment, StatefulSet, etc.)
async fn get_apps_v1_scale<S: GenericCommandState + 'static>(
    state: Arc<S>,
    kind: &str,
    namespace: String,
    name: String,
) -> Result<Json<Value>, AppError> {
    let resource = generic_read::get_resource(
        state.command_store().resource_query(),
        "apps/v1",
        kind,
        Some(&namespace),
        &name,
    )
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
async fn update_apps_v1_scale<S: GenericCommandState + 'static>(
    state: Arc<S>,
    kind: &str,
    namespace: String,
    name: String,
    body: Value,
) -> Result<Json<Value>, AppError> {
    let target = ScaleMutationTarget::namespaced("apps/v1", kind, namespace, name);
    let pipeline = ScaleMutationPipeline::new(
        DatastoreScaleMutationWriter::new(state),
        JsonScaleMutationResponder::new(ScaleSelectorStyle::MatchLabels),
    );
    pipeline
        .execute(&target, &ScalePutOperation::new(body))
        .await
}

pub async fn get_deployment_scale<S: GenericCommandState + 'static>(
    State(state): State<Arc<S>>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    get_apps_v1_scale(state, "Deployment", namespace, name).await
}

pub async fn update_deployment_scale<S: GenericCommandState + 'static>(
    State(state): State<Arc<S>>,
    Path((namespace, name)): Path<(String, String)>,
    LenientJson(body): LenientJson<Value>,
) -> Result<Json<Value>, AppError> {
    update_apps_v1_scale(state, "Deployment", namespace, name, body).await
}

pub async fn get_statefulset_scale<S: GenericCommandState + 'static>(
    State(state): State<Arc<S>>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    get_apps_v1_scale(state, "StatefulSet", namespace, name).await
}

pub async fn update_statefulset_scale<S: GenericCommandState + 'static>(
    State(state): State<Arc<S>>,
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
async fn patch_apps_v1_scale<S: GenericCommandState + 'static>(
    state: Arc<S>,
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
    let patch = super::decode_patch_body(&body)?;
    let target = ScaleMutationTarget::namespaced("apps/v1", kind, namespace, name);
    let pipeline = ScaleMutationPipeline::new(
        DatastoreScaleMutationWriter::new(state),
        JsonScaleMutationResponder::new(ScaleSelectorStyle::MatchLabels),
    );
    pipeline
        .execute(&target, &ScalePatchOperation::new(patch, content_type))
        .await
}

pub async fn patch_deployment_scale<S: GenericCommandState + 'static>(
    State(state): State<Arc<S>>,
    Path((namespace, name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    patch_apps_v1_scale(state, "Deployment", namespace, name, headers, body).await
}

pub async fn patch_statefulset_scale<S: GenericCommandState + 'static>(
    State(state): State<Arc<S>>,
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
pub async fn patch_replicaset_scale<S: GenericCommandState + 'static>(
    State(state): State<Arc<S>>,
    Path((namespace, name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    patch_apps_v1_scale(state, "ReplicaSet", namespace, name, headers, body).await
}

pub async fn get_replicationcontroller_scale<S: GenericCommandState + 'static>(
    State(state): State<Arc<S>>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    let rc = generic_read::get_resource(
        state.command_store().resource_query(),
        "v1",
        "ReplicationController",
        Some(&namespace),
        &name,
    )
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

pub async fn update_replicationcontroller_scale<S: GenericCommandState + 'static>(
    State(state): State<Arc<S>>,
    Path((namespace, name)): Path<(String, String)>,
    LenientJson(body): LenientJson<Value>,
) -> Result<Json<Value>, AppError> {
    let target = ScaleMutationTarget::namespaced("v1", "ReplicationController", namespace, name);
    let pipeline = ScaleMutationPipeline::new(
        DatastoreScaleMutationWriter::new(state),
        JsonScaleMutationResponder::new(ScaleSelectorStyle::FlatSelector),
    );
    pipeline
        .execute(&target, &ScalePutOperation::new(body))
        .await
}

pub async fn patch_replicationcontroller_scale<S: GenericCommandState + 'static>(
    State(state): State<Arc<S>>,
    Path((namespace, name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let patch = super::decode_patch_body(&body)?;
    let target = ScaleMutationTarget::namespaced("v1", "ReplicationController", namespace, name);
    let pipeline = ScaleMutationPipeline::new(
        DatastoreScaleMutationWriter::new(state),
        JsonScaleMutationResponder::new(ScaleSelectorStyle::FlatSelector),
    );
    pipeline
        .execute(&target, &ScalePatchOperation::new(patch, content_type))
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
}
