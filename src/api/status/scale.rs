use axum::{
    Json,
    body::Bytes,
    extract::{Path, State},
    http::HeaderMap,
};
use serde_json::Value;
use std::sync::Arc;

use crate::api::status::{
    DatastoreScaleMutationWriter, JsonScaleMutationResponder, ScaleMutationPipeline,
    ScaleMutationTarget, ScalePatchOperation, ScalePutOperation, ScaleSelectorStyle,
    build_scale_response,
};
use crate::api::{ApiState, AppError, LenientJson};
use klights_cluster_core::Resource;

#[cfg(test)]
use crate::api::status::{extract_scale_replicas, extract_scale_resource_version};
#[cfg(test)]
use klights_cluster_core::ResourcePreconditions;

// Scale endpoints are split from helpers to keep each file manageable.
// Authorization for scale subresources is enforced by the global
// `authorize_request` middleware chokepoint (see src/api/auth_middleware.rs).

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

pub(in crate::api) async fn get_replicaset_scale(
    State(state): State<Arc<ApiState>>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    let rs = crate::api::resource_query_ports::get_resource(
        state.resource_mutation().resource_query.as_ref(),
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

pub(in crate::api) async fn update_replicaset_scale(
    State(state): State<Arc<ApiState>>,
    Path((namespace, name)): Path<(String, String)>,
    LenientJson(body): LenientJson<Value>,
) -> Result<Json<Value>, AppError> {
    update_apps_v1_scale(state, "ReplicaSet", namespace, name, body).await
}

/// Generic scale GET handler for apps/v1 resources (Deployment, StatefulSet, etc.)
async fn get_apps_v1_scale(
    state: Arc<ApiState>,
    kind: &str,
    namespace: String,
    name: String,
) -> Result<Json<Value>, AppError> {
    let resource = crate::api::resource_query_ports::get_resource(
        state.resource_mutation().resource_query.as_ref(),
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
async fn update_apps_v1_scale(
    state: Arc<ApiState>,
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

pub(in crate::api) async fn get_deployment_scale(
    State(state): State<Arc<ApiState>>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    get_apps_v1_scale(state, "Deployment", namespace, name).await
}

pub(in crate::api) async fn update_deployment_scale(
    State(state): State<Arc<ApiState>>,
    Path((namespace, name)): Path<(String, String)>,
    LenientJson(body): LenientJson<Value>,
) -> Result<Json<Value>, AppError> {
    update_apps_v1_scale(state, "Deployment", namespace, name, body).await
}

pub(in crate::api) async fn get_statefulset_scale(
    State(state): State<Arc<ApiState>>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    get_apps_v1_scale(state, "StatefulSet", namespace, name).await
}

pub(in crate::api) async fn update_statefulset_scale(
    State(state): State<Arc<ApiState>>,
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
    state: Arc<ApiState>,
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
    let patch = crate::api::status::decode_patch_body(&body)?;
    let target = ScaleMutationTarget::namespaced("apps/v1", kind, namespace, name);
    let pipeline = ScaleMutationPipeline::new(
        DatastoreScaleMutationWriter::new(state),
        JsonScaleMutationResponder::new(ScaleSelectorStyle::MatchLabels),
    );
    pipeline
        .execute(&target, &ScalePatchOperation::new(patch, content_type))
        .await
}

pub(in crate::api) async fn patch_deployment_scale(
    State(state): State<Arc<ApiState>>,
    Path((namespace, name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    patch_apps_v1_scale(state, "Deployment", namespace, name, headers, body).await
}

pub(in crate::api) async fn patch_statefulset_scale(
    State(state): State<Arc<ApiState>>,
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
pub(in crate::api) async fn patch_replicaset_scale(
    State(state): State<Arc<ApiState>>,
    Path((namespace, name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    patch_apps_v1_scale(state, "ReplicaSet", namespace, name, headers, body).await
}

pub(in crate::api) async fn get_replicationcontroller_scale(
    State(state): State<Arc<ApiState>>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    let rc = crate::api::resource_query_ports::get_resource(
        state.resource_mutation().resource_query.as_ref(),
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

pub(in crate::api) async fn update_replicationcontroller_scale(
    State(state): State<Arc<ApiState>>,
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

pub(in crate::api) async fn patch_replicationcontroller_scale(
    State(state): State<Arc<ApiState>>,
    Path((namespace, name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let patch = crate::api::status::decode_patch_body(&body)?;
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

    #[tokio::test]
    async fn patch_statefulset_scale_preserves_current_status_after_status_update() {
        use axum::body::{Body, to_bytes};
        use axum::http::{Request, StatusCode};
        use std::sync::Arc;
        use tower::ServiceExt;

        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        let db_handle: crate::datastore::DatastoreHandle = Arc::new(db.clone());
        let passive_reads = crate::datastore::test_support::sqlite_passive_read_ports(&db);
        let state =
            crate::api::test_support::build_test_app_state_with_db(db_handle, passive_reads).await;
        let app = crate::api::build_router(state);
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

        db.update_status_only_with_preconditions(
            "apps/v1",
            "StatefulSet",
            Some("default"),
            "scale-race",
            json!({"replicas": 5, "readyReplicas": 4}),
            ResourcePreconditions::uid("scale-race-uid"),
        )
        .await
        .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/apis/apps/v1/namespaces/default/statefulsets/scale-race/scale")
                    .header("content-type", "application/merge-patch+json")
                    .body(Body::from(json!({"spec": {"replicas": 7}}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let scale: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(scale["spec"]["replicas"], 7);
        assert_eq!(scale["status"]["replicas"], 5);
    }

    #[tokio::test]
    async fn patch_replicaset_scale_preserves_current_status_after_status_update() {
        use axum::body::{Body, to_bytes};
        use axum::http::{Request, StatusCode};
        use std::sync::Arc;
        use tower::ServiceExt;

        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        let db_handle: crate::datastore::DatastoreHandle = Arc::new(db.clone());
        let passive_reads = crate::datastore::test_support::sqlite_passive_read_ports(&db);
        let state =
            crate::api::test_support::build_test_app_state_with_db(db_handle, passive_reads).await;
        let app = crate::api::build_router(state);
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

        db.update_status_only_with_preconditions(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "scale-race-rs",
            json!({"replicas": 5, "readyReplicas": 4}),
            ResourcePreconditions::uid("scale-race-rs-uid"),
        )
        .await
        .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/apis/apps/v1/namespaces/default/replicasets/scale-race-rs/scale")
                    .header("content-type", "application/merge-patch+json")
                    .body(Body::from(json!({"spec": {"replicas": 7}}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let scale: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(scale["spec"]["replicas"], 7);
        assert_eq!(scale["status"]["replicas"], 5);
    }

    #[tokio::test]
    async fn update_replicaset_scale_with_empty_resource_version_survives_status_update() {
        use axum::body::{Body, to_bytes};
        use axum::http::{Request, StatusCode};
        use std::sync::Arc;
        use tower::ServiceExt;

        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        let db_handle: crate::datastore::DatastoreHandle = Arc::new(db.clone());
        let passive_reads = crate::datastore::test_support::sqlite_passive_read_ports(&db);
        let state =
            crate::api::test_support::build_test_app_state_with_db(db_handle, passive_reads).await;
        let app = crate::api::build_router(state);
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

        db.update_status_only_with_preconditions(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "scale-put-race-rs",
            json!({"replicas": 5, "readyReplicas": 4}),
            ResourcePreconditions::uid("scale-put-race-rs-uid"),
        )
        .await
        .unwrap();

        let response = app
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
                            "spec": {"replicas": 7}
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let scale: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(scale["spec"]["replicas"], 7);
        assert_eq!(scale["status"]["replicas"], 5);
    }

    #[tokio::test]
    async fn update_replicaset_scale_with_stale_resource_version_returns_conflict() {
        use axum::body::{Body, to_bytes};
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

        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            status,
            StatusCode::CONFLICT,
            "stale non-empty scale resourceVersion must remain a CAS precondition: {}",
            String::from_utf8_lossy(&body),
        );
    }

    #[tokio::test]
    async fn patch_replicationcontroller_scale_preserves_current_status_after_status_update() {
        use axum::body::{Body, to_bytes};
        use axum::http::{Request, StatusCode};
        use std::sync::Arc;
        use tower::ServiceExt;

        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        let db_handle: crate::datastore::DatastoreHandle = Arc::new(db.clone());
        let passive_reads = crate::datastore::test_support::sqlite_passive_read_ports(&db);
        let state =
            crate::api::test_support::build_test_app_state_with_db(db_handle, passive_reads).await;
        let app = crate::api::build_router(state);
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

        db.update_status_only_with_preconditions(
            "v1",
            "ReplicationController",
            Some("default"),
            "scale-race-rc",
            json!({"replicas": 5, "readyReplicas": 4}),
            ResourcePreconditions::uid("scale-race-rc-uid"),
        )
        .await
        .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/api/v1/namespaces/default/replicationcontrollers/scale-race-rc/scale")
                    .header("content-type", "application/merge-patch+json")
                    .body(Body::from(json!({"spec": {"replicas": 7}}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let scale: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(scale["spec"]["replicas"], 7);
        assert_eq!(scale["status"]["replicas"], 5);
    }
}
