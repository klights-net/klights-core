use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::{
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode},
};
use klights_cluster_core::{Resource, ResourcePreconditions};
use klights_controllers::resource_quota::{
    ResourceQuotaRuntime, reconcile_resource_quotas_with_runtime,
};
use klights_reconcile_api::{ControllerStoreError, ControllerStoreResult};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::bootstrap::composition_tests::native_api::support::build_test_app_state;

const CONVERGENCE_BUDGET: Duration = Duration::from_secs(1);

fn map_store_error(error: anyhow::Error) -> ControllerStoreError {
    if klights_cluster_datastore::errors::is_conflict_error(&error) {
        ControllerStoreError::conflict(error.to_string())
    } else {
        ControllerStoreError::unavailable(error.to_string())
    }
}

#[derive(Clone, Copy)]
enum ConflictInjection {
    ResourceQuotaSpec,
    ResourceQuotaStatus,
}

struct RealResourceQuotaRuntime {
    db: klights_cluster_datastore::test_support::ResourceTestStore,
    injection: ConflictInjection,
    injected: AtomicBool,
}

impl RealResourceQuotaRuntime {
    fn new(
        db: klights_cluster_datastore::test_support::ResourceTestStore,
        injection: ConflictInjection,
    ) -> Self {
        Self {
            db,
            injection,
            injected: AtomicBool::new(false),
        }
    }

    async fn inject_overlap_once(&self, namespace: &str) -> ControllerStoreResult<()> {
        if self.injected.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        match self.injection {
            ConflictInjection::ResourceQuotaSpec => {
                let current = self
                    .db
                    .get_resource("v1", "ResourceQuota", Some(namespace), "test-rq")
                    .await
                    .map_err(map_store_error)?
                    .expect("test ResourceQuota should exist");
                let mut updated = (*current.data).clone();
                updated["spec"]["hard"] = json!({
                    "pods": "10",
                    "secrets": "5"
                });
                self.db
                    .update_resource(
                        "v1",
                        "ResourceQuota",
                        Some(namespace),
                        "test-rq",
                        updated,
                        current.resource_version,
                    )
                    .await
                    .map(|_| ())
                    .map_err(map_store_error)
            }
            ConflictInjection::ResourceQuotaStatus => {
                let current = self
                    .db
                    .get_resource("v1", "ResourceQuota", Some(namespace), "test-rq")
                    .await
                    .map_err(map_store_error)?
                    .expect("test ResourceQuota should exist");
                self.db
                    .update_status_only_with_preconditions(
                        "v1",
                        "ResourceQuota",
                        Some(namespace),
                        "test-rq",
                        json!({
                            "hard": {"pods": "4"},
                            "used": {"pods": "77"}
                        }),
                        ResourcePreconditions::from_resource(&current),
                    )
                    .await
                    .map(|_| ())
                    .map_err(map_store_error)
            }
        }
    }
}

#[async_trait]
impl ResourceQuotaRuntime for RealResourceQuotaRuntime {
    async fn list_quota_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: &str,
    ) -> ControllerStoreResult<Vec<Resource>> {
        self.db
            .list_resources(
                api_version,
                kind,
                Some(namespace),
                klights_cluster_store::ResourceListOptions::new(None, None, None, None),
            )
            .await
            .map(|listing| listing.items)
            .map_err(map_store_error)
    }

    async fn list_namespace_pods(&self, namespace: &str) -> ControllerStoreResult<Vec<Resource>> {
        self.inject_overlap_once(namespace).await?;
        self.db
            .list_resources(
                "v1",
                "Pod",
                Some(namespace),
                klights_cluster_store::ResourceListOptions::new(None, None, None, None),
            )
            .await
            .map(|listing| listing.items)
            .map_err(map_store_error)
    }

    async fn write_resource_quota_status(
        &self,
        resource: &Resource,
        status: &Value,
    ) -> ControllerStoreResult<()> {
        self.db
            .update_status_only_with_preconditions(
                "v1",
                "ResourceQuota",
                resource.namespace.as_deref(),
                &resource.name,
                status.clone(),
                ResourcePreconditions::from_resource(resource),
            )
            .await
            .map(|_| ())
            .map_err(map_store_error)
    }
}

async fn request_json(
    app: &axum::Router,
    method: Method,
    uri: &str,
    content_type: Option<&str>,
    body: Option<Value>,
    expected_status: StatusCode,
) -> Value {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(content_type) = content_type {
        builder = builder.header("content-type", content_type);
    }
    let request = builder
        .body(body.map_or_else(Body::empty, |body| Body::from(body.to_string())))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), expected_status, "{uri}");
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    }
}

async fn create_namespace(app: &axum::Router, namespace: &str) {
    request_json(
        app,
        Method::POST,
        "/api/v1/namespaces",
        Some("application/json"),
        Some(json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": namespace}
        })),
        StatusCode::CREATED,
    )
    .await;
}

fn rq_with_pods_hard(namespace: &str, name: &str, pods: u32) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {"hard": {"pods": pods.to_string()}},
        "status": {"hard": {}, "used": {}}
    })
}

fn pod(namespace: &str, name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    })
}

async fn read_rq_status(
    db: &klights_cluster_datastore::test_support::ResourceTestStore,
    namespace: &str,
    name: &str,
) -> Value {
    let resource = db
        .get_resource("v1", "ResourceQuota", Some(namespace), name)
        .await
        .unwrap()
        .expect("ResourceQuota present");
    resource
        .data
        .pointer("/status")
        .cloned()
        .unwrap_or(Value::Null)
}

fn assert_within(start: Instant, label: &str) {
    let elapsed = start.elapsed();
    assert!(
        elapsed < CONVERGENCE_BUDGET,
        "{label}: convergence took {elapsed:?}, exceeded {CONVERGENCE_BUDGET:?} budget"
    );
}

#[tokio::test]
async fn test_reconcile_resource_quota_rejects_stale_status_overlap() {
    let state = build_test_app_state().await;
    let db = state.resource_store();
    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "test-rq",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "test-rq", "namespace": "default"},
            "spec": {"hard": {"pods": "4"}},
            "status": {"hard": {"pods": "4"}, "used": {"pods": "0"}}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "test-pod",
        pod("default", "test-pod"),
    )
    .await
    .unwrap();

    let runtime = RealResourceQuotaRuntime::new(db.clone(), ConflictInjection::ResourceQuotaStatus);
    let error = reconcile_resource_quotas_with_runtime(&runtime, "default")
        .await
        .expect_err("stale ResourceQuota status overlap must be rejected");
    assert!(
        error
            .downcast_ref::<ControllerStoreError>()
            .is_some_and(ControllerStoreError::is_conflict),
        "expected status conflict, got {error:#}"
    );
    let status = read_rq_status(&db, "default", "test-rq").await;
    assert_eq!(
        status.pointer("/used/pods").and_then(Value::as_str),
        Some("77")
    );
}

#[tokio::test]
async fn test_reconcile_resource_quota_rejects_stale_spec_overlap() {
    let state = build_test_app_state().await;
    let db = state.resource_store();
    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "test-rq",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "test-rq", "namespace": "default"},
            "spec": {"hard": {"pods": "4"}},
            "status": {"hard": {"pods": "4"}, "used": {"pods": "0"}}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "test-pod",
        pod("default", "test-pod"),
    )
    .await
    .unwrap();

    let runtime = RealResourceQuotaRuntime::new(db.clone(), ConflictInjection::ResourceQuotaSpec);
    let error = reconcile_resource_quotas_with_runtime(&runtime, "default")
        .await
        .expect_err("stale ResourceQuota spec overlap must be rejected");
    assert!(
        error
            .downcast_ref::<ControllerStoreError>()
            .is_some_and(ControllerStoreError::is_conflict),
        "expected status conflict, got {error:#}"
    );
    let quota = db
        .get_resource("v1", "ResourceQuota", Some("default"), "test-rq")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        quota
            .data
            .pointer("/spec/hard/secrets")
            .and_then(Value::as_str),
        Some("5")
    );
    assert_eq!(
        quota
            .data
            .pointer("/status/used/secrets")
            .and_then(Value::as_str),
        None
    );
}

#[tokio::test]
async fn test_reconcile_resource_quota_writes_status_through_raft_status_subresource() {
    let state = build_test_app_state().await;
    let app = state.router();
    let db = state.resource_store();
    create_namespace(&app, "rq-raft-status").await;
    request_json(
        &app,
        Method::POST,
        "/api/v1/namespaces/rq-raft-status/resourcequotas",
        Some("application/json"),
        Some(json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "test-quota", "namespace": "rq-raft-status"},
            "spec": {"hard": {"resourcequotas": "1", "secrets": "1"}},
            "status": {
                "hard": {"resourcequotas": "1", "secrets": "1"},
                "used": {"resourcequotas": "0", "secrets": "0"}
            }
        })),
        StatusCode::CREATED,
    )
    .await;
    let status = read_rq_status(&db, "rq-raft-status", "test-quota").await;
    assert_eq!(
        status
            .pointer("/used/resourcequotas")
            .and_then(Value::as_str),
        Some("1"),
        "root-composed mutation and side-effect path must persist ResourceQuota status"
    );
}

#[tokio::test]
async fn rq_status_hard_synced_within_1s() {
    let state = build_test_app_state().await;
    let app = state.router();
    let db = state.resource_store();
    create_namespace(&app, "rq-hard-sync").await;
    let start = Instant::now();
    request_json(
        &app,
        Method::POST,
        "/api/v1/namespaces/rq-hard-sync/resourcequotas",
        Some("application/json"),
        Some(rq_with_pods_hard("rq-hard-sync", "rq", 5)),
        StatusCode::CREATED,
    )
    .await;
    let status = read_rq_status(&db, "rq-hard-sync", "rq").await;
    assert_within(start, "Status.Hard initial sync");
    assert_eq!(
        status.pointer("/hard/pods").and_then(Value::as_str),
        Some("5")
    );
}

#[tokio::test]
async fn rq_status_used_increments_on_pod_create_within_1s() {
    let state = build_test_app_state().await;
    let app = state.router();
    let db = state.resource_store();
    create_namespace(&app, "rq-pod-create").await;
    request_json(
        &app,
        Method::POST,
        "/api/v1/namespaces/rq-pod-create/resourcequotas",
        Some("application/json"),
        Some(rq_with_pods_hard("rq-pod-create", "rq", 5)),
        StatusCode::CREATED,
    )
    .await;
    let start = Instant::now();
    request_json(
        &app,
        Method::POST,
        "/api/v1/namespaces/rq-pod-create/pods",
        Some("application/json"),
        Some(pod("rq-pod-create", "p1")),
        StatusCode::CREATED,
    )
    .await;
    let status = read_rq_status(&db, "rq-pod-create", "rq").await;
    assert_within(start, "Status.Used after Pod create");
    assert_eq!(
        status.pointer("/used/pods").and_then(Value::as_str),
        Some("1")
    );
}

#[tokio::test]
async fn rq_status_used_decrements_on_pod_delete_within_1s() {
    let state = build_test_app_state().await;
    let app = state.router();
    let db = state.resource_store();
    create_namespace(&app, "rq-pod-delete").await;
    request_json(
        &app,
        Method::POST,
        "/api/v1/namespaces/rq-pod-delete/resourcequotas",
        Some("application/json"),
        Some(rq_with_pods_hard("rq-pod-delete", "rq", 5)),
        StatusCode::CREATED,
    )
    .await;
    request_json(
        &app,
        Method::POST,
        "/api/v1/namespaces/rq-pod-delete/pods",
        Some("application/json"),
        Some(pod("rq-pod-delete", "p1")),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(
        read_rq_status(&db, "rq-pod-delete", "rq")
            .await
            .pointer("/used/pods")
            .and_then(Value::as_str),
        Some("1")
    );
    let start = Instant::now();
    request_json(
        &app,
        Method::DELETE,
        "/api/v1/namespaces/rq-pod-delete/pods/p1",
        None,
        None,
        StatusCode::ACCEPTED,
    )
    .await;
    let status = read_rq_status(&db, "rq-pod-delete", "rq").await;
    assert_within(start, "Status.Used after Pod delete");
    assert_eq!(
        status.pointer("/used/pods").and_then(Value::as_str),
        Some("0")
    );
}

#[tokio::test]
async fn rq_status_hard_resyncs_after_status_patch_within_1s() {
    let state = build_test_app_state().await;
    let app = state.router();
    let db = state.resource_store();
    create_namespace(&app, "rq-status-patch").await;
    request_json(
        &app,
        Method::POST,
        "/api/v1/namespaces/rq-status-patch/resourcequotas",
        Some("application/json"),
        Some(rq_with_pods_hard("rq-status-patch", "rq", 5)),
        StatusCode::CREATED,
    )
    .await;
    let start = Instant::now();
    let patched = request_json(
        &app,
        Method::PATCH,
        "/api/v1/namespaces/rq-status-patch/resourcequotas/rq/status",
        Some("application/merge-patch+json"),
        Some(json!({
            "status": {"hard": {"pods": "99"}, "used": {"pods": "0"}}
        })),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        patched.pointer("/status/hard/pods").and_then(Value::as_str),
        Some("99"),
        "PATCH response must expose the client-written status before reconciliation"
    );
    let status = read_rq_status(&db, "rq-status-patch", "rq").await;
    assert_within(start, "Status.Hard resync after /status PATCH");
    assert_eq!(
        status.pointer("/hard/pods").and_then(Value::as_str),
        Some("5")
    );
}
