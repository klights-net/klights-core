//! Base-owned integration coverage for controller policy crossing root adapters.
//!
//! These cases intentionally use the already assembled native API harness,
//! real datastore, controller dispatcher, and focused scheduler ports. They do
//! not expose root-private adapters or recreate controller policy in test code.

use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode},
};
use klights_cluster_core::{PatchKind, Resource, ResourcePatchRequest, ResourcePreconditions};
use klights_controllers::common::ControllerStatusStore;
use klights_controllers::cronjob::CronJobStore;
use klights_pod_api::{
    PodActorFinalizeRequest, PodControlPlaneEventRequest, PodControlPlaneEventSink,
    PodDeleteMarkOutcome, PodDeleteMarkRequest, PodDeleteOrchestration, PodGetRequest,
    PodListRequest, PodListResult, PodMarkedRetryRequest, PodOwnerListRequest, PodPersistence,
    PodPersistenceCreateRequest, PodPersistenceReplaceRequest, PodQuery, PodRepositoryError,
    PodScheduling as _, PodStatusPersistence, PodStatusWriteRequest,
};
use klights_reconcile_api::{
    ControllerStoreError, ControllerStoreResult, ResourceMutationEffectsFuture,
    ResourceMutationEffectsPort, ResourceMutationEffectsRequest,
};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::bootstrap::native_api_composition::support::{
    build_test_app_state, resource_query_for_test_datastore,
};

fn controller_store_error(error: anyhow::Error) -> ControllerStoreError {
    if klights_cluster_datastore::errors::is_conflict_error(&error) {
        ControllerStoreError::conflict(error.to_string())
    } else {
        ControllerStoreError::unavailable(error.to_string())
    }
}

fn pod_store_error(error: anyhow::Error) -> PodRepositoryError {
    if klights_cluster_datastore::errors::is_conflict_error(&error) {
        PodRepositoryError::conflict(error.to_string())
    } else {
        PodRepositoryError::unavailable(error.to_string())
    }
}

async fn send_json(
    app: &axum::Router,
    method: Method,
    uri: &str,
    content_type: Option<&str>,
    body: Option<Value>,
) -> anyhow::Result<(StatusCode, Value)> {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(content_type) = content_type {
        builder = builder.header("content-type", content_type);
    }
    let response = app
        .clone()
        .oneshot(
            builder.body(body.map_or_else(Body::empty, |value| Body::from(value.to_string())))?,
        )
        .await?;
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await?;
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)?
    };
    Ok((status, body))
}

async fn request_json(
    app: &axum::Router,
    method: Method,
    uri: &str,
    content_type: Option<&str>,
    body: Option<Value>,
    expected_status: StatusCode,
) -> Value {
    let (status, response) = send_json(app, method, uri, content_type, body)
        .await
        .expect("native API request");
    assert_eq!(status, expected_status, "{uri}: {response}");
    response
}

fn with_resource_version(resource: &Resource) -> Value {
    let mut body = Arc::unwrap_or_clone(resource.data.clone());
    body["metadata"]["resourceVersion"] = json!(resource.resource_version.to_string());
    body
}

async fn seed_namespace(
    db: &klights_cluster_datastore::test_support::ResourceTestStore,
    namespace: &str,
) {
    db.create_resource(
        "v1",
        "Namespace",
        None,
        namespace,
        json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": namespace, "uid": format!("{namespace}-uid")}
        }),
    )
    .await
    .unwrap();
}

struct ApiCronJobStore {
    db: klights_cluster_datastore::test_support::ResourceTestStore,
    app: axum::Router,
}

#[async_trait]
impl ControllerStatusStore for ApiCronJobStore {
    async fn get_status_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.db
            .get_resource(api_version, kind, namespace, name)
            .await
            .map_err(controller_store_error)
    }

    async fn update_status(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        preconditions: ResourcePreconditions,
    ) -> ControllerStoreResult<Resource> {
        if api_version != "batch/v1" || kind != "CronJob" {
            return Err(ControllerStoreError::unavailable(
                "CronJob integration store received a non-CronJob status write",
            ));
        }
        let namespace = namespace.unwrap_or("default");
        let body = json!({
            "apiVersion": api_version,
            "kind": kind,
            "metadata": {
                "name": name,
                "namespace": namespace,
                "uid": preconditions.uid,
                "resourceVersion": preconditions.resource_version.map(|rv| rv.to_string()),
            },
            "status": status,
        });
        let uri = format!("/apis/batch/v1/namespaces/{namespace}/cronjobs/{name}/status");
        let (http_status, response) = send_json(
            &self.app,
            Method::PUT,
            &uri,
            Some("application/json"),
            Some(body),
        )
        .await
        .map_err(|error| ControllerStoreError::unavailable(error.to_string()))?;
        if http_status != StatusCode::OK {
            return Err(if http_status == StatusCode::CONFLICT {
                ControllerStoreError::conflict(response.to_string())
            } else {
                ControllerStoreError::unavailable(format!(
                    "CronJob status API returned {http_status}: {response}"
                ))
            });
        }
        self.db
            .get_resource(api_version, kind, Some(namespace), name)
            .await
            .map_err(controller_store_error)?
            .ok_or_else(|| ControllerStoreError::unavailable("updated CronJob disappeared"))
    }

    fn log_noop_status_write(
        &self,
        _operation: &'static str,
        _resource: &Resource,
        _reason: &'static str,
    ) {
    }
}

#[async_trait]
impl CronJobStore for ApiCronJobStore {
    async fn get_cronjob(
        &self,
        namespace: &str,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.db
            .get_resource("batch/v1", "CronJob", Some(namespace), name)
            .await
            .map_err(controller_store_error)
    }

    async fn get_job(
        &self,
        namespace: &str,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.db
            .get_resource("batch/v1", "Job", Some(namespace), name)
            .await
            .map_err(controller_store_error)
    }

    async fn create_job(
        &self,
        namespace: &str,
        _name: &str,
        value: Value,
    ) -> ControllerStoreResult<Resource> {
        let uri = format!("/apis/batch/v1/namespaces/{namespace}/jobs");
        let (status, response) = send_json(
            &self.app,
            Method::POST,
            &uri,
            Some("application/json"),
            Some(value),
        )
        .await
        .map_err(|error| ControllerStoreError::unavailable(error.to_string()))?;
        if status != StatusCode::CREATED {
            return Err(ControllerStoreError::unavailable(format!(
                "CronJob Job create returned {status}: {response}"
            )));
        }
        Resource::try_from_data(Arc::new(response))
            .map_err(|error| ControllerStoreError::unavailable(error.to_string()))
    }

    async fn list_jobs(&self, namespace: &str) -> ControllerStoreResult<Vec<Resource>> {
        self.db
            .list_resources(
                "batch/v1",
                "Job",
                Some(namespace),
                klights_cluster_store::ResourceListOptions::all(),
            )
            .await
            .map(|listing| listing.items)
            .map_err(controller_store_error)
    }

    async fn delete_job(
        &self,
        namespace: &str,
        name: &str,
        uid: String,
        resource_version: i64,
    ) -> ControllerStoreResult<()> {
        self.db
            .delete_resource_with_preconditions(
                "batch/v1",
                "Job",
                Some(namespace),
                name,
                ResourcePreconditions::uid_and_resource_version(uid, resource_version),
            )
            .await
            .map_err(controller_store_error)
    }
}

#[derive(Clone)]
struct DatastorePodPorts {
    db: klights_cluster_datastore::test_support::ResourceTestStore,
}

impl PodQuery for DatastorePodPorts {
    fn get_pod(
        &self,
        request: PodGetRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Option<Resource>> {
        Box::pin(async move {
            let resource = self
                .db
                .get_resource("v1", "Pod", Some(request.namespace()), request.name())
                .await
                .map_err(pod_store_error)?;
            Ok(resource.filter(|resource| {
                request
                    .uid()
                    .is_none_or(|expected_uid| resource.uid == expected_uid)
            }))
        })
    }

    fn list_pods(
        &self,
        request: PodListRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, PodListResult> {
        Box::pin(async move {
            let listing = self
                .db
                .list_resources(
                    "v1",
                    "Pod",
                    request.namespace(),
                    klights_cluster_store::ResourceListOptions::new(
                        request.label_selector(),
                        request.field_selector(),
                        request.limit(),
                        request.continue_token(),
                    ),
                )
                .await
                .map_err(pod_store_error)?;
            PodListResult::try_new(
                listing.items,
                listing.resource_version,
                listing.continue_token,
                listing.remaining_item_count,
            )
        })
    }

    fn list_pods_by_owner_uid(
        &self,
        request: PodOwnerListRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Vec<Resource>> {
        Box::pin(async move {
            let listing = self
                .db
                .list_resources(
                    "v1",
                    "Pod",
                    Some(request.namespace()),
                    klights_cluster_store::ResourceListOptions::all(),
                )
                .await
                .map_err(pod_store_error)?;
            Ok(listing
                .items
                .into_iter()
                .filter(|pod| {
                    pod.data
                        .pointer("/metadata/ownerReferences")
                        .and_then(Value::as_array)
                        .is_some_and(|owners| {
                            owners.iter().any(|owner| {
                                owner.get("uid").and_then(Value::as_str)
                                    == Some(request.owner_uid())
                            })
                        })
                })
                .collect())
        })
    }
}

impl PodPersistence for DatastorePodPorts {
    fn create_pod(
        &self,
        request: PodPersistenceCreateRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        Box::pin(async move {
            self.db
                .create_resource(
                    "v1",
                    "Pod",
                    Some(&request.namespace),
                    &request.name,
                    request.body,
                )
                .await
                .map_err(pod_store_error)
        })
    }

    fn replace_pod(
        &self,
        request: PodPersistenceReplaceRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        self.replace_pod_including_status(request)
    }

    fn replace_pod_including_status(
        &self,
        request: PodPersistenceReplaceRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        Box::pin(async move {
            self.db
                .update_resource(
                    "v1",
                    "Pod",
                    Some(&request.namespace),
                    &request.name,
                    request.body,
                    request.expected_resource_version,
                )
                .await
                .map_err(pod_store_error)
        })
    }

    fn patch_pod_metadata(
        &self,
        request: klights_pod_api::PodMetadataPatchRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        Box::pin(async move {
            self.db
                .patch_resource_latest_with_preconditions(
                    "v1",
                    "Pod",
                    Some(&request.namespace),
                    &request.name,
                    ResourcePatchRequest::new(
                        PatchKind::Merge,
                        request.patch,
                        ResourcePreconditions::uid_and_resource_version(
                            request.expected_uid,
                            request.expected_resource_version,
                        ),
                    )
                    .with_strict_resource_version(),
                )
                .await
                .map_err(pod_store_error)?
                .ok_or_else(|| PodRepositoryError::not_found(&request.namespace, &request.name))
        })
    }
}

impl PodStatusPersistence for DatastorePodPorts {
    fn write_pod_status(
        &self,
        request: PodStatusWriteRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        Box::pin(async move {
            self.db
                .update_status_only(
                    "v1",
                    "Pod",
                    Some(&request.namespace),
                    &request.name,
                    request.status,
                    request.expected_resource_version,
                )
                .await
                .map_err(pod_store_error)
        })
    }
}

impl PodDeleteOrchestration for DatastorePodPorts {
    fn preview_delete(
        &self,
        resource: &Resource,
        _requested_grace_period_seconds: Option<i64>,
    ) -> Result<Value, PodRepositoryError> {
        Ok(Arc::unwrap_or_clone(resource.data.clone()))
    }

    fn mark_and_queue_delete(
        &self,
        request: PodDeleteMarkRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, PodDeleteMarkOutcome> {
        Box::pin(async move {
            let previous = request.initial_resource;
            let uid = previous.uid.clone();
            let mut body = Arc::unwrap_or_clone(previous.data.clone());
            body["metadata"]["deletionTimestamp"] = json!(
                klights_cluster_core::k8s_time::format_legacy_timestamp(chrono::Utc::now())
            );
            let updated = self
                .db
                .update_resource_with_preconditions(
                    "v1",
                    "Pod",
                    Some(&request.namespace),
                    &request.name,
                    body,
                    request.preconditions,
                )
                .await
                .map_err(pod_store_error)?;
            Ok(PodDeleteMarkOutcome {
                updated,
                previous,
                uid,
                changed: true,
            })
        })
    }

    fn enqueue_actor_finalize_if_ready(
        &self,
        _request: PodActorFinalizeRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn enqueue_marked_retry(
        &self,
        _request: PodMarkedRetryRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

impl PodControlPlaneEventSink for DatastorePodPorts {
    fn emit_pod_event(
        &self,
        _request: PodControlPlaneEventRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

struct NoopMutationEffects;

impl ResourceMutationEffectsPort for NoopMutationEffects {
    fn dispatch_resource_mutation_effects<'a>(
        &'a self,
        _request: ResourceMutationEffectsRequest<'a>,
    ) -> ResourceMutationEffectsFuture<'a> {
        Box::pin(async {})
    }
}

fn scheduler_for_datastore(
    db: klights_cluster_datastore::test_support::ResourceTestStore,
) -> Arc<klights_controllers::scheduler::SchedulerService> {
    let pod_ports = Arc::new(DatastorePodPorts { db: db.clone() });
    klights_controllers::scheduler::SchedulerService::new(
        klights_controllers::scheduler::SchedulerServiceDependencies {
            pod_query: pod_ports.clone(),
            persistence: pod_ports.clone(),
            status_persistence: pod_ports.clone(),
            deletion: pod_ports.clone(),
            event_sink: pod_ports,
            placement: Arc::new(klights_controllers::scheduler::SchedulerPlacement::new()),
            resource_query: resource_query_for_test_datastore(db),
            supervisor: Arc::new(klights_supervisor::TaskSupervisor::new(
                klights_supervisor::TaskCategoryConfig::default(),
            )),
            mutation_effects: Arc::new(NoopMutationEffects),
            wall_clock: Arc::new(klights_supervisor::SystemWallClock),
        },
    )
}

#[tokio::test]
async fn test_cronjob_reconcile_persists_last_schedule_time_through_raft_status_path() {
    let state = build_test_app_state().await;
    let db = state.resource_store();
    let app = state.router();
    seed_namespace(&db, "default").await;
    let created_body = request_json(
        &app,
        Method::POST,
        "/apis/batch/v1/namespaces/default/cronjobs",
        Some("application/json"),
        Some(json!({
            "apiVersion": "batch/v1",
            "kind": "CronJob",
            "metadata": {"name": "test-cj-raft-status", "namespace": "default"},
            "spec": {
                "schedule": "* * * * *",
                "concurrencyPolicy": "Allow",
                "jobTemplate": {"spec": {"template": {"spec": {
                    "containers": [{"name": "c", "image": "nginx"}],
                    "restartPolicy": "Never"
                }}}}
            }
        })),
        StatusCode::CREATED,
    )
    .await;
    let created = db
        .get_resource(
            "batch/v1",
            "CronJob",
            Some("default"),
            "test-cj-raft-status",
        )
        .await
        .unwrap()
        .unwrap();
    let store = ApiCronJobStore {
        db: db.clone(),
        app,
    };
    klights_controllers::cronjob::reconcile_cronjob_one_at(
        &store,
        None,
        &created_body,
        created.resource_version,
        chrono::Utc::now() + chrono::Duration::minutes(2),
    )
    .await
    .unwrap();

    let updated = db
        .get_resource(
            "batch/v1",
            "CronJob",
            Some("default"),
            "test-cj-raft-status",
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        updated
            .data
            .pointer("/status/lastScheduleTime")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty()),
        "Raft-routed CronJob reconcile must persist status.lastScheduleTime: {:?}",
        updated.data
    );
}

#[tokio::test]
async fn test_replicaset_child_pods_are_scheduled_by_pod_create_pipeline() {
    let state = build_test_app_state().await;
    let db = state.resource_store();
    let app = state.router();
    seed_namespace(&db, "test-ns").await;
    db.create_resource(
        "v1",
        "Node",
        None,
        "test-node",
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "test-node", "uid": "test-node-uid"},
            "spec": {},
            "status": {
                "conditions": [{"type": "Ready", "status": "True"}],
                "allocatable": {"cpu": "8", "memory": "8Gi", "pods": "110", "example.com/fakecpu": "0"}
            }
        }),
    )
    .await
    .unwrap();
    request_json(
        &app,
        Method::POST,
        "/apis/apps/v1/namespaces/test-ns/replicasets",
        Some("application/json"),
        Some(json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {"name": "test-rs", "namespace": "test-ns"},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": "test"}},
                "template": {
                    "metadata": {"labels": {"app": "test"}},
                    "spec": {"containers": [{
                        "name": "nginx",
                        "image": "nginx",
                        "resources": {"requests": {"example.com/fakecpu": "1"}}
                    }]}
                }
            }
        })),
        StatusCode::CREATED,
    )
    .await;
    state
        .controller_runtime_fixture()
        .drain_ready()
        .await
        .unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            klights_cluster_store::ResourceListOptions::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods.items.len(), 1);
    assert!(pods.items[0].data.pointer("/spec/nodeName").is_none());
    assert_eq!(
        pods.items[0]
            .data
            .pointer("/status/conditions")
            .and_then(Value::as_array)
            .and_then(|conditions| conditions.iter().find(|condition| {
                condition.get("type").and_then(Value::as_str) == Some("PodScheduled")
            }))
            .and_then(|condition| condition.get("status"))
            .and_then(Value::as_str),
        Some("False")
    );
}

#[tokio::test]
async fn test_replicaset_created_pod_gets_api_pipeline_defaults() {
    let state = build_test_app_state().await;
    let db = state.resource_store();
    let app = state.router();
    seed_namespace(&db, "test-ns").await;
    request_json(
        &app,
        Method::POST,
        "/apis/apps/v1/namespaces/test-ns/replicasets",
        Some("application/json"),
        Some(json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {"name": "defaults-rs", "namespace": "test-ns"},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": "demo"}},
                "template": {
                    "metadata": {"labels": {"app": "demo"}},
                    "spec": {"containers": [{
                        "name": "app",
                        "image": "busybox",
                        "terminationMessagePath": "",
                        "terminationMessagePolicy": "",
                        "livenessProbe": {"httpGet": {"port": 8080, "path": "", "scheme": ""}}
                    }]}
                }
            }
        })),
        StatusCode::CREATED,
    )
    .await;
    state
        .controller_runtime_fixture()
        .drain_ready()
        .await
        .unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            klights_cluster_store::ResourceListOptions::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods.items.len(), 1);
    let pod = &pods.items[0].data;
    assert_eq!(
        pod.pointer("/status/phase").and_then(Value::as_str),
        Some("Pending")
    );
    assert_eq!(
        pod.pointer("/status/qosClass").and_then(Value::as_str),
        Some("BestEffort")
    );
    assert!(
        pod.pointer("/status/conditions")
            .and_then(Value::as_array)
            .is_some_and(|conditions| !conditions.is_empty())
    );
    assert_eq!(
        pod.pointer("/spec/containers/0/terminationMessagePath")
            .and_then(Value::as_str),
        Some("/dev/termination-log")
    );
    assert_eq!(
        pod.pointer("/spec/containers/0/terminationMessagePolicy")
            .and_then(Value::as_str),
        Some("File")
    );
    assert_eq!(
        pod.pointer("/spec/containers/0/livenessProbe/httpGet/path")
            .and_then(Value::as_str),
        Some("/")
    );
    assert_eq!(
        pod.pointer("/spec/containers/0/livenessProbe/httpGet/scheme")
            .and_then(Value::as_str),
        Some("HTTP")
    );
}

#[tokio::test]
async fn test_replicaset_child_pods_participate_in_priority_preemption() {
    let state = build_test_app_state().await;
    let db = state.resource_store();
    let app = state.router();
    seed_namespace(&db, "test-ns").await;
    db.create_resource(
        "v1",
        "Node",
        None,
        "test-node",
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "test-node", "uid": "test-node-uid"},
            "spec": {"unschedulable": false},
            "status": {
                "conditions": [{"type": "Ready", "status": "True"}],
                "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110", "example.com/fakecpu": "1k"}
            }
        }),
    )
    .await
    .unwrap();
    for (name, value) in [("p1", 1), ("p2", 2), ("p3", 3), ("p4", 4)] {
        db.create_resource(
            "scheduling.k8s.io/v1",
            "PriorityClass",
            None,
            name,
            json!({
                "apiVersion": "scheduling.k8s.io/v1",
                "kind": "PriorityClass",
                "metadata": {"name": name, "uid": format!("{name}-uid")},
                "value": value
            }),
        )
        .await
        .unwrap();
    }

    for (rs_name, request, priority_class) in [
        ("rs-one", "200", "p1"),
        ("rs-two", "300", "p2"),
        ("rs-three", "450", "p3"),
    ] {
        request_json(
            &app,
            Method::POST,
            "/apis/apps/v1/namespaces/test-ns/replicasets",
            Some("application/json"),
            Some(json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "metadata": {"name": rs_name, "namespace": "test-ns"},
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": rs_name}},
                    "template": {
                        "metadata": {"labels": {"app": rs_name}},
                        "spec": {
                            "priorityClassName": priority_class,
                            "containers": [{
                                "name": "c",
                                "image": "registry.k8s.io/pause:3.10",
                                "resources": {"requests": {"example.com/fakecpu": request}}
                            }]
                        }
                    }
                }
            })),
            StatusCode::CREATED,
        )
        .await;
        state
            .controller_runtime_fixture()
            .drain_ready()
            .await
            .unwrap();
    }

    let scheduler = scheduler_for_datastore(db.clone());
    scheduler.schedule_all_unbound_pods().await.unwrap();
    request_json(
        &app,
        Method::POST,
        "/api/v1/namespaces/test-ns/pods",
        Some("application/json"),
        Some(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "pod4", "namespace": "test-ns"},
            "spec": {
                "priorityClassName": "p4",
                "containers": [{
                    "name": "c",
                    "image": "registry.k8s.io/pause:3.10",
                    "resources": {"requests": {"example.com/fakecpu": "500"}}
                }]
            }
        })),
        StatusCode::CREATED,
    )
    .await;
    scheduler.schedule_all_unbound_pods().await.unwrap();

    let scheduled = db
        .get_resource("v1", "Pod", Some("test-ns"), "pod4")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        scheduled
            .data
            .pointer("/spec/nodeName")
            .and_then(Value::as_str),
        Some("test-node")
    );
    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            klights_cluster_store::ResourceListOptions::all(),
        )
        .await
        .unwrap();
    let active_rs_pods: Vec<_> = pods
        .items
        .iter()
        .filter(|pod| {
            pod.name != "pod4" && pod.data.pointer("/metadata/deletionTimestamp").is_none()
        })
        .collect();
    assert_eq!(
        active_rs_pods.len(),
        1,
        "high-priority Pod must preempt enough lower-priority ReplicaSet children: {:?}",
        pods.items
    );
    assert_eq!(
        active_rs_pods[0]
            .data
            .pointer("/spec/priorityClassName")
            .and_then(Value::as_str),
        Some("p3")
    );
}

#[tokio::test]
async fn test_rollover_adoption_redrives_zero_replica_old_rs_pod_delete() {
    let state = build_test_app_state().await;
    let db = state.resource_store();
    let app = state.router();
    seed_namespace(&db, "default").await;
    let old_rs_uid = "old-rs-uid-adopted-rollover";
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "test-rolling-update-controller",
        json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": "test-rolling-update-controller",
                "namespace": "default",
                "uid": old_rs_uid,
                "labels": {"name": "sample-pod", "pod": "httpd"},
                "annotations": {"deployment.kubernetes.io/revision": "1"}
            },
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"name": "sample-pod", "pod": "httpd"}},
                "template": {
                    "metadata": {"labels": {"name": "sample-pod", "pod": "httpd"}},
                    "spec": {"containers": [{
                        "name": "httpd",
                        "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"
                    }]}
                }
            },
            "status": {"replicas": 1, "readyReplicas": 1, "availableReplicas": 1, "observedGeneration": 1}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "test-rolling-update-controller-130dc",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "test-rolling-update-controller-130dc",
                "namespace": "default",
                "uid": "old-pod-uid-adopted-rollover",
                "labels": {"name": "sample-pod", "pod": "httpd"},
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "ReplicaSet",
                    "name": "test-rolling-update-controller",
                    "uid": old_rs_uid,
                    "controller": true,
                    "blockOwnerDeletion": true
                }]
            },
            "spec": {"containers": [{"name": "httpd", "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"}]},
            "status": {
                "phase": "Running",
                "conditions": [
                    {"type": "Ready", "status": "True"},
                    {"type": "ContainersReady", "status": "True"}
                ]
            }
        }),
    )
    .await
    .unwrap();
    request_json(
        &app,
        Method::POST,
        "/apis/apps/v1/namespaces/default/deployments",
        Some("application/json"),
        Some(json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "test-rolling-update-deployment", "namespace": "default"},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"name": "sample-pod"}},
                "strategy": {"type": "RollingUpdate", "rollingUpdate": {"maxSurge": 1, "maxUnavailable": 0}},
                "template": {
                    "metadata": {"labels": {"name": "sample-pod"}},
                    "spec": {"containers": [{
                        "name": "agnhost",
                        "image": "registry.k8s.io/e2e-test-images/agnhost:2.56"
                    }]}
                }
            }
        })),
        StatusCode::CREATED,
    )
    .await;
    state
        .controller_runtime_fixture()
        .drain_ready()
        .await
        .unwrap();

    let created_pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            klights_cluster_store::ResourceListOptions::new(
                Some("name=sample-pod"),
                None,
                None,
                None,
            ),
        )
        .await
        .unwrap();
    let new_pod = created_pods
        .items
        .iter()
        .find(|pod| pod.uid != "old-pod-uid-adopted-rollover")
        .expect("first rollout reconcile must create a new ReplicaSet Pod");
    db.update_status_only_with_preconditions(
        "v1",
        "Pod",
        Some("default"),
        &new_pod.name,
        json!({
            "phase": "Running",
            "conditions": [
                {"type": "Ready", "status": "True"},
                {"type": "ContainersReady", "status": "True"}
            ]
        }),
        ResourcePreconditions::from_resource(new_pod),
    )
    .await
    .unwrap();
    let deployment = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "test-rolling-update-deployment",
        )
        .await
        .unwrap()
        .unwrap();
    request_json(
        &app,
        Method::PUT,
        "/apis/apps/v1/namespaces/default/deployments/test-rolling-update-deployment",
        Some("application/json"),
        Some(with_resource_version(&deployment)),
        StatusCode::OK,
    )
    .await;
    state
        .controller_runtime_fixture()
        .drain_ready()
        .await
        .unwrap();

    let live_old_rs = db
        .get_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "test-rolling-update-controller",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(live_old_rs.data["spec"]["replicas"], json!(0));
    let old_pod = db
        .get_resource(
            "v1",
            "Pod",
            Some("default"),
            "test-rolling-update-controller-130dc",
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        old_pod
            .data
            .pointer("/metadata/deletionTimestamp")
            .is_some()
    );
    let deployment_after = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "test-rolling-update-deployment",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(deployment_after.data["status"]["updatedReplicas"], json!(1));
}

#[tokio::test]
async fn test_rc_adopts_and_releases_through_leader_repository_with_worker_outbox() {
    let state = build_test_app_state().await;
    let db = state.resource_store();
    let app = state.router();
    seed_namespace(&db, "default").await;
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "orphan",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "orphan",
                "namespace": "default",
                "uid": "orphan-uid",
                "labels": {"app": "rc"}
            },
            "spec": {
                "nodeName": "worker-b",
                "containers": [{"name": "app", "image": "nginx"}]
            },
            "status": {"phase": "Running"}
        }),
    )
    .await
    .unwrap();
    request_json(
        &app,
        Method::POST,
        "/api/v1/namespaces/default/replicationcontrollers",
        Some("application/json"),
        Some(json!({
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "metadata": {"name": "rc", "namespace": "default"},
            "spec": {
                "replicas": 1,
                "selector": {"app": "rc"},
                "template": {
                    "metadata": {"labels": {"app": "rc"}},
                    "spec": {"containers": [{"name": "app", "image": "nginx"}]}
                }
            }
        })),
        StatusCode::CREATED,
    )
    .await;
    state
        .controller_runtime_fixture()
        .drain_ready()
        .await
        .unwrap();

    let adopted = db
        .get_resource("v1", "Pod", Some("default"), "orphan")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        adopted
            .data
            .pointer("/metadata/ownerReferences/0/uid")
            .and_then(Value::as_str),
        db.get_resource("v1", "ReplicationController", Some("default"), "rc")
            .await
            .unwrap()
            .unwrap()
            .data
            .pointer("/metadata/uid")
            .and_then(Value::as_str)
    );

    request_json(
        &app,
        Method::PATCH,
        "/api/v1/namespaces/default/pods/orphan",
        Some("application/strategic-merge-patch+json"),
        Some(json!({"metadata": {"labels": {"app": "other"}}})),
        StatusCode::OK,
    )
    .await;
    let rc = db
        .get_resource("v1", "ReplicationController", Some("default"), "rc")
        .await
        .unwrap()
        .unwrap();
    request_json(
        &app,
        Method::PUT,
        "/api/v1/namespaces/default/replicationcontrollers/rc",
        Some("application/json"),
        Some(with_resource_version(&rc)),
        StatusCode::OK,
    )
    .await;
    state
        .controller_runtime_fixture()
        .drain_ready()
        .await
        .unwrap();

    let released = db
        .get_resource("v1", "Pod", Some("default"), "orphan")
        .await
        .unwrap()
        .unwrap();
    assert!(
        released
            .data
            .pointer("/metadata/ownerReferences")
            .and_then(Value::as_array)
            .is_none_or(Vec::is_empty),
        "RC release must be visible after the leader repository/outbox path"
    );
}

#[tokio::test]
async fn rc_reconcile_created_pod_remains_selector_visible_after_annotation_patch() {
    let state = build_test_app_state().await;
    let db = state.resource_store();
    let app = state.router();
    seed_namespace(&db, "kubectl-rc").await;
    request_json(
        &app,
        Method::POST,
        "/api/v1/namespaces/kubectl-rc/replicationcontrollers",
        Some("application/json"),
        Some(json!({
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "metadata": {"name": "agnhost-primary", "namespace": "kubectl-rc"},
            "spec": {
                "replicas": 1,
                "selector": {"name": "agnhost-primary"},
                "template": {
                    "metadata": {"labels": {"name": "agnhost-primary"}},
                    "spec": {"containers": [{
                        "name": "agnhost",
                        "image": "registry.k8s.io/e2e-test-images/agnhost:2.56"
                    }]}
                }
            }
        })),
        StatusCode::CREATED,
    )
    .await;
    state
        .controller_runtime_fixture()
        .drain_ready()
        .await
        .unwrap();
    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("kubectl-rc"),
            klights_cluster_store::ResourceListOptions::new(
                Some("name=agnhost-primary"),
                None,
                None,
                None,
            ),
        )
        .await
        .unwrap();
    assert_eq!(pods.items.len(), 1);
    let pod_name = pods.items[0].name.clone();

    request_json(
        &app,
        Method::PATCH,
        &format!("/api/v1/namespaces/kubectl-rc/pods/{pod_name}"),
        Some("application/strategic-merge-patch+json"),
        Some(json!({"metadata": {"annotations": {"patched": "true"}}})),
        StatusCode::OK,
    )
    .await;
    let patched = request_json(
        &app,
        Method::GET,
        "/api/v1/namespaces/kubectl-rc/pods?labelSelector=name%3Dagnhost-primary",
        None,
        None,
        StatusCode::OK,
    )
    .await;
    let items = patched["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].pointer("/metadata/labels/name"),
        Some(&json!("agnhost-primary"))
    );
    assert!(
        items[0]
            .pointer("/metadata/ownerReferences")
            .and_then(Value::as_array)
            .is_some_and(|owners| owners.iter().any(|owner| {
                owner.get("kind").and_then(Value::as_str) == Some("ReplicationController")
            }))
    );
}

#[tokio::test]
async fn test_deployment_replicaset_creation_failure_sets_condition() {
    let state = build_test_app_state().await;
    let db = state.resource_store();
    let app = state.router();
    seed_namespace(&db, "default").await;
    db.create_resource(
        "apps/v1",
        "Deployment",
        Some("default"),
        "webhook-test",
        json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "webhook-test",
                "namespace": "default",
                "uid": "test-uid-123"
            },
            "spec": {"replicas": 1}
        }),
    )
    .await
    .unwrap();
    request_json(
        &app,
        Method::PATCH,
        "/apis/apps/v1/namespaces/default/deployments/webhook-test/status",
        Some("application/merge-patch+json"),
        Some(json!({"status": {}})),
        StatusCode::OK,
    )
    .await;
    state
        .controller_runtime_fixture()
        .drain_ready()
        .await
        .unwrap();

    let updated = db
        .get_resource("apps/v1", "Deployment", Some("default"), "webhook-test")
        .await
        .unwrap()
        .unwrap();
    let conditions = updated.data["status"]["conditions"].as_array().unwrap();
    let condition = conditions
        .iter()
        .find(|condition| condition["type"] == "ReplicaFailure")
        .expect("Deployment must report ReplicaFailure instead of returning an error");
    assert_eq!(condition["status"], "True");
    let message = condition["message"].as_str().unwrap();
    assert!(message.contains("missing") || message.contains("Failed"));
}

#[tokio::test]
async fn controller_runtime_fixture_rejects_busy_worker_and_restarts_after_join() {
    let state = build_test_app_state().await;
    let fixture = state.controller_runtime_fixture();
    let competing_fixture = fixture.clone();
    let cancel = tokio_util::sync::CancellationToken::new();
    let worker = fixture.spawn_worker(cancel.clone()).await.unwrap();

    let second_worker = fixture
        .spawn_worker(tokio_util::sync::CancellationToken::new())
        .await
        .expect_err("a second worker must be rejected before it mutates dispatcher state");
    assert!(second_worker.to_string().contains("execution is busy"));
    let competing_drain = competing_fixture
        .drain_ready()
        .await
        .expect_err("a drain must be rejected while the worker owns execution");
    assert!(competing_drain.to_string().contains("execution is busy"));

    cancel.cancel();
    worker.join().await.unwrap();

    let restart_cancel = tokio_util::sync::CancellationToken::new();
    let restarted = fixture.spawn_worker(restart_cancel.clone()).await.unwrap();
    restart_cancel.cancel();
    restarted.join().await.unwrap();
    assert!(fixture.drain_ready().await.unwrap().is_empty());
}
