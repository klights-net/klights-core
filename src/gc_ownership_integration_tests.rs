use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::{Resource, ResourcePreconditions};
use klights_controllers::gc::*;
use klights_reconcile_api::{
    ControllerStoreError, ControllerStoreResult, GcPodDeleteError, GcPodDeleteFuture,
    GcPodDeleteRequest, GcPodDeleteSink,
};

use serde_json::json;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Notify;

fn test_gc_coordination() -> &'static klights_controllers::ControllerCoordination {
    static COORDINATION: std::sync::LazyLock<klights_controllers::ControllerCoordination> =
        std::sync::LazyLock::new(klights_controllers::ControllerCoordination::new);
    &COORDINATION
}

async fn reconcile_owner_references(
    db: &crate::datastore::sqlite::Datastore,
    resource: Resource,
    pod_delete_sink: &dyn GcPodDeleteSink,
    non_pod_finalization: &dyn klights_reconcile_api::GcNonPodFinalizationPort,
) -> Result<OwnerReferenceReconcile> {
    let store = crate::bootstrap::controller_adapters::controller_runtime_adapter::controller_store_for_test(db);
    klights_controllers::gc::reconcile_owner_references(
        &store,
        resource,
        pod_delete_sink,
        non_pod_finalization,
        test_gc_coordination(),
    )
    .await
}

#[expect(
    clippy::too_many_arguments,
    reason = "test compatibility wrapper preserves the UID-qualified owner identity"
)]
async fn cascade_delete_with_uid(
    db: &crate::datastore::sqlite::Datastore,
    owner_uid: &str,
    owner_api_version: &str,
    owner_name: &str,
    owner_kind: &str,
    namespace: Option<String>,
    pod_delete_sink: &dyn GcPodDeleteSink,
    non_pod_finalization: &dyn klights_reconcile_api::GcNonPodFinalizationPort,
) -> Result<()> {
    let store = crate::bootstrap::controller_adapters::controller_runtime_adapter::controller_store_for_test(db);
    klights_controllers::gc::cascade_delete_with_uid(
        &store,
        owner_uid,
        owner_api_version,
        owner_name,
        owner_kind,
        namespace,
        pod_delete_sink,
        non_pod_finalization,
        test_gc_coordination(),
    )
    .await
}

#[expect(
    clippy::too_many_arguments,
    reason = "test compatibility wrapper preserves the UID-qualified owner identity"
)]
async fn owner_cascade_sweep_once(
    db: &crate::datastore::sqlite::Datastore,
    owner_uid: &str,
    owner_api_version: &str,
    owner_name: &str,
    owner_kind: &str,
    namespace: Option<String>,
    pod_delete_sink: &dyn GcPodDeleteSink,
    non_pod_finalization: &dyn klights_reconcile_api::GcNonPodFinalizationPort,
) -> Result<bool> {
    let store = crate::bootstrap::controller_adapters::controller_runtime_adapter::controller_store_for_test(db);
    klights_controllers::gc::owner_cascade_sweep_once(
        &store,
        owner_uid,
        owner_api_version,
        owner_name,
        owner_kind,
        namespace,
        pod_delete_sink,
        non_pod_finalization,
        test_gc_coordination(),
    )
    .await
}

#[expect(
    clippy::too_many_arguments,
    reason = "test compatibility wrapper preserves the UID-qualified owner identity"
)]
async fn check_foreground_deletion_ready(
    db: &crate::datastore::sqlite::Datastore,
    owner_uid: &str,
    owner_api_version: &str,
    owner_name: &str,
    owner_kind: &str,
    namespace: Option<String>,
    pod_delete_sink: &dyn GcPodDeleteSink,
    non_pod_finalization: &dyn klights_reconcile_api::GcNonPodFinalizationPort,
) -> Result<bool> {
    let store = crate::bootstrap::controller_adapters::controller_runtime_adapter::controller_store_for_test(db);
    klights_controllers::gc::check_foreground_deletion_ready(
        &store,
        owner_uid,
        owner_api_version,
        owner_name,
        owner_kind,
        namespace,
        pod_delete_sink,
        non_pod_finalization,
        test_gc_coordination(),
    )
    .await
}

async fn finalize_foreground_owner_if_ready(
    db: &crate::datastore::sqlite::Datastore,
    owner: &Resource,
    pod_delete_sink: &dyn GcPodDeleteSink,
    non_pod_finalization: &dyn klights_reconcile_api::GcNonPodFinalizationPort,
) -> Result<bool> {
    let store = crate::bootstrap::controller_adapters::controller_runtime_adapter::controller_store_for_test(db);
    klights_controllers::gc::finalize_foreground_owner_if_ready(
        &store,
        owner,
        pod_delete_sink,
        non_pod_finalization,
        test_gc_coordination(),
    )
    .await
}

fn non_pod_finalization(
    db: &crate::datastore::sqlite::Datastore,
) -> crate::bootstrap::controller_adapters::gc_delete_adapter::GcNonPodFinalizationAdapter {
    crate::bootstrap::controller_adapters::gc_delete_adapter::GcNonPodFinalizationAdapter::new(
        Arc::new(db.clone()),
    )
}

struct StatusChurningGcStore {
    db: crate::datastore::sqlite::Datastore,
    update_attempts: AtomicUsize,
    typed_conflicts_remaining: AtomicUsize,
    fail_main_update: bool,
}

impl StatusChurningGcStore {
    async fn bump_status(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> ControllerStoreResult<()> {
        let store = &self.db as &dyn crate::datastore::DatastoreBackend;
        let live = GcResourceStore::get_resource(store, api_version, kind, namespace, name)
            .await?
            .expect("status-race resource must remain live");
        let attempt = self.update_attempts.fetch_add(1, Ordering::SeqCst) + 1;
        self.db
            .update_status_only(
                api_version,
                kind,
                namespace,
                name,
                json!({"replicas": attempt, "readyReplicas": attempt}),
                Some(live.resource_version),
            )
            .await
            .map(|_| ())
            .map_err(|error| ControllerStoreError::internal(error.to_string()))
    }
}

#[async_trait]
impl GcResourceStore for StatusChurningGcStore {
    async fn list_custom_resource_definitions(&self) -> ControllerStoreResult<Vec<Resource>> {
        GcResourceStore::list_custom_resource_definitions(
            &self.db as &dyn crate::datastore::DatastoreBackend,
        )
        .await
    }

    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        GcResourceStore::get_resource(
            &self.db as &dyn crate::datastore::DatastoreBackend,
            api_version,
            kind,
            namespace,
            name,
        )
        .await
    }

    async fn update_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> ControllerStoreResult<Resource> {
        self.bump_status(api_version, kind, namespace, name).await?;
        GcResourceStore::update_resource_with_preconditions(
            &self.db as &dyn crate::datastore::DatastoreBackend,
            api_version,
            kind,
            namespace,
            name,
            data,
            preconditions,
        )
        .await
    }

    async fn update_main_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> ControllerStoreResult<Resource> {
        if self.fail_main_update {
            return Err(ControllerStoreError::unavailable(
                "injected owner-reference update failure",
            ));
        }
        self.bump_status(api_version, kind, namespace, name).await?;
        if self
            .typed_conflicts_remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(ControllerStoreError::conflict(
                "injected committed-apply conflict",
            ));
        }
        GcResourceStore::update_main_resource_with_preconditions(
            &self.db as &dyn crate::datastore::DatastoreBackend,
            api_version,
            kind,
            namespace,
            name,
            data,
            preconditions,
        )
        .await
    }

    async fn find_owned_resources(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> ControllerStoreResult<Vec<Resource>> {
        GcResourceStore::find_owned_resources(
            &self.db as &dyn crate::datastore::DatastoreBackend,
            owner_uid,
            namespace,
        )
        .await
    }

    async fn find_owned_by_name_kind_empty_uid(
        &self,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        namespace: Option<&str>,
    ) -> ControllerStoreResult<Vec<Resource>> {
        GcResourceStore::find_owned_by_name_kind_empty_uid(
            &self.db as &dyn crate::datastore::DatastoreBackend,
            owner_api_version,
            owner_name,
            owner_kind,
            namespace,
        )
        .await
    }
}

/// Test sink that records every Pod delete request without touching the datastore.
struct RecordingGcPodDeleteSink {
    requests: Mutex<Vec<(String, String, String)>>, // (namespace, name, uid)
}

impl RecordingGcPodDeleteSink {
    fn new() -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
        }
    }

    fn recorded_requests(&self) -> Vec<(String, String, String)> {
        self.requests.lock().unwrap().clone()
    }

    fn has_request(&self, ns: &str, name: &str, uid: &str) -> bool {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .any(|(n, na, u)| n == ns && na == name && u == uid)
    }
}

impl GcPodDeleteSink for RecordingGcPodDeleteSink {
    fn request_gc_pod_delete(&self, request: GcPodDeleteRequest) -> GcPodDeleteFuture<'_> {
        Box::pin(async move {
            let identity = request.into_identity();
            self.requests
                .lock()
                .unwrap()
                .push((identity.namespace, identity.name, identity.uid));
            Ok(())
        })
    }
}

#[derive(Clone, Copy)]
enum FirstGcPodDeleteResult {
    Unavailable,
    Gone,
}

struct FailOrLoseFirstGcPodDeleteSink {
    first_result: FirstGcPodDeleteResult,
    attempts: AtomicUsize,
}

impl FailOrLoseFirstGcPodDeleteSink {
    fn new(first_result: FirstGcPodDeleteResult) -> Self {
        Self {
            first_result,
            attempts: AtomicUsize::new(0),
        }
    }
}

impl GcPodDeleteSink for FailOrLoseFirstGcPodDeleteSink {
    fn request_gc_pod_delete(&self, _request: GcPodDeleteRequest) -> GcPodDeleteFuture<'_> {
        Box::pin(async move {
            if self.attempts.fetch_add(1, Ordering::SeqCst) != 0 {
                return Ok(());
            }
            Err(match self.first_result {
                FirstGcPodDeleteResult::Unavailable => {
                    GcPodDeleteError::unavailable("transient delete failure")
                }
                FirstGcPodDeleteResult::Gone => GcPodDeleteError::not_found("Pod already gone"),
            })
        })
    }
}

/// No-op sink for existing tests that don't involve Pod children.
pub(crate) struct NoOpGcPodDeleteSink;

impl GcPodDeleteSink for NoOpGcPodDeleteSink {
    fn request_gc_pod_delete(&self, _request: GcPodDeleteRequest) -> GcPodDeleteFuture<'_> {
        Box::pin(async {
            Err(GcPodDeleteError::unavailable(
                "no-op sink must not be called for Pod deletes — use RecordingGcPodDeleteSink for Pod tests",
            ))
        })
    }
}

struct ConcurrentBlockingGcPodDeleteSink {
    started: AtomicUsize,
    max_in_flight: AtomicUsize,
    in_flight: AtomicUsize,
    required_started: usize,
    notify: Notify,
}

impl ConcurrentBlockingGcPodDeleteSink {
    fn new(required_started: usize) -> Arc<Self> {
        Arc::new(Self {
            started: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            required_started,
            notify: Notify::new(),
        })
    }

    fn max_in_flight(&self) -> usize {
        self.max_in_flight.load(Ordering::SeqCst)
    }
}

impl GcPodDeleteSink for ConcurrentBlockingGcPodDeleteSink {
    fn request_gc_pod_delete(&self, _request: GcPodDeleteRequest) -> GcPodDeleteFuture<'_> {
        Box::pin(async move {
            let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(in_flight, Ordering::SeqCst);
            let started = self.started.fetch_add(1, Ordering::SeqCst) + 1;
            if started >= self.required_started {
                self.notify.notify_waiters();
            }
            while self.started.load(Ordering::SeqCst) < self.required_started {
                self.notify.notified().await;
            }
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

#[tokio::test]
async fn test_cascade_delete_three_level_chain() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();

    // Create Deployment (level 1)
    let deploy_uid = "deploy-uid-gc";
    db.create_resource(
        "apps/v1",
        "Deployment",
        Some("default"),
        "my-deploy",
        json!({
            "metadata": {"name": "my-deploy", "namespace": "default", "uid": deploy_uid}
        }),
    )
    .await
    .unwrap();

    // Create ReplicaSet owned by Deployment (level 2)
    let rs_uid = "rs-uid-gc";
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "my-deploy-rs",
        json!({
            "metadata": {
                "name": "my-deploy-rs", "namespace": "default", "uid": rs_uid,
                "ownerReferences": [{"uid": deploy_uid, "kind": "Deployment"}]
            }
        }),
    )
    .await
    .unwrap();

    // Create ConfigMap owned by ReplicaSet (level 3)
    let cm_uid = "cm-uid-gc";
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "my-deploy-rs-cm1",
        json!({
            "metadata": {
                "name": "my-deploy-rs-cm1", "namespace": "default", "uid": cm_uid,
                "ownerReferences": [{"uid": rs_uid, "kind": "ReplicaSet"}]
            }
        }),
    )
    .await
    .unwrap();

    let sink = NoOpGcPodDeleteSink;

    // Cascade delete from Deployment UID
    cascade_delete_with_uid(
        &db,
        deploy_uid,
        "apps/v1",
        "my-deploy",
        "Deployment",
        Some("default".to_string()),
        &sink,
        &non_pod_finalization(&db),
    )
    .await
    .unwrap();

    // ReplicaSet should be deleted (not visible in list)
    let rs = db
        .get_resource("apps/v1", "ReplicaSet", Some("default"), "my-deploy-rs")
        .await
        .unwrap();
    assert!(rs.is_none(), "ReplicaSet should be deleted by cascade");

    // ConfigMap should be deleted
    let cm = db
        .get_resource("v1", "ConfigMap", Some("default"), "my-deploy-rs-cm1")
        .await
        .unwrap();
    assert!(cm.is_none(), "ConfigMap should be deleted by cascade");
}

#[tokio::test]
async fn test_cascade_delete_single_level() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();

    let rs_uid = "rs-uid-single";
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "my-rs",
        json!({
            "metadata": {"name": "my-rs", "namespace": "default", "uid": rs_uid}
        }),
    )
    .await
    .unwrap();

    // Create 3 ConfigMaps owned by the ReplicaSet
    for i in 1..=3 {
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            &format!("cm-{}", i),
            json!({
                "metadata": {
                    "name": format!("cm-{}", i), "namespace": "default",
                    "uid": format!("cm-uid-{}", i),
                    "ownerReferences": [{"uid": rs_uid, "kind": "ReplicaSet"}]
                }
            }),
        )
        .await
        .unwrap();
    }

    let sink = NoOpGcPodDeleteSink;

    cascade_delete_with_uid(
        &db,
        rs_uid,
        "apps/v1",
        "my-rs",
        "ReplicaSet",
        Some("default".to_string()),
        &sink,
        &non_pod_finalization(&db),
    )
    .await
    .unwrap();

    // All 3 ConfigMaps should be deleted
    let cms = db
        .list_resources(
            "v1",
            "ConfigMap",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        cms.items.len(),
        0,
        "All owned ConfigMaps should be cascade deleted"
    );
}

#[tokio::test]
async fn test_cascade_delete_marks_finalizer_held_child_without_recursing() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();

    let owner_uid = "owner-finalizer-cascade";
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "owner-rs",
        json!({
            "metadata": {"name": "owner-rs", "namespace": "default", "uid": owner_uid}
        }),
    )
    .await
    .unwrap();

    let child_uid = "child-finalizer-cascade";
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "held-child",
        json!({
            "metadata": {
                "name": "held-child",
                "namespace": "default",
                "uid": child_uid,
                "finalizers": ["example.com/hold"],
                "ownerReferences": [{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "owner-rs", "uid": owner_uid}]
            }
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Secret",
        Some("default"),
        "grandchild",
        json!({
            "metadata": {
                "name": "grandchild",
                "namespace": "default",
                "uid": "grandchild-finalizer-cascade",
                "ownerReferences": [{"apiVersion": "v1", "kind": "ConfigMap", "name": "held-child", "uid": child_uid}]
            }
        }),
    )
    .await
    .unwrap();

    let sink = NoOpGcPodDeleteSink;
    cascade_delete_with_uid(
        &db,
        owner_uid,
        "apps/v1",
        "owner-rs",
        "ReplicaSet",
        Some("default".to_string()),
        &sink,
        &non_pod_finalization(&db),
    )
    .await
    .unwrap();

    let child = db
        .get_resource("v1", "ConfigMap", Some("default"), "held-child")
        .await
        .unwrap()
        .expect("finalizer-held cascade child must remain");
    assert!(
        child.data.pointer("/metadata/deletionTimestamp").is_some(),
        "finalizer-held cascade child must be marked terminating: {:?}",
        child.data
    );
    assert_eq!(
        child.data.pointer("/metadata/finalizers/0"),
        Some(&json!("example.com/hold"))
    );

    assert!(
        db.get_resource("v1", "Secret", Some("default"), "grandchild")
            .await
            .unwrap()
            .is_some(),
        "GC must not recurse into children of a resource that was only marked terminating"
    );
}

#[tokio::test]
async fn test_cascade_delete_preserves_dependents_with_another_live_owner() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();

    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "rc-to-delete",
        json!({
            "metadata": {
                "name": "rc-to-delete",
                "namespace": "default",
                "uid": "rc-delete-uid"
            }
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "rc-to-stay",
        json!({
            "metadata": {
                "name": "rc-to-stay",
                "namespace": "default",
                "uid": "rc-stay-uid"
            }
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "shared-cm",
        json!({
            "metadata": {
                "name": "shared-cm",
                "namespace": "default",
                "uid": "shared-cm-uid",
                "ownerReferences": [
                    {
                        "apiVersion": "v1",
                        "kind": "ReplicationController",
                        "name": "rc-to-delete",
                        "uid": "rc-delete-uid"
                    },
                    {
                        "apiVersion": "v1",
                        "kind": "ReplicationController",
                        "name": "rc-to-stay",
                        "uid": "rc-stay-uid"
                    }
                ]
            }
        }),
    )
    .await
    .unwrap();

    db.delete_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "rc-to-delete",
    )
    .await
    .unwrap();

    let sink = NoOpGcPodDeleteSink;

    cascade_delete_with_uid(
        &db,
        "rc-delete-uid",
        "v1",
        "rc-to-delete",
        "ReplicationController",
        Some("default".to_string()),
        &sink,
        &non_pod_finalization(&db),
    )
    .await
    .unwrap();

    let cm = db
        .get_resource("v1", "ConfigMap", Some("default"), "shared-cm")
        .await
        .unwrap()
        .expect("ConfigMap with another live owner must survive cascade delete");
    let owner_refs = cm
        .data
        .pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
        .expect("surviving ConfigMap must keep live owner reference");
    assert_eq!(owner_refs.len(), 1);
    assert_eq!(owner_refs[0]["uid"], "rc-stay-uid");
}

#[tokio::test]
async fn orphan_owner_ref_removal_survives_continuous_child_status_churn() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let owner_uid = "deployment-status-churn-uid";

    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "status-churn-rs",
        json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": "status-churn-rs",
                "namespace": "default",
                "uid": "status-churn-rs-uid",
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "name": "status-churn-deployment",
                    "uid": owner_uid,
                    "controller": true
                }]
            },
            "spec": {"replicas": 1},
            "status": {"replicas": 0, "readyReplicas": 0}
        }),
    )
    .await
    .unwrap();

    let racing = StatusChurningGcStore {
        db: db.clone(),
        update_attempts: AtomicUsize::new(0),
        typed_conflicts_remaining: AtomicUsize::new(1),
        fail_main_update: false,
    };
    orphan_children(
        &racing,
        owner_uid,
        "apps/v1",
        "status-churn-deployment",
        "Deployment",
        Some("default".to_string()),
    )
    .await
    .expect("orphaning should tolerate status-only child updates");

    let child = db
        .get_resource("apps/v1", "ReplicaSet", Some("default"), "status-churn-rs")
        .await
        .unwrap()
        .expect("orphaned ReplicaSet must survive");
    assert!(
        child
            .data
            .pointer("/metadata/ownerReferences")
            .and_then(|refs| refs.as_array())
            .is_none_or(Vec::is_empty),
        "orphaned ReplicaSet must not retain the Deployment owner reference"
    );
    assert_eq!(
        child
            .data
            .pointer("/status/readyReplicas")
            .and_then(serde_json::Value::as_u64),
        Some(2),
        "main-resource ownerRef removal must preserve the racing status update"
    );
    assert_eq!(
        racing.update_attempts.load(Ordering::SeqCst),
        2,
        "typed committed-apply conflict must refetch once without exhausting GC retries"
    );
}

#[tokio::test]
async fn orphan_children_returns_error_when_owner_ref_update_does_not_commit() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let owner_uid = "deployment-orphan-failure-uid";

    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "orphan-failure-rs",
        json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": "orphan-failure-rs",
                "namespace": "default",
                "uid": "orphan-failure-rs-uid",
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "name": "orphan-failure-deployment",
                    "uid": owner_uid,
                    "controller": true
                }]
            },
            "spec": {"replicas": 1},
            "status": {"replicas": 1}
        }),
    )
    .await
    .unwrap();

    let failing = StatusChurningGcStore {
        db,
        update_attempts: AtomicUsize::new(0),
        typed_conflicts_remaining: AtomicUsize::new(0),
        fail_main_update: true,
    };
    let error = orphan_children(
        &failing,
        owner_uid,
        "apps/v1",
        "orphan-failure-deployment",
        "Deployment",
        Some("default".to_string()),
    )
    .await
    .expect_err("orphaning must fail closed while a child still retains the owner");

    assert!(
        error
            .to_string()
            .contains("injected owner-reference update failure")
    );
}

#[tokio::test]
async fn test_reconcile_owner_references_deletes_dangling_dependent_and_cascades_children() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();

    let rs = db
        .create_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "dangling-rs",
            json!({
                "metadata": {
                    "name": "dangling-rs",
                    "namespace": "default",
                    "uid": "dangling-rs-uid",
                    "ownerReferences": [{
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "name": "missing-deploy",
                        "uid": "missing-deploy-uid",
                        "controller": true
                    }]
                },
                "spec": {}
            }),
        )
        .await
        .unwrap();

    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "owned-cm",
        json!({
            "metadata": {
                "name": "owned-cm",
                "namespace": "default",
                "uid": "owned-cm-uid",
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "ReplicaSet",
                    "name": "dangling-rs",
                    "uid": "dangling-rs-uid",
                    "controller": true
                }]
            }
        }),
    )
    .await
    .unwrap();

    let sink = NoOpGcPodDeleteSink;

    let outcome = reconcile_owner_references(&db, rs, &sink, &non_pod_finalization(&db))
        .await
        .unwrap();
    assert_eq!(outcome, OwnerReferenceReconcile::Deleted);

    assert!(
        db.get_resource("apps/v1", "ReplicaSet", Some("default"), "dangling-rs")
            .await
            .unwrap()
            .is_none(),
        "dependent with only dangling owners must be garbage collected"
    );
    assert!(
        db.get_resource("v1", "ConfigMap", Some("default"), "owned-cm")
            .await
            .unwrap()
            .is_none(),
        "garbage collecting a dependent must cascade to its dependents"
    );
}

#[tokio::test]
async fn test_reconcile_owner_references_preserves_live_owner_and_removes_dangling_refs() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();

    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "live-owner",
        json!({
            "metadata": {
                "name": "live-owner",
                "namespace": "default",
                "uid": "live-owner-uid"
            }
        }),
    )
    .await
    .unwrap();

    let cm = db
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "shared-cm",
            json!({
                "metadata": {
                    "name": "shared-cm",
                    "namespace": "default",
                    "uid": "shared-cm-uid",
                    "ownerReferences": [
                        {
                            "apiVersion": "v1",
                            "kind": "ReplicationController",
                            "name": "missing-owner",
                            "uid": "missing-owner-uid"
                        },
                        {
                            "apiVersion": "v1",
                            "kind": "ReplicationController",
                            "name": "live-owner",
                            "uid": "live-owner-uid"
                        }
                    ]
                }
            }),
        )
        .await
        .unwrap();

    let sink = NoOpGcPodDeleteSink;

    let outcome = reconcile_owner_references(&db, cm, &sink, &non_pod_finalization(&db))
        .await
        .unwrap();
    assert_eq!(outcome, OwnerReferenceReconcile::OwnerReferencesUpdated);

    let cm = db
        .get_resource("v1", "ConfigMap", Some("default"), "shared-cm")
        .await
        .unwrap()
        .expect("ConfigMap with a live owner must remain");
    let refs = cm
        .data
        .pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
        .unwrap();
    assert_eq!(refs.len(), 1);
    assert_eq!(
        refs[0].get("uid").and_then(|v| v.as_str()),
        Some("live-owner-uid")
    );
}

#[tokio::test]
async fn test_reconcile_owner_references_treats_non_foreground_deleting_owner_as_live() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();

    db.create_resource(
        "apps/v1",
        "Deployment",
        Some("default"),
        "finalizing-owner",
        json!({
            "metadata": {
                "name": "finalizing-owner",
                "namespace": "default",
                "uid": "finalizing-owner-uid",
                "deletionTimestamp": "2026-04-29T00:00:00Z",
                "finalizers": ["example.com/cleanup"]
            }
        }),
    )
    .await
    .unwrap();

    let rs = db
        .create_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "preserved-rs",
            json!({
                "metadata": {
                    "name": "preserved-rs",
                    "namespace": "default",
                    "uid": "preserved-rs-uid",
                    "ownerReferences": [{
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "name": "finalizing-owner",
                        "uid": "finalizing-owner-uid",
                        "controller": true
                    }]
                },
                "spec": {}
            }),
        )
        .await
        .unwrap();

    let sink = NoOpGcPodDeleteSink;
    let outcome = reconcile_owner_references(&db, rs, &sink, &non_pod_finalization(&db))
        .await
        .unwrap();
    assert_eq!(outcome, OwnerReferenceReconcile::HasLiveOwner);
    assert!(
        db.get_resource("apps/v1", "ReplicaSet", Some("default"), "preserved-rs")
            .await
            .unwrap()
            .is_some(),
        "ordinary finalizer deletion must not make the owner dangling"
    );
}

#[tokio::test]
async fn test_reconcile_owner_references_treats_foreground_deleting_owner_as_collectable() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();

    db.create_resource(
        "apps/v1",
        "Deployment",
        Some("default"),
        "foreground-owner",
        json!({
            "metadata": {
                "name": "foreground-owner",
                "namespace": "default",
                "uid": "foreground-owner-uid",
                "deletionTimestamp": "2026-04-29T00:00:00Z",
                "finalizers": ["foregroundDeletion"]
            }
        }),
    )
    .await
    .unwrap();

    let rs = db
        .create_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "foreground-child",
            json!({
                "metadata": {
                    "name": "foreground-child",
                    "namespace": "default",
                    "uid": "foreground-child-uid",
                    "ownerReferences": [{
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "name": "foreground-owner",
                        "uid": "foreground-owner-uid",
                        "controller": true
                    }]
                },
                "spec": {}
            }),
        )
        .await
        .unwrap();

    let sink = NoOpGcPodDeleteSink;
    let outcome = reconcile_owner_references(&db, rs, &sink, &non_pod_finalization(&db))
        .await
        .unwrap();
    assert_eq!(outcome, OwnerReferenceReconcile::Deleted);
    assert!(
        db.get_resource("apps/v1", "ReplicaSet", Some("default"), "foreground-child")
            .await
            .unwrap()
            .is_none(),
        "foreground-deleting owner should not block dependent GC"
    );
}

#[tokio::test]
async fn test_reconcile_owner_references_preserves_cluster_dependent_with_namespaced_custom_owner_ref()
 {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();

    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "widgets.example.com",
        json!({
            "metadata": {"name": "widgets.example.com"},
            "spec": {
                "group": "example.com",
                "scope": "Namespaced",
                "names": {"kind": "Widget", "plural": "widgets"},
                "versions": [{"name": "v1", "served": true, "storage": true}]
            }
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "example.com/v1",
        "Widget",
        Some("default"),
        "namespaced-owner",
        json!({
            "metadata": {
                "name": "namespaced-owner",
                "namespace": "default",
                "uid": "widget-owner-uid"
            }
        }),
    )
    .await
    .unwrap();

    let cluster_dependent = db
        .create_resource(
            "rbac.authorization.k8s.io/v1",
            "ClusterRoleBinding",
            None,
            "cluster-dependent",
            json!({
                "metadata": {
                    "name": "cluster-dependent",
                    "uid": "cluster-dependent-uid",
                    "ownerReferences": [{
                        "apiVersion": "example.com/v1",
                        "kind": "Widget",
                        "name": "namespaced-owner",
                        "uid": "widget-owner-uid"
                    }]
                },
                "roleRef": {
                    "apiGroup": "rbac.authorization.k8s.io",
                    "kind": "ClusterRole",
                    "name": "view"
                },
                "subjects": []
            }),
        )
        .await
        .unwrap();

    let sink = NoOpGcPodDeleteSink;
    let outcome =
        reconcile_owner_references(&db, cluster_dependent, &sink, &non_pod_finalization(&db))
            .await
            .unwrap();
    assert_eq!(outcome, OwnerReferenceReconcile::HasLiveOwner);
    assert!(
        db.get_resource(
            "rbac.authorization.k8s.io/v1",
            "ClusterRoleBinding",
            None,
            "cluster-dependent"
        )
        .await
        .unwrap()
        .is_some(),
        "cluster-scoped dependents with namespaced ownerRefs are unresolvable and must not be GC'd"
    );
}

#[tokio::test]
async fn test_cascade_delete_no_owned_resources() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();

    // Create a pod with no ownerReferences
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "standalone-pod",
        json!({"metadata": {"name": "standalone-pod"}}),
    )
    .await
    .unwrap();

    // Cascade delete with a UID that owns nothing
    let sink = NoOpGcPodDeleteSink;
    cascade_delete_with_uid(
        &db,
        "nonexistent-owner",
        "v1",
        "standalone-pod",
        "Pod",
        Some("default".to_string()),
        &sink,
        &non_pod_finalization(&db),
    )
    .await
    .unwrap();

    // Pod should remain
    let pod = db
        .get_resource("v1", "Pod", Some("default"), "standalone-pod")
        .await
        .unwrap();
    assert!(pod.is_some(), "Unowned pod should not be affected");
}

#[tokio::test]
async fn test_cascade_delete_empty_uid() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();

    // Create a resource that should NOT be deleted
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "safe-pod",
        json!({"metadata": {"name": "safe-pod"}}),
    )
    .await
    .unwrap();

    // Empty UID and empty name should be a no-op (guard clause)
    let sink = NoOpGcPodDeleteSink;
    cascade_delete_with_uid(
        &db,
        "",
        "",
        "",
        "",
        Some("default".to_string()),
        &sink,
        &non_pod_finalization(&db),
    )
    .await
    .unwrap();

    // Pod should still exist
    let pod = db
        .get_resource("v1", "Pod", Some("default"), "safe-pod")
        .await
        .unwrap();
    assert!(
        pod.is_some(),
        "Pod should not be affected by empty UID cascade"
    );
}

#[tokio::test]
async fn test_orphan_children_removes_owner_references() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();

    let owner_uid = "owner-uid";
    db.create_resource(
        "apps/v1",
        "Deployment",
        Some("default"),
        "owner",
        json!({"metadata": {"name": "owner", "namespace": "default", "uid": owner_uid}}),
    )
    .await
    .unwrap();

    // Create child with ownerReference
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "child-rs",
        json!({
            "metadata": {
                "name": "child-rs",
                "namespace": "default",
                "uid": "child-uid",
                "ownerReferences": [{
                    "uid": owner_uid,
                    "kind": "Deployment",
                    "blockOwnerDeletion": true
                }]
            }
        }),
    )
    .await
    .unwrap();

    // Orphan children
    orphan_children(
        &crate::bootstrap::controller_adapters::controller_runtime_adapter::controller_store_for_test(&db),
        owner_uid,
        "apps/v1",
        "owner",
        "Deployment",
        Some("default".to_string()),
    )
    .await
    .unwrap();

    // Child should still exist
    let child = db
        .get_resource("apps/v1", "ReplicaSet", Some("default"), "child-rs")
        .await
        .unwrap();
    assert!(child.is_some(), "Child should not be deleted");

    // ownerReferences should be removed
    let child_data = child.unwrap().data;
    let has_owner_refs = child_data
        .get("metadata")
        .and_then(|m| m.get("ownerReferences"))
        .and_then(|refs| refs.as_array())
        .map(|arr| !arr.is_empty())
        .unwrap_or(false);
    assert!(
        !has_owner_refs,
        "ownerReferences should be removed or empty"
    );
}

#[tokio::test]
async fn test_orphan_children_removes_empty_uid_ownerrefs_by_name_kind() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();

    db.create_resource(
        "apps/v1",
        "Deployment",
        Some("default"),
        "owner-empty-uid",
        json!({"metadata": {"name": "owner-empty-uid", "namespace": "default"}}),
    )
    .await
    .unwrap();

    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "child-rs-empty-uid",
        json!({
            "metadata": {
                "name": "child-rs-empty-uid",
                "namespace": "default",
                "uid": "child-empty-uid",
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "name": "owner-empty-uid",
                    "uid": "",
                    "controller": true
                }]
            }
        }),
    )
    .await
    .unwrap();

    orphan_children(
        &crate::bootstrap::controller_adapters::controller_runtime_adapter::controller_store_for_test(&db),
        "",
        "apps/v1",
        "owner-empty-uid",
        "Deployment",
        Some("default".to_string()),
    )
    .await
    .unwrap();

    let child = db
        .get_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "child-rs-empty-uid",
        )
        .await
        .unwrap()
        .expect("child should still exist");

    let has_owner_refs = child
        .data
        .get("metadata")
        .and_then(|m| m.get("ownerReferences"))
        .and_then(|refs| refs.as_array())
        .map(|arr| !arr.is_empty())
        .unwrap_or(false);
    assert!(
        !has_owner_refs,
        "ownerReferences with empty uid must be removed during orphan"
    );
}

#[tokio::test]
async fn test_foreground_deletion_deletes_children_first() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();

    let owner_uid = "owner-uid-fg";
    db.create_resource(
        "apps/v1",
        "Deployment",
        Some("default"),
        "owner-fg",
        json!({"metadata": {"name": "owner-fg", "namespace": "default", "uid": owner_uid}}),
    )
    .await
    .unwrap();

    // Create child
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "child-rs-fg",
        json!({
            "metadata": {
                "name": "child-rs-fg",
                "namespace": "default",
                "uid": "child-uid-fg",
                "ownerReferences": [{
                    "uid": owner_uid,
                    "kind": "Deployment",
                    "blockOwnerDeletion": true
                }]
            }
        }),
    )
    .await
    .unwrap();

    let sink = NoOpGcPodDeleteSink;

    // Check foreground deletion (should delete children)
    let ready = check_foreground_deletion_ready(
        &db,
        owner_uid,
        "apps/v1",
        "owner-fg",
        "Deployment",
        Some("default".to_string()),
        &sink,
        &non_pod_finalization(&db),
    )
    .await
    .unwrap();

    assert!(
        ready,
        "foreground deletion should be ready in the same event turn after hard-deleting the only child"
    );

    // Child should now be deleted
    let child = db
        .get_resource("apps/v1", "ReplicaSet", Some("default"), "child-rs-fg")
        .await
        .unwrap();
    assert!(child.is_none(), "Child should be deleted");

    // A follow-up call must still observe readiness, but foreground GC must not
    // require this extra event turn to make progress.
    let ready_now = check_foreground_deletion_ready(
        &db,
        owner_uid,
        "apps/v1",
        "owner-fg",
        "Deployment",
        Some("default".to_string()),
        &sink,
        &non_pod_finalization(&db),
    )
    .await
    .unwrap();
    assert!(ready_now, "Should return true when no children remain");
}

#[tokio::test]
async fn foreground_delete_finalizes_owner_after_shared_dependents_are_unblocked() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();

    let owner = db
        .create_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "rc-to-delete",
            json!({
                "metadata": {
                    "name": "rc-to-delete",
                    "namespace": "default",
                    "uid": "rc-delete-uid",
                    "deletionTimestamp": "2026-05-21T20:34:35Z",
                    "finalizers": ["foregroundDeletion"]
                }
            }),
        )
        .await
        .unwrap();

    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "rc-to-stay",
        json!({
            "metadata": {
                "name": "rc-to-stay",
                "namespace": "default",
                "uid": "rc-stay-uid"
            }
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "shared-child",
        json!({
            "metadata": {
                "name": "shared-child",
                "namespace": "default",
                "uid": "shared-child-uid",
                "ownerReferences": [
                    {
                        "apiVersion": "v1",
                        "kind": "ReplicationController",
                        "name": "rc-to-delete",
                        "uid": "rc-delete-uid",
                        "blockOwnerDeletion": true
                    },
                    {
                        "apiVersion": "v1",
                        "kind": "ReplicationController",
                        "name": "rc-to-stay",
                        "uid": "rc-stay-uid"
                    }
                ]
            }
        }),
    )
    .await
    .unwrap();

    let sink = NoOpGcPodDeleteSink;
    let finalized =
        finalize_foreground_owner_if_ready(&db, &owner, &sink, &non_pod_finalization(&db))
            .await
            .unwrap();

    assert!(
        finalized,
        "foreground owner should finalize once all dependents only retain other live owners"
    );
    assert!(
        db.get_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "rc-to-delete"
        )
        .await
        .unwrap()
        .is_none(),
        "foreground owner must be deleted after shared dependents are unblocked"
    );

    let child = db
        .get_resource("v1", "ConfigMap", Some("default"), "shared-child")
        .await
        .unwrap()
        .expect("shared child must survive foreground owner deletion");
    let refs = child
        .data
        .pointer("/metadata/ownerReferences")
        .and_then(|value| value.as_array())
        .unwrap();
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0]["uid"], "rc-stay-uid");
}

#[tokio::test]
async fn foreground_owner_ready_after_hard_deleted_child_and_shared_dependents_orphaned() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();

    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "owner-rc-to-delete",
        json!({
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "metadata": {
                "name": "owner-rc-to-delete",
                "namespace": "default",
                "uid": "rc-foreground-delete-uid",
                "deletionTimestamp": "2026-06-07T10:00:00Z",
                "finalizers": ["foregroundDeletion"]
            }
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "owner-rc-keep",
        json!({
            "metadata": {
                "name": "owner-rc-keep",
                "namespace": "default",
                "uid": "rc-keep-uid"
            }
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "unique-child",
        json!({
            "metadata": {
                "name": "unique-child",
                "namespace": "default",
                "uid": "unique-child-uid",
                "ownerReferences": [{
                    "apiVersion": "v1",
                    "kind": "ReplicationController",
                    "name": "owner-rc-to-delete",
                    "uid": "rc-foreground-delete-uid",
                    "blockOwnerDeletion": true
                }]
            }
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "shared-child",
        json!({
            "metadata": {
                "name": "shared-child",
                "namespace": "default",
                "uid": "shared-child-uid",
                "ownerReferences": [{
                    "apiVersion": "v1",
                    "kind": "ReplicationController",
                    "name": "owner-rc-to-delete",
                    "uid": "rc-foreground-delete-uid",
                    "blockOwnerDeletion": true
                }, {
                    "apiVersion": "v1",
                    "kind": "ReplicationController",
                    "name": "owner-rc-keep",
                    "uid": "rc-keep-uid"
                }]
            }
        }),
    )
    .await
    .unwrap();

    let owner = db
        .get_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "owner-rc-to-delete",
        )
        .await
        .unwrap()
        .expect("owner must exist");

    let sink = NoOpGcPodDeleteSink;
    let ready = finalize_foreground_owner_if_ready(&db, &owner, &sink, &non_pod_finalization(&db))
        .await
        .unwrap();

    assert!(
        ready,
        "foreground owner should finalize in the same event turn after hard-deleted children are gone and shared dependents are unblocked"
    );
    assert!(
        db.get_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "owner-rc-to-delete"
        )
        .await
        .unwrap()
        .is_none(),
        "foreground owner should be deleted when ready"
    );
}

#[tokio::test]
async fn gc_foreground_owner_with_mixed_pods_waits_for_pod_cleanup() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let sink = RecordingGcPodDeleteSink::new();

    let owner_uid = "owner-rc-to-delete-uid";
    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "owner-rc-to-delete",
        json!({
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "metadata": {
                "name": "owner-rc-to-delete",
                "namespace": "default",
                "uid": owner_uid,
                "deletionTimestamp": "2026-06-07T10:00:00Z",
                "finalizers": ["foregroundDeletion"]
            }
        }),
    )
    .await
    .unwrap();

    let stay_uid = "owner-rc-to-stay-uid";
    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "owner-rc-to-stay",
        json!({
            "metadata": {
                "name": "owner-rc-to-stay",
                "namespace": "default",
                "uid": stay_uid
            }
        }),
    )
    .await
    .unwrap();

    let mut unique_pod_names = Vec::new();
    for i in 0..4 {
        let name = format!("unique-pod-{i}");
        let uid = format!("unique-pod-{i}-uid");
        unique_pod_names.push(name.clone());
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            &name,
            json!({
                "metadata": {
                    "name": name,
                    "namespace": "default",
                    "uid": uid,
                    "ownerReferences": [{
                        "apiVersion":"v1",
                        "kind":"ReplicationController",
                        "name":"owner-rc-to-delete",
                        "uid": owner_uid,
                        "blockOwnerDeletion": true
                    }]
                }
            }),
        )
        .await
        .unwrap();
    }

    let mut shared_pod_names = Vec::new();
    for i in 0..4 {
        let name = format!("shared-pod-{i}");
        let uid = format!("shared-pod-{i}-uid");
        shared_pod_names.push(name.clone());
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            &name,
            json!({
                "metadata": {
                    "name": name,
                    "namespace": "default",
                    "uid": uid,
                    "ownerReferences": [
                        {
                            "apiVersion":"v1",
                            "kind":"ReplicationController",
                            "name":"owner-rc-to-delete",
                            "uid": owner_uid,
                            "blockOwnerDeletion": true
                        },
                        {
                            "apiVersion":"v1",
                            "kind":"ReplicationController",
                            "name":"owner-rc-to-stay",
                            "uid": stay_uid
                        }
                    ]
                }
            }),
        )
        .await
        .unwrap();
    }

    let ready = check_foreground_deletion_ready(
        &db,
        owner_uid,
        "v1",
        "owner-rc-to-delete",
        "ReplicationController",
        Some("default".to_string()),
        &sink,
        &non_pod_finalization(&db),
    )
    .await
    .unwrap();

    assert!(
        !ready,
        "foreground owner should remain blocking while unique Pods are pending deletion"
    );

    let owner = db
        .get_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "owner-rc-to-delete",
        )
        .await
        .unwrap()
        .expect("owner must still exist before finalizer clear");

    for name in unique_pod_names.iter() {
        let unique = db
            .get_resource("v1", "Pod", Some("default"), name)
            .await
            .unwrap()
            .expect("unique pod should still exist after delete request");
        let owner_refs = unique
            .data
            .pointer("/metadata/ownerReferences")
            .and_then(|refs| refs.as_array())
            .expect("unique pod should retain ownerReferences");
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(owner_refs[0]["uid"], owner_uid);
    }

    assert_eq!(
        sink.recorded_requests().len(),
        4,
        "all unique Pods should be delete-requested"
    );

    for name in shared_pod_names.iter() {
        let shared = db
            .get_resource("v1", "Pod", Some("default"), name)
            .await
            .unwrap()
            .expect("shared pod should remain while being orphaned");
        let owner_refs = shared
            .data
            .pointer("/metadata/ownerReferences")
            .and_then(|refs| refs.as_array())
            .expect("shared pod should retain ownerReferences");
        assert_eq!(
            owner_refs.len(),
            1,
            "shared pod should only keep the stay owner"
        );
        assert_eq!(owner_refs[0]["uid"], stay_uid);
    }

    for i in 0..4 {
        let name = format!("unique-pod-{i}");
        db.delete_resource("v1", "Pod", Some("default"), &name)
            .await
            .unwrap();
    }

    let finalized =
        finalize_foreground_owner_if_ready(&db, &owner, &sink, &non_pod_finalization(&db))
            .await
            .unwrap();

    assert!(
        finalized,
        "foreground owner should be finalized once dependents are clear"
    );

    assert!(
        db.get_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "owner-rc-to-delete"
        )
        .await
        .unwrap()
        .is_none(),
        "owner should be deleted after pod cleanup"
    );

    for name in shared_pod_names {
        let shared = db
            .get_resource("v1", "Pod", Some("default"), &name)
            .await
            .unwrap()
            .expect("shared pod should remain while being orphaned");
        let owner_refs = shared
            .data
            .pointer("/metadata/ownerReferences")
            .and_then(|refs| refs.as_array())
            .expect("shared pod should retain ownerReferences");
        assert_eq!(
            owner_refs.len(),
            1,
            "shared pod should only keep the stay owner"
        );
        assert_eq!(owner_refs[0]["uid"], stay_uid);
    }
}

#[tokio::test]
async fn foreground_gc_requests_independent_pod_deletes_concurrently() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();

    let owner_uid = "owner-rc-concurrent-delete-uid";
    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "owner-rc-concurrent-delete",
        json!({
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "metadata": {
                "name": "owner-rc-concurrent-delete",
                "namespace": "default",
                "uid": owner_uid,
                "deletionTimestamp": "2026-06-07T10:00:00Z",
                "finalizers": ["foregroundDeletion"]
            }
        }),
    )
    .await
    .unwrap();

    for i in 0..4 {
        let name = format!("unique-pod-{i}");
        let uid = format!("unique-pod-{i}-uid");
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            &name,
            json!({
                "metadata": {
                    "name": name,
                    "namespace": "default",
                    "uid": uid,
                    "ownerReferences": [{
                        "apiVersion":"v1",
                        "kind":"ReplicationController",
                        "name":"owner-rc-concurrent-delete",
                        "uid": owner_uid,
                        "blockOwnerDeletion": true
                    }]
                }
            }),
        )
        .await
        .unwrap();
    }

    let sink = ConcurrentBlockingGcPodDeleteSink::new(2);
    let ready = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        check_foreground_deletion_ready(
            &db,
            owner_uid,
            "v1",
            "owner-rc-concurrent-delete",
            "ReplicationController",
            Some("default".to_string()),
            sink.as_ref(),
            &non_pod_finalization(&db),
        ),
    )
    .await
    .expect("foreground GC must issue independent Pod delete requests concurrently")
    .unwrap();

    assert!(
        !ready,
        "foreground owner must still wait for actor-owned Pod deletion after delete requests are issued"
    );
    assert!(
        sink.max_in_flight() >= 2,
        "expected at least two concurrent Pod delete requests, got {}",
        sink.max_in_flight()
    );
}

#[tokio::test]
async fn foreground_gc_skips_duplicate_pod_delete_requests_across_retries() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();

    let owner_uid = "owner-rc-no-dup-delete-uid";
    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "owner-rc-no-dup-delete",
        json!({
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "metadata": {
                "name": "owner-rc-no-dup-delete",
                "namespace": "default",
                "uid": owner_uid,
                "deletionTimestamp": "2026-06-07T10:00:00Z",
                "finalizers": ["foregroundDeletion"]
            }
        }),
    )
    .await
    .unwrap();

    for i in 0..3 {
        let name = format!("fg-no-dup-pod-{i}");
        let uid = format!("fg-no-dup-pod-{i}-uid");
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            &name,
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": name,
                    "namespace": "default",
                    "uid": uid,
                    "ownerReferences": [{
                        "apiVersion": "v1",
                        "kind": "ReplicationController",
                        "name": "owner-rc-no-dup-delete",
                        "uid": owner_uid,
                        "blockOwnerDeletion": true
                    }]
                }
            }),
        )
        .await
        .unwrap();
    }

    let sink = RecordingGcPodDeleteSink::new();

    let ready = check_foreground_deletion_ready(
        &db,
        owner_uid,
        "v1",
        "owner-rc-no-dup-delete",
        "ReplicationController",
        Some("default".to_string()),
        &sink,
        &non_pod_finalization(&db),
    )
    .await
    .unwrap();
    assert!(
        !ready,
        "owner must wait while Pod dependents still have no deletion timestamp"
    );

    // No progress is made by sink alone in this test; a second retry must not
    // send duplicate delete requests for the same Pod UID.
    let first_attempt = sink.recorded_requests().len();
    assert_eq!(
        first_attempt, 3,
        "first GC pass should request all Pod children"
    );

    let _ = check_foreground_deletion_ready(
        &db,
        owner_uid,
        "v1",
        "owner-rc-no-dup-delete",
        "ReplicationController",
        Some("default".to_string()),
        &sink,
        &non_pod_finalization(&db),
    )
    .await
    .unwrap();

    let second_attempt = sink.recorded_requests().len();
    assert_eq!(
        second_attempt, first_attempt,
        "GC should not duplicate delete requests for unchanged non-terminated Pods"
    );
}

async fn assert_foreground_delete_reservation_released_after(
    first_result: FirstGcPodDeleteResult,
    suffix: &str,
) {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let owner_uid = format!("foreground-release-owner-{suffix}");
    let owner_name = format!("foreground-release-{suffix}");
    let child_uid = format!("foreground-release-child-{suffix}");
    let child_name = format!("foreground-release-pod-{suffix}");
    db.create_resource(
        "apps/v1",
        "Deployment",
        Some("default"),
        &owner_name,
        json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": owner_name,
                "namespace": "default",
                "uid": owner_uid,
                "deletionTimestamp": "2026-06-07T10:00:00Z",
                "finalizers": ["foregroundDeletion"]
            }
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        &child_name,
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": child_name,
                "namespace": "default",
                "uid": child_uid,
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "name": owner_name,
                    "uid": owner_uid,
                    "blockOwnerDeletion": true
                }]
            }
        }),
    )
    .await
    .unwrap();

    let coordination = klights_controllers::ControllerCoordination::new();
    let sink = FailOrLoseFirstGcPodDeleteSink::new(first_result);
    let store = crate::bootstrap::controller_adapters::controller_runtime_adapter::controller_store_for_test(&db);
    for _ in 0..2 {
        let ready = klights_controllers::gc::check_foreground_deletion_ready(
            &store,
            &owner_uid,
            "apps/v1",
            &owner_name,
            "Deployment",
            Some("default".to_string()),
            &sink,
            &non_pod_finalization(&db),
            &coordination,
        )
        .await
        .unwrap();
        assert!(!ready);
    }
    assert_eq!(
        sink.attempts.load(Ordering::SeqCst),
        2,
        "a failed or terminally Gone request must release its reservation for retry"
    );
}

#[tokio::test]
async fn foreground_gc_releases_failed_and_gone_delete_reservations() {
    assert_foreground_delete_reservation_released_after(
        FirstGcPodDeleteResult::Unavailable,
        "failed",
    )
    .await;
    assert_foreground_delete_reservation_released_after(FirstGcPodDeleteResult::Gone, "gone").await;
}

#[tokio::test]
async fn test_cascade_delete_circular_empty_uid_ownerrefs() {
    // Replicates the K8s GC conformance test: 3 ConfigMaps in a circular
    // ownerRef chain where all ownerRef.uid fields are "". Deleting cm1
    // should cascade-delete cm2 and cm3 by name+kind lookup.
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();

    db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm1",
            json!({
                "metadata": {
                    "name": "cm1", "namespace": "default", "uid": "cm1-uid",
                    "ownerReferences": [{"apiVersion":"v1","kind":"ConfigMap","name":"cm3","uid":"","controller":true}]
                }
            }),
        )
        .await
        .unwrap();

    db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm2",
            json!({
                "metadata": {
                    "name": "cm2", "namespace": "default", "uid": "cm2-uid",
                    "ownerReferences": [{"apiVersion":"v1","kind":"ConfigMap","name":"cm1","uid":"","controller":true}]
                }
            }),
        )
        .await
        .unwrap();

    db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm3",
            json!({
                "metadata": {
                    "name": "cm3", "namespace": "default", "uid": "cm3-uid",
                    "ownerReferences": [{"apiVersion":"v1","kind":"ConfigMap","name":"cm2","uid":"","controller":true}]
                }
            }),
        )
        .await
        .unwrap();

    // Delete cm1 and cascade
    db.delete_resource("v1", "ConfigMap", Some("default"), "cm1")
        .await
        .unwrap();
    let sink = NoOpGcPodDeleteSink;
    cascade_delete_with_uid(
        &db,
        "cm1-uid",
        "v1",
        "cm1",
        "ConfigMap",
        Some("default".to_string()),
        &sink,
        &non_pod_finalization(&db),
    )
    .await
    .unwrap();

    // cm2 and cm3 should be deleted via empty-UID name+kind cascade
    let cm2 = db
        .get_resource("v1", "ConfigMap", Some("default"), "cm2")
        .await
        .unwrap();
    assert!(
        cm2.is_none(),
        "cm2 should be cascade-deleted via empty-UID ownerRef"
    );

    let cm3 = db
        .get_resource("v1", "ConfigMap", Some("default"), "cm3")
        .await
        .unwrap();
    assert!(
        cm3.is_none(),
        "cm3 should be cascade-deleted via empty-UID ownerRef chain"
    );
}

#[tokio::test]
async fn cascade_delete_pod_owner_cycle_marks_each_dependent_once() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let sink = RecordingGcPodDeleteSink::new();

    for (name, uid, owner_name, owner_uid) in [
        ("pod1", "pod-1-uid", "pod3", "pod-3-uid"),
        ("pod2", "pod-2-uid", "pod1", "pod-1-uid"),
        ("pod3", "pod-3-uid", "pod2", "pod-2-uid"),
    ] {
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            name,
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": name,
                    "namespace": "default",
                    "uid": uid,
                    "ownerReferences": [{
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "name": owner_name,
                        "uid": owner_uid,
                        "controller": true
                    }]
                },
                "spec": {"containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]},
                "status": {"phase": "Running"}
            }),
        )
        .await
        .unwrap();
    }

    cascade_delete_with_uid(
        &db,
        "pod-1-uid",
        "v1",
        "pod1",
        "Pod",
        Some("default".to_string()),
        &sink,
        &non_pod_finalization(&db),
    )
    .await
    .unwrap();

    let requests = sink.recorded_requests();
    assert!(
        sink.has_request("default", "pod2", "pod-2-uid"),
        "GC must actor-delete pod2, the direct dependent: {requests:?}"
    );
    assert!(
        sink.has_request("default", "pod3", "pod-3-uid"),
        "GC must continue through actor-deleted Pod owners to pod3: {requests:?}"
    );
    assert!(
        requests.len() <= 3,
        "GC must stop when the Pod owner graph cycles back to an ancestor: {requests:?}"
    );
}

#[tokio::test]
async fn test_cascade_delete_matches_owner_uid_in_nonzero_ownerref_position() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();

    db.create_resource(
        "apps/v1",
        "Deployment",
        Some("default"),
        "owner-deploy",
        json!({
            "metadata": {
                "name": "owner-deploy",
                "namespace": "default",
                "uid": "owner-deploy-uid"
            }
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "child-rs",
        json!({
            "metadata": {
                "name": "child-rs",
                "namespace": "default",
                "uid": "child-rs-uid",
                "ownerReferences": [
                    {"apiVersion":"v1","kind":"ConfigMap","name":"other-owner","uid":"other-owner-uid"},
                    {"apiVersion":"apps/v1","kind":"Deployment","name":"owner-deploy","uid":"owner-deploy-uid","controller":true}
                ]
            }
        }),
    )
    .await
    .unwrap();

    db.delete_resource("apps/v1", "Deployment", Some("default"), "owner-deploy")
        .await
        .unwrap();

    let sink = NoOpGcPodDeleteSink;
    cascade_delete_with_uid(
        &db,
        "owner-deploy-uid",
        "apps/v1",
        "owner-deploy",
        "Deployment",
        Some("default".to_string()),
        &sink,
        &non_pod_finalization(&db),
    )
    .await
    .unwrap();

    let child = db
        .get_resource("apps/v1", "ReplicaSet", Some("default"), "child-rs")
        .await
        .unwrap();
    assert!(
        child.is_none(),
        "ReplicaSet must be cascade-deleted even when ownerRef is not index 0"
    );
}

// ── GC Pod-actor-delete regression tests ──

#[tokio::test]
async fn gc_cascade_pod_child_uses_actor_delete_sink() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let sink = RecordingGcPodDeleteSink::new();

    // Create a Deployment owner
    db.create_resource(
        "apps/v1",
        "Deployment",
        Some("default"),
        "owner-deploy",
        json!({"metadata": {"name": "owner-deploy", "namespace": "default", "uid": "owner-uid-1"}}),
    )
    .await
    .unwrap();

    // Create a Pod child
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "child-pod",
        json!({
            "metadata": {
                "name": "child-pod", "namespace": "default", "uid": "child-pod-uid",
                "ownerReferences": [{"uid": "owner-uid-1", "kind": "Deployment", "apiVersion": "apps/v1"}]
            }
        }),
    )
    .await
    .unwrap();

    // Create a non-Pod sibling child (ConfigMap)
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "child-cm",
        json!({
            "metadata": {
                "name": "child-cm", "namespace": "default", "uid": "child-cm-uid",
                "ownerReferences": [{"uid": "owner-uid-1", "kind": "Deployment", "apiVersion": "apps/v1"}]
            }
        }),
    )
    .await
    .unwrap();

    // Cascade delete from the Deployment owner
    cascade_delete_with_uid(
        &db,
        "owner-uid-1",
        "apps/v1",
        "owner-deploy",
        "Deployment",
        Some("default".to_string()),
        &sink,
        &non_pod_finalization(&db),
    )
    .await
    .unwrap();

    // The Pod child should still exist (not hard-deleted)
    let pod = db
        .get_resource("v1", "Pod", Some("default"), "child-pod")
        .await
        .unwrap();
    assert!(
        pod.is_some(),
        "Pod child must NOT be hard-deleted by GC — it must go through the actor"
    );

    // The recording sink should have received exactly one UID-bound delete request
    assert!(
        sink.has_request("default", "child-pod", "child-pod-uid"),
        "GC must route Pod delete through GcPodDeleteSink with correct UID"
    );
    assert_eq!(
        sink.recorded_requests().len(),
        1,
        "Only the Pod child should be sent to the sink"
    );

    // The non-Pod sibling should still be hard-deleted
    let cm = db
        .get_resource("v1", "ConfigMap", Some("default"), "child-cm")
        .await
        .unwrap();
    assert!(
        cm.is_none(),
        "Non-Pod child must still be hard-deleted by GC"
    );
}

#[tokio::test]
async fn gc_foreground_pod_child_uses_actor_delete_sink() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let sink = RecordingGcPodDeleteSink::new();

    // Create a foreground-deleting owner (Deployment with foregroundDeletion finalizer)
    db.create_resource(
        "apps/v1",
        "Deployment",
        Some("default"),
        "fg-owner",
        json!({
            "metadata": {
                "name": "fg-owner", "namespace": "default", "uid": "fg-owner-uid",
                "deletionTimestamp": "2026-05-01T00:00:00Z",
                "finalizers": ["foregroundDeletion"]
            }
        }),
    )
    .await
    .unwrap();

    // Create a Pod child owned by the foreground-deleting owner
    // (sole owner — no other live owner)
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "fg-child-pod",
        json!({
            "metadata": {
                "name": "fg-child-pod", "namespace": "default", "uid": "fg-child-pod-uid",
                "ownerReferences": [{
                    "uid": "fg-owner-uid",
                    "kind": "Deployment",
                    "apiVersion": "apps/v1",
                    "blockOwnerDeletion": true
                }]
            }
        }),
    )
    .await
    .unwrap();

    // Check foreground deletion readiness
    let ready = check_foreground_deletion_ready(
        &db,
        "fg-owner-uid",
        "apps/v1",
        "fg-owner",
        "Deployment",
        Some("default".to_string()),
        &sink,
        &non_pod_finalization(&db),
    )
    .await
    .unwrap();

    // Foreground deletion should return false — children still need processing
    assert!(!ready, "Should not be ready while children remain");

    // Pod should still exist (not hard-deleted)
    let pod = db
        .get_resource("v1", "Pod", Some("default"), "fg-child-pod")
        .await
        .unwrap();
    assert!(
        pod.is_some(),
        "Pod child must NOT be hard-deleted — it goes through the actor"
    );

    // Sink should have received the delete request
    assert!(
        sink.has_request("default", "fg-child-pod", "fg-child-pod-uid"),
        "Foreground GC must route Pod delete through GcPodDeleteSink"
    );
}

#[tokio::test]
async fn gc_foreground_pod_child_already_terminating_is_not_redeleted() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let sink = RecordingGcPodDeleteSink::new();

    db.create_resource(
        "apps/v1",
        "Deployment",
        Some("default"),
        "fg-owner",
        json!({
            "metadata": {
                "name": "fg-owner",
                "namespace": "default",
                "uid": "fg-owner-uid",
                "deletionTimestamp": "2026-05-01T00:00:00Z",
                "finalizers": ["foregroundDeletion"]
            }
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "fg-child-pod",
        json!({
            "metadata": {
                "name": "fg-child-pod",
                "namespace": "default",
                "uid": "fg-child-pod-uid",
                "deletionTimestamp": "2026-05-01T00:00:01Z",
                "ownerReferences": [{
                    "uid": "fg-owner-uid",
                    "kind": "Deployment",
                    "apiVersion": "apps/v1",
                    "blockOwnerDeletion": true
                }]
            }
        }),
    )
    .await
    .unwrap();

    let ready = check_foreground_deletion_ready(
        &db,
        "fg-owner-uid",
        "apps/v1",
        "fg-owner",
        "Deployment",
        Some("default".to_string()),
        &sink,
        &non_pod_finalization(&db),
    )
    .await
    .unwrap();

    assert!(
        !ready,
        "foreground owner must wait while the terminating Pod row still exists"
    );
    assert!(
        sink.recorded_requests().is_empty(),
        "foreground GC must not re-delete an already terminating Pod"
    );
}

#[tokio::test]
async fn gc_foreground_pod_child_already_terminating_allows_owner_finalization_after_cleanup() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let sink = RecordingGcPodDeleteSink::new();

    let owner = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "fg-owner",
            json!({
                "metadata": {
                    "name": "fg-owner",
                    "namespace": "default",
                    "uid": "fg-owner-uid",
                    "deletionTimestamp": "2026-05-01T00:00:00Z",
                    "finalizers": ["foregroundDeletion"]
                }
            }),
        )
        .await
        .unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "fg-child-pod",
        json!({
            "metadata": {
                "name": "fg-child-pod",
                "namespace": "default",
                "uid": "fg-child-pod-uid",
                "deletionTimestamp": "2026-05-01T00:00:01Z",
                "ownerReferences": [{
                    "uid": "fg-owner-uid",
                    "kind": "Deployment",
                    "apiVersion": "apps/v1",
                    "blockOwnerDeletion": true
                }]
            }
        }),
    )
    .await
    .unwrap();

    let ready = check_foreground_deletion_ready(
        &db,
        "fg-owner-uid",
        "apps/v1",
        "fg-owner",
        "Deployment",
        Some("default".to_string()),
        &sink,
        &non_pod_finalization(&db),
    )
    .await
    .unwrap();
    assert!(
        !ready,
        "Owner must stay blocked while terminating Pod still exists"
    );

    db.delete_resource("v1", "Pod", Some("default"), "fg-child-pod")
        .await
        .unwrap();

    let ready_after_cleanup =
        finalize_foreground_owner_if_ready(&db, &owner, &sink, &non_pod_finalization(&db))
            .await
            .unwrap();
    assert!(
        ready_after_cleanup,
        "Owner should become ready once terminating child is gone"
    );
    assert!(
        db.get_resource("apps/v1", "Deployment", Some("default"), "fg-owner")
            .await
            .unwrap()
            .is_none(),
        "Owner should be deleted after foreground deletion is finalized"
    );
}

#[tokio::test]
async fn gc_reconcile_dangling_pod_owner_uses_actor_delete_sink() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let sink = RecordingGcPodDeleteSink::new();

    // Create a Pod with only a dangling owner reference
    let pod = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "orphan-pod",
            json!({
                "metadata": {
                    "name": "orphan-pod", "namespace": "default", "uid": "orphan-pod-uid",
                    "ownerReferences": [{
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "name": "missing-deploy",
                        "uid": "missing-deploy-uid"
                    }]
                }
            }),
        )
        .await
        .unwrap();

    let outcome = reconcile_owner_references(&db, pod, &sink, &non_pod_finalization(&db))
        .await
        .unwrap();
    assert_eq!(outcome, OwnerReferenceReconcile::Deleted);

    // Pod should still exist (not hard-deleted)
    let pod = db
        .get_resource("v1", "Pod", Some("default"), "orphan-pod")
        .await
        .unwrap();
    assert!(
        pod.is_some(),
        "Pod with dangling owners must NOT be hard-deleted by reconcile_owner_references"
    );

    // Sink should have received the delete request
    assert!(
        sink.has_request("default", "orphan-pod", "orphan-pod-uid"),
        "reconcile_owner_references must route Pod delete through GcPodDeleteSink"
    );
}

#[tokio::test]
async fn gc_pod_delete_is_uid_guarded_against_same_name_replacement() {
    // This test verifies the UID-precondition design: when GC tries to delete
    // a Pod by a stale UID, and a replacement Pod with the same name but
    // different UID exists, the GC request must NOT affect the replacement.
    // The sink records the UID, and production code uses DeleteOptions with
    // UID preconditions to reject stale requests.
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();

    // Create a "stale" Pod identity with old UID
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "same-name-pod",
        json!({
            "metadata": {
                "name": "same-name-pod", "namespace": "default", "uid": "old-uid",
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "name": "missing-deploy",
                    "uid": "missing-deploy-uid"
                }]
            }
        }),
    )
    .await
    .unwrap();

    // Delete and recreate with a new UID (simulating same-name replacement)
    db.delete_resource_with_preconditions(
        "v1",
        "Pod",
        Some("default"),
        "same-name-pod",
        crate::datastore::ResourcePreconditions::uid("old-uid"),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "same-name-pod",
        json!({
            "metadata": {
                "name": "same-name-pod", "namespace": "default", "uid": "new-uid"
            }
        }),
    )
    .await
    .unwrap();

    // Now try to GC-delete the old UID Pod via reconcile_owner_references
    // (using a resource snapshot that has the old UID)
    let sink = RecordingGcPodDeleteSink::new();
    let stale_resource = crate::datastore::Resource {
        id: 0,
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "same-name-pod".to_string(),
        uid: "old-uid".to_string(),
        data: std::sync::Arc::new(json!({
            "metadata": {
                "name": "same-name-pod", "namespace": "default", "uid": "old-uid",
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "name": "missing-deploy",
                    "uid": "missing-deploy-uid"
                }]
            }
        })),
        resource_version: 1,
    };

    let outcome =
        reconcile_owner_references(&db, stale_resource, &sink, &non_pod_finalization(&db))
            .await
            .unwrap();
    assert_eq!(outcome, OwnerReferenceReconcile::Deleted);

    // The sink should have received a request with the OLD UID
    assert!(
        sink.has_request("default", "same-name-pod", "old-uid"),
        "GC must issue delete request with the stale UID"
    );

    // The new-uid Pod must still exist and not be deleted
    let live = db
        .get_resource("v1", "Pod", Some("default"), "same-name-pod")
        .await
        .unwrap();
    assert!(
        live.is_some(),
        "Replacement Pod with new UID must survive stale GC observation"
    );
    assert_eq!(
        live.unwrap().uid,
        "new-uid",
        "Live Pod must have the new UID"
    );
}

#[tokio::test]
async fn gc_cascade_non_pod_child_still_hard_deletes() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let sink = NoOpGcPodDeleteSink;

    // Create a Deployment owner
    db.create_resource(
        "apps/v1",
        "Deployment",
        Some("default"),
        "deploy-owner",
        json!({"metadata": {"name": "deploy-owner", "namespace": "default", "uid": "deploy-uid-2"}}),
    )
    .await
    .unwrap();

    // Create a non-Pod child (ReplicaSet)
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "rs-child",
        json!({
            "metadata": {
                "name": "rs-child", "namespace": "default", "uid": "rs-child-uid",
                "ownerReferences": [{"uid": "deploy-uid-2", "kind": "Deployment", "apiVersion": "apps/v1"}]
            }
        }),
    )
    .await
    .unwrap();

    // Cascade delete from the Deployment owner
    cascade_delete_with_uid(
        &db,
        "deploy-uid-2",
        "apps/v1",
        "deploy-owner",
        "Deployment",
        Some("default".to_string()),
        &sink,
        &non_pod_finalization(&db),
    )
    .await
    .unwrap();

    // The non-Pod child should be hard-deleted
    let rs = db
        .get_resource("apps/v1", "ReplicaSet", Some("default"), "rs-child")
        .await
        .unwrap();
    assert!(
        rs.is_none(),
        "Non-Pod children must still be hard-deleted by GC cascade"
    );
}

// ---------------------------------------------------------------------------
// bug-grpc Pillar C — durable owner-cascade sweep.
// ---------------------------------------------------------------------------

fn rc_child_pod(ns: &str, name: &str, uid: &str, rc_uid: &str) -> serde_json::Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": ns,
            "uid": uid,
            "ownerReferences": [{
                "apiVersion": "v1",
                "kind": "ReplicationController",
                "name": "rc",
                "uid": rc_uid,
                "controller": true,
            }],
        },
        "spec": {"nodeName": "node-a", "containers": [{"name": "c", "image": "x"}]},
        "status": {"phase": "Running"},
    })
}

#[tokio::test]
async fn owner_cascade_sweep_marks_all_children_and_self_extinguishes() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    db.create_namespace("ed", json!({"metadata": {"name": "ed"}}))
        .await
        .unwrap();
    let rc_uid = "rc-sweep-uid";
    db.create_resource(
        "v1",
        "ReplicationController",
        Some("ed"),
        "rc",
        json!({
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "metadata": {"name": "rc", "namespace": "ed", "uid": rc_uid},
            "spec": {"replicas": 3},
        }),
    )
    .await
    .unwrap();
    for i in 0..3 {
        let name = format!("rc-pod-{i}");
        let uid = format!("pod-uid-{i}");
        db.create_resource(
            "v1",
            "Pod",
            Some("ed"),
            &name,
            rc_child_pod("ed", &name, &uid, rc_uid),
        )
        .await
        .unwrap();
    }

    // The real PodRepository sink marks terminating + enqueues per child.
    let repo = crate::kubelet::pod_repository::pod_repository_for_test(&db);
    let needs_more = owner_cascade_sweep_once(
        &db,
        rc_uid,
        "v1",
        "rc",
        "ReplicationController",
        Some("ed".to_string()),
        repo.as_ref() as &dyn GcPodDeleteSink,
        &non_pod_finalization(&db),
    )
    .await
    .unwrap();

    assert!(
        !needs_more,
        "all children marked terminating -> sweep must self-extinguish"
    );
    for i in 0..3 {
        let name = format!("rc-pod-{i}");
        let pod = db
            .get_resource("v1", "Pod", Some("ed"), &name)
            .await
            .unwrap()
            .unwrap();
        assert!(
            pod.data
                .pointer("/metadata/deletionTimestamp")
                .and_then(|v| v.as_str())
                .is_some_and(|s| !s.trim().is_empty()),
            "child {name} must be marked terminating by the sweep"
        );
    }
}

#[tokio::test]
async fn rc_background_delete_drives_all_running_children_to_finalization() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    db.create_namespace("ed-finalize", json!({"metadata": {"name": "ed-finalize"}}))
        .await
        .unwrap();
    let rc_uid = "rc-finalize-uid";
    db.create_resource(
        "v1",
        "ReplicationController",
        Some("ed-finalize"),
        "rc",
        json!({
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "metadata": {"name": "rc", "namespace": "ed-finalize", "uid": rc_uid},
            "spec": {"replicas": 5},
        }),
    )
    .await
    .unwrap();
    for i in 0..5 {
        let name = format!("rc-pod-{i}");
        let uid = format!("pod-uid-{i}");
        db.create_resource(
            "v1",
            "Pod",
            Some("ed-finalize"),
            &name,
            rc_child_pod("ed-finalize", &name, &uid, rc_uid),
        )
        .await
        .unwrap();
    }

    let (repo, node_local) =
        crate::kubelet::pod_repository::pod_repository_with_node_local_for_test(&db).await;
    let needs_more = owner_cascade_sweep_once(
        &db,
        rc_uid,
        "v1",
        "rc",
        "ReplicationController",
        Some("ed-finalize".to_string()),
        repo.as_ref() as &dyn GcPodDeleteSink,
        &non_pod_finalization(&db),
    )
    .await
    .unwrap();
    assert!(
        !needs_more,
        "all existing children should be marked in one sweep"
    );

    let mut queued = std::collections::HashSet::new();
    for _ in 0..10 {
        let Some(row) = node_local.claim_workqueue_due(i64::MAX).await.unwrap() else {
            break;
        };
        if row.namespace == "ed-finalize" {
            queued.insert(row.uid);
        }
    }
    for i in 0..5 {
        let name = format!("rc-pod-{i}");
        let uid = format!("pod-uid-{i}");
        assert!(
            queued.contains(&uid),
            "RC cascade must enqueue UID-bound work for child {uid}"
        );
        repo.finalize_pod_deletion_after_actor_cleanup("ed-finalize", &name, &uid)
            .await
            .unwrap();
        assert!(
            db.get_resource("v1", "Pod", Some("ed-finalize"), &name)
                .await
                .unwrap()
                .is_none(),
            "child {name} should be removed by actor-owned UID finalization"
        );
    }
}

#[tokio::test]
async fn owner_cascade_sweep_signals_reschedule_when_child_not_yet_terminating() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    db.create_namespace("ed", json!({"metadata": {"name": "ed"}}))
        .await
        .unwrap();
    let rc_uid = "rc-late-uid";
    db.create_resource(
        "v1",
        "ReplicationController",
        Some("ed"),
        "rc",
        json!({
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "metadata": {"name": "rc", "namespace": "ed", "uid": rc_uid},
            "spec": {"replicas": 1},
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("ed"),
        "rc-pod-0",
        rc_child_pod("ed", "rc-pod-0", "pod-uid-0", rc_uid),
    )
    .await
    .unwrap();

    // A recording sink that does NOT mark the row simulates a child whose mark
    // has not yet landed (the cascade-vs-create race window): the re-enumeration
    // still sees a non-terminating owned child, so the sweep must signal a
    // reschedule rather than declaring itself done.
    let sink = RecordingGcPodDeleteSink::new();
    let needs_more = owner_cascade_sweep_once(
        &db,
        rc_uid,
        "v1",
        "rc",
        "ReplicationController",
        Some("ed".to_string()),
        &sink,
        &non_pod_finalization(&db),
    )
    .await
    .unwrap();

    assert!(
        needs_more,
        "a still-non-terminating owned child must signal another sweep"
    );
    assert!(
        sink.has_request("ed", "rc-pod-0", "pod-uid-0"),
        "the sweep must have routed the child through the delete sink"
    );
}

#[tokio::test]
async fn burst_delete_of_many_rcs_leaves_no_orphan_pods() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    db.create_namespace("ed-burst", json!({"metadata": {"name": "ed-burst"}}))
        .await
        .unwrap();
    let repo = crate::kubelet::pod_repository::pod_repository_for_test(&db);

    for rc in 0..3 {
        let rc_name = format!("rc-{rc}");
        let rc_uid = format!("rc-burst-uid-{rc}");
        db.create_resource(
            "v1",
            "ReplicationController",
            Some("ed-burst"),
            &rc_name,
            json!({
                "apiVersion": "v1",
                "kind": "ReplicationController",
                "metadata": {"name": rc_name, "namespace": "ed-burst", "uid": rc_uid},
                "spec": {"replicas": 5},
            }),
        )
        .await
        .unwrap();
        for pod in 0..5 {
            let name = format!("rc-{rc}-pod-{pod}");
            let uid = format!("pod-burst-uid-{rc}-{pod}");
            db.create_resource(
                "v1",
                "Pod",
                Some("ed-burst"),
                &name,
                rc_child_pod("ed-burst", &name, &uid, &rc_uid),
            )
            .await
            .unwrap();
        }
    }

    for rc in 0..3 {
        let rc_name = format!("rc-{rc}");
        let rc_uid = format!("rc-burst-uid-{rc}");
        let needs_more = owner_cascade_sweep_once(
            &db,
            &rc_uid,
            "v1",
            &rc_name,
            "ReplicationController",
            Some("ed-burst".to_string()),
            repo.as_ref() as &dyn GcPodDeleteSink,
            &non_pod_finalization(&db),
        )
        .await
        .unwrap();
        assert!(
            !needs_more,
            "RC {rc_name} should have no unmarked children after sweep"
        );
    }

    for rc in 0..3 {
        for pod in 0..5 {
            let name = format!("rc-{rc}-pod-{pod}");
            let uid = format!("pod-burst-uid-{rc}-{pod}");
            repo.finalize_pod_deletion_after_actor_cleanup("ed-burst", &name, &uid)
                .await
                .unwrap();
        }
    }
    let remaining = db
        .list_resources(
            "v1",
            "Pod",
            Some("ed-burst"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert!(
        remaining.items.is_empty(),
        "burst RC background delete should leave no orphan Pod rows"
    );
}
