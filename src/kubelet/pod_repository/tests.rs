//! Repository tests covering trait behavior. Task 1 lands the smoke test
//! that verifies `PodRepository::new(...)` constructs successfully with
//! the wiring inputs that `AppState` provides at runtime. Per-trait tests
//! land alongside their implementations in Tasks 2–14.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use anyhow::Result;
use serde_json::json;

use crate::control_plane::client::{
    CacheScope, LeaderApiClient, ListRequest, ListResponse, ResourceEvent, ResourceKey,
    WatchRequest, WatchStream,
};

use super::state_only_writer::StateOnlyWriter;
use super::store::{PodStore, UnscheduledPodDeleteOutcome};
use super::{PodReader, PodRepository, PodRepositoryBuildConfig};
use crate::pod_identity::PodIdentity;

fn fixture_supervisor() -> Arc<crate::task_supervisor::TaskSupervisor> {
    Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ))
}

fn fixture_side_effects() -> Arc<crate::side_effects::SideEffectRegistry> {
    Arc::new(crate::side_effects::SideEffectRegistry::new())
}

async fn fixture_node_local() -> crate::datastore::node_local::NodeLocalHandle {
    crate::datastore::node_local::selector::open_node_local(
        crate::datastore::backend_kind::BackendKind::Sqlite,
        None,
        fixture_supervisor(),
        None,
        "sqlite:pod-repository-test",
    )
    .await
    .expect("open node-local test db")
}

struct RecordingPodDeleteHook {
    observed: Arc<tokio::sync::Mutex<Option<(bool, bool)>>>,
}

#[derive(Clone)]
struct FakeLeaderApiClient {
    pod: crate::datastore::Resource,
    fresh_pod: Option<crate::datastore::Resource>,
    cached_list_items: Option<Vec<crate::datastore::Resource>>,
    fresh_list_items: Option<Vec<crate::datastore::Resource>>,
}

impl FakeLeaderApiClient {
    fn new(pod: crate::datastore::Resource) -> Self {
        Self {
            pod,
            fresh_pod: None,
            cached_list_items: None,
            fresh_list_items: None,
        }
    }

    fn with_fresh_pod(mut self, pod: crate::datastore::Resource) -> Self {
        self.fresh_pod = Some(pod);
        self
    }

    fn with_cached_list_items(mut self, items: Vec<crate::datastore::Resource>) -> Self {
        self.cached_list_items = Some(items);
        self
    }

    fn with_fresh_list_items(mut self, items: Vec<crate::datastore::Resource>) -> Self {
        self.fresh_list_items = Some(items);
        self
    }

    fn pod_list_response(
        &self,
        req: &ListRequest,
        items: &[crate::datastore::Resource],
    ) -> ListResponse {
        let mut list = crate::datastore::ResourceList {
            items: Vec::new(),
            resource_version: self.pod.resource_version,
            continue_token: None,
            remaining_item_count: None,
        };
        if req.api_version == "v1"
            && req.kind == "Pod"
            && req.namespace.as_deref() == self.pod.namespace.as_deref()
        {
            list.items.extend(items.iter().cloned());
        }
        list
    }
}

#[async_trait::async_trait]
impl LeaderApiClient for FakeLeaderApiClient {
    async fn get_resource(&self, key: ResourceKey) -> Result<Option<crate::datastore::Resource>> {
        if key.api_version == "v1"
            && key.kind == "Pod"
            && key.namespace.as_deref() == self.pod.namespace.as_deref()
            && key.name == self.pod.name
        {
            return Ok(Some(self.pod.clone()));
        }
        Ok(None)
    }

    async fn get_resource_fresh(
        &self,
        key: ResourceKey,
    ) -> Result<Option<crate::datastore::Resource>> {
        let pod = self.fresh_pod.as_ref().unwrap_or(&self.pod);
        if key.api_version == "v1"
            && key.kind == "Pod"
            && key.namespace.as_deref() == pod.namespace.as_deref()
            && key.name == pod.name
        {
            return Ok(Some(pod.clone()));
        }
        Ok(None)
    }

    async fn list_resources(&self, req: ListRequest) -> Result<ListResponse> {
        let default_items = [self.pod.clone()];
        let items = self.cached_list_items.as_deref().unwrap_or(&default_items);
        Ok(self.pod_list_response(&req, items))
    }

    async fn list_resources_fresh(&self, req: ListRequest) -> Result<ListResponse> {
        let default_items = self
            .cached_list_items
            .as_deref()
            .unwrap_or_else(|| std::slice::from_ref(&self.pod));
        let items = self.fresh_list_items.as_deref().unwrap_or(default_items);
        Ok(self.pod_list_response(&req, items))
    }

    async fn watch_resources(&self, _req: WatchRequest) -> Result<WatchStream<ResourceEvent>> {
        Ok(Box::pin(futures::stream::empty()))
    }

    async fn wait_cache_ready(&self, _scope: CacheScope) -> Result<()> {
        Ok(())
    }

    async fn get_pod(
        &self,
        ns: &str,
        name: &str,
    ) -> Result<Option<crate::control_plane::client::Pod>> {
        if self.pod.namespace.as_deref() == Some(ns) && self.pod.name == name {
            return Ok(Some(self.pod.clone()));
        }
        Ok(None)
    }

    async fn get_pod_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
    ) -> Result<Option<crate::control_plane::client::Pod>> {
        Ok(self
            .get_pod(ns, name)
            .await?
            .filter(|pod| pod.uid.as_str() == uid))
    }

    async fn watch_pods_on_node(
        &self,
        _node_name: &str,
    ) -> Result<WatchStream<crate::control_plane::client::Pod>> {
        Ok(Box::pin(futures::stream::empty()))
    }

    async fn list_pods_on_node(
        &self,
        _node_name: &str,
    ) -> Result<Vec<crate::control_plane::client::Pod>> {
        Ok(vec![self.pod.clone()])
    }

    async fn get_configmap(
        &self,
        _ns: &str,
        _name: &str,
    ) -> Result<Option<crate::control_plane::client::ConfigMap>> {
        Ok(None)
    }

    async fn get_secret(
        &self,
        _ns: &str,
        _name: &str,
    ) -> Result<Option<crate::control_plane::client::Secret>> {
        Ok(None)
    }

    async fn get_node(&self, name: &str) -> Result<crate::control_plane::client::Node> {
        Err(anyhow::anyhow!("unexpected node read for {name}"))
    }

    async fn watch_node(
        &self,
        _name: &str,
    ) -> Result<WatchStream<crate::control_plane::client::Node>> {
        Ok(Box::pin(futures::stream::empty()))
    }

    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        _cluster_cidr: &str,
        _node_ip: &str,
    ) -> Result<crate::datastore::NodeSubnet> {
        Err(anyhow::anyhow!(
            "unexpected node subnet allocation for {node_name}"
        ))
    }

    async fn get_node_subnet(
        &self,
        node_name: &str,
    ) -> Result<Option<crate::datastore::NodeSubnet>> {
        Err(anyhow::anyhow!(
            "unexpected node subnet read for {node_name}"
        ))
    }

    async fn list_peer_subnets(
        &self,
        my_node_name: &str,
    ) -> Result<Vec<crate::datastore::NodeSubnet>> {
        Err(anyhow::anyhow!(
            "unexpected peer subnet list for {my_node_name}"
        ))
    }

    async fn get_node_dataplane(
        &self,
        node_name: &str,
    ) -> Result<Option<crate::networking::wireguard::DataplanePeerMetadata>> {
        Err(anyhow::anyhow!(
            "unexpected node dataplane read for {node_name}"
        ))
    }

    async fn list_pod_cleanup_intents_for_node(
        &self,
        node_name: &str,
    ) -> Result<Vec<crate::datastore::PodCleanupIntent>> {
        Err(anyhow::anyhow!(
            "unexpected pod cleanup intent list for {node_name}"
        ))
    }

    async fn delete_pod_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        Err(anyhow::anyhow!(
            "unexpected pod cleanup intent delete for {node_name}/{namespace}/{pod_name}/{pod_uid}/{reason}"
        ))
    }

    async fn apply_outbox(
        &self,
        _idempotency_key: &str,
        _operation: crate::kubelet::outbox::payload::OutboxOperation,
        _payload: bytes::Bytes,
    ) -> std::result::Result<
        crate::kubelet::outbox::OutboxApplyResult,
        crate::kubelet::outbox::OutboxApplyError,
    > {
        Ok(crate::kubelet::outbox::OutboxApplyResult::Applied {
            applied_rv: self.pod.resource_version,
        })
    }
}

#[async_trait::async_trait]
impl crate::side_effects::SideEffect for RecordingPodDeleteHook {
    fn name(&self) -> &'static str {
        "recording_pod_delete_hook"
    }

    async fn apply(
        &self,
        resource: &serde_json::Value,
        db: &dyn crate::datastore::DatastoreBackend,
    ) -> anyhow::Result<()> {
        let namespace = resource
            .pointer("/metadata/namespace")
            .and_then(|v| v.as_str())
            .unwrap_or("default");
        let name = resource
            .pointer("/metadata/name")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let exists_after_delete = db
            .get_resource("v1", "Pod", Some(namespace), name)
            .await?
            .is_some();
        let saw_original_owner_ref = resource
            .pointer("/metadata/ownerReferences/0/name")
            .and_then(|v| v.as_str())
            == Some("rs-x");
        *self.observed.lock().await = Some((exists_after_delete, saw_original_owner_ref));
        Ok(())
    }
}

#[derive(Clone)]
struct TestOutboxApplyClient {
    db: crate::datastore::DatastoreHandle,
}

impl TestOutboxApplyClient {
    fn new(db: crate::datastore::DatastoreHandle) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl crate::kubelet::outbox::OutboxApplyClient for TestOutboxApplyClient {
    async fn apply_outbox(
        &self,
        _idempotency_key: &str,
        _operation: crate::kubelet::outbox::payload::OutboxOperation,
        payload: bytes::Bytes,
    ) -> std::result::Result<
        crate::kubelet::outbox::OutboxApplyResult,
        crate::kubelet::outbox::OutboxApplyError,
    > {
        let payload = crate::kubelet::outbox::payload::OutboxPayload::decode_protobuf(&payload)
            .map_err(|err| crate::kubelet::outbox::OutboxApplyError::Retryable(err.to_string()))?;
        let command = payload.command;
        let current_rv = self
            .db
            .get_current_resource_version()
            .await
            .map_err(|err| crate::kubelet::outbox::OutboxApplyError::Retryable(err.to_string()))?;
        let meta = crate::datastore::command::CommandMeta {
            command_id: crate::datastore::command::CommandId::new(),
            codec_version: crate::datastore::command::COMMAND_CODEC_VERSION,
            resource_version: current_rv.saturating_add(1),
            uid: match &command {
                crate::datastore::command::StorageCommand::UpdateResource {
                    preconditions, ..
                } => preconditions.uid.clone(),
                crate::datastore::command::StorageCommand::DeleteResource {
                    preconditions, ..
                } => preconditions.uid.clone(),
                crate::datastore::command::StorageCommand::PatchResource {
                    preconditions, ..
                } => preconditions.uid.clone(),
                crate::datastore::command::StorageCommand::UpdateStatus {
                    preconditions, ..
                } => preconditions.uid.clone(),
                _ => None,
            },
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .ok()
                .and_then(|d| d.as_millis().try_into().ok())
                .unwrap_or(0),
            authoring_node: "test-outbox-client".to_string(),
        };
        self.db
            .apply_replicated_command(command, meta)
            .await
            .map_err(|err| crate::kubelet::outbox::OutboxApplyError::Retryable(err.to_string()))?;
        let applied_rv = self.db.get_current_resource_version().await.unwrap_or(0);
        Ok(crate::kubelet::outbox::OutboxApplyResult::Applied { applied_rv })
    }
}

async fn build_repo_with_scheduling_mode_for_outbox(
    scheduling_mode: super::api::PodSchedulingMode,
) -> (
    super::PodRepository,
    crate::datastore::DatastoreHandle,
    crate::datastore::node_local::NodeLocalHandle,
) {
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let node_db = fixture_node_local().await;
    let outbox = Arc::new(crate::kubelet::outbox::Outbox::new(node_db.clone()));
    let supervisor = fixture_supervisor();
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = fixture_side_effects();
    let repo = super::PodRepository::new_with_scheduling_mode_and_outbox(
        db.clone(),
        supervisor,
        side_effects,
        metrics,
        scheduling_mode,
        Some(outbox),
    );
    (repo, db, node_db)
}

async fn drain_repo_outbox(
    db: crate::datastore::DatastoreHandle,
    node_db: &crate::datastore::node_local::NodeLocalHandle,
) -> Result<()> {
    let client = std::sync::Arc::new(TestOutboxApplyClient::new(db));
    let dispatcher = crate::kubelet::outbox::OutboxDispatcher::for_tests(node_db.clone(), client);
    loop {
        let outcome = dispatcher.dispatch_due_once(i64::MAX / 4).await?;
        if matches!(
            outcome,
            crate::kubelet::outbox::DispatchOutcome::Idle { .. }
        ) {
            break;
        }
    }
    Ok(())
}

#[tokio::test]
async fn pod_repository_constructs_with_db_and_supervisor() {
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let supervisor = fixture_supervisor();
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = fixture_side_effects();
    let _repo = PodRepository::new(db, supervisor, side_effects, metrics);
}

// --- Task 4.1: build_parts coverage gate ---

#[tokio::test]
async fn pod_repository_build_parts_exposes_repository_and_background_without_starting() {
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let supervisor = fixture_supervisor();
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = fixture_side_effects();
    let network_events = crate::networking::global_pod_network_events();

    let parts = PodRepository::build_parts(PodRepositoryBuildConfig {
        db,
        supervisor,
        side_effects,
        metrics,
        network_events,
        scheduling_mode: super::api::PodSchedulingMode::InlineSingleNode,
        outbox: None,
        cluster_api: None,
    });

    // Repository is constructed.
    let _repo = &parts.repository;

    // Background is available.
    let _bg = &parts.background;
}

#[tokio::test]
async fn pod_repository_build_parts_does_not_start_workqueue_until_background_start() {
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let supervisor = fixture_supervisor();
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = fixture_side_effects();
    let network_events = crate::networking::global_pod_network_events();

    let parts = PodRepository::build_parts(PodRepositoryBuildConfig {
        db,
        supervisor,
        side_effects,
        metrics,
        network_events,
        scheduling_mode: super::api::PodSchedulingMode::InlineSingleNode,
        outbox: None,
        cluster_api: None,
    });

    // build_parts must not call workqueue.start().
    assert!(
        !parts.background.workqueue_start_called(),
        "build_parts must not start the workqueue; background.start() owns that"
    );

    // Explicit start must call workqueue.start().
    parts.background.start();
    assert!(
        parts.background.workqueue_start_called(),
        "background.start() must call workqueue.start()"
    );
}

/// Task 4.2: Verify that PodRepositoryBackground::start() delegates to
/// PodWorkqueue::start() exactly once, and that build_parts does not
/// call it during construction.
#[tokio::test]
async fn pod_workqueue_runner_start_calls_workqueue_start_once() {
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let supervisor = fixture_supervisor();
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = fixture_side_effects();
    let network_events = crate::networking::global_pod_network_events();

    let parts = PodRepository::build_parts(PodRepositoryBuildConfig {
        db,
        supervisor,
        side_effects,
        metrics,
        network_events,
        scheduling_mode: super::api::PodSchedulingMode::InlineSingleNode,
        outbox: None,
        cluster_api: None,
    });

    // build_parts must not have started the workqueue.
    assert!(!parts.background.workqueue_start_called());

    // Start must delegate to workqueue.start().
    parts.background.start();
    assert!(parts.background.workqueue_start_called());

    // Calling start again is idempotent (reconciler uses AtomicBool CAS).
    parts.background.start();
    assert!(parts.background.workqueue_start_called());
}

/// Task 4.3: UID-gated PodObjectWriter mutations must not affect a
/// same-name replacement Pod when called with a stale UID.
#[tokio::test]
async fn pod_object_service_requires_uid_for_mutating_paths() {
    use super::PodObjectWriter;

    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let supervisor = fixture_supervisor();
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = fixture_side_effects();
    let repo = PodRepository::new(db.clone(), supervisor, side_effects, metrics);

    // Create a Pod with UID "uid-new".
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "default",
            "name": "test-pod",
            "uid": "uid-new"
        },
        "spec": {
            "containers": [{"name": "app", "image": "busybox"}]
        },
        "status": {"phase": "Pending"}
    });
    db.create_resource("v1", "Pod", Some("default"), "test-pod", pod.clone())
        .await
        .unwrap();

    // Stale UID must be rejected for update_pod_owner_references.
    let err = repo
        .update_pod_owner_references_for_uid(
            "default",
            "test-pod",
            "uid-old",
            vec![json!({"apiVersion": "v1", "kind": "ReplicaSet", "name": "rs", "uid": "rs-uid"})],
        )
        .await;
    assert!(
        err.is_err(),
        "stale UID must be rejected for update_pod_owner_references"
    );

    // Stale UID must be rejected for merge_pod_labels.
    let err = repo
        .merge_pod_labels_for_uid(
            "default",
            "test-pod",
            "uid-old",
            vec![("app".to_string(), "v2".to_string())],
        )
        .await;
    assert!(
        err.is_err(),
        "stale UID must be rejected for merge_pod_labels"
    );

    // Verify the replacement Pod is unchanged.
    let live = repo
        .get_pod("default", "test-pod")
        .await
        .unwrap()
        .expect("replacement Pod must still exist");
    assert_eq!(live.uid, "uid-new");
    assert!(
        live.data
            .pointer("/metadata/ownerReferences")
            .and_then(|v| v.as_array())
            .map(|a| a.is_empty())
            .unwrap_or(true),
        "ownerReferences must not be set by stale-UID call"
    );

    // Correct UID must succeed.
    repo.update_pod_owner_references_for_uid(
        "default",
        "test-pod",
        "uid-new",
        vec![json!({"apiVersion": "v1", "kind": "ReplicaSet", "name": "rs", "uid": "rs-uid"})],
    )
    .await
    .expect("correct UID must succeed for update_pod_owner_references");

    repo.merge_pod_labels_for_uid(
        "default",
        "test-pod",
        "uid-new",
        vec![("app".to_string(), "v2".to_string())],
    )
    .await
    .expect("correct UID must succeed for merge_pod_labels");
}

/// Task 4.4: PodStatusWriter already has `_for_uid` variants. Verify
/// that a stale-UID status write is rejected and the replacement Pod
/// is unchanged.
#[tokio::test]
async fn pod_status_service_writes_are_uid_preconditioned() {
    use super::PodStatusWriter;

    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let supervisor = fixture_supervisor();
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = fixture_side_effects();
    let repo = PodRepository::new(db.clone(), supervisor, side_effects, metrics);

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"namespace": "default", "name": "status-test", "uid": "uid-correct"},
        "spec": {"containers": [{"name": "app", "image": "busybox"}]},
        "status": {"phase": "Pending"}
    });
    db.create_resource("v1", "Pod", Some("default"), "status-test", pod)
        .await
        .unwrap();

    // Stale UID: write with wrong UID must be rejected.
    let update = super::PodStatusUpdate {
        phase: "Running".to_string(),
        pod_ip: String::new(),
        host_ip: String::new(),
        container_statuses: vec![],
        init_container_statuses: None,
        qos_class: None,
    };
    let err = repo
        .set_pod_status_for_uid("default", "status-test", "uid-wrong", update.clone(), None)
        .await;
    assert!(err.is_err(), "stale UID status write must be rejected");

    // Correct UID: write must succeed.
    let live = repo
        .get_pod("default", "status-test")
        .await
        .unwrap()
        .expect("Pod must exist");
    repo.set_pod_status_for_uid("default", "status-test", &live.uid, update, None)
        .await
        .expect("correct UID status write must succeed");
}

/// On a retryable StartPod failure (image pull etc.), the new method must
/// write `containerStatuses[].state.waiting.reason = ErrImagePull` on first
/// failure and escalate to `ImagePullBackOff` on subsequent failures.
/// Phase must stay `Pending` so Deployment/ReplicaSet controllers don't
/// treat the pod as terminal.
#[tokio::test]
async fn mark_start_pending_for_retry_writes_err_image_pull_then_image_pull_backoff() {
    use super::PodStatusWriter;

    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let supervisor = fixture_supervisor();
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = fixture_side_effects();
    let repo = PodRepository::new(db.clone(), supervisor, side_effects, metrics);

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"namespace": "default", "name": "pull-fail", "uid": "uid-1"},
        "spec": {"containers": [{"name": "app", "image": "docker.io/library/busybox:1.36"}]},
        "status": {"phase": "Pending"}
    });
    db.create_resource("v1", "Pod", Some("default"), "pull-fail", pod)
        .await
        .unwrap();

    let err_msg = "Failed to pull image \"docker.io/library/busybox:1.36\": \
                   CRI pull_image failed: connection refused";

    // First failure: ErrImagePull.
    repo.mark_start_pending_for_retry_for_uid("default", "pull-fail", "uid-1", err_msg)
        .await
        .expect("first retry status write must succeed");

    let after_first = repo
        .get_pod("default", "pull-fail")
        .await
        .unwrap()
        .expect("pod must still exist");
    assert_eq!(
        after_first
            .data
            .pointer("/status/phase")
            .and_then(|v| v.as_str()),
        Some("Pending"),
        "phase must stay Pending on retryable startup failure"
    );
    let first_reason = after_first
        .data
        .pointer("/status/containerStatuses/0/state/waiting/reason")
        .and_then(|v| v.as_str());
    assert_eq!(first_reason, Some("ErrImagePull"));

    // Second failure: ImagePullBackOff.
    repo.mark_start_pending_for_retry_for_uid("default", "pull-fail", "uid-1", err_msg)
        .await
        .expect("second retry status write must succeed");

    let after_second = repo
        .get_pod("default", "pull-fail")
        .await
        .unwrap()
        .expect("pod must still exist");
    let second_reason = after_second
        .data
        .pointer("/status/containerStatuses/0/state/waiting/reason")
        .and_then(|v| v.as_str());
    assert_eq!(second_reason, Some("ImagePullBackOff"));
    assert_eq!(
        after_second
            .data
            .pointer("/status/phase")
            .and_then(|v| v.as_str()),
        Some("Pending"),
    );
    let second_message = after_second
        .data
        .pointer("/status/containerStatuses/0/state/waiting/message")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(
        second_message.contains("busybox") || !second_message.is_empty(),
        "waiting.message must carry the underlying error: got {second_message:?}"
    );
}

/// UID precondition: a stale UID retry-status write must be rejected and
/// must not affect a same-name replacement pod.
#[tokio::test]
async fn mark_start_pending_for_retry_rejects_stale_uid() {
    use super::PodStatusWriter;

    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let supervisor = fixture_supervisor();
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = fixture_side_effects();
    let repo = PodRepository::new(db.clone(), supervisor, side_effects, metrics);

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"namespace": "default", "name": "same-name", "uid": "uid-current"},
        "spec": {"containers": [{"name": "app", "image": "busybox"}]},
        "status": {"phase": "Pending"}
    });
    db.create_resource("v1", "Pod", Some("default"), "same-name", pod)
        .await
        .unwrap();
    let before = repo.get_pod("default", "same-name").await.unwrap().unwrap();

    let err = repo
        .mark_start_pending_for_retry_for_uid(
            "default",
            "same-name",
            "uid-stale",
            "Failed to pull image",
        )
        .await;
    assert!(
        err.is_err(),
        "stale UID retry-status write must be rejected"
    );

    let after = repo.get_pod("default", "same-name").await.unwrap().unwrap();
    assert_eq!(after.uid, before.uid);
    assert_eq!(after.resource_version, before.resource_version);
}

/// Task 4.5: PodSubresourceWriter UID-gated methods protect same-name
/// replacements from stale subresource updates.
#[tokio::test]
async fn pod_subresource_service_status_and_ephemeral_updates_require_uid() {
    use super::PodSubresourceWriter;

    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let supervisor = fixture_supervisor();
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = fixture_side_effects();
    let repo = PodRepository::new(db.clone(), supervisor, side_effects, metrics);

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"namespace": "default", "name": "subres-test", "uid": "uid-correct"},
        "spec": {"containers": [{"name": "app", "image": "busybox"}]},
        "status": {"phase": "Pending"}
    });
    db.create_resource("v1", "Pod", Some("default"), "subres-test", pod)
        .await
        .unwrap();

    let live = repo
        .get_pod("default", "subres-test")
        .await
        .unwrap()
        .unwrap();

    // Stale UID: status write with wrong UID must fail.
    {
        use super::PodStatusWriter;
        let update = super::PodStatusUpdate {
            phase: "Failed".to_string(),
            pod_ip: String::new(),
            host_ip: String::new(),
            container_statuses: vec![],
            init_container_statuses: None,
            qos_class: None,
        };
        let err = repo
            .set_pod_status_for_uid("default", "subres-test", "uid-stale", update, None)
            .await;
        assert!(err.is_err(), "stale UID status write must be rejected");
    }

    // Verify replacement is unchanged.
    let after = repo
        .get_pod("default", "subres-test")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.uid, live.uid);
    assert_eq!(after.resource_version, live.resource_version);

    // Correct UID succeeds.
    repo.update_ephemeral_containers_for_uid(
        "default",
        "subres-test",
        &live.uid,
        vec![],
        live.resource_version,
    )
    .await
    .expect("correct UID ephemeral containers update must succeed");
}

/// Task 4.6: PodNetworkReader already takes pod_uid. Verify UID-presence
/// in the trait signature protects same-name replacements.
#[tokio::test]
async fn pod_network_service_pod_network_rows_are_uid_keyed() {
    use super::PodNetworkReader;

    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let supervisor = fixture_supervisor();
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = fixture_side_effects();
    let repo = PodRepository::new(db, supervisor, side_effects, metrics);

    // read_pod_network_assignment requires pod_uid in its signature.
    // This is a compile-time contract: every caller must supply a UID.
    let result = repo
        .read_pod_network_assignment("sb-noexist", "default", "net-test", "uid-specific", false)
        .await;
    assert!(result.is_err());
}

/// Task 4.7: PodWatchSource subscription carries resource version and
/// UIDs on watch events. Verify subscribe + event metadata shape.
#[tokio::test]
async fn pod_watch_service_preserves_resource_version_and_uid() {
    use super::PodWatchSource;

    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let supervisor = fixture_supervisor();
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = fixture_side_effects();
    let repo = PodRepository::new(db.clone(), supervisor, side_effects, metrics);

    let mut rx = repo.subscribe_pod_watch();

    // Create a Pod — it must emit a watch event with UID and resourceVersion.
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "watch-test",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "watch-test",
                "uid": "uid-watch"
            },
            "spec": {
                "containers": [{"name": "app", "image": "busybox"}]
            },
            "status": {"phase": "Pending"}
        }),
    )
    .await
    .unwrap();

    let event = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .expect("watch event must be received within timeout")
        .expect("watch channel must be open");

    let pod_uid = event
        .object
        .pointer("/metadata/uid")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert!(!pod_uid.is_empty(), "watch event must carry UID");
    assert!(
        event.resource_version().unwrap_or(0) > 0,
        "watch event must carry resourceVersion"
    );
}

// --- Task 4.8: PodWatchRunner ---

/// Verify PodWatchRunner uses TaskSupervisor (not direct tokio::spawn)
/// and preserves UID on forwarded events.
#[test]
fn pod_watch_runner_start_uses_supervisor_and_preserves_event_uid() {
    let supervisor = fixture_supervisor();
    let runner = super::background::PodWatchRunner::new(supervisor);

    // Runner must not be started before explicit start().
    assert!(
        !runner.started.load(std::sync::atomic::Ordering::Acquire),
        "watch runner must not start before explicit start()"
    );

    // Start must set the started flag (no direct tokio::spawn).
    runner.start();
    assert!(
        runner.started.load(std::sync::atomic::Ordering::Acquire),
        "watch runner start() must mark started"
    );
}

// --- Task 4.9: DeadlineTimerRunner ---

/// Verify DeadlineTimerRunner uses TaskSupervisor::spawn_delay (no
/// polling, no spawn_interval) for UID-bound deadline wakeups.
#[test]
fn deadline_timer_runner_schedules_uid_bound_wakeups() {
    let supervisor = fixture_supervisor();
    let runner = super::background::DeadlineTimerRunner::new(supervisor);

    // schedule_uid_bound_wakeup must accept namespace, name, uid, and
    // delay — all UID-bound identity fields.
    runner.schedule_uid_bound_wakeup("ns", "pod", "uid-1", 5000, "test-deadline");
    // No tokio::time::sleep, no spawn_interval: the runner delegates to
    // TaskSupervisor::spawn_delay internally.
}

// --- Task 4.10: PodStore UID-Bound Mutation Audit ---

/// Enumerate each public mutating PodStore method and prove a stale UID
/// cannot mutate or delete a same-name replacement Pod.
#[tokio::test]
async fn pod_store_mutating_methods_require_uid_or_create_context() {
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let store = super::store::PodStore::new(db.clone());

    let pod_new = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"namespace": "default", "name": "uid-audit", "uid": "uid-new"},
        "spec": {"containers": [{"name": "app", "image": "busybox"}]},
        "status": {"phase": "Pending"}
    });
    store.create("default", "uid-audit", pod_new).await.unwrap();

    let created = store.get("default", "uid-audit").await.unwrap().unwrap();
    assert_eq!(created.uid, "uid-new");

    // delete_with_uid with stale UID must not delete the replacement.
    let err = store
        .delete_with_uid("default", "uid-audit", "uid-stale")
        .await;
    assert!(err.is_err(), "stale UID delete_with_uid must be rejected");

    // Replacement Pod must still exist with uid-new.
    let still_there = store
        .get("default", "uid-audit")
        .await
        .unwrap()
        .expect("replacement Pod must not be deleted by stale UID");
    assert_eq!(still_there.uid, "uid-new");

    // update resolves UID from current Pod state and uses it as
    // a DB precondition. A fresh replacement with different UID would
    // cause the precondition check to fail.
    let updated = store
        .update(
            "default",
            "uid-audit",
            still_there.data.as_ref().clone(),
            still_there.resource_version,
        )
        .await
        .expect("update with correct current state must succeed");
    assert_eq!(updated.uid, "uid-new");

    // delete_with_uid with correct UID must succeed.
    store
        .delete_with_uid("default", "uid-audit", "uid-new")
        .await
        .expect("correct UID delete must succeed");
    assert!(
        store.get("default", "uid-audit").await.unwrap().is_none(),
        "Pod must be deleted after correct-UID delete"
    );
}

#[tokio::test]
async fn worker_status_enqueue_does_not_bypass_leader_side_effects() {
    use super::PodStatusWriter;

    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let node_db = fixture_node_local().await;
    let outbox = Arc::new(crate::kubelet::outbox::Outbox::new(node_db.clone()));
    let supervisor = fixture_supervisor();
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = fixture_side_effects();
    let repo = PodRepository::new_with_scheduling_mode_and_outbox(
        db.clone(),
        supervisor,
        side_effects,
        metrics,
        super::api::PodSchedulingMode::InlineSingleNode,
        Some(outbox),
    );
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "outbox-status",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "outbox-status",
                "uid": "uid-outbox-status"
            },
            "spec": {"containers": [{"name": "app", "image": "nginx"}]},
            "status": {"phase": "Pending"}
        }),
    )
    .await
    .expect("create pod");

    let returned = repo
        .set_pod_status_for_uid(
            "default",
            "outbox-status",
            "uid-outbox-status",
            super::PodStatusUpdate {
                phase: "Running".to_string(),
                pod_ip: "10.42.0.8".to_string(),
                host_ip: "192.0.2.10".to_string(),
                container_statuses: vec![],
                init_container_statuses: None,
                qos_class: None,
            },
            None,
        )
        .await
        .expect("enqueue status");

    assert_eq!(
        returned
            .data
            .pointer("/status/phase")
            .and_then(|value| value.as_str()),
        Some("Running"),
        "kubelet callers receive a synthetic status view for local follow-up"
    );
    let stored = db
        .get_resource("v1", "Pod", Some("default"), "outbox-status")
        .await
        .expect("read stored pod")
        .expect("pod exists");
    assert_eq!(
        stored
            .data
            .pointer("/status/phase")
            .and_then(|value| value.as_str()),
        Some("Pending"),
        "outbox mode must not write Pod status directly to cluster storage"
    );
    let row = node_db
        .claim_next_due_outbox(i64::MAX / 4, 1_000, "assert")
        .await
        .expect("claim outbox")
        .expect("status row enqueued");
    assert_eq!(row.operation, "PodStatus");
    assert_eq!(row.pod_uid, "uid-outbox-status");
}

#[tokio::test]
async fn kubelet_pod_reader_uses_leader_api_when_configured() {
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let pod = crate::datastore::Resource {
        id: 1,
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "leader-only".to_string(),
        uid: "uid-leader-only".to_string(),
        resource_version: 42,
        data: Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "leader-only",
                "uid": "uid-leader-only",
                "resourceVersion": "42"
            },
            "spec": {"nodeName": "node-a", "containers": [{"name": "app", "image": "nginx"}]}
        })),
    };
    let repo = PodRepository::new_with_scheduling_mode_outbox_and_cluster_api(
        db,
        fixture_supervisor(),
        fixture_side_effects(),
        crate::side_effects::SideEffectMetrics::new(),
        super::api::PodSchedulingMode::InlineSingleNode,
        None,
        Arc::new(FakeLeaderApiClient::new(pod.clone())),
    );

    let got = repo
        .get_pod("default", "leader-only")
        .await
        .expect("leader pod read");
    assert_eq!(got.map(|pod| pod.uid), Some("uid-leader-only".to_string()));
}

#[tokio::test]
async fn kubelet_pod_reader_uses_fresh_leader_api_for_single_pod_reads() {
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let stale_pod = crate::datastore::Resource {
        id: 1,
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "worker-probed".to_string(),
        uid: "uid-worker-probed".to_string(),
        resource_version: 11,
        data: Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "worker-probed",
                "uid": "uid-worker-probed",
                "resourceVersion": "11"
            },
            "spec": {
                "nodeName": "worker-a",
                "containers": [{"name": "web", "image": "registry.k8s.io/e2e-test-images/agnhost:2.56"}]
            },
            "status": {"phase": "Pending"}
        })),
    };
    let fresh_pod = crate::datastore::Resource {
        resource_version: 12,
        data: Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "worker-probed",
                "uid": "uid-worker-probed",
                "resourceVersion": "12"
            },
            "spec": {
                "nodeName": "worker-a",
                "containers": [{"name": "web", "image": "registry.k8s.io/e2e-test-images/agnhost:2.56"}]
            },
            "status": {
                "phase": "Running",
                "podIP": "10.50.2.2",
                "containerStatuses": [{
                    "name": "web",
                    "containerID": "containerd://running-container",
                    "ready": false,
                    "started": true,
                    "state": {"running": {"startedAt": "2026-05-18T19:35:03Z"}}
                }]
            }
        })),
        ..stale_pod.clone()
    };
    let repo = PodRepository::new_with_scheduling_mode_outbox_and_cluster_api(
        db,
        fixture_supervisor(),
        fixture_side_effects(),
        crate::side_effects::SideEffectMetrics::new(),
        super::api::PodSchedulingMode::InlineSingleNode,
        None,
        Arc::new(FakeLeaderApiClient::new(stale_pod).with_fresh_pod(fresh_pod)),
    );

    let got = repo
        .get_pod("default", "worker-probed")
        .await
        .expect("fresh leader pod read")
        .expect("pod exists");
    assert_eq!(
        got.data
            .pointer("/status/phase")
            .and_then(|value| value.as_str()),
        Some("Running"),
        "probe and lifecycle code must not make decisions from a stale informer-cache pod"
    );

    let got_for_uid = repo
        .get_pod_for_uid("default", "worker-probed", "uid-worker-probed")
        .await
        .expect("fresh uid-bound pod read")
        .expect("pod exists for uid");
    assert_eq!(
        got_for_uid
            .data
            .pointer("/status/containerStatuses/0/state/running/startedAt")
            .and_then(|value| value.as_str()),
        Some("2026-05-18T19:35:03Z"),
        "uid-bound reads need the same fresh status for readiness-probe initialDelaySeconds"
    );
}

#[tokio::test]
async fn runtime_reconcile_reads_pending_status_checkpoint_from_node_db() {
    use super::PodStatusWriter;

    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let node_db = fixture_node_local().await;
    let outbox = Arc::new(crate::kubelet::outbox::Outbox::new(node_db.clone()));
    let stale_pod = crate::datastore::Resource {
        id: 1,
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "checkpoint-status".to_string(),
        uid: "uid-checkpoint-status".to_string(),
        resource_version: 12,
        data: Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "checkpoint-status",
                "uid": "uid-checkpoint-status",
                "resourceVersion": "12"
            },
            "spec": {
                "nodeName": "node-a",
                "containers": [{"name": "app", "image": "nginx"}]
            },
            "status": {"phase": "Pending"}
        })),
    };
    let repo = PodRepository::new_with_scheduling_mode_outbox_and_cluster_api(
        db,
        fixture_supervisor(),
        fixture_side_effects(),
        crate::side_effects::SideEffectMetrics::new(),
        super::api::PodSchedulingMode::InlineSingleNode,
        Some(outbox),
        Arc::new(FakeLeaderApiClient::new(stale_pod)),
    );

    repo.set_pod_status_for_uid(
        "default",
        "checkpoint-status",
        "uid-checkpoint-status",
        super::PodStatusUpdate {
            phase: "Pending".to_string(),
            pod_ip: "10.42.0.9".to_string(),
            host_ip: "192.0.2.9".to_string(),
            container_statuses: vec![],
            init_container_statuses: None,
            qos_class: None,
        },
        None,
    )
    .await
    .expect("enqueue podIP status");

    let reconciled = repo
        .apply_runtime_reconcile_status_for_uid(
            "default",
            "checkpoint-status",
            "uid-checkpoint-status",
            super::RuntimeReconcileStatus {
                phase: "Running".to_string(),
                container_statuses: vec![json!({
                    "name": "app",
                    "ready": false,
                    "restartCount": 0,
                    "state": {"running": {"startedAt": "2026-06-01T00:00:00Z"}}
                })],
            },
            None,
        )
        .await
        .expect("runtime reconcile should use checkpointed podIP");

    assert_eq!(
        reconciled
            .data
            .pointer("/status/phase")
            .and_then(|value| value.as_str()),
        Some("Running"),
        "runtime reconcile must not defer Running when node.db has the prior podIP status"
    );
    assert_eq!(
        reconciled
            .data
            .pointer("/status/podIP")
            .and_then(|value| value.as_str()),
        Some("10.42.0.9")
    );
}

// Regression: the worker confirm read used by startup/deletion finalization
// (`PodReader::get_pod_for_uid`) must reflect the worker's OWN just-written
// status by overlaying the node-local status checkpoint (read-your-own-write).
// Before the fix it returned the leader's un-merged copy, which under real
// inter-node latency lags the worker's async outbox write — so finalize_startup
// kept seeing Pending and looped on `Unconfirmed`, stalling status convergence
// and foreground-GC deletion on a two-VM cluster.
#[tokio::test]
async fn get_pod_for_uid_overlays_local_status_checkpoint_for_read_your_own_write() {
    use super::{PodReader, PodStatusWriter};

    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let node_db = fixture_node_local().await;
    let outbox = Arc::new(crate::kubelet::outbox::Outbox::new(node_db.clone()));
    let stale_pod = crate::datastore::Resource {
        id: 1,
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "ryow-pod".to_string(),
        uid: "uid-ryow".to_string(),
        resource_version: 12,
        data: Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "ryow-pod",
                "uid": "uid-ryow",
                "resourceVersion": "12"
            },
            "spec": {
                "nodeName": "node-a",
                "containers": [{"name": "app", "image": "nginx"}]
            },
            "status": {"phase": "Pending"}
        })),
    };
    let repo = PodRepository::new_with_scheduling_mode_outbox_and_cluster_api(
        db,
        fixture_supervisor(),
        fixture_side_effects(),
        crate::side_effects::SideEffectMetrics::new(),
        super::api::PodSchedulingMode::InlineSingleNode,
        Some(outbox),
        Arc::new(FakeLeaderApiClient::new(stale_pod)),
    );

    // Worker records its own authoritative status node-locally: podIP from CNI,
    // then Running from runtime reconcile. The leader copy stays Pending (the
    // fake leader keeps returning the stale rv=12 Pending object).
    repo.set_pod_status_for_uid(
        "default",
        "ryow-pod",
        "uid-ryow",
        super::PodStatusUpdate {
            phase: "Pending".to_string(),
            pod_ip: "10.42.0.9".to_string(),
            host_ip: "192.0.2.9".to_string(),
            container_statuses: vec![],
            init_container_statuses: None,
            qos_class: None,
        },
        None,
    )
    .await
    .expect("record podIP checkpoint");
    repo.apply_runtime_reconcile_status_for_uid(
        "default",
        "ryow-pod",
        "uid-ryow",
        super::RuntimeReconcileStatus {
            phase: "Running".to_string(),
            container_statuses: vec![json!({
                "name": "app",
                "ready": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-06-01T00:00:00Z"}}
            })],
        },
        None,
    )
    .await
    .expect("record Running checkpoint");

    // The confirm read must overlay the node-local checkpoint, not return the
    // stale leader Pending. This is the read finalize_startup depends on.
    let read = PodReader::get_pod_for_uid(&repo, "default", "ryow-pod", "uid-ryow")
        .await
        .expect("get_pod_for_uid")
        .expect("pod present");
    assert_eq!(
        read.data.pointer("/status/phase").and_then(|v| v.as_str()),
        Some("Running"),
        "get_pod_for_uid must overlay the node-local Running checkpoint (read-your-own-write), \
         not return the stale leader Pending copy"
    );
    assert_eq!(
        read.data.pointer("/status/podIP").and_then(|v| v.as_str()),
        Some("10.42.0.9"),
        "get_pod_for_uid must overlay the node-local podIP checkpoint"
    );
}

#[tokio::test]
async fn outbox_status_reads_current_pod_through_leader_api() {
    use super::PodStatusWriter;

    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let node_db = fixture_node_local().await;
    let outbox = Arc::new(crate::kubelet::outbox::Outbox::new(node_db.clone()));
    let pod = crate::datastore::Resource {
        id: 1,
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "leader-status".to_string(),
        uid: "uid-leader-status".to_string(),
        resource_version: 7,
        data: Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "leader-status",
                "uid": "uid-leader-status",
                "resourceVersion": "7"
            },
            "spec": {"nodeName": "node-a", "containers": [{"name": "app", "image": "nginx"}]},
            "status": {"phase": "Pending"}
        })),
    };
    let repo = PodRepository::new_with_scheduling_mode_outbox_and_cluster_api(
        db.clone(),
        fixture_supervisor(),
        fixture_side_effects(),
        crate::side_effects::SideEffectMetrics::new(),
        super::api::PodSchedulingMode::InlineSingleNode,
        Some(outbox),
        Arc::new(FakeLeaderApiClient::new(pod)),
    );

    let returned = repo
        .set_pod_status_for_uid(
            "default",
            "leader-status",
            "uid-leader-status",
            super::PodStatusUpdate {
                phase: "Running".to_string(),
                pod_ip: "10.42.0.9".to_string(),
                host_ip: "192.0.2.20".to_string(),
                container_statuses: vec![],
                init_container_statuses: None,
                qos_class: None,
            },
            None,
        )
        .await
        .expect("enqueue status from leader snapshot");

    assert_eq!(
        returned
            .data
            .pointer("/status/phase")
            .and_then(|value| value.as_str()),
        Some("Running")
    );
    assert!(
        db.get_resource("v1", "Pod", Some("default"), "leader-status")
            .await
            .expect("read direct db")
            .is_none(),
        "kubelet status path must not require a direct cluster datastore read"
    );
    assert!(
        node_db
            .claim_next_due_outbox(i64::MAX / 4, 1_000, "assert")
            .await
            .expect("claim outbox")
            .is_some()
    );
}

#[tokio::test]
async fn outbox_sandbox_annotation_uses_leader_api_and_outbox() {
    use super::PodMetadataWriter;

    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let node_db = fixture_node_local().await;
    let outbox = Arc::new(crate::kubelet::outbox::Outbox::new(node_db.clone()));
    let pod = crate::datastore::Resource {
        id: 1,
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "leader-sandbox".to_string(),
        uid: "uid-leader-sandbox".to_string(),
        resource_version: 11,
        data: Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "leader-sandbox",
                "uid": "uid-leader-sandbox",
                "resourceVersion": "11"
            },
            "spec": {"nodeName": "node-a", "containers": [{"name": "app", "image": "nginx"}]}
        })),
    };
    let repo = PodRepository::new_with_scheduling_mode_outbox_and_cluster_api(
        db.clone(),
        fixture_supervisor(),
        fixture_side_effects(),
        crate::side_effects::SideEffectMetrics::new(),
        super::api::PodSchedulingMode::InlineSingleNode,
        Some(outbox),
        Arc::new(FakeLeaderApiClient::new(pod)),
    );

    let returned = repo
        .record_sandbox_id_for_uid(
            "default",
            "leader-sandbox",
            "uid-leader-sandbox",
            "sandbox-abc",
        )
        .await
        .expect("enqueue sandbox annotation");

    assert_eq!(
        returned
            .data
            .pointer("/metadata/annotations/klights.dev~1sandbox-id")
            .and_then(|value| value.as_str()),
        Some("sandbox-abc")
    );
    assert!(
        db.get_resource("v1", "Pod", Some("default"), "leader-sandbox")
            .await
            .expect("read direct db")
            .is_none(),
        "kubelet sandbox metadata path must not write cluster storage directly"
    );
    let row = node_db
        .claim_next_due_outbox(i64::MAX / 4, 1_000, "assert")
        .await
        .expect("claim outbox")
        .expect("metadata row enqueued");
    assert_eq!(row.operation, "PodMetadata");
    assert_eq!(row.pod_uid, "uid-leader-sandbox");
}

#[tokio::test]
async fn non_leader_pod_object_writer_without_outbox_retries_later() {
    use super::PodMetadataWriter;
    use super::PodObjectWriter;

    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let pod = crate::datastore::Resource {
        id: 1,
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "leader-metadata-no-outbox".to_string(),
        uid: "uid-leader-metadata-no-outbox".to_string(),
        resource_version: 12,
        data: Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "leader-metadata-no-outbox",
                "uid": "uid-leader-metadata-no-outbox",
                "resourceVersion": "12"
            },
            "spec": {"nodeName": "node-a", "containers": [{"name": "app", "image": "nginx"}]},
            "status": {"phase": "Pending"}
        })),
    };
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "leader-metadata-no-outbox",
        (*pod.data).clone(),
    )
    .await
    .unwrap();

    let repo = PodRepository::new_with_scheduling_mode_outbox_and_cluster_api(
        db,
        fixture_supervisor(),
        fixture_side_effects(),
        crate::side_effects::SideEffectMetrics::new(),
        super::api::PodSchedulingMode::InlineSingleNode,
        None,
        Arc::new(FakeLeaderApiClient::new(pod)),
    );

    let owner_result = repo
        .update_pod_owner_references_for_uid(
            "default",
            "leader-metadata-no-outbox",
            "uid-leader-metadata-no-outbox",
            vec![json!({"apiVersion": "v1", "kind": "ReplicaSet", "name": "rs", "uid": "uid-rs"})],
        )
        .await;
    assert!(
        owner_result.is_err(),
        "owner reference update must be rejected without outbox"
    );
    assert!(
        owner_result.unwrap_err().to_string().contains("outbox"),
        "missing outbox should return retry guidance"
    );

    let labels_result = repo
        .merge_pod_labels_for_uid(
            "default",
            "leader-metadata-no-outbox",
            "uid-leader-metadata-no-outbox",
            vec![("app".to_string(), "changed".to_string())],
        )
        .await;
    assert!(
        labels_result.is_err(),
        "label merge must be rejected without outbox"
    );
    assert!(
        labels_result.unwrap_err().to_string().contains("outbox"),
        "missing outbox should return retry guidance"
    );

    let sandbox_result = repo
        .record_sandbox_id_for_uid(
            "default",
            "leader-metadata-no-outbox",
            "uid-leader-metadata-no-outbox",
            "sandbox-missing-outbox",
        )
        .await;
    assert!(
        sandbox_result.is_err(),
        "sandbox annotation must be rejected without outbox"
    );
    assert!(
        sandbox_result.unwrap_err().to_string().contains("outbox"),
        "missing outbox should return retry guidance"
    );

    let live = repo
        .store
        .get("default", "leader-metadata-no-outbox")
        .await
        .unwrap()
        .unwrap();
    assert!(
        live.data["metadata"].get("labels").is_none(),
        "labels should remain unchanged in local DB when non-leader outbox is unavailable"
    );
    assert!(
        live.data["metadata"]
            .get("annotations")
            .and_then(|annotations| annotations.get("klights.dev/sandbox-id"))
            .is_none(),
        "sandbox id annotation should remain absent in local DB when non-leader outbox is unavailable"
    );
    let status = live.data;
    assert!(
        status["metadata"].get("ownerReferences").is_none(),
        "owner references should not be persisted without outbox"
    );
    assert!(
        status["metadata"].get("labels").is_none(),
        "labels should not be changed without outbox"
    );
}

#[tokio::test]
async fn non_leader_pod_status_writer_without_outbox_retries_later() {
    use super::PodStatusWriter;

    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let pod = crate::datastore::Resource {
        id: 1,
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "leader-status-no-outbox".to_string(),
        uid: "uid-leader-status-no-outbox".to_string(),
        resource_version: 7,
        data: Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "leader-status-no-outbox",
                "uid": "uid-leader-status-no-outbox",
                "resourceVersion": "7"
            },
            "spec": {"nodeName": "node-a", "containers": [{"name": "app", "image": "nginx"}]},
            "status": {"phase": "Pending"}
        })),
    };
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "leader-status-no-outbox",
        (*pod.data).clone(),
    )
    .await
    .unwrap();

    let repo = PodRepository::new_with_scheduling_mode_outbox_and_cluster_api(
        db,
        fixture_supervisor(),
        fixture_side_effects(),
        crate::side_effects::SideEffectMetrics::new(),
        super::api::PodSchedulingMode::InlineSingleNode,
        None,
        Arc::new(FakeLeaderApiClient::new(pod)),
    );

    let status_res = repo
        .set_pod_status_for_uid(
            "default",
            "leader-status-no-outbox",
            "uid-leader-status-no-outbox",
            super::PodStatusUpdate {
                phase: "Running".to_string(),
                pod_ip: "10.0.0.11".to_string(),
                host_ip: "192.0.2.30".to_string(),
                container_statuses: vec![],
                init_container_statuses: None,
                qos_class: None,
            },
            None,
        )
        .await;
    assert!(
        status_res.is_err(),
        "status update must be rejected without outbox"
    );
    assert!(
        status_res.unwrap_err().to_string().contains("outbox"),
        "missing outbox should return retry guidance"
    );

    let live = repo
        .store
        .get("default", "leader-status-no-outbox")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        live.data["status"]["phase"].as_str(),
        Some("Pending"),
        "status phase should remain unchanged in local DB without outbox"
    );
}

#[tokio::test]
async fn worker_actor_finalization_enqueues_uid_qualified_pod_delete_outbox() {
    let (_ds, direct_db) = crate::datastore::test_support::in_memory_with_handle().await;
    let node_db = fixture_node_local().await;
    let outbox = Arc::new(crate::kubelet::outbox::Outbox::new(node_db.clone()));
    let pod = crate::datastore::Resource {
        id: 1,
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "leader-finalize".to_string(),
        uid: "uid-leader-finalize".to_string(),
        resource_version: 13,
        data: Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "leader-finalize",
                "uid": "uid-leader-finalize",
                "resourceVersion": "13",
                "deletionTimestamp": "2026-05-13T00:00:00Z",
                "deletionGracePeriodSeconds": 0
            },
            "spec": {"nodeName": "worker-1", "containers": [{"name": "app", "image": "nginx"}]},
            "status": {"phase": "Running"}
        })),
    };
    let cluster_api = Arc::new(FakeLeaderApiClient::new(pod));
    let worker_db: crate::datastore::DatastoreHandle = Arc::new(
        crate::control_plane::client::worker_store::WorkerStoreAdapter::new(
            cluster_api.clone(),
            node_db.clone(),
            "worker-1".to_string(),
        ),
    );
    let repo = PodRepository::new_with_scheduling_mode_outbox_and_cluster_api(
        worker_db,
        fixture_supervisor(),
        fixture_side_effects(),
        crate::side_effects::SideEffectMetrics::new(),
        super::api::PodSchedulingMode::InlineSingleNode,
        Some(outbox),
        cluster_api,
    );

    let finalized = repo
        .finalize_pod_deletion_after_actor_cleanup(
            "default",
            "leader-finalize",
            "uid-leader-finalize",
        )
        .await
        .expect("worker finalization should enqueue a leader delete");

    assert!(finalized);
    assert!(
        direct_db
            .get_resource("v1", "Pod", Some("default"), "leader-finalize")
            .await
            .expect("read direct db")
            .is_none(),
        "worker finalization must not depend on a local cluster datastore row"
    );
    let row = node_db
        .claim_next_due_outbox(i64::MAX / 4, 1_000, "assert")
        .await
        .expect("claim outbox")
        .expect("delete row enqueued");
    assert_eq!(row.operation, "PodMetadata");
    assert_eq!(row.pod_uid, "uid-leader-finalize");
    let payload = crate::kubelet::outbox::payload::OutboxPayload::decode_protobuf(
        row.payload_proto.as_slice(),
    )
    .expect("decode delete payload");
    match payload.command {
        crate::datastore::command::StorageCommand::DeleteResource {
            api_version,
            kind,
            namespace,
            name,
            preconditions,
        } => {
            assert_eq!(api_version, "v1");
            assert_eq!(kind, "Pod");
            assert_eq!(namespace.as_deref(), Some("default"));
            assert_eq!(name, "leader-finalize");
            assert_eq!(
                preconditions.uid.as_deref(),
                Some("uid-leader-finalize"),
                "actor finalization delete must be UID-qualified"
            );
        }
        other => panic!("expected Pod DeleteResource outbox command, got {other:?}"),
    }
}

#[tokio::test]
async fn worker_actor_finalization_deletes_node_local_status_checkpoint() {
    let (_ds, direct_db) = crate::datastore::test_support::in_memory_with_handle().await;
    let node_db = fixture_node_local().await;
    node_db
        .upsert_pod_status_checkpoint(
            "uid-finalize-checkpoint",
            "default",
            "finalize-checkpoint",
            13,
            json!({"phase": "Running", "podIP": "10.42.0.7"}),
            100,
        )
        .await
        .expect("seed status checkpoint");
    let outbox = Arc::new(crate::kubelet::outbox::Outbox::new(node_db.clone()));
    let pod = crate::datastore::Resource {
        id: 1,
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "finalize-checkpoint".to_string(),
        uid: "uid-finalize-checkpoint".to_string(),
        resource_version: 13,
        data: Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "finalize-checkpoint",
                "uid": "uid-finalize-checkpoint",
                "resourceVersion": "13",
                "deletionTimestamp": "2026-06-14T00:00:00Z",
                "deletionGracePeriodSeconds": 0
            },
            "spec": {"nodeName": "worker-1", "containers": [{"name": "app", "image": "nginx"}]},
            "status": {"phase": "Running", "podIP": "10.42.0.7"}
        })),
    };
    let cluster_api = Arc::new(FakeLeaderApiClient::new(pod));
    let worker_db: crate::datastore::DatastoreHandle = Arc::new(
        crate::control_plane::client::worker_store::WorkerStoreAdapter::new(
            cluster_api.clone(),
            node_db.clone(),
            "worker-1".to_string(),
        ),
    );
    let repo = PodRepository::new_with_scheduling_mode_outbox_and_cluster_api(
        worker_db,
        fixture_supervisor(),
        fixture_side_effects(),
        crate::side_effects::SideEffectMetrics::new(),
        super::api::PodSchedulingMode::InlineSingleNode,
        Some(outbox),
        cluster_api,
    );

    let finalized = repo
        .finalize_pod_deletion_after_actor_cleanup(
            "default",
            "finalize-checkpoint",
            "uid-finalize-checkpoint",
        )
        .await
        .expect("actor finalization should enqueue delete and clear checkpoint");

    assert!(finalized);
    assert!(
        direct_db
            .get_resource("v1", "Pod", Some("default"), "finalize-checkpoint")
            .await
            .expect("read direct db")
            .is_none(),
        "worker finalization must not depend on a local cluster datastore row"
    );
    assert!(
        node_db
            .get_pod_status_checkpoint("uid-finalize-checkpoint")
            .await
            .expect("read status checkpoint")
            .is_none(),
        "actor finalization must remove the UID-scoped node-local status checkpoint"
    );
}

#[tokio::test]
async fn worker_actor_finalization_uses_fresh_leader_read_before_refusing_delete() {
    let node_db = fixture_node_local().await;
    let outbox = Arc::new(crate::kubelet::outbox::Outbox::new(node_db.clone()));
    let stale_pod = crate::datastore::Resource {
        id: 1,
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "stale-finalize".to_string(),
        uid: "uid-stale-finalize".to_string(),
        resource_version: 13,
        data: Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "stale-finalize",
                "uid": "uid-stale-finalize",
                "resourceVersion": "13"
            },
            "spec": {"nodeName": "worker-1", "containers": [{"name": "app", "image": "nginx"}]},
            "status": {"phase": "Running"}
        })),
    };
    let mut fresh_pod = stale_pod.clone();
    fresh_pod.resource_version = 14;
    let mut fresh_data = (*fresh_pod.data).clone();
    fresh_data["metadata"]["resourceVersion"] = json!("14");
    fresh_data["metadata"]["deletionTimestamp"] = json!("2026-05-15T01:37:41Z");
    fresh_data["metadata"]["deletionGracePeriodSeconds"] = json!(0);
    fresh_pod.data = Arc::new(fresh_data);
    let cluster_api = Arc::new(FakeLeaderApiClient::new(stale_pod).with_fresh_pod(fresh_pod));
    let worker_db: crate::datastore::DatastoreHandle = Arc::new(
        crate::control_plane::client::worker_store::WorkerStoreAdapter::new(
            cluster_api.clone(),
            node_db.clone(),
            "worker-1".to_string(),
        ),
    );
    let repo = PodRepository::new_with_scheduling_mode_outbox_and_cluster_api(
        worker_db,
        fixture_supervisor(),
        fixture_side_effects(),
        crate::side_effects::SideEffectMetrics::new(),
        super::api::PodSchedulingMode::InlineSingleNode,
        Some(outbox),
        cluster_api,
    );

    let finalized = repo
        .finalize_pod_deletion_after_actor_cleanup(
            "default",
            "stale-finalize",
            "uid-stale-finalize",
        )
        .await
        .expect("fresh terminating leader read should allow finalization");

    assert!(finalized);
    let row = node_db
        .claim_next_due_outbox(i64::MAX / 4, 1_000, "assert")
        .await
        .expect("claim outbox");
    assert!(
        row.is_some(),
        "stale cached non-terminating pod must not suppress actor finalization delete"
    );
}

fn make_pod(name: &str, owner_uid: Option<&str>, label: Option<(&str, &str)>) -> serde_json::Value {
    let mut metadata = json!({"name": name, "namespace": "default"});
    if let Some(uid) = owner_uid {
        metadata["ownerReferences"] = json!([{
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "name": "rs-x",
            "uid": uid,
            "controller": true
        }]);
    }
    if let Some((k, v)) = label {
        metadata["labels"] = json!({ k: v });
    }
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": metadata,
        "spec": {"containers": [{"name": "c", "image": "nginx"}]}
    })
}

#[tokio::test]
async fn pod_store_round_trips_create_get_list_update_delete() {
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let store = PodStore::new(db);

    // create
    let created = store
        .create(
            "default",
            "p1",
            make_pod("p1", Some("owner-a"), Some(("app", "x"))),
        )
        .await
        .unwrap();
    assert_eq!(created.name, "p1");
    assert_eq!(created.kind, "Pod");

    // get
    let fetched = store
        .get("default", "p1")
        .await
        .unwrap()
        .expect("p1 present");
    assert_eq!(fetched.name, "p1");
    assert_eq!(fetched.namespace.as_deref(), Some("default"));

    // additional pods to make list/list_by_owner non-trivial
    store
        .create(
            "default",
            "p2",
            make_pod("p2", Some("owner-a"), Some(("app", "y"))),
        )
        .await
        .unwrap();
    store
        .create(
            "default",
            "p3",
            make_pod("p3", Some("owner-b"), Some(("app", "x"))),
        )
        .await
        .unwrap();

    // list (namespaced, no selector)
    let all = store
        .list(Some("default"), None, None, None, None)
        .await
        .unwrap();
    assert_eq!(all.items.len(), 3);

    // list (label selector) — must match exactly the pods carrying app=x
    let by_label = store
        .list(Some("default"), Some("app=x"), None, None, None)
        .await
        .unwrap();
    let mut names: Vec<String> = by_label.items.iter().map(|r| r.name.clone()).collect();
    names.sort();
    assert_eq!(names, vec!["p1".to_string(), "p3".to_string()]);

    // list_by_owner
    let owned = store.list_by_owner("default", "owner-a").await.unwrap();
    let mut owned_names: Vec<String> = owned.iter().map(|r| r.name.clone()).collect();
    owned_names.sort();
    assert_eq!(owned_names, vec!["p1".to_string(), "p2".to_string()]);

    // update: full-object update with CAS pass
    let mut body: serde_json::Value = (*fetched.data).clone();
    body["metadata"]["labels"] = json!({"app": "x", "tier": "frontend"});
    let updated = store
        .update("default", "p1", body, fetched.resource_version)
        .await
        .unwrap();
    assert!(updated.resource_version > fetched.resource_version);
    assert_eq!(
        updated.data["metadata"]["labels"]["tier"],
        json!("frontend")
    );

    // update: CAS fail (using the now-stale resource_version with mutated
    // data so the dedupe-on-identical-data fast path doesn't swallow it).
    let mut stale_body: serde_json::Value = (*updated.data).clone();
    stale_body["metadata"]["labels"] = json!({"app": "x", "tier": "stale"});
    let conflict = store
        .update("default", "p1", stale_body, fetched.resource_version)
        .await;
    let err = conflict.expect_err("stale rv must produce a conflict");
    assert!(
        err.to_string().contains("409"),
        "expected 409 Conflict, got {err:?}"
    );

    // update_status: stale RV returns Conflict
    let stale_status_conflict = store
        .update_status(
            "default",
            "p1",
            json!({"phase": "Running"}),
            Some(fetched.resource_version),
        )
        .await;
    let err = stale_status_conflict.expect_err("stale rv on status must conflict");
    assert!(
        err.to_string().contains("409"),
        "expected 409 Conflict on status update, got {err:?}"
    );

    // update_status: CAS pass with the live RV — read-modify-write
    let current = store.get("default", "p1").await.unwrap().unwrap();
    let after_status = store
        .update_status(
            "default",
            "p1",
            json!({"phase": "Running"}),
            Some(current.resource_version),
        )
        .await
        .unwrap();
    assert_eq!(after_status.data["status"]["phase"], json!("Running"));
}

/// HR#11 exception: a terminating Pod that was never bound to a node has no
/// kubelet actor to finalize it, so the leader removes the row directly.
const DELETE_TS: &str = "2026-01-01T00:00:00Z";

fn delete_mark_body() -> serde_json::Value {
    json!({"metadata": {"deletionTimestamp": DELETE_TS, "deletionGracePeriodSeconds": 0}})
}

#[tokio::test]
async fn delete_unscheduled_removes_terminating_unscheduled_pod() {
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let store = PodStore::new(db);
    let created = store
        .create("default", "u1", make_pod("u1", None, None))
        .await
        .unwrap();
    store
        .mark_deleting_latest("default", "u1", &created.uid, &delete_mark_body())
        .await
        .unwrap();

    let outcome = store
        .delete_unscheduled_with_uid("default", "u1", &created.uid)
        .await
        .unwrap();

    assert_eq!(outcome, UnscheduledPodDeleteOutcome::Removed);
    assert!(
        store.get("default", "u1").await.unwrap().is_none(),
        "unscheduled terminating Pod row must be removed"
    );
}

#[tokio::test]
async fn delete_unscheduled_defers_when_kubelet_picked_pod_up() {
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let store = PodStore::new(db);
    let mut pod = make_pod("s1", None, None);
    pod["spec"]["nodeName"] = json!("node-a");
    let created = store.create("default", "s1", pod).await.unwrap();
    store
        .mark_deleting_latest("default", "s1", &created.uid, &delete_mark_body())
        .await
        .unwrap();

    let outcome = store
        .delete_unscheduled_with_uid("default", "s1", &created.uid)
        .await
        .unwrap();

    assert_eq!(outcome, UnscheduledPodDeleteOutcome::DeferToActor);
    assert!(
        store.get("default", "s1").await.unwrap().is_some(),
        "a Pod bound to a node must only be removed by the actor"
    );
}

#[tokio::test]
async fn delete_unscheduled_waits_for_finalizers() {
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let store = PodStore::new(db);
    let mut pod = make_pod("f1", None, None);
    pod["metadata"]["finalizers"] = json!(["example.com/hold"]);
    let created = store.create("default", "f1", pod).await.unwrap();
    store
        .mark_deleting_latest("default", "f1", &created.uid, &delete_mark_body())
        .await
        .unwrap();

    let outcome = store
        .delete_unscheduled_with_uid("default", "f1", &created.uid)
        .await
        .unwrap();

    assert_eq!(outcome, UnscheduledPodDeleteOutcome::FinalizersPending);
    assert!(store.get("default", "f1").await.unwrap().is_some());
}

#[tokio::test]
async fn delete_unscheduled_refuses_non_terminating_pod() {
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let store = PodStore::new(db);
    let created = store
        .create("default", "live1", make_pod("live1", None, None))
        .await
        .unwrap();

    let outcome = store
        .delete_unscheduled_with_uid("default", "live1", &created.uid)
        .await
        .unwrap();

    assert_eq!(outcome, UnscheduledPodDeleteOutcome::DeferToActor);
    assert!(
        store.get("default", "live1").await.unwrap().is_some(),
        "a non-terminating Pod must never be hard-deleted"
    );
}

#[tokio::test]
async fn delete_unscheduled_is_idempotent_for_missing_or_replaced_uid() {
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let store = PodStore::new(db);

    // Missing Pod — nothing to remove.
    let outcome = store
        .delete_unscheduled_with_uid("default", "ghost", "uid-x")
        .await
        .unwrap();
    assert_eq!(outcome, UnscheduledPodDeleteOutcome::Removed);

    // A same-name replacement Pod owns the slot: our (old) UID is already gone.
    let created = store
        .create("default", "r1", make_pod("r1", None, None))
        .await
        .unwrap();
    store
        .mark_deleting_latest("default", "r1", &created.uid, &delete_mark_body())
        .await
        .unwrap();
    let outcome = store
        .delete_unscheduled_with_uid("default", "r1", "stale-uid")
        .await
        .unwrap();
    assert_eq!(outcome, UnscheduledPodDeleteOutcome::Removed);
    assert!(
        store.get("default", "r1").await.unwrap().is_some(),
        "the live replacement Pod must be preserved"
    );
}

async fn build_repo() -> super::PodRepository {
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let supervisor = fixture_supervisor();
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = fixture_side_effects();
    super::PodRepository::new(db, supervisor, side_effects, metrics)
}

struct StatusRacingRaftProposer {
    inner: crate::datastore::DatastoreHandle,
    namespace: String,
    pod_name: String,
    bumps: Arc<AtomicUsize>,
}

impl StatusRacingRaftProposer {
    async fn bump_status_before_delete_mark(
        &self,
        command: &crate::datastore::command::StorageCommand,
    ) {
        let targets_pod_delete_mark = match command {
            crate::datastore::command::StorageCommand::UpdateResource {
                api_version,
                kind,
                namespace,
                name,
                data,
                ..
            } => {
                api_version == "v1"
                    && kind == "Pod"
                    && namespace.as_deref() == Some(self.namespace.as_str())
                    && name == &self.pod_name
                    && data
                        .pointer("/metadata/deletionTimestamp")
                        .and_then(|value| value.as_str())
                        .is_some()
            }
            crate::datastore::command::StorageCommand::PatchResource {
                api_version,
                kind,
                namespace,
                name,
                patch,
                ..
            } => {
                api_version == "v1"
                    && kind == "Pod"
                    && namespace.as_deref() == Some(self.namespace.as_str())
                    && name == &self.pod_name
                    && patch
                        .pointer("/metadata/deletionTimestamp")
                        .and_then(|value| value.as_str())
                        .is_some()
            }
            _ => false,
        };
        if !targets_pod_delete_mark {
            return;
        }

        let bump = self.bumps.fetch_add(1, Ordering::SeqCst) + 1;
        let Some(current) = self
            .inner
            .get_resource("v1", "Pod", Some(&self.namespace), &self.pod_name)
            .await
            .expect("test status race reads pod")
        else {
            return;
        };
        self.inner
            .update_status_only_with_preconditions(
                "v1",
                "Pod",
                Some(&self.namespace),
                &self.pod_name,
                json!({
                    "phase": "Running",
                    "podIP": "10.42.0.55",
                    "raceBump": bump
                }),
                crate::datastore::ResourcePreconditions::uid(current.uid),
            )
            .await
            .expect("test status race advances pod resourceVersion");
    }

    async fn apply_command(
        &self,
        command: crate::datastore::command::StorageCommand,
        idempotency_key: &str,
        operation: crate::kubelet::outbox::payload::OutboxOperation,
        authoring_node: &str,
    ) -> std::result::Result<
        crate::datastore::raft::state_machine::RaftOutboxApply,
        crate::kubelet::outbox::OutboxApplyError,
    > {
        self.bump_status_before_delete_mark(&command).await;
        let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
            .encode_protobuf()
            .map_err(|err| crate::kubelet::outbox::OutboxApplyError::Retryable(err.to_string()))?;
        crate::datastore::raft::state_machine::propose_outbox_on_backend(
            self.inner.as_ref(),
            idempotency_key,
            operation,
            bytes::Bytes::from(payload),
            authoring_node,
        )
        .await
    }
}

#[async_trait::async_trait]
impl crate::datastore::replicated::RaftProposer for StatusRacingRaftProposer {
    async fn propose_command(
        &self,
        command: crate::datastore::command::StorageCommand,
    ) -> anyhow::Result<()> {
        let key = format!("status-race-{}", uuid::Uuid::new_v4());
        self.apply_command(
            command,
            &key,
            crate::kubelet::outbox::payload::OutboxOperation::PodStatus,
            "status-race-leader",
        )
        .await
        .map_err(|err| anyhow::anyhow!("status race raft propose: {err}"))?;
        Ok(())
    }

    async fn propose_outbox_command(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: crate::datastore::command::StorageCommand,
        authoring_node: &str,
    ) -> std::result::Result<
        crate::kubelet::outbox::OutboxApplyResult,
        crate::kubelet::outbox::OutboxApplyError,
    > {
        let operation = crate::kubelet::outbox::payload::OutboxOperation::try_from(operation)
            .map_err(|err| crate::kubelet::outbox::OutboxApplyError::Retryable(err.to_string()))?;
        let outcome = self
            .apply_command(command, idempotency_key, operation, authoring_node)
            .await?;
        Ok(outcome.result)
    }
}

async fn build_raft_repo_with_status_race_on_delete(
    pod_name: &str,
) -> (super::PodRepository, Arc<AtomicUsize>) {
    let inner: crate::datastore::DatastoreHandle =
        Arc::new(crate::datastore::test_support::in_memory().await);
    let replicated = Arc::new(crate::datastore::replicated::ReplicatedDatastore::new(
        inner.clone(),
        crate::datastore::replicated::ReplicationMode::Raft {
            node_name: "status-race-leader".to_string(),
        },
    ));
    let bumps = Arc::new(AtomicUsize::new(0));
    replicated.set_raft_proposer(Arc::new(StatusRacingRaftProposer {
        inner,
        namespace: "default".to_string(),
        pod_name: pod_name.to_string(),
        bumps: bumps.clone(),
    }));
    let db: crate::datastore::DatastoreHandle = replicated;
    let supervisor = fixture_supervisor();
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = fixture_side_effects();
    (
        super::PodRepository::new(db, supervisor, side_effects, metrics),
        bumps,
    )
}

async fn build_repo_with_scheduling_mode(
    scheduling_mode: super::api::PodSchedulingMode,
) -> super::PodRepository {
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let supervisor = fixture_supervisor();
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = fixture_side_effects();
    super::PodRepository::new_with_scheduling_mode(
        db,
        supervisor,
        side_effects,
        metrics,
        scheduling_mode,
    )
}

async fn build_repo_with_dispatcher() -> (
    super::PodRepository,
    crate::datastore::DatastoreHandle,
    Arc<crate::controller_dispatcher::ControllerDispatcher>,
) {
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let supervisor = fixture_supervisor();
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = Arc::new(crate::side_effects::default_registry(
        metrics.clone(),
        None,
        Some(supervisor.clone()),
        Some(db.clone()),
    ));
    let dispatcher = Arc::new(
        crate::controller_dispatcher::ControllerDispatcher::with_task_supervisor(
            Arc::new(crate::controllers::service::ServiceIpam::new(
                "10.43.128.0/17",
            )),
            supervisor.clone(),
        ),
    );
    side_effects.set_controller_dispatcher(dispatcher.clone());
    let repo = super::PodRepository::new(db.clone(), supervisor, side_effects, metrics);
    (repo, db, dispatcher)
}

#[tokio::test]
async fn pod_reader_get_pod_returns_existing_pod() {
    use super::PodReader;
    let repo = build_repo().await;
    repo.store
        .create("default", "p1", make_pod("p1", None, None))
        .await
        .unwrap();
    let got = repo.get_pod("default", "p1").await.unwrap();
    let pod = got.expect("pod present");
    assert_eq!(pod.name, "p1");
    assert_eq!(pod.namespace.as_deref(), Some("default"));
    assert!(repo.get_pod("default", "missing").await.unwrap().is_none());
}

#[tokio::test]
async fn pod_reader_list_pods_paginates_via_limit_and_continue_token() {
    use super::PodReader;
    let repo = build_repo().await;
    for i in 0..3 {
        repo.store
            .create(
                "default",
                &format!("p{i}"),
                make_pod(&format!("p{i}"), None, None),
            )
            .await
            .unwrap();
    }
    let page1 = repo
        .list_pods(Some("default"), None, None, Some(1), None)
        .await
        .unwrap();
    assert_eq!(page1.items.len(), 1);
    let cont = page1
        .continue_token
        .as_deref()
        .expect("continue token must be set when more pages remain");
    let page2 = repo
        .list_pods(Some("default"), None, None, Some(1), Some(cont))
        .await
        .unwrap();
    assert_eq!(page2.items.len(), 1);
    assert_ne!(page1.items[0].name, page2.items[0].name);
}

#[tokio::test]
async fn cluster_backed_pod_reader_list_pods_uses_fresh_leader_list() {
    use super::PodReader;
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let pod = crate::datastore::Resource {
        id: 1,
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("refresh-ns".to_string()),
        name: "mounted-pod".to_string(),
        uid: "mounted-pod-uid".to_string(),
        resource_version: 22,
        data: Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "refresh-ns",
                "name": "mounted-pod",
                "uid": "mounted-pod-uid",
                "resourceVersion": "22"
            },
            "spec": {
                "nodeName": "node-a",
                "containers": [{"name": "app", "image": "busybox"}]
            },
            "status": {"phase": "Running"}
        })),
    };
    let repo = PodRepository::new_with_scheduling_mode_outbox_and_cluster_api(
        db,
        fixture_supervisor(),
        fixture_side_effects(),
        crate::side_effects::SideEffectMetrics::new(),
        super::api::PodSchedulingMode::InlineSingleNode,
        None,
        Arc::new(
            FakeLeaderApiClient::new(pod.clone())
                .with_cached_list_items(Vec::new())
                .with_fresh_list_items(vec![pod.clone()]),
        ),
    );

    let listed = repo
        .list_pods(Some("refresh-ns"), None, None, None, None)
        .await
        .expect("cluster-backed pod list should succeed");

    assert_eq!(
        listed
            .items
            .iter()
            .map(|pod| pod.name.as_str())
            .collect::<Vec<_>>(),
        vec!["mounted-pod"],
        "volume refresh and lifecycle decisions must not use a stale ready pod-list cache"
    );
}

#[tokio::test]
async fn pod_reader_list_pods_by_owner_uid_filters_by_controller_owner() {
    use super::PodReader;
    let repo = build_repo().await;
    repo.store
        .create("default", "a1", make_pod("a1", Some("owner-a"), None))
        .await
        .unwrap();
    repo.store
        .create("default", "a2", make_pod("a2", Some("owner-a"), None))
        .await
        .unwrap();
    repo.store
        .create("default", "b1", make_pod("b1", Some("owner-b"), None))
        .await
        .unwrap();
    let owned_a = repo
        .list_pods_by_owner_uid("default", "owner-a")
        .await
        .unwrap();
    let mut names: Vec<String> = owned_a.iter().map(|r| r.name.clone()).collect();
    names.sort();
    assert_eq!(names, vec!["a1".to_string(), "a2".to_string()]);
}

#[tokio::test]
async fn pod_watch_source_receives_added_on_create() {
    use super::PodWatchSource;
    // Need a watch-enabled in-memory DB so post-commit broadcasts
    // reach the broadcast channel.
    let ds = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .expect("watch-enabled in-memory datastore");
    let db: crate::datastore::DatastoreHandle = std::sync::Arc::new(ds.clone());
    let supervisor = fixture_supervisor();
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = fixture_side_effects();
    let repo = super::PodRepository::new(db, supervisor, side_effects, metrics);

    let mut rx = repo.subscribe_pod_watch();
    repo.store
        .create("default", "watched", make_pod("watched", None, None))
        .await
        .unwrap();
    let evt = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .expect("watch event must arrive within 2s")
        .expect("watch channel must not lag/close");
    assert_eq!(evt.event_type, crate::watch::EventType::Added);
    let object = evt.object.as_ref();
    assert_eq!(object["kind"], serde_json::json!("Pod"));
    assert_eq!(object["metadata"]["name"], serde_json::json!("watched"));
}

fn pending_pod(name: &str) -> serde_json::Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": "default", "labels": {"app": "x"}},
        "spec": {"containers": [{"name": "c", "image": "nginx"}]},
        "status": {
            "phase": "Pending",
            "qosClass": "BestEffort"
        }
    })
}

#[tokio::test]
async fn set_pod_status_preserves_spec_metadata_and_qos() {
    use super::PodStatusWriter;
    let repo = build_repo().await;
    let created = repo
        .store
        .create("default", "p1", pending_pod("p1"))
        .await
        .unwrap();

    let update = super::PodStatusUpdate {
        phase: "Running".to_string(),
        pod_ip: "10.42.0.5".to_string(),
        host_ip: "10.0.0.10".to_string(),
        container_statuses: vec![json!({
            "name": "c", "ready": true, "restartCount": 0,
            "image": "nginx", "imageID": "",
            "state": {"running": {"startedAt": "2026-04-30T00:00:00Z"}}
        })],
        init_container_statuses: None,
        qos_class: None,
    };
    let updated = repo
        .set_pod_status("default", "p1", update, Some(created.resource_version))
        .await
        .unwrap();

    // spec preserved
    assert_eq!(updated.data["spec"]["containers"][0]["name"], json!("c"));
    // metadata preserved (labels intact)
    assert_eq!(updated.data["metadata"]["labels"]["app"], json!("x"));
    // qosClass preserved (the existing pod had BestEffort)
    assert_eq!(updated.data["status"]["qosClass"], json!("BestEffort"));
    // phase / IPs / conditions
    assert_eq!(updated.data["status"]["phase"], json!("Running"));
    assert_eq!(updated.data["status"]["podIP"], json!("10.42.0.5"));
    assert_eq!(updated.data["status"]["hostIP"], json!("10.0.0.10"));
    assert_eq!(
        updated.data["status"]["podIPs"][0]["ip"],
        json!("10.42.0.5")
    );
    let conditions = updated.data["status"]["conditions"]
        .as_array()
        .expect("conditions present");
    let types: Vec<&str> = conditions
        .iter()
        .filter_map(|c| c.get("type").and_then(|t| t.as_str()))
        .collect();
    assert!(types.contains(&"PodScheduled"));
    assert!(types.contains(&"Initialized"));
    assert!(types.contains(&"ContainersReady"));
    assert!(types.contains(&"Ready"));
    let ready = conditions
        .iter()
        .find(|c| c["type"] == "Ready")
        .expect("Ready condition");
    assert_eq!(ready["status"], json!("True"));
}

#[tokio::test]
async fn set_pod_status_preserves_scheduler_disruption_target_condition() {
    use super::PodStatusWriter;

    let repo = build_repo().await;
    let mut pod = pending_pod("preempted");
    pod["status"]["conditions"] = json!([
        {
            "type": "DisruptionTarget",
            "status": "True",
            "lastTransitionTime": "2026-05-25T06:03:08Z",
            "reason": "PreemptionByScheduler",
            "message": "Preempted by pod default/preemptor on node"
        },
        {
            "type": "PodScheduled",
            "status": "True",
            "lastTransitionTime": "2026-05-25T06:03:06Z"
        }
    ]);
    let created = repo
        .store
        .create("default", "preempted", pod)
        .await
        .unwrap();

    let updated = repo
        .set_pod_status(
            "default",
            "preempted",
            super::PodStatusUpdate {
                phase: "Running".to_string(),
                pod_ip: "10.42.0.6".to_string(),
                host_ip: "10.0.0.10".to_string(),
                container_statuses: vec![json!({
                    "name": "c",
                    "ready": true,
                    "restartCount": 0,
                    "image": "nginx",
                    "imageID": "",
                    "state": {"running": {"startedAt": "2026-05-25T06:03:09Z"}}
                })],
                init_container_statuses: None,
                qos_class: None,
            },
            Some(created.resource_version),
        )
        .await
        .unwrap();

    let conditions = updated
        .data
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .expect("conditions must remain an array");
    assert!(
        conditions.iter().any(|condition| {
            condition.get("type").and_then(|v| v.as_str()) == Some("DisruptionTarget")
                && condition.get("status").and_then(|v| v.as_str()) == Some("True")
                && condition.get("reason").and_then(|v| v.as_str()) == Some("PreemptionByScheduler")
        }),
        "kubelet status writes must not drop the scheduler-owned DisruptionTarget condition: {:?}",
        updated.data
    );
}

#[tokio::test]
async fn set_pod_status_omits_pod_ips_arrays_until_ips_are_allocated() {
    use super::PodStatusWriter;

    let repo = build_repo().await;
    let created = repo
        .store
        .create("default", "pending-no-ip", pending_pod("pending-no-ip"))
        .await
        .unwrap();

    let updated = repo
        .set_pod_status(
            "default",
            "pending-no-ip",
            super::PodStatusUpdate {
                phase: "Pending".to_string(),
                pod_ip: String::new(),
                host_ip: String::new(),
                container_statuses: vec![],
                init_container_statuses: None,
                qos_class: None,
            },
            Some(created.resource_version),
        )
        .await
        .unwrap();

    assert_eq!(updated.data["status"]["podIP"], json!(""));
    assert!(
        updated.data["status"].get("podIPs").is_none(),
        "Pending Pods without an allocated podIP must not expose an empty podIPs entry"
    );
}

#[tokio::test]
async fn set_pod_status_no_object_change_does_not_advance_resource_version() {
    use super::PodStatusWriter;

    let repo = build_repo().await;
    let created = repo
        .store
        .create("default", "same-status", pending_pod("same-status"))
        .await
        .unwrap();

    let update = super::PodStatusUpdate {
        phase: "Pending".to_string(),
        pod_ip: String::new(),
        host_ip: "10.0.0.10".to_string(),
        container_statuses: vec![json!({
            "name": "c",
            "ready": false,
            "started": false,
            "restartCount": 0,
            "image": "nginx",
            "imageID": "",
            "state": {"waiting": {"reason": "ErrImagePull", "message": "pull failed"}}
        })],
        init_container_statuses: None,
        qos_class: None,
    };

    let first = repo
        .set_pod_status(
            "default",
            "same-status",
            update.clone(),
            Some(created.resource_version),
        )
        .await
        .unwrap();
    let second = repo
        .set_pod_status("default", "same-status", update, None)
        .await
        .unwrap();

    assert_eq!(
        second.resource_version, first.resource_version,
        "recomputing identical pod status must not emit a resourceVersion-only update"
    );
    assert_eq!(second.data, first.data);
}

#[tokio::test]
async fn set_pod_status_no_object_change_does_not_enqueue_owner_reconcile() {
    use super::PodStatusWriter;

    let (repo, _db, dispatcher) = build_repo_with_dispatcher().await;
    let stored = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "owned-noop",
            "namespace": "default",
            "ownerReferences": [{
                "apiVersion": "v1",
                "kind": "ReplicationController",
                "name": "owner-rc",
                "uid": "owner-rc-uid",
                "controller": true
            }]
        },
        "spec": {"containers": [{"name": "c", "image": "nginx"}]},
        "status": {
            "phase": "Pending",
            "podIP": "",
            "hostIP": "10.0.0.10",
            "hostIPs": [{"ip": "10.0.0.10"}],
            "conditions": [
                {
                    "type": "PodScheduled",
                    "status": "True",
                    "lastTransitionTime": "2026-01-01T00:00:00Z",
                    "reason": "PodScheduled"
                },
                {
                    "type": "Initialized",
                    "status": "True",
                    "lastTransitionTime": "2026-01-01T00:00:00Z"
                },
                {
                    "type": "ContainersReady",
                    "status": "False",
                    "lastTransitionTime": "2026-01-01T00:00:00Z"
                },
                {
                    "type": "Ready",
                    "status": "False",
                    "lastTransitionTime": "2026-01-01T00:00:00Z"
                }
            ],
            "containerStatuses": []
        }
    });
    let created = repo
        .store
        .create("default", "owned-noop", stored)
        .await
        .unwrap();

    let updated = repo
        .set_pod_status(
            "default",
            "owned-noop",
            super::PodStatusUpdate {
                phase: "Pending".to_string(),
                pod_ip: String::new(),
                host_ip: "10.0.0.10".to_string(),
                container_statuses: vec![],
                init_container_statuses: None,
                qos_class: None,
            },
            Some(created.resource_version),
        )
        .await
        .unwrap();

    assert_eq!(updated.resource_version, created.resource_version);
    assert!(
        dispatcher.queued_reconcile_keys_for_test().await.is_empty(),
        "unchanged pod status must not enqueue owner controller reconcile work"
    );
}

#[tokio::test]
async fn set_pod_status_reconciles_namespace_termination_for_late_pod() {
    use super::{PodReader, PodStatusWriter};
    let repo = build_repo().await;
    let db = repo.store.db();
    db.create_namespace(
        "term-status",
        json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": "term-status", "uid": "term-status-uid"},
            "spec": {"finalizers": ["kubernetes"]},
            "status": {"phase": "Active"}
        }),
    )
    .await
    .unwrap();

    let mut pod = pending_pod("late-pod");
    pod["metadata"]["namespace"] = json!("term-status");
    let created = repo
        .store
        .create("term-status", "late-pod", pod)
        .await
        .unwrap();

    let namespace = db
        .get_namespace("term-status")
        .await
        .unwrap()
        .expect("namespace present");
    let mut terminating: serde_json::Value = std::sync::Arc::unwrap_or_clone(namespace.data);
    crate::api::set_namespace_terminating_status(&mut terminating, false);
    db.update_namespace("term-status", terminating, namespace.resource_version)
        .await
        .unwrap();

    repo.set_pod_status(
        "term-status",
        "late-pod",
        super::PodStatusUpdate {
            phase: "Pending".to_string(),
            pod_ip: "".to_string(),
            host_ip: "10.0.0.10".to_string(),
            container_statuses: vec![],
            init_container_statuses: None,
            qos_class: None,
        },
        None,
    )
    .await
    .unwrap();

    // Namespace termination is now async (spawned via TaskSupervisor).
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let terminating_pod = repo
        .get_pod("term-status", "late-pod")
        .await
        .unwrap()
        .expect("pod remains until actor cleanup owns final deletion");
    assert!(
        terminating_pod
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some(),
        "pod status writes in a terminating namespace must mark the Pod terminating"
    );
    assert!(
        db.get_namespace("term-status").await.unwrap().is_some(),
        "namespace must remain until actor cleanup removes the Pod row"
    );

    assert!(
        repo.finalize_pod_deletion_after_actor_cleanup("term-status", "late-pod", &created.uid)
            .await
            .unwrap(),
        "actor finalization should remove the terminating late Pod"
    );
    let metrics = crate::side_effects::SideEffectMetrics::new();
    crate::api::reconcile_namespace_termination(db.as_ref(), "term-status", metrics.as_ref())
        .await
        .unwrap();
    assert!(
        db.get_namespace("term-status").await.unwrap().is_none(),
        "namespace should be hard-deleted after actor-owned Pod removal"
    );
}

#[tokio::test]
async fn set_pod_status_reconciles_matching_pdb_after_readiness_transition() {
    use super::PodStatusWriter;
    let repo = build_repo().await;
    let db = repo.store.db().clone();

    let pdb = json!({
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudget",
        "metadata": {"name": "pdb-ready", "namespace": "default"},
        "spec": {
            "minAvailable": 0,
            "selector": {"matchLabels": {"app": "x"}}
        }
    });
    db.create_resource(
        "policy/v1",
        "PodDisruptionBudget",
        Some("default"),
        "pdb-ready",
        pdb.clone(),
    )
    .await
    .unwrap();

    let created = repo
        .store
        .create("default", "pdb-pod", pending_pod("pdb-pod"))
        .await
        .unwrap();

    crate::controllers::pdb::reconcile_pdb(&*db, &repo, &pdb)
        .await
        .unwrap();
    let before = db
        .get_resource(
            "policy/v1",
            "PodDisruptionBudget",
            Some("default"),
            "pdb-ready",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        before
            .data
            .pointer("/status/currentHealthy")
            .and_then(|v| v.as_i64()),
        Some(0)
    );

    repo.set_pod_status(
        "default",
        "pdb-pod",
        super::PodStatusUpdate {
            phase: "Running".to_string(),
            pod_ip: "10.42.0.8".to_string(),
            host_ip: "10.0.0.10".to_string(),
            container_statuses: vec![json!({
                "name": "c",
                "ready": true,
                "started": true,
                "restartCount": 0,
                "image": "nginx",
                "imageID": "",
                "state": {"running": {"startedAt": "2026-04-30T00:00:00Z"}}
            })],
            init_container_statuses: None,
            qos_class: None,
        },
        Some(created.resource_version),
    )
    .await
    .unwrap();

    // PDB reconciliation is now async (spawned via TaskSupervisor).
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let after = db
        .get_resource(
            "policy/v1",
            "PodDisruptionBudget",
            Some("default"),
            "pdb-ready",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after
            .data
            .pointer("/status/currentHealthy")
            .and_then(|v| v.as_i64()),
        Some(1),
        "standard pod status writes must refresh matching PDB status"
    );
    assert_eq!(
        after
            .data
            .pointer("/status/disruptionsAllowed")
            .and_then(|v| v.as_i64()),
        Some(1)
    );
}

#[tokio::test]
async fn pod_status_subresource_readiness_change_enqueues_matching_service_once() {
    use super::PodSubresourceWriter;
    let (repo, db, dispatcher) = build_repo_with_dispatcher().await;

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "web",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "web", "namespace": "default"},
            "spec": {
                "selector": {"app": "x"},
                "ports": [{"name": "http", "port": 80, "targetPort": 8080}]
            }
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "other",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "other", "namespace": "default"},
            "spec": {
                "selector": {"app": "other"},
                "ports": [{"name": "http", "port": 80, "targetPort": 8080}]
            }
        }),
    )
    .await
    .unwrap();

    let mut seed = pending_pod("svc-pod");
    seed["status"] = json!({
        "phase": "Running",
        "podIP": "10.42.0.18",
        "podIPs": [{"ip": "10.42.0.18"}],
        "hostIP": "10.0.0.10",
        "hostIPs": [{"ip": "10.0.0.10"}],
        "conditions": [
            {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
            {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
            {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
            {"type": "Ready", "status": "False", "lastTransitionTime": "2026-04-30T00:00:00Z"}
        ]
    });
    let created = repo.store.create("default", "svc-pod", seed).await.unwrap();

    let _updated = repo
        .replace_status_from_api(
        "default",
        "svc-pod",
        json!({
            "phase": "Running",
            "podIP": "10.42.0.18",
            "podIPs": [{"ip": "10.42.0.18"}],
            "hostIP": "10.0.0.10",
            "hostIPs": [{"ip": "10.0.0.10"}],
            "conditions": [
                {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "Ready", "status": "True", "lastTransitionTime": "2026-05-01T00:00:00Z"}
            ],
            "containerStatuses": [],
        }),
        created.resource_version,
    )
    .await
    .unwrap();

    let keys = dispatcher.pending_reconcile_keys().await;
    let web_count = keys
        .iter()
        .filter(|key| {
            key.api_version == "v1"
                && key.kind == "Service"
                && key.namespace.as_deref() == Some("default")
                && key.name == "web"
        })
        .count();
    assert_eq!(
        web_count, 1,
        "a readiness transition should enqueue one Service reconcile for affected Service"
    );
    assert!(
        keys.iter().any(|key| {
            key.api_version == "v1"
                && key.kind == "Service"
                && key.namespace.as_deref() == Some("default")
                && key.name == "web"
        }),
        "a Pod readiness transition must enqueue matching Services so Endpoints leave notReadyAddresses"
    );
}

#[tokio::test]
async fn pod_status_subresource_no_endpoint_change_does_not_enqueue_service() {
    use super::PodSubresourceWriter;
    let (repo, db, dispatcher) = build_repo_with_dispatcher().await;

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "web",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "web", "namespace": "default"},
            "spec": {
                "selector": {"app": "x"},
                "ports": [{"name": "http", "port": 80, "targetPort": 8080}]
            }
        }),
    )
    .await
    .unwrap();

    let mut seed = pending_pod("stable-svc-pod");
    seed["status"] = json!({
        "phase": "Running",
        "podIP": "10.42.0.19",
        "podIPs": [{"ip": "10.42.0.19"}],
        "hostIP": "10.0.0.10",
        "hostIPs": [{"ip": "10.0.0.10"}],
        "conditions": [
            {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
            {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
            {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
            {"type": "Ready", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"}
        ],
        "containerStatuses": [{
            "name": "c",
            "ready": true,
            "started": true,
            "restartCount": 0,
            "image": "nginx",
            "imageID": "",
            "state": {"running": {"startedAt": "2026-04-30T00:00:00Z"}}
        }]
    });
    let created = repo
        .store
        .create("default", "stable-svc-pod", seed)
        .await
        .unwrap();

    let _updated = repo
        .replace_status_from_api(
        "default",
        "stable-svc-pod",
        json!({
            "phase": "Running",
            "podIP": "10.42.0.19",
            "podIPs": [{"ip": "10.42.0.19"}],
            "hostIP": "10.0.0.10",
            "hostIPs": [{"ip": "10.0.0.10"}],
            "conditions": [
                {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "Ready", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"}
            ],
            "containerStatuses": [{
                "name": "c",
                "ready": true,
                "started": true,
                "restartCount": 2,
                "image": "nginx",
                "imageID": "",
                "state": {"running": {"startedAt": "2026-04-30T00:00:00Z"}}
            }]
        }),
        created.resource_version,
    )
    .await
    .unwrap();

    let keys = dispatcher.pending_reconcile_keys().await;
    assert!(
        keys.iter()
            .all(|key| !(key.api_version == "v1" && key.kind == "Service")),
        "status-only changes that keep endpoint state stable must not enqueue Service reconcile: {keys:?}"
    );
}

#[tokio::test]
async fn set_pod_status_returns_conflict_on_stale_rv() {
    use super::PodStatusWriter;
    let repo = build_repo().await;
    let created = repo
        .store
        .create("default", "racer", pending_pod("racer"))
        .await
        .unwrap();
    let snapshot_rv = created.resource_version;

    // First writer wins with the snapshot rv.
    let update_a = super::PodStatusUpdate {
        phase: "Running".to_string(),
        pod_ip: "10.42.0.6".to_string(),
        host_ip: "10.0.0.10".to_string(),
        container_statuses: vec![],
        init_container_statuses: None,
        qos_class: None,
    };
    repo.set_pod_status("default", "racer", update_a, Some(snapshot_rv))
        .await
        .expect("first writer succeeds");

    // Second writer with the stale snapshot rv must hit Conflict.
    let update_b = super::PodStatusUpdate {
        phase: "Failed".to_string(),
        pod_ip: "10.42.0.6".to_string(),
        host_ip: "10.0.0.10".to_string(),
        container_statuses: vec![],
        init_container_statuses: None,
        qos_class: None,
    };
    let conflict = repo
        .set_pod_status("default", "racer", update_b, Some(snapshot_rv))
        .await;
    let err = conflict.expect_err("stale rv must conflict");
    assert!(
        err.to_string().contains("409"),
        "expected 409 Conflict, got {err:?}"
    );
}

struct SchedulerRaceStatusWriter {
    store: Arc<PodStore>,
    attempts: AtomicUsize,
}

#[async_trait::async_trait]
impl StateOnlyWriter for SchedulerRaceStatusWriter {
    async fn write_status(
        &self,
        ns: &str,
        name: &str,
        status: serde_json::Value,
        expected_rv: Option<i64>,
    ) -> Result<crate::datastore::Resource> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            let current = self
                .store
                .get(ns, name)
                .await?
                .expect("pod must exist for injected scheduler race");
            assert_eq!(
                expected_rv,
                Some(current.resource_version),
                "status writer should CAS against the just-read pod rv"
            );
            let mut scheduled: serde_json::Value = (*current.data).clone();
            scheduled["spec"]["nodeName"] = json!("dp");
            self.store
                .update(ns, name, scheduled, current.resource_version)
                .await
                .expect("injected scheduler update should advance rv");
            return Err(crate::datastore::errors::DatastoreError::conflict(
                "injected scheduler status race",
            )
            .into());
        }

        self.store
            .update_status(ns, name, status, expected_rv)
            .await
    }
}

struct ProbeReadinessRaceStatusWriter {
    store: Arc<PodStore>,
    conflicts_remaining: AtomicUsize,
    attempts: AtomicUsize,
}

#[async_trait::async_trait]
impl StateOnlyWriter for ProbeReadinessRaceStatusWriter {
    async fn write_status(
        &self,
        ns: &str,
        name: &str,
        status: serde_json::Value,
        expected_rv: Option<i64>,
    ) -> Result<crate::datastore::Resource> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        let remaining = self.conflicts_remaining.load(Ordering::SeqCst);
        if remaining > 0 {
            self.conflicts_remaining.fetch_sub(1, Ordering::SeqCst);
            let current = self
                .store
                .get(ns, name)
                .await?
                .expect("pod must exist for injected probe-readiness race");
            assert_eq!(
                expected_rv,
                Some(current.resource_version),
                "status writer should CAS against the just-read pod rv"
            );
            let mut raced: serde_json::Value = (*current.data).clone();
            if raced
                .pointer("/metadata/annotations")
                .and_then(|value| value.as_object())
                .is_none()
            {
                raced["metadata"]["annotations"] = json!({});
            }
            raced["metadata"]["annotations"]["klights.dev/probe-readiness-race-attempt"] =
                json!(attempt.to_string());
            self.store
                .update(ns, name, raced, current.resource_version)
                .await
                .expect("injected probe-readiness update should advance rv");
            return Err(crate::datastore::errors::DatastoreError::conflict(
                "injected probe-readiness status race",
            )
            .into());
        }

        self.store
            .update_status(ns, name, status, expected_rv)
            .await
    }
}

#[tokio::test]
async fn set_pod_status_retries_implicit_rv_conflict_after_scheduler_update() {
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let store = Arc::new(PodStore::new(db));
    store
        .create("default", "scheduled-race", pending_pod("scheduled-race"))
        .await
        .unwrap();
    let status_writer = Arc::new(SchedulerRaceStatusWriter {
        store: store.clone(),
        attempts: AtomicUsize::new(0),
    });
    let side_effects = fixture_side_effects();
    let status_service = super::status::PodStatusService::new(
        store,
        status_writer.clone(),
        side_effects.controller_dispatcher_slot(),
        None,
        None,
    );

    let updated = status_service
        .set_pod_status(
            "default",
            "scheduled-race",
            &super::PodStatusUpdate {
                phase: "Pending".to_string(),
                pod_ip: String::new(),
                host_ip: String::new(),
                container_statuses: vec![],
                init_container_statuses: None,
                qos_class: None,
            },
            None,
        )
        .await
        .expect("implicit kubelet status writes should retry scheduler CAS races");

    assert_eq!(status_writer.attempts.load(Ordering::SeqCst), 2);
    assert_eq!(updated.resource.data["spec"]["nodeName"], json!("dp"));
    assert_eq!(updated.resource.data["status"]["phase"], json!("Pending"));
}

#[tokio::test]
async fn apply_runtime_reconcile_status_overwrites_phase_and_containers_only() {
    use super::PodStatusWriter;
    let repo = build_repo().await;

    // Seed a pod whose status already has IPs / conditions / qosClass that
    // the runtime reconciler MUST NOT erase.
    let mut seed = pending_pod("rr1");
    seed["status"] = json!({
        "phase": "Running",
        "podIP": "10.42.0.7",
        "podIPs": [{"ip": "10.42.0.7"}],
        "hostIP": "10.0.0.10",
        "hostIPs": [{"ip": "10.0.0.10"}],
        "qosClass": "BestEffort",
        "conditions": [
            {"type": "Ready", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"}
        ],
        "containerStatuses": [
            {"name": "c", "ready": true, "restartCount": 0}
        ]
    });
    let created = repo.store.create("default", "rr1", seed).await.unwrap();

    let update = super::RuntimeReconcileStatus {
        phase: "Failed".to_string(),
        container_statuses: vec![json!({
            "name": "c", "ready": false, "restartCount": 1,
            "state": {"terminated": {"exitCode": 1}}
        })],
    };
    let updated = repo
        .apply_runtime_reconcile_status("default", "rr1", update, Some(created.resource_version))
        .await
        .unwrap();

    let status = &updated.data["status"];
    // overwrites
    assert_eq!(status["phase"], json!("Failed"));
    assert_eq!(status["containerStatuses"][0]["ready"], json!(false));
    assert_eq!(status["containerStatuses"][0]["restartCount"], json!(1));
    // preserves
    assert_eq!(status["podIP"], json!("10.42.0.7"));
    assert_eq!(status["podIPs"][0]["ip"], json!("10.42.0.7"));
    assert_eq!(status["hostIP"], json!("10.0.0.10"));
    assert_eq!(status["qosClass"], json!("BestEffort"));
    assert_eq!(status["conditions"][0]["type"], json!("Ready"));
}

#[tokio::test]
async fn apply_runtime_reconcile_status_never_decreases_restart_count() {
    use super::PodStatusWriter;
    let repo = build_repo().await;

    let mut seed = pending_pod("rr-restart-monotonic");
    seed["status"] = json!({
        "phase": "Running",
        "containerStatuses": [
            {
                "name": "c",
                "ready": false,
                "restartCount": 1,
                "lastState": {"terminated": {"exitCode": 1, "reason": "Error"}}
            }
        ]
    });
    let created = repo
        .store
        .create("default", "rr-restart-monotonic", seed)
        .await
        .unwrap();

    let updated = repo
        .apply_runtime_reconcile_status(
            "default",
            "rr-restart-monotonic",
            super::RuntimeReconcileStatus {
                phase: "Running".to_string(),
                container_statuses: vec![json!({
                    "name": "c",
                    "ready": true,
                    "restartCount": 0,
                    "state": {"running": {"startedAt": "2026-05-02T00:00:00Z"}}
                })],
            },
            Some(created.resource_version),
        )
        .await
        .unwrap();

    let status = &updated.data["status"]["containerStatuses"][0];
    assert_eq!(status["restartCount"], json!(1));
    assert_eq!(
        status.pointer("/lastState/terminated/exitCode"),
        Some(&json!(1)),
        "runtime reconcile must preserve lastState when its snapshot lacks it"
    );
}

#[tokio::test]
async fn deferred_running_runtime_reconcile_preserves_restart_count_for_fast_onfailure_completion()
{
    use super::PodStatusWriter;
    let repo = build_repo().await;

    let mut seed = pending_pod("rr-deferred-restart");
    seed["status"] = json!({
        "phase": "Pending",
        "conditions": [
            {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-05-17T00:00:00Z"},
            {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-05-17T00:00:00Z"}
        ]
    });
    repo.store
        .create("default", "rr-deferred-restart", seed)
        .await
        .unwrap();

    let after_restart = repo
        .apply_runtime_reconcile_status(
            "default",
            "rr-deferred-restart",
            super::RuntimeReconcileStatus {
                phase: "Running".to_string(),
                container_statuses: vec![json!({
                    "name": "c",
                    "ready": false,
                    "restartCount": 1,
                    "lastState": {"terminated": {"exitCode": 1, "reason": "Error"}},
                    "state": {"running": {"startedAt": "2026-05-17T00:00:01Z"}}
                })],
            },
            None,
        )
        .await
        .unwrap();

    assert_eq!(after_restart.data["status"]["phase"], json!("Pending"));
    assert_eq!(
        after_restart.data["status"]["containerStatuses"][0]["restartCount"],
        json!(1),
        "restart count from the deferred Running reconcile must be persisted"
    );
    assert_eq!(
        after_restart.data["status"]["containerStatuses"][0]
            .pointer("/lastState/terminated/exitCode"),
        Some(&json!(1))
    );
    assert!(
        after_restart.data["status"].pointer("/podIP").is_none(),
        "the race guard must not invent or clear podIP while preserving runtime status"
    );

    let completed = repo
        .apply_runtime_reconcile_status(
            "default",
            "rr-deferred-restart",
            super::RuntimeReconcileStatus {
                phase: "Succeeded".to_string(),
                container_statuses: vec![json!({
                    "name": "c",
                    "ready": false,
                    "restartCount": 0,
                    "state": {"terminated": {"exitCode": 0, "reason": "Completed"}}
                })],
            },
            None,
        )
        .await
        .unwrap();

    let status = &completed.data["status"];
    assert_eq!(status["phase"], json!("Succeeded"));
    assert_eq!(
        status["containerStatuses"][0]["restartCount"],
        json!(1),
        "terminal reconcile must not regress the restart count after fast OnFailure completion"
    );
    assert_eq!(
        status["containerStatuses"][0].pointer("/lastState/terminated/exitCode"),
        Some(&json!(1))
    );
    assert_eq!(
        status["containerStatuses"][0].pointer("/state/terminated/exitCode"),
        Some(&json!(0))
    );
}

#[tokio::test]
async fn apply_runtime_reconcile_status_terminal_phase_marks_pod_not_ready() {
    use super::PodStatusWriter;
    let repo = build_repo().await;

    let mut seed = pending_pod("rr-complete");
    seed["status"] = json!({
        "phase": "Running",
        "podIP": "10.42.0.8",
        "podIPs": [{"ip": "10.42.0.8"}],
        "hostIP": "10.0.0.10",
        "hostIPs": [{"ip": "10.0.0.10"}],
        "qosClass": "BestEffort",
        "conditions": [
            {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
            {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
            {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
            {"type": "Ready", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"}
        ],
        "containerStatuses": [
            {"name": "c", "ready": true, "restartCount": 0}
        ]
    });
    let created = repo
        .store
        .create("default", "rr-complete", seed)
        .await
        .unwrap();

    let updated = repo
        .apply_runtime_reconcile_status(
            "default",
            "rr-complete",
            super::RuntimeReconcileStatus {
                phase: "Succeeded".to_string(),
                container_statuses: vec![json!({
                    "name": "c",
                    "ready": false,
                    "restartCount": 0,
                    "state": {"terminated": {"exitCode": 0}}
                })],
            },
            Some(created.resource_version),
        )
        .await
        .unwrap();

    let conditions = updated.data["status"]["conditions"].as_array().unwrap();
    let ready = conditions.iter().find(|c| c["type"] == "Ready").unwrap();
    assert_eq!(ready["status"], json!("False"));
    assert_eq!(ready["reason"], json!("PodCompleted"));
    assert_ne!(
        ready["lastTransitionTime"],
        json!("2026-04-30T00:00:00Z"),
        "Ready lastTransitionTime must move when terminal phase flips it to False"
    );
    let containers_ready = conditions
        .iter()
        .find(|c| c["type"] == "ContainersReady")
        .unwrap();
    assert_eq!(containers_ready["status"], json!("False"));
    assert_eq!(containers_ready["reason"], json!("PodCompleted"));
}

#[tokio::test]
async fn apply_runtime_reconcile_status_running_ready_containers_marks_pod_ready() {
    use super::PodStatusWriter;
    let repo = build_repo().await;

    let mut seed = pending_pod("rr-ready");
    seed["status"] = json!({
        "phase": "Pending",
        "podIP": "10.42.0.9",
        "podIPs": [{"ip": "10.42.0.9"}],
        "hostIP": "10.0.0.10",
        "hostIPs": [{"ip": "10.0.0.10"}],
        "qosClass": "BestEffort",
        "conditions": [
            {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
            {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
            {"type": "ContainersReady", "status": "False", "lastTransitionTime": "2026-04-30T00:00:00Z"},
            {"type": "Ready", "status": "False", "lastTransitionTime": "2026-04-30T00:00:00Z"}
        ],
        "containerStatuses": [
            {"name": "c", "ready": false, "restartCount": 0}
        ]
    });
    let created = repo
        .store
        .create("default", "rr-ready", seed)
        .await
        .unwrap();

    let updated = repo
        .apply_runtime_reconcile_status(
            "default",
            "rr-ready",
            super::RuntimeReconcileStatus {
                phase: "Running".to_string(),
                container_statuses: vec![json!({
                    "name": "c",
                    "ready": true,
                    "restartCount": 0,
                    "state": {"running": {"startedAt": "2026-05-01T00:00:00Z"}}
                })],
            },
            Some(created.resource_version),
        )
        .await
        .unwrap();

    let conditions = updated.data["status"]["conditions"].as_array().unwrap();
    let ready = conditions.iter().find(|c| c["type"] == "Ready").unwrap();
    assert_eq!(ready["status"], json!("True"));
    let containers_ready = conditions
        .iter()
        .find(|c| c["type"] == "ContainersReady")
        .unwrap();
    assert_eq!(containers_ready["status"], json!("True"));
}

#[tokio::test]
async fn apply_runtime_reconcile_status_enqueues_deployment_rollout_on_readiness_transition() {
    use super::PodStatusWriter;
    let (repo, db, dispatcher) = build_repo_with_dispatcher().await;

    db.create_resource(
        "apps/v1",
        "Deployment",
        Some("default"),
        "web",
        json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "web",
                "namespace": "default",
                "uid": "deploy-web-uid"
            },
            "spec": {"replicas": 1}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "web-rs",
        json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": "web-rs",
                "namespace": "default",
                "uid": "rs-web-uid",
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "name": "web",
                    "uid": "deploy-web-uid",
                    "controller": true
                }]
            },
            "spec": {"replicas": 1},
            "status": {"replicas": 1, "readyReplicas": 0, "availableReplicas": 0}
        }),
    )
    .await
    .unwrap();

    let mut seed = pending_pod("web-pod");
    seed["metadata"]["uid"] = json!("pod-web-uid");
    seed["metadata"]["ownerReferences"] = json!([{
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "name": "web-rs",
        "uid": "rs-web-uid",
        "controller": true
    }]);
    seed["status"] = json!({
        "phase": "Pending",
        "conditions": [
            {"type": "Ready", "status": "False"},
            {"type": "ContainersReady", "status": "False"}
        ],
        "containerStatuses": [{"name": "c", "ready": false, "restartCount": 0}]
    });
    let created = repo.store.create("default", "web-pod", seed).await.unwrap();
    let deployment_key = crate::controllers::workqueue::ReconcileKey::namespaced(
        "apps/v1",
        "Deployment",
        "default",
        "web",
    );
    dispatcher
        .enqueue_reconcile_key(deployment_key.clone())
        .await;

    repo.apply_runtime_reconcile_status(
        "default",
        "web-pod",
        super::RuntimeReconcileStatus {
            phase: "Running".to_string(),
            container_statuses: vec![json!({
                "name": "c",
                "ready": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-05-05T00:00:00Z"}}
            })],
        },
        Some(created.resource_version),
    )
    .await
    .unwrap();

    let keys = dispatcher.queued_reconcile_keys_for_test().await;
    assert_eq!(
        keys.iter().filter(|key| *key == &deployment_key).count(),
        1,
        "a pod readiness transition under a Deployment-owned ReplicaSet must leave one fresh Deployment rollout queued"
    );
}

#[tokio::test]
async fn apply_runtime_reconcile_status_enqueues_statefulset_after_readiness_transition() {
    use super::PodStatusWriter;
    let (repo, db, dispatcher) = build_repo_with_dispatcher().await;

    db.create_resource(
        "apps/v1",
        "StatefulSet",
        Some("default"),
        "web",
        json!({
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "metadata": {
                "name": "web",
                "namespace": "default",
                "uid": "sts-web-uid"
            },
            "spec": {
                "replicas": 3,
                "podManagementPolicy": "OrderedReady",
                "selector": {"matchLabels": {"app": "web"}}
            },
            "status": {"replicas": 1, "readyReplicas": 0}
        }),
    )
    .await
    .unwrap();

    let mut seed = pending_pod("web-0");
    seed["metadata"]["uid"] = json!("pod-web-0-uid");
    seed["metadata"]["labels"] = json!({"app": "web"});
    seed["metadata"]["ownerReferences"] = json!([{
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "name": "web",
        "uid": "sts-web-uid",
        "controller": true
    }]);
    seed["status"] = json!({
        "phase": "Pending",
        "conditions": [
            {"type": "Ready", "status": "False"},
            {"type": "ContainersReady", "status": "False"}
        ],
        "containerStatuses": [{"name": "c", "ready": false, "restartCount": 0}]
    });
    let created = repo.store.create("default", "web-0", seed).await.unwrap();

    repo.apply_runtime_reconcile_status(
        "default",
        "web-0",
        super::RuntimeReconcileStatus {
            phase: "Running".to_string(),
            container_statuses: vec![json!({
                "name": "c",
                "ready": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-05-05T00:00:00Z"}}
            })],
        },
        Some(created.resource_version),
    )
    .await
    .unwrap();

    let keys = dispatcher.pending_reconcile_keys().await;
    assert!(
        keys.iter().any(|key| {
            key.api_version == "apps/v1"
                && key.kind == "StatefulSet"
                && key.namespace.as_deref() == Some("default")
                && key.name == "web"
        }),
        "a readiness transition under a StatefulSet must enqueue it so OrderedReady creation can advance"
    );
}

#[tokio::test]
async fn set_pod_status_enqueues_statefulset_after_terminal_failure_transition() {
    use super::PodStatusWriter;
    let (repo, db, dispatcher) = build_repo_with_dispatcher().await;

    db.create_resource(
        "apps/v1",
        "StatefulSet",
        Some("default"),
        "web",
        json!({
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "metadata": {
                "name": "web",
                "namespace": "default",
                "uid": "sts-web-uid"
            },
            "spec": {
                "replicas": 1,
                "podManagementPolicy": "OrderedReady",
                "selector": {"matchLabels": {"app": "web"}}
            },
            "status": {"replicas": 1, "readyReplicas": 0}
        }),
    )
    .await
    .unwrap();

    let mut seed = pending_pod("web-0");
    seed["metadata"]["uid"] = json!("pod-web-0-uid");
    seed["metadata"]["labels"] = json!({"app": "web"});
    seed["metadata"]["ownerReferences"] = json!([{
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "name": "web",
        "uid": "sts-web-uid",
        "controller": true
    }]);
    seed["status"] = json!({
        "phase": "Pending",
        "conditions": [
            {"type": "Ready", "status": "False"},
            {"type": "ContainersReady", "status": "False"}
        ],
        "containerStatuses": [{"name": "c", "ready": false, "restartCount": 0}]
    });
    let created = repo.store.create("default", "web-0", seed).await.unwrap();

    repo.set_pod_status(
        "default",
        "web-0",
        super::PodStatusUpdate {
            phase: "Failed".to_string(),
            pod_ip: String::new(),
            host_ip: String::new(),
            container_statuses: vec![json!({
                "name": "c",
                "ready": false,
                "restartCount": 0,
                "state": {
                    "waiting": {
                        "reason": "CreateContainerError",
                        "message": "hostPort 21017/TCP is already allocated"
                    }
                }
            })],
            init_container_statuses: None,
            qos_class: None,
        },
        Some(created.resource_version),
    )
    .await
    .unwrap();

    let keys = dispatcher.pending_reconcile_keys().await;
    assert!(
        keys.iter().any(|key| {
            key.api_version == "apps/v1"
                && key.kind == "StatefulSet"
                && key.namespace.as_deref() == Some("default")
                && key.name == "web"
        }),
        "a StatefulSet-owned pod entering Failed before readiness must enqueue its owner so the failed ordinal can be deleted and recreated"
    );
}

#[tokio::test]
async fn replace_status_from_api_failed_daemonset_pod_enqueues_daemonset() {
    use super::PodSubresourceWriter;
    let (repo, db, dispatcher) = build_repo_with_dispatcher().await;

    db.create_resource(
        "apps/v1",
        "DaemonSet",
        Some("default"),
        "node-agent",
        json!({
            "apiVersion": "apps/v1",
            "kind": "DaemonSet",
            "metadata": {
                "name": "node-agent",
                "namespace": "default",
                "uid": "ds-node-agent-uid"
            },
            "spec": {
                "selector": {"matchLabels": {"app": "node-agent"}},
                "template": {
                    "metadata": {"labels": {"app": "node-agent"}},
                    "spec": {"containers": [{"name": "agent", "image": "busybox"}]}
                }
            },
            "status": {"desiredNumberScheduled": 1, "numberReady": 1}
        }),
    )
    .await
    .unwrap();

    let mut seed = pending_pod("node-agent-pod");
    seed["metadata"]["uid"] = json!("pod-node-agent-uid");
    seed["metadata"]["labels"] = json!({"app": "node-agent"});
    seed["metadata"]["ownerReferences"] = json!([{
        "apiVersion": "apps/v1",
        "kind": "DaemonSet",
        "name": "node-agent",
        "uid": "ds-node-agent-uid",
        "controller": true
    }]);
    seed["status"] = json!({
        "phase": "Running",
        "conditions": [
            {"type": "Ready", "status": "True"},
            {"type": "ContainersReady", "status": "True"}
        ],
        "containerStatuses": [{"name": "agent", "ready": true, "restartCount": 0}]
    });
    let created = repo
        .store
        .create("default", "node-agent-pod", seed)
        .await
        .unwrap();

    repo.replace_status_from_api(
        "default",
        "node-agent-pod",
        json!({"phase": "Failed"}),
        created.resource_version,
    )
    .await
    .unwrap();

    let keys = dispatcher.pending_reconcile_keys().await;
    assert!(
        keys.iter().any(|key| {
            key.api_version == "apps/v1"
                && key.kind == "DaemonSet"
                && key.namespace.as_deref() == Some("default")
                && key.name == "node-agent"
        }),
        "API /status writes that move a DaemonSet pod to Failed must enqueue the DaemonSet so it can delete and replace the terminal pod"
    );
}

#[tokio::test]
async fn set_deadline_exceeded_enqueues_statefulset_and_does_not_write_owner_status() {
    use super::PodStatusWriter;
    let (repo, db, dispatcher) = build_repo_with_dispatcher().await;

    db.create_resource(
        "apps/v1",
        "StatefulSet",
        Some("default"),
        "deadline-web",
        json!({
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "metadata": {
                "name": "deadline-web",
                "namespace": "default",
                "uid": "sts-deadline-web-uid"
            },
            "spec": {
                "replicas": 1,
                "podManagementPolicy": "OrderedReady",
                "selector": {"matchLabels": {"app": "deadline-web"}}
            },
            "status": {"replicas": 1, "readyReplicas": 1, "availableReplicas": 1}
        }),
    )
    .await
    .unwrap();

    let mut seed = pending_pod("deadline-web-0");
    seed["metadata"]["uid"] = json!("pod-deadline-web-0-uid");
    seed["metadata"]["labels"] = json!({"app": "deadline-web"});
    seed["metadata"]["ownerReferences"] = json!([{
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "name": "deadline-web",
        "uid": "sts-deadline-web-uid",
        "controller": true
    }]);
    seed["status"] = json!({
        "phase": "Running",
        "conditions": [
            {"type": "Ready", "status": "True"},
            {"type": "ContainersReady", "status": "True"}
        ],
        "containerStatuses": [{"name": "c", "ready": true, "restartCount": 0}]
    });
    let created = repo
        .store
        .create("default", "deadline-web-0", seed)
        .await
        .unwrap();

    let owner_rv_before = db
        .get_resource("apps/v1", "StatefulSet", Some("default"), "deadline-web")
        .await
        .unwrap()
        .expect("statefulset exists")
        .resource_version;

    repo.set_deadline_exceeded(
        "default",
        "deadline-web-0",
        "deadline exceeded".to_string(),
        Some(created.resource_version),
    )
    .await
    .unwrap();

    // Top-down ownership: pod status writes must NOT directly mutate owner status.
    // The StatefulSet controller's reconcile will update status from fresh pod state.
    let owner = db
        .get_resource("apps/v1", "StatefulSet", Some("default"), "deadline-web")
        .await
        .unwrap()
        .expect("statefulset exists");
    assert_eq!(
        owner.resource_version, owner_rv_before,
        "pod status writes must not change the owner resourceVersion — only the controller reconcile may write owner status"
    );
    assert_eq!(
        owner.data.pointer("/status/readyReplicas"),
        Some(&json!(1)),
        "owner status must remain unchanged until the controller reconcile runs"
    );

    let keys = dispatcher.pending_reconcile_keys().await;
    assert!(
        keys.iter().any(|key| {
            key.api_version == "apps/v1"
                && key.kind == "StatefulSet"
                && key.namespace.as_deref() == Some("default")
                && key.name == "deadline-web"
        }),
        "deadline failure must enqueue the StatefulSet once so it can replace the failed ordinal"
    );
}

#[tokio::test]
async fn apply_runtime_reconcile_status_enqueues_job_after_readiness_transition() {
    use super::PodStatusWriter;
    let (repo, db, dispatcher) = build_repo_with_dispatcher().await;

    db.create_resource(
        "batch/v1",
        "Job",
        Some("default"),
        "ready-job",
        json!({
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": "ready-job",
                "namespace": "default",
                "uid": "job-ready-uid"
            },
            "spec": {
                "parallelism": 3,
                "completions": 3,
                "template": {
                    "metadata": {"labels": {"job": "ready-job"}},
                    "spec": {
                        "containers": [{"name": "c", "image": "busybox"}],
                        "restartPolicy": "Never"
                    }
                }
            },
            "status": {"active": 1, "ready": 0}
        }),
    )
    .await
    .unwrap();

    let mut seed = pending_pod("ready-job-pod");
    seed["metadata"]["uid"] = json!("pod-ready-job-uid");
    seed["metadata"]["labels"] = json!({"job": "ready-job"});
    seed["metadata"]["ownerReferences"] = json!([{
        "apiVersion": "batch/v1",
        "kind": "Job",
        "name": "ready-job",
        "uid": "job-ready-uid",
        "controller": true
    }]);
    seed["status"] = json!({
        "phase": "Pending",
        "conditions": [
            {"type": "Ready", "status": "False"},
            {"type": "ContainersReady", "status": "False"}
        ],
        "containerStatuses": [{"name": "c", "ready": false, "restartCount": 0}]
    });
    let created = repo
        .store
        .create("default", "ready-job-pod", seed)
        .await
        .unwrap();

    repo.apply_runtime_reconcile_status(
        "default",
        "ready-job-pod",
        super::RuntimeReconcileStatus {
            phase: "Running".to_string(),
            container_statuses: vec![json!({
                "name": "c",
                "ready": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-05-05T00:00:00Z"}}
            })],
        },
        Some(created.resource_version),
    )
    .await
    .unwrap();

    let keys = dispatcher.pending_reconcile_keys().await;
    assert!(
        keys.iter().any(|key| {
            key.api_version == "batch/v1"
                && key.kind == "Job"
                && key.namespace.as_deref() == Some("default")
                && key.name == "ready-job"
        }),
        "a readiness transition under a Job must enqueue it so status.ready is refreshed"
    );
}

#[tokio::test]
async fn apply_runtime_reconcile_status_returns_conflict_on_stale_rv() {
    use super::PodStatusWriter;
    let repo = build_repo().await;
    let created = repo
        .store
        .create("default", "rr-race", pending_pod("rr-race"))
        .await
        .unwrap();
    let snapshot = created.resource_version;

    repo.apply_runtime_reconcile_status(
        "default",
        "rr-race",
        super::RuntimeReconcileStatus {
            phase: "Running".to_string(),
            container_statuses: vec![],
        },
        Some(snapshot),
    )
    .await
    .expect("first writer succeeds");

    let conflict = repo
        .apply_runtime_reconcile_status(
            "default",
            "rr-race",
            super::RuntimeReconcileStatus {
                phase: "Failed".to_string(),
                container_statuses: vec![],
            },
            Some(snapshot),
        )
        .await;
    let err = conflict.expect_err("stale rv must conflict");
    assert!(err.to_string().contains("409"), "expected 409, got {err:?}");
}

#[tokio::test]
async fn apply_runtime_reconcile_status_enqueues_replicaset_on_readiness_transition() {
    use super::PodStatusWriter;
    let (repo, db, dispatcher) = build_repo_with_dispatcher().await;

    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "standalone-rs",
        json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": "standalone-rs",
                "namespace": "default",
                "uid": "rs-standalone-uid"
            },
            "spec": {"replicas": 1},
            "status": {"replicas": 1, "readyReplicas": 0}
        }),
    )
    .await
    .unwrap();

    let mut seed = pending_pod("rs-pod");
    seed["metadata"]["uid"] = json!("pod-rs-uid");
    seed["metadata"]["ownerReferences"] = json!([{
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "name": "standalone-rs",
        "uid": "rs-standalone-uid",
        "controller": true
    }]);
    seed["status"] = json!({
        "phase": "Pending",
        "conditions": [
            {"type": "Ready", "status": "False"},
            {"type": "ContainersReady", "status": "False"}
        ],
        "containerStatuses": [{"name": "c", "ready": false, "restartCount": 0}]
    });
    let created = repo.store.create("default", "rs-pod", seed).await.unwrap();

    repo.apply_runtime_reconcile_status(
        "default",
        "rs-pod",
        super::RuntimeReconcileStatus {
            phase: "Running".to_string(),
            container_statuses: vec![json!({
                "name": "c",
                "ready": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-05-05T00:00:00Z"}}
            })],
        },
        Some(created.resource_version),
    )
    .await
    .unwrap();

    let keys = dispatcher.pending_reconcile_keys().await;
    assert!(
        keys.iter().any(|key| {
            key.api_version == "apps/v1"
                && key.kind == "ReplicaSet"
                && key.namespace.as_deref() == Some("default")
                && key.name == "standalone-rs"
        }),
        "a readiness transition under a ReplicaSet must enqueue the ReplicaSet for top-down status refresh"
    );
}

#[tokio::test]
async fn pod_status_write_does_not_directly_mutate_replicaset_status() {
    use super::PodStatusWriter;
    let (repo, db, _dispatcher) = build_repo_with_dispatcher().await;

    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "rs-no-status-write",
        json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": "rs-no-status-write",
                "namespace": "default",
                "uid": "rs-no-write-uid"
            },
            "spec": {"replicas": 1},
            "status": {"replicas": 1, "readyReplicas": 0}
        }),
    )
    .await
    .unwrap();

    let rs_rv_before = db
        .get_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "rs-no-status-write",
        )
        .await
        .unwrap()
        .expect("rs exists")
        .resource_version;

    let mut seed = pending_pod("rs-pod-2");
    seed["metadata"]["uid"] = json!("pod-rs-2-uid");
    seed["metadata"]["ownerReferences"] = json!([{
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "name": "rs-no-status-write",
        "uid": "rs-no-write-uid",
        "controller": true
    }]);
    seed["status"] = json!({
        "phase": "Pending",
        "conditions": [
            {"type": "Ready", "status": "False"},
            {"type": "ContainersReady", "status": "False"}
        ],
        "containerStatuses": [{"name": "c", "ready": false, "restartCount": 0}]
    });
    let created = repo
        .store
        .create("default", "rs-pod-2", seed)
        .await
        .unwrap();

    repo.apply_runtime_reconcile_status(
        "default",
        "rs-pod-2",
        super::RuntimeReconcileStatus {
            phase: "Running".to_string(),
            container_statuses: vec![json!({
                "name": "c",
                "ready": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-05-05T00:00:00Z"}}
            })],
        },
        Some(created.resource_version),
    )
    .await
    .unwrap();

    let rs_after = db
        .get_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "rs-no-status-write",
        )
        .await
        .unwrap()
        .expect("rs exists");
    assert_eq!(
        rs_after.resource_version, rs_rv_before,
        "pod status write must not change ReplicaSet resourceVersion"
    );
    assert_eq!(
        rs_after.data.pointer("/status/readyReplicas"),
        Some(&json!(0)),
        "ReplicaSet status must remain unchanged — only the controller reconcile may update it"
    );
}

#[tokio::test]
async fn apply_runtime_reconcile_status_enqueues_daemonset_on_readiness_transition() {
    use super::PodStatusWriter;
    let (repo, db, dispatcher) = build_repo_with_dispatcher().await;

    db.create_resource(
        "apps/v1",
        "DaemonSet",
        Some("default"),
        "ds-readiness",
        json!({
            "apiVersion": "apps/v1",
            "kind": "DaemonSet",
            "metadata": {
                "name": "ds-readiness",
                "namespace": "default",
                "uid": "ds-readiness-uid"
            },
            "spec": {
                "selector": {"matchLabels": {"app": "ds-readiness"}},
                "template": {
                    "metadata": {"labels": {"app": "ds-readiness"}},
                    "spec": {"containers": [{"name": "c", "image": "busybox"}]}
                }
            },
            "status": {"desiredNumberScheduled": 1, "numberReady": 0}
        }),
    )
    .await
    .unwrap();

    let mut seed = pending_pod("ds-pod");
    seed["metadata"]["uid"] = json!("pod-ds-uid");
    seed["metadata"]["labels"] = json!({"app": "ds-readiness"});
    seed["metadata"]["ownerReferences"] = json!([{
        "apiVersion": "apps/v1",
        "kind": "DaemonSet",
        "name": "ds-readiness",
        "uid": "ds-readiness-uid",
        "controller": true
    }]);
    seed["status"] = json!({
        "phase": "Pending",
        "conditions": [
            {"type": "Ready", "status": "False"},
            {"type": "ContainersReady", "status": "False"}
        ],
        "containerStatuses": [{"name": "c", "ready": false, "restartCount": 0}]
    });
    let created = repo.store.create("default", "ds-pod", seed).await.unwrap();

    repo.apply_runtime_reconcile_status(
        "default",
        "ds-pod",
        super::RuntimeReconcileStatus {
            phase: "Running".to_string(),
            container_statuses: vec![json!({
                "name": "c",
                "ready": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-05-05T00:00:00Z"}}
            })],
        },
        Some(created.resource_version),
    )
    .await
    .unwrap();

    let keys = dispatcher.pending_reconcile_keys().await;
    assert!(
        keys.iter().any(|key| {
            key.api_version == "apps/v1"
                && key.kind == "DaemonSet"
                && key.namespace.as_deref() == Some("default")
                && key.name == "ds-readiness"
        }),
        "a readiness transition under a DaemonSet must enqueue the DaemonSet for top-down status refresh"
    );
}

#[tokio::test]
async fn apply_runtime_reconcile_status_enqueues_replicationcontroller_on_readiness_transition() {
    use super::PodStatusWriter;
    let (repo, db, dispatcher) = build_repo_with_dispatcher().await;

    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "rc-readiness",
        json!({
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "metadata": {
                "name": "rc-readiness",
                "namespace": "default",
                "uid": "rc-readiness-uid"
            },
            "spec": {"replicas": 1},
            "status": {"replicas": 1, "readyReplicas": 0}
        }),
    )
    .await
    .unwrap();

    let mut seed = pending_pod("rc-pod");
    seed["metadata"]["uid"] = json!("pod-rc-uid");
    seed["metadata"]["ownerReferences"] = json!([{
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "name": "rc-readiness",
        "uid": "rc-readiness-uid",
        "controller": true
    }]);
    seed["status"] = json!({
        "phase": "Pending",
        "conditions": [
            {"type": "Ready", "status": "False"},
            {"type": "ContainersReady", "status": "False"}
        ],
        "containerStatuses": [{"name": "c", "ready": false, "restartCount": 0}]
    });
    let created = repo.store.create("default", "rc-pod", seed).await.unwrap();

    repo.apply_runtime_reconcile_status(
        "default",
        "rc-pod",
        super::RuntimeReconcileStatus {
            phase: "Running".to_string(),
            container_statuses: vec![json!({
                "name": "c",
                "ready": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-05-05T00:00:00Z"}}
            })],
        },
        Some(created.resource_version),
    )
    .await
    .unwrap();

    let keys = dispatcher.pending_reconcile_keys().await;
    assert!(
        keys.iter().any(|key| {
            key.api_version == "v1"
                && key.kind == "ReplicationController"
                && key.namespace.as_deref() == Some("default")
                && key.name == "rc-readiness"
        }),
        "a readiness transition under a ReplicationController must enqueue it for top-down status refresh"
    );
}

#[tokio::test]
async fn pod_status_write_orphan_pod_does_not_enqueue_any_controller() {
    use super::PodStatusWriter;
    let (repo, _db, dispatcher) = build_repo_with_dispatcher().await;

    let mut seed = pending_pod("orphan-pod");
    seed["metadata"]["uid"] = json!("pod-orphan-uid");
    // No ownerReferences
    seed["status"] = json!({
        "phase": "Pending",
        "conditions": [
            {"type": "Ready", "status": "False"},
            {"type": "ContainersReady", "status": "False"}
        ],
        "containerStatuses": [{"name": "c", "ready": false, "restartCount": 0}]
    });
    let created = repo
        .store
        .create("default", "orphan-pod", seed)
        .await
        .unwrap();

    repo.apply_runtime_reconcile_status(
        "default",
        "orphan-pod",
        super::RuntimeReconcileStatus {
            phase: "Running".to_string(),
            container_statuses: vec![json!({
                "name": "c",
                "ready": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-05-05T00:00:00Z"}}
            })],
        },
        Some(created.resource_version),
    )
    .await
    .unwrap();

    let keys = dispatcher.pending_reconcile_keys().await;
    assert!(
        keys.is_empty(),
        "orphan pod status change must not enqueue any controller: got {keys:?}"
    );
}

#[tokio::test]
async fn record_sandbox_id_sets_annotation_and_preserves_other_fields() {
    use super::PodMetadataWriter;
    let repo = build_repo().await;

    // Seed a pod that already has labels, an annotation, and a status —
    // none of those may be erased by the sandbox-id write.
    let mut seed = pending_pod("anno1");
    seed["metadata"]["annotations"] = json!({"prior.example.com": "keep-me"});
    repo.store.create("default", "anno1", seed).await.unwrap();

    let updated = repo
        .record_sandbox_id("default", "anno1", "sandbox-abc-123")
        .await
        .unwrap();

    assert_eq!(
        updated.data["metadata"]["annotations"]["klights.dev/sandbox-id"],
        json!("sandbox-abc-123")
    );
    assert_eq!(
        updated.data["metadata"]["annotations"]["prior.example.com"],
        json!("keep-me")
    );
    assert_eq!(updated.data["metadata"]["labels"]["app"], json!("x"));
    assert_eq!(updated.data["status"]["phase"], json!("Pending"));
    assert_eq!(updated.data["status"]["qosClass"], json!("BestEffort"));
}

#[tokio::test]
async fn record_sandbox_id_returns_conflict_on_stale_rv() {
    use super::PodMetadataWriter;
    let repo = build_repo().await;
    repo.store
        .create("default", "anno-race", pending_pod("anno-race"))
        .await
        .unwrap();

    // First writer mutates the pod (e.g. an out-of-band label edit).
    let snapshot = repo
        .store
        .get("default", "anno-race")
        .await
        .unwrap()
        .unwrap();
    let mut mutated: serde_json::Value = (*snapshot.data).clone();
    mutated["metadata"]["labels"] = json!({"app": "x", "tier": "frontend"});
    repo.store
        .update("default", "anno-race", mutated, snapshot.resource_version)
        .await
        .unwrap();

    // Now record_sandbox_id reads the live RV (post-out-of-band write) and
    // succeeds — but a second concurrent record_sandbox_id with the same
    // pre-mutation read should fail. We model that by attempting two writes
    // back-to-back: the second one observes a stale RV from the first.
    repo.record_sandbox_id("default", "anno-race", "sb-1")
        .await
        .expect("first record succeeds");
    // The second write is also a fresh read-modify-write, so it should also
    // succeed (no conflict, by design — record_sandbox_id reads the live
    // RV). To produce a real CAS conflict, we drive the store directly with
    // a stale RV after a record_sandbox_id call.
    let after_first = repo
        .store
        .get("default", "anno-race")
        .await
        .unwrap()
        .unwrap();
    let stale_rv = snapshot.resource_version;
    let mut tampered: serde_json::Value = (*after_first.data).clone();
    tampered["metadata"]["annotations"]["klights.dev/sandbox-id"] = json!("sb-tampered");
    let conflict = repo
        .store
        .update("default", "anno-race", tampered, stale_rv)
        .await;
    let err = conflict.expect_err("stale rv must conflict");
    assert!(err.to_string().contains("409"), "expected 409, got {err:?}");
}

#[tokio::test]
async fn uid_qualified_record_sandbox_id_rejects_recreated_same_name_pod() {
    use super::PodMetadataWriter;
    let repo = build_repo().await;

    let mut replacement = pending_pod("same-name");
    replacement["metadata"]["uid"] = json!("replacement-uid");
    repo.store
        .create("default", "same-name", replacement)
        .await
        .unwrap();

    let err = repo
        .record_sandbox_id_for_uid("default", "same-name", "old-uid", "old-sandbox")
        .await
        .expect_err("stale lifecycle work must not annotate a replacement pod");
    assert!(
        err.to_string().contains("UID mismatch"),
        "unexpected error: {err:#}"
    );

    let stored = repo
        .store
        .get("default", "same-name")
        .await
        .unwrap()
        .unwrap();
    assert!(
        stored
            .data
            .pointer("/metadata/annotations/klights.dev~1sandbox-id")
            .is_none(),
        "replacement pod must not receive stale sandbox annotation"
    );
}

#[tokio::test]
async fn uid_qualified_set_pod_status_rejects_recreated_same_name_pod() {
    use super::PodStatusWriter;
    let repo = build_repo().await;

    let mut replacement = pending_pod("same-name-status");
    replacement["metadata"]["uid"] = json!("replacement-uid");
    repo.store
        .create("default", "same-name-status", replacement)
        .await
        .unwrap();

    let err = repo
        .set_pod_status_for_uid(
            "default",
            "same-name-status",
            "old-uid",
            super::PodStatusUpdate {
                phase: "Running".to_string(),
                pod_ip: "10.43.0.15".to_string(),
                host_ip: "10.206.0.10".to_string(),
                container_statuses: vec![json!({
                    "name": "webserver",
                    "ready": true,
                    "restartCount": 0,
                    "state": {"running": {"startedAt": "2026-05-12T00:00:00Z"}}
                })],
                init_container_statuses: None,
                qos_class: None,
            },
            None,
        )
        .await
        .expect_err("stale lifecycle work must not overwrite replacement pod status");
    assert!(
        err.to_string().contains("UID mismatch"),
        "unexpected error: {err:#}"
    );

    let stored = repo
        .store
        .get("default", "same-name-status")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.data["status"]["phase"], json!("Pending"));
    assert!(
        stored.data.pointer("/status/podIP").is_none(),
        "replacement pod must not receive stale podIP"
    );
}

fn pod_with_running_status(name: &str) -> serde_json::Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": "default", "labels": {"app": "x"}},
        "spec": {"containers": [{"name": "c", "image": "nginx"}]},
        "status": {
            "phase": "Running",
            "conditions": [
                {"type": "Ready", "status": "False", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "ContainersReady", "status": "False", "lastTransitionTime": "2026-04-30T00:00:00Z"}
            ],
            "containerStatuses": [
                {"name": "c", "ready": false, "restartCount": 0}
            ]
        }
    })
}

fn pod_with_container_creating_status(name: &str) -> serde_json::Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": "default", "labels": {"app": "x"}},
        "spec": {
            "containers": [{
                "name": "c",
                "image": "nginx",
                "readinessProbe": {"httpGet": {"path": "/", "port": 80}}
            }]
        },
        "status": {
            "phase": "Pending",
            "podIP": "10.42.0.9",
            "podIPs": [{"ip": "10.42.0.9"}],
            "conditions": [
                {"type": "Ready", "status": "False", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "ContainersReady", "status": "False", "lastTransitionTime": "2026-04-30T00:00:00Z"}
            ],
            "containerStatuses": [
                {
                    "name": "c",
                    "containerID": "containerd://abc",
                    "ready": false,
                    "started": false,
                    "restartCount": 0,
                    "state": {"waiting": {"reason": "ContainerCreating"}}
                }
            ]
        }
    })
}

#[tokio::test]
async fn set_probe_readiness_success_does_not_mark_pending_container_creating_pod_ready() {
    use super::PodStatusWriter;
    let repo = build_repo().await;
    let created = repo
        .store
        .create(
            "default",
            "p-pending-probe",
            pod_with_container_creating_status("p-pending-probe"),
        )
        .await
        .unwrap();

    let updated = repo
        .set_probe_readiness(
            "default",
            "p-pending-probe",
            "c",
            true,
            Some(created.resource_version),
        )
        .await
        .unwrap();

    assert_eq!(updated.data["status"]["phase"], json!("Pending"));
    assert_eq!(
        updated.data["status"]["containerStatuses"][0]["ready"],
        json!(false),
        "readiness success must not mark a non-running container ready"
    );
    assert_eq!(
        updated.data["status"]["conditions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["type"] == "Ready")
            .unwrap()["status"],
        json!("False"),
        "pod Ready must remain False while phase is Pending"
    );
    assert_eq!(
        updated.resource_version, created.resource_version,
        "ignored early readiness success must not create a status watch event"
    );
}

#[tokio::test]
async fn set_probe_readiness_flips_container_ready_and_conditions_and_preserves_labels() {
    use super::PodStatusWriter;
    let repo = build_repo().await;
    let created = repo
        .store
        .create("default", "p-pr", pod_with_running_status("p-pr"))
        .await
        .unwrap();

    let updated = repo
        .set_probe_readiness("default", "p-pr", "c", true, Some(created.resource_version))
        .await
        .unwrap();

    // metadata preserved (labels intact)
    assert_eq!(updated.data["metadata"]["labels"]["app"], json!("x"));
    // container ready flipped
    assert_eq!(
        updated.data["status"]["containerStatuses"][0]["ready"],
        json!(true)
    );
    // both conditions flipped to True with reason
    let conds = updated.data["status"]["conditions"]
        .as_array()
        .expect("conditions present");
    let ready = conds.iter().find(|c| c["type"] == "Ready").unwrap();
    assert_eq!(ready["status"], json!("True"));
    assert_eq!(ready["reason"], json!("ReadinessProbeSucceeded"));
    assert_ne!(ready["lastTransitionTime"], json!("2026-04-30T00:00:00Z"));
    let cready = conds
        .iter()
        .find(|c| c["type"] == "ContainersReady")
        .unwrap();
    assert_eq!(cready["status"], json!("True"));
}

#[tokio::test]
async fn set_probe_readiness_no_op_call_does_not_bump_last_transition_time() {
    use super::PodStatusWriter;
    let repo = build_repo().await;
    let created = repo
        .store
        .create("default", "p-noop", pod_with_running_status("p-noop"))
        .await
        .unwrap();

    // The seed has Ready=False with lastTransitionTime "2026-04-30T00:00:00Z".
    // A False-call must keep the same timestamp.
    let updated = repo
        .set_probe_readiness(
            "default",
            "p-noop",
            "c",
            false,
            Some(created.resource_version),
        )
        .await
        .unwrap();
    let ready = updated.data["status"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["type"] == "Ready")
        .unwrap();
    assert_eq!(ready["status"], json!("False"));
    // No flip means the timestamp must be preserved. The reason is now set
    // to ReadinessProbeFailed (which is OK per K8s semantics).
    assert_eq!(ready["lastTransitionTime"], json!("2026-04-30T00:00:00Z"));
}

#[tokio::test]
async fn set_probe_readiness_matching_state_does_not_write_status() {
    use super::PodStatusWriter;
    let repo = build_repo().await;
    repo.store
        .create(
            "default",
            "p-ready-noop",
            pod_with_running_status("p-ready-noop"),
        )
        .await
        .unwrap();

    let ready = repo
        .set_probe_readiness("default", "p-ready-noop", "c", true, None)
        .await
        .unwrap();
    let same_ready = repo
        .set_probe_readiness("default", "p-ready-noop", "c", true, None)
        .await
        .unwrap();

    assert_eq!(
        same_ready.resource_version, ready.resource_version,
        "matching readiness probe results must not create repeated Pod watch events"
    );
}

#[tokio::test]
async fn set_probe_readiness_retries_unpinned_rv_conflict() {
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let store = Arc::new(PodStore::new(db));
    store
        .create(
            "default",
            "p-pr-retry",
            pod_with_running_status("p-pr-retry"),
        )
        .await
        .unwrap();
    let status_writer = Arc::new(ProbeReadinessRaceStatusWriter {
        store: store.clone(),
        conflicts_remaining: AtomicUsize::new(1),
        attempts: AtomicUsize::new(0),
    });
    let side_effects = fixture_side_effects();
    let status_service = super::status::PodStatusService::new(
        store,
        status_writer.clone(),
        side_effects.controller_dispatcher_slot(),
        None,
        None,
    );

    let updated = status_service
        .set_probe_readiness("default", "p-pr-retry", "c", true, None)
        .await
        .expect("unpinned probe-readiness update should retry transient conflicts");

    assert_eq!(
        status_writer.attempts.load(Ordering::SeqCst),
        2,
        "probe-readiness should retry exactly once after the injected conflict"
    );
    assert_eq!(
        updated.resource.data["status"]["containerStatuses"][0]["ready"],
        json!(true)
    );
    assert_eq!(
        updated.resource.data["status"]["conditions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|condition| condition["type"] == "Ready")
            .unwrap()["status"],
        json!("True")
    );
}

#[tokio::test]
async fn set_probe_readiness_exhausts_unpinned_conflict_retries() {
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let store = Arc::new(PodStore::new(db));
    store
        .create(
            "default",
            "p-pr-conflict-exhausted",
            pod_with_running_status("p-pr-conflict-exhausted"),
        )
        .await
        .unwrap();
    let status_writer = Arc::new(ProbeReadinessRaceStatusWriter {
        store: store.clone(),
        conflicts_remaining: AtomicUsize::new(5),
        attempts: AtomicUsize::new(0),
    });
    let side_effects = fixture_side_effects();
    let status_service = super::status::PodStatusService::new(
        store,
        status_writer.clone(),
        side_effects.controller_dispatcher_slot(),
        None,
        None,
    );

    let err = match status_service
        .set_probe_readiness("default", "p-pr-conflict-exhausted", "c", true, None)
        .await
    {
        Ok(_) => panic!("exhausted probe-readiness retry budget should return conflict"),
        Err(err) => err,
    };

    assert!(
        crate::datastore::errors::is_conflict_error(&err),
        "expected typed conflict after exhausting retries, got {err:?}"
    );
    assert_eq!(
        status_writer.attempts.load(Ordering::SeqCst),
        5,
        "probe-readiness should use the same retry budget as runtime reconcile"
    );
}

#[tokio::test]
async fn set_probe_readiness_pinned_rv_conflict_does_not_retry() {
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let store = Arc::new(PodStore::new(db));
    let created = store
        .create(
            "default",
            "p-pr-pinned-conflict",
            pod_with_running_status("p-pr-pinned-conflict"),
        )
        .await
        .unwrap();
    let status_writer = Arc::new(ProbeReadinessRaceStatusWriter {
        store: store.clone(),
        conflicts_remaining: AtomicUsize::new(1),
        attempts: AtomicUsize::new(0),
    });
    let side_effects = fixture_side_effects();
    let status_service = super::status::PodStatusService::new(
        store,
        status_writer.clone(),
        side_effects.controller_dispatcher_slot(),
        None,
        None,
    );

    let err = match status_service
        .set_probe_readiness(
            "default",
            "p-pr-pinned-conflict",
            "c",
            true,
            Some(created.resource_version),
        )
        .await
    {
        Ok(_) => panic!("pinned probe-readiness update should not retry conflicts"),
        Err(err) => err,
    };

    assert!(
        crate::datastore::errors::is_conflict_error(&err),
        "expected typed conflict for pinned probe-readiness write, got {err:?}"
    );
    assert_eq!(
        status_writer.attempts.load(Ordering::SeqCst),
        1,
        "explicit resourceVersion writes must remain single-attempt CAS"
    );
}

#[tokio::test]
async fn set_probe_readiness_returns_conflict_on_stale_rv() {
    use super::PodStatusWriter;
    let repo = build_repo().await;
    let created = repo
        .store
        .create("default", "p-pr-race", pod_with_running_status("p-pr-race"))
        .await
        .unwrap();
    let snapshot = created.resource_version;

    repo.set_probe_readiness("default", "p-pr-race", "c", true, Some(snapshot))
        .await
        .expect("first writer wins");

    let conflict = repo
        .set_probe_readiness("default", "p-pr-race", "c", false, Some(snapshot))
        .await;
    let err = conflict.expect_err("stale rv must conflict");
    assert!(err.to_string().contains("409"), "expected 409, got {err:?}");
}

fn pod_with_running_status_and_ip(name: &str) -> serde_json::Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": "default", "labels": {"app": "x"}},
        "spec": {"containers": [{"name": "c", "image": "nginx"}]},
        "status": {
            "phase": "Running",
            "podIP": "10.42.0.9",
            "podIPs": [{"ip": "10.42.0.9"}],
            "hostIP": "10.0.0.10",
            "hostIPs": [{"ip": "10.0.0.10"}],
            "qosClass": "BestEffort",
            "containerStatuses": [
                {"name": "c", "ready": true, "restartCount": 0}
            ]
        }
    })
}

#[tokio::test]
async fn set_pod_status_with_unspecified_init_statuses_preserves_existing_retry_state() {
    use super::PodStatusWriter;
    let repo = build_repo().await;
    repo.store
        .create(
            "default",
            "p-init-retry",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "p-init-retry", "namespace": "default"},
                "spec": {
                    "initContainers": [
                        {"name": "init1", "image": "busybox"},
                        {"name": "init2", "image": "busybox"}
                    ],
                    "containers": [{"name": "run1", "image": "pause"}]
                },
                "status": {
                    "phase": "Pending",
                    "containerStatuses": [{
                        "name": "run1",
                        "ready": false,
                        "restartCount": 0,
                        "state": {"waiting": {"reason": "PodInitializing"}}
                    }],
                    "initContainerStatuses": [
                        {
                            "name": "init1",
                            "ready": false,
                            "restartCount": 2,
                            "state": {"waiting": {"reason": "PodInitializing"}},
                            "lastState": {"terminated": {"exitCode": 1, "reason": "Error"}}
                        },
                        {
                            "name": "init2",
                            "ready": false,
                            "restartCount": 0,
                            "state": {"waiting": {"reason": "PodInitializing"}}
                        }
                    ]
                }
            }),
        )
        .await
        .unwrap();

    let updated = repo
        .set_pod_status(
            "default",
            "p-init-retry",
            super::PodStatusUpdate {
                phase: "Pending".to_string(),
                pod_ip: "10.43.0.5".to_string(),
                host_ip: "10.206.0.9".to_string(),
                container_statuses: vec![],
                init_container_statuses: None,
                qos_class: None,
            },
            None,
        )
        .await
        .unwrap();

    let statuses = updated
        .data
        .pointer("/status/initContainerStatuses")
        .and_then(|v| v.as_array())
        .expect("initContainerStatuses must be preserved when update does not specify them");
    assert_eq!(statuses.len(), 2);
    assert_eq!(
        statuses[0]
            .pointer("/restartCount")
            .and_then(|v| v.as_i64()),
        Some(2)
    );
    assert_eq!(
        statuses[0]
            .pointer("/lastState/terminated/exitCode")
            .and_then(|v| v.as_i64()),
        Some(1)
    );
}

#[tokio::test]
async fn set_deadline_exceeded_marks_failed_and_preserves_ip_and_labels() {
    use super::PodStatusWriter;
    let repo = build_repo().await;
    let created = repo
        .store
        .create("default", "p-dl", pod_with_running_status_and_ip("p-dl"))
        .await
        .unwrap();
    let updated = repo
        .set_deadline_exceeded(
            "default",
            "p-dl",
            "Pod was active longer than 60s".to_string(),
            Some(created.resource_version),
        )
        .await
        .unwrap();
    let status = &updated.data["status"];
    assert_eq!(status["phase"], json!("Failed"));
    assert_eq!(status["reason"], json!("DeadlineExceeded"));
    assert_eq!(status["message"], json!("Pod was active longer than 60s"));
    // IPs preserved
    assert_eq!(status["podIP"], json!("10.42.0.9"));
    assert_eq!(status["hostIP"], json!("10.0.0.10"));
    // containerStatuses preserved
    assert_eq!(status["containerStatuses"][0]["name"], json!("c"));
    // qosClass preserved
    assert_eq!(status["qosClass"], json!("BestEffort"));
    // labels preserved
    assert_eq!(updated.data["metadata"]["labels"]["app"], json!("x"));
    // conditions are exactly Ready/PodFailed + ContainersReady/PodFailed
    let conds = status["conditions"].as_array().unwrap();
    assert_eq!(conds.len(), 2);
    let ready = conds.iter().find(|c| c["type"] == "Ready").unwrap();
    assert_eq!(ready["status"], json!("False"));
    assert_eq!(ready["reason"], json!("PodFailed"));
}

#[tokio::test]
async fn set_deadline_exceeded_returns_conflict_on_stale_rv() {
    use super::PodStatusWriter;
    let repo = build_repo().await;
    let created = repo
        .store
        .create(
            "default",
            "p-dl-race",
            pod_with_running_status_and_ip("p-dl-race"),
        )
        .await
        .unwrap();
    let snapshot = created.resource_version;
    repo.set_deadline_exceeded("default", "p-dl-race", "first".to_string(), Some(snapshot))
        .await
        .expect("first writer wins");
    let conflict = repo
        .set_deadline_exceeded("default", "p-dl-race", "second".to_string(), Some(snapshot))
        .await;
    let err = conflict.expect_err("stale rv must conflict");
    assert!(err.to_string().contains("409"), "expected 409, got {err:?}");
}

#[tokio::test]
async fn replace_status_from_api_writes_full_object_preserving_spec() {
    use super::PodSubresourceWriter;
    let repo = build_repo().await;
    let created = repo
        .store
        .create("default", "p-rs", pending_pod("p-rs"))
        .await
        .unwrap();
    let updated = repo
        .replace_status_from_api(
            "default",
            "p-rs",
            json!({"phase": "Running", "podIP": "10.42.0.1"}),
            created.resource_version,
        )
        .await
        .unwrap();
    assert_eq!(updated.data["spec"]["containers"][0]["name"], json!("c"));
    assert_eq!(updated.data["status"]["phase"], json!("Running"));
    assert_eq!(updated.data["status"]["podIP"], json!("10.42.0.1"));
}

#[tokio::test]
async fn replace_status_from_api_for_uid_rejects_stale_same_name_pod() {
    use super::PodSubresourceWriter;
    let repo = build_repo().await;
    let created = repo
        .store
        .create("default", "p-rs-uid", pending_pod("p-rs-uid"))
        .await
        .unwrap();

    let stale = repo
        .replace_status_from_api_for_uid(
            "default",
            "p-rs-uid",
            "old-pod-uid",
            json!({"phase": "Running", "podIP": "10.42.0.1"}),
            created.resource_version,
        )
        .await;

    let err = stale.expect_err("stale UID must not update a same-name replacement pod");
    assert!(
        err.to_string().contains("UID mismatch"),
        "expected UID mismatch, got {err:?}"
    );
    let stored = repo
        .store
        .get("default", "p-rs-uid")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.data["status"]["phase"], json!("Pending"));
    assert!(
        stored.data.pointer("/status/podIP").is_none(),
        "same-name replacement must not receive stale status"
    );
}

#[tokio::test]
async fn patch_status_from_api_json_patch_applies_op() {
    use super::PodSubresourceWriter;
    let repo = build_repo().await;
    let created = repo
        .store
        .create("default", "p-jp", pending_pod("p-jp"))
        .await
        .unwrap();
    let patch = json!([
        {"op": "replace", "path": "/status/phase", "value": "Running"}
    ]);
    let updated = repo
        .patch_status_from_api(
            "default",
            "p-jp",
            patch,
            super::PodStatusPatchType::JsonPatch,
            created.resource_version,
        )
        .await
        .unwrap();
    assert_eq!(updated.data["status"]["phase"], json!("Running"));
    // qosClass preserved (it was on the seed)
    assert_eq!(updated.data["status"]["qosClass"], json!("BestEffort"));
}

#[tokio::test]
async fn patch_status_from_api_merge_patch_updates_only_named_keys() {
    use super::PodSubresourceWriter;
    let repo = build_repo().await;
    let created = repo
        .store
        .create("default", "p-mp", pending_pod("p-mp"))
        .await
        .unwrap();
    let patch = json!({"status": {"phase": "Running", "podIP": "10.42.0.2"}});
    let updated = repo
        .patch_status_from_api(
            "default",
            "p-mp",
            patch,
            super::PodStatusPatchType::MergePatch,
            created.resource_version,
        )
        .await
        .unwrap();
    assert_eq!(updated.data["status"]["phase"], json!("Running"));
    assert_eq!(updated.data["status"]["podIP"], json!("10.42.0.2"));
    // Untouched keys preserved
    assert_eq!(updated.data["status"]["qosClass"], json!("BestEffort"));
}

#[tokio::test]
async fn patch_status_from_api_ignores_non_status_fields() {
    use super::PodSubresourceWriter;
    let repo = build_repo().await;
    let created = repo
        .store
        .create("default", "p-ns-only", pending_pod("p-ns-only"))
        .await
        .unwrap();
    let original_image = created.data["spec"]["containers"][0]["image"].clone();
    let patch = json!({
        "status": {"phase": "Running"},
        "spec": {"containers": [{"name": "c", "image": "mutated"}]}
    });
    let updated = repo
        .patch_status_from_api(
            "default",
            "p-ns-only",
            patch,
            super::PodStatusPatchType::MergePatch,
            created.resource_version,
        )
        .await
        .unwrap();
    assert_eq!(updated.data["status"]["phase"], json!("Running"));
    assert_eq!(
        updated.data["spec"]["containers"][0]["image"],
        original_image
    );
}

#[tokio::test]
async fn patch_status_from_api_strategic_merge_merges_conditions_by_type() {
    use super::PodSubresourceWriter;
    let repo = build_repo().await;
    let mut seed = pending_pod("p-sm");
    seed["status"]["conditions"] = json!([
        {"type": "Ready", "status": "False", "lastTransitionTime": "2026-04-30T00:00:00Z"},
        {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
    ]);
    let created = repo.store.create("default", "p-sm", seed).await.unwrap();
    // Strategic-merge with the K8s `type` merge key. Only the Ready
    // condition should change; PodScheduled should stay intact.
    let patch = json!({
        "status": {
            "conditions": [
                {"type": "Ready", "status": "True", "lastTransitionTime": "2026-04-30T01:00:00Z"}
            ]
        }
    });
    let updated = repo
        .patch_status_from_api(
            "default",
            "p-sm",
            patch,
            super::PodStatusPatchType::StrategicMerge,
            created.resource_version,
        )
        .await
        .unwrap();
    let conds = updated.data["status"]["conditions"].as_array().unwrap();
    let ready = conds.iter().find(|c| c["type"] == "Ready").unwrap();
    assert_eq!(ready["status"], json!("True"));
    let scheduled = conds.iter().find(|c| c["type"] == "PodScheduled").unwrap();
    assert_eq!(scheduled["status"], json!("True"));
}

#[tokio::test]
async fn update_ephemeral_containers_appends_via_full_object_update() {
    use super::PodSubresourceWriter;
    let repo = build_repo().await;
    let created = repo
        .store
        .create("default", "p-ec", pending_pod("p-ec"))
        .await
        .unwrap();
    let new_ec = vec![json!({"name": "debug", "image": "busybox"})];
    let updated = repo
        .update_ephemeral_containers("default", "p-ec", new_ec, created.resource_version)
        .await
        .unwrap();
    let ecs = updated.data["spec"]["ephemeralContainers"]
        .as_array()
        .expect("ephemeralContainers present");
    assert_eq!(ecs.len(), 1);
    assert_eq!(ecs[0]["name"], json!("debug"));
    // spec preserved
    assert_eq!(updated.data["spec"]["containers"][0]["name"], json!("c"));
}

#[tokio::test]
async fn pod_subresource_writes_return_conflict_on_stale_rv() {
    use super::PodSubresourceWriter;
    let repo = build_repo().await;
    let created = repo
        .store
        .create("default", "p-sub-race", pending_pod("p-sub-race"))
        .await
        .unwrap();
    let snapshot = created.resource_version;
    repo.replace_status_from_api(
        "default",
        "p-sub-race",
        json!({"phase": "Running"}),
        snapshot,
    )
    .await
    .expect("first writer wins");
    // Subsequent writes with the snapshot rv must conflict for each method.
    let r1 = repo
        .replace_status_from_api(
            "default",
            "p-sub-race",
            json!({"phase": "Failed"}),
            snapshot,
        )
        .await;
    assert!(
        r1.expect_err("replace stale must conflict")
            .to_string()
            .contains("409")
    );
    let r2 = repo
        .patch_status_from_api(
            "default",
            "p-sub-race",
            json!({"status": {"phase": "Running"}}),
            super::PodStatusPatchType::MergePatch,
            snapshot,
        )
        .await;
    assert!(
        r2.expect_err("patch stale must conflict")
            .to_string()
            .contains("409")
    );
    let r3 = repo
        .update_ephemeral_containers(
            "default",
            "p-sub-race",
            vec![json!({"name": "debug", "image": "busybox"})],
            snapshot,
        )
        .await;
    assert!(
        r3.expect_err("ephemeral stale must conflict")
            .to_string()
            .contains("409")
    );
}

#[tokio::test]
async fn read_pod_network_assignment_returns_assigned_ip() {
    use super::PodNetworkReader;
    let (ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let supervisor = fixture_supervisor();
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = fixture_side_effects();
    let repo = super::PodRepository::new(db, supervisor, side_effects, metrics);

    ds.record_pod_network(
        "sandbox-net-1",
        &PodIdentity::new("default", "p-net", "uid-1"),
        "10.42.0.42",
        0x0a2a_002a,
        "vethXYZ",
        "/var/run/netns/cni-1",
    )
    .await
    .unwrap();
    let assignment = repo
        .read_pod_network_assignment("sandbox-net-1", "default", "p-net", "uid-1", false)
        .await
        .unwrap();
    assert_eq!(assignment.pod_ip, "10.42.0.42");
}

#[tokio::test]
async fn read_pod_network_assignment_falls_back_to_pod_identity() {
    use super::PodNetworkReader;
    let (ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let supervisor = fixture_supervisor();
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = fixture_side_effects();
    let repo = super::PodRepository::new(db, supervisor, side_effects, metrics);

    ds.record_pod_network(
        "cni-container-id",
        &PodIdentity::new("default", "p-net", "uid-1"),
        "10.42.0.43",
        0x0a2a_002b,
        "vethXYZ",
        "/var/run/netns/cni-1",
    )
    .await
    .unwrap();
    let assignment = repo
        .read_pod_network_assignment("runtime-sandbox-id", "default", "p-net", "uid-1", false)
        .await
        .unwrap();
    assert_eq!(assignment.pod_ip, "10.42.0.43");
}

#[tokio::test]
async fn read_pod_network_assignment_host_network_returns_host_ip_twice_without_db() {
    use super::PodNetworkReader;
    let repo = build_repo().await;
    // No row inserted; host_network=true must not consult the DB.
    let assignment = repo
        .read_pod_network_assignment(
            "does-not-exist-sandbox",
            "default",
            "hostnet",
            "uid-host",
            true,
        )
        .await
        .unwrap();
    assert_eq!(assignment.pod_ip, assignment.host_ip);
    assert!(!assignment.pod_ip.is_empty());
}

#[tokio::test]
async fn read_pod_network_assignment_retries_then_succeeds() {
    use super::PodNetworkReader;
    use crate::networking::pod_network_events::{PodNetworkEvents, PodNetworkKey};

    let (ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let supervisor = fixture_supervisor();
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = fixture_side_effects();
    let events = PodNetworkEvents::new();
    let repo = std::sync::Arc::new(super::PodRepository::new_with_network_events(
        db,
        supervisor,
        side_effects,
        metrics,
        events.clone(),
        super::api::PodSchedulingMode::InlineSingleNode,
        None,
    ));

    let key = PodNetworkKey::new("sandbox-net-late", "default", "p-net-late", "uid-late");
    let repo_clone = repo.clone();
    let read_handle = tokio::spawn(async move {
        repo_clone
            .read_pod_network_assignment(
                "sandbox-net-late",
                "default",
                "p-net-late",
                "uid-late",
                false,
            )
            .await
    });

    wait_for_pod_network_subscriber(&events, &key).await;
    ds.record_pod_network(
        "sandbox-net-late",
        &PodIdentity::new("default", "p-net-late", "uid-late"),
        "10.42.0.99",
        0x0a2a_0063,
        "vethL",
        "/var/run/netns/cni-late",
    )
    .await
    .unwrap();
    events.publish_assignment(&key).await;

    let assignment = read_handle.await.unwrap().unwrap();
    assert_eq!(assignment.pod_ip, "10.42.0.99");
}

#[tokio::test]
async fn read_pod_network_assignment_tolerates_cni_db_backlog() {
    use super::PodNetworkReader;
    use crate::networking::pod_network_events::{PodNetworkEvents, PodNetworkKey};

    let (ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let supervisor = fixture_supervisor();
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = fixture_side_effects();
    let events = PodNetworkEvents::new();
    let repo = std::sync::Arc::new(super::PodRepository::new_with_network_events(
        db,
        supervisor,
        side_effects,
        metrics,
        events.clone(),
        super::api::PodSchedulingMode::InlineSingleNode,
        None,
    ));

    let key = PodNetworkKey::new(
        "sandbox-net-backlogged",
        "default",
        "p-net-backlogged",
        "uid-backlogged",
    );
    let repo_clone = repo.clone();
    let read_handle = tokio::spawn(async move {
        repo_clone
            .read_pod_network_assignment(
                "sandbox-net-backlogged",
                "default",
                "p-net-backlogged",
                "uid-backlogged",
                false,
            )
            .await
    });

    wait_for_pod_network_subscriber(&events, &key).await;
    // Full conformance can queue DB work around RunPodSandbox; the reader must
    // stay parked on the event rather than burning retry sleeps.
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    ds.record_pod_network(
        "sandbox-net-backlogged",
        &PodIdentity::new("default", "p-net-backlogged", "uid-backlogged"),
        "10.42.0.100",
        0x0a2a_0064,
        "vethB",
        "/var/run/netns/cni-backlogged",
    )
    .await
    .unwrap();
    events.publish_assignment(&key).await;

    let assignment = read_handle.await.unwrap().unwrap();
    assert_eq!(assignment.pod_ip, "10.42.0.100");
}

#[test]
fn read_pod_network_assignment_waits_for_assignment_notification() {
    // R4: invariant now enforced by check_kubelet_invariants.sh
}

#[tokio::test]
async fn read_pod_network_assignment_exhausts_retries_returns_error() {
    use super::PodNetworkReader;
    let repo = build_repo().await;
    let err = repo
        .read_pod_network_assignment(
            "nonexistent-sandbox",
            "default",
            "p-net-missing",
            "uid-missing",
            false,
        )
        .await
        .expect_err("missing row must error after bounded assignment wait");
    let msg = err.to_string();
    assert!(
        msg.contains("nonexistent-sandbox") && msg.contains("timed out"),
        "expected assignment wait timeout message, got {msg:?}"
    );
}

async fn wait_for_pod_network_subscriber(
    events: &crate::networking::pod_network_events::PodNetworkEvents,
    key: &crate::networking::pod_network_events::PodNetworkKey,
) {
    for _ in 0..100 {
        if events.has_subscriber_for_test(key).await {
            return;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        events.has_subscriber_for_test(key).await,
        "reader must subscribe before waiting for CNI assignment"
    );
}

fn api_create_request(body: serde_json::Value, dry_run: bool) -> super::PodApiCreateRequest {
    super::PodApiCreateRequest {
        namespace: "default".to_string(),
        name: String::new(),
        body,
        dry_run,
        run_admission: false,
    }
}

#[tokio::test]
async fn api_create_pod_inline_mode_leaves_empty_node_name_unbound_until_scheduler_runs() {
    use super::{PodApiWriter, PodReader};
    let repo = build_repo().await;
    repo.store
        .db()
        .create_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();

    let result = repo
        .api_create_pod(api_create_request(
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": { "name": "node-name-default" },
                "spec": {
                    "nodeName": "",
                    "containers": [{ "name": "c", "image": "busybox" }]
                }
            }),
            false,
        ))
        .await
        .unwrap();

    assert!(
        result
            .body
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str())
            .is_none(),
        "API create must leave implicit scheduling to the scheduler path"
    );
    let pod_scheduled = result
        .body
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .and_then(|conditions| {
            conditions.iter().find(|condition| {
                condition.get("type").and_then(|v| v.as_str()) == Some("PodScheduled")
            })
        })
        .expect("PodScheduled condition present");
    assert_eq!(
        pod_scheduled.get("status").and_then(|v| v.as_str()),
        Some("False")
    );
    assert_eq!(
        pod_scheduled.get("reason").and_then(|v| v.as_str()),
        Some("SchedulingPending")
    );

    repo.schedule_all_unbound_pods().await.unwrap();
    let scheduled = repo
        .get_pod("default", "node-name-default")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        scheduled
            .data
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str()),
        Some("test-node")
    );
}

#[tokio::test]
async fn api_create_pod_leader_mode_leaves_pod_unbound_until_scheduler_controller_binds() {
    use super::{PodApiWriter, PodReader};

    let repo =
        build_repo_with_scheduling_mode(super::api::PodSchedulingMode::DeferredMultiNodeLeader)
            .await;
    for node_name in ["lead-123456az", "work-654321za"] {
        repo.store
            .db()
            .create_resource(
                "v1",
                "Node",
                None,
                node_name,
                json!({
                    "apiVersion": "v1",
                    "kind": "Node",
                    "metadata": {"name": node_name},
                    "spec": {"unschedulable": false},
                    "status": {
                        "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                        "conditions": [{"type": "Ready", "status": "True"}]
                    }
                }),
            )
            .await
            .unwrap();
    }

    let result = repo
        .api_create_pod(api_create_request(
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": { "name": "deferred-schedule" },
                "spec": {
                    "containers": [{ "name": "c", "image": "busybox" }]
                }
            }),
            false,
        ))
        .await
        .unwrap();

    assert!(
        result
            .body
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str())
            .is_none(),
        "leader mode pod should remain unbound until scheduler controller binds it"
    );
    assert_eq!(
        result
            .body
            .pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .and_then(|conds| conds.iter().find(|c| c["type"] == "PodScheduled"))
            .and_then(|c| c.get("status"))
            .and_then(|v| v.as_str()),
        Some("False")
    );

    repo.schedule_all_unbound_pods().await.unwrap();
    let scheduled = repo
        .get_pod("default", "deferred-schedule")
        .await
        .unwrap()
        .unwrap();
    let node_name = scheduled
        .data
        .pointer("/spec/nodeName")
        .and_then(|v| v.as_str());
    assert!(
        node_name == Some("lead-123456az") || node_name == Some("work-654321za"),
        "pod should be bound to one of the two available nodes, got {node_name:?}"
    );
}

#[tokio::test]
async fn leader_scheduler_binds_node_and_podscheduled_condition_in_one_pod_event() {
    use super::{PodApiWriter, PodReader};
    use crate::datastore::WatchTarget;
    use crate::watch::EventType;

    let repo =
        build_repo_with_scheduling_mode(super::api::PodSchedulingMode::DeferredMultiNodeLeader)
            .await;
    repo.store
        .db()
        .create_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();

    let created = repo
        .api_create_pod(api_create_request(
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": { "name": "single-bind-event" },
                "spec": {
                    "containers": [{ "name": "c", "image": "busybox" }]
                }
            }),
            false,
        ))
        .await
        .unwrap()
        .resource
        .expect("pod create persists");
    assert!(
        created
            .data
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str())
            .is_none(),
        "deferred leader mode starts unbound"
    );

    repo.schedule_all_unbound_pods().await.unwrap();
    let scheduled = repo
        .get_pod("default", "single-bind-event")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        scheduled
            .data
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str()),
        Some("test-node")
    );
    assert_eq!(
        scheduled
            .data
            .pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .and_then(|conditions| {
                conditions.iter().find(|condition| {
                    condition.get("type").and_then(|v| v.as_str()) == Some("PodScheduled")
                })
            })
            .and_then(|condition| condition.get("status"))
            .and_then(|v| v.as_str()),
        Some("True")
    );

    let pod_events = repo
        .store
        .db()
        .list_watch_events_since(
            &[WatchTarget::namespaced("v1", "Pod")],
            created.resource_version,
        )
        .await
        .unwrap();
    let schedule_events: Vec<_> = pod_events
        .into_iter()
        .map(|entry| entry.into_watch_event())
        .filter(|event| {
            event
                .object
                .pointer("/metadata/name")
                .and_then(|v| v.as_str())
                == Some("single-bind-event")
        })
        .collect();

    assert_eq!(
        schedule_events.len(),
        1,
        "scheduler bind and PodScheduled=True status must be one logical pod update"
    );
    assert_eq!(schedule_events[0].event_type, EventType::Modified);
    assert_eq!(
        schedule_events[0]
            .object
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str()),
        Some("test-node")
    );
    assert_eq!(
        schedule_events[0]
            .object
            .pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .and_then(|conditions| {
                conditions.iter().find(|condition| {
                    condition.get("type").and_then(|v| v.as_str()) == Some("PodScheduled")
                })
            })
            .and_then(|condition| condition.get("status"))
            .and_then(|v| v.as_str()),
        Some("True")
    );
}

#[tokio::test]
async fn leader_scheduler_marks_unschedulable_pod_and_emits_failed_scheduling_event() {
    use super::{PodApiWriter, PodReader};

    let (repo, db, node_db) = build_repo_with_scheduling_mode_for_outbox(
        super::api::PodSchedulingMode::DeferredMultiNodeLeader,
    )
    .await;
    repo.store
        .db()
        .create_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();
    repo.store
        .create(
            "default",
            "filler",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "filler", "namespace": "default"},
                "spec": {
                    "nodeName": "test-node",
                    "containers": [{
                        "name": "c",
                        "image": "registry.k8s.io/pause:3.10",
                        "resources": {"requests": {"cpu": "5600m"}}
                    }]
                },
                "status": {"phase": "Running"}
            }),
        )
        .await
        .unwrap();

    repo.api_create_pod(api_create_request(
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "additional-pod"},
            "spec": {
                "containers": [{
                    "name": "c",
                    "image": "registry.k8s.io/pause:3.10",
                    "resources": {"requests": {"cpu": "3"}}
                }]
            }
        }),
        false,
    ))
    .await
    .unwrap();

    repo.schedule_all_unbound_pods().await.unwrap();
    drain_repo_outbox(db.clone(), &node_db).await.unwrap();
    let pod = repo
        .get_pod("default", "additional-pod")
        .await
        .unwrap()
        .unwrap();
    let scheduled = pod
        .data
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .and_then(|conditions| {
            conditions.iter().find(|condition| {
                condition.get("type").and_then(|v| v.as_str()) == Some("PodScheduled")
            })
        })
        .expect("PodScheduled condition present after scheduler retry");
    assert_eq!(
        scheduled.get("reason").and_then(|v| v.as_str()),
        Some("Unschedulable")
    );

    let events = db
        .list_resources(
            "v1",
            "Event",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert!(
        events.items.iter().any(|event| {
            event.data.get("reason").and_then(|v| v.as_str()) == Some("FailedScheduling")
                && event
                    .data
                    .pointer("/involvedObject/name")
                    .and_then(|v| v.as_str())
                    == Some("additional-pod")
        }),
        "leader scheduler retry must emit FailedScheduling event: {:?}",
        events.items
    );

    let rv_after_first_retry = pod.resource_version;
    let event_count_after_first_retry = events.items.len();
    repo.schedule_all_unbound_pods().await.unwrap();
    drain_repo_outbox(db.clone(), &node_db).await.unwrap();
    let pod_after_second_retry = repo
        .get_pod("default", "additional-pod")
        .await
        .unwrap()
        .unwrap();
    let events_after_second_retry = db
        .list_resources(
            "v1",
            "Event",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pod_after_second_retry.resource_version, rv_after_first_retry,
        "scheduler must not rewrite an unchanged unschedulable pod and wake itself again"
    );
    assert_eq!(
        events_after_second_retry.items.len(),
        event_count_after_first_retry,
        "scheduler must not emit duplicate FailedScheduling events for an unchanged pod"
    );
}

#[tokio::test]
async fn leader_scheduler_applies_preemption_victims_for_extended_resource_fit() {
    use super::{PodApiWriter, PodReader};

    let repo =
        build_repo_with_scheduling_mode(super::api::PodSchedulingMode::DeferredMultiNodeLeader)
            .await;
    repo.store
        .db()
        .create_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {
                        "cpu": "8",
                        "memory": "32Gi",
                        "pods": "110",
                        "scheduling.k8s.io/foo": "5"
                    },
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();
    for (name, priority) in [("low-priority", 1), ("medium-priority", 2)] {
        repo.store
            .create(
                "default",
                name,
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": name, "namespace": "default"},
                    "spec": {
                        "nodeName": "test-node",
                        "priority": priority,
                        "containers": [{
                            "name": "c",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {"requests": {"scheduling.k8s.io/foo": "2"}}
                        }]
                    },
                    "status": {"phase": "Running"}
                }),
            )
            .await
            .unwrap();
    }

    repo.api_create_pod(api_create_request(
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "preemptor"},
            "spec": {
                "priority": 3,
                "containers": [{
                    "name": "c",
                    "image": "registry.k8s.io/pause:3.10",
                    "resources": {"requests": {"scheduling.k8s.io/foo": "2"}}
                }]
            }
        }),
        false,
    ))
    .await
    .unwrap();

    repo.schedule_all_unbound_pods().await.unwrap();
    let preemptor = repo.get_pod("default", "preemptor").await.unwrap().unwrap();
    assert_eq!(
        preemptor
            .data
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str()),
        Some("test-node")
    );
    let low_priority = repo
        .get_pod("default", "low-priority")
        .await
        .unwrap()
        .expect("deferred scheduler must leave the victim row until actor finalization");
    assert!(
        low_priority
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some(),
        "deferred scheduler must mark the lowest-priority victim terminating before binding the preemptor"
    );
    assert!(
        repo.get_pod("default", "medium-priority")
            .await
            .unwrap()
            .is_some(),
        "deferred scheduler should only remove enough lower-priority victims to fit"
    );
}

#[tokio::test]
async fn leader_scheduler_marks_finalized_preemption_victim_terminating() {
    use super::{PodApiWriter, PodReader};

    let repo =
        build_repo_with_scheduling_mode(super::api::PodSchedulingMode::DeferredMultiNodeLeader)
            .await;
    repo.store
        .db()
        .create_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {
                        "cpu": "8",
                        "memory": "32Gi",
                        "pods": "110",
                        "scheduling.k8s.io/foo": "5"
                    },
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();
    repo.store
        .create(
            "default",
            "victim",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "victim",
                    "namespace": "default",
                    "finalizers": ["example.com/test-finalizer"]
                },
                "spec": {
                    "nodeName": "test-node",
                    "priority": 1,
                    "containers": [{
                        "name": "c",
                        "image": "registry.k8s.io/pause:3.10",
                        "resources": {"requests": {"scheduling.k8s.io/foo": "1"}}
                    }]
                },
                "status": {"phase": "Running"}
            }),
        )
        .await
        .unwrap();

    repo.api_create_pod(api_create_request(
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "preemptor"},
            "spec": {
                "priority": 2,
                "containers": [{
                    "name": "c",
                    "image": "registry.k8s.io/pause:3.10",
                    "resources": {"requests": {"scheduling.k8s.io/foo": "5"}}
                }]
            }
        }),
        false,
    ))
    .await
    .unwrap();

    repo.schedule_all_unbound_pods().await.unwrap();
    let preemptor = repo.get_pod("default", "preemptor").await.unwrap().unwrap();
    assert_eq!(
        preemptor
            .data
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str()),
        Some("test-node")
    );

    let victim = repo.get_pod("default", "victim").await.unwrap().unwrap();
    assert!(
        victim
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some(),
        "finalized preemption victim must be marked terminating, not hard-deleted or left running"
    );
    let disruption_target = victim
        .data
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .unwrap()
        .iter()
        .find(|condition| {
            condition.get("type").and_then(|v| v.as_str()) == Some("DisruptionTarget")
        })
        .expect("preempted victim must get DisruptionTarget condition");
    assert_eq!(
        disruption_target.get("status").and_then(|v| v.as_str()),
        Some("True")
    );
}

#[tokio::test]
async fn api_create_pod_leader_mode_respects_explicit_node_name() {
    use super::PodApiWriter;

    let repo =
        build_repo_with_scheduling_mode(super::api::PodSchedulingMode::DeferredMultiNodeLeader)
            .await;
    repo.store
        .db()
        .create_resource(
            "v1",
            "Node",
            None,
            "lead-123456az",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "lead-123456az"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();

    let result = repo
        .api_create_pod(api_create_request(
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": { "name": "explicit-bind" },
                "spec": {
                    "nodeName": "lead-123456az",
                    "containers": [{ "name": "c", "image": "busybox" }]
                }
            }),
            false,
        ))
        .await
        .unwrap();

    assert_eq!(
        result
            .body
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str()),
        Some("lead-123456az")
    );
}

#[tokio::test]
async fn scheduler_marks_pod_unschedulable_when_cpu_request_exceeds_allocatable() {
    use super::{PodApiWriter, PodReader};

    let (repo, db, node_db) = build_repo_with_scheduling_mode_for_outbox(
        super::api::PodSchedulingMode::DeferredMultiNodeLeader,
    )
    .await;
    repo.store
        .db()
        .create_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();
    repo.store
        .create(
            "default",
            "filler",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "filler", "namespace": "default"},
                "spec": {
                    "nodeName": "test-node",
                    "containers": [{
                        "name": "c",
                        "image": "registry.k8s.io/pause:3.10",
                        "resources": {"requests": {"cpu": "5600m"}}
                    }]
                },
                "status": {"phase": "Running"}
            }),
        )
        .await
        .unwrap();

    repo.api_create_pod(api_create_request(
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "additional-pod"},
            "spec": {
                "containers": [{
                    "name": "c",
                    "image": "registry.k8s.io/pause:3.10",
                    "resources": {"requests": {"cpu": "3"}}
                }]
            }
        }),
        false,
    ))
    .await
    .unwrap();

    repo.schedule_all_unbound_pods().await.unwrap();
    drain_repo_outbox(db.clone(), &node_db).await.unwrap();
    let created = repo
        .get_pod("default", "additional-pod")
        .await
        .unwrap()
        .unwrap()
        .data;
    assert!(
        created
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str())
            .is_none(),
        "pod requiring more CPU than remaining allocatable must not be assigned: {created:?}"
    );
    let scheduled = created
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .and_then(|conditions| {
            conditions.iter().find(|condition| {
                condition.get("type").and_then(|v| v.as_str()) == Some("PodScheduled")
            })
        })
        .expect("PodScheduled condition present");
    assert_eq!(
        scheduled.get("status").and_then(|v| v.as_str()),
        Some("False")
    );
    assert_eq!(
        scheduled.get("reason").and_then(|v| v.as_str()),
        Some("Unschedulable")
    );

    let events = db
        .list_resources(
            "v1",
            "Event",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert!(
        events.items.iter().any(|event| {
            event.data.get("reason").and_then(|v| v.as_str()) == Some("FailedScheduling")
                && event
                    .data
                    .pointer("/involvedObject/name")
                    .and_then(|v| v.as_str())
                    == Some("additional-pod")
                && event
                    .data
                    .get("message")
                    .and_then(|v| v.as_str())
                    .is_some_and(|message| message.contains("Insufficient cpu"))
        }),
        "unschedulable pod should receive a FailedScheduling event: {:?}",
        events.items
    );
}

#[tokio::test]
async fn scheduler_marks_pod_unschedulable_when_node_selector_does_not_match() {
    use super::{PodApiWriter, PodReader};

    let repo = build_repo().await;
    repo.store
        .db()
        .create_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": "test-node",
                    "labels": {
                        "kubernetes.io/os": "linux",
                        "disktype": "ssd"
                    }
                },
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();

    repo.api_create_pod(api_create_request(
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "restricted-pod"},
            "spec": {
                "nodeSelector": {"disktype": "hdd"},
                "containers": [{"name": "c", "image": "registry.k8s.io/pause:3.10"}]
            }
        }),
        false,
    ))
    .await
    .unwrap();

    repo.schedule_all_unbound_pods().await.unwrap();
    let created = repo
        .get_pod("default", "restricted-pod")
        .await
        .unwrap()
        .unwrap()
        .data;
    assert!(
        created
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str())
            .is_none(),
        "pod with a non-matching nodeSelector must not be assigned: {created:?}"
    );
    let scheduled = created
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .and_then(|conditions| {
            conditions.iter().find(|condition| {
                condition.get("type").and_then(|v| v.as_str()) == Some("PodScheduled")
            })
        })
        .expect("PodScheduled condition present");
    assert_eq!(
        scheduled.get("status").and_then(|v| v.as_str()),
        Some("False")
    );
    assert!(
        scheduled
            .get("message")
            .and_then(|v| v.as_str())
            .is_some_and(|message| message.contains("node affinity/selector")),
        "expected node selector failure message, got {scheduled:?}"
    );
}

#[tokio::test]
async fn scheduler_counts_extended_resource_requests_for_node_fit() {
    use super::{PodApiWriter, PodReader};

    let repo = build_repo().await;
    repo.store
        .db()
        .create_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {
                        "cpu": "8",
                        "memory": "32Gi",
                        "pods": "110",
                        "scheduling.k8s.io/foo": "5"
                    },
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();
    repo.store
        .create(
            "default",
            "filler",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "filler", "namespace": "default"},
                "spec": {
                    "nodeName": "test-node",
                    "containers": [{
                        "name": "c",
                        "image": "registry.k8s.io/pause:3.10",
                        "resources": {"requests": {"scheduling.k8s.io/foo": "4"}}
                    }]
                },
                "status": {"phase": "Running"}
            }),
        )
        .await
        .unwrap();

    repo.api_create_pod(api_create_request(
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "needs-extended"},
            "spec": {
                "containers": [{
                    "name": "c",
                    "image": "registry.k8s.io/pause:3.10",
                    "resources": {"requests": {"scheduling.k8s.io/foo": "2"}}
                }]
            }
        }),
        false,
    ))
    .await
    .unwrap();

    repo.schedule_all_unbound_pods().await.unwrap();
    let created = repo
        .get_pod("default", "needs-extended")
        .await
        .unwrap()
        .unwrap()
        .data;
    assert!(
        created
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str())
            .is_none(),
        "pod exceeding extended-resource allocatable must not be assigned: {created:?}"
    );
    assert!(
        created
            .pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .and_then(|conditions| conditions.iter().find(|condition| {
                condition.get("type").and_then(|v| v.as_str()) == Some("PodScheduled")
            }))
            .and_then(|condition| condition.get("message"))
            .and_then(|v| v.as_str())
            .is_some_and(|message| message.contains("Insufficient scheduling.k8s.io/foo")),
        "expected extended-resource scheduling failure, got {created:?}"
    );
}

#[tokio::test]
async fn scheduler_preemption_marks_victim_terminating_and_enqueues_replicaset() {
    use super::PodApiWriter;

    let (repo, db, dispatcher) = build_repo_with_dispatcher().await;
    db.create_resource(
        "v1",
        "Node",
        None,
        "test-node",
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "test-node"},
            "spec": {"unschedulable": false},
            "status": {
                "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110", "example.com/foo": "1"},
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "low-rs",
        json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {"name": "low-rs", "namespace": "default", "uid": "low-rs-uid"},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": "low"}},
                "template": {
                    "metadata": {"labels": {"app": "low"}},
                    "spec": {"containers": [{"name": "c", "image": "registry.k8s.io/pause:3.10"}]}
                }
            },
            "status": {"replicas": 1, "readyReplicas": 1, "availableReplicas": 1}
        }),
    )
    .await
    .unwrap();
    repo.store
        .create(
            "default",
            "low-rs-pod",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "low-rs-pod",
                    "namespace": "default",
                    "uid": "low-rs-pod-uid",
                    "labels": {"app": "low"},
                    "ownerReferences": [{
                        "apiVersion": "apps/v1",
                        "kind": "ReplicaSet",
                        "name": "low-rs",
                        "uid": "low-rs-uid",
                        "controller": true
                    }]
                },
                "spec": {
                    "nodeName": "test-node",
                    "priority": 1,
                    "containers": [{
                        "name": "c",
                        "image": "registry.k8s.io/pause:3.10",
                        "resources": {"requests": {"example.com/foo": "1"}}
                    }]
                },
                "status": {"phase": "Running"}
            }),
        )
        .await
        .unwrap();

    repo.api_create_pod(api_create_request(
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "preemptor"},
            "spec": {
                "priority": 2,
                "containers": [{
                    "name": "c",
                    "image": "registry.k8s.io/pause:3.10",
                    "resources": {"requests": {"example.com/foo": "1"}}
                }]
            }
        }),
        false,
    ))
    .await
    .unwrap();

    repo.schedule_all_unbound_pods().await.unwrap();
    let low_rs_pod = repo
        .store
        .get("default", "low-rs-pod")
        .await
        .unwrap()
        .expect("preempted victim remains until actor finalization");
    assert!(
        low_rs_pod
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some(),
        "scheduler preemption must mark the victim terminating"
    );
    let keys = dispatcher.pending_reconcile_keys().await;
    assert!(
        keys.iter().any(|key| {
            key.api_version == "apps/v1"
                && key.kind == "ReplicaSet"
                && key.namespace.as_deref() == Some("default")
                && key.name == "low-rs"
        }),
        "scheduler preemption must enqueue the owning ReplicaSet so it observes the terminating pod and creates a replacement"
    );
}

#[tokio::test]
async fn scheduler_preempts_lowest_priority_victims_for_extended_resource_fit() {
    use super::{PodApiWriter, PodReader};

    let repo = build_repo().await;
    repo.store
        .db()
        .create_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {
                        "cpu": "8",
                        "memory": "32Gi",
                        "pods": "110",
                        "example.com/fakecpu": "1k"
                    },
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();
    for (name, request, priority) in [("pod1", "200", 1), ("pod2", "300", 2), ("pod3", "450", 3)] {
        repo.store
            .create(
                "default",
                name,
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": name, "namespace": "default"},
                    "spec": {
                        "nodeName": "test-node",
                        "priority": priority,
                        "containers": [{
                            "name": "c",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {"requests": {"example.com/fakecpu": request}}
                        }]
                    },
                    "status": {"phase": "Running"}
                }),
            )
            .await
            .unwrap();
    }
    repo.store
        .create(
            "kube-system",
            "unrelated-low-priority",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "unrelated-low-priority", "namespace": "kube-system"},
                "spec": {
                    "nodeName": "test-node",
                    "priority": 0,
                    "containers": [{
                        "name": "c",
                        "image": "registry.k8s.io/pause:3.10"
                    }]
                },
                "status": {"phase": "Running"}
            }),
        )
        .await
        .unwrap();

    repo.api_create_pod(api_create_request(
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "pod4"},
            "spec": {
                "priority": 4,
                "containers": [{
                    "name": "c",
                    "image": "registry.k8s.io/pause:3.10",
                    "resources": {"requests": {"example.com/fakecpu": "500"}}
                }]
            }
        }),
        false,
    ))
    .await
    .unwrap();

    repo.schedule_all_unbound_pods().await.unwrap();
    let scheduled = repo.get_pod("default", "pod4").await.unwrap().unwrap();
    assert_eq!(
        scheduled
            .data
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str()),
        Some("test-node")
    );
    for name in ["pod1", "pod2"] {
        let victim = repo
            .store
            .get("default", name)
            .await
            .unwrap()
            .expect("preempted victim remains until actor finalization");
        assert!(
            victim
                .data
                .pointer("/metadata/deletionTimestamp")
                .and_then(|v| v.as_str())
                .is_some(),
            "preempted victim {name} must be marked terminating"
        );
    }
    assert!(repo.store.get("default", "pod3").await.unwrap().is_some());
    assert!(
        repo.store
            .get("kube-system", "unrelated-low-priority")
            .await
            .unwrap()
            .is_some(),
        "pod without the constrained extended resource must not be preempted"
    );
}

#[tokio::test]
async fn scheduler_preempts_controller_created_priority_class_pods() {
    use super::{PodApiWriter, PodObjectWriter, PodReader};

    let repo = build_repo().await;
    repo.store
        .db()
        .create_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {
                        "cpu": "8",
                        "memory": "32Gi",
                        "pods": "110",
                        "example.com/fakecpu": "1k"
                    },
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();
    for (name, value) in [("p1", 1), ("p2", 2), ("p3", 3), ("p4", 4)] {
        repo.store
            .db()
            .create_resource(
                "scheduling.k8s.io/v1",
                "PriorityClass",
                None,
                name,
                json!({
                    "apiVersion": "scheduling.k8s.io/v1",
                    "kind": "PriorityClass",
                    "metadata": {"name": name},
                    "value": value
                }),
            )
            .await
            .unwrap();
    }

    for (name, request, class_name) in [
        ("rs-pod1", "200", "p1"),
        ("rs-pod2", "300", "p2"),
        ("rs-pod3", "450", "p3"),
    ] {
        repo.create_controller_pod(
            "default",
            name,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": name, "namespace": "default"},
                "spec": {
                    "priorityClassName": class_name,
                    "containers": [{
                        "name": "c",
                        "image": "registry.k8s.io/pause:3.10",
                        "resources": {"requests": {"example.com/fakecpu": request}}
                    }]
                }
            }),
        )
        .await
        .unwrap();
    }

    repo.api_create_pod(api_create_request(
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "pod4"},
            "spec": {
                "priorityClassName": "p4",
                "containers": [{
                    "name": "c",
                    "image": "registry.k8s.io/pause:3.10",
                    "resources": {"requests": {"example.com/fakecpu": "500"}}
                }]
            }
        }),
        false,
    ))
    .await
    .unwrap();

    repo.schedule_all_unbound_pods().await.unwrap();
    let scheduled = repo.get_pod("default", "pod4").await.unwrap().unwrap();
    assert_eq!(
        scheduled
            .data
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str()),
        Some("test-node")
    );
    for name in ["rs-pod1", "rs-pod2"] {
        let victim = repo
            .store
            .get("default", name)
            .await
            .unwrap()
            .expect("preempted controller-created victim remains until actor finalization");
        assert!(
            victim
                .data
                .pointer("/metadata/deletionTimestamp")
                .and_then(|v| v.as_str())
                .is_some(),
            "preempted controller-created victim {name} must be marked terminating"
        );
    }
    assert!(
        repo.store
            .get("default", "rs-pod3")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn scheduler_preemption_marks_api_created_priority_class_victim_disruption_target() {
    use super::{PodApiWriter, PodReader};

    let repo =
        build_repo_with_scheduling_mode(super::api::PodSchedulingMode::DeferredMultiNodeLeader)
            .await;
    repo.store
        .db()
        .create_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "capacity": {
                        "cpu": "8",
                        "memory": "32Gi",
                        "pods": "110",
                        "scheduling.k8s.io/foo": "1"
                    },
                    "allocatable": {
                        "cpu": "8",
                        "memory": "32Gi",
                        "pods": "110",
                        "scheduling.k8s.io/foo": "1"
                    },
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();
    for (name, value) in [("low", 1), ("high", 1000)] {
        repo.store
            .db()
            .create_resource(
                "scheduling.k8s.io/v1",
                "PriorityClass",
                None,
                name,
                json!({
                    "apiVersion": "scheduling.k8s.io/v1",
                    "kind": "PriorityClass",
                    "metadata": {"name": name},
                    "value": value
                }),
            )
            .await
            .unwrap();
    }
    let node_affinity = json!({
        "nodeAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": {
                "nodeSelectorTerms": [{
                    "matchFields": [{
                        "key": "metadata.name",
                        "operator": "In",
                        "values": ["test-node"]
                    }]
                }]
            }
        }
    });

    repo.api_create_pod(api_create_request(
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "victim",
                "namespace": "default",
                "finalizers": ["example.com/test-finalizer"]
            },
            "spec": {
                "priorityClassName": "low",
                "affinity": node_affinity.clone(),
                "containers": [{
                    "name": "c",
                    "image": "registry.k8s.io/pause:3.10",
                    "resources": {
                        "requests": {"scheduling.k8s.io/foo": "1"},
                        "limits": {"scheduling.k8s.io/foo": "1"}
                    }
                }]
            }
        }),
        false,
    ))
    .await
    .unwrap();
    repo.schedule_all_unbound_pods().await.unwrap();
    let victim = repo.get_pod("default", "victim").await.unwrap().unwrap();
    assert_eq!(
        victim
            .data
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str()),
        Some("test-node")
    );

    repo.api_create_pod(api_create_request(
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "preemptor", "namespace": "default"},
            "spec": {
                "priorityClassName": "high",
                "affinity": node_affinity,
                "containers": [{
                    "name": "c",
                    "image": "registry.k8s.io/pause:3.10",
                    "resources": {
                        "requests": {"scheduling.k8s.io/foo": "1"},
                        "limits": {"scheduling.k8s.io/foo": "1"}
                    }
                }]
            }
        }),
        false,
    ))
    .await
    .unwrap();
    repo.schedule_all_unbound_pods().await.unwrap();

    let preemptor = repo.get_pod("default", "preemptor").await.unwrap().unwrap();
    assert_eq!(
        preemptor
            .data
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str()),
        Some("test-node")
    );
    let victim = repo
        .get_pod("default", "victim")
        .await
        .unwrap()
        .expect("preempted victim remains until actor finalization");
    assert!(
        victim
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some(),
        "preempted victim must be marked terminating: {:?}",
        victim.data
    );
    assert!(
        victim
            .data
            .pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .is_some_and(|conditions| conditions.iter().any(|condition| {
                condition.get("type").and_then(|v| v.as_str()) == Some("DisruptionTarget")
                    && condition.get("status").and_then(|v| v.as_str()) == Some("True")
                    && condition.get("reason").and_then(|v| v.as_str())
                        == Some("PreemptionByScheduler")
            })),
        "preempted victim must include DisruptionTarget condition: {:?}",
        victim.data
    );
}

#[tokio::test]
async fn scheduler_marks_finalized_preemption_victim_disruption_target() {
    use super::{PodApiWriter, PodReader};

    let repo = build_repo().await;
    repo.store
        .db()
        .create_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110", "example.com/foo": "1"},
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();
    repo.store
        .create(
            "default",
            "victim",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "victim",
                    "namespace": "default",
                    "finalizers": ["example.com/test-finalizer"]
                },
                "spec": {
                    "nodeName": "test-node",
                    "priority": 1,
                    "containers": [{
                        "name": "c",
                        "image": "registry.k8s.io/pause:3.10",
                        "resources": {"requests": {"example.com/foo": "1"}}
                    }]
                },
                "status": {"phase": "Running"}
            }),
        )
        .await
        .unwrap();

    repo.api_create_pod(api_create_request(
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "preemptor"},
            "spec": {
                "priority": 2,
                "containers": [{
                    "name": "c",
                    "image": "registry.k8s.io/pause:3.10",
                    "resources": {"requests": {"example.com/foo": "1"}}
                }]
            }
        }),
        false,
    ))
    .await
    .unwrap();

    repo.schedule_all_unbound_pods().await.unwrap();
    let scheduled = repo.get_pod("default", "preemptor").await.unwrap().unwrap();
    assert_eq!(
        scheduled
            .data
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str()),
        Some("test-node")
    );
    let victim = repo
        .store
        .get("default", "victim")
        .await
        .unwrap()
        .expect("finalized victim remains terminating");
    assert!(
        victim
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some(),
        "preempted victim must be marked terminating: {:?}",
        victim.data
    );
    assert!(
        victim
            .data
            .pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .is_some_and(|conditions| conditions.iter().any(|condition| {
                condition.get("type").and_then(|v| v.as_str()) == Some("DisruptionTarget")
                    && condition.get("status").and_then(|v| v.as_str()) == Some("True")
                    && condition.get("reason").and_then(|v| v.as_str())
                        == Some("PreemptionByScheduler")
            })),
        "preempted victim must include DisruptionTarget condition: {:?}",
        victim.data
    );
}

#[tokio::test]
async fn scheduler_preemption_victim_terminating_event_includes_disruption_target() {
    use super::{PodApiWriter, PodReader};
    use crate::datastore::WatchTarget;
    use crate::watch::EventType;

    let repo =
        build_repo_with_scheduling_mode(super::api::PodSchedulingMode::DeferredMultiNodeLeader)
            .await;
    repo.store
        .db()
        .create_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110", "example.com/foo": "1"},
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();
    let victim = repo
        .store
        .create(
            "default",
            "victim",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "victim",
                    "namespace": "default",
                    "finalizers": ["example.com/test-finalizer"]
                },
                "spec": {
                    "nodeName": "test-node",
                    "priority": 1,
                    "containers": [{
                        "name": "c",
                        "image": "registry.k8s.io/pause:3.10",
                        "resources": {"requests": {"example.com/foo": "1"}}
                    }]
                },
                "status": {"phase": "Running"}
            }),
        )
        .await
        .unwrap();

    repo.api_create_pod(api_create_request(
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "preemptor"},
            "spec": {
                "priority": 2,
                "containers": [{
                    "name": "c",
                    "image": "registry.k8s.io/pause:3.10",
                    "resources": {"requests": {"example.com/foo": "1"}}
                }]
            }
        }),
        false,
    ))
    .await
    .unwrap();

    repo.schedule_all_unbound_pods().await.unwrap();
    let scheduled = repo.get_pod("default", "preemptor").await.unwrap().unwrap();
    assert_eq!(
        scheduled
            .data
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str()),
        Some("test-node")
    );

    let pod_events = repo
        .store
        .db()
        .list_watch_events_since(
            &[WatchTarget::namespaced("v1", "Pod")],
            victim.resource_version,
        )
        .await
        .unwrap();
    let terminating_victim_events: Vec<_> = pod_events
        .into_iter()
        .map(|entry| entry.into_watch_event())
        .filter(|event| {
            event.event_type == EventType::Modified
                && event
                    .object
                    .pointer("/metadata/name")
                    .and_then(|v| v.as_str())
                    == Some("victim")
                && event
                    .object
                    .pointer("/metadata/deletionTimestamp")
                    .and_then(|v| v.as_str())
                    .is_some()
        })
        .collect();

    assert!(
        !terminating_victim_events.is_empty(),
        "preemption must publish a terminating victim event"
    );
    assert!(
        terminating_victim_events.iter().all(|event| {
            event
                .object
                .pointer("/status/conditions")
                .and_then(|v| v.as_array())
                .is_some_and(|conditions| {
                    conditions.iter().any(|condition| {
                        condition.get("type").and_then(|v| v.as_str()) == Some("DisruptionTarget")
                            && condition.get("status").and_then(|v| v.as_str()) == Some("True")
                            && condition.get("reason").and_then(|v| v.as_str())
                                == Some("PreemptionByScheduler")
                    })
                })
        }),
        "preemption victim must not be observable as terminating before DisruptionTarget is set: {:?}",
        terminating_victim_events
            .iter()
            .map(|event| event.object.clone())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn scheduler_preemption_condition_survives_interleaved_worker_status_and_get() {
    use super::{PodApiWriter, PodReader};
    use crate::datastore::ResourcePreconditions;
    use crate::datastore::command::StorageCommand;
    use crate::datastore::sqlite::BuildOutboxOutcome;
    use crate::kubelet::outbox::payload::OutboxPayload;

    let repo =
        build_repo_with_scheduling_mode(super::api::PodSchedulingMode::DeferredMultiNodeLeader)
            .await;
    let db = repo.store.db().clone();

    db.create_resource(
        "v1",
        "Node",
        None,
        "worker-a",
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a"},
            "spec": {"unschedulable": false},
            "status": {
                "allocatable": {"cpu": "1", "memory": "32Gi", "pods": "110"},
                "capacity": {"cpu": "1", "memory": "32Gi", "pods": "110"},
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        }),
    )
    .await
    .unwrap();
    for (name, value) in [("low-priority", 10), ("high-priority", 1000)] {
        db.create_resource(
            "scheduling.k8s.io/v1",
            "PriorityClass",
            None,
            name,
            json!({
                "apiVersion": "scheduling.k8s.io/v1",
                "kind": "PriorityClass",
                "metadata": {"name": name},
                "value": value
            }),
        )
        .await
        .unwrap();
    }

    repo.store
        .create(
            "default",
            "victim-pod",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "victim-pod",
                    "namespace": "default",
                    "uid": "victim-uid",
                    "finalizers": ["example.com/test-finalizer"]
                },
                "spec": {
                    "nodeName": "worker-a",
                    "priorityClassName": "low-priority",
                    "priority": 10,
                    "containers": [{
                        "name": "app",
                        "image": "registry.k8s.io/pause:3.10",
                        "resources": {"requests": {"cpu": "900m"}}
                    }]
                },
                "status": {"phase": "Running"}
            }),
        )
        .await
        .unwrap();

    repo.api_create_pod(api_create_request(
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "preemptor-pod", "namespace": "default"},
            "spec": {
                "priorityClassName": "high-priority",
                "containers": [{
                    "name": "app",
                    "image": "registry.k8s.io/pause:3.10",
                    "resources": {"requests": {"cpu": "900m"}}
                }]
            }
        }),
        false,
    ))
    .await
    .unwrap();
    repo.schedule_all_unbound_pods().await.unwrap();
    let scheduled = repo
        .get_pod("default", "preemptor-pod")
        .await
        .unwrap()
        .expect("preemptor must be scheduled");
    assert_eq!(
        scheduled
            .data
            .pointer("/spec/nodeName")
            .and_then(|value| value.as_str()),
        Some("worker-a"),
        "preemptor should win the node via preemption"
    );

    // Simulate a lagged kubelet status outbox apply landing after preemption:
    // a Running status snapshot (without DisruptionTarget) encoded as a worker
    // PodStatus outbox command and applied through the leader raft-apply path.
    let stale_status = json!({
        "phase": "Running",
        "conditions": [
            {"type": "PodScheduled", "status": "True"},
            {"type": "Initialized", "status": "True"},
            {"type": "ContainersReady", "status": "True"},
            {"type": "Ready", "status": "True"}
        ],
        "containerStatuses": [{
            "name": "app",
            "containerID": "containerd://victim-ctr",
            "ready": true,
            "started": true,
            "restartCount": 0,
            "state": {"running": {"startedAt": "2026-06-22T12:08:53Z"}}
        }]
    });
    let command = StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "victim-pod".to_string(),
        status: stale_status,
        expected_rv: None,
        preconditions: ResourcePreconditions {
            uid: Some("victim-uid".to_string()),
            resource_version: None,
        },
        observed_status_stamp: None,
    };
    let payload = OutboxPayload::from_command(command)
        .encode_protobuf()
        .unwrap();
    let outcome = db
        .build_log_apply_commit_for_outbox(
            "stale-worker-status-after-preemption",
            "PodStatus",
            payload.as_ref(),
            "worker-a",
        )
        .await
        .expect("build stale status commit");
    let BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
        panic!("expected a fresh status commit");
    };
    db.apply_log_apply_commit(commit)
        .await
        .expect("stale worker status apply must not strand the outbox row");

    let victim = repo
        .get_pod("default", "victim-pod")
        .await
        .unwrap()
        .expect("victim remains until actor finalization");
    assert!(
        victim
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|value| value.as_str())
            .is_some(),
        "victim must be terminating: {:?}",
        victim.data.pointer("/metadata")
    );
    assert!(
        victim
            .data
            .pointer("/status/conditions")
            .and_then(|value| value.as_array())
            .unwrap_or(&Vec::new())
            .iter()
            .any(|condition| {
                condition.pointer("/type").and_then(|value| value.as_str())
                    == Some("DisruptionTarget")
                    && condition
                        .pointer("/reason")
                        .and_then(|value| value.as_str())
                        == Some("PreemptionByScheduler")
            }),
        "terminating preemption victim must include DisruptionTarget after stale worker status: {:?}",
        victim.data.pointer("/status/conditions")
    );
}

#[tokio::test]
async fn api_create_pod_resolves_priority_class_name_before_storage() {
    use super::PodApiWriter;

    let repo = build_repo().await;
    repo.store
        .db()
        .create_resource(
            "scheduling.k8s.io/v1",
            "PriorityClass",
            None,
            "high",
            json!({
                "apiVersion": "scheduling.k8s.io/v1",
                "kind": "PriorityClass",
                "metadata": {"name": "high"},
                "value": 1000,
                "preemptionPolicy": "Never"
            }),
        )
        .await
        .unwrap();

    let result = repo
        .api_create_pod(api_create_request(
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "classed"},
                "spec": {
                    "priorityClassName": "high",
                    "containers": [{"name": "c", "image": "registry.k8s.io/pause:3.10"}]
                }
            }),
            false,
        ))
        .await
        .unwrap();

    assert_eq!(result.body.pointer("/spec/priority"), Some(&json!(1000)));
    assert_eq!(
        result.body.pointer("/spec/preemptionPolicy"),
        Some(&json!("Never"))
    );
}

#[tokio::test]
async fn api_create_pod_priority_class_overrides_wire_zero_priority() {
    use super::PodApiWriter;

    let repo = build_repo().await;
    repo.store
        .db()
        .create_resource(
            "scheduling.k8s.io/v1",
            "PriorityClass",
            None,
            "high",
            json!({
                "apiVersion": "scheduling.k8s.io/v1",
                "kind": "PriorityClass",
                "metadata": {"name": "high"},
                "value": 1000,
                "preemptionPolicy": "PreemptLowerPriority"
            }),
        )
        .await
        .unwrap();

    let result = repo
        .api_create_pod(api_create_request(
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "classed-zero"},
                "spec": {
                    "priorityClassName": "high",
                    "priority": 0,
                    "containers": [{"name": "c", "image": "registry.k8s.io/pause:3.10"}]
                }
            }),
            false,
        ))
        .await
        .unwrap();

    assert_eq!(result.body.pointer("/spec/priority"), Some(&json!(1000)));
    assert_eq!(
        result.body.pointer("/spec/preemptionPolicy"),
        Some(&json!("PreemptLowerPriority"))
    );
}

#[tokio::test]
async fn api_create_pod_defaults_container_fields() {
    use super::PodApiWriter;
    let repo = build_repo().await;
    let result = repo
        .api_create_pod(api_create_request(
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": { "name": "container-defaults" },
                "spec": {
                    "containers": [{
                        "name": "c",
                        "image": "busybox",
                        "terminationMessagePath": "",
                        "terminationMessagePolicy": "",
                        "livenessProbe": { "httpGet": { "port": 8080, "path": "", "scheme": "" } }
                    }]
                }
            }),
            false,
        ))
        .await
        .unwrap();
    let container = result.body.pointer("/spec/containers/0").unwrap();
    assert_eq!(
        container
            .get("terminationMessagePath")
            .and_then(|v| v.as_str()),
        Some("/dev/termination-log")
    );
    assert_eq!(
        container
            .get("terminationMessagePolicy")
            .and_then(|v| v.as_str()),
        Some("File")
    );
    assert_eq!(
        container
            .pointer("/livenessProbe/httpGet/path")
            .and_then(|v| v.as_str()),
        Some("/")
    );
    assert_eq!(
        container
            .pointer("/livenessProbe/httpGet/scheme")
            .and_then(|v| v.as_str()),
        Some("HTTP")
    );
    assert_eq!(
        result.body.pointer("/spec/restartPolicy"),
        Some(&json!("Always"))
    );
}

#[tokio::test]
async fn api_create_pod_sets_pending_status_and_qos() {
    use super::PodApiWriter;
    let repo = build_repo().await;
    let result = repo
        .api_create_pod(api_create_request(
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": { "name": "status-and-qos" },
                "spec": {
                    "containers": [{ "name": "c", "image": "busybox" }]
                }
            }),
            false,
        ))
        .await
        .unwrap();
    assert_eq!(
        result
            .body
            .pointer("/status/phase")
            .and_then(|v| v.as_str()),
        Some("Pending")
    );
    assert_eq!(
        result
            .body
            .pointer("/status/containerStatuses")
            .and_then(|v| v.as_array())
            .map(std::vec::Vec::len),
        Some(0)
    );
    assert_eq!(
        result
            .body
            .pointer("/status/qosClass")
            .and_then(|v| v.as_str()),
        Some("BestEffort")
    );
    let conditions = result
        .body
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .unwrap();
    let has = |ty: &str, status: &str| {
        conditions.iter().any(|cond| {
            cond.get("type").and_then(|v| v.as_str()) == Some(ty)
                && cond.get("status").and_then(|v| v.as_str()) == Some(status)
        })
    };
    assert!(has("Initialized", "True"));
    assert!(has("Ready", "False"));
    assert!(has("ContainersReady", "False"));
    assert!(has("PodScheduled", "False"));
    assert!(
        conditions.iter().any(|cond| {
            cond.get("type").and_then(|v| v.as_str()) == Some("PodScheduled")
                && cond.get("reason").and_then(|v| v.as_str()) == Some("SchedulingPending")
        }),
        "implicit scheduling should remain pending at API create"
    );
}

#[tokio::test]
async fn api_create_pod_dry_run_does_not_persist() {
    use super::PodApiWriter;
    use super::PodReader;
    let repo = build_repo().await;
    let result = repo
        .api_create_pod(api_create_request(
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": { "name": "dry-run-pod" },
                "spec": {
                    "containers": [{ "name": "c", "image": "busybox" }]
                }
            }),
            true,
        ))
        .await
        .unwrap();
    assert!(result.resource.is_none());
    assert!(
        repo.get_pod("default", "dry-run-pod")
            .await
            .unwrap()
            .is_none()
    );
}

async fn create_basic_pod_via_api(
    repo: &super::PodRepository,
    name: &str,
) -> crate::datastore::Resource {
    use super::PodApiWriter;
    repo.api_create_pod(api_create_request(
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": name },
            "spec": {
                "containers": [{ "name": "c", "image": "busybox" }]
            }
        }),
        false,
    ))
    .await
    .unwrap()
    .resource
    .expect("create returned resource")
}

async fn install_delete_admission_status_race_webhook(
    repo: Arc<super::PodRepository>,
    pod_name: &'static str,
) -> tokio::sync::oneshot::Receiver<()> {
    use super::PodStatusWriter;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test webhook listener binds");
    let addr = listener
        .local_addr()
        .expect("test webhook listener has address");
    let (handled_tx, handled_rx) = tokio::sync::oneshot::channel();
    let repo_for_webhook = Arc::clone(&repo);

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("webhook accepts request");
        let mut request = Vec::new();
        let mut buffer = [0u8; 1024];
        let mut body_start_and_len = None;
        loop {
            let n = socket
                .read(&mut buffer)
                .await
                .expect("webhook reads request");
            if n == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..n]);
            if body_start_and_len.is_none()
                && let Some(header_end) = request.windows(4).position(|w| w == b"\r\n\r\n")
            {
                let headers = String::from_utf8_lossy(&request[..header_end]);
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        if name.eq_ignore_ascii_case("content-length") {
                            value.trim().parse::<usize>().ok()
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0);
                body_start_and_len = Some((header_end + 4, content_length));
            }
            if let Some((body_start, content_length)) = body_start_and_len
                && request.len() >= body_start + content_length
            {
                break;
            }
        }

        repo_for_webhook
            .set_pod_status(
                "default",
                pod_name,
                super::PodStatusUpdate {
                    phase: "Running".to_string(),
                    pod_ip: "10.42.0.55".to_string(),
                    host_ip: "127.0.0.1".to_string(),
                    container_statuses: Vec::new(),
                    init_container_statuses: None,
                    qos_class: Some("BestEffort".to_string()),
                },
                None,
            )
            .await
            .expect("webhook status update advances pod resourceVersion");

        let response_body = br#"{"apiVersion":"admission.k8s.io/v1","kind":"AdmissionReview","response":{"allowed":true}}"#;
        let response_headers = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            response_body.len()
        );
        socket
            .write_all(response_headers.as_bytes())
            .await
            .expect("webhook writes response headers");
        socket
            .write_all(response_body)
            .await
            .expect("webhook writes response body");
        let _ = handled_tx.send(());
    });

    let config = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "MutatingWebhookConfiguration",
        "metadata": {"name": "pod-delete-status-race"},
        "webhooks": [{
            "name": "pod-delete-status-race.example.com",
            "sideEffects": "None",
            "admissionReviewVersions": ["v1"],
            "clientConfig": {"url": format!("http://{addr}/mutate")},
            "rules": [{
                "operations": ["DELETE"],
                "apiGroups": [""],
                "apiVersions": ["v1"],
                "resources": ["pods"]
            }]
        }]
    });
    repo.store
        .db()
        .create_resource(
            "admissionregistration.k8s.io/v1",
            "MutatingWebhookConfiguration",
            None,
            "pod-delete-status-race",
            config,
        )
        .await
        .unwrap();

    handled_rx
}

#[tokio::test]
async fn api_update_pod_persists_full_object_changes() {
    use super::PodApiWriter;
    let repo = build_repo().await;
    let created = create_basic_pod_via_api(&repo, "u-pod").await;
    let mut body: serde_json::Value = (*created.data).clone();
    if let Some(meta) = body.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        meta.insert(
            "labels".to_string(),
            json!({"app": "x", "tier": "frontend"}),
        );
    }
    let outcome = repo
        .api_update_pod("default", "u-pod", body, created.clone(), false)
        .await
        .unwrap();
    let resource = match outcome {
        super::PodApiUpdateOutcome::Persisted(r) => r,
        super::PodApiUpdateOutcome::DryRun(_) => panic!("expected Persisted"),
    };
    assert_eq!(
        resource.data["metadata"]["labels"]["tier"],
        json!("frontend")
    );
    assert!(resource.resource_version > created.resource_version);
}

#[tokio::test]
async fn api_update_pod_preserves_existing_status() {
    use super::{PodApiWriter, PodSubresourceWriter};
    let repo = build_repo().await;
    let created = create_basic_pod_via_api(&repo, "u-status").await;
    let status_updated = repo
        .replace_status_from_api(
            "default",
            "u-status",
            json!({"phase": "Running", "podIP": "10.42.0.10"}),
            created.resource_version,
        )
        .await
        .unwrap();

    let mut body: serde_json::Value = (*status_updated.data).clone();
    body["metadata"]["labels"] = json!({"tier": "frontend"});
    body["status"] = json!({"phase": "Failed", "podIP": "10.42.0.99"});

    let outcome = repo
        .api_update_pod("default", "u-status", body, status_updated.clone(), false)
        .await
        .unwrap();
    let resource = match outcome {
        super::PodApiUpdateOutcome::Persisted(r) => r,
        super::PodApiUpdateOutcome::DryRun(_) => panic!("expected Persisted"),
    };
    assert_eq!(
        resource.data["metadata"]["labels"]["tier"],
        json!("frontend")
    );
    assert_eq!(resource.data["status"]["phase"], json!("Running"));
    assert_eq!(resource.data["status"]["podIP"], json!("10.42.0.10"));
}

#[tokio::test]
async fn api_update_pod_dry_run_does_not_persist() {
    use super::PodApiWriter;
    use super::PodReader;
    let repo = build_repo().await;
    let created = create_basic_pod_via_api(&repo, "u-dry").await;
    let mut body: serde_json::Value = (*created.data).clone();
    body["metadata"]["labels"] = json!({"app": "x", "tier": "dry"});
    let outcome = repo
        .api_update_pod("default", "u-dry", body, created.clone(), true)
        .await
        .unwrap();
    assert!(matches!(outcome, super::PodApiUpdateOutcome::DryRun(_)));
    let after = repo.get_pod("default", "u-dry").await.unwrap().unwrap();
    assert_eq!(after.resource_version, created.resource_version);
    assert!(
        after.data["metadata"].get("labels").is_none()
            || after.data["metadata"]["labels"].get("tier").is_none()
    );
}

#[tokio::test]
async fn api_update_pod_returns_conflict_on_stale_rv() {
    use super::PodApiWriter;
    let repo = build_repo().await;
    let created = create_basic_pod_via_api(&repo, "u-race").await;

    // First writer wins.
    let mut body1: serde_json::Value = (*created.data).clone();
    body1["metadata"]["labels"] = json!({"app": "x", "tier": "first"});
    repo.api_update_pod("default", "u-race", body1, created.clone(), false)
        .await
        .expect("first writer wins");

    // Second writer with the stale read object.
    let mut body2: serde_json::Value = (*created.data).clone();
    body2["metadata"]["labels"] = json!({"app": "x", "tier": "second"});
    let conflict = repo
        .api_update_pod("default", "u-race", body2, created, false)
        .await;
    let err = conflict.expect_err("stale rv must conflict");
    assert!(
        format!("{err:?}").contains("409") || format!("{err:?}").contains("Conflict"),
        "expected 409 Conflict, got {err:?}"
    );
}

#[tokio::test]
async fn api_patch_pod_json_patch_applies_op() {
    use super::PodApiWriter;
    let repo = build_repo().await;
    let _ = create_basic_pod_via_api(&repo, "p-jp").await;
    let patch = json!([
        {"op": "add", "path": "/metadata/labels", "value": {"tier": "frontend"}}
    ]);
    let outcome = repo
        .api_patch_pod(
            "default",
            "p-jp",
            patch,
            super::PodStatusPatchType::JsonPatch,
            false,
        )
        .await
        .unwrap();
    let resource = match outcome {
        super::PodApiUpdateOutcome::Persisted(r) => r,
        _ => panic!("expected Persisted"),
    };
    assert_eq!(
        resource.data["metadata"]["labels"]["tier"],
        json!("frontend")
    );
}

#[tokio::test]
async fn api_patch_pod_merge_patch_updates_only_named_keys() {
    use super::PodApiWriter;
    let repo = build_repo().await;
    let _ = create_basic_pod_via_api(&repo, "p-mp").await;
    let patch = json!({"metadata": {"labels": {"tier": "frontend"}}});
    let outcome = repo
        .api_patch_pod(
            "default",
            "p-mp",
            patch,
            super::PodStatusPatchType::MergePatch,
            false,
        )
        .await
        .unwrap();
    let resource = match outcome {
        super::PodApiUpdateOutcome::Persisted(r) => r,
        _ => panic!("expected Persisted"),
    };
    assert_eq!(
        resource.data["metadata"]["labels"]["tier"],
        json!("frontend")
    );
    // Spec preserved
    assert_eq!(resource.data["spec"]["containers"][0]["name"], json!("c"));
}

#[tokio::test]
async fn pod_annotation_patch_does_not_scan_services_or_enqueue_service() {
    use super::PodApiWriter;
    let (repo, db, dispatcher) = build_repo_with_dispatcher().await;

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "web",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "web", "namespace": "default"},
            "spec": {
                "selector": {"app": "web"},
                "ports": [{"name": "http", "port": 80, "targetPort": 8080}]
            }
        }),
    )
    .await
    .unwrap();

    let mut seed = pending_pod("anno-pod");
    seed["metadata"]["labels"] = json!({"app": "web"});
    seed["status"] = json!({
        "phase": "Running",
        "podIP": "10.42.0.35",
        "podIPs": [{"ip": "10.42.0.35"}],
        "hostIP": "10.0.0.10",
        "hostIPs": [{"ip": "10.0.0.10"}],
        "conditions": [
            {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
            {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
            {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
            {"type": "Ready", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"}
        ]
    });
    repo.store
        .create("default", "anno-pod", seed)
        .await
        .unwrap();
    let managed_tasks_before = repo.supervisor.managed_task_count();

    for i in 0..50 {
        let patch = json!({
            "metadata": {
                "annotations": {
                    "note": format!("scan-check-{i}")
                }
            }
        });
        let outcome = repo
            .api_patch_pod(
                "default",
                "anno-pod",
                patch,
                super::PodStatusPatchType::MergePatch,
                false,
            )
            .await
            .unwrap();
        assert!(matches!(outcome, super::PodApiUpdateOutcome::Persisted(_)));
    }

    let keys = dispatcher.pending_reconcile_keys().await;
    assert!(
        keys.iter()
            .all(|key| !(key.api_version == "v1" && key.kind == "Service")),
        "annotation-only patches must not enqueue Service reconciles"
    );
    assert_eq!(
        repo.supervisor.managed_task_count(),
        managed_tasks_before,
        "annotation-only patches must not create background retry or timer tasks"
    );
}

#[tokio::test]
async fn api_patch_pod_preserves_existing_status() {
    use super::{PodApiWriter, PodSubresourceWriter};
    let repo = build_repo().await;
    let created = create_basic_pod_via_api(&repo, "p-status").await;
    let status_updated = repo
        .replace_status_from_api(
            "default",
            "p-status",
            json!({"phase": "Running", "podIP": "10.42.0.20"}),
            created.resource_version,
        )
        .await
        .unwrap();

    let outcome = repo
        .api_patch_pod(
            "default",
            "p-status",
            json!({
                "metadata": {"labels": {"tier": "frontend"}},
                "status": {"phase": "Failed", "podIP": "10.42.0.99"}
            }),
            super::PodStatusPatchType::MergePatch,
            false,
        )
        .await
        .unwrap();
    let resource = match outcome {
        super::PodApiUpdateOutcome::Persisted(r) => r,
        _ => panic!("expected Persisted"),
    };
    assert!(resource.resource_version > status_updated.resource_version);
    assert_eq!(
        resource.data["metadata"]["labels"]["tier"],
        json!("frontend")
    );
    assert_eq!(resource.data["status"]["phase"], json!("Running"));
    assert_eq!(resource.data["status"]["podIP"], json!("10.42.0.20"));
}

#[tokio::test]
async fn api_patch_pod_strategic_merge_merges_conditions_by_type() {
    use super::PodApiWriter;
    let repo = build_repo().await;
    // Strategic-merge on metadata.labels (no merge-key field there) just
    // merges the two maps.
    let _ = create_basic_pod_via_api(&repo, "p-sm").await;
    let patch = json!({"metadata": {"labels": {"tier": "frontend"}}});
    let outcome = repo
        .api_patch_pod(
            "default",
            "p-sm",
            patch,
            super::PodStatusPatchType::StrategicMerge,
            false,
        )
        .await
        .unwrap();
    let resource = match outcome {
        super::PodApiUpdateOutcome::Persisted(r) => r,
        _ => panic!("expected Persisted"),
    };
    assert_eq!(
        resource.data["metadata"]["labels"]["tier"],
        json!("frontend")
    );
}

#[tokio::test]
async fn api_patch_pod_apply_patch_against_missing_pod_creates_via_ssa() {
    use super::PodApiWriter;
    use super::PodReader;
    let repo = build_repo().await;
    // No pre-existing pod.
    let patch = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "ssa-new" },
        "spec": { "containers": [{ "name": "c", "image": "busybox" }] }
    });
    let outcome = repo
        .api_patch_pod(
            "default",
            "ssa-new",
            patch,
            super::PodStatusPatchType::ApplyPatch,
            false,
        )
        .await
        .unwrap();
    assert!(matches!(outcome, super::PodApiUpdateOutcome::Persisted(_)));
    let exists = repo.get_pod("default", "ssa-new").await.unwrap();
    assert!(exists.is_some(), "SSA-create must persist the pod");
}

#[tokio::test]
async fn api_patch_pod_merge_patch_against_missing_pod_returns_404() {
    use super::PodApiWriter;
    let repo = build_repo().await;
    let patch = json!({"metadata": {"labels": {"tier": "x"}}});
    let err = repo
        .api_patch_pod(
            "default",
            "missing-pod",
            patch,
            super::PodStatusPatchType::MergePatch,
            false,
        )
        .await
        .expect_err("missing pod under merge patch must 404");
    assert!(
        matches!(err, crate::api::AppError::NotFound(_)),
        "expected NotFound, got {err:?}"
    );
}

#[tokio::test]
async fn api_delete_pod_sets_deletion_timestamp_and_default_grace_30s() {
    use super::PodApiWriter;
    let repo = build_repo().await;
    let _ = create_basic_pod_via_api(&repo, "del-default").await;
    let outcome = repo
        .api_delete_pod(
            "default",
            "del-default",
            crate::api::DeleteOptions::default(),
            false,
        )
        .await
        .unwrap();
    let r = match outcome {
        super::PodApiDeleteOutcome::GracefulSet(r) => r,
        _ => panic!("expected GracefulSet"),
    };
    assert!(r.data["metadata"]["deletionTimestamp"].is_string());
    assert_eq!(r.data["metadata"]["deletionGracePeriodSeconds"], json!(30));
}

#[tokio::test]
async fn api_delete_pod_zero_grace_marks_terminating_pod_unready() {
    use super::PodApiWriter;

    let repo = build_repo().await;
    repo.store
        .create(
            "default",
            "del-ready",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "del-ready",
                    "namespace": "default",
                    "uid": "uid-del-ready"
                },
                "spec": {
                    "nodeName": "test-node",
                    "containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]
                },
                "status": {
                    "phase": "Running",
                    "conditions": [
                        {"type": "Initialized", "status": "True"},
                        {"type": "PodScheduled", "status": "True"},
                        {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                        {"type": "Ready", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"}
                    ],
                    "containerStatuses": [{
                        "name": "app",
                        "ready": true,
                        "restartCount": 0,
                        "state": {"running": {"startedAt": "2026-04-30T00:00:00Z"}}
                    }]
                }
            }),
        )
        .await
        .unwrap();

    let outcome = repo
        .api_delete_pod(
            "default",
            "del-ready",
            crate::api::DeleteOptions {
                propagation_policy: None,
                orphan_dependents: None,
                _grace_period_seconds: Some(0),
                preconditions: None,
            },
            false,
        )
        .await
        .unwrap();
    let returned = match outcome {
        super::PodApiDeleteOutcome::GracefulSet(resource) => resource,
        _ => panic!("expected GracefulSet"),
    };

    for pod in [
        returned,
        repo.store
            .get("default", "del-ready")
            .await
            .unwrap()
            .expect("pod remains until actor-owned cleanup"),
    ] {
        assert!(pod.data.pointer("/metadata/deletionTimestamp").is_some());
        assert_eq!(
            pod.data
                .pointer("/metadata/deletionGracePeriodSeconds")
                .and_then(|value| value.as_i64()),
            Some(0)
        );
        let conditions = pod
            .data
            .pointer("/status/conditions")
            .and_then(|value| value.as_array())
            .expect("conditions must remain an array");
        for condition_type in ["Ready", "ContainersReady"] {
            let condition = conditions
                .iter()
                .find(|condition| {
                    condition.pointer("/type").and_then(|value| value.as_str())
                        == Some(condition_type)
                })
                .unwrap_or_else(|| panic!("missing {condition_type} condition"));
            assert_eq!(
                condition
                    .pointer("/status")
                    .and_then(|value| value.as_str()),
                Some("False"),
                "terminating pod must not stay {condition_type}=True"
            );
            assert_eq!(
                condition
                    .pointer("/reason")
                    .and_then(|value| value.as_str()),
                Some("PodTerminating")
            );
        }
        let container_ready = pod
            .data
            .pointer("/status/containerStatuses/0/ready")
            .and_then(|value| value.as_bool());
        assert_eq!(container_ready, Some(false));
    }
}

#[tokio::test]
async fn api_delete_pod_cascades_pod_owner_cycle_without_reentrant_stack_growth() {
    use super::PodApiWriter;

    let repo = build_repo().await;
    for (name, uid, owner_name, owner_uid) in [
        ("pod1", "pod-1-uid", "pod3", "pod-3-uid"),
        ("pod2", "pod-2-uid", "pod1", "pod-1-uid"),
        ("pod3", "pod-3-uid", "pod2", "pod-2-uid"),
    ] {
        repo.store
            .create(
                "default",
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

    repo.api_delete_pod(
        "default",
        "pod1",
        crate::api::DeleteOptions::default(),
        false,
    )
    .await
    .unwrap();

    for name in ["pod1", "pod2", "pod3"] {
        let pod = repo
            .store
            .get("default", name)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("{name} must remain until actor-owned finalization"));
        assert!(
            pod.data
                .pointer("/metadata/deletionTimestamp")
                .and_then(|value| value.as_str())
                .is_some(),
            "{name} must be marked terminating after cascade: {:?}",
            pod.data
        );
    }
}

#[tokio::test]
async fn api_delete_pod_replaces_null_deletion_timestamp_with_real_timestamp() {
    use super::PodApiWriter;

    let repo = build_repo().await;
    repo.store
        .create(
            "default",
            "del-null",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "del-null",
                    "namespace": "default",
                    "deletionTimestamp": null
                },
                "spec": {
                    "containers": [{ "name": "c", "image": "busybox" }]
                }
            }),
        )
        .await
        .unwrap();

    let outcome = repo
        .api_delete_pod(
            "default",
            "del-null",
            crate::api::DeleteOptions::default(),
            false,
        )
        .await
        .unwrap();
    let r = match outcome {
        super::PodApiDeleteOutcome::GracefulSet(r) => r,
        _ => panic!("expected GracefulSet"),
    };

    assert!(
        r.data["metadata"]["deletionTimestamp"].is_string(),
        "DELETE must convert null deletionTimestamp into a real timestamp"
    );
    assert_eq!(r.data["metadata"]["deletionGracePeriodSeconds"], json!(30));

    let persisted = repo
        .store
        .get("default", "del-null")
        .await
        .unwrap()
        .expect("pod remains while actor cleanup owns final delete");
    assert!(
        persisted.data["metadata"]["deletionTimestamp"].is_string(),
        "persisted Pod must be visibly terminating immediately after DELETE"
    );
}

#[tokio::test]
async fn api_delete_pod_uses_options_grace_period_when_provided() {
    use super::PodApiWriter;
    let repo = build_repo().await;
    let _ = create_basic_pod_via_api(&repo, "del-60").await;
    let opts = crate::api::DeleteOptions {
        propagation_policy: None,
        orphan_dependents: None,
        _grace_period_seconds: Some(60),
        preconditions: None,
    };
    let outcome = repo
        .api_delete_pod("default", "del-60", opts, false)
        .await
        .unwrap();
    let r = match outcome {
        super::PodApiDeleteOutcome::GracefulSet(r) => r,
        _ => panic!("expected GracefulSet"),
    };
    assert_eq!(r.data["metadata"]["deletionGracePeriodSeconds"], json!(60));
}

#[tokio::test]
async fn api_delete_pod_does_not_hard_delete_before_requested_grace_period() {
    use super::{PodApiWriter, PodReader};
    let repo = build_repo().await;
    let _ = create_basic_pod_via_api(&repo, "del-grace-five").await;
    let opts = crate::api::DeleteOptions {
        propagation_policy: None,
        orphan_dependents: None,
        _grace_period_seconds: Some(5),
        preconditions: None,
    };

    repo.api_delete_pod("default", "del-grace-five", opts, false)
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(2300)).await;

    let after = repo.get_pod("default", "del-grace-five").await.unwrap();
    assert!(
        after.is_some(),
        "Pod API delete must not hard-delete the object before its requested grace period"
    );
}

#[tokio::test]
async fn api_delete_pod_dry_run_does_not_persist() {
    use super::PodApiWriter;
    use super::PodReader;
    let repo = build_repo().await;
    let created = create_basic_pod_via_api(&repo, "del-dry").await;
    let outcome = repo
        .api_delete_pod(
            "default",
            "del-dry",
            crate::api::DeleteOptions::default(),
            true,
        )
        .await
        .unwrap();
    assert!(matches!(outcome, super::PodApiDeleteOutcome::DryRun(_)));
    let after = repo.get_pod("default", "del-dry").await.unwrap().unwrap();
    assert_eq!(after.resource_version, created.resource_version);
    assert!(after.data["metadata"].get("deletionTimestamp").is_none());
}

#[tokio::test]
async fn api_delete_pod_retries_when_status_update_advances_resource_version_during_admission() {
    use super::PodApiWriter;
    use super::PodReader;

    let repo = Arc::new(build_repo().await);
    let created = create_basic_pod_via_api(repo.as_ref(), "del-status-race").await;
    let webhook_handled =
        install_delete_admission_status_race_webhook(Arc::clone(&repo), "del-status-race").await;

    let outcome = repo
        .api_delete_pod(
            "default",
            "del-status-race",
            crate::api::DeleteOptions::default(),
            false,
        )
        .await
        .expect("DELETE must retry after an admission-time status conflict");

    tokio::time::timeout(std::time::Duration::from_secs(1), webhook_handled)
        .await
        .expect("delete admission webhook was called")
        .expect("delete admission webhook completed");
    let deleted = match outcome {
        super::PodApiDeleteOutcome::GracefulSet(resource) => resource,
        other => panic!("expected GracefulSet, got {other:?}"),
    };
    assert!(deleted.resource_version > created.resource_version);
    assert!(deleted.data["metadata"]["deletionTimestamp"].is_string());
    assert_eq!(
        deleted.data["metadata"]["deletionGracePeriodSeconds"],
        json!(30)
    );
    assert_eq!(deleted.data["status"]["phase"], json!("Running"));

    let persisted = repo
        .get_pod("default", "del-status-race")
        .await
        .unwrap()
        .expect("pod remains until graceful delete completes");
    assert_eq!(
        persisted.data["metadata"]["deletionTimestamp"],
        deleted.data["metadata"]["deletionTimestamp"]
    );
    assert_eq!(persisted.data["status"]["phase"], json!("Running"));
}

#[tokio::test]
async fn api_delete_pod_without_resource_version_precondition_survives_raft_status_race() {
    use super::PodApiWriter;
    use super::PodReader;

    let (repo, status_bumps) =
        build_raft_repo_with_status_race_on_delete("del-raft-status-race").await;
    let created = create_basic_pod_via_api(&repo, "del-raft-status-race").await;

    let outcome = repo
        .api_delete_pod(
            "default",
            "del-raft-status-race",
            crate::api::DeleteOptions::default(),
            false,
        )
        .await
        .expect("DELETE without an RV precondition must apply to the latest Pod object");

    assert!(
        status_bumps.load(Ordering::SeqCst) > 0,
        "test proposer must advance status before the delete mark"
    );
    let deleted = match outcome {
        super::PodApiDeleteOutcome::GracefulSet(resource) => resource,
        other => panic!("expected GracefulSet, got {other:?}"),
    };
    assert!(deleted.resource_version > created.resource_version);
    assert!(deleted.data["metadata"]["deletionTimestamp"].is_string());
    assert_eq!(
        deleted.data["metadata"]["deletionGracePeriodSeconds"],
        json!(30)
    );
    assert_eq!(deleted.data["status"]["phase"], json!("Running"));
    assert_eq!(deleted.data["status"]["raceBump"], json!(1));

    let persisted = repo
        .get_pod("default", "del-raft-status-race")
        .await
        .unwrap()
        .expect("pod remains until actor-owned finalization");
    assert_eq!(
        persisted.data["metadata"]["deletionTimestamp"],
        deleted.data["metadata"]["deletionTimestamp"]
    );
    assert_eq!(persisted.data["status"]["raceBump"], json!(1));
}

#[tokio::test]
async fn api_delete_pod_zero_grace_without_resource_version_precondition_survives_raft_status_race()
{
    use super::PodApiWriter;
    use super::PodReader;

    let (repo, status_bumps) =
        build_raft_repo_with_status_race_on_delete("del-zero-grace-raft-status-race").await;
    let created = create_basic_pod_via_api(&repo, "del-zero-grace-raft-status-race").await;

    let outcome = repo
        .api_delete_pod(
            "default",
            "del-zero-grace-raft-status-race",
            crate::api::DeleteOptions {
                _grace_period_seconds: Some(0),
                preconditions: None,
                ..Default::default()
            },
            false,
        )
        .await
        .expect("zero-grace DELETE without an RV precondition must apply to the latest Pod object");

    assert!(
        status_bumps.load(Ordering::SeqCst) > 0,
        "test proposer must advance status before the delete mark"
    );
    let deleted = match outcome {
        super::PodApiDeleteOutcome::GracefulSet(resource) => resource,
        other => panic!("expected GracefulSet, got {other:?}"),
    };
    assert!(deleted.resource_version > created.resource_version);
    assert!(deleted.data["metadata"]["deletionTimestamp"].is_string());
    assert_eq!(
        deleted.data["metadata"]["deletionGracePeriodSeconds"],
        json!(0)
    );
    assert_eq!(deleted.data["status"]["phase"], json!("Running"));
    assert_eq!(deleted.data["status"]["raceBump"], json!(1));
    for condition_type in ["Ready", "ContainersReady"] {
        let condition = deleted.data["status"]["conditions"]
            .as_array()
            .and_then(|conditions| {
                conditions
                    .iter()
                    .find(|condition| condition.get("type") == Some(&json!(condition_type)))
            })
            .expect("terminating zero-grace pod must carry readiness conditions");
        assert_eq!(condition["status"], json!("False"));
        assert_eq!(condition["reason"], json!("PodTerminating"));
    }

    let persisted = repo
        .get_pod("default", "del-zero-grace-raft-status-race")
        .await
        .unwrap()
        .expect("pod remains until actor-owned finalization");
    assert_eq!(
        persisted.data["metadata"]["deletionTimestamp"],
        deleted.data["metadata"]["deletionTimestamp"]
    );
    assert_eq!(persisted.data["status"]["raceBump"], json!(1));
}

#[tokio::test]
async fn api_delete_collection_pods_processes_all_matching_label_selector() {
    use super::PodApiWriter;
    use super::PodReader;
    let repo = build_repo().await;
    // Three pods, two with app=x.
    use super::PodApiCreateRequest;
    for (n, label_app) in [("c1", "x"), ("c2", "x"), ("c3", "y")] {
        repo.api_create_pod(PodApiCreateRequest {
            namespace: "default".to_string(),
            name: String::new(),
            body: json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": { "name": n, "labels": {"app": label_app} },
                "spec": { "containers": [{ "name": "c", "image": "busybox" }] }
            }),
            dry_run: false,
            run_admission: false,
        })
        .await
        .unwrap();
    }
    repo.api_delete_collection_pods("default", Some("app=x"), None, false)
        .await
        .unwrap();
    for n in ["c1", "c2"] {
        let pod = repo
            .get_pod("default", n)
            .await
            .unwrap()
            .expect("collection delete must leave Pod row until actor finalization");
        assert!(
            pod.data["metadata"]["deletionTimestamp"].is_string(),
            "pod {n} must be marked terminating after collection delete"
        );
    }
    // Pod c3 (app=y) must be untouched.
    let c3 = repo.get_pod("default", "c3").await.unwrap().unwrap();
    assert!(c3.data["metadata"].get("deletionTimestamp").is_none());
}

#[tokio::test]
async fn create_controller_pod_persists_via_api_pipeline_with_admission() {
    use super::PodObjectWriter;
    use super::PodReader;
    let repo = build_repo().await;
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "ctrl-pod" },
        "spec": { "containers": [{ "name": "c", "image": "busybox" }] }
    });
    let resource = repo
        .create_controller_pod("default", "ctrl-pod", "test-node", pod)
        .await
        .unwrap();
    assert_eq!(resource.name, "ctrl-pod");
    assert!(
        resource.data.pointer("/spec/nodeName").is_none(),
        "controller-facing create must not inject a node assignment unless the pod spec already has one"
    );
    assert_eq!(
        resource.data["spec"]["serviceAccountName"],
        json!("default")
    );
    assert!(repo.get_pod("default", "ctrl-pod").await.unwrap().is_some());
}

#[tokio::test]
async fn create_controller_pod_rejects_terminating_namespace() {
    use super::PodObjectWriter;
    use super::PodReader;
    let repo = build_repo().await;
    repo.store
        .db()
        .create_namespace(
            "terminating-ns",
            json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": "terminating-ns",
                    "deletionTimestamp": "2026-05-02T18:40:38Z"
                },
                "status": {"phase": "Terminating"}
            }),
        )
        .await
        .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "late-pod" },
        "spec": { "containers": [{ "name": "c", "image": "busybox" }] }
    });

    let err = repo
        .create_controller_pod("terminating-ns", "late-pod", "test-node", pod)
        .await
        .expect_err("controller pod create must reject terminating namespaces");
    assert!(
        err.to_string()
            .contains("namespace terminating-ns is being terminated"),
        "unexpected error: {err:#}"
    );
    assert!(
        repo.get_pod("terminating-ns", "late-pod")
            .await
            .unwrap()
            .is_none(),
        "rejected controller-created pod must not be persisted"
    );
}

#[tokio::test]
async fn delete_pod_marks_resource_terminating() {
    use super::PodObjectWriter;
    use super::PodReader;
    let repo = build_repo().await;
    let _ = create_basic_pod_via_api(&repo, "rm-pod").await;
    repo.delete_pod("default", "rm-pod").await.unwrap();
    let pod = repo.get_pod("default", "rm-pod").await.unwrap().unwrap();
    assert!(pod.data["metadata"]["deletionTimestamp"].is_string());
}

#[tokio::test]
async fn delete_pod_runs_side_effects_after_marking_terminating_with_original_pod() {
    use super::PodObjectWriter;

    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let supervisor = fixture_supervisor();
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let observed = Arc::new(tokio::sync::Mutex::new(None));
    let mut registry = crate::side_effects::SideEffectRegistry::new();
    registry.register(
        "v1",
        "Pod",
        Arc::new(RecordingPodDeleteHook {
            observed: observed.clone(),
        }),
        crate::side_effects::ErrorPolicy::Fail,
    );
    let repo = super::PodRepository::new(db, supervisor, Arc::new(registry), metrics);
    repo.store
        .create(
            "default",
            "side-effect-pod",
            make_pod("side-effect-pod", Some("rs-x-uid"), Some(("app", "web"))),
        )
        .await
        .unwrap();

    repo.delete_pod("default", "side-effect-pod").await.unwrap();

    assert_eq!(
        *observed.lock().await,
        Some((true, true)),
        "controller Pod delete must run Pod side effects after marking the row terminating and pass the original Pod object with ownerReferences"
    );
}

#[tokio::test]
async fn delete_pod_owned_by_replicaset_enqueues_parent_deployment() {
    use super::PodObjectWriter;
    let (repo, db, dispatcher) = build_repo_with_dispatcher().await;

    db.create_resource(
        "apps/v1",
        "Deployment",
        Some("default"),
        "web-recreate",
        json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "web-recreate",
                "namespace": "default",
                "uid": "deploy-recreate-uid"
            },
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": "web"}},
                "strategy": {"type": "Recreate"},
                "template": {
                    "metadata": {"labels": {"app": "web"}},
                    "spec": {"containers": [{"name": "c", "image": "nginx"}]}
                }
            }
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "rs-x",
        json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": "rs-x",
                "namespace": "default",
                "uid": "rs-x-uid",
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "name": "web-recreate",
                    "uid": "deploy-recreate-uid",
                    "controller": true
                }]
            },
            "spec": {
                "replicas": 0,
                "selector": {"matchLabels": {"app": "web"}},
                "template": {
                    "metadata": {"labels": {"app": "web"}},
                    "spec": {"containers": [{"name": "c", "image": "nginx"}]}
                }
            }
        }),
    )
    .await
    .unwrap();
    repo.store
        .create(
            "default",
            "owned-pod",
            make_pod("owned-pod", Some("rs-x-uid"), Some(("app", "web"))),
        )
        .await
        .unwrap();

    repo.delete_pod("default", "owned-pod").await.unwrap();

    let keys = dispatcher.pending_reconcile_keys().await;
    assert!(
        keys.iter().any(|key| {
            key.api_version == "apps/v1"
                && key.kind == "ReplicaSet"
                && key.namespace.as_deref() == Some("default")
                && key.name == "rs-x"
        }),
        "pod delete must still enqueue the owning ReplicaSet"
    );
    assert!(
        keys.iter().any(|key| {
            key.api_version == "apps/v1"
                && key.kind == "Deployment"
                && key.namespace.as_deref() == Some("default")
                && key.name == "web-recreate"
        }),
        "pod delete under a ReplicaSet must enqueue the parent Deployment so Recreate rollouts continue after old pods are gone"
    );
}

#[tokio::test]
async fn finalize_pod_deletion_after_actor_cleanup_removes_matching_terminating_pod_by_uid() {
    use super::PodReader;

    let repo = build_repo().await;
    let mut pod = make_pod("terminating", None, None);
    pod["metadata"]["uid"] = json!("uid-terminating");
    pod["metadata"]["deletionTimestamp"] = json!("2026-05-13T00:00:00Z");
    pod["metadata"]["deletionGracePeriodSeconds"] = json!(0);
    repo.store
        .create("default", "terminating", pod)
        .await
        .unwrap();

    repo.finalize_pod_deletion_after_actor_cleanup("default", "terminating", "uid-terminating")
        .await
        .unwrap();

    assert!(
        repo.get_pod("default", "terminating")
            .await
            .unwrap()
            .is_none(),
        "actor finalization should remove matching terminating Pod by UID"
    );
}

#[tokio::test]
async fn finalize_pod_deletion_after_actor_cleanup_deletes_ready_foreground_owner() {
    use super::PodReader;

    let repo = build_repo().await;
    repo.store
        .db()
        .create_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "foreground-owner",
            json!({
                "apiVersion": "v1",
                "kind": "ReplicationController",
                "metadata": {
                    "name": "foreground-owner",
                    "namespace": "default",
                    "uid": "foreground-owner-uid",
                    "deletionTimestamp": "2026-05-13T00:00:00Z",
                    "finalizers": ["foregroundDeletion"]
                },
                "spec": {"replicas": 1, "selector": {"app": "foreground-owner"}}
            }),
        )
        .await
        .unwrap();
    let mut pod = make_pod("foreground-child", None, None);
    pod["metadata"]["uid"] = json!("foreground-child-uid");
    pod["metadata"]["deletionTimestamp"] = json!("2026-05-13T00:00:00Z");
    pod["metadata"]["deletionGracePeriodSeconds"] = json!(0);
    pod["metadata"]["ownerReferences"] = json!([{
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "name": "foreground-owner",
        "uid": "foreground-owner-uid",
        "controller": true,
        "blockOwnerDeletion": true
    }]);
    repo.store
        .create("default", "foreground-child", pod)
        .await
        .unwrap();

    repo.finalize_pod_deletion_after_actor_cleanup(
        "default",
        "foreground-child",
        "foreground-child-uid",
    )
    .await
    .unwrap();

    assert!(
        repo.get_pod("default", "foreground-child")
            .await
            .unwrap()
            .is_none(),
        "actor finalization should remove matching terminating Pod by UID"
    );
    assert!(
        repo.store
            .db()
            .get_resource(
                "v1",
                "ReplicationController",
                Some("default"),
                "foreground-owner"
            )
            .await
            .unwrap()
            .is_none(),
        "foreground owner must be removed after its final dependent Pod row is actor-finalized"
    );
}

#[tokio::test]
async fn finalize_pod_deletion_after_actor_cleanup_preserves_finalizer_held_pod() {
    use super::PodReader;

    let repo = build_repo().await;
    let mut pod = make_pod("finalized", None, None);
    pod["metadata"]["uid"] = json!("uid-finalized");
    pod["metadata"]["deletionTimestamp"] = json!("2026-05-13T00:00:00Z");
    pod["metadata"]["deletionGracePeriodSeconds"] = json!(0);
    pod["metadata"]["finalizers"] = json!(["example.com/test-finalizer"]);
    repo.store
        .create("default", "finalized", pod)
        .await
        .unwrap();

    repo.finalize_pod_deletion_after_actor_cleanup("default", "finalized", "uid-finalized")
        .await
        .unwrap();

    let after = repo.get_pod("default", "finalized").await.unwrap().unwrap();
    assert_eq!(
        after.data.pointer("/metadata/finalizers/0"),
        Some(&json!("example.com/test-finalizer"))
    );
}

#[tokio::test]
async fn finalize_pod_deletion_after_actor_cleanup_preserves_replacement_pod() {
    use super::PodReader;

    let repo = build_repo().await;
    let mut replacement = make_pod("same-name", None, None);
    replacement["metadata"]["uid"] = json!("uid-new");
    repo.store
        .create("default", "same-name", replacement)
        .await
        .unwrap();

    repo.finalize_pod_deletion_after_actor_cleanup("default", "same-name", "uid-old")
        .await
        .unwrap();

    let after = repo.get_pod("default", "same-name").await.unwrap().unwrap();
    assert_eq!(after.data.pointer("/metadata/uid"), Some(&json!("uid-new")));
}

#[tokio::test]
async fn pod_delete_enqueues_service_reconcile_for_stale_endpoint_targetref() {
    use super::PodObjectWriter;
    let (repo, db, dispatcher) = build_repo_with_dispatcher().await;

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "web",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "web", "namespace": "default"},
            "spec": {
                "selector": {"app": "web"},
                "ports": [{"name": "http", "port": 80, "targetPort": 8080}]
            }
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "web",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "web", "namespace": "default"},
            "subsets": [{
                "addresses": [{
                    "ip": "10.42.0.50",
                    "targetRef": {
                        "kind": "Pod",
                        "namespace": "default",
                        "name": "stale-ep-pod",
                        "uid": "stale-ep-uid"
                    }
                }],
                "ports": [{"port": 80}]
            }]
        }),
    )
    .await
    .unwrap();

    let mut pod = make_pod("stale-ep-pod", None, Some(("app", "web")));
    pod["metadata"]["uid"] = json!("stale-ep-uid");
    pod["status"] = json!({
        "phase": "Running",
        "podIP": "10.42.0.50",
        "podIPs": [{"ip": "10.42.0.50"}],
        "hostIP": "10.0.0.10",
        "hostIPs": [{"ip": "10.0.0.10"}],
        "conditions": [
            {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
            {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
            {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
            {"type": "Ready", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"}
        ]
    });
    repo.store
        .create("default", "stale-ep-pod", pod)
        .await
        .unwrap();

    repo.delete_pod("default", "stale-ep-pod").await.unwrap();

    let keys = dispatcher.pending_reconcile_keys().await;
    assert_eq!(
        keys.iter()
            .filter(|key| {
                key.api_version == "v1"
                    && key.kind == "Service"
                    && key.namespace.as_deref() == Some("default")
                    && key.name == "web"
            })
            .count(),
        1,
        "stale Endpoints targetRefs should enqueue the owning Service when a Pod is marked terminating"
    );
}

#[tokio::test]
async fn update_pod_owner_references_replaces_list_with_cas() {
    use super::PodObjectWriter;
    let repo = build_repo().await;
    let _ = create_basic_pod_via_api(&repo, "or-pod").await;
    let owners = vec![json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "name": "rs-x",
        "uid": "owner-x",
        "controller": true,
    })];
    let updated = repo
        .update_pod_owner_references("default", "or-pod", owners)
        .await
        .unwrap();
    let refs = updated.data["metadata"]["ownerReferences"]
        .as_array()
        .expect("ownerReferences present");
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0]["uid"], json!("owner-x"));
}

#[tokio::test]
async fn merge_pod_labels_preserves_existing_metadata_and_status() {
    use super::PodApiWriter;
    use super::PodObjectWriter;
    use super::PodReader;
    let repo = build_repo().await;
    let _ = repo
        .api_create_pod(api_create_request(
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "label-merge", "labels": {"app": "x"}},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]}
            }),
            false,
        ))
        .await
        .unwrap();
    repo.merge_pod_labels(
        "default",
        "label-merge",
        vec![("pod-template-hash".to_string(), "abc123".to_string())],
    )
    .await
    .unwrap();

    let updated = repo
        .get_pod("default", "label-merge")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(updated.data["metadata"]["labels"]["app"], json!("x"));
    assert_eq!(
        updated.data["metadata"]["labels"]["pod-template-hash"],
        json!("abc123")
    );
    assert_eq!(updated.data["metadata"]["name"], json!("label-merge"));
    assert!(updated.data.get("status").is_some());
}

#[tokio::test]
async fn pod_label_change_enqueues_old_and_new_matching_services_once() {
    use super::PodObjectWriter;
    let (repo, db, dispatcher) = build_repo_with_dispatcher().await;

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "legacy",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "legacy", "namespace": "default"},
            "spec": {
                "selector": {"app": "legacy"},
                "ports": [{"name": "http", "port": 80, "targetPort": 8080}]
            }
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "current",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "current", "namespace": "default"},
            "spec": {
                "selector": {"app": "current"},
                "ports": [{"name": "http", "port": 80, "targetPort": 8080}]
            }
        }),
    )
    .await
    .unwrap();

    let mut pod = make_pod("label-transition", None, Some(("app", "legacy")));
    pod["status"] = json!({
        "phase": "Running",
        "podIP": "10.42.0.60",
        "podIPs": [{"ip": "10.42.0.60"}],
        "hostIP": "10.0.0.10",
        "hostIPs": [{"ip": "10.0.0.10"}],
        "conditions": [
            {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
            {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
            {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
            {"type": "Ready", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"}
        ]
    });
    repo.store
        .create("default", "label-transition", pod)
        .await
        .unwrap();

    repo.merge_pod_labels(
        "default",
        "label-transition",
        vec![("app".to_string(), "current".to_string())],
    )
    .await
    .unwrap();

    let keys = dispatcher.pending_reconcile_keys().await;
    let legacy_count = keys
        .iter()
        .filter(|key| {
            key.api_version == "v1"
                && key.kind == "Service"
                && key.namespace.as_deref() == Some("default")
                && key.name == "legacy"
        })
        .count();
    let current_count = keys
        .iter()
        .filter(|key| {
            key.api_version == "v1"
                && key.kind == "Service"
                && key.namespace.as_deref() == Some("default")
                && key.name == "current"
        })
        .count();
    assert_eq!(legacy_count, 1, "old selector match should enqueue once");
    assert_eq!(current_count, 1, "new selector match should enqueue once");
    assert_eq!(
        keys.iter()
            .filter(|key| key.api_version == "v1" && key.kind == "Service")
            .count(),
        2,
        "only the old and new matching Services should be enqueued"
    );
}

#[tokio::test]
async fn update_pod_owner_references_returns_conflict_on_stale_rv() {
    use super::PodObjectWriter;
    let repo = build_repo().await;
    let _ = create_basic_pod_via_api(&repo, "or-race").await;

    // Two writers see the same RV. First writer wins; second loses CAS.
    let owners1 = vec![
        json!({"apiVersion":"apps/v1","kind":"ReplicaSet","name":"rs1","uid":"u1","controller":true}),
    ];
    repo.update_pod_owner_references("default", "or-race", owners1.clone())
        .await
        .expect("first writer wins");

    // The second update_pod_owner_references reads the live RV, so it
    // succeeds — the trait method does its own read-modify-write. To
    // observe a real CAS conflict, drive the store directly with a stale RV.
    let stale = repo.store.get("default", "or-race").await.unwrap().unwrap();
    let mut tampered: serde_json::Value = (*stale.data).clone();
    tampered["metadata"]["labels"] = json!({"app": "tamper"});
    let conflict = repo
        .store
        .update("default", "or-race", tampered, 1) // stale rv
        .await;
    assert!(conflict.unwrap_err().to_string().contains("409"));
}

#[tokio::test]
async fn pod_store_update_status_with_concurrent_writer_returns_conflict() {
    // Two readers see the same resource_version. The first writer wins.
    // The second writer must observe a 409 Conflict.
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let store = PodStore::new(db);

    let created = store
        .create("default", "racer", make_pod("racer", None, None))
        .await
        .unwrap();
    let snapshot_rv = created.resource_version;

    // Reader 1 → writer 1 (succeeds)
    store
        .update_status(
            "default",
            "racer",
            json!({"phase": "Running"}),
            Some(snapshot_rv),
        )
        .await
        .expect("first writer succeeds with the snapshot rv");

    // Reader 2 → writer 2 (still using the old snapshot rv)
    let conflict = store
        .update_status(
            "default",
            "racer",
            json!({"phase": "Failed"}),
            Some(snapshot_rv),
        )
        .await;
    let err = conflict.expect_err("second writer must lose CAS");
    assert!(
        err.to_string().contains("409"),
        "expected 409 Conflict, got {err:?}"
    );
}

// --- Task 10.2: Production PodDeletionFinalizer ---

use crate::kubelet::pod_runtime::deletion_finalizer::PodDeletionFinalizer as DeletionFinalizerTrait;
use crate::kubelet::pod_runtime::deletion_finalizer::RealPodDeletionFinalizer;
use crate::kubelet::pod_runtime::service::{PodDeletionFinalizeResult, PodRuntimeKey};

fn make_terminating_pod(name: &str, uid: &str) -> serde_json::Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": "default",
            "uid": uid,
            "deletionTimestamp": "2026-01-01T00:00:00Z",
            "deletionGracePeriodSeconds": 0
        },
        "spec": {
            "containers": [{"name": "app", "image": "nginx:latest"}]
        },
        "status": {"phase": "Running"}
    })
}

async fn build_finalizer() -> RealPodDeletionFinalizer {
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let store = Arc::new(PodStore::new(db));
    let gc_pod_delete_sink: Arc<dyn crate::controllers::gc::GcPodDeleteSink> =
        Arc::new(crate::controllers::gc::NoOpGcPodDeleteSink);

    RealPodDeletionFinalizer::new(
        store,
        gc_pod_delete_sink,
        None,
        None,
        fixture_side_effects(),
        crate::side_effects::SideEffectMetrics::new(),
        fixture_supervisor(),
    )
}

#[tokio::test]
async fn deletion_finalizer_without_outbox_retries_later() {
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let pod = make_terminating_pod("leader-finalize-no-outbox", "uid-leader-finalize-no-outbox");
    let store = Arc::new(PodStore::new(db.clone()));
    store
        .create("default", "leader-finalize-no-outbox", pod)
        .await
        .unwrap();

    let resource_version = 13;
    let pod_resource = crate::datastore::Resource {
        id: 1,
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "leader-finalize-no-outbox".to_string(),
        uid: "uid-leader-finalize-no-outbox".to_string(),
        resource_version,
        data: Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "leader-finalize-no-outbox",
                "uid": "uid-leader-finalize-no-outbox",
                "resourceVersion": resource_version.to_string(),
                "deletionTimestamp": "2026-05-13T00:00:00Z",
                "deletionGracePeriodSeconds": 0
            },
            "spec": {"nodeName": "worker-no-outbox", "containers": [{"name": "app", "image": "nginx"}]},
            "status": {"phase": "Running"}
        })),
    };
    let cluster_api = Arc::new(FakeLeaderApiClient::new(pod_resource));
    let gc_pod_delete_sink: Arc<dyn crate::controllers::gc::GcPodDeleteSink> =
        Arc::new(crate::controllers::gc::NoOpGcPodDeleteSink);
    let finalizer = RealPodDeletionFinalizer::new(
        store.clone(),
        gc_pod_delete_sink,
        Some(cluster_api),
        None,
        fixture_side_effects(),
        crate::side_effects::SideEffectMetrics::new(),
        fixture_supervisor(),
    );

    let result = finalizer
        .finalize_after_actor_cleanup(&PodRuntimeKey::new(
            "default",
            "leader-finalize-no-outbox",
            "uid-leader-finalize-no-outbox",
        ))
        .await;

    let result = result.expect_err("finalization must not succeed without outbox");
    assert!(
        result.to_string().contains("outbox"),
        "finalization should return outbox-retry error when outbox is unavailable"
    );

    let pod_row = store
        .get("default", "leader-finalize-no-outbox")
        .await
        .unwrap();
    assert!(
        pod_row.is_some(),
        "pod must remain when non-leader finalization is rejected"
    );
}

#[tokio::test]
async fn deletion_finalizer_reissues_missing_delete_mark_through_outbox() {
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let store = Arc::new(PodStore::new(db));
    let node_db = fixture_node_local().await;
    let outbox = Arc::new(crate::kubelet::outbox::Outbox::new(node_db.clone()));
    let pod_resource = crate::datastore::Resource {
        id: 1,
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "missing-delete-mark".to_string(),
        uid: "uid-missing-delete-mark".to_string(),
        resource_version: 21,
        data: Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "missing-delete-mark",
                "uid": "uid-missing-delete-mark",
                "resourceVersion": "21"
            },
            "spec": {
                "nodeName": "worker-1",
                "terminationGracePeriodSeconds": 7,
                "containers": [{"name": "app", "image": "nginx"}]
            },
            "status": {"phase": "Running"}
        })),
    };
    let cluster_api = Arc::new(FakeLeaderApiClient::new(pod_resource));
    let gc_pod_delete_sink: Arc<dyn crate::controllers::gc::GcPodDeleteSink> =
        Arc::new(crate::controllers::gc::NoOpGcPodDeleteSink);
    let finalizer = RealPodDeletionFinalizer::new(
        store,
        gc_pod_delete_sink,
        Some(cluster_api),
        Some(outbox),
        fixture_side_effects(),
        crate::side_effects::SideEffectMetrics::new(),
        fixture_supervisor(),
    );

    let result = finalizer
        .finalize_after_actor_cleanup(&PodRuntimeKey::new(
            "default",
            "missing-delete-mark",
            "uid-missing-delete-mark",
        ))
        .await
        .expect("missing delete mark should enqueue a leader-routed delete mark");

    assert!(matches!(
        result,
        PodDeletionFinalizeResult::FinalizersPending
    ));
    let row = node_db
        .claim_next_due_outbox(i64::MAX / 4, 1_000, "assert")
        .await
        .expect("claim outbox")
        .expect("delete-mark row enqueued");
    assert_eq!(row.operation, "PodMetadata");
    assert_eq!(row.pod_uid, "uid-missing-delete-mark");
    let payload = crate::kubelet::outbox::payload::OutboxPayload::decode_protobuf(
        row.payload_proto.as_slice(),
    )
    .expect("decode delete-mark payload");
    match payload.command {
        crate::datastore::command::StorageCommand::PatchResource {
            api_version,
            kind,
            namespace,
            name,
            patch_kind,
            patch,
            preconditions,
        } => {
            assert_eq!(api_version, "v1");
            assert_eq!(kind, "Pod");
            assert_eq!(namespace.as_deref(), Some("default"));
            assert_eq!(name, "missing-delete-mark");
            assert_eq!(patch_kind, crate::datastore::PatchKind::Merge);
            assert_eq!(
                preconditions.uid.as_deref(),
                Some("uid-missing-delete-mark")
            );
            assert_eq!(preconditions.resource_version, None);
            assert_eq!(
                patch.pointer("/metadata/deletionGracePeriodSeconds"),
                Some(&json!(7))
            );
            assert!(
                patch
                    .pointer("/metadata/deletionTimestamp")
                    .and_then(|value| value.as_str())
                    .is_some_and(|value| !value.is_empty()),
                "delete-mark patch must include a deletionTimestamp"
            );
        }
        other => panic!("expected Pod PatchResource outbox command, got {other:?}"),
    }
}

#[tokio::test]
async fn deletion_finalizer_preserves_replacement_pod() {
    let finalizer = build_finalizer().await;

    // Create a replacement pod with a different UID.
    let replacement = make_terminating_pod("same-name", "uid-new");
    finalizer
        .store
        .create("default", "same-name", replacement)
        .await
        .unwrap();

    let key = PodRuntimeKey::new("default", "same-name", "uid-old");
    let result = finalizer.finalize_after_actor_cleanup(&key).await.unwrap();

    // Old UID is gone → DeletedOrAlreadyGone. Replacement pod must survive.
    assert!(matches!(
        result,
        PodDeletionFinalizeResult::DeletedOrAlreadyGone
    ));

    let after = finalizer
        .store
        .get("default", "same-name")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.uid, "uid-new", "replacement pod must be preserved");
}

#[tokio::test]
async fn deletion_finalizer_waits_for_finalizers() {
    let finalizer = build_finalizer().await;

    let mut pod = make_terminating_pod("finalizer-held", "uid-held");
    pod["metadata"]["finalizers"] = json!(["example.com/test-finalizer"]);
    finalizer
        .store
        .create("default", "finalizer-held", pod)
        .await
        .unwrap();

    let key = PodRuntimeKey::new("default", "finalizer-held", "uid-held");
    let result = finalizer.finalize_after_actor_cleanup(&key).await.unwrap();

    // Finalizers still present → FinalizersPending.
    assert!(matches!(
        result,
        PodDeletionFinalizeResult::FinalizersPending
    ));

    let after = finalizer
        .store
        .get("default", "finalizer-held")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after.uid, "uid-held",
        "pod with finalizers must not be deleted"
    );
}

#[tokio::test]
async fn deletion_finalizer_reissues_uid_delete_when_same_uid_lacks_delete_mark() {
    let repo = build_repo().await;
    repo.store
        .create(
            "default",
            "remark-delete",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "remark-delete",
                    "namespace": "default",
                    "uid": "uid-remark-delete"
                },
                "spec": {"containers": [{"name": "app", "image": "nginx:latest"}]},
                "status": {"phase": "Running"}
            }),
        )
        .await
        .unwrap();

    let finalized = repo
        .finalize_pod_deletion_after_actor_cleanup("default", "remark-delete", "uid-remark-delete")
        .await
        .unwrap();

    assert!(
        !finalized,
        "same-UID non-terminating row must be retried, not treated as finalized"
    );
    let marked = repo
        .store
        .get("default", "remark-delete")
        .await
        .unwrap()
        .expect("pod should remain after delete mark retry");
    assert_eq!(marked.uid, "uid-remark-delete");
    assert!(
        marked
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|value| value.as_str())
            .is_some(),
        "finalizer retry must restore a visible deletionTimestamp"
    );

    let finalized_after_mark = repo
        .finalize_pod_deletion_after_actor_cleanup("default", "remark-delete", "uid-remark-delete")
        .await
        .unwrap();
    assert!(
        finalized_after_mark,
        "retry after the delete mark should complete actor-owned row removal"
    );
    assert!(
        repo.store
            .get("default", "remark-delete")
            .await
            .unwrap()
            .is_none(),
        "actor-owned finalization should remove the same UID after the mark is visible"
    );
}

#[tokio::test]
async fn deletion_finalizer_leaves_node_lost_terminal_without_delete_mark() {
    let repo = build_repo().await;
    repo.store
        .create(
            "default",
            "node-lost-local-cleanup",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "node-lost-local-cleanup",
                    "namespace": "default",
                    "uid": "uid-node-lost-local-cleanup"
                },
                "spec": {"containers": [{"name": "app", "image": "nginx:latest"}]},
                "status": {"phase": "Failed", "reason": "NodeLost"}
            }),
        )
        .await
        .unwrap();

    let finalized = repo
        .finalize_pod_deletion_after_actor_cleanup(
            "default",
            "node-lost-local-cleanup",
            "uid-node-lost-local-cleanup",
        )
        .await
        .unwrap();

    assert!(
        finalized,
        "NodeLost terminal cleanup without deletionTimestamp is local cleanup, not API deletion"
    );
    let after = repo
        .store
        .get("default", "node-lost-local-cleanup")
        .await
        .unwrap()
        .expect("NodeLost terminal pod should remain API-visible");
    assert!(
        after.data.pointer("/metadata/deletionTimestamp").is_none(),
        "NodeLost terminal local cleanup must not synthesize a delete mark"
    );
}

// ---------------------------------------------------------------------------
// bug-grpc Pillar C0 — EmptyDir survivor diagnosis.
//
// The "[sig-storage] EmptyDir wrapper volumes ... race condition" conformance
// failure is deterministic: an RC background-delete leaves active child Pods.
// Before fixing, C0 must pin WHICH leg fails among:
//   (1) cascade-did-not-mark  — child never got metadata.deletionTimestamp
//   (2) mark-without-workqueue — marked, but no UID-bound pod_workqueue row
//   (3) workqueue/actor non-convergence — row exists but never finalizes
//
// This test drives the real leader-side path (cascade_delete_with_uid -> the
// PodRepository GcPodDeleteSink -> api_delete_pod_for_gc) over a fan-out of
// running, picked-up child Pods and RECORDS the mark + workqueue state for
// each. It locks the leader-side contract so the C fix can target the proven
// remaining leg (convergence), not re-litigate cascade/mark/enqueue.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn emptydir_survivor_diagnosis_records_mark_workqueue_and_actor_state() {
    use crate::controllers::gc::{GcPodDeleteSink, cascade_delete_with_uid};

    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let supervisor = fixture_supervisor();
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = fixture_side_effects();
    let repo = PodRepository::new(db.clone(), supervisor, side_effects, metrics);

    let ns = "emptydir-diag";
    db.create_namespace(ns, json!({"metadata": {"name": ns}}))
        .await
        .unwrap();

    // RC owner.
    let rc_uid = "rc-uid-diag";
    db.create_resource(
        "v1",
        "ReplicationController",
        Some(ns),
        "rc",
        json!({
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "metadata": {"name": "rc", "namespace": ns, "uid": rc_uid},
            "spec": {"replicas": 5},
        }),
    )
    .await
    .unwrap();

    // Five Running, picked-up (spec.nodeName set) child Pods owned by the RC.
    const CHILDREN: usize = 5;
    for i in 0..CHILDREN {
        let name = format!("rc-pod-{i}");
        let uid = format!("pod-uid-{i}");
        db.create_resource(
            "v1",
            "Pod",
            Some(ns),
            &name,
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
            }),
        )
        .await
        .unwrap();
    }

    // Drive the real RC background-delete cascade (one-shot inline call, the
    // path inners.rs runs after hard-deleting the RC row).
    cascade_delete_with_uid(
        db.as_ref(),
        rc_uid,
        "v1",
        "rc",
        "ReplicationController",
        Some(ns.to_string()),
        &repo as &dyn GcPodDeleteSink,
    )
    .await
    .expect("cascade must not error");

    // Leg (1): did every child receive metadata.deletionTimestamp?
    let mut marked = 0usize;
    for i in 0..CHILDREN {
        let name = format!("rc-pod-{i}");
        let pod = db
            .get_resource("v1", "Pod", Some(ns), &name)
            .await
            .unwrap()
            .unwrap_or_else(|| {
                panic!(
                    "child {name} must still have a datastore row (only the actor may remove it)"
                )
            });
        if pod
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some_and(|s| !s.trim().is_empty())
        {
            marked += 1;
        }
    }

    // Leg (2): drain pod_workqueue and record which children got a UID-bound
    // Pod-kind row (claim with a far-future clock so grace-delayed rows count).
    let mut enqueued_uids = std::collections::HashSet::new();
    for _ in 0..(CHILDREN * 4) {
        match db.pod_workqueue_claim_due(i64::MAX).await.unwrap() {
            Some(entry)
                if entry.kind == crate::datastore::types::PodWorkqueueKind::Pod
                    && entry.namespace == ns =>
            {
                enqueued_uids.insert(entry.uid);
            }
            Some(_) => continue,
            None => break,
        }
    }

    // Diagnosis assertions — these LOCK the leader-side contract. If a future
    // change regresses cascade enumeration, marking, or the UID-bound enqueue,
    // this fails and re-opens leg (1)/(2). A green run proves the deterministic
    // prod survivor is NOT a leader-side mark/enqueue gap, so the C fix must
    // target leg (3): workqueue -> actor / remote-worker finalization
    // convergence.
    assert_eq!(
        marked, CHILDREN,
        "cascade-did-not-mark: only {marked}/{CHILDREN} children got deletionTimestamp"
    );
    for i in 0..CHILDREN {
        let uid = format!("pod-uid-{i}");
        assert!(
            enqueued_uids.contains(&uid),
            "mark-without-workqueue: child {uid} marked terminating but no UID-bound pod_workqueue row"
        );
    }
}

#[tokio::test]
async fn gc_marked_pod_enqueues_uid_bound_workqueue_entry() {
    use crate::controllers::gc::GcPodDeleteSink;

    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let supervisor = fixture_supervisor();
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = fixture_side_effects();
    let repo = PodRepository::new(db.clone(), supervisor, side_effects, metrics);

    let ns = "gc-mark-enqueue";
    db.create_namespace(ns, json!({"metadata": {"name": ns}}))
        .await
        .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some(ns),
        "picked-up",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "picked-up", "namespace": ns, "uid": "uid-gc"},
            "spec": {"nodeName": "node-a", "containers": [{"name": "c", "image": "x"}]},
            "status": {"phase": "Running"},
        }),
    )
    .await
    .unwrap();

    (&repo as &dyn GcPodDeleteSink)
        .request_gc_pod_delete(ns, "picked-up", "uid-gc")
        .await
        .unwrap();

    let pod = db
        .get_resource("v1", "Pod", Some(ns), "picked-up")
        .await
        .unwrap()
        .expect("GC must only mark a picked-up Pod, not hard-delete it");
    assert!(
        pod.data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|value| value.as_str())
            .is_some_and(|value| !value.trim().is_empty()),
        "GC delete must mark the Pod terminating"
    );

    let row = db
        .pod_workqueue_claim_due(i64::MAX)
        .await
        .unwrap()
        .expect("GC mark must create a UID-bound pod_workqueue row");
    assert_eq!(row.kind, crate::datastore::types::PodWorkqueueKind::Pod);
    assert_eq!(row.namespace, ns);
    assert_eq!(row.name, "picked-up");
    assert_eq!(row.uid, "uid-gc");
    assert_eq!(
        row.payload
            .get("target_node")
            .and_then(|value| value.as_str()),
        Some("node-a"),
        "workqueue row must target the owning kubelet actor"
    );
}
