//! Focused Pod-repository composition fixtures owned by the base integration package.

use std::sync::Arc;

use klights_cluster_datastore::sqlite::embedded::ResourceMutationPauseOperation as IntegrationResourceMutationPauseOperation;
use klights_pod_api::PodSubresourceMutation as _;
use klights_reconcile_api::ControllerDispatcherPort as _;

pub struct IntegrationSchedulerBindGate {
    gate: Arc<crate::bootstrap::composition_adapters::pod_native_adapter::SchedulerBindGateForTest>,
}

impl IntegrationSchedulerBindGate {
    pub async fn wait_for_entered_at_least(&self, target: usize) {
        self.gate.wait_for_entered_at_least(target).await;
    }

    pub fn release_all(&self) {
        self.gate.release_all();
    }
}

/// Opaque worker-owned repository composition for node-local/outbox tests.
pub struct IntegrationPodWorkerComposition {
    repository: Arc<crate::kubelet::pod_repository::PodRepository>,
    node_local: Arc<crate::datastore::node_local::NodeLocalStores>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntegrationPodFinalizationOutcome {
    DeletedOrAlreadyGone,
    Queued,
    FinalizersPending,
}

async fn integration_finalize_pod_after_actor_cleanup(
    repository: &crate::kubelet::pod_repository::PodRepository,
    namespace: &str,
    name: &str,
    uid: &str,
) -> anyhow::Result<IntegrationPodFinalizationOutcome> {
    let key = crate::kubelet::pod_runtime::service::PodRuntimeKey::new(namespace, name, uid);
    Ok(match repository.deletion_finalizer().finalize_after_actor_cleanup(&key).await? {
        crate::kubelet::pod_runtime::service::PodDeletionFinalizeResult::DeletedOrAlreadyGone => {
            IntegrationPodFinalizationOutcome::DeletedOrAlreadyGone
        }
        crate::kubelet::pod_runtime::service::PodDeletionFinalizeResult::Queued => {
            IntegrationPodFinalizationOutcome::Queued
        }
        crate::kubelet::pod_runtime::service::PodDeletionFinalizeResult::FinalizersPending => {
            IntegrationPodFinalizationOutcome::FinalizersPending
        }
    })
}

impl IntegrationPodWorkerComposition {
    pub async fn new(resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>) -> Self {
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let node_local = Arc::new(
            crate::datastore::node_local::selector::open_node_local(
                crate::datastore::backend_kind::BackendKind::Sqlite,
                None,
                supervisor.clone(),
                None,
                "sqlite:pod-worker-composition-integration",
            )
            .await
            .expect("worker repository node-local store"),
        );
        let ports = klights_kubelet::node_outbox::OutboxStores::new(
            node_local.outbox_producer(),
            node_local.outbox_dispatcher(),
            node_local.pod_status_checkpoints(),
            node_local.runtime_observation_checkpoints(),
            node_local.outbox_status_stamps(),
        );
        let outbox = Arc::new(klights_kubelet::node_outbox::Outbox::compose(
            ports,
            crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec(),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(klights_supervisor::SystemWallClock),
        ));
        let parts = crate::pod_repository_composition::build_worker_pod_repository_parts(
            crate::pod_repository_composition::WorkerPodRepositoryBuildConfig {
                resource_query,
                pod_workqueue_store: node_local.pod_workqueue(),
                supervisor,
                metrics: klights_controllers::side_effects::SideEffectMetrics::new(),
                pod_network_cache: Arc::new(IntegrationEmptyPodNetworkCache),
                assignment_waiter: Arc::new(klights_networking::PodNetworkAssignmentBus::new()),
                outbox,
            },
        );
        Self {
            repository: Arc::new(parts.repository),
            node_local,
        }
    }

    pub async fn claim_next_due_outbox(
        &self,
        now_ms: i64,
        lease_ms: i64,
        lease_token: &str,
    ) -> anyhow::Result<Option<IntegrationClaimedPodOutbox>> {
        claim_pod_outbox(&self.node_local, now_ms, lease_ms, lease_token).await
    }

    pub async fn finalize_pod_deletion_after_actor_cleanup(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<IntegrationPodFinalizationOutcome> {
        integration_finalize_pod_after_actor_cleanup(self.repository.as_ref(), namespace, name, uid)
            .await
    }

    pub async fn get_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        crate::kubelet::pod_repository::PodReader::get_pod(
            self.repository.as_ref(),
            namespace,
            name,
        )
        .await
    }

    pub async fn get_pod_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        crate::kubelet::pod_repository::PodReader::get_pod_for_uid(
            self.repository.as_ref(),
            namespace,
            name,
            uid,
        )
        .await
    }

    pub async fn set_pod_status_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        update: crate::kubelet::pod_repository::PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::set_pod_status_for_uid(
            self.repository.as_ref(),
            namespace,
            name,
            uid,
            update,
            expected_rv,
        )
        .await
    }

    pub async fn apply_runtime_reconcile_status_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        update: crate::kubelet::pod_repository::RuntimeReconcileStatus,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::apply_runtime_reconcile_status_for_uid(
            self.repository.as_ref(),
            namespace,
            name,
            uid,
            update,
            expected_rv,
        )
        .await
    }

    pub async fn record_sandbox_id_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        sandbox_id: &str,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodMetadataWriter::record_sandbox_id_for_uid(
            self.repository.as_ref(),
            namespace,
            name,
            uid,
            sandbox_id,
        )
        .await
    }

    #[allow(dead_code)]
    pub async fn update_pod_owner_references_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        owner_references: Vec<serde_json::Value>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodObjectWriter::update_pod_owner_references_for_uid(
            self.repository.as_ref(),
            namespace,
            name,
            uid,
            owner_references,
        )
        .await
    }

    #[allow(dead_code)]
    pub async fn merge_pod_labels_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        labels: Vec<(String, String)>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodObjectWriter::merge_pod_labels_for_uid(
            self.repository.as_ref(),
            namespace,
            name,
            uid,
            labels,
        )
        .await
    }

    pub async fn seed_status_checkpoint(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        base_position: i64,
        status: serde_json::Value,
        updated_ms: i64,
    ) -> anyhow::Result<()> {
        let checkpoint = klights_node_store::PodStatusCheckpointUpsert::try_new(
            klights_types::PodIdentity::new(namespace, name, uid),
            base_position,
            serde_json::to_vec(&status)?,
            updated_ms,
        )?;
        self.node_local
            .pod_status_checkpoints()
            .upsert_pod_status_checkpoint(checkpoint)
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))
    }

    pub async fn has_status_checkpoint(&self, uid: &str) -> anyhow::Result<bool> {
        let key = klights_node_store::PodCheckpointKey::try_new(uid)?;
        self.node_local
            .pod_status_checkpoints()
            .get_pod_status_checkpoint(key)
            .await
            .map(|value| value.is_some())
            .map_err(|error| anyhow::anyhow!(error.to_string()))
    }

    async fn dispatch_due_once(
        &self,
        delivery: Arc<dyn klights_leader_api::LeaderOutboxDelivery>,
    ) -> anyhow::Result<klights_kubelet::node_outbox::DispatchOutcome> {
        let stores = klights_kubelet::node_outbox::OutboxStores::new(
            self.node_local.outbox_producer(),
            self.node_local.outbox_dispatcher(),
            self.node_local.pod_status_checkpoints(),
            self.node_local.runtime_observation_checkpoints(),
            self.node_local.outbox_status_stamps(),
        );
        klights_kubelet::node_outbox::OutboxDispatcher::new(
            stores,
            crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec(),
            delivery,
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(klights_supervisor::SystemWallClock),
        )
        .dispatch_due_once(i64::MAX / 4)
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))
    }
}

pub struct IntegrationWorkerFinalizationRaceOutcome {
    pub initially_pending: bool,
    pub resource_version_advanced: bool,
    pub dispatched: bool,
    pub removed_after_dispatch: bool,
    pub completed_after_committed_absence: bool,
    pub node_mismatch_rejected: bool,
}

pub struct IntegrationWorkerFinalizationDeliveryOutcome {
    pub queued: bool,
    pub exact_uid_bound_command: bool,
    pub committed_resource_receipt: bool,
    pub authoritative_pod_removed: bool,
}

pub async fn run_worker_actor_finalization_delivery_scenario()
-> anyhow::Result<IntegrationWorkerFinalizationDeliveryOutcome> {
    let sqlite = crate::datastore::sqlite::Datastore::new_in_memory().await?;
    let db: crate::datastore::DatastoreHandle = Arc::new(sqlite.clone());
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "leader-finalize",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "leader-finalize",
                "uid": "uid-leader-finalize",
                "deletionTimestamp": "2026-05-13T00:00:00Z",
                "deletionGracePeriodSeconds": 0
            },
            "spec": {
                "nodeName": "worker-1",
                "containers": [{"name": "app", "image": "nginx"}]
            },
            "status": {"phase": "Running"}
        }),
    )
    .await?;
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let cluster_api = Arc::new(
        crate::control_plane::client::local::LocalApiClient::new_with_node_lease_tracker_and_passive_reads(
            db.clone(),
            crate::datastore::selector::sqlite_passive_read_ports(&sqlite),
            "worker-1".to_string(),
            Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(chrono::Utc::now())),
            crate::control_plane::client::local::always_leader_watch(),
            klights_supervisor::FileProcessExecutor::new(supervisor),
        ),
    );
    let repository = IntegrationPodWorkerComposition::new(cluster_api).await;
    let queued = repository
        .finalize_pod_deletion_after_actor_cleanup(
            "default",
            "leader-finalize",
            "uid-leader-finalize",
        )
        .await?
        == IntegrationPodFinalizationOutcome::Queued;
    let request = klights_node_store::OutboxClaimRequest::try_new(
        i64::MAX / 4,
        1_000,
        "finalization-delivery",
    )?;
    let row = repository
        .node_local
        .outbox_dispatcher()
        .claim_next_due_outbox(request)
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?
        .expect("worker finalization must enqueue an outbox row");
    let command = crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec()
        .decode(row.payload())
        .expect("worker finalization command must decode");
    let exact_uid_bound_command = matches!(
        &command,
        klights_cluster_core::StorageCommand::FinalizeBoundPod {
            namespace,
            name,
            pod_uid,
            node_name,
            observed_resource_version,
        } if namespace == "default"
            && name == "leader-finalize"
            && pod_uid == "uid-leader-finalize"
            && node_name == "worker-1"
            && *observed_resource_version > 0
    );
    let applied = crate::bootstrap::outbox_apply_adapter::propose_outbox_command_on_backend(
        db.as_ref(),
        row.idempotency_key(),
        klights_kubelet::outbox::OutboxOperation::PodMetadata,
        command,
        "worker-1",
        None,
    )
    .await
    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let (_, _, _, committed_resource) = applied.into_parts();
    let authoritative_pod_removed = db
        .get_resource("v1", "Pod", Some("default"), "leader-finalize")
        .await?
        .is_none();
    Ok(IntegrationWorkerFinalizationDeliveryOutcome {
        queued,
        exact_uid_bound_command,
        committed_resource_receipt: committed_resource.is_some(),
        authoritative_pod_removed,
    })
}

pub async fn run_worker_actor_finalization_race()
-> anyhow::Result<IntegrationWorkerFinalizationRaceOutcome> {
    let sqlite = crate::datastore::sqlite::Datastore::new_in_memory().await?;
    let db: crate::datastore::DatastoreHandle = Arc::new(sqlite.clone());
    let created = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "rv-retry-finalize",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "rv-retry-finalize",
                    "uid": "uid-rv-retry-finalize",
                    "deletionTimestamp": "2026-07-24T00:00:00Z",
                    "deletionGracePeriodSeconds": 0
                },
                "spec": {
                    "nodeName": "worker-1",
                    "containers": [{"name": "app", "image": "nginx"}]
                },
                "status": {"phase": "Running"}
            }),
        )
        .await?;
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let cluster_api = Arc::new(
        crate::control_plane::client::local::LocalApiClient::new_with_node_lease_tracker_and_passive_reads(
            db.clone(),
            crate::datastore::selector::sqlite_passive_read_ports(&sqlite),
            "worker-1".to_string(),
            Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(chrono::Utc::now())),
            crate::control_plane::client::local::always_leader_watch(),
            klights_supervisor::FileProcessExecutor::new(supervisor),
        ),
    );
    let repository = IntegrationPodWorkerComposition::new(cluster_api.clone()).await;
    let initially_pending = repository
        .finalize_pod_deletion_after_actor_cleanup(
            "default",
            "rv-retry-finalize",
            "uid-rv-retry-finalize",
        )
        .await?
        == IntegrationPodFinalizationOutcome::Queued;
    let raced = db
        .update_status_only(
            "v1",
            "Pod",
            Some("default"),
            "rv-retry-finalize",
            serde_json::json!({"phase": "Running", "reason": "ConcurrentStatus"}),
            Some(created.resource_version),
        )
        .await?;
    let dispatched = repository.dispatch_due_once(cluster_api.clone()).await?
        == klights_kubelet::node_outbox::DispatchOutcome::Dispatched;
    let removed_after_dispatch = db
        .get_resource("v1", "Pod", Some("default"), "rv-retry-finalize")
        .await?
        .is_none();
    let completed_after_committed_absence = repository
        .finalize_pod_deletion_after_actor_cleanup(
            "default",
            "rv-retry-finalize",
            "uid-rv-retry-finalize",
        )
        .await?
        == IntegrationPodFinalizationOutcome::DeletedOrAlreadyGone;
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "wrong-node-finalize",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "wrong-node-finalize",
                "uid": "uid-wrong-node-finalize",
                "deletionTimestamp": "2026-07-24T00:00:00Z",
                "deletionGracePeriodSeconds": 0
            },
            "spec": {
                "nodeName": "worker-2",
                "containers": [{"name": "app", "image": "nginx"}]
            }
        }),
    )
    .await?;
    repository
        .finalize_pod_deletion_after_actor_cleanup(
            "default",
            "wrong-node-finalize",
            "uid-wrong-node-finalize",
        )
        .await?;
    let _ = repository.dispatch_due_once(cluster_api).await?;
    let node_mismatch_rejected = db
        .get_resource("v1", "Pod", Some("default"), "wrong-node-finalize")
        .await?
        .is_some();
    Ok(IntegrationWorkerFinalizationRaceOutcome {
        initially_pending,
        resource_version_advanced: raced.resource_version > created.resource_version,
        dispatched,
        removed_after_dispatch,
        completed_after_committed_absence,
        node_mismatch_rejected,
    })
}

/// Opaque root-owned repository fixture for base composition tests.
pub struct IntegrationPodRepositoryComposition {
    _sqlite: crate::datastore::sqlite::Datastore,
    db: crate::datastore::DatastoreHandle,
    repository: Arc<crate::kubelet::pod_repository::PodRepository>,
    pod_api: Arc<k8s_native_service::PodApiService>,
    pod_subresource: Arc<k8s_native_service::PodSubresourceService>,
    pod_scheduling: Arc<dyn klights_pod_api::PodScheduling>,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
    background: crate::kubelet::pod_repository::background::PodRepositoryBackground,
    controller_dispatcher: Option<Arc<klights_controllers::ControllerDispatcher>>,
    node_local: Option<Arc<crate::datastore::node_local::NodeLocalStores>>,
    outbox_delivery: Option<Arc<dyn klights_leader_api::LeaderOutboxDelivery>>,
    delete_observation: Option<Arc<tokio::sync::Mutex<Option<(bool, bool)>>>>,
}

struct IntegrationEmptyPodNetworkCache;

impl klights_node_store::PodNetworkCache for IntegrationEmptyPodNetworkCache {
    fn get_network_for_uid(
        &self,
        _pod_uid: klights_node_store::PodUidKey,
    ) -> klights_node_store::CacheNetworkFuture<'_, Option<klights_node_store::PodNetworkEndpoint>>
    {
        Box::pin(async { Ok(None) })
    }

    fn get_network_for_pod(
        &self,
        _pod: klights_types::PodIdentity,
    ) -> klights_node_store::CacheNetworkFuture<'_, Option<klights_node_store::PodNetworkEndpoint>>
    {
        Box::pin(async { Ok(None) })
    }

    fn get_network_for_sandbox(
        &self,
        _sandbox_id: klights_node_store::SandboxKey,
    ) -> klights_node_store::CacheNetworkFuture<'_, Option<klights_node_store::PodNetworkEndpoint>>
    {
        Box::pin(async { Ok(None) })
    }

    fn get_network_for_assignment(
        &self,
        _sandbox_id: klights_node_store::SandboxKey,
        _pod: klights_types::PodIdentity,
    ) -> klights_node_store::CacheNetworkFuture<'_, Option<klights_node_store::PodNetworkEndpoint>>
    {
        Box::pin(async { Ok(None) })
    }

    fn delete_network_for_sandbox(
        &self,
        _sandbox_id: klights_node_store::SandboxKey,
    ) -> klights_node_store::CacheNetworkFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn delete_network_if_matches(
        &self,
        _request: klights_node_store::PodNetworkAllocationRequest,
    ) -> klights_node_store::CacheNetworkFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }

    fn list_network_assignments(
        &self,
    ) -> klights_node_store::CacheNetworkFuture<
        '_,
        Vec<klights_node_store::PodNetworkAssignmentSnapshot>,
    > {
        Box::pin(async { Ok(Vec::new()) })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntegrationBoundPodDeleteOutcome {
    Removed,
    IdentityChanged,
    FinalizersPending,
    Retry,
}

pub struct IntegrationStatusRaceOutcome {
    pub attempts: usize,
    pub resource: Option<crate::datastore::Resource>,
    pub conflict: bool,
}

pub struct IntegrationApiDeleteStatusRaceOutcome {
    pub created: crate::datastore::Resource,
    pub deleted: crate::datastore::Resource,
    pub persisted: crate::datastore::Resource,
    pub status_bumps: usize,
}

struct IntegrationDeleteStatusRacingRaftProposal {
    inner: crate::datastore::DatastoreHandle,
    pod_name: String,
    bumps: Arc<std::sync::atomic::AtomicUsize>,
}

impl IntegrationDeleteStatusRacingRaftProposal {
    async fn apply(
        &self,
        command: klights_cluster_core::StorageCommand,
        idempotency_key: &str,
        operation: klights_kubelet::outbox::OutboxOperation,
        authoring_node: &str,
    ) -> Result<
        klights_replication::proposal::RaftProposalEffect,
        klights_cluster_core::OutboxApplyError,
    > {
        let targets_delete_mark = match &command {
            klights_cluster_core::StorageCommand::UpdateResource {
                api_version,
                kind,
                namespace,
                name,
                data,
                ..
            } => {
                api_version == "v1"
                    && kind == "Pod"
                    && namespace.as_deref() == Some("default")
                    && name == &self.pod_name
                    && data
                        .pointer("/metadata/deletionTimestamp")
                        .and_then(serde_json::Value::as_str)
                        .is_some()
            }
            klights_cluster_core::StorageCommand::PatchResource {
                api_version,
                kind,
                namespace,
                name,
                patch,
                ..
            } => {
                api_version == "v1"
                    && kind == "Pod"
                    && namespace.as_deref() == Some("default")
                    && name == &self.pod_name
                    && patch
                        .pointer("/metadata/deletionTimestamp")
                        .and_then(serde_json::Value::as_str)
                        .is_some()
            }
            _ => false,
        };
        if targets_delete_mark {
            let bump = self.bumps.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            if let Some(current) = self
                .inner
                .get_resource("v1", "Pod", Some("default"), &self.pod_name)
                .await
                .map_err(|error| {
                    klights_cluster_core::OutboxApplyError::Retryable(error.to_string())
                })?
            {
                self.inner.update_status_only_with_preconditions(
                    "v1", "Pod", Some("default"), &self.pod_name,
                    serde_json::json!({"phase": "Running", "podIP": "10.42.0.55", "raceBump": bump}),
                    crate::datastore::ResourcePreconditions::uid(current.uid),
                ).await.map_err(|error| klights_cluster_core::OutboxApplyError::Retryable(error.to_string()))?;
            }
        }
        crate::bootstrap::outbox_apply_adapter::propose_outbox_command_on_backend(
            self.inner.as_ref(),
            idempotency_key,
            operation,
            command,
            authoring_node,
            None,
        )
        .await
    }
}

#[async_trait::async_trait]
impl klights_replication::proposal::RaftProposal for IntegrationDeleteStatusRacingRaftProposal {
    async fn propose_command(
        &self,
        command: klights_cluster_core::StorageCommand,
    ) -> anyhow::Result<klights_cluster_store::StorageCommandResult> {
        let effect = self
            .apply(
                command,
                &format!("delete-race-{}", uuid::Uuid::new_v4()),
                klights_kubelet::outbox::OutboxOperation::PodStatus,
                "delete-race-leader",
            )
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let (result, resource_effect, pod_endpoint_effect, committed_resource) =
            effect.into_parts();
        let applied_rv = match result {
            klights_cluster_core::OutboxApplyOutcome::Applied { applied_rv } => Some(applied_rv),
            klights_cluster_core::OutboxApplyOutcome::AlreadyApplied { applied_rv } => applied_rv,
        };
        Ok(klights_cluster_store::StorageCommandResult::new(
            applied_rv,
            None,
            None,
            resource_effect == klights_cluster_core::ResourceMutationEffect::Changed,
            committed_resource.map(klights_cluster_store::AppliedMutation::Resource),
            pod_endpoint_effect,
        ))
    }

    async fn propose_outbox_command(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: klights_cluster_core::StorageCommand,
        authoring_node: &str,
        _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> Result<klights_cluster_core::OutboxApplyOutcome, klights_cluster_core::OutboxApplyError>
    {
        let operation =
            klights_kubelet::outbox::OutboxOperation::try_from(operation).map_err(|error| {
                klights_cluster_core::OutboxApplyError::Retryable(error.to_string())
            })?;
        Ok(self
            .apply(command, idempotency_key, operation, authoring_node)
            .await?
            .into_parts()
            .0)
    }
}

pub async fn run_raft_delete_mark_status_race(
    pod_name: &str,
    grace_period_seconds: Option<i64>,
) -> anyhow::Result<IntegrationApiDeleteStatusRaceOutcome> {
    let sqlite = crate::datastore::sqlite::Datastore::new_in_memory().await?;
    let inner: crate::datastore::DatastoreHandle = Arc::new(sqlite.clone());
    let bumps = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let proposal = Arc::new(IntegrationDeleteStatusRacingRaftProposal {
        inner: inner.clone(),
        pod_name: pod_name.to_string(),
        bumps: bumps.clone(),
    });
    let db: crate::datastore::DatastoreHandle = Arc::new(
        crate::bootstrap::sequenced_datastore::SequencedDatastore::new_with_clock(
            inner,
            proposal,
            Arc::new(klights_supervisor::SystemWallClock),
        ),
    );
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let local_query: Arc<dyn klights_leader_api::LeaderResourceQuery> = Arc::new(crate::control_plane::client::local::LocalApiClient::new_with_node_lease_tracker_and_passive_reads(
        db.clone(), crate::datastore::selector::sqlite_passive_read_ports(&sqlite), "delete-race-leader".to_string(),
        Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(chrono::Utc::now())), crate::control_plane::client::local::always_leader_watch(), klights_supervisor::FileProcessExecutor::new(supervisor.clone()),
    ));
    let parts = crate::pod_repository_composition::build_integration_pod_repository_parts(
        crate::pod_repository_composition::PodRepositoryBuildConfig {
            db: db.clone(), pod_workqueue_store: None, supervisor, side_effects: Arc::new(klights_controllers::side_effects::SideEffectRegistry::new()),
            metrics: klights_controllers::side_effects::SideEffectMetrics::new(), pod_network_cache: Arc::new(IntegrationEmptyPodNetworkCache), assignment_waiter: Arc::new(klights_networking::PodNetworkAssignmentBus::new()),
            scheduling_mode: crate::pod_repository_composition::PodSchedulingMode::InlineSingleNode, outbox: None, cluster_api: None,
            controller_identity: crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
            #[cfg(not(test))]
            api_identity: Arc::new(crate::bootstrap::controller_adapters::system_identity_adapter::SystemIdentityGenerator),
            #[cfg(not(test))]
            gc_coordination: Arc::new(klights_controllers::ControllerCoordination::new()),
            scheduler_bind_gate: None,
        }, local_query,
    );
    use klights_pod_api::PodApiMutation as _;
    let created = parts.api.create_pod(klights_pod_api::PodApiCreateRequest { namespace: "default".to_string(), body: serde_json::json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":pod_name},"spec":{"containers":[{"name":"c","image":"busybox"}]}}), dry_run: false }).await.map_err(anyhow::Error::new)?.resource.expect("race create persists");
    let deleted = match parts
        .api
        .delete_pod(klights_pod_api::PodApiDeleteRequest {
            namespace: "default".to_string(),
            name: pod_name.to_string(),
            options: k8s_native_service::DeleteOptions {
                _grace_period_seconds: grace_period_seconds,
                preconditions: None,
                ..Default::default()
            }
            .into(),
            dry_run: false,
        })
        .await
        .map_err(anyhow::Error::new)?
    {
        klights_pod_api::PodApiDeleteOutcome::GracefulSet(resource) => resource,
        klights_pod_api::PodApiDeleteOutcome::DryRun(_) => {
            anyhow::bail!("raft delete race unexpectedly dry-ran")
        }
    };
    let persisted = db
        .get_resource("v1", "Pod", Some("default"), pod_name)
        .await?
        .expect("actor-owned row remains");
    Ok(IntegrationApiDeleteStatusRaceOutcome {
        created,
        deleted,
        persisted,
        status_bumps: bumps.load(std::sync::atomic::Ordering::SeqCst),
    })
}

#[allow(dead_code)]
pub async fn run_api_delete_status_race(
    pod_name: &str,
    grace_period_seconds: Option<i64>,
) -> anyhow::Result<IntegrationApiDeleteStatusRaceOutcome> {
    let repo = IntegrationPodRepositoryComposition::new_inline().await;
    let created = repo
        .api_create_pod(crate::kubelet::pod_repository::PodApiCreateRequest {
            namespace: "default".to_string(),
            name: String::new(),
            body: serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": pod_name},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]}
            }),
            dry_run: false,
            run_admission: false,
        })
        .await
        .map_err(anyhow::Error::new)?
        .resource
        .expect("delete race Pod create persists");
    let pause = repo._sqlite.install_resource_mutation_pause(
        IntegrationResourceMutationPauseOperation::BuildPatchCommand,
        "v1",
        "Pod",
        Some("default"),
        pod_name,
    );
    let delete = repo.api_delete_pod(
        "default",
        pod_name,
        k8s_native_service::DeleteOptions {
            _grace_period_seconds: grace_period_seconds,
            preconditions: None,
            ..Default::default()
        },
        false,
    );
    let race = async {
        pause.wait_until_reached().await;
        let current = repo
            .read_pod("default", pod_name)
            .await?
            .expect("delete race Pod exists before mark");
        let updated = repo
            .update_pod_status(
                "default",
                pod_name,
                serde_json::json!({"phase": "Running", "raceBump": 1}),
                Some(current.resource_version),
            )
            .await;
        pause.resume();
        updated
    };
    let (deleted, raced) = tokio::join!(delete, race);
    raced?;
    let deleted = match deleted.map_err(anyhow::Error::new)? {
        crate::kubelet::pod_repository::PodApiDeleteOutcome::GracefulSet(resource) => resource,
        crate::kubelet::pod_repository::PodApiDeleteOutcome::DryRun(_) => {
            anyhow::bail!("delete race unexpectedly dry-ran")
        }
    };
    let persisted = repo
        .read_pod("default", pod_name)
        .await?
        .expect("actor-owned row remains after delete mark");
    Ok(IntegrationApiDeleteStatusRaceOutcome {
        created,
        deleted,
        persisted,
        status_bumps: 1,
    })
}

pub struct IntegrationClaimedPodOutbox {
    pub operation: String,
    pub pod_uid: String,
    pub command: IntegrationPodOutboxCommand,
}

pub struct IntegrationPodWatchEvent {
    pub event_type: String,
    pub resource: crate::datastore::Resource,
}

pub struct IntegrationPodWorkqueueEntry {
    pub namespace: String,
    pub name: String,
    pub uid: String,
    pub target_node: Option<String>,
}

struct IntegrationRecordingPodDeleteHook {
    db: crate::datastore::DatastoreHandle,
    observed: Arc<tokio::sync::Mutex<Option<(bool, bool)>>>,
}

#[async_trait::async_trait]
impl klights_controllers::side_effects::SideEffect for IntegrationRecordingPodDeleteHook {
    fn name(&self) -> &'static str {
        "integration_recording_pod_delete_hook"
    }

    async fn apply(&self, resource: &serde_json::Value) -> anyhow::Result<()> {
        let namespace = resource
            .pointer("/metadata/namespace")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("default");
        let name = resource
            .pointer("/metadata/name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let exists = self
            .db
            .get_resource("v1", "Pod", Some(namespace), name)
            .await?
            .is_some();
        let original_owner = resource
            .pointer("/metadata/ownerReferences/0/name")
            .and_then(serde_json::Value::as_str)
            == Some("rs-x");
        *self.observed.lock().await = Some((exists, original_owner));
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub enum IntegrationPodOutboxCommand {
    SandboxAnnotationPatch {
        api_version: String,
        kind: String,
        namespace: Option<String>,
        name: String,
        patch_kind: klights_cluster_core::PatchKind,
        pod_uid: String,
        resource_version: i64,
        strict_resource_version: bool,
        sandbox_id: String,
    },
    DeleteMarkPatch {
        api_version: String,
        kind: String,
        namespace: Option<String>,
        name: String,
        patch_kind: klights_cluster_core::PatchKind,
        pod_uid: String,
        resource_version: Option<i64>,
        strict_resource_version: bool,
        grace_period_seconds: i64,
        has_deletion_timestamp: bool,
    },
    FinalizeBoundPod {
        namespace: String,
        name: String,
        pod_uid: String,
        node_name: String,
        observed_resource_version: i64,
    },
    Other,
}

async fn claim_pod_outbox(
    stores: &crate::datastore::node_local::NodeLocalStores,
    now_ms: i64,
    lease_ms: i64,
    lease_token: &str,
) -> anyhow::Result<Option<IntegrationClaimedPodOutbox>> {
    let request = klights_node_store::OutboxClaimRequest::try_new(now_ms, lease_ms, lease_token)?;
    stores
        .outbox_dispatcher()
        .claim_next_due_outbox(request)
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))
        .map(|row| {
            row.map(|row| {
                let command = crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec()
                    .decode(row.payload().as_ref())
                    .expect("integration outbox command must decode");
                let command = match command {
                    klights_cluster_core::StorageCommand::PatchResource {
                        api_version,
                        kind,
                        namespace,
                        name,
                        patch_kind,
                        patch,
                        preconditions,
                        strict_resource_version,
                    } if patch.pointer("/metadata/annotations/klights.dev~1sandbox-id").is_some() => {
                        IntegrationPodOutboxCommand::SandboxAnnotationPatch {
                            api_version,
                            kind,
                            namespace,
                            name,
                            patch_kind,
                            pod_uid: preconditions.uid.unwrap_or_default(),
                            resource_version: preconditions.resource_version.unwrap_or_default(),
                            strict_resource_version,
                            sandbox_id: patch
                                .pointer("/metadata/annotations/klights.dev~1sandbox-id")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or_default()
                                .to_string(),
                        }
                    }
                    klights_cluster_core::StorageCommand::PatchResource {
                        api_version,
                        kind,
                        namespace,
                        name,
                        patch_kind,
                        patch,
                        preconditions,
                        strict_resource_version,
                        ..
                    } if patch.pointer("/metadata/deletionTimestamp").is_some() => {
                        IntegrationPodOutboxCommand::DeleteMarkPatch {
                            api_version,
                            kind,
                            namespace,
                            name,
                            patch_kind,
                            pod_uid: preconditions.uid.unwrap_or_default(),
                            resource_version: preconditions.resource_version,
                            strict_resource_version,
                            grace_period_seconds: patch
                                .pointer("/metadata/deletionGracePeriodSeconds")
                                .and_then(serde_json::Value::as_i64)
                                .unwrap_or_default(),
                            has_deletion_timestamp: patch
                                .pointer("/metadata/deletionTimestamp")
                                .and_then(serde_json::Value::as_str)
                                .is_some_and(|value| !value.is_empty()),
                        }
                    }
                    klights_cluster_core::StorageCommand::FinalizeBoundPod {
                        namespace,
                        name,
                        pod_uid,
                        node_name,
                        observed_resource_version,
                    } => IntegrationPodOutboxCommand::FinalizeBoundPod {
                        namespace,
                        name,
                        pod_uid,
                        node_name,
                        observed_resource_version,
                    },
                    _ => IntegrationPodOutboxCommand::Other,
                };
                IntegrationClaimedPodOutbox {
                operation: row.operation().to_string(),
                pod_uid: row.subject().pod_uid().to_string(),
                command,
            }})
        })
}

#[derive(Clone, Copy)]
pub enum IntegrationDeferredRuntimeFinalizerOutcome {
    Deleted,
    Pending,
    Error,
}

struct IntegrationFixedDeletionFinalizer {
    outcome: IntegrationDeferredRuntimeFinalizerOutcome,
}

#[async_trait::async_trait]
impl crate::kubelet::pod_runtime::deletion_finalizer::PodDeletionFinalizer
    for IntegrationFixedDeletionFinalizer
{
    async fn finalize_after_actor_cleanup(
        &self,
        _key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
    ) -> anyhow::Result<klights_kubelet::runtime_types::PodDeletionFinalizeResult> {
        use klights_kubelet::runtime_types::PodDeletionFinalizeResult;
        match self.outcome {
            IntegrationDeferredRuntimeFinalizerOutcome::Deleted => {
                Ok(PodDeletionFinalizeResult::DeletedOrAlreadyGone)
            }
            IntegrationDeferredRuntimeFinalizerOutcome::Pending => {
                Ok(PodDeletionFinalizeResult::FinalizersPending)
            }
            IntegrationDeferredRuntimeFinalizerOutcome::Error => {
                anyhow::bail!("injected finalizer error")
            }
        }
    }
}

pub async fn run_deferred_runtime_cleanup_case(
    uid: &str,
    outcome: IntegrationDeferredRuntimeFinalizerOutcome,
) -> (bool, bool) {
    use crate::kubelet::pod_runtime::deletion_finalizer::PodDeletionFinalizer as _;
    let deferred = crate::kubelet::pod_repository::status::DeferredRuntimeReducerHandle::default();
    deferred.insert_marker(uid);
    let finalizer = crate::kubelet::pod_repository::DeferredRuntimeCleanupFinalizer::new(
        Arc::new(IntegrationFixedDeletionFinalizer { outcome }),
        deferred.clone(),
    );
    let result = finalizer
        .finalize_after_actor_cleanup(&crate::kubelet::pod_runtime::service::PodRuntimeKey::new(
            "default",
            "deferred-runtime",
            uid,
        ))
        .await;
    (result.is_ok(), !deferred.contains(uid))
}

enum IntegrationStatusRaceMode {
    Scheduler,
    Probe {
        conflicts_remaining: std::sync::atomic::AtomicUsize,
    },
}

struct IntegrationStatusRaceWriter {
    store: Arc<crate::kubelet::pod_repository::store::PodStore>,
    attempts: std::sync::atomic::AtomicUsize,
    mode: IntegrationStatusRaceMode,
}

#[async_trait::async_trait]
impl crate::kubelet::pod_repository::state_only_writer::StateOnlyWriter
    for IntegrationStatusRaceWriter
{
    async fn write_status(
        &self,
        namespace: &str,
        name: &str,
        status: serde_json::Value,
        expected_resource_version: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        let attempt = self
            .attempts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        let inject = match &self.mode {
            IntegrationStatusRaceMode::Scheduler => attempt == 1,
            IntegrationStatusRaceMode::Probe {
                conflicts_remaining,
            } => conflicts_remaining
                .fetch_update(
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                    |remaining| remaining.checked_sub(1),
                )
                .is_ok(),
        };
        if inject {
            let current = self.store.get(namespace, name).await?.expect("race pod");
            let mut raced = current.data.as_ref().clone();
            match self.mode {
                IntegrationStatusRaceMode::Scheduler => {
                    raced["spec"]["nodeName"] = serde_json::json!("dp")
                }
                IntegrationStatusRaceMode::Probe { .. } => {
                    if raced
                        .pointer("/metadata/annotations")
                        .and_then(serde_json::Value::as_object)
                        .is_none()
                    {
                        raced["metadata"]["annotations"] = serde_json::json!({});
                    }
                    raced["metadata"]["annotations"]["klights.dev/probe-readiness-race-attempt"] =
                        serde_json::json!(attempt.to_string());
                }
            }
            self.store
                .update(namespace, name, raced, current.resource_version)
                .await?;
            return Err(anyhow::Error::new(
                klights_pod_api::PodRepositoryError::conflict("injected status race"),
            ));
        }
        self.store
            .integration_update_status(namespace, name, status, expected_resource_version)
            .await
    }
}

struct IntegrationNoopPodMutationReconcile;

impl klights_reconcile_api::PodMutationReconcileSink for IntegrationNoopPodMutationReconcile {
    fn reconcile_pod_mutation(
        &self,
        _request: klights_reconcile_api::PodMutationReconcileRequest,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

async fn integration_status_race_service(
    pod_name: &str,
    pod: serde_json::Value,
    mode: IntegrationStatusRaceMode,
) -> (
    crate::kubelet::pod_repository::status::PodStatusService,
    Arc<IntegrationStatusRaceWriter>,
    crate::datastore::Resource,
) {
    let sqlite = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let db: crate::datastore::DatastoreHandle = Arc::new(sqlite);
    let store = Arc::new(crate::pod_repository_composition::new_pod_store(db));
    let created = store.create("default", pod_name, pod).await.unwrap();
    let writer = Arc::new(IntegrationStatusRaceWriter {
        store: store.clone(),
        attempts: std::sync::atomic::AtomicUsize::new(0),
        mode,
    });
    let service = crate::kubelet::pod_repository::status::PodStatusService::new(
        store,
        writer.clone(),
        Arc::new(IntegrationNoopPodMutationReconcile),
        None,
        None,
        crate::kubelet::context::HostIpState::default(),
        Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
    );
    (service, writer, created)
}

pub async fn run_scheduler_status_race(
    pod: serde_json::Value,
    update: crate::kubelet::pod_repository::PodStatusUpdate,
) -> IntegrationStatusRaceOutcome {
    let (service, writer, _) = integration_status_race_service(
        "scheduled-race",
        pod,
        IntegrationStatusRaceMode::Scheduler,
    )
    .await;
    let result = service
        .integration_set_pod_status("default", "scheduled-race", &update, None)
        .await;
    let conflict = result
        .as_ref()
        .err()
        .is_some_and(klights_cluster_datastore::errors::is_conflict_error);
    IntegrationStatusRaceOutcome {
        attempts: writer.attempts.load(std::sync::atomic::Ordering::SeqCst),
        conflict,
        resource: result.ok(),
    }
}

pub async fn run_probe_readiness_status_race(
    pod_name: &str,
    pod: serde_json::Value,
    conflicts: usize,
    pin_resource_version: bool,
) -> IntegrationStatusRaceOutcome {
    let (service, writer, created) = integration_status_race_service(
        pod_name,
        pod,
        IntegrationStatusRaceMode::Probe {
            conflicts_remaining: std::sync::atomic::AtomicUsize::new(conflicts),
        },
    )
    .await;
    let result = service
        .integration_set_probe_readiness(
            "default",
            pod_name,
            "c",
            true,
            pin_resource_version.then_some(created.resource_version),
        )
        .await;
    let conflict = result
        .as_ref()
        .err()
        .is_some_and(klights_cluster_datastore::errors::is_conflict_error);
    IntegrationStatusRaceOutcome {
        attempts: writer.attempts.load(std::sync::atomic::Ordering::SeqCst),
        conflict,
        resource: result.ok(),
    }
}

pub struct IntegrationPodWatchRunnerFixture {
    runner: crate::kubelet::pod_repository::background::PodWatchRunner,
}

pub struct IntegrationPodNetworkFixture {
    stores: Option<Arc<crate::datastore::node_local::NodeLocalStores>>,
    service: crate::kubelet::pod_repository::network::PodNetworkService,
}

impl IntegrationPodNetworkFixture {
    pub fn with_cache_and_waiter(
        cache: Arc<dyn klights_node_store::PodNetworkCache>,
        waiter: Arc<dyn klights_network_api::PodNetworkAssignmentWaiter>,
    ) -> Self {
        Self {
            stores: None,
            service: crate::kubelet::pod_repository::network::PodNetworkService::new(
                cache,
                Arc::new(klights_supervisor::TaskSupervisor::new(
                    klights_supervisor::TaskCategoryConfig::default(),
                )),
                waiter,
                crate::kubelet::context::HostIpState::default(),
            ),
        }
    }

    pub async fn node_local_with_waiter(
        waiter: Arc<dyn klights_network_api::PodNetworkAssignmentWaiter>,
    ) -> Self {
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let stores = Arc::new(
            crate::datastore::node_local::selector::open_node_local(
                crate::datastore::backend_kind::BackendKind::Sqlite,
                None,
                supervisor.clone(),
                None,
                "sqlite:pod-network-integration",
            )
            .await
            .expect("Pod network integration store"),
        );
        let service = crate::kubelet::pod_repository::network::PodNetworkService::new(
            stores.pod_network_cache(),
            supervisor,
            waiter,
            crate::kubelet::context::HostIpState::default(),
        );
        Self {
            stores: Some(stores),
            service,
        }
    }

    pub async fn reserve_assignment(
        &self,
        sandbox_id: &str,
        pod_name: &str,
        pod_uid: &str,
        veth_host: &str,
        netns_path: &str,
    ) -> anyhow::Result<()> {
        let stores = self.stores.as_ref().expect("node-local network fixture");
        stores
            .pod_ipam()
            .reserve_ip_and_insert_network(
                klights_node_store::PodNetworkAllocationRequest::try_new(
                    sandbox_id,
                    klights_types::PodIdentity::new("default", pod_name, pod_uid),
                    0x0a2a_0000,
                    256,
                    veth_host,
                    netns_path,
                )?,
            )
            .await
            .map(|_| ())
            .map_err(anyhow::Error::from)
    }

    pub async fn read_assignment(
        &self,
        sandbox_id: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        host_network: bool,
    ) -> anyhow::Result<crate::kubelet::pod_repository::PodNetworkAssignment> {
        self.service
            .read_pod_network_assignment(sandbox_id, namespace, pod_name, pod_uid, host_network)
            .await
    }
}

impl IntegrationPodWatchRunnerFixture {
    pub fn new() -> Self {
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        Self {
            runner: crate::kubelet::pod_repository::background::PodWatchRunner::new(supervisor),
        }
    }

    pub fn started(&self) -> bool {
        self.runner
            .started
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn start(&self) {
        self.runner.start();
    }
}

pub struct IntegrationDeadlineTimerRunnerFixture {
    runner: crate::kubelet::pod_repository::background::DeadlineTimerRunner,
}

impl IntegrationDeadlineTimerRunnerFixture {
    pub fn new() -> Self {
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        Self {
            runner: crate::kubelet::pod_repository::background::DeadlineTimerRunner::new(
                supervisor,
            ),
        }
    }

    pub fn schedule_uid_bound_wakeup(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        delay_ms: u64,
        reason: &'static str,
    ) {
        self.runner
            .schedule_uid_bound_wakeup(namespace, name, uid, delay_ms, reason);
    }
}

pub struct IntegrationPodStoreFixture {
    _sqlite: crate::datastore::sqlite::Datastore,
    store: Arc<crate::kubelet::pod_repository::store::PodStore>,
}

impl IntegrationPodStoreFixture {
    pub async fn new() -> Self {
        let sqlite = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .expect("Pod store integration fixture");
        let datastore: crate::datastore::DatastoreHandle = Arc::new(sqlite.clone());
        let store = Arc::new(crate::pod_repository_composition::new_pod_store(datastore));
        Self {
            _sqlite: sqlite,
            store,
        }
    }

    pub async fn seed_pod(
        &self,
        namespace: &str,
        name: &str,
        pod: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.store.create(namespace, name, pod).await
    }

    pub async fn read_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        self.store.get(namespace, name).await
    }

    pub async fn list_pods(
        &self,
        namespace: Option<&str>,
        label_selector: Option<&str>,
    ) -> anyhow::Result<crate::kubelet::pod_repository::PodResourceList> {
        self.store
            .list(namespace, label_selector, None, None, None)
            .await
    }

    pub async fn list_pods_by_owner_uid(
        &self,
        namespace: &str,
        owner_uid: &str,
    ) -> anyhow::Result<Vec<crate::datastore::Resource>> {
        self.store
            .integration_list_by_owner(namespace, owner_uid)
            .await
    }

    pub async fn mark_pod_deleting_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        deletion_body: &serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.store
            .integration_mark_deleting_latest(namespace, name, uid, deletion_body)
            .await
    }

    pub async fn update_pod(
        &self,
        namespace: &str,
        name: &str,
        pod: serde_json::Value,
        expected_resource_version: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.store
            .update(namespace, name, pod, expected_resource_version)
            .await
    }

    pub async fn update_pod_status(
        &self,
        namespace: &str,
        name: &str,
        status: serde_json::Value,
        expected_resource_version: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.store
            .integration_update_status(namespace, name, status, expected_resource_version)
            .await
    }

    pub async fn finalize_bound_pod_after_actor_cleanup(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<IntegrationBoundPodDeleteOutcome> {
        let finalization =
            crate::bootstrap::composition_adapters::bound_pod_finalization_adapter::new_for_root(
                self.store.clone(),
                None,
                None,
                Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
            );
        let outcome = finalization
            .finalize_bound_pod(klights_pod_api::BoundPodFinalizationRequest::try_new(
                klights_types::PodIdentity::new(namespace, name, uid),
            )?)
            .await
            .map_err(anyhow::Error::new)?;
        Ok(map_bound_delete_outcome(outcome))
    }

    pub async fn delete_unscheduled_pod_with_uid_and_observed_resource_version(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        observed_resource_version: i64,
    ) -> anyhow::Result<klights_pod_api::UnscheduledPodDeletionOutcome> {
        let deletion = crate::kubelet::pod_repository::workqueue::test_leader_unscheduled_deletion(
            self.store.clone(),
        );
        deletion
            .delete_unscheduled_pod(klights_pod_api::UnscheduledPodDeletionRequest::try_new(
                klights_types::PodIdentity::new(namespace, name, uid),
                observed_resource_version,
            )?)
            .await
            .map_err(anyhow::Error::new)
    }
}

fn map_bound_delete_outcome(
    outcome: klights_pod_api::BoundPodFinalizationOutcome,
) -> IntegrationBoundPodDeleteOutcome {
    match outcome {
        klights_pod_api::BoundPodFinalizationOutcome::Removed
        | klights_pod_api::BoundPodFinalizationOutcome::Accepted => {
            IntegrationBoundPodDeleteOutcome::Removed
        }
        klights_pod_api::BoundPodFinalizationOutcome::IdentityChanged => {
            IntegrationBoundPodDeleteOutcome::IdentityChanged
        }
        klights_pod_api::BoundPodFinalizationOutcome::FinalizersPending => {
            IntegrationBoundPodDeleteOutcome::FinalizersPending
        }
        klights_pod_api::BoundPodFinalizationOutcome::Retry => {
            IntegrationBoundPodDeleteOutcome::Retry
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntegrationPodDeleteCasRaceKind {
    SchedulerBind,
    StatusUpdate,
}

pub struct IntegrationUnscheduledPodDeleteCasRaceOutcome {
    pub disposition: klights_pod_api::UnscheduledPodDeletionOutcome,
    pub raced: bool,
    pub created_resource_version: i64,
    pub live: crate::datastore::Resource,
}

pub struct IntegrationBoundPodDeleteCasRaceOutcome {
    pub disposition: IntegrationBoundPodDeleteOutcome,
    pub raced: bool,
    pub created_resource_version: i64,
    pub live: crate::datastore::Resource,
}

struct IntegrationPodDeleteCasRacingProposal {
    inner: crate::datastore::DatastoreHandle,
    pod_name: String,
    race: IntegrationPodDeleteCasRaceKind,
    raced: Arc<std::sync::atomic::AtomicBool>,
}

impl IntegrationPodDeleteCasRacingProposal {
    fn targets_pod_delete(&self, command: &klights_cluster_core::StorageCommand) -> bool {
        matches!(
            command,
            klights_cluster_core::StorageCommand::DeleteResource {
                api_version,
                kind,
                namespace,
                name,
                ..
            } if api_version == "v1"
                && kind == "Pod"
                && namespace.as_deref() == Some("default")
                && name == &self.pod_name
        )
    }

    async fn inject_race(&self) -> anyhow::Result<()> {
        let current = self
            .inner
            .get_resource("v1", "Pod", Some("default"), &self.pod_name)
            .await?
            .expect("CAS race target Pod exists");
        match self.race {
            IntegrationPodDeleteCasRaceKind::SchedulerBind => {
                let mut body = (*current.data).clone();
                body["spec"]["nodeName"] = serde_json::json!("node-bound-by-scheduler");
                self.inner
                    .update_main_resource_with_preconditions(
                        "v1",
                        "Pod",
                        Some("default"),
                        &self.pod_name,
                        body,
                        crate::datastore::ResourcePreconditions {
                            uid: Some(current.uid),
                            resource_version: Some(current.resource_version),
                        },
                    )
                    .await?;
            }
            IntegrationPodDeleteCasRaceKind::StatusUpdate => {
                self.inner
                    .update_status_only_with_preconditions(
                        "v1",
                        "Pod",
                        Some("default"),
                        &self.pod_name,
                        serde_json::json!({
                            "phase": "Running",
                            "podIP": "10.42.0.77",
                            "raceBump": true
                        }),
                        crate::datastore::ResourcePreconditions::uid(current.uid),
                    )
                    .await?;
            }
        }
        self.raced.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    async fn apply(
        &self,
        command: klights_cluster_core::StorageCommand,
        idempotency_key: &str,
        operation: klights_kubelet::outbox::OutboxOperation,
        authoring_node: &str,
    ) -> Result<
        klights_replication::proposal::RaftProposalEffect,
        klights_cluster_core::OutboxApplyError,
    > {
        if self.targets_pod_delete(&command) {
            self.inject_race().await.map_err(|error| {
                klights_cluster_core::OutboxApplyError::Retryable(error.to_string())
            })?;
        }
        crate::bootstrap::outbox_apply_adapter::propose_outbox_command_on_backend(
            self.inner.as_ref(),
            idempotency_key,
            operation,
            command,
            authoring_node,
            None,
        )
        .await
    }
}

#[async_trait::async_trait]
impl klights_replication::proposal::RaftProposal for IntegrationPodDeleteCasRacingProposal {
    async fn propose_command(
        &self,
        command: klights_cluster_core::StorageCommand,
    ) -> anyhow::Result<klights_cluster_store::StorageCommandResult> {
        let effect = self
            .apply(
                command,
                &format!("delete-cas-race-{}", uuid::Uuid::new_v4()),
                klights_kubelet::outbox::OutboxOperation::PodStatus,
                "delete-cas-race-leader",
            )
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let (result, resource_effect, pod_endpoint_effect, committed_resource) =
            effect.into_parts();
        let applied_resource_version = match result {
            klights_cluster_core::OutboxApplyOutcome::Applied { applied_rv } => Some(applied_rv),
            klights_cluster_core::OutboxApplyOutcome::AlreadyApplied { applied_rv } => applied_rv,
        };
        Ok(klights_cluster_store::StorageCommandResult::new(
            applied_resource_version,
            None,
            None,
            resource_effect == klights_cluster_core::ResourceMutationEffect::Changed,
            committed_resource.map(klights_cluster_store::AppliedMutation::Resource),
            pod_endpoint_effect,
        ))
    }

    async fn propose_outbox_command(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: klights_cluster_core::StorageCommand,
        authoring_node: &str,
        _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> Result<klights_cluster_core::OutboxApplyOutcome, klights_cluster_core::OutboxApplyError>
    {
        let operation =
            klights_kubelet::outbox::OutboxOperation::try_from(operation).map_err(|error| {
                klights_cluster_core::OutboxApplyError::Retryable(error.to_string())
            })?;
        Ok(self
            .apply(command, idempotency_key, operation, authoring_node)
            .await?
            .into_parts()
            .0)
    }
}

async fn integration_pod_delete_cas_race_store(
    pod_name: &str,
    race: IntegrationPodDeleteCasRaceKind,
) -> (
    Arc<crate::kubelet::pod_repository::store::PodStore>,
    crate::datastore::DatastoreHandle,
    Arc<std::sync::atomic::AtomicBool>,
) {
    let sqlite = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .expect("delete CAS race datastore");
    let inner: crate::datastore::DatastoreHandle = Arc::new(sqlite);
    let raced = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let proposal = Arc::new(IntegrationPodDeleteCasRacingProposal {
        inner: inner.clone(),
        pod_name: pod_name.to_string(),
        race,
        raced: raced.clone(),
    });
    let datastore: crate::datastore::DatastoreHandle = Arc::new(
        crate::bootstrap::sequenced_datastore::SequencedDatastore::new_with_clock(
            inner,
            proposal,
            Arc::new(klights_supervisor::SystemWallClock),
        ),
    );
    (
        Arc::new(crate::pod_repository_composition::new_pod_store(
            datastore.clone(),
        )),
        datastore,
        raced,
    )
}

pub async fn run_unscheduled_pod_delete_cas_race(
    pod_name: &str,
    pod_uid: &str,
    race: IntegrationPodDeleteCasRaceKind,
) -> anyhow::Result<IntegrationUnscheduledPodDeleteCasRaceOutcome> {
    let (store, datastore, raced) = integration_pod_delete_cas_race_store(pod_name, race).await;
    let created = store
        .create(
            "default",
            pod_name,
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": pod_name,
                    "namespace": "default",
                    "uid": pod_uid,
                    "deletionTimestamp": "2026-01-01T00:00:00Z",
                    "deletionGracePeriodSeconds": 0
                },
                "spec": {"nodeName": "", "containers": [{"name": "app", "image": "nginx:latest"}]},
                "status": {"phase": "Running"}
            }),
        )
        .await?;
    let deletion =
        crate::kubelet::pod_repository::workqueue::test_leader_unscheduled_deletion(store);
    let disposition = deletion
        .delete_unscheduled_pod(klights_pod_api::UnscheduledPodDeletionRequest::try_new(
            klights_types::PodIdentity::new("default", pod_name, pod_uid),
            created.resource_version,
        )?)
        .await
        .map_err(anyhow::Error::new)?;
    let live = datastore
        .get_resource("v1", "Pod", Some("default"), pod_name)
        .await?
        .expect("Pod survives lost unscheduled delete CAS");
    Ok(IntegrationUnscheduledPodDeleteCasRaceOutcome {
        disposition,
        raced: raced.load(std::sync::atomic::Ordering::SeqCst),
        created_resource_version: created.resource_version,
        live,
    })
}

pub async fn run_bound_pod_delete_cas_race(
    pod_name: &str,
    pod_uid: &str,
) -> anyhow::Result<IntegrationBoundPodDeleteCasRaceOutcome> {
    let (store, datastore, raced) = integration_pod_delete_cas_race_store(
        pod_name,
        IntegrationPodDeleteCasRaceKind::StatusUpdate,
    )
    .await;
    let created = store
        .create(
            "default",
            pod_name,
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": pod_name,
                    "namespace": "default",
                    "uid": pod_uid,
                    "deletionTimestamp": "2026-01-01T00:00:00Z",
                    "deletionGracePeriodSeconds": 0
                },
                "spec": {"nodeName": "worker-a", "containers": [{"name": "app", "image": "nginx:latest"}]},
                "status": {"phase": "Running"}
            }),
        )
        .await?;
    let finalization =
        crate::bootstrap::composition_adapters::bound_pod_finalization_adapter::new_for_root(
            store,
            None,
            None,
            Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
        );
    let disposition = map_bound_delete_outcome(
        finalization
            .finalize_bound_pod(klights_pod_api::BoundPodFinalizationRequest::try_new(
                klights_types::PodIdentity::new("default", pod_name, pod_uid),
            )?)
            .await
            .map_err(anyhow::Error::new)?,
    );
    let live = datastore
        .get_resource("v1", "Pod", Some("default"), pod_name)
        .await?
        .expect("Pod survives lost actor finalization CAS");
    Ok(IntegrationBoundPodDeleteCasRaceOutcome {
        disposition,
        raced: raced.load(std::sync::atomic::Ordering::SeqCst),
        created_resource_version: created.resource_version,
        live,
    })
}

impl IntegrationPodRepositoryComposition {
    pub async fn new_inline() -> Self {
        Self::new_exact(
            None,
            false,
            false,
            false,
            crate::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
            None,
            None,
        )
        .await
    }

    pub async fn new_deferred_leader() -> Self {
        Self::new_exact(
            None,
            false,
            false,
            false,
            crate::pod_repository_composition::PodSchedulingMode::DeferredMultiNodeLeader,
            None,
            None,
        )
        .await
    }

    pub async fn new_deferred_leader_with_node_outbox() -> Self {
        Self::new_exact(
            None,
            false,
            true,
            false,
            crate::pod_repository_composition::PodSchedulingMode::DeferredMultiNodeLeader,
            None,
            None,
        )
        .await
    }

    pub async fn new_deferred_leader_with_bind_gate() -> (Self, IntegrationSchedulerBindGate) {
        let gate = Arc::new(
            crate::bootstrap::composition_adapters::pod_native_adapter::SchedulerBindGateForTest::new(),
        );
        let fixture = Self::new_exact(
            None,
            false,
            false,
            false,
            crate::pod_repository_composition::PodSchedulingMode::DeferredMultiNodeLeader,
            Some(gate.clone()),
            None,
        )
        .await;
        (fixture, IntegrationSchedulerBindGate { gate })
    }

    pub async fn new_cluster_backed(
        resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    ) -> Self {
        Self::new_exact(
            Some(resource_query),
            false,
            false,
            false,
            crate::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
            None,
            None,
        )
        .await
    }

    pub async fn new_with_node_outbox() -> Self {
        Self::new_exact(
            None,
            false,
            true,
            false,
            crate::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
            None,
            None,
        )
        .await
    }

    #[allow(dead_code)]
    pub async fn new_cluster_backed_with_node_outbox(
        resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    ) -> Self {
        Self::new_exact(
            Some(resource_query),
            false,
            true,
            false,
            crate::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
            None,
            None,
        )
        .await
    }

    pub async fn new_with_status_dispatcher() -> Self {
        Self::new_exact(
            None,
            true,
            false,
            false,
            crate::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
            None,
            None,
        )
        .await
    }

    pub async fn new_with_gc_workqueue() -> Self {
        Self::new_exact(
            None,
            false,
            false,
            true,
            crate::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
            None,
            None,
        )
        .await
    }

    pub async fn new_with_delete_side_effect_observation() -> Self {
        let observation = Arc::new(tokio::sync::Mutex::new(None));
        Self::new_exact(
            None,
            false,
            false,
            false,
            crate::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
            None,
            Some(observation),
        )
        .await
    }

    async fn new_exact(
        repository_cluster_api: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
        with_dispatcher: bool,
        with_outbox: bool,
        with_workqueue: bool,
        scheduling_mode: crate::pod_repository_composition::PodSchedulingMode,
        scheduler_bind_gate: Option<Arc<crate::bootstrap::composition_adapters::pod_native_adapter::SchedulerBindGateForTest>>,
        delete_observation: Option<Arc<tokio::sync::Mutex<Option<(bool, bool)>>>>,
    ) -> Self {
        Self::new_exact_on(
            None,
            repository_cluster_api,
            with_dispatcher,
            with_outbox,
            with_workqueue,
            scheduling_mode,
            scheduler_bind_gate,
            delete_observation,
        )
        .await
    }

    async fn new_exact_on(
        sqlite: Option<crate::datastore::sqlite::Datastore>,
        repository_cluster_api: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
        with_dispatcher: bool,
        with_outbox: bool,
        with_workqueue: bool,
        scheduling_mode: crate::pod_repository_composition::PodSchedulingMode,
        scheduler_bind_gate: Option<Arc<crate::bootstrap::composition_adapters::pod_native_adapter::SchedulerBindGateForTest>>,
        delete_observation: Option<Arc<tokio::sync::Mutex<Option<(bool, bool)>>>>,
    ) -> Self {
        let sqlite = match sqlite {
            Some(sqlite) => sqlite,
            None => crate::datastore::sqlite::Datastore::new_in_memory()
                .await
                .expect("Pod repository integration composition"),
        };
        let db: crate::datastore::DatastoreHandle = Arc::new(sqlite.clone());
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let local_client = Arc::new(
            crate::control_plane::client::local::LocalApiClient::new_with_node_lease_tracker_and_passive_reads(
                db.clone(),
                crate::datastore::selector::sqlite_passive_read_ports(&sqlite),
                "pod-repository-composition".to_string(),
                Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(
                    chrono::Utc::now(),
                )),
                crate::control_plane::client::local::always_leader_watch(),
                klights_supervisor::FileProcessExecutor::new(supervisor.clone()),
            ),
        );
        let local_query: Arc<dyn klights_leader_api::LeaderResourceQuery> = local_client.clone();
        let native_resource_query = repository_cluster_api.clone().unwrap_or(local_query);
        let metrics = klights_controllers::side_effects::SideEffectMetrics::new();
        let controller_dispatcher = with_dispatcher.then(|| {
            Arc::new(
                klights_controllers::ControllerDispatcher::with_task_supervisor(
                    Arc::new(klights_controllers::service::ServiceIpam::new(
                        "10.43.128.0/17",
                    )),
                    supervisor.clone(),
                ),
            )
        });
        let mut side_effect_registry = if with_dispatcher {
            crate::bootstrap::side_effects::default_registry(
                metrics.clone(),
                None,
                Some(supervisor.clone()),
                Some(db.clone()),
                crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
            )
        } else {
            klights_controllers::side_effects::SideEffectRegistry::new()
        };
        if let Some(observed) = &delete_observation {
            side_effect_registry.register(
                "v1",
                "Pod",
                Arc::new(IntegrationRecordingPodDeleteHook {
                    db: db.clone(),
                    observed: observed.clone(),
                }),
                klights_controllers::side_effects::ErrorPolicy::Fail,
            );
        }
        let side_effects = Arc::new(side_effect_registry);
        if let Some(dispatcher) = &controller_dispatcher {
            side_effects.set_controller_dispatcher(dispatcher.clone());
        }
        let node_local = if with_outbox || with_workqueue {
            Some(Arc::new(
                crate::datastore::node_local::selector::open_node_local(
                    crate::datastore::backend_kind::BackendKind::Sqlite,
                    None,
                    supervisor.clone(),
                    None,
                    "sqlite:pod-repository-outbox-integration",
                )
                .await
                .expect("Pod repository outbox node-local store"),
            ))
        } else {
            None
        };
        let outbox = with_outbox.then(|| {
            let stores = node_local.as_ref().expect("node outbox fixture");
            let ports = klights_kubelet::node_outbox::OutboxStores::new(
                stores.outbox_producer(),
                stores.outbox_dispatcher(),
                stores.pod_status_checkpoints(),
                stores.runtime_observation_checkpoints(),
                stores.outbox_status_stamps(),
            );
            Arc::new(klights_kubelet::node_outbox::Outbox::compose(
                ports,
                crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec(),
                Arc::new(tokio::sync::Notify::new()),
                Arc::new(klights_supervisor::SystemWallClock),
            ))
        });
        let parts = crate::pod_repository_composition::build_integration_pod_repository_parts(
            crate::pod_repository_composition::PodRepositoryBuildConfig {
                db: db.clone(),
                pod_workqueue_store: with_workqueue.then(|| node_local.as_ref().expect("GC workqueue fixture").pod_workqueue()),
                supervisor: supervisor.clone(),
                side_effects: side_effects.clone(),
                metrics,
                pod_network_cache: Arc::new(IntegrationEmptyPodNetworkCache),
                assignment_waiter: Arc::new(
                    klights_networking::PodNetworkAssignmentBus::new(),
                ),
                scheduling_mode,
                outbox,
                cluster_api: repository_cluster_api,
                controller_identity: crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
                #[cfg(not(test))]
                api_identity: Arc::new(
                    crate::bootstrap::controller_adapters::system_identity_adapter::SystemIdentityGenerator,
                ),
                #[cfg(not(test))]
                gc_coordination: Arc::new(klights_controllers::ControllerCoordination::new()),
                scheduler_bind_gate,
            },
            native_resource_query,
        );
        let repository_parts = parts.repository_parts;
        let repository = Arc::new(repository_parts.repository);
        if with_dispatcher {
            side_effects.set_pod_ports(repository.clone(), repository.clone());
        }
        Self {
            _sqlite: sqlite,
            db,
            repository,
            pod_api: parts.api,
            pod_subresource: parts.subresource,
            pod_scheduling: parts.scheduling,
            supervisor,
            background: repository_parts.background,
            controller_dispatcher,
            node_local,
            outbox_delivery: with_outbox.then_some(local_client),
            delete_observation,
        }
    }

    pub fn background_is_available(&self) -> bool {
        true
    }

    pub fn workqueue_start_called(&self) -> bool {
        self.background.workqueue_start_called()
    }

    pub async fn start_background(&self) -> anyhow::Result<()> {
        self.background.start().await
    }

    pub async fn pending_reconcile_keys(&self) -> Vec<klights_reconcile_api::ReconcileKey> {
        self.controller_dispatcher
            .as_ref()
            .expect("status dispatcher fixture")
            .pending_reconcile_keys()
            .await
    }

    pub async fn enqueue_reconcile_key(&self, key: klights_reconcile_api::ReconcileKey) {
        klights_reconcile_api::ControllerDispatcherPort::enqueue_reconcile(
            self.controller_dispatcher
                .as_ref()
                .expect("status dispatcher fixture")
                .as_ref(),
            key,
        )
        .await;
    }

    pub async fn claim_next_due_outbox(
        &self,
        now_ms: i64,
        lease_ms: i64,
        lease_token: &str,
    ) -> anyhow::Result<Option<IntegrationClaimedPodOutbox>> {
        claim_pod_outbox(
            self.node_local.as_ref().expect("node outbox fixture"),
            now_ms,
            lease_ms,
            lease_token,
        )
        .await
    }

    pub async fn drain_node_outbox_to_local_leader(&self) -> anyhow::Result<()> {
        let stores = self.node_local.as_ref().expect("node outbox fixture");
        let ports = klights_kubelet::node_outbox::OutboxStores::new(
            stores.outbox_producer(),
            stores.outbox_dispatcher(),
            stores.pod_status_checkpoints(),
            stores.runtime_observation_checkpoints(),
            stores.outbox_status_stamps(),
        );
        let dispatcher = klights_kubelet::node_outbox::OutboxDispatcher::new(
            ports,
            crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec(),
            self.outbox_delivery
                .as_ref()
                .expect("outbox delivery fixture")
                .clone(),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(klights_supervisor::SystemWallClock),
        );
        loop {
            if matches!(
                dispatcher.dispatch_due_once(i64::MAX / 4).await?,
                klights_kubelet::node_outbox::DispatchOutcome::Idle { .. }
            ) {
                return Ok(());
            }
        }
    }

    pub fn active_supervised_task_count(&self) -> usize {
        self.supervisor.active_tasks(None).len()
    }

    pub async fn request_gc_pod_delete(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<()> {
        klights_reconcile_api::GcPodDeleteSink::request_gc_pod_delete(
            self.repository.as_ref(),
            klights_reconcile_api::GcPodDeleteRequest::new(klights_types::PodIdentity::new(
                namespace, name, uid,
            )),
        )
        .await
        .map_err(anyhow::Error::new)
    }

    pub async fn run_delete_side_effect_order_case(&self) -> anyhow::Result<Option<(bool, bool)>> {
        let observed = self
            .delete_observation
            .as_ref()
            .expect("delete side-effect observation fixture");
        self.seed_pod(
            "default",
            "side-effect-pod",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "side-effect-pod",
                    "namespace": "default",
                    "uid": "uid-side-effect-pod",
                    "labels": {"app": "web"},
                    "ownerReferences": [{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "rs-x", "uid": "rs-x-uid", "controller": true}]
                },
                "spec": {"containers": [{"name": "c", "image": "nginx"}]},
                "status": {"phase": "Pending"}
            }),
        )
        .await?;
        crate::kubelet::pod_repository::PodObjectWriter::delete_pod(
            self.repository.as_ref(),
            "default",
            "side-effect-pod",
        )
        .await?;
        let value = *observed.lock().await;
        Ok(value)
    }

    pub async fn claim_uid_bound_pod_work(
        &self,
    ) -> anyhow::Result<Option<IntegrationPodWorkqueueEntry>> {
        let stores = self.node_local.as_ref().expect("GC workqueue fixture");
        let row = stores
            .pod_workqueue()
            .claim_due_work(klights_node_store::DueTimeMs::try_new(i64::MAX)?)
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        Ok(row.and_then(|row| {
            let klights_node_store::PodWorkIdentity::Pod(identity) = row.identity() else {
                return None;
            };
            let payload: serde_json::Value = serde_json::from_slice(row.payload()).ok()?;
            Some(IntegrationPodWorkqueueEntry {
                namespace: identity.namespace.clone(),
                name: identity.name.clone(),
                uid: identity.uid.clone(),
                target_node: payload
                    .get("target_node")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
            })
        }))
    }

    pub async fn run_gc_cascade(
        &self,
        owner_uid: &str,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        namespace: &str,
    ) -> anyhow::Result<()> {
        let coordination = klights_controllers::ControllerCoordination::new();
        klights_controllers::gc::cascade_delete_with_uid(
            self.db.as_ref(),
            owner_uid,
            owner_api_version,
            owner_name,
            owner_kind,
            Some(namespace.to_string()),
            self.repository.as_ref(),
            &crate::bootstrap::controller_adapters::gc_delete_adapter::GcNonPodFinalizationAdapter::new(self.db.clone()),
            &coordination,
        )
        .await
    }

    /// Exercises the committed outbox reducer with a fixed authenticated-node
    /// input. This is a reducer scenario, not a delivery-authentication fixture.
    pub async fn apply_uid_bound_worker_status_reducer_scenario(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        authenticated_node: &str,
        status: serde_json::Value,
    ) -> anyhow::Result<()> {
        let command = klights_cluster_core::StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some(namespace.to_string()),
            name: name.to_string(),
            status,
            expected_rv: None,
            preconditions: crate::datastore::ResourcePreconditions::uid(uid),
            observed_status_stamp: None,
        };
        let codec =
            crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec();
        let payload = codec.encode(&command)?;
        let command = codec.decode(payload.as_ref())?;
        let built = self
            .db
            .build_log_apply_commit_for_outbox(
                "integration-uid-bound-worker-status",
                "PodStatus",
                command,
                authenticated_node,
            )
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let klights_cluster_core::BuildOutboxOutcome::NeedsPropose { commit, .. } = built else {
            anyhow::bail!("expected fresh UID-bound worker status commit");
        };
        self.db.apply_log_apply_commit(commit).await?;
        Ok(())
    }

    pub async fn seed_pod(
        &self,
        namespace: &str,
        name: &str,
        pod: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.repository
            .integration_seed_pod(namespace, name, pod)
            .await
    }

    pub async fn seed_mutating_webhook_configuration(
        &self,
        name: &str,
        configuration: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.db
            .create_resource(
                "admissionregistration.k8s.io/v1",
                "MutatingWebhookConfiguration",
                None,
                name,
                configuration,
            )
            .await
    }

    pub async fn read_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        self.repository.integration_read_pod(namespace, name).await
    }

    pub async fn update_pod(
        &self,
        namespace: &str,
        name: &str,
        pod: serde_json::Value,
        expected_resource_version: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.repository
            .integration_update_pod(namespace, name, pod, expected_resource_version)
            .await
    }

    pub async fn update_pod_status(
        &self,
        namespace: &str,
        name: &str,
        status: serde_json::Value,
        expected_resource_version: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.repository
            .integration_update_pod_status(namespace, name, status, expected_resource_version)
            .await
    }

    pub async fn finalize_bound_pod_after_actor_cleanup(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<IntegrationBoundPodDeleteOutcome> {
        let outcome = self
            .repository
            .integration_finalize_bound_pod(namespace, name, uid)
            .await?;
        Ok(map_bound_delete_outcome(outcome))
    }

    pub async fn finalize_pod_deletion_after_actor_cleanup(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<IntegrationPodFinalizationOutcome> {
        integration_finalize_pod_after_actor_cleanup(self.repository.as_ref(), namespace, name, uid)
            .await
    }

    pub fn has_deferred_runtime_for_uid(&self, pod_uid: &str) -> bool {
        self.repository
            .integration_has_deferred_runtime_for_uid(pod_uid)
    }

    pub async fn api_create_pod(
        &self,
        request: crate::kubelet::pod_repository::PodApiCreateRequest,
    ) -> Result<
        crate::kubelet::pod_repository::PodApiCreateResult,
        klights_pod_api::PodRepositoryError,
    > {
        use klights_pod_api::PodApiMutation as _;
        let result = self
            .pod_api
            .create_pod(klights_pod_api::PodApiCreateRequest {
                namespace: request.namespace,
                body: request.body,
                dry_run: request.dry_run,
            })
            .await?;
        Ok(crate::kubelet::pod_repository::PodApiCreateResult {
            resource: result.resource,
            body: result.body,
        })
    }

    pub async fn api_update_pod(
        &self,
        namespace: &str,
        name: &str,
        body: serde_json::Value,
        current: crate::datastore::Resource,
        dry_run: bool,
    ) -> Result<
        crate::kubelet::pod_repository::PodApiUpdateOutcome,
        klights_pod_api::PodRepositoryError,
    > {
        use klights_pod_api::PodApiMutation as _;
        match self
            .pod_api
            .update_pod(klights_pod_api::PodApiUpdateRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                body,
                current,
                dry_run,
            })
            .await?
        {
            klights_pod_api::PodApiWriteOutcome::Persisted(resource) => {
                Ok(crate::kubelet::pod_repository::PodApiUpdateOutcome::Persisted(resource))
            }
            klights_pod_api::PodApiWriteOutcome::DryRun(value) => Ok(
                crate::kubelet::pod_repository::PodApiUpdateOutcome::DryRun(value),
            ),
        }
    }

    pub async fn api_patch_pod(
        &self,
        namespace: &str,
        name: &str,
        patch: serde_json::Value,
        patch_type: crate::kubelet::pod_repository::PodStatusPatchType,
        dry_run: bool,
    ) -> Result<
        crate::kubelet::pod_repository::PodApiUpdateOutcome,
        klights_pod_api::PodRepositoryError,
    > {
        use klights_pod_api::PodApiMutation as _;
        match self
            .pod_api
            .patch_pod(klights_pod_api::PodApiPatchRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                patch,
                patch_kind: integration_pod_patch_kind(patch_type),
                dry_run,
            })
            .await?
        {
            klights_pod_api::PodApiWriteOutcome::Persisted(resource) => {
                Ok(crate::kubelet::pod_repository::PodApiUpdateOutcome::Persisted(resource))
            }
            klights_pod_api::PodApiWriteOutcome::DryRun(value) => Ok(
                crate::kubelet::pod_repository::PodApiUpdateOutcome::DryRun(value),
            ),
        }
    }

    pub async fn api_delete_pod<O>(
        &self,
        namespace: &str,
        name: &str,
        options: O,
        dry_run: bool,
    ) -> Result<
        crate::kubelet::pod_repository::PodApiDeleteOutcome,
        klights_pod_api::PodRepositoryError,
    >
    where
        O: Into<klights_pod_api::PodDeleteOptions> + Send,
    {
        use klights_pod_api::PodApiMutation as _;
        match self
            .pod_api
            .delete_pod(klights_pod_api::PodApiDeleteRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                options: options.into(),
                dry_run,
            })
            .await?
        {
            klights_pod_api::PodApiDeleteOutcome::GracefulSet(resource) => {
                Ok(crate::kubelet::pod_repository::PodApiDeleteOutcome::GracefulSet(resource))
            }
            klights_pod_api::PodApiDeleteOutcome::DryRun(value) => Ok(
                crate::kubelet::pod_repository::PodApiDeleteOutcome::DryRun(value),
            ),
        }
    }

    pub async fn api_delete_collection_pods(
        &self,
        namespace: &str,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        dry_run: bool,
    ) -> Result<(), klights_pod_api::PodRepositoryError> {
        use klights_pod_api::PodApiMutation as _;
        self.pod_api
            .delete_collection_pods(klights_pod_api::PodApiDeleteCollectionRequest {
                namespace: namespace.to_string(),
                label_selector: label_selector.map(str::to_string),
                field_selector: field_selector.map(str::to_string),
                dry_run,
            })
            .await
    }

    pub async fn ordinary_mark_pod_terminating(
        &self,
        request: klights_pod_api::PodMarkTerminatingRequest,
    ) -> Result<crate::datastore::Resource, klights_pod_api::PodRepositoryError> {
        let target = request.into_target();
        let options = target
            .uid()
            .map(k8s_native_service::DeleteOptions::with_uid_precondition)
            .unwrap_or_default();
        match self
            .api_delete_pod(target.namespace(), target.name(), options, false)
            .await?
        {
            crate::kubelet::pod_repository::PodApiDeleteOutcome::GracefulSet(resource) => {
                Ok(resource)
            }
            crate::kubelet::pod_repository::PodApiDeleteOutcome::DryRun(_) => {
                unreachable!("ordinary mark is never dry-run")
            }
        }
    }

    pub async fn schedule_all_unbound_pods(
        &self,
    ) -> Result<(), klights_pod_api::PodRepositoryError> {
        self.pod_scheduling.schedule_all_unbound_pods().await
    }

    pub async fn replace_status_from_api(
        &self,
        namespace: &str,
        name: &str,
        status: serde_json::Value,
        expected_resource_version: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.pod_subresource
            .replace_status(klights_pod_api::PodStatusReplaceRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                expected_uid: None,
                status,
                expected_resource_version,
            })
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn replace_status_from_api_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        status: serde_json::Value,
        expected_resource_version: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.pod_subresource
            .replace_status(klights_pod_api::PodStatusReplaceRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                expected_uid: Some(uid.to_string()),
                status,
                expected_resource_version,
            })
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn patch_status_from_api(
        &self,
        namespace: &str,
        name: &str,
        patch: serde_json::Value,
        patch_type: crate::kubelet::pod_repository::PodStatusPatchType,
        expected_resource_version: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.pod_subresource
            .patch_status(klights_pod_api::PodStatusPatchRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                patch,
                patch_kind: integration_pod_patch_kind(patch_type),
                expected_resource_version: Some(expected_resource_version),
            })
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn update_ephemeral_containers(
        &self,
        namespace: &str,
        name: &str,
        containers: Vec<serde_json::Value>,
        expected_resource_version: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.pod_subresource
            .update_ephemeral_containers(klights_pod_api::PodEphemeralContainersRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                containers,
                expected_resource_version,
            })
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn seed_non_pod_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        anyhow::ensure!(kind != "Pod", "Pod fixtures must use seed_pod");
        self.db
            .create_resource(api_version, kind, Some(namespace), name, value)
            .await
    }

    pub async fn seed_scheduling_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        anyhow::ensure!(kind != "Pod", "Pod fixtures must use seed_pod");
        self.db
            .create_resource(api_version, kind, namespace, name, value)
            .await
    }

    pub async fn list_scheduling_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
    ) -> anyhow::Result<crate::datastore::ResourceList> {
        anyhow::ensure!(kind != "Pod", "Pod fixtures must use list_pods");
        self.db
            .list_resources(
                api_version,
                kind,
                namespace,
                crate::datastore::ResourceListQuery::all(),
            )
            .await
    }

    pub async fn pod_watch_events_since(
        &self,
        resource_version: i64,
    ) -> anyhow::Result<Vec<IntegrationPodWatchEvent>> {
        self.db
            .list_watch_events_since(
                &[crate::datastore::WatchTarget::namespaced("v1", "Pod")],
                resource_version,
            )
            .await
            .map(|events| {
                events
                    .into_iter()
                    .map(|event| IntegrationPodWatchEvent {
                        event_type: event.event_type.into_owned(),
                        resource: event.resource,
                    })
                    .collect()
            })
    }

    pub async fn read_non_pod_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        anyhow::ensure!(kind != "Pod", "Pod fixtures must use read_pod");
        self.db
            .get_resource(api_version, kind, Some(namespace), name)
            .await
    }

    pub async fn seed_namespace(
        &self,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.db.create_namespace(name, value).await
    }

    pub async fn read_namespace(
        &self,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        self.db.get_namespace(name).await
    }

    pub async fn update_namespace(
        &self,
        name: &str,
        value: serde_json::Value,
        expected_resource_version: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.db
            .update_namespace(name, value, expected_resource_version)
            .await
    }

    pub async fn reconcile_namespace_termination(
        &self,
        name: &str,
        now: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<()> {
        let store = crate::bootstrap::composition_adapters::api_state_adapter::RootNamespaceTerminationStore::new(
            self.db.clone(),
        );
        k8s_native_service::reconcile_namespace_termination_at(
            store.as_ref(),
            name,
            klights_controllers::side_effects::SideEffectMetrics::new().as_ref(),
            now,
        )
        .await
        .map_err(|error| anyhow::anyhow!("{error:?}"))
    }

    pub async fn reconcile_pod_disruption_budget(
        &self,
        pdb: &serde_json::Value,
        now: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<()> {
        klights_controllers::pdb::reconcile_pdb_at(self.db.as_ref(), self, pdb, now).await
    }
}

fn integration_pod_patch_kind(
    patch_type: crate::kubelet::pod_repository::PodStatusPatchType,
) -> klights_pod_api::PodStatusPatchKind {
    match patch_type {
        crate::kubelet::pod_repository::PodStatusPatchType::JsonPatch => {
            klights_pod_api::PodStatusPatchKind::JsonPatch
        }
        crate::kubelet::pod_repository::PodStatusPatchType::MergePatch => {
            klights_pod_api::PodStatusPatchKind::MergePatch
        }
        crate::kubelet::pod_repository::PodStatusPatchType::StrategicMerge => {
            klights_pod_api::PodStatusPatchKind::StrategicMerge
        }
        crate::kubelet::pod_repository::PodStatusPatchType::ApplyPatch => {
            klights_pod_api::PodStatusPatchKind::ApplyPatch
        }
    }
}

#[async_trait::async_trait]
impl crate::kubelet::pod_repository::PodReader for IntegrationPodRepositoryComposition {
    async fn get_pod(
        &self,
        ns: &str,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        crate::kubelet::pod_repository::PodReader::get_pod(self.repository.as_ref(), ns, name).await
    }

    async fn get_pod_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        crate::kubelet::pod_repository::PodReader::get_pod_for_uid(
            self.repository.as_ref(),
            ns,
            name,
            uid,
        )
        .await
    }

    async fn list_pods(
        &self,
        ns: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> anyhow::Result<crate::kubelet::pod_repository::PodResourceList> {
        crate::kubelet::pod_repository::PodReader::list_pods(
            self.repository.as_ref(),
            ns,
            label_selector,
            field_selector,
            limit,
            continue_token,
        )
        .await
    }

    async fn list_pods_by_owner_uid(
        &self,
        ns: &str,
        owner_uid: &str,
    ) -> anyhow::Result<Vec<crate::datastore::Resource>> {
        crate::kubelet::pod_repository::PodReader::list_pods_by_owner_uid(
            self.repository.as_ref(),
            ns,
            owner_uid,
        )
        .await
    }
}

impl klights_pod_api::PodQuery for IntegrationPodRepositoryComposition {
    fn get_pod(
        &self,
        request: klights_pod_api::PodGetRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Option<crate::datastore::Resource>> {
        klights_pod_api::PodQuery::get_pod(self.repository.as_ref(), request)
    }

    fn list_pods(
        &self,
        request: klights_pod_api::PodListRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodListResult> {
        klights_pod_api::PodQuery::list_pods(self.repository.as_ref(), request)
    }

    fn list_pods_by_owner_uid(
        &self,
        request: klights_pod_api::PodOwnerListRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Vec<crate::datastore::Resource>> {
        klights_pod_api::PodQuery::list_pods_by_owner_uid(self.repository.as_ref(), request)
    }
}

impl klights_pod_api::PodUpdate for IntegrationPodRepositoryComposition {
    fn update_pod(
        &self,
        request: klights_pod_api::PodUpdateRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, crate::datastore::Resource> {
        klights_pod_api::PodUpdate::update_pod(self.repository.as_ref(), request)
    }
}

#[async_trait::async_trait]
impl crate::kubelet::pod_repository::PodStatusWriter for IntegrationPodRepositoryComposition {
    async fn set_pod_status(
        &self,
        ns: &str,
        name: &str,
        update: crate::kubelet::pod_repository::PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::set_pod_status(
            self.repository.as_ref(),
            ns,
            name,
            update,
            expected_rv,
        )
        .await
    }
    async fn set_pod_status_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        update: crate::kubelet::pod_repository::PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::set_pod_status_for_uid(
            self.repository.as_ref(),
            ns,
            name,
            uid,
            update,
            expected_rv,
        )
        .await
    }
    async fn apply_runtime_reconcile_status(
        &self,
        ns: &str,
        name: &str,
        update: crate::kubelet::pod_repository::RuntimeReconcileStatus,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::apply_runtime_reconcile_status(
            self.repository.as_ref(),
            ns,
            name,
            update,
            expected_rv,
        )
        .await
    }
    async fn apply_runtime_reconcile_status_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        update: crate::kubelet::pod_repository::RuntimeReconcileStatus,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::apply_runtime_reconcile_status_for_uid(
            self.repository.as_ref(),
            ns,
            name,
            uid,
            update,
            expected_rv,
        )
        .await
    }
    async fn mark_start_pending_for_retry_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        error_message: &str,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::mark_start_pending_for_retry_for_uid(
            self.repository.as_ref(),
            ns,
            name,
            uid,
            error_message,
        )
        .await
    }
    async fn set_probe_readiness(
        &self,
        ns: &str,
        name: &str,
        container_name: &str,
        ready: bool,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::set_probe_readiness(
            self.repository.as_ref(),
            ns,
            name,
            container_name,
            ready,
            expected_rv,
        )
        .await
    }
    async fn set_probe_readiness_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        container_name: &str,
        ready: bool,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::set_probe_readiness_for_uid(
            self.repository.as_ref(),
            ns,
            name,
            uid,
            container_name,
            ready,
            expected_rv,
        )
        .await
    }
    async fn set_deadline_exceeded(
        &self,
        ns: &str,
        name: &str,
        message: String,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::set_deadline_exceeded(
            self.repository.as_ref(),
            ns,
            name,
            message,
            expected_rv,
        )
        .await
    }
    async fn set_deadline_exceeded_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        message: String,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::set_deadline_exceeded_for_uid(
            self.repository.as_ref(),
            ns,
            name,
            uid,
            message,
            expected_rv,
        )
        .await
    }
    async fn apply_ephemeral_container_statuses(
        &self,
        ns: &str,
        name: &str,
        statuses: Vec<serde_json::Value>,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::apply_ephemeral_container_statuses(
            self.repository.as_ref(),
            ns,
            name,
            statuses,
            expected_rv,
        )
        .await
    }
    async fn apply_ephemeral_container_statuses_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        statuses: Vec<serde_json::Value>,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::apply_ephemeral_container_statuses_for_uid(
            self.repository.as_ref(),
            ns,
            name,
            uid,
            statuses,
            expected_rv,
        )
        .await
    }
    async fn note_container_restart(
        &self,
        ns: &str,
        name: &str,
        container_name: &str,
        terminated: serde_json::Value,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        crate::kubelet::pod_repository::PodStatusWriter::note_container_restart(
            self.repository.as_ref(),
            ns,
            name,
            container_name,
            terminated,
            expected_rv,
        )
        .await
    }
    async fn note_container_restart_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        container_name: &str,
        terminated: serde_json::Value,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        crate::kubelet::pod_repository::PodStatusWriter::note_container_restart_for_uid(
            self.repository.as_ref(),
            ns,
            name,
            uid,
            container_name,
            terminated,
            expected_rv,
        )
        .await
    }
}

#[async_trait::async_trait]
impl crate::kubelet::pod_repository::PodMetadataWriter for IntegrationPodRepositoryComposition {
    async fn record_sandbox_id(
        &self,
        ns: &str,
        name: &str,
        sandbox_id: &str,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodMetadataWriter::record_sandbox_id(
            self.repository.as_ref(),
            ns,
            name,
            sandbox_id,
        )
        .await
    }
    async fn record_sandbox_id_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        sandbox_id: &str,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodMetadataWriter::record_sandbox_id_for_uid(
            self.repository.as_ref(),
            ns,
            name,
            uid,
            sandbox_id,
        )
        .await
    }
}

#[async_trait::async_trait]
impl crate::kubelet::pod_repository::PodObjectWriter for IntegrationPodRepositoryComposition {
    async fn create_controller_pod(
        &self,
        ns: &str,
        name: &str,
        _node_name: &str,
        pod: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.api_create_pod(crate::kubelet::pod_repository::PodApiCreateRequest {
            namespace: ns.to_string(),
            name: name.to_string(),
            body: pod,
            dry_run: false,
            run_admission: true,
        })
        .await
        .map_err(anyhow::Error::new)?
        .resource
        .ok_or_else(|| anyhow::anyhow!("controller pod {ns}/{name} create returned dry-run"))
    }
    async fn delete_pod(&self, ns: &str, name: &str) -> anyhow::Result<()> {
        crate::kubelet::pod_repository::PodObjectWriter::delete_pod(
            self.repository.as_ref(),
            ns,
            name,
        )
        .await
    }
    async fn update_pod_owner_references(
        &self,
        ns: &str,
        name: &str,
        owner_refs: Vec<serde_json::Value>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodObjectWriter::update_pod_owner_references(
            self.repository.as_ref(),
            ns,
            name,
            owner_refs,
        )
        .await
    }
    async fn update_pod_owner_references_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        owner_refs: Vec<serde_json::Value>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodObjectWriter::update_pod_owner_references_for_uid(
            self.repository.as_ref(),
            ns,
            name,
            uid,
            owner_refs,
        )
        .await
    }
    async fn merge_pod_labels(
        &self,
        ns: &str,
        name: &str,
        labels: Vec<(String, String)>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodObjectWriter::merge_pod_labels(
            self.repository.as_ref(),
            ns,
            name,
            labels,
        )
        .await
    }
    async fn merge_pod_labels_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        labels: Vec<(String, String)>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodObjectWriter::merge_pod_labels_for_uid(
            self.repository.as_ref(),
            ns,
            name,
            uid,
            labels,
        )
        .await
    }
}

#[async_trait::async_trait]
impl crate::kubelet::pod_repository::PodNetworkReader for IntegrationPodRepositoryComposition {
    async fn read_pod_network_assignment(
        &self,
        sandbox_id: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        host_network: bool,
    ) -> anyhow::Result<crate::kubelet::pod_repository::PodNetworkAssignment> {
        crate::kubelet::pod_repository::PodNetworkReader::read_pod_network_assignment(
            self.repository.as_ref(),
            sandbox_id,
            namespace,
            pod_name,
            pod_uid,
            host_network,
        )
        .await
    }
}

impl crate::kubelet::pod_repository::PodWatchSource for IntegrationPodRepositoryComposition {
    fn subscribe_pod_watch(&self) -> tokio::sync::broadcast::Receiver<klights_watch::WatchEvent> {
        crate::kubelet::pod_repository::PodWatchSource::subscribe_pod_watch(
            self.repository.as_ref(),
        )
    }
}

#[async_trait::async_trait]
impl crate::kubelet::pod_repository::PodSubresourceWriter for IntegrationPodRepositoryComposition {
    async fn replace_status_from_api(
        &self,
        ns: &str,
        name: &str,
        status: serde_json::Value,
        expected_rv: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        IntegrationPodRepositoryComposition::replace_status_from_api(
            self,
            ns,
            name,
            status,
            expected_rv,
        )
        .await
    }

    async fn replace_status_from_api_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        status: serde_json::Value,
        expected_rv: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        IntegrationPodRepositoryComposition::replace_status_from_api_for_uid(
            self,
            ns,
            name,
            uid,
            status,
            expected_rv,
        )
        .await
    }

    async fn patch_status_from_api(
        &self,
        ns: &str,
        name: &str,
        patch: serde_json::Value,
        patch_type: crate::kubelet::pod_repository::PodStatusPatchType,
        expected_rv: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        IntegrationPodRepositoryComposition::patch_status_from_api(
            self,
            ns,
            name,
            patch,
            patch_type,
            expected_rv,
        )
        .await
    }

    async fn update_ephemeral_containers(
        &self,
        ns: &str,
        name: &str,
        containers: Vec<serde_json::Value>,
        expected_rv: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        IntegrationPodRepositoryComposition::update_ephemeral_containers(
            self,
            ns,
            name,
            containers,
            expected_rv,
        )
        .await
    }

    async fn update_ephemeral_containers_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        containers: Vec<serde_json::Value>,
        expected_rv: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        let current = self
            .read_pod(ns, name)
            .await?
            .ok_or_else(|| klights_pod_api::PodRepositoryError::not_found(ns, name))?;
        crate::kubelet::pod_repository::ensure_pod_uid_matches(&current.data, uid, ns, name)?;
        IntegrationPodRepositoryComposition::update_ephemeral_containers(
            self,
            ns,
            name,
            containers,
            expected_rv,
        )
        .await
    }
}

impl klights_pod_api::PodSubresourceMutation for IntegrationPodRepositoryComposition {
    fn replace_status(
        &self,
        request: klights_pod_api::PodStatusReplaceRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, crate::datastore::Resource> {
        self.pod_subresource.replace_status(request)
    }

    fn patch_status(
        &self,
        request: klights_pod_api::PodStatusPatchRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, crate::datastore::Resource> {
        self.pod_subresource.patch_status(request)
    }

    fn update_ephemeral_containers(
        &self,
        request: klights_pod_api::PodEphemeralContainersRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, crate::datastore::Resource> {
        self.pod_subresource.update_ephemeral_containers(request)
    }
}

#[async_trait::async_trait]
impl klights_controllers::pdb::PdbPodReader for IntegrationPodRepositoryComposition {
    async fn list_namespace_pods(
        &self,
        namespace: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<Vec<crate::datastore::Resource>> {
        crate::kubelet::pod_repository::PodReader::list_pods(
            self,
            Some(namespace),
            None,
            None,
            None,
            None,
        )
        .await
        .map(|list| list.items)
        .map_err(|error| klights_reconcile_api::ControllerStoreError::internal(error.to_string()))
    }
}
