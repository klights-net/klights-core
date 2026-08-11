//! Focused Pod-repository composition fixtures owned by the base integration package.

use std::sync::Arc;

use klights_cluster_datastore::sqlite::embedded::ResourceMutationPauseOperation as IntegrationResourceMutationPauseOperation;
use klights_pod_api::PodSubresourceMutation as _;

fn integration_owner_references(
    values: Vec<serde_json::Value>,
) -> Result<Vec<klights_pod_api::PodOwnerReference>, klights_pod_api::PodRepositoryError> {
    values
        .into_iter()
        .map(|value| {
            let required = |field: &'static str| {
                value
                    .get(field)
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| {
                        klights_pod_api::PodRepositoryError::invalid_request(
                            "owner_reference",
                            format!("missing {field}"),
                        )
                    })
            };
            klights_pod_api::PodOwnerReference::try_new(
                required("apiVersion")?,
                required("kind")?,
                required("name")?,
                required("uid")?,
                value.get("controller").and_then(serde_json::Value::as_bool),
                value
                    .get("blockOwnerDeletion")
                    .and_then(serde_json::Value::as_bool),
            )
        })
        .collect()
}

#[derive(Default)]
struct PodRepositoryRecordingReconcileSink {
    keys: tokio::sync::Mutex<Vec<klights_reconcile_api::ReconcileKey>>,
}

impl PodRepositoryRecordingReconcileSink {
    async fn record(&self, keys: impl IntoIterator<Item = klights_reconcile_api::ReconcileKey>) {
        let mut recorded = self.keys.lock().await;
        for key in keys {
            if !recorded.contains(&key) {
                recorded.push(key);
            }
        }
    }

    async fn enqueue_key(&self, key: klights_reconcile_api::ReconcileKey) {
        self.record([key]).await;
    }

    async fn pending_keys(&self) -> Vec<klights_reconcile_api::ReconcileKey> {
        self.keys.lock().await.clone()
    }
}

impl klights_reconcile_api::ControllerReconcileSink for PodRepositoryRecordingReconcileSink {
    fn enqueue_reconcile_batch(
        &self,
        keys: Vec<klights_reconcile_api::ReconcileKey>,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        Box::pin(async move {
            if keys
                .iter()
                .any(|key| key.api_version() == "v1" && key.kind() == "Service")
            {
                return Err(klights_reconcile_api::ReconcileSinkError::unsupported_key(
                    "Service reconcile keys must use ServiceReconcileSink",
                ));
            }
            self.record(keys).await;
            Ok(())
        })
    }
}

impl klights_reconcile_api::ServiceReconcileSink for PodRepositoryRecordingReconcileSink {
    fn enqueue_service_reconcile_batch(
        &self,
        keys: Vec<klights_reconcile_api::ServiceReconcileKey>,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        Box::pin(async move {
            self.record(
                keys.into_iter()
                    .map(klights_reconcile_api::ServiceReconcileKey::into_reconcile_key),
            )
            .await;
            Ok(())
        })
    }
}

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

/// Focused worker-status/outbox fixture.  It owns only worker-safe ports and
/// node-local state; cluster API capabilities stay outside this type.
pub struct IntegrationPodWorkerFixture {
    pod_query: Arc<dyn klights_pod_api::PodQuery>,
    pod_update: Arc<dyn klights_pod_api::PodUpdate>,
    pod_status_writer: Arc<dyn klights_kubelet::pod_repository::status::PodStatusWriter>,
    deletion_finalizer: Arc<dyn klights_kubelet::pod_deletion_finalizer::PodDeletionFinalizer>,
    node_local: Arc<crate::bootstrap::node_store::NodeLocalStores>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntegrationPodFinalizationOutcome {
    DeletedOrAlreadyGone,
    Queued,
    FinalizersPending,
}

/// Finalizes bound-pod cleanup through the focused deletion-finalizer port
/// only — callers pass the port obtained from their own concrete backing
/// repository rather than the repository itself.
async fn integration_finalize_pod_after_actor_cleanup(
    finalizer: &dyn klights_kubelet::pod_deletion_finalizer::PodDeletionFinalizer,
    namespace: &str,
    name: &str,
    uid: &str,
) -> anyhow::Result<IntegrationPodFinalizationOutcome> {
    let key = klights_kubelet::runtime_types::PodRuntimeKey::new(namespace, name, uid);
    Ok(match finalizer.finalize_after_actor_cleanup(&key).await? {
        klights_kubelet::runtime_types::PodDeletionFinalizeResult::DeletedOrAlreadyGone => {
            IntegrationPodFinalizationOutcome::DeletedOrAlreadyGone
        }
        klights_kubelet::runtime_types::PodDeletionFinalizeResult::Queued => {
            IntegrationPodFinalizationOutcome::Queued
        }
        klights_kubelet::runtime_types::PodDeletionFinalizeResult::FinalizersPending => {
            IntegrationPodFinalizationOutcome::FinalizersPending
        }
    })
}

impl IntegrationPodWorkerFixture {
    pub async fn new(resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>) -> Self {
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let node_local = Arc::new(
            crate::bootstrap::node_store::open_node_local(
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
        let (
            pod_query,
            _pod_snapshot,
            pod_update,
            pod_status_writer,
            _pod_workqueue,
            _pod_network_assignment,
            _pod_host_ip,
            _background,
            deletion_finalizer,
            _dirty_counter,
            _mutation_reconcile,
            _gc_delete,
            _eviction_admission,
            _namespace_bootstrap,
            _namespace_termination_queue,
            _pod_api,
            _pod_subresource,
            _pod_scheduling,
            _watch_source,
            _bound_finalization,
            _deferred_runtime,
            _test_api,
            _test_subresource,
        ) = crate::bootstrap::pod_repository_composition::build_worker_pod_repository_parts(
            crate::bootstrap::pod_repository_composition::WorkerPodRepositoryBuildConfig {
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
            pod_query,
            pod_update,
            pod_status_writer,
            deletion_finalizer,
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
        integration_finalize_pod_after_actor_cleanup(
            self.deletion_finalizer.as_ref(),
            namespace,
            name,
            uid,
        )
        .await
    }

    pub async fn get_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        self.pod_query
            .get_pod(klights_pod_api::PodGetRequest::try_by_name(
                namespace, name,
            )?)
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn get_pod_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        self.pod_query
            .get_pod(klights_pod_api::PodGetRequest::try_by_identity(
                klights_types::PodIdentity::new(namespace, name, uid),
            )?)
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn set_pod_status_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        update: klights_kubelet::pod_repository::PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        klights_kubelet::pod_repository::PodStatusWriter::set_pod_status_for_uid(
            self.pod_status_writer.as_ref(),
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
        update: klights_kubelet::pod_repository::RuntimeReconcileStatus,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        klights_kubelet::pod_repository::PodStatusWriter::apply_runtime_reconcile_status_for_uid(
            self.pod_status_writer.as_ref(),
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
        self.pod_update
            .update_pod(klights_pod_api::PodUpdateRequest::try_record_sandbox_id(
                klights_pod_api::PodMutationTarget::try_by_identity(
                    klights_types::PodIdentity::new(namespace, name, uid),
                )?,
                sandbox_id,
            )?)
            .await
            .map_err(anyhow::Error::new)
    }

    #[allow(dead_code)]
    pub async fn update_pod_owner_references_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        owner_references: Vec<serde_json::Value>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.pod_update
            .update_pod(klights_pod_api::PodUpdateRequest::replace_owner_references(
                klights_pod_api::PodMutationTarget::try_by_identity(
                    klights_types::PodIdentity::new(namespace, name, uid),
                )?,
                integration_owner_references(owner_references)?,
            ))
            .await
            .map_err(anyhow::Error::new)
    }

    #[allow(dead_code)]
    pub async fn merge_pod_labels_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        labels: Vec<(String, String)>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.pod_update
            .update_pod(klights_pod_api::PodUpdateRequest::merge_labels(
                klights_pod_api::PodMutationTarget::try_by_identity(
                    klights_types::PodIdentity::new(namespace, name, uid),
                )?,
                labels
                    .into_iter()
                    .map(|(key, value)| klights_pod_api::PodLabel::try_new(key, value))
                    .collect::<Result<Vec<_>, _>>()?,
            ))
            .await
            .map_err(anyhow::Error::new)
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
    let cluster_api = crate::bootstrap::composition_adapters::resource_query_adapter::
        DatastoreResourceQueryAdapter::new(
            db.clone(),
            crate::bootstrap::composition_adapters::authority_adapter::always_leader_watch(),
        );
    let repository = IntegrationPodWorkerFixture::new(cluster_api).await;
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
    use klights_replication::proposal::RaftProposal as _;
    let proposal = crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(db.clone());
    let applied = proposal
        .propose_outbox_command_effect(
            row.idempotency_key(),
            klights_kubelet::outbox::OutboxOperation::PodMetadata.as_str(),
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
    let cluster_api = crate::bootstrap::composition_adapters::resource_query_adapter::
        DatastoreResourceQueryAdapter::new(
            db.clone(),
            crate::bootstrap::composition_adapters::authority_adapter::always_leader_watch(),
        );
    let authority = crate::bootstrap::authority::AuthorityHandle::from(
        crate::bootstrap::composition_adapters::authority_adapter::always_leader_watch(),
    );
    let delivery = crate::bootstrap::composition_adapters::
        committed_outbox_delivery_adapter::test_outbox_delivery(
            db.clone(),
            &authority,
            Arc::new(
                crate::bootstrap::composition_adapters::
                    committed_outbox_delivery_adapter::RootOutboxSideEffectState::new(
                    db.clone(),
                ),
            ),
            "worker-1".to_string(),
        );
    let repository = IntegrationPodWorkerFixture::new(cluster_api.clone()).await;
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
    let dispatched = repository.dispatch_due_once(delivery.clone()).await?
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
    let _ = repository.dispatch_due_once(delivery).await?;
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

/// Focused query capability used by the integration harness.
pub struct IntegrationPodQueryPorts {
    query: Arc<dyn klights_pod_api::PodQuery>,
    snapshot: Arc<dyn klights_pod_api::PodSnapshotQuery>,
}

impl IntegrationPodQueryPorts {
    pub async fn get_pod(
        &self,
        request: klights_pod_api::PodGetRequest,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        self.query
            .get_pod(request)
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn list_pods(
        &self,
        request: klights_pod_api::PodListRequest,
    ) -> anyhow::Result<klights_pod_api::PodListResult> {
        self.query
            .list_pods(request)
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn list_pods_by_owner_uid(
        &self,
        request: klights_pod_api::PodOwnerListRequest,
    ) -> anyhow::Result<Vec<crate::datastore::Resource>> {
        self.query
            .list_pods_by_owner_uid(request)
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn snapshot_pods(
        &self,
        request: klights_pod_api::PodSnapshotListRequest,
    ) -> anyhow::Result<klights_pod_api::PodSnapshotListOutcome> {
        self.snapshot
            .snapshot_pods(request)
            .await
            .map_err(anyhow::Error::new)
    }
}

/// Focused metadata mutation capability used by integration tests.
pub struct IntegrationPodUpdatePorts {
    update: Arc<dyn klights_pod_api::PodUpdate>,
}

impl IntegrationPodUpdatePorts {
    pub async fn update_pod(
        &self,
        request: klights_pod_api::PodUpdateRequest,
    ) -> Result<crate::datastore::Resource, klights_pod_api::PodRepositoryError> {
        self.update.update_pod(request).await
    }
}

/// Focused status capability used by integration tests.
pub struct IntegrationPodStatusPorts {
    writer: Arc<dyn klights_kubelet::pod_repository::status::PodStatusWriter>,
}

impl IntegrationPodStatusPorts {
    pub async fn set_pod_status(
        &self,
        namespace: &str,
        name: &str,
        update: klights_kubelet::pod_repository::PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.writer
            .set_pod_status(namespace, name, update, expected_rv)
            .await
    }

    pub async fn set_pod_status_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        update: klights_kubelet::pod_repository::PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.writer
            .set_pod_status_for_uid(namespace, name, uid, update, expected_rv)
            .await
    }

    pub async fn apply_runtime_reconcile_status(
        &self,
        namespace: &str,
        name: &str,
        update: klights_kubelet::pod_repository::RuntimeReconcileStatus,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.writer
            .apply_runtime_reconcile_status(namespace, name, update, expected_rv)
            .await
    }

    pub async fn apply_runtime_reconcile_status_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        update: klights_kubelet::pod_repository::RuntimeReconcileStatus,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.writer
            .apply_runtime_reconcile_status_for_uid(namespace, name, uid, update, expected_rv)
            .await
    }

    pub async fn mark_start_pending_for_retry_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        error_message: &str,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.writer
            .mark_start_pending_for_retry_for_uid(namespace, name, uid, error_message)
            .await
    }

    pub async fn set_probe_readiness(
        &self,
        namespace: &str,
        name: &str,
        container_name: &str,
        ready: bool,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.writer
            .set_probe_readiness(namespace, name, container_name, ready, expected_rv)
            .await
    }

    pub async fn set_probe_readiness_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        container_name: &str,
        ready: bool,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.writer
            .set_probe_readiness_for_uid(namespace, name, uid, container_name, ready, expected_rv)
            .await
    }

    pub async fn set_deadline_exceeded(
        &self,
        namespace: &str,
        name: &str,
        message: String,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.writer
            .set_deadline_exceeded(namespace, name, message, expected_rv)
            .await
    }

    pub async fn set_deadline_exceeded_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        message: String,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.writer
            .set_deadline_exceeded_for_uid(namespace, name, uid, message, expected_rv)
            .await
    }

    pub async fn apply_ephemeral_container_statuses_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        statuses: Vec<serde_json::Value>,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.writer
            .apply_ephemeral_container_statuses_for_uid(namespace, name, uid, statuses, expected_rv)
            .await
    }
}

/// Focused node-local network-assignment capability used by tests.
pub struct IntegrationPodNetworkPorts {
    assignment: Arc<dyn klights_kubelet::pod_repository::PodNetworkAssignmentQuery>,
}

impl IntegrationPodNetworkPorts {
    pub async fn read(
        &self,
        request: klights_kubelet::pod_repository::PodNetworkAssignmentRequest,
    ) -> anyhow::Result<klights_kubelet::pod_repository::PodNetworkAssignment> {
        self.assignment
            .read_pod_network_assignment(request)
            .await
            .map_err(anyhow::Error::new)
    }
}

/// Focused API and subresource capabilities.  These keep API-facing tests
/// from depending on any repository-wide trait implementation.
pub struct IntegrationPodApiPorts {
    api: Arc<k8s_native_service::PodApiService>,
    subresource: Arc<k8s_native_service::PodSubresourceService>,
}

impl IntegrationPodApiPorts {
    pub async fn create(
        &self,
        request: klights_pod_api::PodApiCreateRequest,
    ) -> Result<klights_pod_api::PodApiCreateResult, klights_pod_api::PodRepositoryError> {
        use klights_pod_api::PodApiMutation as _;
        self.api.create_pod(request).await
    }

    pub async fn update_pod(
        &self,
        namespace: &str,
        name: &str,
        body: serde_json::Value,
        current: crate::datastore::Resource,
        dry_run: bool,
    ) -> Result<klights_pod_api::PodApiWriteOutcome, klights_pod_api::PodRepositoryError> {
        self.update(klights_pod_api::PodApiUpdateRequest {
            namespace: namespace.to_string(),
            name: name.to_string(),
            body,
            current,
            dry_run,
        })
        .await
    }

    pub async fn patch_pod(
        &self,
        namespace: &str,
        name: &str,
        patch: serde_json::Value,
        patch_type: klights_pod_api::PodStatusPatchKind,
        dry_run: bool,
    ) -> Result<klights_pod_api::PodApiWriteOutcome, klights_pod_api::PodRepositoryError> {
        self.patch(klights_pod_api::PodApiPatchRequest {
            namespace: namespace.to_string(),
            name: name.to_string(),
            patch,
            patch_kind: patch_type,
            dry_run,
        })
        .await
    }

    pub async fn update(
        &self,
        request: klights_pod_api::PodApiUpdateRequest,
    ) -> Result<klights_pod_api::PodApiWriteOutcome, klights_pod_api::PodRepositoryError> {
        use klights_pod_api::PodApiMutation as _;
        self.api.update_pod(request).await
    }

    pub async fn patch(
        &self,
        request: klights_pod_api::PodApiPatchRequest,
    ) -> Result<klights_pod_api::PodApiWriteOutcome, klights_pod_api::PodRepositoryError> {
        use klights_pod_api::PodApiMutation as _;
        self.api.patch_pod(request).await
    }

    pub async fn delete(
        &self,
        request: klights_pod_api::PodApiDeleteRequest,
    ) -> Result<klights_pod_api::PodApiDeleteOutcome, klights_pod_api::PodRepositoryError> {
        use klights_pod_api::PodApiMutation as _;
        self.api.delete_pod(request).await
    }

    pub async fn delete_pod<O>(
        &self,
        namespace: &str,
        name: &str,
        options: O,
        dry_run: bool,
    ) -> Result<klights_pod_api::PodApiDeleteOutcome, klights_pod_api::PodRepositoryError>
    where
        O: Into<klights_pod_api::PodDeleteOptions> + Send,
    {
        self.delete(klights_pod_api::PodApiDeleteRequest {
            namespace: namespace.to_string(),
            name: name.to_string(),
            options: options.into(),
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
            .delete_pod(target.namespace(), target.name(), options, false)
            .await?
        {
            klights_pod_api::PodApiDeleteOutcome::GracefulSet(resource) => Ok(resource),
            klights_pod_api::PodApiDeleteOutcome::DryRun(_) => {
                unreachable!("ordinary mark is never dry-run")
            }
        }
    }

    pub async fn delete_collection(
        &self,
        request: klights_pod_api::PodApiDeleteCollectionRequest,
    ) -> Result<(), klights_pod_api::PodRepositoryError> {
        use klights_pod_api::PodApiMutation as _;
        self.api.delete_collection_pods(request).await
    }

    pub async fn delete_collection_pods(
        &self,
        namespace: &str,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        dry_run: bool,
    ) -> Result<(), klights_pod_api::PodRepositoryError> {
        self.delete_collection(klights_pod_api::PodApiDeleteCollectionRequest {
            namespace: namespace.to_string(),
            label_selector: label_selector.map(str::to_string),
            field_selector: field_selector.map(str::to_string),
            dry_run,
        })
        .await
    }

    pub async fn replace_status(
        &self,
        request: klights_pod_api::PodStatusReplaceRequest,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.subresource
            .replace_status(request)
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn replace_status_from_api(
        &self,
        namespace: &str,
        name: &str,
        status: serde_json::Value,
        expected_resource_version: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.replace_status(klights_pod_api::PodStatusReplaceRequest {
            namespace: namespace.to_string(),
            name: name.to_string(),
            expected_uid: None,
            status,
            expected_resource_version,
        })
        .await
    }

    pub async fn replace_status_from_api_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        status: serde_json::Value,
        expected_resource_version: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.replace_status(klights_pod_api::PodStatusReplaceRequest {
            namespace: namespace.to_string(),
            name: name.to_string(),
            expected_uid: Some(uid.to_string()),
            status,
            expected_resource_version,
        })
        .await
    }

    pub async fn patch_status(
        &self,
        request: klights_pod_api::PodStatusPatchRequest,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.subresource
            .patch_status(request)
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn patch_status_from_api(
        &self,
        namespace: &str,
        name: &str,
        patch: serde_json::Value,
        patch_type: klights_pod_api::PodStatusPatchKind,
        expected_resource_version: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.patch_status(klights_pod_api::PodStatusPatchRequest {
            namespace: namespace.to_string(),
            name: name.to_string(),
            patch,
            patch_kind: patch_type,
            expected_resource_version: Some(expected_resource_version),
        })
        .await
    }

    pub async fn update_ephemeral_containers(
        &self,
        request: klights_pod_api::PodEphemeralContainersRequest,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.subresource
            .update_ephemeral_containers(request)
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn update_ephemeral_containers_for_pod(
        &self,
        namespace: &str,
        name: &str,
        containers: Vec<serde_json::Value>,
        expected_resource_version: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.update_ephemeral_containers(klights_pod_api::PodEphemeralContainersRequest {
            namespace: namespace.to_string(),
            name: name.to_string(),
            containers,
            expected_resource_version,
        })
        .await
    }
}

/// Private lifetime owner for one integration scenario.
///
/// Public tests receive a suite-specific handle below.  Keeping this owner
/// private means the wiring can never become a reusable all-capability test
/// facade or leak a datastore/PodStore through a constructor boundary.
struct PodRepositoryScenarioOwner {
    _sqlite: crate::datastore::sqlite::Datastore,
    db: crate::datastore::DatastoreHandle,
    query_ports: IntegrationPodQueryPorts,
    update_ports: IntegrationPodUpdatePorts,
    status_ports: IntegrationPodStatusPorts,
    network_ports: IntegrationPodNetworkPorts,
    api_ports: IntegrationPodApiPorts,
    scheduling: Arc<dyn klights_pod_api::PodScheduling>,
    gc_delete: Arc<dyn klights_reconcile_api::GcPodDeleteSink>,
    deletion_finalizer: Arc<dyn klights_kubelet::pod_deletion_finalizer::PodDeletionFinalizer>,
    bound_pod_finalization: Arc<dyn klights_pod_api::BoundPodFinalization>,
    deferred_runtime: klights_kubelet::pod_repository::status::DeferredRuntimeReducerHandle,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
    background: klights_kubelet::pod_repository::background::PodRepositoryBackground,
    watch_source: Arc<dyn crate::bootstrap::pod_repository_composition::PodWatchSource>,
    controller_dispatcher: Option<Arc<PodRepositoryRecordingReconcileSink>>,
    node_local: Option<Arc<crate::bootstrap::node_store::NodeLocalStores>>,
    outbox_delivery: Option<Arc<dyn klights_leader_api::LeaderOutboxDelivery>>,
    delete_observation: Option<Arc<tokio::sync::Mutex<Option<(bool, bool)>>>>,
    post_write_maintenance_notify: Arc<tokio::sync::Notify>,
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

pub struct IntegrationSameNameStatusRaceOutcome {
    pub old_uid: String,
    pub replacement: crate::datastore::Resource,
    pub persisted_after: crate::datastore::Resource,
    pub persistence_attempts: usize,
    pub reconcile_effects: usize,
    pub outbox_enqueues: usize,
    pub conflict: bool,
}

pub struct IntegrationApiDeleteStatusRaceOutcome {
    pub created: crate::datastore::Resource,
    pub deleted: crate::datastore::Resource,
    pub persisted: crate::datastore::Resource,
    pub status_bumps: usize,
}

pub async fn run_raft_delete_mark_status_race(
    pod_name: &str,
    grace_period_seconds: Option<i64>,
) -> anyhow::Result<IntegrationApiDeleteStatusRaceOutcome> {
    run_api_delete_status_race(pod_name, grace_period_seconds).await
}

#[allow(dead_code)]
pub async fn run_api_delete_status_race(
    pod_name: &str,
    grace_period_seconds: Option<i64>,
) -> anyhow::Result<IntegrationApiDeleteStatusRaceOutcome> {
    let repo = PodRepositoryScenarioOwner::new_inline().await;
    let created = repo
        .api_ports()
        .create(klights_pod_api::PodApiCreateRequest {
            namespace: "default".to_string(),
            body: serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": pod_name},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]}
            }),
            dry_run: false,
        })
        .await
        .map_err(anyhow::Error::new)?
        .resource
        .expect("delete race Pod create persists");
    let pause = repo._sqlite.install_resource_mutation_pause(
        IntegrationResourceMutationPauseOperation::PatchLatest,
        "v1",
        "Pod",
        Some("default"),
        pod_name,
    );
    let delete = repo.api_ports().delete_pod(
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
            .query_ports()
            .get_pod(klights_pod_api::PodGetRequest::try_by_name(
                "default", pod_name,
            )?)
            .await?
            .expect("delete race Pod exists before mark");
        let updated = repo
            .api_ports()
            .replace_status_from_api(
                "default",
                pod_name,
                serde_json::json!({"phase": "Running", "raceBump": 1}),
                current.resource_version,
            )
            .await;
        pause.resume();
        updated
    };
    let (deleted, raced) = tokio::join!(delete, race);
    raced?;
    let deleted = match deleted.map_err(anyhow::Error::new)? {
        klights_pod_api::PodApiDeleteOutcome::GracefulSet(resource) => resource,
        klights_pod_api::PodApiDeleteOutcome::DryRun(_) => {
            anyhow::bail!("delete race unexpectedly dry-ran")
        }
    };
    let persisted = repo
        .query_ports()
        .get_pod(klights_pod_api::PodGetRequest::try_by_name(
            "default", pod_name,
        )?)
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
    stores: &crate::bootstrap::node_store::NodeLocalStores,
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
impl klights_kubelet::pod_deletion_finalizer::PodDeletionFinalizer
    for IntegrationFixedDeletionFinalizer
{
    async fn finalize_after_actor_cleanup(
        &self,
        _key: &klights_kubelet::runtime_types::PodRuntimeKey,
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
    use klights_kubelet::pod_deletion_finalizer::PodDeletionFinalizer as _;
    let deferred = klights_kubelet::pod_repository::status::DeferredRuntimeReducerHandle::default();
    deferred.insert_marker(uid);
    let finalizer =
        crate::bootstrap::pod_repository_composition::DeferredRuntimeCleanupFinalizer::new(
            Arc::new(IntegrationFixedDeletionFinalizer { outcome }),
            deferred.clone(),
        );
    let result = finalizer
        .finalize_after_actor_cleanup(&klights_kubelet::runtime_types::PodRuntimeKey::new(
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
    store: Arc<IntegrationPodStoreFixture>,
    attempts: std::sync::atomic::AtomicUsize,
    mode: IntegrationStatusRaceMode,
}

impl klights_pod_api::PodStatusPersistence for IntegrationStatusRaceWriter {
    fn write_pod_status(
        &self,
        request: klights_pod_api::PodStatusWriteRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, crate::datastore::Resource> {
        Box::pin(async move {
            let klights_pod_api::PodStatusWriteRequest {
                namespace,
                name,
                status,
                expected_resource_version,
            } = request;
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
                let current = self
                    .store
                    .read_pod(&namespace, &name)
                    .await
                    .map_err(|error| {
                        klights_pod_api::PodRepositoryError::unavailable(error.to_string())
                    })?
                    .expect("race pod");
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
                    .update_pod(&namespace, &name, raced, current.resource_version)
                    .await
                    .map_err(|error| {
                        klights_pod_api::PodRepositoryError::unavailable(error.to_string())
                    })?;
                return Err(klights_pod_api::PodRepositoryError::conflict(
                    "injected status race",
                ));
            }
            self.store
                .update_pod_status(&namespace, &name, status, expected_resource_version)
                .await
                .map_err(|error| {
                    klights_pod_api::PodRepositoryError::unavailable(error.to_string())
                })
        })
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

struct IntegrationCountingPodMutationReconcile {
    effects: std::sync::atomic::AtomicUsize,
}

impl klights_reconcile_api::PodMutationReconcileSink for IntegrationCountingPodMutationReconcile {
    fn reconcile_pod_mutation(
        &self,
        _request: klights_reconcile_api::PodMutationReconcileRequest,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        self.effects
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

struct IntegrationPausedStatusWriter {
    store: Arc<IntegrationPodStoreFixture>,
    entered: Arc<tokio::sync::Barrier>,
    release: Arc<tokio::sync::Barrier>,
    requested_status: std::sync::Mutex<Option<serde_json::Value>>,
    attempts: std::sync::atomic::AtomicUsize,
}

impl klights_pod_api::PodStatusPersistence for IntegrationPausedStatusWriter {
    fn write_pod_status(
        &self,
        request: klights_pod_api::PodStatusWriteRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, crate::datastore::Resource> {
        Box::pin(async move {
            self.attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.requested_status
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .replace(request.status.clone());
            self.entered.wait().await;
            self.release.wait().await;
            self.store
                .update_pod_status(
                    &request.namespace,
                    &request.name,
                    request.status,
                    request.expected_resource_version,
                )
                .await
                .map_err(|error| {
                    klights_pod_api::PodRepositoryError::unavailable(error.to_string())
                })
        })
    }
}

pub async fn run_same_name_replacement_status_race(
    mut pod: serde_json::Value,
    update: klights_kubelet::pod_repository::PodStatusUpdate,
) -> IntegrationSameNameStatusRaceOutcome {
    let pod_name = "same-name-status-race";
    pod["metadata"]["name"] = serde_json::json!(pod_name);
    let store = Arc::new(IntegrationPodStoreFixture::new().await);
    let created = store.seed_pod("default", pod_name, pod).await.unwrap();
    let entered = Arc::new(tokio::sync::Barrier::new(2));
    let release = Arc::new(tokio::sync::Barrier::new(2));
    let writer = Arc::new(IntegrationPausedStatusWriter {
        store: store.clone(),
        entered: entered.clone(),
        release: release.clone(),
        requested_status: std::sync::Mutex::new(None),
        attempts: std::sync::atomic::AtomicUsize::new(0),
    });
    let reconcile = Arc::new(IntegrationCountingPodMutationReconcile {
        effects: std::sync::atomic::AtomicUsize::new(0),
    });
    let service = klights_kubelet::pod_repository::status::PodStatusService::new(
        klights_kubelet::pod_repository::status::PodStatusServiceDependencies {
            pod_query: store.query_port(),
            status_persistence: writer.clone(),
            mutation_reconcile: reconcile.clone(),
            outbox: None,
            remote_delivery_required: false,
            cluster_api: None,
            host_ip: klights_kubelet::context::HostIpState::default(),
            wall_clock: Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
        },
    );

    let write = service.integration_set_pod_status(
        "default",
        pod_name,
        &update,
        Some(created.resource_version),
    );
    let replace = async {
        entered.wait().await;
        let requested_status = writer
            .requested_status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .expect("paused status request");
        let mut replacement_body = created.data.as_ref().clone();
        let metadata = replacement_body["metadata"]
            .as_object_mut()
            .expect("Pod metadata object");
        metadata.remove("uid");
        metadata.remove("resourceVersion");
        replacement_body["status"] = requested_status;
        // This test-only datastore replacement models the exact interval
        // between the status service's read and persistence CAS. Production
        // Pod deletion remains actor-owned; no production path reaches this
        // characterization fixture.
        let replacement = store
            .replace_same_name_for_test("default", pod_name, replacement_body)
            .await
            .expect("install same-name replacement while status write is paused");
        release.wait().await;
        replacement
    };
    let (result, replacement) = tokio::join!(write, replace);
    let conflict = result
        .as_ref()
        .err()
        .is_some_and(|error| error.to_string().contains("409"));
    let persisted_after = store
        .read_pod("default", pod_name)
        .await
        .unwrap()
        .expect("replacement remains persisted");

    IntegrationSameNameStatusRaceOutcome {
        old_uid: created.uid,
        replacement,
        persisted_after,
        persistence_attempts: writer.attempts.load(std::sync::atomic::Ordering::SeqCst),
        reconcile_effects: reconcile.effects.load(std::sync::atomic::Ordering::SeqCst),
        // This fixture deliberately supplies neither an outbox nor a remote
        // leader query, so the local CAS path has no outbox route to invoke.
        outbox_enqueues: 0,
        conflict,
    }
}

async fn integration_status_race_service(
    pod_name: &str,
    pod: serde_json::Value,
    mode: IntegrationStatusRaceMode,
) -> (
    klights_kubelet::pod_repository::status::PodStatusService,
    Arc<IntegrationStatusRaceWriter>,
    crate::datastore::Resource,
) {
    let store = Arc::new(IntegrationPodStoreFixture::new().await);
    let created = store.seed_pod("default", pod_name, pod).await.unwrap();
    let writer = Arc::new(IntegrationStatusRaceWriter {
        store: store.clone(),
        attempts: std::sync::atomic::AtomicUsize::new(0),
        mode,
    });
    let service = klights_kubelet::pod_repository::status::PodStatusService::new(
        klights_kubelet::pod_repository::status::PodStatusServiceDependencies {
            pod_query: store.query_port(),
            status_persistence: writer.clone(),
            mutation_reconcile: Arc::new(IntegrationNoopPodMutationReconcile),
            outbox: None,
            remote_delivery_required: false,
            cluster_api: None,
            host_ip: klights_kubelet::context::HostIpState::default(),
            wall_clock: Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
        },
    );
    (service, writer, created)
}

pub async fn run_scheduler_status_race(
    pod: serde_json::Value,
    update: klights_kubelet::pod_repository::PodStatusUpdate,
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

pub struct IntegrationPodNetworkFixture {
    stores: Option<Arc<crate::bootstrap::node_store::NodeLocalStores>>,
    service: klights_kubelet::pod_repository::PodNetworkService,
}

impl IntegrationPodNetworkFixture {
    pub fn with_cache_and_waiter(
        cache: Arc<dyn klights_node_store::PodNetworkCache>,
        waiter: Arc<dyn klights_network_api::PodNetworkAssignmentWaiter>,
    ) -> Self {
        Self {
            stores: None,
            service: klights_kubelet::pod_repository::PodNetworkService::new(
                cache,
                Arc::new(klights_supervisor::TaskSupervisor::new(
                    klights_supervisor::TaskCategoryConfig::default(),
                )),
                waiter,
                klights_kubelet::context::HostIpState::default(),
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
            crate::bootstrap::node_store::open_node_local(
                crate::datastore::backend_kind::BackendKind::Sqlite,
                None,
                supervisor.clone(),
                None,
                "sqlite:pod-network-integration",
            )
            .await
            .expect("Pod network integration store"),
        );
        let service = klights_kubelet::pod_repository::PodNetworkService::new(
            stores.pod_network_cache(),
            supervisor,
            waiter,
            klights_kubelet::context::HostIpState::default(),
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
    ) -> Result<
        klights_kubelet::pod_repository::PodNetworkAssignment,
        klights_kubelet::pod_repository::PodNetworkAssignmentError,
    > {
        use klights_kubelet::pod_repository::PodNetworkAssignmentQuery as _;
        self.service
            .read_pod_network_assignment(
                klights_kubelet::pod_repository::PodNetworkAssignmentRequest::try_new(
                    sandbox_id,
                    klights_types::PodIdentity::new(namespace, pod_name, pod_uid),
                    host_network,
                )?,
            )
            .await
    }
}

pub struct IntegrationPodStoreFixture {
    _sqlite: crate::datastore::sqlite::Datastore,
    db: crate::datastore::DatastoreHandle,
    store: Arc<klights_kubelet::pod_repository::store::PodStore>,
    bound_finalization: Arc<
        dyn crate::bootstrap::composition_adapters::pod_repository_persistence_adapter::LocalBoundPodFinalizationPersistence,
    >,
    unscheduled_deletion: Arc<dyn klights_pod_api::UnscheduledPodDeletion>,
}

impl IntegrationPodStoreFixture {
    pub async fn new() -> Self {
        let sqlite = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .expect("Pod store integration fixture");
        let datastore: crate::datastore::DatastoreHandle = Arc::new(sqlite.clone());
        let persistence = crate::bootstrap::composition_adapters::pod_repository_persistence_adapter::new_root_parts(
            datastore.clone(),
            Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
        );
        Self {
            _sqlite: sqlite,
            db: datastore,
            store: persistence.store,
            bound_finalization: persistence.bound_finalization,
            unscheduled_deletion: persistence.unscheduled_deletion,
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

    pub fn query_port(&self) -> Arc<dyn klights_pod_api::PodQuery> {
        self.store.clone()
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
    ) -> anyhow::Result<klights_pod_api::PodListResult> {
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

    pub async fn replace_same_name_for_test(
        &self,
        namespace: &str,
        name: &str,
        replacement: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.db
            .delete_resource("v1", "Pod", Some(namespace), name)
            .await?;
        self.db
            .create_resource("v1", "Pod", Some(namespace), name, replacement)
            .await
    }

    pub async fn finalize_bound_pod_after_actor_cleanup(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<IntegrationBoundPodDeleteOutcome> {
        let outcome = self
            .bound_finalization
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
        self.unscheduled_deletion
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

struct IntegrationPodDeleteCasRaceHook {
    inner: crate::datastore::DatastoreHandle,
    pod_name: String,
    race: IntegrationPodDeleteCasRaceKind,
    raced: Arc<std::sync::atomic::AtomicBool>,
}

impl IntegrationPodDeleteCasRaceHook {
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
}

impl
    crate::bootstrap::composition_adapters::pod_repository_persistence_adapter::PodDeleteCasTestHook
    for IntegrationPodDeleteCasRaceHook
{
    fn before_delete_cas<'a>(
        &'a self,
        identity: &'a klights_types::PodIdentity,
        _observed_resource_version: i64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            anyhow::ensure!(
                identity.namespace == "default" && identity.name == self.pod_name,
                "unexpected Pod delete CAS target {}/{}",
                identity.namespace,
                identity.name
            );
            self.inject_race().await
        })
    }
}

async fn integration_pod_delete_cas_race_store(
    pod_name: &str,
    race: IntegrationPodDeleteCasRaceKind,
) -> (
    crate::bootstrap::composition_adapters::pod_repository_persistence_adapter::RootPodRepositoryPersistenceParts,
    crate::datastore::DatastoreHandle,
    Arc<std::sync::atomic::AtomicBool>,
){
    let sqlite = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .expect("delete CAS race datastore");
    let inner: crate::datastore::DatastoreHandle = Arc::new(sqlite);
    let raced = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let hook = Arc::new(IntegrationPodDeleteCasRaceHook {
        inner: inner.clone(),
        pod_name: pod_name.to_string(),
        race,
        raced: raced.clone(),
    });
    let datastore: crate::datastore::DatastoreHandle = inner;
    let persistence = crate::bootstrap::composition_adapters::
        pod_repository_persistence_adapter::new_root_parts_with_delete_cas_hook(
            datastore.clone(),
            hook,
        );
    (persistence, datastore, raced)
}

pub async fn run_unscheduled_pod_delete_cas_race(
    pod_name: &str,
    pod_uid: &str,
    race: IntegrationPodDeleteCasRaceKind,
) -> anyhow::Result<IntegrationUnscheduledPodDeleteCasRaceOutcome> {
    let (persistence, datastore, raced) =
        integration_pod_delete_cas_race_store(pod_name, race).await;
    let store = persistence.store.clone();
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
    let disposition = persistence
        .unscheduled_deletion
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
    let (persistence, datastore, raced) = integration_pod_delete_cas_race_store(
        pod_name,
        IntegrationPodDeleteCasRaceKind::StatusUpdate,
    )
    .await;
    let store = persistence.store.clone();
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
            persistence.bound_finalization,
            None,
            None,
            false,
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

impl PodRepositoryScenarioOwner {
    pub fn query_ports(&self) -> &IntegrationPodQueryPorts {
        &self.query_ports
    }

    pub fn update_ports(&self) -> &IntegrationPodUpdatePorts {
        &self.update_ports
    }

    pub fn status_ports(&self) -> &IntegrationPodStatusPorts {
        &self.status_ports
    }

    pub fn network_ports(&self) -> &IntegrationPodNetworkPorts {
        &self.network_ports
    }

    pub fn api_ports(&self) -> &IntegrationPodApiPorts {
        &self.api_ports
    }

    pub fn scheduling_ports(&self) -> &dyn klights_pod_api::PodScheduling {
        self.scheduling.as_ref()
    }

    /// Test-only watch subscription over the canonical store-owned event
    /// source. This keeps the compatibility fixture focused while avoiding
    /// any public root repository facade.
    pub fn subscribe_pod_watch(
        &self,
    ) -> tokio::sync::broadcast::Receiver<klights_watch::WatchEvent> {
        crate::bootstrap::pod_repository_composition::PodWatchSource::subscribe_pod_watch(
            self.watch_source.as_ref(),
        )
    }

    pub async fn new_inline() -> Self {
        Self::new_exact(
            None,
            false,
            false,
            false,
            false,
            crate::bootstrap::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
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
            false,
            crate::bootstrap::pod_repository_composition::PodSchedulingMode::DeferredMultiNodeLeader,
            None,
            None,
        )
        .await
    }

    pub async fn new_deferred_leader_with_node_outbox() -> Self {
        Self::new_exact(
            None,
            false,
            false,
            true,
            false,
            crate::bootstrap::pod_repository_composition::PodSchedulingMode::DeferredMultiNodeLeader,
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
            false,
            crate::bootstrap::pod_repository_composition::PodSchedulingMode::DeferredMultiNodeLeader,
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
            true,
            false,
            false,
            false,
            crate::bootstrap::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
            None,
            None,
        )
        .await
    }

    pub async fn new_with_node_outbox() -> Self {
        Self::new_exact(
            None,
            false,
            false,
            true,
            false,
            crate::bootstrap::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
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
            true,
            false,
            true,
            false,
            crate::bootstrap::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
            None,
            None,
        )
        .await
    }

    pub async fn new_with_status_dispatcher() -> Self {
        Self::new_exact(
            None,
            false,
            true,
            false,
            false,
            crate::bootstrap::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
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
            false,
            true,
            crate::bootstrap::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
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
            false,
            crate::bootstrap::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
            None,
            Some(observation),
        )
        .await
    }

    async fn new_exact(
        repository_cluster_api: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
        remote_delivery_required: bool,
        with_dispatcher: bool,
        with_outbox: bool,
        with_workqueue: bool,
        scheduling_mode: crate::bootstrap::pod_repository_composition::PodSchedulingMode,
        scheduler_bind_gate: Option<Arc<crate::bootstrap::composition_adapters::pod_native_adapter::SchedulerBindGateForTest>>,
        delete_observation: Option<Arc<tokio::sync::Mutex<Option<(bool, bool)>>>>,
    ) -> Self {
        Self::new_exact_on(
            None,
            repository_cluster_api,
            remote_delivery_required,
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
        remote_delivery_required: bool,
        with_dispatcher: bool,
        with_outbox: bool,
        with_workqueue: bool,
        scheduling_mode: crate::bootstrap::pod_repository_composition::PodSchedulingMode,
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
        let post_write_maintenance_notify = Arc::new(tokio::sync::Notify::new());
        let authority = crate::bootstrap::authority::AuthorityHandle::from(
            crate::bootstrap::composition_adapters::authority_adapter::always_leader_watch(),
        );
        let local_query = crate::bootstrap::composition_adapters::resource_query_adapter::
            DatastoreResourceQueryAdapter::new(db.clone(), authority.clone());
        let local_outbox_delivery = crate::bootstrap::composition_adapters::
            committed_outbox_delivery_adapter::test_outbox_delivery(
                db.clone(),
                &authority,
                crate::bootstrap::local_leader_adapters::new_local_outbox_side_effect_state(
                    db.clone(),
                ),
                "pod-repository-composition".to_string(),
            );
        let native_resource_query = repository_cluster_api.clone().unwrap_or(local_query);
        let metrics = klights_controllers::side_effects::SideEffectMetrics::new();
        let controller_dispatcher =
            with_dispatcher.then(|| Arc::new(PodRepositoryRecordingReconcileSink::default()));
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
                crate::bootstrap::node_store::open_node_local(
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
        let (
            pod_query,
            pod_snapshot,
            pod_update,
            pod_status_writer,
            _pod_workqueue,
            pod_network_assignment,
            _pod_host_ip,
            background,
            deletion_finalizer,
            _dirty_counter,
            _mutation_reconcile,
            gc_delete,
            _eviction_admission,
            _namespace_bootstrap,
            _namespace_termination_queue,
            api,
            subresource,
            scheduling,
            watch_source,
            bound_pod_finalization,
            deferred_runtime,
            _test_api,
            _test_subresource,
        ) = crate::bootstrap::pod_repository_composition::build_integration_pod_repository_parts(
            crate::bootstrap::pod_repository_composition::PodRepositoryBuildConfig {
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
                remote_delivery_required,
                controller_identity: crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
                #[cfg(not(test))]
                api_identity: Arc::new(
                    crate::bootstrap::controller_adapters::system_identity_adapter::SystemIdentityGenerator,
                ),
                #[cfg(not(test))]
                gc_coordination: Arc::new(klights_controllers::ControllerCoordination::new()),
                scheduler_bind_gate,
                post_write_maintenance_notify: Some(post_write_maintenance_notify.clone()),
            },
            native_resource_query,
        );
        if with_dispatcher {
            side_effects.set_pod_ports(pod_query.clone(), gc_delete.clone());
        }
        Self {
            _sqlite: sqlite,
            db,
            query_ports: IntegrationPodQueryPorts {
                query: pod_query,
                snapshot: pod_snapshot,
            },
            update_ports: IntegrationPodUpdatePorts { update: pod_update },
            status_ports: IntegrationPodStatusPorts {
                writer: pod_status_writer,
            },
            network_ports: IntegrationPodNetworkPorts {
                assignment: pod_network_assignment,
            },
            api_ports: IntegrationPodApiPorts {
                api: api.expect("integration root Pod API"),
                subresource: subresource.expect("integration root Pod subresource"),
            },
            scheduling: scheduling.expect("integration root Pod scheduler"),
            gc_delete,
            deletion_finalizer,
            bound_pod_finalization,
            deferred_runtime,
            supervisor,
            background,
            watch_source,
            controller_dispatcher,
            node_local,
            outbox_delivery: with_outbox.then_some(local_outbox_delivery),
            delete_observation,
            post_write_maintenance_notify,
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
            .pending_keys()
            .await
    }

    pub async fn enqueue_reconcile_key(&self, key: klights_reconcile_api::ReconcileKey) {
        self.controller_dispatcher
            .as_ref()
            .expect("status dispatcher fixture")
            .enqueue_key(key)
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

    pub async fn wait_for_post_write_maintenance(&self) {
        self.post_write_maintenance_notify.notified().await;
    }

    pub async fn request_gc_pod_delete(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<()> {
        klights_reconcile_api::GcPodDeleteSink::request_gc_pod_delete(
            self.gc_delete.as_ref(),
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
        match self
            .api_ports()
            .delete_pod(
                "default",
                "side-effect-pod",
                klights_pod_api::PodDeleteOptions::default(),
                false,
            )
            .await
            .map_err(anyhow::Error::new)?
        {
            klights_pod_api::PodApiDeleteOutcome::GracefulSet(_) => {}
            klights_pod_api::PodApiDeleteOutcome::DryRun(_) => {
                anyhow::bail!("side-effect delete unexpectedly dry-ran")
            }
        }
        let value = *observed.lock().await;
        Ok(value)
    }

    pub async fn claim_uid_bound_pod_work(
        &self,
    ) -> anyhow::Result<Option<IntegrationPodWorkqueueEntry>> {
        let stores = self.node_local.as_ref().expect("GC workqueue fixture");
        let lease = stores
            .pod_workqueue()
            .claim_due_work_with_lease(klights_node_store::PodWorkqueueClaimRequest::try_new(
                i64::MAX - 1,
                1,
            )?)
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let Some(lease) = lease else {
            return Ok(None);
        };
        let result = {
            let row = lease.entry();
            match row.identity() {
                klights_node_store::PodWorkIdentity::Pod(identity) => {
                    let payload: serde_json::Value = serde_json::from_slice(row.payload())?;
                    Some(IntegrationPodWorkqueueEntry {
                        namespace: identity.namespace.clone(),
                        name: identity.name.clone(),
                        uid: identity.uid.clone(),
                        target_node: payload
                            .get("target_node")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_string),
                    })
                }
                klights_node_store::PodWorkIdentity::Namespace { .. } => None,
            }
        };
        stores
            .pod_workqueue()
            .acknowledge_work(lease.token().clone())
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        Ok(result)
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
            self.gc_delete.as_ref(),
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
        self.db
            .create_resource("v1", "Pod", Some(namespace), name, pod)
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

    pub async fn finalize_bound_pod_after_actor_cleanup(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<IntegrationBoundPodDeleteOutcome> {
        let request = klights_pod_api::BoundPodFinalizationRequest::try_new(
            klights_types::PodIdentity::new(namespace, name, uid),
        )?;
        let outcome = self
            .bound_pod_finalization
            .finalize_bound_pod(request)
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        Ok(map_bound_delete_outcome(outcome))
    }

    pub async fn finalize_pod_deletion_after_actor_cleanup(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<IntegrationPodFinalizationOutcome> {
        integration_finalize_pod_after_actor_cleanup(
            self.deletion_finalizer.as_ref(),
            namespace,
            name,
            uid,
        )
        .await
    }

    pub fn has_deferred_runtime_for_uid(&self, pod_uid: &str) -> bool {
        self.deferred_runtime.contains(pod_uid)
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
        klights_controllers::pdb::reconcile_pdb_at(
            self.db.as_ref(),
            self.query_ports.query.as_ref(),
            pdb,
            now,
        )
        .await
    }
}

/// Construction scenarios use one focused handle for lifecycle wiring and
/// metadata/status assertions.  The private owner keeps the backing stores
/// alive without exposing an aggregate repository object.
pub struct IntegrationPodConstructionFixture {
    owner: Arc<PodRepositoryScenarioOwner>,
}

impl IntegrationPodConstructionFixture {
    pub async fn new_inline() -> Self {
        Self {
            owner: Arc::new(PodRepositoryScenarioOwner::new_inline().await),
        }
    }

    pub fn background_is_available(&self) -> bool {
        self.owner.background_is_available()
    }

    pub fn workqueue_start_called(&self) -> bool {
        self.owner.workqueue_start_called()
    }

    pub async fn start_background(&self) -> anyhow::Result<()> {
        self.owner.start_background().await
    }
}

/// Metadata mutation scenarios carry only Pod query/update capabilities.
pub struct IntegrationPodMetadataFixture {
    owner: Arc<PodRepositoryScenarioOwner>,
}

impl IntegrationPodMetadataFixture {
    pub async fn new_inline() -> Self {
        Self {
            owner: Arc::new(PodRepositoryScenarioOwner::new_inline().await),
        }
    }

    pub async fn get_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        self.owner
            .query_ports()
            .get_pod(klights_pod_api::PodGetRequest::try_by_name(
                namespace, name,
            )?)
            .await
    }

    pub async fn seed_pod(
        &self,
        namespace: &str,
        name: &str,
        pod: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.owner.seed_pod(namespace, name, pod).await
    }

    pub async fn update_pod_owner_references_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        owner_references: Vec<serde_json::Value>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.owner
            .update_ports()
            .update_pod(klights_pod_api::PodUpdateRequest::replace_owner_references(
                klights_pod_api::PodMutationTarget::try_by_identity(
                    klights_types::PodIdentity::new(namespace, name, uid),
                )?,
                integration_owner_references(owner_references)?,
            ))
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn merge_pod_labels_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        labels: Vec<(String, String)>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.owner
            .update_ports()
            .update_pod(klights_pod_api::PodUpdateRequest::merge_labels(
                klights_pod_api::PodMutationTarget::try_by_identity(
                    klights_types::PodIdentity::new(namespace, name, uid),
                )?,
                labels
                    .into_iter()
                    .map(|(key, value)| klights_pod_api::PodLabel::try_new(key, value))
                    .collect::<Result<Vec<_>, _>>()?,
            ))
            .await
            .map_err(anyhow::Error::new)
    }
}

/// Repository-backed network lookup scenario.  It exposes only the focused
/// assignment query; the standalone `IntegrationPodNetworkFixture` remains
/// the node-local allocator scenario.
pub struct IntegrationPodNetworkScenarioFixture {
    owner: Arc<PodRepositoryScenarioOwner>,
}

impl IntegrationPodNetworkScenarioFixture {
    pub async fn new_inline() -> Self {
        Self {
            owner: Arc::new(PodRepositoryScenarioOwner::new_inline().await),
        }
    }

    pub async fn read_pod_network_assignment(
        &self,
        sandbox_id: &str,
        namespace: &str,
        name: &str,
        uid: &str,
        host_network: bool,
    ) -> anyhow::Result<klights_kubelet::pod_repository::PodNetworkAssignment> {
        self.owner
            .network_ports()
            .read(
                klights_kubelet::pod_repository::PodNetworkAssignmentRequest::try_new(
                    sandbox_id,
                    klights_types::PodIdentity::new(namespace, name, uid),
                    host_network,
                )?,
            )
            .await
    }
}

/// Status/namespace scenarios receive only the status, query and namespace
/// handles needed by those tests.
pub struct IntegrationPodStatusFixture {
    owner: Arc<PodRepositoryScenarioOwner>,
}

impl IntegrationPodStatusFixture {
    pub async fn new_inline() -> Self {
        Self {
            owner: Arc::new(PodRepositoryScenarioOwner::new_inline().await),
        }
    }

    pub async fn new_with_status_dispatcher() -> Self {
        Self {
            owner: Arc::new(PodRepositoryScenarioOwner::new_with_status_dispatcher().await),
        }
    }

    pub fn query_ports(&self) -> &IntegrationPodQueryPorts {
        self.owner.query_ports()
    }

    pub fn update_ports(&self) -> &IntegrationPodUpdatePorts {
        self.owner.update_ports()
    }

    pub fn status_ports(&self) -> &IntegrationPodStatusPorts {
        self.owner.status_ports()
    }

    pub fn api_ports(&self) -> &IntegrationPodApiPorts {
        self.owner.api_ports()
    }

    pub async fn get_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        self.query_ports()
            .get_pod(klights_pod_api::PodGetRequest::try_by_name(
                namespace, name,
            )?)
            .await
    }

    pub async fn seed_pod(
        &self,
        namespace: &str,
        name: &str,
        pod: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.owner.seed_pod(namespace, name, pod).await
    }

    pub async fn seed_non_pod_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.owner
            .seed_non_pod_resource(api_version, kind, namespace, name, value)
            .await
    }

    pub async fn read_non_pod_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        self.owner
            .read_non_pod_resource(api_version, kind, namespace, name)
            .await
    }

    pub async fn seed_namespace(
        &self,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.owner.seed_namespace(name, value).await
    }

    pub async fn read_namespace(
        &self,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        self.owner.read_namespace(name).await
    }

    pub async fn update_namespace(
        &self,
        name: &str,
        value: serde_json::Value,
        expected_resource_version: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.owner
            .update_namespace(name, value, expected_resource_version)
            .await
    }

    pub async fn reconcile_namespace_termination(
        &self,
        name: &str,
        now: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<()> {
        self.owner.reconcile_namespace_termination(name, now).await
    }

    pub async fn reconcile_pod_disruption_budget(
        &self,
        pdb: &serde_json::Value,
        now: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<()> {
        self.owner.reconcile_pod_disruption_budget(pdb, now).await
    }

    pub async fn pending_reconcile_keys(&self) -> Vec<klights_reconcile_api::ReconcileKey> {
        self.owner.pending_reconcile_keys().await
    }

    pub async fn wait_for_post_write_maintenance(&self) {
        self.owner.wait_for_post_write_maintenance().await;
    }

    pub async fn enqueue_reconcile_key(&self, key: klights_reconcile_api::ReconcileKey) {
        self.owner.enqueue_reconcile_key(key).await
    }

    pub fn has_deferred_runtime_for_uid(&self, uid: &str) -> bool {
        self.owner.has_deferred_runtime_for_uid(uid)
    }

    pub async fn finalize_pod_deletion_after_actor_cleanup(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<IntegrationPodFinalizationOutcome> {
        self.owner
            .finalize_pod_deletion_after_actor_cleanup(namespace, name, uid)
            .await
    }

    pub async fn record_sandbox_id(
        &self,
        namespace: &str,
        name: &str,
        sandbox_id: &str,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.update_ports()
            .update_pod(klights_pod_api::PodUpdateRequest::try_record_sandbox_id(
                klights_pod_api::PodMutationTarget::try_by_name(namespace, name)?,
                sandbox_id,
            )?)
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn record_sandbox_id_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        sandbox_id: &str,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.update_ports()
            .update_pod(klights_pod_api::PodUpdateRequest::try_record_sandbox_id(
                klights_pod_api::PodMutationTarget::try_by_identity(
                    klights_types::PodIdentity::new(namespace, name, uid),
                )?,
                sandbox_id,
            )?)
            .await
            .map_err(anyhow::Error::new)
    }
}

/// Watch/store scenarios own a query handle and the canonical watch source;
/// no mutation, status or controller family is exposed.
pub struct IntegrationPodStoreWatchFixture {
    owner: Arc<PodRepositoryScenarioOwner>,
}

impl IntegrationPodStoreWatchFixture {
    pub async fn new_inline() -> Self {
        Self {
            owner: Arc::new(PodRepositoryScenarioOwner::new_inline().await),
        }
    }

    pub async fn new_cluster_backed(
        resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    ) -> Self {
        Self {
            owner: Arc::new(PodRepositoryScenarioOwner::new_cluster_backed(resource_query).await),
        }
    }

    pub fn query_ports(&self) -> &IntegrationPodQueryPorts {
        self.owner.query_ports()
    }

    pub async fn get_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        self.query_ports()
            .get_pod(klights_pod_api::PodGetRequest::try_by_name(
                namespace, name,
            )?)
            .await
    }

    pub async fn list_pods(
        &self,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> anyhow::Result<klights_pod_api::PodListResult> {
        self.query_ports()
            .list_pods(klights_pod_api::PodListRequest::try_new(
                namespace.map(str::to_string),
                label_selector.map(str::to_string),
                field_selector.map(str::to_string),
                limit,
                continue_token.map(str::to_string),
            )?)
            .await
    }

    pub async fn list_pods_by_owner_uid(
        &self,
        namespace: &str,
        owner_uid: &str,
    ) -> anyhow::Result<Vec<crate::datastore::Resource>> {
        self.query_ports()
            .list_pods_by_owner_uid(klights_pod_api::PodOwnerListRequest::try_new(
                namespace, owner_uid,
            )?)
            .await
    }

    pub async fn seed_pod(
        &self,
        namespace: &str,
        name: &str,
        pod: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.owner.seed_pod(namespace, name, pod).await
    }

    pub fn subscribe_pod_watch(
        &self,
    ) -> tokio::sync::broadcast::Receiver<klights_watch::WatchEvent> {
        self.owner.subscribe_pod_watch()
    }

    pub async fn pod_watch_events_since(
        &self,
        resource_version: i64,
    ) -> anyhow::Result<Vec<IntegrationPodWatchEvent>> {
        self.owner.pod_watch_events_since(resource_version).await
    }
}

/// API/deadline scenarios expose API and query ports plus persistence needed
/// to seed a Pod and inspect its canonical watch history.
pub struct IntegrationPodApiFixture {
    owner: Arc<PodRepositoryScenarioOwner>,
}

impl IntegrationPodApiFixture {
    pub async fn new_inline() -> Self {
        Self {
            owner: Arc::new(PodRepositoryScenarioOwner::new_inline().await),
        }
    }

    pub fn query_ports(&self) -> &IntegrationPodQueryPorts {
        self.owner.query_ports()
    }

    pub fn api_ports(&self) -> &IntegrationPodApiPorts {
        self.owner.api_ports()
    }

    pub async fn seed_pod(
        &self,
        namespace: &str,
        name: &str,
        pod: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.owner.seed_pod(namespace, name, pod).await
    }

    pub async fn pod_watch_events_since(
        &self,
        resource_version: i64,
    ) -> anyhow::Result<Vec<IntegrationPodWatchEvent>> {
        self.owner.pod_watch_events_since(resource_version).await
    }
}

/// Scheduling scenarios expose scheduling/query/API ports and node-local
/// delivery helpers, but no status, network, deletion or lifecycle facade.
pub struct IntegrationPodSchedulingFixture {
    owner: Arc<PodRepositoryScenarioOwner>,
}

impl IntegrationPodSchedulingFixture {
    pub async fn new_inline() -> Self {
        Self {
            owner: Arc::new(PodRepositoryScenarioOwner::new_inline().await),
        }
    }

    pub async fn new_deferred_leader() -> Self {
        Self {
            owner: Arc::new(PodRepositoryScenarioOwner::new_deferred_leader().await),
        }
    }

    pub async fn new_deferred_leader_with_node_outbox() -> Self {
        Self {
            owner: Arc::new(
                PodRepositoryScenarioOwner::new_deferred_leader_with_node_outbox().await,
            ),
        }
    }

    pub async fn new_deferred_leader_with_bind_gate() -> (Self, IntegrationSchedulerBindGate) {
        let (owner, gate) = PodRepositoryScenarioOwner::new_deferred_leader_with_bind_gate().await;
        (
            Self {
                owner: Arc::new(owner),
            },
            gate,
        )
    }

    pub async fn new_with_status_dispatcher() -> Self {
        Self {
            owner: Arc::new(PodRepositoryScenarioOwner::new_with_status_dispatcher().await),
        }
    }

    pub fn query_ports(&self) -> &IntegrationPodQueryPorts {
        self.owner.query_ports()
    }

    pub fn api_ports(&self) -> &IntegrationPodApiPorts {
        self.owner.api_ports()
    }

    pub fn scheduling_ports(&self) -> &dyn klights_pod_api::PodScheduling {
        self.owner.scheduling_ports()
    }

    pub async fn get_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        self.query_ports()
            .get_pod(klights_pod_api::PodGetRequest::try_by_name(
                namespace, name,
            )?)
            .await
    }

    pub async fn list_pods(
        &self,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> anyhow::Result<klights_pod_api::PodListResult> {
        self.query_ports()
            .list_pods(klights_pod_api::PodListRequest::try_new(
                namespace.map(str::to_string),
                label_selector.map(str::to_string),
                field_selector.map(str::to_string),
                limit,
                continue_token.map(str::to_string),
            )?)
            .await
    }

    pub async fn seed_pod(
        &self,
        namespace: &str,
        name: &str,
        pod: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.owner.seed_pod(namespace, name, pod).await
    }

    pub async fn seed_scheduling_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.owner
            .seed_scheduling_resource(api_version, kind, namespace, name, value)
            .await
    }

    pub async fn pending_reconcile_keys(&self) -> Vec<klights_reconcile_api::ReconcileKey> {
        self.owner.pending_reconcile_keys().await
    }

    pub async fn pod_watch_events_since(
        &self,
        resource_version: i64,
    ) -> anyhow::Result<Vec<IntegrationPodWatchEvent>> {
        self.owner.pod_watch_events_since(resource_version).await
    }

    pub async fn list_scheduling_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
    ) -> anyhow::Result<crate::datastore::ResourceList> {
        self.owner
            .list_scheduling_resources(api_version, kind, namespace)
            .await
    }

    pub fn active_supervised_task_count(&self) -> usize {
        self.owner.active_supervised_task_count()
    }

    pub async fn drain_node_outbox_to_local_leader(&self) -> anyhow::Result<()> {
        self.owner.drain_node_outbox_to_local_leader().await
    }

    pub async fn apply_uid_bound_worker_status_reducer_scenario(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        authenticated_node: &str,
        status: serde_json::Value,
    ) -> anyhow::Result<()> {
        self.owner
            .apply_uid_bound_worker_status_reducer_scenario(
                namespace,
                name,
                uid,
                authenticated_node,
                status,
            )
            .await
    }

    pub async fn create_controller_pod(
        &self,
        namespace: &str,
        name: &str,
        _node_name: &str,
        pod: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.api_ports()
            .create(klights_pod_api::PodApiCreateRequest {
                namespace: namespace.to_string(),
                body: pod,
                dry_run: false,
            })
            .await
            .map_err(anyhow::Error::new)?
            .resource
            .ok_or_else(|| {
                anyhow::anyhow!("controller Pod {namespace}/{name} unexpectedly dry-ran")
            })
    }
}

/// Root worker-role scenario fixture.  This is intentionally distinct from
/// `IntegrationPodWorkerFixture`, which models the worker-local adapter; the
/// scenario below only exposes the status/metadata/query ports needed to
/// exercise node-outbox routing.
pub struct IntegrationPodWorkerScenarioFixture {
    owner: Arc<PodRepositoryScenarioOwner>,
}

impl IntegrationPodWorkerScenarioFixture {
    pub async fn new_with_node_outbox() -> Self {
        Self {
            owner: Arc::new(PodRepositoryScenarioOwner::new_with_node_outbox().await),
        }
    }

    pub async fn new_cluster_backed(
        resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    ) -> Self {
        Self {
            owner: Arc::new(PodRepositoryScenarioOwner::new_cluster_backed(resource_query).await),
        }
    }

    pub fn query_ports(&self) -> &IntegrationPodQueryPorts {
        self.owner.query_ports()
    }

    pub fn update_ports(&self) -> &IntegrationPodUpdatePorts {
        self.owner.update_ports()
    }

    pub fn status_ports(&self) -> &IntegrationPodStatusPorts {
        self.owner.status_ports()
    }

    pub async fn get_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        self.query_ports()
            .get_pod(klights_pod_api::PodGetRequest::try_by_name(
                namespace, name,
            )?)
            .await
    }

    pub async fn seed_pod(
        &self,
        namespace: &str,
        name: &str,
        pod: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.owner.seed_pod(namespace, name, pod).await
    }

    pub async fn set_pod_status_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        update: klights_kubelet::pod_repository::PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.status_ports()
            .set_pod_status_for_uid(namespace, name, uid, update, expected_rv)
            .await
    }

    pub async fn apply_runtime_reconcile_status_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        update: klights_kubelet::pod_repository::RuntimeReconcileStatus,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.status_ports()
            .apply_runtime_reconcile_status_for_uid(namespace, name, uid, update, expected_rv)
            .await
    }

    pub async fn record_sandbox_id_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        sandbox_id: &str,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.update_ports()
            .update_pod(klights_pod_api::PodUpdateRequest::try_record_sandbox_id(
                klights_pod_api::PodMutationTarget::try_by_identity(
                    klights_types::PodIdentity::new(namespace, name, uid),
                )?,
                sandbox_id,
            )?)
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn update_pod_owner_references(
        &self,
        namespace: &str,
        name: &str,
        refs: Vec<serde_json::Value>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.update_ports()
            .update_pod(klights_pod_api::PodUpdateRequest::replace_owner_references(
                klights_pod_api::PodMutationTarget::try_by_name(namespace, name)?,
                integration_owner_references(refs)?,
            ))
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn update_pod_owner_references_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        refs: Vec<serde_json::Value>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.update_ports()
            .update_pod(klights_pod_api::PodUpdateRequest::replace_owner_references(
                klights_pod_api::PodMutationTarget::try_by_identity(
                    klights_types::PodIdentity::new(namespace, name, uid),
                )?,
                integration_owner_references(refs)?,
            ))
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn merge_pod_labels_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        labels: Vec<(String, String)>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.update_ports()
            .update_pod(klights_pod_api::PodUpdateRequest::merge_labels(
                klights_pod_api::PodMutationTarget::try_by_identity(
                    klights_types::PodIdentity::new(namespace, name, uid),
                )?,
                labels
                    .into_iter()
                    .map(|(key, value)| klights_pod_api::PodLabel::try_new(key, value))
                    .collect::<Result<Vec<_>, _>>()?,
            ))
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn claim_next_due_outbox(
        &self,
        now_ms: i64,
        lease_ms: i64,
        lease_token: &str,
    ) -> anyhow::Result<Option<IntegrationClaimedPodOutbox>> {
        self.owner
            .claim_next_due_outbox(now_ms, lease_ms, lease_token)
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
        self.owner
            .node_local
            .as_ref()
            .expect("worker scenario node-local store")
            .pod_status_checkpoints()
            .upsert_pod_status_checkpoint(checkpoint)
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))
    }

    pub async fn has_status_checkpoint(&self, uid: &str) -> anyhow::Result<bool> {
        let key = klights_node_store::PodCheckpointKey::try_new(uid)?;
        self.owner
            .node_local
            .as_ref()
            .expect("worker scenario node-local store")
            .pod_status_checkpoints()
            .get_pod_status_checkpoint(key)
            .await
            .map(|value| value.is_some())
            .map_err(|error| anyhow::anyhow!(error.to_string()))
    }
}

/// Deletion scenarios own the UID-bound deletion/query/API and GC ports for
/// that scenario only.  This is intentionally separate from status and
/// network fixtures.
pub struct IntegrationPodDeletionFixture {
    owner: Arc<PodRepositoryScenarioOwner>,
}

impl IntegrationPodDeletionFixture {
    pub async fn new_inline() -> Self {
        Self {
            owner: Arc::new(PodRepositoryScenarioOwner::new_inline().await),
        }
    }

    pub async fn new_with_status_dispatcher() -> Self {
        Self {
            owner: Arc::new(PodRepositoryScenarioOwner::new_with_status_dispatcher().await),
        }
    }

    pub async fn new_with_delete_side_effect_observation() -> Self {
        Self {
            owner: Arc::new(
                PodRepositoryScenarioOwner::new_with_delete_side_effect_observation().await,
            ),
        }
    }

    pub async fn new_with_gc_workqueue() -> Self {
        Self {
            owner: Arc::new(PodRepositoryScenarioOwner::new_with_gc_workqueue().await),
        }
    }

    pub async fn new_cluster_backed(
        resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    ) -> Self {
        Self {
            owner: Arc::new(PodRepositoryScenarioOwner::new_cluster_backed(resource_query).await),
        }
    }

    pub fn query_ports(&self) -> &IntegrationPodQueryPorts {
        self.owner.query_ports()
    }

    pub fn update_ports(&self) -> &IntegrationPodUpdatePorts {
        self.owner.update_ports()
    }

    pub fn status_ports(&self) -> &IntegrationPodStatusPorts {
        self.owner.status_ports()
    }

    pub fn api_ports(&self) -> &IntegrationPodApiPorts {
        self.owner.api_ports()
    }

    pub async fn get_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        self.query_ports()
            .get_pod(klights_pod_api::PodGetRequest::try_by_name(
                namespace, name,
            )?)
            .await
    }

    pub async fn list_pods(
        &self,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> anyhow::Result<klights_pod_api::PodListResult> {
        self.query_ports()
            .list_pods(klights_pod_api::PodListRequest::try_new(
                namespace.map(str::to_string),
                label_selector.map(str::to_string),
                field_selector.map(str::to_string),
                limit,
                continue_token.map(str::to_string),
            )?)
            .await
    }

    pub async fn list_pods_by_owner_uid(
        &self,
        namespace: &str,
        owner_uid: &str,
    ) -> anyhow::Result<Vec<crate::datastore::Resource>> {
        self.query_ports()
            .list_pods_by_owner_uid(klights_pod_api::PodOwnerListRequest::try_new(
                namespace, owner_uid,
            )?)
            .await
    }

    pub async fn seed_pod(
        &self,
        namespace: &str,
        name: &str,
        pod: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.owner.seed_pod(namespace, name, pod).await
    }

    pub async fn seed_scheduling_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.owner
            .seed_scheduling_resource(api_version, kind, namespace, name, value)
            .await
    }

    pub async fn seed_namespace(
        &self,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.owner.seed_namespace(name, value).await
    }

    pub async fn seed_non_pod_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.owner
            .seed_non_pod_resource(api_version, kind, namespace, name, value)
            .await
    }

    pub async fn read_non_pod_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        self.owner
            .read_non_pod_resource(api_version, kind, namespace, name)
            .await
    }

    pub async fn pending_reconcile_keys(&self) -> Vec<klights_reconcile_api::ReconcileKey> {
        self.owner.pending_reconcile_keys().await
    }

    pub async fn request_gc_pod_delete(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<()> {
        self.owner.request_gc_pod_delete(namespace, name, uid).await
    }

    pub async fn run_gc_cascade(
        &self,
        owner_uid: &str,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        namespace: &str,
    ) -> anyhow::Result<()> {
        self.owner
            .run_gc_cascade(
                owner_uid,
                owner_api_version,
                owner_name,
                owner_kind,
                namespace,
            )
            .await
    }

    pub async fn claim_uid_bound_pod_work(
        &self,
    ) -> anyhow::Result<Option<IntegrationPodWorkqueueEntry>> {
        self.owner.claim_uid_bound_pod_work().await
    }

    pub async fn run_delete_side_effect_order_case(&self) -> anyhow::Result<Option<(bool, bool)>> {
        self.owner.run_delete_side_effect_order_case().await
    }

    pub async fn finalize_pod_deletion_after_actor_cleanup(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<IntegrationPodFinalizationOutcome> {
        self.owner
            .finalize_pod_deletion_after_actor_cleanup(namespace, name, uid)
            .await
    }

    pub async fn finalize_bound_pod_after_actor_cleanup(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<IntegrationBoundPodDeleteOutcome> {
        self.owner
            .finalize_bound_pod_after_actor_cleanup(namespace, name, uid)
            .await
    }

    pub async fn seed_mutating_webhook_configuration(
        &self,
        name: &str,
        configuration: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.owner
            .seed_mutating_webhook_configuration(name, configuration)
            .await
    }

    pub async fn pod_watch_events_since(
        &self,
        resource_version: i64,
    ) -> anyhow::Result<Vec<IntegrationPodWatchEvent>> {
        self.owner.pod_watch_events_since(resource_version).await
    }

    pub async fn read_namespace(
        &self,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        self.owner.read_namespace(name).await
    }

    pub async fn update_pod_owner_references(
        &self,
        namespace: &str,
        name: &str,
        refs: Vec<serde_json::Value>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.update_ports()
            .update_pod(klights_pod_api::PodUpdateRequest::replace_owner_references(
                klights_pod_api::PodMutationTarget::try_by_name(namespace, name)?,
                integration_owner_references(refs)?,
            ))
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn merge_pod_labels(
        &self,
        namespace: &str,
        name: &str,
        labels: Vec<(String, String)>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.update_ports()
            .update_pod(klights_pod_api::PodUpdateRequest::merge_labels(
                klights_pod_api::PodMutationTarget::try_by_name(namespace, name)?,
                labels
                    .into_iter()
                    .map(|(key, value)| klights_pod_api::PodLabel::try_new(key, value))
                    .collect::<Result<Vec<_>, _>>()?,
            ))
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn merge_pod_labels_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        labels: Vec<(String, String)>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.update_ports()
            .update_pod(klights_pod_api::PodUpdateRequest::merge_labels(
                klights_pod_api::PodMutationTarget::try_by_identity(
                    klights_types::PodIdentity::new(namespace, name, uid),
                )?,
                labels
                    .into_iter()
                    .map(|(key, value)| klights_pod_api::PodLabel::try_new(key, value))
                    .collect::<Result<Vec<_>, _>>()?,
            ))
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn update_pod_owner_references_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        refs: Vec<serde_json::Value>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.update_ports()
            .update_pod(klights_pod_api::PodUpdateRequest::replace_owner_references(
                klights_pod_api::PodMutationTarget::try_by_identity(
                    klights_types::PodIdentity::new(namespace, name, uid),
                )?,
                integration_owner_references(refs)?,
            ))
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn create_controller_pod(
        &self,
        namespace: &str,
        name: &str,
        _node_name: &str,
        pod: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.api_ports()
            .create(klights_pod_api::PodApiCreateRequest {
                namespace: namespace.to_string(),
                body: pod,
                dry_run: false,
            })
            .await
            .map_err(anyhow::Error::new)?
            .resource
            .ok_or_else(|| {
                anyhow::anyhow!("controller Pod {namespace}/{name} unexpectedly dry-ran")
            })
    }

    pub async fn delete_pod(&self, namespace: &str, name: &str) -> anyhow::Result<()> {
        match self
            .api_ports()
            .delete(klights_pod_api::PodApiDeleteRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                options: klights_pod_api::PodDeleteOptions::default(),
                dry_run: false,
            })
            .await
            .map_err(anyhow::Error::new)?
        {
            klights_pod_api::PodApiDeleteOutcome::GracefulSet(_) => Ok(()),
            klights_pod_api::PodApiDeleteOutcome::DryRun(_) => {
                anyhow::bail!("Pod delete unexpectedly dry-ran")
            }
        }
    }
}

struct BoundFinalizationHostileLeaderQuery {
    pod: crate::datastore::Resource,
}

impl klights_leader_api::LeaderResourceQuery for BoundFinalizationHostileLeaderQuery {
    fn get_resource(
        &self,
        request: klights_leader_api::ResourceGetRequest,
    ) -> klights_leader_api::ResourceQueryFuture<'_, Option<crate::datastore::Resource>> {
        Box::pin(async move {
            let key = request.into_key();
            Ok((key.api_version == "v1"
                && key.kind == "Pod"
                && key.namespace.as_deref() == self.pod.namespace.as_deref()
                && key.name == self.pod.name)
                .then(|| self.pod.clone()))
        })
    }

    fn list_resources(
        &self,
        request: klights_leader_api::ResourceListRequest,
    ) -> klights_leader_api::ResourceQueryFuture<'_, klights_leader_api::ResourceListResult> {
        Box::pin(async move {
            let items = (request.api_version() == "v1"
                && request.kind() == "Pod"
                && request.namespace() == self.pod.namespace.as_deref())
            .then(|| self.pod.clone())
            .into_iter()
            .collect();
            klights_leader_api::ResourceListResult::try_new(
                items,
                self.pod.resource_version,
                None,
                None,
                None,
            )
        })
    }
}

/// Runs the PR-BOUND real-composition regression without extending the frozen historical harness.
pub async fn run_local_bound_finalization_with_incidental_delivery_handles() -> anyhow::Result<()> {
    let incidental_remote = crate::datastore::Resource {
        id: 99,
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "local-finalize-child".to_string(),
        uid: "hostile-remote-replacement-uid".to_string(),
        resource_version: 99,
        data: Arc::new(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "local-finalize-child",
                "uid": "hostile-remote-replacement-uid",
                "resourceVersion": "99"
            },
            "spec": {
                "nodeName": "remote-worker",
                "containers": [{"name": "app", "image": "nginx"}]
            }
        })),
    };
    let repository = PodRepositoryScenarioOwner::new_exact(
        Some(Arc::new(BoundFinalizationHostileLeaderQuery {
            pod: incidental_remote,
        })),
        false,
        true,
        true,
        false,
        crate::bootstrap::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
        None,
        None,
    )
    .await;
    repository
        .seed_scheduling_resource(
            "v1",
            "Service",
            Some("default"),
            "local-finalize-service",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": "local-finalize-service", "namespace": "default"},
                "spec": {
                    "selector": {"app": "local-finalize"},
                    "ports": [{"port": 80}]
                }
            }),
        )
        .await?;
    repository
        .seed_scheduling_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "local-finalize-owner",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ReplicationController",
                "metadata": {
                    "name": "local-finalize-owner",
                    "namespace": "default",
                    "uid": "local-finalize-owner-uid",
                    "deletionTimestamp": "2026-08-08T00:00:00Z",
                    "finalizers": ["foregroundDeletion"]
                },
                "spec": {"replicas": 1, "selector": {"app": "local-finalize"}}
            }),
        )
        .await?;
    repository
        .seed_pod(
            "default",
            "local-finalize-child",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "local-finalize-child",
                    "namespace": "default",
                    "uid": "local-finalize-child-uid",
                    "labels": {"app": "local-finalize"},
                    "deletionTimestamp": "2026-08-08T00:00:00Z",
                    "deletionGracePeriodSeconds": 0,
                    "ownerReferences": [{
                        "apiVersion": "v1",
                        "kind": "ReplicationController",
                        "name": "local-finalize-owner",
                        "uid": "local-finalize-owner-uid",
                        "controller": true,
                        "blockOwnerDeletion": true
                    }]
                },
                "spec": {
                    "nodeName": "local-node",
                    "containers": [{"name": "app", "image": "nginx"}]
                },
                "status": {"phase": "Running"}
            }),
        )
        .await?;
    let checkpoint = klights_node_store::PodStatusCheckpointUpsert::try_new(
        klights_types::PodIdentity::new(
            "default",
            "local-finalize-child",
            "local-finalize-child-uid",
        ),
        1,
        serde_json::to_vec(&serde_json::json!({"phase": "Running"}))?,
        100,
    )?;
    let node_local = repository
        .node_local
        .as_ref()
        .expect("scenario requires incidental node-local delivery handles");
    node_local
        .pod_status_checkpoints()
        .upsert_pod_status_checkpoint(checkpoint)
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;

    let outcome = repository
        .finalize_pod_deletion_after_actor_cleanup(
            "default",
            "local-finalize-child",
            "local-finalize-child-uid",
        )
        .await?;

    anyhow::ensure!(
        outcome == IntegrationPodFinalizationOutcome::DeletedOrAlreadyGone,
        "explicit local role did not complete finalization synchronously"
    );
    anyhow::ensure!(
        repository
            .db
            .get_resource("v1", "Pod", Some("default"), "local-finalize-child",)
            .await?
            .is_none(),
        "local persistence retained the finalized Pod"
    );
    let checkpoint_key = klights_node_store::PodCheckpointKey::try_new("local-finalize-child-uid")?;
    anyhow::ensure!(
        node_local
            .pod_status_checkpoints()
            .get_pod_status_checkpoint(checkpoint_key)
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?
            .is_none(),
        "local finalization retained the UID-scoped status checkpoint"
    );
    anyhow::ensure!(
        repository
            .read_non_pod_resource(
                "v1",
                "ReplicationController",
                "default",
                "local-finalize-owner",
            )
            .await?
            .is_none(),
        "local finalization did not complete the unblocked foreground owner"
    );
    anyhow::ensure!(
        repository.pending_reconcile_keys().await.iter().any(|key| {
            key.api_version() == "v1"
                && key.kind() == "Service"
                && key.namespace() == Some("default")
                && key.name() == "local-finalize-service"
        }),
        "local finalization did not enqueue matching Service maintenance"
    );
    anyhow::ensure!(
        repository
            .claim_next_due_outbox(i64::MAX / 4, 1_000, "assert")
            .await?
            .is_none(),
        "local finalization emitted an incidental FinalizeBoundPod outbox command"
    );
    Ok(())
}
