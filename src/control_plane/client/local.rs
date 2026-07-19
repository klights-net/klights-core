use async_trait::async_trait;
use klights_leader_api::{
    LeaderOutboxDelivery, OutboxDeliveryError, OutboxDeliveryFuture, OutboxDeliveryRequest,
};
use std::sync::Arc;
use tokio::sync::OnceCell;
use tokio::sync::watch;

use crate::control_plane::client::{
    CacheReadinessFuture, CacheReadinessRequest, LeaderApiClient, LeaderCacheReadiness,
    LeaderNetworkTopologyQuery, LeaderNodeLeaseRenewal, LeaderNodeLifecycleStatus,
    LeaderNodeSubnetAllocation, LeaderPodCleanupIntents, LeaderProjectedServiceAccountToken,
    LeaderResourceCommand, LeaderResourceQuery, LeaderWatch, LeaderWatchError, LeaderWatchFuture,
    NetworkTopologyError, NetworkTopologyFuture, NodeDataplaneQuery, NodeDataplaneResult,
    NodeLeaseRenewalError, NodeLeaseRenewalFuture, NodeLeaseRenewalRequest, NodeLeaseRenewalResult,
    NodeLifecycleStatusError, NodeLifecycleStatusFuture, NodeLifecycleStatusRequest,
    NodeLifecycleStatusResult, NodeSubnetAllocationError, NodeSubnetAllocationFuture,
    NodeSubnetAllocationRequest, NodeSubnetAllocationResult, NodeSubnetQuery, NodeSubnetResult,
    PeerSubnetsQuery, PeerSubnetsResult, PodCleanupIntent, PodCleanupIntentAckRequest,
    PodCleanupIntentError, PodCleanupIntentFuture, PodCleanupIntentListRequest,
    ProjectedServiceAccountTokenError, ProjectedServiceAccountTokenFuture,
    ProjectedServiceAccountTokenRequest, ResourceCommandError, ResourceCommandFuture,
    ResourceCommandRequest, ResourceCommandResult, ResourceGetRequest, ResourceListRequest,
    ResourceListResult, ResourceQueryFuture, WatchRequest, focused_dataplane, focused_node_subnet,
    focused_watch_event, query_error, query_list_result,
};
use crate::controller_dispatcher::ControllerDispatcher;
use crate::datastore::command::StorageCommand;
use crate::datastore::replicated::WriteRejection;
use crate::datastore::sqlite::DatastoreWatchReplaySource;
use crate::datastore::{
    DatastoreBackendWatchStore, DatastoreHandle, PodCleanupIntent as StoredPodCleanupIntent,
    Resource, SnapshotAtRv, WatchReplayAnchorStore, WatchTarget,
};
use crate::kubelet::outbox::OutboxApplyError;
use crate::kubelet::pod_repository::store::PodStore;

#[cfg(test)]
use crate::control_plane::client::{ResourceQueryConsistency, pod_get_request};

/// T6 step 1: builds a `watch::Receiver<bool>` that is permanently true.
///
/// Use cases:
/// - Tests that exercise leader-only write paths (the only role they
///   model) and don't care about the gate.
/// - Boot paths that have already established "this is the leader" before
///   any write originates (e.g. a single-voter seed after
///   `bootstrap_single_voter` succeeds).
///
/// Production code that runs on cp/replica must NOT use this helper —
/// it must subscribe to the bootstrap's real `is_leader_tx` watch so the
/// gate tracks live raft state. A source guard added in T6 step 5 will
/// enforce that.
pub fn always_leader_watch() -> watch::Receiver<bool> {
    let (tx, rx) = watch::channel(true);
    // Keep the sender alive forever so the receiver never observes a
    // sender-dropped closure. `Box::leak` is the simplest way to express
    // "this channel lives for the program's lifetime" without an Arc
    // dance, and it's only invoked from boot/test wiring (never hot).
    Box::leak(Box::new(tx));
    rx
}

pub(crate) fn focused_pod_cleanup_intent(
    intent: StoredPodCleanupIntent,
) -> std::result::Result<PodCleanupIntent, PodCleanupIntentError> {
    let snapshot = Resource::try_from_data(Arc::new(intent.pod_data)).map_err(|error| {
        PodCleanupIntentError::corrupt_intent(format!(
            "cleanup intent Pod snapshot has invalid identity: {error}"
        ))
    })?;
    PodCleanupIntent::try_new(
        intent.node_name,
        intent.namespace,
        intent.pod_name,
        intent.pod_uid,
        intent.reason,
        intent.resource_version,
        intent.created_at_ms,
        snapshot,
    )
}

#[derive(Clone)]
pub struct LocalApiClient {
    db: DatastoreHandle,
    pod_store: Arc<PodStore>,
    raft: crate::datastore::raft::state_machine::N1Raft,
    authoring_node: String,
    containerd_namespace: String,
    node_lease_tracker: Arc<crate::node_lease_tracker::NodeLeaseTracker>,
    /// Set once the leader's `ControllerDispatcher` is constructed (later in
    /// bootstrap than `LocalApiClient`). When present, every successful
    /// outbox apply on a Pod status fires the same Service / workload
    /// reconcile keys that the gRPC `Replication::apply_outbox` handler
    /// fires for remote-worker forwarded writes.
    controller_dispatcher: Arc<OnceCell<Arc<ControllerDispatcher>>>,
    /// T6 step 1 inner gate: every mutation method on this client first
    /// reads `*is_leader_rx.borrow()`. When false (this node is not the
    /// elected raft leader) the call is refused with
    /// `WriteRejection::FollowerWrite`; reads stay allowed. Promotion is
    /// a watch flip — no rewiring needed. The receiver is mandatory in
    /// the constructor so the gate cannot be skipped by accident.
    is_leader_rx: watch::Receiver<bool>,
}

impl LocalApiClient {
    pub fn new(
        db: DatastoreHandle,
        authoring_node: String,
        is_leader_rx: watch::Receiver<bool>,
    ) -> Self {
        Self::new_with_node_lease_tracker_and_containerd_namespace(
            db,
            authoring_node,
            std::env::var("KLIGHTS_CONTAINERD_NAMESPACE").unwrap_or_else(|_| "klights".to_string()),
            Arc::new(crate::node_lease_tracker::NodeLeaseTracker::new()),
            is_leader_rx,
        )
    }

    pub fn new_with_node_lease_tracker(
        db: DatastoreHandle,
        authoring_node: String,
        node_lease_tracker: Arc<crate::node_lease_tracker::NodeLeaseTracker>,
        is_leader_rx: watch::Receiver<bool>,
    ) -> Self {
        Self::new_with_node_lease_tracker_and_containerd_namespace(
            db,
            authoring_node,
            std::env::var("KLIGHTS_CONTAINERD_NAMESPACE").unwrap_or_else(|_| "klights".to_string()),
            node_lease_tracker,
            is_leader_rx,
        )
    }

    pub fn new_with_node_lease_tracker_and_containerd_namespace(
        db: DatastoreHandle,
        authoring_node: String,
        containerd_namespace: String,
        node_lease_tracker: Arc<crate::node_lease_tracker::NodeLeaseTracker>,
        is_leader_rx: watch::Receiver<bool>,
    ) -> Self {
        let pod_store = Arc::new(PodStore::new(db.clone()));
        Self {
            raft: crate::datastore::raft::state_machine::N1Raft::new(db.clone()),
            db,
            pod_store,
            authoring_node,
            containerd_namespace,
            node_lease_tracker,
            controller_dispatcher: Arc::new(OnceCell::new()),
            is_leader_rx,
        }
    }

    /// `OutboxApplyError`-returning equivalent of `check_leader` for
    /// `apply_outbox` paths. Maps the rejection to `Retryable` so the
    /// outbox dispatcher leaves the command in the queue and re-attempts
    /// once leadership flips.
    fn check_leader_outbox(&self) -> std::result::Result<(), OutboxApplyError> {
        if *self.is_leader_rx.borrow() {
            Ok(())
        } else {
            Err(OutboxApplyError::Retryable(
                WriteRejection::FollowerWrite.to_string(),
            ))
        }
    }

    #[cfg(test)]
    pub(crate) async fn deliver_test_outbox(
        &self,
        idempotency_key: &str,
        operation: crate::kubelet::outbox::payload::OutboxOperation,
        payload: bytes::Bytes,
        client_id: &str,
        stream_id: i64,
        stream_seq: i64,
    ) -> std::result::Result<
        klights_leader_api::OutboxDeliveryResult,
        klights_leader_api::OutboxDeliveryError,
    > {
        let request = OutboxDeliveryRequest::try_new(
            idempotency_key,
            operation.try_delivery_operation()?,
            Arc::<[u8]>::from(payload.to_vec()),
            client_id,
            stream_id,
            stream_seq,
        )?;
        self.deliver_outbox(request).await
    }

    /// Wire in the leader's `ControllerDispatcher`. Called from the bootstrap
    /// runtime once the dispatcher has been built. Idempotent: a second call
    /// is silently ignored (OnceCell::set returns Err on repeat).
    pub fn set_controller_dispatcher(&self, dispatcher: Arc<ControllerDispatcher>) {
        let _ = self.controller_dispatcher.set(dispatcher);
    }

    #[cfg(test)]
    pub async fn last_raft_commit_index_for_test(&self) -> i64 {
        self.raft.last_commit_index().await
    }
}

impl LeaderResourceQuery for LocalApiClient {
    fn get_resource(
        &self,
        request: ResourceGetRequest,
    ) -> ResourceQueryFuture<'_, Option<Resource>> {
        Box::pin(async move {
            let leader_fresh = request.consistency()
                == crate::control_plane::client::ResourceQueryConsistency::LeaderFresh;
            let mut leadership_rx = self.is_leader_rx.clone();
            let sampled_is_leader = *leadership_rx.borrow_and_update();
            if leader_fresh && !sampled_is_leader {
                return Err(crate::control_plane::client::ResourceQueryError::retryable(
                    "leader-fresh resource query reached a non-leader local client",
                ));
            }
            let key = request.key();
            let resource = self
                .db
                .get_resource(
                    &key.api_version,
                    &key.kind,
                    key.namespace.as_deref(),
                    &key.name,
                )
                .await
                .map_err(query_error)?;
            if leader_fresh && leadership_rx.has_changed().unwrap_or(true) {
                return Err(crate::control_plane::client::ResourceQueryError::retryable(
                    "leadership changed during local leader-fresh resource query",
                ));
            }
            Ok(resource)
        })
    }

    fn list_resources(
        &self,
        request: ResourceListRequest,
    ) -> ResourceQueryFuture<'_, ResourceListResult> {
        Box::pin(async move {
            let leader_fresh = request.consistency()
                == crate::control_plane::client::ResourceQueryConsistency::LeaderFresh;
            let mut leadership_rx = self.is_leader_rx.clone();
            let sampled_is_leader = *leadership_rx.borrow_and_update();
            if leader_fresh && !sampled_is_leader {
                return Err(crate::control_plane::client::ResourceQueryError::retryable(
                    "leader-fresh resource query reached a non-leader local client",
                ));
            }
            let list = self
                .db
                .list_resources(
                    request.api_version(),
                    request.kind(),
                    request.namespace(),
                    crate::datastore::ResourceListQuery::new(
                        request.label_selector(),
                        request.field_selector(),
                        request.limit(),
                        request.continue_token(),
                    ),
                )
                .await
                .map_err(query_error)?;
            if leader_fresh && leadership_rx.has_changed().unwrap_or(true) {
                return Err(crate::control_plane::client::ResourceQueryError::retryable(
                    "leadership changed during local leader-fresh resource query",
                ));
            }
            query_list_result(list)
        })
    }
}

pub(crate) async fn submit_resource_command_to_store(
    db: &DatastoreHandle,
    request: ResourceCommandRequest,
) -> std::result::Result<ResourceCommandResult, ResourceCommandError> {
    use crate::datastore::ResourcePatchRequest;
    use crate::datastore::command::StorageCommand;

    let result = match request.into_command() {
        StorageCommand::CreateResource {
            api_version,
            kind,
            namespace,
            name,
            data,
        } => ResourceCommandResult::Resource(
            db.create_resource(&api_version, &kind, namespace.as_deref(), &name, data)
                .await
                .map_err(resource_command_store_error)?,
        ),
        StorageCommand::UpdateResource {
            api_version,
            kind,
            namespace,
            name,
            data,
            expected_rv: _,
            preconditions,
        } => ResourceCommandResult::Resource(
            db.update_resource_with_preconditions(
                &api_version,
                &kind,
                namespace.as_deref(),
                &name,
                data,
                preconditions,
            )
            .await
            .map_err(resource_command_store_error)?,
        ),
        StorageCommand::PatchResource {
            api_version,
            kind,
            namespace,
            name,
            patch_kind,
            patch,
            preconditions,
            strict_resource_version,
        } => ResourceCommandResult::Resource(
            db.patch_resource_latest_with_preconditions(
                &api_version,
                &kind,
                namespace.as_deref(),
                &name,
                ResourcePatchRequest {
                    patch_kind,
                    patch,
                    preconditions,
                    strict_resource_version,
                },
            )
            .await
            .map_err(resource_command_store_error)?
            .ok_or_else(|| ResourceCommandError::NotFound {
                message: format!("{api_version}/{kind}/{name} not found"),
            })?,
        ),
        StorageCommand::DeleteResource {
            api_version,
            kind,
            namespace,
            name,
            preconditions,
        } => {
            let resource_version = db
                .delete_resource_with_preconditions_observed_rv(
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    preconditions,
                )
                .await
                .map_err(resource_command_store_error)?;
            ResourceCommandResult::Ack { resource_version }
        }
        StorageCommand::DeleteResourceWithTombstone {
            api_version,
            kind,
            namespace,
            name,
            preconditions,
            grace_seconds,
        } => ResourceCommandResult::Resource(
            db.delete_resource_without_watch_with_tombstone(
                &api_version,
                &kind,
                namespace.as_deref(),
                &name,
                preconditions,
                grace_seconds,
            )
            .await
            .map_err(resource_command_store_error)?,
        ),
        command => {
            return Err(ResourceCommandError::UnsupportedCommand {
                command: command.variant_name(),
            });
        }
    };
    Ok(result)
}

fn resource_command_store_error(error: anyhow::Error) -> ResourceCommandError {
    if let Some(error) = error.downcast_ref::<crate::datastore::errors::DatastoreError>() {
        return match error {
            crate::datastore::errors::DatastoreError::Conflict { message } => {
                ResourceCommandError::Conflict {
                    message: message.clone(),
                }
            }
            crate::datastore::errors::DatastoreError::NotFound { message } => {
                ResourceCommandError::NotFound {
                    message: message.clone(),
                }
            }
        };
    }
    if crate::datastore::errors::is_conflict_error(&error) {
        return ResourceCommandError::Conflict {
            message: error.to_string(),
        };
    }
    if format!("{error:#}")
        .to_ascii_lowercase()
        .contains("not found")
    {
        return ResourceCommandError::NotFound {
            message: error.to_string(),
        };
    }
    ResourceCommandError::submission_failed(error.to_string())
}

impl LeaderResourceCommand for LocalApiClient {
    fn submit_resource_command(
        &self,
        request: ResourceCommandRequest,
    ) -> ResourceCommandFuture<'_, ResourceCommandResult> {
        Box::pin(async move {
            if !*self.is_leader_rx.borrow() {
                return Err(ResourceCommandError::NotLeader);
            }
            submit_resource_command_to_store(&self.db, request).await
        })
    }
}

impl LeaderNodeLeaseRenewal for LocalApiClient {
    fn renew_node_lease(
        &self,
        request: NodeLeaseRenewalRequest,
    ) -> NodeLeaseRenewalFuture<'_, NodeLeaseRenewalResult> {
        Box::pin(async move {
            if !*self.is_leader_rx.borrow() {
                return Err(NodeLeaseRenewalError::NotLeader);
            }
            let (node_name, renew_time, lease_duration_seconds) = request.into_parts();
            self.node_lease_tracker
                .record_from_lease_object(
                    &node_name,
                    &serde_json::json!({
                        "metadata": {
                            "name": node_name,
                            "namespace": "kube-node-lease"
                        },
                        "spec": {
                            "holderIdentity": node_name,
                            "leaseDurationSeconds": lease_duration_seconds,
                            "renewTime": renew_time
                        }
                    }),
                )
                .await
                .map_err(|error| NodeLeaseRenewalError::InvalidRequest {
                    field: "lease.renew_time",
                    message: error.to_string(),
                })?;
            Ok(NodeLeaseRenewalResult::Renewed)
        })
    }
}

impl LeaderNodeLifecycleStatus for LocalApiClient {
    fn submit_node_lifecycle_status(
        &self,
        request: NodeLifecycleStatusRequest,
    ) -> NodeLifecycleStatusFuture<'_, NodeLifecycleStatusResult> {
        Box::pin(async move {
            if !*self.is_leader_rx.borrow() {
                return Err(NodeLifecycleStatusError::NotLeader);
            }
            let get = crate::control_plane::client::node_get_request(
                request.node_name(),
                crate::control_plane::client::ResourceQueryConsistency::LeaderFresh,
            )
            .map_err(|error| NodeLifecycleStatusError::apply_failed(error.to_string()))?;
            let current = LeaderResourceQuery::get_resource(self, get)
                .await
                .map_err(|error| NodeLifecycleStatusError::apply_failed(error.to_string()))?
                .ok_or(NodeLifecycleStatusError::NotFound)?;
            if current.uid != request.node_uid() {
                return Err(NodeLifecycleStatusError::UidMismatch);
            }
            if current.resource_version != request.resource_version() {
                return Err(NodeLifecycleStatusError::conflict(format!(
                    "Node resourceVersion changed from {} to {}",
                    request.resource_version(),
                    current.resource_version
                )));
            }
            let command = request.into_command();
            let StorageCommand::UpdateStatus {
                api_version,
                kind,
                namespace,
                name,
                status,
                preconditions,
                ..
            } = command
            else {
                unreachable!("NodeLifecycleStatusRequest admits only UpdateStatus")
            };
            let resource = self
                .db
                .update_status_only_with_preconditions(
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    status,
                    preconditions,
                )
                .await
                .map_err(node_lifecycle_status_store_error)?;
            Ok(NodeLifecycleStatusResult::Updated {
                resource_version: resource.resource_version,
            })
        })
    }
}

fn node_lifecycle_status_store_error(error: anyhow::Error) -> NodeLifecycleStatusError {
    let message = format!("{error:#}");
    let lower = message.to_ascii_lowercase();
    if lower.contains("uid mismatch") {
        NodeLifecycleStatusError::UidMismatch
    } else if lower.contains("not found") || lower.contains("query returned no rows") {
        NodeLifecycleStatusError::NotFound
    } else if lower.contains("conflict") || lower.contains("precondition") {
        NodeLifecycleStatusError::conflict(message)
    } else if lower.contains("not raft leader") || lower.contains("follower") {
        NodeLifecycleStatusError::NotLeader
    } else {
        NodeLifecycleStatusError::apply_failed(message)
    }
}

impl LeaderWatch for LocalApiClient {
    fn watch_resources(&self, req: WatchRequest) -> LeaderWatchFuture<'_> {
        Box::pin(async move {
            let topic = crate::watch::WatchTopic::new(req.api_version(), req.kind());
            let legacy_start_rv = req.start_resource_version().unwrap_or(0).max(0);
            let requested_position = req.start_watch_replay_position();
            let has_selector = super::watch_request_has_selector(&req);
            let watch_anchor = DatastoreBackendWatchStore::new(self.db.clone());
            // Capture the durable handoff before any selector baseline await. No
            // await occurs between final establishment and signal subscription;
            // replay closes every interval beginning at this early anchor.
            let early_anchor = if requested_position.is_none() {
                Some(
                    watch_anchor
                        .current_watch_replay_position()
                        .await
                        .map_err(|error| LeaderWatchError::transport(error.to_string()))?,
                )
            } else {
                None
            };
            let mut selector_membership = crate::watch::SelectorMembership::default();
            let mut current_baseline_position = None;
            if has_selector {
                let snapshot_position = requested_position.or_else(|| {
                    (legacy_start_rv > 0).then(|| {
                    crate::datastore::WatchReplayPosition::from_resource_version_through_event_id(
                        legacy_start_rv,
                        early_anchor.unwrap_or_default().event_id,
                    )
                })
                });
                let query = || {
                    crate::datastore::ResourceListQuery::new(
                        req.label_selector(),
                        req.field_selector(),
                        None,
                        None,
                    )
                };
                let baseline = if let Some(snapshot_position) = snapshot_position {
                    match watch_anchor
                        .snapshot_resources_at_position(
                            std::slice::from_ref(&watch_target_for_request(&req)),
                            req.label_selector(),
                            req.field_selector(),
                            snapshot_position,
                        )
                        .await
                        .map_err(|error| LeaderWatchError::transport(error.to_string()))?
                    {
                        SnapshotAtRv::List(list) => list,
                        SnapshotAtRv::Current => self
                            .db
                            .list_resources(req.api_version(), req.kind(), req.namespace(), query())
                            .await
                            .map_err(|error| LeaderWatchError::transport(error.to_string()))?,
                        SnapshotAtRv::Expired => {
                            return Err(LeaderWatchError::ReplayExpired {
                                accepted_resource_version: snapshot_position.resource_version,
                            });
                        }
                    }
                } else {
                    self.db
                        .list_resources(req.api_version(), req.kind(), req.namespace(), query())
                        .await
                        .map_err(|error| LeaderWatchError::transport(error.to_string()))?
                };
                selector_membership.replace_from_resources(&baseline.items);
                current_baseline_position = baseline.watch_replay_position;
            }
            let replay_position = if let Some(position) = requested_position {
                position
            } else if legacy_start_rv > 0 {
                crate::datastore::WatchReplayPosition::from_resource_version_through_event_id(
                    legacy_start_rv,
                    early_anchor.unwrap_or_default().event_id,
                )
            } else if let Some(position) = current_baseline_position {
                position
            } else {
                early_anchor.unwrap_or_default()
            };
            let start_rv = legacy_start_rv.max(replay_position.resource_version);
            let signal_rx = self.db.subscribe_watch_signals(topic.clone());
            let replay_source = DatastoreWatchReplaySource::new(
                std::sync::Arc::new(crate::datastore::DatastoreBackendWatchStore::new(
                    self.db.clone(),
                )),
                vec![watch_target_for_request(&req)],
            );
            let scope = watch_delivery_scope_for_request(&req);
            let stream = async_stream::stream! {
                let mut cursor = crate::watch::SignalWatchCursor::new_many_at_position(
                    signal_rx,
                    replay_source,
                    vec![topic],
                    scope,
                    start_rv,
                    replay_position,
                    crate::watch::WindowPolicy::default_watch_delivery(),
                );
                if let Err(err) = cursor.prime_replay_or_expired().await
                {
                    yield Err(local_watch_cursor_error(err, cursor.accepted_rv()));
                    return;
                }
                loop {
                    match cursor.next_event().await {
                        Ok(event) => {
                            let matches = super::watch_request_matches_event(&req, &event);
                            let event = if has_selector {
                                selector_membership.transition(event, matches)
                            } else {
                                matches.then_some(event)
                            };
                            if let Some(event) = event {
                                yield focused_watch_event(
                                    event,
                                    Some(cursor.processed_position()),
                                ).and_then(|event| {
                                    event.validate_for(&req)?;
                                    Ok(event)
                                });
                            }
                        }
                        Err(crate::watch::WatchCursorError::Closed) => {
                            yield Err(LeaderWatchError::unavailable(
                                "local watch signal channel closed",
                            ));
                            return;
                        }
                        Err(err) => {
                            yield Err(local_watch_cursor_error(err, cursor.accepted_rv()));
                            return;
                        }
                    }
                }
            };
            Ok(Box::pin(stream) as crate::control_plane::client::WatchStream)
        })
    }
}

impl LeaderCacheReadiness for LocalApiClient {
    fn wait_cache_ready(&self, _scope: CacheReadinessRequest) -> CacheReadinessFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

impl LeaderProjectedServiceAccountToken for LocalApiClient {
    fn issue_projected_service_account_token(
        &self,
        request: ProjectedServiceAccountTokenRequest,
    ) -> ProjectedServiceAccountTokenFuture<'_> {
        Box::pin(async move {
            if !*self.is_leader_rx.borrow() {
                return Err(ProjectedServiceAccountTokenError::NotLeader);
            }
            if request.bound_node_name() != self.authoring_node {
                return Err(ProjectedServiceAccountTokenError::Unauthorized);
            }
            let signing_key_pem =
                crate::auth::read_service_account_signing_key_async(&self.containerd_namespace)
                    .await
                    .map_err(|error| {
                        ProjectedServiceAccountTokenError::signing_failed(format!(
                            "ServiceAccount signing key for {} is unavailable: {error}",
                            self.containerd_namespace
                        ))
                    })?;
            crate::control_plane::service_account_tokens::issue_projected_service_account_token(
                self.db.as_ref(),
                self.pod_store.as_ref(),
                &signing_key_pem,
                &request,
            )
            .await
        })
    }
}

impl LeaderPodCleanupIntents for LocalApiClient {
    fn list_pod_cleanup_intents(
        &self,
        request: PodCleanupIntentListRequest,
    ) -> PodCleanupIntentFuture<'_, Vec<PodCleanupIntent>> {
        Box::pin(async move {
            if !*self.is_leader_rx.borrow() {
                return Err(PodCleanupIntentError::NotLeader);
            }
            if request.node_name() != self.authoring_node {
                return Err(PodCleanupIntentError::Unauthorized);
            }
            self.db
                .list_pod_cleanup_intents_for_node(request.node_name())
                .await
                .map_err(|error| PodCleanupIntentError::unavailable(error.to_string()))?
                .into_iter()
                .map(focused_pod_cleanup_intent)
                .collect()
        })
    }

    fn acknowledge_pod_cleanup_intent(
        &self,
        request: PodCleanupIntentAckRequest,
    ) -> PodCleanupIntentFuture<'_, ()> {
        Box::pin(async move {
            if !*self.is_leader_rx.borrow() {
                return Err(PodCleanupIntentError::NotLeader);
            }
            if request.node_name() != self.authoring_node {
                return Err(PodCleanupIntentError::Unauthorized);
            }
            let (node_name, namespace, pod_name, pod_uid, reason) = request.into_parts();
            self.db
                .delete_pod_cleanup_intent(&node_name, &namespace, &pod_name, &pod_uid, &reason)
                .await
                .map_err(|error| PodCleanupIntentError::unavailable(error.to_string()))
        })
    }
}

impl LeaderNodeSubnetAllocation for LocalApiClient {
    fn allocate_node_subnet(
        &self,
        request: NodeSubnetAllocationRequest,
    ) -> NodeSubnetAllocationFuture<'_, NodeSubnetAllocationResult> {
        Box::pin(async move {
            if !*self.is_leader_rx.borrow() {
                return Err(NodeSubnetAllocationError::NotLeader);
            }
            let (node_name, cluster_cidr, node_ip) = request.into_parts();
            let subnet = self
                .db
                .allocate_node_subnet(&node_name, &cluster_cidr, &node_ip.to_string())
                .await
                .map_err(|error| {
                    let message = error.to_string();
                    if super::node_subnet_allocation_is_exhausted(&message) {
                        NodeSubnetAllocationError::exhausted(cluster_cidr.clone())
                    } else if message.to_ascii_lowercase().contains("conflict") {
                        NodeSubnetAllocationError::conflict(message)
                    } else {
                        NodeSubnetAllocationError::allocation_failed(message)
                    }
                })?;
            let subnet = focused_node_subnet(subnet)
                .map_err(|error| NodeSubnetAllocationError::corrupt_response(error.to_string()))?;
            NodeSubnetAllocationResult::try_from_wire(&node_name, Some(subnet))
        })
    }
}

impl LeaderNetworkTopologyQuery for LocalApiClient {
    fn get_node_subnet(
        &self,
        request: NodeSubnetQuery,
    ) -> NetworkTopologyFuture<'_, NodeSubnetResult> {
        Box::pin(async move {
            if !*self.is_leader_rx.borrow() {
                return Err(NetworkTopologyError::NotLeader);
            }
            let node_name = request.into_node_name();
            let subnet = self
                .db
                .get_node_subnet(&node_name)
                .await
                .map_err(|error| NetworkTopologyError::query_failed(error.to_string()))?
                .map(focused_node_subnet)
                .transpose()?;
            NodeSubnetResult::try_from_wire(&node_name, subnet.is_some(), subnet)
        })
    }

    fn list_peer_subnets(
        &self,
        request: PeerSubnetsQuery,
    ) -> NetworkTopologyFuture<'_, PeerSubnetsResult> {
        Box::pin(async move {
            if !*self.is_leader_rx.borrow() {
                return Err(NetworkTopologyError::NotLeader);
            }
            let node_name = request.into_node_name();
            let subnets = self
                .db
                .list_peer_subnets(&node_name)
                .await
                .map_err(|error| NetworkTopologyError::query_failed(error.to_string()))?
                .into_iter()
                .map(focused_node_subnet)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            PeerSubnetsResult::try_new(&node_name, subnets)
        })
    }

    fn get_node_dataplane(
        &self,
        request: NodeDataplaneQuery,
    ) -> NetworkTopologyFuture<'_, NodeDataplaneResult> {
        Box::pin(async move {
            if !*self.is_leader_rx.borrow() {
                return Err(NetworkTopologyError::NotLeader);
            }
            let node_name = request.into_node_name();
            let metadata = self
                .db
                .get_node_dataplane(&node_name)
                .await
                .map_err(|error| NetworkTopologyError::query_failed(error.to_string()))?
                .map(focused_dataplane)
                .transpose()?;
            NodeDataplaneResult::try_from_wire(&node_name, metadata.is_some(), metadata)
        })
    }
}

#[async_trait]
impl LeaderApiClient for LocalApiClient {}

impl LeaderOutboxDelivery for LocalApiClient {
    fn deliver_outbox(&self, request: OutboxDeliveryRequest) -> OutboxDeliveryFuture<'_> {
        Box::pin(async move {
            self.check_leader_outbox()?;
            let (idempotency_key, operation, payload, client_id, stream_id, stream_seq) =
                request.into_parts();
            let watermark = crate::control_plane::client::apply::outbox_stream_watermark(
                &client_id, stream_id, stream_seq,
            );
            let decoded = match crate::kubelet::outbox::payload::OutboxPayload::decode_protobuf(
                payload.as_ref(),
            ) {
                Ok(decoded) => decoded,
                Err(error) => {
                    let terminal =
                        OutboxDeliveryError::invalid("delivery.payload", error.to_string());
                    crate::control_plane::client::apply::consume_terminal_outbox_sequence(
                        self.db.as_ref(),
                        &idempotency_key,
                        operation.into(),
                        &self.authoring_node,
                        watermark.clone(),
                    )
                    .await?;
                    return Err(terminal);
                }
            };
            if let Err(error) = crate::control_plane::client::apply::authorize_outbox_command(
                operation,
                &decoded.command,
                &self.authoring_node,
            ) {
                crate::control_plane::client::apply::consume_terminal_outbox_sequence(
                    self.db.as_ref(),
                    &idempotency_key,
                    operation.into(),
                    &self.authoring_node,
                    watermark.clone(),
                )
                .await?;
                return Err(error);
            }
            if operation == klights_leader_api::OutboxDeliveryOperation::PodMetadata
                && let Err(error) =
                    crate::control_plane::client::apply::authorize_live_pod_metadata_command(
                        self.db.as_ref(),
                        &decoded.command,
                        &self.authoring_node,
                    )
                    .await
            {
                if error.is_terminal() {
                    crate::control_plane::client::apply::consume_terminal_outbox_sequence(
                        self.db.as_ref(),
                        &idempotency_key,
                        operation.into(),
                        &self.authoring_node,
                        watermark.clone(),
                    )
                    .await?;
                }
                return Err(error);
            }
            if operation == klights_leader_api::OutboxDeliveryOperation::NodeStatus
                && let Err(error) =
                    klights_leader_api::NodeSelfStatusRequest::validate_command(&decoded.command)
                        .map_err(|error| match error {
                            klights_leader_api::NodeSelfStatusError::InvalidRequest {
                                field,
                                message,
                            } => OutboxDeliveryError::invalid(field, message),
                            other => {
                                OutboxDeliveryError::invalid("delivery.payload", other.to_string())
                            }
                        })
            {
                crate::control_plane::client::apply::consume_terminal_outbox_sequence(
                    self.db.as_ref(),
                    &idempotency_key,
                    operation.into(),
                    &self.authoring_node,
                    watermark.clone(),
                )
                .await?;
                return Err(error);
            }
            let outcome = self
                .raft
                .propose_outbox_with_watermark(
                    &idempotency_key,
                    operation.into(),
                    bytes::Bytes::from_owner(payload),
                    &self.authoring_node,
                    watermark,
                )
                .await?;
            if let Some(command) = outcome.command.as_ref() {
                crate::control_plane::client::pod_status_side_effects::handle_applied_pod_side_effects(
                    self.controller_dispatcher.get(),
                    command,
                    outcome.resource.as_ref(),
                    self.db.as_ref(),
                )
                .await;
            }
            Ok(outcome.result)
        })
    }
}

fn watch_target_for_request(req: &WatchRequest) -> WatchTarget {
    if let Some(namespace) = req.namespace() {
        return WatchTarget::namespaced_in_namespace(req.api_version(), req.kind(), namespace);
    }
    if crate::datastore::sqlite::scope::is_namespaced(req.kind()) {
        WatchTarget::namespaced(req.api_version(), req.kind())
    } else {
        WatchTarget::cluster(req.api_version(), req.kind())
    }
}

fn watch_delivery_scope_for_request(req: &WatchRequest) -> crate::watch::WatchDeliveryScope {
    if let Some(namespace) = req.namespace() {
        return crate::watch::WatchDeliveryScope::Namespaced(namespace.to_string());
    }
    if crate::datastore::sqlite::scope::is_namespaced(req.kind()) {
        crate::watch::WatchDeliveryScope::NamespacedAll
    } else {
        crate::watch::WatchDeliveryScope::Cluster
    }
}

fn local_watch_cursor_error(
    err: crate::watch::WatchCursorError,
    accepted_rv: i64,
) -> LeaderWatchError {
    match err {
        crate::watch::WatchCursorError::Expired => LeaderWatchError::ReplayExpired {
            accepted_resource_version: accepted_rv,
        },
        crate::watch::WatchCursorError::Replay(err) => {
            LeaderWatchError::transport(format!("local watch replay failed: {err}"))
        }
        crate::watch::WatchCursorError::Closed => {
            LeaderWatchError::unavailable("local watch signal channel closed")
        }
    }
}

#[cfg(test)]
mod inner_gate_tests {
    //! T6 step 1: `LocalApiClient` inner write gate.
    //!
    //! Every mutation method must consult `is_leader_rx` and refuse with
    //! `WriteRejection::FollowerWrite` (or the OutboxApplyError equivalent)
    //! when this node is not the elected raft leader. Reads stay allowed.
    //! Promotion is a watch flip — the same instance starts accepting
    //! writes the moment the receiver observes `true`.

    use super::*;
    use crate::datastore::ResourcePreconditions;
    use crate::datastore::command::StorageCommand;
    use crate::datastore::{ReplicatedCreateOptions, ResourceListQuery};
    use crate::kubelet::outbox::OutboxApplyError;
    use crate::kubelet::outbox::payload::{OutboxOperation, OutboxPayload};
    use futures::StreamExt as _;
    use klights_leader_api::{
        LeaderResourceCommand, ResourceCommandError, ResourceCommandRequest, ResourceCommandResult,
        ResourceQueryError, WatchEventType,
    };
    use klights_types::ResourceKey;

    fn pod_status_payload() -> bytes::Bytes {
        let command = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web".to_string(),
            status: serde_json::json!({"phase": "Running"}),
            expected_rv: None,
            preconditions: ResourcePreconditions {
                uid: Some("uid-1".to_string()),
                resource_version: None,
            },
            observed_status_stamp: None,
        };
        bytes::Bytes::from(
            OutboxPayload::from_command(command)
                .encode_protobuf()
                .expect("encode pod status payload"),
        )
    }

    async fn make_pod(db: &crate::datastore::sqlite::Datastore) {
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "web",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"namespace": "default", "name": "web", "uid": "uid-1"},
                "spec": {"containers": [{"name": "app", "image": "nginx"}]}
            }),
        )
        .await
        .expect("create pod");
    }

    /// Mutation gate: every `LeaderApiClient` mutation refuses when
    /// `is_leader_rx=false`. Asserts the gate fires before any datastore
    /// work happens.
    #[tokio::test]
    async fn local_api_client_refuses_apply_outbox_when_not_leader() {
        let db = crate::datastore::test_support::in_memory().await;
        make_pod(&db).await;
        let (_tx, rx) = watch::channel(false);
        let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);

        let err = client
            .deliver_test_outbox(
                "idem-1",
                OutboxOperation::PodStatus,
                pod_status_payload(),
                "client",
                1,
                1,
            )
            .await
            .expect_err("non-leader apply_outbox must be rejected");
        match err {
            OutboxApplyError::Retryable(msg) => {
                assert!(
                    msg.contains("follower"),
                    "expected FollowerWrite message, got: {msg}"
                );
            }
            other => panic!("expected Retryable(follower-write), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn outbox_terminal_decision_local_invalid_and_malformed_rows_consume_in_order() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        db.create_resource(
            "v1",
            "Node",
            None,
            "node-a",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "node-a", "uid": "node-uid-a"},
                "status": {"conditions": []}
            }),
        )
        .await
        .expect("create local Node");
        let client = LocalApiClient::new(
            db.clone(),
            "node-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        );
        let command = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: "node-a".to_string(),
            status: serde_json::json!({"conditions": []}),
            expected_rv: Some(7),
            preconditions: ResourcePreconditions {
                uid: Some("node-uid-a".to_string()),
                resource_version: Some(7),
            },
            observed_status_stamp: None,
        };
        let payload = bytes::Bytes::from(
            OutboxPayload::from_command(command)
                .encode_protobuf()
                .expect("encode invalid worker Node status"),
        );

        let error = client
            .deliver_test_outbox(
                "invalid-node-status-rv",
                OutboxOperation::NodeStatus,
                payload,
                "client",
                1,
                1,
            )
            .await
            .expect_err("local focused delivery must enforce NodeSelfStatusRequest validation");
        assert!(matches!(
            error,
            klights_leader_api::OutboxDeliveryError::InvalidRequest {
                field: "status.resource_version",
                ..
            }
        ));
        assert_eq!(
            db.list_outbox_stream_watermarks().await.unwrap()[0].stream_seq,
            1,
            "local authorization rejection must durably consume sequence one"
        );

        let valid_status = || {
            bytes::Bytes::from(
                OutboxPayload::from_command(StorageCommand::UpdateStatus {
                    api_version: "v1".to_string(),
                    kind: "Node".to_string(),
                    namespace: None,
                    name: "node-a".to_string(),
                    status: serde_json::json!({"conditions": []}),
                    expected_rv: None,
                    preconditions: ResourcePreconditions::uid("node-uid-a"),
                    observed_status_stamp: None,
                })
                .encode_protobuf()
                .expect("encode valid local Node status"),
            )
        };
        client
            .deliver_test_outbox(
                "valid-node-status-after-invalid",
                OutboxOperation::NodeStatus,
                valid_status(),
                "client",
                1,
                2,
            )
            .await
            .expect("sequence two applies after terminal authorization decision");

        let malformed = client
            .deliver_test_outbox(
                "malformed-node-status",
                OutboxOperation::NodeStatus,
                bytes::Bytes::from_static(&[0xff, 0x00, 0x81]),
                "client",
                1,
                3,
            )
            .await
            .expect_err("malformed delivery stays fail-closed");
        assert!(malformed.is_terminal());
        assert_eq!(
            db.list_outbox_stream_watermarks().await.unwrap()[0].stream_seq,
            3,
            "malformed sequence must receive a durable terminal decision"
        );
        client
            .deliver_test_outbox(
                "valid-node-status-after-malformed",
                OutboxOperation::NodeStatus,
                valid_status(),
                "client",
                1,
                4,
            )
            .await
            .expect("sequence four applies after malformed terminal decision");
    }

    #[tokio::test]
    async fn local_resource_command_is_leader_gated_before_datastore_mutation() {
        let db = crate::datastore::test_support::in_memory().await;
        let (_tx, rx) = watch::channel(false);
        let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);
        let request = ResourceCommandRequest::try_new(StorageCommand::CreateResource {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "settings".to_string(),
            data: serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "default", "name": "settings"}
            }),
        })
        .expect("valid command");

        let error = LeaderResourceCommand::submit_resource_command(&client, request)
            .await
            .expect_err("a follower must reject resource commands");
        assert_eq!(error, ResourceCommandError::NotLeader);
        assert!(
            client
                .db
                .get_resource("v1", "ConfigMap", Some("default"), "settings")
                .await
                .expect("read after rejection")
                .is_none()
        );
    }

    #[tokio::test]
    async fn local_resource_command_returns_the_created_resource() {
        let db = crate::datastore::test_support::in_memory().await;
        let (_tx, rx) = watch::channel(true);
        let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);
        let request = ResourceCommandRequest::try_new(StorageCommand::CreateResource {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "settings".to_string(),
            data: serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "default", "name": "settings"}
            }),
        })
        .expect("valid command");

        let result = LeaderResourceCommand::submit_resource_command(&client, request)
            .await
            .expect("leader command");
        assert!(
            matches!(result, ResourceCommandResult::Resource(resource) if resource.name == "settings")
        );
    }

    #[tokio::test]
    async fn local_resource_command_maps_duplicate_create_to_conflict() {
        let db = crate::datastore::test_support::in_memory().await;
        let (_tx, rx) = watch::channel(true);
        let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);
        let command = StorageCommand::CreateResource {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "settings".to_string(),
            data: serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "default", "name": "settings"}
            }),
        };
        LeaderResourceCommand::submit_resource_command(
            &client,
            ResourceCommandRequest::try_new(command.clone()).expect("valid command"),
        )
        .await
        .expect("first create");
        let error = LeaderResourceCommand::submit_resource_command(
            &client,
            ResourceCommandRequest::try_new(command).expect("valid command"),
        )
        .await
        .expect_err("duplicate create must conflict");
        assert!(matches!(error, ResourceCommandError::Conflict { .. }));
    }

    /// `allocate_node_subnet` writes cluster state and must be gated.
    #[tokio::test]
    async fn local_api_client_refuses_allocate_node_subnet_when_not_leader() {
        let db = crate::datastore::test_support::in_memory().await;
        let (_tx, rx) = watch::channel(false);
        let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);

        let request = klights_leader_api::NodeSubnetAllocationRequest::try_new(
            "node-a",
            "10.50.0.0/16",
            "10.99.0.10",
        )
        .expect("valid allocation request");
        let err =
            klights_leader_api::LeaderNodeSubnetAllocation::allocate_node_subnet(&client, request)
                .await
                .expect_err("non-leader subnet allocation must be rejected");
        assert!(
            matches!(
                err,
                klights_leader_api::NodeSubnetAllocationError::NotLeader
            ),
            "expected typed NotLeader, got: {err}"
        );
    }

    #[tokio::test]
    async fn local_api_client_maps_subnet_exhaustion_to_typed_error() {
        let db = crate::datastore::test_support::in_memory().await;
        let (_tx, rx) = watch::channel(true);
        let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);

        for node_name in ["node-a", "node-b"] {
            let request = klights_leader_api::NodeSubnetAllocationRequest::try_new(
                node_name,
                "10.50.0.0/24",
                "10.99.0.10",
            )
            .expect("valid allocation request");
            let result = klights_leader_api::LeaderNodeSubnetAllocation::allocate_node_subnet(
                &client, request,
            )
            .await;
            if node_name == "node-a" {
                result.expect("the only /24 must be allocated");
            } else {
                assert!(
                    matches!(
                        result,
                        Err(klights_leader_api::NodeSubnetAllocationError::Exhausted { .. })
                    ),
                    "the second allocation must report typed exhaustion, got {result:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn local_api_client_refuses_network_topology_query_when_not_leader() {
        let db = crate::datastore::test_support::in_memory().await;
        let (_tx, rx) = watch::channel(false);
        let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);
        let request =
            klights_leader_api::NodeSubnetQuery::try_new("node-a").expect("valid topology query");

        let err = klights_leader_api::LeaderNetworkTopologyQuery::get_node_subnet(&client, request)
            .await
            .expect_err("non-leader topology query must fail closed");
        assert!(matches!(
            err,
            klights_leader_api::NetworkTopologyError::NotLeader
        ));
    }

    /// Cached reads may use follower-applied state, but LeaderFresh must not.
    #[tokio::test]
    async fn local_api_client_allows_reads_when_not_leader() {
        let db = crate::datastore::test_support::in_memory().await;
        make_pod(&db).await;
        let (_tx, rx) = watch::channel(false);
        let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);

        let key = ResourceKey {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web".to_string(),
        };
        assert!(
            client
                .get_resource(
                    ResourceGetRequest::try_new(key.clone(), ResourceQueryConsistency::Cached)
                        .expect("valid Pod request"),
                )
                .await
                .expect("read allowed")
                .is_some(),
            "non-leader get_resource must succeed"
        );
        assert!(
            client
                .get_resource(
                    pod_get_request("default", "web", ResourceQueryConsistency::Cached)
                        .expect("valid Pod request"),
                )
                .await
                .expect("read allowed")
                .is_some(),
            "non-leader get_pod must succeed"
        );
        let listed = client
            .list_resources(
                ResourceListRequest::try_new(
                    "v1",
                    "Pod",
                    Some("default".to_string()),
                    None,
                    None,
                    None,
                    None,
                    ResourceQueryConsistency::Cached,
                )
                .expect("valid Pod list request"),
            )
            .await
            .expect("list allowed");
        assert_eq!(
            listed.items().len(),
            1,
            "non-leader list_resources must succeed"
        );
        assert!(matches!(
            client
                .get_resource(
                    ResourceGetRequest::try_new(key, ResourceQueryConsistency::LeaderFresh)
                        .expect("valid fresh Pod request"),
                )
                .await,
            Err(ResourceQueryError::Retryable { .. })
        ));
        assert!(matches!(
            client
                .list_resources(
                    ResourceListRequest::try_new(
                        "v1",
                        "Pod",
                        Some("default".to_string()),
                        None,
                        None,
                        None,
                        None,
                        ResourceQueryConsistency::LeaderFresh,
                    )
                    .expect("valid fresh Pod list request"),
                )
                .await,
            Err(ResourceQueryError::Retryable { .. })
        ));
    }

    #[tokio::test]
    async fn local_selector_watch_synthesizes_deleted_when_pod_leaves_node() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        let pod = db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "moving",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"namespace": "default", "name": "moving", "uid": "uid-moving"},
                    "spec": {"nodeName": "node-a", "containers": [{"name": "app", "image": "pause"}]}
                }),
            )
            .await
            .unwrap();
        let (_tx, rx) = watch::channel(true);
        let client = LocalApiClient::new(db.clone(), "node-a".to_string(), rx);
        let mut stream = client
            .watch_resources(
                WatchRequest::try_new(
                    "v1",
                    "Pod",
                    None,
                    None,
                    Some("spec.nodeName=node-a".to_string()),
                    None,
                    None,
                )
                .expect("valid Pod watch"),
            )
            .await
            .unwrap();

        let mut moved = (*pod.data).clone();
        moved["spec"]["nodeName"] = serde_json::Value::String("node-b".to_string());
        db.update_resource(
            "v1",
            "Pod",
            Some("default"),
            "moving",
            moved,
            pod.resource_version,
        )
        .await
        .unwrap();

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .expect("leave transition should arrive")
            .expect("stream should remain open")
            .expect("event should decode");
        assert_eq!(event.event_type(), WatchEventType::Deleted);
        assert_eq!(event.resource().data["metadata"]["name"], "moving");
    }

    #[tokio::test]
    async fn exact_position_selector_watch_replays_late_lower_rv_leave_as_deleted() {
        let db = crate::datastore::test_support::in_memory().await;
        let selected = serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "namespace": "default",
                "name": "selected",
                "uid": "uid-selected",
                "labels": {"track": "yes"}
            }
        });
        db.apply_replicated_create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "selected",
            selected.clone(),
            ReplicatedCreateOptions {
                resource_version: 40,
                meta_uid: Some("uid-selected".into()),
            },
        )
        .await
        .unwrap();
        db.apply_replicated_create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "rv-high-water",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "namespace": "default",
                    "name": "rv-high-water",
                    "uid": "uid-high-water"
                }
            }),
            ReplicatedCreateOptions {
                resource_version: 50,
                meta_uid: Some("uid-high-water".into()),
            },
        )
        .await
        .unwrap();

        let list = db
            .list_resources(
                "v1",
                "ConfigMap",
                Some("default"),
                ResourceListQuery::new(Some("track=yes"), None, None, None),
            )
            .await
            .unwrap();
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.resource_version, 50);
        let list_position = list
            .watch_replay_position
            .expect("LIST must carry its exact durable position");

        let mut nonmatching = selected;
        nonmatching["metadata"]["labels"]["track"] = serde_json::json!("no");
        db.apply_replicated_create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "selected",
            nonmatching,
            ReplicatedCreateOptions {
                resource_version: 45,
                meta_uid: Some("uid-selected".into()),
            },
        )
        .await
        .unwrap();

        let db: DatastoreHandle = Arc::new(db);
        let (_tx, rx) = watch::channel(true);
        let client = LocalApiClient::new(db, "node-a".to_string(), rx);
        let mut stream = client
            .watch_resources(
                WatchRequest::try_new(
                    "v1",
                    "ConfigMap",
                    Some("default".into()),
                    Some("track=yes".into()),
                    None,
                    Some(50),
                    Some(list_position),
                )
                .expect("valid positioned selector watch"),
            )
            .await
            .unwrap();

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .expect("retained lower-RV leave must replay")
            .expect("watch remains open")
            .expect("event decodes");
        assert_eq!(event.event_type(), WatchEventType::Deleted);
        assert_eq!(event.resource().data["metadata"]["labels"]["track"], "yes");
        assert!(
            event
                .resume_position()
                .is_some_and(|position| position.event_id > list_position.event_id),
            "resume cursor must advance through the lower-RV mutation"
        );
    }

    #[tokio::test]
    async fn local_omitted_rv_watch_starts_after_existing_objects() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "existing",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "default", "name": "existing"}
            }),
        )
        .await
        .unwrap();
        let (_tx, rx) = watch::channel(true);
        let client = LocalApiClient::new(db.clone(), "node-a".to_string(), rx);
        let mut stream = client
            .watch_resources(
                WatchRequest::try_new(
                    "v1",
                    "ConfigMap",
                    Some("default".to_string()),
                    None,
                    None,
                    None,
                    None,
                )
                .expect("valid ConfigMap watch"),
            )
            .await
            .unwrap();
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "fresh",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "default", "name": "fresh"}
            }),
        )
        .await
        .unwrap();

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .expect("post-establishment event should arrive")
            .expect("stream should remain open")
            .expect("event should decode");
        assert_eq!(event.resource().data["metadata"]["name"], "fresh");
    }

    /// Promotion is a watch flip. The same client instance must start
    /// accepting writes the moment is_leader_rx observes `true`. No
    /// re-construction or rewiring.
    #[tokio::test]
    async fn local_api_client_flips_to_accepting_writes_on_promotion() {
        let db = crate::datastore::test_support::in_memory().await;
        make_pod(&db).await;
        let (tx, rx) = watch::channel(false);
        let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);

        // Pre-promotion: write refused.
        let pre = client
            .deliver_test_outbox(
                "idem-2",
                OutboxOperation::PodStatus,
                pod_status_payload(),
                "client",
                1,
                1,
            )
            .await;
        assert!(pre.is_err(), "pre-promotion write must be refused");

        // Promotion: flip the watch.
        tx.send(true).expect("send promotion signal");

        // Post-promotion: same client instance, write succeeds.
        let post = client
            .deliver_test_outbox(
                "idem-3",
                OutboxOperation::PodStatus,
                pod_status_payload(),
                "client",
                1,
                1,
            )
            .await;
        assert!(
            post.is_ok(),
            "post-promotion write must succeed on the same instance, got: {post:?}"
        );
    }

    /// Demotion is the symmetric flip. A live leader that loses
    /// leadership (term lost, voluntary step-down) must stop accepting
    /// writes on the next call.
    #[tokio::test]
    async fn local_api_client_revokes_writes_on_demotion() {
        let db = crate::datastore::test_support::in_memory().await;
        make_pod(&db).await;
        let (tx, rx) = watch::channel(true);
        let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);

        // Pre-demotion: write succeeds.
        let pre = client
            .deliver_test_outbox(
                "idem-4",
                OutboxOperation::PodStatus,
                pod_status_payload(),
                "client",
                1,
                1,
            )
            .await;
        assert!(pre.is_ok(), "pre-demotion write must succeed");

        // Demotion: flip the watch to false.
        tx.send(false).expect("send demotion signal");

        // Post-demotion: same client instance, write refused.
        let post = client
            .deliver_test_outbox(
                "idem-5",
                OutboxOperation::PodStatus,
                pod_status_payload(),
                "client",
                1,
                1,
            )
            .await
            .expect_err("post-demotion write must be refused");
        assert!(
            matches!(post, OutboxApplyError::Retryable(_)),
            "demoted write surfaces as Retryable, got {post:?}"
        );
    }

    /// The focused delivery port uses the same leader gate as every local
    /// mutation and must surface a retryable result after demotion.
    #[tokio::test]
    async fn outbox_apply_client_respects_leader_gate() {
        let db = crate::datastore::test_support::in_memory().await;
        make_pod(&db).await;
        let (_tx, rx) = watch::channel(false);
        let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);
        let trait_obj: &dyn klights_leader_api::LeaderOutboxDelivery = &client;

        let err = trait_obj
            .deliver_outbox(
                klights_leader_api::OutboxDeliveryRequest::try_new(
                    "idem-6",
                    klights_leader_api::OutboxDeliveryOperation::PodStatus,
                    Arc::<[u8]>::from(pod_status_payload().to_vec()),
                    "client",
                    1,
                    1,
                )
                .expect("valid delivery request"),
            )
            .await
            .expect_err("non-leader outbox apply must be refused");
        assert!(
            matches!(err, OutboxApplyError::Retryable(_)),
            "outbox dispatcher must see Retryable so it re-queues on next leadership flip"
        );
    }

    /// Compile-time pin: the `is_leader_rx` field is a required
    /// `watch::Receiver<bool>` and the constructor signature demands it.
    /// If a future refactor moves the field behind an `Option<>` or
    /// adds a default-true fallback, this test breaks at compile time
    /// (it asserts the exact constructor arity and parameter type).
    #[test]
    fn local_api_client_constructor_requires_is_leader_rx() {
        // Force the compiler to verify the constructor signature. This
        // closure can only be constructed if `LocalApiClient::new` has
        // exactly the (DatastoreHandle, String, watch::Receiver<bool>)
        // shape — any change to the watch arg breaks the binding.
        let _check: fn(DatastoreHandle, String, watch::Receiver<bool>) -> LocalApiClient =
            LocalApiClient::new;
        let _check_with_tracker: fn(
            DatastoreHandle,
            String,
            Arc<crate::node_lease_tracker::NodeLeaseTracker>,
            watch::Receiver<bool>,
        ) -> LocalApiClient = LocalApiClient::new_with_node_lease_tracker;
    }

    /// `always_leader_watch()` returns a receiver permanently held at
    /// `true`. Required for tests and for boot paths where leadership
    /// has already been established (e.g. cp1 after bootstrap_single_voter
    /// runs synchronously, before any real watch wiring exists).
    #[test]
    fn always_leader_watch_observes_true_forever() {
        let rx = always_leader_watch();
        assert!(*rx.borrow(), "always_leader_watch must start true");
        // The internal sender is leaked — drop the rx clone we have and
        // recreate; both copies must still observe true.
        drop(rx);
        let rx2 = always_leader_watch();
        assert!(*rx2.borrow(), "always_leader_watch must stay true");
    }

    /// T6 step 2 (audit): `LocalApiClient`'s embedded `N1Raft` writer is
    /// a *private field* invoked only from `apply_outbox`. There is no
    /// public method on `LocalApiClient` that exposes the N1Raft handle
    /// or lets it write outside the gated apply path. Combined with
    /// step 1's `apply_outbox` gate, this proves the N1Raft writer
    /// inherits the leadership refusal: a non-leader's apply_outbox
    /// returns `Retryable` before reaching N1Raft.
    ///
    /// This test exercises the path end-to-end: invoke apply_outbox
    /// with watch=false → assert refusal → confirm the cluster.db has
    /// no trace of the would-be write (i.e., N1Raft never ran).
    #[tokio::test]
    async fn n1raft_inside_local_api_client_writes_via_gated_apply_outbox() {
        let db = crate::datastore::test_support::in_memory().await;
        make_pod(&db).await;
        let pre_rv = db
            .get_resource("v1", "Pod", Some("default"), "web")
            .await
            .expect("read pod")
            .expect("pod exists")
            .resource_version;
        let (_tx, rx) = watch::channel(false);
        let client = LocalApiClient::new(Arc::new(db.clone()), "node-a".to_string(), rx);

        let err = client
            .deliver_test_outbox(
                "n1raft-audit",
                OutboxOperation::PodStatus,
                pod_status_payload(),
                "client",
                1,
                1,
            )
            .await
            .expect_err("non-leader apply_outbox must refuse before reaching N1Raft");
        assert!(matches!(err, OutboxApplyError::Retryable(_)));

        // Confirm N1Raft never executed: the Pod's resource_version
        // and status are unchanged from the pre-call state.
        let post = db
            .get_resource("v1", "Pod", Some("default"), "web")
            .await
            .expect("re-read pod")
            .expect("pod still exists");
        assert_eq!(
            post.resource_version, pre_rv,
            "N1Raft must not have written: cluster.db rv must be unchanged"
        );
        assert!(
            post.data.pointer("/status/phase").is_none(),
            "N1Raft must not have written: status must be absent (no Running phase)"
        );
    }
}
