use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;

use std::sync::Arc;
use tokio::sync::OnceCell;
use tokio::sync::watch;

use crate::control_plane::client::{
    CacheScope, ConfigMap, LeaderApiClient, ListRequest, ListResponse, Node, Pod,
    ProjectedServiceAccountToken, ProjectedServiceAccountTokenRequest, ResourceEvent, ResourceKey,
    Secret, WatchRequest, WatchStream,
};
use crate::controller_dispatcher::ControllerDispatcher;
use crate::datastore::replicated::WriteRejection;
use crate::datastore::sqlite::DatastoreWatchReplaySource;
use crate::datastore::{DatastoreHandle, NodeSubnet, PodCleanupIntent, Resource, WatchTarget};
use crate::kubelet::outbox::payload::OutboxOperation;
use crate::kubelet::outbox::{OutboxApplyClient, OutboxApplyError, OutboxApplyResult};
use crate::networking::wireguard::DataplanePeerMetadata;
use crate::watch::{WatchEvent, WatchEventSelection};

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

pub(crate) async fn read_projected_service_account_token_bound_pod(
    db: &DatastoreHandle,
    request: &ProjectedServiceAccountTokenRequest,
) -> Result<Option<Resource>> {
    let Some(pod_name) = request.bound_pod_name.as_deref() else {
        return Ok(None);
    };

    db.get_resource("v1", "Pod", Some(&request.namespace), pod_name)
        .await
}

#[derive(Clone)]
pub struct LocalApiClient {
    db: DatastoreHandle,
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
        Self {
            raft: crate::datastore::raft::state_machine::N1Raft::new(db.clone()),
            db,
            authoring_node,
            containerd_namespace,
            node_lease_tracker,
            controller_dispatcher: Arc::new(OnceCell::new()),
            is_leader_rx,
        }
    }

    /// T6 step 1: returns `Ok(())` when this node is the elected raft
    /// leader, `Err(WriteRejection::FollowerWrite)` otherwise. Every
    /// mutation method on the `LeaderApiClient` and `OutboxApplyClient`
    /// impls calls this before touching the datastore.
    fn check_leader(&self) -> Result<()> {
        if *self.is_leader_rx.borrow() {
            Ok(())
        } else {
            Err(anyhow!(WriteRejection::FollowerWrite))
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

#[async_trait]
impl LeaderApiClient for LocalApiClient {
    async fn get_resource(&self, key: ResourceKey) -> Result<Option<Resource>> {
        self.db
            .get_resource(
                &key.api_version,
                &key.kind,
                key.namespace.as_deref(),
                &key.name,
            )
            .await
    }

    async fn get_resource_fresh(&self, key: ResourceKey) -> Result<Option<Resource>> {
        self.get_resource(key).await
    }

    async fn list_resources(&self, req: ListRequest) -> Result<ListResponse> {
        self.db
            .list_resources(
                &req.api_version,
                &req.kind,
                req.namespace.as_deref(),
                crate::datastore::ResourceListQuery::new(
                    req.label_selector.as_deref(),
                    req.field_selector.as_deref(),
                    req.limit,
                    req.continue_token.as_deref(),
                ),
            )
            .await
    }

    async fn watch_resources(&self, req: WatchRequest) -> Result<WatchStream<ResourceEvent>> {
        let topic = crate::watch::WatchTopic::new(&req.api_version, &req.kind);
        let signal_rx = self.db.subscribe_watch_signals(topic.clone());
        let replay_source =
            DatastoreWatchReplaySource::new(self.db.clone(), vec![watch_target_for_request(&req)]);
        let scope = watch_delivery_scope_for_request(&req);
        let start_rv = req.start_resource_version.unwrap_or(0).max(0);
        let stream = async_stream::stream! {
            let mut cursor = crate::watch::SignalWatchCursor::new(
                signal_rx,
                replay_source,
                topic,
                scope,
                start_rv,
                crate::watch::WindowPolicy::default_watch_delivery(),
            );
            if start_rv > 0
                && let Err(err) = cursor.prime_replay_or_expired().await
            {
                yield Err(local_watch_cursor_error(err, cursor.accepted_rv()));
                return;
            }
            loop {
                match cursor.next_event().await {
                    Ok(event) => {
                        if watch_event_matches(&event, &req) {
                            yield Ok(ResourceEvent { event });
                        }
                    }
                    Err(crate::watch::WatchCursorError::Closed) => {
                        yield Err(anyhow!("local watch signal channel closed"));
                        return;
                    }
                    Err(err) => {
                        yield Err(local_watch_cursor_error(err, cursor.accepted_rv()));
                        return;
                    }
                }
            }
        };
        Ok(Box::pin(stream))
    }

    async fn wait_cache_ready(&self, _scope: CacheScope) -> Result<()> {
        Ok(())
    }

    async fn projected_service_account_token(
        &self,
        request: ProjectedServiceAccountTokenRequest,
    ) -> Result<ProjectedServiceAccountToken> {
        self.check_leader()?;
        let bound_pod = read_projected_service_account_token_bound_pod(&self.db, &request).await?;
        let signing_key_pem =
            crate::auth::read_service_account_signing_key_async(&self.containerd_namespace)
                .await
                .with_context(|| {
                    format!(
                        "Failed to read ServiceAccount signing key for {}",
                        self.containerd_namespace
                    )
                })?;
        crate::control_plane::service_account_tokens::issue_projected_service_account_token(
            self.db.as_ref(),
            &signing_key_pem,
            &request,
            bound_pod.as_ref(),
        )
        .await
    }

    async fn get_pod(&self, ns: &str, name: &str) -> Result<Option<Pod>> {
        self.get_resource(ResourceKey {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some(ns.to_string()),
            name: name.to_string(),
        })
        .await
    }

    async fn get_pod_for_uid(&self, ns: &str, name: &str, uid: &str) -> Result<Option<Pod>> {
        Ok(self
            .get_pod(ns, name)
            .await?
            .filter(|pod| pod.uid.as_str() == uid))
    }

    async fn watch_pods_on_node(&self, node_name: &str) -> Result<WatchStream<Pod>> {
        let watch = self
            .watch_resources(WatchRequest {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: None,
                label_selector: None,
                field_selector: Some(format!("spec.nodeName={node_name}")),
                start_resource_version: None,
            })
            .await?;
        let node_name = node_name.to_string();
        Ok(Box::pin(watch.filter_map(move |event| {
            let node_name = node_name.clone();
            async move {
                match event {
                    Ok(event)
                        if event
                            .event
                            .object
                            .pointer("/spec/nodeName")
                            .and_then(|value| value.as_str())
                            == Some(node_name.as_str()) =>
                    {
                        Some(Ok(Resource::from_watch_event(event.event)))
                    }
                    Ok(_) => None,
                    Err(err) => Some(Err(err)),
                }
            }
        })))
    }

    async fn list_pods_on_node(&self, node_name: &str) -> Result<Vec<Pod>> {
        Ok(self
            .list_resources(ListRequest {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: None,
                label_selector: None,
                field_selector: Some(format!("spec.nodeName={node_name}")),
                limit: None,
                continue_token: None,
            })
            .await?
            .items)
    }

    async fn get_configmap(&self, ns: &str, name: &str) -> Result<Option<ConfigMap>> {
        self.get_resource(ResourceKey {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some(ns.to_string()),
            name: name.to_string(),
        })
        .await
    }

    async fn get_secret(&self, ns: &str, name: &str) -> Result<Option<Secret>> {
        self.get_resource(ResourceKey {
            api_version: "v1".to_string(),
            kind: "Secret".to_string(),
            namespace: Some(ns.to_string()),
            name: name.to_string(),
        })
        .await
    }

    async fn get_node(&self, name: &str) -> Result<Node> {
        self.get_resource(ResourceKey {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: name.to_string(),
        })
        .await?
        .ok_or_else(|| anyhow!("Node {name} not found"))
    }

    async fn watch_node(&self, name: &str) -> Result<WatchStream<Node>> {
        let watch = self
            .watch_resources(WatchRequest {
                api_version: "v1".to_string(),
                kind: "Node".to_string(),
                namespace: None,
                label_selector: None,
                field_selector: None,
                start_resource_version: None,
            })
            .await?;
        let name = name.to_string();
        Ok(Box::pin(watch.filter_map(move |event| {
            let name = name.clone();
            async move {
                match event {
                    Ok(event)
                        if event
                            .event
                            .object
                            .pointer("/metadata/name")
                            .and_then(|value| value.as_str())
                            == Some(name.as_str()) =>
                    {
                        Some(Ok(Resource::from_watch_event(event.event)))
                    }
                    Ok(_) => None,
                    Err(err) => Some(Err(err)),
                }
            }
        })))
    }

    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<NodeSubnet> {
        self.check_leader()?;
        self.db
            .allocate_node_subnet(node_name, cluster_cidr, node_ip)
            .await
    }

    async fn get_node_subnet(&self, node_name: &str) -> Result<Option<NodeSubnet>> {
        self.db.get_node_subnet(node_name).await
    }

    async fn list_peer_subnets(&self, my_node_name: &str) -> Result<Vec<NodeSubnet>> {
        self.db.list_peer_subnets(my_node_name).await
    }

    async fn get_node_dataplane(&self, node_name: &str) -> Result<Option<DataplanePeerMetadata>> {
        self.db.get_node_dataplane(node_name).await
    }

    async fn list_pod_cleanup_intents_for_node(
        &self,
        node_name: &str,
    ) -> Result<Vec<PodCleanupIntent>> {
        self.db.list_pod_cleanup_intents_for_node(node_name).await
    }

    async fn delete_pod_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        self.check_leader()?;
        self.db
            .delete_pod_cleanup_intent(node_name, namespace, pod_name, pod_uid, reason)
            .await
    }

    async fn get_cluster_membership(
        &self,
    ) -> Result<crate::control_plane::client::membership::ClusterMembership> {
        crate::bootstrap::cluster_meta::read_cluster_membership(self.db.as_ref()).await
    }

    async fn apply_outbox(
        &self,
        idempotency_key: &str,
        operation: OutboxOperation,
        payload: Bytes,
    ) -> std::result::Result<OutboxApplyResult, OutboxApplyError> {
        // T6 step 1 inner gate. Refuse all apply_outbox calls when this
        // node is not the elected leader, including LeaseRenew — the
        // node_lease_tracker is leader-only authoritative state, and a
        // follower must not record lease renewals locally either.
        self.check_leader_outbox()?;
        if operation == OutboxOperation::LeaseRenew {
            let decoded = crate::kubelet::outbox::payload::OutboxPayload::decode_protobuf(&payload)
                .map_err(|err| OutboxApplyError::Retryable(err.to_string()))?;
            self.node_lease_tracker
                .record_from_command(&decoded.command, &self.authoring_node)
                .await
                .map_err(|err| OutboxApplyError::ConflictTerminal(err.to_string()))?;
            return Ok(OutboxApplyResult::Applied { applied_rv: 0 });
        }
        let outcome = self
            .raft
            .propose_outbox(idempotency_key, operation, payload, &self.authoring_node)
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
    }
}

#[async_trait]
impl OutboxApplyClient for LocalApiClient {
    async fn apply_outbox(
        &self,
        idempotency_key: &str,
        operation: OutboxOperation,
        payload: Bytes,
    ) -> std::result::Result<OutboxApplyResult, OutboxApplyError> {
        LeaderApiClient::apply_outbox(self, idempotency_key, operation, payload).await
    }
}

fn watch_event_matches(event: &WatchEvent, req: &WatchRequest) -> bool {
    WatchEventSelection::new(&req.api_version, &req.kind)
        .namespace(req.namespace.as_deref())
        .label_selector(req.label_selector.as_deref())
        .field_selector(req.field_selector.as_deref())
        .matches(event)
}

fn watch_target_for_request(req: &WatchRequest) -> WatchTarget {
    if let Some(namespace) = req.namespace.as_ref() {
        return WatchTarget::namespaced_in_namespace(
            req.api_version.clone(),
            req.kind.clone(),
            namespace.clone(),
        );
    }
    if crate::datastore::sqlite::scope::is_namespaced(&req.kind) {
        WatchTarget::namespaced(req.api_version.clone(), req.kind.clone())
    } else {
        WatchTarget::cluster(req.api_version.clone(), req.kind.clone())
    }
}

fn watch_delivery_scope_for_request(req: &WatchRequest) -> crate::watch::WatchDeliveryScope {
    if let Some(namespace) = req.namespace.as_ref() {
        return crate::watch::WatchDeliveryScope::Namespaced(namespace.clone());
    }
    if crate::datastore::sqlite::scope::is_namespaced(&req.kind) {
        crate::watch::WatchDeliveryScope::NamespacedAll
    } else {
        crate::watch::WatchDeliveryScope::Cluster
    }
}

fn local_watch_cursor_error(
    err: crate::watch::WatchCursorError,
    accepted_rv: i64,
) -> anyhow::Error {
    match err {
        crate::watch::WatchCursorError::Expired => {
            anyhow!("local watch replay window expired: resume rv {accepted_rv} requires relist")
        }
        crate::watch::WatchCursorError::Replay(err) => anyhow!("local watch replay failed: {err}"),
        crate::watch::WatchCursorError::Closed => anyhow!("local watch signal channel closed"),
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
    use crate::control_plane::client::LeaderApiClient;
    use crate::control_plane::client::{ListRequest, ResourceKey};
    use crate::datastore::ResourcePreconditions;
    use crate::datastore::command::StorageCommand;
    use crate::kubelet::outbox::OutboxApplyError;
    use crate::kubelet::outbox::payload::{OutboxOperation, OutboxPayload};

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

    fn lease_renew_payload(node_name: &str) -> bytes::Bytes {
        let command = StorageCommand::CreateResource {
            api_version: "coordination.k8s.io/v1".to_string(),
            kind: "Lease".to_string(),
            namespace: Some("kube-node-lease".to_string()),
            name: node_name.to_string(),
            data: serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": node_name, "namespace": "kube-node-lease"},
                "spec": {"holderIdentity": node_name, "renewTime": "2026-05-29T05:00:00.000000Z"}
            }),
        };
        bytes::Bytes::from(
            OutboxPayload::from_command(command)
                .encode_protobuf()
                .expect("encode lease renew payload"),
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

        let err = LeaderApiClient::apply_outbox(
            &client,
            "idem-1",
            OutboxOperation::PodStatus,
            pod_status_payload(),
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

    /// Apply gate covers `LeaseRenew` too. The node_lease_tracker is
    /// leader-only authoritative state; a follower must not record
    /// renewals locally either.
    #[tokio::test]
    async fn local_api_client_refuses_apply_outbox_lease_renew_when_not_leader() {
        let db = crate::datastore::test_support::in_memory().await;
        let (_tx, rx) = watch::channel(false);
        let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);

        let err = LeaderApiClient::apply_outbox(
            &client,
            "idem-lease",
            OutboxOperation::LeaseRenew,
            lease_renew_payload("node-a"),
        )
        .await
        .expect_err("non-leader lease renew must be rejected");
        assert!(
            matches!(err, OutboxApplyError::Retryable(_)),
            "lease renew gate must surface as Retryable, got {err:?}"
        );
    }

    /// `allocate_node_subnet` writes cluster state and must be gated.
    #[tokio::test]
    async fn local_api_client_refuses_allocate_node_subnet_when_not_leader() {
        let db = crate::datastore::test_support::in_memory().await;
        let (_tx, rx) = watch::channel(false);
        let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);

        let err = client
            .allocate_node_subnet("node-a", "10.50.0.0/16", "10.99.0.10")
            .await
            .expect_err("non-leader subnet allocation must be rejected");
        assert!(
            err.to_string().contains("follower"),
            "expected FollowerWrite, got: {err}"
        );
    }

    /// Reads are unconditionally allowed regardless of leadership state.
    /// Followers serve reads of their own raft-applied cluster.db.
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
                .get_resource(key.clone())
                .await
                .expect("read allowed")
                .is_some(),
            "non-leader get_resource must succeed"
        );
        assert!(
            client
                .get_pod("default", "web")
                .await
                .expect("read allowed")
                .is_some(),
            "non-leader get_pod must succeed"
        );
        let listed = client
            .list_resources(ListRequest {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                label_selector: None,
                field_selector: None,
                continue_token: None,
                limit: None,
            })
            .await
            .expect("list allowed");
        assert_eq!(
            listed.items.len(),
            1,
            "non-leader list_resources must succeed"
        );
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
        let pre = LeaderApiClient::apply_outbox(
            &client,
            "idem-2",
            OutboxOperation::PodStatus,
            pod_status_payload(),
        )
        .await;
        assert!(pre.is_err(), "pre-promotion write must be refused");

        // Promotion: flip the watch.
        tx.send(true).expect("send promotion signal");

        // Post-promotion: same client instance, write succeeds.
        let post = LeaderApiClient::apply_outbox(
            &client,
            "idem-3",
            OutboxOperation::PodStatus,
            pod_status_payload(),
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
        let pre = LeaderApiClient::apply_outbox(
            &client,
            "idem-4",
            OutboxOperation::PodStatus,
            pod_status_payload(),
        )
        .await;
        assert!(pre.is_ok(), "pre-demotion write must succeed");

        // Demotion: flip the watch to false.
        tx.send(false).expect("send demotion signal");

        // Post-demotion: same client instance, write refused.
        let post = LeaderApiClient::apply_outbox(
            &client,
            "idem-5",
            OutboxOperation::PodStatus,
            pod_status_payload(),
        )
        .await
        .expect_err("post-demotion write must be refused");
        assert!(
            matches!(post, OutboxApplyError::Retryable(_)),
            "demoted write surfaces as Retryable, got {post:?}"
        );
    }

    /// The `OutboxApplyClient` trait delegates to `LeaderApiClient::apply_outbox`
    /// so the same gate fires for outbox-dispatcher-driven applies. The
    /// dispatcher uses this trait — it must see `Retryable` and re-enqueue.
    #[tokio::test]
    async fn outbox_apply_client_respects_leader_gate() {
        let db = crate::datastore::test_support::in_memory().await;
        make_pod(&db).await;
        let (_tx, rx) = watch::channel(false);
        let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);
        let trait_obj: &dyn OutboxApplyClient = &client;

        let err = trait_obj
            .apply_outbox("idem-6", OutboxOperation::PodStatus, pod_status_payload())
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

        let err = LeaderApiClient::apply_outbox(
            &client,
            "n1raft-audit",
            OutboxOperation::PodStatus,
            pod_status_payload(),
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
