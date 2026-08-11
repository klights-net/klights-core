//! Authority-routed leader capability dispatch coverage.
//!
//! Each test uses a `RecordingApiClient` fake on both sides
//! (local and remote) so we can assert which arm received the
//! call. No real datastore, no real gRPC; the proxy's dispatch
//! logic is pure and unit-testable.

use super::*;
use crate::datastore::Resource;
use klights_cluster_core::StorageCommand;
use klights_kubelet::node_outbox::payload::OutboxOperation;
use klights_leader_api::node_get_request;
use klights_leader_api::{
    LeaderResourceCommand, ResourceCommandError, ResourceCommandFuture, ResourceCommandRequest,
    ResourceCommandResult,
};
use klights_types::ResourceKey;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::watch;

/// Recording stub: each method bumps a counter and returns a
/// minimal Ok value. Used on both sides of the proxy to assert
/// the dispatch table.
#[derive(Default)]
struct RecordingApiClient {
    name: &'static str,
    get_resource: AtomicUsize,
    list_resources: AtomicUsize,
    watch_resources: AtomicUsize,
    get_pod: AtomicUsize,
    get_node: AtomicUsize,
    projected_service_account_token: AtomicUsize,
    allocate_node_subnet: AtomicUsize,
    list_pod_cleanup_intents: AtomicUsize,
    delete_pod_cleanup_intents: AtomicUsize,
    cleanup_intents: Mutex<Vec<PodCleanupIntent>>,
    apply_outbox: AtomicUsize,
    submit_resource_command: AtomicUsize,
    renew_node_lease: AtomicUsize,
    demote_on_get: Mutex<Option<(watch::Sender<bool>, bool)>>,
    demote_on_watch: Mutex<Option<(watch::Sender<bool>, bool)>>,
}

impl RecordingApiClient {
    fn new(name: &'static str) -> Arc<Self> {
        Arc::new(Self {
            name,
            ..Default::default()
        })
    }

    fn with_cleanup_intent(self: &Arc<Self>, intent: PodCleanupIntent) {
        self.cleanup_intents.lock().unwrap().push(intent);
    }

    fn demote_during_get(&self, sender: watch::Sender<bool>) {
        *self.demote_on_get.lock().unwrap() = Some((sender, false));
    }

    fn demote_during_watch(&self, sender: watch::Sender<bool>) {
        *self.demote_on_watch.lock().unwrap() = Some((sender, false));
    }

    fn flap_during_get(&self, sender: watch::Sender<bool>) {
        *self.demote_on_get.lock().unwrap() = Some((sender, true));
    }

    fn flap_during_watch(&self, sender: watch::Sender<bool>) {
        *self.demote_on_watch.lock().unwrap() = Some((sender, true));
    }
}

impl LeaderResourceQuery for RecordingApiClient {
    fn get_resource(
        &self,
        request: ResourceGetRequest,
    ) -> ResourceQueryFuture<'_, Option<Resource>> {
        match request.key().kind.as_str() {
            "Pod" => {
                self.get_pod.fetch_add(1, Ordering::Relaxed);
            }
            "Node" => {
                self.get_node.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                self.get_resource.fetch_add(1, Ordering::Relaxed);
            }
        }
        if let Some((sender, restore)) = self.demote_on_get.lock().unwrap().take() {
            sender.send(false).expect("demote during get");
            if restore {
                sender.send(true).expect("restore after transient demotion");
            }
        }
        Box::pin(async { Ok(None) })
    }

    fn list_resources(
        &self,
        _request: ResourceListRequest,
    ) -> ResourceQueryFuture<'_, ResourceListResult> {
        self.list_resources.fetch_add(1, Ordering::Relaxed);
        Box::pin(async { ResourceListResult::try_new(Vec::new(), 0, None, None, None) })
    }
}

impl LeaderResourceCommand for RecordingApiClient {
    fn submit_resource_command(
        &self,
        _request: ResourceCommandRequest,
    ) -> ResourceCommandFuture<'_, ResourceCommandResult> {
        self.submit_resource_command.fetch_add(1, Ordering::Relaxed);
        Box::pin(async {
            Ok(ResourceCommandResult::Ack {
                resource_version: 1,
            })
        })
    }
}

impl LeaderNodeLeaseRenewal for RecordingApiClient {
    fn renew_node_lease(
        &self,
        _request: NodeLeaseRenewalRequest,
    ) -> NodeLeaseRenewalFuture<'_, NodeLeaseRenewalResult> {
        self.renew_node_lease.fetch_add(1, Ordering::Relaxed);
        Box::pin(async { Ok(NodeLeaseRenewalResult::Renewed) })
    }
}

impl LeaderWatch for RecordingApiClient {
    fn watch_resources(&self, _req: WatchRequest) -> LeaderWatchFuture<'_> {
        self.watch_resources.fetch_add(1, Ordering::Relaxed);
        if let Some((sender, restore)) = self.demote_on_watch.lock().unwrap().take() {
            sender.send(false).expect("demote during watch open");
            if restore {
                sender.send(true).expect("restore after transient demotion");
            }
        }
        Box::pin(async {
            Ok(WatchStream::unpositioned_test_stream(
                futures::stream::pending(),
            ))
        })
    }
}

impl LeaderCacheReadiness for RecordingApiClient {
    fn wait_cache_ready(&self, _scope: CacheReadinessRequest) -> CacheReadinessFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

impl LeaderProjectedServiceAccountToken for RecordingApiClient {
    fn issue_projected_service_account_token(
        &self,
        _request: ProjectedServiceAccountTokenRequest,
    ) -> ProjectedServiceAccountTokenFuture<'_> {
        self.projected_service_account_token
            .fetch_add(1, Ordering::Relaxed);
        Box::pin(async move {
            klights_leader_api::ProjectedServiceAccountToken::try_new(format!(
                "{}-token",
                self.name
            ))
        })
    }
}

impl LeaderPodCleanupIntents for RecordingApiClient {
    fn list_pod_cleanup_intents(
        &self,
        _request: PodCleanupIntentListRequest,
    ) -> PodCleanupIntentFuture<'_, Vec<PodCleanupIntent>> {
        self.list_pod_cleanup_intents
            .fetch_add(1, Ordering::Relaxed);
        let intents = self.cleanup_intents.lock().unwrap().clone();
        Box::pin(async move { Ok(intents) })
    }

    fn acknowledge_pod_cleanup_intent(
        &self,
        _request: PodCleanupIntentAckRequest,
    ) -> PodCleanupIntentFuture<'_, ()> {
        self.delete_pod_cleanup_intents
            .fetch_add(1, Ordering::Relaxed);
        Box::pin(async { Ok(()) })
    }
}

impl LeaderNodeSubnetAllocation for RecordingApiClient {
    fn allocate_node_subnet(
        &self,
        _request: NodeSubnetAllocationRequest,
    ) -> NodeSubnetAllocationFuture<'_, NodeSubnetAllocationResult> {
        self.allocate_node_subnet.fetch_add(1, Ordering::Relaxed);
        let message = format!("recording client {} allocate_node_subnet", self.name);
        Box::pin(async move { Err(NodeSubnetAllocationError::retryable(message)) })
    }
}

impl LeaderNetworkTopologyQuery for RecordingApiClient {
    fn get_node_subnet(
        &self,
        request: NodeSubnetQuery,
    ) -> NetworkTopologyFuture<'_, NodeSubnetResult> {
        let node_name = request.into_node_name();
        Box::pin(async move { NodeSubnetResult::try_from_wire(&node_name, false, None) })
    }

    fn list_peer_subnets(
        &self,
        request: PeerSubnetsQuery,
    ) -> NetworkTopologyFuture<'_, PeerSubnetsResult> {
        let node_name = request.into_node_name();
        Box::pin(async move { PeerSubnetsResult::try_new(&node_name, Vec::new()) })
    }

    fn get_node_dataplane(
        &self,
        request: NodeDataplaneQuery,
    ) -> NetworkTopologyFuture<'_, NodeDataplaneResult> {
        let node_name = request.into_node_name();
        Box::pin(async move { NodeDataplaneResult::try_from_wire(&node_name, false, None) })
    }
}

impl LeaderOutboxDelivery for RecordingApiClient {
    fn deliver_outbox(&self, _request: OutboxDeliveryRequest) -> OutboxDeliveryFuture<'_> {
        self.apply_outbox.fetch_add(1, Ordering::Relaxed);
        Box::pin(async { Ok(klights_leader_api::OutboxDeliveryResult::Applied { applied_rv: 1 }) })
    }
}

fn make_routed_leader<L, R>(
    local: Arc<L>,
    remote: Arc<R>,
    authority: impl Into<AuthorityHandle>,
) -> AuthorityRoutedLeader
where
    L: LeaderResourceQuery
        + LeaderWatch
        + LeaderCacheReadiness
        + LeaderProjectedServiceAccountToken
        + LeaderPodCleanupIntents
        + LeaderNodeSubnetAllocation
        + LeaderNetworkTopologyQuery
        + Send
        + Sync
        + 'static,
    R: LeaderResourceQuery
        + LeaderWatch
        + LeaderCacheReadiness
        + LeaderProjectedServiceAccountToken
        + LeaderPodCleanupIntents
        + LeaderNodeSubnetAllocation
        + LeaderNetworkTopologyQuery
        + Send
        + Sync
        + 'static,
{
    AuthorityRoutedLeader::new(
        local.clone(),
        local.clone(),
        local.clone(),
        local.clone(),
        local.clone(),
        local.clone(),
        local,
        remote.clone(),
        remote.clone(),
        remote.clone(),
        remote.clone(),
        remote.clone(),
        remote.clone(),
        remote,
        authority,
    )
}

fn make_proxy(
    local: Arc<RecordingApiClient>,
    remote: Arc<RecordingApiClient>,
    initial_leader: bool,
) -> (AuthorityRoutedLeader, watch::Sender<bool>) {
    let (tx, rx) = watch::channel(initial_leader);
    let proxy = make_routed_leader(local.clone(), remote.clone(), rx)
        .with_resource_command_targets(local.clone(), remote.clone())
        .with_outbox_delivery_targets(local.clone(), remote.clone())
        .with_node_lease_renewal_targets(local, remote);
    (proxy, tx)
}

#[tokio::test]
async fn node_effect_lease_dispatch_switches_per_call_without_local_follower_mutation() {
    let local = RecordingApiClient::new("local");
    let remote = RecordingApiClient::new("remote");
    let (proxy, tx) = make_proxy(local.clone(), remote.clone(), false);
    let request = NodeLeaseRenewalRequest::try_new("cp-1", "2026-07-18T12:34:56Z", 30)
        .expect("valid lease renewal");

    proxy
        .renew_node_lease(request.clone())
        .await
        .expect("follower forwards renewal");
    assert_eq!(local.renew_node_lease.load(Ordering::Relaxed), 0);
    assert_eq!(remote.renew_node_lease.load(Ordering::Relaxed), 1);

    tx.send(true).expect("promote proxy");
    proxy
        .renew_node_lease(request)
        .await
        .expect("leader renews locally");
    assert_eq!(local.renew_node_lease.load(Ordering::Relaxed), 1);
    assert_eq!(remote.renew_node_lease.load(Ordering::Relaxed), 1);
}

fn config_map_create_request() -> ResourceCommandRequest {
    ResourceCommandRequest::try_new(StorageCommand::CreateResource {
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
    .expect("valid command")
}

#[tokio::test]
async fn resource_command_dispatch_tracks_leadership_without_local_fallback() {
    let local = RecordingApiClient::new("local");
    let remote = RecordingApiClient::new("remote");
    let (proxy, tx) = make_proxy(local.clone(), remote.clone(), true);

    LeaderResourceCommand::submit_resource_command(&proxy, config_map_create_request())
        .await
        .expect("local command");
    assert_eq!(local.submit_resource_command.load(Ordering::Relaxed), 1);
    assert_eq!(remote.submit_resource_command.load(Ordering::Relaxed), 0);

    tx.send(false).expect("demote");
    LeaderResourceCommand::submit_resource_command(&proxy, config_map_create_request())
        .await
        .expect("remote command");
    assert_eq!(local.submit_resource_command.load(Ordering::Relaxed), 1);
    assert_eq!(remote.submit_resource_command.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn resource_command_refuses_stale_leadership_generation_before_target_invocation() {
    let local = RecordingApiClient::new("local");
    let remote = RecordingApiClient::new("remote");
    let (proxy, tx) = make_proxy(local.clone(), remote.clone(), true);

    let stale = LeaderResourceCommand::submit_resource_command(&proxy, config_map_create_request());
    tx.send(false).expect("demote before polling command");

    let error = stale
        .await
        .expect_err("stale leadership generation must fail closed before dispatch");
    assert!(matches!(error, ResourceCommandError::Retryable { .. }));
    assert_eq!(
        local.submit_resource_command.load(Ordering::Relaxed),
        0,
        "a target selected under the stale leader generation must not be invoked"
    );
    assert_eq!(
        remote.submit_resource_command.load(Ordering::Relaxed),
        0,
        "generation failure must not silently switch targets inside one command"
    );

    LeaderResourceCommand::submit_resource_command(&proxy, config_map_create_request())
        .await
        .expect("fresh follower generation forwards to the current leader");
    assert_eq!(local.submit_resource_command.load(Ordering::Relaxed), 0);
    assert_eq!(remote.submit_resource_command.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn stub_resource_command_is_retryable_and_never_falls_back() {
    let stub = StubRemoteForwarder::new("cp2".to_string());
    let error = LeaderResourceCommand::submit_resource_command(&stub, config_map_create_request())
        .await
        .expect_err("stub must fail closed");
    assert!(matches!(error, ResourceCommandError::Retryable { .. }));
}

/// Self-is-leader: every write lands on the local client.
#[tokio::test]
async fn authority_routed_leader_dispatches_local_when_self_is_leader() {
    let local = RecordingApiClient::new("local");
    let remote = RecordingApiClient::new("remote");
    let (proxy, _tx) = make_proxy(local.clone(), remote.clone(), true);

    proxy
        .deliver_test_outbox(
            "k-1",
            OutboxOperation::PodStatus,
            Bytes::from_static(b"x"),
            "client",
            1,
            1,
        )
        .await
        .expect("apply_outbox");
    proxy
        .allocate_node_subnet(
            NodeSubnetAllocationRequest::try_new("n", "10.0.0.0/16", "10.0.0.1")
                .expect("valid request"),
        )
        .await
        .expect_err("recording client returns Err");
    assert_eq!(local.apply_outbox.load(Ordering::Relaxed), 1);
    assert_eq!(local.allocate_node_subnet.load(Ordering::Relaxed), 1);
    assert_eq!(remote.apply_outbox.load(Ordering::Relaxed), 0);
    assert_eq!(remote.allocate_node_subnet.load(Ordering::Relaxed), 0);
}

/// Self-is-follower: writes land on
/// remote so the call reaches the current elected leader.
#[tokio::test]
async fn authority_routed_leader_dispatches_remote_when_follower() {
    let local = RecordingApiClient::new("local");
    let remote = RecordingApiClient::new("remote");
    let (proxy, _tx) = make_proxy(local.clone(), remote.clone(), false);

    proxy
        .deliver_test_outbox(
            "k-2",
            OutboxOperation::PodStatus,
            Bytes::from_static(b"x"),
            "client",
            1,
            1,
        )
        .await
        .expect("apply_outbox");
    proxy
        .allocate_node_subnet(
            NodeSubnetAllocationRequest::try_new("n", "10.0.0.0/16", "10.0.0.1")
                .expect("valid request"),
        )
        .await
        .expect_err("recording client returns Err");

    assert_eq!(remote.apply_outbox.load(Ordering::Relaxed), 1);
    assert_eq!(remote.allocate_node_subnet.load(Ordering::Relaxed), 1);
    assert_eq!(local.apply_outbox.load(Ordering::Relaxed), 0);
    assert_eq!(local.allocate_node_subnet.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn authority_routed_leader_dispatches_projected_serviceaccount_token_as_write() {
    let request = ProjectedServiceAccountTokenRequest::try_new(
        "kube-system",
        "coredns",
        vec!["https://kubernetes.default.svc.cluster.local".to_string()],
        3600,
        "coredns",
        "pod-uid",
        "mn-controlplane1",
        Some("node-uid".to_string()),
    )
    .unwrap();

    let local = RecordingApiClient::new("local");
    let remote = RecordingApiClient::new("remote");
    let (authority_routed_leader, _tx) = make_proxy(local.clone(), remote.clone(), true);
    let leader_token = authority_routed_leader
        .issue_projected_service_account_token(request.clone())
        .await
        .expect("leader token");
    assert_eq!(leader_token.token(), "local-token");
    assert_eq!(
        local
            .projected_service_account_token
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        remote
            .projected_service_account_token
            .load(Ordering::Relaxed),
        0
    );

    let local = RecordingApiClient::new("local");
    let remote = RecordingApiClient::new("remote");
    let (follower_proxy, _tx) = make_proxy(local.clone(), remote.clone(), false);
    let follower_token = follower_proxy
        .issue_projected_service_account_token(request)
        .await
        .expect("follower token");
    assert_eq!(follower_token.token(), "remote-token");
    assert_eq!(
        local
            .projected_service_account_token
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        remote
            .projected_service_account_token
            .load(Ordering::Relaxed),
        1
    );
}

#[tokio::test]
async fn authority_routed_leader_lists_cleanup_intents_from_remote_when_follower() {
    let local = RecordingApiClient::new("local");
    let remote = RecordingApiClient::new("remote");
    let (proxy, _tx) = make_proxy(local.clone(), remote.clone(), false);

    proxy
        .list_pod_cleanup_intents(PodCleanupIntentListRequest::try_new("mn-controlplane1").unwrap())
        .await
        .expect("list cleanup intents");

    assert_eq!(remote.list_pod_cleanup_intents.load(Ordering::Relaxed), 1);
    assert_eq!(local.list_pod_cleanup_intents.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn authority_routed_leader_reads_cleanup_intents_from_remote_when_local_leader_is_stale() {
    let local = RecordingApiClient::new("local");
    let remote = RecordingApiClient::new("remote");
    remote.with_cleanup_intent(
        PodCleanupIntent::try_new(
            "mn-controlplane1",
            "kube-system",
            "coredns-old",
            "uid-old",
            crate::datastore::POD_CLEANUP_REASON_NODE_LOST,
            205,
            1_700_000_000_000,
            Resource::try_from_data(Arc::new(serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "kube-system",
                    "name": "coredns-old",
                    "uid": "uid-old",
                    "resourceVersion": "204"
                },
                "spec": {"nodeName": "mn-controlplane1"}
            })))
            .unwrap(),
        )
        .unwrap(),
    );
    let (proxy, _tx) = make_proxy(local.clone(), remote.clone(), true);

    let intents = proxy
        .list_pod_cleanup_intents(PodCleanupIntentListRequest::try_new("mn-controlplane1").unwrap())
        .await
        .expect("list cleanup intents");

    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].pod_uid(), "uid-old");
    assert_eq!(remote.list_pod_cleanup_intents.load(Ordering::Relaxed), 1);
    assert_eq!(local.list_pod_cleanup_intents.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn authority_routed_leader_deletes_cleanup_intent_through_remote_when_local_leader_is_stale()
{
    let local = RecordingApiClient::new("local");
    let remote = RecordingApiClient::new("remote");
    let (proxy, _tx) = make_proxy(local.clone(), remote.clone(), true);

    proxy
        .acknowledge_pod_cleanup_intent(
            PodCleanupIntentAckRequest::try_new(
                "mn-controlplane1",
                "kube-system",
                "coredns-old",
                "uid-old",
                crate::datastore::POD_CLEANUP_REASON_NODE_LOST,
            )
            .unwrap(),
        )
        .await
        .expect("delete cleanup intent");

    assert_eq!(remote.delete_pod_cleanup_intents.load(Ordering::Relaxed), 1);
    assert_eq!(local.delete_pod_cleanup_intents.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn authority_routed_leader_reads_dispatch_remote_when_follower_local_when_leader() {
    let local = RecordingApiClient::new("local");
    let remote = RecordingApiClient::new("remote");
    let (proxy, _tx) = make_proxy(local.clone(), remote.clone(), false);

    exercise_read_dispatch(&proxy).await;

    assert_eq!(remote.get_resource.load(Ordering::Relaxed), 1);
    assert_eq!(remote.get_pod.load(Ordering::Relaxed), 1);
    assert_eq!(remote.get_node.load(Ordering::Relaxed), 1);
    assert_eq!(remote.list_resources.load(Ordering::Relaxed), 1);
    assert_eq!(local.get_resource.load(Ordering::Relaxed), 0);
    assert_eq!(local.get_pod.load(Ordering::Relaxed), 0);
    assert_eq!(local.get_node.load(Ordering::Relaxed), 0);
    assert_eq!(local.list_resources.load(Ordering::Relaxed), 0);

    let local = RecordingApiClient::new("local");
    let remote = RecordingApiClient::new("remote");
    let (proxy, _tx) = make_proxy(local.clone(), remote.clone(), true);

    exercise_read_dispatch(&proxy).await;

    assert_eq!(local.get_resource.load(Ordering::Relaxed), 1);
    assert_eq!(local.get_pod.load(Ordering::Relaxed), 1);
    assert_eq!(local.get_node.load(Ordering::Relaxed), 1);
    assert_eq!(local.list_resources.load(Ordering::Relaxed), 1);
    assert_eq!(remote.get_resource.load(Ordering::Relaxed), 0);
    assert_eq!(remote.get_pod.load(Ordering::Relaxed), 0);
    assert_eq!(remote.get_node.load(Ordering::Relaxed), 0);
    assert_eq!(remote.list_resources.load(Ordering::Relaxed), 0);
}

async fn exercise_read_dispatch(proxy: &AuthorityRoutedLeader) {
    proxy
        .get_resource(
            ResourceGetRequest::try_new(
                ResourceKey {
                    api_version: "v1".into(),
                    kind: "ConfigMap".into(),
                    namespace: Some("default".into()),
                    name: "x".into(),
                },
                ResourceQueryConsistency::Cached,
            )
            .expect("valid request"),
        )
        .await
        .expect("get");
    proxy
        .get_resource(
            pod_get_request("default", "x", ResourceQueryConsistency::Cached)
                .expect("valid Pod request"),
        )
        .await
        .expect("get_pod");
    proxy
        .get_resource(
            node_get_request("n", ResourceQueryConsistency::Cached).expect("valid Node request"),
        )
        .await
        .expect("get_node");
    proxy
        .list_resources(
            ResourceListRequest::try_new(
                "v1",
                "Pod",
                None,
                None,
                None,
                None,
                None,
                ResourceQueryConsistency::Cached,
            )
            .expect("valid list request"),
        )
        .await
        .expect("list");
}

#[tokio::test]
async fn authority_routed_leader_watch_terminates_on_leadership_change() {
    use futures::StreamExt as _;
    use tokio::time::{Duration, timeout};

    for initial_leader in [false, true] {
        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        let (proxy, tx) = make_proxy(local.clone(), remote.clone(), initial_leader);
        let mut stream = proxy
            .watch_resources(
                WatchRequest::try_new("v1", "Pod", None, None, None, None, None)
                    .expect("valid watch"),
            )
            .await
            .expect("watch");

        if initial_leader {
            assert_eq!(local.watch_resources.load(Ordering::Relaxed), 1);
            assert_eq!(remote.watch_resources.load(Ordering::Relaxed), 0);
        } else {
            assert_eq!(remote.watch_resources.load(Ordering::Relaxed), 1);
            assert_eq!(local.watch_resources.load(Ordering::Relaxed), 0);
        }

        tx.send(!initial_leader).expect("flip leadership");

        let ended = timeout(Duration::from_millis(100), stream.next())
            .await
            .expect("watch should end promptly after leadership change");
        assert!(
            ended.is_none(),
            "watch stream should terminate so callers reconnect against the new leader target"
        );
    }
}

#[tokio::test]
async fn authority_routed_leader_rejects_samples_and_watches_when_demoted_during_open() {
    let local = RecordingApiClient::new("local");
    let remote = RecordingApiClient::new("remote");
    let (proxy, tx) = make_proxy(local.clone(), remote, true);
    local.demote_during_get(tx.clone());
    assert!(matches!(
        proxy
            .get_resource(
                pod_get_request("default", "web", ResourceQueryConsistency::LeaderFresh).unwrap(),
            )
            .await,
        Err(klights_leader_api::ResourceQueryError::Retryable { .. })
    ));

    tx.send(true).expect("promote for watch race");
    local.demote_during_watch(tx);
    assert!(matches!(
        proxy
            .watch_resources(
                WatchRequest::try_new("v1", "Pod", None, None, None, None, None).unwrap(),
            )
            .await,
        Err(LeaderWatchError::Unavailable { .. })
    ));
}

#[tokio::test]
async fn authority_routed_leader_rejects_transient_leadership_flaps_during_fresh_open() {
    let local = RecordingApiClient::new("local");
    let remote = RecordingApiClient::new("remote");
    let (proxy, tx) = make_proxy(local.clone(), remote, true);
    local.flap_during_get(tx.clone());
    assert!(matches!(
        proxy
            .get_resource(
                pod_get_request("default", "web", ResourceQueryConsistency::LeaderFresh).unwrap(),
            )
            .await,
        Err(klights_leader_api::ResourceQueryError::Retryable { .. })
    ));

    local.flap_during_watch(tx);
    assert!(matches!(
        proxy
            .watch_resources(
                WatchRequest::try_new("v1", "Pod", None, None, None, None, None).unwrap(),
            )
            .await,
        Err(LeaderWatchError::Unavailable { .. })
    ));
}

/// Leader-change is a state flip on the same instance: same
/// proxy, different watch value, next write dispatches to the
/// new target. No reconstruction, no rewiring.
#[tokio::test]
async fn authority_routed_leader_flips_dispatch_on_leader_change_event() {
    let local = RecordingApiClient::new("local");
    let remote = RecordingApiClient::new("remote");
    let (proxy, tx) = make_proxy(local.clone(), remote.clone(), true);

    // Initially leader: write goes local.
    proxy
        .deliver_test_outbox(
            "pre",
            OutboxOperation::PodStatus,
            Bytes::from_static(b"x"),
            "client",
            1,
            1,
        )
        .await
        .expect("pre");
    assert_eq!(local.apply_outbox.load(Ordering::Relaxed), 1);
    assert_eq!(remote.apply_outbox.load(Ordering::Relaxed), 0);

    // Lose leadership: next write goes remote.
    tx.send(false).expect("send loss");
    proxy
        .deliver_test_outbox(
            "lost",
            OutboxOperation::PodStatus,
            Bytes::from_static(b"x"),
            "client",
            1,
            1,
        )
        .await
        .expect("lost");
    assert_eq!(local.apply_outbox.load(Ordering::Relaxed), 1);
    assert_eq!(remote.apply_outbox.load(Ordering::Relaxed), 1);

    // Regain leadership: next write goes local again.
    tx.send(true).expect("send regain");
    proxy
        .deliver_test_outbox(
            "regain",
            OutboxOperation::PodStatus,
            Bytes::from_static(b"x"),
            "client",
            1,
            1,
        )
        .await
        .expect("regain");
    assert_eq!(local.apply_outbox.load(Ordering::Relaxed), 2);
    assert_eq!(remote.apply_outbox.load(Ordering::Relaxed), 1);
}

/// "No leader currently elected" pass-through: when self is a
/// follower and the remote forwarder fails (e.g. election window,
/// transient gRPC failure, leader unreachable), the proxy must
/// surface the error to the caller — no panic, no hang, no
/// silent local fallback. The remote client owns the
/// leader-finding logic (and retry/backoff if any); the proxy
/// only dispatches.
#[tokio::test]
async fn authority_routed_leader_returns_no_leader_error_during_election_window() {
    /// Stub remote that always fails with a "no leader" error,
    /// modeling the gRPC client during an election window or
    /// when every known leader endpoint is unreachable.
    #[derive(Default)]
    struct NoLeaderRemote;

    impl LeaderResourceQuery for NoLeaderRemote {
        fn get_resource(
            &self,
            _request: ResourceGetRequest,
        ) -> ResourceQueryFuture<'_, Option<Resource>> {
            Box::pin(async { Ok(None) })
        }

        fn list_resources(
            &self,
            _request: ResourceListRequest,
        ) -> ResourceQueryFuture<'_, ResourceListResult> {
            Box::pin(async { ResourceListResult::try_new(Vec::new(), 0, None, None, None) })
        }
    }

    impl LeaderWatch for NoLeaderRemote {
        fn watch_resources(&self, _req: WatchRequest) -> LeaderWatchFuture<'_> {
            Box::pin(async { Err(LeaderWatchError::unavailable("no leader")) })
        }
    }

    impl LeaderCacheReadiness for NoLeaderRemote {
        fn wait_cache_ready(&self, _scope: CacheReadinessRequest) -> CacheReadinessFuture<'_> {
            Box::pin(async { Err(CacheReadinessError::unavailable("no leader")) })
        }
    }

    impl LeaderProjectedServiceAccountToken for NoLeaderRemote {
        fn issue_projected_service_account_token(
            &self,
            _request: ProjectedServiceAccountTokenRequest,
        ) -> ProjectedServiceAccountTokenFuture<'_> {
            Box::pin(async {
                Err(klights_leader_api::ProjectedServiceAccountTokenError::unavailable("no leader"))
            })
        }
    }

    impl LeaderPodCleanupIntents for NoLeaderRemote {
        fn list_pod_cleanup_intents(
            &self,
            _request: PodCleanupIntentListRequest,
        ) -> PodCleanupIntentFuture<'_, Vec<PodCleanupIntent>> {
            Box::pin(async { Err(PodCleanupIntentError::unavailable("no leader")) })
        }

        fn acknowledge_pod_cleanup_intent(
            &self,
            _request: PodCleanupIntentAckRequest,
        ) -> PodCleanupIntentFuture<'_, ()> {
            Box::pin(async { Err(PodCleanupIntentError::unavailable("no leader")) })
        }
    }

    impl LeaderNodeSubnetAllocation for NoLeaderRemote {
        fn allocate_node_subnet(
            &self,
            _request: NodeSubnetAllocationRequest,
        ) -> NodeSubnetAllocationFuture<'_, NodeSubnetAllocationResult> {
            Box::pin(async {
                Err(NodeSubnetAllocationError::retryable(
                    "no leader currently elected; retry later",
                ))
            })
        }
    }

    impl LeaderNetworkTopologyQuery for NoLeaderRemote {
        fn get_node_subnet(
            &self,
            _request: NodeSubnetQuery,
        ) -> NetworkTopologyFuture<'_, NodeSubnetResult> {
            Box::pin(async { Err(NetworkTopologyError::NotLeader) })
        }

        fn list_peer_subnets(
            &self,
            _request: PeerSubnetsQuery,
        ) -> NetworkTopologyFuture<'_, PeerSubnetsResult> {
            Box::pin(async { Err(NetworkTopologyError::NotLeader) })
        }

        fn get_node_dataplane(
            &self,
            _request: NodeDataplaneQuery,
        ) -> NetworkTopologyFuture<'_, NodeDataplaneResult> {
            Box::pin(async { Err(NetworkTopologyError::NotLeader) })
        }
    }

    impl LeaderOutboxDelivery for NoLeaderRemote {
        fn deliver_outbox(&self, _request: OutboxDeliveryRequest) -> OutboxDeliveryFuture<'_> {
            Box::pin(async {
                Err(OutboxDeliveryError::unavailable(
                    "no leader currently elected; retry later",
                ))
            })
        }
    }

    let local = RecordingApiClient::new("local");
    let remote = Arc::new(NoLeaderRemote);
    let (_tx, rx) = watch::channel(false); // follower
    let proxy = make_routed_leader(local.clone(), remote.clone(), rx)
        .with_outbox_delivery_targets(local.clone(), remote);

    // apply_outbox must surface the remote's Retryable, not panic
    // or fall back to local.
    let err = proxy
        .deliver_test_outbox(
            "no-leader",
            OutboxOperation::PodStatus,
            Bytes::from_static(b"x"),
            "client",
            1,
            1,
        )
        .await
        .expect_err("must surface no-leader error");
    match err {
        OutboxDeliveryError::Retryable(msg) => {
            assert!(
                msg.contains("no leader"),
                "error must identify the no-leader condition, got: {msg}"
            );
        }
        other => panic!("expected Retryable, got {other:?}"),
    }

    // allocate_node_subnet must also surface a clean error (no
    // panic, no hang, no silent local fallback).
    let err = proxy
        .allocate_node_subnet(
            NodeSubnetAllocationRequest::try_new("n", "10.0.0.0/16", "10.0.0.1")
                .expect("valid request"),
        )
        .await
        .expect_err("must surface no-leader error");
    assert!(
        err.to_string().contains("no leader"),
        "subnet error must identify the no-leader condition, got: {err}"
    );

    // Local was never called for writes — the proxy must not
    // silently fall back.
    assert_eq!(local.apply_outbox.load(Ordering::Relaxed), 0);
    assert_eq!(local.allocate_node_subnet.load(Ordering::Relaxed), 0);
}

/// Every focused capability remains independently object-safe.
#[test]
fn authority_routed_leader_focused_ports_are_object_safe() {
    let local = RecordingApiClient::new("local");
    let remote = RecordingApiClient::new("remote");
    let (_tx, rx) = watch::channel(true);
    let proxy = Arc::new(make_routed_leader(local, remote, rx));
    let _: Arc<dyn LeaderResourceQuery> = proxy.clone();
    let _: Arc<dyn LeaderWatch> = proxy.clone();
    let _: Arc<dyn LeaderCacheReadiness> = proxy.clone();
    let _: Arc<dyn LeaderProjectedServiceAccountToken> = proxy.clone();
    let _: Arc<dyn LeaderPodCleanupIntents> = proxy.clone();
    let _: Arc<dyn LeaderNodeSubnetAllocation> = proxy.clone();
    let _: Arc<dyn LeaderNetworkTopologyQuery> = proxy;
}

/// T6 step 4: the boot-time `StubRemoteForwarder` refuses every
/// write with `Retryable("…not yet wired…")`. The outbox dispatcher
/// treats this as a transient error and re-queues, so follower
/// writes pile up safely until step 4b ships the real forwarder
/// (or until promotion swings the proxy back to local).
#[tokio::test]
async fn stub_remote_forwarder_refuses_writes_with_retryable() {
    let stub = StubRemoteForwarder::new("cp2".into());
    let err = stub
        .deliver_outbox(
            OutboxDeliveryRequest::try_new(
                "boot",
                klights_leader_api::OutboxDeliveryOperation::PodStatus,
                Arc::<[u8]>::from(&b"x"[..]),
                "client",
                1,
                1,
            )
            .expect("valid delivery request"),
        )
        .await
        .expect_err("stub must refuse");
    match err {
        OutboxDeliveryError::Retryable(msg) => {
            assert!(msg.contains("cp2"), "msg must name this node: {msg}");
            assert!(
                msg.contains("not yet wired"),
                "msg must explain forwarder is unwired: {msg}"
            );
        }
        other => panic!("expected Retryable, got {other:?}"),
    }
    // The test stub's read methods return empty. Production leader-class
    // controlplanes use a real RemoteApiClient for follower reads; the
    // stub is only reachable for non-HA seed/worker construction.
    assert!(
        stub.get_resource(
            pod_get_request("default", "x", ResourceQueryConsistency::Cached)
                .expect("valid Pod request"),
        )
        .await
        .expect("read pass")
        .is_none()
    );
}

/// T6 step 4: open_leader composes the switching proxy correctly.
/// This unit test exercises the same composition the bootstrap
/// does: focused local bootstrap capabilities + a stub remote + the same
/// watch → AuthorityRoutedLeader. It proves the
/// boot-time wiring of the three pieces is sound at the type and
/// dispatch level without standing up the full bootstrap.
#[tokio::test]
async fn bootstrap_style_proxy_composition_dispatches_correctly() {
    let concrete_db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let passive_reads = crate::datastore::selector::sqlite_passive_read_ports(&concrete_db);
    let db: crate::datastore::DatastoreHandle = Arc::new(concrete_db);
    let (tx, rx) = watch::channel(true); // simulate seed cp1
    let authority = crate::bootstrap::authority::AuthorityHandle::from(rx.clone());
    let local_cache_readiness =
        Arc::new(crate::bootstrap::local_leader_adapters::LocalCacheReadinessAdapter);
    let local_projected_token = Arc::new(
        crate::bootstrap::local_leader_adapters::LocalProjectedTokenAdapter::new(
            db.clone(),
            "cp1".to_string(),
            "klights".to_string(),
            crate::paths::service_account_signing_key_path("klights"),
            rx.clone(),
            crate::bootstrap::file_blocking::test_file_process_executor(),
        ),
    );
    let proposal =
        Arc::new(crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(db.clone()));
    let network = Arc::new(
        crate::bootstrap::composition_adapters::leader_topology_cleanup_adapter::ClusterStoreLeaderNetwork::new(
            db.clone(),
            proposal.clone(),
            rx.clone(),
        ),
    );
    let pod_cleanup = Arc::new(
        crate::bootstrap::composition_adapters::leader_topology_cleanup_adapter::ClusterStoreLeaderPodCleanup::new(
            db.clone(),
            proposal,
            rx.clone(),
        ),
    );
    let stub_remote = Arc::new(StubRemoteForwarder::new("cp1".into()));
    let local_resource_query =
        crate::bootstrap::composition_adapters::resource_query_adapter::DatastoreResourceQueryAdapter::new(
            db.clone(),
            rx.clone(),
        );
    let local_watch = Arc::new(
        crate::bootstrap::composition_adapters::positioned_watch_adapter::for_test(
            &passive_reads,
            db.clone(),
        ),
    );
    let local_side_effects =
        crate::bootstrap::local_leader_adapters::new_local_outbox_side_effect_state(db.clone());
    let local_outbox_delivery = crate::bootstrap::composition_adapters::
        committed_outbox_delivery_adapter::test_outbox_delivery(
            db.clone(),
            &authority,
            local_side_effects,
            "cp1".to_string(),
    );
    let proxy = AuthorityRoutedLeader::new(
        local_resource_query,
        local_watch,
        local_cache_readiness,
        local_projected_token,
        pod_cleanup,
        network.clone(),
        network,
        stub_remote.clone(),
        stub_remote.clone(),
        stub_remote.clone(),
        stub_remote.clone(),
        stub_remote.clone(),
        stub_remote.clone(),
        stub_remote.clone(),
        rx,
    )
    .with_outbox_delivery_targets(local_outbox_delivery, stub_remote);

    // As leader: write reaches local, succeeds (no Pod precondition).
    let res = proxy
        .deliver_test_outbox(
            "boot-1",
            OutboxOperation::PodStatus,
            pod_status_minimal_payload(),
            "client",
            1,
            1,
        )
        .await;
    // The payload references a Pod that doesn't exist, so the
    // local arm returns a terminal NotFound — but the key point
    // is the call REACHED the local arm (would be Retryable from
    // the stub otherwise).
    match res {
        Err(OutboxDeliveryError::NotFound(_)) => {} // reached local, terminal
        Err(OutboxDeliveryError::Retryable(msg)) if msg.contains("not yet wired") => {
            panic!("write reached stub remote — proxy dispatched WRONG side when leader=true");
        }
        other => {
            // Other terminal errors are acceptable — the assertion is
            // only that we didn't hit the stub.
            tracing::debug!(?other, "local arm returned a non-stub error as expected");
        }
    }

    // Lose leadership: the same instance now routes to remote.
    tx.send(false).expect("demote");
    let err = proxy
        .deliver_test_outbox(
            "boot-2",
            OutboxOperation::PodStatus,
            pod_status_minimal_payload(),
            "client",
            1,
            1,
        )
        .await
        .expect_err("non-leader write goes to stub remote");
    match err {
        OutboxDeliveryError::Retryable(msg) => {
            assert!(
                msg.contains("not yet wired"),
                "must hit the stub remote, got: {msg}"
            );
        }
        other => panic!("expected stub Retryable, got {other:?}"),
    }
}

fn pod_status_minimal_payload() -> Bytes {
    use crate::bootstrap::composition_tests::support::OutboxPayload;
    use crate::datastore::ResourcePreconditions;
    use klights_cluster_core::command::StorageCommand;
    let command = StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "absent".to_string(),
        status: serde_json::json!({"phase": "Running"}),
        expected_rv: None,
        preconditions: ResourcePreconditions {
            uid: Some("absent-uid".to_string()),
            resource_version: None,
        },
        observed_status_stamp: None,
    };
    Bytes::from(
        OutboxPayload::from_command(command)
            .encode_protobuf()
            .expect("encode"),
    )
}

/// The proxy never spawns or sleeps; per-call dispatch is a
/// single authority route sample plus an arc deref. This is a
/// structural check that the impl above does no I/O on its own —
/// dispatch happens inline.
#[test]
fn authority_routed_leader_holds_no_background_resources() {
    // The dispatcher has exactly four thin fields: two public API
    // trait-object Arcs, one optional Arc to the focused target bundle,
    // and one authority handle. Both target pairs live behind that single
    // heap pointer. There is no supervisor, spawn handle, timer, or
    // background task state.
    use std::mem::size_of;
    let thin_dispatcher_fields = size_of::<ArcPair<dyn LeaderResourceQuery>>() * 7
        + size_of::<AuthorityHandle>()
        + size_of::<Option<Arc<FocusedLeaderTargets>>>();
    assert_eq!(
        size_of::<AuthorityRoutedLeader>(),
        thin_dispatcher_fields,
        "AuthorityRoutedLeader must stay a thin per-call dispatcher; \
             field growth probably introduced spawn / timer / supervisor state \
             that violates HR #1 (zero idle CPU)."
    );
}

// ──────────────────────────────────────────────────────────────────
// T6 step 6: convergence unit tests.
//
// These tests prove the dispatch invariants that make cluster.db
// convergence possible at the wiring level:
//   1. A follower-originated write through the switching proxy
//      reaches the leader's apply path (modeled by a shared
//      backend that both the local-leader-arm and the
//      remote-as-leader-arm write to).
//   2. Promotion does not require rewiring cluster_api or the
//      proposer: the same instance flips its dispatch and the
//      same backend is written.
//
// The full netns-level convergence test
// (tests/multinode_netns/convergence_failover_test.sh) covers
// end-to-end cluster.db parity; the unit tests below cover the
// wiring invariants in isolation.
// ──────────────────────────────────────────────────────────────────

/// `cluster_db_converges_after_multinode_write_through_proxy`:
/// model 3 cluster members (1 leader + 2 followers). All three
/// have a `AuthorityRoutedLeader` as their `cluster_api`. The
/// followers' remote arm points at a shared "leader API" mock
/// (modeling the leader's API server reached via gRPC). When a
/// follower issues a write through its proxy, the shared backend
/// records exactly one apply — proving the dispatch routes
/// across the boundary correctly. The leader's own write through
/// its own proxy hits the same shared backend, so all three
/// members' "view" of the apply set is identical.
#[tokio::test]
async fn cluster_db_converges_after_multinode_write_through_proxy() {
    // The shared "leader apply" surface — both the leader's local
    // arm AND every follower's remote arm route writes here. In
    // production this is the leader's private bootstrap capability → raft
    // proposer → raft → state-machine apply on every member.
    let leader_backend = RecordingApiClient::new("leader-shared");

    // Leader member: cluster_api proxy whose local arm IS the
    // shared leader backend. is_leader=true.
    let (_tx_l, rx_l) = watch::channel(true);
    let leader_unused_remote = RecordingApiClient::new("leader-unused-remote");
    let authority_routed_leader =
        make_routed_leader(leader_backend.clone(), leader_unused_remote.clone(), rx_l)
            .with_outbox_delivery_targets(leader_backend.clone(), leader_unused_remote);

    // Follower 1: cluster_api proxy whose REMOTE arm is the
    // shared leader backend (modeling the gRPC forward).
    // is_leader=false.
    let (_tx_f1, rx_f1) = watch::channel(false);
    let follower1_local = RecordingApiClient::new("f1-local-unused");
    let follower1_proxy =
        make_routed_leader(follower1_local.clone(), leader_backend.clone(), rx_f1)
            .with_outbox_delivery_targets(follower1_local, leader_backend.clone());

    // Follower 2: same shape as follower 1.
    let (_tx_f2, rx_f2) = watch::channel(false);
    let follower2_local = RecordingApiClient::new("f2-local-unused");
    let follower2_proxy =
        make_routed_leader(follower2_local.clone(), leader_backend.clone(), rx_f2)
            .with_outbox_delivery_targets(follower2_local, leader_backend.clone());

    // Each member issues one write. All three calls must reach
    // the shared leader backend.
    authority_routed_leader
        .deliver_test_outbox(
            "leader-write",
            OutboxOperation::PodStatus,
            Bytes::from_static(b"x"),
            "client",
            1,
            1,
        )
        .await
        .expect("leader write");
    follower1_proxy
        .deliver_test_outbox(
            "f1-write",
            OutboxOperation::PodStatus,
            Bytes::from_static(b"x"),
            "client",
            1,
            1,
        )
        .await
        .expect("follower1 write");
    follower2_proxy
        .deliver_test_outbox(
            "f2-write",
            OutboxOperation::PodStatus,
            Bytes::from_static(b"x"),
            "client",
            1,
            1,
        )
        .await
        .expect("follower2 write");

    assert_eq!(
        leader_backend.apply_outbox.load(Ordering::Relaxed),
        3,
        "all three members must converge writes on the leader's apply path"
    );
}

/// `promotion_does_not_rewire_cluster_api_or_proposer`: a
/// follower's cluster_api proxy + closed gate → flip leader
/// state via the shared watch → the SAME instances become
/// active without reconstruction. This is the structural
/// guarantee that promotion is a state flip, not a rewire.
#[tokio::test]
async fn promotion_does_not_rewire_cluster_api_or_proposer() {
    let local = RecordingApiClient::new("local");
    let remote = RecordingApiClient::new("remote");
    let (proxy, tx) = make_proxy(local.clone(), remote.clone(), false);
    let proxy = Arc::new(proxy);

    // Capture the addresses of the underlying Arcs BEFORE
    // promotion to prove no reconstruction happens.
    let proxy_addr_before = Arc::as_ptr(&proxy) as *const () as usize;
    let local_addr_before = Arc::as_ptr(&local) as *const () as usize;
    let remote_addr_before = Arc::as_ptr(&remote) as *const () as usize;

    // Follower write → remote.
    proxy
        .deliver_test_outbox(
            "pre",
            OutboxOperation::PodStatus,
            Bytes::from_static(b"x"),
            "client",
            1,
            1,
        )
        .await
        .expect("pre");
    assert_eq!(remote.apply_outbox.load(Ordering::Relaxed), 1);
    assert_eq!(local.apply_outbox.load(Ordering::Relaxed), 0);

    // Promotion: pure state flip, no construction.
    tx.send(true).expect("promote");

    // Same proxy instance now dispatches writes to local.
    proxy
        .deliver_test_outbox(
            "post-promote",
            OutboxOperation::PodStatus,
            Bytes::from_static(b"x"),
            "client",
            1,
            1,
        )
        .await
        .expect("post");
    assert_eq!(remote.apply_outbox.load(Ordering::Relaxed), 1);
    assert_eq!(local.apply_outbox.load(Ordering::Relaxed), 1);

    // Pointer identity proves no Arcs were swapped under us:
    // same proxy struct, same local arm, same remote arm.
    let proxy_addr_after = Arc::as_ptr(&proxy) as *const () as usize;
    let local_addr_after = Arc::as_ptr(&local) as *const () as usize;
    let remote_addr_after = Arc::as_ptr(&remote) as *const () as usize;
    assert_eq!(proxy_addr_before, proxy_addr_after);
    assert_eq!(local_addr_before, local_addr_after);
    assert_eq!(remote_addr_before, remote_addr_after);
}

/// T7.6: verify that when is_leader_rx flips to false, the
/// switching proxy routes writes to the remote arm instead of
/// the local arm. This proves that seed identity is not a
/// permanent write permission — the proxy respects live
/// raft leadership state.
#[tokio::test]
async fn seed_loses_leadership_proxies_writes_to_remote() {
    let local = RecordingApiClient::new("local");
    let remote = RecordingApiClient::new("remote");
    let (proxy, tx) = make_proxy(local.clone(), remote.clone(), true);

    // As leader, writes go to local
    proxy
        .deliver_test_outbox(
            "key",
            OutboxOperation::PodStatus,
            Bytes::from_static(b"x"),
            "client",
            1,
            1,
        )
        .await
        .unwrap();
    assert_eq!(local.apply_outbox.load(Ordering::Relaxed), 1);
    assert_eq!(remote.apply_outbox.load(Ordering::Relaxed), 0);

    // Simulate leadership loss
    tx.send(false).unwrap();

    // After leadership loss, writes go to remote
    proxy
        .deliver_test_outbox(
            "key2",
            OutboxOperation::PodStatus,
            Bytes::from_static(b"y"),
            "client",
            1,
            1,
        )
        .await
        .unwrap();
    assert_eq!(
        local.apply_outbox.load(Ordering::Relaxed),
        1,
        "local must not receive post-loss writes"
    );
    assert_eq!(
        remote.apply_outbox.load(Ordering::Relaxed),
        1,
        "remote must receive post-loss writes"
    );
}
