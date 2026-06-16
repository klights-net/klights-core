//! T6 step 3: switching `LeaderApiClient` for non-leader leader-class boots.
//!
//! Every leader-class member (cp + replica) holds the same `cluster_api`
//! binding — a `LeaderProxyApiClient` that wraps a local
//! `LocalApiClient` and a remote forwarder. Per-call dispatch:
//!
//! - **Kubernetes API reads and watches** go to the elected leader. When this
//!   member is leader they use the local client; otherwise they use the remote
//!   forwarder. Followers do not serve application reads from their local
//!   raft-applied `cluster.db`.
//! - **Pod cleanup intent reads/deletes** prefer the remote leader path.
//!   Startup recovery may run before a rejoining old leader has observed
//!   its demotion, so the local leadership watch can briefly be stale.
//! - **Writes** consult `is_leader_rx` on entry: when `true` they
//!   dispatch to the local client (which routes through the local
//!   datastore → raft proposer → raft → state-machine apply); when
//!   `false` they dispatch to the remote forwarder, which carries the
//!   call to the current elected leader's API server over gRPC.
//!
//! Leadership change is a state flip on the same instance — no
//! re-construction, no rewiring. The proxy reads `is_leader_rx` per
//! call, so the next read or write after promotion / demotion picks the new
//! dispatch target without any setup.
//!
//! Both `local` and `remote` are `Arc<dyn LeaderApiClient>` so the
//! proxy is fully mockable: tests inject recording fakes and assert
//! the dispatch table without spinning up a real cluster.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt as _;
use tokio::sync::watch;

use crate::control_plane::client::{
    CacheScope, ConfigMap, LeaderApiClient, ListRequest, ListResponse, Node, Pod,
    ProjectedServiceAccountToken, ProjectedServiceAccountTokenRequest, ResourceEvent, ResourceKey,
    Secret, WatchRequest, WatchStream,
};
use crate::datastore::{NodeSubnet, PodCleanupIntent, Resource};
use crate::kubelet::outbox::payload::OutboxOperation;
use crate::kubelet::outbox::{OutboxApplyError, OutboxApplyResult};
use crate::networking::wireguard::DataplanePeerMetadata;

/// Leader-aware `LeaderApiClient` that dispatches each call to a local
/// `LocalApiClient` (reads, plus writes when self is the elected
/// leader) or a remote forwarder (writes when self is a follower /
/// learner). The decision is per-call; promotion / demotion flips the
/// watch and the next write picks the new target without rewiring.
pub struct LeaderProxyApiClient {
    local: Arc<dyn LeaderApiClient>,
    remote: Arc<dyn LeaderApiClient>,
    is_leader_rx: watch::Receiver<bool>,
}

impl LeaderProxyApiClient {
    /// Construct a switching proxy.
    ///
    /// `local` handles all reads and writes-while-leader. `remote`
    /// handles writes-while-follower (forwarding to the current
    /// elected leader). `is_leader_rx` is the bootstrap's leadership
    /// watch — the SAME receiver fed to `LocalApiClient`'s gate, so
    /// the two layers can never disagree about who the leader is.
    pub fn new(
        local: Arc<dyn LeaderApiClient>,
        remote: Arc<dyn LeaderApiClient>,
        is_leader_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            local,
            remote,
            is_leader_rx,
        }
    }

    fn is_leader(&self) -> bool {
        *self.is_leader_rx.borrow()
    }

    fn leader_target(&self) -> &Arc<dyn LeaderApiClient> {
        if self.is_leader() {
            &self.local
        } else {
            &self.remote
        }
    }

    fn terminate_watch_on_leadership_change<T>(&self, stream: WatchStream<T>) -> WatchStream<T>
    where
        T: Send + 'static,
    {
        terminate_watch_on_leadership_change(stream, self.is_leader_rx.clone(), self.is_leader())
    }
}

fn terminate_watch_on_leadership_change<T>(
    stream: WatchStream<T>,
    leadership_rx: watch::Receiver<bool>,
    initial_is_leader: bool,
) -> WatchStream<T>
where
    T: Send + 'static,
{
    Box::pin(futures::stream::unfold(
        (stream, leadership_rx),
        move |(mut stream, mut leadership_rx)| async move {
            loop {
                tokio::select! {
                    changed = leadership_rx.changed() => {
                        if changed.is_err() || *leadership_rx.borrow() != initial_is_leader {
                            return None;
                        }
                    }
                    item = stream.next() => {
                        return item.map(|item| (item, (stream, leadership_rx)));
                    }
                }
            }
        },
    ))
}

#[async_trait]
impl LeaderApiClient for LeaderProxyApiClient {
    // --- Kubernetes API reads and watches go to the elected leader ---

    async fn get_resource(&self, key: ResourceKey) -> Result<Option<Resource>> {
        self.leader_target().get_resource(key).await
    }

    async fn get_resource_fresh(&self, key: ResourceKey) -> Result<Option<Resource>> {
        self.leader_target().get_resource_fresh(key).await
    }

    async fn list_resources(&self, req: ListRequest) -> Result<ListResponse> {
        self.leader_target().list_resources(req).await
    }

    async fn list_resources_fresh(&self, req: ListRequest) -> Result<ListResponse> {
        self.leader_target().list_resources_fresh(req).await
    }

    async fn watch_resources(&self, req: WatchRequest) -> Result<WatchStream<ResourceEvent>> {
        let stream = self.leader_target().watch_resources(req).await?;
        Ok(self.terminate_watch_on_leadership_change(stream))
    }

    async fn wait_cache_ready(&self, scope: CacheScope) -> Result<()> {
        self.leader_target().wait_cache_ready(scope).await
    }

    async fn get_pod(&self, ns: &str, name: &str) -> Result<Option<Pod>> {
        self.leader_target().get_pod(ns, name).await
    }

    async fn get_pod_for_uid(&self, ns: &str, name: &str, uid: &str) -> Result<Option<Pod>> {
        self.leader_target().get_pod_for_uid(ns, name, uid).await
    }

    async fn watch_pods_on_node(&self, node_name: &str) -> Result<WatchStream<Pod>> {
        let stream = self.leader_target().watch_pods_on_node(node_name).await?;
        Ok(self.terminate_watch_on_leadership_change(stream))
    }

    async fn list_pods_on_node(&self, node_name: &str) -> Result<Vec<Pod>> {
        self.leader_target().list_pods_on_node(node_name).await
    }

    async fn get_configmap(&self, ns: &str, name: &str) -> Result<Option<ConfigMap>> {
        self.leader_target().get_configmap(ns, name).await
    }

    async fn get_secret(&self, ns: &str, name: &str) -> Result<Option<Secret>> {
        self.leader_target().get_secret(ns, name).await
    }

    async fn get_node(&self, name: &str) -> Result<Node> {
        self.leader_target().get_node(name).await
    }

    async fn watch_node(&self, name: &str) -> Result<WatchStream<Node>> {
        let stream = self.leader_target().watch_node(name).await?;
        Ok(self.terminate_watch_on_leadership_change(stream))
    }

    async fn get_node_subnet(&self, node_name: &str) -> Result<Option<NodeSubnet>> {
        self.leader_target().get_node_subnet(node_name).await
    }

    async fn list_peer_subnets(&self, my_node_name: &str) -> Result<Vec<NodeSubnet>> {
        self.leader_target().list_peer_subnets(my_node_name).await
    }

    async fn get_node_dataplane(&self, node_name: &str) -> Result<Option<DataplanePeerMetadata>> {
        self.leader_target().get_node_dataplane(node_name).await
    }

    async fn list_pod_cleanup_intents_for_node(
        &self,
        node_name: &str,
    ) -> Result<Vec<PodCleanupIntent>> {
        let local_is_leader = self.is_leader();
        match self
            .remote
            .list_pod_cleanup_intents_for_node(node_name)
            .await
        {
            Ok(intents) => Ok(intents),
            Err(_) if local_is_leader => {
                self.local
                    .list_pod_cleanup_intents_for_node(node_name)
                    .await
            }
            Err(err) => Err(err),
        }
    }

    async fn delete_pod_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        match self
            .remote
            .delete_pod_cleanup_intent(node_name, namespace, pod_name, pod_uid, reason)
            .await
        {
            Ok(()) => Ok(()),
            Err(err) if self.is_leader() => {
                let _ = err;
                self.local
                    .delete_pod_cleanup_intent(node_name, namespace, pod_name, pod_uid, reason)
                    .await
            }
            Err(err) => Err(err),
        }
    }

    async fn get_cluster_membership(
        &self,
    ) -> Result<crate::control_plane::client::membership::ClusterMembership> {
        // Membership is the carve-out: followers need local raft topology to
        // locate the elected leader before they can forward ordinary API calls.
        self.local.get_cluster_membership().await
    }

    // --- Writes dispatch on is_leader_rx ---

    async fn projected_service_account_token(
        &self,
        request: ProjectedServiceAccountTokenRequest,
    ) -> Result<ProjectedServiceAccountToken> {
        self.leader_target()
            .projected_service_account_token(request)
            .await
    }

    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<NodeSubnet> {
        self.leader_target()
            .allocate_node_subnet(node_name, cluster_cidr, node_ip)
            .await
    }

    async fn apply_outbox(
        &self,
        idempotency_key: &str,
        operation: OutboxOperation,
        payload: Bytes,
    ) -> std::result::Result<OutboxApplyResult, OutboxApplyError> {
        self.leader_target()
            .apply_outbox(idempotency_key, operation, payload)
            .await
    }
}

/// T6 step 4 placeholder remote: surfaces every write attempt as a
/// clean Retryable / anyhow error pointing at "remote forwarder not
/// yet wired". This is the boot-time stub used until step 4b builds
/// the real gRPC forwarder pointing at the current elected leader.
///
/// The proxy never falls back from remote to local — when a non-leader
/// member's write hits this stub it returns immediately and the outbox
/// dispatcher re-queues. Combined with step 1's inner gate (which
/// refuses the same write on the local arm) the cluster.db on
/// non-leader members stays unchanged: writes pile up in the outbox
/// until either promotion (local arm opens) or step 4b ships (remote
/// arm forwards). No silent local-only writes happen.
pub struct StubRemoteForwarder {
    node_name: String,
}

impl StubRemoteForwarder {
    pub fn new(node_name: String) -> Self {
        Self { node_name }
    }

    fn unavailable(&self) -> String {
        format!(
            "remote leader forwarder not yet wired on {} (T6 step 4b); \
             non-leader writes pile up in the outbox until promotion or until \
             the forwarder lands",
            self.node_name
        )
    }
}

#[async_trait]
impl LeaderApiClient for StubRemoteForwarder {
    async fn get_resource(&self, _key: ResourceKey) -> Result<Option<Resource>> {
        Ok(None)
    }
    async fn list_resources(&self, _req: ListRequest) -> Result<ListResponse> {
        Ok(crate::datastore::ResourceList {
            items: vec![],
            resource_version: 0,
            continue_token: None,
            remaining_item_count: None,
        })
    }
    async fn watch_resources(&self, _req: WatchRequest) -> Result<WatchStream<ResourceEvent>> {
        anyhow::bail!("{}", self.unavailable())
    }
    async fn wait_cache_ready(&self, _scope: CacheScope) -> Result<()> {
        Ok(())
    }
    async fn get_pod(&self, _ns: &str, _name: &str) -> Result<Option<Pod>> {
        Ok(None)
    }
    async fn get_pod_for_uid(&self, _ns: &str, _name: &str, _uid: &str) -> Result<Option<Pod>> {
        Ok(None)
    }
    async fn watch_pods_on_node(&self, _n: &str) -> Result<WatchStream<Pod>> {
        anyhow::bail!("{}", self.unavailable())
    }
    async fn list_pods_on_node(&self, _n: &str) -> Result<Vec<Pod>> {
        Ok(vec![])
    }
    async fn get_configmap(&self, _ns: &str, _name: &str) -> Result<Option<ConfigMap>> {
        Ok(None)
    }
    async fn get_secret(&self, _ns: &str, _name: &str) -> Result<Option<Secret>> {
        Ok(None)
    }
    async fn get_node(&self, _name: &str) -> Result<Node> {
        anyhow::bail!("{}", self.unavailable())
    }
    async fn watch_node(&self, _name: &str) -> Result<WatchStream<Node>> {
        anyhow::bail!("{}", self.unavailable())
    }
    async fn allocate_node_subnet(&self, _n: &str, _c: &str, _ip: &str) -> Result<NodeSubnet> {
        anyhow::bail!("{}", self.unavailable())
    }
    async fn get_node_subnet(&self, _n: &str) -> Result<Option<NodeSubnet>> {
        Ok(None)
    }
    async fn list_peer_subnets(&self, _n: &str) -> Result<Vec<NodeSubnet>> {
        Ok(vec![])
    }
    async fn get_node_dataplane(&self, _n: &str) -> Result<Option<DataplanePeerMetadata>> {
        Ok(None)
    }
    async fn list_pod_cleanup_intents_for_node(
        &self,
        node_name: &str,
    ) -> Result<Vec<PodCleanupIntent>> {
        let _ = node_name;
        anyhow::bail!("{}", self.unavailable())
    }
    async fn delete_pod_cleanup_intent(
        &self,
        _node_name: &str,
        _namespace: &str,
        _pod_name: &str,
        _pod_uid: &str,
        _reason: &str,
    ) -> Result<()> {
        anyhow::bail!("{}", self.unavailable())
    }
    async fn get_cluster_membership(
        &self,
    ) -> Result<crate::control_plane::client::membership::ClusterMembership> {
        anyhow::bail!("{}", self.unavailable())
    }
    async fn apply_outbox(
        &self,
        _k: &str,
        _o: OutboxOperation,
        _p: Bytes,
    ) -> std::result::Result<OutboxApplyResult, OutboxApplyError> {
        Err(OutboxApplyError::Retryable(self.unavailable()))
    }
}

#[cfg(test)]
mod tests {
    //! T6 step 3: switching `LeaderProxyApiClient` dispatch coverage.
    //!
    //! Each test uses a `RecordingApiClient` fake on both sides
    //! (local and remote) so we can assert which arm received the
    //! call. No real datastore, no real gRPC; the proxy's dispatch
    //! logic is pure and unit-testable.

    use super::*;
    use crate::control_plane::client::membership::ClusterMembership;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
        watch_pods_on_node: AtomicUsize,
        watch_node: AtomicUsize,
        get_cluster_membership: AtomicUsize,
        projected_service_account_token: AtomicUsize,
        allocate_node_subnet: AtomicUsize,
        list_pod_cleanup_intents: AtomicUsize,
        delete_pod_cleanup_intents: AtomicUsize,
        cleanup_intents: Mutex<Vec<PodCleanupIntent>>,
        apply_outbox: AtomicUsize,
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
    }

    #[async_trait]
    impl LeaderApiClient for RecordingApiClient {
        async fn get_resource(&self, _key: ResourceKey) -> Result<Option<Resource>> {
            self.get_resource.fetch_add(1, Ordering::Relaxed);
            Ok(None)
        }

        async fn list_resources(&self, _req: ListRequest) -> Result<ListResponse> {
            self.list_resources.fetch_add(1, Ordering::Relaxed);
            Ok(crate::datastore::ResourceList {
                items: vec![],
                resource_version: 0,
                continue_token: None,
                remaining_item_count: None,
            })
        }

        async fn watch_resources(&self, _req: WatchRequest) -> Result<WatchStream<ResourceEvent>> {
            self.watch_resources.fetch_add(1, Ordering::Relaxed);
            Ok(Box::pin(futures::stream::pending()))
        }

        async fn wait_cache_ready(&self, _scope: CacheScope) -> Result<()> {
            Ok(())
        }

        async fn get_pod(&self, _ns: &str, _name: &str) -> Result<Option<Pod>> {
            self.get_pod.fetch_add(1, Ordering::Relaxed);
            Ok(None)
        }

        async fn get_pod_for_uid(&self, _ns: &str, _name: &str, _uid: &str) -> Result<Option<Pod>> {
            Ok(None)
        }

        async fn watch_pods_on_node(&self, _node: &str) -> Result<WatchStream<Pod>> {
            self.watch_pods_on_node.fetch_add(1, Ordering::Relaxed);
            Ok(Box::pin(futures::stream::pending()))
        }

        async fn list_pods_on_node(&self, _node: &str) -> Result<Vec<Pod>> {
            Ok(vec![])
        }

        async fn get_configmap(&self, _ns: &str, _name: &str) -> Result<Option<ConfigMap>> {
            Ok(None)
        }

        async fn get_secret(&self, _ns: &str, _name: &str) -> Result<Option<Secret>> {
            Ok(None)
        }

        async fn get_node(&self, _name: &str) -> Result<Node> {
            self.get_node.fetch_add(1, Ordering::Relaxed);
            Ok(Resource {
                id: 0,
                api_version: "v1".into(),
                kind: "Node".into(),
                namespace: None,
                name: "stub".into(),
                uid: "stub".into(),
                resource_version: 0,
                data: Arc::new(serde_json::json!({})),
            })
        }

        async fn watch_node(&self, _name: &str) -> Result<WatchStream<Node>> {
            self.watch_node.fetch_add(1, Ordering::Relaxed);
            Ok(Box::pin(futures::stream::pending()))
        }

        async fn projected_service_account_token(
            &self,
            _request: ProjectedServiceAccountTokenRequest,
        ) -> Result<ProjectedServiceAccountToken> {
            self.projected_service_account_token
                .fetch_add(1, Ordering::Relaxed);
            Ok(ProjectedServiceAccountToken {
                token: format!("{}-token", self.name),
            })
        }

        async fn allocate_node_subnet(
            &self,
            _node_name: &str,
            _cluster_cidr: &str,
            _node_ip: &str,
        ) -> Result<NodeSubnet> {
            self.allocate_node_subnet.fetch_add(1, Ordering::Relaxed);
            // Dispatch tests assert on the counter; return a synthetic
            // error so we don't need to construct a real NodeSubnet.
            anyhow::bail!("recording client {} allocate_node_subnet", self.name)
        }

        async fn get_node_subnet(&self, _node: &str) -> Result<Option<NodeSubnet>> {
            Ok(None)
        }

        async fn list_peer_subnets(&self, _my_node: &str) -> Result<Vec<NodeSubnet>> {
            Ok(vec![])
        }

        async fn get_node_dataplane(&self, _node: &str) -> Result<Option<DataplanePeerMetadata>> {
            Ok(None)
        }

        async fn list_pod_cleanup_intents_for_node(
            &self,
            _node_name: &str,
        ) -> Result<Vec<PodCleanupIntent>> {
            self.list_pod_cleanup_intents
                .fetch_add(1, Ordering::Relaxed);
            Ok(self.cleanup_intents.lock().unwrap().clone())
        }

        async fn delete_pod_cleanup_intent(
            &self,
            _node_name: &str,
            _namespace: &str,
            _pod_name: &str,
            _pod_uid: &str,
            _reason: &str,
        ) -> Result<()> {
            self.delete_pod_cleanup_intents
                .fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn get_cluster_membership(&self) -> Result<ClusterMembership> {
            self.get_cluster_membership.fetch_add(1, Ordering::Relaxed);
            Ok(ClusterMembership {
                cluster_id: format!("{}-cluster", self.name),
                voters: vec![self.name.to_string()],
                term: 1,
                leader_hint: Some(self.name.to_string()),
            })
        }

        async fn apply_outbox(
            &self,
            _idempotency_key: &str,
            _operation: OutboxOperation,
            _payload: Bytes,
        ) -> std::result::Result<OutboxApplyResult, OutboxApplyError> {
            self.apply_outbox.fetch_add(1, Ordering::Relaxed);
            Ok(OutboxApplyResult::Applied { applied_rv: 0 })
        }
    }

    fn make_proxy(
        local: Arc<RecordingApiClient>,
        remote: Arc<RecordingApiClient>,
        initial_leader: bool,
    ) -> (LeaderProxyApiClient, watch::Sender<bool>) {
        let (tx, rx) = watch::channel(initial_leader);
        let proxy = LeaderProxyApiClient::new(local, remote, rx);
        (proxy, tx)
    }

    /// Self-is-leader: every write lands on the local client.
    #[tokio::test]
    async fn leader_proxy_dispatches_local_when_self_is_leader() {
        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        let (proxy, _tx) = make_proxy(local.clone(), remote.clone(), true);

        proxy
            .apply_outbox("k-1", OutboxOperation::PodStatus, Bytes::from_static(b"x"))
            .await
            .expect("apply_outbox");
        proxy
            .allocate_node_subnet("n", "10.0.0.0/16", "10.0.0.1")
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
    async fn leader_proxy_dispatches_remote_when_follower() {
        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        let (proxy, _tx) = make_proxy(local.clone(), remote.clone(), false);

        proxy
            .apply_outbox("k-2", OutboxOperation::PodStatus, Bytes::from_static(b"x"))
            .await
            .expect("apply_outbox");
        proxy
            .allocate_node_subnet("n", "10.0.0.0/16", "10.0.0.1")
            .await
            .expect_err("recording client returns Err");

        assert_eq!(remote.apply_outbox.load(Ordering::Relaxed), 1);
        assert_eq!(remote.allocate_node_subnet.load(Ordering::Relaxed), 1);
        assert_eq!(local.apply_outbox.load(Ordering::Relaxed), 0);
        assert_eq!(local.allocate_node_subnet.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn leader_proxy_dispatches_projected_serviceaccount_token_as_write() {
        let request = ProjectedServiceAccountTokenRequest {
            namespace: "kube-system".to_string(),
            service_account_name: "coredns".to_string(),
            audiences: vec!["https://kubernetes.default.svc.cluster.local".to_string()],
            expiration_seconds: 3600,
            bound_pod_name: Some("coredns".to_string()),
            bound_pod_uid: Some("pod-uid".to_string()),
            bound_node_name: Some("mn-controlplane1".to_string()),
            bound_node_uid: Some("node-uid".to_string()),
        };

        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        let (leader_proxy, _tx) = make_proxy(local.clone(), remote.clone(), true);
        let leader_token = leader_proxy
            .projected_service_account_token(request.clone())
            .await
            .expect("leader token");
        assert_eq!(leader_token.token, "local-token");
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
            .projected_service_account_token(request)
            .await
            .expect("follower token");
        assert_eq!(follower_token.token, "remote-token");
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
    async fn leader_proxy_lists_cleanup_intents_from_remote_when_follower() {
        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        let (proxy, _tx) = make_proxy(local.clone(), remote.clone(), false);

        proxy
            .list_pod_cleanup_intents_for_node("mn-controlplane1")
            .await
            .expect("list cleanup intents");

        assert_eq!(remote.list_pod_cleanup_intents.load(Ordering::Relaxed), 1);
        assert_eq!(local.list_pod_cleanup_intents.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn leader_proxy_reads_cleanup_intents_from_remote_when_local_leader_is_stale() {
        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        remote.with_cleanup_intent(PodCleanupIntent {
            node_name: "mn-controlplane1".to_string(),
            namespace: "kube-system".to_string(),
            pod_name: "coredns-old".to_string(),
            pod_uid: "uid-old".to_string(),
            reason: crate::datastore::POD_CLEANUP_REASON_NODE_LOST.to_string(),
            resource_version: 205,
            created_at_ms: 0,
            pod_data: serde_json::json!({}),
        });
        let (proxy, _tx) = make_proxy(local.clone(), remote.clone(), true);

        let intents = proxy
            .list_pod_cleanup_intents_for_node("mn-controlplane1")
            .await
            .expect("list cleanup intents");

        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].pod_uid, "uid-old");
        assert_eq!(remote.list_pod_cleanup_intents.load(Ordering::Relaxed), 1);
        assert_eq!(local.list_pod_cleanup_intents.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn leader_proxy_deletes_cleanup_intent_through_remote_when_local_leader_is_stale() {
        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        let (proxy, _tx) = make_proxy(local.clone(), remote.clone(), true);

        proxy
            .delete_pod_cleanup_intent(
                "mn-controlplane1",
                "kube-system",
                "coredns-old",
                "uid-old",
                crate::datastore::POD_CLEANUP_REASON_NODE_LOST,
            )
            .await
            .expect("delete cleanup intent");

        assert_eq!(remote.delete_pod_cleanup_intents.load(Ordering::Relaxed), 1);
        assert_eq!(local.delete_pod_cleanup_intents.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn leader_proxy_reads_dispatch_remote_when_follower_local_when_leader() {
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

    async fn exercise_read_dispatch(proxy: &LeaderProxyApiClient) {
        proxy
            .get_resource(ResourceKey {
                api_version: "v1".into(),
                kind: "Pod".into(),
                namespace: Some("default".into()),
                name: "x".into(),
            })
            .await
            .expect("get");
        proxy.get_pod("default", "x").await.expect("get_pod");
        proxy.get_node("n").await.expect("get_node");
        proxy
            .list_resources(ListRequest {
                api_version: "v1".into(),
                kind: "Pod".into(),
                namespace: None,
                label_selector: None,
                field_selector: None,
                limit: None,
                continue_token: None,
            })
            .await
            .expect("list");
    }

    #[tokio::test]
    async fn leader_proxy_membership_read_stays_local() {
        for is_leader in [false, true] {
            let local = RecordingApiClient::new("local");
            let remote = RecordingApiClient::new("remote");
            let (proxy, _tx) = make_proxy(local.clone(), remote.clone(), is_leader);

            let membership = proxy
                .get_cluster_membership()
                .await
                .expect("cluster membership");

            assert_eq!(membership.cluster_id, "local-cluster");
            assert_eq!(local.get_cluster_membership.load(Ordering::Relaxed), 1);
            assert_eq!(remote.get_cluster_membership.load(Ordering::Relaxed), 0);
        }
    }

    #[tokio::test]
    async fn leader_proxy_watch_terminates_on_leadership_change() {
        use futures::StreamExt as _;
        use tokio::time::{Duration, timeout};

        for initial_leader in [false, true] {
            let local = RecordingApiClient::new("local");
            let remote = RecordingApiClient::new("remote");
            let (proxy, tx) = make_proxy(local.clone(), remote.clone(), initial_leader);
            let mut stream = proxy
                .watch_resources(WatchRequest {
                    api_version: "v1".into(),
                    kind: "Pod".into(),
                    namespace: None,
                    label_selector: None,
                    field_selector: None,
                    start_resource_version: None,
                })
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

    /// Leader-change is a state flip on the same instance: same
    /// proxy, different watch value, next write dispatches to the
    /// new target. No reconstruction, no rewiring.
    #[tokio::test]
    async fn leader_proxy_flips_dispatch_on_leader_change_event() {
        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        let (proxy, tx) = make_proxy(local.clone(), remote.clone(), true);

        // Initially leader: write goes local.
        proxy
            .apply_outbox("pre", OutboxOperation::PodStatus, Bytes::from_static(b"x"))
            .await
            .expect("pre");
        assert_eq!(local.apply_outbox.load(Ordering::Relaxed), 1);
        assert_eq!(remote.apply_outbox.load(Ordering::Relaxed), 0);

        // Lose leadership: next write goes remote.
        tx.send(false).expect("send loss");
        proxy
            .apply_outbox("lost", OutboxOperation::PodStatus, Bytes::from_static(b"x"))
            .await
            .expect("lost");
        assert_eq!(local.apply_outbox.load(Ordering::Relaxed), 1);
        assert_eq!(remote.apply_outbox.load(Ordering::Relaxed), 1);

        // Regain leadership: next write goes local again.
        tx.send(true).expect("send regain");
        proxy
            .apply_outbox(
                "regain",
                OutboxOperation::PodStatus,
                Bytes::from_static(b"x"),
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
    async fn leader_proxy_returns_no_leader_error_during_election_window() {
        /// Stub remote that always fails with a "no leader" error,
        /// modeling the gRPC client during an election window or
        /// when every known leader endpoint is unreachable.
        #[derive(Default)]
        struct NoLeaderRemote;

        #[async_trait]
        impl LeaderApiClient for NoLeaderRemote {
            async fn get_resource(&self, _key: ResourceKey) -> Result<Option<Resource>> {
                Ok(None)
            }
            async fn list_resources(&self, _req: ListRequest) -> Result<ListResponse> {
                Ok(crate::datastore::ResourceList {
                    items: vec![],
                    resource_version: 0,
                    continue_token: None,
                    remaining_item_count: None,
                })
            }
            async fn watch_resources(
                &self,
                _req: WatchRequest,
            ) -> Result<WatchStream<ResourceEvent>> {
                anyhow::bail!("no leader: watch unavailable")
            }
            async fn wait_cache_ready(&self, _scope: CacheScope) -> Result<()> {
                Ok(())
            }
            async fn get_pod(&self, _ns: &str, _name: &str) -> Result<Option<Pod>> {
                Ok(None)
            }
            async fn get_pod_for_uid(
                &self,
                _ns: &str,
                _name: &str,
                _uid: &str,
            ) -> Result<Option<Pod>> {
                Ok(None)
            }
            async fn watch_pods_on_node(&self, _n: &str) -> Result<WatchStream<Pod>> {
                anyhow::bail!("no leader")
            }
            async fn list_pods_on_node(&self, _n: &str) -> Result<Vec<Pod>> {
                Ok(vec![])
            }
            async fn get_configmap(&self, _ns: &str, _name: &str) -> Result<Option<ConfigMap>> {
                Ok(None)
            }
            async fn get_secret(&self, _ns: &str, _name: &str) -> Result<Option<Secret>> {
                Ok(None)
            }
            async fn get_node(&self, _name: &str) -> Result<Node> {
                anyhow::bail!("no leader")
            }
            async fn watch_node(&self, _name: &str) -> Result<WatchStream<Node>> {
                anyhow::bail!("no leader")
            }
            async fn allocate_node_subnet(
                &self,
                _n: &str,
                _c: &str,
                _ip: &str,
            ) -> Result<NodeSubnet> {
                anyhow::bail!("no leader currently elected; retry later")
            }
            async fn get_node_subnet(&self, _n: &str) -> Result<Option<NodeSubnet>> {
                Ok(None)
            }
            async fn list_peer_subnets(&self, _n: &str) -> Result<Vec<NodeSubnet>> {
                Ok(vec![])
            }
            async fn get_node_dataplane(&self, _n: &str) -> Result<Option<DataplanePeerMetadata>> {
                Ok(None)
            }
            async fn list_pod_cleanup_intents_for_node(
                &self,
                _node_name: &str,
            ) -> Result<Vec<PodCleanupIntent>> {
                Ok(vec![])
            }
            async fn delete_pod_cleanup_intent(
                &self,
                _node_name: &str,
                _namespace: &str,
                _pod_name: &str,
                _pod_uid: &str,
                _reason: &str,
            ) -> Result<()> {
                anyhow::bail!("no leader")
            }
            async fn get_cluster_membership(&self) -> Result<ClusterMembership> {
                anyhow::bail!("no leader")
            }
            async fn apply_outbox(
                &self,
                _k: &str,
                _o: OutboxOperation,
                _p: Bytes,
            ) -> std::result::Result<OutboxApplyResult, OutboxApplyError> {
                Err(OutboxApplyError::Retryable(
                    "no leader currently elected; retry later".to_string(),
                ))
            }
        }

        let local = RecordingApiClient::new("local");
        let remote: Arc<dyn LeaderApiClient> = Arc::new(NoLeaderRemote);
        let (_tx, rx) = watch::channel(false); // follower
        let proxy = LeaderProxyApiClient::new(local.clone(), remote, rx);

        // apply_outbox must surface the remote's Retryable, not panic
        // or fall back to local.
        let err = proxy
            .apply_outbox(
                "no-leader",
                OutboxOperation::PodStatus,
                Bytes::from_static(b"x"),
            )
            .await
            .expect_err("must surface no-leader error");
        match err {
            OutboxApplyError::Retryable(msg) => {
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
            .allocate_node_subnet("n", "10.0.0.0/16", "10.0.0.1")
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

    /// Object safety: `Arc<dyn LeaderApiClient>` must work for the
    /// proxy. Compile-time check via type ascription.
    #[test]
    fn leader_proxy_is_object_safe() {
        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        let (_tx, rx) = watch::channel(true);
        let proxy: Arc<dyn LeaderApiClient> =
            Arc::new(LeaderProxyApiClient::new(local, remote, rx));
        // Use the trait object so the compiler proves object safety.
        let _: &dyn LeaderApiClient = proxy.as_ref();
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
            .apply_outbox("boot", OutboxOperation::PodStatus, Bytes::from_static(b"x"))
            .await
            .expect_err("stub must refuse");
        match err {
            OutboxApplyError::Retryable(msg) => {
                assert!(msg.contains("cp2"), "msg must name this node: {msg}");
                assert!(
                    msg.contains("not yet wired"),
                    "msg must explain forwarder is unwired: {msg}"
                );
            }
            other => panic!("expected Retryable, got {other:?}"),
        }
        // Reads are open (return empty); they are served by the proxy's
        // local arm in production. The stub is only on the write path.
        assert!(
            stub.get_resource(ResourceKey {
                api_version: "v1".into(),
                kind: "Pod".into(),
                namespace: Some("default".into()),
                name: "x".into(),
            })
            .await
            .expect("read pass")
            .is_none()
        );
    }

    /// T6 step 4: open_leader composes the switching proxy correctly.
    /// This unit test exercises the same composition the bootstrap
    /// does: LocalApiClient (with the real is_leader_rx) + a stub
    /// remote + the same watch → LeaderProxyApiClient. It proves the
    /// boot-time wiring of the three pieces is sound at the type and
    /// dispatch level without standing up the full bootstrap.
    #[tokio::test]
    async fn bootstrap_style_proxy_composition_dispatches_correctly() {
        use crate::control_plane::client::local::LocalApiClient;
        let db = crate::datastore::test_support::in_memory().await;
        let (tx, rx) = watch::channel(true); // simulate seed cp1
        let local_real: Arc<dyn LeaderApiClient> =
            Arc::new(LocalApiClient::new(Arc::new(db), "cp1".into(), rx.clone()));
        let stub_remote: Arc<dyn LeaderApiClient> =
            Arc::new(StubRemoteForwarder::new("cp1".into()));
        let proxy = LeaderProxyApiClient::new(local_real, stub_remote, rx);

        // As leader: write reaches local, succeeds (no Pod precondition).
        let res = proxy
            .apply_outbox(
                "boot-1",
                OutboxOperation::PodStatus,
                pod_status_minimal_payload(),
            )
            .await;
        // The payload references a Pod that doesn't exist, so the
        // local arm returns a terminal NotFound — but the key point
        // is the call REACHED the local arm (would be Retryable from
        // the stub otherwise).
        match res {
            Err(OutboxApplyError::NotFound(_)) => {} // reached local, terminal
            Err(OutboxApplyError::Retryable(msg)) if msg.contains("not yet wired") => {
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
            .apply_outbox(
                "boot-2",
                OutboxOperation::PodStatus,
                pod_status_minimal_payload(),
            )
            .await
            .expect_err("non-leader write goes to stub remote");
        match err {
            OutboxApplyError::Retryable(msg) => {
                assert!(
                    msg.contains("not yet wired"),
                    "must hit the stub remote, got: {msg}"
                );
            }
            other => panic!("expected stub Retryable, got {other:?}"),
        }
    }

    fn pod_status_minimal_payload() -> Bytes {
        use crate::datastore::ResourcePreconditions;
        use crate::datastore::command::StorageCommand;
        use crate::kubelet::outbox::payload::OutboxPayload;
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
    /// single `watch::Receiver::borrow` plus an arc deref. This is a
    /// structural check that the impl above does no I/O on its own —
    /// dispatch happens inline.
    #[test]
    fn leader_proxy_holds_no_background_resources() {
        // The struct has exactly three fields: two Arcs + one watch
        // receiver. No supervisor, no spawn handle, no timer.
        // size_of asserts the layout has not silently grown.
        use std::mem::size_of;
        let three_arc_sized =
            size_of::<Arc<dyn LeaderApiClient>>() * 2 + size_of::<watch::Receiver<bool>>();
        assert!(
            size_of::<LeaderProxyApiClient>() <= three_arc_sized + 32,
            "LeaderProxyApiClient must stay a thin per-call dispatcher; \
             field growth probably introduced spawn / timer / supervisor state \
             that violates HR #1 (zero idle CPU). If a new field is justified, \
             update this bound."
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
    /// have a `LeaderProxyApiClient` as their `cluster_api`. The
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
        // production this is the leader's LocalApiClient → raft
        // proposer → raft → state-machine apply on every member.
        let leader_backend = RecordingApiClient::new("leader-shared");

        // Leader member: cluster_api proxy whose local arm IS the
        // shared leader backend. is_leader=true.
        let (_tx_l, rx_l) = watch::channel(true);
        let leader_proxy = LeaderProxyApiClient::new(
            leader_backend.clone(),
            RecordingApiClient::new("leader-unused-remote"),
            rx_l,
        );

        // Follower 1: cluster_api proxy whose REMOTE arm is the
        // shared leader backend (modeling the gRPC forward).
        // is_leader=false.
        let (_tx_f1, rx_f1) = watch::channel(false);
        let follower1_proxy = LeaderProxyApiClient::new(
            RecordingApiClient::new("f1-local-unused"),
            leader_backend.clone(),
            rx_f1,
        );

        // Follower 2: same shape as follower 1.
        let (_tx_f2, rx_f2) = watch::channel(false);
        let follower2_proxy = LeaderProxyApiClient::new(
            RecordingApiClient::new("f2-local-unused"),
            leader_backend.clone(),
            rx_f2,
        );

        // Each member issues one write. All three calls must reach
        // the shared leader backend.
        leader_proxy
            .apply_outbox(
                "leader-write",
                OutboxOperation::PodStatus,
                Bytes::from_static(b"x"),
            )
            .await
            .expect("leader write");
        follower1_proxy
            .apply_outbox(
                "f1-write",
                OutboxOperation::PodStatus,
                Bytes::from_static(b"x"),
            )
            .await
            .expect("follower1 write");
        follower2_proxy
            .apply_outbox(
                "f2-write",
                OutboxOperation::PodStatus,
                Bytes::from_static(b"x"),
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
        let (tx, rx) = watch::channel(false); // start as follower
        let proxy = Arc::new(LeaderProxyApiClient::new(local.clone(), remote.clone(), rx));

        // Capture the addresses of the underlying Arcs BEFORE
        // promotion to prove no reconstruction happens.
        let proxy_addr_before = Arc::as_ptr(&proxy) as *const () as usize;
        let local_addr_before = Arc::as_ptr(&local) as *const () as usize;
        let remote_addr_before = Arc::as_ptr(&remote) as *const () as usize;

        // Follower write → remote.
        proxy
            .apply_outbox("pre", OutboxOperation::PodStatus, Bytes::from_static(b"x"))
            .await
            .expect("pre");
        assert_eq!(remote.apply_outbox.load(Ordering::Relaxed), 1);
        assert_eq!(local.apply_outbox.load(Ordering::Relaxed), 0);

        // Promotion: pure state flip, no construction.
        tx.send(true).expect("promote");

        // Same proxy instance now dispatches writes to local.
        proxy
            .apply_outbox(
                "post-promote",
                OutboxOperation::PodStatus,
                Bytes::from_static(b"x"),
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
        let (tx, rx) = watch::channel(true); // start as leader
        let proxy = LeaderProxyApiClient::new(local.clone(), remote.clone(), rx);

        // As leader, writes go to local
        proxy
            .apply_outbox("key", OutboxOperation::PodStatus, Bytes::from_static(b"x"))
            .await
            .unwrap();
        assert_eq!(local.apply_outbox.load(Ordering::Relaxed), 1);
        assert_eq!(remote.apply_outbox.load(Ordering::Relaxed), 0);

        // Simulate leadership loss
        tx.send(false).unwrap();

        // After leadership loss, writes go to remote
        proxy
            .apply_outbox("key2", OutboxOperation::PodStatus, Bytes::from_static(b"y"))
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
}
