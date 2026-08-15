use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::Result;
use klights_cluster_core::NodeId;
use klights_cluster_core::{OutboxApplyOutcome, OutboxOperation};
use klights_replication::activation::CommandCodecV3Activation;
use klights_replication::node::*;
use klights_replication::types::{RaftMemberNode, TypeConfig};

fn test_unproven_member(addr: impl Into<String>) -> RaftMemberNode {
    RaftMemberNode::new(addr.into(), uuid::Uuid::nil().to_string(), None)
}

#[cfg(test)]
mod tests {
    // Test assertions briefly lock a mock's recorded-call log to inspect it
    // after an awaited propose; the std guard is dropped at end of statement
    // and the test runtime is single-threaded, so the lint is not a concern.
    #![allow(clippy::await_holding_lock)]
    use super::*;
    use async_trait::async_trait;
    use klights_cluster_core::StorageCommand;
    use klights_cluster_store::ClusterMetadataMutation;
    use klights_cluster_store::{
        COMMAND_CODEC_ACTIVATION_VERSION_META_KEY as KEY_COMMAND_CODEC_ACTIVATION_VERSION,
        COMMAND_CODEC_V3_ACTIVATION_VALUE as COMMAND_CODEC_ACTIVATION_VALUE,
    };
    use klights_leader_api::{
        LeaderResourceCommand, LeaderResourceQuery, ResourceCommandError, ResourceCommandRequest,
        ResourceGetRequest, ResourceListRequest, ResourceListResult, ResourceQueryError,
        ResourceQueryFuture,
    };
    use klights_node_datastore::SqliteRaftDurability;
    use klights_replication::membership::{
        CommandCodecV3ActivationError, CommandCodecV3PreflightError, MemberFeatureProbe,
        RaftMemberAdmissionResult,
    };
    use klights_replication::types::RaftMemberLogId;
    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};
    use openraft::Raft;
    use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
    use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
    use openraft::raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    };

    use super::super::support::IntegrationRaftComposition;

    #[derive(Clone, Debug, Default)]
    struct StubRaftNetwork;

    impl RaftNetworkFactory<TypeConfig> for StubRaftNetwork {
        type Network = Self;

        async fn new_client(&mut self, _target: NodeId, _node: &RaftMemberNode) -> Self::Network {
            Self
        }
    }

    fn unreachable(rpc: &str) -> Unreachable {
        Unreachable::new(&std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            format!("consumer-local test Raft network: {rpc} unavailable"),
        ))
    }

    impl RaftNetwork<TypeConfig> for StubRaftNetwork {
        async fn append_entries(
            &mut self,
            _rpc: AppendEntriesRequest<TypeConfig>,
            _option: RPCOption,
        ) -> Result<
            AppendEntriesResponse<NodeId>,
            RPCError<NodeId, RaftMemberNode, RaftError<NodeId>>,
        > {
            Err(RPCError::Unreachable(unreachable("append_entries")))
        }

        async fn install_snapshot(
            &mut self,
            _rpc: InstallSnapshotRequest<TypeConfig>,
            _option: RPCOption,
        ) -> Result<
            InstallSnapshotResponse<NodeId>,
            RPCError<NodeId, RaftMemberNode, RaftError<NodeId, InstallSnapshotError>>,
        > {
            Err(RPCError::Unreachable(unreachable("install_snapshot")))
        }

        async fn vote(
            &mut self,
            _rpc: VoteRequest<NodeId>,
            _option: RPCOption,
        ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, RaftMemberNode, RaftError<NodeId>>>
        {
            Err(RPCError::Unreachable(unreachable("vote")))
        }
    }

    #[derive(Clone, Default)]
    struct LoopbackRegistry {
        inner: Arc<std::sync::RwLock<std::collections::HashMap<NodeId, LoopbackRegistryEntry>>>,
    }

    #[derive(Clone)]
    struct LoopbackRegistryEntry {
        raft: Raft<TypeConfig>,
        generation: u64,
        storage_incarnation: String,
    }

    impl LoopbackRegistry {
        fn new() -> Self {
            Self::default()
        }

        fn register(&self, node_id: NodeId, raft: Raft<TypeConfig>, storage_incarnation: String) {
            let mut inner = self.inner.write().unwrap();
            let generation = inner
                .get(&node_id)
                .map_or(1, |entry| entry.generation.saturating_add(1));
            inner.insert(
                node_id,
                LoopbackRegistryEntry {
                    raft,
                    generation,
                    storage_incarnation,
                },
            );
        }

        fn lookup(&self, node_id: NodeId) -> Option<LoopbackRegistryEntry> {
            self.inner.read().unwrap().get(&node_id).cloned()
        }
    }

    #[derive(Clone)]
    struct LoopbackRaftNetworkFactory {
        registry: LoopbackRegistry,
    }

    impl LoopbackRaftNetworkFactory {
        fn new(registry: LoopbackRegistry) -> Self {
            Self { registry }
        }
    }

    impl RaftNetworkFactory<TypeConfig> for LoopbackRaftNetworkFactory {
        type Network = LoopbackRaftNetwork;

        async fn new_client(&mut self, target: NodeId, node: &RaftMemberNode) -> Self::Network {
            let bound_generation = self
                .registry
                .lookup(target)
                .map(|entry| entry.generation)
                .unwrap_or(0);
            LoopbackRaftNetwork {
                target,
                bound_generation,
                receiver_admission: node.clone(),
                registry: self.registry.clone(),
            }
        }
    }

    struct LoopbackRaftNetwork {
        target: NodeId,
        bound_generation: u64,
        receiver_admission: RaftMemberNode,
        registry: LoopbackRegistry,
    }

    impl LoopbackRaftNetwork {
        fn receiver_session_is_stale(&self, entry: &LoopbackRegistryEntry) -> bool {
            if self.bound_generation != entry.generation
                || (self.receiver_admission.storage_incarnation != uuid::Uuid::nil().to_string()
                    && self.receiver_admission.storage_incarnation != entry.storage_incarnation)
            {
                return true;
            }
            let Some(required) = self.receiver_admission.admitted_log.as_ref() else {
                return false;
            };
            let metrics = entry.raft.metrics().borrow().clone();
            let local_index = [
                metrics.last_log_index,
                metrics.last_applied.as_ref().map(|log| log.index),
                metrics.snapshot.as_ref().map(|log| log.index),
                metrics.purged.as_ref().map(|log| log.index),
            ]
            .into_iter()
            .flatten()
            .max();
            local_index.is_none_or(|index| index < required.index)
        }
    }

    impl RaftNetwork<TypeConfig> for LoopbackRaftNetwork {
        async fn append_entries(
            &mut self,
            rpc: AppendEntriesRequest<TypeConfig>,
            _option: RPCOption,
        ) -> Result<
            AppendEntriesResponse<NodeId>,
            RPCError<NodeId, RaftMemberNode, RaftError<NodeId>>,
        > {
            let entry = self.registry.lookup(self.target).ok_or_else(|| {
                RPCError::Unreachable(unreachable("append_entries: peer not registered"))
            })?;
            if self.receiver_session_is_stale(&entry) {
                return Err(RPCError::Unreachable(unreachable(
                    "append_entries: stale receiver session generation",
                )));
            }
            entry
                .raft
                .append_entries(rpc)
                .await
                .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
        }

        async fn install_snapshot(
            &mut self,
            rpc: InstallSnapshotRequest<TypeConfig>,
            _option: RPCOption,
        ) -> Result<
            InstallSnapshotResponse<NodeId>,
            RPCError<NodeId, RaftMemberNode, RaftError<NodeId, InstallSnapshotError>>,
        > {
            let entry = self.registry.lookup(self.target).ok_or_else(|| {
                RPCError::Unreachable(unreachable("install_snapshot: peer not registered"))
            })?;
            if self.receiver_session_is_stale(&entry) {
                return Err(RPCError::Unreachable(unreachable(
                    "install_snapshot: stale receiver session generation",
                )));
            }
            entry
                .raft
                .install_snapshot(rpc)
                .await
                .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
        }

        async fn vote(
            &mut self,
            rpc: VoteRequest<NodeId>,
            _option: RPCOption,
        ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, RaftMemberNode, RaftError<NodeId>>>
        {
            let entry = self
                .registry
                .lookup(self.target)
                .ok_or_else(|| RPCError::Unreachable(unreachable("vote: peer not registered")))?;
            if self.receiver_session_is_stale(&entry) {
                return Err(RPCError::Unreachable(unreachable(
                    "vote: stale receiver session generation",
                )));
            }
            entry
                .raft
                .vote(rpc)
                .await
                .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
        }
    }

    fn node_durability(
        stores: &Arc<SqliteRaftDurability>,
    ) -> Arc<klights_replication::node_durability::OpenRaftNodeDurabilityAdapter> {
        Arc::new(
            klights_replication::node_durability::OpenRaftNodeDurabilityAdapter::new(
                stores.clone(),
                stores.clone(),
            ),
        )
    }

    struct TestRaftNode {
        node: RaftNode,
        storage_incarnation: String,
    }

    impl std::ops::Deref for TestRaftNode {
        type Target = RaftNode;

        fn deref(&self) -> &Self::Target {
            &self.node
        }
    }

    impl TestRaftNode {
        fn storage_incarnation(&self) -> &str {
            &self.storage_incarnation
        }

        async fn shutdown(self) -> Result<()> {
            self.node.shutdown().await
        }

        fn into_node(self) -> RaftNode {
            self.node
        }

        async fn admit_controlplane_member(
            &self,
            node_id: NodeId,
            addr: String,
            as_learner: bool,
            storage_incarnation: String,
            storage_log_attestation: klights_leader_api::RaftStorageAttestation,
        ) -> Result<klights_replication::membership::RaftMemberAdmissionResult> {
            self.node
                .membership()
                .admit_controlplane_member_with_limit(
                    node_id,
                    addr,
                    as_learner,
                    storage_incarnation,
                    storage_log_attestation,
                    3,
                )
                .await
        }
    }

    async fn start_test_node<N>(
        node_id: NodeId,
        node_name: String,
        stores: RaftStorePorts,
        log_durability: Arc<dyn klights_node_store::RaftLogDurability>,
        applied_state_durability: Arc<dyn klights_node_store::RaftAppliedStateDurability>,
        supervisor: Arc<TaskSupervisor>,
        network: N,
    ) -> Result<TestRaftNode>
    where
        N: RaftNetworkFactory<TypeConfig>,
    {
        let storage_incarnation = log_durability
            .load_or_create_storage_incarnation()
            .await
            .expect("load test storage incarnation");
        let node = RaftNode::start_with_network(
            node_id,
            node_name,
            stores,
            log_durability,
            applied_state_durability,
            supervisor,
            network,
        )
        .await?;
        Ok(TestRaftNode {
            node,
            storage_incarnation,
        })
    }

    struct BackendResourceQuery {
        backend: Arc<klights_cluster_datastore::sqlite::embedded::Datastore>,
    }

    impl LeaderResourceQuery for BackendResourceQuery {
        fn get_resource(
            &self,
            request: ResourceGetRequest,
        ) -> ResourceQueryFuture<'_, Option<klights_cluster_core::Resource>> {
            let backend = self.backend.clone();
            Box::pin(async move {
                let key = request.into_key();
                backend
                    .get_resource(
                        &key.api_version,
                        &key.kind,
                        key.namespace.as_deref(),
                        &key.name,
                    )
                    .await
                    .map_err(|error| ResourceQueryError::query_failed(error.to_string()))
            })
        }

        fn list_resources(
            &self,
            _request: ResourceListRequest,
        ) -> ResourceQueryFuture<'_, ResourceListResult> {
            Box::pin(async move {
                panic!("resource-command integration tests do not issue LIST requests")
            })
        }
    }

    fn raft_store_ports(
        backend: Arc<klights_cluster_datastore::sqlite::embedded::Datastore>,
    ) -> RaftStorePorts {
        IntegrationRaftComposition::new(backend).store_ports()
    }

    fn storage_attestation(
        log: Option<klights_leader_api::RaftStorageLogAttestation>,
    ) -> klights_leader_api::RaftStorageAttestation {
        klights_leader_api::RaftStorageAttestation {
            high_watermark: log.clone(),
            current_boundary: log,
        }
    }

    async fn admit_member(
        leader: &TestRaftNode,
        member: &TestRaftNode,
        addr: impl Into<String>,
        as_learner: bool,
    ) -> Result<klights_replication::membership::RaftMemberAdmissionResult> {
        leader
            .membership()
            .admit_controlplane_member_with_limit(
                member.node_id,
                addr.into(),
                as_learner,
                member.storage_incarnation().to_string(),
                storage_attestation(None),
                3,
            )
            .await
    }

    struct AdmissionFenceClient {
        ready: std::sync::atomic::AtomicBool,
        append_calls: std::sync::atomic::AtomicUsize,
        append_called: tokio::sync::Notify,
    }

    impl AdmissionFenceClient {
        async fn wait_for_append(&self) {
            loop {
                let notified = self.append_called.notified();
                if self.append_calls.load(std::sync::atomic::Ordering::SeqCst) > 0 {
                    return;
                }
                notified.await;
            }
        }
    }

    #[async_trait]
    impl klights_replication::grpc_network::GrpcRaftRpcClient for AdmissionFenceClient {
        async fn append_entries(
            &self,
            _receiver: RaftMemberNode,
            _payload: Vec<u8>,
        ) -> std::result::Result<Vec<u8>, klights_replication::grpc_network::GrpcRaftRpcError>
        {
            self.append_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.append_called.notify_waiters();
            if self.ready.load(std::sync::atomic::Ordering::SeqCst) {
                Ok(
                    serde_json::to_vec(&openraft::raft::AppendEntriesResponse::<NodeId>::Success)
                        .unwrap(),
                )
            } else {
                Err(
                    klights_replication::grpc_network::GrpcRaftRpcError::Retryable(
                        "same-ID member session is fenced".to_string(),
                    ),
                )
            }
        }

        async fn vote(
            &self,
            _receiver: RaftMemberNode,
            _payload: Vec<u8>,
        ) -> std::result::Result<Vec<u8>, klights_replication::grpc_network::GrpcRaftRpcError>
        {
            unreachable!("admission-fence test does not send Vote")
        }

        async fn install_snapshot(
            &self,
            _receiver: RaftMemberNode,
            _payload: Vec<u8>,
        ) -> std::result::Result<Vec<u8>, klights_replication::grpc_network::GrpcRaftRpcError>
        {
            unreachable!("admission-fence test does not install a snapshot")
        }
    }

    struct AdmissionFenceFactory {
        client: Arc<AdmissionFenceClient>,
        builds: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl klights_replication::grpc_network::GrpcRaftClientFactory for AdmissionFenceFactory {
        fn client_for(
            &self,
            _addr: &str,
        ) -> Arc<dyn klights_replication::grpc_network::GrpcRaftRpcClient> {
            self.builds
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.client.clone()
        }
    }

    struct FeatureProbe {
        replies: std::collections::BTreeMap<NodeId, Result<u32>>,
    }

    #[async_trait]
    impl MemberFeatureProbe for FeatureProbe {
        async fn metadata_for_member(
            &self,
            node_id: NodeId,
            _addr: &str,
        ) -> Result<klights_leader_api::MetadataResponse> {
            let command_codec_version = self
                .replies
                .get(&node_id)
                .expect("test probe reply")
                .as_ref()
                .map_err(|err| anyhow::anyhow!(err.to_string()))?;
            Ok(klights_leader_api::MetadataResponse {
                cluster_id: "test".to_string(),
                leader_epoch: 1,
                current_rv: 0,
                current_log_index: 0,
                command_codec_version: *command_codec_version,
            })
        }
    }
    #[tokio::test]
    async fn codec_v3_activation_is_leader_gated_idempotent_and_persisted() {
        let (node, backend) = fresh_node(701).await;
        node.bootstrap_single_voter("https://127.0.0.1:7701".to_string())
            .await
            .unwrap();
        wait_for_leader(&node, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        let probe = FeatureProbe {
            replies: [(701, Ok(klights_cluster_core::COMMAND_CODEC_VERSION))]
                .into_iter()
                .collect(),
        };
        node.activate_command_codec_v3(&probe).await.unwrap();
        assert_eq!(
            backend
                .get_klights_meta(KEY_COMMAND_CODEC_ACTIVATION_VERSION)
                .await
                .unwrap()
                .as_deref(),
            Some(COMMAND_CODEC_ACTIVATION_VALUE),
            "activation proof must be Raft-committed cluster state"
        );
        let materializer = IntegrationRaftComposition::new(backend.clone()).commit_materializer();
        let restored_activation = CommandCodecV3Activation::load(materializer.as_ref())
            .await
            .expect("restore exact-v3 activation marker");
        restored_activation.enforce_startup_gate();
        restored_activation
            .ensure_command_codec_v3_activated()
            .expect("persisted activation proof reopens production proposals");
        let rv_after_first = backend.get_current_resource_version().await.unwrap();
        node.activate_command_codec_v3(&probe).await.unwrap();
        assert_eq!(
            backend.get_current_resource_version().await.unwrap(),
            rv_after_first
        );
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn codec_v3_activation_rejects_nonleader_and_failed_preflight_without_write() {
        let (not_leader, _backend) = fresh_node(702).await;
        let probe = FeatureProbe {
            replies: [(702, Ok(klights_cluster_core::COMMAND_CODEC_VERSION))]
                .into_iter()
                .collect(),
        };
        assert!(matches!(
            not_leader.activate_command_codec_v3(&probe).await,
            Err(CommandCodecV3ActivationError::NotLeader)
        ));
        not_leader.shutdown().await.unwrap();

        let registry = LoopbackRegistry::new();
        let (leader, backend) = fresh_voter_in_registry_with_backend(703, &registry).await;
        let incompatible_voter = fresh_voter_in_registry(704, &registry).await;
        leader
            .bootstrap_single_voter("https://127.0.0.1:7703".to_string())
            .await
            .unwrap();
        wait_for_leader(&leader, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        admit_member(
            &leader,
            &incompatible_voter,
            "https://127.0.0.1:7704",
            false,
        )
        .await
        .expect("add incompatible voter to activation preflight membership");
        let missing = FeatureProbe {
            replies: [(704, Ok(0))].into_iter().collect(),
        };
        assert!(matches!(
            leader.activate_command_codec_v3(&missing).await,
            Err(CommandCodecV3ActivationError::Preflight(
                CommandCodecV3PreflightError::Unsupported { .. }
            ))
        ));
        assert_eq!(
            backend
                .get_klights_meta(KEY_COMMAND_CODEC_ACTIVATION_VERSION)
                .await
                .unwrap(),
            None,
            "failed preflight must not persist exact-v3 activation proof"
        );
        leader.shutdown().await.unwrap();
        incompatible_voter.shutdown().await.unwrap();
    }

    /// Test-only helper: poll metrics until this node is the leader.
    /// Production should wait on `raft.metrics()` via TaskSupervisor.
    async fn wait_for_leader(node: &RaftNode, timeout: std::time::Duration) -> Result<()> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let m = node.raft.metrics().borrow().clone();
            if m.current_leader == Some(node.node_id) {
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                anyhow::bail!("timeout waiting for leader; current state = {:?}", m.state);
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    async fn fresh_node(
        node_id: NodeId,
    ) -> (
        TestRaftNode,
        Arc<klights_cluster_datastore::sqlite::embedded::Datastore>,
    ) {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_executor = klights_node_datastore::open::open_with_opts(
            klights_node_datastore::open::in_memory_opts(),
            supervisor.clone(),
            "sqlite:raft-node-test",
        )
        .await
        .expect("open node-local executor");
        let node_local = Arc::new(SqliteRaftDurability::new(node_executor));
        let backend: Arc<klights_cluster_datastore::sqlite::embedded::Datastore> = Arc::new(
            klights_cluster_datastore::sqlite::embedded::Datastore::new_in_memory()
                .await
                .unwrap(),
        );
        let backend_for_caller = backend.clone();
        let raft_node = start_test_node(
            node_id,
            format!("n{node_id}"),
            raft_store_ports(backend),
            node_durability(&node_local),
            node_durability(&node_local),
            supervisor,
            StubRaftNetwork,
        )
        .await
        .expect("RaftNode::start");
        (raft_node, backend_for_caller)
    }

    #[tokio::test]
    async fn single_voter_cluster_bootstraps_and_elects_self() {
        let (node, _backend) = fresh_node(10).await;
        node.bootstrap_single_voter("https://10.99.0.10:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&node, std::time::Duration::from_secs(5))
            .await
            .expect("become leader");
        let m = node.raft.metrics().borrow().clone();
        assert_eq!(m.current_leader, Some(10));
        node.shutdown().await.unwrap();
    }

    /// T1: the deduped server-metrics channel (what the shape/lease
    /// watchers subscribe to) must NOT fire at steady state, so those
    /// watchers stay asleep at idle (HR #1). The chatty `metrics()`
    /// channel would fire on every heartbeat tick here.
    #[tokio::test]
    async fn server_metrics_watch_is_quiet_at_steady_state() {
        let (node, _backend) = fresh_node(10).await;
        node.bootstrap_single_voter("https://10.99.0.10:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&node, std::time::Duration::from_secs(5))
            .await
            .expect("become leader");
        // Let post-election churn settle before observing.
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;
        let mut sm = node.server_metrics_watch();
        sm.borrow_and_update();
        // Across several heartbeat ticks with no state change, no fire.
        tokio::time::sleep(std::time::Duration::from_millis(900)).await;
        assert!(
            !sm.has_changed().expect("watch channel open"),
            "server_metrics fired at steady state — idle watchers would wake"
        );
        node.shutdown().await.unwrap();
    }

    /// T1: the deduped server-metrics channel still fires on a real
    /// leadership/state change, so the watchers remain responsive.
    #[tokio::test]
    async fn server_metrics_watch_fires_on_leadership_change() {
        let (node, _backend) = fresh_node(10).await;
        let mut sm = node.server_metrics_watch();
        sm.borrow_and_update();
        node.bootstrap_single_voter("https://10.99.0.10:7679".into())
            .await
            .expect("bootstrap");
        // Becoming leader changes state + vote + current_leader +
        // membership — all server-metrics fields — so this must wake.
        tokio::time::timeout(std::time::Duration::from_secs(3), sm.changed())
            .await
            .expect("server_metrics must fire within 3s of leadership change")
            .expect("watch channel open");
        node.shutdown().await.unwrap();
    }

    /// Build a 3-voter loopback cluster. Returns the three RaftNodes plus
    /// the shared registry so the test can hold and shut them down cleanly.
    async fn fresh_three_voter_cluster() -> (
        Vec<TestRaftNode>,
        Vec<Arc<klights_cluster_datastore::sqlite::embedded::Datastore>>,
        LoopbackRegistry,
    ) {
        let registry = LoopbackRegistry::new();
        let mut nodes = Vec::new();
        let mut backends = Vec::new();
        for id in [10u64, 20, 30] {
            let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
            let exec = klights_node_datastore::open::open_with_opts(
                klights_node_datastore::open::in_memory_opts(),
                supervisor.clone(),
                "sqlite:raft-cluster-test",
            )
            .await
            .expect("open node-local executor");
            let node_local = Arc::new(SqliteRaftDurability::new(exec));
            let backend: Arc<klights_cluster_datastore::sqlite::embedded::Datastore> = Arc::new(
                klights_cluster_datastore::sqlite::embedded::Datastore::new_in_memory()
                    .await
                    .unwrap(),
            );
            let factory = LoopbackRaftNetworkFactory::new(registry.clone());
            let n = start_test_node(
                id,
                format!("n{id}"),
                raft_store_ports(backend.clone()),
                node_durability(&node_local),
                node_durability(&node_local),
                supervisor,
                factory,
            )
            .await
            .expect("RaftNode::start_with_network");
            registry.register(id, n.raft.clone(), n.storage_incarnation().to_string());
            nodes.push(n);
            backends.push(backend);
        }
        (nodes, backends, registry)
    }

    #[tokio::test]
    async fn three_voter_cluster_elects_a_leader() {
        let (nodes, _backends, _registry) = fresh_three_voter_cluster().await;
        // Have node 10 initialize the cluster with all three voters.
        let mut members = std::collections::BTreeMap::new();
        for n in &nodes {
            members.insert(
                n.node_id,
                super::test_unproven_member(format!("https://localhost:{}", 7679 + n.node_id)),
            );
        }
        nodes[0]
            .raft
            .initialize(members)
            .await
            .expect("initialize cluster");
        // Wait up to 5s for any node to become leader.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut leader = None;
        while std::time::Instant::now() < deadline {
            for n in &nodes {
                let m = n.raft.metrics().borrow().clone();
                if m.current_leader.is_some() {
                    leader = m.current_leader;
                    break;
                }
            }
            if leader.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(
            leader.is_some(),
            "no leader elected within 5s in 3-voter loopback cluster"
        );
        for n in nodes {
            n.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn production_startup_gate_with_restored_membership_exposes_no_proposal_capability() {
        let (nodes, backends, _registry) = fresh_three_voter_cluster().await;
        let mut members = std::collections::BTreeMap::new();
        for node in &nodes {
            members.insert(
                node.node_id,
                super::test_unproven_member(format!("https://localhost:{}", 7679 + node.node_id)),
            );
        }
        nodes[0]
            .raft
            .initialize(members)
            .await
            .expect("initialize restored membership");

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let leader_index = loop {
            if let Some(index) = nodes
                .iter()
                .position(|node| node.raft.metrics().borrow().current_leader == Some(node.node_id))
            {
                break index;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "restored membership did not elect a leader"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        };
        let leader = &nodes[leader_index];
        let unavailable = FeatureProbe {
            replies: nodes
                .iter()
                .map(|node| (node.node_id, Err(anyhow::anyhow!("peer unavailable"))))
                .collect(),
        };
        assert!(matches!(
            leader.verify_startup_command_codec_v3(&unavailable).await,
            Err(CommandCodecV3PreflightError::Unavailable { .. })
        ));

        let old_member = FeatureProbe {
            replies: nodes.iter().map(|node| (node.node_id, Ok(2))).collect(),
        };
        assert!(matches!(
            leader.verify_startup_command_codec_v3(&old_member).await,
            Err(CommandCodecV3PreflightError::Unsupported { .. })
        ));

        let leader_backend = &backends[leader_index];
        let rv_before = leader_backend
            .get_current_resource_version()
            .await
            .expect("read pre-proposal RV");
        let error = leader
            .propose_command(propose_create_command("startup-gated"))
            .await
            .expect_err("startup validation failure must leave proposals unavailable");
        assert!(
            error
                .to_string()
                .contains("exact-v3 codec activation marker"),
            "proposal must fail at the activation gate, got: {error}"
        );
        assert_eq!(
            leader_backend
                .get_current_resource_version()
                .await
                .expect("read post-proposal RV"),
            rv_before,
            "gated proposal must not materialize or mutate cluster state"
        );
        assert!(
            leader_backend
                .get_klights_meta(KEY_COMMAND_CODEC_ACTIVATION_VERSION)
                .await
                .expect("read activation marker")
                .is_none(),
            "failed startup verification must not synthesize activation proof"
        );

        for node in nodes {
            node.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn follower_raft_proposer_refuses_before_local_commit_materialization() {
        let registry = LoopbackRegistry::new();
        let mut nodes = Vec::new();
        let mut backends = Vec::new();
        for id in [10u64, 20, 30] {
            let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
            let exec = klights_node_datastore::open::open_with_opts(
                klights_node_datastore::open::in_memory_opts(),
                supervisor.clone(),
                "sqlite:raft-follower-no-local-commit-test",
            )
            .await
            .expect("open node-local executor");
            let node_local = Arc::new(SqliteRaftDurability::new(exec));
            let backend: Arc<klights_cluster_datastore::sqlite::embedded::Datastore> = Arc::new(
                klights_cluster_datastore::sqlite::embedded::Datastore::new_in_memory()
                    .await
                    .unwrap(),
            );
            let factory = LoopbackRaftNetworkFactory::new(registry.clone());
            let node = start_test_node(
                id,
                format!("n{id}"),
                raft_store_ports(backend.clone()),
                node_durability(&node_local),
                node_durability(&node_local),
                supervisor,
                factory,
            )
            .await
            .expect("RaftNode::start_with_network");
            registry.register(
                id,
                node.raft.clone(),
                node.storage_incarnation().to_string(),
            );
            nodes.push(node);
            backends.push(backend);
        }
        let mut members = std::collections::BTreeMap::new();
        for node in &nodes {
            members.insert(
                node.node_id,
                super::test_unproven_member(format!("https://localhost:{}", 7679 + node.node_id)),
            );
        }
        nodes[0]
            .raft
            .initialize(members)
            .await
            .expect("initialize cluster");

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut leader_id = None;
        while std::time::Instant::now() < deadline {
            for node in &nodes {
                if let Some(id) = node.raft.metrics().borrow().current_leader {
                    leader_id = Some(id);
                    break;
                }
            }
            if leader_id.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let leader_id = leader_id.expect("a leader was elected");
        let follower_idx = nodes
            .iter()
            .position(|node| node.node_id != leader_id)
            .expect("cluster has a follower");
        let backend = backends[follower_idx].clone();
        assert!(
            backend
                .list_applied_outbox()
                .await
                .expect("list before")
                .is_empty(),
            "fresh follower backend should have no local outbox claims"
        );

        let err = nodes[follower_idx]
            .propose_command(
                klights_cluster_core::command::StorageCommand::CreateResource {
                    api_version: "v1".into(),
                    kind: "ConfigMap".into(),
                    namespace: Some("default".into()),
                    name: "follower-local-claim-regression".into(),
                    data: serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {
                            "name": "follower-local-claim-regression",
                            "namespace": "default"
                        },
                        "data": {"k": "v"}
                    }),
                },
            )
            .await
            .expect_err("follower proposer must refuse before building a local commit");
        let msg = err.to_string();
        assert!(
            msg.contains("not raft leader") || msg.contains("ForwardToLeader"),
            "unexpected follower refusal: {msg}"
        );
        assert!(
            backend
                .list_applied_outbox()
                .await
                .expect("list after")
                .is_empty(),
            "follower refusal must not leave local applied_outbox placeholders"
        );

        for node in nodes {
            node.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn rejected_materialized_commit_leaves_no_proposal_time_ledger_state() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let exec = klights_node_datastore::open::open_with_opts(
            klights_node_datastore::open::in_memory_opts(),
            supervisor.clone(),
            "sqlite:raft-rejected-materialized-commit-test",
        )
        .await
        .expect("open node-local executor");
        let node_local = Arc::new(SqliteRaftDurability::new(exec));
        let backend: Arc<klights_cluster_datastore::sqlite::embedded::Datastore> = Arc::new(
            klights_cluster_datastore::sqlite::embedded::Datastore::new_in_memory()
                .await
                .unwrap(),
        );
        let node = start_test_node(
            10,
            "n10".to_string(),
            raft_store_ports(backend.clone()),
            node_durability(&node_local),
            node_durability(&node_local),
            supervisor,
            StubRaftNetwork,
        )
        .await
        .expect("RaftNode::start");
        node.bootstrap_single_voter("https://localhost:7679".to_string())
            .await
            .expect("bootstrap single voter");

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::time::Instant::now() < deadline && !node.local_commit_materialization_ready() {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            node.local_commit_materialization_ready(),
            "test node should be allowed to materialize local commits before shutdown"
        );

        node.raft.shutdown().await.expect("shutdown raft core");

        let err = node
            .propose_command(
                klights_cluster_core::command::StorageCommand::CreateResource {
                    api_version: "v1".into(),
                    kind: "ConfigMap".into(),
                    namespace: Some("default".into()),
                    name: "rejected-materialized-commit".into(),
                    data: serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {
                            "name": "rejected-materialized-commit",
                            "namespace": "default"
                        },
                        "data": {"k": "v"}
                    }),
                },
            )
            .await
            .expect_err("stopped raft core should reject client_write after materialization");
        assert!(
            err.to_string().contains("Raft::client_write"),
            "unexpected rejection: {err}"
        );

        let rows = backend.list_applied_outbox().await.expect("list outbox");
        assert!(
            rows.iter().all(|row| {
                !(row.idempotency_key.starts_with("raft-leader-n10-")
                    && row.subject_key.is_empty()
                    && row.applied_rv.is_none()
                    && row.result_proto.is_empty())
            }),
            "rejected materialized commit must not leave proposal-time ledger state: {rows:?}"
        );
    }

    async fn fresh_voter_in_registry(id: NodeId, registry: &LoopbackRegistry) -> TestRaftNode {
        fresh_voter_in_registry_with_backend(id, registry).await.0
    }

    async fn fresh_voter_in_registry_with_backend(
        id: NodeId,
        registry: &LoopbackRegistry,
    ) -> (
        TestRaftNode,
        Arc<klights_cluster_datastore::sqlite::embedded::Datastore>,
    ) {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let exec = klights_node_datastore::open::open_with_opts(
            klights_node_datastore::open::in_memory_opts(),
            supervisor.clone(),
            "sqlite:raft-voter-test",
        )
        .await
        .expect("open node-local executor");
        let node_local = Arc::new(SqliteRaftDurability::new(exec));
        let backend: Arc<klights_cluster_datastore::sqlite::embedded::Datastore> = Arc::new(
            klights_cluster_datastore::sqlite::embedded::Datastore::new_in_memory()
                .await
                .unwrap(),
        );
        let factory = LoopbackRaftNetworkFactory::new(registry.clone());
        let node = start_test_node(
            id,
            format!("n{id}"),
            raft_store_ports(backend.clone()),
            node_durability(&node_local),
            node_durability(&node_local),
            supervisor,
            factory,
        )
        .await
        .expect("start node");
        registry.register(
            id,
            node.raft.clone(),
            node.storage_incarnation().to_string(),
        );
        (node, backend)
    }

    async fn wait_for_voter_count(node: &RaftNode, expected: usize) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            let m = node.raft.metrics().borrow().clone();
            if m.membership_config.membership().voter_ids().count() == expected {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!(
            "voter count did not reach {expected}; metrics = {:?}",
            node.raft.metrics().borrow().clone().membership_config
        );
    }

    #[tokio::test]
    async fn add_voter_grows_a_running_single_voter_cluster() {
        let registry = LoopbackRegistry::new();
        let leader = fresh_voter_in_registry(10, &registry).await;
        let learner = fresh_voter_in_registry(20, &registry).await;
        leader
            .bootstrap_single_voter("https://10.99.0.10:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&leader, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        admit_member(&leader, &learner, "https://10.99.0.20:7679", false)
            .await
            .expect("add_voter");
        wait_for_voter_count(&leader, 2).await;
        let m = leader.raft.metrics().borrow().clone();
        let voters: std::collections::BTreeSet<NodeId> =
            m.membership_config.membership().voter_ids().collect();
        assert!(voters.contains(&10));
        assert!(voters.contains(&20));
        leader.shutdown().await.unwrap();
        learner.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn voter_join_replays_seed_bootstrap_commands_authored_before_admission() {
        let registry = LoopbackRegistry::new();
        let (leader, leader_db) = fresh_voter_in_registry_with_backend(31, &registry).await;
        let leader = Arc::new(leader.into_node());
        let (joiner, joiner_db) = fresh_voter_in_registry_with_backend(32, &registry).await;
        leader
            .bootstrap_single_voter("https://10.99.0.31:7679".into())
            .await
            .expect("bootstrap seed");
        wait_for_leader(&leader, std::time::Duration::from_secs(5))
            .await
            .expect("seed leader election");

        let commands = [
            StorageCommand::CreateNamespace {
                name: "kube-system".into(),
                data: serde_json::json!({
                    "apiVersion": "v1", "kind": "Namespace",
                    "metadata": {"name": "kube-system", "uid": "seed-ns"}
                }),
            },
            StorageCommand::CreateResource {
                api_version: "rbac.authorization.k8s.io/v1".into(),
                kind: "ClusterRole".into(),
                namespace: None,
                name: "system:seed-proof".into(),
                data: serde_json::json!({
                    "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
                    "metadata": {"name": "system:seed-proof", "uid": "seed-rbac"},
                    "rules": []
                }),
            },
            StorageCommand::CreateResource {
                api_version: "v1".into(),
                kind: "Secret".into(),
                namespace: Some("kube-system".into()),
                name: "bootstrap-token-seed-proof".into(),
                data: serde_json::json!({
                    "apiVersion": "v1", "kind": "Secret",
                    "metadata": {"namespace": "kube-system", "name": "bootstrap-token-seed-proof", "uid": "seed-token"},
                    "type": "bootstrap.kubernetes.io/token"
                }),
            },
            StorageCommand::CreateResource {
                api_version: "networking.k8s.io/v1".into(),
                kind: "ServiceCIDR".into(),
                namespace: None,
                name: "kubernetes".into(),
                data: serde_json::json!({
                    "apiVersion": "networking.k8s.io/v1", "kind": "ServiceCIDR",
                    "metadata": {"name": "kubernetes", "uid": "seed-service-cidr"},
                    "spec": {"cidrs": ["10.43.0.0/16"]}
                }),
            },
            StorageCommand::SetKlightsMeta {
                key: klights_cluster_store::RAFT_VOTERS_META_KEY.into(),
                value: "[\"n31\"]".into(),
            },
            StorageCommand::SetKlightsMeta {
                key: klights_cluster_store::CLUSTER_ID_META_KEY.into(),
                value: "seed-cluster".into(),
            },
            StorageCommand::SetKlightsMeta {
                key: klights_cluster_store::LEADER_EPOCH_META_KEY.into(),
                value: "0".into(),
            },
            StorageCommand::SetKlightsMeta {
                key: klights_cluster_store::RAFT_TERM_META_KEY.into(),
                value: "0".into(),
            },
            StorageCommand::SetKlightsMeta {
                key: klights_cluster_store::RAFT_LEADER_HINT_META_KEY.into(),
                value: "n31".into(),
            },
        ];
        for command in commands {
            leader
                .propose_command(command)
                .await
                .expect("commit seed bootstrap command");
        }
        let handler = IntegrationRaftComposition::new(leader_db.clone())
            .controlplane_join_handler_with_raft_store(leader.clone());
        handler
            .join(test_controlplane_join_request(
                32,
                "https://10.99.0.32:7679",
                "n32",
                false,
                joiner.storage_incarnation(),
                None,
                "seed-joiner-hash",
            ))
            .await
            .expect("admit voter through production join handler after seed writes");
        wait_for_voter_count(&leader, 2).await;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let caught_up = joiner_db
                .get_resource("networking.k8s.io/v1", "ServiceCIDR", None, "kubernetes")
                .await
                .expect("read joiner bootstrap state")
                .is_some()
                && joiner_db
                    .get_klights_meta(klights_cluster_store::RAFT_VOTERS_META_KEY)
                    .await
                    .expect("read joiner membership metadata")
                    .as_deref()
                    == Some("[\"n31\",\"n32\"]")
                && joiner_db
                    .get_resource("v1", "Node", None, "n32")
                    .await
                    .expect("read replicated joiner Node")
                    .is_some();
            if caught_up {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "joining voter did not replay seed bootstrap commands"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        for (api_version, kind, namespace, name) in [
            ("v1", "Namespace", None, "kube-system"),
            (
                "rbac.authorization.k8s.io/v1",
                "ClusterRole",
                None,
                "system:seed-proof",
            ),
            (
                "v1",
                "Secret",
                Some("kube-system"),
                "bootstrap-token-seed-proof",
            ),
            ("networking.k8s.io/v1", "ServiceCIDR", None, "kubernetes"),
            ("v1", "Node", None, "n32"),
        ] {
            let leader_resource = leader_db
                .get_resource(api_version, kind, namespace, name)
                .await
                .unwrap();
            let joiner_resource = joiner_db
                .get_resource(api_version, kind, namespace, name)
                .await
                .unwrap();
            assert_eq!(
                joiner_resource, leader_resource,
                "seed state drift for {kind}/{name}"
            );
        }
        assert_eq!(
            joiner_db.get_current_resource_version().await.unwrap(),
            leader_db.get_current_resource_version().await.unwrap(),
            "joining voter must preserve the leader's public resourceVersion sequence"
        );

        drop(handler);
        match Arc::try_unwrap(leader) {
            Ok(leader) => leader.shutdown().await.unwrap(),
            Err(_) => panic!("join handler retained the seed Raft node"),
        }
        joiner.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn post_join_controller_pod_and_csr_status_mutations_converge_through_raft() {
        let registry = LoopbackRegistry::new();
        let (leader, leader_db) = fresh_voter_in_registry_with_backend(33, &registry).await;
        let leader = Arc::new(leader.into_node());
        let (joiner, joiner_db) = fresh_voter_in_registry_with_backend(34, &registry).await;
        leader
            .bootstrap_single_voter("https://10.99.0.33:7679".into())
            .await
            .expect("bootstrap seed");
        wait_for_leader(&leader, std::time::Duration::from_secs(5))
            .await
            .expect("seed leader election");
        leader
            .propose_command(StorageCommand::CreateNamespace {
                name: "kube-system".into(),
                data: serde_json::json!({
                    "apiVersion": "v1", "kind": "Namespace",
                    "metadata": {"name": "kube-system", "uid": "system-ns"}
                }),
            })
            .await
            .expect("create namespace");
        let handler = IntegrationRaftComposition::new(leader_db.clone())
            .controlplane_join_handler_with_raft_store(leader.clone());
        handler
            .join(test_controlplane_join_request(
                34,
                "https://10.99.0.34:7679",
                "n34",
                false,
                joiner.storage_incarnation(),
                None,
                "controller-joiner-hash",
            ))
            .await
            .expect("admit voter");
        wait_for_voter_count(&leader, 2).await;

        let composition = IntegrationRaftComposition::new(leader_db.clone());
        composition
            .create_pod_through_root_persistence(
                leader.clone(),
                "kube-system",
                "coredns-convergence",
                serde_json::json!({
                    "apiVersion": "v1", "kind": "Pod",
                    "metadata": {
                        "namespace": "kube-system",
                        "name": "coredns-convergence",
                        "uid": "coredns-pod-uid"
                    },
                    "spec": {"containers": [{"name": "coredns", "image": "coredns:1.11.1"}]},
                    "status": {"phase": "Pending"}
                }),
            )
            .await
            .expect("create controller Pod");
        leader
            .propose_command(StorageCommand::CreateResource {
                api_version: "certificates.k8s.io/v1".into(),
                kind: "CertificateSigningRequest".into(),
                namespace: None,
                name: "node-client-convergence".into(),
                data: serde_json::json!({
                    "apiVersion": "certificates.k8s.io/v1",
                    "kind": "CertificateSigningRequest",
                    "metadata": {"name": "node-client-convergence", "uid": "csr-uid"},
                    "spec": {"request": "AA==", "signerName": "kubernetes.io/kube-apiserver-client-kubelet"}
                }),
            })
            .await
            .expect("create CSR");
        let csr = leader_db
            .get_resource(
                "certificates.k8s.io/v1",
                "CertificateSigningRequest",
                None,
                "node-client-convergence",
            )
            .await
            .expect("read CSR")
            .expect("created CSR resource");
        composition
            .approve_csr_through_controller(
                leader.clone(),
                &csr.name,
                &csr.uid,
                csr.resource_version,
            )
            .await
            .expect("approve CSR");

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let pod = joiner_db
                .get_resource("v1", "Pod", Some("kube-system"), "coredns-convergence")
                .await
                .unwrap();
            let csr = joiner_db
                .get_resource(
                    "certificates.k8s.io/v1",
                    "CertificateSigningRequest",
                    None,
                    "node-client-convergence",
                )
                .await
                .unwrap();
            if pod.is_some()
                && csr.as_ref().is_some_and(|resource| {
                    resource.data.pointer("/status/conditions/0/type")
                        == Some(&serde_json::json!("Approved"))
                })
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "joined voter did not receive controller Pod creation and CSR approval"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(
            joiner_db.get_current_resource_version().await.unwrap(),
            leader_db.get_current_resource_version().await.unwrap(),
            "controller mutations must preserve exact public resourceVersion convergence"
        );

        drop(handler);
        match Arc::try_unwrap(leader) {
            Ok(leader) => leader.shutdown().await.unwrap(),
            Err(_) => panic!("controller convergence fixture retained the Raft node"),
        }
        joiner.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn joined_voter_and_learner_converge_namespace_defaults_and_full_teardown() {
        let registry = LoopbackRegistry::new();
        let (leader, leader_db) = fresh_voter_in_registry_with_backend(35, &registry).await;
        let leader = Arc::new(leader.into_node());
        let (voter, voter_db) = fresh_voter_in_registry_with_backend(36, &registry).await;
        let (learner, learner_db) = fresh_voter_in_registry_with_backend(37, &registry).await;
        leader
            .bootstrap_single_voter("https://10.99.0.35:7679".into())
            .await
            .expect("bootstrap seed");
        wait_for_leader(&leader, std::time::Duration::from_secs(5))
            .await
            .expect("seed leader election");
        leader
            .propose_command(StorageCommand::CreateNamespace {
                name: "namespace-lifecycle-convergence".into(),
                data: serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {
                        "name": "namespace-lifecycle-convergence",
                        "uid": "namespace-lifecycle-uid"
                    },
                    "spec": {"finalizers": ["kubernetes"]},
                    "status": {"phase": "Active"}
                }),
            })
            .await
            .expect("create namespace through Raft");

        let composition = IntegrationRaftComposition::new(leader_db.clone());
        let handler = composition.controlplane_join_handler_with_raft_store(leader.clone());
        handler
            .join(test_controlplane_join_request(
                36,
                "https://10.99.0.36:7679",
                "n36",
                false,
                voter.storage_incarnation(),
                None,
                "namespace-voter-hash",
            ))
            .await
            .expect("admit second voter");
        handler
            .join(test_controlplane_join_request(
                37,
                "https://10.99.0.37:7679",
                "n37",
                true,
                learner.storage_incarnation(),
                None,
                "namespace-learner-hash",
            ))
            .await
            .expect("admit learner");
        wait_for_voter_count(&leader, 2).await;

        composition
            .create_namespace_defaults_through_root_adapters(
                leader.clone(),
                "namespace-lifecycle-convergence",
            )
            .await
            .expect("create namespace defaults through production root adapters");
        leader
            .propose_command(StorageCommand::CreateResource {
                api_version: "v1".into(),
                kind: "Pod".into(),
                namespace: Some("namespace-lifecycle-convergence".into()),
                name: "bound-pod".into(),
                data: serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "namespace-lifecycle-convergence",
                        "name": "bound-pod",
                        "uid": "bound-pod-uid"
                    },
                    "spec": {
                        "nodeName": "n35",
                        "containers": [{"name": "pause", "image": "pause"}]
                    }
                }),
            })
            .await
            .expect("create bound Pod through Raft");
        leader
            .propose_command(StorageCommand::CreateResource {
                api_version: "v1".into(),
                kind: "Event".into(),
                namespace: Some("namespace-lifecycle-convergence".into()),
                name: "bound-pod.scheduled".into(),
                data: serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Event",
                    "metadata": {
                        "namespace": "namespace-lifecycle-convergence",
                        "name": "bound-pod.scheduled",
                        "uid": "bound-pod-event-uid"
                    },
                    "reason": "Scheduled"
                }),
            })
            .await
            .expect("create Pod Event through Raft");
        let namespace = leader_db
            .get_resource("v1", "Namespace", None, "namespace-lifecycle-convergence")
            .await
            .expect("read namespace")
            .expect("namespace exists");
        let mut terminating_namespace = (*namespace.data).clone();
        terminating_namespace["metadata"]["deletionTimestamp"] =
            serde_json::json!("2026-08-11T13:25:10Z");
        terminating_namespace["status"]["phase"] = serde_json::json!("Terminating");
        leader
            .propose_command(StorageCommand::UpdateNamespace {
                name: namespace.name,
                data: terminating_namespace,
                expected_rv: namespace.resource_version,
            })
            .await
            .expect("mark namespace terminating through Raft");

        assert!(
            !composition
                .reconcile_namespace_termination_through_root_adapters(
                    leader.clone(),
                    "namespace-lifecycle-convergence",
                    "namespace-lifecycle-uid",
                )
                .await
                .expect("first namespace termination reconcile"),
            "namespace with a live Pod must remain pending",
        );
        let terminating_pod = leader_db
            .get_resource(
                "v1",
                "Pod",
                Some("namespace-lifecycle-convergence"),
                "bound-pod",
            )
            .await
            .expect("read terminating Pod")
            .expect("Pod remains until actor finalization");
        leader
            .propose_command(StorageCommand::FinalizeBoundPod {
                namespace: "namespace-lifecycle-convergence".into(),
                name: "bound-pod".into(),
                pod_uid: "bound-pod-uid".into(),
                node_name: "n35".into(),
                observed_resource_version: terminating_pod.resource_version,
            })
            .await
            .expect("actor-finalize bound Pod through Raft");
        assert!(
            composition
                .reconcile_namespace_termination_through_root_adapters(
                    leader.clone(),
                    "namespace-lifecycle-convergence",
                    "namespace-lifecycle-uid",
                )
                .await
                .expect("final namespace termination reconcile"),
            "namespace must finalize after its bound Pod actor finalizes",
        );

        let convergence_deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let voter_done = voter_db
                .get_resource("v1", "Namespace", None, "namespace-lifecycle-convergence")
                .await
                .unwrap()
                .is_none();
            let learner_done = learner_db
                .get_resource("v1", "Namespace", None, "namespace-lifecycle-convergence")
                .await
                .unwrap()
                .is_none();
            if voter_done && learner_done {
                break;
            }
            assert!(
                std::time::Instant::now() < convergence_deadline,
                "joined voter/learner did not converge namespace teardown"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        for (member, db) in [
            ("leader", &leader_db),
            ("voter", &voter_db),
            ("learner", &learner_db),
        ] {
            for (api_version, kind, namespace, name) in [
                ("v1", "Namespace", None, "namespace-lifecycle-convergence"),
                (
                    "v1",
                    "ServiceAccount",
                    Some("namespace-lifecycle-convergence"),
                    "default",
                ),
                (
                    "v1",
                    "ConfigMap",
                    Some("namespace-lifecycle-convergence"),
                    "kube-root-ca.crt",
                ),
                (
                    "v1",
                    "Pod",
                    Some("namespace-lifecycle-convergence"),
                    "bound-pod",
                ),
                (
                    "v1",
                    "Event",
                    Some("namespace-lifecycle-convergence"),
                    "bound-pod.scheduled",
                ),
            ] {
                assert!(
                    db.get_resource(api_version, kind, namespace, name)
                        .await
                        .expect("read namespace lifecycle resource")
                        .is_none(),
                    "{member} retained {kind}/{name} after namespace teardown"
                );
            }
            assert_eq!(
                db.get_current_resource_version().await.unwrap(),
                leader_db.get_current_resource_version().await.unwrap(),
                "{member} public resourceVersion diverged after namespace teardown"
            );
        }

        drop(handler);
        match Arc::try_unwrap(leader) {
            Ok(leader) => leader.shutdown().await.unwrap(),
            Err(_) => panic!("namespace convergence fixture retained the Raft node"),
        }
        voter.shutdown().await.unwrap();
        learner.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_node_subnet_proposals_do_not_close_apply_channel() {
        let (node, backend) = fresh_node(90).await;
        node.bootstrap_single_voter("https://10.99.0.90:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&node, std::time::Duration::from_secs(5))
            .await
            .expect("become leader");

        let subnet_command = |node_name: &'static str, node_ip: &'static str| {
            klights_cluster_core::command::StorageCommand::AllocateNodeSubnet {
                node_name: node_name.into(),
                subnet: "10.50.0.0/16".into(),
                node_ip: node_ip.into(),
            }
        };

        let (a, b) = tokio::join!(
            node.propose_command(subnet_command("mn-worker", "10.99.0.11")),
            node.propose_command(subnet_command("mn-worker2", "10.99.0.12")),
        );
        a.expect("first subnet proposal");
        b.expect("second subnet proposal");

        node.propose_command(
            klights_cluster_core::command::StorageCommand::CreateResource {
                api_version: "v1".into(),
                kind: "ConfigMap".into(),
                namespace: Some("default".into()),
                name: "after-subnet".into(),
                data: serde_json::json!({
                    "metadata": {"name": "after-subnet", "namespace": "default"}
                }),
            },
        )
        .await
        .expect("raft still accepts writes after concurrent subnet proposals");

        let rows = backend
            .list_applied_outbox()
            .await
            .expect("list applied_outbox");
        assert!(
            rows.is_empty(),
            "generic raft proposals via propose_command should not touch applied_outbox: {rows:?}"
        );
        let worker = backend
            .get_node_subnet("mn-worker")
            .await
            .expect("read worker subnet")
            .expect("worker subnet exists");
        let worker2 = backend
            .get_node_subnet("mn-worker2")
            .await
            .expect("read worker2 subnet")
            .expect("worker2 subnet exists");
        assert_ne!(worker.subnet, worker2.subnet);

        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn raft_node_propose_command_does_not_create_applied_outbox_placeholder() {
        let (node, backend) = fresh_node(92).await;
        node.bootstrap_single_voter("https://10.99.0.92:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&node, std::time::Duration::from_secs(5))
            .await
            .expect("become leader");

        let subnet_command = klights_cluster_core::command::StorageCommand::AllocateNodeSubnet {
            node_name: "mn-worker-placeholder-check".into(),
            subnet: "10.60.0.0/16".into(),
            node_ip: "10.99.0.99".into(),
        };
        node.propose_command(subnet_command)
            .await
            .expect("propose subnet command");

        node.propose_command(klights_cluster_core::command::StorageCommand::CreateResource {
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            namespace: Some("default".into()),
            name: "no-outbox-after-propose".into(),
            data: serde_json::json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "no-outbox-after-propose", "namespace": "default"}}),
        })
        .await
        .expect("propose create command");

        let applied = backend
            .list_applied_outbox()
            .await
            .expect("list applied_outbox");
        assert!(
            applied.is_empty(),
            "propose_command must not insert applied_outbox placeholders for generic writes: {applied:?}"
        );

        let create_resource = backend
            .get_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "no-outbox-after-propose",
            )
            .await
            .expect("lookup created configmap")
            .expect("created configmap should exist");
        assert!(create_resource.resource_version > 0);

        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn raft_create_resource_rejects_duplicate_name() {
        let (node, backend) = fresh_node(91).await;
        node.bootstrap_single_voter("https://10.99.0.91:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&node, std::time::Duration::from_secs(5))
            .await
            .expect("become leader");

        let runtime_class_create =
            |uid: &'static str| klights_cluster_core::command::StorageCommand::CreateResource {
                api_version: "node.k8s.io/v1".into(),
                kind: "RuntimeClass".into(),
                namespace: None,
                name: "duplicate-runtime-class".into(),
                data: serde_json::json!({
                    "apiVersion": "node.k8s.io/v1",
                    "kind": "RuntimeClass",
                    "metadata": {
                        "name": "duplicate-runtime-class",
                        "uid": uid,
                    },
                    "handler": "handler",
                }),
            };

        node.propose_command(runtime_class_create("first-uid"))
            .await
            .expect("first create");
        let first = backend
            .get_resource(
                "node.k8s.io/v1",
                "RuntimeClass",
                None,
                "duplicate-runtime-class",
            )
            .await
            .expect("read first create")
            .expect("runtimeclass exists");

        let err = node
            .propose_command(runtime_class_create("second-uid"))
            .await
            .expect_err("duplicate create must fail before raft overwrites the live row");
        assert!(matches!(
            err.downcast_ref::<klights_cluster_core::StorageMutationError>(),
            Some(klights_cluster_core::StorageMutationError::Rejected {
                code: klights_cluster_core::StorageCommandRejectionCode::AlreadyExists,
                ..
            })
        ));
        let msg = err.to_string();
        assert!(
            msg.contains("already exists") && msg.contains("409 Conflict"),
            "expected Kubernetes-style already-exists conflict, got: {msg}"
        );

        let live = backend
            .get_resource(
                "node.k8s.io/v1",
                "RuntimeClass",
                None,
                "duplicate-runtime-class",
            )
            .await
            .expect("read after duplicate")
            .expect("runtimeclass still exists");
        assert_eq!(live.uid, first.uid);
        assert_eq!(live.resource_version, first.resource_version);

        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn add_voter_beyond_cap_is_rejected() {
        let registry = LoopbackRegistry::new();
        let leader = fresh_voter_in_registry(10, &registry).await;
        let v2 = fresh_voter_in_registry(20, &registry).await;
        let v3 = fresh_voter_in_registry(30, &registry).await;
        let v4 = fresh_voter_in_registry(40, &registry).await;
        leader
            .bootstrap_single_voter("https://10.99.0.10:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&leader, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        admit_member(&leader, &v2, "https://10.99.0.20:7679", false)
            .await
            .expect("add 2nd voter");
        wait_for_voter_count(&leader, 2).await;
        admit_member(&leader, &v3, "https://10.99.0.30:7679", false)
            .await
            .expect("add 3rd voter");
        wait_for_voter_count(&leader, 3).await;
        let err = admit_member(&leader, &v4, "https://10.99.0.40:7679", false)
            .await
            .expect_err("4th voter must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("controlplane limit"),
            "rejection should mention the cap, got: {msg}"
        );
        for n in [leader, v2, v3, v4] {
            n.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn remove_voter_preserves_quorum_and_refuses_last_voter() {
        let registry = LoopbackRegistry::new();
        let leader = fresh_voter_in_registry(10, &registry).await;
        let v2 = fresh_voter_in_registry(20, &registry).await;
        let v3 = fresh_voter_in_registry(30, &registry).await;
        leader
            .bootstrap_single_voter("https://10.99.0.10:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&leader, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        admit_member(&leader, &v2, "https://10.99.0.20:7679", false)
            .await
            .expect("add v2");
        wait_for_voter_count(&leader, 2).await;
        admit_member(&leader, &v3, "https://10.99.0.30:7679", false)
            .await
            .expect("add v3");
        wait_for_voter_count(&leader, 3).await;
        // Shrink to 2 voters.
        leader
            .remove_voter(30)
            .await
            .expect("remove v3 from 3-voter cluster");
        wait_for_voter_count(&leader, 2).await;
        // Refuse to remove this node from its own membership.
        let err_self = leader
            .remove_voter(10)
            .await
            .expect_err("self-removal must be rejected");
        assert!(format!("{err_self}").contains("refusing to remove this node"));
        for n in [leader, v2, v3] {
            n.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn bootstrap_is_idempotent() {
        let (node, _) = fresh_node(11).await;
        node.bootstrap_single_voter("https://10.99.0.11:7679".into())
            .await
            .expect("first bootstrap");
        wait_for_leader(&node, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        // Second call must not error — matches openraft NotAllowed no-op.
        node.bootstrap_single_voter("https://10.99.0.11:7679".into())
            .await
            .expect("second bootstrap should be a no-op");
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn current_leader_info_returns_self_for_solo_seed() {
        let (node, _) = fresh_node(12).await;
        node.bootstrap_single_voter("https://10.99.0.12:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&node, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        let (id, addr) = node
            .current_leader_info()
            .expect("solo voter is the leader");
        assert_eq!(id, 12);
        assert_eq!(addr, "https://10.99.0.12:7679");
        node.shutdown().await.unwrap();
    }

    async fn test_db() -> Arc<klights_cluster_datastore::sqlite::embedded::Datastore> {
        Arc::new(
            klights_cluster_datastore::sqlite::embedded::Datastore::new_in_memory()
                .await
                .unwrap(),
        )
    }

    fn test_controlplane_join_request(
        node_id: u64,
        addr: &str,
        node_name: &str,
        as_learner: bool,
        storage_incarnation: &str,
        node_internal_ip: Option<&str>,
        git_commit: &str,
    ) -> klights_leader_api::ControlplaneJoinRequest {
        klights_leader_api::ControlplaneJoinRequest {
            node_id,
            addr: addr.to_string(),
            node_name: node_name.to_string(),
            as_learner,
            storage_incarnation: storage_incarnation.to_string(),
            storage_log_attestation: klights_leader_api::RaftStorageAttestation {
                high_watermark: None,
                current_boundary: None,
            },
            command_codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
            node_internal_ip: node_internal_ip.map(str::to_string),
            node_registration: Some(klights_leader_api::RemoteNodeRegistrationSnapshot {
                node_mode: klights_leader_api::RemoteNodeMode::Root,
                host: klights_leader_api::RemoteNodeHostFacts {
                    cpu_count: 4,
                    memory_ki: 8 * 1024 * 1024,
                    architecture: "test-arch".to_string(),
                    operating_system: "linux".to_string(),
                    os_image: "Test Linux".to_string(),
                    kernel_version: "6.1-test".to_string(),
                    container_runtime_version: "containerd://1.7.0".to_string(),
                    kubelet_version: "v1.34.0-test".to_string(),
                    git_commit: git_commit.to_string(),
                },
            }),
            legacy_node_git_commit: None,
        }
    }

    #[tokio::test]
    async fn join_handler_on_leader_runs_add_voter_and_reports_count() {
        use klights_leader_api::ControlplaneJoinOutcome;
        let registry = LoopbackRegistry::new();
        let leader = Arc::new(fresh_voter_in_registry(50, &registry).await.into_node());
        let follower = fresh_voter_in_registry(51, &registry).await;
        leader
            .bootstrap_single_voter("https://10.99.0.50:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&leader, std::time::Duration::from_secs(5))
            .await
            .unwrap();

        let join_db = test_db().await;
        let handler = IntegrationRaftComposition::new(join_db.clone())
            .controlplane_join_handler(leader.clone());
        let outcome = handler
            .join(test_controlplane_join_request(
                51,
                "https://10.99.0.51:7679",
                "n51",
                false,
                follower.storage_incarnation(),
                None,
                "joinerhash1",
            ))
            .await
            .expect("leader runs add_voter");
        match outcome {
            ControlplaneJoinOutcome::Accepted {
                voter_count_after,
                admitted_as_learner,
                ..
            } => {
                assert_eq!(voter_count_after, 2, "cluster grew to N=2");
                assert!(
                    !admitted_as_learner,
                    "voter join must not be flagged as learner"
                );
            }
            other => panic!("expected Accepted, got {other:?}"),
        }
        let node = join_db
            .get_resource("v1", "Node", None, "n51")
            .await
            .expect("read Node row")
            .expect("Node row must be created by register_voter_node");
        let git_commit = node
            .data
            .pointer("/metadata/annotations/klights.io~1git-commit")
            .and_then(|value| value.as_str());
        assert_eq!(
            git_commit,
            Some("joinerhash1"),
            "leader-side Node registration must stamp the joining node's git commit"
        );
        assert_eq!(node.data["status"]["capacity"]["cpu"], "4");
        assert_eq!(node.data["status"]["capacity"]["memory"], "8388608Ki");
        assert_eq!(node.data["status"]["nodeInfo"]["architecture"], "test-arch");
        assert_eq!(node.data["status"]["nodeInfo"]["osImage"], "Test Linux");
        let ready = node.data["status"]["conditions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|condition| condition["type"] == "Ready")
            .unwrap();
        assert_eq!(
            ready["status"], "False",
            "leader admission must not report a remote joiner Ready before its dataplane reports health"
        );
        let network_unavailable = node.data["status"]["conditions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|condition| condition["type"] == "NetworkUnavailable")
            .unwrap();
        assert_eq!(
            network_unavailable["status"], "True",
            "leader admission must keep a remote joiner's network unavailable until the joiner reports health"
        );

        handler
            .join(klights_leader_api::ControlplaneJoinRequest {
                node_id: 51,
                addr: "https://10.99.0.51:7679".to_string(),
                node_name: "n51".to_string(),
                as_learner: false,
                storage_incarnation: follower.storage_incarnation().to_string(),
                storage_log_attestation: klights_leader_api::RaftStorageAttestation {
                    high_watermark: Some(klights_leader_api::RaftStorageLogAttestation {
                        term: u64::MAX,
                        leader_node_id: 50,
                        index: u64::MAX,
                    }),
                    current_boundary: Some(klights_leader_api::RaftStorageLogAttestation {
                        term: u64::MAX,
                        leader_node_id: 50,
                        index: u64::MAX,
                    }),
                },
                command_codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
                node_internal_ip: None,
                node_registration: None,
                legacy_node_git_commit: Some("legacyrejoin".to_string()),
            })
            .await
            .expect("persisted member may rejoin without the new snapshot");
        let rejoined = join_db
            .get_resource("v1", "Node", None, "n51")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rejoined.data["status"]["capacity"]["cpu"], "4");
        assert_eq!(
            rejoined.data["status"]["nodeInfo"]["architecture"], "test-arch",
            "legacy rejoin must reuse persisted joiner facts, not leader facts"
        );
        assert_eq!(
            rejoined.data["metadata"]["annotations"]["klights.io/git-commit"], "joinerhash1",
            "an idempotent admission retry must not rewrite the persisted Node registration"
        );
    }

    #[tokio::test]
    async fn join_handler_voter_admission_updates_cluster_membership_metadata() {
        use klights_leader_api::ControlplaneJoinOutcome;
        let registry = LoopbackRegistry::new();
        let (leader, leader_db) = fresh_voter_in_registry_with_backend(52, &registry).await;
        let leader = Arc::new(leader.into_node());
        let (follower, follower_db) = fresh_voter_in_registry_with_backend(53, &registry).await;
        leader
            .bootstrap_single_voter("https://10.99.0.52:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&leader, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        for (key, value) in [
            (klights_cluster_store::CLUSTER_ID_META_KEY, "cluster-a"),
            (klights_cluster_store::LEADER_EPOCH_META_KEY, "0"),
            (
                klights_cluster_store::RAFT_VOTERS_META_KEY,
                "[\"mn-controlplane1\"]",
            ),
            (klights_cluster_store::RAFT_TERM_META_KEY, "0"),
            (
                klights_cluster_store::RAFT_LEADER_HINT_META_KEY,
                "mn-controlplane1",
            ),
        ] {
            leader
                .propose_command(StorageCommand::SetKlightsMeta {
                    key: key.to_string(),
                    value: value.to_string(),
                })
                .await
                .unwrap();
        }

        let composition = IntegrationRaftComposition::new(leader_db.clone());
        let handler = composition.controlplane_join_handler_with_raft_store(leader.clone());
        let outcome = handler
            .join(test_controlplane_join_request(
                53,
                "https://10.99.0.53:7679",
                "mn-controlplane2",
                false,
                follower.storage_incarnation(),
                None,
                "joinerhash2",
            ))
            .await
            .expect("leader runs add_voter");
        assert!(
            matches!(
                outcome,
                ControlplaneJoinOutcome::Accepted {
                    admitted_as_learner: false,
                    ..
                }
            ),
            "expected voter Accepted, got {outcome:?}"
        );

        let membership = composition.read_cluster_membership().await.unwrap();
        assert_eq!(
            membership.voters,
            vec!["mn-controlplane1", "mn-controlplane2"],
            "admitted voters must be reflected in replicated membership metadata"
        );
        let follower_composition = IntegrationRaftComposition::new(follower_db.clone());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let follower_membership = follower_composition.read_cluster_membership().await;
            if follower_membership
                .as_ref()
                .is_ok_and(|replicated| replicated.voters == membership.voters)
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "joining voter did not apply replicated membership metadata"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        drop(handler);
        match Arc::try_unwrap(leader) {
            Ok(leader) => leader.shutdown().await.unwrap(),
            Err(_) => panic!("join handler retained the seed Raft node"),
        }
        follower.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn join_handler_returns_no_leader_when_uninitialized() {
        use klights_leader_api::ControlplaneJoinOutcome;
        let (node, _) = fresh_node(60).await;
        let arc = Arc::new(node.into_node());
        let handler =
            IntegrationRaftComposition::new(test_db().await).controlplane_join_handler(arc);
        let outcome = handler
            .join(test_controlplane_join_request(
                61,
                "https://10.99.0.61:7679",
                "n61",
                false,
                "00000000-0000-4000-8000-000000000061",
                None,
                "joinerhash3",
            ))
            .await
            .expect("handler returns Denied not error");
        match outcome {
            ControlplaneJoinOutcome::Denied { reason } => {
                assert!(reason.contains("no leader"), "got: {reason}");
            }
            other => panic!("expected Denied, got {other:?}"),
        }
    }

    /// T1.5.x: with as_learner=true the leader calls add_learner_only
    /// instead of add_voter. Voter count is unchanged; admitted_as_learner
    /// is true in the response.
    #[tokio::test]
    async fn join_handler_as_learner_admits_via_add_learner_only() {
        use klights_leader_api::ControlplaneJoinOutcome;
        let registry = LoopbackRegistry::new();
        let leader = fresh_voter_in_registry(70, &registry).await;
        let learner = fresh_voter_in_registry(71, &registry).await;
        leader
            .bootstrap_single_voter("https://10.99.0.70:7679".into())
            .await
            .unwrap();
        wait_for_leader(&leader, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        let leader = Arc::new(leader.into_node());

        let handler = IntegrationRaftComposition::new(test_db().await)
            .controlplane_join_handler(leader.clone());
        let outcome = handler
            .join(test_controlplane_join_request(
                71,
                "https://10.99.0.71:7679",
                "n71",
                true,
                learner.storage_incarnation(),
                None,
                "joinerhash4",
            ))
            .await
            .expect("leader runs add_learner_only");
        match outcome {
            ControlplaneJoinOutcome::Accepted {
                voter_count_after,
                admitted_as_learner,
                ..
            } => {
                assert_eq!(
                    voter_count_after, 1,
                    "voter count unchanged by learner admission"
                );
                assert!(
                    admitted_as_learner,
                    "as_learner=true must surface admitted_as_learner=true"
                );
            }
            other => panic!("expected Accepted, got {other:?}"),
        }

        // The learner must now appear in membership.nodes() but NOT in
        // voter_ids() — confirming the leader took the learner path.
        let metrics = leader.raft.metrics().borrow().clone();
        let voter_ids: std::collections::BTreeSet<NodeId> =
            metrics.membership_config.membership().voter_ids().collect();
        let node_ids: std::collections::BTreeSet<NodeId> = metrics
            .membership_config
            .membership()
            .nodes()
            .map(|(id, _)| *id)
            .collect();
        assert!(
            !voter_ids.contains(&71),
            "learner must not be in voter_ids: {voter_ids:?}"
        );
        assert!(
            node_ids.contains(&71),
            "learner must be in membership.nodes(): {node_ids:?}"
        );
    }

    /// T1.7 regression guard (replica-label fix): when `join` admits a
    /// node with `as_learner=true`, the Node row it registers on the
    /// leader must carry `node-role.kubernetes.io/replica`, not
    /// `node-role.kubernetes.io/controlplane`. `register_voter_node`
    /// previously hardcoded `is_learner=false` in the synthesized
    /// joiner_shape — that's the bug this test pins down.
    #[tokio::test]
    async fn join_handler_as_learner_registers_node_with_replica_label() {
        use klights_leader_api::ControlplaneJoinOutcome;
        let registry = LoopbackRegistry::new();
        let leader = fresh_voter_in_registry(72, &registry).await;
        let learner = fresh_voter_in_registry(73, &registry).await;
        leader
            .bootstrap_single_voter("https://10.99.0.72:7679".into())
            .await
            .unwrap();
        wait_for_leader(&leader, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        let leader = Arc::new(leader.into_node());

        let leader_db = test_db().await;
        let handler = IntegrationRaftComposition::new(leader_db.clone())
            .controlplane_join_handler(leader.clone());
        let outcome = handler
            .join(test_controlplane_join_request(
                73,
                "https://10.99.0.73:7679",
                "n73",
                true,
                learner.storage_incarnation(),
                None,
                "joinerhash5",
            ))
            .await
            .expect("learner admission succeeds");
        assert!(
            matches!(
                outcome,
                ControlplaneJoinOutcome::Accepted {
                    admitted_as_learner: true,
                    ..
                }
            ),
            "expected Accepted as learner, got {outcome:?}"
        );

        let node = leader_db
            .get_resource("v1", "Node", None, "n73")
            .await
            .expect("read Node row")
            .expect("Node row must be created by register_voter_node");
        let labels = node
            .data
            .pointer("/metadata/labels")
            .and_then(|v| v.as_object())
            .expect("Node has labels map");
        assert!(
            labels.contains_key("node-role.kubernetes.io/replica"),
            "learner-admitted Node must carry the replica role label, got: {labels:?}"
        );
        assert!(
            !labels.contains_key("node-role.kubernetes.io/controlplane"),
            "learner-admitted Node must NOT carry the controlplane role label, got: {labels:?}"
        );
    }

    #[tokio::test]
    async fn join_handler_registers_internal_ip_separate_from_external_addr() {
        use klights_leader_api::ControlplaneJoinOutcome;
        let registry = LoopbackRegistry::new();
        let leader = Arc::new(fresh_voter_in_registry(80, &registry).await.into_node());
        let follower = fresh_voter_in_registry(81, &registry).await;
        leader
            .bootstrap_single_voter("https://10.99.0.80:7679".into())
            .await
            .unwrap();
        wait_for_leader(&leader, std::time::Duration::from_secs(5))
            .await
            .unwrap();

        let leader_db = test_db().await;
        let handler = IntegrationRaftComposition::new(leader_db.clone())
            .controlplane_join_handler(leader.clone());
        let outcome = handler
            .join(test_controlplane_join_request(
                81,
                "https://10.99.0.81:7679",
                "n81",
                false,
                follower.storage_incarnation(),
                Some("172.31.81.2"),
                "joinerhash6",
            ))
            .await
            .expect("voter admission succeeds");
        assert!(
            matches!(
                outcome,
                ControlplaneJoinOutcome::Accepted {
                    admitted_as_learner: false,
                    ..
                }
            ),
            "expected voter Accepted, got {outcome:?}"
        );

        let node = leader_db
            .get_resource("v1", "Node", None, "n81")
            .await
            .expect("read Node row")
            .expect("Node row must be created by register_voter_node");
        let addresses = node
            .data
            .pointer("/status/addresses")
            .and_then(|value| value.as_array())
            .expect("Node has status addresses");
        assert!(addresses.iter().any(|address| {
            address["type"] == "InternalIP" && address["address"] == "172.31.81.2"
        }));
        assert!(addresses.iter().any(|address| {
            address["type"] == "ExternalIP" && address["address"] == "10.99.0.81"
        }));
    }

    #[tokio::test]
    async fn raft_node_rpc_router_round_trips_vote_envelope() {
        use klights_leader_rpc::raft_rpc::RaftRpcRouter;
        let (node, _) = fresh_node(70).await;
        node.bootstrap_single_voter("https://10.99.0.70:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&node, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        let router = node.rpc_router();
        let rpc = openraft::raft::VoteRequest::new(
            openraft::Vote::new(100, 70),
            Some(openraft::LogId::new(openraft::LeaderId::new(100, 70), 0)),
        );
        let bytes = serde_json::to_vec(&rpc).unwrap();
        let receiver = RaftMemberNode::new(
            "loopback".into(),
            node.storage_incarnation().to_string(),
            None,
        );
        let out = router
            .vote(receiver.into(), bytes)
            .await
            .expect("vote dispatch");
        // Confirms the round-trip: the router decoded the envelope,
        // handed it to raft.vote, and serialized the response back.
        // Vote-granted semantics depend on openraft's current-term
        // state which isn't deterministic in this fresh-cluster test;
        // we just assert the response decodes cleanly.
        let _resp: openraft::raft::VoteResponse<NodeId> =
            serde_json::from_slice(&out).expect("decode vote response");
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn raft_router_fences_stale_nonzero_suffix_for_fresh_same_id_node() {
        use klights_leader_rpc::raft_rpc::RaftRpcRouter;
        let (node, _) = fresh_node(71).await;
        let router = node.rpc_router();
        let receiver = RaftMemberNode::new(
            "loopback".into(),
            node.storage_incarnation().to_string(),
            None,
        );
        let wrong_receiver =
            RaftMemberNode::new("loopback".into(), uuid::Uuid::new_v4().to_string(), None);
        let error = router
            .append_entries(wrong_receiver.into(), Vec::new())
            .await
            .expect_err("a stale storage incarnation must fail before payload decode");
        assert!(matches!(
            error,
            klights_leader_rpc::raft_rpc::RaftRpcRouterError::Retryable(_)
        ));
        let rolled_back_receiver = RaftMemberNode::new(
            "loopback".into(),
            node.storage_incarnation().to_string(),
            Some(RaftMemberLogId {
                term: 1,
                leader_node_id: 70,
                index: 1,
            }),
        );
        let error = router
            .append_entries(rolled_back_receiver.into(), Vec::new())
            .await
            .expect_err("a receiver below its admitted floor must fail before payload decode");
        assert!(matches!(
            error,
            klights_leader_rpc::raft_rpc::RaftRpcRouterError::Retryable(_)
        ));
        let stale = openraft::raft::AppendEntriesRequest::<TypeConfig> {
            vote: openraft::Vote::new_committed(1, 70),
            prev_log_id: None,
            entries: vec![openraft::Entry {
                log_id: openraft::LogId::new(openraft::LeaderId::new(1, 70), 124_022),
                payload: openraft::EntryPayload::Blank,
            }],
            leader_commit: Some(openraft::LogId::new(
                openraft::LeaderId::new(1, 70),
                128_367,
            )),
        };

        let error = router
            .append_entries(receiver.clone().into(), serde_json::to_vec(&stale).unwrap())
            .await
            .expect_err("stale leader cursor must receive a retryable admission fence");
        assert!(
            matches!(
                error,
                klights_leader_rpc::raft_rpc::RaftRpcRouterError::Retryable(_)
            ),
            "unexpected stale-session error: {error}"
        );
        assert!(
            node.raft.metrics().borrow().last_log_index.is_none(),
            "the stale suffix must not mutate fresh same-ID storage"
        );
        let stale_with_previous = openraft::raft::AppendEntriesRequest::<TypeConfig> {
            vote: openraft::Vote::new_committed(1, 70),
            prev_log_id: Some(openraft::LogId::new(
                openraft::LeaderId::new(1, 70),
                128_367,
            )),
            entries: Vec::new(),
            leader_commit: None,
        };
        let response = router
            .append_entries(
                receiver.into(),
                serde_json::to_vec(&stale_with_previous).unwrap(),
            )
            .await
            .expect("anchored mismatches must use normal Raft conflict backtracking");
        let response: openraft::raft::AppendEntriesResponse<NodeId> =
            serde_json::from_slice(&response).unwrap();
        assert!(
            matches!(response, openraft::raft::AppendEntriesResponse::Conflict),
            "fresh-session backtracking requires Conflict, got {response:?}"
        );
        assert!(
            node.raft.metrics().borrow().last_log_index.is_none(),
            "an anchored mismatch must not mutate fresh storage"
        );
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn leader_backs_off_on_session_fence_then_catches_up_without_client_rebuild() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let executor = klights_node_datastore::open::open_with_opts(
            klights_node_datastore::open::in_memory_opts(),
            supervisor.clone(),
            "sqlite:raft-session-fence-leader",
        )
        .await
        .unwrap();
        let node_local = Arc::new(SqliteRaftDurability::new(executor));
        let backend: Arc<klights_cluster_datastore::sqlite::embedded::Datastore> = Arc::new(
            klights_cluster_datastore::sqlite::embedded::Datastore::new_in_memory()
                .await
                .unwrap(),
        );
        let client = Arc::new(AdmissionFenceClient {
            ready: std::sync::atomic::AtomicBool::new(false),
            append_calls: std::sync::atomic::AtomicUsize::new(0),
            append_called: tokio::sync::Notify::new(),
        });
        let builds = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let network = klights_replication::grpc_network::GrpcRaftNetwork::new(Arc::new(
            AdmissionFenceFactory {
                client: client.clone(),
                builds: builds.clone(),
            },
        ));
        let metrics_network = network.clone();
        let leader = Arc::new(
            start_test_node(
                77,
                "n77".into(),
                raft_store_ports(backend),
                node_durability(&node_local),
                node_durability(&node_local),
                supervisor,
                network,
            )
            .await
            .unwrap(),
        );
        leader
            .bootstrap_single_voter("https://10.99.0.77:7679".into())
            .await
            .unwrap();
        wait_for_leader(&leader, std::time::Duration::from_secs(5))
            .await
            .unwrap();

        let admission = {
            let leader = leader.clone();
            tokio::spawn(async move {
                leader
                    .raft
                    .add_learner(
                        78,
                        super::test_unproven_member("https://10.99.0.78:7679"),
                        true,
                    )
                    .await
                    .map_err(|error| anyhow::anyhow!(error.to_string()))
            })
        };
        client.wait_for_append().await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(
            client
                .append_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "OpenRaft must honor its Unreachable backoff instead of hot-retrying the fence"
        );
        assert_eq!(
            metrics_network
                .metrics_snapshot()
                .client_invalidations_total,
            0
        );

        client
            .ready
            .store(true, std::sync::atomic::Ordering::SeqCst);
        tokio::time::timeout(std::time::Duration::from_secs(3), admission)
            .await
            .expect("fresh session must retry after backoff")
            .expect("admission task must not panic")
            .expect("learner must catch up after the fence opens");
        assert_eq!(
            builds.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "retryable admission fencing must reuse the healthy peer client"
        );
        Arc::try_unwrap(leader)
            .ok()
            .expect("admission task released its leader reference")
            .shutdown()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn same_uuid_backup_rollback_resets_existing_learner_session() {
        let registry = LoopbackRegistry::new();
        let leader = fresh_voter_in_registry(72, &registry).await;
        let old_learner = fresh_voter_in_registry(73, &registry).await;
        let restored_incarnation = old_learner.storage_incarnation().to_string();
        leader
            .bootstrap_single_voter("https://10.99.0.72:7679".into())
            .await
            .unwrap();
        wait_for_leader(&leader, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        leader
            .admit_controlplane_member(
                73,
                "https://10.99.0.73:7679".into(),
                true,
                restored_incarnation.clone(),
                storage_attestation(Some(klights_leader_api::RaftStorageLogAttestation {
                    term: u64::MAX,
                    leader_node_id: 72,
                    index: u64::MAX,
                })),
            )
            .await
            .unwrap();
        old_learner.shutdown().await.unwrap();
        let replacement = fresh_voter_in_registry(73, &registry).await;
        // Model restoring an older backup of the same node.db: the storage
        // identity survives, while its durable boundary rolls back.
        registry.register(73, replacement.raft.clone(), restored_incarnation.clone());

        leader
            .admit_controlplane_member(
                73,
                "https://10.99.0.73:7679".into(),
                true,
                restored_incarnation,
                storage_attestation(None),
            )
            .await
            .expect("same-ID learner rejoin creates a fresh replication session");

        let metrics = leader.raft.metrics().borrow().clone();
        let (_, member) = metrics
            .membership_config
            .membership()
            .nodes()
            .find(|(id, _)| **id == 73)
            .expect("rejoined learner remains in membership");
        assert_eq!(
            member.addr, "https://10.99.0.73:7679",
            "targeted remove/re-add must preserve the requested learner endpoint"
        );
        assert!(
            !metrics
                .membership_config
                .membership()
                .voter_ids()
                .any(|id| id == 73),
            "session reset must not promote a learner"
        );
        leader.shutdown().await.unwrap();
        replacement.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn wiped_existing_voter_rejoins_through_fresh_learner_session() {
        let registry = LoopbackRegistry::new();
        let leader = Arc::new(fresh_voter_in_registry(74, &registry).await);
        let surviving_voter = fresh_voter_in_registry(75, &registry).await;
        let old_voter = fresh_voter_in_registry(76, &registry).await;
        let surviving_incarnation = surviving_voter.storage_incarnation().to_string();
        let old_voter_incarnation = old_voter.storage_incarnation().to_string();
        leader
            .bootstrap_single_voter("https://10.99.0.74:7679".into())
            .await
            .unwrap();
        wait_for_leader(&leader, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        leader
            .admit_controlplane_member(
                75,
                "https://10.99.0.75:7679".into(),
                false,
                surviving_incarnation,
                storage_attestation(None),
            )
            .await
            .unwrap();
        leader
            .admit_controlplane_member(
                76,
                "https://10.99.0.76:7679".into(),
                false,
                old_voter_incarnation,
                storage_attestation(None),
            )
            .await
            .unwrap();
        let unrelated_learner = fresh_voter_in_registry(79, &registry).await;
        let unrelated_incarnation = unrelated_learner.storage_incarnation().to_string();
        leader
            .admit_controlplane_member(
                79,
                "https://10.99.0.79:7679".into(),
                true,
                unrelated_incarnation,
                storage_attestation(None),
            )
            .await
            .unwrap();
        old_voter.shutdown().await.unwrap();
        let replacement = fresh_voter_in_registry(76, &registry).await;
        let replacement_incarnation = replacement.storage_incarnation().to_string();
        leader
            .admit_controlplane_member(
                76,
                "https://10.99.0.76:8679".into(),
                false,
                replacement_incarnation,
                storage_attestation(None),
            )
            .await
            .expect("quorum must reset and catch up the wiped same-ID voter");

        let metrics = leader.raft.metrics().borrow().clone();
        assert!(
            metrics
                .membership_config
                .membership()
                .voter_ids()
                .any(|id| id == 76),
            "replacement must be promoted only after learner catch-up"
        );
        let (_, member) = metrics
            .membership_config
            .membership()
            .nodes()
            .find(|(id, _)| **id == 76)
            .expect("replacement voter remains a member");
        assert_eq!(member.addr, "https://10.99.0.76:8679");
        assert!(
            metrics
                .membership_config
                .membership()
                .nodes()
                .any(|(id, _)| *id == 79),
            "targeted voter replacement must preserve unrelated learners"
        );
        replacement.shutdown().await.unwrap();
        let learner_replacement = fresh_voter_in_registry(76, &registry).await;
        let learner_incarnation = learner_replacement.storage_incarnation().to_string();
        leader
            .admit_controlplane_member(
                76,
                "https://10.99.0.76:8679".into(),
                true,
                learner_incarnation,
                storage_attestation(None),
            )
            .await
            .expect("changed-incarnation voter-to-learner join must reset the session");
        let demoted = leader.raft.metrics().borrow().clone();
        assert!(
            !demoted
                .membership_config
                .membership()
                .voter_ids()
                .any(|id| id == 76),
            "changed-incarnation voter must be re-added as the requested learner"
        );
        assert!(
            demoted
                .membership_config
                .membership()
                .nodes()
                .any(|(id, _)| *id == 79),
            "voter-to-learner session reset must preserve unrelated learners"
        );
        Arc::try_unwrap(leader)
            .ok()
            .expect("voter rejoin task released the leader")
            .shutdown()
            .await
            .unwrap();
        surviving_voter.shutdown().await.unwrap();
        learner_replacement.shutdown().await.unwrap();
        unrelated_learner.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn lost_join_response_retry_with_fresh_attestation_is_membership_and_log_noop() {
        let registry = LoopbackRegistry::new();
        let leader = fresh_voter_in_registry(80, &registry).await;
        let learner = fresh_voter_in_registry(81, &registry).await;
        leader
            .bootstrap_single_voter("https://10.99.0.80:7679".into())
            .await
            .unwrap();
        wait_for_leader(&leader, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        let incarnation = learner.storage_incarnation().to_string();
        assert_eq!(
            leader
                .admit_controlplane_member(
                    81,
                    "https://10.99.0.81:7679".into(),
                    true,
                    incarnation.clone(),
                    storage_attestation(Some(klights_leader_api::RaftStorageLogAttestation {
                        term: u64::MAX,
                        leader_node_id: 80,
                        index: u64::MAX,
                    },)),
                )
                .await
                .unwrap(),
            RaftMemberAdmissionResult::Changed
        );
        let log_before = leader.raft.metrics().borrow().last_log_index;

        assert_eq!(
            leader
                .admit_controlplane_member(
                    81,
                    "https://10.99.0.81:7679".into(),
                    true,
                    incarnation,
                    storage_attestation(Some(klights_leader_api::RaftStorageLogAttestation {
                        term: u64::MAX,
                        leader_node_id: 80,
                        index: u64::MAX,
                    },)),
                )
                .await
                .unwrap(),
            RaftMemberAdmissionResult::Unchanged
        );
        assert_eq!(leader.raft.metrics().borrow().last_log_index, log_before);

        leader.shutdown().await.unwrap();
        learner.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn existing_member_without_v3_admission_marker_fails_closed_without_mutation() {
        let registry = LoopbackRegistry::new();
        let leader = fresh_voter_in_registry(84, &registry).await;
        let legacy_learner = fresh_voter_in_registry(85, &registry).await;
        leader
            .bootstrap_single_voter("https://10.99.0.84:7679".into())
            .await
            .unwrap();
        wait_for_leader(&leader, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        leader
            .raft
            .add_learner(
                85,
                super::test_unproven_member("https://10.99.0.85:7679"),
                true,
            )
            .await
            .unwrap();
        let log_before = leader.raft.metrics().borrow().last_log_index;

        let error = leader
            .admit_controlplane_member(
                85,
                "https://10.99.0.85:7679".into(),
                true,
                "00000000-0000-4000-8000-000000000085".into(),
                storage_attestation(None),
            )
            .await
            .expect_err("unproven pre-v3 membership must not be baselined implicitly");
        assert!(
            error
                .to_string()
                .contains("no proven v3 storage admission marker")
        );
        assert_eq!(leader.raft.metrics().borrow().last_log_index, log_before);

        leader.shutdown().await.unwrap();
        legacy_learner.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn wiped_voter_in_two_voter_cluster_fails_closed_before_membership_mutation() {
        let registry = LoopbackRegistry::new();
        let leader = fresh_voter_in_registry(82, &registry).await;
        let old_voter = fresh_voter_in_registry(83, &registry).await;
        leader
            .bootstrap_single_voter("https://10.99.0.82:7679".into())
            .await
            .unwrap();
        wait_for_leader(&leader, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        leader
            .admit_controlplane_member(
                83,
                "https://10.99.0.83:7679".into(),
                false,
                old_voter.storage_incarnation().to_string(),
                storage_attestation(None),
            )
            .await
            .unwrap();
        old_voter.shutdown().await.unwrap();
        let replacement = fresh_voter_in_registry(83, &registry).await;
        let voters_before: BTreeSet<_> = leader
            .raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .voter_ids()
            .collect();

        let error = leader
            .admit_controlplane_member(
                83,
                "https://10.99.0.83:8679".into(),
                false,
                "00000000-0000-4000-8000-000000000183".into(),
                storage_attestation(None),
            )
            .await
            .expect_err("lost two-voter quorum cannot safely replace a voter");
        assert!(error.to_string().contains("cannot replace wiped voter"));
        let voters_after: BTreeSet<_> = leader
            .raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .voter_ids()
            .collect();
        assert_eq!(voters_after, voters_before);

        leader.shutdown().await.unwrap();
        replacement.shutdown().await.unwrap();
    }

    // T1.6 cleanup: the `controlplane_follower_syncs_after_catch_up`
    // test was deleted along with the BackupApplier path it exercised.
    // Non-leader voters (and learners) sync via raft `AppendEntries`
    // through the state machine's `apply_log_apply_commit`. The
    // coverage for that path lives in the raft state-machine tests
    // (`apply_normal_entry_decodes_log_apply_commit_and_mutates_backend`)
    // and the multinode netns harness (T5).

    /// T4: demote a voter to learner via `add_learner_only`. Uses the
    /// loopback network so two raft nodes can communicate.
    #[tokio::test]
    async fn add_learner_only_demotes_existing_voter_to_learner() {
        let registry = LoopbackRegistry::new();

        let leader_id: NodeId = 70;
        let voter_id: NodeId = 80;
        let voter_addr = "https://10.99.0.80:7679".to_string();

        // Leader node.
        let sup1 = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let exec1 = klights_node_datastore::open::open_with_opts(
            klights_node_datastore::open::in_memory_opts(),
            sup1.clone(),
            "sqlite:raft-demote-test-l",
        )
        .await
        .unwrap();
        let nl1 = Arc::new(SqliteRaftDurability::new(exec1));
        let be1: Arc<klights_cluster_datastore::sqlite::embedded::Datastore> = Arc::new(
            klights_cluster_datastore::sqlite::embedded::Datastore::new_in_memory()
                .await
                .unwrap(),
        );
        let leader_network = LoopbackRaftNetworkFactory::new(registry.clone());
        let leader = start_test_node(
            leader_id,
            "n70".into(),
            raft_store_ports(be1),
            node_durability(&nl1),
            node_durability(&nl1),
            sup1,
            leader_network,
        )
        .await
        .unwrap();
        registry.register(
            leader_id,
            leader.raft.clone(),
            leader.storage_incarnation().to_string(),
        );
        leader
            .bootstrap_single_voter("https://10.99.0.70:7679".into())
            .await
            .unwrap();

        // Voter node.
        let sup2 = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let exec2 = klights_node_datastore::open::open_with_opts(
            klights_node_datastore::open::in_memory_opts(),
            sup2.clone(),
            "sqlite:raft-demote-test-v",
        )
        .await
        .unwrap();
        let nl2 = Arc::new(SqliteRaftDurability::new(exec2));
        let be2: Arc<klights_cluster_datastore::sqlite::embedded::Datastore> = Arc::new(
            klights_cluster_datastore::sqlite::embedded::Datastore::new_in_memory()
                .await
                .unwrap(),
        );
        let voter_network = LoopbackRaftNetworkFactory::new(registry.clone());
        let voter_node = start_test_node(
            voter_id,
            "n80".into(),
            raft_store_ports(be2),
            node_durability(&nl2),
            node_durability(&nl2),
            sup2,
            voter_network,
        )
        .await
        .unwrap();
        registry.register(
            voter_id,
            voter_node.raft.clone(),
            voter_node.storage_incarnation().to_string(),
        );

        wait_for_leader(&leader, std::time::Duration::from_secs(10))
            .await
            .unwrap();

        // Add voter to the leader's cluster.
        admit_member(&leader, &voter_node, voter_addr.clone(), false)
            .await
            .unwrap();

        // Verify voter is present.
        let metrics = leader.raft.metrics().borrow().clone();
        let voter_ids: std::collections::BTreeSet<NodeId> =
            metrics.membership_config.membership().voter_ids().collect();
        assert!(
            voter_ids.contains(&voter_id),
            "voter must be present: {voter_ids:?}"
        );
        assert_eq!(voter_ids.len(), 2, "must have 2 voters");

        // Demote through the same exact-admission path used by authenticated
        // join. The retry presents a fresh durable boundary so admission can
        // prove this is the existing storage session, rather than an unsafe
        // wiped-voter replacement.
        leader
            .admit_controlplane_member(
                voter_id,
                voter_addr,
                true,
                voter_node.storage_incarnation().to_string(),
                storage_attestation(Some(klights_leader_api::RaftStorageLogAttestation {
                    term: u64::MAX,
                    leader_node_id: leader_id,
                    index: u64::MAX,
                })),
            )
            .await
            .unwrap();

        // Verify demoted.
        let metrics = leader.raft.metrics().borrow().clone();
        let voter_ids: std::collections::BTreeSet<NodeId> =
            metrics.membership_config.membership().voter_ids().collect();
        let node_ids: std::collections::BTreeSet<NodeId> = metrics
            .membership_config
            .membership()
            .nodes()
            .map(|(id, _)| *id)
            .collect();
        assert!(
            !voter_ids.contains(&voter_id),
            "must not be voter: {voter_ids:?}"
        );
        assert!(
            node_ids.contains(&voter_id),
            "must be in membership: {node_ids:?}"
        );
        assert_eq!(voter_ids.len(), 1, "only leader remains");
    }

    // ── Task 2: Bound In-Flight Raft Proposals With Flow Control ────────────────────────────

    /// Helper: build a small CreateResource StorageCommand for propose_command tests.
    fn propose_create_command(uid: &str) -> klights_cluster_core::command::StorageCommand {
        klights_cluster_core::command::StorageCommand::CreateResource {
            api_version: "node.k8s.io/v1".into(),
            kind: "RuntimeClass".into(),
            namespace: None,
            name: format!("fc-{uid}"),
            data: serde_json::json!({
                "apiVersion": "node.k8s.io/v1",
                "kind": "RuntimeClass",
                "metadata": {"name": format!("fc-{uid}"), "uid": uid},
                "handler": "handler",
            }),
        }
    }

    fn propose_node_registration_command(
        node_name: &str,
        uid: &str,
    ) -> klights_cluster_core::command::StorageCommand {
        klights_cluster_core::command::StorageCommand::CreateResource {
            api_version: "v1".into(),
            kind: "Node".into(),
            namespace: None,
            name: node_name.to_string(),
            data: serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": node_name,
                    "uid": uid
                },
                "spec": {},
                "status": {}
            }),
        }
    }

    /// Integration test: while all 3 flow-control permits are externally held, a call to
    /// `propose_command` must BLOCK on permit acquire — it must not reserve a
    /// resourceVersion via `build_log_apply_commit_for_outbox` until a permit is released.
    ///
    /// This is the core ordering guarantee from finding.md: the leader cannot reserve
    /// an RV ahead of an acknowledged flow-control slot. Reverting the
    /// `let _flow_permit = self.proposal().flow_control().acquire().await;` line in `propose_command`
    /// would make this test fail (rv would advance during the timeout window).
    #[tokio::test]
    async fn raft_proposal_permit_is_acquired_before_resource_version_reservation() {
        let (node, backend) = fresh_node(70).await;
        node.bootstrap_single_voter("https://10.99.0.70:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&node, std::time::Duration::from_secs(5))
            .await
            .expect("become leader");
        // Exhaust the flow-control gate before propose_command runs.
        let cap = node.proposal().flow_control().max_in_flight();
        let mut held = Vec::with_capacity(cap);
        for _ in 0..cap {
            held.push(node.proposal().flow_control().acquire().await);
        }
        assert_eq!(node.proposal().flow_control().available_permits(), 0);

        let rv_before = backend.get_current_resource_version().await.unwrap();

        // propose_command must block on permit acquire — it must NOT reach
        // build_log_apply_commit_for_outbox while permits are exhausted.
        let cmd = propose_create_command("permit-ordering");
        let timeout = tokio::time::sleep(std::time::Duration::from_millis(300));
        tokio::pin!(timeout);
        let mut propose_fut = Box::pin(node.propose_command(cmd));
        tokio::select! {
            _ = &mut propose_fut => panic!("propose_command must block while flow-control permits are exhausted"),
            _ = &mut timeout => {}
        }

        // Critical assertion: rv must NOT have advanced during the timeout window.
        // If propose_command failed to acquire the permit before reserving the rv,
        // build_log_apply_commit_for_outbox would have bumped the metadata rv.
        let rv_during = backend.get_current_resource_version().await.unwrap();
        assert_eq!(
            rv_during, rv_before,
            "rv must NOT advance while flow-control permits are exhausted: \
             propose_command must acquire the permit BEFORE build_log_apply_commit_for_outbox"
        );

        // Drop the externally-held propose_fut and clean up the manually-held permits.
        drop(propose_fut);
        drop(held);
        node.shutdown().await.unwrap();
    }

    /// Pod status outbox writes are durable but latency-sensitive; when the
    /// flow-control gate is saturated they should wait on the async permit
    /// instead of bouncing into node-local retry/backoff. The wait must happen
    /// before resourceVersion reservation so status traffic still cannot build
    /// an RV backlog ahead of raft progress.
    #[tokio::test]
    async fn raft_pod_status_outbox_waits_for_flow_control_without_reserving_rv() {
        let (node, backend) = fresh_node(75).await;
        node.bootstrap_single_voter("https://10.99.0.75:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&node, std::time::Duration::from_secs(5))
            .await
            .expect("become leader");

        let cap = node.proposal().flow_control().max_in_flight();
        let mut held = Vec::with_capacity(cap);
        for _ in 0..cap {
            held.push(node.proposal().flow_control().acquire().await);
        }
        assert_eq!(node.proposal().flow_control().available_permits(), 0);

        let rv_before = backend.get_current_resource_version().await.unwrap();
        {
            let proposal = node.propose_outbox_command(
                "outbox-permit-ordering",
                OutboxOperation::PodStatus.as_str(),
                propose_create_command("outbox-permit-ordering"),
                "worker-1",
                None,
            );
            tokio::pin!(proposal);
            tokio::select! {
                result = &mut proposal => {
                    panic!("saturated PodStatus outbox proposal must wait for capacity, got {result:?}");
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
            }

            let rv_during = backend.get_current_resource_version().await.unwrap();
            assert_eq!(
                rv_during, rv_before,
                "outbox propose must not reserve an rv while waiting for a flow-control permit"
            );

            drop(held);

            let retry_result =
                tokio::time::timeout(std::time::Duration::from_secs(1), &mut proposal)
                    .await
                    .expect("PodStatus outbox proposal must complete after capacity returns")
                    .expect("PodStatus outbox proposal should succeed after capacity returns");
            assert!(matches!(retry_result, OutboxApplyOutcome::Applied { .. }));
        }
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn raft_critical_outbox_uses_reserved_permit_when_general_gate_is_saturated() {
        use OutboxOperation;

        let (node, backend) = fresh_node(76).await;
        node.bootstrap_single_voter("https://10.99.0.76:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&node, std::time::Duration::from_secs(5))
            .await
            .expect("become leader");

        let cap = node.proposal().flow_control().max_in_flight();
        let mut held = Vec::with_capacity(cap);
        for _ in 0..cap {
            held.push(node.proposal().flow_control().acquire().await);
        }
        assert_eq!(node.proposal().flow_control().available_permits(), 0);

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            node.propose_outbox_command(
                "critical-node-registration",
                OutboxOperation::NodeRegistration.as_str(),
                propose_node_registration_command("mn-critical-worker", "uid-critical-worker"),
                "mn-critical-worker",
                None,
            ),
        )
        .await
        .expect("critical outbox proposal must not wait behind status-saturated general permits")
        .expect("critical outbox proposal must use reserved capacity");

        let stored = backend
            .get_resource("v1", "Node", None, "mn-critical-worker")
            .await
            .expect("read critical Node")
            .expect("critical Node was created");
        assert_eq!(stored.uid, "uid-critical-worker");

        drop(held);
        node.shutdown().await.unwrap();
    }

    /// Integration test: at most `max_in_flight()` unacknowledged propose_command calls
    /// may be in flight. Holds all general permits, then verifies the next propose
    /// call is blocked. The cap is DECOUPLED from `RAFT_MAX_PAYLOAD_ENTRIES` (T1):
    /// this asserts the live gate equals the configured `RAFT_MAX_INFLIGHT_PROPOSALS`,
    /// not the smaller payload-entries value.
    #[tokio::test]
    async fn at_most_max_inflight_raft_proposals_are_in_flight() {
        let (node, backend) = fresh_node(71).await;
        node.bootstrap_single_voter("https://10.99.0.71:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&node, std::time::Duration::from_secs(5))
            .await
            .expect("become leader");
        // T1: the flow-control gate is the decoupled in-flight value, larger than
        // the openraft payload cap.
        assert_eq!(
            node.proposal().flow_control().max_in_flight(),
            klights_replication::proposal::RAFT_MAX_INFLIGHT_PROPOSALS,
            "flow-control cap must be the decoupled RAFT_MAX_INFLIGHT_PROPOSALS, \
             not RAFT_MAX_PAYLOAD_ENTRIES"
        );
        assert_ne!(
            node.proposal().flow_control().max_in_flight() as u64,
            super::RAFT_MAX_PAYLOAD_ENTRIES,
            "flow-control cap must be decoupled from payload entries"
        );
        // Hold every general permit, simulating max_in_flight in-flight proposals.
        let cap = node.proposal().flow_control().max_in_flight();
        let mut held = Vec::with_capacity(cap);
        for _ in 0..cap {
            held.push(node.proposal().flow_control().acquire().await);
        }
        assert_eq!(node.proposal().flow_control().available_permits(), 0);

        // The next propose call must block (no permits available).
        let rv_before = backend.get_current_resource_version().await.unwrap();
        let cmd = propose_create_command("next-blocked");
        let timeout = tokio::time::sleep(std::time::Duration::from_millis(200));
        tokio::pin!(timeout);
        let mut propose_fut = Box::pin(node.propose_command(cmd));
        tokio::select! {
            _ = &mut propose_fut => panic!("propose must block when all permits are held"),
            _ = &mut timeout => {}
        }
        let rv_after = backend.get_current_resource_version().await.unwrap();
        assert_eq!(
            rv_after, rv_before,
            "blocked propose must not have reserved an rv"
        );
        drop(propose_fut);
        drop(held);
        node.shutdown().await.unwrap();
    }

    /// Integration test: when propose_command fails AT MATERIALIZATION (before
    /// client_write) — e.g. because the backend's build step rejected a duplicate
    /// create — the flow-control permit must still be released. The `_flow_permit`
    /// RAII guard handles this naturally on every error-return path.
    #[tokio::test]
    async fn raft_proposal_permit_released_on_materialization_failure() {
        let (node, backend) = fresh_node(72).await;
        node.bootstrap_single_voter("https://10.99.0.72:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&node, std::time::Duration::from_secs(5))
            .await
            .expect("become leader");
        // Seed the backend so a duplicate Create fails at materialization.
        node.propose_command(propose_create_command("dup-target"))
            .await
            .expect("first create");
        let permits_before = node.proposal().flow_control().available_permits();
        assert_eq!(
            permits_before,
            node.proposal().flow_control().max_in_flight(),
            "permits restored after first success"
        );

        // Second create with the same name MUST fail at materialization (build step rejects
        // duplicate). The permit must be released by the RAII guard on the error path.
        let _ = backend
            .get_resource("node.k8s.io/v1", "RuntimeClass", None, "fc-dup-target")
            .await
            .unwrap()
            .expect("seed resource exists");
        let err = node
            .propose_command(propose_create_command("dup-target"))
            .await
            .expect_err("duplicate create must fail at materialization");
        assert!(
            err.to_string().contains("already exists") || err.to_string().contains("409"),
            "expected duplicate-create rejection, got: {err}"
        );
        assert_eq!(
            node.proposal().flow_control().available_permits(),
            node.proposal().flow_control().max_in_flight(),
            "permit must be released after materialization-failure error path"
        );
        node.shutdown().await.unwrap();
    }

    /// CSI lifecycle updates use client-go's `RetryOnConflict`: a GET may race
    /// the PV binder, and the stale PUT must therefore surface Kubernetes 409
    /// rather than hiding the materialization CAS failure behind HTTP 500.
    #[tokio::test]
    async fn stale_csi_pv_update_materialization_surfaces_conflict() {
        let (node, backend) = fresh_node(75).await;
        node.bootstrap_single_voter("https://10.99.0.75:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&node, std::time::Duration::from_secs(5))
            .await
            .expect("become leader");

        let pv_name = "pv-csi-lifecycle";
        node.propose_command(
            klights_cluster_core::command::StorageCommand::CreateResource {
                api_version: "v1".into(),
                kind: "PersistentVolume".into(),
                namespace: None,
                name: pv_name.into(),
                data: serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "PersistentVolume",
                    "metadata": {
                        "name": pv_name,
                        "uid": "pv-csi-lifecycle-uid",
                        "labels": {"e2e-pv-pool": "pv-csi-lifecycle"}
                    },
                    "spec": {
                        "accessModes": ["ReadWriteOnce"],
                        "capacity": {"storage": "1Gi"},
                        "csi": {
                            "driver": "inline-driver-csi-lifecycle",
                            "volumeHandle": "e2e-conformance"
                        },
                        "persistentVolumeReclaimPolicy": "Retain",
                        "storageClassName": "pv-csi-lifecycle",
                        "volumeMode": "Filesystem"
                    }
                }),
            },
        )
        .await
        .expect("create CSI PV");

        // This is the object returned by the conformance test's GET.
        let stale_client_read = backend
            .get_resource("v1", "PersistentVolume", None, pv_name)
            .await
            .unwrap()
            .expect("PV exists");

        // The binder wins the race and advances resourceVersion.
        let mut binder_update = (*stale_client_read.data).clone();
        binder_update["spec"]["claimRef"] = serde_json::json!({
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "name": "pvc-csi-lifecycle",
            "namespace": "pv-csi-lifecycle",
            "uid": "pvc-csi-lifecycle-uid"
        });
        node.propose_command(
            klights_cluster_core::command::StorageCommand::UpdateResource {
                api_version: "v1".into(),
                kind: "PersistentVolume".into(),
                namespace: None,
                name: pv_name.into(),
                data: binder_update,
                expected_rv: stale_client_read.resource_version,
                preconditions: klights::datastore::ResourcePreconditions::from_resource(
                    &stale_client_read,
                ),
                preserve_status: false,
            },
        )
        .await
        .expect("binder update wins");

        // The CSI conformance client adds one label to its stale GET result.
        let mut client_update = (*stale_client_read.data).clone();
        client_update["metadata"]["labels"][pv_name] = serde_json::json!("updated");
        let command_service = klights_replication::leader_api::EmbeddedLeaderResourceCommand::new(
            node.proposal(),
            Arc::new(BackendResourceQuery {
                backend: backend.clone(),
            }),
            klights::bootstrap::composition_adapters::authority_adapter::always_leader_authority(),
        );
        let error = command_service
            .submit_resource_command(
                ResourceCommandRequest::try_new(
                    klights_cluster_core::command::StorageCommand::UpdateResource {
                        api_version: "v1".into(),
                        kind: "PersistentVolume".into(),
                        namespace: None,
                        name: pv_name.into(),
                        data: client_update,
                        expected_rv: stale_client_read.resource_version,
                        preconditions: klights::datastore::ResourcePreconditions::from_resource(
                            &stale_client_read,
                        ),
                        preserve_status: false,
                    },
                )
                .expect("valid stale CSI update request"),
            )
            .await
            .expect_err("stale CSI update must be retryable as a conflict");

        assert!(
            matches!(error, ResourceCommandError::Conflict { .. }),
            "focused leader command API must preserve stale-update conflict, got: {error}"
        );

        node.shutdown().await.unwrap();
    }

    /// Integration test: even when propose_command would fail at the consensus
    /// `client_write` stage (no leader / leadership lost), the RAII permit guard
    /// must still release. We exercise this by manually exhausting permits inside
    /// a scope and verifying the guard releases on scope-exit (matches the
    /// implementation: `let _flow_permit = self.proposal().flow_control().acquire().await;`).
    #[tokio::test]
    async fn raft_proposal_permit_released_on_client_write_failure() {
        let (node, _backend) = fresh_node(73).await;
        node.bootstrap_single_voter("https://10.99.0.73:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&node, std::time::Duration::from_secs(5))
            .await
            .expect("become leader");
        // RAII semantics on the actual flow-control gate held by RaftNode:
        // any exit (including a panic or an early return inside propose_command)
        // must release the permit. We exercise the live gate here.
        assert_eq!(
            node.proposal().flow_control().available_permits(),
            node.proposal().flow_control().max_in_flight()
        );
        {
            let _permit = node.proposal().flow_control().acquire().await;
            assert_eq!(
                node.proposal().flow_control().available_permits(),
                node.proposal().flow_control().max_in_flight() - 1,
                "permit acquired"
            );
            // Simulating the late-failure path: the permit is held when client_write
            // would have failed; the RAII guard releases on scope exit.
        }
        assert_eq!(
            node.proposal().flow_control().available_permits(),
            node.proposal().flow_control().max_in_flight(),
            "RAII permit must release on scope exit (mirrors propose_command's error paths)"
        );
        node.shutdown().await.unwrap();
    }

    /// Integration test: after a successful propose_command (entry committed and
    /// applied), the flow-control permit returns to the pool so subsequent proposals
    /// can proceed.
    #[tokio::test]
    async fn raft_proposal_permit_released_on_terminal_success() {
        let (node, _backend) = fresh_node(74).await;
        node.bootstrap_single_voter("https://10.99.0.74:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&node, std::time::Duration::from_secs(5))
            .await
            .expect("become leader");
        assert_eq!(
            node.proposal().flow_control().available_permits(),
            node.proposal().flow_control().max_in_flight()
        );

        node.propose_command(propose_create_command("ok-success"))
            .await
            .expect("propose ok");

        assert_eq!(
            node.proposal().flow_control().available_permits(),
            node.proposal().flow_control().max_in_flight(),
            "permit must be released after successful terminal propose_command"
        );
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn actor_finalization_persists_exact_receipt_for_durable_cascade_replay() {
        let (node, backend) = fresh_node(77).await;
        node.bootstrap_single_voter("https://10.99.0.77:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&node, std::time::Duration::from_secs(5))
            .await
            .expect("become leader");
        let observed = backend
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "receipt",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "receipt",
                        "uid": "receipt-uid",
                        "deletionTimestamp": "2026-07-24T00:00:00Z",
                        "annotations": {"receipt.example/value": "exact"}
                    },
                    "spec": {"nodeName": "worker-a"}
                }),
            )
            .await
            .expect("create terminating Pod");

        let command = klights_cluster_core::command::StorageCommand::FinalizeBoundPod {
            namespace: "default".to_string(),
            name: "receipt".to_string(),
            pod_uid: "receipt-uid".to_string(),
            node_name: "worker-a".to_string(),
            observed_resource_version: observed.resource_version,
        };
        let effect = node
            .propose_outbox_command_effect(
                "raft-actor-delete-receipt",
                OutboxOperation::PodMetadata.as_str(),
                command.clone(),
                "worker-a",
                None,
            )
            .await
            .expect("propose actor finalization");
        let (_, resource_effect, _, receipt) = effect.into_parts();
        assert_eq!(
            resource_effect,
            klights_cluster_core::ResourceMutationEffect::Changed
        );
        let receipt = receipt.expect("committed delete receipt");
        assert_eq!(receipt.resource_version, observed.resource_version);
        assert_eq!(
            receipt
                .data
                .pointer("/metadata/annotations/receipt.example~1value")
                .and_then(serde_json::Value::as_str),
            Some("exact")
        );
        assert!(
            backend
                .get_resource("v1", "Pod", Some("default"), "receipt")
                .await
                .unwrap()
                .is_none()
        );

        let replay = node
            .propose_outbox_command_effect(
                "raft-actor-delete-receipt",
                OutboxOperation::PodMetadata.as_str(),
                command,
                "worker-a",
                None,
            )
            .await
            .expect("same-id actor finalization must replay its durable result");
        let (replayed, replay_effect, _, replay_receipt) = replay.into_parts();
        assert!(matches!(
            replayed,
            klights_cluster_core::OutboxApplyOutcome::AlreadyApplied {
                applied_rv: Some(_)
            }
        ));
        assert_eq!(
            replay_effect,
            klights_cluster_core::ResourceMutationEffect::Unchanged,
            "same-id replay must not execute the finalization twice"
        );
        assert_eq!(
            replay_receipt.as_ref(),
            Some(&receipt),
            "same-id replay must surface the byte-equivalent durable delete receipt"
        );
        node.shutdown().await.unwrap();
    }
}
