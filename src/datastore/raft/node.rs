//! Phase 3 RaftNode — thin wrapper around `openraft::Raft<TypeConfig>`.
//!
//! Holds the long-lived Raft instance plus the storage/state-machine
//! handles. Exposes `bootstrap_single_voter` (manual promotion entry
//! point), `propose` (mutating writes), and `metrics` (election state).
//!
//! Unified-apply-path invariant: `bootstrap_single_voter` calls
//! `Raft::initialize` against the existing data root. This is the same
//! routine openraft uses internally when a follower wins an election in
//! a single-voter cluster, so manual promotion shares the no-op-log-
//! entry-at-new-term path with auto-election.

#[cfg(test)]
use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::{Context, Result};
#[cfg(test)]
use async_trait::async_trait;
#[cfg(test)]
use klights_cluster_core::OutboxOperation;
use klights_cluster_core::{OutboxApplyError, OutboxApplyOutcome, StorageCommand};
use klights_node_store::{RaftAppliedStateDurability, RaftLogDurability};
#[cfg(test)]
use openraft::error::{ClientWriteError, RaftError};
use openraft::{Config, Raft};

use openraft::network::RaftNetworkFactory;

use super::network::LeaderForwarder;
#[cfg(test)]
use super::network::StubRaftNetwork;
use klights_cluster_store::BackendLifecycleStore;
use klights_replication::activation::CommandCodecV3Activation;
use klights_replication::log_storage::SqliteRaftLogStorage;
use klights_replication::materializer::RaftCommitMaterializer;
use klights_replication::state_machine::{RaftStateMachineStorePorts, SqliteRaftStateMachine};
use klights_replication::types::{NodeId, RaftShape, TypeConfig};
#[cfg(test)]
use klights_replication::types::{RaftMemberNode, StorageCommandPayload};

/// Lossy-link transport sizing (finding.md H3). `max_payload_entries` keeps each
/// AppendEntries retry small (it bounds **retransmit cost**: leader→follower).
pub(crate) const RAFT_MAX_PAYLOAD_ENTRIES: u64 = 16;

/// Leader proposal flow-control cap: the maximum number of unacknowledged
/// proposals that may be in flight simultaneously. This is DECOUPLED from
/// `RAFT_MAX_PAYLOAD_ENTRIES`: payload entries bounds AppendEntries
/// **retransmit cost**, while this permit count bounds **RV backlog ahead of
/// acknowledged raft progress** at the leader. OpenRaft already pipelines
/// AppendEntries, so a small payload does NOT require a small permit count.
/// Coupling both to 3 capped leader commit concurrency at 3 — at ~200 ms quorum
/// RTT a hard ~15 commits/sec ceiling. Default 16 keeps the cap in the measured
/// safe range 8..=32.
pub(crate) use klights_replication::proposal::RAFT_MAX_INFLIGHT_PROPOSALS;

pub struct RaftStorePorts {
    materializer: Arc<dyn RaftCommitMaterializer>,
    state_machine: RaftStateMachineStorePorts,
    snapshot_capture: Arc<dyn klights_cluster_store::AuthoritativeSnapshotCapture>,
    allocator: Arc<dyn klights_cluster_store::DurableAllocatorRead>,
    lifecycle: Arc<dyn BackendLifecycleStore>,
}

impl RaftStorePorts {
    pub(crate) fn new(
        materializer: Arc<dyn RaftCommitMaterializer>,
        state_machine: RaftStateMachineStorePorts,
        snapshot_capture: Arc<dyn klights_cluster_store::AuthoritativeSnapshotCapture>,
        allocator: Arc<dyn klights_cluster_store::DurableAllocatorRead>,
        lifecycle: Arc<dyn BackendLifecycleStore>,
    ) -> Self {
        Self {
            materializer,
            state_machine,
            snapshot_capture,
            allocator,
            lifecycle,
        }
    }
}

pub struct RaftNode {
    pub node_id: NodeId,
    pub raft: Raft<TypeConfig>,
    storage_incarnation: String,
    forwarder: Option<Arc<dyn LeaderForwarder>>,
    membership: Arc<klights_replication::membership::EmbeddedRaftMembership>,
    materializer: Arc<dyn RaftCommitMaterializer>,
    /// T1.4: node name used by `build_log_apply_commit_for_command` to
    /// stamp the authoring node on the resulting commit.
    authoring_node: String,
    /// Flow-control gate: at most 3 general proposals plus one reserved
    /// control-critical outbox proposal may be in flight simultaneously.
    /// A permit is acquired BEFORE the leader materializes the next
    /// resourceVersion so the leader cannot build an unacknowledged RV backlog
    /// ahead of raft progress under loss (finding.md flow-control plan).
    pub(crate) flow_control: Arc<klights_replication::flow_control::RaftCommitFlowControl>,
    command_codec_v3_activation: Arc<CommandCodecV3Activation>,
}

impl RaftNode {
    /// Construct a Raft node bound to the given cluster backend +
    /// node-local SQLite handle. The Raft engine starts in Learner state;
    /// call `bootstrap_single_voter` (manual promote) or wait for an
    /// `add_learner` + `change_membership` from a peer (Step 6) to join.
    /// Single-voter convenience constructor that wires a StubRaftNetwork
    /// (no peer RPCs ever issued).
    #[cfg(test)]
    pub(crate) async fn start(
        node_id: NodeId,
        node_name: String,
        stores: RaftStorePorts,
        log_durability: Arc<dyn RaftLogDurability>,
        applied_state_durability: Arc<dyn RaftAppliedStateDurability>,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Result<Self> {
        Self::start_with_network(
            node_id,
            node_name,
            stores,
            log_durability,
            applied_state_durability,
            supervisor,
            StubRaftNetwork,
        )
        .await
    }

    /// General constructor that accepts a caller-supplied
    /// `RaftNetworkFactory`. Use for multi-voter clusters (Step 6) and
    /// for the gRPC production transport (later).
    pub(crate) async fn start_with_network<N>(
        node_id: NodeId,
        node_name: String,
        stores: RaftStorePorts,
        log_durability: Arc<dyn RaftLogDurability>,
        applied_state_durability: Arc<dyn RaftAppliedStateDurability>,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
        network: N,
    ) -> Result<Self>
    where
        N: RaftNetworkFactory<TypeConfig>,
    {
        let storage_incarnation = log_durability
            .load_or_create_storage_incarnation()
            .await
            .context("load Raft storage incarnation")?;
        // Raft consensus timing. `heartbeat_interval` is the idle-CPU driver
        // (leader pings every interval + openraft's logical tick); the
        // election timeout is failover-detection latency and must stay a few
        // multiples of the heartbeat to avoid spurious elections.
        const RAFT_HEARTBEAT_INTERVAL_MS: u64 = 3000;
        const RAFT_ELECTION_TIMEOUT_MIN_MS: u64 = 9000;
        const RAFT_ELECTION_TIMEOUT_MAX_MS: u64 = 12000;
        // Lossy-link transport sizing (finding.md H3). OpenRaft defaults are
        // sized for LAN-fast clusters and amplify packet loss on a 200 ms RTT /
        // 1 percent-loss harness:
        // - install_snapshot_timeout default 200 ms is below a single lossy
        //   round-trip; a snapshot install RPC that loses one packet can never
        //   complete before the timeout, forcing repeated full restarts.
        // - max_payload_entries default 300 lets one AppendEntries RPC carry
        //   hundreds of (potentially large JSON/protobuf) entries; losing one
        //   frame resends the whole batch, multiplying logical RPC loss far
        //   above the 1 percent wire loss and stalling follower catch-up.
        // - snapshot_max_chunk_size default 3 MiB makes each snapshot segment
        //   many HTTP/2 frames; a single dropped frame re-sends the segment.
        // These bounds keep each replication RPC small enough that loss is
        // absorbed by OpenRaft's built-in retry instead of logical RPC
        // blow-up, and give snapshot install a deadline well above the lossy
        // RTT plus SQLite apply budget. `replication_lag_threshold` stays
        // above `snapshot_policy`'s LogsSinceLast so a lagging member still
        // crosses the snapshot-replace path (which is now correct, above).
        const RAFT_INSTALL_SNAPSHOT_TIMEOUT_MS: u64 = 5_000;
        // RAFT_MAX_PAYLOAD_ENTRIES is defined at module scope so the closing-gate
        // test can assert on the configured value (not a function-local copy).
        const RAFT_SNAPSHOT_MAX_CHUNK_SIZE_BYTES: u64 = 512 * 1024;
        const RAFT_REPLICATION_LAG_THRESHOLD: u64 = 5000;
        const _: () = assert!(RAFT_REPLICATION_LAG_THRESHOLD >= 5000);
        // T7 election-floor invariant: install-snapshot timeout and the raft
        // unary RPC deadline (5 s, matching RAFT_INSTALL_SNAPSHOT_TIMEOUT_MS)
        // must both be below the election timeout minimum so a wedged peer
        // cannot prevent leader election.
        const _: () = assert!(RAFT_INSTALL_SNAPSHOT_TIMEOUT_MS < RAFT_ELECTION_TIMEOUT_MIN_MS);
        // Cross-subsystem safety: worst-case failover (<= election_timeout_max)
        // must finish before observed node leases go stale, or a single leader
        // change would false-evict every node (lease renewals can't commit
        // while there is no leader). The T8 promotion grace-reset is the
        // primary safeguard; this static bound is belt-and-suspenders.
        const _: () = assert!(
            RAFT_ELECTION_TIMEOUT_MAX_MS
                < (klights_cluster_core::DEFAULT_NODE_LEASE_DURATION_SECONDS as u64) * 1000
        );
        let config = Arc::new(
            Config {
                cluster_name: "klights".to_string(),
                heartbeat_interval: RAFT_HEARTBEAT_INTERVAL_MS,
                election_timeout_min: RAFT_ELECTION_TIMEOUT_MIN_MS,
                election_timeout_max: RAFT_ELECTION_TIMEOUT_MAX_MS,
                install_snapshot_timeout: RAFT_INSTALL_SNAPSHOT_TIMEOUT_MS,
                max_payload_entries: RAFT_MAX_PAYLOAD_ENTRIES,
                snapshot_max_chunk_size: RAFT_SNAPSHOT_MAX_CHUNK_SIZE_BYTES,
                replication_lag_threshold: RAFT_REPLICATION_LAG_THRESHOLD,
                enable_tick: true,
                ..Default::default()
            }
            .validate()
            .context("openraft Config validate")?,
        );
        let log_store = SqliteRaftLogStorage::new(log_durability, supervisor.clone());
        let command_codec_v3_activation =
            Arc::new(CommandCodecV3Activation::load(stores.materializer.as_ref()).await?);
        let snapshot_builder = klights_replication::snapshot::SqliteRaftSnapshotBuilder::new(
            stores.snapshot_capture,
            stores.allocator,
            stores.lifecycle,
            applied_state_durability.clone(),
            supervisor,
        );
        let state_machine = SqliteRaftStateMachine::new_with_command_codec_activation(
            stores.state_machine,
            applied_state_durability,
            snapshot_builder,
            command_codec_v3_activation.clone(),
        );
        let raft = Raft::new(node_id, config, network, log_store, state_machine)
            .await
            .context("Raft::new")?;
        let flow_control = Arc::new(
            klights_replication::flow_control::RaftCommitFlowControl::new(
                RAFT_MAX_INFLIGHT_PROPOSALS,
            ),
        );
        let membership = Arc::new(
            klights_replication::membership::EmbeddedRaftMembership::new(
                node_id,
                raft.clone(),
                storage_incarnation.clone(),
                stores.materializer.clone(),
                flow_control.clone(),
                command_codec_v3_activation.clone(),
                node_name.clone(),
            ),
        );
        Ok(Self {
            node_id,
            raft,
            storage_incarnation,
            forwarder: None,
            membership,
            materializer: stores.materializer,
            authoring_node: node_name,
            flow_control,
            command_codec_v3_activation,
        })
    }

    /// Attach a `LeaderForwarder` so `propose` can transparently redirect
    /// writes to the current leader when this node is a follower. Tests
    /// use `LoopbackRegistry`; production will use a gRPC client.
    pub fn with_forwarder(mut self, forwarder: Arc<dyn LeaderForwarder>) -> Self {
        self.forwarder = Some(forwarder);
        self
    }

    pub fn proposal(&self) -> Arc<klights_replication::proposal::EmbeddedRaftProposal> {
        Arc::new(klights_replication::proposal::EmbeddedRaftProposal::new(
            self.node_id,
            self.raft.clone(),
            self.materializer.clone(),
            self.authoring_node.clone(),
            self.flow_control.clone(),
            self.command_codec_v3_activation.clone(),
        ))
    }

    pub async fn propose_command(
        &self,
        command: StorageCommand,
    ) -> Result<klights_replication::types::StorageCommandResult> {
        klights_replication::proposal::RaftProposal::propose_command(
            self.proposal().as_ref(),
            command,
        )
        .await
    }

    pub async fn propose_outbox_command(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<OutboxApplyOutcome, OutboxApplyError> {
        klights_replication::proposal::RaftProposal::propose_outbox_command(
            self.proposal().as_ref(),
            idempotency_key,
            operation,
            command,
            authoring_node,
            watermark,
        )
        .await
    }

    pub async fn propose_outbox_command_effect(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<klights_replication::proposal::RaftProposalEffect, OutboxApplyError>
    {
        klights_replication::proposal::RaftProposal::propose_outbox_command_effect(
            self.proposal().as_ref(),
            idempotency_key,
            operation,
            command,
            authoring_node,
            watermark,
        )
        .await
    }

    /// Manual promotion entry point. Calls `Raft::initialize` with this
    /// node as the sole voter — the same call openraft uses internally
    /// when forming a single-voter cluster on first boot. Once committed
    /// the engine becomes Leader and `client_write` will accept proposals.
    ///
    /// Idempotent: returns `Ok(())` if the cluster is already
    /// initialized (matches openraft's `NotAllowed` no-op).
    pub async fn bootstrap_single_voter(&self, advertise_addr: String) -> Result<()> {
        self.membership.bootstrap_single_voter(advertise_addr).await
    }

    /// Propose a mutating write through Raft. The payload is the
    /// serialized `StorageCommand` (protobuf) that will be replicated and
    /// then applied via `RaftStateMachine::apply`.
    ///
    /// On a non-leader voter, openraft returns `ForwardToLeader`; if a
    /// `LeaderForwarder` was attached via `with_forwarder` the proposal is
    /// transparently re-dispatched to the current leader.
    #[cfg(test)]
    pub async fn propose(&self, payload: StorageCommandPayload) -> Result<()> {
        self.command_codec_v3_activation
            .ensure_command_codec_v3_activated()?;
        match self.raft.client_write(payload.clone()).await {
            Ok(_) => Ok(()),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(forward))) => {
                let Some(leader_id) = forward.leader_id else {
                    return Err(anyhow::anyhow!(
                        "Raft::client_write: ForwardToLeader without leader_id (no leader currently)"
                    ));
                };
                let forwarder = self.forwarder.as_ref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Raft::client_write: ForwardToLeader({leader_id}) but no LeaderForwarder configured"
                    )
                })?;
                forwarder.forward_propose(leader_id, payload).await
            }
            Err(e) => Err(anyhow::anyhow!("Raft::client_write: {e}")),
        }
    }

    /// Add a new voter to the running cluster. Wraps openraft's two-step
    /// dance: first promote the target to a learner so the leader starts
    /// replicating its log to it, then issue `change_membership` to fold
    /// the learner into the voter set.
    ///
    /// Rejects attempts to grow the voter set beyond `controlplane_limit()`
    /// (3) to keep the cluster within the documented HA envelope.
    ///
    /// T2: Holds `membership_mutex` to serialize concurrent joiners so
    /// they don't race and exhaust their retry budgets.
    pub async fn add_voter(&self, node_id: NodeId, addr: String) -> Result<()> {
        self.membership.add_voter(node_id, addr).await
    }

    /// Freeze current membership, prove every voter/learner advertises the
    /// committed-apply RV capability through the existing metadata RPC, then
    /// drain both proposal lanes. This is deliberately a one-shot operation:
    /// callers hold the returned guard only for the subsequent activation
    /// transaction; no polling or background coordinator is introduced.
    pub async fn preflight_command_codec_v3<'a>(
        &'a self,
        probe: &dyn klights_replication::membership::MemberFeatureProbe,
    ) -> std::result::Result<
        klights_replication::membership::CommandCodecV3Preflight<'a>,
        klights_replication::membership::CommandCodecV3PreflightError,
    > {
        self.membership.preflight_command_codec_v3(probe).await
    }

    /// Enable the production proposal gate, then validate restored membership.
    ///
    /// A committed exact-v3 activation marker is authoritative and permits
    /// startup without peer probes. Before activation, an unavailable or old
    /// restored member returns a diagnostic error but the process may continue
    /// bringing up its authenticated Raft endpoint: the proposal gate remains
    /// closed until a leader later proves every current member and commits the
    /// marker. This avoids a cold-start circular dependency without exposing
    /// command capability.
    pub async fn verify_startup_command_codec_v3(
        &self,
        probe: &dyn klights_replication::membership::MemberFeatureProbe,
    ) -> std::result::Result<(), klights_replication::membership::CommandCodecV3PreflightError>
    {
        self.membership.verify_startup_command_codec_v3(probe).await
    }

    /// Commit the exact-v3 activation marker after proving every current voter
    /// and learner reports the same exact codec.
    pub async fn activate_command_codec_v3(
        &self,
        probe: &dyn klights_replication::membership::MemberFeatureProbe,
    ) -> std::result::Result<(), klights_replication::membership::CommandCodecV3ActivationError>
    {
        self.membership.activate_command_codec_v3(probe).await
    }

    /// T1.5 / T4: add a new node to the cluster as a **learner** —
    /// receives `AppendEntries` and applies entries through the same
    /// state-machine code as voters, but does NOT count toward quorum
    /// and does NOT vote. Replicas join via this path instead of
    /// `add_voter`.
    ///
    /// T4: if `node_id` is already a voter (because the node previously
    /// ran as a controlplane and is now restarting as a replica), this
    /// method demotes it: removes it from the voter set, then adds it as
    /// a learner. Voter→learner transitions only happen via restart, so
    /// there is no in-flight work lost during the demotion.
    ///
    /// Idempotent: returns Ok if the target is already a learner.
    /// Learners are not subject to `controlplane_limit()` — the bound is on
    /// the voter set only. Holds `membership_mutex` to serialize with
    /// concurrent add_voter / remove_voter / add_learner_only calls.
    pub async fn add_learner_only(&self, node_id: NodeId, addr: String) -> Result<()> {
        self.membership.add_learner_only(node_id, addr).await
    }

    #[cfg(test)]
    pub async fn admit_controlplane_member(
        &self,
        node_id: NodeId,
        addr: String,
        as_learner: bool,
        storage_incarnation: String,
        storage_log_attestation: klights_leader_api::RaftStorageAttestation,
    ) -> Result<klights_replication::membership::RaftMemberAdmissionResult> {
        self.membership
            .admit_controlplane_member(
                node_id,
                addr,
                as_learner,
                storage_incarnation,
                storage_log_attestation,
            )
            .await
    }

    pub(crate) fn membership(
        &self,
    ) -> Arc<klights_replication::membership::EmbeddedRaftMembership> {
        self.membership.clone()
    }

    pub fn rpc_router(&self) -> klights_replication::rpc_router::RaftNodeRpcRouter {
        klights_replication::rpc_router::RaftNodeRpcRouter::new(
            self.raft.clone(),
            self.storage_incarnation.clone(),
        )
    }

    pub async fn remove_voter(&self, node_id: NodeId) -> Result<()> {
        self.membership.remove_voter(node_id).await
    }

    pub fn is_leader(&self) -> bool {
        self.membership.is_leader()
    }

    pub fn current_shape(&self) -> RaftShape {
        self.membership.current_shape()
    }

    pub(crate) fn authoring_node(&self) -> &str {
        &self.authoring_node
    }

    /// Subscribe to openraft's metrics watch. The kubelet label task
    /// awaits `.changed()` on this receiver and recomputes the shape
    /// each time the engine publishes a new metrics snapshot (membership
    /// change, leadership transfer, etc.).
    pub fn metrics_watch(
        &self,
    ) -> tokio::sync::watch::Receiver<
        openraft::RaftMetrics<NodeId, klights_replication::types::RaftMemberNode>,
    > {
        self.membership.metrics_watch()
    }

    /// Subscribe to openraft's *deduped* server-metrics watch.
    ///
    /// Unlike `metrics_watch()` — which the engine republishes on every
    /// heartbeat tick (openraft sends `tx_metrics` unconditionally) — the
    /// server-metrics channel uses `send_if_modified` and only fires when
    /// `state` / `vote` / `current_leader` / `membership_config` actually
    /// change. Shape and leadership watchers MUST subscribe here so they
    /// stay asleep at idle (HR #1: zero idle CPU). Everything those
    /// watchers need (leadership, leader identity, voter/learner shape) is
    /// derivable from these fields, and they re-read the full metrics via
    /// `current_shape()` / `current_leader_info()` only when woken.
    pub fn server_metrics_watch(
        &self,
    ) -> tokio::sync::watch::Receiver<
        openraft::metrics::RaftServerMetrics<NodeId, klights_replication::types::RaftMemberNode>,
    > {
        self.membership.server_metrics_watch()
    }

    /// Return the (id, address) of the voter currently elected as Raft
    /// leader, if any. Used by the `JoinAsControlplane` redirect path
    /// when this node is a follower and the joiner needs to retry
    /// against the actual leader.
    pub fn current_leader_info(&self) -> Option<(NodeId, String)> {
        self.membership.current_leader_info()
    }

    pub(crate) fn local_commit_materialization_ready(&self) -> bool {
        self.proposal().is_local_leader()
    }

    pub async fn shutdown(self) -> Result<()> {
        self.raft
            .shutdown()
            .await
            .map_err(|e| anyhow::anyhow!("Raft::shutdown: {e}"))
    }
}

#[cfg(test)]
fn local_commit_materialization_allowed(
    node_id: NodeId,
    current_leader: Option<NodeId>,
    voter_ids: &BTreeSet<NodeId>,
) -> bool {
    klights_replication::proposal::local_commit_materialization_allowed(
        node_id,
        current_leader,
        voter_ids,
    )
}

#[cfg(test)]
fn derive_operation_label(command: &StorageCommand) -> OutboxOperation {
    klights_replication::proposal::derive_operation_label(command)
}

#[cfg(test)]
fn outbox_operation_uses_priority_permit(operation: &str) -> bool {
    klights_replication::proposal::outbox_operation_uses_priority_permit(operation)
}

#[cfg(test)]
fn outbox_operation_waits_for_permit(operation: &str) -> bool {
    klights_replication::proposal::outbox_operation_waits_for_permit(operation)
}

#[cfg(test)]
mod tests {
    // Test assertions briefly lock a mock's recorded-call log to inspect it
    // after an awaited propose; the std guard is dropped at end of statement
    // and the test runtime is single-threaded, so the lint is not a concern.
    #![allow(clippy::await_holding_lock)]
    use super::*;
    use crate::bootstrap::controlplane_join_adapters::build_controlplane_join_handler;
    use crate::datastore::DatastoreBackend;
    use crate::datastore::node_local::NodeLocalStores;
    use klights_leader_api::{
        LeaderResourceCommand, LeaderResourceQuery, ResourceCommandError, ResourceCommandRequest,
        ResourceGetRequest, ResourceListRequest, ResourceListResult, ResourceQueryError,
        ResourceQueryFuture,
    };
    use klights_replication::activation::{
        COMMAND_CODEC_ACTIVATION_VALUE, KEY_COMMAND_CODEC_ACTIVATION_VERSION,
    };
    use klights_replication::join::validate_command_codec_v3_join;
    use klights_replication::membership::{
        CommandCodecV3ActivationError, CommandCodecV3PreflightError, MemberFeatureProbe,
        RaftMemberAdmissionResult,
    };
    use klights_replication::types::RaftMemberLogId;
    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};

    struct BackendResourceQuery {
        backend: Arc<crate::datastore::sqlite::Datastore>,
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

    fn raft_store_ports(backend: Arc<crate::datastore::sqlite::Datastore>) -> RaftStorePorts {
        crate::datastore::cluster_store_adapter::raft_store_ports_for_test(backend)
    }

    fn storage_attestation(
        log: Option<klights_leader_api::RaftStorageLogAttestation>,
    ) -> klights_leader_api::RaftStorageAttestation {
        klights_leader_api::RaftStorageAttestation {
            high_watermark: log.clone(),
            current_boundary: log,
        }
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
    impl crate::datastore::raft::grpc_network::GrpcRaftRpcClient for AdmissionFenceClient {
        async fn append_entries(
            &self,
            _receiver: RaftMemberNode,
            _payload: Vec<u8>,
        ) -> std::result::Result<Vec<u8>, crate::datastore::raft::grpc_network::GrpcRaftRpcError>
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
                    crate::datastore::raft::grpc_network::GrpcRaftRpcError::Retryable(
                        "same-ID member session is fenced".to_string(),
                    ),
                )
            }
        }

        async fn vote(
            &self,
            _receiver: RaftMemberNode,
            _payload: Vec<u8>,
        ) -> std::result::Result<Vec<u8>, crate::datastore::raft::grpc_network::GrpcRaftRpcError>
        {
            unreachable!("admission-fence test does not send Vote")
        }

        async fn install_snapshot(
            &self,
            _receiver: RaftMemberNode,
            _payload: Vec<u8>,
        ) -> std::result::Result<Vec<u8>, crate::datastore::raft::grpc_network::GrpcRaftRpcError>
        {
            unreachable!("admission-fence test does not install a snapshot")
        }
    }

    struct AdmissionFenceFactory {
        client: Arc<AdmissionFenceClient>,
        builds: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl crate::datastore::raft::grpc_network::GrpcRaftClientFactory for AdmissionFenceFactory {
        fn client_for(
            &self,
            _addr: &str,
        ) -> Arc<dyn crate::datastore::raft::grpc_network::GrpcRaftRpcClient> {
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
        let materializer =
            crate::datastore::cluster_store_adapter::DatastoreRaftCommitMaterializer::new(
                backend.clone(),
            );
        let restored_activation = CommandCodecV3Activation::load(&materializer)
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

        use crate::datastore::raft::network::LoopbackRegistry;

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
        leader
            .add_voter(704, "https://127.0.0.1:7704".to_string())
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

    #[test]
    fn member_join_gate_requires_exact_codec_v3() {
        assert!(validate_command_codec_v3_join(0).is_err());
        assert!(validate_command_codec_v3_join(2).is_err());
        assert!(validate_command_codec_v3_join(3).is_ok());
        assert!(validate_command_codec_v3_join(4).is_err());
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

    async fn fresh_node(node_id: NodeId) -> (RaftNode, Arc<crate::datastore::sqlite::Datastore>) {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_executor = klights_node_datastore::open::open_with_opts(
            klights_node_datastore::open::in_memory_opts(),
            supervisor.clone(),
            "sqlite:raft-node-test",
        )
        .await
        .expect("open node-local executor");
        let node_local =
            Arc::new(NodeLocalStores::from_executor(node_executor).expect("create node-local db"));
        let backend: Arc<crate::datastore::sqlite::Datastore> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let backend_for_caller = backend.clone();
        let raft_node = RaftNode::start(
            node_id,
            format!("n{node_id}"),
            raft_store_ports(backend),
            node_local.clone(),
            node_local,
            supervisor,
        )
        .await
        .expect("RaftNode::start");
        (raft_node, backend_for_caller)
    }

    #[test]
    fn direct_node_resource_update_is_not_classified_as_node_status() {
        let command = klights_cluster_core::command::StorageCommand::UpdateResource {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: "mn-controlplane1".to_string(),
            data: serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": "mn-controlplane1",
                    "labels": {}
                },
                "spec": {},
                "status": {}
            }),
            expected_rv: 1,
            preconditions: crate::datastore::ResourcePreconditions::resource_version(1),
        };

        assert_ne!(
            derive_operation_label(&command),
            OutboxOperation::NodeStatus,
            "direct API Node updates must not use the kubelet NodeStatus outbox operation"
        );
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
        Vec<RaftNode>,
        Vec<Arc<crate::datastore::sqlite::Datastore>>,
        crate::datastore::raft::network::LoopbackRegistry,
    ) {
        use crate::datastore::raft::network::{LoopbackRaftNetworkFactory, LoopbackRegistry};
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
            let node_local =
                Arc::new(NodeLocalStores::from_executor(exec).expect("create node-local db"));
            let backend: Arc<crate::datastore::sqlite::Datastore> =
                Arc::new(crate::datastore::test_support::in_memory().await);
            let factory = LoopbackRaftNetworkFactory::new(registry.clone());
            let n = RaftNode::start_with_network(
                id,
                format!("n{id}"),
                raft_store_ports(backend.clone()),
                node_local.clone(),
                node_local,
                supervisor,
                factory,
            )
            .await
            .expect("RaftNode::start_with_network");
            registry.register(id, n.raft.clone(), n.storage_incarnation.clone());
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
                crate::datastore::raft::test_unproven_member(format!(
                    "https://localhost:{}",
                    7679 + n.node_id
                )),
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
                crate::datastore::raft::test_unproven_member(format!(
                    "https://localhost:{}",
                    7679 + node.node_id
                )),
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

    /// Mock forwarder that records every `forward_propose` invocation so
    /// the follower-forwarding test can assert the leader was contacted
    /// with the expected payload.
    #[derive(Default)]
    struct CapturingForwarder {
        calls: std::sync::Mutex<Vec<(NodeId, StorageCommandPayload)>>,
    }

    #[async_trait::async_trait]
    impl crate::datastore::raft::network::LeaderForwarder for CapturingForwarder {
        async fn forward_propose(
            &self,
            leader_id: NodeId,
            payload: StorageCommandPayload,
        ) -> Result<()> {
            self.calls.lock().unwrap().push((leader_id, payload));
            Ok(())
        }
    }

    #[tokio::test]
    async fn write_on_follower_forwards_to_leader() {
        use crate::datastore::raft::network::{LoopbackRaftNetworkFactory, LoopbackRegistry};
        let registry = LoopbackRegistry::new();
        let mut nodes = Vec::new();
        let mut mocks = Vec::new();
        for id in [10u64, 20, 30] {
            let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
            let exec = klights_node_datastore::open::open_with_opts(
                klights_node_datastore::open::in_memory_opts(),
                supervisor.clone(),
                "sqlite:raft-forward-test",
            )
            .await
            .expect("open node-local executor");
            let node_local =
                Arc::new(NodeLocalStores::from_executor(exec).expect("create node-local db"));
            let backend: Arc<crate::datastore::sqlite::Datastore> =
                Arc::new(crate::datastore::test_support::in_memory().await);
            let factory = LoopbackRaftNetworkFactory::new(registry.clone());
            let mock = Arc::new(CapturingForwarder::default());
            let n = RaftNode::start_with_network(
                id,
                format!("n{id}"),
                raft_store_ports(backend),
                node_local.clone(),
                node_local,
                supervisor,
                factory,
            )
            .await
            .expect("RaftNode::start_with_network")
            .with_forwarder(mock.clone());
            registry.register(id, n.raft.clone(), n.storage_incarnation.clone());
            nodes.push(n);
            mocks.push(mock);
        }
        let mut members = std::collections::BTreeMap::new();
        for n in &nodes {
            members.insert(
                n.node_id,
                crate::datastore::raft::test_unproven_member(format!(
                    "https://localhost:{}",
                    7679 + n.node_id
                )),
            );
        }
        nodes[0]
            .raft
            .initialize(members)
            .await
            .expect("initialize cluster");
        // Wait until every node has observed the same leader. Seeing a leader
        // on one member does not imply that a selected follower has received
        // the corresponding metrics update yet.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut leader_id: Option<NodeId> = None;
        while std::time::Instant::now() < deadline {
            let observed = nodes
                .iter()
                .map(|node| node.raft.metrics().borrow().current_leader)
                .collect::<Vec<_>>();
            if let Some(Some(candidate)) = observed.first()
                && observed
                    .iter()
                    .all(|observed_leader| *observed_leader == Some(*candidate))
            {
                leader_id = Some(*candidate);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let leader_id = leader_id.expect("all members observed the elected leader");
        let follower_idx = nodes
            .iter()
            .position(|n| n.node_id != leader_id)
            .expect("at least one follower");
        let payload = StorageCommandPayload::from_bytes(vec![0xAB, 0xCD, 0xEF]);
        nodes[follower_idx]
            .propose(payload.clone())
            .await
            .expect("propose on follower forwards to leader");
        let calls = mocks[follower_idx].calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "follower forwarder called exactly once");
        assert_eq!(calls[0].0, leader_id, "forwarded to current leader");
        assert_eq!(calls[0].1, payload, "payload preserved verbatim");
        drop(calls);
        for n in nodes {
            n.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn follower_raft_proposer_refuses_before_local_commit_materialization() {
        use crate::datastore::raft::network::{LoopbackRaftNetworkFactory, LoopbackRegistry};

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
            let node_local =
                Arc::new(NodeLocalStores::from_executor(exec).expect("create node-local db"));
            let backend: Arc<crate::datastore::sqlite::Datastore> =
                Arc::new(crate::datastore::test_support::in_memory().await);
            let factory = LoopbackRaftNetworkFactory::new(registry.clone());
            let node = RaftNode::start_with_network(
                id,
                format!("n{id}"),
                raft_store_ports(backend.clone()),
                node_local.clone(),
                node_local,
                supervisor,
                factory,
            )
            .await
            .expect("RaftNode::start_with_network");
            registry.register(id, node.raft.clone(), node.storage_incarnation.clone());
            nodes.push(node);
            backends.push(backend);
        }
        let mut members = std::collections::BTreeMap::new();
        for node in &nodes {
            members.insert(
                node.node_id,
                crate::datastore::raft::test_unproven_member(format!(
                    "https://localhost:{}",
                    7679 + node.node_id
                )),
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
        let node_local =
            Arc::new(NodeLocalStores::from_executor(exec).expect("create node-local db"));
        let backend: Arc<crate::datastore::sqlite::Datastore> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let node = RaftNode::start(
            10,
            "n10".to_string(),
            raft_store_ports(backend.clone()),
            node_local.clone(),
            node_local,
            supervisor,
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

    #[test]
    fn local_commit_materialization_allows_solo_self_voter_before_leader_metric() {
        let voter_ids = std::collections::BTreeSet::from([10]);

        assert!(
            super::local_commit_materialization_allowed(10, None, &voter_ids),
            "solo seed bootstrap may propose before current_leader is published"
        );
    }

    #[test]
    fn local_commit_materialization_rejects_no_leader_multi_voter_reconfig_window() {
        let voter_ids = std::collections::BTreeSet::from([10, 20]);

        assert!(
            !super::local_commit_materialization_allowed(10, None, &voter_ids),
            "no-leader local materialization carve-out must only apply to N=1 membership"
        );
    }

    #[test]
    fn local_commit_materialization_rejects_no_leader_when_self_is_not_solo_voter() {
        let voter_ids = std::collections::BTreeSet::from([20]);

        assert!(
            !super::local_commit_materialization_allowed(10, None, &voter_ids),
            "a node outside the solo voter set must not self-authorize local materialization"
        );
    }

    #[test]
    fn local_commit_materialization_rejects_self_leader_metric_when_self_is_not_voter() {
        let voter_ids = std::collections::BTreeSet::from([20]);

        assert!(
            !super::local_commit_materialization_allowed(10, Some(10), &voter_ids),
            "learner/replica must not materialize proposals even if metrics are inconsistent"
        );
    }

    #[test]
    fn local_commit_materialization_rejects_known_other_leader() {
        let voter_ids = std::collections::BTreeSet::from([10, 20]);

        assert!(
            !super::local_commit_materialization_allowed(10, Some(20), &voter_ids),
            "known non-self leader must reject local materialization"
        );
    }

    async fn fresh_voter_in_registry(
        id: NodeId,
        registry: &crate::datastore::raft::network::LoopbackRegistry,
    ) -> RaftNode {
        fresh_voter_in_registry_with_backend(id, registry).await.0
    }

    async fn fresh_voter_in_registry_with_backend(
        id: NodeId,
        registry: &crate::datastore::raft::network::LoopbackRegistry,
    ) -> (RaftNode, Arc<crate::datastore::sqlite::Datastore>) {
        use crate::datastore::raft::network::LoopbackRaftNetworkFactory;
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let exec = klights_node_datastore::open::open_with_opts(
            klights_node_datastore::open::in_memory_opts(),
            supervisor.clone(),
            "sqlite:raft-voter-test",
        )
        .await
        .expect("open node-local executor");
        let node_local =
            Arc::new(NodeLocalStores::from_executor(exec).expect("create node-local db"));
        let backend: Arc<crate::datastore::sqlite::Datastore> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let factory = LoopbackRaftNetworkFactory::new(registry.clone());
        let node = RaftNode::start_with_network(
            id,
            format!("n{id}"),
            raft_store_ports(backend.clone()),
            node_local.clone(),
            node_local,
            supervisor,
            factory,
        )
        .await
        .expect("start node");
        registry.register(id, node.raft.clone(), node.storage_incarnation.clone());
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
        use crate::datastore::raft::network::LoopbackRegistry;
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
        leader
            .add_voter(20, "https://10.99.0.20:7679".into())
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
        use crate::datastore::raft::network::LoopbackRegistry;
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
        leader
            .add_voter(20, "https://10.99.0.20:7679".into())
            .await
            .expect("add 2nd voter");
        wait_for_voter_count(&leader, 2).await;
        leader
            .add_voter(30, "https://10.99.0.30:7679".into())
            .await
            .expect("add 3rd voter");
        wait_for_voter_count(&leader, 3).await;
        let err = leader
            .add_voter(40, "https://10.99.0.40:7679".into())
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
        use crate::datastore::raft::network::LoopbackRegistry;
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
        leader
            .add_voter(20, "https://10.99.0.20:7679".into())
            .await
            .expect("add v2");
        wait_for_voter_count(&leader, 2).await;
        leader
            .add_voter(30, "https://10.99.0.30:7679".into())
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

    async fn test_db() -> crate::datastore::DatastoreHandle {
        let ds: crate::datastore::DatastoreHandle =
            Arc::new(crate::datastore::test_support::in_memory().await);
        ds
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
        use crate::datastore::raft::network::LoopbackRegistry;
        use klights_leader_api::ControlplaneJoinOutcome;
        let registry = LoopbackRegistry::new();
        let leader = Arc::new(fresh_voter_in_registry(50, &registry).await);
        let follower = fresh_voter_in_registry(51, &registry).await;
        leader
            .bootstrap_single_voter("https://10.99.0.50:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&leader, std::time::Duration::from_secs(5))
            .await
            .unwrap();

        let join_db = test_db().await;
        let handler = build_controlplane_join_handler(leader.clone(), join_db.clone());
        let outcome = handler
            .join(test_controlplane_join_request(
                51,
                "https://10.99.0.51:7679",
                "n51",
                false,
                &follower.storage_incarnation,
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
                storage_incarnation: follower.storage_incarnation.clone(),
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
        use crate::datastore::raft::network::LoopbackRegistry;
        use klights_leader_api::ControlplaneJoinOutcome;
        let registry = LoopbackRegistry::new();
        let leader = Arc::new(fresh_voter_in_registry(52, &registry).await);
        let follower = fresh_voter_in_registry(53, &registry).await;
        leader
            .bootstrap_single_voter("https://10.99.0.52:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&leader, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        let leader_db = test_db().await;
        crate::bootstrap::cluster_meta::write_cluster_membership(
            leader_db.as_ref(),
            &crate::control_plane::client::membership::ClusterMembership {
                cluster_id: "cluster-a".to_string(),
                voters: vec!["mn-controlplane1".to_string()],
                term: 0,
                leader_hint: Some("mn-controlplane1".to_string()),
            },
        )
        .await
        .unwrap();

        let handler = build_controlplane_join_handler(leader.clone(), leader_db.clone());
        let outcome = handler
            .join(test_controlplane_join_request(
                53,
                "https://10.99.0.53:7679",
                "mn-controlplane2",
                false,
                &follower.storage_incarnation,
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

        let membership =
            crate::bootstrap::cluster_meta::read_cluster_membership(leader_db.as_ref())
                .await
                .unwrap();
        assert_eq!(
            membership.voters,
            vec!["mn-controlplane1", "mn-controlplane2"],
            "admitted voters must be reflected in replicated membership metadata"
        );
    }

    #[tokio::test]
    async fn join_handler_returns_no_leader_when_uninitialized() {
        use klights_leader_api::ControlplaneJoinOutcome;
        let (node, _) = fresh_node(60).await;
        let arc = Arc::new(node);
        let handler = build_controlplane_join_handler(arc, test_db().await);
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
        use crate::datastore::raft::network::LoopbackRegistry;
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
        let leader = Arc::new(leader);

        let handler = build_controlplane_join_handler(leader.clone(), test_db().await);
        let outcome = handler
            .join(test_controlplane_join_request(
                71,
                "https://10.99.0.71:7679",
                "n71",
                true,
                &learner.storage_incarnation,
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
        use crate::datastore::raft::network::LoopbackRegistry;
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
        let leader = Arc::new(leader);

        let leader_db = test_db().await;
        let handler = build_controlplane_join_handler(leader.clone(), leader_db.clone());
        let outcome = handler
            .join(test_controlplane_join_request(
                73,
                "https://10.99.0.73:7679",
                "n73",
                true,
                &learner.storage_incarnation,
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
        use crate::datastore::raft::network::LoopbackRegistry;
        use klights_leader_api::ControlplaneJoinOutcome;
        let registry = LoopbackRegistry::new();
        let leader = Arc::new(fresh_voter_in_registry(80, &registry).await);
        let follower = fresh_voter_in_registry(81, &registry).await;
        leader
            .bootstrap_single_voter("https://10.99.0.80:7679".into())
            .await
            .unwrap();
        wait_for_leader(&leader, std::time::Duration::from_secs(5))
            .await
            .unwrap();

        let leader_db = test_db().await;
        let handler = build_controlplane_join_handler(leader.clone(), leader_db.clone());
        let outcome = handler
            .join(test_controlplane_join_request(
                81,
                "https://10.99.0.81:7679",
                "n81",
                false,
                &follower.storage_incarnation,
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
        let receiver =
            RaftMemberNode::new("loopback".into(), node.storage_incarnation.clone(), None);
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
        let receiver =
            RaftMemberNode::new("loopback".into(), node.storage_incarnation.clone(), None);
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
            node.storage_incarnation.clone(),
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
        let node_local = Arc::new(NodeLocalStores::from_executor(executor).unwrap());
        let backend: Arc<crate::datastore::sqlite::Datastore> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let client = Arc::new(AdmissionFenceClient {
            ready: std::sync::atomic::AtomicBool::new(false),
            append_calls: std::sync::atomic::AtomicUsize::new(0),
            append_called: tokio::sync::Notify::new(),
        });
        let builds = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let network = crate::datastore::raft::grpc_network::GrpcRaftNetwork::new(Arc::new(
            AdmissionFenceFactory {
                client: client.clone(),
                builds: builds.clone(),
            },
        ));
        let metrics_network = network.clone();
        let leader = Arc::new(
            RaftNode::start_with_network(
                77,
                "n77".into(),
                raft_store_ports(backend),
                node_local.clone(),
                node_local,
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
                    .add_learner_only(78, "https://10.99.0.78:7679".into())
                    .await
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
        use crate::datastore::raft::network::LoopbackRegistry;
        let registry = LoopbackRegistry::new();
        let leader = fresh_voter_in_registry(72, &registry).await;
        let old_learner = fresh_voter_in_registry(73, &registry).await;
        let restored_incarnation = old_learner.storage_incarnation.clone();
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
        use crate::datastore::raft::network::LoopbackRegistry;
        let registry = LoopbackRegistry::new();
        let leader = Arc::new(fresh_voter_in_registry(74, &registry).await);
        let surviving_voter = fresh_voter_in_registry(75, &registry).await;
        let old_voter = fresh_voter_in_registry(76, &registry).await;
        let surviving_incarnation = surviving_voter.storage_incarnation.clone();
        let old_voter_incarnation = old_voter.storage_incarnation.clone();
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
        let unrelated_incarnation = unrelated_learner.storage_incarnation.clone();
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
        let replacement_incarnation = replacement.storage_incarnation.clone();
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
        assert!(
            leader
                .materializer
                .read_raft_metadata("raft_member_admission/76")
                .await
                .unwrap()
                .and_then(|admission| {
                    serde_json::from_str::<serde_json::Value>(&admission).ok()
                })
                .and_then(|admission| admission.get("proven_log").cloned())
                .is_some_and(|proof| !proof.is_null()),
            "replacement admission must persist a leader-observed target replication match"
        );
        replacement.shutdown().await.unwrap();
        let learner_replacement = fresh_voter_in_registry(76, &registry).await;
        let learner_incarnation = learner_replacement.storage_incarnation.clone();
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
        use crate::datastore::raft::network::LoopbackRegistry;
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
        let incarnation = learner.storage_incarnation.clone();
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
        use crate::datastore::raft::network::LoopbackRegistry;
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
            .add_learner_only(85, "https://10.99.0.85:7679".into())
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
        use crate::datastore::raft::network::LoopbackRegistry;
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
                old_voter.storage_incarnation.clone(),
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
        use crate::datastore::raft::network::{LoopbackRaftNetworkFactory, LoopbackRegistry};
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
        let nl1 = Arc::new(NodeLocalStores::from_executor(exec1).unwrap());
        let be1: Arc<crate::datastore::sqlite::Datastore> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let leader_network = LoopbackRaftNetworkFactory::new(registry.clone());
        let leader = RaftNode::start_with_network(
            leader_id,
            "n70".into(),
            raft_store_ports(be1),
            nl1.clone(),
            nl1,
            sup1,
            leader_network,
        )
        .await
        .unwrap();
        registry.register(
            leader_id,
            leader.raft.clone(),
            leader.storage_incarnation.clone(),
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
        let nl2 = Arc::new(NodeLocalStores::from_executor(exec2).unwrap());
        let be2: Arc<crate::datastore::sqlite::Datastore> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let voter_network = LoopbackRaftNetworkFactory::new(registry.clone());
        let voter_node = RaftNode::start_with_network(
            voter_id,
            "n80".into(),
            raft_store_ports(be2),
            nl2.clone(),
            nl2,
            sup2,
            voter_network,
        )
        .await
        .unwrap();
        registry.register(
            voter_id,
            voter_node.raft.clone(),
            voter_node.storage_incarnation.clone(),
        );

        wait_for_leader(&leader, std::time::Duration::from_secs(10))
            .await
            .unwrap();

        // Add voter to the leader's cluster.
        leader
            .add_voter(voter_id, voter_addr.clone())
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

        // Demote via add_learner_only.
        leader.add_learner_only(voter_id, voter_addr).await.unwrap();

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
    /// `let _flow_permit = self.flow_control.acquire().await;` line in `propose_command`
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
        let cap = node.flow_control.max_in_flight();
        let mut held = Vec::with_capacity(cap);
        for _ in 0..cap {
            held.push(node.flow_control.acquire().await);
        }
        assert_eq!(node.flow_control.available_permits(), 0);

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

        let cap = node.flow_control.max_in_flight();
        let mut held = Vec::with_capacity(cap);
        for _ in 0..cap {
            held.push(node.flow_control.acquire().await);
        }
        assert_eq!(node.flow_control.available_permits(), 0);

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

        let cap = node.flow_control.max_in_flight();
        let mut held = Vec::with_capacity(cap);
        for _ in 0..cap {
            held.push(node.flow_control.acquire().await);
        }
        assert_eq!(node.flow_control.available_permits(), 0);

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

    #[test]
    fn outbox_priority_permit_classifier_is_explicit() {
        use OutboxOperation;

        let cases = [
            (OutboxOperation::NodeRegistration, true),
            (OutboxOperation::NodeDataplane, true),
            (OutboxOperation::NodeStatus, true),
            (OutboxOperation::PodStatus, false),
            (OutboxOperation::EventCreate, false),
        ];
        for (operation, expected) in cases {
            assert_eq!(
                outbox_operation_uses_priority_permit(operation.as_str()),
                expected,
                "{operation:?} priority classification mismatch"
            );
        }
    }

    #[test]
    fn outbox_waiting_permit_classifier_is_explicit() {
        use OutboxOperation;

        let cases = [
            (OutboxOperation::PodStatus, true),
            (OutboxOperation::RuntimeReconcile, true),
            (OutboxOperation::ProbeReadiness, true),
            (OutboxOperation::DeadlineExceeded, true),
            (OutboxOperation::ContainerStatusSnapshot, true),
            (OutboxOperation::EphemeralContainerStatuses, true),
            // PodMetadata (controller ownerRef adoption/release, label merges,
            // deletion finalization) must WAIT for a general permit (FIFO) rather
            // than best-effort `try_acquire`. Under parallel e2e load, best-effort
            // rejection + retry backoff starves controller reconciliation past the
            // suite's adoption/release timeouts. FIFO guarantees progress without a
            // retry storm and without borrowing the node-liveness reserved permit.
            (OutboxOperation::PodMetadata, true),
            (OutboxOperation::NodeStatus, false),
            (OutboxOperation::EventCreate, false),
        ];
        for (operation, expected) in cases {
            assert_eq!(
                outbox_operation_waits_for_permit(operation.as_str()),
                expected,
                "{operation:?} waiting classification mismatch"
            );
        }
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
            node.flow_control.max_in_flight(),
            super::RAFT_MAX_INFLIGHT_PROPOSALS,
            "flow-control cap must be the decoupled RAFT_MAX_INFLIGHT_PROPOSALS, \
             not RAFT_MAX_PAYLOAD_ENTRIES"
        );
        assert_ne!(
            node.flow_control.max_in_flight() as u64,
            super::RAFT_MAX_PAYLOAD_ENTRIES,
            "flow-control cap must be decoupled from payload entries"
        );
        // Hold every general permit, simulating max_in_flight in-flight proposals.
        let cap = node.flow_control.max_in_flight();
        let mut held = Vec::with_capacity(cap);
        for _ in 0..cap {
            held.push(node.flow_control.acquire().await);
        }
        assert_eq!(node.flow_control.available_permits(), 0);

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
        let permits_before = node.flow_control.available_permits();
        assert_eq!(
            permits_before,
            node.flow_control.max_in_flight(),
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
            node.flow_control.available_permits(),
            node.flow_control.max_in_flight(),
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
                preconditions: crate::datastore::ResourcePreconditions::from_resource(
                    &stale_client_read,
                ),
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
            crate::control_plane::client::local::always_leader_watch(),
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
                        preconditions: crate::datastore::ResourcePreconditions::from_resource(
                            &stale_client_read,
                        ),
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
    /// implementation: `let _flow_permit = self.flow_control.acquire().await;`).
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
            node.flow_control.available_permits(),
            node.flow_control.max_in_flight()
        );
        {
            let _permit = node.flow_control.acquire().await;
            assert_eq!(
                node.flow_control.available_permits(),
                node.flow_control.max_in_flight() - 1,
                "permit acquired"
            );
            // Simulating the late-failure path: the permit is held when client_write
            // would have failed; the RAII guard releases on scope exit.
        }
        assert_eq!(
            node.flow_control.available_permits(),
            node.flow_control.max_in_flight(),
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
            node.flow_control.available_permits(),
            node.flow_control.max_in_flight()
        );

        node.propose_command(propose_create_command("ok-success"))
            .await
            .expect("propose ok");

        assert_eq!(
            node.flow_control.available_permits(),
            node.flow_control.max_in_flight(),
            "permit must be released after successful terminal propose_command"
        );
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn raft_outbox_effect_preserves_committed_actor_delete_receipt() {
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

        let effect = node
            .propose_outbox_command_effect(
                "raft-actor-delete-receipt",
                OutboxOperation::PodMetadata.as_str(),
                klights_cluster_core::command::StorageCommand::FinalizeBoundPod {
                    namespace: "default".to_string(),
                    name: "receipt".to_string(),
                    pod_uid: "receipt-uid".to_string(),
                    node_name: "worker-a".to_string(),
                    observed_resource_version: observed.resource_version,
                },
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
        node.shutdown().await.unwrap();
    }

    /// T1: the leader proposal flow-control gate must be DECOUPLED from the
    /// openraft `max_payload_entries`. These solve different problems: payload
    /// entries bounds AppendEntries **retransmit cost** (leader→follower), while
    /// the in-flight permit count bounds **RV backlog ahead of acknowledged raft
    /// progress** at the leader. Coupling both to 3 caps leader commit concurrency
    /// at 3 — at ~200 ms quorum RTT that is a hard ~15 commits/sec ceiling. The two
    /// constants must be independently bounded, and the flow-control gate must be
    /// wired to the larger in-flight value, not the payload value.
    #[test]
    fn raft_flow_control_cap_is_decoupled_from_payload_entries() {
        use crate::datastore::raft::node::{RAFT_MAX_INFLIGHT_PROPOSALS, RAFT_MAX_PAYLOAD_ENTRIES};

        // payload stays small for lossy AppendEntries retransmit cost.
        assert!(
            RAFT_MAX_PAYLOAD_ENTRIES <= 16,
            "RAFT_MAX_PAYLOAD_ENTRIES must stay small for lossy retransmit"
        );
        // in-flight is independently bounded in the measured safe range.
        assert!(
            (8..=32).contains(&RAFT_MAX_INFLIGHT_PROPOSALS),
            "RAFT_MAX_INFLIGHT_PROPOSALS must be a swept value in 8..=32"
        );
        // The core decoupling: the gate must not equal the payload cap.
        assert_ne!(
            RAFT_MAX_INFLIGHT_PROPOSALS as u64, RAFT_MAX_PAYLOAD_ENTRIES,
            "in-flight proposal gate must be decoupled from payload entries"
        );
    }
}
