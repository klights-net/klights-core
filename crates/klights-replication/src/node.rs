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

use std::sync::Arc;

use anyhow::{Context, Result};
use klights_cluster_core::{OutboxApplyError, OutboxApplyOutcome, StorageCommand};
use klights_node_store::{RaftAppliedStateDurability, RaftLogDurability};
#[cfg(any(test, feature = "test-support"))]
use openraft::error::{ClientWriteError, RaftError};
use openraft::{Config, Raft};

use openraft::network::RaftNetworkFactory;

use super::network::LeaderForwarder;
#[cfg(any(test, feature = "test-support"))]
use super::network::StubRaftNetwork;
use crate::activation::CommandCodecV3Activation;
use crate::log_storage::SqliteRaftLogStorage;
use crate::materializer::RaftCommitMaterializer;
use crate::state_machine::{RaftStateMachineStorePorts, SqliteRaftStateMachine};
#[cfg(any(test, feature = "test-support"))]
use crate::types::StorageCommandPayload;
use crate::types::{NodeId, RaftShape, TypeConfig};
use klights_cluster_store::BackendLifecycleStore;

/// Lossy-link transport sizing (finding.md H3). `max_payload_entries` keeps each
/// AppendEntries retry small (it bounds **retransmit cost**: leader→follower).
pub const RAFT_MAX_PAYLOAD_ENTRIES: u64 = 16;

/// Leader proposal flow-control cap: the maximum number of unacknowledged
/// proposals that may be in flight simultaneously. This is DECOUPLED from
/// `RAFT_MAX_PAYLOAD_ENTRIES`: payload entries bounds AppendEntries
/// **retransmit cost**, while this permit count bounds **RV backlog ahead of
/// acknowledged raft progress** at the leader. OpenRaft already pipelines
/// AppendEntries, so a small payload does NOT require a small permit count.
/// Coupling both to 3 capped leader commit concurrency at 3 — at ~200 ms quorum
/// RTT a hard ~15 commits/sec ceiling. Default 16 keeps the cap in the measured
/// safe range 8..=32.
pub use crate::proposal::RAFT_MAX_INFLIGHT_PROPOSALS;

pub struct RaftStorePorts {
    materializer: Arc<dyn RaftCommitMaterializer>,
    state_machine: RaftStateMachineStorePorts,
    snapshot_capture: Arc<dyn klights_cluster_store::AuthoritativeSnapshotCapture>,
    allocator: Arc<dyn klights_cluster_store::DurableAllocatorRead>,
    lifecycle: Arc<dyn BackendLifecycleStore>,
}

impl RaftStorePorts {
    pub fn new(
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
    membership: Arc<crate::membership::EmbeddedRaftMembership>,
    materializer: Arc<dyn RaftCommitMaterializer>,
    /// T1.4: node name used by `build_log_apply_commit_for_command` to
    /// stamp the authoring node on the resulting commit.
    authoring_node: String,
    /// Flow-control gate: at most 3 general proposals plus one reserved
    /// control-critical outbox proposal may be in flight simultaneously.
    /// A permit is acquired BEFORE the leader materializes the next
    /// resourceVersion so the leader cannot build an unacknowledged RV backlog
    /// ahead of raft progress under loss (finding.md flow-control plan).
    pub(crate) flow_control: Arc<crate::flow_control::RaftCommitFlowControl>,
    command_codec_v3_activation: Arc<CommandCodecV3Activation>,
}

impl RaftNode {
    /// Construct a Raft node bound to the given cluster backend +
    /// node-local SQLite handle. The Raft engine starts in Learner state;
    /// call `bootstrap_single_voter` (manual promote) or wait for an
    /// `add_learner` + `change_membership` from a peer (Step 6) to join.
    /// Single-voter convenience constructor that wires a StubRaftNetwork
    /// (no peer RPCs ever issued).
    #[cfg(any(test, feature = "test-support"))]
    pub async fn start(
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
    pub async fn start_with_network<N>(
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
        let snapshot_builder = crate::snapshot::SqliteRaftSnapshotBuilder::new(
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
        let flow_control = Arc::new(crate::flow_control::RaftCommitFlowControl::new(
            RAFT_MAX_INFLIGHT_PROPOSALS,
        ));
        let membership = Arc::new(crate::membership::EmbeddedRaftMembership::new(
            node_id,
            raft.clone(),
            storage_incarnation.clone(),
            stores.materializer.clone(),
            flow_control.clone(),
            command_codec_v3_activation.clone(),
            node_name.clone(),
        ));
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

    pub fn proposal(&self) -> Arc<crate::proposal::EmbeddedRaftProposal> {
        Arc::new(crate::proposal::EmbeddedRaftProposal::new(
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
    ) -> Result<crate::types::StorageCommandResult> {
        crate::proposal::RaftProposal::propose_command(self.proposal().as_ref(), command).await
    }

    pub async fn propose_outbox_command(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<OutboxApplyOutcome, OutboxApplyError> {
        crate::proposal::RaftProposal::propose_outbox_command(
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
    ) -> std::result::Result<crate::proposal::RaftProposalEffect, OutboxApplyError> {
        crate::proposal::RaftProposal::propose_outbox_command_effect(
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
    #[cfg(any(test, feature = "test-support"))]
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
        probe: &dyn crate::membership::MemberFeatureProbe,
    ) -> std::result::Result<
        crate::membership::CommandCodecV3Preflight<'a>,
        crate::membership::CommandCodecV3PreflightError,
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
        probe: &dyn crate::membership::MemberFeatureProbe,
    ) -> std::result::Result<(), crate::membership::CommandCodecV3PreflightError> {
        self.membership.verify_startup_command_codec_v3(probe).await
    }

    /// Commit the exact-v3 activation marker after proving every current voter
    /// and learner reports the same exact codec.
    pub async fn activate_command_codec_v3(
        &self,
        probe: &dyn crate::membership::MemberFeatureProbe,
    ) -> std::result::Result<(), crate::membership::CommandCodecV3ActivationError> {
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

    #[cfg(any(test, feature = "test-support"))]
    pub async fn admit_controlplane_member(
        &self,
        node_id: NodeId,
        addr: String,
        as_learner: bool,
        storage_incarnation: String,
        storage_log_attestation: klights_leader_api::RaftStorageAttestation,
    ) -> Result<crate::membership::RaftMemberAdmissionResult> {
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

    pub fn membership(&self) -> Arc<crate::membership::EmbeddedRaftMembership> {
        self.membership.clone()
    }

    pub fn rpc_router(&self) -> crate::rpc_router::RaftNodeRpcRouter {
        crate::rpc_router::RaftNodeRpcRouter::new(
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

    pub fn authoring_node(&self) -> &str {
        &self.authoring_node
    }

    /// Subscribe to openraft's metrics watch. The kubelet label task
    /// awaits `.changed()` on this receiver and recomputes the shape
    /// each time the engine publishes a new metrics snapshot (membership
    /// change, leadership transfer, etc.).
    pub fn metrics_watch(
        &self,
    ) -> tokio::sync::watch::Receiver<openraft::RaftMetrics<NodeId, crate::types::RaftMemberNode>>
    {
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
        openraft::metrics::RaftServerMetrics<NodeId, crate::types::RaftMemberNode>,
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

    pub fn local_commit_materialization_ready(&self) -> bool {
        self.proposal().is_local_leader()
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn storage_incarnation_for_test(&self) -> &str {
        &self.storage_incarnation
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn materializer_for_test(&self) -> Arc<dyn RaftCommitMaterializer> {
        self.materializer.clone()
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn flow_control_for_test(&self) -> Arc<crate::flow_control::RaftCommitFlowControl> {
        self.flow_control.clone()
    }

    pub async fn shutdown(self) -> Result<()> {
        self.raft
            .shutdown()
            .await
            .map_err(|e| anyhow::anyhow!("Raft::shutdown: {e}"))
    }
}
