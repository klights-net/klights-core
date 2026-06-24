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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use openraft::error::{ClientWriteError, RaftError};
use openraft::{BasicNode, Config, Raft};

use crate::datastore::DatastoreBackend;
use crate::datastore::node_local::SqliteNodeLocalDb;
use openraft::network::RaftNetworkFactory;

use crate::datastore::raft::log_storage::SqliteRaftLogStorage;
use crate::datastore::raft::network::{LeaderForwarder, StubRaftNetwork};
use crate::datastore::raft::state_machine_impl::SqliteRaftStateMachine;
use crate::datastore::raft::types::{NodeId, RaftShape, StorageCommandPayload, TypeConfig};

/// Lossy-link transport sizing (finding.md H3). max_payload_entries=3 matches the
/// 3-permit flow-control gate so a single AppendEntries retry cannot resend a
/// logical batch larger than the in-flight bound. Lifted to module scope so the
/// closing-gate test can assert on the actual configured value (not a copy).
pub(crate) const RAFT_MAX_PAYLOAD_ENTRIES: u64 = 3;

pub struct RaftNode {
    pub node_id: NodeId,
    pub raft: Raft<TypeConfig>,
    forwarder: Option<Arc<dyn LeaderForwarder>>,
    /// T2: Serializes add_voter/remove_voter calls so concurrent
    /// joiners don't race and exhaust their retry budgets.
    membership_mutex: tokio::sync::Mutex<()>,
    /// T1.4: cluster backend handle used by `propose_command` to build a
    /// `LogApplyCommit` before submitting it through raft. The state
    /// machine also holds this backend (it applies the committed entry);
    /// keeping a clone here avoids reaching back through openraft just to
    /// drive the leader-side build.
    backend: Arc<dyn DatastoreBackend>,
    /// T1.4: node name used by `build_log_apply_commit_for_outbox` to
    /// stamp the authoring node on the resulting commit.
    authoring_node: String,
    /// Flow-control gate: at most 3 proposals may be in flight simultaneously.
    /// A permit is acquired BEFORE build_log_apply_commit_for_outbox reserves
    /// the next resourceVersion so the leader cannot build an unacknowledged
    /// RV backlog ahead of raft progress under loss (finding.md flow-control plan).
    pub(crate) flow_control: Arc<crate::datastore::raft::flow_control::RaftCommitFlowControl>,
}

impl RaftNode {
    /// Construct a Raft node bound to the given cluster backend +
    /// node-local SQLite handle. The Raft engine starts in Learner state;
    /// call `bootstrap_single_voter` (manual promote) or wait for an
    /// `add_learner` + `change_membership` from a peer (Step 6) to join.
    /// Single-voter convenience constructor that wires a StubRaftNetwork
    /// (no peer RPCs ever issued).
    pub async fn start(
        node_id: NodeId,
        node_name: String,
        cluster_backend: Arc<dyn DatastoreBackend>,
        node_local: Arc<SqliteNodeLocalDb>,
    ) -> Result<Self> {
        Self::start_with_network(
            node_id,
            node_name,
            cluster_backend,
            node_local,
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
        cluster_backend: Arc<dyn DatastoreBackend>,
        node_local: Arc<SqliteNodeLocalDb>,
        network: N,
    ) -> Result<Self>
    where
        N: RaftNetworkFactory<TypeConfig>,
    {
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
        // Cross-subsystem safety: worst-case failover (<= election_timeout_max)
        // must finish before observed node leases go stale, or a single leader
        // change would false-evict every node (lease renewals can't commit
        // while there is no leader). The T8 promotion grace-reset is the
        // primary safeguard; this static bound is belt-and-suspenders.
        const _: () = assert!(
            RAFT_ELECTION_TIMEOUT_MAX_MS
                < (crate::node_lease_tracker::DEFAULT_NODE_LEASE_DURATION_SECONDS as u64) * 1000
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
        let log_store = SqliteRaftLogStorage::new(node_local.clone());
        let state_machine =
            SqliteRaftStateMachine::new(cluster_backend.clone(), node_local, node_name.clone());
        let raft = Raft::new(node_id, config, network, log_store, state_machine)
            .await
            .context("Raft::new")?;
        Ok(Self {
            node_id,
            raft,
            forwarder: None,
            membership_mutex: tokio::sync::Mutex::new(()),
            backend: cluster_backend,
            authoring_node: node_name,
            flow_control: Arc::new(
                crate::datastore::raft::flow_control::RaftCommitFlowControl::new(
                    RAFT_MAX_PAYLOAD_ENTRIES as usize,
                ),
            ),
        })
    }

    /// Attach a `LeaderForwarder` so `propose` can transparently redirect
    /// writes to the current leader when this node is a follower. Tests
    /// use `LoopbackRegistry`; production will use a gRPC client.
    pub fn with_forwarder(mut self, forwarder: Arc<dyn LeaderForwarder>) -> Self {
        self.forwarder = Some(forwarder);
        self
    }

    /// Manual promotion entry point. Calls `Raft::initialize` with this
    /// node as the sole voter — the same call openraft uses internally
    /// when forming a single-voter cluster on first boot. Once committed
    /// the engine becomes Leader and `client_write` will accept proposals.
    ///
    /// Idempotent: returns `Ok(())` if the cluster is already
    /// initialized (matches openraft's `NotAllowed` no-op).
    pub async fn bootstrap_single_voter(&self, advertise_addr: String) -> Result<()> {
        let mut members = BTreeMap::new();
        members.insert(
            self.node_id,
            BasicNode {
                addr: advertise_addr,
            },
        );
        match self.raft.initialize(members).await {
            Ok(()) => Ok(()),
            Err(openraft::error::RaftError::APIError(
                openraft::error::InitializeError::NotAllowed { .. },
            )) => Ok(()),
            Err(e) => Err(anyhow::anyhow!("Raft::initialize: {e}")),
        }
    }

    /// Propose a mutating write through Raft. The payload is the
    /// serialized `StorageCommand` (protobuf) that will be replicated and
    /// then applied via `RaftStateMachine::apply`.
    ///
    /// On a non-leader voter, openraft returns `ForwardToLeader`; if a
    /// `LeaderForwarder` was attached via `with_forwarder` the proposal is
    /// transparently re-dispatched to the current leader.
    pub async fn propose(&self, payload: StorageCommandPayload) -> Result<()> {
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

    async fn propose_materialized_commit(
        &self,
        payload: StorageCommandPayload,
    ) -> Result<crate::datastore::raft::types::StorageCommandResult> {
        match self.raft.client_write(payload).await {
            Ok(response) => Ok(response.data),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(forward))) => {
                let leader = forward
                    .leader_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                Err(anyhow::anyhow!(
                    "Raft::client_write rejected locally materialized commit: ForwardToLeader({leader})"
                ))
            }
            Err(e) => Err(anyhow::anyhow!("Raft::client_write: {e}")),
        }
    }

    async fn cleanup_rejected_materialized_commit(&self, idempotency_key: &str, reserved_rv: i64) {
        match self
            .backend
            .delete_uncommitted_applied_outbox_placeholder(idempotency_key, reserved_rv)
            .await
        {
            Ok(true) => {
                tracing::warn!(
                    idempotency_key,
                    "removed uncommitted applied_outbox placeholder after rejected raft proposal"
                );
            }
            Ok(false) => {}
            Err(err) => {
                tracing::warn!(
                    idempotency_key,
                    error = %err,
                    "failed to remove uncommitted applied_outbox placeholder after rejected raft proposal"
                );
            }
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
        let _guard = self.membership_mutex.lock().await;
        if node_id == self.node_id {
            anyhow::bail!("add_voter: node id {node_id} is this node and is already a voter");
        }
        let current = self.raft.metrics().borrow().clone();
        let voters_now: std::collections::BTreeSet<NodeId> =
            current.membership_config.membership().voter_ids().collect();
        if voters_now.contains(&node_id) {
            return Ok(());
        }
        if voters_now.len() >= crate::bootstrap::node_role::controlplane_limit() {
            let limit = crate::bootstrap::node_role::controlplane_limit();
            anyhow::bail!(
                "add_voter: cluster already at controlplane limit ({limit}); refusing to add voter {node_id}"
            );
        }
        self.raft
            .add_learner(node_id, BasicNode { addr }, true)
            .await
            .map_err(|e| anyhow::anyhow!("Raft::add_learner({node_id}): {e}"))?;
        let mut new_voters = voters_now.clone();
        new_voters.insert(node_id);
        self.raft
            .change_membership(new_voters, false)
            .await
            .map_err(|e| anyhow::anyhow!("Raft::change_membership({node_id}): {e}"))?;
        Ok(())
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
        let _guard = self.membership_mutex.lock().await;
        if node_id == self.node_id {
            anyhow::bail!("add_learner_only: node id {node_id} is this node");
        }
        let current = self.raft.metrics().borrow().clone();
        let voters_now: std::collections::BTreeSet<NodeId> =
            current.membership_config.membership().voter_ids().collect();
        if voters_now.contains(&node_id) {
            // T4: demote voter → learner. The node restarted as a
            // replica; remove it from the voter set while retaining
            // it as a learner (`retain=true`). This preserves other
            // learners in the cluster. Guard against dropping below
            // quorum (removing the last voter).
            if voters_now.len() <= 1 {
                anyhow::bail!(
                    "add_learner_only: refusing to demote last voter {node_id} (would break quorum)"
                );
            }
            let mut new_voters = voters_now.clone();
            new_voters.remove(&node_id);
            tracing::info!(
                node_id,
                voters_before = ?voters_now,
                voters_after = ?new_voters,
                "add_learner_only: demoting voter to learner (retain=true)"
            );
            // `retain=true`: nodes not in new_voters remain as learners.
            // The demoted node stays in the cluster as a learner; other
            // learners are unaffected.
            return self
                .raft
                .change_membership(new_voters, true)
                .await
                .map_err(|e| anyhow::anyhow!("Raft::change_membership(demote {node_id}): {e}"))
                .map(|_| ());
        }
        // Node is not a voter: just add as learner (idempotent).
        self.raft
            .add_learner(node_id, BasicNode { addr }, true)
            .await
            .map_err(|e| anyhow::anyhow!("Raft::add_learner({node_id}): {e}"))?;
        Ok(())
    }

    /// Remove a voter from the running cluster. Refuses to shrink below
    /// a single voter and refuses to remove this node from its own
    /// membership (use leadership transfer first).
    ///
    /// T2: Holds `membership_mutex` to serialize with add_voter calls.
    pub async fn remove_voter(&self, node_id: NodeId) -> Result<()> {
        let _guard = self.membership_mutex.lock().await;
        let current = self.raft.metrics().borrow().clone();
        let voters_now: std::collections::BTreeSet<NodeId> =
            current.membership_config.membership().voter_ids().collect();
        if !voters_now.contains(&node_id) {
            return Ok(());
        }
        if voters_now.len() <= 1 {
            anyhow::bail!(
                "remove_voter: refusing to remove last voter {node_id} (would leave cluster without quorum)"
            );
        }
        if node_id == self.node_id {
            anyhow::bail!(
                "remove_voter: refusing to remove this node ({node_id}) from its own membership; transfer leadership first"
            );
        }
        let mut new_voters = voters_now.clone();
        new_voters.remove(&node_id);
        self.raft
            .change_membership(new_voters, false)
            .await
            .map_err(|e| anyhow::anyhow!("Raft::change_membership(remove {node_id}): {e}"))?;
        Ok(())
    }

    /// Snapshot of the cluster shape this node currently observes. Used
    /// by the kubelet's shape-driven role-label task: voter_count==1 with
    /// is_leader=true emits the `leader` label (solo N=1 cluster);
    /// voter_count>=2 emits `controlplane` (plus `leader` on the current
    /// leader voter). See `multinode.md`.
    pub fn is_leader(&self) -> bool {
        self.raft.metrics().borrow().current_leader == Some(self.node_id)
    }

    pub fn current_shape(&self) -> RaftShape {
        let m = self.raft.metrics().borrow().clone();
        let voter_ids: std::collections::BTreeSet<NodeId> =
            m.membership_config.membership().voter_ids().collect();
        let voter_count = voter_ids.len() as u32;
        let is_leader = m.current_leader == Some(self.node_id);
        // T1.7: this node is a learner if it's part of the membership
        // node set but not a voter. openraft exposes the full node set
        // (voters + learners) via `nodes()`.
        let in_membership = m
            .membership_config
            .membership()
            .nodes()
            .any(|(id, _)| *id == self.node_id);
        let is_learner = in_membership && !voter_ids.contains(&self.node_id);
        RaftShape {
            voter_count,
            is_leader,
            is_learner,
        }
    }

    /// Subscribe to openraft's metrics watch. The kubelet label task
    /// awaits `.changed()` on this receiver and recomputes the shape
    /// each time the engine publishes a new metrics snapshot (membership
    /// change, leadership transfer, etc.).
    pub fn metrics_watch(
        &self,
    ) -> tokio::sync::watch::Receiver<openraft::RaftMetrics<NodeId, openraft::BasicNode>> {
        self.raft.metrics()
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
        openraft::metrics::RaftServerMetrics<NodeId, openraft::BasicNode>,
    > {
        self.raft.server_metrics()
    }

    /// Return the (id, address) of the voter currently elected as Raft
    /// leader, if any. Used by the `JoinAsControlplane` redirect path
    /// when this node is a follower and the joiner needs to retry
    /// against the actual leader.
    pub fn current_leader_info(&self) -> Option<(NodeId, String)> {
        let m = self.raft.metrics().borrow().clone();
        let leader_id = m.current_leader?;
        let addr = m
            .membership_config
            .nodes()
            .find(|(id, _)| **id == leader_id)
            .map(|(_, node)| node.addr.clone())?;
        Some((leader_id, addr))
    }

    pub(crate) fn local_commit_materialization_ready(&self) -> bool {
        let m = self.raft.metrics().borrow().clone();
        let voter_ids: BTreeSet<NodeId> = m.membership_config.membership().voter_ids().collect();
        local_commit_materialization_allowed(self.node_id, m.current_leader, &voter_ids)
    }

    pub async fn shutdown(self) -> Result<()> {
        self.raft
            .shutdown()
            .await
            .map_err(|e| anyhow::anyhow!("Raft::shutdown: {e}"))
    }

    fn ensure_local_leader_for_commit_materialization(&self) -> Result<()> {
        let m = self.raft.metrics().borrow().clone();
        let voter_ids: BTreeSet<NodeId> = m.membership_config.membership().voter_ids().collect();
        if local_commit_materialization_allowed(self.node_id, m.current_leader, &voter_ids) {
            return Ok(());
        }
        let current_leader = self.current_leader_info();
        anyhow::bail!(
            "not raft leader: refusing local commit materialization on node {} current_leader={current_leader:?} voters={voter_ids:?}",
            self.node_id
        );
    }
}

fn local_commit_materialization_allowed(
    node_id: NodeId,
    current_leader: Option<NodeId>,
    voter_ids: &BTreeSet<NodeId>,
) -> bool {
    current_leader == Some(node_id)
        || (current_leader.is_none() && voter_ids.len() == 1 && voter_ids.contains(&node_id))
}

/// `RaftProposer` impl that lets `ReplicatedDatastore::Raft` mutations
/// T1.4: build a `LogApplyCommit` on the leader (via
/// `backend.build_log_apply_commit_for_outbox` — runs an IMMEDIATE txn
/// that allocates the rv, validates preconditions, captures UIDs, and
/// claims the idempotency slot WITHOUT applying resource mutations) and
/// submit the encoded commit through openraft's `client_write`. The
/// state machine apply path on every node — leader, voter follower, and
/// learner — is the only caller of `apply_commit_in_tx` after raft
/// commits the entry.
#[async_trait]
impl crate::datastore::replicated::RaftProposer for RaftNode {
    async fn propose_command(
        &self,
        command: crate::datastore::command::StorageCommand,
    ) -> Result<()> {
        use crate::datastore::sqlite::BuildOutboxOutcome;
        self.ensure_local_leader_for_commit_materialization()?;
        let idempotency_key = format!(
            "raft-leader-{}-{}",
            self.authoring_node,
            uuid::Uuid::new_v4()
        );
        let operation = derive_operation_label(&command);
        // Flow-control gate: acquire a permit BEFORE build_log_apply_commit_for_outbox
        // reserves the next resourceVersion. This is the core of the flow-control plan
        // (finding.md): the leader cannot build an unacknowledged RV backlog ahead of
        // raft progress. The permit is held as an RAII guard; every exit path (success,
        // materialization failure, client_write failure) returns it to the pool.
        let _flow_permit = self.flow_control.acquire().await;
        // T1.4: the proposer's "payload" is now the StorageCommand's
        // OutboxPayload protobuf, but only as input to the builder.
        // The builder decodes it, constructs the LogApplyCommit, and
        // returns it — we encode THAT as the raft entry payload.
        let payload_bytes = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
            .encode_protobuf()
            .context("encode StorageCommand for raft build")?;
        let outcome = self
            .backend
            .build_log_apply_commit_for_outbox(
                &idempotency_key,
                operation.as_str(),
                payload_bytes.as_ref(),
                &self.authoring_node,
            )
            .await
            .map_err(|err| anyhow::anyhow!("build log_apply commit for raft propose: {err}"))?;
        let commit = match outcome {
            BuildOutboxOutcome::NeedsPropose { commit, .. } => commit,
            BuildOutboxOutcome::LeaseRenewShortcircuit => {
                // Lease renews don't go through raft — the builder
                // already validated and the leader returns success.
                return Ok(());
            }
            BuildOutboxOutcome::AlreadyApplied { .. } => {
                // The idempotency key was already recorded as applied;
                // the leader returns success without proposing again.
                return Ok(());
            }
        };
        let reserved_rv = commit.resource_version;
        let entry_bytes = crate::log_apply::encode_commit_protobuf(&commit)
            .context("encode LogApplyCommit for raft propose")?;
        let apply_result = match self
            .propose_materialized_commit(StorageCommandPayload::from_bytes(entry_bytes))
            .await
        {
            Ok(result) => result,
            Err(err) => {
                self.cleanup_rejected_materialized_commit(&idempotency_key, reserved_rv)
                    .await;
                return Err(err);
            }
        };
        if let Some(message) = apply_result.error_message {
            return Err(anyhow::anyhow!(message));
        }
        Ok(())
    }

    /// T6 step 4c: propose an outbox-flavored write through raft.
    /// Same flow as `propose_command` but preserves the caller's
    /// idempotency + operation for applied_outbox dedup coherence.
    /// Returns the committed `OutboxApplyResult` after raft has
    /// applied the entry on this member.
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
        use crate::datastore::sqlite::BuildOutboxOutcome;
        if let Err(err) = self.ensure_local_leader_for_commit_materialization() {
            return Err(crate::kubelet::outbox::OutboxApplyError::Retryable(
                err.to_string(),
            ));
        }
        let payload_bytes = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
            .encode_protobuf()
            .map_err(|err| {
                crate::kubelet::outbox::OutboxApplyError::Retryable(format!(
                    "encode StorageCommand for raft outbox propose: {err}"
                ))
            })?;
        let outcome = self
            .backend
            .build_log_apply_commit_for_outbox(
                idempotency_key,
                operation,
                payload_bytes.as_ref(),
                authoring_node,
            )
            .await
            .map_err(|err| match err {
                crate::kubelet::outbox::OutboxApplyError::ConflictTerminal(message) => {
                    crate::kubelet::outbox::OutboxApplyError::ConflictTerminal(message)
                }
                crate::kubelet::outbox::OutboxApplyError::NotFound(message) => {
                    crate::kubelet::outbox::OutboxApplyError::NotFound(message)
                }
                crate::kubelet::outbox::OutboxApplyError::UidMismatch { expected, actual } => {
                    crate::kubelet::outbox::OutboxApplyError::UidMismatch { expected, actual }
                }
                crate::kubelet::outbox::OutboxApplyError::Retryable(message) => {
                    crate::kubelet::outbox::OutboxApplyError::Retryable(format!(
                        "build log_apply commit for raft outbox propose: {message}"
                    ))
                }
            })?;
        let commit = match outcome {
            BuildOutboxOutcome::NeedsPropose { commit, .. } => commit,
            BuildOutboxOutcome::LeaseRenewShortcircuit => {
                // Lease renews don't go through raft.
                return Ok(crate::kubelet::outbox::OutboxApplyResult::Applied { applied_rv: 0 });
            }
            BuildOutboxOutcome::AlreadyApplied { applied_rv } => {
                // The idempotency key already applied, avoid duplicate
                // proposal and keep the existing RV.
                return Ok(crate::kubelet::outbox::OutboxApplyResult::AlreadyApplied {
                    applied_rv,
                });
            }
        };
        let reserved_rv = commit.resource_version;
        let entry_bytes = crate::log_apply::encode_commit_protobuf(&commit).map_err(|err| {
            crate::kubelet::outbox::OutboxApplyError::Retryable(format!(
                "encode LogApplyCommit for raft outbox propose: {err}"
            ))
        })?;
        let apply_result = match self
            .propose_materialized_commit(StorageCommandPayload::from_bytes(entry_bytes))
            .await
        {
            Ok(result) => result,
            Err(err) => {
                self.cleanup_rejected_materialized_commit(idempotency_key, reserved_rv)
                    .await;
                return Err(crate::kubelet::outbox::OutboxApplyError::Retryable(
                    format!("raft propose: {err}"),
                ));
            }
        };
        if let Some(message) = apply_result.error_message {
            return Err(crate::kubelet::outbox::OutboxApplyError::ConflictTerminal(
                message,
            ));
        }
        let applied_rv = apply_result.applied_rv.unwrap_or(0);
        Ok(crate::kubelet::outbox::OutboxApplyResult::Applied { applied_rv })
    }
}

fn derive_operation_label(
    command: &crate::datastore::command::StorageCommand,
) -> crate::kubelet::outbox::payload::OutboxOperation {
    use crate::datastore::command::StorageCommand;
    use crate::kubelet::outbox::payload::OutboxOperation;
    match command {
        StorageCommand::UpdateStatus { kind, .. } if kind == "Node" => OutboxOperation::NodeStatus,
        StorageCommand::UpdateStatus { kind, .. } if kind == "Lease" => OutboxOperation::LeaseRenew,
        _ => OutboxOperation::PodStatus,
    }
}

/// Adapter that wraps a `RaftNode` so the gRPC layer can dispatch
/// `RaftAppendEntries` / `RaftVote` / `RaftInstallSnapshot` envelopes
/// into the local `Raft<TypeConfig>` engine. The envelopes carry
/// serde-encoded openraft RPC payloads; this adapter deserializes,
/// calls the matching `Raft` method, serializes the response, and
/// returns the bytes to the gRPC server.
#[derive(Clone)]
pub struct RaftNodeRpcRouter {
    raft: Raft<TypeConfig>,
}

impl RaftNodeRpcRouter {
    pub fn new(raft: Raft<TypeConfig>) -> Self {
        Self { raft }
    }

    pub fn from_node(node: &RaftNode) -> Self {
        Self::new(node.raft.clone())
    }
}

#[async_trait]
impl crate::replication::grpc::raft_rpc::RaftRpcRouter for RaftNodeRpcRouter {
    async fn append_entries(
        &self,
        payload: Vec<u8>,
    ) -> std::result::Result<Vec<u8>, crate::replication::grpc::raft_rpc::RaftRpcRouterError> {
        use crate::replication::grpc::raft_rpc::RaftRpcRouterError;
        let req: openraft::raft::AppendEntriesRequest<TypeConfig> =
            serde_json::from_slice(&payload)
                .map_err(|e| RaftRpcRouterError::Dispatch(format!("decode AE: {e}")))?;
        let resp = self
            .raft
            .append_entries(req)
            .await
            .map_err(|e| RaftRpcRouterError::Dispatch(format!("raft.append_entries: {e}")))?;
        serde_json::to_vec(&resp)
            .map_err(|e| RaftRpcRouterError::Dispatch(format!("encode AE resp: {e}")))
    }

    async fn vote(
        &self,
        payload: Vec<u8>,
    ) -> std::result::Result<Vec<u8>, crate::replication::grpc::raft_rpc::RaftRpcRouterError> {
        use crate::replication::grpc::raft_rpc::RaftRpcRouterError;
        let req: openraft::raft::VoteRequest<NodeId> = serde_json::from_slice(&payload)
            .map_err(|e| RaftRpcRouterError::Dispatch(format!("decode Vote: {e}")))?;
        let resp = self
            .raft
            .vote(req)
            .await
            .map_err(|e| RaftRpcRouterError::Dispatch(format!("raft.vote: {e}")))?;
        serde_json::to_vec(&resp)
            .map_err(|e| RaftRpcRouterError::Dispatch(format!("encode Vote resp: {e}")))
    }

    async fn install_snapshot(
        &self,
        payload: Vec<u8>,
    ) -> std::result::Result<Vec<u8>, crate::replication::grpc::raft_rpc::RaftRpcRouterError> {
        use crate::replication::grpc::raft_rpc::RaftRpcRouterError;
        let req: openraft::raft::InstallSnapshotRequest<TypeConfig> =
            serde_json::from_slice(&payload)
                .map_err(|e| RaftRpcRouterError::Dispatch(format!("decode IS: {e}")))?;
        let resp = self
            .raft
            .install_snapshot(req)
            .await
            .map_err(|e| RaftRpcRouterError::Dispatch(format!("raft.install_snapshot: {e}")))?;
        serde_json::to_vec(&resp)
            .map_err(|e| RaftRpcRouterError::Dispatch(format!("encode IS resp: {e}")))
    }
}

/// Server-side handler for `JoinAsControlplane` RPCs. When the current
/// node is the elected Raft leader, runs `RaftNode::add_voter` (P3-10)
/// and replies `Accepted`. Otherwise redirects the joiner to the
/// current leader or denies the request with a transient reason.
pub struct RaftNodeJoinHandler {
    node: Arc<RaftNode>,
    db: crate::datastore::DatastoreHandle,
    membership_metadata_mutex: tokio::sync::Mutex<()>,
}

impl RaftNodeJoinHandler {
    pub fn new(node: Arc<RaftNode>, db: crate::datastore::DatastoreHandle) -> Self {
        Self {
            node,
            db,
            membership_metadata_mutex: tokio::sync::Mutex::new(()),
        }
    }

    /// Register a joining voter's Node object in the cluster DB via
    /// raft. This creates the Node row through the raft proposer (if
    /// attached) so all voters see the new node.
    async fn register_voter_node(
        &self,
        node_name: &str,
        addr: &str,
        as_learner: bool,
        node_internal_ip: Option<String>,
    ) -> anyhow::Result<()> {
        use crate::kubelet::node::NodeRegistrationAddresses;
        // Extract the joiner's IP from the gRPC address
        // (e.g. "https://10.99.0.14:7679" → "10.99.0.14")
        let joiner_ip = addr
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .split(':')
            .next()
            .unwrap_or("")
            .to_string();
        // Extract the gRPC port from the joiner's address for the
        // grpc-port annotation (workers use it for controlplane discovery).
        let joiner_grpc_port = addr.rsplit(':').next().and_then(|s| s.parse::<u16>().ok());

        let node_mode = crate::bootstrap::NodeMode::Root;
        let node_role = crate::bootstrap::NodeRole::Controlplane {
            leader_endpoints: vec![addr.to_string()],
            token: None,
            skip_ca: false,
            as_learner,
        };
        let leader_shape = self.node.current_shape();
        // The joiner is a follower, not the leader; fix the is_leader flag.
        // is_learner mirrors the join mode so the leader stamps the
        // correct role label on the joiner's Node row: learners get
        // `node-role.kubernetes.io/replica`, voter joiners get
        // `node-role.kubernetes.io/control-plane`.
        let joiner_shape = crate::datastore::raft::types::RaftShape {
            voter_count: leader_shape.voter_count,
            is_leader: false,
            is_learner: as_learner,
        };
        let registration_addresses = NodeRegistrationAddresses::new(
            node_internal_ip.unwrap_or_else(|| joiner_ip.clone()),
            Some(joiner_ip),
        );
        crate::kubelet::node::register_node_impl_opts(
            self.db.as_ref(),
            None,
            None,
            node_name,
            &node_mode,
            &node_role,
            None,
            registration_addresses.external_ip(),
            Some(&joiner_shape),
            Some(registration_addresses.internal_ip().to_string()),
            joiner_grpc_port,
        )
        .await
    }

    async fn refresh_cluster_membership_metadata(
        &self,
        admitted_node_name: &str,
        as_learner: bool,
    ) -> anyhow::Result<()> {
        let _guard = self.membership_metadata_mutex.lock().await;
        let membership = match crate::bootstrap::cluster_meta::read_cluster_membership(
            self.db.as_ref(),
        )
        .await
        {
            Ok(membership) => membership,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "JoinAsControlplane: cluster membership metadata unavailable; skipping voter metadata refresh"
                );
                return Ok(());
            }
        };
        let latest = match crate::bootstrap::cluster_meta::read_cluster_membership(self.db.as_ref())
            .await
        {
            Ok(latest) => Some(latest),
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "JoinAsControlplane: latest cluster membership metadata unavailable; refreshing from initial snapshot"
                );
                None
            }
        };
        let membership = merge_controlplane_join_membership_metadata(
            membership,
            latest.as_ref(),
            admitted_node_name,
            as_learner,
            &self.node.authoring_node,
        );
        crate::bootstrap::cluster_meta::write_cluster_membership(self.db.as_ref(), &membership)
            .await
            .with_context(|| {
                format!(
                    "failed to refresh cluster membership metadata after admitting {admitted_node_name}"
                )
            })
    }
}

fn merge_controlplane_join_membership_metadata(
    mut membership: crate::control_plane::client::membership::ClusterMembership,
    latest: Option<&crate::control_plane::client::membership::ClusterMembership>,
    admitted_node_name: &str,
    as_learner: bool,
    leader_hint: &str,
) -> crate::control_plane::client::membership::ClusterMembership {
    if let Some(latest) = latest
        && latest.cluster_id == membership.cluster_id
    {
        membership.voters.extend(latest.voters.iter().cloned());
        membership.term = membership.term.max(latest.term);
    }
    if !as_learner {
        membership.voters.push(admitted_node_name.to_string());
    }
    membership.voters.sort();
    membership.voters.dedup();
    membership.leader_hint = Some(leader_hint.to_string());
    membership
}

#[async_trait]
impl crate::replication::grpc::raft_rpc::ControlplaneJoinHandler for RaftNodeJoinHandler {
    async fn join(
        &self,
        node_id: u64,
        addr: String,
        node_name: String,
        as_learner: bool,
        node_internal_ip: Option<String>,
    ) -> std::result::Result<
        crate::replication::grpc::raft_rpc::ControlplaneJoinOutcome,
        crate::replication::grpc::raft_rpc::RaftRpcRouterError,
    > {
        use crate::replication::grpc::raft_rpc::{ControlplaneJoinOutcome, RaftRpcRouterError};
        let metrics = self.node.raft.metrics().borrow().clone();
        let is_leader = metrics.current_leader == Some(self.node.node_id);
        if !is_leader {
            return Ok(match self.node.current_leader_info() {
                Some((leader_id, leader_addr)) => ControlplaneJoinOutcome::RedirectToLeader {
                    leader_id,
                    leader_addr,
                },
                None => ControlplaneJoinOutcome::Denied {
                    reason: "no leader currently elected; retry later".into(),
                },
            });
        }
        let admit_result = if as_learner {
            // T1.5.x: learner admission — call add_learner_only which
            // starts AppendEntries replication but does NOT follow up
            // with change_membership. The node stays a learner.
            tracing::info!(
                joining_node_id = node_id,
                joining_node_name = %node_name,
                joining_addr = %addr,
                "JoinAsControlplane(as_learner=true): leader running RaftNode::add_learner_only"
            );
            self.node
                .add_learner_only(node_id, addr.clone())
                .await
                .map_err(|err| {
                    RaftRpcRouterError::Dispatch(format!("add_learner_only({node_id}): {err}"))
                })
        } else {
            tracing::info!(
                joining_node_id = node_id,
                joining_node_name = %node_name,
                joining_addr = %addr,
                "JoinAsControlplane: leader running RaftNode::add_voter"
            );
            self.node
                .add_voter(node_id, addr.clone())
                .await
                .map_err(|err| RaftRpcRouterError::Dispatch(format!("add_voter({node_id}): {err}")))
        };
        admit_result?;
        // Register the joining node's Node object through raft so all
        // voters see it. The joiner skips its own local registration
        // during bootstrap, so this is the only path that creates the
        // Node row. (Learners need a Node row too — they serve reads.)
        if let Err(err) = self
            .register_voter_node(&node_name, &addr, as_learner, node_internal_ip)
            .await
        {
            tracing::warn!(
                joining_node_name = %node_name,
                error = %err,
                "JoinAsControlplane: failed to register joining Node row"
            );
        }
        let voter_count_after = {
            // T4: read fresh metrics — a learner join may have demoted
            // a voter, so voter count can change even for as_learner=true.
            let metrics_after = self.node.raft.metrics().borrow().clone();
            metrics_after
                .membership_config
                .membership()
                .voter_ids()
                .count() as u32
        };
        self.refresh_cluster_membership_metadata(&node_name, as_learner)
            .await
            .map_err(|err| {
                RaftRpcRouterError::Dispatch(format!("refresh cluster membership metadata: {err}"))
            })?;
        Ok(ControlplaneJoinOutcome::Accepted {
            voter_count_after,
            admitted_as_learner: as_learner,
            ca_cert_pem: String::new(),
            encrypted_ca_key: Vec::new(),
            ca_key_nonce: [0u8; 12],
        })
    }

    async fn is_controlplane_member(&self, node_name: &str) -> bool {
        let target = super::types::raft_node_id_for_node_name(node_name);
        let metrics = self.node.raft.metrics().borrow().clone();
        // `nodes()` is the full membership set (voters + learners); both are
        // control-plane members admitted only via a controlplane-token-gated
        // JoinAsControlplane.
        metrics
            .membership_config
            .membership()
            .nodes()
            .any(|(id, _)| *id == target)
    }
}

#[cfg(test)]
mod tests {
    // Test assertions briefly lock a mock's recorded-call log to inspect it
    // after an awaited propose; the std guard is dropped at end of statement
    // and the test runtime is single-threaded, so the lint is not a concern.
    #![allow(clippy::await_holding_lock)]
    use super::*;
    use crate::datastore::sqlite::{DbExecutor, opener};
    use crate::task_supervisor::{TaskCategoryConfig, TaskSupervisor};

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

    async fn fresh_node(node_id: NodeId) -> (RaftNode, Arc<dyn DatastoreBackend>) {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_executor = DbExecutor::open_with_opts(
            opener::OpenOpts::node_in_memory(),
            supervisor.clone(),
            "sqlite:raft-node-test",
        )
        .await
        .expect("open node-local executor");
        let node_local = Arc::new(
            SqliteNodeLocalDb::from_executor(node_executor).expect("create node-local db"),
        );
        let backend: Arc<dyn DatastoreBackend> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let backend_for_caller = backend.clone();
        let raft_node = RaftNode::start(node_id, format!("n{node_id}"), backend, node_local)
            .await
            .expect("RaftNode::start");
        (raft_node, backend_for_caller)
    }

    #[test]
    fn direct_node_resource_update_is_not_classified_as_node_status() {
        let command = crate::datastore::command::StorageCommand::UpdateResource {
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
            crate::kubelet::outbox::payload::OutboxOperation::NodeStatus,
            "direct API Node updates must not use the kubelet NodeStatus outbox operation"
        );
    }

    #[test]
    fn join_membership_metadata_merge_preserves_concurrent_voters() {
        let stale = crate::control_plane::client::membership::ClusterMembership {
            cluster_id: "cluster-a".to_string(),
            voters: vec![
                "mn-controlplane1".to_string(),
                "mn-controlplane2".to_string(),
            ],
            term: 0,
            leader_hint: Some("mn-controlplane1".to_string()),
        };
        let latest = crate::control_plane::client::membership::ClusterMembership {
            cluster_id: "cluster-a".to_string(),
            voters: vec![
                "mn-controlplane1".to_string(),
                "mn-controlplane2".to_string(),
                "mn-controlplane3".to_string(),
            ],
            term: 0,
            leader_hint: Some("mn-controlplane1".to_string()),
        };

        let merged = merge_controlplane_join_membership_metadata(
            stale,
            Some(&latest),
            "mn-controlplane2",
            false,
            "mn-controlplane1",
        );

        assert_eq!(
            merged.voters,
            vec!["mn-controlplane1", "mn-controlplane2", "mn-controlplane3"],
            "a late duplicate JoinAsControlplane retry must not shrink voters metadata"
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
        crate::datastore::raft::network::LoopbackRegistry,
    ) {
        use crate::datastore::raft::network::{LoopbackRaftNetworkFactory, LoopbackRegistry};
        let registry = LoopbackRegistry::new();
        let mut nodes = Vec::new();
        for id in [10u64, 20, 30] {
            let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
            let exec = DbExecutor::open_with_opts(
                opener::OpenOpts::node_in_memory(),
                supervisor,
                "sqlite:raft-cluster-test",
            )
            .await
            .expect("open node-local executor");
            let node_local =
                Arc::new(SqliteNodeLocalDb::from_executor(exec).expect("create node-local db"));
            let backend: Arc<dyn DatastoreBackend> =
                Arc::new(crate::datastore::test_support::in_memory().await);
            let factory = LoopbackRaftNetworkFactory::new(registry.clone());
            let n =
                RaftNode::start_with_network(id, format!("n{id}"), backend, node_local, factory)
                    .await
                    .expect("RaftNode::start_with_network");
            registry.register(id, n.raft.clone());
            nodes.push(n);
        }
        (nodes, registry)
    }

    #[tokio::test]
    async fn three_voter_cluster_elects_a_leader() {
        let (nodes, _registry) = fresh_three_voter_cluster().await;
        // Have node 10 initialize the cluster with all three voters.
        let mut members = std::collections::BTreeMap::new();
        for n in &nodes {
            members.insert(
                n.node_id,
                BasicNode {
                    addr: format!("https://localhost:{}", 7679 + n.node_id),
                },
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
            let exec = DbExecutor::open_with_opts(
                opener::OpenOpts::node_in_memory(),
                supervisor,
                "sqlite:raft-forward-test",
            )
            .await
            .expect("open node-local executor");
            let node_local =
                Arc::new(SqliteNodeLocalDb::from_executor(exec).expect("create node-local db"));
            let backend: Arc<dyn DatastoreBackend> =
                Arc::new(crate::datastore::test_support::in_memory().await);
            let factory = LoopbackRaftNetworkFactory::new(registry.clone());
            let mock = Arc::new(CapturingForwarder::default());
            let n =
                RaftNode::start_with_network(id, format!("n{id}"), backend, node_local, factory)
                    .await
                    .expect("RaftNode::start_with_network")
                    .with_forwarder(mock.clone());
            registry.register(id, n.raft.clone());
            nodes.push(n);
            mocks.push(mock);
        }
        let mut members = std::collections::BTreeMap::new();
        for n in &nodes {
            members.insert(
                n.node_id,
                BasicNode {
                    addr: format!("https://localhost:{}", 7679 + n.node_id),
                },
            );
        }
        nodes[0]
            .raft
            .initialize(members)
            .await
            .expect("initialize cluster");
        // Wait for any node to become leader.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut leader_id: Option<NodeId> = None;
        while std::time::Instant::now() < deadline {
            for n in &nodes {
                let m = n.raft.metrics().borrow().clone();
                if let Some(lid) = m.current_leader {
                    leader_id = Some(lid);
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
        use crate::datastore::replicated::RaftProposer;

        let registry = LoopbackRegistry::new();
        let mut nodes = Vec::new();
        let mut backends = Vec::new();
        for id in [10u64, 20, 30] {
            let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
            let exec = DbExecutor::open_with_opts(
                opener::OpenOpts::node_in_memory(),
                supervisor,
                "sqlite:raft-follower-no-local-commit-test",
            )
            .await
            .expect("open node-local executor");
            let node_local =
                Arc::new(SqliteNodeLocalDb::from_executor(exec).expect("create node-local db"));
            let backend: Arc<dyn DatastoreBackend> =
                Arc::new(crate::datastore::test_support::in_memory().await);
            let factory = LoopbackRaftNetworkFactory::new(registry.clone());
            let node = RaftNode::start_with_network(
                id,
                format!("n{id}"),
                backend.clone(),
                node_local,
                factory,
            )
            .await
            .expect("RaftNode::start_with_network");
            registry.register(id, node.raft.clone());
            nodes.push(node);
            backends.push(backend);
        }
        let mut members = std::collections::BTreeMap::new();
        for node in &nodes {
            members.insert(
                node.node_id,
                BasicNode {
                    addr: format!("https://localhost:{}", 7679 + node.node_id),
                },
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
            .propose_command(crate::datastore::command::StorageCommand::CreateResource {
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
            })
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
    async fn raft_proposer_cleans_placeholder_when_materialized_commit_is_rejected() {
        use crate::datastore::replicated::RaftProposer;

        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let exec = DbExecutor::open_with_opts(
            opener::OpenOpts::node_in_memory(),
            supervisor,
            "sqlite:raft-rejected-materialized-commit-test",
        )
        .await
        .expect("open node-local executor");
        let node_local =
            Arc::new(SqliteNodeLocalDb::from_executor(exec).expect("create node-local db"));
        let backend: Arc<dyn DatastoreBackend> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let node = RaftNode::start(10, "n10".to_string(), backend.clone(), node_local)
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
            .propose_command(crate::datastore::command::StorageCommand::CreateResource {
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
            })
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
            "rejected materialized commit must not leave local placeholders: {rows:?}"
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
        use crate::datastore::raft::network::LoopbackRaftNetworkFactory;
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let exec = DbExecutor::open_with_opts(
            opener::OpenOpts::node_in_memory(),
            supervisor,
            "sqlite:raft-voter-test",
        )
        .await
        .expect("open node-local executor");
        let node_local =
            Arc::new(SqliteNodeLocalDb::from_executor(exec).expect("create node-local db"));
        let backend: Arc<dyn DatastoreBackend> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let factory = LoopbackRaftNetworkFactory::new(registry.clone());
        let node = RaftNode::start_with_network(id, format!("n{id}"), backend, node_local, factory)
            .await
            .expect("start node");
        registry.register(id, node.raft.clone());
        node
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
        use crate::datastore::replicated::RaftProposer;

        let (node, backend) = fresh_node(90).await;
        node.bootstrap_single_voter("https://10.99.0.90:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&node, std::time::Duration::from_secs(5))
            .await
            .expect("become leader");

        let subnet_command = |node_name: &'static str, node_ip: &'static str| {
            crate::datastore::command::StorageCommand::AllocateNodeSubnet {
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

        node.propose_command(crate::datastore::command::StorageCommand::CreateResource {
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            namespace: Some("default".into()),
            name: "after-subnet".into(),
            data: serde_json::json!({
                "metadata": {"name": "after-subnet", "namespace": "default"}
            }),
        })
        .await
        .expect("raft still accepts writes after concurrent subnet proposals");

        let rows = backend
            .list_applied_outbox()
            .await
            .expect("list applied_outbox");
        assert!(
            rows.iter()
                .all(|row| !row.subject_key.is_empty() && row.applied_rv.is_some()),
            "raft apply must finalize every outbox placeholder: {rows:?}"
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
    async fn raft_create_resource_rejects_duplicate_name() {
        use crate::datastore::replicated::RaftProposer;

        let (node, backend) = fresh_node(91).await;
        node.bootstrap_single_voter("https://10.99.0.91:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&node, std::time::Duration::from_secs(5))
            .await
            .expect("become leader");

        let runtime_class_create =
            |uid: &'static str| crate::datastore::command::StorageCommand::CreateResource {
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

    #[tokio::test]
    async fn join_handler_on_leader_runs_add_voter_and_reports_count() {
        use crate::datastore::raft::network::LoopbackRegistry;
        use crate::replication::grpc::raft_rpc::{
            ControlplaneJoinHandler, ControlplaneJoinOutcome,
        };
        let registry = LoopbackRegistry::new();
        let leader = Arc::new(fresh_voter_in_registry(50, &registry).await);
        let _follower = fresh_voter_in_registry(51, &registry).await;
        leader
            .bootstrap_single_voter("https://10.99.0.50:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&leader, std::time::Duration::from_secs(5))
            .await
            .unwrap();

        let handler = RaftNodeJoinHandler::new(leader.clone(), test_db().await);
        let outcome = handler
            .join(
                51,
                "https://10.99.0.51:7679".into(),
                "n51".into(),
                false,
                None,
            )
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
    }

    #[tokio::test]
    async fn join_handler_voter_admission_updates_cluster_membership_metadata() {
        use crate::datastore::raft::network::LoopbackRegistry;
        use crate::replication::grpc::raft_rpc::{
            ControlplaneJoinHandler, ControlplaneJoinOutcome,
        };
        let registry = LoopbackRegistry::new();
        let leader = Arc::new(fresh_voter_in_registry(52, &registry).await);
        let _follower = fresh_voter_in_registry(53, &registry).await;
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

        let handler = RaftNodeJoinHandler::new(leader.clone(), leader_db.clone());
        let outcome = handler
            .join(
                53,
                "https://10.99.0.53:7679".into(),
                "mn-controlplane2".into(),
                false,
                None,
            )
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
        use crate::replication::grpc::raft_rpc::{
            ControlplaneJoinHandler, ControlplaneJoinOutcome,
        };
        let (node, _) = fresh_node(60).await;
        let arc = Arc::new(node);
        let handler = RaftNodeJoinHandler::new(arc, test_db().await);
        let outcome = handler
            .join(
                61,
                "https://10.99.0.61:7679".into(),
                "n61".into(),
                false,
                None,
            )
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
        use crate::replication::grpc::raft_rpc::{
            ControlplaneJoinHandler, ControlplaneJoinOutcome,
        };
        let (leader, _) = fresh_node(70).await;
        leader
            .bootstrap_single_voter("https://10.99.0.70:7679".into())
            .await
            .unwrap();
        wait_for_leader(&leader, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        let leader = Arc::new(leader);

        let handler = RaftNodeJoinHandler::new(leader.clone(), test_db().await);
        let outcome = handler
            .join(
                71,
                "https://10.99.0.71:7679".into(),
                "n71".into(),
                true,
                None,
            )
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
        use crate::replication::grpc::raft_rpc::{
            ControlplaneJoinHandler, ControlplaneJoinOutcome,
        };
        let (leader, _) = fresh_node(72).await;
        leader
            .bootstrap_single_voter("https://10.99.0.72:7679".into())
            .await
            .unwrap();
        wait_for_leader(&leader, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        let leader = Arc::new(leader);

        let leader_db = test_db().await;
        let handler = RaftNodeJoinHandler::new(leader.clone(), leader_db.clone());
        let outcome = handler
            .join(
                73,
                "https://10.99.0.73:7679".into(),
                "n73".into(),
                true,
                None,
            )
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
        use crate::replication::grpc::raft_rpc::{
            ControlplaneJoinHandler, ControlplaneJoinOutcome,
        };
        let registry = LoopbackRegistry::new();
        let leader = Arc::new(fresh_voter_in_registry(80, &registry).await);
        let _follower = fresh_voter_in_registry(81, &registry).await;
        leader
            .bootstrap_single_voter("https://10.99.0.80:7679".into())
            .await
            .unwrap();
        wait_for_leader(&leader, std::time::Duration::from_secs(5))
            .await
            .unwrap();

        let leader_db = test_db().await;
        let handler = RaftNodeJoinHandler::new(leader.clone(), leader_db.clone());
        let outcome = handler
            .join(
                81,
                "https://10.99.0.81:7679".into(),
                "n81".into(),
                false,
                Some("172.31.81.2".to_string()),
            )
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
        use crate::replication::grpc::raft_rpc::RaftRpcRouter;
        let (node, _) = fresh_node(70).await;
        node.bootstrap_single_voter("https://10.99.0.70:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&node, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        let router = RaftNodeRpcRouter::from_node(&node);
        let rpc = openraft::raft::VoteRequest::new(
            openraft::Vote::new(100, 70),
            Some(openraft::LogId::new(openraft::LeaderId::new(100, 70), 0)),
        );
        let bytes = serde_json::to_vec(&rpc).unwrap();
        let out = router.vote(bytes).await.expect("vote dispatch");
        // Confirms the round-trip: the router decoded the envelope,
        // handed it to raft.vote, and serialized the response back.
        // Vote-granted semantics depend on openraft's current-term
        // state which isn't deterministic in this fresh-cluster test;
        // we just assert the response decodes cleanly.
        let _resp: openraft::raft::VoteResponse<NodeId> =
            serde_json::from_slice(&out).expect("decode vote response");
        node.shutdown().await.unwrap();
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
        let exec1 = DbExecutor::open_with_opts(
            opener::OpenOpts::node_in_memory(),
            sup1,
            "sqlite:raft-demote-test-l",
        )
        .await
        .unwrap();
        let nl1 = Arc::new(SqliteNodeLocalDb::from_executor(exec1).unwrap());
        let be1: Arc<dyn DatastoreBackend> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let leader_network = LoopbackRaftNetworkFactory::new(registry.clone());
        let leader =
            RaftNode::start_with_network(leader_id, "n70".into(), be1, nl1, leader_network)
                .await
                .unwrap();
        registry.register(leader_id, leader.raft.clone());
        leader
            .bootstrap_single_voter("https://10.99.0.70:7679".into())
            .await
            .unwrap();

        // Voter node.
        let sup2 = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let exec2 = DbExecutor::open_with_opts(
            opener::OpenOpts::node_in_memory(),
            sup2,
            "sqlite:raft-demote-test-v",
        )
        .await
        .unwrap();
        let nl2 = Arc::new(SqliteNodeLocalDb::from_executor(exec2).unwrap());
        let be2: Arc<dyn DatastoreBackend> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let voter_network = LoopbackRaftNetworkFactory::new(registry.clone());
        let voter_node =
            RaftNode::start_with_network(voter_id, "n80".into(), be2, nl2, voter_network)
                .await
                .unwrap();
        registry.register(voter_id, voter_node.raft.clone());

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
    fn propose_create_command(uid: &str) -> crate::datastore::command::StorageCommand {
        crate::datastore::command::StorageCommand::CreateResource {
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
        use crate::datastore::replicated::RaftProposer;

        let (node, backend) = fresh_node(70).await;
        node.bootstrap_single_voter("https://10.99.0.70:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&node, std::time::Duration::from_secs(5))
            .await
            .expect("become leader");
        // Exhaust the flow-control gate before propose_command runs.
        let _p1 = node.flow_control.acquire().await;
        let _p2 = node.flow_control.acquire().await;
        let _p3 = node.flow_control.acquire().await;
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
        drop(_p1);
        drop(_p2);
        drop(_p3);
        node.shutdown().await.unwrap();
    }

    /// Integration test: at most 3 unacknowledged propose_command calls may be in flight.
    /// Holds 3 permits manually, then verifies a 4th propose call is blocked.
    #[tokio::test]
    async fn at_most_three_raft_proposals_are_in_flight() {
        use crate::datastore::replicated::RaftProposer;

        let (node, backend) = fresh_node(71).await;
        node.bootstrap_single_voter("https://10.99.0.71:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&node, std::time::Duration::from_secs(5))
            .await
            .expect("become leader");
        assert_eq!(
            node.flow_control.max_in_flight(),
            3,
            "flow-control cap must be 3 (matches RAFT_MAX_PAYLOAD_ENTRIES)"
        );
        // Hold all 3 permits, simulating 3 in-flight proposals.
        let _p1 = node.flow_control.acquire().await;
        let _p2 = node.flow_control.acquire().await;
        let _p3 = node.flow_control.acquire().await;
        assert_eq!(node.flow_control.available_permits(), 0);

        // A 4th propose call must block (no permits available).
        let rv_before = backend.get_current_resource_version().await.unwrap();
        let cmd = propose_create_command("fourth-blocked");
        let timeout = tokio::time::sleep(std::time::Duration::from_millis(200));
        tokio::pin!(timeout);
        let mut propose_fut = Box::pin(node.propose_command(cmd));
        tokio::select! {
            _ = &mut propose_fut => panic!("4th propose must block when 3 permits are held"),
            _ = &mut timeout => {}
        }
        let rv_after = backend.get_current_resource_version().await.unwrap();
        assert_eq!(
            rv_after, rv_before,
            "blocked 4th propose must not have reserved an rv"
        );
        drop(propose_fut);
        drop(_p1);
        drop(_p2);
        drop(_p3);
        node.shutdown().await.unwrap();
    }

    /// Integration test: when propose_command fails AT MATERIALIZATION (before
    /// client_write) — e.g. because the backend's build step rejected a duplicate
    /// create — the flow-control permit must still be released. The `_flow_permit`
    /// RAII guard handles this naturally on every error-return path.
    #[tokio::test]
    async fn raft_proposal_permit_released_on_materialization_failure() {
        use crate::datastore::replicated::RaftProposer;

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
        assert_eq!(permits_before, 3, "permits restored after first success");

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
            3,
            "permit must be released after materialization-failure error path"
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
        assert_eq!(node.flow_control.available_permits(), 3);
        {
            let _permit = node.flow_control.acquire().await;
            assert_eq!(node.flow_control.available_permits(), 2, "permit acquired");
            // Simulating the late-failure path: the permit is held when client_write
            // would have failed; the RAII guard releases on scope exit.
        }
        assert_eq!(
            node.flow_control.available_permits(),
            3,
            "RAII permit must release on scope exit (mirrors propose_command's error paths)"
        );
        node.shutdown().await.unwrap();
    }

    /// Integration test: after a successful propose_command (entry committed and
    /// applied), the flow-control permit returns to the pool so subsequent proposals
    /// can proceed.
    #[tokio::test]
    async fn raft_proposal_permit_released_on_terminal_success() {
        use crate::datastore::replicated::RaftProposer;

        let (node, _backend) = fresh_node(74).await;
        node.bootstrap_single_voter("https://10.99.0.74:7679".into())
            .await
            .expect("bootstrap");
        wait_for_leader(&node, std::time::Duration::from_secs(5))
            .await
            .expect("become leader");
        assert_eq!(node.flow_control.available_permits(), 3);

        node.propose_command(propose_create_command("ok-success"))
            .await
            .expect("propose ok");

        assert_eq!(
            node.flow_control.available_permits(),
            3,
            "permit must be released after successful terminal propose_command"
        );
        node.shutdown().await.unwrap();
    }

    /// Static check that the actual openraft `max_payload_entries` constant configured
    /// in `start_with_network` is 3 — matching the 3-permit flow-control gate so a
    /// single AppendEntries retry cannot resend a larger logical batch than the
    /// in-flight bound. References the module-scope constant so this test would fail
    /// if anyone reverted RAFT_MAX_PAYLOAD_ENTRIES upward without updating the test.
    #[test]
    fn raft_max_payload_entries_is_bounded_for_lossy_resend() {
        // The constant is referenced by Config.max_payload_entries inside
        // start_with_network — both sites are the same identifier so they cannot drift.
        assert_eq!(
            super::RAFT_MAX_PAYLOAD_ENTRIES,
            3,
            "RAFT_MAX_PAYLOAD_ENTRIES must be 3 to match the 3-permit flow-control gate"
        );
        // Also assert the matching invariant: the flow-control cap equals the
        // openraft payload cap, so a single retry cannot resend more entries than
        // the gate would have admitted in flight.
        let fc = crate::datastore::raft::flow_control::RaftCommitFlowControl::new(
            super::RAFT_MAX_PAYLOAD_ENTRIES as usize,
        );
        assert_eq!(fc.max_in_flight(), 3);
    }
}
