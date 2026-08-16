//! Embedded OpenRaft membership, join admission, and codec-v3 fencing.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use klights_cluster_core::{NodeId, RaftShape, raft_node_id_for_node_name};
use klights_cluster_store::{
    COMMAND_CODEC_ACTIVATION_VERSION_META_KEY, COMMAND_CODEC_V3_ACTIVATION_VALUE,
    StorageCommandResult,
};
use openraft::{ChangeMembers, Raft};

use crate::activation::CommandCodecV3Activation;
use crate::flow_control::{RaftCommitFlowControl, RaftCommitFlowControlDrain};
use crate::materializer::RaftCommitMaterializer;
use crate::proposal::EmbeddedRaftProposal;
use crate::types::{RaftMemberLogId, RaftMemberNode, StorageCommandPayload, TypeConfig};

const RAFT_MEMBER_ADMISSION_META_PREFIX: &str = "raft_member_admission/";

/// A fresh learner may need several lossy round trips before its replication
/// match becomes observable. Keep this bounded per admission attempt and below
/// the join RPC deadline; the durable provisional marker lets retries continue
/// after a lossy 200 ms RTT attempt expires.
pub const CONTROLPLANE_REPLICATION_WAIT_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(20);

/// Return the supervised retry delay for a rejected or unavailable
/// control-plane membership join. The caller owns the timer and cancellation;
/// this membership policy only determines the bounded delay.
pub fn controlplane_join_retry_delay(attempt: u32) -> std::time::Duration {
    std::time::Duration::from_secs(u64::from(attempt.saturating_mul(5).min(60)))
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
struct RaftMemberAdmission {
    storage_incarnation: String,
    addr: String,
    as_learner: bool,
    proven_log: Option<klights_leader_api::RaftStorageLogAttestation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftMemberAdmissionResult {
    Changed,
    Unchanged,
}

/// Authenticated storage identity and requested role used by a control-plane
/// admission attempt.
#[derive(Debug, Clone)]
pub struct ControlplaneAdmissionRequest {
    pub node_id: NodeId,
    pub addr: String,
    pub as_learner: bool,
    pub storage_incarnation: String,
    pub storage_log_attestation: klights_leader_api::RaftStorageAttestation,
    pub controlplane_limit: usize,
}

/// One-shot metadata RPC port used by the exact codec-v3 activation preflight.
#[async_trait]
pub trait MemberFeatureProbe: Send + Sync {
    async fn metadata_for_member(
        &self,
        node_id: NodeId,
        addr: &str,
    ) -> Result<klights_leader_api::MetadataResponse>;
}

#[derive(Debug, thiserror::Error)]
pub enum CommandCodecV3PreflightError {
    #[error("command codec v3 preflight is not ready: {0}")]
    NotReady(String),
    #[error("command codec v3 is unsupported by member {node_id}")]
    Unsupported { node_id: NodeId },
    #[error("command codec v3 metadata probe for member {node_id} is unavailable: {message}")]
    Unavailable { node_id: NodeId, message: String },
}

/// Held only after membership has been frozen and every proposal lane drained.
pub struct CommandCodecV3Preflight<'a> {
    _membership_guard: tokio::sync::MutexGuard<'a, ()>,
    _proposal_drain: RaftCommitFlowControlDrain,
}

#[derive(Debug, thiserror::Error)]
pub enum CommandCodecV3ActivationError {
    #[error("command codec v3 activation requires the current Raft leader")]
    NotLeader,
    #[error(transparent)]
    Preflight(#[from] CommandCodecV3PreflightError),
    #[error("command codec v3 activation apply failed: {0}")]
    Apply(String),
}

impl CommandCodecV3ActivationError {
    pub fn retryable(&self) -> bool {
        matches!(
            self,
            Self::NotLeader
                | Self::Preflight(CommandCodecV3PreflightError::NotReady(_))
                | Self::Preflight(CommandCodecV3PreflightError::Unavailable { .. })
                | Self::Apply(_)
        )
    }
}

async fn verify_command_codec_v3_members(
    members: Vec<(NodeId, String)>,
    probe: &dyn MemberFeatureProbe,
) -> std::result::Result<(), CommandCodecV3PreflightError> {
    for (node_id, addr) in members {
        let metadata = probe
            .metadata_for_member(node_id, &addr)
            .await
            .map_err(|error| CommandCodecV3PreflightError::Unavailable {
                node_id,
                message: error.to_string(),
            })?;
        if metadata.command_codec_version != klights_cluster_core::COMMAND_CODEC_VERSION {
            return Err(CommandCodecV3PreflightError::Unsupported { node_id });
        }
    }
    Ok(())
}

/// Embedded OpenRaft membership owner.
///
/// The composition root supplies the already-constructed Raft engine and its
/// lower-level proposal/materialization dependencies. This service owns all
/// voter/learner mutation, exact-v3 activation, durable incarnation admission,
/// and membership-derived authority policy.
pub struct EmbeddedRaftMembership {
    node_id: NodeId,
    raft: Raft<TypeConfig>,
    storage_incarnation: String,
    membership_mutex: tokio::sync::Mutex<()>,
    materializer: Arc<dyn RaftCommitMaterializer>,
    flow_control: Arc<RaftCommitFlowControl>,
    command_codec_v3_activation: Arc<CommandCodecV3Activation>,
    authoring_node: String,
}

impl EmbeddedRaftMembership {
    pub fn new(
        node_id: NodeId,
        raft: Raft<TypeConfig>,
        storage_incarnation: String,
        materializer: Arc<dyn RaftCommitMaterializer>,
        flow_control: Arc<RaftCommitFlowControl>,
        command_codec_v3_activation: Arc<CommandCodecV3Activation>,
        authoring_node: String,
    ) -> Self {
        Self {
            node_id,
            raft,
            storage_incarnation,
            membership_mutex: tokio::sync::Mutex::new(()),
            materializer,
            flow_control,
            command_codec_v3_activation,
            authoring_node,
        }
    }

    fn proposal(&self) -> EmbeddedRaftProposal {
        EmbeddedRaftProposal::new(
            self.node_id,
            self.raft.clone(),
            self.materializer.clone(),
            self.authoring_node.clone(),
            self.flow_control.clone(),
            self.command_codec_v3_activation.clone(),
        )
    }

    async fn propose_materialized_commit(
        &self,
        payload: StorageCommandPayload,
    ) -> Result<StorageCommandResult> {
        self.proposal().propose_materialized_commit(payload).await
    }

    pub async fn bootstrap_single_voter(&self, advertise_addr: String) -> Result<()> {
        let mut members = BTreeMap::new();
        members.insert(
            self.node_id,
            RaftMemberNode::new(advertise_addr, self.storage_incarnation.clone(), None),
        );
        match self.raft.initialize(members).await {
            Ok(()) => Ok(()),
            Err(openraft::error::RaftError::APIError(
                openraft::error::InitializeError::NotAllowed { .. },
            )) => Ok(()),
            Err(error) => Err(anyhow::anyhow!("Raft::initialize: {error}")),
        }
    }

    pub async fn add_voter(&self, node_id: NodeId, addr: String) -> Result<()> {
        let _ = (node_id, addr);
        anyhow::bail!(
            "add_voter without exact-v3 receiver admission is disabled; use authenticated control-plane Join"
        )
    }

    pub async fn preflight_command_codec_v3<'a>(
        &'a self,
        probe: &dyn MemberFeatureProbe,
    ) -> std::result::Result<CommandCodecV3Preflight<'a>, CommandCodecV3PreflightError> {
        let membership_guard = self.membership_mutex.lock().await;
        let metrics = self.raft.metrics().borrow().clone();
        if metrics.current_leader != Some(self.node_id) {
            return Err(CommandCodecV3PreflightError::NotReady(
                "local Raft node is not the current leader".to_string(),
            ));
        }
        let members: Vec<(NodeId, String)> = metrics
            .membership_config
            .nodes()
            .filter(|(node_id, _)| **node_id != self.node_id)
            .map(|(node_id, node)| (*node_id, node.addr.clone()))
            .collect();
        if metrics.membership_config.nodes().next().is_none() {
            return Err(CommandCodecV3PreflightError::NotReady(
                "Raft membership has no voters or learners".to_string(),
            ));
        }
        verify_command_codec_v3_members(members, probe).await?;
        let proposal_drain = self.flow_control.acquire_exclusive_drain().await;
        Ok(CommandCodecV3Preflight {
            _membership_guard: membership_guard,
            _proposal_drain: proposal_drain,
        })
    }

    pub async fn verify_startup_command_codec_v3(
        &self,
        probe: &dyn MemberFeatureProbe,
    ) -> std::result::Result<(), CommandCodecV3PreflightError> {
        self.command_codec_v3_activation.enforce_startup_gate();
        if self.command_codec_v3_activation.is_activated() {
            return Ok(());
        }
        let _membership_guard = self.membership_mutex.lock().await;
        let metrics = self.raft.metrics().borrow().clone();
        let members = metrics
            .membership_config
            .nodes()
            .filter(|(node_id, _)| **node_id != self.node_id)
            .map(|(node_id, node)| (*node_id, node.addr.clone()))
            .collect();
        verify_command_codec_v3_members(members, probe).await
    }

    pub async fn activate_command_codec_v3(
        &self,
        probe: &dyn MemberFeatureProbe,
    ) -> std::result::Result<(), CommandCodecV3ActivationError> {
        if !self.is_leader() {
            return Err(CommandCodecV3ActivationError::NotLeader);
        }
        let codec_activated = self
            .materializer
            .read_raft_metadata(COMMAND_CODEC_ACTIVATION_VERSION_META_KEY)
            .await
            .map_err(|error| CommandCodecV3ActivationError::Apply(error.to_string()))?
            .as_deref()
            == Some(COMMAND_CODEC_V3_ACTIVATION_VALUE);
        if codec_activated {
            return Ok(());
        }
        let _preflight = self.preflight_command_codec_v3(probe).await?;
        let codec_activated = self
            .materializer
            .read_raft_metadata(COMMAND_CODEC_ACTIVATION_VERSION_META_KEY)
            .await
            .map_err(|error| CommandCodecV3ActivationError::Apply(error.to_string()))?
            .as_deref()
            == Some(COMMAND_CODEC_V3_ACTIVATION_VALUE);
        if codec_activated {
            return Ok(());
        }
        let commit = klights_cluster_core::LogApplyCommit::try_from_cluster_mutations(vec![
            klights_cluster_core::ClusterMutation::ClusterMeta(
                klights_cluster_core::ClusterMetaMutation::PutKlightsMeta {
                    key: COMMAND_CODEC_ACTIVATION_VERSION_META_KEY.to_string(),
                    value: COMMAND_CODEC_V3_ACTIVATION_VALUE.to_string(),
                },
            ),
        ])
        .map_err(|error| CommandCodecV3ActivationError::Apply(error.to_string()))?;
        let bytes = crate::log_apply_wire::encode_commit_protobuf(&commit)
            .map_err(|error| CommandCodecV3ActivationError::Apply(error.to_string()))?;
        let result = self
            .propose_materialized_commit(StorageCommandPayload::from_bytes(bytes))
            .await
            .map_err(|error| CommandCodecV3ActivationError::Apply(error.to_string()))?;
        if let Some(error) = result.error_message {
            return Err(CommandCodecV3ActivationError::Apply(error));
        }
        Ok(())
    }

    pub async fn add_learner_only(&self, node_id: NodeId, addr: String) -> Result<()> {
        let _ = (node_id, addr);
        anyhow::bail!(
            "add_learner without exact-v3 receiver admission is disabled; use authenticated control-plane Join"
        )
    }

    fn member_admission_meta_key(node_id: NodeId) -> String {
        format!("{RAFT_MEMBER_ADMISSION_META_PREFIX}{node_id}")
    }

    fn admission_marker_is_complete(admission: Option<&RaftMemberAdmission>) -> bool {
        admission.is_some_and(|admitted| admitted.proven_log.is_some())
    }

    async fn read_member_admission(&self, node_id: NodeId) -> Result<Option<RaftMemberAdmission>> {
        self.materializer
            .read_raft_metadata(&Self::member_admission_meta_key(node_id))
            .await?
            .map(|raw| serde_json::from_str(&raw).context("decode Raft member admission marker"))
            .transpose()
    }

    async fn persist_member_admission(
        &self,
        node_id: NodeId,
        admission: &RaftMemberAdmission,
    ) -> Result<()> {
        let mutation = klights_cluster_core::ClusterMutation::ClusterMeta(
            klights_cluster_core::ClusterMetaMutation::PutKlightsMeta {
                key: Self::member_admission_meta_key(node_id),
                value: serde_json::to_string(admission)?,
            },
        );
        let commit =
            klights_cluster_core::LogApplyCommit::try_from_cluster_mutations(vec![mutation])?;
        let bytes = crate::log_apply_wire::encode_commit_protobuf(&commit)?;
        let result = self
            .propose_materialized_commit(StorageCommandPayload::from_bytes(bytes))
            .await?;
        if let Some(error) = result.error_message {
            anyhow::bail!("persist Raft member admission marker: {error}");
        }
        Ok(())
    }

    async fn wait_for_uniform_membership(&self, operation: &str) -> Result<()> {
        self.raft
            .wait(Some(CONTROLPLANE_REPLICATION_WAIT_TIMEOUT))
            .metrics(
                |metrics| {
                    metrics
                        .membership_config
                        .membership()
                        .get_joint_config()
                        .len()
                        == 1
                },
                operation,
            )
            .await
            .map(|_| ())
            .map_err(|error| anyhow::anyhow!("{operation}: {error}"))
    }

    async fn wait_for_member_absent(&self, node_id: NodeId, operation: &str) -> Result<()> {
        self.raft
            .wait(Some(std::time::Duration::from_secs(5)))
            .metrics(
                |metrics| {
                    let membership_absent = !metrics
                        .membership_config
                        .membership()
                        .nodes()
                        .any(|(id, _)| *id == node_id);
                    let replication_absent = metrics
                        .replication
                        .as_ref()
                        .is_none_or(|replication| !replication.contains_key(&node_id));
                    membership_absent && replication_absent
                },
                operation,
            )
            .await
            .map(|_| ())
            .map_err(|error| anyhow::anyhow!("{operation}: {error}"))
    }

    fn target_replication_match(
        &self,
        node_id: NodeId,
    ) -> Option<klights_leader_api::RaftStorageLogAttestation> {
        self.raft
            .metrics()
            .borrow()
            .replication
            .as_ref()
            .and_then(|replication| replication.get(&node_id))
            .and_then(|matched| matched.as_ref())
            .map(|matched| klights_leader_api::RaftStorageLogAttestation {
                term: matched.leader_id.term,
                leader_node_id: matched.leader_id.node_id,
                index: matched.index,
            })
    }

    async fn wait_for_target_replication_match_with_timeout(
        &self,
        node_id: NodeId,
        timeout: std::time::Duration,
    ) -> Result<klights_leader_api::RaftStorageLogAttestation> {
        let metrics = self
            .raft
            .wait(Some(timeout))
            .metrics(
                |metrics| {
                    metrics
                        .replication
                        .as_ref()
                        .and_then(|replication| replication.get(&node_id))
                        .and_then(|matched| matched.as_ref())
                        .is_some()
                },
                format!("wait for target {node_id} replication match proof"),
            )
            .await
            .map_err(|error| {
                anyhow::anyhow!("wait for target {node_id} replication match proof: {error}")
            })?;
        let matched = metrics
            .replication
            .as_ref()
            .and_then(|replication| replication.get(&node_id))
            .and_then(|matched| matched.as_ref())
            .ok_or_else(|| anyhow::anyhow!("target {node_id} match proof disappeared"))?;
        Ok(klights_leader_api::RaftStorageLogAttestation {
            term: matched.leader_id.term,
            leader_node_id: matched.leader_id.node_id,
            index: matched.index,
        })
    }

    fn attestation_is_behind(
        reported: Option<&klights_leader_api::RaftStorageLogAttestation>,
        required: Option<&klights_leader_api::RaftStorageLogAttestation>,
    ) -> bool {
        match (reported, required) {
            (_, None) => false,
            (None, Some(_)) => true,
            (Some(reported), Some(required)) if reported.index < required.index => true,
            (Some(reported), Some(required)) if reported.index == required.index => {
                reported.term != required.term || reported.leader_node_id != required.leader_node_id
            }
            (Some(_), Some(_)) => false,
        }
    }

    pub async fn admit_controlplane_member_with_limit(
        &self,
        node_id: NodeId,
        addr: String,
        as_learner: bool,
        storage_incarnation: String,
        storage_log_attestation: klights_leader_api::RaftStorageAttestation,
        controlplane_limit: usize,
    ) -> Result<RaftMemberAdmissionResult> {
        self.admit_controlplane_member_with_limit_and_timeout(
            ControlplaneAdmissionRequest {
                node_id,
                addr,
                as_learner,
                storage_incarnation,
                storage_log_attestation,
                controlplane_limit,
            },
            CONTROLPLANE_REPLICATION_WAIT_TIMEOUT,
        )
        .await
    }

    /// Admit a control-plane member with an explicit bounded replication-proof
    /// wait. Production callers use [`Self::admit_controlplane_member_with_limit`]
    /// so the 20-second lossy-link budget remains a single policy constant;
    /// composition tests use this seam to inject an interrupted proof without
    /// waiting for the production timeout.
    pub async fn admit_controlplane_member_with_limit_and_timeout(
        &self,
        request: ControlplaneAdmissionRequest,
        replication_wait_timeout: std::time::Duration,
    ) -> Result<RaftMemberAdmissionResult> {
        let ControlplaneAdmissionRequest {
            node_id,
            addr,
            as_learner,
            storage_incarnation,
            storage_log_attestation,
            controlplane_limit,
        } = request;
        let _guard = self.membership_mutex.lock().await;
        if node_id == self.node_id {
            anyhow::bail!("control-plane Join node id {node_id} is this leader");
        }
        anyhow::ensure!(
            uuid::Uuid::parse_str(&storage_incarnation).is_ok(),
            "control-plane Join has invalid storage incarnation"
        );

        let previous = self.read_member_admission(node_id).await?;
        let current = self.raft.metrics().borrow().clone();
        let membership = current.membership_config.membership();
        let voters_now: BTreeSet<NodeId> = membership.voter_ids().collect();
        let is_voter = voters_now.contains(&node_id);
        let is_member = membership.nodes().any(|(id, _)| *id == node_id);
        if is_member && previous.is_none() {
            anyhow::bail!(
                "existing Raft member {node_id} has no proven v3 storage admission marker; refusing unsafe baseline migration—recreate this member or cluster"
            );
        }
        let incarnation_matches = previous.as_ref().is_some_and(|admitted| {
            admitted.storage_incarnation == storage_incarnation && admitted.addr == addr
        });
        let admission_complete = Self::admission_marker_is_complete(previous.as_ref());
        let behind_admitted = previous.as_ref().is_some_and(|admitted| {
            Self::attestation_is_behind(
                storage_log_attestation.high_watermark.as_ref(),
                admitted.proven_log.as_ref(),
            )
        });
        let live_match = self.target_replication_match(node_id);
        let behind_live = Self::attestation_is_behind(
            storage_log_attestation.current_boundary.as_ref(),
            live_match.as_ref(),
        );
        let requested_role_matches = is_member && (is_voter != as_learner);
        if incarnation_matches
            && admission_complete
            && !behind_admitted
            && !behind_live
            && requested_role_matches
        {
            return Ok(RaftMemberAdmissionResult::Unchanged);
        }

        let session_changed = is_member && (!incarnation_matches || behind_admitted || behind_live);
        if !is_voter && !as_learner && voters_now.len() >= controlplane_limit {
            anyhow::bail!(
                "cluster already at controlplane limit ({controlplane_limit}); refusing to promote voter {node_id}"
            );
        }

        let mut voters_after = voters_now.clone();
        if session_changed && is_voter {
            if voters_now.len() <= 2 {
                anyhow::bail!(
                    "cannot replace wiped voter {node_id} in a {}-voter cluster: the surviving membership cannot commit the required joint-consensus removal; restore the old node.db or recover quorum",
                    voters_now.len()
                );
            }
            voters_after.remove(&node_id);
            self.raft
                .change_membership(ChangeMembers::RemoveVoters(BTreeSet::from([node_id])), true)
                .await
                .map_err(|error| {
                    anyhow::anyhow!(
                        "Raft::change_membership(demote replaced voter {node_id}): {error}"
                    )
                })?;
            self.wait_for_uniform_membership("wait for replaced voter demotion")
                .await?;
            self.raft
                .change_membership(ChangeMembers::RemoveNodes(BTreeSet::from([node_id])), true)
                .await
                .map_err(|error| {
                    anyhow::anyhow!(
                        "Raft::change_membership(reset voter {node_id} session): {error}"
                    )
                })?;
            self.wait_for_member_absent(node_id, "wait for replaced voter session removal")
                .await?;
        } else if session_changed {
            self.raft
                .change_membership(ChangeMembers::RemoveNodes(BTreeSet::from([node_id])), true)
                .await
                .map_err(|error| {
                    anyhow::anyhow!(
                        "Raft::change_membership(reset learner {node_id} session): {error}"
                    )
                })?;
            self.wait_for_member_absent(node_id, "wait for replaced learner session removal")
                .await?;
        } else if is_voter && as_learner {
            if voters_now.len() <= 1 {
                anyhow::bail!("refusing to demote last voter {node_id}");
            }
            voters_after.remove(&node_id);
            self.raft
                .change_membership(voters_after.clone(), true)
                .await
                .map_err(|error| {
                    anyhow::anyhow!("Raft::change_membership(demote {node_id}): {error}")
                })?;
        }

        let needs_learner_add = !is_member || session_changed;
        let needs_catchup = needs_learner_add
            || (!is_voter && !as_learner)
            || (previous.is_some() && !admission_complete);
        if needs_learner_add {
            // Record the exact-v3 admission intent before changing Raft
            // membership.  If the first replication proof times out or the
            // response is lost, a retry can safely resume this same session
            // instead of mistaking its learner entry for legacy membership.
            self.persist_member_admission(
                node_id,
                &RaftMemberAdmission {
                    storage_incarnation: storage_incarnation.clone(),
                    addr: addr.clone(),
                    as_learner,
                    proven_log: None,
                },
            )
            .await?;
        }
        if needs_learner_add {
            self.raft
                .add_learner(
                    node_id,
                    RaftMemberNode::new(addr.clone(), storage_incarnation.clone(), None),
                    true,
                )
                .await
                .map_err(|error| anyhow::anyhow!("Raft::add_learner({node_id}): {error}"))?;
        }
        let caught_up_match = if needs_catchup {
            Some(
                self.wait_for_target_replication_match_with_timeout(
                    node_id,
                    replication_wait_timeout,
                )
                .await?,
            )
        } else {
            None
        };
        if let Some(proven) = caught_up_match.as_ref() {
            let receiver = RaftMemberNode::new(
                addr.clone(),
                storage_incarnation.clone(),
                Some(RaftMemberLogId {
                    term: proven.term,
                    leader_node_id: proven.leader_node_id,
                    index: proven.index,
                }),
            );
            self.raft
                .change_membership(
                    ChangeMembers::SetNodes(BTreeMap::from([(node_id, receiver)])),
                    true,
                )
                .await
                .map_err(|error| {
                    anyhow::anyhow!(
                        "Raft::change_membership(bind receiver proof for {node_id}): {error}"
                    )
                })?;
        }
        let proven_log = if needs_catchup {
            caught_up_match.or(storage_log_attestation.high_watermark)
        } else {
            previous
                .as_ref()
                .and_then(|admitted| admitted.proven_log.clone())
                .or(storage_log_attestation.high_watermark)
        };
        self.persist_member_admission(
            node_id,
            &RaftMemberAdmission {
                storage_incarnation,
                addr,
                as_learner,
                proven_log,
            },
        )
        .await?;
        if !as_learner && (!is_voter || session_changed) {
            voters_after.insert(node_id);
            self.raft
                .change_membership(voters_after, true)
                .await
                .map_err(|error| {
                    anyhow::anyhow!("Raft::change_membership(promote {node_id}): {error}")
                })?;
        }
        Ok(RaftMemberAdmissionResult::Changed)
    }

    pub async fn remove_voter(&self, node_id: NodeId) -> Result<()> {
        let _guard = self.membership_mutex.lock().await;
        let current = self.raft.metrics().borrow().clone();
        let voters_now: BTreeSet<NodeId> =
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
        let mut new_voters = voters_now;
        new_voters.remove(&node_id);
        self.raft
            .change_membership(new_voters, false)
            .await
            .map_err(|error| {
                anyhow::anyhow!("Raft::change_membership(remove {node_id}): {error}")
            })?;
        Ok(())
    }

    pub fn is_leader(&self) -> bool {
        self.raft.metrics().borrow().current_leader == Some(self.node_id)
    }

    pub fn current_shape(&self) -> RaftShape {
        let metrics = self.raft.metrics().borrow().clone();
        let voters: BTreeSet<NodeId> = metrics.membership_config.membership().voter_ids().collect();
        let in_membership = metrics
            .membership_config
            .membership()
            .nodes()
            .any(|(id, _)| *id == self.node_id);
        RaftShape {
            voter_count: voters.len() as u32,
            is_leader: metrics.current_leader == Some(self.node_id),
            is_learner: in_membership && !voters.contains(&self.node_id),
        }
    }

    pub fn current_leader_info(&self) -> Option<(NodeId, String)> {
        let metrics = self.raft.metrics().borrow().clone();
        let leader_id = metrics.current_leader?;
        let addr = metrics
            .membership_config
            .nodes()
            .find(|(id, _)| **id == leader_id)
            .map(|(_, node)| node.addr.clone())?;
        Some((leader_id, addr))
    }

    pub fn metrics_watch(
        &self,
    ) -> tokio::sync::watch::Receiver<openraft::RaftMetrics<NodeId, RaftMemberNode>> {
        self.raft.metrics()
    }

    pub fn server_metrics_watch(
        &self,
    ) -> tokio::sync::watch::Receiver<openraft::metrics::RaftServerMetrics<NodeId, RaftMemberNode>>
    {
        self.raft.server_metrics()
    }

    pub fn is_controlplane_member(&self, node_name: &str) -> bool {
        let target = raft_node_id_for_node_name(node_name);
        self.raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .nodes()
            .any(|(id, _)| *id == target)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    struct FeatureProbe {
        replies: BTreeMap<NodeId, Result<u32>>,
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
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
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
    async fn codec_v3_preflight_member_probe_requires_every_member() {
        let members = vec![
            (1, "https://node-1".to_string()),
            (2, "https://node-2".to_string()),
        ];
        let all_capable = FeatureProbe {
            replies: [
                (1, Ok(klights_cluster_core::COMMAND_CODEC_VERSION)),
                (2, Ok(klights_cluster_core::COMMAND_CODEC_VERSION)),
            ]
            .into_iter()
            .collect(),
        };
        verify_command_codec_v3_members(members.clone(), &all_capable)
            .await
            .expect("all current members support exact codec v3");

        let missing = FeatureProbe {
            replies: [
                (1, Ok(klights_cluster_core::COMMAND_CODEC_VERSION)),
                (2, Ok(2)),
            ]
            .into_iter()
            .collect(),
        };
        assert!(matches!(
            verify_command_codec_v3_members(members.clone(), &missing).await,
            Err(CommandCodecV3PreflightError::Unsupported { node_id: 2 })
        ));

        let unavailable = FeatureProbe {
            replies: [
                (1, Ok(klights_cluster_core::COMMAND_CODEC_VERSION)),
                (2, Err(anyhow::anyhow!("transport unavailable"))),
            ]
            .into_iter()
            .collect(),
        };
        assert!(matches!(
            verify_command_codec_v3_members(members, &unavailable).await,
            Err(CommandCodecV3PreflightError::Unavailable { node_id: 2, .. })
        ));
    }

    #[tokio::test]
    async fn startup_codec_probe_rejects_any_voter_or_learner_without_v3() {
        let members = vec![
            (1, "https://voter".to_string()),
            (2, "https://learner".to_string()),
        ];
        let compatible = FeatureProbe {
            replies: [
                (1, Ok(klights_cluster_core::COMMAND_CODEC_VERSION)),
                (2, Ok(klights_cluster_core::COMMAND_CODEC_VERSION)),
            ]
            .into_iter()
            .collect(),
        };
        verify_command_codec_v3_members(members.clone(), &compatible)
            .await
            .expect("all restored members advertise codec v3");

        let old_learner = FeatureProbe {
            replies: [
                (1, Ok(klights_cluster_core::COMMAND_CODEC_VERSION)),
                (2, Ok(2)),
            ]
            .into_iter()
            .collect(),
        };
        assert!(matches!(
            verify_command_codec_v3_members(members, &old_learner).await,
            Err(CommandCodecV3PreflightError::Unsupported { node_id: 2 })
        ));
    }

    #[test]
    fn current_boundary_detects_truncation_even_when_highwater_stays_proven() {
        let admitted = klights_leader_api::RaftStorageLogAttestation {
            term: 3,
            leader_node_id: 80,
            index: 100,
        };
        let monotonic = admitted.clone();
        let truncated_boundary = klights_leader_api::RaftStorageLogAttestation {
            term: 3,
            leader_node_id: 80,
            index: 50,
        };

        assert!(!EmbeddedRaftMembership::attestation_is_behind(
            Some(&monotonic),
            Some(&admitted),
        ));
        assert!(EmbeddedRaftMembership::attestation_is_behind(
            Some(&truncated_boundary),
            Some(&admitted),
        ));
    }

    #[test]
    fn provisional_admission_marker_is_not_treated_as_complete() {
        let pending = RaftMemberAdmission {
            storage_incarnation: uuid::Uuid::new_v4().to_string(),
            addr: "https://pending.example:7679".to_string(),
            as_learner: false,
            proven_log: None,
        };
        let proven = RaftMemberAdmission {
            proven_log: Some(klights_leader_api::RaftStorageLogAttestation {
                term: 1,
                leader_node_id: 1,
                index: 10,
            }),
            ..pending.clone()
        };

        assert!(!EmbeddedRaftMembership::admission_marker_is_complete(Some(
            &pending,
        )));
        assert!(EmbeddedRaftMembership::admission_marker_is_complete(Some(
            &proven,
        )));
        assert!(!EmbeddedRaftMembership::admission_marker_is_complete(None));
    }

    #[test]
    fn lossy_join_wait_budget_exceeds_original_single_retry_window() {
        assert!(
            CONTROLPLANE_REPLICATION_WAIT_TIMEOUT > std::time::Duration::from_secs(5),
            "200 ms RTT with packet loss needs more than the old five-second proof window"
        );
        assert!(
            CONTROLPLANE_REPLICATION_WAIT_TIMEOUT
                < klights_leader_api::CONTROLPLANE_JOIN_RPC_DEADLINE,
            "the server proof wait must finish before the join RPC deadline"
        );
    }

    #[test]
    fn controlplane_join_retry_delay_is_linear_and_capped() {
        let cases = [
            (1, 5),
            (2, 10),
            (3, 15),
            (4, 20),
            (5, 25),
            (6, 30),
            (7, 35),
            (8, 40),
            (9, 45),
            (10, 50),
            (11, 55),
            (12, 60),
            (13, 60),
        ];

        for (attempt, expected_secs) in cases {
            assert_eq!(
                controlplane_join_retry_delay(attempt),
                std::time::Duration::from_secs(expected_secs),
                "attempt {attempt} should back off for {expected_secs}s"
            );
        }
    }
}
