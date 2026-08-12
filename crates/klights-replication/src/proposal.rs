//! Embedded command and durable-outbox proposal orchestration.

use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use klights_cluster_core::{
    BuildOutboxOutcome, NodeId, OutboxApplyError, OutboxApplyOutcome, OutboxOperation,
    OutboxStreamWatermark, PodEndpointEffect, Resource, ResourceMutationEffect, StorageCommand,
    StorageCommandRejectionCode, StorageMutationError,
};
use klights_cluster_store::{AppliedMutation, StorageCommandResult};
use openraft::Raft;
use openraft::error::{ClientWriteError, RaftError};

use crate::activation::CommandCodecV3Activation;
use crate::flow_control::RaftCommitFlowControl;
use crate::log_apply_wire;
use crate::materializer::RaftCommitMaterializer;
use crate::types::{StorageCommandPayload, TypeConfig};

pub const RAFT_MAX_INFLIGHT_PROPOSALS: usize = 32;

pub struct RaftProposalEffect {
    result: OutboxApplyOutcome,
    resource_effect: ResourceMutationEffect,
    pod_endpoint_effect: PodEndpointEffect,
    committed_resource: Option<Resource>,
}

impl RaftProposalEffect {
    pub const fn new(
        result: OutboxApplyOutcome,
        resource_effect: ResourceMutationEffect,
        pod_endpoint_effect: PodEndpointEffect,
    ) -> Self {
        Self {
            result,
            resource_effect,
            pod_endpoint_effect,
            committed_resource: None,
        }
    }

    pub fn with_committed_resource(mut self, resource: Option<Resource>) -> Self {
        self.committed_resource = resource;
        self
    }

    pub fn into_parts(
        self,
    ) -> (
        OutboxApplyOutcome,
        ResourceMutationEffect,
        PodEndpointEffect,
        Option<Resource>,
    ) {
        (
            self.result,
            self.resource_effect,
            self.pod_endpoint_effect,
            self.committed_resource,
        )
    }
}

#[async_trait]
pub trait RaftProposal: Send + Sync {
    async fn propose_command(&self, command: StorageCommand) -> Result<StorageCommandResult>;

    async fn propose_outbox_command(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
        watermark: Option<OutboxStreamWatermark>,
    ) -> std::result::Result<OutboxApplyOutcome, OutboxApplyError>;

    async fn propose_outbox_command_effect(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
        watermark: Option<OutboxStreamWatermark>,
    ) -> std::result::Result<RaftProposalEffect, OutboxApplyError> {
        let result = self
            .propose_outbox_command(
                idempotency_key,
                operation,
                command,
                authoring_node,
                watermark,
            )
            .await?;
        let resource_effect = if matches!(result, OutboxApplyOutcome::Applied { .. }) {
            ResourceMutationEffect::Changed
        } else {
            ResourceMutationEffect::Unchanged
        };
        Ok(RaftProposalEffect::new(
            result,
            resource_effect,
            PodEndpointEffect::NotApplicable,
        ))
    }
}

/// Immutable proposal service bound to one embedded OpenRaft instance.
pub struct EmbeddedRaftProposal {
    node_id: NodeId,
    raft: Raft<TypeConfig>,
    materializer: Arc<dyn RaftCommitMaterializer>,
    authoring_node: String,
    flow_control: Arc<RaftCommitFlowControl>,
    command_codec_v3_activation: Arc<CommandCodecV3Activation>,
}

impl EmbeddedRaftProposal {
    pub fn new(
        node_id: NodeId,
        raft: Raft<TypeConfig>,
        materializer: Arc<dyn RaftCommitMaterializer>,
        authoring_node: String,
        flow_control: Arc<RaftCommitFlowControl>,
        command_codec_v3_activation: Arc<CommandCodecV3Activation>,
    ) -> Self {
        Self {
            node_id,
            raft,
            materializer,
            authoring_node,
            flow_control,
            command_codec_v3_activation,
        }
    }

    pub fn flow_control(&self) -> &Arc<RaftCommitFlowControl> {
        &self.flow_control
    }

    pub fn is_local_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        let voters: BTreeSet<NodeId> = metrics.membership_config.membership().voter_ids().collect();
        local_commit_materialization_allowed(self.node_id, metrics.current_leader, &voters)
    }

    pub async fn propose_command_result(
        &self,
        command: StorageCommand,
    ) -> Result<StorageCommandResult> {
        klights_leader_api::validate_authority_if_scoped().map_err(|error| {
            anyhow::anyhow!("leader authority changed before proposal: {error}")
        })?;
        self.command_codec_v3_activation
            .ensure_command_codec_v3_activated()?;
        self.ensure_local_leader_for_commit_materialization()?;
        let operation = derive_operation_label(&command);
        let _flow_permit = self.flow_control.acquire().await;
        let commit = self
            .materializer
            .build_command(command, operation.as_str(), &self.authoring_node)
            .await
            .map_err(map_commit_materialization_error)?;
        let entry_bytes = log_apply_wire::encode_commit_protobuf(&commit)
            .context("encode LogApplyCommit for raft propose")?;
        klights_leader_api::validate_authority_if_scoped().map_err(|error| {
            anyhow::anyhow!("leader authority changed before raft commit: {error}")
        })?;
        self.propose_materialized_commit(StorageCommandPayload::from_bytes(entry_bytes))
            .await
    }

    pub async fn propose_materialized_commit(
        &self,
        payload: StorageCommandPayload,
    ) -> Result<StorageCommandResult> {
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
            Err(error) => Err(anyhow::anyhow!("Raft::client_write: {error}")),
        }
    }

    fn ensure_local_leader_for_commit_materialization(&self) -> Result<()> {
        if self.is_local_leader() {
            return Ok(());
        }
        let metrics = self.raft.metrics().borrow().clone();
        let voters: BTreeSet<NodeId> = metrics.membership_config.membership().voter_ids().collect();
        anyhow::bail!(
            "not raft leader: refusing local commit materialization on node {} current_leader={:?} voters={voters:?}",
            self.node_id,
            metrics.current_leader,
        );
    }
}

#[async_trait]
impl RaftProposal for EmbeddedRaftProposal {
    async fn propose_command(&self, command: StorageCommand) -> Result<StorageCommandResult> {
        let result = self.propose_command_result(command).await?;
        if let Some(message) = result.error_message.clone() {
            let code = result
                .rejection_code
                .unwrap_or(StorageCommandRejectionCode::InvalidCommit);
            return Err(StorageMutationError::rejected(code, message).into());
        }
        Ok(result)
    }

    async fn propose_outbox_command(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
        watermark: Option<OutboxStreamWatermark>,
    ) -> std::result::Result<OutboxApplyOutcome, OutboxApplyError> {
        self.propose_outbox_command_effect(
            idempotency_key,
            operation,
            command,
            authoring_node,
            watermark,
        )
        .await
        .map(|effect| effect.into_parts().0)
    }

    async fn propose_outbox_command_effect(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
        watermark: Option<OutboxStreamWatermark>,
    ) -> std::result::Result<RaftProposalEffect, OutboxApplyError> {
        klights_leader_api::validate_authority_if_scoped()
            .map_err(|error| OutboxApplyError::Retryable(error.to_string()))?;
        self.command_codec_v3_activation
            .ensure_command_codec_v3_activated()
            .map_err(|error| OutboxApplyError::Retryable(error.to_string()))?;
        self.ensure_local_leader_for_commit_materialization()
            .map_err(|error| OutboxApplyError::Retryable(error.to_string()))?;

        let _flow_permit = if outbox_operation_uses_priority_permit(operation) {
            self.flow_control.try_acquire_priority().ok_or_else(|| {
                OutboxApplyError::Retryable(
                    "raft proposal flow control saturated; retry outbox later".to_string(),
                )
            })?
        } else if outbox_operation_waits_for_permit(operation) {
            self.flow_control.acquire().await
        } else {
            self.flow_control.try_acquire().ok_or_else(|| {
                OutboxApplyError::Retryable(
                    "raft proposal flow control saturated; retry outbox later".to_string(),
                )
            })?
        };

        let outcome = self
            .materializer
            .build_outbox(
                idempotency_key,
                operation,
                command,
                authoring_node,
                watermark,
            )
            .await
            .map_err(|error| match error {
                OutboxApplyError::Retryable(message) => OutboxApplyError::Retryable(format!(
                    "build log_apply commit for raft outbox propose: {message}"
                )),
                other => other,
            })?;
        let (commit, terminal_error) = match outcome {
            BuildOutboxOutcome::NeedsPropose {
                commit,
                terminal_error,
                ..
            } => (commit, terminal_error),
            BuildOutboxOutcome::LeaseRenewShortcircuit => {
                return Ok(RaftProposalEffect::new(
                    OutboxApplyOutcome::Applied { applied_rv: 0 },
                    ResourceMutationEffect::Unchanged,
                    PodEndpointEffect::NotApplicable,
                ));
            }
            BuildOutboxOutcome::AlreadyApplied {
                applied_rv,
                committed_resource,
            } => {
                return Ok(RaftProposalEffect::new(
                    OutboxApplyOutcome::AlreadyApplied { applied_rv },
                    ResourceMutationEffect::Unchanged,
                    PodEndpointEffect::Unchanged,
                )
                .with_committed_resource(committed_resource));
            }
        };
        klights_leader_api::validate_authority_if_scoped()
            .map_err(|error| OutboxApplyError::Retryable(error.to_string()))?;
        let entry_bytes = log_apply_wire::encode_commit_protobuf(&commit).map_err(|error| {
            OutboxApplyError::Retryable(format!(
                "encode LogApplyCommit for raft outbox propose: {error}"
            ))
        })?;
        let apply_result = self
            .propose_materialized_commit(StorageCommandPayload::from_bytes(entry_bytes))
            .await
            .map_err(|error| OutboxApplyError::Retryable(format!("raft propose: {error}")))?;
        let resource_effect = if apply_result.public_resource_changed {
            ResourceMutationEffect::Changed
        } else {
            ResourceMutationEffect::Unchanged
        };
        let pod_endpoint_effect = apply_result.pod_endpoint_effect();
        let committed_resource =
            apply_result
                .applied_mutation
                .as_ref()
                .map(|mutation| match mutation {
                    AppliedMutation::Resource(resource) => resource.clone(),
                });
        if let Some(message) = apply_result.error_message {
            return Err(OutboxApplyError::ConflictTerminal(message));
        }
        if let Some(error) = terminal_error {
            return Err(error);
        }
        Ok(RaftProposalEffect::new(
            OutboxApplyOutcome::Applied {
                applied_rv: apply_result.applied_rv.unwrap_or(0),
            },
            resource_effect,
            pod_endpoint_effect,
        )
        .with_committed_resource(committed_resource))
    }
}

pub fn local_commit_materialization_allowed(
    node_id: NodeId,
    current_leader: Option<NodeId>,
    voter_ids: &BTreeSet<NodeId>,
) -> bool {
    (current_leader == Some(node_id) && voter_ids.contains(&node_id))
        || (current_leader.is_none() && voter_ids.len() == 1 && voter_ids.contains(&node_id))
}

fn map_commit_materialization_error(error: StorageMutationError) -> anyhow::Error {
    let rejection_code = error.rejection_code();
    let diagnostic = format!(
        "build log_apply commit for raft propose: {}",
        error.message()
    );
    match rejection_code {
        Some(code) => StorageMutationError::rejected(code, diagnostic).into(),
        None => StorageMutationError::persistence(diagnostic).into(),
    }
}

pub fn derive_operation_label(command: &StorageCommand) -> OutboxOperation {
    match command {
        StorageCommand::UpdateStatus { kind, .. } if kind == "Node" => OutboxOperation::NodeStatus,
        StorageCommand::UpdateStatus { kind, .. } if kind == "Lease" => OutboxOperation::LeaseRenew,
        _ => OutboxOperation::PodStatus,
    }
}

pub fn outbox_operation_uses_priority_permit(operation: &str) -> bool {
    matches!(
        OutboxOperation::try_from(operation),
        Ok(OutboxOperation::NodeRegistration)
            | Ok(OutboxOperation::NodeDataplane)
            | Ok(OutboxOperation::NodeStatus)
    )
}

pub fn outbox_operation_waits_for_permit(operation: &str) -> bool {
    matches!(
        OutboxOperation::try_from(operation),
        Ok(OutboxOperation::PodStatus)
            | Ok(OutboxOperation::PodMetadata)
            | Ok(OutboxOperation::RuntimeReconcile)
            | Ok(OutboxOperation::ProbeReadiness)
            | Ok(OutboxOperation::DeadlineExceeded)
            | Ok(OutboxOperation::ContainerStatusSnapshot)
            | Ok(OutboxOperation::EphemeralContainerStatuses)
    )
}
