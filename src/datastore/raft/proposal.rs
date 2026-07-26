use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::{
    OutboxApplyError, OutboxApplyOutcome, OutboxStreamWatermark, PodEndpointEffect, Resource,
    ResourceMutationEffect, StorageCommand,
};

pub(crate) struct RaftProposalEffect {
    result: OutboxApplyOutcome,
    resource_effect: ResourceMutationEffect,
    pod_endpoint_effect: PodEndpointEffect,
    committed_resource: Option<Resource>,
}

impl RaftProposalEffect {
    pub(crate) const fn new(
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

    pub(crate) fn with_committed_resource(mut self, resource: Option<Resource>) -> Self {
        self.committed_resource = resource;
        self
    }

    pub(crate) fn into_parts(
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

/// Immutable replication-private proposal capability consumed by the root
/// sequencing compatibility adapter.
#[async_trait]
pub(crate) trait RaftProposal: Send + Sync {
    async fn propose_command(
        &self,
        command: StorageCommand,
    ) -> Result<super::types::StorageCommandResult>;

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
