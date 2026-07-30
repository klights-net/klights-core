//! Focused embedded-Raft commit materialization port.

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::{
    BuildOutboxOutcome, LogApplyCommit, OutboxApplyError, OutboxStreamWatermark, StorageCommand,
    StorageMutationError,
};

#[async_trait]
pub trait RaftCommitMaterializer: Send + Sync {
    async fn read_raft_metadata(&self, key: &str) -> Result<Option<String>, StorageMutationError>;

    async fn build_command(
        &self,
        command: StorageCommand,
        operation: &str,
        authoring_node: &str,
    ) -> Result<LogApplyCommit, StorageMutationError>;

    async fn build_outbox(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
        watermark: Option<OutboxStreamWatermark>,
    ) -> Result<BuildOutboxOutcome, OutboxApplyError>;
}
