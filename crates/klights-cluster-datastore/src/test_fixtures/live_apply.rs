#![cfg(test)]

use async_trait::async_trait;
use klights_cluster_core::{
    CommandMeta, CommittedApplyOutcome, CommittedApplyRejection, LogApplyCommit, LogApplyMutation,
    StorageCommand, StorageCommandRejectionCode,
};
use klights_cluster_store::{AppliedMutation, StorageCommandResult};

#[async_trait]
pub trait DatastoreApplier: Send + Sync {
    async fn apply_command(&self, command: StorageCommand, meta: CommandMeta)
    -> anyhow::Result<()>;
}

/// Test-only projection from the canonical committed receipt to the legacy
/// response DTO asserted by the SQLite characterization suite.
#[async_trait]
pub(crate) trait TestCommittedApplyResult: Send + Sync {
    async fn apply_raft_log_apply_commit(
        &self,
        commit: LogApplyCommit,
    ) -> anyhow::Result<StorageCommandResult>;
}

#[async_trait]
impl TestCommittedApplyResult for crate::sqlite::embedded::Datastore {
    async fn apply_raft_log_apply_commit(
        &self,
        commit: LogApplyCommit,
    ) -> anyhow::Result<StorageCommandResult> {
        let receipt = self.apply_raft_log_apply_commit_receipt(commit).await?;
        let applied_mutation = receipt
            .applied_resource()
            .cloned()
            .map(AppliedMutation::Resource);
        Ok(match receipt.outcome() {
            CommittedApplyOutcome::Visible {
                resource_version, ..
            } => StorageCommandResult::new(
                Some(*resource_version),
                None,
                None,
                true,
                applied_mutation,
                receipt.pod_endpoint_effect(),
            ),
            CommittedApplyOutcome::NoPublicChange {
                resource_version, ..
            } => StorageCommandResult::new(
                Some(*resource_version),
                None,
                None,
                false,
                applied_mutation,
                receipt.pod_endpoint_effect(),
            ),
            CommittedApplyOutcome::Rejected(rejection) => {
                let rejection_code = match rejection {
                    CommittedApplyRejection::AlreadyExists { .. } => {
                        StorageCommandRejectionCode::AlreadyExists
                    }
                    CommittedApplyRejection::NotFound { .. } => {
                        StorageCommandRejectionCode::NotFound
                    }
                    CommittedApplyRejection::UidConflict { .. }
                    | CommittedApplyRejection::ResourceVersionConflict { .. } => {
                        StorageCommandRejectionCode::Conflict
                    }
                    CommittedApplyRejection::InvalidCommit { .. } => {
                        StorageCommandRejectionCode::InvalidCommit
                    }
                    _ => StorageCommandRejectionCode::InvalidCommit,
                };
                StorageCommandResult::new(
                    None,
                    Some(rejection.message().to_string()),
                    Some(rejection_code),
                    false,
                    None,
                    receipt.pod_endpoint_effect(),
                )
            }
            _ => StorageCommandResult::new(
                None,
                Some("unsupported canonical committed-apply outcome".to_string()),
                Some(StorageCommandRejectionCode::InvalidCommit),
                false,
                None,
                receipt.pod_endpoint_effect(),
            ),
        })
    }
}

pub(crate) fn test_live_commit(
    candidate_resource_version: i64,
    mut mutations: Vec<LogApplyMutation>,
) -> LogApplyCommit {
    fn clear_nested_resource_version(data: &mut serde_json::Value) {
        if let Some(metadata) = data
            .get_mut("metadata")
            .and_then(serde_json::Value::as_object_mut)
        {
            metadata.remove("resourceVersion");
        }
    }

    for mutation in &mut mutations {
        match mutation {
            LogApplyMutation::PutResource(row) => {
                row.resource_version = 0;
                clear_nested_resource_version(&mut row.data);
            }
            LogApplyMutation::PatchResourceLatest(row) => {
                row.resource_version = 0;
                clear_nested_resource_version(&mut row.patch);
            }
            LogApplyMutation::PutNamespace(row) => {
                row.resource_version = 0;
                clear_nested_resource_version(&mut row.data);
            }
            LogApplyMutation::PutWatchEvent(row) => {
                row.resource_version = 0;
                clear_nested_resource_version(&mut row.data);
                if let Some(object) = row.data.get_mut("object") {
                    clear_nested_resource_version(object);
                }
            }
            LogApplyMutation::PutPodCleanupIntent(row) => row.resource_version = 0,
            LogApplyMutation::PutAppliedOutbox(row) => row.applied_rv = None,
            LogApplyMutation::AdvanceResourceVersion { resource_version } => {
                *resource_version = 0;
            }
            _ => {}
        }
    }
    let _ = candidate_resource_version;
    LogApplyCommit::try_new(mutations).expect("test live commit must be an RV-zero template")
}
