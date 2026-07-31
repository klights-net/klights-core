//! Embedded state-machine delivery of the privileged committed-apply capability.

use std::sync::Arc;

use klights_cluster_core::{CommittedApplyOutcome, LogApplyCommit, LogApplyMutation};
use klights_cluster_store::{
    CommittedApplyError, CommittedRaftApplyReceipt, CommittedRaftApplyRequest,
    PrivilegedCommittedRaftApply,
};

use crate::state_machine::RaftCommittedApply;
use klights_cluster_store::{AppliedMutation, StorageCommandResult};

/// Embedded OpenRaft decorator that maps the canonical persistence receipt
/// into its Raft response and publishes active post-commit wakeups.
pub struct ObservedCommittedRaftApply {
    persistence: Arc<dyn PrivilegedCommittedRaftApply>,
    wakeups: Arc<dyn klights_leader_api::PostCommitWakeup>,
}

impl ObservedCommittedRaftApply {
    pub fn new(
        persistence: Arc<dyn PrivilegedCommittedRaftApply>,
        wakeups: Arc<dyn klights_leader_api::PostCommitWakeup>,
    ) -> Self {
        Self {
            persistence,
            wakeups,
        }
    }

    fn publish_visible_commit(&self, commit: &LogApplyCommit, resource_version: i64) {
        let advances = post_commit_advances(commit, resource_version);
        self.wakeups.wake(&advances);
        for mutation in commit.mutations() {
            if let LogApplyMutation::DeleteNamespaceContents { name } = mutation {
                self.wakeups.wake_namespace_contents(name, resource_version);
            }
        }
    }
}

#[async_trait::async_trait]
impl RaftCommittedApply for ObservedCommittedRaftApply {
    async fn apply_committed(
        &self,
        request: CommittedRaftApplyRequest,
    ) -> Result<StorageCommandResult, CommittedApplyError> {
        let commit = request.commit().clone();
        let receipt = self.persistence.apply_committed_raft(request).await?;
        if let CommittedApplyOutcome::Visible {
            resource_version, ..
        } = receipt.outcome()
        {
            self.publish_visible_commit(&commit, *resource_version);
        }
        Ok(storage_command_result_from_committed_outcome(&receipt))
    }
}

pub fn storage_command_result_from_committed_outcome(
    receipt: &CommittedRaftApplyReceipt,
) -> StorageCommandResult {
    let applied_mutation = receipt
        .applied_resource()
        .cloned()
        .map(AppliedMutation::Resource);
    match receipt.outcome() {
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
            use klights_cluster_core::{CommittedApplyRejection, StorageCommandRejectionCode};
            let rejection_code = match rejection {
                CommittedApplyRejection::AlreadyExists { .. } => {
                    StorageCommandRejectionCode::AlreadyExists
                }
                CommittedApplyRejection::NotFound { .. } => StorageCommandRejectionCode::NotFound,
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
            Some(klights_cluster_core::StorageCommandRejectionCode::InvalidCommit),
            false,
            None,
            receipt.pod_endpoint_effect(),
        ),
    }
}

fn post_commit_advances(
    commit: &LogApplyCommit,
    resource_version: i64,
) -> Vec<klights_leader_api::PostCommitAdvance> {
    commit
        .mutations()
        .iter()
        .filter_map(|mutation| {
            let (api_version, kind, namespace) = match mutation {
                LogApplyMutation::PutResource(row) => {
                    (&row.api_version, &row.kind, row.namespace.clone())
                }
                LogApplyMutation::PatchResourceLatest(row) => {
                    (&row.api_version, &row.kind, row.namespace.clone())
                }
                LogApplyMutation::DeleteResource(row) => {
                    (&row.api_version, &row.kind, row.namespace.clone())
                }
                LogApplyMutation::FinalizeBoundPod(row) => {
                    return Some(klights_leader_api::PostCommitAdvance::new(
                        "v1",
                        "Pod",
                        Some(row.namespace.clone()),
                        resource_version,
                    ));
                }
                LogApplyMutation::PutNamespace(_) | LogApplyMutation::DeleteNamespace { .. } => {
                    return Some(klights_leader_api::PostCommitAdvance::new(
                        "v1",
                        "Namespace",
                        None,
                        resource_version,
                    ));
                }
                LogApplyMutation::PutWatchEvent(row) => {
                    (&row.api_version, &row.kind, row.namespace.clone())
                }
                _ => return None,
            };
            Some(klights_leader_api::PostCommitAdvance::new(
                api_version,
                kind,
                namespace,
                resource_version,
            ))
        })
        .collect()
}
