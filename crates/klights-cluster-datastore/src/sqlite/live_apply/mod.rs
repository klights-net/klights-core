//! Corrected Phase 10C.2 SQLite live committed-apply packet.
//!
//! This module owns the complete transaction coordinator and every mutation
//! variant it dispatches. Root datastore facades and post-commit composition
//! remain outside this packet.

mod context;
mod coordinator;
mod state;
pub mod status;

use std::sync::Arc;

use klights_cluster_store::{
    CommittedApplyError, CommittedApplyFuture, CommittedRaftApplyReceipt,
    CommittedRaftApplyRequest, OutboxResponseCodec, PrivilegedCommittedRaftApply, StagedPostCommit,
};
use klights_supervisor::DbExecutor;

pub use context::TransactionContext;
#[cfg(any(test, feature = "test-support"))]
pub use coordinator::is_terminal_apply_conflict;
pub use coordinator::{
    ApplyConflictCode, RaftLogApplyOutcome, apply_commit_in_tx_for_raft_with_context,
    apply_commit_in_tx_returning_rv_and_mutation_with_context, apply_commit_in_tx_with_context,
    apply_conflict_error, apply_snapshot_restore_operation_in_tx, other_error,
};
#[cfg(any(test, feature = "test-support"))]
pub use state::watch_history::watch_events_min_scope_rows_for_scope_count;
pub use state::watch_history::{gc_watch_events_in_tx, watch_events_min_scope_rows_in_conn};

/// SQLite owner of the indivisible live committed-apply transaction.
#[derive(Clone)]
pub struct SqliteLiveCommittedApplyStore {
    executor: DbExecutor,
    outbox_codec: Arc<dyn OutboxResponseCodec>,
}

impl SqliteLiveCommittedApplyStore {
    pub fn new(executor: DbExecutor, outbox_codec: Arc<dyn OutboxResponseCodec>) -> Self {
        Self {
            executor,
            outbox_codec,
        }
    }

    pub async fn apply_log_apply_commit(
        &self,
        commit: klights_cluster_core::LogApplyCommit,
    ) -> anyhow::Result<Vec<StagedPostCommit>> {
        let codec = self.outbox_codec.clone();
        self.executor
            .call_raw("apply_log_apply_commit", move |connection| {
                let context = TransactionContext::new(codec.as_ref());
                let transaction = connection.transaction()?;
                let pending = apply_commit_in_tx_with_context(&transaction, commit, &context)?;
                transaction.commit()?;
                Ok(pending)
            })
            .await
            .map_err(|error| anyhow::anyhow!("failed to apply log_apply commit: {error}"))
    }

    pub async fn apply_committed_with_pending(
        &self,
        commit: klights_cluster_core::LogApplyCommit,
    ) -> anyhow::Result<(CommittedRaftApplyReceipt, Vec<StagedPostCommit>)> {
        let codec = self.outbox_codec.clone();
        self.executor
            .call_raw("apply_raft_log_apply_commit", move |connection| {
                let context = TransactionContext::new(codec.as_ref());
                let transaction = connection.transaction()?;
                let outcome =
                    apply_commit_in_tx_for_raft_with_context(&transaction, commit, &context)?;
                transaction.commit()?;
                let receipt = CommittedRaftApplyReceipt::new(
                    outcome.committed_outcome,
                    outcome.pod_endpoint_effect,
                )
                .with_returned_resource(outcome.returned_resource);
                Ok((receipt, outcome.pending))
            })
            .await
            .map_err(|error| anyhow::anyhow!("failed to apply raft log_apply commit: {error}"))
    }
}

impl PrivilegedCommittedRaftApply for SqliteLiveCommittedApplyStore {
    fn apply_committed_raft(
        &self,
        request: CommittedRaftApplyRequest,
    ) -> CommittedApplyFuture<'_, CommittedRaftApplyReceipt> {
        Box::pin(async move {
            self.apply_committed_with_pending(request.into_commit())
                .await
                .map(|(receipt, _)| receipt)
                .map_err(map_committed_apply_error)
        })
    }
}

pub(crate) fn map_committed_apply_error(error: anyhow::Error) -> CommittedApplyError {
    let message = format!("{error:#}");
    let lower = message.to_ascii_lowercase();
    if lower.contains("unsupported")
        || lower.contains("does not support")
        || lower.contains("does not implement")
    {
        CommittedApplyError::UnsupportedMode { message }
    } else if lower.contains("corrupt")
        || lower.contains("decode")
        || lower.contains("invalid data")
    {
        CommittedApplyError::CorruptData { message }
    } else if lower.contains("cancel") {
        CommittedApplyError::Cancelled
    } else if lower.contains("timeout") || lower.contains("timed out") {
        CommittedApplyError::Timeout
    } else if lower.contains("busy") || lower.contains("locked") {
        CommittedApplyError::Retryable { message }
    } else {
        CommittedApplyError::persistence_failed(message)
    }
}

// Temporary root-local dependency aliases. Each names a lower packet that
// moves before 10C.2; no Raft, replication, leader API, or broad datastore
// owner is reachable through this module.
use super::scope::use_namespaced_table;
use super::{
    mutation_helpers, mutation_queries as queries, owner_ref_index, resource_shape,
    transaction_primitives,
};

fn create_staged_post_commit(
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    resource_version: i64,
    event_type: &str,
    data: impl Into<std::sync::Arc<serde_json::Value>>,
) -> klights_cluster_store::StagedPostCommit {
    let staged = klights_cluster_store::StagedPostCommit::new(
        api_version,
        kind,
        namespace,
        resource_version,
    );
    #[cfg(not(any(test, feature = "test-support")))]
    {
        let _ = (name, event_type, data);
        staged
    }
    #[cfg(any(test, feature = "test-support"))]
    {
        use bytes::Bytes;
        use serde::Serialize;

        #[derive(Serialize)]
        struct TestWatchEnvelope<'a> {
            #[serde(rename = "type")]
            event_type: &'a str,
            object: &'a serde_json::Value,
        }

        let data = hydrate_staged_test_resource(
            std::sync::Arc::unwrap_or_clone(data.into()),
            api_version,
            kind,
            namespace,
            name,
            resource_version,
        );
        let is_envelope = data
            .get("type")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| matches!(value, "ADDED" | "MODIFIED" | "DELETED" | "ERROR"))
            && data.get("object").is_some();
        let resource_data = if is_envelope {
            data.get("object")
                .expect("checked staged watch envelope object")
                .clone()
        } else {
            data.clone()
        };
        let resource =
            klights_cluster_core::Resource::try_from_data(std::sync::Arc::new(resource_data))
                .expect("staged test resource has canonical identity");
        let encoded_json = if is_envelope {
            serde_json::to_vec(&data)
        } else {
            serde_json::to_vec(&TestWatchEnvelope {
                event_type,
                object: resource.data.as_ref(),
            })
        }
        .ok()
        .map(Bytes::from);
        staged.with_test_event(event_type, resource, encoded_json)
    }
}

#[cfg(any(test, feature = "test-support"))]
fn hydrate_staged_test_resource(
    mut data: serde_json::Value,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    resource_version: i64,
) -> serde_json::Value {
    if data
        .get("type")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|event_type| matches!(event_type, "ADDED" | "MODIFIED" | "DELETED" | "ERROR"))
        && let Some(object) = data.get_mut("object")
    {
        *object = hydrate_staged_test_resource(
            std::mem::take(object),
            api_version,
            kind,
            namespace,
            name,
            resource_version,
        );
        return data;
    }
    resource_shape::hydrate_watch_event_data(
        data,
        api_version,
        kind,
        namespace,
        name,
        resource_version,
    )
}
