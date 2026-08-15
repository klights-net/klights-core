//! Root composition adapters from the concrete datastore to focused
//! cluster-store and replication ports.

pub(crate) struct DatastoreRaftCommitMaterializer {
    metadata: std::sync::Arc<dyn klights_cluster_store::ClusterMetadataMutation>,
    outbox: std::sync::Arc<dyn klights_cluster_store::AppliedOutboxLedger>,
}

impl DatastoreRaftCommitMaterializer {
    pub(crate) fn new(
        metadata: std::sync::Arc<dyn klights_cluster_store::ClusterMetadataMutation>,
        outbox: std::sync::Arc<dyn klights_cluster_store::AppliedOutboxLedger>,
    ) -> Self {
        Self { metadata, outbox }
    }
}

fn map_focused_storage_mutation_error(
    error: klights_cluster_store::ClusterStoreError,
) -> klights_cluster_core::StorageMutationError {
    use klights_cluster_core::{StorageCommandRejectionCode, StorageMutationError};
    use klights_cluster_store::ClusterStoreErrorKind;

    let diagnostic = error.to_string();
    let lower = diagnostic.to_ascii_lowercase();
    let rejection = match error.kind() {
        ClusterStoreErrorKind::Conflict if lower.contains("already exists") => {
            Some(StorageCommandRejectionCode::AlreadyExists)
        }
        ClusterStoreErrorKind::Conflict => Some(StorageCommandRejectionCode::Conflict),
        ClusterStoreErrorKind::NotFound => Some(StorageCommandRejectionCode::NotFound),
        ClusterStoreErrorKind::InvalidRequest => Some(StorageCommandRejectionCode::InvalidCommit),
        ClusterStoreErrorKind::Unsupported
        | ClusterStoreErrorKind::CorruptData
        | ClusterStoreErrorKind::Persistence
        | ClusterStoreErrorKind::Retryable
            if lower.contains("already exists") =>
        {
            Some(StorageCommandRejectionCode::AlreadyExists)
        }
        ClusterStoreErrorKind::Unsupported
        | ClusterStoreErrorKind::CorruptData
        | ClusterStoreErrorKind::Persistence
        | ClusterStoreErrorKind::Retryable
            if lower.contains("409 conflict")
                || lower.contains("version conflict")
                || lower.contains("rv conflict") =>
        {
            Some(StorageCommandRejectionCode::Conflict)
        }
        ClusterStoreErrorKind::Unsupported
        | ClusterStoreErrorKind::CorruptData
        | ClusterStoreErrorKind::Persistence
        | ClusterStoreErrorKind::Retryable
            if lower.contains("not found") =>
        {
            Some(StorageCommandRejectionCode::NotFound)
        }
        ClusterStoreErrorKind::Unsupported
        | ClusterStoreErrorKind::CorruptData
        | ClusterStoreErrorKind::Persistence
        | ClusterStoreErrorKind::Retryable => None,
    };
    match rejection {
        Some(code) => StorageMutationError::rejected(code, diagnostic),
        None => StorageMutationError::persistence(diagnostic),
    }
}

#[cfg(test)]
fn map_storage_mutation_error(error: anyhow::Error) -> klights_cluster_core::StorageMutationError {
    use klights_cluster_core::{StorageCommandRejectionCode, StorageMutationError};
    use klights_cluster_datastore::errors::DatastoreError;

    let diagnostic = format!("{error:#}");
    let concrete_code = error.chain().find_map(|source| {
        source
            .downcast_ref::<DatastoreError>()
            .map(|error| match error {
                DatastoreError::AlreadyExists { .. } => StorageCommandRejectionCode::AlreadyExists,
                DatastoreError::NotFound { .. } => StorageCommandRejectionCode::NotFound,
                DatastoreError::Conflict { .. } => StorageCommandRejectionCode::Conflict,
            })
    });
    let lower = diagnostic.to_ascii_lowercase();
    let rejection_code = concrete_code.or_else(|| {
        if lower.contains("already exists") && lower.contains("409 conflict") {
            Some(StorageCommandRejectionCode::AlreadyExists)
        } else if lower.contains("409 conflict")
            || lower.contains("version conflict")
            || lower.contains("rv conflict")
        {
            Some(StorageCommandRejectionCode::Conflict)
        } else if lower.contains("not found") {
            Some(StorageCommandRejectionCode::NotFound)
        } else {
            None
        }
    });

    match rejection_code {
        Some(code) => StorageMutationError::rejected(code, diagnostic),
        None => StorageMutationError::persistence(diagnostic),
    }
}

#[cfg(test)]
pub(crate) fn map_storage_mutation_error_for_test(
    error: anyhow::Error,
) -> klights_cluster_core::StorageMutationError {
    map_storage_mutation_error(error)
}

#[async_trait::async_trait]
impl klights_replication::materializer::RaftCommitMaterializer for DatastoreRaftCommitMaterializer {
    async fn read_raft_metadata(
        &self,
        key: &str,
    ) -> Result<Option<String>, klights_cluster_core::StorageMutationError> {
        self.metadata.get_klights_meta(key).await.map_err(|error| {
            klights_cluster_core::StorageMutationError::persistence(error.to_string())
        })
    }

    async fn build_command(
        &self,
        command: klights_cluster_core::StorageCommand,
        operation: &str,
        authoring_node: &str,
    ) -> Result<klights_cluster_core::LogApplyCommit, klights_cluster_core::StorageMutationError>
    {
        self.outbox
            .build_log_apply_commit_for_command(command, operation, authoring_node)
            .await
            .map_err(map_focused_storage_mutation_error)
    }

    async fn build_outbox(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: klights_cluster_core::StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> Result<klights_cluster_core::BuildOutboxOutcome, klights_cluster_core::OutboxApplyError>
    {
        self.outbox
            .build_log_apply_commit_for_outbox_with_watermark(
                idempotency_key,
                operation,
                command,
                authoring_node,
                watermark,
            )
            .await
    }
}

#[cfg(test)]
struct TestNoopPostCommitWakeup;

#[cfg(test)]
impl klights_leader_api::PostCommitWakeup for TestNoopPostCommitWakeup {
    fn wake(&self, _advances: &[klights_leader_api::PostCommitAdvance]) {}

    fn wake_namespace_contents(&self, _namespace: &str, _resource_version: i64) {}
}

#[cfg(test)]
pub(crate) fn raft_state_machine_store_ports_for_test(
    db: std::sync::Arc<klights_cluster_datastore::sqlite::embedded::Datastore>,
) -> klights_replication::state_machine::RaftStateMachineStorePorts {
    let passive = db.clone();
    let materializer = std::sync::Arc::new(DatastoreRaftCommitMaterializer::new(
        passive.clone(),
        passive.clone(),
    ));
    let persistence = passive.focused_committed_apply();
    klights_replication::state_machine::RaftStateMachineStorePorts::new(
        std::sync::Arc::new(
            klights_replication::committed_apply::ObservedCommittedRaftApply::new(
                persistence,
                std::sync::Arc::new(TestNoopPostCommitWakeup),
            ),
        ),
        std::sync::Arc::new(
            klights_replication::snapshot::RaftSnapshotRestoreAdapter::new(
                passive.focused_recovery_store(),
                std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(Default::default())),
            ),
        ),
        passive,
        materializer,
    )
}

#[cfg(test)]
pub(crate) fn raft_store_ports_for_test(
    db: std::sync::Arc<klights_cluster_datastore::sqlite::embedded::Datastore>,
) -> klights_replication::node::RaftStorePorts {
    let passive = db.clone();
    let materializer = std::sync::Arc::new(DatastoreRaftCommitMaterializer::new(
        passive.clone(),
        passive.clone(),
    ));
    let snapshot_capture = passive.focused_recovery_store();
    let allocator = passive.focused_read_store();
    let lifecycle = passive;
    klights_replication::node::RaftStorePorts::new(
        materializer,
        raft_state_machine_store_ports_for_test(db),
        snapshot_capture,
        allocator,
        lifecycle,
    )
}
