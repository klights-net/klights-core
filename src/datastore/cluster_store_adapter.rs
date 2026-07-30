//! Temporary root adapters from the legacy datastore to cluster-store ports.

use klights_cluster_core::LogApplyCommit;
use klights_cluster_store::SnapshotPersistenceError;

use super::DatastoreHandle;

pub(crate) struct DatastoreRaftCommitMaterializer {
    db: DatastoreHandle,
}

impl DatastoreRaftCommitMaterializer {
    pub(crate) fn new(db: DatastoreHandle) -> Self {
        Self { db }
    }
}

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
        self.db.get_klights_meta(key).await.map_err(|error| {
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
        self.db
            .build_log_apply_commit_for_command(command, operation, authoring_node)
            .await
            .map_err(map_storage_mutation_error)
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
        self.db
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
    db: std::sync::Arc<super::sqlite::Datastore>,
) -> klights_replication::state_machine::RaftStateMachineStorePorts {
    let db_handle: DatastoreHandle = db.clone();
    let materializer = std::sync::Arc::new(DatastoreRaftCommitMaterializer::new(db_handle.clone()));
    let persistence = db.focused_committed_apply();
    klights_replication::state_machine::RaftStateMachineStorePorts::new(
        std::sync::Arc::new(
            klights_replication::committed_apply::ObservedCommittedRaftApply::new(
                persistence,
                std::sync::Arc::new(TestNoopPostCommitWakeup),
            ),
        ),
        std::sync::Arc::new(SqliteRaftSnapshotRestore::new(
            db.focused_recovery_store(),
            crate::datastore::raft::snapshot_install(),
            std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(Default::default())),
        )),
        std::sync::Arc::new(crate::datastore::DatastoreBackendLifecyclePort::new(
            db_handle,
        )),
        materializer,
    )
}

#[cfg(test)]
pub(crate) fn raft_store_ports_for_test(
    db: std::sync::Arc<super::sqlite::Datastore>,
) -> crate::datastore::raft::node::RaftStorePorts {
    let db_handle: DatastoreHandle = db.clone();
    let materializer = std::sync::Arc::new(DatastoreRaftCommitMaterializer::new(db_handle));
    let recovery = std::sync::Arc::new(crate::datastore::DatastoreDurableRecoveryPort::new(
        db.clone(),
    ));
    let lifecycle = std::sync::Arc::new(crate::datastore::DatastoreBackendLifecyclePort::new(
        db.clone(),
    ));
    crate::datastore::raft::node::RaftStorePorts::new(
        materializer,
        raft_state_machine_store_ports_for_test(db),
        recovery,
        lifecycle,
    )
}

impl super::sqlite::Datastore {
    pub async fn apply_raft_log_apply_commit(
        &self,
        commit: LogApplyCommit,
    ) -> anyhow::Result<klights_replication::types::StorageCommandResult> {
        let receipt = self.apply_raft_log_apply_commit_receipt(commit).await?;
        Ok(
            klights_replication::committed_apply::storage_command_result_from_committed_outcome(
                &receipt,
            ),
        )
    }
}

/// Root-only OpenRaft envelope adapter over the SQLite recovery port.
pub(crate) struct SqliteRaftSnapshotRestore {
    recovery: std::sync::Arc<klights_cluster_datastore::sqlite::recovery::SqliteRecoveryStore>,
    supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
}

impl SqliteRaftSnapshotRestore {
    pub(crate) fn new(
        recovery: std::sync::Arc<klights_cluster_datastore::sqlite::recovery::SqliteRecoveryStore>,
        _authority: crate::datastore::raft::SnapshotInstallAuthority,
        supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
    ) -> Self {
        Self {
            recovery,
            supervisor,
        }
    }
}

#[async_trait::async_trait]
impl klights_replication::state_machine::RaftSnapshotRestore for SqliteRaftSnapshotRestore {
    async fn restore_snapshot(
        &self,
        snapshot_bytes: Vec<u8>,
    ) -> Result<(), SnapshotPersistenceError> {
        let data = self
            .supervisor
            .run_blocking(
                klights_supervisor::TaskCategory::Others,
                "raft-snapshot-json-zstd-decode",
                move || {
                    crate::datastore::raft::snapshot::RaftSnapshotData::deserialize_from_bytes(
                        &snapshot_bytes,
                    )
                },
            )
            .await
            .map_err(|error| SnapshotPersistenceError::PersistenceFailed {
                message: error.to_string(),
            })?
            .map_err(|error| SnapshotPersistenceError::PersistenceFailed {
                message: error.to_string(),
            })?;
        let metadata = data.cluster_metadata;
        let membership = match data.cluster_membership {
            None => klights_cluster_datastore::sqlite::recovery::SnapshotMembership::LegacyOmitted,
            Some(crate::datastore::raft::snapshot::RaftSnapshotMembership::AuthoritativeAbsent) => {
                klights_cluster_datastore::sqlite::recovery::SnapshotMembership::AuthoritativeAbsent
            }
            Some(crate::datastore::raft::snapshot::RaftSnapshotMembership::Present(value)) => {
                klights_cluster_datastore::sqlite::recovery::SnapshotMembership::Present(value)
            }
        };
        self.recovery
            .restore_snapshot_parts(
                data.operations,
                data.current_rv,
                data.watch_event_high_water,
                data.watch_replay_floors.map(|floors| {
                    floors
                        .into_iter()
                        .map(|floor| {
                            klights_cluster_datastore::sqlite::recovery::SnapshotReplayFloor {
                                api_version: floor.api_version,
                                kind: floor.kind,
                                namespace_key: floor.namespace_key,
                                floor_resource_version: floor.floor_resource_version,
                                floor_event_id: floor.floor_event_id,
                                position_is_exact: floor.position_is_exact,
                            }
                        })
                        .collect()
                }),
                Some(
                    klights_cluster_datastore::sqlite::recovery::SnapshotMetadata {
                        cluster_id: metadata
                            .as_ref()
                            .map(|metadata| metadata.cluster_id.clone())
                            .unwrap_or_default(),
                        leader_epoch: metadata
                            .as_ref()
                            .map_or(0, |metadata| metadata.leader_epoch),
                        membership,
                        command_codec_activation_version: data.command_codec_activation_version,
                    },
                ),
            )
            .await
    }
}
