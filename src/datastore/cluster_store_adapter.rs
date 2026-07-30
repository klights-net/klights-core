//! Temporary root adapters from the legacy datastore to cluster-store ports.

use klights_cluster_core::{CommittedApplyOutcome, LogApplyCommit, LogApplyMutation};
use klights_cluster_store::{
    CommittedRaftApplyReceipt, CommittedRaftApplyRequest, PrivilegedCommittedRaftApply,
    SnapshotPersistenceError,
};

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

#[async_trait::async_trait]
impl crate::datastore::raft::node::RaftCommitMaterializer for DatastoreRaftCommitMaterializer {
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
pub(crate) fn raft_state_machine_store_ports_for_test(
    db: std::sync::Arc<super::sqlite::Datastore>,
) -> crate::datastore::raft::state_machine_impl::RaftStateMachineStorePorts {
    let db_handle: DatastoreHandle = db.clone();
    let materializer = std::sync::Arc::new(DatastoreRaftCommitMaterializer::new(db_handle.clone()));
    let persistence = db.focused_committed_apply();
    crate::datastore::raft::state_machine_impl::RaftStateMachineStorePorts::new(
        std::sync::Arc::new(ObservedCommittedRaftApply::new_for_test(persistence)),
        std::sync::Arc::new(SqliteRaftSnapshotRestore::new(
            db.focused_recovery_store(),
            crate::datastore::raft::snapshot_install(),
        )),
        std::sync::Arc::new(crate::datastore::DatastoreDurableRecoveryPort::new(
            db_handle.clone(),
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
    crate::datastore::raft::node::RaftStorePorts::new(
        materializer,
        raft_state_machine_store_ports_for_test(db),
    )
}

/// Root decorator that projects the canonical persistence receipt into the
/// OpenRaft response and publishes active post-commit wakeups.
pub(crate) struct ObservedCommittedRaftApply {
    persistence: std::sync::Arc<dyn PrivilegedCommittedRaftApply>,
    wakeups: Option<std::sync::Arc<dyn klights_leader_api::PostCommitWakeup>>,
}

impl ObservedCommittedRaftApply {
    pub(crate) fn new(
        persistence: std::sync::Arc<dyn PrivilegedCommittedRaftApply>,
        wakeups: std::sync::Arc<dyn klights_leader_api::PostCommitWakeup>,
    ) -> Self {
        Self {
            persistence,
            wakeups: Some(wakeups),
        }
    }

    #[cfg(test)]
    fn new_for_test(persistence: std::sync::Arc<dyn PrivilegedCommittedRaftApply>) -> Self {
        Self {
            persistence,
            wakeups: None,
        }
    }

    fn publish_visible_commit(&self, commit: &LogApplyCommit, resource_version: i64) {
        let Some(wakeups) = &self.wakeups else {
            return;
        };
        let advances = post_commit_advances(commit, resource_version);
        wakeups.wake(&advances);
        for mutation in commit.mutations() {
            if let LogApplyMutation::DeleteNamespaceContents { name } = mutation {
                wakeups.wake_namespace_contents(name, resource_version);
            }
        }
    }
}

#[async_trait::async_trait]
impl crate::datastore::raft::state_machine_impl::RaftCommittedApply for ObservedCommittedRaftApply {
    async fn apply_committed(
        &self,
        request: CommittedRaftApplyRequest,
    ) -> Result<
        crate::datastore::raft::types::StorageCommandResult,
        klights_cluster_store::CommittedApplyError,
    > {
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

pub(crate) fn storage_command_result_from_committed_outcome(
    receipt: &CommittedRaftApplyReceipt,
) -> crate::datastore::raft::types::StorageCommandResult {
    let applied_mutation = receipt
        .applied_resource()
        .cloned()
        .map(crate::datastore::raft::types::AppliedMutation::Resource);
    match receipt.outcome() {
        CommittedApplyOutcome::Visible {
            resource_version, ..
        } => crate::datastore::raft::types::StorageCommandResult {
            applied_rv: Some(*resource_version),
            error_message: None,
            rejection_code: None,
            public_resource_changed: true,
            applied_mutation,
            pod_endpoint_effect: receipt.pod_endpoint_effect(),
        },
        CommittedApplyOutcome::NoPublicChange {
            resource_version, ..
        } => crate::datastore::raft::types::StorageCommandResult {
            applied_rv: Some(*resource_version),
            error_message: None,
            rejection_code: None,
            public_resource_changed: false,
            applied_mutation,
            pod_endpoint_effect: receipt.pod_endpoint_effect(),
        },
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
            crate::datastore::raft::types::StorageCommandResult {
                applied_rv: None,
                error_message: Some(rejection.message().to_string()),
                rejection_code: Some(rejection_code),
                public_resource_changed: false,
                applied_mutation: None,
                pod_endpoint_effect: receipt.pod_endpoint_effect(),
            }
        }
        _ => crate::datastore::raft::types::StorageCommandResult {
            applied_rv: None,
            error_message: Some("unsupported canonical committed-apply outcome".to_string()),
            rejection_code: Some(klights_cluster_core::StorageCommandRejectionCode::InvalidCommit),
            public_resource_changed: false,
            applied_mutation: None,
            pod_endpoint_effect: receipt.pod_endpoint_effect(),
        },
    }
}

impl super::sqlite::Datastore {
    pub async fn apply_raft_log_apply_commit(
        &self,
        commit: LogApplyCommit,
    ) -> anyhow::Result<crate::datastore::raft::types::StorageCommandResult> {
        let receipt = self.apply_raft_log_apply_commit_receipt(commit).await?;
        Ok(storage_command_result_from_committed_outcome(&receipt))
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

/// Root-only OpenRaft envelope adapter over the SQLite recovery port.
pub(crate) struct SqliteRaftSnapshotRestore {
    recovery: std::sync::Arc<klights_cluster_datastore::sqlite::recovery::SqliteRecoveryStore>,
}

impl SqliteRaftSnapshotRestore {
    pub(crate) fn new(
        recovery: std::sync::Arc<klights_cluster_datastore::sqlite::recovery::SqliteRecoveryStore>,
        _authority: crate::datastore::raft::SnapshotInstallAuthority,
    ) -> Self {
        Self { recovery }
    }
}

#[async_trait::async_trait]
impl crate::datastore::raft::state_machine_impl::RaftSnapshotRestore for SqliteRaftSnapshotRestore {
    async fn restore_snapshot(
        &self,
        data: crate::datastore::raft::snapshot::RaftSnapshotData,
    ) -> Result<(), SnapshotPersistenceError> {
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
