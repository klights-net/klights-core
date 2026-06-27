//! SQLite implementation of `DatastoreBackend`.
//!
//! Submodules (crud, schema, watch, etc.) reach back here for shared
//! types via `use super::*;` — the re-exports below make the `types::`
//! and `backend::` symbols visible to them.

mod applier;
mod cluster_replace;
mod cluster_state_apply;
mod crud;
mod executor;
mod filters;
mod fingerprint;
mod gc;
mod merge_patch;
pub mod opener;
pub(super) mod owner_ref_index;
mod queries;
mod replay;
mod resource_shape;
mod rv_helpers;
mod schema;
pub(crate) mod scope;
mod selector_index;
#[cfg(test)]
pub mod test_support;
#[cfg(test)]
mod tests;
mod watch;
// DSB-04: broadcast mode probe. Dead code until DSB-HA-02 consumes it.
pub(super) mod watch_mode;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use rusqlite::{OptionalExtension, TransactionBehavior};
use serde_json::Value;
use std::net::Ipv4Addr;
use tokio::sync::broadcast;

use crate::datastore::WatchReplayRead;
use crate::networking::{NodeName, PodSubnet, VtepMac};
use crate::task_supervisor::TaskSupervisor;
use crate::watch::{WatchBus, WatchSignal, WatchTopic};

impl std::fmt::Debug for Datastore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Datastore").finish_non_exhaustive()
    }
}

// Re-export types and trait surface so `use super::*;` in submodules picks
// them up the same way the legacy `db/mod.rs` exposed them.
pub use super::backend::{
    DatastoreBackend, DatastoreHandle, NamespaceStore, NetworkStore, ResourceStore, WatchStore,
};
pub use super::types::{
    AppliedOutboxRecord, CatchUpResource, ListPageRequest, NodeSubnet, PatchKind,
    PendingWatchEvent, PodCleanupIntent, PodEndpointEvent, PodEndpointMode, PodEndpointRow,
    PodNetworkEndpoint, PodSlotAdmissionEvent, PodSlotAdmissionResult, PodSlotAdmissionState,
    PodWorkqueueEntry, PodWorkqueueKind, ReplicatedCreateOptions, ReplicatedSnapshotMetadata,
    Resource, ResourceBatchOperation, ResourceList, ResourceListQuery, ResourcePatchRequest,
    ResourcePreconditions, SandboxRef, SnapshotAtRv, WatchTarget, WatchTargetScope,
};

pub use executor::DbExecutor;
pub use replay::DatastoreWatchReplaySource;
pub use watch::create_pending_watch_event;
pub use watch::publish_pending;

use crate::datastore::pod_serviceaccount::{
    inject_serviceaccount_volume, should_inject_serviceaccount_volume,
};
#[cfg(test)]
use filters::filter_by_field_selector;
use filters::{
    matches_field_selector_conditions, matches_label_requirements, parse_field_selector_conditions,
    parse_label_selector, split_sql_pushdown_conditions,
};
#[cfg(test)]
use filters::{resolve_field_path, split_selector};
use resource_shape::{
    ensure_metadata_create_defaults, ensure_metadata_identity, ensure_metadata_uid,
    ensure_pod_status_ip_arrays, ensure_resource_type_meta, hydrate_watch_event_data, metadata_uid,
    preserve_server_metadata_fields_from_existing, resource_client_owned_state_equal,
    validate_metadata_uid_immutable, validate_resource_preconditions,
    warn_uid_precondition_mismatch,
};
use scope::use_namespaced_table;

/// Bound for the internal pod_endpoints broadcast channel. Generous because
/// every Phase 2 rootless reconciler subscribes here and watch lag must not
/// drop events on a healthy node.
const POD_ENDPOINT_CHANNEL_BOUND: usize = 4_096;
const POD_SLOT_ADMISSION_CHANNEL_BOUND: usize = 4_096;
const APPLIED_OUTBOX_PLACEHOLDER_RECOVERY_TTL_MS: i64 = 60_000;

#[derive(Clone)]
pub struct Datastore {
    executor: DbExecutor,
    node_local: crate::datastore::node_local::SqliteNodeLocalDb,
    watch_bus: std::sync::Arc<WatchBus>,
    pod_endpoint_tx: broadcast::Sender<PodEndpointEvent>,
    pod_slot_admission_tx: broadcast::Sender<PodSlotAdmissionEvent>,
}

struct AtomicOutboxMutation {
    applied_rv: Option<i64>,
    result_proto: Vec<u8>,
    pending: Option<PendingWatchEvent>,
}

enum OutboxTxnOutcome {
    Applied {
        applied_rv: i64,
        pending: Option<PendingWatchEvent>,
    },
    InFlightPlaceholder,
    AlreadyApplied(Option<AppliedOutboxRecord>),
}

/// T1.3/T1.4: result of `Datastore::build_log_apply_commit_for_outbox`.
/// Carries the unencoded `LogApplyCommit` the leader should propose, or
/// an already-applied marker for short-circuiting duplicate proposals.
pub enum BuildOutboxOutcome {
    /// The build claimed the idempotency slot and produced a commit that
    /// must be proposed via raft. The state machine apply path on every
    /// node will run `apply_log_apply_commit(commit)`.
    NeedsPropose {
        commit: crate::log_apply::LogApplyCommit,
        applied_rv: i64,
    },
    /// Lease-renew is a no-op shortcut that never goes through raft; the
    /// leader returns success immediately without proposing.
    LeaseRenewShortcircuit,
    /// The idempotency key is already recorded as applied; the leader
    /// returns the cached `applied_rv` (if any) without proposing.
    AlreadyApplied { applied_rv: Option<i64> },
}

enum BuildOutboxTxnOutcome {
    Built {
        commit: crate::log_apply::LogApplyCommit,
        rv: i64,
    },
    InFlightPlaceholder,
    AlreadyApplied(Option<AppliedOutboxRecord>),
}

#[cfg(test)]
#[async_trait::async_trait]
impl crate::kubelet::volume_sources::VolumeSourceReader for Datastore {
    async fn config_map(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.get_resource("v1", "ConfigMap", Some(namespace), name)
            .await
    }

    async fn secret(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.get_resource("v1", "Secret", Some(namespace), name)
            .await
    }

    async fn service_account(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.get_resource("v1", "ServiceAccount", Some(namespace), name)
            .await
    }

    async fn pod(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.get_resource("v1", "Pod", Some(namespace), name).await
    }

    async fn node(&self, name: &str) -> Result<Option<Resource>> {
        self.get_resource("v1", "Node", None, name).await
    }

    async fn persistent_volume_claim(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Resource>> {
        self.get_resource("v1", "PersistentVolumeClaim", Some(namespace), name)
            .await
    }

    async fn persistent_volume(&self, name: &str) -> Result<Option<Resource>> {
        self.get_resource("v1", "PersistentVolume", None, name)
            .await
    }
}

impl Datastore {
    pub async fn db_call<T, F>(&self, query_name: &'static str, f: F) -> tokio_rusqlite::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut rusqlite::Connection) -> tokio_rusqlite::Result<T> + Send + 'static,
    {
        self.executor.call_raw(query_name, f).await
    }

    pub async fn node_db_call<T, F>(
        &self,
        query_name: &'static str,
        f: F,
    ) -> tokio_rusqlite::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut rusqlite::Connection) -> tokio_rusqlite::Result<T> + Send + 'static,
    {
        self.node_local.db_call(query_name, f).await
    }

    pub async fn get_applied_outbox(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<AppliedOutboxRecord>> {
        let idempotency_key = idempotency_key.to_string();
        self.db_call("db_applied_outbox_get", move |conn| {
            conn.query_row(queries::APPLIED_OUTBOX_GET, [idempotency_key], |row| {
                Ok(AppliedOutboxRecord {
                    idempotency_key: row.get(0)?,
                    subject_key: row.get(1)?,
                    operation: row.get(2)?,
                    first_seen_ms: row.get(3)?,
                    applied_rv: row.get(4)?,
                    result_proto: row.get(5)?,
                    status_stamp: row.get(6)?,
                })
            })
            .optional()
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|e| anyhow!("applied outbox get failed: {e}"))
    }

    pub async fn insert_applied_outbox(&self, record: AppliedOutboxRecord) -> Result<bool> {
        self.db_call("db_applied_outbox_insert", move |conn| {
            let changed = conn.execute(
                queries::APPLIED_OUTBOX_INSERT,
                rusqlite::params![
                    record.idempotency_key,
                    record.subject_key,
                    record.operation,
                    record.first_seen_ms,
                    record.applied_rv,
                    record.result_proto,
                    record.status_stamp
                ],
            )?;
            Ok(changed > 0)
        })
        .await
        .map_err(|e| anyhow!("applied outbox insert failed: {e}"))
    }

    pub async fn list_applied_outbox(&self) -> Result<Vec<AppliedOutboxRecord>> {
        self.db_call("db_applied_outbox_list_all", move |conn| {
            let rows = conn
                .prepare(queries::APPLIED_OUTBOX_LIST_ALL)?
                .query_map([], |row| {
                    Ok(AppliedOutboxRecord {
                        idempotency_key: row.get(0)?,
                        subject_key: row.get(1)?,
                        operation: row.get(2)?,
                        first_seen_ms: row.get(3)?,
                        applied_rv: row.get(4)?,
                        result_proto: row.get(5)?,
                        status_stamp: row.get(6)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .map_err(|e| anyhow!("applied outbox list failed: {e}"))
    }

    pub async fn apply_resource_batch(
        &self,
        operations: Vec<ResourceBatchOperation>,
    ) -> Result<()> {
        if operations.is_empty() {
            return Ok(());
        }
        let command = crate::datastore::command::StorageCommand::ApplyResourceBatch { operations };
        // Build + apply in a single IMMEDIATE transaction so the reserved
        // resourceVersion and the written rows are always committed together.
        // A two-step approach (build in one txn, apply in a second) leaves a
        // window where the metadata_rv is advanced but no rows exist yet —
        // visible to concurrent readers as a reserved-but-not-applied batch.
        let pending = self
            .db_call("db_apply_resource_batch", move |conn| {
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let (commit, _rv) = Self::build_log_apply_commit_in_tx_from_command(
                    &tx,
                    command,
                    "ResourceBatch",
                    "",
                )?;
                let pending =
                    crate::datastore::sqlite::cluster_replace::apply_commit_in_tx(&tx, commit)?;
                tx.commit()?;
                Ok(pending)
            })
            .await
            .map_err(|e| anyhow!("apply resource batch failed: {e}"))?;
        self.publish_watch_events(pending);
        Ok(())
    }

    pub async fn delete_uncommitted_applied_outbox_placeholder(
        &self,
        idempotency_key: &str,
        reserved_rv: i64,
    ) -> Result<bool> {
        let idempotency_key = idempotency_key.to_string();
        self.db_call(
            "db_applied_outbox_delete_uncommitted_placeholder",
            move |conn| {
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let changed = tx.execute(
                    queries::APPLIED_OUTBOX_DELETE_UNCOMMITTED_PLACEHOLDER_BY_KEY,
                    rusqlite::params![idempotency_key],
                )?;
                if changed > 0 {
                    crate::datastore::sqlite::cluster_replace::rollback_uncommitted_metadata_rv_if_current_tx(
                        &tx,
                        reserved_rv,
                    )?;
                }
                tx.commit()?;
                Ok(changed > 0)
            },
        )
        .await
        .map_err(|e| anyhow!("applied outbox placeholder delete failed: {e}"))
    }

    pub async fn move_pod_to_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        let command = crate::datastore::command::StorageCommand::MovePodToCleanupIntent {
            node_name: node_name.to_string(),
            namespace: namespace.to_string(),
            pod_name: pod_name.to_string(),
            pod_uid: pod_uid.to_string(),
            reason: reason.to_string(),
        };
        let authoring_node = node_name.to_string();
        let pending = self
            .db_call("db_move_pod_to_cleanup_intent", move |conn| {
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let (commit, _rv) = Self::build_log_apply_commit_in_tx_from_command(
                    &tx,
                    command,
                    "ClusterMaintenance",
                    &authoring_node,
                )?;
                let pending =
                    crate::datastore::sqlite::cluster_replace::apply_commit_in_tx(&tx, commit)?;
                tx.commit()?;
                Ok(pending)
            })
            .await
            .map_err(|e| anyhow!("move pod to cleanup intent failed: {e}"))?;
        self.publish_watch_events(pending);
        Ok(())
    }

    pub async fn list_pod_cleanup_intents_for_node(
        &self,
        node_name: &str,
    ) -> Result<Vec<PodCleanupIntent>> {
        let node_name = node_name.to_string();
        self.db_call("db_list_pod_cleanup_intents_for_node", move |conn| {
            let rows = conn
                .prepare(queries::POD_CLEANUP_INTENT_LIST_BY_NODE)?
                .query_map([node_name], |row| {
                    let pod_data_bytes: Vec<u8> = row.get(7)?;
                    let pod_data = serde_json::from_slice(&pod_data_bytes).map_err(|err| {
                        rusqlite::Error::FromSqlConversionFailure(
                            7,
                            rusqlite::types::Type::Blob,
                            Box::new(err),
                        )
                    })?;
                    Ok(PodCleanupIntent {
                        node_name: row.get(0)?,
                        namespace: row.get(1)?,
                        pod_name: row.get(2)?,
                        pod_uid: row.get(3)?,
                        reason: row.get(4)?,
                        resource_version: row.get(5)?,
                        created_at_ms: row.get(6)?,
                        pod_data,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .map_err(|e| anyhow!("list pod cleanup intents failed: {e}"))
    }

    pub async fn delete_pod_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        let command = crate::datastore::command::StorageCommand::DeletePodCleanupIntent {
            node_name: node_name.to_string(),
            namespace: namespace.to_string(),
            pod_name: pod_name.to_string(),
            pod_uid: pod_uid.to_string(),
            reason: reason.to_string(),
        };
        self.apply_cluster_maintenance_command(command, node_name)
            .await
    }

    pub async fn delete_pod_cleanup_intents_for_node(&self, node_name: &str) -> Result<()> {
        let command = crate::datastore::command::StorageCommand::DeletePodCleanupIntentsForNode {
            node_name: node_name.to_string(),
        };
        self.apply_cluster_maintenance_command(command, node_name)
            .await
    }

    async fn apply_cluster_maintenance_command(
        &self,
        command: crate::datastore::command::StorageCommand,
        authoring_node: &str,
    ) -> Result<()> {
        let authoring_node = authoring_node.to_string();
        let pending = self
            .db_call("db_apply_cluster_maintenance_command", move |conn| {
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let (commit, _rv) = Self::build_log_apply_commit_in_tx_from_command(
                    &tx,
                    command,
                    "ClusterMaintenance",
                    &authoring_node,
                )?;
                let pending =
                    crate::datastore::sqlite::cluster_replace::apply_commit_in_tx(&tx, commit)?;
                tx.commit()?;
                Ok(pending)
            })
            .await
            .map_err(|e| anyhow!("apply cluster maintenance command failed: {e}"))?;
        self.publish_watch_events(pending);
        Ok(())
    }

    pub async fn current_log_apply_index(&self) -> Result<i64> {
        self.node_local.current_log_apply_index().await
    }

    async fn gc_stale_applied_outbox_placeholders(&self, now_ms: i64) -> Result<usize> {
        let cutoff_ms = now_ms.saturating_sub(APPLIED_OUTBOX_PLACEHOLDER_RECOVERY_TTL_MS);
        self.db_call("db_gc_stale_applied_outbox_placeholders", move |conn| {
            Ok(conn.execute(
                queries::APPLIED_OUTBOX_DELETE_STALE_PLACEHOLDERS,
                [cutoff_ms],
            )?)
        })
        .await
        .map_err(|e| anyhow!("applied outbox stale placeholder gc failed: {e}"))
    }

    /// T1.3/T1.4: build (without applying) the `LogApplyCommit` that the
    /// leader's raft proposer should submit. Mirrors the early stages of
    /// `apply_outbox_transactionally` (decode payload, lease-renew shortcut,
    /// idempotency check, placeholder claim) but stops short of calling
    /// `apply_commit_in_tx`. The leader reserves the resourceVersion in the
    /// committed payload so every raft member materializes the same object RV.
    ///
    /// On committed exit an `applied_outbox` placeholder row is recorded so
    /// a duplicate proposal for the same `idempotency_key` short-circuits.
    pub async fn build_log_apply_commit_for_outbox(
        &self,
        idempotency_key: &str,
        operation: &str,
        payload: &[u8],
        authoring_node: &str,
    ) -> std::result::Result<BuildOutboxOutcome, crate::kubelet::outbox::OutboxApplyError> {
        use crate::control_plane::client::apply::subject_key_for_command;
        use crate::kubelet::outbox::OutboxApplyError;
        use crate::kubelet::outbox::payload::{OutboxOperation, OutboxPayload};
        use crate::log_apply::{LogApplyAppliedOutboxRow, LogApplyMutation};

        let decoded = OutboxPayload::decode_protobuf(payload)
            .map_err(|e| OutboxApplyError::Retryable(e.to_string()))?;
        if operation == OutboxOperation::LeaseRenew.as_str() {
            crate::node_lease_tracker::ensure_lease_renew_command(&decoded.command, authoring_node)
                .map_err(|err| OutboxApplyError::ConflictTerminal(err.to_string()))?;
            return Ok(BuildOutboxOutcome::LeaseRenewShortcircuit);
        }
        let subject_key = subject_key_for_command(&decoded.command);
        let status_stamp = Self::pod_status_stamp_of(&decoded.command);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let claim_key = idempotency_key.to_string();
        let claim_operation = operation.to_string();
        let stale_cutoff_ms = now.saturating_sub(APPLIED_OUTBOX_PLACEHOLDER_RECOVERY_TTL_MS);
        let authoring_node_owned = authoring_node.to_string();

        let outcome = self
            .db_call("db_build_log_apply_commit_for_outbox", move |conn| {
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

                // Idempotency check + placeholder claim. Mirrors the same
                // logic in apply_outbox_transactionally so the leader does
                // not double-propose for a key that has already been
                // applied (or whose placeholder is still in-flight).
                let existing: Option<AppliedOutboxRecord> = tx
                    .query_row(queries::APPLIED_OUTBOX_GET, [&claim_key], |row| {
                        Ok(AppliedOutboxRecord {
                            idempotency_key: row.get(0)?,
                            subject_key: row.get(1)?,
                            operation: row.get(2)?,
                            first_seen_ms: row.get(3)?,
                            applied_rv: row.get(4)?,
                            result_proto: row.get(5)?,
                            status_stamp: row.get(6)?,
                        })
                    })
                    .optional()?;
                if let Some(ref row) = existing {
                    let placeholder = row.applied_rv.is_none() && row.result_proto.is_empty();
                    if placeholder && row.first_seen_ms < stale_cutoff_ms {
                        tx.execute(queries::APPLIED_OUTBOX_DELETE_BY_KEY, [&claim_key])?;
                    } else if placeholder {
                        tx.commit()?;
                        return Ok(BuildOutboxTxnOutcome::InFlightPlaceholder);
                    } else {
                        tx.commit()?;
                        return Ok(BuildOutboxTxnOutcome::AlreadyApplied(existing));
                    }
                }

                let (mut commit, rv) = Self::build_log_apply_commit_in_tx_from_command(
                    &tx,
                    decoded.command,
                    &claim_operation,
                    &authoring_node_owned,
                )?;

                // Claim the placeholder in the same transaction that reserves
                // the RV so list snapshots can avoid anchoring past unapplied
                // raft entries for this resource collection.
                tx.execute(
                    queries::APPLIED_OUTBOX_INSERT_PLACEHOLDER_WITH_RESERVED_RV,
                    rusqlite::params![&claim_key, &subject_key, &claim_operation, now, rv],
                )?;

                // Append PutAppliedOutbox so the state-machine apply on
                // every node records the final idempotency outcome. On
                // the leader this UPSERT overwrites the placeholder we
                // just claimed; on followers it inserts fresh.
                use crate::datastore::command::{StorageResponse, encode_response_protobuf};
                commit.mutations.push(LogApplyMutation::PutAppliedOutbox(
                    LogApplyAppliedOutboxRow {
                        idempotency_key: claim_key.clone(),
                        subject_key,
                        operation: claim_operation,
                        first_seen_ms: now,
                        applied_rv: None,
                        result_proto: encode_response_protobuf(&StorageResponse::Ack {
                            resource_version: 0,
                        })
                        .unwrap_or_default(),
                        status_stamp,
                    },
                ));

                tx.commit()?;
                Ok(BuildOutboxTxnOutcome::Built { commit, rv })
            })
            .await
            .map_err(Self::outbox_apply_error_from_db_error)?;

        match outcome {
            BuildOutboxTxnOutcome::Built { commit, rv } => Ok(BuildOutboxOutcome::NeedsPropose {
                commit,
                applied_rv: rv,
            }),
            BuildOutboxTxnOutcome::AlreadyApplied(record) => {
                if let Some(message) = Self::cached_outbox_terminal_error(record.as_ref())? {
                    return Err(crate::kubelet::outbox::OutboxApplyError::ConflictTerminal(
                        message,
                    ));
                }
                Ok(BuildOutboxOutcome::AlreadyApplied {
                    applied_rv: record.and_then(|r| r.applied_rv),
                })
            }
            BuildOutboxTxnOutcome::InFlightPlaceholder => {
                Err(Self::inflight_outbox_placeholder_error(idempotency_key))
            }
        }
    }

    pub async fn apply_outbox_transactionally(
        &self,
        idempotency_key: &str,
        operation: &str,
        payload: &[u8],
        authoring_node: &str,
    ) -> std::result::Result<
        crate::kubelet::outbox::OutboxApplyResult,
        crate::kubelet::outbox::OutboxApplyError,
    > {
        use crate::control_plane::client::apply::subject_key_for_command;
        use crate::kubelet::outbox::payload::{OutboxOperation, OutboxPayload};
        use crate::kubelet::outbox::{OutboxApplyError, OutboxApplyResult};
        let decoded = OutboxPayload::decode_protobuf(payload)
            .map_err(|e| OutboxApplyError::Retryable(e.to_string()))?;
        if operation == OutboxOperation::LeaseRenew.as_str() {
            crate::node_lease_tracker::ensure_lease_renew_command(&decoded.command, authoring_node)
                .map_err(|err| OutboxApplyError::ConflictTerminal(err.to_string()))?;
            return Ok(OutboxApplyResult::Applied { applied_rv: 0 });
        }
        let subject_key = subject_key_for_command(&decoded.command);
        let status_stamp = Self::pod_status_stamp_of(&decoded.command);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let claim_key = idempotency_key.to_string();
        let claim_operation = operation.to_string();
        let stale_cutoff_ms = now.saturating_sub(APPLIED_OUTBOX_PLACEHOLDER_RECOVERY_TTL_MS);
        let authoring_node = authoring_node.to_string();
        let outcome = self
            .db_call("db_apply_outbox_atomic", move |conn| {
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

                let mut existing: Option<AppliedOutboxRecord> = tx
                    .query_row(queries::APPLIED_OUTBOX_GET, [&claim_key], |row| {
                        Ok(AppliedOutboxRecord {
                            idempotency_key: row.get(0)?,
                            subject_key: row.get(1)?,
                            operation: row.get(2)?,
                            first_seen_ms: row.get(3)?,
                            applied_rv: row.get(4)?,
                            result_proto: row.get(5)?,
                            status_stamp: row.get(6)?,
                        })
                    })
                    .optional()?;

                if let Some(ref row) = existing {
                    let placeholder = row.applied_rv.is_none() && row.result_proto.is_empty();
                    if placeholder && row.first_seen_ms < stale_cutoff_ms {
                        tx.execute(queries::APPLIED_OUTBOX_DELETE_BY_KEY, [&claim_key])?;
                    } else if placeholder {
                        tx.commit()?;
                        return Ok(OutboxTxnOutcome::InFlightPlaceholder);
                    } else {
                        tx.commit()?;
                        return Ok(OutboxTxnOutcome::AlreadyApplied(existing));
                    }
                }

                tx.execute(
                    queries::APPLIED_OUTBOX_INSERT,
                    rusqlite::params![
                        &claim_key,
                        "",
                        &claim_operation,
                        now,
                        Option::<i64>::None,
                        Vec::<u8>::new(),
                        Option::<i64>::None
                    ],
                )?;
                let mutation = Self::apply_outbox_command_in_tx(
                    &tx,
                    decoded.command,
                    &claim_operation,
                    &authoring_node,
                )?;
                tx.execute(
                    queries::APPLIED_OUTBOX_UPDATE_RESULT,
                    rusqlite::params![
                        &claim_key,
                        subject_key,
                        mutation.applied_rv,
                        mutation.result_proto,
                        status_stamp
                    ],
                )?;
                if tx.changes() == 0 {
                    existing = tx
                        .query_row(queries::APPLIED_OUTBOX_GET, [&claim_key], |row| {
                            Ok(AppliedOutboxRecord {
                                idempotency_key: row.get(0)?,
                                subject_key: row.get(1)?,
                                operation: row.get(2)?,
                                first_seen_ms: row.get(3)?,
                                applied_rv: row.get(4)?,
                                result_proto: row.get(5)?,
                                status_stamp: row.get(6)?,
                            })
                        })
                        .optional()?;
                    tx.commit()?;
                    return Ok(OutboxTxnOutcome::AlreadyApplied(existing));
                }
                tx.commit()?;
                Ok(OutboxTxnOutcome::Applied {
                    applied_rv: mutation.applied_rv.unwrap_or(0),
                    pending: mutation.pending,
                })
            })
            .await
            .map_err(Self::outbox_apply_error_from_db_error)?;

        match outcome {
            OutboxTxnOutcome::Applied {
                applied_rv,
                pending,
            } => {
                if let Some(pending) = pending {
                    self.publish_watch_event(pending);
                }
                Ok(OutboxApplyResult::Applied { applied_rv })
            }
            OutboxTxnOutcome::AlreadyApplied(record) => {
                if let Some(message) = Self::cached_outbox_terminal_error(record.as_ref())? {
                    return Err(OutboxApplyError::ConflictTerminal(message));
                }
                Ok(OutboxApplyResult::AlreadyApplied {
                    applied_rv: record.and_then(|record| record.applied_rv),
                })
            }
            OutboxTxnOutcome::InFlightPlaceholder => {
                Err(Self::inflight_outbox_placeholder_error(idempotency_key))
            }
        }
    }

    /// Apply a StorageCommand within a transaction by converting it to
    /// a LogApplyCommit and routing through `apply_commit_in_tx` — the
    /// same path used by the replica BackupApplier. This ensures:
    ///   - All StorageCommand variants are supported (no "unsupported" gap)
    ///   - Both raft state-machine apply and replica sync use one code path
    ///   - Log-apply entries are produced for downstream replicas
    fn apply_outbox_command_in_tx(
        tx: &rusqlite::Transaction<'_>,
        command: crate::datastore::command::StorageCommand,
        operation: &str,
        authoring_node: &str,
    ) -> tokio_rusqlite::Result<AtomicOutboxMutation> {
        use crate::datastore::command::StorageResponse;
        use crate::datastore::command::encode_response_protobuf;

        let (commit, _provisional_rv) = Self::build_log_apply_commit_in_tx_from_command(
            tx,
            command,
            operation,
            authoring_node,
        )?;

        let (applied_rv, pending) =
            crate::datastore::sqlite::cluster_replace::apply_commit_in_tx_returning_rv(
                tx, commit, false,
            )?;

        let pending_event = pending.into_iter().next();
        let result_proto = encode_response_protobuf(&StorageResponse::Ack {
            resource_version: applied_rv,
        })
        .unwrap_or_default();
        Ok(AtomicOutboxMutation {
            applied_rv: Some(applied_rv),
            result_proto,
            pending: pending_event,
        })
    }

    /// T1.3/T1.4: build a `LogApplyCommit` from a `StorageCommand` inside
    /// an open transaction WITHOUT applying it. The leader's raft proposer
    /// uses this to construct the commit, encode it as the raft entry
    /// payload, and submit through `client_write`. The state machine apply
    /// path on every node (leader included) is the only caller of
    /// `apply_commit_in_tx` after raft commits.
    ///
    /// The returned commit reserves the leader's next resourceVersion without
    /// applying resource mutations. Raft then serializes that materialized RV so
    /// followers do not derive divergent object RVs from their local counters.
    pub(crate) fn build_log_apply_commit_in_tx_from_command(
        tx: &rusqlite::Transaction<'_>,
        command: crate::datastore::command::StorageCommand,
        operation: &str,
        authoring_node: &str,
    ) -> tokio_rusqlite::Result<(crate::log_apply::LogApplyCommit, i64)> {
        use crate::datastore::command::StorageCommand;
        use crate::datastore::sqlite::crud::helpers::serde_to_sqlite_error;
        use crate::datastore::sqlite::resource_shape::{
            ensure_metadata_create_defaults, ensure_metadata_identity, ensure_metadata_uid,
            ensure_pod_status_ip_arrays, ensure_resource_type_meta,
            validate_resource_preconditions,
        };
        use crate::datastore::types::{
            ResourceBatchOperation, ResourceBatchPutMode, ResourcePreconditions,
        };
        use crate::log_apply::{
            LogApplyCommit, LogApplyMutation, LogApplyNamespaceRow, LogApplyNodeDataplaneRow,
            LogApplyNodeSubnetAllocation, LogApplyPodCleanupIntentKey, LogApplyPodCleanupIntentRow,
            LogApplyResourceKey, LogApplyResourcePatch, LogApplyResourceRow, LogApplyWatchEventRow,
        };
        use serde_json::Value;

        let rv = Self::next_resource_version_in_tx(tx)?;

        let commit = match command {
            StorageCommand::CreateResource {
                api_version,
                kind,
                namespace,
                name,
                mut data,
            } => {
                ensure_resource_type_meta(&mut data, &api_version, &kind);
                ensure_metadata_identity(&mut data, namespace.as_deref(), &name);
                ensure_metadata_create_defaults(&mut data);
                ensure_pod_status_ip_arrays(&mut data, &api_version, &kind);
                if operation
                    == crate::kubelet::outbox::payload::OutboxOperation::NodeRegistration.as_str()
                    && api_version == "v1"
                    && kind == "Node"
                    && namespace.is_none()
                    && name == authoring_node
                {
                    crate::kubelet::node::set_node_external_ip_from_dataplane_annotation(&mut data);
                }
                let uid = ensure_metadata_uid(&mut data);
                LogApplyCommit::new(
                    rv,
                    vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                        api_version,
                        kind,
                        namespace,
                        name,
                        uid,
                        resource_version: rv,
                        data,
                        require_absent: true,
                        require_existing: false,
                        precondition_uid: None,
                        precondition_resource_version: None,
                        status_only: false,
                    })],
                )
            }

            StorageCommand::UpdateResource {
                api_version,
                kind,
                namespace,
                name,
                mut data,
                expected_rv,
                preconditions,
            } => {
                let apply_against_latest = Self::should_apply_outbox_update_against_latest(
                    operation,
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    authoring_node,
                );
                let (live_rv, live_uid, live_data) = Self::resource_row_for_update_in_tx(
                    tx,
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                )?;
                let live: Value =
                    serde_json::from_slice(&live_data).map_err(serde_to_sqlite_error)?;
                let mut effective_preconditions = preconditions.clone();
                if apply_against_latest {
                    let uid_preconditions = ResourcePreconditions {
                        uid: preconditions.uid.clone(),
                        resource_version: None,
                    };
                    validate_resource_preconditions(&uid_preconditions, Some(&live_uid), live_rv)
                        .map_err(Self::sqlite_conversion_error)?;
                    if api_version == "v1" && kind == "Node" && namespace.is_none() {
                        crate::kubelet::node::merge_existing_node_mutable_fields(&mut data, &live);
                    } else if api_version == "coordination.k8s.io/v1"
                        && kind == "Lease"
                        && namespace.as_deref() == Some("kube-node-lease")
                    {
                        Self::merge_forwarded_lease_with_live(&live, &mut data);
                    }
                } else {
                    effective_preconditions.resource_version = preconditions
                        .resource_version
                        .or_else(|| (expected_rv > 0).then_some(expected_rv));
                    if let Some(expected) = effective_preconditions.resource_version
                        && expected != live_rv
                        && crate::resource_semantics::has_builtin_status_subresource(
                            &api_version,
                            &kind,
                        )
                        && let Some(base) = Self::resource_snapshot_for_key_at_rv_in_tx(
                            tx,
                            &api_version,
                            &kind,
                            namespace.as_deref(),
                            &name,
                            expected,
                        )?
                        && metadata_uid(&base) == Some(live_uid.as_str())
                        && resource_client_owned_state_equal(&base, &live)
                    {
                        effective_preconditions.resource_version = Some(live_rv);
                    }
                    validate_resource_preconditions(
                        &effective_preconditions,
                        Some(&live_uid),
                        live_rv,
                    )
                    .map_err(Self::sqlite_conversion_error)?;
                }
                ensure_resource_type_meta(&mut data, &api_version, &kind);
                ensure_metadata_identity(&mut data, namespace.as_deref(), &name);
                ensure_pod_status_ip_arrays(&mut data, &api_version, &kind);
                crate::resource_semantics::preserve_status_subresource_on_main_update(
                    &api_version,
                    &kind,
                    &live,
                    &mut data,
                );
                preserve_server_metadata_fields_from_existing(&mut data, &live);
                let uid = ensure_metadata_uid(&mut data);
                let precondition_resource_version = if apply_against_latest {
                    None
                } else {
                    effective_preconditions.resource_version
                };
                LogApplyCommit::new(
                    rv,
                    vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                        api_version,
                        kind,
                        namespace,
                        name,
                        uid,
                        resource_version: rv,
                        data,
                        require_absent: false,
                        require_existing: true,
                        precondition_uid: preconditions.uid,
                        precondition_resource_version,
                        status_only: false,
                    })],
                )
            }

            StorageCommand::UpdateStatus {
                api_version,
                kind,
                namespace,
                name,
                status,
                expected_rv,
                preconditions,
                observed_status_stamp,
            } => {
                let apply_against_latest = Self::should_apply_outbox_status_against_latest(
                    operation,
                    &api_version,
                    &kind,
                    &preconditions,
                );
                let (live_rv, live_uid, live_data) = Self::resource_row_for_update_in_tx(
                    tx,
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                )?;
                if apply_against_latest {
                    let uid_preconditions = ResourcePreconditions {
                        uid: preconditions.uid.clone(),
                        resource_version: None,
                    };
                    validate_resource_preconditions(&uid_preconditions, Some(&live_uid), live_rv)
                        .map_err(Self::sqlite_conversion_error)?;
                    // Lost-update guard for pipelined status dispatch. The
                    // status outbox drops the live-RV precondition (so a slow
                    // status no longer stalls behind a newer RV), which
                    // reopened the classic "an older snapshot retried after a
                    // newer one applied clobbers it" race. Each worker stamps
                    // its status snapshots monotonically; the leader records
                    // the highest stamp applied per Pod subject and no-ops any
                    // snapshot whose stamp is older-or-equal. UID is already
                    // validated above, so same-name replacement Pods (distinct
                    // subject key) are unaffected.
                    if let Some(incoming_stamp) = observed_status_stamp {
                        let subject_key = Self::pod_status_subject_key(
                            &api_version,
                            &kind,
                            namespace.as_deref(),
                            &name,
                            preconditions.uid.as_deref(),
                        );
                        let last_applied_stamp: Option<i64> = tx.query_row(
                            queries::APPLIED_OUTBOX_MAX_STATUS_STAMP_FOR_SUBJECT,
                            rusqlite::params![subject_key],
                            |row| row.get::<_, Option<i64>>(0),
                        )?;
                        if last_applied_stamp.is_some_and(|last| incoming_stamp <= last) {
                            // Stale snapshot: produce a commit with no resource
                            // mutation so the live status is preserved and no
                            // watch event is emitted. The outer apply still
                            // records the idempotency ledger row so the worker
                            // row completes instead of retrying forever.
                            return Ok((LogApplyCommit::new(rv, Vec::new()), rv));
                        }
                    }
                } else {
                    validate_resource_preconditions(&preconditions, Some(&live_uid), live_rv)
                        .map_err(Self::sqlite_conversion_error)?;
                    if let Some(expected_rv) = expected_rv
                        && expected_rv > 0
                        && expected_rv != live_rv
                    {
                        return Err(Self::sqlite_conversion_error(anyhow!(
                            "resourceVersion precondition failed: expected {expected_rv} got {live_rv} (409 Conflict)"
                        )));
                    }
                }
                let mut live: Value =
                    serde_json::from_slice(&live_data).map_err(serde_to_sqlite_error)?;
                let mut next_status = status;
                if apply_against_latest {
                    crate::pod_status_merge::merge_pod_status_for_update(
                        &api_version,
                        &kind,
                        &live,
                        &mut next_status,
                        crate::pod_status_merge::PodStatusOwner::KubeletRuntime,
                    );
                }
                if api_version == "v1" && kind == "Node" && namespace.is_none() {
                    crate::kubelet::node::merge_node_status_for_update(&mut next_status, &live);
                }
                live["status"] = next_status;
                ensure_resource_type_meta(&mut live, &api_version, &kind);
                let uid = ensure_metadata_uid(&mut live);
                let precondition_resource_version = if apply_against_latest {
                    None
                } else {
                    preconditions
                        .resource_version
                        .or_else(|| expected_rv.filter(|rv| *rv > 0))
                };
                LogApplyCommit::new(
                    rv,
                    vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                        api_version,
                        kind,
                        namespace,
                        name,
                        uid,
                        resource_version: rv,
                        data: live,
                        require_absent: false,
                        require_existing: true,
                        precondition_uid: preconditions.uid,
                        precondition_resource_version,
                        status_only: true,
                    })],
                )
            }

            StorageCommand::ApplyResourceBatch { operations } => {
                let mut mutations = Vec::with_capacity(operations.len());
                for operation in operations {
                    match operation {
                        ResourceBatchOperation::Put {
                            api_version,
                            kind,
                            namespace,
                            name,
                            mut data,
                            mode,
                            preconditions,
                        } => {
                            match mode {
                                ResourceBatchPutMode::Create => {
                                    if Self::resource_row_optional_for_update_in_tx(
                                        tx,
                                        &api_version,
                                        &kind,
                                        namespace.as_deref(),
                                        &name,
                                    )?
                                    .is_some()
                                    {
                                        return Err(Self::sqlite_conversion_error(anyhow!(
                                            "{}/{} {}/{} already exists",
                                            api_version,
                                            kind,
                                            namespace.as_deref().unwrap_or(""),
                                            name
                                        )));
                                    }
                                }
                                ResourceBatchPutMode::Update => {
                                    let (live_rv, live_uid, _) =
                                        Self::resource_row_for_update_in_tx(
                                            tx,
                                            &api_version,
                                            &kind,
                                            namespace.as_deref(),
                                            &name,
                                        )?;
                                    validate_resource_preconditions(
                                        &preconditions,
                                        Some(&live_uid),
                                        live_rv,
                                    )
                                    .map_err(Self::sqlite_conversion_error)?;
                                }
                            }
                            ensure_resource_type_meta(&mut data, &api_version, &kind);
                            ensure_metadata_identity(&mut data, namespace.as_deref(), &name);
                            if mode == ResourceBatchPutMode::Create {
                                ensure_metadata_create_defaults(&mut data);
                            }
                            ensure_pod_status_ip_arrays(&mut data, &api_version, &kind);
                            let uid = ensure_metadata_uid(&mut data);
                            mutations.push(LogApplyMutation::PutResource(LogApplyResourceRow {
                                api_version,
                                kind,
                                namespace,
                                name,
                                uid,
                                resource_version: rv,
                                data,
                                require_absent: mode == ResourceBatchPutMode::Create,
                                require_existing: mode == ResourceBatchPutMode::Update,
                                precondition_uid: preconditions.uid,
                                precondition_resource_version: preconditions.resource_version,
                                status_only: false,
                            }));
                        }
                        ResourceBatchOperation::Delete {
                            api_version,
                            kind,
                            namespace,
                            name,
                            preconditions,
                        } => {
                            let (live_rv, live_uid, _) = Self::resource_row_for_update_in_tx(
                                tx,
                                &api_version,
                                &kind,
                                namespace.as_deref(),
                                &name,
                            )?;
                            validate_resource_preconditions(
                                &preconditions,
                                Some(&live_uid),
                                live_rv,
                            )
                            .map_err(Self::sqlite_conversion_error)?;
                            mutations.push(LogApplyMutation::DeleteResource(LogApplyResourceKey {
                                api_version,
                                kind,
                                namespace,
                                name,
                                uid: live_uid,
                                precondition_resource_version: preconditions.resource_version,
                            }));
                        }
                    }
                }
                LogApplyCommit::new(rv, mutations)
            }

            StorageCommand::DeleteResource {
                api_version,
                kind,
                namespace,
                name,
                preconditions,
            } => {
                let (current_rv, current_uid, _data_bytes) = Self::resource_row_for_update_in_tx(
                    tx,
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                )?;
                validate_resource_preconditions(&preconditions, Some(&current_uid), current_rv)
                    .map_err(Self::sqlite_conversion_error)?;
                LogApplyCommit::new(
                    rv,
                    vec![LogApplyMutation::DeleteResource(LogApplyResourceKey {
                        api_version,
                        kind,
                        namespace,
                        name,
                        uid: current_uid,
                        precondition_resource_version: preconditions.resource_version,
                    })],
                )
            }

            StorageCommand::UpdateNodeDataplane {
                node_name,
                mode,
                encryption,
                public_key,
                endpoint,
                port,
            } => {
                // Also stamp routing metadata in the cluster_resource row
                let metadata = crate::networking::wireguard::DataplanePeerMetadata::try_new(
                    node_name.clone(),
                    crate::networking::wireguard::DataplaneMode::parse(&mode)
                        .map_err(Self::sqlite_conversion_error)?,
                    crate::networking::wireguard::DataplaneEncryption::parse(Some(&encryption))
                        .map_err(Self::sqlite_conversion_error)?,
                    public_key.clone(),
                    Some(endpoint.clone()),
                    port,
                )
                .map_err(Self::sqlite_conversion_error)?;
                let stamped_node =
                    Self::node_routing_metadata_resource_row_in_tx(tx, &node_name, &metadata, rv)?;
                let mut mutations = vec![LogApplyMutation::PutNodeDataplane(
                    LogApplyNodeDataplaneRow {
                        node_name,
                        mode,
                        encryption,
                        public_key,
                        endpoint,
                        port,
                    },
                )];
                if let Some(row) = stamped_node {
                    mutations.push(LogApplyMutation::PutResource(row));
                }
                LogApplyCommit::new(rv, mutations)
            }

            StorageCommand::CreateNamespace { name, data } => LogApplyCommit::new(
                rv,
                vec![LogApplyMutation::PutNamespace(LogApplyNamespaceRow {
                    name: name.clone(),
                    uid: String::new(),
                    resource_version: rv,
                    data,
                })],
            ),

            StorageCommand::UpdateNamespace { name, data, .. } => LogApplyCommit::new(
                rv,
                vec![LogApplyMutation::PutNamespace(LogApplyNamespaceRow {
                    name: name.clone(),
                    uid: String::new(),
                    resource_version: rv,
                    data,
                })],
            ),

            StorageCommand::DeleteNamespace { name } => LogApplyCommit::new(
                rv,
                vec![
                    LogApplyMutation::DeleteNamespaceContents { name: name.clone() },
                    LogApplyMutation::DeleteNamespace { name },
                ],
            ),

            StorageCommand::DeleteNamespaceContents { name } => {
                LogApplyCommit::new(rv, vec![LogApplyMutation::DeleteNamespaceContents { name }])
            }

            StorageCommand::PatchResource {
                api_version,
                kind,
                namespace,
                name,
                patch_kind,
                patch,
                preconditions,
                strict_resource_version,
            } => {
                let (live_rv, live_uid, live_data) = Self::resource_row_for_update_in_tx(
                    tx,
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                )?;
                let live: Value =
                    serde_json::from_slice(&live_data).map_err(serde_to_sqlite_error)?;
                let mut effective_preconditions = preconditions.clone();
                if !strict_resource_version
                    && let Some(expected) = effective_preconditions.resource_version
                    && expected != live_rv
                    && crate::resource_semantics::has_builtin_status_subresource(
                        &api_version,
                        &kind,
                    )
                    && let Some(base) = Self::resource_snapshot_for_key_at_rv_in_tx(
                        tx,
                        &api_version,
                        &kind,
                        namespace.as_deref(),
                        &name,
                        expected,
                    )?
                    && metadata_uid(&base) == Some(live_uid.as_str())
                    && resource_client_owned_state_equal(&base, &live)
                {
                    effective_preconditions.resource_version = Some(live_rv);
                }
                validate_resource_preconditions(&effective_preconditions, Some(&live_uid), live_rv)
                    .map_err(Self::sqlite_conversion_error)?;
                if Self::should_apply_outbox_patch_against_latest(
                    &api_version,
                    &kind,
                    patch_kind,
                    &patch,
                    &preconditions,
                ) {
                    let terminating_pod_unready_timestamp =
                        crate::resource_semantics::is_zero_grace_pod_delete_mark_patch(
                            &api_version,
                            &kind,
                            &patch,
                        )
                        .then(crate::utils::k8s_timestamp);
                    return Ok((
                        LogApplyCommit::new(
                            rv,
                            vec![LogApplyMutation::PatchResourceLatest(
                                LogApplyResourcePatch {
                                    api_version,
                                    kind,
                                    namespace,
                                    name,
                                    resource_version: rv,
                                    patch_kind,
                                    patch,
                                    require_existing: true,
                                    precondition_uid: Some(live_uid),
                                    precondition_resource_version: None,
                                    terminating_pod_unready_timestamp,
                                },
                            )],
                        ),
                        rv,
                    ));
                }
                let live_before_patch = live.clone();
                let mut live = live;
                Self::apply_outbox_patch(&api_version, &kind, &mut live, patch_kind, patch)?;
                ensure_resource_type_meta(&mut live, &api_version, &kind);
                ensure_metadata_identity(&mut live, namespace.as_deref(), &name);
                ensure_pod_status_ip_arrays(&mut live, &api_version, &kind);
                crate::resource_semantics::preserve_status_subresource_on_main_update(
                    &api_version,
                    &kind,
                    &live_before_patch,
                    &mut live,
                );
                preserve_server_metadata_fields_from_existing(&mut live, &live_before_patch);
                let uid = ensure_metadata_uid(&mut live);
                LogApplyCommit::new(
                    rv,
                    vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                        api_version,
                        kind,
                        namespace,
                        name,
                        uid,
                        resource_version: rv,
                        data: live,
                        require_absent: false,
                        require_existing: true,
                        precondition_uid: effective_preconditions.uid,
                        precondition_resource_version: effective_preconditions
                            .resource_version
                            .or(Some(live_rv)),
                        status_only: false,
                    })],
                )
            }

            StorageCommand::AllocateNodeSubnet {
                node_name,
                subnet,
                node_ip,
            } => LogApplyCommit::new(
                rv,
                vec![LogApplyMutation::AllocateNodeSubnet(
                    LogApplyNodeSubnetAllocation {
                        node_name,
                        cluster_cidr: subnet,
                        node_ip,
                    },
                )],
            ),

            StorageCommand::DeleteNodeSubnet { node_name } => {
                LogApplyCommit::new(rv, vec![LogApplyMutation::DeleteNodeSubnet { node_name }])
            }

            StorageCommand::UpdateNodeVtepMac {
                node_name: _,
                vtep_mac: _,
            } => {
                // VTEP MAC is a node routing attribute. Store as NodeSubnet-like row
                // so the routing layer picks it up.
                LogApplyCommit::new(rv, Vec::new())
            }

            StorageCommand::UpdateNodePeerAttributes { .. } => {
                // Projected from Node annotations — applied via PutResource on Node.
                LogApplyCommit::new(rv, Vec::new())
            }

            StorageCommand::PodSlotTryAdmit { .. }
            | StorageCommand::PodSlotMarkTerminating { .. }
            | StorageCommand::PodSlotClearIfUid { .. } => {
                // Pod slots are managed by the pod repository actors.
                LogApplyCommit::new(rv, Vec::new())
            }

            StorageCommand::MovePodToCleanupIntent {
                node_name,
                namespace,
                pod_name,
                pod_uid,
                reason,
            } => {
                let (_live_rv, live_uid, pod_bytes) = Self::resource_row_for_update_in_tx(
                    tx,
                    "v1",
                    "Pod",
                    Some(namespace.as_str()),
                    &pod_name,
                )?;
                if live_uid != pod_uid {
                    return Err(tokio_rusqlite::Error::Rusqlite(
                        rusqlite::Error::QueryReturnedNoRows,
                    ));
                }
                let pod_data: Value =
                    serde_json::from_slice(&pod_bytes).map_err(serde_to_sqlite_error)?;
                if pod_data
                    .pointer("/spec/nodeName")
                    .and_then(|value| value.as_str())
                    != Some(node_name.as_str())
                {
                    return Err(tokio_rusqlite::Error::Rusqlite(
                        rusqlite::Error::QueryReturnedNoRows,
                    ));
                }
                let created_at_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                LogApplyCommit::new(
                    rv,
                    vec![
                        LogApplyMutation::PutPodCleanupIntent(LogApplyPodCleanupIntentRow {
                            node_name,
                            namespace: namespace.clone(),
                            pod_name: pod_name.clone(),
                            pod_uid: pod_uid.clone(),
                            reason,
                            resource_version: rv,
                            created_at_ms,
                            pod_data,
                        }),
                        LogApplyMutation::DeleteResource(LogApplyResourceKey {
                            api_version: "v1".to_string(),
                            kind: "Pod".to_string(),
                            namespace: Some(namespace),
                            name: pod_name,
                            uid: live_uid,
                            precondition_resource_version: None,
                        }),
                    ],
                )
            }

            StorageCommand::DeletePodCleanupIntent {
                node_name,
                namespace,
                pod_name,
                pod_uid,
                reason,
            } => LogApplyCommit::new(
                rv,
                vec![LogApplyMutation::DeletePodCleanupIntent(
                    LogApplyPodCleanupIntentKey {
                        node_name,
                        namespace,
                        pod_name,
                        pod_uid,
                        reason,
                    },
                )],
            ),

            StorageCommand::DeletePodCleanupIntentsForNode { node_name } => LogApplyCommit::new(
                rv,
                vec![LogApplyMutation::DeletePodCleanupIntentsForNode { node_name }],
            ),

            StorageCommand::WatchEventAppend {
                event_bytes,
                rv: watch_rv,
            } => {
                // The event_bytes is a JSON WatchEvent; extract fields
                // for the LogApplyWatchEventRow
                let event: Value =
                    serde_json::from_slice(&event_bytes).map_err(serde_to_sqlite_error)?;
                let api_version = event["api_version"].as_str().unwrap_or("").to_string();
                let kind = event["kind"].as_str().unwrap_or("").to_string();
                let namespace = event["namespace"].as_str().map(str::to_string);
                let name = event["name"].as_str().unwrap_or("").to_string();
                let event_type = event["type"].as_str().unwrap_or("ADDED").to_string();
                let data = event["object"].clone();
                LogApplyCommit::new(
                    rv,
                    vec![LogApplyMutation::PutWatchEvent(LogApplyWatchEventRow {
                        api_version,
                        kind,
                        namespace,
                        name,
                        resource_version: watch_rv.max(rv),
                        event_type,
                        data,
                    })],
                )
            }

            StorageCommand::GcWatchEvents {
                max_rows,
                batch_cap,
            } => LogApplyCommit::new(
                rv,
                vec![LogApplyMutation::GcWatchEvents {
                    max_rows,
                    batch_cap,
                }],
            ),

            StorageCommand::AdvanceResourceVersion { min_rv: _, new_rv } => LogApplyCommit::new(
                rv,
                vec![LogApplyMutation::AdvanceResourceVersion {
                    resource_version: new_rv.max(rv),
                }],
            ),
            StorageCommand::EnsureClusterMetadata { cluster_id } => {
                let existing: Option<String> = tx
                    .query_row(
                        crate::datastore::sqlite::queries::SELECT_KLIGHTS_META,
                        [&"cluster_id"],
                        |row| row.get::<_, String>(0),
                    )
                    .ok();
                if existing.is_none() {
                    LogApplyCommit::new(
                        rv,
                        vec![
                            LogApplyMutation::PutKlightsMeta {
                                key: crate::bootstrap::cluster_meta::KEY_CLUSTER_ID.to_string(),
                                value: cluster_id,
                            },
                            LogApplyMutation::PutKlightsMeta {
                                key: crate::bootstrap::cluster_meta::KEY_LEADER_EPOCH.to_string(),
                                value: "0".to_string(),
                            },
                        ],
                    )
                } else {
                    // cluster_id already set — idempotent no-op
                    LogApplyCommit::new(rv, Vec::new())
                }
            }
            StorageCommand::SetKlightsMeta { key, value } => {
                LogApplyCommit::new(rv, vec![LogApplyMutation::PutKlightsMeta { key, value }])
            }
        };

        Ok((commit, rv))
    }

    fn resource_snapshot_for_key_at_rv_in_tx(
        tx: &rusqlite::Transaction<'_>,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        resource_version: i64,
    ) -> tokio_rusqlite::Result<Option<Value>> {
        let earliest: Option<i64> = tx
            .query_row(queries::WATCH_EVENTS_MIN_RV, [], |row| row.get(0))
            .optional()?;
        match earliest {
            Some(earliest) if resource_version + 1 >= earliest => {}
            _ => return Ok(None),
        }

        let namespace_key = namespace.unwrap_or("#cluster");
        let row: Option<(String, Vec<u8>)> = tx
            .query_row(
                "SELECT event_type, data FROM watch_events \
                 WHERE api_version = ?1 \
                   AND kind = ?2 \
                   AND COALESCE(namespace, '#cluster') = ?3 \
                   AND name = ?4 \
                   AND resource_version <= ?5 \
                 ORDER BY resource_version DESC, id DESC \
                 LIMIT 1",
                rusqlite::params![api_version, kind, namespace_key, name, resource_version],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((event_type, bytes)) = row else {
            return Ok(None);
        };
        if event_type == "DELETED" {
            return Ok(None);
        }
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(crate::datastore::sqlite::crud::helpers::serde_to_sqlite_error)
    }

    fn should_apply_outbox_patch_against_latest(
        api_version: &str,
        kind: &str,
        patch_kind: PatchKind,
        patch: &Value,
        preconditions: &ResourcePreconditions,
    ) -> bool {
        if patch_kind != PatchKind::Merge
            || preconditions.uid.is_none()
            || preconditions.resource_version.is_some()
        {
            return false;
        }
        Self::is_unconditional_workload_scale_patch(api_version, kind, patch)
            || Self::is_pod_delete_mark_patch(api_version, kind, patch)
    }

    fn is_unconditional_workload_scale_patch(api_version: &str, kind: &str, patch: &Value) -> bool {
        if !matches!(
            (api_version, kind),
            ("apps/v1", "Deployment")
                | ("apps/v1", "ReplicaSet")
                | ("apps/v1", "StatefulSet")
                | ("v1", "ReplicationController")
        ) {
            return false;
        }
        let Some(patch_obj) = patch.as_object() else {
            return false;
        };
        if patch_obj.len() != 1 {
            return false;
        }
        let Some(spec_obj) = patch_obj.get("spec").and_then(Value::as_object) else {
            return false;
        };
        spec_obj.len() == 1 && spec_obj.contains_key("replicas")
    }

    fn is_pod_delete_mark_patch(api_version: &str, kind: &str, patch: &Value) -> bool {
        crate::resource_semantics::is_pod_delete_mark_patch(api_version, kind, patch)
    }

    fn apply_outbox_patch(
        api_version: &str,
        kind: &str,
        live: &mut Value,
        patch_kind: crate::datastore::types::PatchKind,
        patch: Value,
    ) -> tokio_rusqlite::Result<()> {
        let _ = (api_version, kind);
        let existing = live.clone();
        match patch_kind {
            crate::datastore::types::PatchKind::Merge => {
                crate::json_patch::apply_merge_patch(live, &patch)
                    .map_err(Self::sqlite_conversion_error)?;
            }
        }
        crate::datastore::sqlite::resource_shape::validate_metadata_uid_immutable(live, &existing)
            .map_err(Self::sqlite_conversion_error)?;
        crate::datastore::sqlite::resource_shape::preserve_server_metadata_fields_from_existing(
            live, &existing,
        );
        Ok(())
    }

    // merge_forwarded_lease_with_live is defined later in this file

    fn node_routing_metadata_resource_row_in_tx(
        tx: &rusqlite::Transaction<'_>,
        node_name: &str,
        metadata: &crate::networking::wireguard::DataplanePeerMetadata,
        resource_version: i64,
    ) -> tokio_rusqlite::Result<Option<crate::log_apply::LogApplyResourceRow>> {
        let Some((_current_rv, current_uid, current_bytes)) = tx
            .query_row(
                queries::CLUSTER_GET_DATA_FOR_DELETE,
                rusqlite::params!["v1", "Node", node_name],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                },
            )
            .optional()?
        else {
            return Ok(None);
        };
        let mut node: Value = serde_json::from_slice(&current_bytes)
            .map_err(crate::datastore::sqlite::crud::helpers::serde_to_sqlite_error)?;
        let mut changed = false;
        let pod_cidr = tx
            .query_row(queries::NODE_SUBNET_SELECT_BY_NAME, [node_name], |row| {
                row.get::<_, String>(1)
            })
            .optional()?;
        if let Some(pod_cidr) = pod_cidr.as_deref() {
            changed |= crate::kubelet::node::set_node_pod_cidr(&mut node, pod_cidr);
        }
        changed |=
            crate::kubelet::node::set_node_external_ip(&mut node, &metadata.endpoint.to_string());
        changed |= crate::kubelet::node::set_node_dataplane_annotations(&mut node, metadata);
        if !changed {
            return Ok(None);
        }

        Ok(Some(crate::log_apply::LogApplyResourceRow {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: node_name.to_string(),
            uid: current_uid,
            resource_version,
            data: node,
            require_absent: false,
            require_existing: true,
            precondition_uid: None,
            precondition_resource_version: None,
            status_only: false,
        }))
    }

    fn sqlite_conversion_error(err: anyhow::Error) -> tokio_rusqlite::Error {
        tokio_rusqlite::Error::Rusqlite(rusqlite::Error::ToSqlConversionFailure(Box::new(
            std::io::Error::other(err.to_string()),
        )))
    }

    fn outbox_apply_error_from_db_error(
        err: tokio_rusqlite::Error,
    ) -> crate::kubelet::outbox::OutboxApplyError {
        let msg = err.to_string();
        if msg.contains("409 Conflict") {
            crate::kubelet::outbox::OutboxApplyError::ConflictTerminal(msg)
        } else {
            crate::kubelet::outbox::OutboxApplyError::Retryable(msg)
        }
    }

    fn cached_outbox_terminal_error(
        record: Option<&AppliedOutboxRecord>,
    ) -> std::result::Result<Option<String>, crate::kubelet::outbox::OutboxApplyError> {
        let Some(record) = record else {
            return Ok(None);
        };
        if record.result_proto.is_empty() {
            return Ok(None);
        }
        match crate::datastore::command::decode_response_protobuf(&record.result_proto) {
            Ok(crate::datastore::command::StorageResponse::Error { message }) => Ok(Some(message)),
            Ok(_) => Ok(None),
            Err(err) => Err(crate::kubelet::outbox::OutboxApplyError::Retryable(
                format!("decode cached applied_outbox response: {err}"),
            )),
        }
    }

    fn inflight_outbox_placeholder_error(
        idempotency_key: &str,
    ) -> crate::kubelet::outbox::OutboxApplyError {
        crate::kubelet::outbox::OutboxApplyError::Retryable(format!(
            "outbox idempotency key {idempotency_key} is still in-flight"
        ))
    }

    fn should_apply_outbox_update_against_latest(
        operation: &str,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        authoring_node: &str,
    ) -> bool {
        name == authoring_node
            && ((operation
                == crate::kubelet::outbox::payload::OutboxOperation::NodeStatus.as_str()
                && api_version == "v1"
                && kind == "Node"
                && namespace.is_none())
                || (operation
                    == crate::kubelet::outbox::payload::OutboxOperation::LeaseRenew.as_str()
                    && api_version == "coordination.k8s.io/v1"
                    && kind == "Lease"
                    && namespace == Some("kube-node-lease")))
    }

    /// Reconstruct the applied_outbox `subject_key` for a Pod status command,
    /// matching `subject_key_for_command` so the stale-stamp gate reads the
    /// same ledger rows the outbox apply writes.
    fn pod_status_subject_key(
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        uid: Option<&str>,
    ) -> String {
        let mut key = match namespace {
            Some(namespace) => format!("{api_version}/{kind}/{namespace}/{name}"),
            None => format!("{api_version}/{kind}/{name}"),
        };
        if let Some(uid) = uid.filter(|uid| !uid.is_empty()) {
            key.push('/');
            key.push_str(uid);
        }
        key
    }

    /// Worker-observed status stamp carried by a Pod status outbox command, if
    /// any. Used by the outer apply paths to persist the stamp in the
    /// idempotency ledger so the gate can compare future snapshots.
    fn pod_status_stamp_of(command: &crate::datastore::command::StorageCommand) -> Option<i64> {
        match command {
            crate::datastore::command::StorageCommand::UpdateStatus {
                observed_status_stamp,
                ..
            } => *observed_status_stamp,
            _ => None,
        }
    }

    fn should_apply_outbox_status_against_latest(
        operation: &str,
        api_version: &str,
        kind: &str,
        preconditions: &ResourcePreconditions,
    ) -> bool {
        api_version == "v1"
            && kind == "Pod"
            && preconditions
                .uid
                .as_deref()
                .is_some_and(|uid| !uid.is_empty())
            && matches!(
                operation,
                "PodStatus"
                    | "RuntimeReconcile"
                    | "ProbeReadiness"
                    | "DeadlineExceeded"
                    | "ContainerStatusSnapshot"
                    | "EphemeralContainerStatuses"
            )
    }

    fn resource_row_for_update_in_tx(
        tx: &rusqlite::Transaction<'_>,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> tokio_rusqlite::Result<(i64, String, Vec<u8>)> {
        if use_namespaced_table(api_version, kind, &namespace) {
            tx.query_row(
                queries::NAMESPACED_GET_DATA_FOR_DELETE,
                rusqlite::params![api_version, kind, namespace.unwrap_or("default"), name],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(|err| match err {
                rusqlite::Error::QueryReturnedNoRows => Self::sqlite_conversion_error(anyhow!(
                    "Resource not found: {api_version}/{kind} {}/{}",
                    namespace.unwrap_or("default"),
                    name
                )),
                other => tokio_rusqlite::Error::Rusqlite(other),
            })
        } else {
            tx.query_row(
                queries::CLUSTER_GET_DATA_FOR_DELETE,
                rusqlite::params![api_version, kind, name],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(|err| match err {
                rusqlite::Error::QueryReturnedNoRows => Self::sqlite_conversion_error(anyhow!(
                    "Resource not found: {api_version}/{kind} {name}"
                )),
                other => tokio_rusqlite::Error::Rusqlite(other),
            })
        }
    }

    fn resource_row_optional_for_update_in_tx(
        tx: &rusqlite::Transaction<'_>,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> tokio_rusqlite::Result<Option<(i64, String, Vec<u8>)>> {
        if use_namespaced_table(api_version, kind, &namespace) {
            tx.query_row(
                queries::NAMESPACED_GET_DATA_FOR_DELETE,
                rusqlite::params![api_version, kind, namespace.unwrap_or("default"), name],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(tokio_rusqlite::Error::Rusqlite)
        } else {
            tx.query_row(
                queries::CLUSTER_GET_DATA_FOR_DELETE,
                rusqlite::params![api_version, kind, name],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(tokio_rusqlite::Error::Rusqlite)
        }
    }

    fn merge_forwarded_lease_with_live(live: &Value, incoming: &mut Value) {
        if let Some(metadata) = live.get("metadata").cloned() {
            incoming["metadata"] = metadata;
        }

        let live_renew_time = live.pointer("/spec/renewTime").and_then(|v| v.as_str());
        let incoming_renew_time = incoming.pointer("/spec/renewTime").and_then(|v| v.as_str());
        if Self::lease_renew_time_newer(live_renew_time, incoming_renew_time)
            && let Some(live_renew_time) = live_renew_time
        {
            incoming["spec"]["renewTime"] = Value::String(live_renew_time.to_string());
        }
    }

    fn lease_renew_time_newer(left: Option<&str>, right: Option<&str>) -> bool {
        let Some(left) = left else {
            return false;
        };
        let Some(right) = right else {
            return true;
        };
        match (
            chrono::DateTime::parse_from_rfc3339(left),
            chrono::DateTime::parse_from_rfc3339(right),
        ) {
            (Ok(left), Ok(right)) => left > right,
            _ => left > right,
        }
    }

    pub async fn gc_applied_outbox(&self, now_ms: i64, ttl_ms: i64) -> Result<usize> {
        let cutoff = now_ms.saturating_sub(ttl_ms);
        self.db_call("db_applied_outbox_gc", move |conn| {
            conn.execute(
                queries::APPLIED_OUTBOX_DELETE_EXPIRED,
                rusqlite::params![cutoff],
            )
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|e| anyhow!("applied outbox gc failed: {e}"))
    }

    // -------------------------------------------------------------------
    // Constructors — every path funnels through `from_executor` so the
    // shared body (broadcast channels, schema init if not already done)
    // is never duplicated. DSB-03 makes this the single source of truth.
    // -------------------------------------------------------------------

    /// Shared constructor body called by every public constructor.
    async fn from_executors(
        executor: DbExecutor,
        node_local: crate::datastore::node_local::SqliteNodeLocalDb,
    ) -> Result<Self> {
        let (pod_endpoint_tx, _) = broadcast::channel(POD_ENDPOINT_CHANNEL_BOUND);
        let (pod_slot_admission_tx, _) = broadcast::channel(POD_SLOT_ADMISSION_CHANNEL_BOUND);
        // Schema + fingerprint already applied inside DbExecutor::open_with_opts.
        let ds = Self {
            executor,
            node_local,
            watch_bus: std::sync::Arc::new(WatchBus::new(1024)),
            pod_endpoint_tx,
            pod_slot_admission_tx,
        };
        ds.gc_stale_applied_outbox_placeholders(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
        )
        .await?;
        Ok(ds)
    }

    async fn open_node_local_sqlite(
        path: Option<&std::path::Path>,
        supervisor: std::sync::Arc<TaskSupervisor>,
        key_file: Option<&std::path::Path>,
        connection_key: &'static str,
    ) -> Result<crate::datastore::node_local::SqliteNodeLocalDb> {
        let opts = match path {
            Some(path) => opener::OpenOpts::node_disk(path.to_path_buf()),
            None => opener::OpenOpts::node_in_memory(),
        }
        .with_key_file(key_file)?;
        let executor = DbExecutor::open_with_opts(opts, supervisor, connection_key).await?;
        crate::datastore::node_local::SqliteNodeLocalDb::from_executor(executor)
    }

    async fn from_executor(executor: DbExecutor) -> Result<Self> {
        let node_local = Self::open_node_local_sqlite(
            None,
            executor.task_supervisor(),
            None,
            "sqlite:node-local-memory",
        )
        .await?;
        Self::from_executors(executor, node_local).await
    }

    /// Production constructor — open a persistent on-disk database.
    ///
    /// Opens explicit cluster and node-local SQLite database files.
    ///
    /// If `key_file` is `Some`, the DB is opened with SQLCipher encryption
    /// (requires the `sqlcipher` cargo feature).
    pub async fn new_persistent_paths(
        cluster_db_path: &std::path::Path,
        node_db_path: &std::path::Path,
        supervisor: std::sync::Arc<TaskSupervisor>,
        key_file: Option<&std::path::Path>,
    ) -> Result<Self> {
        let db_path = cluster_db_path.to_path_buf();
        let opts = opener::OpenOpts::disk(db_path.clone()).with_key_file(key_file)?;
        if let Some(kf) = key_file {
            tracing::info!(
                key_file = %kf.display(),
                "opening encrypted datastore"
            );
        }

        let executor = DbExecutor::open_with_opts(opts, supervisor.clone(), "sqlite:cluster")
            .await
            .map_err(|e| {
                anyhow!(
                    "failed to open persistent cluster datastore at {}: {}",
                    db_path.display(),
                    e
                )
            })?;
        let node_local = Self::open_node_local_sqlite(
            Some(node_db_path),
            supervisor,
            key_file,
            "sqlite:node-local",
        )
        .await
        .map_err(|e| {
            anyhow!(
                "failed to open persistent node-local datastore at {}: {}",
                node_db_path.display(),
                e
            )
        })?;
        let ds = Self::from_executors(executor, node_local).await?;

        // Log DB size at startup for operator triage (DSB-05).
        let db_size = std::fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0);
        let mut wal_path = db_path.as_os_str().to_owned();
        wal_path.push("-wal");
        let wal_path = std::path::PathBuf::from(wal_path);
        let wal_size = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
        tracing::info!(
            db_path = %db_path.display(),
            db_size_bytes = db_size,
            wal_size_bytes = wal_size,
            total_kb = (db_size + wal_size) / 1024,
            "persistent datastore opened"
        );

        Ok(ds)
    }

    /// Compatibility constructor for tests and helper call sites that still pass
    /// the DB root. Production bootstrap uses `new_persistent_paths`.
    #[cfg(test)]
    pub async fn new_persistent(
        db_root: &std::path::Path,
        supervisor: std::sync::Arc<TaskSupervisor>,
        key_file: Option<&std::path::Path>,
    ) -> Result<Self> {
        Self::new_persistent_paths(
            &db_root.join("sqlite").join("cluster.db"),
            &db_root.join("sqlite").join("node.db"),
            supervisor,
            key_file,
        )
        .await
    }

    /// Test-only convenience constructor.
    #[cfg(test)]
    pub async fn new_in_memory() -> Result<Self> {
        let supervisor = std::sync::Arc::new(TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let executor =
            DbExecutor::open_in_memory(supervisor.clone(), "sqlite:memory:cluster").await?;
        let node_local =
            Self::open_node_local_sqlite(None, supervisor, None, "sqlite:node-local-memory")
                .await?;
        Self::from_executors(executor, node_local).await
    }

    /// Shared production + test constructor when an externally-created
    /// `DbExecutor` is already available (in-memory or persistent).
    pub async fn new_in_memory_with_watch_and_executor(executor: DbExecutor) -> Result<Self> {
        Self::from_executor(executor).await
    }
}

#[async_trait]
impl DatastoreBackend for Datastore {
    #[cfg(test)]
    async fn seed_namespace_for_test(&self, name: &str) {
        Datastore::seed_namespace_no_rv(self, name)
            .await
            .expect("seed namespace for test");
    }

    fn subscribe_watch_signals(&self, topic: WatchTopic) -> broadcast::Receiver<WatchSignal> {
        Datastore::subscribe_watch_signals(self, topic)
    }

    #[cfg(test)]
    fn subscribe_watch(&self, topic: WatchTopic) -> broadcast::Receiver<crate::watch::WatchEvent> {
        Datastore::subscribe_watch(self, topic)
    }

    #[cfg(test)]
    fn subscribe_watch_many(&self, topics: Vec<WatchTopic>) -> crate::watch::WatchReceiver {
        Datastore::subscribe_watch_many(self, topics)
    }

    #[cfg(test)]
    fn broadcast_watch_event(&self, pending: PendingWatchEvent) {
        Datastore::broadcast_watch_event(self, pending);
    }

    async fn replace_replicated_resource_state(
        &self,
        entries: Vec<crate::log_apply::LogApplyCommit>,
        current_rv: i64,
        metadata: Option<ReplicatedSnapshotMetadata>,
    ) -> Result<()> {
        Datastore::replace_replicated_resource_state(self, entries, current_rv, metadata).await
    }

    async fn apply_log_apply_commit(&self, commit: crate::log_apply::LogApplyCommit) -> Result<()> {
        Datastore::apply_log_apply_commit(self, commit).await
    }

    async fn apply_raft_log_apply_commit(
        &self,
        commit: crate::log_apply::LogApplyCommit,
    ) -> Result<crate::datastore::raft::types::StorageCommandResult> {
        Datastore::apply_raft_log_apply_commit(self, commit).await
    }

    async fn create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
    ) -> Result<Resource> {
        Datastore::create_resource(self, api_version, kind, namespace, name, data).await
    }

    async fn apply_replicated_create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        options: ReplicatedCreateOptions,
    ) -> Result<Resource> {
        Datastore::apply_replicated_create_resource(
            self,
            api_version,
            kind,
            namespace,
            name,
            data,
            options,
        )
        .await
    }

    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        Datastore::get_resource(self, api_version, kind, namespace, name).await
    }

    async fn list_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        query: ResourceListQuery<'_>,
    ) -> Result<ResourceList> {
        Datastore::list_resources(self, api_version, kind, namespace, query).await
    }

    async fn list_resources_page(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
        Datastore::list_resources_page(
            self,
            api_version,
            kind,
            namespace,
            label_selector,
            field_selector,
            page,
        )
        .await
    }

    async fn snapshot_resources_at_rv(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        query: ResourceListQuery<'_>,
        snapshot_rv: i64,
    ) -> Result<crate::datastore::types::SnapshotAtRv> {
        Datastore::snapshot_resources_at_rv(self, api_version, kind, namespace, query, snapshot_rv)
            .await
    }

    async fn list_resource_keys_for_scope(
        &self,
        api_version: String,
        kind: String,
        namespaced: bool,
    ) -> Result<Vec<(Option<String>, String)>> {
        Datastore::list_resource_keys_for_scope(self, api_version, kind, namespaced).await
    }

    async fn update_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        Datastore::update_resource(self, api_version, kind, namespace, name, data, expected_rv)
            .await
    }

    async fn update_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        Datastore::update_resource_with_preconditions(
            self,
            api_version,
            kind,
            namespace,
            name,
            data,
            preconditions,
        )
        .await
    }

    async fn update_main_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        Datastore::update_main_resource_with_preconditions(
            self,
            api_version,
            kind,
            namespace,
            name,
            data,
            preconditions,
        )
        .await
    }

    async fn apply_resource_batch(&self, operations: Vec<ResourceBatchOperation>) -> Result<()> {
        Datastore::apply_resource_batch(self, operations).await
    }

    async fn update_status_only(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        Datastore::update_status_only(
            self,
            api_version,
            kind,
            namespace,
            name,
            status,
            expected_rv,
        )
        .await
    }

    async fn update_status_only_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        Datastore::update_status_only_with_preconditions(
            self,
            api_version,
            kind,
            namespace,
            name,
            status,
            preconditions,
        )
        .await
    }

    async fn delete_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<()> {
        Datastore::delete_resource(self, api_version, kind, namespace, name).await
    }

    async fn delete_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<()> {
        Datastore::delete_resource_with_preconditions(
            self,
            api_version,
            kind,
            namespace,
            name,
            preconditions,
        )
        .await
    }

    async fn delete_resource_with_preconditions_observed_rv(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<i64> {
        Datastore::delete_resource_with_preconditions_observed_rv(
            self,
            api_version,
            kind,
            namespace,
            name,
            preconditions,
        )
        .await
    }

    async fn get_current_resource_version(&self) -> Result<i64> {
        Datastore::get_current_resource_version(self).await
    }

    async fn create_namespace(&self, name: &str, data: Value) -> Result<Resource> {
        Datastore::create_namespace(self, name, data).await
    }

    async fn get_namespace(&self, name: &str) -> Result<Option<Resource>> {
        Datastore::get_namespace(self, name).await
    }

    async fn list_namespaces(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
    ) -> Result<ResourceList> {
        Datastore::list_namespaces(self, label_selector, field_selector).await
    }

    async fn list_namespaces_page(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
        Datastore::list_namespaces_page(self, label_selector, field_selector, page).await
    }

    async fn update_namespace(
        &self,
        name: &str,
        data: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        Datastore::update_namespace(self, name, data, expected_rv).await
    }

    async fn delete_namespace_contents(&self, name: &str) -> Result<()> {
        Datastore::delete_namespace_contents(self, name).await
    }

    async fn delete_namespace(&self, name: &str) -> Result<()> {
        Datastore::delete_namespace(self, name).await
    }

    async fn delete_namespace_observed_rv(&self, name: &str) -> Result<i64> {
        Datastore::delete_namespace_observed_rv(self, name).await
    }

    async fn pod_workqueue_enqueue(
        &self,
        kind: PodWorkqueueKind,
        pod: &crate::pod_identity::PodIdentity,
        payload: Value,
        attempt_count: i64,
        min_delay_ms: i64,
        last_error: Option<&str>,
    ) -> Result<()> {
        Datastore::pod_workqueue_enqueue(
            self,
            kind,
            pod,
            payload,
            attempt_count,
            min_delay_ms,
            last_error,
        )
        .await
    }

    async fn pod_workqueue_peek_next_due(&self) -> Result<Option<i64>> {
        Datastore::pod_workqueue_peek_next_due(self).await
    }

    async fn pod_workqueue_claim_due(&self, now_ms: i64) -> Result<Option<PodWorkqueueEntry>> {
        Datastore::pod_workqueue_claim_due(self, now_ms).await
    }

    async fn pod_workqueue_complete(&self, id: i64) -> Result<()> {
        Datastore::pod_workqueue_complete(self, id).await
    }

    async fn pod_workqueue_record_failure(
        &self,
        row: PodWorkqueueEntry,
        min_delay_ms: i64,
        error: &str,
    ) -> Result<()> {
        Datastore::pod_workqueue_record_failure(self, row, min_delay_ms, error).await
    }

    async fn pod_workqueue_dead_letter(&self, id: i64, error: &str) -> Result<()> {
        Datastore::pod_workqueue_dead_letter(self, id, error).await
    }

    async fn record_sandbox(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        sandbox_id: &str,
    ) -> Result<()> {
        Datastore::record_sandbox(self, namespace, pod_name, pod_uid, sandbox_id).await
    }

    async fn get_sandbox(&self, namespace: &str, pod_name: &str) -> Result<Option<String>> {
        Datastore::get_sandbox(self, namespace, pod_name).await
    }

    async fn get_sandbox_for_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
    ) -> Result<Option<String>> {
        if pod_uid.is_empty() {
            Datastore::get_sandbox(self, namespace, pod_name).await
        } else {
            Datastore::get_sandbox_for_uid(self, namespace, pod_name, pod_uid).await
        }
    }

    async fn delete_sandbox(&self, namespace: &str, pod_name: &str) -> Result<()> {
        Datastore::delete_sandbox(self, namespace, pod_name).await
    }

    async fn delete_sandbox_for_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        sandbox_id: &str,
    ) -> Result<()> {
        Datastore::delete_sandbox_for_uid(self, namespace, pod_name, pod_uid, sandbox_id).await
    }

    async fn delete_pod_network(&self, sandbox_id: &str) -> Result<()> {
        Datastore::delete_pod_network(self, sandbox_id).await
    }

    async fn find_owned_resources(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        Datastore::find_owned_resources(self, owner_uid, namespace).await
    }

    async fn list_resources_by_owner_uid(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        owner_uid: &str,
    ) -> Result<Vec<Resource>> {
        Datastore::list_resources_by_owner_uid(self, api_version, kind, namespace, owner_uid).await
    }

    async fn find_owned_by_name_kind_empty_uid(
        &self,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        Datastore::find_owned_by_name_kind_empty_uid(
            self,
            owner_api_version,
            owner_name,
            owner_kind,
            namespace,
        )
        .await
    }

    async fn list_cluster_resources_modified_since(
        &self,
        api_version: &str,
        kind: &str,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        Datastore::list_cluster_resources_modified_since(self, api_version, kind, since_rv).await
    }

    async fn list_cluster_resources(&self) -> Result<Vec<Resource>> {
        Datastore::list_cluster_resources(self).await
    }

    async fn list_resources_modified_since(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        Datastore::list_resources_modified_since(self, api_version, kind, namespace, since_rv).await
    }

    async fn advance_resource_version_after(&self, min_rv: i64) -> Result<i64> {
        Datastore::advance_resource_version_after(self, min_rv).await
    }

    async fn list_namespace_resources(&self, namespace: &str) -> Result<Vec<Resource>> {
        Datastore::list_namespace_resources(self, namespace).await
    }

    async fn list_namespace_resources_of_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        Datastore::list_namespace_resources_of_kind(self, namespace, kind).await
    }

    async fn list_namespace_resources_excluding_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        Datastore::list_namespace_resources_excluding_kind(self, namespace, kind).await
    }

    async fn count_namespace_resources(&self, namespace: &str) -> Result<i64> {
        Datastore::count_namespace_resources(self, namespace).await
    }

    async fn list_watch_events_since(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        Datastore::list_watch_events_since(self, targets, since_rv).await
    }

    async fn list_watch_events_since_checked(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<WatchReplayRead> {
        Datastore::list_watch_events_since_checked(self, targets, since_rv).await
    }

    async fn list_watch_events_since_checked_bounded(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead> {
        Datastore::list_watch_events_since_checked_bounded(self, targets, since_rv, limit).await
    }

    async fn earliest_watch_event_rv(&self) -> Result<Option<i64>> {
        Datastore::earliest_watch_event_rv(self).await
    }

    async fn list_all_watch_events_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>> {
        Datastore::list_all_watch_events_since(self, since_rv).await
    }

    async fn list_deleted_watch_events_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>> {
        Datastore::list_deleted_watch_events_since(self, since_rv).await
    }

    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<NodeSubnet> {
        Datastore::allocate_node_subnet(self, node_name, cluster_cidr, node_ip).await
    }

    async fn update_node_vtep_mac(&self, node_name: &str, vtep_mac: &VtepMac) -> Result<()> {
        Datastore::update_node_vtep_mac(self, node_name, vtep_mac).await
    }

    async fn update_node_peer_attributes(
        &self,
        node_name: &str,
        mode: crate::controllers::annotations::NodePeerMode,
        hostport_range: Option<crate::networking::types::HostPortRange>,
    ) -> Result<()> {
        Datastore::update_node_peer_attributes(self, node_name, mode, hostport_range).await
    }

    async fn update_node_dataplane(
        &self,
        metadata: crate::networking::wireguard::DataplanePeerMetadata,
    ) -> Result<()> {
        Datastore::update_node_dataplane(self, metadata).await
    }

    async fn get_node_dataplane(
        &self,
        node_name: &str,
    ) -> Result<Option<crate::networking::wireguard::DataplanePeerMetadata>> {
        Datastore::get_node_dataplane(self, node_name).await
    }

    async fn get_node_subnet(&self, node_name: &str) -> Result<Option<NodeSubnet>> {
        Datastore::get_node_subnet(self, node_name).await
    }

    async fn list_peer_subnets(&self, my_node_name: &str) -> Result<Vec<NodeSubnet>> {
        Datastore::list_peer_subnets(self, my_node_name).await
    }

    async fn delete_node_subnet(&self, node_name: &str) -> Result<()> {
        Datastore::delete_node_subnet(self, node_name).await
    }

    async fn move_pod_to_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        Datastore::move_pod_to_cleanup_intent(self, node_name, namespace, pod_name, pod_uid, reason)
            .await
    }

    async fn list_pod_cleanup_intents_for_node(
        &self,
        node_name: &str,
    ) -> Result<Vec<PodCleanupIntent>> {
        Datastore::list_pod_cleanup_intents_for_node(self, node_name).await
    }

    async fn delete_pod_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        Datastore::delete_pod_cleanup_intent(self, node_name, namespace, pod_name, pod_uid, reason)
            .await
    }

    async fn delete_pod_cleanup_intents_for_node(&self, node_name: &str) -> Result<()> {
        Datastore::delete_pod_cleanup_intents_for_node(self, node_name).await
    }

    async fn pod_slot_try_admit(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
    ) -> Result<PodSlotAdmissionResult> {
        Datastore::pod_slot_try_admit(self, namespace, pod_name, pod_uid, node_name).await
    }

    async fn pod_slot_mark_terminating(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
    ) -> Result<()> {
        Datastore::pod_slot_mark_terminating(self, namespace, pod_name, pod_uid, node_name).await
    }

    async fn pod_slot_clear_if_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
    ) -> Result<()> {
        Datastore::pod_slot_clear_if_uid(self, namespace, pod_name, pod_uid, node_name).await
    }

    fn subscribe_pod_slot_admissions(&self) -> broadcast::Receiver<PodSlotAdmissionEvent> {
        Datastore::subscribe_pod_slot_admissions(self)
    }

    async fn patch_resource_latest(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        patch_kind: PatchKind,
        patch: Value,
    ) -> Result<Option<Resource>> {
        Datastore::patch_resource_latest(
            self,
            api_version,
            kind,
            namespace,
            name,
            patch_kind,
            patch,
        )
        .await
    }

    async fn patch_resource_latest_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        request: ResourcePatchRequest,
    ) -> Result<Option<Resource>> {
        Datastore::patch_resource_latest_with_preconditions(
            self,
            api_version,
            kind,
            namespace,
            name,
            request,
        )
        .await
    }

    async fn get_pod_network(&self, sandbox_id: &str) -> Result<Option<PodNetworkEndpoint>> {
        Datastore::get_pod_network(self, sandbox_id).await
    }

    async fn get_pod_network_for_pod(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
    ) -> Result<Option<PodNetworkEndpoint>> {
        Datastore::get_pod_network_for_pod(self, namespace, pod_name, pod_uid).await
    }

    async fn ipam_allocate_and_record_pod_network(
        &self,
        sandbox_id: &str,
        pod: &crate::pod_identity::PodIdentity,
        subnet_base_int: u32,
        subnet_size: u32,
        veth_host: &str,
        netns_path: &str,
    ) -> Result<(String, u32)> {
        Datastore::ipam_allocate_and_record_pod_network(
            self,
            sandbox_id,
            pod,
            subnet_base_int,
            subnet_size,
            veth_host,
            netns_path,
        )
        .await
    }

    async fn list_sandboxes(&self) -> Result<Vec<SandboxRef>> {
        Datastore::list_sandboxes(self).await
    }

    async fn list_pod_network_sandbox_ids(&self) -> Result<Vec<String>> {
        Datastore::list_pod_network_sandbox_ids(self).await
    }

    async fn watch_events_gc_prunable_count(&self, max_rows: i64, batch_cap: i64) -> Result<usize> {
        Datastore::watch_events_gc_prunable_count(self, max_rows, batch_cap).await
    }

    async fn gc_watch_events(&self, max_rows: i64, batch_cap: i64) -> Result<usize> {
        Datastore::gc_watch_events(self, max_rows, batch_cap).await
    }

    async fn pod_endpoint_get_by_pod_ip(
        &self,
        pod_ip: std::net::Ipv4Addr,
    ) -> Result<Option<PodEndpointRow>> {
        Datastore::pod_endpoint_get_by_pod_ip(self, pod_ip).await
    }

    async fn pod_endpoint_list_all(&self) -> Result<Vec<PodEndpointRow>> {
        Datastore::pod_endpoint_list_all(self).await
    }

    fn subscribe_pod_endpoints(&self) -> broadcast::Receiver<PodEndpointEvent> {
        Datastore::subscribe_pod_endpoints(self)
    }

    async fn get_klights_meta(&self, key: &str) -> anyhow::Result<Option<String>> {
        let key = key.to_string();
        self.db_call("get_klights_meta", move |conn| {
            use rusqlite::OptionalExtension;
            Ok(conn
                .query_row(queries::SELECT_KLIGHTS_META, [&key], |row| {
                    row.get::<_, String>(0)
                })
                .optional()?)
        })
        .await
        .map_err(|e| anyhow::anyhow!("get_klights_meta failed: {}", e))
    }

    async fn set_klights_meta(&self, key: &str, value: &str) -> anyhow::Result<()> {
        let key = key.to_string();
        let value = value.to_string();
        self.db_call("set_klights_meta", move |conn| {
            conn.execute(
                queries::UPSERT_KLIGHTS_META,
                rusqlite::params![&key, &value],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("set_klights_meta failed: {}", e))
    }

    async fn get_applied_outbox(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<AppliedOutboxRecord>> {
        Datastore::get_applied_outbox(self, idempotency_key).await
    }

    async fn insert_applied_outbox(&self, record: AppliedOutboxRecord) -> Result<bool> {
        Datastore::insert_applied_outbox(self, record).await
    }

    async fn list_applied_outbox(&self) -> Result<Vec<AppliedOutboxRecord>> {
        Datastore::list_applied_outbox(self).await
    }

    async fn delete_uncommitted_applied_outbox_placeholder(
        &self,
        idempotency_key: &str,
        reserved_rv: i64,
    ) -> Result<bool> {
        Datastore::delete_uncommitted_applied_outbox_placeholder(self, idempotency_key, reserved_rv)
            .await
    }

    async fn apply_outbox_transactionally(
        &self,
        idempotency_key: &str,
        operation: &str,
        payload: &[u8],
        authoring_node: &str,
    ) -> std::result::Result<
        crate::kubelet::outbox::OutboxApplyResult,
        crate::kubelet::outbox::OutboxApplyError,
    > {
        Datastore::apply_outbox_transactionally(
            self,
            idempotency_key,
            operation,
            payload,
            authoring_node,
        )
        .await
    }

    async fn build_log_apply_commit_for_outbox(
        &self,
        idempotency_key: &str,
        operation: &str,
        payload: &[u8],
        authoring_node: &str,
    ) -> std::result::Result<BuildOutboxOutcome, crate::kubelet::outbox::OutboxApplyError> {
        Datastore::build_log_apply_commit_for_outbox(
            self,
            idempotency_key,
            operation,
            payload,
            authoring_node,
        )
        .await
    }

    async fn gc_applied_outbox(&self, now_ms: i64, ttl_ms: i64) -> Result<usize> {
        Datastore::gc_applied_outbox(self, now_ms, ttl_ms).await
    }

    async fn current_log_apply_index(&self) -> Result<i64> {
        Datastore::current_log_apply_index(self).await
    }
}
