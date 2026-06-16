mod queries;
pub mod schema;

use anyhow::{Result, anyhow};
use rusqlite::OptionalExtension;
use serde_json::Value;
use tokio::sync::broadcast;

use crate::datastore::command::{StorageCommand, decode_command_protobuf};
use crate::datastore::sqlite::DbExecutor;
use crate::datastore::{
    PodEndpointEvent, PodEndpointMode, PodEndpointRow, PodNetworkAllocationRequest,
    PodNetworkEndpoint, PodSlotAdmissionEvent, PodWorkqueueEntry, PodWorkqueueKind,
};

const POD_ENDPOINT_CHANNEL_BOUND: usize = 4_096;
const POD_SLOT_ADMISSION_CHANNEL_BOUND: usize = 4_096;

#[derive(Clone)]
pub struct SqliteNodeLocalDb {
    executor: DbExecutor,
    pod_endpoint_tx: broadcast::Sender<PodEndpointEvent>,
    pod_slot_admission_tx: broadcast::Sender<PodSlotAdmissionEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodRuntimeRow {
    pub pod_uid: String,
    pub namespace: String,
    pub pod_name: String,
    pub node_name: String,
    pub sandbox_id: Option<String>,
    pub cgroup_path: Option<String>,
    pub created_ms: i64,
    pub started_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PodStatusCheckpoint {
    pub pod_uid: String,
    pub namespace: String,
    pub pod_name: String,
    pub base_rv: i64,
    pub applied_rv: Option<i64>,
    pub status: Value,
    pub updated_ms: i64,
}

/// Node-local snapshot of runtime reconcile observations.
///
/// Mirrors `RuntimeReconcileObservations` (kubelet/pod_runtime/observations.rs)
/// but persisted to node.db so CRI events observed for a Pod UID survive an
/// actor or worker restart when CRI/containerd may have already dropped the
/// short-lived container details. UID-bound and node-local only; never
/// replicated through cluster.db or raft.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeObservationCheckpoint {
    pub pod_uid: String,
    pub container_ids: Vec<String>,
    pub generation: u64,
    pub updated_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxInsert {
    pub idempotency_key: String,
    pub enqueued_ms: i64,
    pub subject_key: String,
    pub subject_api_version: String,
    pub subject_kind: String,
    pub subject_namespace: Option<String>,
    pub subject_name: String,
    pub subject_uid: Option<String>,
    pub pod_uid: String,
    pub operation: String,
    pub payload_proto: Vec<u8>,
    pub next_due_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxRow {
    pub id: i64,
    pub idempotency_key: String,
    pub enqueued_ms: i64,
    pub subject_key: String,
    pub subject_api_version: String,
    pub subject_kind: String,
    pub subject_namespace: Option<String>,
    pub subject_name: String,
    pub subject_uid: Option<String>,
    pub pod_uid: String,
    pub operation: String,
    pub is_terminal_pod_delete: bool,
    pub payload_proto: Vec<u8>,
    pub attempt: i64,
    pub next_due_ms: i64,
    pub leased_until_ms: i64,
    pub lease_token: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeStateRow {
    pub pod_uid: String,
    pub container_name: String,
    pub probe_kind: String,
    pub last_result_ms: Option<i64>,
    pub last_success: Option<bool>,
    pub consecutive_fail: i64,
    pub next_eligible_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationCheckpoint {
    pub last_applied_rv: i64,
    pub leader_epoch: i64,
    pub cluster_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct DeadLetterRow {
    pub id: i64,
    pub original_id: i64,
    pub idempotency_key: String,
    pub enqueued_ms: i64,
    pub subject_key: String,
    pub subject_api_version: String,
    pub subject_kind: String,
    pub subject_namespace: Option<String>,
    pub subject_name: String,
    pub subject_uid: Option<String>,
    pub pod_uid: String,
    pub operation: String,
    pub payload_proto: Vec<u8>,
    pub attempts: i64,
    pub last_error: String,
    pub moved_at_ms: i64,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetterTestInsert<'a> {
    pub idempotency_key: &'a str,
    pub operation: &'a str,
    pub subject_key: &'a str,
    pub subject_api_version: &'a str,
    pub subject_kind: &'a str,
    pub subject_namespace: Option<&'a str>,
    pub subject_name: &'a str,
    pub subject_uid: Option<&'a str>,
    pub pod_uid: &'a str,
    pub payload_proto: &'a [u8],
    pub attempts: i64,
    pub last_error: &'a str,
    pub moved_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub struct OutboxStats {
    pub pending: i64,
    pub oldest_age_seconds: f64,
    pub dead_letter_count: i64,
    pub dispatch_total: i64,
    pub dispatch_errors_total: i64,
}

impl SqliteNodeLocalDb {
    pub fn from_executor(executor: DbExecutor) -> Result<Self> {
        let (pod_endpoint_tx, _) = broadcast::channel(POD_ENDPOINT_CHANNEL_BOUND);
        let (pod_slot_admission_tx, _) = broadcast::channel(POD_SLOT_ADMISSION_CHANNEL_BOUND);
        Ok(Self {
            executor,
            pod_endpoint_tx,
            pod_slot_admission_tx,
        })
    }

    pub async fn db_call<T, F>(&self, query_name: &'static str, f: F) -> tokio_rusqlite::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut rusqlite::Connection) -> tokio_rusqlite::Result<T> + Send + 'static,
    {
        self.executor.call_raw(query_name, f).await
    }

    pub fn subscribe_pod_endpoints(&self) -> broadcast::Receiver<PodEndpointEvent> {
        self.pod_endpoint_tx.subscribe()
    }

    pub fn subscribe_pod_slot_admissions(&self) -> broadcast::Receiver<PodSlotAdmissionEvent> {
        self.pod_slot_admission_tx.subscribe()
    }

    pub async fn ensure_node_identity(&self, cluster_id: &str, node_uid: &str) -> Result<()> {
        let cluster_id = cluster_id.to_string();
        let node_uid = node_uid.to_string();
        self.db_call("node_local:ensure_identity", move |conn| {
            ensure_meta_matches_or_insert(conn, "cluster_id", &cluster_id)?;
            ensure_meta_matches_or_insert(conn, "node_uid", &node_uid)?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("node.db identity check failed: {e}"))
    }

    pub async fn get_meta(&self, key: &str) -> Result<Option<String>> {
        let key = key.to_string();
        self.db_call("node_local:get_meta", move |conn| {
            conn.query_row(queries::META_GET, [key], |row| row.get(0))
                .optional()
                .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|e| anyhow!("node meta get failed: {e}"))
    }

    pub async fn set_meta(&self, key: &str, value: &str) -> Result<()> {
        let key = key.to_string();
        let value = value.to_string();
        self.db_call("node_local:set_meta", move |conn| {
            conn.execute(queries::META_SET, rusqlite::params![key, value])?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("node meta set failed: {e}"))
    }

    pub async fn admit_pod_runtime(
        &self,
        pod_uid: &str,
        namespace: &str,
        pod_name: &str,
        node_name: &str,
    ) -> Result<()> {
        let pod_uid = pod_uid.to_string();
        let namespace = namespace.to_string();
        let pod_name = pod_name.to_string();
        let node_name = node_name.to_string();
        let now = now_ms();
        self.db_call("node_local:pod_runtime_admit", move |conn| {
            conn.execute(
                queries::POD_RUNTIME_ADMIT,
                rusqlite::params![pod_uid, namespace, pod_name, node_name, now],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("pod_runtime admit failed: {e}"))
    }

    pub async fn record_sandbox(&self, pod_uid: &str, sandbox_id: &str) -> Result<()> {
        let pod_uid = pod_uid.to_string();
        let sandbox_id = sandbox_id.to_string();
        self.db_call("node_local:pod_runtime_record_sandbox", move |conn| {
            conn.execute(
                queries::POD_RUNTIME_RECORD_SANDBOX,
                rusqlite::params![pod_uid, sandbox_id],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("pod_runtime record sandbox failed: {e}"))
    }

    pub async fn record_cgroup(&self, pod_uid: &str, cgroup_path: &str) -> Result<()> {
        let pod_uid = pod_uid.to_string();
        let cgroup_path = cgroup_path.to_string();
        self.db_call("node_local:pod_runtime_record_cgroup", move |conn| {
            conn.execute(
                queries::POD_RUNTIME_RECORD_CGROUP,
                rusqlite::params![pod_uid, cgroup_path],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("pod_runtime record cgroup failed: {e}"))
    }

    pub async fn delete_pod_runtime_for_uid(&self, pod_uid: &str) -> Result<()> {
        let pod_uid = pod_uid.to_string();
        self.db_call("node_local:pod_runtime_delete", move |conn| {
            conn.execute(queries::POD_RUNTIME_DELETE_UID, [pod_uid])?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("pod_runtime delete failed: {e}"))
    }

    pub async fn get_pod_runtime(&self, pod_uid: &str) -> Result<Option<PodRuntimeRow>> {
        let pod_uid = pod_uid.to_string();
        self.db_call("node_local:pod_runtime_get", move |conn| {
            conn.query_row(queries::POD_RUNTIME_GET_UID, [pod_uid], row_to_pod_runtime)
                .optional()
                .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|e| anyhow!("pod_runtime get failed: {e}"))
    }

    pub async fn list_pod_runtime(&self) -> Result<Vec<PodRuntimeRow>> {
        self.db_call("node_local:pod_runtime_list", move |conn| {
            let mut stmt = conn.prepare(queries::POD_RUNTIME_LIST)?;
            let rows = stmt
                .query_map([], row_to_pod_runtime)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .map_err(|e| anyhow!("pod_runtime list failed: {e}"))
    }

    pub async fn list_pod_runtime_by_namespace(
        &self,
        namespace: &str,
    ) -> Result<Vec<PodRuntimeRow>> {
        let namespace = namespace.to_string();
        self.db_call("node_local:pod_runtime_list_ns", move |conn| {
            let mut stmt = conn.prepare(queries::POD_RUNTIME_LIST_NS)?;
            let rows = stmt
                .query_map([namespace], row_to_pod_runtime)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .map_err(|e| anyhow!("pod_runtime list namespace failed: {e}"))
    }

    pub async fn upsert_pod_status_checkpoint(
        &self,
        pod_uid: &str,
        namespace: &str,
        pod_name: &str,
        base_rv: i64,
        status: Value,
        updated_ms: i64,
    ) -> Result<()> {
        let pod_uid = pod_uid.to_string();
        let namespace = namespace.to_string();
        let pod_name = pod_name.to_string();
        let status_json = serde_json::to_vec(&status)?;
        self.db_call("node_local:pod_status_checkpoint_upsert", move |conn| {
            conn.execute(
                queries::POD_STATUS_CHECKPOINT_UPSERT,
                rusqlite::params![
                    pod_uid,
                    namespace,
                    pod_name,
                    base_rv,
                    status_json,
                    updated_ms
                ],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("pod_status_checkpoint upsert failed: {e}"))
    }

    pub async fn get_pod_status_checkpoint(
        &self,
        pod_uid: &str,
    ) -> Result<Option<PodStatusCheckpoint>> {
        let pod_uid = pod_uid.to_string();
        self.db_call("node_local:pod_status_checkpoint_get", move |conn| {
            conn.query_row(
                queries::POD_STATUS_CHECKPOINT_GET_UID,
                [pod_uid],
                row_to_pod_status_checkpoint,
            )
            .optional()
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|e| anyhow!("pod_status_checkpoint get failed: {e}"))
    }

    pub async fn mark_pod_status_checkpoint_applied(
        &self,
        pod_uid: &str,
        applied_rv: i64,
        updated_ms: i64,
    ) -> Result<()> {
        let pod_uid = pod_uid.to_string();
        self.db_call(
            "node_local:pod_status_checkpoint_mark_applied",
            move |conn| {
                conn.execute(
                    queries::POD_STATUS_CHECKPOINT_MARK_APPLIED,
                    rusqlite::params![pod_uid, applied_rv, updated_ms],
                )?;
                Ok(())
            },
        )
        .await
        .map_err(|e| anyhow!("pod_status_checkpoint mark applied failed: {e}"))
    }

    pub async fn delete_pod_status_checkpoint(&self, pod_uid: &str) -> Result<()> {
        let pod_uid = pod_uid.to_string();
        self.db_call("node_local:pod_status_checkpoint_delete", move |conn| {
            conn.execute(queries::POD_STATUS_CHECKPOINT_DELETE_UID, [pod_uid])?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("pod_status_checkpoint delete failed: {e}"))
    }

    pub async fn upsert_runtime_observation_checkpoint(
        &self,
        checkpoint: RuntimeObservationCheckpoint,
    ) -> Result<()> {
        let RuntimeObservationCheckpoint {
            pod_uid,
            container_ids,
            generation,
            updated_ms,
        } = checkpoint;
        let container_ids_json = serde_json::to_string(&container_ids)?;
        self.db_call(
            "node_local:runtime_observation_checkpoint_upsert",
            move |conn| {
                conn.execute(
                    queries::RUNTIME_OBSERVATION_CHECKPOINT_UPSERT,
                    rusqlite::params![pod_uid, container_ids_json, generation, updated_ms],
                )?;
                Ok(())
            },
        )
        .await
        .map_err(|e| anyhow!("runtime_observation_checkpoint upsert failed: {e}"))
    }

    pub async fn get_runtime_observation_checkpoint(
        &self,
        pod_uid: &str,
    ) -> Result<Option<RuntimeObservationCheckpoint>> {
        let pod_uid = pod_uid.to_string();
        self.db_call(
            "node_local:runtime_observation_checkpoint_get",
            move |conn| {
                conn.query_row(
                    queries::RUNTIME_OBSERVATION_CHECKPOINT_GET_UID,
                    [pod_uid],
                    row_to_runtime_observation_checkpoint,
                )
                .optional()
                .map_err(tokio_rusqlite::Error::from)
            },
        )
        .await
        .map_err(|e| anyhow!("runtime_observation_checkpoint get failed: {e}"))
    }

    pub async fn delete_runtime_observation_checkpoint(&self, pod_uid: &str) -> Result<()> {
        let pod_uid = pod_uid.to_string();
        self.db_call(
            "node_local:runtime_observation_checkpoint_delete",
            move |conn| {
                conn.execute(
                    queries::RUNTIME_OBSERVATION_CHECKPOINT_DELETE_UID,
                    [pod_uid],
                )?;
                Ok(())
            },
        )
        .await
        .map_err(|e| anyhow!("runtime_observation_checkpoint delete failed: {e}"))
    }

    pub async fn enqueue_outbox(&self, row: OutboxInsert) -> Result<()> {
        let is_terminal_pod_delete =
            is_terminal_pod_delete_outbox_row(row.operation.as_str(), row.payload_proto.as_slice());
        self.db_call("node_local:outbox_enqueue", move |conn| {
            conn.execute(
                queries::OUTBOX_INSERT,
                rusqlite::params![
                    row.idempotency_key,
                    row.enqueued_ms,
                    row.subject_key,
                    row.subject_api_version,
                    row.subject_kind,
                    row.subject_namespace,
                    row.subject_name,
                    row.subject_uid,
                    row.pod_uid,
                    row.operation,
                    if is_terminal_pod_delete { 1_i64 } else { 0_i64 },
                    row.payload_proto,
                    row.next_due_ms
                ],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("outbox enqueue failed: {e}"))
    }

    pub async fn claim_next_due_outbox(
        &self,
        now_ms: i64,
        lease_ms: i64,
        lease_token: &str,
    ) -> Result<Option<OutboxRow>> {
        let lease_token = lease_token.to_string();
        self.db_call("node_local:outbox_claim_next_due", move |conn| {
            let tx = conn.transaction()?;
            let id: Option<i64> = tx
                .query_row(queries::OUTBOX_CLAIM_NEXT_DUE, [now_ms], |row| row.get(0))
                .optional()?;
            let Some(id) = id else {
                tx.commit()?;
                return Ok(None);
            };
            let leased_until_ms = now_ms.saturating_add(lease_ms.max(1));
            tx.execute(
                queries::OUTBOX_SET_LEASE,
                rusqlite::params![id, leased_until_ms, lease_token],
            )?;
            let row = tx.query_row(queries::OUTBOX_ROW_SELECT, [id], row_to_outbox)?;
            tx.commit()?;
            Ok(Some(row))
        })
        .await
        .map_err(|e| anyhow!("outbox claim failed: {e}"))
    }

    pub async fn renew_outbox_lease(
        &self,
        id: i64,
        lease_token: &str,
        leased_until_ms: i64,
    ) -> Result<bool> {
        let lease_token = lease_token.to_string();
        self.db_call("node_local:outbox_renew_lease", move |conn| {
            let changed = conn.execute(
                queries::OUTBOX_RENEW_LEASE,
                rusqlite::params![id, lease_token, leased_until_ms],
            )?;
            Ok(changed > 0)
        })
        .await
        .map_err(|e| anyhow!("outbox renew lease failed: {e}"))
    }

    pub async fn mark_outbox_attempt_failed(
        &self,
        id: i64,
        lease_token: &str,
        backoff_until_ms: i64,
        error: &str,
    ) -> Result<bool> {
        let lease_token = lease_token.to_string();
        let error = error.to_string();
        self.db_call("node_local:outbox_mark_failed", move |conn| {
            let changed = conn.execute(
                queries::OUTBOX_MARK_FAILED,
                rusqlite::params![id, lease_token, backoff_until_ms, error],
            )?;
            Ok(changed > 0)
        })
        .await
        .map_err(|e| anyhow!("outbox mark failed failed: {e}"))
    }

    pub async fn complete_outbox(&self, id: i64, lease_token: &str) -> Result<bool> {
        let lease_token = lease_token.to_string();
        self.db_call("node_local:outbox_complete", move |conn| {
            let changed =
                conn.execute(queries::OUTBOX_COMPLETE, rusqlite::params![id, lease_token])?;
            Ok(changed > 0)
        })
        .await
        .map_err(|e| anyhow!("outbox complete failed: {e}"))
    }

    /// Claim up to `limit` due outbox rows in a single transaction.
    /// Preserves strict per-subject_key FIFO single-in-flight: a row is never
    /// claimed while an older row for the same subject still exists (whether due
    /// or leased), so at most one row per subject is ever in flight across
    /// batches. Cross-subject rows still pipeline freely.
    pub async fn claim_due_outbox_batch(
        &self,
        now_ms: i64,
        limit: usize,
        lease_ms: i64,
        lease_token: &str,
    ) -> Result<Vec<OutboxRow>> {
        let lease_token = lease_token.to_string();
        let limit_i64 = limit.min(256) as i64;
        // Find due row IDs (outside transaction — safe since there is only
        // one dispatcher; any concurrent insert will be caught in the next batch).
        let ids: Vec<i64> = self
            .db_call("node_local:outbox_claim_batch_find", move |conn| {
                let mut stmt = conn.prepare(queries::OUTBOX_CLAIM_DUE_BATCH)?;
                let rows =
                    stmt.query_map(rusqlite::params![now_ms, limit_i64], |row| row.get(0))?;
                let result = rows.collect::<rusqlite::Result<Vec<i64>>>()?;
                Ok(result)
            })
            .await
            .map_err(|e| anyhow!("outbox batch claim find failed: {e}"))?;

        if ids.is_empty() {
            return Ok(Vec::new());
        }

        // Set leases and fetch full rows in a single transaction.
        let rows = self
            .db_call("node_local:outbox_claim_batch_set", move |conn| {
                let tx = conn.transaction()?;
                let leased_until_ms = now_ms.saturating_add(lease_ms.max(1));
                let mut leased_ids = Vec::new();
                for &id in &ids {
                    let changed = tx.execute(
                        queries::OUTBOX_SET_LEASE,
                        rusqlite::params![id, leased_until_ms, lease_token],
                    )?;
                    if changed > 0 {
                        leased_ids.push(id);
                    }
                }
                let mut rows = Vec::with_capacity(leased_ids.len());
                {
                    let mut stmt = tx.prepare("SELECT id, idempotency_key, enqueued_ms, \
                        subject_key, subject_api_version, subject_kind, subject_namespace, subject_name, \
                        subject_uid, pod_uid, operation, is_terminal_pod_delete, payload_proto, attempt, next_due_ms, \
                        leased_until_ms, lease_token, last_error FROM outbox WHERE id = ?1")?;
                    for id in leased_ids {
                        if let Some(row) = stmt.query_row([id], row_to_outbox).optional()? {
                            rows.push(row);
                        }
                    }
                }
                tx.commit()?;
                Ok(rows)
            })
            .await
            .map_err(|e| anyhow!("outbox batch claim set failed: {e}"))?;

        Ok(rows)
    }

    /// Complete multiple outbox rows in a single transaction.
    pub async fn complete_outbox_batch(&self, ids: &[i64]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let ids = ids.to_vec();
        self.db_call("node_local:outbox_complete_batch", move |conn| {
            let tx = conn.transaction()?;
            for id in &ids {
                tx.execute(queries::OUTBOX_COMPLETE_BY_ID, [*id])?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("outbox batch complete failed: {e}"))
    }

    pub async fn complete_superseded_status_outbox_for_terminal_pod_delete(
        &self,
        subject_key: &str,
        terminal_delete_id: i64,
    ) -> Result<usize> {
        let subject_key = subject_key.to_string();
        self.db_call(
            "node_local:outbox_complete_superseded_terminal_pod_delete_status",
            move |conn| {
                conn.execute(
                    queries::OUTBOX_COMPLETE_SUPERSEDED_TERMINAL_POD_DELETE_STATUS,
                    rusqlite::params![subject_key, terminal_delete_id],
                )
                .map_err(tokio_rusqlite::Error::from)
            },
        )
        .await
        .map_err(|e| anyhow!("outbox complete superseded status failed: {e}"))
    }

    pub async fn requeue_expired_outbox_leases(&self, now_ms: i64) -> Result<usize> {
        self.db_call("node_local:outbox_requeue_expired", move |conn| {
            conn.execute(queries::OUTBOX_REQUEUE_EXPIRED, [now_ms])
                .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|e| anyhow!("outbox requeue expired failed: {e}"))
    }

    pub async fn next_outbox_wake_ms(&self, now_ms: i64) -> Result<Option<i64>> {
        self.db_call("node_local:outbox_next_wake", move |conn| {
            conn.query_row(queries::OUTBOX_NEXT_WAKE, [now_ms], |row| {
                row.get::<_, Option<i64>>(0)
            })
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|e| anyhow!("outbox next wake failed: {e}"))
    }

    pub async fn move_outbox_to_dead_letter_if_max_attempts(
        &self,
        idempotency_key: &str,
        max_attempts: i64,
    ) -> Result<bool> {
        let idempotency_key = idempotency_key.to_string();
        let now = now_ms();
        self.db_call("node_local:outbox_dead_letter_move", move |conn| {
            let tx = conn.transaction()?;
            let row = tx
                .query_row(
                    "SELECT id, attempt FROM outbox WHERE idempotency_key = ?1",
                    [&idempotency_key],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                )
                .optional()?;
            let Some((_original_id, attempt)) = row else {
                tx.commit()?;
                return Ok(false);
            };
            if attempt < max_attempts {
                tx.commit()?;
                return Ok(false);
            }
            let dead_row: Option<OutboxRow> = tx
                .query_row(queries::OUTBOX_ROW_SELECT, [&_original_id], row_to_outbox)
                .optional()?;
            let Some(dead_row) = dead_row else {
                tx.commit()?;
                return Ok(false);
            };
            tx.execute(
                queries::DEAD_LETTER_INSERT,
                rusqlite::params![
                    dead_row.id,
                    dead_row.idempotency_key,
                    dead_row.enqueued_ms,
                    dead_row.subject_key,
                    dead_row.subject_api_version,
                    dead_row.subject_kind,
                    dead_row.subject_namespace,
                    dead_row.subject_name,
                    dead_row.subject_uid,
                    dead_row.pod_uid,
                    dead_row.operation,
                    dead_row.payload_proto,
                    dead_row.attempt,
                    dead_row.last_error.unwrap_or_default(),
                    now,
                ],
            )?;
            tx.execute("DELETE FROM outbox WHERE id = ?1", [dead_row.id])?;
            tx.commit()?;
            Ok(true)
        })
        .await
        .map_err(|e| anyhow!("outbox dead letter move failed: {e}"))
    }

    pub async fn list_dead_letter(&self) -> Result<Vec<DeadLetterRow>> {
        self.db_call("node_local:outbox_dead_letter_list", move |conn| {
            let mut stmt = conn.prepare(queries::DEAD_LETTER_LIST)?;
            let rows = stmt
                .query_map([], row_to_dead_letter)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .map_err(|e| anyhow!("dead letter list failed: {e}"))
    }

    pub async fn get_dead_letter(&self, id: i64) -> Result<Option<DeadLetterRow>> {
        self.db_call("node_local:outbox_dead_letter_get", move |conn| {
            conn.query_row(queries::DEAD_LETTER_GET, [id], row_to_dead_letter)
                .optional()
                .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|e| anyhow!("dead letter get failed: {e}"))
    }

    pub async fn delete_dead_letter(&self, id: i64) -> Result<bool> {
        self.db_call("node_local:outbox_dead_letter_delete", move |conn| {
            let changed = conn.execute(queries::DEAD_LETTER_DELETE, [id])?;
            Ok(changed > 0)
        })
        .await
        .map_err(|e| anyhow!("dead letter delete failed: {e}"))
    }

    pub async fn replay_dead_letter(&self, id: i64) -> Result<bool> {
        let now = now_ms();
        let row = self
            .db_call("node_local:outbox_dead_letter_replay_get", move |conn| {
                Ok(conn
                    .query_row(queries::DEAD_LETTER_GET, [id], row_to_dead_letter)
                    .optional()?)
            })
            .await
            .map_err(|e| anyhow!("outbox dead letter replay read failed: {e}"))?;
        let Some(row) = row else {
            return Ok(false);
        };
        let is_terminal_pod_delete =
            is_terminal_pod_delete_outbox_row(row.operation.as_str(), row.payload_proto.as_slice());

        self.db_call("node_local:outbox_dead_letter_replay", move |conn| {
            let tx = conn.transaction()?;
            tx.execute(
                queries::OUTBOX_INSERT,
                rusqlite::params![
                    row.idempotency_key,
                    row.enqueued_ms,
                    row.subject_key,
                    row.subject_api_version,
                    row.subject_kind,
                    row.subject_namespace,
                    row.subject_name,
                    row.subject_uid,
                    row.pod_uid,
                    row.operation,
                    if is_terminal_pod_delete { 1_i64 } else { 0_i64 },
                    row.payload_proto,
                    now,
                ],
            )?;
            tx.execute(queries::DEAD_LETTER_DELETE, [id])?;
            tx.commit()?;
            Ok(true)
        })
        .await
        .map_err(|e| anyhow!("dead letter replay failed: {e}"))
    }

    pub async fn outbox_stats(&self) -> Result<OutboxStats> {
        let now = now_ms();
        self.db_call("node_local:outbox_stats", move |conn| {
            let pending: i64 = conn.query_row(queries::OUTBOX_COUNT, [], |row| row.get(0))?;
            let oldest_ms: Option<i64> = conn
                .query_row(queries::OUTBOX_OLDEST_ENQUEUED, [], |row| row.get(0))
                .optional()?
                .flatten();
            let oldest_age_seconds = oldest_ms
                .map(|ms| (now.saturating_sub(ms) as f64) / 1000.0)
                .unwrap_or(0.0);
            let dead_letter_count: i64 =
                conn.query_row(queries::DEAD_LETTER_COUNT, [], |row| row.get(0))?;
            let dispatch_total: i64 = conn
                .query_row(
                    "SELECT COALESCE(CAST(value AS INTEGER), 0) FROM _node_meta WHERE key = 'outbox_dispatch_total'",
                    [],
                    |row| row.get(0),
                )
                .unwrap_or(0);
            let dispatch_errors_total: i64 = conn
                .query_row(
                    "SELECT COALESCE(CAST(value AS INTEGER), 0) FROM _node_meta WHERE key = 'outbox_dispatch_errors_total'",
                    [],
                    |row| row.get(0),
                )
                .unwrap_or(0);
            Ok(OutboxStats {
                pending,
                oldest_age_seconds,
                dead_letter_count,
                dispatch_total,
                dispatch_errors_total,
            })
        })
        .await
        .map_err(|e| anyhow!("outbox stats failed: {e}"))
    }

    #[cfg(test)]
    pub async fn insert_dead_letter_test_only(&self, row: DeadLetterTestInsert<'_>) -> Result<()> {
        let idempotency_key = row.idempotency_key.to_string();
        let operation = row.operation.to_string();
        let subject_key = row.subject_key.to_string();
        let subject_api_version = row.subject_api_version.to_string();
        let subject_kind = row.subject_kind.to_string();
        let subject_namespace = row.subject_namespace.map(str::to_string);
        let subject_name = row.subject_name.to_string();
        let subject_uid = row.subject_uid.map(str::to_string);
        let pod_uid = row.pod_uid.to_string();
        let payload_proto = row.payload_proto.to_vec();
        let attempts = row.attempts;
        let last_error = row.last_error.to_string();
        let moved_at_ms = row.moved_at_ms;
        self.db_call("node_local:dead_letter_test_insert", move |conn| {
            conn.execute(
                queries::DEAD_LETTER_INSERT,
                rusqlite::params![
                    0_i64,
                    idempotency_key,
                    0_i64,
                    subject_key,
                    subject_api_version,
                    subject_kind,
                    subject_namespace,
                    subject_name,
                    subject_uid,
                    pod_uid,
                    operation,
                    payload_proto,
                    attempts,
                    last_error,
                    moved_at_ms,
                ],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("dead letter test insert failed: {e}"))
    }

    pub async fn record_probe_result(
        &self,
        pod_uid: &str,
        container_name: &str,
        probe_kind: &str,
        success: bool,
        ts_ms: i64,
    ) -> Result<()> {
        let pod_uid = pod_uid.to_string();
        let container_name = container_name.to_string();
        let probe_kind = probe_kind.to_string();
        let success_int = if success { 1 } else { 0 };
        self.db_call("node_local:probe_record", move |conn| {
            conn.execute(
                queries::PROBE_STATE_UPSERT,
                rusqlite::params![pod_uid, container_name, probe_kind, ts_ms, success_int],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("probe_state record failed: {e}"))
    }

    pub async fn get_probe_state(
        &self,
        pod_uid: &str,
        container_name: &str,
        probe_kind: &str,
    ) -> Result<Option<ProbeStateRow>> {
        let pod_uid = pod_uid.to_string();
        let container_name = container_name.to_string();
        let probe_kind = probe_kind.to_string();
        self.db_call("node_local:probe_get", move |conn| {
            conn.query_row(
                queries::PROBE_STATE_GET,
                rusqlite::params![pod_uid, container_name, probe_kind],
                |row| {
                    Ok(ProbeStateRow {
                        pod_uid: row.get(0)?,
                        container_name: row.get(1)?,
                        probe_kind: row.get(2)?,
                        last_result_ms: row.get(3)?,
                        last_success: row.get::<_, Option<i64>>(4)?.map(|v| v != 0),
                        consecutive_fail: row.get(5)?,
                        next_eligible_ms: row.get(6)?,
                    })
                },
            )
            .optional()
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|e| anyhow!("probe_state get failed: {e}"))
    }

    pub async fn read_replication_checkpoint(&self) -> Result<Option<ReplicationCheckpoint>> {
        self.db_call("node_local:checkpoint_get", move |conn| {
            conn.query_row(queries::REPLICATION_CHECKPOINT_GET, [], |row| {
                Ok(ReplicationCheckpoint {
                    last_applied_rv: row.get(0)?,
                    leader_epoch: row.get(1)?,
                    cluster_id: row.get(2)?,
                })
            })
            .optional()
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|e| anyhow!("replication checkpoint get failed: {e}"))
    }

    pub async fn write_replication_checkpoint(
        &self,
        last_applied_rv: i64,
        leader_epoch: i64,
        cluster_id: &str,
    ) -> Result<()> {
        let cluster_id = cluster_id.to_string();
        self.db_call("node_local:checkpoint_set", move |conn| {
            conn.execute(
                queries::REPLICATION_CHECKPOINT_SET,
                rusqlite::params![last_applied_rv, leader_epoch, cluster_id],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("replication checkpoint set failed: {e}"))
    }

    // T3: `append_log_apply_entry`, `list_log_apply_entries_after`,
    // `load_log_apply_checkpoint`, `save_log_apply_checkpoint` removed.
    // These were consumed only by the BackupApplier (deleted in T1.6).
    // The `raft_log_entries` table (used by openraft's RaftLogStorage)
    // is the sole durable log.

    pub async fn current_log_apply_index(&self) -> Result<i64> {
        // T3: returns 0 since the `log_apply_entries` table is gone.
        // The raft `last_applied` index is the authoritative source.
        Ok(0)
    }

    // --- Phase 3 Raft (openraft storage-v2) -------------------------------
    // Thin synchronous DB wrappers used by `crate::datastore::raft::log_storage`.
    // Serialization of openraft Entry blobs happens in the caller — these
    // methods stay format-agnostic so the same SQL primitives can serve
    // serde_json, bincode, or protobuf depending on what storage-v2 lands
    // on. Vote and committed-id rows are stored in raft_meta.

    pub async fn raft_log_append(
        &self,
        log_index: u64,
        term: u64,
        leader_node_id: u64,
        entry_blob: Vec<u8>,
    ) -> Result<()> {
        self.db_call("node_local:raft_log_append", move |conn| {
            conn.execute(
                queries::RAFT_LOG_INSERT,
                rusqlite::params![
                    log_index as i64,
                    term as i64,
                    leader_node_id as i64,
                    &entry_blob,
                ],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("raft_log_append failed: {e}"))
    }

    pub async fn raft_log_get_range(&self, start: u64, end: u64) -> Result<Vec<Vec<u8>>> {
        self.db_call("node_local:raft_log_get_range", move |conn| {
            let mut stmt = conn.prepare(queries::RAFT_LOG_GET_RANGE)?;
            let rows = stmt
                .query_map(rusqlite::params![start as i64, end as i64], |row| {
                    row.get::<_, Vec<u8>>(0)
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .map_err(|e| anyhow!("raft_log_get_range failed: {e}"))
    }

    /// Return `(log_index, term, leader_node_id)` of the highest-index
    /// stored entry, or `None` if the log is empty.
    pub async fn raft_log_last(&self) -> Result<Option<(u64, u64, u64)>> {
        self.db_call("node_local:raft_log_last", move |conn| {
            conn.query_row(queries::RAFT_LOG_LAST, [], |row| {
                Ok((
                    row.get::<_, i64>(0)? as u64,
                    row.get::<_, i64>(1)? as u64,
                    row.get::<_, i64>(2)? as u64,
                ))
            })
            .optional()
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|e| anyhow!("raft_log_last failed: {e}"))
    }

    /// Delete log entries with index >= `from_inclusive`. Used to recover
    /// from leader-side divergence (the fix for the F1 cycle-2 bug).
    pub async fn raft_log_truncate_from(&self, from_inclusive: u64) -> Result<()> {
        self.db_call("node_local:raft_log_truncate_from", move |conn| {
            conn.execute(
                queries::RAFT_LOG_TRUNCATE_FROM,
                rusqlite::params![from_inclusive as i64],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("raft_log_truncate_from failed: {e}"))
    }

    /// Delete log entries with index <= `upto_inclusive`. Used after a
    /// snapshot install supersedes log history below `upto_inclusive`.
    pub async fn raft_log_purge_upto(&self, upto_inclusive: u64) -> Result<()> {
        self.db_call("node_local:raft_log_purge_upto", move |conn| {
            conn.execute(
                queries::RAFT_LOG_PURGE_UPTO,
                rusqlite::params![upto_inclusive as i64],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("raft_log_purge_upto failed: {e}"))
    }

    pub async fn raft_meta_get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let key = key.to_string();
        self.db_call("node_local:raft_meta_get", move |conn| {
            conn.query_row(queries::RAFT_META_GET, rusqlite::params![&key], |row| {
                row.get::<_, Vec<u8>>(0)
            })
            .optional()
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|e| anyhow!("raft_meta_get failed: {e}"))
    }

    pub async fn raft_meta_set(&self, key: &str, value: Vec<u8>) -> Result<()> {
        let key = key.to_string();
        self.db_call("node_local:raft_meta_set", move |conn| {
            conn.execute(queries::RAFT_META_SET, rusqlite::params![&key, &value])?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("raft_meta_set failed: {e}"))
    }

    #[cfg(test)]
    pub async fn table_names_for_test(&self) -> Result<Vec<String>> {
        self.db_call("node_local:test_table_names", move |conn| {
            let rows = conn
                .prepare(
                    "SELECT name FROM sqlite_master \
                     WHERE type='table' AND name NOT LIKE 'sqlite_%' \
                     ORDER BY name",
                )?
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .map_err(|e| anyhow!("test table names failed: {e}"))
    }

    #[cfg(test)]
    pub async fn table_has_not_null_column_for_test(
        &self,
        table: &str,
        column: &str,
    ) -> Result<bool> {
        let table = table.to_string();
        let column = column.to_string();
        self.db_call("node_local:test_not_null_column", move |conn| {
            let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", quote_ident(&table)))?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let name: String = row.get(1)?;
                let ty: String = row.get(2)?;
                let not_null: i64 = row.get(3)?;
                if name == column {
                    return Ok(not_null == 1 && ty.eq_ignore_ascii_case("TEXT"));
                }
            }
            Ok(false)
        })
        .await
        .map_err(|e| anyhow!("test column check failed: {e}"))
    }

    #[cfg(test)]
    pub async fn schema_contains_full_resource_body_column_for_test(&self) -> Result<bool> {
        self.db_call("node_local:test_body_column", move |conn| {
            let tables = conn
                .prepare(
                    "SELECT name FROM sqlite_master \
                     WHERE type='table' AND name NOT LIKE 'sqlite_%'",
                )?
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            for table in tables {
                let mut stmt =
                    conn.prepare(&format!("PRAGMA table_info({})", quote_ident(&table)))?;
                let mut rows = stmt.query([])?;
                while let Some(row) = rows.next()? {
                    let name: String = row.get(1)?;
                    let ty: String = row.get(2)?;
                    if name == "data" && ty.eq_ignore_ascii_case("BLOB") {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        })
        .await
        .map_err(|e| anyhow!("test body column check failed: {e}"))
    }
}

fn ensure_meta_matches_or_insert(
    conn: &rusqlite::Connection,
    key: &str,
    expected: &str,
) -> rusqlite::Result<()> {
    let live: Option<String> = conn
        .query_row(queries::META_GET, [key], |row| row.get(0))
        .optional()?;
    match live {
        None => {
            conn.execute(queries::META_SET, rusqlite::params![key, expected])?;
            Ok(())
        }
        Some(actual) if actual == expected => Ok(()),
        Some(actual) => Err(rusqlite::Error::ToSqlConversionFailure(Box::new(
            std::io::Error::other(format!(
                "node.db identity mismatch for {key}: expected {expected}, found {actual}"
            )),
        ))),
    }
}

fn row_to_pod_runtime(row: &rusqlite::Row<'_>) -> rusqlite::Result<PodRuntimeRow> {
    Ok(PodRuntimeRow {
        pod_uid: row.get(0)?,
        namespace: row.get(1)?,
        pod_name: row.get(2)?,
        node_name: row.get(3)?,
        sandbox_id: row.get(4)?,
        cgroup_path: row.get(5)?,
        created_ms: row.get(6)?,
        started_ms: row.get(7)?,
    })
}

fn row_to_outbox(row: &rusqlite::Row<'_>) -> rusqlite::Result<OutboxRow> {
    Ok(OutboxRow {
        id: row.get(0)?,
        idempotency_key: row.get(1)?,
        enqueued_ms: row.get(2)?,
        subject_key: row.get(3)?,
        subject_api_version: row.get(4)?,
        subject_kind: row.get(5)?,
        subject_namespace: row.get(6)?,
        subject_name: row.get(7)?,
        subject_uid: row.get(8)?,
        pod_uid: row.get(9)?,
        operation: row.get(10)?,
        is_terminal_pod_delete: row.get::<_, i64>(11)? != 0,
        payload_proto: row.get(12)?,
        attempt: row.get(13)?,
        next_due_ms: row.get(14)?,
        leased_until_ms: row.get(15)?,
        lease_token: row.get(16)?,
        last_error: row.get(17)?,
    })
}

fn is_terminal_pod_delete_outbox_row(operation: &str, payload_proto: &[u8]) -> bool {
    if operation != "PodMetadata" {
        return false;
    }
    matches!(
        decode_command_protobuf(payload_proto),
        Ok(StorageCommand::DeleteResource {
            api_version,
            kind,
            preconditions,
            ..
        }) if api_version == "v1" && kind == "Pod" && preconditions.uid.is_some()
    )
}

fn row_to_dead_letter(row: &rusqlite::Row<'_>) -> rusqlite::Result<DeadLetterRow> {
    Ok(DeadLetterRow {
        id: row.get(0)?,
        original_id: row.get(1)?,
        idempotency_key: row.get(2)?,
        enqueued_ms: row.get(3)?,
        subject_key: row.get(4)?,
        subject_api_version: row.get(5)?,
        subject_kind: row.get(6)?,
        subject_namespace: row.get(7)?,
        subject_name: row.get(8)?,
        subject_uid: row.get(9)?,
        pod_uid: row.get(10)?,
        operation: row.get(11)?,
        payload_proto: row.get(12)?,
        attempts: row.get(13)?,
        last_error: row.get(14)?,
        moved_at_ms: row.get(15)?,
    })
}

#[cfg(test)]
fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

impl SqliteNodeLocalDb {
    pub async fn reserve_ip_and_insert_network(
        &self,
        request: PodNetworkAllocationRequest<'_>,
    ) -> Result<(String, u32)> {
        let request = request.into_owned();
        let now = now_ms();
        self.db_call("node_local:reserve_ip_network", move |conn| {
            reserve_ip_and_insert_network_in_conn(conn, request.as_borrowed(), now)
        })
        .await
        .map_err(|e| anyhow!("pod network reserve failed: {e}"))
    }

    pub async fn get_network_for_uid(&self, pod_uid: &str) -> Result<Option<PodNetworkEndpoint>> {
        let pod_uid = pod_uid.to_string();
        self.db_call("node_local:get_network_uid", move |conn| {
            conn.query_row(
                "SELECT ip_addr, veth_host, netns_path FROM pod_networks \
                 WHERE pod_uid = ?1 ORDER BY created_ms DESC LIMIT 1",
                [pod_uid],
                row_to_pod_network_endpoint,
            )
            .optional()
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|e| anyhow!("pod network get uid failed: {e}"))
    }

    pub async fn get_network_for_sandbox(
        &self,
        sandbox_id: &str,
    ) -> Result<Option<PodNetworkEndpoint>> {
        let sandbox_id = sandbox_id.to_string();
        self.db_call("node_local:get_network_sandbox", move |conn| {
            conn.query_row(
                "SELECT ip_addr, veth_host, netns_path FROM pod_networks WHERE sandbox_id = ?1",
                [sandbox_id],
                row_to_pod_network_endpoint,
            )
            .optional()
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|e| anyhow!("pod network get sandbox failed: {e}"))
    }

    pub async fn delete_network_for_sandbox(&self, sandbox_id: &str) -> Result<()> {
        let sandbox_id = sandbox_id.to_string();
        self.db_call("node_local:delete_network_sandbox", move |conn| {
            conn.execute(
                "DELETE FROM pod_networks WHERE sandbox_id = ?1",
                [sandbox_id],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("pod network delete failed: {e}"))
    }

    pub async fn list_networks(&self) -> Result<Vec<String>> {
        self.db_call("node_local:list_networks", move |conn| {
            let rows = conn
                .prepare("SELECT sandbox_id FROM pod_networks ORDER BY sandbox_id")?
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .map_err(|e| anyhow!("pod network list failed: {e}"))
    }

    pub async fn upsert_endpoint(&self, row: PodEndpointRow) -> Result<()> {
        let tx = self.pod_endpoint_tx.clone();
        let event_row = row.clone();
        self.db_call("node_local:upsert_endpoint", move |conn| {
            let prior_ip: Option<String> = conn
                .query_row(
                    "SELECT pod_ip FROM pod_endpoints WHERE pod_uid = ?1",
                    [&row.pod_uid],
                    |row| row.get(0),
                )
                .optional()?;
            conn.execute(
                "INSERT OR REPLACE INTO pod_endpoints \
                 (pod_uid, namespace, pod_name, node_name, mode, pod_ip, node_ip, \
                  host_port_tcp, host_port_udp, generation, updated_ms) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                rusqlite::params![
                    row.pod_uid,
                    row.namespace,
                    row.pod_name,
                    row.node_name,
                    row.mode.as_str(),
                    row.pod_ip.to_string(),
                    row.node_ip.to_string(),
                    row.host_port_tcp.map(|p| p as i64),
                    row.host_port_udp.map(|p| p as i64),
                    row.generation,
                    row.updated_at
                ],
            )?;
            Ok(prior_ip)
        })
        .await
        .map(|prior_ip| {
            if let Some(prior_ip) = prior_ip
                && prior_ip != event_row.pod_ip.to_string()
                && let Ok(pod_ip) = prior_ip.parse()
            {
                let _ = tx.send(PodEndpointEvent::Delete {
                    pod_uid: event_row.pod_uid.clone(),
                    pod_ip,
                });
            }
            let _ = tx.send(PodEndpointEvent::Upsert(event_row));
        })
        .map_err(|e| anyhow!("pod endpoint upsert failed: {e}"))
    }

    pub async fn delete_endpoint_for_uid(&self, pod_uid: &str) -> Result<()> {
        let pod_uid = pod_uid.to_string();
        let event_pod_uid = pod_uid.clone();
        let tx = self.pod_endpoint_tx.clone();
        self.db_call("node_local:delete_endpoint", move |conn| {
            let old_ip: Option<String> = conn
                .query_row(
                    "SELECT pod_ip FROM pod_endpoints WHERE pod_uid = ?1",
                    [&pod_uid],
                    |row| row.get(0),
                )
                .optional()?;
            conn.execute("DELETE FROM pod_endpoints WHERE pod_uid = ?1", [&pod_uid])?;
            Ok(old_ip)
        })
        .await
        .map(|old_ip| {
            if let Some(ip) = old_ip.and_then(|s| s.parse().ok()) {
                let _ = tx.send(PodEndpointEvent::Delete {
                    pod_uid: event_pod_uid,
                    pod_ip: ip,
                });
            }
        })
        .map_err(|e| anyhow!("pod endpoint delete failed: {e}"))
    }

    pub async fn get_endpoint_by_pod_ip(
        &self,
        pod_ip: std::net::Ipv4Addr,
    ) -> Result<Option<PodEndpointRow>> {
        let pod_ip = pod_ip.to_string();
        self.db_call("node_local:get_endpoint_by_pod_ip", move |conn| {
            Ok(conn
                .query_row(
                    "SELECT pod_uid, namespace, pod_name, node_name, mode, pod_ip, node_ip, \
                        host_port_tcp, host_port_udp, generation, updated_ms \
                 FROM pod_endpoints WHERE pod_ip = ?1",
                    [&pod_ip],
                    row_to_pod_endpoint,
                )
                .optional()?)
        })
        .await
        .map_err(|e| anyhow!("pod endpoint get by pod_ip failed: {e}"))
    }

    pub async fn list_endpoints_for_node(&self, node_name: &str) -> Result<Vec<PodEndpointRow>> {
        let node_name = node_name.to_string();
        self.db_call("node_local:list_endpoints_node", move |conn| {
            let mut stmt = conn.prepare(
                "SELECT pod_uid, namespace, pod_name, node_name, mode, pod_ip, node_ip, \
                        host_port_tcp, host_port_udp, generation, updated_ms \
                 FROM pod_endpoints WHERE node_name = ?1 ORDER BY pod_uid",
            )?;
            let rows = stmt
                .query_map([node_name], row_to_pod_endpoint)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .map_err(|e| anyhow!("pod endpoint list by node failed: {e}"))
    }

    pub async fn list_endpoints_all(&self) -> Result<Vec<PodEndpointRow>> {
        self.db_call("node_local:list_endpoints_all", move |conn| {
            let mut stmt = conn.prepare(
                "SELECT pod_uid, namespace, pod_name, node_name, mode, pod_ip, node_ip, \
                        host_port_tcp, host_port_udp, generation, updated_ms \
                 FROM pod_endpoints ORDER BY node_name, pod_uid",
            )?;
            let rows = stmt
                .query_map([], row_to_pod_endpoint)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .map_err(|e| anyhow!("pod endpoint list all failed: {e}"))
    }

    pub async fn enqueue_workqueue(
        &self,
        kind: PodWorkqueueKind,
        pod: &crate::pod_identity::PodIdentity,
        payload: Value,
        attempt_count: i64,
        min_delay_ms: i64,
        last_error: Option<&str>,
    ) -> Result<()> {
        let kind = kind.as_str().to_string();
        let namespace = pod.namespace.clone();
        let name = pod.name.clone();
        let uid = pod.uid.clone();
        let payload = serde_json::to_vec(&payload)?;
        let last_error = last_error.map(str::to_string);
        let now = now_ms();
        let floor = now.saturating_add(min_delay_ms.max(0));
        self.db_call("node_local:workqueue_enqueue", move |conn| {
            let tail_other: i64 = conn.query_row(
                "SELECT COALESCE(MAX(next_due_ms), 0) FROM pod_workqueue \
                 WHERE NOT (kind = ?1 AND namespace = ?2 AND pod_name = ?3 AND pod_uid = ?4)",
                rusqlite::params![kind, namespace, name, uid],
                |row| row.get(0),
            )?;
            let next_due_ms = floor.max(tail_other.saturating_add(1));
            conn.execute(
                "INSERT INTO pod_workqueue \
                 (kind, namespace, pod_name, pod_uid, payload, attempt_count, next_due_ms, last_error, enqueued_ms) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) \
                 ON CONFLICT(kind, namespace, pod_name, pod_uid) DO UPDATE SET \
                   payload = excluded.payload, \
                   attempt_count = excluded.attempt_count, \
                   next_due_ms = excluded.next_due_ms, \
                   last_error = excluded.last_error, \
                   enqueued_ms = excluded.enqueued_ms",
                rusqlite::params![
                    kind,
                    namespace,
                    name,
                    uid,
                    payload,
                    attempt_count,
                    next_due_ms,
                    last_error,
                    now
                ],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("pod_workqueue enqueue failed: {e}"))
    }

    pub async fn peek_workqueue_next_due(&self) -> Result<Option<i64>> {
        self.db_call("node_local:workqueue_peek", move |conn| {
            conn.query_row("SELECT MIN(next_due_ms) FROM pod_workqueue", [], |row| {
                row.get::<_, Option<i64>>(0)
            })
            .optional()
            .map(|v| v.flatten())
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|e| anyhow!("pod_workqueue peek failed: {e}"))
    }

    pub async fn claim_workqueue_due(&self, now_ms: i64) -> Result<Option<PodWorkqueueEntry>> {
        self.db_call("node_local:workqueue_claim", move |conn| {
            let tx = conn.transaction()?;
            let row = tx
                .query_row(
                    "SELECT id, kind, namespace, pod_name, pod_uid, payload, attempt_count, next_due_ms \
                     FROM pod_workqueue WHERE next_due_ms <= ?1 ORDER BY next_due_ms ASC, id ASC LIMIT 1",
                    [now_ms],
                    |row| {
                        let kind_raw: String = row.get(1)?;
                        let kind = PodWorkqueueKind::parse(&kind_raw).map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                1,
                                rusqlite::types::Type::Text,
                                Box::new(std::io::Error::other(e.to_string())),
                            )
                        })?;
                        let payload: Vec<u8> = row.get(5)?;
                        let payload = serde_json::from_slice::<Value>(&payload).map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                5,
                                rusqlite::types::Type::Blob,
                                Box::new(e),
                            )
                        })?;
                        Ok(PodWorkqueueEntry {
                            id: row.get(0)?,
                            kind,
                            namespace: row.get(2)?,
                            name: row.get(3)?,
                            uid: row.get(4)?,
                            payload,
                            attempt_count: row.get(6)?,
                            next_attempt_at_ms: row.get(7)?,
                        })
                    },
                )
                .optional()?;
            if let Some(ref claimed) = row {
                tx.execute("DELETE FROM pod_workqueue WHERE id = ?1", [claimed.id])?;
            }
            tx.commit()?;
            Ok(row)
        })
        .await
        .map_err(|e| anyhow!("pod_workqueue claim failed: {e}"))
    }

    pub async fn complete_workqueue(&self, id: i64) -> Result<()> {
        self.db_call("node_local:workqueue_complete", move |conn| {
            conn.execute("DELETE FROM pod_workqueue WHERE id = ?1", [id])?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("pod_workqueue complete failed: {e}"))
    }
}

fn reserve_ip_and_insert_network_in_conn(
    conn: &rusqlite::Connection,
    request: PodNetworkAllocationRequest<'_>,
    now_ms: i64,
) -> tokio_rusqlite::Result<(String, u32)> {
    if request.subnet.size < 4 {
        return Err(tokio_rusqlite::Error::Other(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "subnet too small for pod IPAM",
        ))));
    }

    if let Some((existing_ip, existing_ip_int)) = conn
        .query_row(
            "SELECT ip_addr, ip_int FROM pod_networks WHERE sandbox_id = ?1",
            [request.sandbox_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u32)),
        )
        .optional()?
    {
        return Ok((existing_ip, existing_ip_int));
    }

    let start = request.subnet.base_int + 2;
    let end = request.subnet.base_int + request.subnet.size - 2;
    if start > end {
        return Err(tokio_rusqlite::Error::Other(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "subnet has no usable pod IPs",
        ))));
    }

    let max_allocated: Option<i64> = conn.query_row(
        "SELECT MAX(ip_int) FROM pod_networks WHERE ip_int >= ?1 AND ip_int <= ?2",
        rusqlite::params![start as i64, end as i64],
        |row| row.get(0),
    )?;
    let next_after_max = max_allocated
        .map(|v| v as u32 + 1)
        .filter(|candidate| *candidate <= end)
        .unwrap_or(start);
    let usable_count = end - start + 1;

    for offset in 0..usable_count {
        let candidate = start + ((next_after_max - start + offset) % usable_count);
        let ip_addr = crate::utils::ip_u32_to_string(candidate);
        let inserted = conn.execute(
            "INSERT INTO pod_networks \
             (sandbox_id, namespace, pod_name, pod_uid, ip_addr, ip_int, veth_host, netns_path, created_ms) \
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) \
             ON CONFLICT(ip_int) DO NOTHING",
            rusqlite::params![
                request.sandbox_id,
                request.pod.namespace,
                request.pod.name,
                request.pod.uid,
                ip_addr,
                candidate as i64,
                request.link.veth_host,
                request.link.netns_path,
                now_ms
            ],
        )?;
        if inserted > 0 {
            return Ok((crate::utils::ip_u32_to_string(candidate), candidate));
        }
    }

    Err(tokio_rusqlite::Error::Other(Box::new(std::io::Error::new(
        std::io::ErrorKind::AddrNotAvailable,
        "no free IPs in pod subnet",
    ))))
}

fn row_to_pod_network_endpoint(row: &rusqlite::Row<'_>) -> rusqlite::Result<PodNetworkEndpoint> {
    Ok(PodNetworkEndpoint {
        ip_addr: row.get(0)?,
        veth_host: row.get(1)?,
        netns_path: row.get(2)?,
    })
}

fn row_to_pod_endpoint(row: &rusqlite::Row<'_>) -> rusqlite::Result<PodEndpointRow> {
    let mode: String = row.get(4)?;
    let mode = match mode.as_str() {
        "encrypted_direct" | "vxlan" => PodEndpointMode::EncryptedDirect,
        "hostport" => PodEndpointMode::Hostport,
        other => {
            return Err(rusqlite::Error::FromSqlConversionFailure(
                4,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::other(format!(
                    "invalid pod endpoint mode {other}"
                ))),
            ));
        }
    };
    let pod_ip: String = row.get(5)?;
    let node_ip: Option<String> = row.get(6)?;
    Ok(PodEndpointRow {
        pod_uid: row.get(0)?,
        namespace: row.get(1)?,
        pod_name: row.get(2)?,
        node_name: row.get(3)?,
        mode,
        pod_ip: pod_ip.parse().map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Text, Box::new(e))
        })?,
        node_ip: node_ip
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or(&pod_ip)
            .parse()
            .map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    6,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?,
        host_port_tcp: row.get::<_, Option<i64>>(7)?.map(|p| p as u16),
        host_port_udp: row.get::<_, Option<i64>>(8)?.map(|p| p as u16),
        generation: row.get(9)?,
        updated_at: row.get(10)?,
    })
}

fn row_to_pod_status_checkpoint(row: &rusqlite::Row<'_>) -> rusqlite::Result<PodStatusCheckpoint> {
    let status_json: Vec<u8> = row.get(5)?;
    let status = serde_json::from_slice(&status_json).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Blob, Box::new(e))
    })?;
    Ok(PodStatusCheckpoint {
        pod_uid: row.get(0)?,
        namespace: row.get(1)?,
        pod_name: row.get(2)?,
        base_rv: row.get(3)?,
        applied_rv: row.get(4)?,
        status,
        updated_ms: row.get(6)?,
    })
}

fn row_to_runtime_observation_checkpoint(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<RuntimeObservationCheckpoint> {
    let container_ids_json: String = row.get(1)?;
    let container_ids: Vec<String> = serde_json::from_str(&container_ids_json).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(e))
    })?;
    Ok(RuntimeObservationCheckpoint {
        pod_uid: row.get(0)?,
        container_ids,
        generation: row.get::<_, i64>(2)? as u64,
        updated_ms: row.get(3)?,
    })
}
