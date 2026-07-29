//! SQLite node-delivery persistence.
//!
//! Payload bytes remain opaque. This module stores caller-supplied
//! classification columns and never imports or decodes a storage command.

use klights_node_store::{
    DeadLetterEntry, DeadLetterKey, DeadLetterMoveRequest, DeadLetterReplayRequest,
    DeadLetterStore, DeliveryError, DeliveryFuture, OutboxAttemptFailure,
    OutboxAttemptFailureRecord, OutboxBatchClaimRequest, OutboxClaimRequest, OutboxClassification,
    OutboxCompletion, OutboxDispatchCounters, OutboxDispatcherStore, OutboxEnqueue,
    OutboxFailureDisposition, OutboxLease, OutboxNow, OutboxProducerStore, OutboxRecord,
    OutboxSequence, OutboxSequencePolicy, OutboxStats, OutboxStatusStampStore,
    OutboxSupersedability, PodCheckpointKey, PodStatusCheckpoint, PodStatusCheckpointApplied,
    PodStatusCheckpointStore, PodStatusCheckpointUpsert, ReplicationCheckpoint,
    ReplicationCheckpointStore, RuntimeObservationCheckpoint, RuntimeObservationCheckpointStore,
    RuntimeObservationGeneration, TerminalDeleteClassification,
};
use klights_supervisor::{DbExecutor, WallClock};
use klights_types::{PodIdentity, ResourceKey};
use rusqlite::OptionalExtension;

use crate::delivery_queries as queries;

const STATUS_STAMP_META_KEY: &str = "pod_status_stamp_high_water";
const OUTBOX_DISPATCH_TOTAL_META_KEY: &str = "outbox_dispatch_total";
const OUTBOX_DISPATCH_ERRORS_META_KEY: &str = "outbox_dispatch_errors_total";

#[derive(Clone)]
pub struct SqliteDeliveryStore {
    executor: DbExecutor,
    wall_clock: std::sync::Arc<dyn WallClock>,
}

impl SqliteDeliveryStore {
    pub fn new(executor: DbExecutor, wall_clock: std::sync::Arc<dyn WallClock>) -> Self {
        Self {
            executor,
            wall_clock,
        }
    }

    async fn call<T, F>(&self, query_name: &'static str, call: F) -> tokio_rusqlite::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut rusqlite::Connection) -> tokio_rusqlite::Result<T> + Send + 'static,
    {
        self.executor.call_raw(query_name, call).await
    }

    fn now_ms(&self) -> i64 {
        self.wall_clock
            .now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(i64::MAX as u128) as i64
    }
}

#[derive(Debug)]
struct RawOutboxInsert {
    idempotency_key: String,
    enqueued_ms: i64,
    subject_key: String,
    subject_api_version: String,
    subject_kind: String,
    subject_namespace: Option<String>,
    subject_name: String,
    subject_uid: Option<String>,
    pod_uid: String,
    operation: String,
    classification: OutboxClassification,
    payload_proto: Vec<u8>,
    next_due_ms: i64,
}

#[derive(Debug)]
struct RawOutboxRow {
    id: i64,
    client_id: String,
    idempotency_key: String,
    enqueued_ms: i64,
    subject_key: String,
    subject_api_version: String,
    subject_kind: String,
    subject_namespace: Option<String>,
    subject_name: String,
    subject_uid: Option<String>,
    pod_uid: String,
    operation: String,
    priority_class: i64,
    supersedable_pod_status: bool,
    is_terminal_pod_delete: bool,
    stream_id: i64,
    stream_seq: i64,
    payload_proto: Vec<u8>,
    attempt: i64,
    next_due_ms: i64,
    leased_until_ms: i64,
    lease_token: Option<String>,
    last_error: Option<String>,
}

#[derive(Debug)]
struct RawDeadLetterRow {
    id: i64,
    original_id: i64,
    client_id: String,
    idempotency_key: String,
    enqueued_ms: i64,
    subject_key: String,
    subject_api_version: String,
    subject_kind: String,
    subject_namespace: Option<String>,
    subject_name: String,
    subject_uid: Option<String>,
    pod_uid: String,
    operation: String,
    stream_id: i64,
    stream_seq: i64,
    payload_proto: Vec<u8>,
    attempts: i64,
    last_error: String,
    moved_at_ms: i64,
}

struct RawPodStatusCheckpoint {
    pod_uid: String,
    namespace: String,
    pod_name: String,
    base_rv: i64,
    applied_rv: Option<i64>,
    status: serde_json::Value,
    updated_ms: i64,
}

struct RawRuntimeObservationCheckpoint {
    pod_uid: String,
    container_ids: Vec<String>,
    generation: u64,
    updated_ms: i64,
}

impl SqliteDeliveryStore {
    async fn get_node_meta(&self, key: &str) -> anyhow::Result<Option<String>> {
        let key = key.to_string();
        self.call("node_local:get_meta", move |conn| {
            conn.query_row(queries::META_GET, [key], |row| row.get(0))
                .optional()
                .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|error| anyhow::anyhow!("node meta get failed: {error}"))
    }

    async fn set_node_meta(&self, key: &str, value: &str) -> anyhow::Result<()> {
        let key = key.to_string();
        let value = value.to_string();
        self.call("node_local:set_meta", move |conn| {
            conn.execute(queries::META_SET, rusqlite::params![key, value])?;
            Ok(())
        })
        .await
        .map_err(|error| anyhow::anyhow!("node meta set failed: {error}"))
    }

    async fn upsert_pod_status_checkpoint(
        &self,
        pod_uid: &str,
        namespace: &str,
        pod_name: &str,
        base_rv: i64,
        status: serde_json::Value,
        updated_ms: i64,
    ) -> anyhow::Result<()> {
        let pod_uid = pod_uid.to_string();
        let namespace = namespace.to_string();
        let pod_name = pod_name.to_string();
        let status_json = serde_json::to_vec(&status)?;
        self.call("node_local:pod_status_checkpoint_upsert", move |conn| {
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
        .map_err(|error| anyhow::anyhow!("pod_status_checkpoint upsert failed: {error}"))
    }

    async fn get_pod_status_checkpoint(
        &self,
        pod_uid: &str,
    ) -> anyhow::Result<Option<RawPodStatusCheckpoint>> {
        let pod_uid = pod_uid.to_string();
        self.call("node_local:pod_status_checkpoint_get", move |conn| {
            conn.query_row(
                queries::POD_STATUS_CHECKPOINT_GET_UID,
                [pod_uid],
                row_to_pod_status_checkpoint,
            )
            .optional()
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|error| anyhow::anyhow!("pod_status_checkpoint get failed: {error}"))
    }

    async fn mark_pod_status_checkpoint_applied(
        &self,
        pod_uid: &str,
        applied_rv: i64,
        updated_ms: i64,
    ) -> anyhow::Result<()> {
        let pod_uid = pod_uid.to_string();
        self.call(
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
        .map_err(|error| anyhow::anyhow!("pod_status_checkpoint mark applied failed: {error}"))
    }

    async fn delete_pod_status_checkpoint(&self, pod_uid: &str) -> anyhow::Result<()> {
        let pod_uid = pod_uid.to_string();
        self.call("node_local:pod_status_checkpoint_delete", move |conn| {
            conn.execute(queries::POD_STATUS_CHECKPOINT_DELETE_UID, [pod_uid])?;
            Ok(())
        })
        .await
        .map_err(|error| anyhow::anyhow!("pod_status_checkpoint delete failed: {error}"))
    }

    async fn upsert_runtime_observation_checkpoint(
        &self,
        checkpoint: RawRuntimeObservationCheckpoint,
    ) -> anyhow::Result<()> {
        let RawRuntimeObservationCheckpoint {
            pod_uid,
            container_ids,
            generation,
            updated_ms,
        } = checkpoint;
        let container_ids_json = serde_json::to_string(&container_ids)?;
        self.call(
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
        .map_err(|error| anyhow::anyhow!("runtime_observation_checkpoint upsert failed: {error}"))
    }

    async fn get_runtime_observation_checkpoint(
        &self,
        pod_uid: &str,
    ) -> anyhow::Result<Option<RawRuntimeObservationCheckpoint>> {
        let pod_uid = pod_uid.to_string();
        self.call(
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
        .map_err(|error| anyhow::anyhow!("runtime_observation_checkpoint get failed: {error}"))
    }

    async fn delete_runtime_observation_checkpoint(&self, pod_uid: &str) -> anyhow::Result<()> {
        let pod_uid = pod_uid.to_string();
        self.call(
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
        .map_err(|error| anyhow::anyhow!("runtime_observation_checkpoint delete failed: {error}"))
    }

    async fn enqueue_outbox(&self, row: RawOutboxInsert) -> anyhow::Result<()> {
        let (priority_class, supersedable_pod_status, is_terminal_pod_delete, sequence_policy) =
            row.classification.persisted_values();
        self.call("node_local:outbox_enqueue", move |conn| {
            let tx = conn.transaction()?;
            let client_id = crate::schema::ensure_outbox_client_id_in_tx(&tx)?;
            let sequenced = sequence_policy
                == klights_node_store::OutboxSequencePolicy::PerSubject.persisted_value();
            let stream_id = if sequenced {
                crate::schema::outbox_stream_id(&row.subject_key)
            } else {
                0
            };
            let stream_seq = if sequenced {
                crate::schema::allocate_next_outbox_stream_seq(&tx, stream_id)?
            } else {
                0
            };
            tx.execute(
                queries::OUTBOX_INSERT,
                rusqlite::params![
                    client_id,
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
                    priority_class,
                    supersedable_pod_status,
                    is_terminal_pod_delete,
                    stream_id,
                    stream_seq,
                    row.payload_proto,
                    row.next_due_ms
                ],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .map_err(|error| anyhow::anyhow!("outbox enqueue failed: {error}"))
    }

    async fn claim_next_due_outbox(
        &self,
        now_ms: i64,
        lease_ms: i64,
        lease_token: &str,
    ) -> anyhow::Result<Option<RawOutboxRow>> {
        let lease_token = lease_token.to_string();
        self.call("node_local:outbox_claim_next_due", move |conn| {
            let tx = conn.transaction()?;
            let id: Option<i64> = tx
                .query_row(queries::outbox_claim_next_due(), [now_ms], |row| row.get(0))
                .optional()?;
            let Some(id) = id else {
                tx.commit()?;
                return Ok(None);
            };
            let leased_until_ms = now_ms.saturating_add(lease_ms.max(1));
            tx.execute(
                queries::OUTBOX_SET_LEASE,
                rusqlite::params![id, leased_until_ms, lease_token, now_ms],
            )?;
            let row = tx.query_row(queries::OUTBOX_ROW_SELECT, [id], row_to_outbox)?;
            tx.commit()?;
            Ok(Some(row))
        })
        .await
        .map_err(|error| anyhow::anyhow!("outbox claim failed: {error}"))
    }

    async fn renew_outbox_lease(
        &self,
        id: i64,
        lease_token: &str,
        leased_until_ms: i64,
    ) -> anyhow::Result<bool> {
        let lease_token = lease_token.to_string();
        self.call("node_local:outbox_renew_lease", move |conn| {
            let changed = conn.execute(
                queries::OUTBOX_RENEW_LEASE,
                rusqlite::params![id, lease_token, leased_until_ms],
            )?;
            Ok(changed > 0)
        })
        .await
        .map_err(|error| anyhow::anyhow!("outbox renew lease failed: {error}"))
    }

    async fn mark_outbox_attempt_failed(
        &self,
        id: i64,
        lease_token: &str,
        backoff_until_ms: i64,
        error: &str,
    ) -> anyhow::Result<bool> {
        let lease_token = lease_token.to_string();
        let error = error.to_string();
        self.call("node_local:outbox_mark_failed", move |conn| {
            let changed = conn.execute(
                queries::OUTBOX_MARK_FAILED,
                rusqlite::params![id, lease_token, backoff_until_ms, error],
            )?;
            Ok(changed > 0)
        })
        .await
        .map_err(|error| anyhow::anyhow!("outbox mark failed failed: {error}"))
    }

    async fn record_outbox_failure(
        &self,
        id: i64,
        lease_token: &str,
        backoff_until_ms: i64,
        error: &str,
        max_attempts: i64,
    ) -> anyhow::Result<OutboxFailureDisposition> {
        let lease_token = lease_token.to_string();
        let error = error.to_string();
        let now = self.now_ms();
        self.call("node_local:outbox_record_failure", move |conn| {
            let tx = conn.transaction()?;
            let row = tx
                .query_row(queries::OUTBOX_ROW_SELECT, [id], row_to_outbox)
                .optional()?;
            let Some(mut row) =
                row.filter(|row| row.lease_token.as_deref() == Some(lease_token.as_str()))
            else {
                tx.commit()?;
                return Ok(OutboxFailureDisposition::LeaseLost);
            };
            row.attempt = row.attempt.saturating_add(1);
            row.last_error = Some(error.clone());
            if row.attempt >= max_attempts.max(1) {
                tx.execute(
                    queries::DEAD_LETTER_INSERT,
                    rusqlite::params![
                        row.id,
                        row.client_id,
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
                        row.stream_id,
                        row.stream_seq,
                        row.payload_proto,
                        row.attempt,
                        error,
                        now,
                    ],
                )?;
                let changed = tx.execute(
                    "DELETE FROM outbox WHERE id = ?1 AND lease_token = ?2",
                    rusqlite::params![id, lease_token],
                )?;
                if changed != 1 {
                    return Err(tokio_rusqlite::Error::Other(Box::new(
                        std::io::Error::other("outbox lease changed during dead-letter move"),
                    )));
                }
                tx.commit()?;
                Ok(OutboxFailureDisposition::DeadLettered)
            } else {
                let changed = tx.execute(
                    queries::OUTBOX_MARK_FAILED,
                    rusqlite::params![id, lease_token, backoff_until_ms, error],
                )?;
                if changed != 1 {
                    tx.commit()?;
                    return Ok(OutboxFailureDisposition::LeaseLost);
                }
                tx.commit()?;
                Ok(OutboxFailureDisposition::RetryScheduled)
            }
        })
        .await
        .map_err(|error| anyhow::anyhow!("outbox record failure failed: {error}"))
    }

    async fn complete_outbox(&self, id: i64, lease_token: &str) -> anyhow::Result<bool> {
        let lease_token = lease_token.to_string();
        self.call("node_local:outbox_complete", move |conn| {
            let changed =
                conn.execute(queries::OUTBOX_COMPLETE, rusqlite::params![id, lease_token])?;
            Ok(changed > 0)
        })
        .await
        .map_err(|error| anyhow::anyhow!("outbox complete failed: {error}"))
    }

    async fn claim_due_outbox_batch(
        &self,
        now_ms: i64,
        limit: usize,
        lease_ms: i64,
        lease_token: &str,
    ) -> anyhow::Result<Vec<RawOutboxRow>> {
        let lease_token = lease_token.to_string();
        let limit_i64 = limit.min(256) as i64;
        self.call("node_local:outbox_claim_batch", move |conn| {
            let tx = conn.transaction()?;
            let ids = {
                let mut stmt = tx.prepare(queries::outbox_claim_due_batch())?;
                let rows =
                    stmt.query_map(rusqlite::params![now_ms, limit_i64], |row| row.get(0))?;
                rows.collect::<rusqlite::Result<Vec<i64>>>()?
            };
            if ids.is_empty() {
                tx.commit()?;
                return Ok(Vec::new());
            }
            let leased_until_ms = now_ms.saturating_add(lease_ms.max(1));
            let mut leased_ids = Vec::new();
            for id in ids {
                let changed = tx.execute(
                    queries::OUTBOX_SET_LEASE,
                    rusqlite::params![id, leased_until_ms, lease_token, now_ms],
                )?;
                if changed > 0 {
                    leased_ids.push(id);
                }
            }
            let mut rows = Vec::with_capacity(leased_ids.len());
            {
                let mut stmt = tx.prepare(queries::OUTBOX_ROW_SELECT)?;
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
        .map_err(|error| anyhow::anyhow!("outbox batch claim failed: {error}"))
    }

    async fn complete_superseded_status_outbox_for_terminal_pod_delete(
        &self,
        subject_key: &str,
        terminal_delete_id: i64,
    ) -> anyhow::Result<usize> {
        let subject_key = subject_key.to_string();
        self.call(
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
        .map_err(|error| anyhow::anyhow!("outbox complete superseded status failed: {error}"))
    }

    async fn requeue_expired_outbox_leases(&self, now_ms: i64) -> anyhow::Result<usize> {
        self.call("node_local:outbox_requeue_expired", move |conn| {
            conn.execute(queries::OUTBOX_REQUEUE_EXPIRED, [now_ms])
                .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|error| anyhow::anyhow!("outbox requeue expired failed: {error}"))
    }

    async fn next_outbox_wake_ms(&self, now_ms: i64) -> anyhow::Result<Option<i64>> {
        self.call("node_local:outbox_next_wake", move |conn| {
            conn.query_row(queries::OUTBOX_NEXT_WAKE, [now_ms], |row| {
                row.get::<_, Option<i64>>(0)
            })
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|error| anyhow::anyhow!("outbox next wake failed: {error}"))
    }

    async fn move_outbox_to_dead_letter_if_max_attempts(
        &self,
        idempotency_key: &str,
        max_attempts: i64,
    ) -> anyhow::Result<bool> {
        let idempotency_key = idempotency_key.to_string();
        let now = self.now_ms();
        self.call("node_local:outbox_dead_letter_move", move |conn| {
            let tx = conn.transaction()?;
            let row = tx
                .query_row(
                    "SELECT id, attempt FROM outbox WHERE idempotency_key = ?1",
                    [&idempotency_key],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                )
                .optional()?;
            let Some((original_id, attempt)) = row else {
                tx.commit()?;
                return Ok(false);
            };
            if attempt < max_attempts {
                tx.commit()?;
                return Ok(false);
            }
            let dead_row = tx
                .query_row(queries::OUTBOX_ROW_SELECT, [original_id], row_to_outbox)
                .optional()?;
            let Some(dead_row) = dead_row else {
                tx.commit()?;
                return Ok(false);
            };
            tx.execute(
                queries::DEAD_LETTER_INSERT,
                rusqlite::params![
                    dead_row.id,
                    dead_row.client_id,
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
                    dead_row.stream_id,
                    dead_row.stream_seq,
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
        .map_err(|error| anyhow::anyhow!("outbox dead letter move failed: {error}"))
    }

    async fn list_dead_letter(&self) -> anyhow::Result<Vec<RawDeadLetterRow>> {
        self.call("node_local:outbox_dead_letter_list", move |conn| {
            let mut stmt = conn.prepare(queries::DEAD_LETTER_LIST)?;
            let rows = stmt
                .query_map([], row_to_dead_letter)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .map_err(|error| anyhow::anyhow!("dead letter list failed: {error}"))
    }

    async fn get_dead_letter(&self, id: i64) -> anyhow::Result<Option<RawDeadLetterRow>> {
        self.call("node_local:outbox_dead_letter_get", move |conn| {
            conn.query_row(queries::DEAD_LETTER_GET, [id], row_to_dead_letter)
                .optional()
                .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|error| anyhow::anyhow!("dead letter get failed: {error}"))
    }

    async fn delete_dead_letter(&self, id: i64) -> anyhow::Result<bool> {
        self.call("node_local:outbox_dead_letter_delete", move |conn| {
            let changed = conn.execute(queries::DEAD_LETTER_DELETE, [id])?;
            Ok(changed > 0)
        })
        .await
        .map_err(|error| anyhow::anyhow!("dead letter delete failed: {error}"))
    }

    async fn replay_dead_letter(
        &self,
        id: i64,
        classification: OutboxClassification,
    ) -> anyhow::Result<bool> {
        let now = self.now_ms();
        let row = self
            .call("node_local:outbox_dead_letter_replay_get", move |conn| {
                Ok(conn
                    .query_row(queries::DEAD_LETTER_GET, [id], row_to_dead_letter)
                    .optional()?)
            })
            .await
            .map_err(|error| anyhow::anyhow!("outbox dead letter replay read failed: {error}"))?;
        let Some(row) = row else {
            return Ok(false);
        };
        let (priority_class, supersedable_pod_status, is_terminal_pod_delete, sequence_policy) =
            classification.persisted_values();
        self.call("node_local:outbox_dead_letter_replay", move |conn| {
            let tx = conn.transaction()?;
            let client_id = if row.client_id.is_empty() {
                crate::schema::ensure_outbox_client_id_in_tx(&tx)?
            } else {
                row.client_id
            };
            let sequenced = sequence_policy
                == klights_node_store::OutboxSequencePolicy::PerSubject.persisted_value();
            let stream_id = if row.stream_id > 0 {
                row.stream_id
            } else if sequenced {
                crate::schema::outbox_stream_id(&row.subject_key)
            } else {
                0
            };
            let stream_seq = if row.stream_seq > 0 {
                row.stream_seq
            } else if sequenced {
                crate::schema::allocate_next_outbox_stream_seq(&tx, stream_id)?
            } else {
                0
            };
            tx.execute(
                queries::OUTBOX_INSERT,
                rusqlite::params![
                    client_id,
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
                    priority_class,
                    supersedable_pod_status,
                    is_terminal_pod_delete,
                    stream_id,
                    stream_seq,
                    row.payload_proto,
                    now,
                ],
            )?;
            tx.execute(queries::DEAD_LETTER_DELETE_AFTER_REPLAY, [id])?;
            tx.commit()?;
            Ok(true)
        })
        .await
        .map_err(|error| anyhow::anyhow!("dead letter replay failed: {error}"))
    }

    async fn outbox_stats(&self) -> anyhow::Result<OutboxStats> {
        let now = self.now_ms();
        self.call("node_local:outbox_stats", move |conn| {
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
            let dispatch_total = conn
                .query_row(
                    "SELECT COALESCE(CAST(value AS INTEGER), 0) FROM _node_meta WHERE key = 'outbox_dispatch_total'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap_or(0);
            let dispatch_errors_total = conn
                .query_row(
                    "SELECT COALESCE(CAST(value AS INTEGER), 0) FROM _node_meta WHERE key = 'outbox_dispatch_errors_total'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap_or(0);
            OutboxStats::try_new(
                pending,
                oldest_age_seconds,
                dead_letter_count,
                dispatch_total,
                dispatch_errors_total,
            )
            .map_err(|error| tokio_rusqlite::Error::Other(Box::new(error)))
        })
        .await
        .map_err(|error| anyhow::anyhow!("outbox stats failed: {error}"))
    }

    async fn read_replication_checkpoint(&self) -> anyhow::Result<Option<ReplicationCheckpoint>> {
        self.call("node_local:checkpoint_get", move |conn| {
            conn.query_row(queries::REPLICATION_CHECKPOINT_GET, [], |row| {
                Ok(ReplicationCheckpoint::new(
                    row.get(0)?,
                    row.get(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .optional()
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|error| anyhow::anyhow!("replication checkpoint get failed: {error}"))
    }

    async fn write_replication_checkpoint(
        &self,
        checkpoint: ReplicationCheckpoint,
    ) -> anyhow::Result<()> {
        let (last_applied_rv, leader_epoch, cluster_id) = checkpoint.into_parts();
        self.call("node_local:checkpoint_set", move |conn| {
            conn.execute(
                queries::REPLICATION_CHECKPOINT_SET,
                rusqlite::params![last_applied_rv, leader_epoch, cluster_id],
            )?;
            Ok(())
        })
        .await
        .map_err(|error| anyhow::anyhow!("replication checkpoint set failed: {error}"))
    }
}

fn row_to_outbox(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawOutboxRow> {
    Ok(RawOutboxRow {
        id: row.get(0)?,
        client_id: row.get(1)?,
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
        priority_class: row.get(12)?,
        supersedable_pod_status: row.get::<_, i64>(13)? != 0,
        is_terminal_pod_delete: row.get::<_, i64>(14)? != 0,
        stream_id: row.get(15)?,
        stream_seq: row.get(16)?,
        payload_proto: row.get(17)?,
        attempt: row.get(18)?,
        next_due_ms: row.get(19)?,
        leased_until_ms: row.get(20)?,
        lease_token: row.get(21)?,
        last_error: row.get(22)?,
    })
}

fn row_to_dead_letter(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawDeadLetterRow> {
    Ok(RawDeadLetterRow {
        id: row.get(0)?,
        original_id: row.get(1)?,
        client_id: row.get(2)?,
        idempotency_key: row.get(3)?,
        enqueued_ms: row.get(4)?,
        subject_key: row.get(5)?,
        subject_api_version: row.get(6)?,
        subject_kind: row.get(7)?,
        subject_namespace: row.get(8)?,
        subject_name: row.get(9)?,
        subject_uid: row.get(10)?,
        pod_uid: row.get(11)?,
        operation: row.get(12)?,
        stream_id: row.get(13)?,
        stream_seq: row.get(14)?,
        payload_proto: row.get(15)?,
        attempts: row.get(16)?,
        last_error: row.get(17)?,
        moved_at_ms: row.get(18)?,
    })
}

fn row_to_pod_status_checkpoint(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<RawPodStatusCheckpoint> {
    let status_json: Vec<u8> = row.get(5)?;
    let status = serde_json::from_slice(&status_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Blob, Box::new(error))
    })?;
    Ok(RawPodStatusCheckpoint {
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
) -> rusqlite::Result<RawRuntimeObservationCheckpoint> {
    let container_ids_json: String = row.get(1)?;
    let container_ids = serde_json::from_str(&container_ids_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(RawRuntimeObservationCheckpoint {
        pod_uid: row.get(0)?,
        container_ids,
        generation: row.get::<_, i64>(2)? as u64,
        updated_ms: row.get(3)?,
    })
}

fn persistence(error: impl std::fmt::Display) -> DeliveryError {
    DeliveryError::persistence_failed(error.to_string())
}

fn classification_for_row(
    row: &RawOutboxRow,
) -> Result<klights_node_store::OutboxClassification, DeliveryError> {
    let supersedability = if row.supersedable_pod_status {
        OutboxSupersedability::PodStatus
    } else {
        OutboxSupersedability::Never
    };
    let terminal_delete = if row.is_terminal_pod_delete {
        TerminalDeleteClassification::ActorOwnedPodDelete
    } else {
        TerminalDeleteClassification::NotTerminalDelete
    };
    let sequence_policy = if row.stream_id > 0 || row.stream_seq > 0 {
        OutboxSequencePolicy::PerSubject
    } else {
        OutboxSequencePolicy::Unsequenced
    };
    klights_node_store::OutboxClassification::try_from_persisted(
        row.priority_class,
        supersedability.persisted_value(),
        terminal_delete.persisted_value(),
        sequence_policy.persisted_value(),
    )
}

fn outbox_record(row: RawOutboxRow) -> Result<OutboxRecord, DeliveryError> {
    let classification = classification_for_row(&row)?;
    let subject = klights_node_store::OutboxSubject::new(
        row.subject_key,
        ResourceKey::new(
            row.subject_api_version,
            row.subject_kind,
            row.subject_namespace,
            row.subject_name,
        ),
        row.subject_uid,
        row.pod_uid,
    );
    OutboxRecord::try_new(
        row.id,
        row.client_id,
        row.idempotency_key,
        row.enqueued_ms,
        subject,
        row.operation,
        classification,
        OutboxSequence::try_new(row.stream_id, row.stream_seq)?,
        row.payload_proto,
        row.attempt,
        row.next_due_ms,
        row.leased_until_ms,
        row.lease_token,
        row.last_error,
    )
}

fn dead_letter_entry(row: RawDeadLetterRow) -> Result<DeadLetterEntry, DeliveryError> {
    let subject = klights_node_store::OutboxSubject::new(
        row.subject_key,
        ResourceKey::new(
            row.subject_api_version,
            row.subject_kind,
            row.subject_namespace,
            row.subject_name,
        ),
        row.subject_uid,
        row.pod_uid,
    );
    DeadLetterEntry::try_new(
        row.id,
        row.original_id,
        row.client_id,
        row.idempotency_key,
        row.enqueued_ms,
        subject,
        row.operation,
        OutboxSequence::try_new(row.stream_id, row.stream_seq)?,
        row.payload_proto,
        row.attempts,
        row.last_error,
        row.moved_at_ms,
    )
}

impl OutboxProducerStore for SqliteDeliveryStore {
    fn enqueue_outbox(&self, entry: OutboxEnqueue) -> DeliveryFuture<'_, ()> {
        Box::pin(async move {
            let (
                idempotency_key,
                enqueued_ms,
                subject,
                operation,
                classification,
                payload_proto,
                next_due_ms,
            ) = entry.into_parts();
            let (subject_key, resource, subject_uid, pod_uid) = subject.into_parts();
            self.enqueue_outbox(RawOutboxInsert {
                idempotency_key,
                enqueued_ms,
                subject_key,
                subject_api_version: resource.api_version,
                subject_kind: resource.kind,
                subject_namespace: resource.namespace,
                subject_name: resource.name,
                subject_uid,
                pod_uid,
                operation,
                classification,
                payload_proto,
                next_due_ms,
            })
            .await
            .map_err(persistence)
        })
    }
}

impl OutboxDispatcherStore for SqliteDeliveryStore {
    fn claim_next_due_outbox(
        &self,
        request: OutboxClaimRequest,
    ) -> DeliveryFuture<'_, Option<OutboxRecord>> {
        Box::pin(async move {
            self.claim_next_due_outbox(request.now_ms(), request.lease_ms(), request.lease_token())
                .await
                .map_err(persistence)?
                .map(outbox_record)
                .transpose()
        })
    }

    fn renew_outbox_lease(&self, lease: OutboxLease) -> DeliveryFuture<'_, bool> {
        Box::pin(async move {
            self.renew_outbox_lease(lease.id(), lease.lease_token(), lease.leased_until_ms())
                .await
                .map_err(persistence)
        })
    }

    fn mark_outbox_attempt_failed(
        &self,
        failure: OutboxAttemptFailure,
    ) -> DeliveryFuture<'_, bool> {
        Box::pin(async move {
            self.mark_outbox_attempt_failed(
                failure.id(),
                failure.lease_token(),
                failure.backoff_until_ms(),
                failure.error(),
            )
            .await
            .map_err(persistence)
        })
    }

    fn record_outbox_failure(
        &self,
        failure: OutboxAttemptFailureRecord,
    ) -> DeliveryFuture<'_, OutboxFailureDisposition> {
        Box::pin(async move {
            self.record_outbox_failure(
                failure.id(),
                failure.lease_token(),
                failure.backoff_until_ms(),
                failure.error(),
                failure.max_attempts(),
            )
            .await
            .map_err(persistence)
        })
    }

    fn complete_outbox(&self, completion: OutboxCompletion) -> DeliveryFuture<'_, bool> {
        Box::pin(async move {
            self.complete_outbox(completion.id(), completion.lease_token())
                .await
                .map_err(persistence)
        })
    }

    fn requeue_expired_outbox_leases(&self, now: OutboxNow) -> DeliveryFuture<'_, usize> {
        Box::pin(async move {
            self.requeue_expired_outbox_leases(now.get())
                .await
                .map_err(persistence)
        })
    }

    fn next_outbox_wake_ms(&self, now: OutboxNow) -> DeliveryFuture<'_, Option<i64>> {
        Box::pin(async move {
            self.next_outbox_wake_ms(now.get())
                .await
                .map_err(persistence)
        })
    }

    fn claim_due_outbox_batch(
        &self,
        request: OutboxBatchClaimRequest,
    ) -> DeliveryFuture<'_, Vec<OutboxRecord>> {
        Box::pin(async move {
            self.claim_due_outbox_batch(
                request.now_ms(),
                request.effective_limit(),
                request.lease_ms(),
                request.lease_token(),
            )
            .await
            .map_err(persistence)?
            .into_iter()
            .map(outbox_record)
            .collect()
        })
    }

    fn complete_superseded_status_outbox_for_terminal_pod_delete(
        &self,
        request: klights_node_store::OutboxSupersedeRequest,
    ) -> DeliveryFuture<'_, usize> {
        Box::pin(async move {
            self.complete_superseded_status_outbox_for_terminal_pod_delete(
                request.subject_key(),
                request.terminal_delete_id(),
            )
            .await
            .map_err(persistence)
        })
    }

    fn write_dispatch_counters(&self, counters: OutboxDispatchCounters) -> DeliveryFuture<'_, ()> {
        Box::pin(async move {
            self.set_node_meta(
                OUTBOX_DISPATCH_TOTAL_META_KEY,
                &counters.dispatch_total().to_string(),
            )
            .await
            .map_err(persistence)?;
            self.set_node_meta(
                OUTBOX_DISPATCH_ERRORS_META_KEY,
                &counters.dispatch_errors_total().to_string(),
            )
            .await
            .map_err(persistence)
        })
    }
}

impl OutboxStatusStampStore for SqliteDeliveryStore {
    fn read_status_stamp_high_water(&self) -> DeliveryFuture<'_, i64> {
        Box::pin(async move {
            let raw = self
                .get_node_meta(STATUS_STAMP_META_KEY)
                .await
                .map_err(persistence)?;
            raw.map(|value| {
                value.parse::<i64>().map_err(|error| {
                    DeliveryError::corrupt_data(format!(
                        "invalid status-stamp high-water {value:?}: {error}"
                    ))
                })
            })
            .transpose()
            .map(|value| value.unwrap_or(0))
        })
    }

    fn write_status_stamp_high_water(&self, high_water: i64) -> DeliveryFuture<'_, ()> {
        Box::pin(async move {
            if high_water < 0 {
                return Err(DeliveryError::corrupt_data(
                    "status-stamp high-water must be non-negative",
                ));
            }
            self.set_node_meta(STATUS_STAMP_META_KEY, &high_water.to_string())
                .await
                .map_err(persistence)
        })
    }
}

impl DeadLetterStore for SqliteDeliveryStore {
    fn move_outbox_to_dead_letter_if_max_attempts(
        &self,
        request: DeadLetterMoveRequest,
    ) -> DeliveryFuture<'_, bool> {
        Box::pin(async move {
            self.move_outbox_to_dead_letter_if_max_attempts(
                request.idempotency_key(),
                request.max_attempts(),
            )
            .await
            .map_err(persistence)
        })
    }

    fn list_dead_letter(&self) -> DeliveryFuture<'_, Vec<DeadLetterEntry>> {
        Box::pin(async move {
            self.list_dead_letter()
                .await
                .map_err(persistence)?
                .into_iter()
                .map(dead_letter_entry)
                .collect()
        })
    }

    fn get_dead_letter(&self, key: DeadLetterKey) -> DeliveryFuture<'_, Option<DeadLetterEntry>> {
        Box::pin(async move {
            self.get_dead_letter(key.get())
                .await
                .map_err(persistence)?
                .map(dead_letter_entry)
                .transpose()
        })
    }

    fn delete_dead_letter(&self, key: DeadLetterKey) -> DeliveryFuture<'_, bool> {
        Box::pin(async move {
            self.delete_dead_letter(key.get())
                .await
                .map_err(persistence)
        })
    }

    fn replay_dead_letter(&self, request: DeadLetterReplayRequest) -> DeliveryFuture<'_, bool> {
        Box::pin(async move {
            self.replay_dead_letter(request.key().get(), request.classification())
                .await
                .map_err(persistence)
        })
    }

    fn outbox_stats(&self) -> DeliveryFuture<'_, OutboxStats> {
        Box::pin(async move { self.outbox_stats().await.map_err(persistence) })
    }
}

impl PodStatusCheckpointStore for SqliteDeliveryStore {
    fn upsert_pod_status_checkpoint(
        &self,
        checkpoint: PodStatusCheckpointUpsert,
    ) -> DeliveryFuture<'_, ()> {
        Box::pin(async move {
            let (pod, base_position, status_payload, updated_ms) = checkpoint.into_parts();
            let status = serde_json::from_slice(&status_payload).map_err(|error| {
                DeliveryError::corrupt_data(format!(
                    "invalid Pod status checkpoint payload: {error}"
                ))
            })?;
            self.upsert_pod_status_checkpoint(
                &pod.uid,
                &pod.namespace,
                &pod.name,
                base_position,
                status,
                updated_ms,
            )
            .await
            .map_err(persistence)
        })
    }

    fn get_pod_status_checkpoint(
        &self,
        key: PodCheckpointKey,
    ) -> DeliveryFuture<'_, Option<PodStatusCheckpoint>> {
        Box::pin(async move {
            let checkpoint = self
                .get_pod_status_checkpoint(key.pod_uid())
                .await
                .map_err(persistence)?;
            checkpoint
                .map(|checkpoint| {
                    let status_payload =
                        serde_json::to_vec(&checkpoint.status).map_err(persistence)?;
                    PodStatusCheckpoint::try_new(
                        PodIdentity::new(
                            &checkpoint.namespace,
                            &checkpoint.pod_name,
                            &checkpoint.pod_uid,
                        ),
                        checkpoint.base_rv,
                        checkpoint.applied_rv,
                        status_payload,
                        checkpoint.updated_ms,
                    )
                })
                .transpose()
        })
    }

    fn mark_pod_status_checkpoint_applied(
        &self,
        applied: PodStatusCheckpointApplied,
    ) -> DeliveryFuture<'_, ()> {
        Box::pin(async move {
            self.mark_pod_status_checkpoint_applied(
                applied.pod_uid(),
                applied.applied_position(),
                applied.updated_ms(),
            )
            .await
            .map_err(persistence)
        })
    }

    fn delete_pod_status_checkpoint(&self, key: PodCheckpointKey) -> DeliveryFuture<'_, ()> {
        Box::pin(async move {
            self.delete_pod_status_checkpoint(key.pod_uid())
                .await
                .map_err(persistence)
        })
    }
}

impl RuntimeObservationCheckpointStore for SqliteDeliveryStore {
    fn upsert_runtime_observation_checkpoint(
        &self,
        checkpoint: RuntimeObservationCheckpoint,
    ) -> DeliveryFuture<'_, ()> {
        Box::pin(async move {
            let (pod_uid, container_ids, generation, updated_ms) = checkpoint.into_parts();
            self.upsert_runtime_observation_checkpoint(RawRuntimeObservationCheckpoint {
                pod_uid,
                container_ids,
                generation: generation.get() as u64,
                updated_ms,
            })
            .await
            .map_err(persistence)
        })
    }

    fn get_runtime_observation_checkpoint(
        &self,
        key: PodCheckpointKey,
    ) -> DeliveryFuture<'_, Option<RuntimeObservationCheckpoint>> {
        Box::pin(async move {
            self.get_runtime_observation_checkpoint(key.pod_uid())
                .await
                .map_err(persistence)?
                .map(|checkpoint| {
                    RuntimeObservationCheckpoint::try_new(
                        checkpoint.pod_uid,
                        checkpoint.container_ids,
                        RuntimeObservationGeneration::try_from(checkpoint.generation)?,
                        checkpoint.updated_ms,
                    )
                })
                .transpose()
        })
    }

    fn delete_runtime_observation_checkpoint(
        &self,
        key: PodCheckpointKey,
    ) -> DeliveryFuture<'_, ()> {
        Box::pin(async move {
            self.delete_runtime_observation_checkpoint(key.pod_uid())
                .await
                .map_err(persistence)
        })
    }
}

impl ReplicationCheckpointStore for SqliteDeliveryStore {
    fn read_replication_checkpoint(&self) -> DeliveryFuture<'_, Option<ReplicationCheckpoint>> {
        Box::pin(async move {
            self.read_replication_checkpoint()
                .await
                .map_err(persistence)
        })
    }

    fn write_replication_checkpoint(
        &self,
        checkpoint: ReplicationCheckpoint,
    ) -> DeliveryFuture<'_, ()> {
        Box::pin(async move {
            self.write_replication_checkpoint(checkpoint)
                .await
                .map_err(persistence)
        })
    }
}
