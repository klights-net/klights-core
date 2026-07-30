use anyhow::Result;
#[cfg(test)]
use anyhow::anyhow;
use klights_node_datastore::{
    SqliteNodeIdentity, SqliteNodeNetworkStateStore, SqliteRaftDurability, SqliteRuntimeWorkStore,
    delivery::SqliteDeliveryStore,
};

use klights_supervisor::DbExecutor;
#[cfg(test)]
use rusqlite::OptionalExtension;
#[cfg(test)]
use serde_json::Value;
#[cfg(test)]
use sha2::{Digest, Sha256};

#[derive(Clone)]
pub struct SqliteNodeLocalDb {
    executor: DbExecutor,
    identity: std::sync::Arc<SqliteNodeIdentity>,
    raft_persistence: std::sync::Arc<SqliteRaftDurability>,
    delivery: std::sync::Arc<SqliteDeliveryStore>,
    network_state: std::sync::Arc<SqliteNodeNetworkStateStore>,
    runtime_work: std::sync::Arc<SqliteRuntimeWorkStore>,
}

#[cfg(test)]
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
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeObservationCheckpoint {
    pub pod_uid: String,
    pub container_ids: Vec<String>,
    pub generation: u64,
    pub updated_ms: i64,
}

#[cfg(test)]
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
    pub classification: klights_node_store::OutboxClassification,
    pub payload_proto: Vec<u8>,
    pub next_due_ms: i64,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxRow {
    pub id: i64,
    pub client_id: String,
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
    pub priority_class: i64,
    pub supersedable_pod_status: bool,
    pub is_terminal_pod_delete: bool,
    pub stream_id: i64,
    pub stream_seq: i64,
    pub payload_proto: Vec<u8>,
    pub attempt: i64,
    pub next_due_ms: i64,
    pub leased_until_ms: i64,
    pub lease_token: Option<String>,
    pub last_error: Option<String>,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationCheckpoint {
    pub last_applied_rv: i64,
    pub leader_epoch: i64,
    pub cluster_id: String,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct DeadLetterRow {
    pub id: i64,
    pub original_id: i64,
    pub client_id: String,
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
    pub stream_id: i64,
    pub stream_seq: i64,
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

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub struct OutboxStats {
    pub pending: i64,
    pub oldest_age_seconds: f64,
    pub dead_letter_count: i64,
    pub dispatch_total: i64,
    pub dispatch_errors_total: i64,
}

impl SqliteNodeLocalDb {
    #[cfg(test)]
    pub fn from_executor(executor: DbExecutor) -> Result<Self> {
        Self::from_executor_with_clock(
            executor,
            std::sync::Arc::new(klights_supervisor::SystemWallClock),
        )
    }

    pub fn from_executor_with_clock(
        executor: DbExecutor,
        wall_clock: std::sync::Arc<dyn klights_supervisor::WallClock>,
    ) -> Result<Self> {
        Ok(Self {
            identity: std::sync::Arc::new(SqliteNodeIdentity::new(executor.clone())),
            raft_persistence: std::sync::Arc::new(SqliteRaftDurability::new(executor.clone())),
            delivery: std::sync::Arc::new(SqliteDeliveryStore::new(
                executor.clone(),
                wall_clock.clone(),
            )),
            network_state: std::sync::Arc::new(SqliteNodeNetworkStateStore::new(
                executor.clone(),
                wall_clock.clone(),
            )),
            runtime_work: std::sync::Arc::new(SqliteRuntimeWorkStore::new(
                executor.clone(),
                wall_clock,
            )),
            executor,
        })
    }

    pub(crate) fn raft_persistence(&self) -> std::sync::Arc<SqliteRaftDurability> {
        self.raft_persistence.clone()
    }

    pub(crate) fn raft_persistence_ref(&self) -> &SqliteRaftDurability {
        &self.raft_persistence
    }

    pub(crate) fn identity_ref(&self) -> &SqliteNodeIdentity {
        &self.identity
    }

    pub(crate) fn delivery_ref(&self) -> &SqliteDeliveryStore {
        &self.delivery
    }

    pub(crate) fn network_state_ref(&self) -> &SqliteNodeNetworkStateStore {
        &self.network_state
    }

    pub(crate) fn runtime_work_ref(&self) -> &SqliteRuntimeWorkStore {
        &self.runtime_work
    }

    pub async fn db_call<T, F>(&self, query_name: &'static str, f: F) -> tokio_rusqlite::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut rusqlite::Connection) -> tokio_rusqlite::Result<T> + Send + 'static,
    {
        self.executor.call_raw(query_name, f).await
    }

    #[cfg(test)]
    pub async fn outbox_stream_position_for_test(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<(i64, i64)>> {
        let idempotency_key = idempotency_key.to_string();
        self.db_call("node_local:outbox_stream_position_test", move |conn| {
            conn.query_row(
                "SELECT stream_id, stream_seq FROM outbox WHERE idempotency_key = ?1",
                [idempotency_key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|error| anyhow!("outbox stream-position test read failed: {error}"))
    }

    #[cfg(test)]
    pub async fn clear_outbox_stream_identity_for_test(&self, idempotency_key: &str) -> Result<()> {
        let idempotency_key = idempotency_key.to_string();
        self.db_call("node_local:outbox_legacy_stream_test", move |conn| {
            let tx = conn.transaction()?;
            let changed = tx.execute(
                "UPDATE outbox SET client_id = '', stream_id = 0, stream_seq = 0 \
                 WHERE idempotency_key = ?1",
                [&idempotency_key],
            )?;
            if changed != 1 {
                return Err(tokio_rusqlite::Error::Other(Box::new(
                    std::io::Error::other(format!(
                        "test legacy identity mutation changed {changed} rows instead of exactly one"
                    )),
                )));
            }
            tx.execute("DELETE FROM outbox_stream_sequences", [])?;
            tx.commit()?;
            Ok(())
        })
        .await
        .map_err(|error| anyhow!("outbox legacy stream test mutation failed: {error}"))
    }

    #[cfg(test)]
    pub async fn clear_all_outbox_stream_identity_for_test(&self) -> Result<()> {
        self.db_call("node_local:outbox_legacy_stream_all_test", move |conn| {
            let tx = conn.transaction()?;
            tx.execute(
                "UPDATE outbox SET client_id = '', stream_id = 0, stream_seq = 0 \
                 WHERE operation != 'LeaseRenew'",
                [],
            )?;
            tx.execute(
                "UPDATE outbox_dead_letter SET client_id = '', stream_id = 0, stream_seq = 0 \
                 WHERE operation != 'LeaseRenew'",
                [],
            )?;
            tx.execute("DELETE FROM outbox_stream_sequences", [])?;
            tx.commit()?;
            Ok(())
        })
        .await
        .map_err(|error| anyhow!("outbox legacy stream test mutation failed: {error}"))
    }

    #[cfg(test)]
    pub async fn set_outbox_operation_for_test(
        &self,
        idempotency_key: &str,
        operation: &str,
    ) -> Result<()> {
        let idempotency_key = idempotency_key.to_string();
        let operation = operation.to_string();
        self.db_call("node_local:outbox_operation_test_update", move |conn| {
            conn.execute_batch("PRAGMA ignore_check_constraints = ON")?;
            let update = conn.execute(
                "UPDATE outbox SET operation = ?2 WHERE idempotency_key = ?1",
                rusqlite::params![idempotency_key, operation],
            );
            conn.execute_batch("PRAGMA ignore_check_constraints = OFF")?;
            let changed = update?;
            if changed != 1 {
                return Err(tokio_rusqlite::Error::Other(Box::new(
                    std::io::Error::other(format!(
                        "test operation-only mutation changed {changed} rows instead of exactly one"
                    )),
                )));
            }
            Ok(())
        })
        .await
        .map_err(|error| anyhow!("outbox operation test update failed: {error}"))
    }

    #[cfg(test)]
    pub async fn outbox_operation_for_test(&self, idempotency_key: &str) -> Result<Option<String>> {
        let idempotency_key = idempotency_key.to_string();
        self.db_call("node_local:outbox_operation_test_read", move |conn| {
            conn.query_row(
                "SELECT operation FROM outbox WHERE idempotency_key = ?1",
                [idempotency_key],
                |row| row.get(0),
            )
            .optional()
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|error| anyhow!("outbox operation test read failed: {error}"))
    }

    #[cfg(test)]
    pub async fn set_outbox_attempt_for_test(
        &self,
        idempotency_key: &str,
        attempt: i64,
    ) -> Result<()> {
        let idempotency_key = idempotency_key.to_string();
        self.db_call("node_local:outbox_attempt_test_update", move |conn| {
            let changed = conn.execute(
                "UPDATE outbox SET attempt = ?2 WHERE idempotency_key = ?1",
                rusqlite::params![idempotency_key, attempt],
            )?;
            if changed != 1 {
                return Err(tokio_rusqlite::Error::Other(Box::new(
                    std::io::Error::other(format!(
                        "test attempt mutation changed {changed} rows instead of exactly one"
                    )),
                )));
            }
            Ok(())
        })
        .await
        .map_err(|error| anyhow!("outbox attempt test update failed: {error}"))
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
                "INSERT INTO outbox_dead_letter \
                 (original_id, client_id, idempotency_key, enqueued_ms, subject_key, \
                  subject_api_version, subject_kind, subject_namespace, subject_name, subject_uid, \
                  pod_uid, operation, stream_id, stream_seq, payload_proto, attempts, last_error, \
                  moved_at_ms) VALUES \
                 (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)",
                rusqlite::params![
                    0_i64,
                    "",
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
                    0_i64,
                    0_i64,
                    payload_proto,
                    attempts,
                    last_error,
                    moved_at_ms,
                ],
            )?;
            Ok(())
        })
        .await
        .map_err(|error| anyhow!("dead letter test insert failed: {error}"))
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

#[cfg(test)]
fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

#[cfg(test)]
pub fn outbox_stream_id(subject_key: &str) -> i64 {
    let digest = Sha256::digest(subject_key.as_bytes());
    let mut shard_bytes = [0_u8; 8];
    shard_bytes.copy_from_slice(&digest[..8]);
    let value = u64::from_be_bytes(shard_bytes);
    let stream_id = (value & i64::MAX as u64) as i64;
    if stream_id == 0 { 1 } else { stream_id }
}
