use anyhow::Result;
#[cfg(any(test, feature = "integration-test-harness"))]
use anyhow::anyhow;
#[cfg(any(test, feature = "integration-test-harness"))]
use rusqlite::OptionalExtension;
#[cfg(any(test, feature = "integration-test-harness"))]
use serde_json::Value;

#[cfg(any(test, feature = "integration-test-harness"))]
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

#[cfg(any(test, feature = "integration-test-harness"))]
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

#[cfg(any(test, feature = "integration-test-harness"))]
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

#[cfg(any(test, feature = "integration-test-harness"))]
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

#[cfg(any(test, feature = "integration-test-harness"))]
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

#[cfg(any(test, feature = "integration-test-harness"))]
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub struct OutboxStats {
    pub pending: i64,
    pub oldest_age_seconds: f64,
    pub dead_letter_count: i64,
    pub dispatch_total: i64,
    pub dispatch_errors_total: i64,
}

#[cfg(any(test, feature = "integration-test-harness"))]
impl super::NodeLocalStores {
    pub(crate) async fn with_test_connection<T, F>(
        &self,
        query_name: &'static str,
        f: F,
    ) -> tokio_rusqlite::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut rusqlite::Connection) -> tokio_rusqlite::Result<T> + Send + 'static,
    {
        self.executor_for_test().call_raw(query_name, f).await
    }

    #[cfg(any(test, feature = "integration-test-harness"))]
    pub async fn outbox_stream_position_for_test(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<(i64, i64)>> {
        let idempotency_key = idempotency_key.to_string();
        self.with_test_connection("node_local:outbox_stream_position_test", move |conn| {
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

    #[cfg(any(test, feature = "integration-test-harness"))]
    pub async fn set_outbox_operation_for_test(
        &self,
        idempotency_key: &str,
        operation: &str,
    ) -> Result<()> {
        let idempotency_key = idempotency_key.to_string();
        let operation = operation.to_string();
        self.with_test_connection("node_local:outbox_operation_test_update", move |conn| {
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

    #[cfg(any(test, feature = "integration-test-harness"))]
    pub async fn outbox_operation_for_test(&self, idempotency_key: &str) -> Result<Option<String>> {
        let idempotency_key = idempotency_key.to_string();
        self.with_test_connection("node_local:outbox_operation_test_read", move |conn| {
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

    #[cfg(any(test, feature = "integration-test-harness"))]
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
        self.with_test_connection("node_local:dead_letter_test_insert", move |conn| {
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
}
