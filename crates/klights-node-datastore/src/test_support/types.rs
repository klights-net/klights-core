use serde_json::Value;

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
