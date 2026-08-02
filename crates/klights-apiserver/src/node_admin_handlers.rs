use std::sync::Arc;

use axum::Json;
use axum::http::StatusCode;
use klights_node_api::{NodeDeadLetter, NodeDeadLetterAdmin, NodeOutboxDiagnostics};

#[derive(Clone)]
pub struct NodeAdminEndpointInputs {
    pub(super) outbox: Arc<dyn NodeOutboxDiagnostics>,
    pub(super) dead_letters: Arc<dyn NodeDeadLetterAdmin>,
}

impl NodeAdminEndpointInputs {
    pub fn new(
        outbox: Arc<dyn NodeOutboxDiagnostics>,
        dead_letters: Arc<dyn NodeDeadLetterAdmin>,
    ) -> Self {
        Self {
            outbox,
            dead_letters,
        }
    }
}

#[derive(serde::Serialize)]
pub(super) struct OutboxStatusResponse {
    outbox_pending: i64,
    outbox_oldest_age_seconds: f64,
    outbox_dispatch_total: u64,
    outbox_dispatch_errors_total: u64,
    outbox_dead_letter_total: i64,
}

#[derive(serde::Serialize)]
pub(super) struct DeadLetterResponse {
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

impl From<NodeDeadLetter> for DeadLetterResponse {
    fn from(entry: NodeDeadLetter) -> Self {
        Self {
            id: entry.id,
            original_id: entry.original_id,
            client_id: entry.client_id,
            idempotency_key: entry.idempotency_key,
            enqueued_ms: entry.enqueued_ms,
            subject_key: entry.subject_key,
            subject_api_version: entry.subject_api_version,
            subject_kind: entry.subject_kind,
            subject_namespace: entry.subject_namespace,
            subject_name: entry.subject_name,
            subject_uid: entry.subject_uid,
            pod_uid: entry.pod_uid,
            operation: entry.operation,
            stream_id: entry.stream_id,
            stream_seq: entry.stream_seq,
            payload_proto: entry.payload_proto,
            attempts: entry.attempts,
            last_error: entry.last_error,
            moved_at_ms: entry.moved_at_ms,
        }
    }
}

pub(super) async fn outbox_status(
    inputs: Arc<NodeAdminEndpointInputs>,
) -> Result<Json<OutboxStatusResponse>, StatusCode> {
    let status = inputs
        .outbox
        .outbox_status()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(OutboxStatusResponse {
        outbox_pending: status.pending,
        outbox_oldest_age_seconds: status.oldest_age_seconds,
        outbox_dispatch_total: status.dispatch_total,
        outbox_dispatch_errors_total: status.dispatch_errors_total,
        outbox_dead_letter_total: status.dead_letter_total,
    }))
}

pub(super) async fn dead_letter_list(
    inputs: Arc<NodeAdminEndpointInputs>,
) -> Result<Json<Vec<DeadLetterResponse>>, StatusCode> {
    let rows = inputs
        .dead_letters
        .list_dead_letters()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(rows.into_iter().map(Into::into).collect()))
}

pub(super) async fn dead_letter_replay(
    inputs: Arc<NodeAdminEndpointInputs>,
    id: i64,
) -> Result<StatusCode, StatusCode> {
    if id <= 0 {
        return Err(StatusCode::NOT_FOUND);
    }
    let replayed = inputs
        .dead_letters
        .replay_dead_letter(id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(if replayed {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    })
}

pub(super) async fn dead_letter_delete(
    inputs: Arc<NodeAdminEndpointInputs>,
    id: i64,
) -> Result<StatusCode, StatusCode> {
    if id <= 0 {
        return Err(StatusCode::NOT_FOUND);
    }
    let deleted = inputs
        .dead_letters
        .delete_dead_letter(id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(if deleted {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    })
}
