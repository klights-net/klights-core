use std::sync::Arc;

use axum::{
    Router,
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{delete, get, post},
};
use serde::Serialize;
use tokio::sync::Notify;

use klights_kubelet::node_outbox::payload::OutboxOperationExt as _;
use klights_node_store::{
    DeadLetterEntry, DeadLetterKey, DeadLetterReplayRequest, DeadLetterStore,
};

#[derive(Clone)]
struct AdminState {
    dead_letters: Arc<dyn DeadLetterStore>,
    outbox_notify: Arc<Notify>,
}

#[derive(Serialize)]
struct OutboxStatusResponse {
    outbox_pending: i64,
    outbox_oldest_age_seconds: f64,
    outbox_dispatch_total: u64,
    outbox_dispatch_errors_total: u64,
    outbox_dead_letter_total: i64,
}

#[derive(Serialize)]
struct DeadLetterResponse {
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

impl From<DeadLetterEntry> for DeadLetterResponse {
    fn from(entry: DeadLetterEntry) -> Self {
        let resource = entry.subject().resource();
        Self {
            id: entry.id(),
            original_id: entry.original_id(),
            client_id: entry.client_id().to_string(),
            idempotency_key: entry.idempotency_key().to_string(),
            enqueued_ms: entry.enqueued_ms(),
            subject_key: entry.subject().subject_key().to_string(),
            subject_api_version: resource.api_version.clone(),
            subject_kind: resource.kind.clone(),
            subject_namespace: resource.namespace.clone(),
            subject_name: resource.name.clone(),
            subject_uid: entry.subject().subject_uid().map(str::to_string),
            pod_uid: entry.subject().pod_uid().to_string(),
            operation: entry.operation().to_string(),
            stream_id: entry.sequence().stream_id(),
            stream_seq: entry.sequence().stream_seq(),
            payload_proto: entry.payload().to_vec(),
            attempts: entry.attempts(),
            last_error: entry.last_error().to_string(),
            moved_at_ms: entry.moved_at_ms(),
        }
    }
}

async fn outbox_status(
    State(state): State<AdminState>,
) -> Result<Json<OutboxStatusResponse>, StatusCode> {
    let stats = state
        .dead_letters
        .outbox_stats()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(OutboxStatusResponse {
        outbox_pending: stats.pending(),
        outbox_oldest_age_seconds: stats.oldest_age_seconds(),
        outbox_dispatch_total: stats.dispatch_total() as u64,
        outbox_dispatch_errors_total: stats.dispatch_errors_total() as u64,
        outbox_dead_letter_total: stats.dead_letter_count(),
    }))
}

async fn dead_letter_list(
    State(state): State<AdminState>,
) -> Result<Json<Vec<DeadLetterResponse>>, StatusCode> {
    let rows = state
        .dead_letters
        .list_dead_letter()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(rows.into_iter().map(Into::into).collect()))
}

async fn dead_letter_replay(
    State(state): State<AdminState>,
    Path(id): Path<i64>,
) -> Result<StatusCode, StatusCode> {
    let row = state
        .dead_letters
        .get_dead_letter(DeadLetterKey::try_new(id).map_err(|_| StatusCode::NOT_FOUND)?)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    let operation =
        klights_kubelet::node_outbox::payload::OutboxOperation::try_from(row.operation())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let payload =
        klights_leader_rpc::storage_wire_codec::decode_outbox_payload_protobuf(row.payload())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let classification = operation
        .classification(payload.command())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let replayed = state
        .dead_letters
        .replay_dead_letter(DeadLetterReplayRequest::new(
            DeadLetterKey::try_new(id).map_err(|_| StatusCode::NOT_FOUND)?,
            classification,
        ))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if replayed {
        state.outbox_notify.notify_one();
        Ok(StatusCode::OK)
    } else {
        Ok(StatusCode::NOT_FOUND)
    }
}

async fn dead_letter_delete(
    State(state): State<AdminState>,
    Path(id): Path<i64>,
) -> Result<StatusCode, StatusCode> {
    let deleted = state
        .dead_letters
        .delete_dead_letter(DeadLetterKey::try_new(id).map_err(|_| StatusCode::NOT_FOUND)?)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Ok(StatusCode::NOT_FOUND)
    }
}

fn build_router(dead_letters: Arc<dyn DeadLetterStore>, outbox_notify: Arc<Notify>) -> Router {
    let state = AdminState {
        dead_letters,
        outbox_notify,
    };
    Router::new()
        .route("/klights/v1/outbox/status", get(outbox_status))
        .route("/klights/v1/outbox/dead-letter", get(dead_letter_list))
        .route(
            "/klights/v1/outbox/dead-letter/{id}/replay",
            post(dead_letter_replay),
        )
        .route(
            "/klights/v1/outbox/dead-letter/{id}",
            delete(dead_letter_delete),
        )
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use crate::datastore::backend_kind::BackendKind;
    use crate::datastore::node_local::sqlite::DeadLetterTestInsert;
    use crate::datastore::node_local::{
        LegacyDeliveryTestStore as _, NodeLocalStores, OutboxInsert, selector,
    };
    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};

    fn supervisor() -> Arc<TaskSupervisor> {
        Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()))
    }

    fn pod_status_classification() -> klights_node_store::OutboxClassification {
        klights_node_store::OutboxClassification::try_new(
            klights_node_store::OutboxPriority::Workload,
            klights_node_store::OutboxSupersedability::PodStatus,
            klights_node_store::TerminalDeleteClassification::NotTerminalDelete,
            klights_node_store::OutboxSequencePolicy::PerSubject,
        )
        .expect("valid Pod status classification")
    }

    fn pod_status_payload() -> Vec<u8> {
        crate::outbox_test_support::OutboxPayload::from_command(
            klights_cluster_core::StorageCommand::UpdateStatus {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "web".to_string(),
                status: serde_json::json!({"phase": "Running"}),
                expected_rv: None,
                preconditions: klights_cluster_core::ResourcePreconditions::uid("uid-1"),
                observed_status_stamp: Some(1),
            },
        )
        .encode_protobuf()
        .expect("encode test Pod status outbox envelope")
    }

    async fn node_db() -> NodeLocalStores {
        selector::open_node_local(
            BackendKind::Sqlite,
            None,
            supervisor(),
            None,
            "sqlite:node-admin-test",
        )
        .await
        .expect("open node-local test db")
    }

    async fn node_db_with_dead_letter() -> (NodeLocalStores, i64) {
        let ndb = node_db().await;
        // Use the concrete NodeLocalStores for test-only insert.
        // We open a separate handle via selector and downcast isn't available,
        // so we insert via enqueue + move.
        ndb.legacy_enqueue_outbox(OutboxInsert {
            idempotency_key: "node-admin-dl-key".to_string(),
            enqueued_ms: 1000,
            subject_key: "v1/Pod/default/web/uid-1".to_string(),
            subject_api_version: "v1".to_string(),
            subject_kind: "Pod".to_string(),
            subject_namespace: Some("default".to_string()),
            subject_name: "web".to_string(),
            subject_uid: Some("uid-1".to_string()),
            pod_uid: "uid-1".to_string(),
            operation: "PodStatus".to_string(),
            payload_proto: pod_status_payload(),
            next_due_ms: 1000,
            classification: pod_status_classification(),
        })
        .await
        .expect("enqueue for dead letter");
        ndb.legacy_move_outbox_to_dead_letter_if_max_attempts("node-admin-dl-key", 0)
            .await
            .expect("move to dead letter");
        let dead = ndb
            .legacy_list_dead_letter()
            .await
            .expect("list dead letter");
        let id = dead.first().expect("dead letter row").id;
        (ndb, id)
    }

    async fn node_db_with_unassigned_dead_letter() -> (NodeLocalStores, i64) {
        let (ndb, sqlite) = selector::open_node_local_with_sqlite(
            BackendKind::Sqlite,
            None,
            supervisor(),
            None,
            "sqlite:node-admin-unassigned-dead-letter-test",
        )
        .await
        .expect("open node-local test db");
        sqlite
            .expect("SQLite backend")
            .insert_dead_letter_test_only(DeadLetterTestInsert {
                idempotency_key: "node-admin-unassigned-dl-key",
                operation: "PodStatus",
                subject_key: "v1/Pod/default/web/uid-1",
                subject_api_version: "v1",
                subject_kind: "Pod",
                subject_namespace: Some("default"),
                subject_name: "web",
                subject_uid: Some("uid-1"),
                pod_uid: "uid-1",
                payload_proto: &[1, 2, 3],
                attempts: 720,
                last_error: "max attempts",
                moved_at_ms: 2_000,
            })
            .await
            .expect("insert unassigned dead letter");
        let dead = ndb
            .legacy_list_dead_letter()
            .await
            .expect("list dead letter");
        let id = dead.first().expect("dead letter row").id;
        (ndb, id)
    }

    fn build_router(node_db: NodeLocalStores) -> axum::Router {
        super::build_router(node_db.dead_letters(), Arc::new(tokio::sync::Notify::new()))
    }

    #[tokio::test]
    async fn outbox_status_endpoint_returns_metrics() {
        let ndb = node_db().await;
        ndb.legacy_enqueue_outbox(OutboxInsert {
            idempotency_key: "status-test-key".to_string(),
            enqueued_ms: 1000,
            subject_key: "v1/Pod/default/web/uid-1".to_string(),
            subject_api_version: "v1".to_string(),
            subject_kind: "Pod".to_string(),
            subject_namespace: Some("default".to_string()),
            subject_name: "web".to_string(),
            subject_uid: Some("uid-1".to_string()),
            pod_uid: "uid-1".to_string(),
            operation: "PodStatus".to_string(),
            payload_proto: vec![],
            next_due_ms: 1000,
            classification: pod_status_classification(),
        })
        .await
        .expect("enqueue");

        let app = build_router(ndb);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/klights/v1/outbox/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["outbox_pending"], 1);
        assert_eq!(json["outbox_dispatch_total"], 0);
        assert_eq!(json["outbox_dispatch_errors_total"], 0);
        assert!(json["outbox_dead_letter_total"].as_i64().unwrap() >= 0);
    }

    #[tokio::test]
    async fn dead_letter_list_endpoint_returns_rows() {
        let (ndb, _id) = node_db_with_dead_letter().await;

        let app = build_router(ndb);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/klights/v1/outbox/dead-letter")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let rows = json.as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["idempotency_key"], "node-admin-dl-key");
    }

    #[tokio::test]
    async fn dead_letter_replay_re_enqueues_and_returns_ok() {
        let (ndb, id) = node_db_with_dead_letter().await;

        let notify = Arc::new(tokio::sync::Notify::new());
        let app = super::build_router(ndb.dead_letters(), notify.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/klights/v1/outbox/dead-letter/{id}/replay"))
                    .method("POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        tokio::time::timeout(std::time::Duration::from_millis(50), notify.notified())
            .await
            .expect("successful replay must wake the idle dispatcher");

        // Dead letter should be empty
        let dead = ndb
            .legacy_list_dead_letter()
            .await
            .expect("list dead letter");
        assert!(dead.is_empty());
    }

    #[tokio::test]
    async fn dead_letter_delete_removes_and_returns_no_content() {
        let (ndb, id) = node_db_with_unassigned_dead_letter().await;

        let app = build_router(ndb.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/klights/v1/outbox/dead-letter/{id}"))
                    .method("DELETE")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let dead = ndb
            .legacy_list_dead_letter()
            .await
            .expect("list dead letter");
        assert!(dead.is_empty());
    }

    #[tokio::test]
    async fn dispatch_counters_persist_to_node_db_and_appear_in_status() {
        let ndb = node_db().await;

        // Simulate what the dispatcher does: write counters to _node_meta.
        ndb.identity()
            .set_node_meta("outbox_dispatch_total", "42")
            .await
            .expect("write counter");
        ndb.identity()
            .set_node_meta("outbox_dispatch_errors_total", "7")
            .await
            .expect("write errors counter");

        // Enqueue a row so oldest_age_seconds has a value.
        ndb.legacy_enqueue_outbox(OutboxInsert {
            idempotency_key: "counter-test-key".to_string(),
            enqueued_ms: 1000,
            subject_key: "v1/Pod/default/web/uid-1".to_string(),
            subject_api_version: "v1".to_string(),
            subject_kind: "Pod".to_string(),
            subject_namespace: Some("default".to_string()),
            subject_name: "web".to_string(),
            subject_uid: Some("uid-1".to_string()),
            pod_uid: "uid-1".to_string(),
            operation: "PodStatus".to_string(),
            payload_proto: vec![],
            next_due_ms: 1000,
            classification: pod_status_classification(),
        })
        .await
        .expect("enqueue");

        let app = build_router(ndb);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/klights/v1/outbox/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["outbox_dispatch_total"], 42);
        assert_eq!(json["outbox_dispatch_errors_total"], 7);
    }

    #[tokio::test]
    async fn dead_letter_replay_nonexistent_returns_not_found() {
        let ndb = node_db().await;
        let app = build_router(ndb);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/klights/v1/outbox/dead-letter/99999/replay")
                    .method("POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
