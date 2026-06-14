use std::sync::Arc;

use axum::{
    Router,
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{delete, get, post},
};
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::datastore::node_local::{DeadLetterRow, NodeLocalHandle};
use crate::task_supervisor::{SupervisedJoinHandle, TaskCategory, TaskSupervisor};

#[derive(Clone)]
struct AdminState {
    node_db: NodeLocalHandle,
}

#[derive(Serialize)]
struct OutboxStatusResponse {
    outbox_pending: i64,
    outbox_oldest_age_seconds: f64,
    outbox_dispatch_total: u64,
    outbox_dispatch_errors_total: u64,
    outbox_dead_letter_total: i64,
}

async fn outbox_status(
    State(state): State<AdminState>,
) -> Result<Json<OutboxStatusResponse>, StatusCode> {
    let stats = state
        .node_db
        .outbox_stats()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(OutboxStatusResponse {
        outbox_pending: stats.pending,
        outbox_oldest_age_seconds: stats.oldest_age_seconds,
        outbox_dispatch_total: stats.dispatch_total as u64,
        outbox_dispatch_errors_total: stats.dispatch_errors_total as u64,
        outbox_dead_letter_total: stats.dead_letter_count,
    }))
}

async fn dead_letter_list(
    State(state): State<AdminState>,
) -> Result<Json<Vec<DeadLetterRow>>, StatusCode> {
    let rows = state
        .node_db
        .list_dead_letter()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(rows))
}

async fn dead_letter_replay(
    State(state): State<AdminState>,
    Path(id): Path<i64>,
) -> Result<StatusCode, StatusCode> {
    let replayed = state
        .node_db
        .replay_dead_letter(id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if replayed {
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
        .node_db
        .delete_dead_letter(id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Ok(StatusCode::NOT_FOUND)
    }
}

fn build_router(node_db: NodeLocalHandle) -> Router {
    let state = AdminState { node_db };
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

pub async fn start_node_admin(
    node_db: NodeLocalHandle,
    supervisor: Arc<TaskSupervisor>,
    cancel: CancellationToken,
) -> anyhow::Result<SupervisedJoinHandle<()>> {
    let port: u16 = std::env::var("KLIGHTS_NODE_ADMIN_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(7781);

    let app = build_router(node_db);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;

    supervisor
        .spawn_async(TaskCategory::Background, "node_admin_server", async move {
            tracing::info!(port, "starting node admin server");
            if let Err(err) = axum::serve(listener, app)
                .with_graceful_shutdown(async move { cancel.cancelled().await })
                .await
            {
                // Listener errors (EADDRINUSE, etc.) are fine after graceful shutdown
                tracing::warn!(error = %err, "node admin server stopped");
            }
        })
        .await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use crate::datastore::backend_kind::BackendKind;
    use crate::datastore::node_local::{NodeLocalHandle, OutboxInsert, selector};
    use crate::task_supervisor::{TaskCategoryConfig, TaskSupervisor};

    fn supervisor() -> Arc<TaskSupervisor> {
        Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()))
    }

    async fn node_db() -> NodeLocalHandle {
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

    async fn node_db_with_dead_letter() -> (NodeLocalHandle, i64) {
        let ndb = node_db().await;
        // Use the concrete SqliteNodeLocalDb for test-only insert.
        // We open a separate handle via selector and downcast isn't available,
        // so we insert via enqueue + move.
        ndb.enqueue_outbox(OutboxInsert {
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
            payload_proto: vec![1, 2, 3],
            next_due_ms: 1000,
        })
        .await
        .expect("enqueue for dead letter");
        ndb.move_outbox_to_dead_letter_if_max_attempts("node-admin-dl-key", 0)
            .await
            .expect("move to dead letter");
        let dead = ndb.list_dead_letter().await.expect("list dead letter");
        let id = dead.first().expect("dead letter row").id;
        (ndb, id)
    }

    #[tokio::test]
    async fn outbox_status_endpoint_returns_metrics() {
        let ndb = node_db().await;
        ndb.enqueue_outbox(OutboxInsert {
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
        })
        .await
        .expect("enqueue");

        let app = super::build_router(ndb);
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

        let app = super::build_router(ndb);
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

        let app = super::build_router(ndb.clone());
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

        // Dead letter should be empty
        let dead = ndb.list_dead_letter().await.expect("list dead letter");
        assert!(dead.is_empty());
    }

    #[tokio::test]
    async fn dead_letter_delete_removes_and_returns_no_content() {
        let (ndb, id) = node_db_with_dead_letter().await;

        let app = super::build_router(ndb.clone());
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

        let dead = ndb.list_dead_letter().await.expect("list dead letter");
        assert!(dead.is_empty());
    }

    #[tokio::test]
    async fn dispatch_counters_persist_to_node_db_and_appear_in_status() {
        let ndb = node_db().await;

        // Simulate what the dispatcher does: write counters to _node_meta.
        ndb.set_node_meta("outbox_dispatch_total", "42")
            .await
            .expect("write counter");
        ndb.set_node_meta("outbox_dispatch_errors_total", "7")
            .await
            .expect("write errors counter");

        // Enqueue a row so oldest_age_seconds has a value.
        ndb.enqueue_outbox(OutboxInsert {
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
        })
        .await
        .expect("enqueue");

        let app = super::build_router(ndb);
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
        let app = super::build_router(ndb);
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
