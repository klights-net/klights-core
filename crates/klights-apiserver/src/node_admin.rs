//! Listener and route-mounting shell for the node-local administrative API.

use std::sync::Arc;

use axum::Router;
use axum::extract::Path;
use axum::routing::{MethodRouter, delete, get, post};

use crate::node_admin_handlers::{self, NodeAdminEndpointInputs};

struct NodeAdminEndpointHandlers<S = ()> {
    outbox_status: MethodRouter<S>,
    dead_letter_list: MethodRouter<S>,
    dead_letter_replay: MethodRouter<S>,
    dead_letter_delete: MethodRouter<S>,
}

impl<S> NodeAdminEndpointHandlers<S>
where
    S: Clone + Send + Sync + 'static,
{
    fn new(inputs: NodeAdminEndpointInputs) -> Self {
        let inputs = Arc::new(inputs);
        Self {
            outbox_status: get({
                let inputs = inputs.clone();
                move || node_admin_handlers::outbox_status(inputs.clone())
            }),
            dead_letter_list: get({
                let inputs = inputs.clone();
                move || node_admin_handlers::dead_letter_list(inputs.clone())
            }),
            dead_letter_replay: post({
                let inputs = inputs.clone();
                move |Path(id): Path<i64>| {
                    node_admin_handlers::dead_letter_replay(inputs.clone(), id)
                }
            }),
            dead_letter_delete: delete({
                let inputs = inputs.clone();
                move |Path(id): Path<i64>| {
                    node_admin_handlers::dead_letter_delete(inputs.clone(), id)
                }
            }),
        }
    }
}

pub fn build_node_admin_router(inputs: NodeAdminEndpointInputs) -> Router {
    let handlers = NodeAdminEndpointHandlers::new(inputs);
    Router::new()
        .route("/klights/v1/outbox/status", handlers.outbox_status)
        .route("/klights/v1/outbox/dead-letter", handlers.dead_letter_list)
        .route(
            "/klights/v1/outbox/dead-letter/{id}/replay",
            handlers.dead_letter_replay,
        )
        .route(
            "/klights/v1/outbox/dead-letter/{id}",
            handlers.dead_letter_delete,
        )
}

pub async fn start_node_admin<F>(
    inputs: NodeAdminEndpointInputs,
    port: u16,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
    shutdown_signal: F,
) -> anyhow::Result<klights_supervisor::SupervisedJoinHandle<()>>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let app = build_node_admin_router(inputs);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    supervisor
        .spawn_async(
            klights_supervisor::TaskCategory::Background,
            "node_admin_server",
            async move {
                tracing::info!(port, "starting node admin server");
                if let Err(error) = axum::serve(listener, app)
                    .with_graceful_shutdown(shutdown_signal)
                    .await
                {
                    tracing::warn!(%error, "node admin server stopped");
                }
            },
        )
        .await
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use klights_node_api::{
        NodeAdminError, NodeAdminFuture, NodeDeadLetter, NodeDeadLetterAdmin,
        NodeOutboxDiagnostics, NodeOutboxStatus,
    };
    use tower::ServiceExt;

    use super::*;

    struct FakeNodeAdmin {
        replayed: AtomicBool,
        deleted: AtomicBool,
    }

    impl NodeOutboxDiagnostics for FakeNodeAdmin {
        fn outbox_status(&self) -> NodeAdminFuture<'_, NodeOutboxStatus> {
            Box::pin(async {
                Ok(NodeOutboxStatus {
                    pending: 2,
                    oldest_age_seconds: 1.5,
                    dispatch_total: 8,
                    dispatch_errors_total: 1,
                    dead_letter_total: 1,
                })
            })
        }
    }

    impl NodeDeadLetterAdmin for FakeNodeAdmin {
        fn list_dead_letters(&self) -> NodeAdminFuture<'_, Vec<NodeDeadLetter>> {
            Box::pin(async {
                Ok(vec![NodeDeadLetter {
                    id: 9,
                    original_id: 4,
                    client_id: "node-a".to_string(),
                    idempotency_key: "key".to_string(),
                    enqueued_ms: 10,
                    subject_key: "v1/Pod/default/p/uid".to_string(),
                    subject_api_version: "v1".to_string(),
                    subject_kind: "Pod".to_string(),
                    subject_namespace: Some("default".to_string()),
                    subject_name: "p".to_string(),
                    subject_uid: Some("uid".to_string()),
                    pod_uid: "uid".to_string(),
                    operation: "PodStatus".to_string(),
                    stream_id: 1,
                    stream_seq: 2,
                    payload_proto: vec![1, 2],
                    attempts: 3,
                    last_error: "retry exhausted".to_string(),
                    moved_at_ms: 20,
                }])
            })
        }

        fn replay_dead_letter(&self, id: i64) -> NodeAdminFuture<'_, bool> {
            Box::pin(async move {
                if id == 9 {
                    self.replayed.store(true, Ordering::SeqCst);
                    Ok(true)
                } else {
                    Ok(false)
                }
            })
        }

        fn delete_dead_letter(&self, id: i64) -> NodeAdminFuture<'_, bool> {
            Box::pin(async move {
                if id == 9 {
                    self.deleted.store(true, Ordering::SeqCst);
                    Ok(true)
                } else {
                    Ok(false)
                }
            })
        }
    }

    fn app(fake: Arc<FakeNodeAdmin>) -> Router {
        build_node_admin_router(NodeAdminEndpointInputs::new(fake.clone(), fake))
    }

    async fn json(response: axum::response::Response) -> serde_json::Value {
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
    }

    #[tokio::test]
    async fn node_admin_handlers_preserve_status_list_replay_and_delete() {
        let fake = Arc::new(FakeNodeAdmin {
            replayed: AtomicBool::new(false),
            deleted: AtomicBool::new(false),
        });
        let status = app(fake.clone())
            .oneshot(
                Request::get("/klights/v1/outbox/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(json(status).await["outbox_dispatch_total"], 8);

        let list = app(fake.clone())
            .oneshot(
                Request::get("/klights/v1/outbox/dead-letter")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(json(list).await[0]["idempotency_key"], "key");

        let replay = app(fake.clone())
            .oneshot(
                Request::post("/klights/v1/outbox/dead-letter/9/replay")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(replay.status(), StatusCode::OK);
        assert!(fake.replayed.load(Ordering::SeqCst));

        let delete = app(fake.clone())
            .oneshot(
                Request::delete("/klights/v1/outbox/dead-letter/9")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(delete.status(), StatusCode::NO_CONTENT);
        assert!(fake.deleted.load(Ordering::SeqCst));

        let missing = app(fake)
            .oneshot(
                Request::post("/klights/v1/outbox/dead-letter/0/replay")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn node_admin_error_remains_transport_neutral() {
        assert_eq!(
            NodeAdminError::unavailable("offline").to_string(),
            "offline"
        );
    }
}
