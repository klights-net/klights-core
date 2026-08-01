//! Fixed mounting shell for operational HTTP endpoints.
//!
//! Phase 17A owns the paths and their placement. The current handlers remain
//! opaque method routers supplied by the transitional implementation until
//! Phase 17F moves those handlers over focused diagnostic capabilities.

use axum::Router;
use axum::routing::MethodRouter;

/// State-compatible handlers for the permanent operational endpoint paths.
pub struct OperationalEndpointHandlers<S = ()> {
    health: MethodRouter<S>,
    metrics: MethodRouter<S>,
    version: MethodRouter<S>,
    status: MethodRouter<S>,
    task_supervisor: Router<S>,
}

impl<S> OperationalEndpointHandlers<S> {
    pub fn new(
        health: MethodRouter<S>,
        metrics: MethodRouter<S>,
        version: MethodRouter<S>,
        status: MethodRouter<S>,
        task_supervisor: Router<S>,
    ) -> Self {
        Self {
            health,
            metrics,
            version,
            status,
            task_supervisor,
        }
    }
}

/// Mount the permanent operational endpoints around an opaque native router.
pub fn mount_operational_endpoints<S>(
    router: Router<S>,
    handlers: OperationalEndpointHandlers<S>,
) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router
        .route("/healthz", handlers.health.clone())
        .route("/livez", handlers.health.clone())
        .route("/readyz", handlers.health)
        .route("/metrics", handlers.metrics)
        .route("/version", handlers.version)
        .nest("/klights/v1/task-supervisor", handlers.task_supervisor)
        .route("/klights/v1/status", handlers.status)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::{get, put};
    use tower::ServiceExt;

    #[tokio::test]
    async fn mounts_operational_paths_without_absorbing_native_routes() {
        let handlers = OperationalEndpointHandlers::new(
            get(|| async { "healthy" }),
            get(|| async { "metrics" }),
            get(|| async { "version" }),
            get(|| async { "status" }),
            Router::new().route("/db-query-logging", put(|| async { "updated" })),
        );
        let app = mount_operational_endpoints(
            Router::new().route("/api", get(|| async { "native" })),
            handlers,
        );

        for path in ["/healthz", "/livez", "/readyz", "/metrics", "/version"] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{path}");
        }
        let response = app
            .clone()
            .oneshot(
                Request::put("/klights/v1/task-supervisor/db-query-logging")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let response = app
            .oneshot(Request::get("/api").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
