//! Fixed mounting shell for operational HTTP endpoints.

use std::marker::PhantomData;
use std::sync::Arc;

use axum::extract::Path;
use axum::http::HeaderMap;
use axum::routing::{MethodRouter, get};
use axum::{Json, Router};

use crate::operational_handlers::{self, OperationalEndpointInputs};

/// State-compatible handlers for the permanent operational endpoint paths.
pub struct OperationalEndpointHandlers<S = ()> {
    health: MethodRouter<S>,
    liveness: MethodRouter<S>,
    readiness: MethodRouter<S>,
    metrics: MethodRouter<S>,
    version: MethodRouter<S>,
    status: MethodRouter<S>,
    task_categories: MethodRouter<S>,
    active_tasks: MethodRouter<S>,
    category_tasks: MethodRouter<S>,
    db_query_logging: MethodRouter<S>,
    state: PhantomData<fn() -> S>,
}

impl<S> OperationalEndpointHandlers<S>
where
    S: Clone + Send + Sync + 'static,
{
    pub fn new(inputs: OperationalEndpointInputs) -> Self {
        let inputs = Arc::new(inputs);
        Self {
            health: get(operational_handlers::health_check),
            liveness: get(operational_handlers::health_check),
            readiness: get(operational_handlers::readiness_check),
            metrics: get({
                let inputs = inputs.clone();
                move || operational_handlers::metrics_handler(inputs.clone())
            }),
            version: get({
                let inputs = inputs.clone();
                move || operational_handlers::version_handler(inputs.clone())
            }),
            status: get({
                let inputs = inputs.clone();
                move || operational_handlers::klights_status_handler(inputs.clone())
            }),
            task_categories: get({
                let inputs = inputs.clone();
                move |headers: HeaderMap| {
                    operational_handlers::get_task_categories(inputs.clone(), headers)
                }
            }),
            active_tasks: get({
                let inputs = inputs.clone();
                move |headers: HeaderMap| {
                    operational_handlers::get_active_tasks(inputs.clone(), headers)
                }
            }),
            category_tasks: get({
                let inputs = inputs.clone();
                move |Path(category): Path<String>, headers: HeaderMap| {
                    operational_handlers::get_active_tasks_by_category(
                        inputs.clone(),
                        category,
                        headers,
                    )
                }
            }),
            db_query_logging: get({
                let inputs = inputs.clone();
                move |headers: HeaderMap| {
                    operational_handlers::get_db_query_logging(inputs.clone(), headers)
                }
            })
            .put({
                let inputs = inputs.clone();
                move |headers: HeaderMap, Json(payload): Json<DbQueryLoggingUpdate>| {
                    operational_handlers::put_db_query_logging(
                        inputs.clone(),
                        headers,
                        payload.enabled,
                    )
                }
            }),
            state: PhantomData,
        }
    }
}

#[derive(serde::Deserialize)]
struct DbQueryLoggingUpdate {
    enabled: bool,
}

/// Mount the permanent operational endpoints around an opaque native router.
pub fn mount_operational_endpoints<S>(
    router: Router<S>,
    handlers: OperationalEndpointHandlers<S>,
) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    let task_supervisor = Router::new()
        .route("/categories", handlers.task_categories)
        .route("/tasks", handlers.active_tasks)
        .route("/categories/{category}/tasks", handlers.category_tasks)
        .route("/db-query-logging", handlers.db_query_logging);
    router
        .route("/healthz", handlers.health)
        .route("/livez", handlers.liveness)
        .route("/readyz", handlers.readiness)
        .route("/metrics", handlers.metrics)
        .route("/version", handlers.version)
        .nest("/klights/v1/task-supervisor", task_supervisor)
        .route("/klights/v1/status", handlers.status)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use axum::routing::get;
    use klights_leader_api::{
        ClusterStatusMetadata, ClusterStatusMetadataFuture, FollowerDiagnostic,
        FollowerDiagnostics, FollowerDiagnosticsFuture, LeaderClusterStatusMetadata,
        LeaderFollowerDiagnostics,
    };
    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};
    use tower::ServiceExt;

    use super::*;
    use crate::{OperationalEndpointInputs, OperationalNodeRole, VersionInfo};

    struct ClusterStatus;

    impl LeaderClusterStatusMetadata for ClusterStatus {
        fn cluster_status_metadata(&self) -> ClusterStatusMetadataFuture<'_> {
            Box::pin(async {
                Ok(ClusterStatusMetadata {
                    cluster_id: "cluster-a".to_string(),
                    leader_epoch: 7,
                    current_resource_version: 42,
                })
            })
        }
    }

    struct Followers;

    impl LeaderFollowerDiagnostics for Followers {
        fn follower_diagnostics(&self) -> FollowerDiagnosticsFuture<'_> {
            Box::pin(async {
                FollowerDiagnostics {
                    follower_count: 1,
                    max_lag: 3,
                    followers: vec![FollowerDiagnostic {
                        node_name: "replica-a".to_string(),
                        applied_resource_version: 39,
                        lag: 3,
                        mode: "replica".to_string(),
                        encryption: "enabled".to_string(),
                        public_key: Some("key".to_string()),
                    }],
                }
            })
        }
    }

    fn app() -> Router {
        let inputs = OperationalEndpointInputs::new(
            OperationalNodeRole::Leader,
            Arc::new(|| "metric_total 2\n".to_string()),
            VersionInfo::new(
                "1",
                "34",
                "v1.34.6+klights-test",
                "abc",
                "clean",
                "",
                "rustc test",
                "test-target",
            ),
            Arc::new(ClusterStatus),
            Some(Arc::new(Followers)),
            Arc::new(TaskSupervisor::new(TaskCategoryConfig::default())),
        );
        mount_operational_endpoints(
            Router::new().route("/api", get(|| async { "native" })),
            OperationalEndpointHandlers::new(inputs),
        )
    }

    async fn json(response: axum::response::Response) -> serde_json::Value {
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
    }

    #[tokio::test]
    async fn permanent_handlers_preserve_health_metrics_version_and_status() {
        for path in ["/healthz", "/livez", "/readyz"] {
            let response = app()
                .oneshot(Request::get(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{path}");
            assert_eq!(to_bytes(response.into_body(), 16).await.unwrap(), "ok");
        }

        let metrics = app()
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            to_bytes(metrics.into_body(), 64).await.unwrap(),
            "metric_total 2\n"
        );

        let version = app()
            .oneshot(Request::get("/version").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(json(version).await["gitVersion"], "v1.34.6+klights-test");

        let status = app()
            .oneshot(
                Request::get("/klights/v1/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = json(status).await;
        assert_eq!(status["role"], "Leader");
        assert_eq!(status["clusterId"], "cluster-a");
        assert_eq!(status["followers"][0]["nodeName"], "replica-a");

        let native = app()
            .oneshot(Request::get("/api").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(to_bytes(native.into_body(), 16).await.unwrap(), "native");
    }

    #[tokio::test]
    async fn supervisor_admin_handlers_preserve_authorization_and_category_errors() {
        let forbidden = app()
            .oneshot(
                Request::get("/klights/v1/task-supervisor/categories")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);
        assert_eq!(json(forbidden).await["reason"], "Forbidden");

        let categories = app()
            .oneshot(
                Request::get("/klights/v1/task-supervisor/categories")
                    .header("x-remote-group", "system:masters")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(categories.status(), StatusCode::OK);
        assert!(
            json(categories)
                .await
                .as_array()
                .is_some_and(|rows| !rows.is_empty())
        );

        let invalid = app()
            .oneshot(
                Request::get("/klights/v1/task-supervisor/categories/FILE/tasks")
                    .header("x-remote-group", "system:masters")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
        assert_eq!(json(invalid).await["reason"], "BadRequest");

        let updated = app()
            .oneshot(
                Request::put("/klights/v1/task-supervisor/db-query-logging")
                    .header("x-remote-group", "system:masters")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"enabled":true}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(updated.status(), StatusCode::OK);
        assert_eq!(json(updated).await["enabled"], true);
    }
}
