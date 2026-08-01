//! Opaque current-router handoff for incremental Phase 17 migration.
//!
//! Later packets move route families into this crate. Phase 17B.1 owns the
//! router lifecycle and state binding while accepting the unchanged route
//! table as an opaque transitional input from root.

/// Current Kubernetes route table awaiting family-by-family migration.
pub struct CurrentRouter<S> {
    router: axum::Router<S>,
}

impl<S> CurrentRouter<S> {
    pub fn from_transitional_routes(router: axum::Router<S>) -> Self {
        Self { router }
    }
}

impl<S> CurrentRouter<S>
where
    S: Clone + Send + Sync + 'static,
{
    pub fn bind_state(self, state: S) -> axum::Router {
        self.router.with_state(state)
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::get;
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn opaque_transitional_routes_survive_state_binding() {
        let router = axum::Router::new().route("/transitional", get(|| async { "ok" }));
        let app = CurrentRouter::from_transitional_routes(router).bind_state(());
        let response = app
            .oneshot(Request::get("/transitional").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
