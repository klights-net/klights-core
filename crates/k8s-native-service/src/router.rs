//! Opaque handoff for the current Kubernetes implementation.

/// Fully bound Kubernetes route table owned by this disposable service crate.
pub struct CurrentRouter {
    router: axum::Router,
}

impl CurrentRouter {
    pub(crate) fn new(router: axum::Router) -> Self {
        Self { router }
    }

    /// Consume the native-service boundary at the permanent server shell.
    pub fn into_router(self) -> axum::Router {
        self.router
    }
}
