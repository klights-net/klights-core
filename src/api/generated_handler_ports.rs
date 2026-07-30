use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::body::Body;
use klights_cluster_core::Resource;
use serde_json::Value;

use crate::api::AppError;

pub(crate) type GeneratedHandlerFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, AppError>> + Send + 'a>>;

pub(crate) trait BuiltinAdmissionDefaultsPort: Send + Sync {
    fn ensure_namespace_active(&self, namespace: String) -> GeneratedHandlerFuture<'_, ()>;

    fn validate_pod_volume_paths(&self, pod: &Value) -> Result<(), AppError>;

    fn prepare_pod_create(
        &self,
        namespace: String,
        pod: Value,
    ) -> GeneratedHandlerFuture<'_, Value>;

    fn prepare_pvc_create(
        &self,
        namespace: String,
        claim: Value,
    ) -> GeneratedHandlerFuture<'_, Value>;
}

pub(crate) trait GeneratedLifecyclePort: Send + Sync {
    fn rotate_bootstrap_token_secret(
        &self,
        resource: Resource,
    ) -> GeneratedHandlerFuture<'_, Resource>;

    fn reconcile_cluster_role_aggregation(&self) -> GeneratedHandlerFuture<'_, ()>;

    fn create_default_service_account(&self, namespace: String) -> GeneratedHandlerFuture<'_, ()>;

    fn create_root_ca_config_map(&self, namespace: String) -> GeneratedHandlerFuture<'_, ()>;

    fn reconcile_root_ca_data(&self, namespace: String) -> GeneratedHandlerFuture<'_, ()>;

    fn reconcile_root_ca(&self, namespace: String) -> GeneratedHandlerFuture<'_, ()>;

    fn delete_node_cleanup_intents(&self, node_name: String) -> GeneratedHandlerFuture<'_, ()>;

    fn maybe_finalize_pod_after_finalizers_drained(
        &self,
        namespace: String,
        name: String,
        pod: Value,
    ) -> GeneratedHandlerFuture<'_, ()>;
}

#[allow(clippy::too_many_arguments)]
pub(crate) trait GeneratedResourceMutationPort: Send + Sync {
    fn update_main_resource(
        &self,
        api_version: String,
        kind: String,
        namespace: Option<String>,
        name: String,
        data: Value,
        preconditions: klights_cluster_core::ResourcePreconditions,
    ) -> GeneratedHandlerFuture<'_, Resource>;
}

pub(crate) struct GeneratedWatchRequest {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub requested_resource_version: i64,
    pub send_initial_events: bool,
    pub send_bookmarks: bool,
    pub label_selector: Option<String>,
    pub field_selector: Option<String>,
    pub table_format: bool,
    pub stream_format: crate::api::watch_stream::WatchStreamFormat,
    pub timeout_seconds: Option<u64>,
    pub emit_initial_state_for_resource_version_zero: bool,
    pub wall_clock: Arc<dyn klights_auth::clock::Clock>,
}

pub(crate) trait GeneratedWatchPort: Send + Sync {
    fn build_watch_stream(
        &self,
        request: GeneratedWatchRequest,
    ) -> Pin<Box<dyn Future<Output = Body> + Send + '_>>;
}
