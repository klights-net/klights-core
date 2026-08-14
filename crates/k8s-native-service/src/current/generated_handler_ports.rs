use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::body::Body;
pub struct GeneratedWatchRequest {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub scope: klights_leader_api::ResourceListScope,
    pub requested_resource_version: i64,
    pub send_initial_events: bool,
    pub send_bookmarks: bool,
    pub label_selector: Option<String>,
    pub field_selector: Option<String>,
    pub table_format: bool,
    pub stream_format: crate::current::watch_stream::WatchStreamFormat,
    pub timeout_seconds: Option<u64>,
    pub emit_initial_state_for_resource_version_zero: bool,
    pub wall_clock: Arc<dyn klights_auth::clock::Clock>,
}

pub trait GeneratedWatchPort: Send + Sync {
    fn build_watch_stream(
        &self,
        request: GeneratedWatchRequest,
    ) -> Pin<Box<dyn Future<Output = Body> + Send + '_>>;
}
