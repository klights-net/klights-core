use std::future::Future;
use std::pin::Pin;

use serde_json::Value;

use crate::api::AppError;

pub(crate) struct ResourceAdmissionRequest {
    pub api_version: String,
    pub kind: String,
    pub operation: String,
    pub namespace: Option<String>,
    pub name: Option<String>,
    pub object: Value,
    pub old_object: Option<Value>,
    pub dry_run: bool,
    pub subresource: Option<String>,
    pub options: Option<Value>,
}

pub(crate) type ResourceAdmissionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Value, AppError>> + Send + 'a>>;

/// Caller-owned admission capability. API handlers supply only a complete
/// Kubernetes admission request; datastore-backed plugin execution remains in
/// the composition adapter.
pub(crate) trait ResourceAdmissionPort: Send + Sync {
    fn admit(&self, request: ResourceAdmissionRequest) -> ResourceAdmissionFuture<'_>;
}
