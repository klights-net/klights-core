//! Transitional owner of the current Kubernetes-native service.
//!
//! Phase 17B.1 owns only the private state, opaque current-router handoff, and
//! Kubernetes error/status adaptation. Route families and handlers migrate in
//! their later packets.

pub mod admission;
pub mod audit;
pub mod auth_http;
pub mod discovery;
mod error;
mod extractor;
pub mod generic_command;
pub mod generic_read;
mod identity;
pub mod policy_inputs;
pub mod priority_fairness;
pub mod request_info;
mod resource_projection;
pub mod response;
mod router;
mod state;
pub mod status;
pub mod streaming;
pub mod subresources;
pub mod watch;

pub use discovery::{
    api_group_by_name, api_groups, custom_resource_discovery, get_openapi_v2,
    get_openapi_v3_api_v1, get_openapi_v3_discovery, get_openapi_v3_group_version,
};
pub use error::{
    AppError, StatusCause, map_mutating_admission_error, map_validating_admission_error,
};
pub use extractor::LenientJson;
pub use identity::ApiIdentityGenerator;
pub use resource_projection::inject_resource_version;
pub use response::K8sResponse;
pub use router::CurrentRouter;
pub use state::ApiState;
pub use streaming::StreamingDependencies;
