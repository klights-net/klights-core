//! Transitional owner of the current Kubernetes-native service.
//!
//! Phase 17B.1 owns only the private state, opaque current-router handoff, and
//! Kubernetes error/status adaptation. Route families and handlers migrate in
//! their later packets.

mod error;
mod router;
mod state;

pub use error::{
    AppError, StatusCause, map_mutating_admission_error, map_validating_admission_error,
};
pub use router::CurrentRouter;
pub use state::ApiState;
