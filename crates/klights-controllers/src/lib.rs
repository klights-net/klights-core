//! Event-driven controller coordination runtime and side-effect registry.

mod coordination;
mod dispatcher;
pub mod endpoints;
pub mod gc;
mod lease_loop;
pub mod resource_projection;
pub mod scheduler;
pub mod service;
pub mod side_effects;
pub mod workqueue;

pub use coordination::{
    ControllerCoordination, ControllerReconcileContext, CoordinatedControllerKind,
};
pub use dispatcher::DispatcherRuntime;
pub use lease_loop::run_under_lease;
