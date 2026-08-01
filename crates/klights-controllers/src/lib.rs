//! Event-driven controller coordination runtime and side-effect registry.

mod coordination;
mod dispatcher;
mod lease_loop;
pub mod scheduler;
pub mod side_effects;
pub mod workqueue;

pub use coordination::{
    ControllerCoordination, ControllerReconcileContext, CoordinatedControllerKind,
};
pub use dispatcher::DispatcherRuntime;
pub use lease_loop::run_under_lease;
