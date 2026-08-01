//! Neutral side-effect policies, metrics, and registry runtime.

pub mod metrics;
mod policy;
mod registry;

pub use metrics::{SideEffectFailureEntry, SideEffectMetrics};
pub use policy::ErrorPolicy;
pub use registry::{
    ControllerDispatcherSlot, PodSideEffectPortsSlot, SideEffect, SideEffectFailure,
    SideEffectRegistry,
};
