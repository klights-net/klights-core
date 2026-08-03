//! Neutral side-effect policies, metrics, and registry runtime.

pub mod apiservice;
pub mod applied_pod;
pub mod daemonset_node;
mod defaults;
pub mod endpoint_mirror;
pub mod endpoint_slice_sync;
pub mod hpa;
pub mod job;
pub mod metrics;
pub mod namespace_termination;
pub mod node_taint_manager;
pub mod pdb;
mod policy;
mod registry;
mod resource_mutation_effects;
pub mod resource_quota;
mod runtime;
pub mod service_account_defaults;
pub mod service_pod;
pub mod workload_pod;

pub use defaults::{DefaultSideEffects, default_registry};
pub use metrics::{SideEffectFailureEntry, SideEffectMetrics};
pub use policy::ErrorPolicy;
pub use registry::{
    ControllerDispatcherSlot, PodSideEffectPortsSlot, SideEffect, SideEffectFailure,
    SideEffectRegistry,
};
pub use resource_mutation_effects::ResourceMutationEffects;
pub use runtime::{run_delete_hooks_logged, run_hooks_logged, run_named_hook_logged};
