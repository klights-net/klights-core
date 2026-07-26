//! Neutral side-effect policies, ports, and DTOs.

pub mod apiservice;
pub mod daemonset_node;
pub mod endpoint_mirror;
pub mod endpoint_slice_sync;
pub mod hpa;
pub mod job;
pub mod metrics;
pub mod namespace_termination;
pub mod node_taint_manager;
pub mod pdb;
pub mod policy;
pub mod resource_quota;
pub mod service_account_defaults;
pub mod service_pod;
pub mod trait_impl;
pub mod workload_pod;
pub use metrics::SideEffectMetrics;
pub use policy::ErrorPolicy;
pub use trait_impl::{
    ControllerDispatcherSlot, PodSideEffectPortsSlot, SideEffect, SideEffectRegistry,
};
