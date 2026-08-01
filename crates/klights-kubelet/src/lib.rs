//! Embedded kubelet implementation for klights.

pub mod containerd_manager;
pub mod context;
pub mod cri;
pub mod cri_events;
pub mod lifecycle;
pub mod log_rotation;
pub mod pod_creation_state;
pub mod pod_deletion_finalizer;
pub mod pod_dns;
pub mod pod_hosts;
pub mod pod_lifecycle_actor;
pub mod pod_lifecycle_core;
pub mod pod_lifecycle_router;
pub mod pod_lifecycle_service;
pub mod pod_repository;
pub mod pod_sandbox;
pub mod pod_sandbox_config;
pub mod pod_startup_error;
pub mod pod_status_logic;
pub mod registry_proxy;
pub mod rootless_runc_wrapper;
pub mod runtime;
pub mod runtime_observations;
pub mod runtime_paths;
pub mod runtime_reconcile_hint;
pub mod runtime_types;
pub mod unscheduled_deletion;

#[cfg(test)]
mod phase15b2_red_tests;
#[cfg(test)]
mod test_support;
