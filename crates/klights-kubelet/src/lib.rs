//! Embedded kubelet implementation for klights.

pub mod cgroup_cleanup;
pub mod cni_readiness;
pub mod containerd_manager;
pub mod context;
pub mod cri;
pub mod cri_events;
pub mod env;
pub mod lifecycle;
pub mod log_rotation;
pub mod metrics;
pub mod node;
pub mod node_api;
pub mod node_capacity;
pub mod node_config;
pub mod node_heartbeat;
pub mod node_ip;
pub mod node_leader_labels;
pub mod node_outbox;
pub mod node_registration;
mod node_role_labels;
mod node_status_merge;
mod node_status_projection;
pub mod outbox;
pub mod pod_container_config;
pub mod pod_creation_state;
pub mod pod_deletion_finalizer;
pub mod pod_dns;
pub mod pod_env;
pub mod pod_events;
pub mod pod_field_ref;
mod pod_fs;
pub mod pod_hosts;
pub mod pod_lifecycle_actor;
pub mod pod_lifecycle_core;
pub mod pod_lifecycle_router;
pub mod pod_lifecycle_service;
pub mod pod_repository;
pub mod pod_resources;
#[cfg(any(test, feature = "test-support"))]
pub mod pod_runtime_state;
pub mod pod_sandbox;
pub mod pod_sandbox_config;
pub mod pod_service_envs;
pub mod pod_startup_error;
pub mod pod_status_builders;
pub mod pod_status_logic;
pub mod pod_subsystem;
mod pod_termination;
pub mod pod_volume_manager;
pub mod pod_watch_handlers;
pub mod pod_watch_source;
pub mod probe_manager;
pub mod probes;
pub mod projected_sa_token_refresh;
pub mod reconciler;
pub mod registry_proxy;
pub mod rootless_runc_wrapper;
pub mod runtime;
pub mod runtime_clock;
pub mod runtime_observations;
pub mod runtime_paths;
pub mod runtime_reconcile_hint;
pub mod runtime_types;
pub mod sandbox_gc;
pub mod unscheduled_deletion;
pub mod volume_registry;
pub mod volume_sources;
pub mod volumes;

#[cfg(test)]
mod node_conditions_tests;
#[cfg(test)]
mod phase15b2_red_tests;

#[cfg(any(test, feature = "test-support"))]
mod phase15d_test_support;
