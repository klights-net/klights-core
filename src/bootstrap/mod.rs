pub(crate) mod auth_adapters;
pub(crate) mod bootstrap_token;
pub(crate) mod certificate_bootstrap;
pub mod cluster_meta;
pub(crate) mod composition;
pub(crate) mod composition_adapters;
pub mod config;
pub(crate) mod controller_adapters;
pub(crate) mod controlplane_join_adapters;
pub mod credential_store;
pub(crate) mod file_blocking;
pub(crate) mod finalizer_lifecycle_adapter;
pub(crate) mod grpc_raft_transport_adapter;
pub(crate) mod grpc_runtime_adapter;
pub mod init;
pub(crate) mod kubelet_ports;
pub(crate) mod leader_conversions;
#[cfg(test)]
pub(crate) mod leader_test_support;
pub(crate) mod local_leader_adapters;
pub mod logging;
pub(crate) mod maintenance;
pub(crate) mod network_adapters;
pub mod node_mode;
pub(crate) mod node_registration_adapter;
pub mod node_role;
pub mod observed_endpoint;
pub(crate) mod operational_adapters;
#[cfg(test)]
pub(crate) mod outbox_apply_adapter;
pub mod phases;
pub(crate) mod pod_watch_handler_adapter;
pub mod runtime;
pub(crate) mod runtime_inputs;
pub(crate) mod scheduler_adapter;
pub(crate) mod service_adapters;
pub(crate) mod side_effects;
pub(crate) mod watch_commit_wiring;
pub mod worker_runtime;
pub mod worker_store_adapter;

#[cfg(test)]
pub(crate) mod composition_tests;
#[cfg(test)]
#[path = "tests/leader_conversion.rs"]
mod leader_conversion_tests;
#[cfg(test)]
#[path = "tests/leader_rpc_remote.rs"]
mod leader_rpc_remote_tests;
#[cfg(test)]
#[path = "tests/worker_store.rs"]
mod worker_store_tests;

pub use node_mode::NodeMode;
pub use node_role::NodeRole;

/// CLI flags parsed by `main()` and handed to bootstrap.
#[derive(Debug, Clone)]
pub struct CliFlags {
    pub rootless: bool,
    pub namespace: Option<String>,
    pub bind_address: Option<String>,
    pub anonymous_auth: Option<bool>,
    pub token_file: Option<std::path::PathBuf>,
    /// Internal node role used by bootstrap dispatch.
    pub role: NodeRole,
}
