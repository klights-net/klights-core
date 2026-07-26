pub(crate) mod auth_adapters;
pub mod bootstrap_token;
pub mod cluster_meta;
pub mod config;
pub mod controlplane_discovery;
pub(crate) mod controlplane_join_handler;
pub mod credential_store;
pub(crate) mod finalizer_lifecycle_adapter;
pub mod init;
pub mod kubelet_ports;
pub mod leader_reconnect;
pub mod logging;
pub(crate) mod network_adapters;
pub mod node_mode;
pub(crate) mod node_registration_adapter;
pub(crate) mod node_registration_profile;
pub mod node_role;
pub mod observed_endpoint;
pub(crate) mod operational_adapters;
pub mod phases;
pub mod raft_transport;
pub mod runtime;
pub(crate) mod runtime_inputs;
pub(crate) mod scheduler_adapter;
pub(crate) mod service_adapters;
pub mod worker_identity;
pub mod worker_runtime;
pub mod worker_store_adapter;

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
