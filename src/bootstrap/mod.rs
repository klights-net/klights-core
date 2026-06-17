pub mod bootstrap_token;
pub mod cluster_meta;
pub mod config;
pub mod controlplane_discovery;
pub mod init;
pub mod leader_reconnect;
pub mod logging;
pub mod node_mode;
pub mod node_role;
pub mod observed_endpoint;
pub mod phases;
pub mod raft_transport;
pub mod runtime;
pub mod worker_identity;
pub mod worker_runtime;

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
