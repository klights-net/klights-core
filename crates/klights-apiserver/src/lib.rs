//! Permanent Kubernetes API-server transport shell for klights.
//!
//! The current Kubernetes route implementation remains in the transitional
//! native service. This crate owns only listener/connection policy, authority
//! routing, fixed operational route mounting, and opaque router delegation.

mod authority;
mod node_admin;
mod node_admin_handlers;
mod operational;
mod operational_handlers;
mod server;
mod version;

pub use authority::{HttpAuthorityRouter, load_proxy_client_identity, wrap_authority_router};
pub use node_admin::{build_node_admin_router, start_node_admin};
pub use node_admin_handlers::NodeAdminEndpointInputs;
pub use operational::{OperationalEndpointHandlers, mount_operational_endpoints};
pub use operational_handlers::{
    OperationalEndpointInputs, OperationalMetrics, OperationalNodeRole,
};
pub use server::{insert_tonic_tcp_connect_info, load_tls_pem_files, serve_https};
pub use version::VersionInfo;
