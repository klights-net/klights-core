//! Permanent Kubernetes API-server transport shell for klights.
//!
//! The current Kubernetes route implementation remains in the transitional
//! native service. This crate owns only listener/connection policy, authority
//! routing, fixed operational route mounting, and opaque router delegation.

mod authority;
mod operational;
mod server;

pub use authority::{HttpAuthorityRouter, load_proxy_client_identity, wrap_authority_router};
pub use operational::{OperationalEndpointHandlers, mount_operational_endpoints};
pub use server::{insert_tonic_tcp_connect_info, load_tls_pem_files, serve_https};
