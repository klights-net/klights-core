//! `PeerRouter` — the narrow trait for cross-node peer endpoint state.
//! Today this means WireGuard and direct-route peer programming, with
//! rootless reachability mediated by `NodeEndpoint`.
//!
//! The `node_subnet` controller takes `&dyn PeerRouter` so it cannot
//! reach datapath methods like `cni_add` it has no business calling.

use anyhow::Result;
use async_trait::async_trait;

use crate::networking::types::NodeEndpoint;

#[async_trait]
pub trait PeerRouter: Send + Sync + 'static {
    /// Program reachability state for a peer node.
    async fn apply_peer_endpoint(&self, peer: &NodeEndpoint) -> Result<()>;

    /// Tear down reachability state for a peer node.
    async fn remove_peer_endpoint(&self, peer: &NodeEndpoint) -> Result<()>;
}
