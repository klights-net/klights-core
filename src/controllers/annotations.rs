//! Shared `klights.io/...` Node annotation keys, parsers, and the
//! `NodePeerMode` enum (F2-05).
//!
//! `klights.io/mode` and `klights.io/hostport-range` are published by
//! `kubelet/node.rs` and read back by `controllers/node_subnet.rs` for peer
//! mode projection. Keeping them in one module prevents publisher/consumer
//! drift.

use crate::bootstrap::NodeMode;
pub use klights_network_api::{
    DATAPLANE_ENCRYPTION_ANNOTATION, DATAPLANE_ENDPOINT_ANNOTATION, DATAPLANE_MODE_ANNOTATION,
    DATAPLANE_PORT_ANNOTATION, DATAPLANE_PUBLIC_KEY_ANNOTATION, DEFAULT_HOSTPORT_RANGE,
    GIT_COMMIT_ANNOTATION, GRPC_PORT_ANNOTATION, HOSTPORT_RANGE_ANNOTATION, NODE_MODE_ANNOTATION,
    NodePeerMode,
};
use thiserror::Error;

const NODE_MODE_ROOT: &str = "root";
const NODE_MODE_ROOTLESS: &str = "rootless";

// F2-04 consumes `parse_node_peer_mode` + `AnnotationError` for the peer-mode
// projection in `controllers/node_subnet.rs`. Until that task lands, the
// symbols ship with the F2-05 module so the constants/parsers don't drift,
// hence the explicit dead-code allow.
#[derive(Debug, Error)]
pub enum AnnotationError {
    #[error(
        "annotation '{NODE_MODE_ANNOTATION}' has invalid value '{0}'; expected 'root' or 'rootless'"
    )]
    InvalidNodeMode(String),
}

/// Parse the `klights.io/mode` annotation into the typed peer mode. `None`
/// returns `Ok(Root)` for backward compatibility with pre-F2-05 nodes that
/// existed before mode was published.
pub fn parse_node_peer_mode(value: Option<&str>) -> Result<NodePeerMode, AnnotationError> {
    match value {
        None => Ok(NodePeerMode::Root),
        Some(NODE_MODE_ROOT) => Ok(NodePeerMode::Root),
        Some(NODE_MODE_ROOTLESS) => Ok(NodePeerMode::Rootless),
        Some(other) => Err(AnnotationError::InvalidNodeMode(other.to_string())),
    }
}

/// Render the runtime `NodeMode` to the wire value used in the
/// `klights.io/mode` annotation.
pub fn node_mode_to_annotation(mode: &NodeMode) -> &'static str {
    match mode {
        NodeMode::Root => NODE_MODE_ROOT,
        NodeMode::Rootless { .. } => NODE_MODE_ROOTLESS,
    }
}

/// Resolve the host-port graft range to publish for the local node.
/// Root mode publishes an empty string so peers see a uniform shape; rootless
/// mode publishes the configured / default rootless range.
pub fn hostport_range_for_local_node(mode: &NodeMode) -> &'static str {
    match mode {
        NodeMode::Root => "",
        NodeMode::Rootless { .. } => DEFAULT_HOSTPORT_RANGE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn parse_node_peer_mode_accepts_root_and_rootless() {
        assert_eq!(
            parse_node_peer_mode(Some("root")).unwrap(),
            NodePeerMode::Root
        );
        assert_eq!(
            parse_node_peer_mode(Some("rootless")).unwrap(),
            NodePeerMode::Rootless
        );
    }

    #[test]
    fn parse_node_peer_mode_defaults_missing_to_root() {
        assert_eq!(parse_node_peer_mode(None).unwrap(), NodePeerMode::Root);
    }

    #[test]
    fn parse_node_peer_mode_rejects_unknown_value() {
        let err = parse_node_peer_mode(Some("hybrid")).unwrap_err();
        assert!(format!("{err}").contains("hybrid"));
    }

    #[test]
    fn node_mode_to_annotation_renders_runtime_variants() {
        assert_eq!(node_mode_to_annotation(&NodeMode::Root), "root");
        let rootless = NodeMode::Rootless {
            rootlesskit_pid: 0,
            user_netns: PathBuf::from("/proc/self/ns/net"),
        };
        assert_eq!(node_mode_to_annotation(&rootless), "rootless");
    }

    #[test]
    fn hostport_range_root_is_empty_for_uniform_shape() {
        assert_eq!(hostport_range_for_local_node(&NodeMode::Root), "");
    }

    #[test]
    fn hostport_range_rootless_uses_default_range() {
        let rootless = NodeMode::Rootless {
            rootlesskit_pid: 0,
            user_netns: PathBuf::from("/proc/self/ns/net"),
        };
        assert_eq!(
            hostport_range_for_local_node(&rootless),
            DEFAULT_HOSTPORT_RANGE
        );
    }
}
