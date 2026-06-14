//! Shared `klights.io/...` Node annotation keys, parsers, and the
//! `NodePeerMode` enum (F2-05).
//!
//! `controllers/node_subnet.rs` previously owned `VTEP_MAC_ANNOTATION` as a
//! file-local constant. F2-05 introduces two more annotations
//! (`klights.io/mode`, `klights.io/hostport-range`) that need to be published
//! by `kubelet/node.rs` *and* read back by `controllers/node_subnet.rs` for
//! the peer-mode projection F2-04 introduces. Without one shared module the
//! constants would drift across the publisher and the consumer.

use crate::bootstrap::NodeMode;
use thiserror::Error;

pub const NODE_MODE_ANNOTATION: &str = "klights.io/mode";
pub const HOSTPORT_RANGE_ANNOTATION: &str = "klights.io/hostport-range";
pub const VTEP_MAC_ANNOTATION: &str = "klights.io/vtep-mac";
pub const DATAPLANE_ENDPOINT_ANNOTATION: &str = "klights.io/dataplane-endpoint";
pub const DATAPLANE_PORT_ANNOTATION: &str = "klights.io/dataplane-port";
pub const DATAPLANE_MODE_ANNOTATION: &str = "klights.io/dataplane-mode";
pub const DATAPLANE_ENCRYPTION_ANNOTATION: &str = "klights.io/dataplane-encryption";
pub const DATAPLANE_PUBLIC_KEY_ANNOTATION: &str = "klights.io/dataplane-public-key";
/// Short git commit hash (first 8 chars of HEAD) of the klights binary
/// running on the node. Surfaced as the wide-only `COMMIT` column of
/// `kubectl get nodes -o wide` so multinode clusters can spot version skew
/// even when peers report the same `kubeletVersion`.
pub const GIT_COMMIT_ANNOTATION: &str = "klights.io/git-commit";
/// gRPC/API server TLS port published by controlplane nodes so workers
/// can discover all controlplane endpoints from Node watch events.
pub const GRPC_PORT_ANNOTATION: &str = "klights.io/grpc-port";

const NODE_MODE_ROOT: &str = "root";
const NODE_MODE_ROOTLESS: &str = "rootless";

/// Default rootless host-port graft range. Honors the conventional Kubernetes
/// NodePort range until F2-04 ships per-node range configuration.
pub const DEFAULT_HOSTPORT_RANGE: &str = "30000-32767";

/// Mode dimension as projected through Node annotations. Cluster-side; not the
/// same type as the runtime `bootstrap::NodeMode` (which carries rootlesskit
/// process identifiers and is local to the running process).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodePeerMode {
    Root,
    Rootless,
}

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
/// existed before mode was published; the caller can layer additional checks
/// (e.g. require a `vtep_mac` for root mode) on top of this.
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
