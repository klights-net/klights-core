//! Shared `klights.io/...` Node annotation keys (F2-05).
//!
//! `klights.io/mode` and `klights.io/hostport-range` are published by
//! `kubelet/node.rs` and read back by `controllers/node_subnet.rs` for peer
//! mode projection. The neutral peer-mode type and parser live in
//! `klights-network-api`.

pub use klights_network_api::{
    DATAPLANE_ENCRYPTION_ANNOTATION, DATAPLANE_ENDPOINT_ANNOTATION, DATAPLANE_MODE_ANNOTATION,
    DATAPLANE_PORT_ANNOTATION, DATAPLANE_PUBLIC_KEY_ANNOTATION, DEFAULT_HOSTPORT_RANGE,
    GIT_COMMIT_ANNOTATION, GRPC_PORT_ANNOTATION, HOSTPORT_RANGE_ANNOTATION, NODE_MODE_ANNOTATION,
    NodePeerMode,
};
