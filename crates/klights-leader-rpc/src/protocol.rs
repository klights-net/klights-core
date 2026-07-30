//! Compatibility facade for transport-neutral replication contracts.
//!
//! Values are owned by lower contract crates; leader-rpc owns only their wire
//! conversion and authenticated transport.

pub use klights_cluster_core::{ReplicationEntry, StreamItem, StreamRequest};
pub use klights_leader_api::{
    JoinRequest, JoinResponse, JoinRole, MetadataRequest, MetadataResponse,
    require_exact_command_codec,
};
pub use klights_node_api::{
    FollowerCompletionContext, FollowerControlMessage, NodeOperationKind, RoutedNodeExecFrame,
    RoutedNodeExecRequest, RoutedNodeExecSyncRequest, RoutedNodeExecSyncResponse,
    RoutedNodeLogEvent, RoutedNodeLogRequest, RoutedNodeMetricsRequest, RoutedNodeMetricsResponse,
};
