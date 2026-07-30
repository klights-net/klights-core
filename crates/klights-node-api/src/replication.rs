//! Transport-neutral node operation routing values.

use crate::{
    NodeExecFrame, NodeExecRequest, NodeExecSyncRequest, NodeExecSyncResult, NodeLogEvent,
    NodeLogRequest, NodeMetricsError, NodeMetricsRequest, NodeMetricsResult,
};

/// Correlated synchronous node-exec request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutedNodeExecSyncRequest {
    pub request_id: String,
    pub request: NodeExecSyncRequest,
}

/// Correlated synchronous node-exec response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutedNodeExecSyncResponse {
    pub request_id: String,
    pub result: NodeExecSyncResult,
}

/// Correlated streaming node-exec request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutedNodeExecRequest {
    pub request_id: String,
    pub request: NodeExecRequest,
}

/// Correlated streaming node-exec frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutedNodeExecFrame {
    pub request_id: String,
    pub frame: NodeExecFrame,
}

/// Correlated node-log request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutedNodeLogRequest {
    pub request_id: String,
    pub follow: bool,
    pub request: NodeLogRequest,
}

/// Correlated node-log event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutedNodeLogEvent {
    pub request_id: String,
    pub event: NodeLogEvent,
}

/// Correlated node-metrics request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutedNodeMetricsRequest {
    pub request_id: String,
    pub request: NodeMetricsRequest,
}

/// Correlated node-metrics response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutedNodeMetricsResponse {
    pub request_id: String,
    pub node_name: String,
    pub result: Result<NodeMetricsResult, NodeMetricsError>,
}

/// Per-follower control messages emitted by the leader.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FollowerControlMessage {
    NodeExecSync(RoutedNodeExecSyncRequest),
    NodeExec(RoutedNodeExecRequest),
    NodeExecFrame(RoutedNodeExecFrame),
    PodLog(RoutedNodeLogRequest),
    NodeMetrics(RoutedNodeMetricsRequest),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeOperationKind {
    ExecSync,
    ExecStream,
    Log,
    Metrics,
}

#[derive(Clone, Copy, Debug)]
pub struct FollowerCompletionContext<'a> {
    pub node_name: &'a str,
    pub follower_session: u64,
    pub kind: NodeOperationKind,
}

impl<'a> FollowerCompletionContext<'a> {
    pub const fn new(node_name: &'a str, follower_session: u64, kind: NodeOperationKind) -> Self {
        Self {
            node_name,
            follower_session,
            kind,
        }
    }
}
