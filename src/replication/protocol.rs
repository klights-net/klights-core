//! Replication protocol types (2A-4).
//!
//! Request/response types for the leader <-> replica replication protocol.
//! All types are serde-serializable for the JSON codec and have protobuf
//! equivalents in the `klights-internal-protobuf` replication schema.

#[cfg(test)]
use crate::datastore::node_local::{PodSlotAdmissionResult, PodSlotAdmissionState};
#[cfg(test)]
use crate::datastore::types::NodeSubnet;
#[cfg(test)]
use anyhow::{Context, Result, anyhow};
use klights_cluster_core::{ClusterMetadata, CommandMeta, Resource, StorageCommand};
use klights_node_api::{
    NodeExecFrame, NodeExecRequest, NodeExecSyncRequest, NodeExecSyncResult, NodeMetricsError,
    NodeMetricsRequest, NodeMetricsResult,
};
#[cfg(test)]
use klights_types::{NodeName, PodSubnet};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::net::Ipv4Addr;

/// A replication envelope wrapping a command with its metadata.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReplicationEntry {
    pub command: StorageCommand,
    pub meta: CommandMeta,
}

/// Handshake request from a joining node.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JoinRequest {
    pub token: String,
    pub node_name: String,
    pub role: JoinRole,
}

/// Role declared by the joining node on the worker replication stream.
/// Raft learners use JoinAsControlplane instead of this worker path.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinRole {
    Worker,
}

/// Leader's response to a join request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum JoinResponse {
    Accepted {
        cluster_id: String,
        leader_epoch: i64,
        current_rv: i64,
    },
    Rejected {
        reason: String,
    },
}

/// Request for leader metadata (cluster_id, leader_epoch, current_rv).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataRequest;

/// Response with leader metadata.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetadataResponse {
    pub cluster_id: String,
    pub leader_epoch: i64,
    pub current_rv: i64,
    pub current_log_index: i64,
    pub command_codec_version: u32,
}

pub fn require_exact_command_codec(
    command_codec_version: u32,
    peer: &str,
) -> std::result::Result<(), String> {
    if command_codec_version == klights_cluster_core::COMMAND_CODEC_VERSION {
        Ok(())
    } else {
        Err(format!(
            "{peer} must advertise exact command codec version {} (received {command_codec_version})",
            klights_cluster_core::COMMAND_CODEC_VERSION,
        ))
    }
}

impl From<ClusterMetadata> for MetadataResponse {
    fn from(m: ClusterMetadata) -> Self {
        MetadataResponse {
            cluster_id: m.cluster_id,
            leader_epoch: m.leader_epoch,
            current_rv: m.current_rv,
            current_log_index: 0,
            command_codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
        }
    }
}

/// Request to subscribe to the command stream from a given resource version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamRequest {
    /// Start streaming from this resource version (inclusive).
    pub start_rv: i64,
}

/// A single item in the command stream.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum StreamItem {
    /// A replicated command with metadata.
    Entry(Box<ReplicationEntry>),
    /// A keep-alive / heartbeat when no commands have been produced.
    Heartbeat { current_rv: i64 },
}

/// Replication-private correlation envelope. The request itself is owned by
/// `klights-node-api`; only transport routing identity lives here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RoutedNodeExecSyncRequest {
    pub(crate) request_id: String,
    pub(crate) request: NodeExecSyncRequest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RoutedNodeExecSyncResponse {
    pub(crate) request_id: String,
    pub(crate) result: NodeExecSyncResult,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RoutedNodeExecRequest {
    pub(crate) request_id: String,
    pub(crate) request: NodeExecRequest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RoutedNodeExecFrame {
    pub(crate) request_id: String,
    pub(crate) frame: NodeExecFrame,
}

/// Replication-private correlation envelope for a node log request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RoutedNodeLogRequest {
    pub(crate) request_id: String,
    pub(crate) follow: bool,
    pub(crate) request: klights_node_api::NodeLogRequest,
}

/// Replication-private correlation envelope for a node log event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RoutedNodeLogEvent {
    pub(crate) request_id: String,
    pub(crate) event: klights_node_api::NodeLogEvent,
}

/// Replication-private correlation envelope for one node metrics request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RoutedNodeMetricsRequest {
    pub(crate) request_id: String,
    pub(crate) request: NodeMetricsRequest,
}

/// Replication-private correlation envelope for one node metrics result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RoutedNodeMetricsResponse {
    pub(crate) request_id: String,
    pub(crate) node_name: String,
    pub(crate) result: Result<NodeMetricsResult, NodeMetricsError>,
}

/// Per-follower control messages emitted by the leader onto the existing
/// follower-initiated stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FollowerControlMessage {
    NodeExecSync(RoutedNodeExecSyncRequest),
    NodeExec(RoutedNodeExecRequest),
    NodeExecFrame(RoutedNodeExecFrame),
    PodLog(RoutedNodeLogRequest),
    NodeMetrics(RoutedNodeMetricsRequest),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NodeOperationKind {
    ExecSync,
    ExecStream,
    Log,
    Metrics,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct FollowerCompletionContext<'a> {
    pub(crate) node_name: &'a str,
    pub(crate) follower_session: u64,
    pub(crate) kind: NodeOperationKind,
}

impl<'a> FollowerCompletionContext<'a> {
    pub(crate) const fn new(
        node_name: &'a str,
        follower_session: u64,
        kind: NodeOperationKind,
    ) -> Self {
        Self {
            node_name,
            follower_session,
            kind,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ForwardedResource {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    pub resource_version: i64,
    pub data: serde_json::Value,
}

impl From<Resource> for ForwardedResource {
    fn from(resource: Resource) -> Self {
        Self {
            api_version: resource.api_version,
            kind: resource.kind,
            namespace: resource.namespace,
            name: resource.name,
            resource_version: resource.resource_version,
            data: std::sync::Arc::unwrap_or_clone(resource.data),
        }
    }
}

impl ForwardedResource {
    pub fn into_resource(self) -> Resource {
        Resource {
            id: 0,
            api_version: self.api_version,
            kind: self.kind,
            namespace: self.namespace,
            name: self.name,
            uid: Resource::uid_from_data(&self.data),
            resource_version: self.resource_version,
            data: std::sync::Arc::new(self.data),
        }
    }
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ForwardedNodeSubnet {
    pub node_name: String,
    pub subnet: String,
    pub subnet_base_int: u32,
    pub gateway_ip: String,
    pub node_ip: String,
    pub mode: String,
    pub hostport_range: Option<String>,
}

#[cfg(test)]
impl From<NodeSubnet> for ForwardedNodeSubnet {
    fn from(subnet: NodeSubnet) -> Self {
        Self {
            node_name: subnet.node_name.to_string(),
            subnet: subnet.subnet.to_string(),
            subnet_base_int: subnet.subnet_base_int,
            gateway_ip: subnet.gateway_ip.to_string(),
            node_ip: subnet.node_ip.to_string(),
            mode: match subnet.mode {
                klights_network_api::NodePeerMode::Root => "root",
                klights_network_api::NodePeerMode::Rootless => "rootless",
            }
            .to_string(),
            hostport_range: subnet.hostport_range.map(|range| range.to_string()),
        }
    }
}

#[cfg(test)]
impl ForwardedNodeSubnet {
    pub fn into_node_subnet(self) -> Result<NodeSubnet> {
        let node_name = NodeName::parse(&self.node_name)
            .map_err(|err| anyhow!("invalid forwarded node name '{}': {}", self.node_name, err))?;
        let subnet = PodSubnet::parse(&self.subnet)
            .map_err(|err| anyhow!("invalid forwarded pod subnet '{}': {}", self.subnet, err))?;
        let gateway_ip: Ipv4Addr = self
            .gateway_ip
            .parse()
            .with_context(|| format!("invalid forwarded gateway IP '{}'", self.gateway_ip))?;
        let node_ip: Ipv4Addr = self
            .node_ip
            .parse()
            .with_context(|| format!("invalid forwarded node IP '{}'", self.node_ip))?;
        let mode = match self.mode.as_str() {
            "rootless" => klights_network_api::NodePeerMode::Rootless,
            _ => klights_network_api::NodePeerMode::Root,
        };
        let hostport_range = self
            .hostport_range
            .as_deref()
            .map(klights_types::HostPortRange::parse)
            .transpose()
            .map_err(|err| anyhow!("invalid forwarded hostport range: {err}"))?;

        Ok(NodeSubnet {
            node_name,
            subnet,
            subnet_base_int: self.subnet_base_int,
            gateway_ip,
            node_ip,
            mode,
            hostport_range,
        })
    }
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForwardedPodSlotAdmission {
    pub admitted: bool,
    pub blocking_uid: Option<String>,
    pub blocking_node: Option<String>,
    pub state: Option<String>,
    pub resource_version: i64,
}

#[cfg(test)]
impl From<PodSlotAdmissionResult> for ForwardedPodSlotAdmission {
    fn from(result: PodSlotAdmissionResult) -> Self {
        match result {
            PodSlotAdmissionResult::Admitted { resource_version } => Self {
                admitted: true,
                blocking_uid: None,
                blocking_node: None,
                state: None,
                resource_version,
            },
            PodSlotAdmissionResult::Blocked {
                blocking_uid,
                blocking_node,
                state,
                resource_version,
            } => Self {
                admitted: false,
                blocking_uid: Some(blocking_uid),
                blocking_node: Some(blocking_node),
                state: Some(state.as_str().to_string()),
                resource_version,
            },
        }
    }
}

#[cfg(test)]
impl ForwardedPodSlotAdmission {
    pub fn into_result(self) -> Result<PodSlotAdmissionResult> {
        if self.admitted {
            return Ok(PodSlotAdmissionResult::Admitted {
                resource_version: self.resource_version,
            });
        }
        Ok(PodSlotAdmissionResult::Blocked {
            blocking_uid: self.blocking_uid.unwrap_or_default(),
            blocking_node: self.blocking_node.unwrap_or_default(),
            state: PodSlotAdmissionState::parse(self.state.as_deref().unwrap_or("Admitted"))?,
            resource_version: self.resource_version,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use klights_cluster_core::command::{COMMAND_CODEC_VERSION, CommandId};

    fn sample_meta() -> CommandMeta {
        CommandMeta {
            command_id: CommandId("protocol-sample-command".to_string()),
            codec_version: COMMAND_CODEC_VERSION,
            resource_version: 1,
            uid: None,
            timestamp_ms: 0,
            authoring_node: "test".into(),
        }
    }

    // The "no `pub enum ReplicationMessage`" invariant is enforced by
    // the base-repo source guard run by `./build.sh`.

    #[test]
    fn join_request_serializes() {
        let req = JoinRequest {
            token: "abc123".into(),
            node_name: "worker-1".into(),
            role: JoinRole::Worker,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"token\":\"abc123\""));
        assert!(json.contains("\"Worker\""));
        assert!(!json.contains("\"replica\""));
    }

    #[test]
    fn join_response_accepted_serializes() {
        let resp = JoinResponse::Accepted {
            cluster_id: "test-cluster".into(),
            leader_epoch: 0,
            current_rv: 42,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"Accepted\""));
        assert!(json.contains("\"cluster_id\":\"test-cluster\""));
        assert!(!json.contains("service_account_signing_key_pem"));
    }

    #[test]
    fn join_response_rejected_serializes() {
        let resp = JoinResponse::Rejected {
            reason: "bad token".into(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"Rejected\""));
    }

    #[test]
    fn replication_entry_round_trip_json() {
        let entry = ReplicationEntry {
            command: StorageCommand::CreateResource {
                api_version: "v1".into(),
                kind: "ConfigMap".into(),
                namespace: Some("default".into()),
                name: "test".into(),
                data: serde_json::json!({"metadata": {"name": "test"}}),
            },
            meta: sample_meta(),
        };

        let json = serde_json::to_vec(&entry).unwrap();
        let decoded: ReplicationEntry = serde_json::from_slice(&json).unwrap();
        assert_eq!(decoded.command, entry.command);
        assert_eq!(decoded.meta.command_id, entry.meta.command_id);
    }

    #[test]
    fn metadata_response_from_cluster_metadata() {
        let meta = ClusterMetadata {
            cluster_id: "cid".into(),
            leader_epoch: 5,
            current_rv: 100,
        };
        let resp = MetadataResponse::from(meta);
        assert_eq!(resp.cluster_id, "cid");
        assert_eq!(resp.leader_epoch, 5);
        assert_eq!(resp.current_rv, 100);
        assert_eq!(resp.current_log_index, 0);
        assert_eq!(
            resp.command_codec_version,
            klights_cluster_core::COMMAND_CODEC_VERSION
        );
        assert_eq!(
            serde_json::from_str::<MetadataResponse>(&serde_json::to_string(&resp).unwrap())
                .unwrap()
                .command_codec_version,
            klights_cluster_core::COMMAND_CODEC_VERSION
        );
        assert!(
            serde_json::from_str::<MetadataResponse>(
                r#"{"cluster_id":"missing","leader_epoch":1,"current_rv":2,"current_log_index":3}"#
            )
            .is_err(),
            "metadata without an exact codec version must fail closed"
        );
    }

    #[test]
    fn command_codec_v3_is_an_exact_fail_closed_version() {
        assert!(
            require_exact_command_codec(klights_cluster_core::COMMAND_CODEC_VERSION, "v3").is_ok()
        );
        assert!(
            require_exact_command_codec(klights_cluster_core::COMMAND_CODEC_VERSION - 1, "v2")
                .is_err()
        );
        assert!(require_exact_command_codec(0, "legacy").is_err());
    }

    #[test]
    fn stream_item_entry_round_trip() {
        let entry = StreamItem::Entry(Box::new(ReplicationEntry {
            command: StorageCommand::CreateNamespace {
                name: "test".into(),
                data: serde_json::json!({}),
            },
            meta: sample_meta(),
        }));
        let json = serde_json::to_vec(&entry).unwrap();
        let decoded: StreamItem = serde_json::from_slice(&json).unwrap();
        match decoded {
            StreamItem::Entry(inner) => {
                assert!(matches!(
                    inner.command,
                    StorageCommand::CreateNamespace { .. }
                ));
            }
            _ => panic!("expected Entry variant"),
        }
    }

    #[test]
    fn stream_item_heartbeat_round_trip() {
        let hb = StreamItem::Heartbeat { current_rv: 42 };
        let json = serde_json::to_vec(&hb).unwrap();
        let decoded: StreamItem = serde_json::from_slice(&json).unwrap();
        assert_eq!(decoded, hb);
    }

    #[test]
    fn stream_request_serializes() {
        let req = StreamRequest { start_rv: 5 };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"start_rv\":5"));
    }

    #[test]
    fn forwarded_resource_round_trips_json() {
        let resource = ForwardedResource {
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            namespace: Some("default".into()),
            name: "forwarded".into(),
            resource_version: 9,
            data: serde_json::json!({
                "metadata": {
                    "name": "forwarded",
                    "namespace": "default",
                    "resourceVersion": "9"
                }
            }),
        };

        let json = serde_json::to_vec(&resource).unwrap();
        let decoded: ForwardedResource = serde_json::from_slice(&json).unwrap();

        assert_eq!(decoded, resource);
    }
}
