//! Replication protocol types (2A-4).
//!
//! Request/response types for the leader <-> replica replication protocol.
//! All types are serde-serializable for the JSON codec and have protobuf
//! equivalents in `proto/replication.proto`.

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::net::Ipv4Addr;

use crate::bootstrap::cluster_meta::ClusterMetadata;
use crate::datastore::command::{CommandMeta, StorageCommand};
use crate::datastore::types::{
    NodeSubnet, PodSlotAdmissionResult, PodSlotAdmissionState, Resource,
};
use crate::networking::{NodeName, PodSubnet};

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
}

impl From<ClusterMetadata> for MetadataResponse {
    fn from(m: ClusterMetadata) -> Self {
        MetadataResponse {
            cluster_id: m.cluster_id,
            leader_epoch: m.leader_epoch,
            current_rv: m.current_rv,
            current_log_index: 0,
        }
    }
}

/// Request for a full state snapshot from the leader.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotRequest {
    /// The last resource version the replica has applied.
    /// If 0, the replica has no data and needs a full snapshot.
    pub last_applied_rv: i64,
}

/// Response to a snapshot request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SnapshotResponse {
    /// Leader will stream the full state.
    Accepted {
        /// Starting resource version of the snapshot.
        start_rv: i64,
        /// Total number of entries that will be streamed.
        entry_count: i64,
    },
    /// Leader is not ready to serve snapshots.
    NotReady { reason: String },
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

/// Leader-to-follower node API request for non-interactive CRI ExecSync.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeExecSyncRequest {
    pub request_id: String,
    pub node_name: String,
    pub namespace: String,
    pub pod_name: String,
    pub container_id: String,
    pub command: Vec<String>,
    pub timeout_seconds: i64,
}

/// Follower-to-leader response for a node-local ExecSync request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeExecSyncResponse {
    pub request_id: String,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: i32,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecStreamChannel {
    Stdin,
    Stdout,
    Stderr,
    Error,
    Resize,
}

impl ExecStreamChannel {
    pub fn as_str(self) -> &'static str {
        match self {
            ExecStreamChannel::Stdin => "stdin",
            ExecStreamChannel::Stdout => "stdout",
            ExecStreamChannel::Stderr => "stderr",
            ExecStreamChannel::Error => "error",
            ExecStreamChannel::Resize => "resize",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "stdin" => Some(ExecStreamChannel::Stdin),
            "stdout" => Some(ExecStreamChannel::Stdout),
            "stderr" => Some(ExecStreamChannel::Stderr),
            "error" => Some(ExecStreamChannel::Error),
            "resize" => Some(ExecStreamChannel::Resize),
            _ => None,
        }
    }
}

/// Leader-to-follower node API request for streaming CRI Exec.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeExecRequest {
    pub request_id: String,
    pub node_name: String,
    pub namespace: String,
    pub pod_name: String,
    pub container_id: String,
    pub command: Vec<String>,
    pub tty: bool,
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
}

/// One data/control frame for a node-local streaming Exec session.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeExecStreamFrame {
    pub request_id: String,
    pub channel: ExecStreamChannel,
    pub data: Vec<u8>,
    pub fin: bool,
}

pub fn exec_error_status_payload_is_terminal(data: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(data)
        .ok()
        .and_then(|value| {
            value
                .get("status")
                .and_then(|status| status.as_str())
                .map(|status| status == "Success" || status == "Failure")
        })
        .unwrap_or(false)
}

pub fn node_exec_error_frame_is_terminal(frame: &NodeExecStreamFrame) -> bool {
    frame.channel == ExecStreamChannel::Error
        && (frame.fin || exec_error_status_payload_is_terminal(&frame.data))
}

/// Request from the leader to a follower node to read pod container logs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PodLogRequest {
    pub request_id: String,
    pub node_name: String,
    pub namespace: String,
    pub pod_name: String,
    pub pod_uid: String,
    pub container_name: String,
    pub follow: Option<String>,
    pub tail_lines: Option<String>,
    pub timestamps: Option<String>,
    pub since_time: Option<String>,
    pub since_seconds: Option<i64>,
    pub limit_bytes: Option<i64>,
    pub previous: Option<String>,
}

/// Response from a follower node with pod container log content.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PodLogResponse {
    pub request_id: String,
    pub log_content: Vec<u8>,
    pub error: Option<String>,
    pub fin: bool,
}

/// Per-follower control messages emitted by the leader onto the existing
/// follower-initiated stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FollowerControlMessage {
    NodeExecSync(NodeExecSyncRequest),
    NodeExec(NodeExecRequest),
    NodeExecFrame(NodeExecStreamFrame),
    PodLog(PodLogRequest),
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ForwardedNodeSubnet {
    pub node_name: String,
    pub subnet: String,
    pub subnet_base_int: u32,
    pub vtep_ip: String,
    pub node_ip: String,
    pub mode: String,
    pub hostport_range: Option<String>,
}

impl From<NodeSubnet> for ForwardedNodeSubnet {
    fn from(subnet: NodeSubnet) -> Self {
        Self {
            node_name: subnet.node_name.to_string(),
            subnet: subnet.subnet.to_string(),
            subnet_base_int: subnet.subnet_base_int,
            vtep_ip: subnet.vtep_ip.to_string(),
            node_ip: subnet.node_ip.to_string(),
            mode: match subnet.mode {
                crate::controllers::annotations::NodePeerMode::Root => "root",
                crate::controllers::annotations::NodePeerMode::Rootless => "rootless",
            }
            .to_string(),
            hostport_range: subnet.hostport_range.map(|range| range.to_string()),
        }
    }
}

impl ForwardedNodeSubnet {
    pub fn into_node_subnet(self) -> Result<NodeSubnet> {
        let node_name = NodeName::parse(&self.node_name)
            .map_err(|err| anyhow!("invalid forwarded node name '{}': {}", self.node_name, err))?;
        let subnet = PodSubnet::parse(&self.subnet)
            .map_err(|err| anyhow!("invalid forwarded pod subnet '{}': {}", self.subnet, err))?;
        let vtep_ip: Ipv4Addr = self
            .vtep_ip
            .parse()
            .with_context(|| format!("invalid forwarded VTEP IP '{}'", self.vtep_ip))?;
        let node_ip: Ipv4Addr = self
            .node_ip
            .parse()
            .with_context(|| format!("invalid forwarded node IP '{}'", self.node_ip))?;
        let mode = crate::controllers::annotations::parse_node_peer_mode(Some(&self.mode))
            .unwrap_or(crate::controllers::annotations::NodePeerMode::Root);
        let hostport_range = self
            .hostport_range
            .as_deref()
            .map(crate::networking::types::HostPortRange::parse)
            .transpose()
            .map_err(|err| anyhow!("invalid forwarded hostport range: {err}"))?;

        Ok(NodeSubnet {
            node_name,
            subnet,
            subnet_base_int: self.subnet_base_int,
            vtep_ip,
            node_ip,
            mode,
            hostport_range,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForwardedPodSlotAdmission {
    pub admitted: bool,
    pub blocking_uid: Option<String>,
    pub blocking_node: Option<String>,
    pub state: Option<String>,
    pub resource_version: i64,
}

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
    use crate::datastore::command::{COMMAND_CODEC_VERSION, CommandId};

    fn sample_meta() -> CommandMeta {
        CommandMeta {
            command_id: CommandId::new(),
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
    fn snapshot_request_serializes() {
        let req = SnapshotRequest {
            last_applied_rv: 10,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"last_applied_rv\":10"));
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
