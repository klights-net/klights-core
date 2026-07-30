//! Transport-neutral leader replication admission values.

use klights_cluster_core::{COMMAND_CODEC_VERSION, ClusterMetadata};
use serde::{Deserialize, Serialize};

/// Handshake request from a joining worker.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JoinRequest {
    pub token: String,
    pub node_name: String,
    pub role: JoinRole,
}

/// Role declared by a joining node on the worker replication stream.
///
/// Raft learners use the separate control-plane join contract.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinRole {
    Worker,
}

/// Leader response to a worker join request.
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

/// Request for current leader metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataRequest;

/// Current leader metadata returned to a peer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetadataResponse {
    pub cluster_id: String,
    pub leader_epoch: i64,
    pub current_rv: i64,
    pub current_log_index: i64,
    pub command_codec_version: u32,
}

/// Require exact command-codec equality at the peer admission boundary.
pub fn require_exact_command_codec(command_codec_version: u32, peer: &str) -> Result<(), String> {
    if command_codec_version == COMMAND_CODEC_VERSION {
        Ok(())
    } else {
        Err(format!(
            "{peer} must advertise exact command codec version {COMMAND_CODEC_VERSION} \
             (received {command_codec_version})"
        ))
    }
}

impl From<ClusterMetadata> for MetadataResponse {
    fn from(metadata: ClusterMetadata) -> Self {
        Self {
            cluster_id: metadata.cluster_id,
            leader_epoch: metadata.leader_epoch,
            current_rv: metadata.current_rv,
            current_log_index: 0,
            command_codec_version: COMMAND_CODEC_VERSION,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_values_preserve_json_contract() {
        let request = JoinRequest {
            token: "abc123".into(),
            node_name: "worker-1".into(),
            role: JoinRole::Worker,
        };
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"token\":\"abc123\""));
        assert!(json.contains("\"Worker\""));
        assert!(!json.contains("\"replica\""));

        let accepted = JoinResponse::Accepted {
            cluster_id: "test-cluster".into(),
            leader_epoch: 0,
            current_rv: 42,
        };
        let json = serde_json::to_string(&accepted).unwrap();
        assert!(json.contains("\"Accepted\""));
        assert!(json.contains("\"cluster_id\":\"test-cluster\""));
        assert!(!json.contains("service_account_signing_key_pem"));

        let rejected = JoinResponse::Rejected {
            reason: "bad token".into(),
        };
        assert!(
            serde_json::to_string(&rejected)
                .unwrap()
                .contains("\"Rejected\"")
        );
    }

    #[test]
    fn metadata_requires_exact_v3_codec() {
        let response = MetadataResponse::from(ClusterMetadata {
            cluster_id: "cid".into(),
            leader_epoch: 5,
            current_rv: 100,
        });
        assert_eq!(response.current_log_index, 0);
        assert_eq!(response.command_codec_version, COMMAND_CODEC_VERSION);
        assert!(
            serde_json::from_str::<MetadataResponse>(
                r#"{"cluster_id":"missing","leader_epoch":1,"current_rv":2,"current_log_index":3}"#
            )
            .is_err(),
            "metadata without an exact codec version must fail closed"
        );
        assert!(require_exact_command_codec(COMMAND_CODEC_VERSION, "v3").is_ok());
        assert!(require_exact_command_codec(COMMAND_CODEC_VERSION - 1, "v2").is_err());
        assert!(require_exact_command_codec(0, "legacy").is_err());
    }
}
