//! Pure replica recovery metadata decisions.
//!
//! Reading metadata, staging snapshots, and replacing persistent state remain
//! adapter responsibilities. This module decides only whether local metadata
//! is safe to replace or requires explicit operator confirmation.

use serde::{Deserialize, Serialize};

/// Cluster identity and resource-version metadata read from cluster state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterMetadata {
    pub cluster_id: String,
    pub leader_epoch: i64,
    pub current_rv: i64,
}

/// Result of comparing local replica metadata against leader metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataComparison {
    /// Local data is behind or at the leader and is safe to reseed.
    Behind {
        local_cluster_id: String,
        local_leader_epoch: i64,
        local_last_rv: i64,
        leader_cluster_id: String,
        leader_leader_epoch: i64,
        leader_current_rv: i64,
    },
    /// Local data is ahead of the leader and must not be wiped silently.
    Ahead {
        local_cluster_id: String,
        local_last_rv: i64,
        leader_current_rv: i64,
    },
    /// Cluster identity or leader epoch differs or is incomplete.
    Mismatch {
        local_cluster_id: Option<String>,
        local_leader_epoch: Option<i64>,
        leader_cluster_id: String,
        leader_leader_epoch: i64,
        reason: String,
    },
    /// No local cluster identity exists, so there is no state to protect.
    NoLocalData,
}

/// Compare local replica metadata against an authoritative leader observation.
pub fn compare_metadata(
    local_cluster_id: Option<String>,
    local_leader_epoch: Option<i64>,
    local_last_rv: Option<i64>,
    leader_cluster_id: &str,
    leader_leader_epoch: i64,
    leader_current_rv: i64,
) -> MetadataComparison {
    let local_cluster_id = match local_cluster_id {
        Some(cluster_id) => cluster_id,
        None => return MetadataComparison::NoLocalData,
    };

    let local_leader_epoch = match local_leader_epoch {
        Some(epoch) => epoch,
        None => {
            return MetadataComparison::Mismatch {
                local_cluster_id: Some(local_cluster_id),
                local_leader_epoch: None,
                leader_cluster_id: leader_cluster_id.to_string(),
                leader_leader_epoch,
                reason: "local leader_epoch missing".to_string(),
            };
        }
    };

    let local_last_rv = local_last_rv.unwrap_or(0);
    if local_cluster_id != leader_cluster_id {
        return MetadataComparison::Mismatch {
            local_cluster_id: Some(local_cluster_id.clone()),
            local_leader_epoch: Some(local_leader_epoch),
            leader_cluster_id: leader_cluster_id.to_string(),
            leader_leader_epoch,
            reason: format!(
                "cluster_id mismatch: local={} leader={}",
                local_cluster_id, leader_cluster_id
            ),
        };
    }

    if local_leader_epoch != leader_leader_epoch {
        return MetadataComparison::Mismatch {
            local_cluster_id: Some(local_cluster_id.clone()),
            local_leader_epoch: Some(local_leader_epoch),
            leader_cluster_id: leader_cluster_id.to_string(),
            leader_leader_epoch,
            reason: format!(
                "leader_epoch mismatch: local={} leader={}",
                local_leader_epoch, leader_leader_epoch
            ),
        };
    }

    if local_last_rv > leader_current_rv {
        return MetadataComparison::Ahead {
            local_cluster_id,
            local_last_rv,
            leader_current_rv,
        };
    }

    MetadataComparison::Behind {
        local_cluster_id,
        local_leader_epoch,
        local_last_rv,
        leader_cluster_id: leader_cluster_id.to_string(),
        leader_leader_epoch,
        leader_current_rv,
    }
}

/// Whether replacing local state requires explicit operator confirmation.
pub const fn needs_confirmation(comparison: &MetadataComparison) -> bool {
    matches!(
        comparison,
        MetadataComparison::Ahead { .. } | MetadataComparison::Mismatch { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    enum Expected {
        NoLocalData,
        Behind { local_rv: i64 },
        Ahead { local_rv: i64 },
        Mismatch { reason: &'static str },
    }

    struct Case {
        name: &'static str,
        local_cluster_id: Option<&'static str>,
        local_epoch: Option<i64>,
        local_rv: Option<i64>,
        leader_cluster_id: &'static str,
        leader_epoch: i64,
        leader_rv: i64,
        expected: Expected,
        confirmation: bool,
    }

    #[test]
    fn metadata_comparison_is_table_driven() {
        let cases = [
            Case {
                name: "no local data",
                local_cluster_id: None,
                local_epoch: None,
                local_rv: None,
                leader_cluster_id: "cluster-a",
                leader_epoch: 0,
                leader_rv: 100,
                expected: Expected::NoLocalData,
                confirmation: false,
            },
            Case {
                name: "behind leader",
                local_cluster_id: Some("cluster-a"),
                local_epoch: Some(0),
                local_rv: Some(50),
                leader_cluster_id: "cluster-a",
                leader_epoch: 0,
                leader_rv: 100,
                expected: Expected::Behind { local_rv: 50 },
                confirmation: false,
            },
            Case {
                name: "at leader",
                local_cluster_id: Some("cluster-a"),
                local_epoch: Some(0),
                local_rv: Some(100),
                leader_cluster_id: "cluster-a",
                leader_epoch: 0,
                leader_rv: 100,
                expected: Expected::Behind { local_rv: 100 },
                confirmation: false,
            },
            Case {
                name: "missing local rv defaults to zero",
                local_cluster_id: Some("cluster-a"),
                local_epoch: Some(0),
                local_rv: None,
                leader_cluster_id: "cluster-a",
                leader_epoch: 0,
                leader_rv: 100,
                expected: Expected::Behind { local_rv: 0 },
                confirmation: false,
            },
            Case {
                name: "ahead of leader",
                local_cluster_id: Some("cluster-a"),
                local_epoch: Some(0),
                local_rv: Some(150),
                leader_cluster_id: "cluster-a",
                leader_epoch: 0,
                leader_rv: 100,
                expected: Expected::Ahead { local_rv: 150 },
                confirmation: true,
            },
            Case {
                name: "cluster id mismatch",
                local_cluster_id: Some("cluster-old"),
                local_epoch: Some(0),
                local_rv: Some(50),
                leader_cluster_id: "cluster-new",
                leader_epoch: 0,
                leader_rv: 100,
                expected: Expected::Mismatch {
                    reason: "cluster_id mismatch: local=cluster-old leader=cluster-new",
                },
                confirmation: true,
            },
            Case {
                name: "leader epoch mismatch",
                local_cluster_id: Some("cluster-a"),
                local_epoch: Some(5),
                local_rv: Some(50),
                leader_cluster_id: "cluster-a",
                leader_epoch: 0,
                leader_rv: 100,
                expected: Expected::Mismatch {
                    reason: "leader_epoch mismatch: local=5 leader=0",
                },
                confirmation: true,
            },
            Case {
                name: "missing leader epoch",
                local_cluster_id: Some("cluster-a"),
                local_epoch: None,
                local_rv: Some(50),
                leader_cluster_id: "cluster-a",
                leader_epoch: 0,
                leader_rv: 100,
                expected: Expected::Mismatch {
                    reason: "local leader_epoch missing",
                },
                confirmation: true,
            },
        ];

        for case in cases {
            let result = compare_metadata(
                case.local_cluster_id.map(str::to_string),
                case.local_epoch,
                case.local_rv,
                case.leader_cluster_id,
                case.leader_epoch,
                case.leader_rv,
            );
            match case.expected {
                Expected::NoLocalData => {
                    assert_eq!(result, MetadataComparison::NoLocalData, "{}", case.name)
                }
                Expected::Behind { local_rv } => match &result {
                    MetadataComparison::Behind {
                        local_last_rv,
                        leader_current_rv,
                        ..
                    } => {
                        assert_eq!(*local_last_rv, local_rv, "{}", case.name);
                        assert_eq!(*leader_current_rv, case.leader_rv, "{}", case.name);
                    }
                    other => panic!("{}: expected Behind, got {other:?}", case.name),
                },
                Expected::Ahead { local_rv } => match &result {
                    MetadataComparison::Ahead {
                        local_last_rv,
                        leader_current_rv,
                        ..
                    } => {
                        assert_eq!(*local_last_rv, local_rv, "{}", case.name);
                        assert_eq!(*leader_current_rv, case.leader_rv, "{}", case.name);
                    }
                    other => panic!("{}: expected Ahead, got {other:?}", case.name),
                },
                Expected::Mismatch { reason } => match &result {
                    MetadataComparison::Mismatch { reason: actual, .. } => {
                        assert_eq!(actual, reason, "{}", case.name)
                    }
                    other => panic!("{}: expected Mismatch, got {other:?}", case.name),
                },
            }
            assert_eq!(
                needs_confirmation(&result),
                case.confirmation,
                "{}: confirmation",
                case.name
            );
        }
    }

    #[test]
    fn metadata_comparison_json_shape_is_stable() {
        let comparison = MetadataComparison::Behind {
            local_cluster_id: "cluster-a".to_string(),
            local_leader_epoch: 1,
            local_last_rv: 40,
            leader_cluster_id: "cluster-a".to_string(),
            leader_leader_epoch: 1,
            leader_current_rv: 42,
        };
        let encoded = serde_json::to_string(&comparison).unwrap();
        assert_eq!(
            encoded,
            r#"{"Behind":{"local_cluster_id":"cluster-a","local_leader_epoch":1,"local_last_rv":40,"leader_cluster_id":"cluster-a","leader_leader_epoch":1,"leader_current_rv":42}}"#
        );
        assert_eq!(
            serde_json::from_str::<MetadataComparison>(&encoded).unwrap(),
            comparison
        );

        let metadata = ClusterMetadata {
            cluster_id: "cluster-a".to_string(),
            leader_epoch: 3,
            current_rv: 42,
        };
        assert_eq!(
            serde_json::to_string(&metadata).unwrap(),
            r#"{"cluster_id":"cluster-a","leader_epoch":3,"current_rv":42}"#
        );
        assert_eq!(
            serde_json::from_str::<ClusterMetadata>(
                r#"{"cluster_id":"cluster-a","leader_epoch":3,"current_rv":42}"#,
            )
            .unwrap(),
            metadata
        );
    }
}
