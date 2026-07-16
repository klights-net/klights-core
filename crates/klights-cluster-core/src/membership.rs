//! Pure cluster membership and identity values.
//!
//! Consensus-engine membership, persistence, and wire conversions remain in
//! their adapters. This module owns only deterministic values and transitions.

use serde::{Deserialize, Serialize};

/// Stable numeric node identity used at the consensus boundary.
pub type NodeId = u64;

/// Derive the stable node identity from its Kubernetes node name.
///
/// This is the existing 64-bit FNV-1a mapping. Zero is reserved, so the empty
/// input's non-zero hash remains usable without a separate allocation path.
pub fn raft_node_id_for_node_name(node_name: &str) -> NodeId {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in node_name.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    if hash == 0 { 1 } else { hash }
}

/// Cluster shape used by the shape-driven role-label transition.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RaftShape {
    /// Number of voters in the current membership.
    pub voter_count: u32,
    /// Whether this node is the currently elected leader.
    pub is_leader: bool,
    /// Whether this node participates as a learner rather than a voter.
    pub is_learner: bool,
}

/// Persisted and remotely reported cluster membership metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterMembership {
    pub cluster_id: String,
    pub voters: Vec<String>,
    pub term: i64,
    pub leader_hint: Option<String>,
}

/// Merge membership metadata after a control-plane admission.
///
/// A same-cluster latest observation is authoritative for concurrent voters
/// and the highest observed term. Learner admission must not add the joining
/// node to the voter metadata. The resulting voter list is canonicalized so
/// retries are idempotent and preserve stable serialized bytes.
pub fn merge_controlplane_join_membership_metadata(
    mut membership: ClusterMembership,
    latest: Option<&ClusterMembership>,
    admitted_node_name: &str,
    as_learner: bool,
    leader_hint: &str,
) -> ClusterMembership {
    if let Some(latest) = latest
        && latest.cluster_id == membership.cluster_id
    {
        membership.voters.extend(latest.voters.iter().cloned());
        membership.term = membership.term.max(latest.term);
    }
    if !as_learner {
        membership.voters.push(admitted_node_name.to_string());
    }
    membership.voters.sort();
    membership.voters.dedup();
    membership.leader_hint = Some(leader_hint.to_string());
    membership
}

#[cfg(test)]
mod tests {
    use super::*;

    fn membership(cluster_id: &str, voters: &[&str], term: i64) -> ClusterMembership {
        ClusterMembership {
            cluster_id: cluster_id.to_string(),
            voters: voters.iter().map(|voter| (*voter).to_string()).collect(),
            term,
            leader_hint: Some("old-leader".to_string()),
        }
    }

    #[test]
    fn node_id_mapping_is_deterministic_distinct_and_non_zero() {
        let first = raft_node_id_for_node_name("mn-controlplane1");
        assert_eq!(first, raft_node_id_for_node_name("mn-controlplane1"));
        assert_ne!(first, raft_node_id_for_node_name("mn-controlplane2"));
        assert_ne!(first, 0);
        assert_ne!(raft_node_id_for_node_name(""), 0);
    }

    #[test]
    fn membership_json_shape_is_stable() {
        let membership = ClusterMembership {
            cluster_id: "cluster-a".to_string(),
            voters: vec!["cp-1".to_string(), "cp-2".to_string()],
            term: 7,
            leader_hint: Some("https://cp-1:7446".to_string()),
        };
        assert_eq!(
            serde_json::to_string(&membership).unwrap(),
            r#"{"cluster_id":"cluster-a","voters":["cp-1","cp-2"],"term":7,"leader_hint":"https://cp-1:7446"}"#
        );
        assert_eq!(
            serde_json::from_str::<ClusterMembership>(
                r#"{"cluster_id":"cluster-a","voters":["cp-1","cp-2"],"term":7,"leader_hint":"https://cp-1:7446"}"#,
            )
            .unwrap(),
            membership
        );
    }

    #[test]
    fn join_membership_merge_is_table_driven() {
        struct Case {
            name: &'static str,
            initial: ClusterMembership,
            latest: Option<ClusterMembership>,
            admitted: &'static str,
            learner: bool,
            expected_voters: &'static [&'static str],
            expected_term: i64,
        }

        let cases = [
            Case {
                name: "same cluster preserves concurrent voters and highest term",
                initial: membership("cluster-a", &["cp-2", "cp-1"], 2),
                latest: Some(membership("cluster-a", &["cp-1", "cp-3"], 4)),
                admitted: "cp-2",
                learner: false,
                expected_voters: &["cp-1", "cp-2", "cp-3"],
                expected_term: 4,
            },
            Case {
                name: "learner is not inserted into voter metadata",
                initial: membership("cluster-a", &["cp-1"], 2),
                latest: Some(membership("cluster-a", &["cp-2", "cp-1"], 3)),
                admitted: "replica-1",
                learner: true,
                expected_voters: &["cp-1", "cp-2"],
                expected_term: 3,
            },
            Case {
                name: "different cluster observation is ignored",
                initial: membership("cluster-a", &["cp-1"], 5),
                latest: Some(membership("cluster-b", &["foreign-cp"], 99)),
                admitted: "cp-2",
                learner: false,
                expected_voters: &["cp-1", "cp-2"],
                expected_term: 5,
            },
            Case {
                name: "retry without latest remains sorted and deduplicated",
                initial: membership("cluster-a", &["cp-2", "cp-1", "cp-2"], 1),
                latest: None,
                admitted: "cp-2",
                learner: false,
                expected_voters: &["cp-1", "cp-2"],
                expected_term: 1,
            },
        ];

        for case in cases {
            let merged = merge_controlplane_join_membership_metadata(
                case.initial,
                case.latest.as_ref(),
                case.admitted,
                case.learner,
                "https://current-leader:7446",
            );
            assert_eq!(merged.voters, case.expected_voters, "{}: voters", case.name);
            assert_eq!(merged.term, case.expected_term, "{}: term", case.name);
            assert_eq!(
                merged.leader_hint.as_deref(),
                Some("https://current-leader:7446"),
                "{}: leader hint",
                case.name
            );
        }
    }
}
