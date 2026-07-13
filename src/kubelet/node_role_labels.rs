fn node_role_label_key(role: &crate::bootstrap::NodeRole) -> &'static str {
    match role {
        crate::bootstrap::NodeRole::Leader { .. }
        | crate::bootstrap::NodeRole::Controlplane { .. } => {
            // Static fallback used only when no `RaftShape` is supplied
            // (e.g. legacy LeaderFollower mode). The shape-driven label
            // arms below in `role_label_keys_for_shape` replace this
            // whenever P3-11d wiring passes a live shape snapshot.
            "node-role.kubernetes.io/leader"
        }
        crate::bootstrap::NodeRole::Worker { .. } => "node-role.kubernetes.io/worker",
    }
}

/// P3-11d: shape-driven role-label selector. For raft control-plane voters,
/// the `node-role.kubernetes.io/*` label set is derived live from the local
/// `RaftShape` (voter_count + is_leader). `controlplane` is the stable
/// voter role label; elected leaders additionally carry `leader`.
///
/// `voter_count == 0` means the node has joined as a controlplane but the
/// seed's `add_voter` hasn't committed yet; we emit no role label to
/// avoid claiming a controlplane stamp before the membership change
/// lands.
///
/// Worker / replica labels are static and unaffected.
pub fn role_label_keys_for_shape(
    role: &crate::bootstrap::NodeRole,
    shape: Option<&crate::datastore::raft::types::RaftShape>,
) -> Vec<&'static str> {
    use crate::bootstrap::NodeRole;
    // T1.7: a node participating as a raft learner emits the `replica`
    // label regardless of its CLI-declared role. Voter state is the
    // ground truth; learners do not count toward quorum.
    if let Some(shape) = shape
        && shape.is_learner
    {
        return vec!["node-role.kubernetes.io/replica"];
    }
    match role {
        NodeRole::Controlplane { .. } => {
            let Some(shape) = shape else {
                return vec![node_role_label_key(role)];
            };
            match (shape.voter_count, shape.is_leader) {
                (0, _) => vec![],
                (_, true) => vec![
                    "node-role.kubernetes.io/controlplane",
                    "node-role.kubernetes.io/leader",
                ],
                (_, false) => vec!["node-role.kubernetes.io/controlplane"],
            }
        }
        NodeRole::Leader { .. } => {
            let Some(shape) = shape else {
                return vec![node_role_label_key(role)];
            };
            match (shape.voter_count, shape.is_leader) {
                (0, _) => vec![],
                (1, true) => vec!["node-role.kubernetes.io/leader"],
                (1, false) => vec![],
                (_, true) => vec![
                    "node-role.kubernetes.io/controlplane",
                    "node-role.kubernetes.io/leader",
                ],
                (_, false) => vec!["node-role.kubernetes.io/controlplane"],
            }
        }
        NodeRole::Worker { .. } => vec!["node-role.kubernetes.io/worker"],
    }
}

pub(crate) fn prune_klights_managed_node_role_labels(node: &mut serde_json::Value) {
    let Some(labels) = node
        .pointer_mut("/metadata/labels")
        .and_then(|labels| labels.as_object_mut())
    else {
        return;
    };
    for key in [
        "node-role.kubernetes.io/controlplane",
        "node-role.kubernetes.io/control-plane",
        "node-role.kubernetes.io/master",
        "node-role.kubernetes.io/leader",
        "node-role.kubernetes.io/replica",
        "node-role.kubernetes.io/worker",
    ] {
        labels.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// P3-11d: shape-driven role labels for a solo N=1 raft voter must keep
    /// `controlplane` stable, and also carry `leader` when elected leader.
    #[test]
    fn role_label_keys_for_shape_solo_voter_is_controlplane_leader() {
        use crate::datastore::raft::types::RaftShape;
        let role = crate::bootstrap::NodeRole::Controlplane {
            leader_endpoints: vec![],
            token: None,
            skip_ca: true,
            as_learner: false,
        };
        let shape = RaftShape {
            voter_count: 1,
            is_leader: true,
            is_learner: false,
        };
        assert_eq!(
            role_label_keys_for_shape(&role, Some(&shape)),
            vec![
                "node-role.kubernetes.io/controlplane",
                "node-role.kubernetes.io/leader",
            ]
        );
    }

    /// P3-11d: once the cluster grows to >=2 voters, the elected leader
    /// must emit BOTH `controlplane` and `leader`.
    #[test]
    fn role_label_keys_for_shape_three_voter_leader_emits_both() {
        use crate::datastore::raft::types::RaftShape;
        let role = crate::bootstrap::NodeRole::Controlplane {
            leader_endpoints: vec![],
            token: None,
            skip_ca: true,
            as_learner: false,
        };
        let shape = RaftShape {
            voter_count: 3,
            is_leader: true,
            is_learner: false,
        };
        assert_eq!(
            role_label_keys_for_shape(&role, Some(&shape)),
            vec![
                "node-role.kubernetes.io/controlplane",
                "node-role.kubernetes.io/leader",
            ]
        );
    }

    /// P3-11d: a follower voter in a 3-voter cluster emits ONLY
    /// `controlplane` - the leader sub-label belongs to the elected
    /// voter, not every controlplane.
    #[test]
    fn role_label_keys_for_shape_three_voter_follower_is_control_plane_only() {
        use crate::datastore::raft::types::RaftShape;
        let role = crate::bootstrap::NodeRole::Controlplane {
            leader_endpoints: vec!["https://10.99.0.10:7679".into()],
            token: Some("tok".into()),
            skip_ca: true,
            as_learner: false,
        };
        let shape = RaftShape {
            voter_count: 3,
            is_leader: false,
            is_learner: false,
        };
        assert_eq!(
            role_label_keys_for_shape(&role, Some(&shape)),
            vec!["node-role.kubernetes.io/controlplane"]
        );
    }

    /// P3-11d: a joining controlplane whose `add_voter` hasn't committed
    /// yet has `voter_count == 0` - emit no role label rather than
    /// claiming a stamp before membership lands.
    #[test]
    fn role_label_keys_for_shape_unjoined_emits_nothing() {
        use crate::datastore::raft::types::RaftShape;
        let role = crate::bootstrap::NodeRole::Controlplane {
            leader_endpoints: vec!["https://10.99.0.10:7679".into()],
            token: Some("tok".into()),
            skip_ca: true,
            as_learner: false,
        };
        let shape = RaftShape {
            voter_count: 0,
            is_leader: false,
            is_learner: false,
        };
        let labels = role_label_keys_for_shape(&role, Some(&shape));
        assert!(labels.is_empty(), "unjoined voter must emit no role label");
    }

    /// P3-11d: without a `RaftShape` we fall back to the static
    /// `node_role_label_key` so legacy LeaderFollower mode and pre-raft
    /// boots remain stamp-correct.
    #[test]
    fn role_label_keys_for_shape_none_falls_back_to_static_label() {
        let role = crate::bootstrap::NodeRole::Leader {
            bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
        };
        assert_eq!(
            role_label_keys_for_shape(&role, None),
            vec!["node-role.kubernetes.io/leader"]
        );
    }

    /// T1.7: a controlplane-class node that is currently in raft
    /// membership as a learner must emit the `replica` label regardless
    /// of its CLI-declared role. This covers the case where an operator
    /// runs `klights controlplane` against a leader that admitted it as
    /// a learner (pending `change_membership` to promote) - until the
    /// promote commits, this node serves as a learner replica.
    #[test]
    fn role_label_keys_for_shape_learner_controlplane_emits_replica() {
        use crate::datastore::raft::types::RaftShape;
        let role = crate::bootstrap::NodeRole::Controlplane {
            leader_endpoints: vec!["https://10.99.0.10:7679".into()],
            token: Some("tok".into()),
            skip_ca: true,
            as_learner: false,
        };
        let shape = RaftShape {
            voter_count: 3,
            is_leader: false,
            is_learner: true,
        };
        assert_eq!(
            role_label_keys_for_shape(&role, Some(&shape)),
            vec!["node-role.kubernetes.io/replica"],
            "learner status overrides controlplane role label"
        );
    }

    /// T1.7: even a `Leader` role declaration emits `replica` when the
    /// node is currently a learner. The shape (live raft metrics) is the
    /// ground truth; the CLI role is only a starting hint.
    #[test]
    fn role_label_keys_for_shape_learner_overrides_leader_role() {
        use crate::datastore::raft::types::RaftShape;
        let role = crate::bootstrap::NodeRole::Leader {
            bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
        };
        let shape = RaftShape {
            voter_count: 1,
            is_leader: false,
            is_learner: true,
        };
        assert_eq!(
            role_label_keys_for_shape(&role, Some(&shape)),
            vec!["node-role.kubernetes.io/replica"]
        );
    }

    /// P3-11d: worker labels are static - the shape-driven rule only
    /// applies to leader-class roles. Replicas (post-T1.6) are
    /// `NodeRole::Controlplane { as_learner: true }`, not Workers; the
    /// replica label comes from `shape.is_learner=true` (covered by
    /// `role_label_keys_for_shape_learner_controlplane_emits_replica`).
    #[test]
    fn role_label_keys_for_shape_worker_stays_static() {
        use crate::datastore::raft::types::RaftShape;
        let shape = RaftShape {
            voter_count: 3,
            is_leader: true,
            is_learner: false,
        };
        let worker = crate::bootstrap::NodeRole::Worker {
            leader_endpoints: vec![],
            token: None,
            skip_ca: true,
        };
        assert_eq!(
            role_label_keys_for_shape(&worker, Some(&shape)),
            vec!["node-role.kubernetes.io/worker"]
        );
    }
}
