fn node_role_label_key(role: &crate::kubelet::node_config::KubeletNodeRole) -> &'static str {
    use crate::kubelet::node_config::KubeletNodeRole;
    match role {
        KubeletNodeRole::Leader | KubeletNodeRole::Controlplane { .. } => {
            "node-role.kubernetes.io/leader"
        }
        KubeletNodeRole::Worker => "node-role.kubernetes.io/worker",
    }
}

/// Project the backend-neutral control-plane role into Kubernetes Node labels.
pub(crate) fn role_label_keys_for_projection(
    role: &crate::kubelet::node_config::KubeletNodeRole,
    projection: Option<klights_leader_api::NodeRoleProjection>,
) -> Vec<&'static str> {
    use crate::kubelet::node_config::KubeletNodeRole;
    use klights_leader_api::NodeRoleProjection;

    if matches!(role, KubeletNodeRole::Worker) {
        return vec!["node-role.kubernetes.io/worker"];
    }
    match projection {
        None => vec![node_role_label_key(role)],
        Some(NodeRoleProjection::Pending) => vec![],
        Some(NodeRoleProjection::StandaloneLeader) => {
            vec!["node-role.kubernetes.io/leader"]
        }
        Some(NodeRoleProjection::ControlPlaneLeader) => vec![
            "node-role.kubernetes.io/controlplane",
            "node-role.kubernetes.io/leader",
        ],
        Some(NodeRoleProjection::ControlPlaneFollower) => {
            vec!["node-role.kubernetes.io/controlplane"]
        }
        Some(NodeRoleProjection::Replica) => vec!["node-role.kubernetes.io/replica"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kubelet::node_config::KubeletNodeRole;
    use klights_leader_api::NodeRoleProjection;

    #[test]
    fn neutral_projections_preserve_control_plane_label_semantics() {
        let role = KubeletNodeRole::Controlplane { as_learner: false };
        let cases = [
            (NodeRoleProjection::Pending, vec![]),
            (
                NodeRoleProjection::ControlPlaneLeader,
                vec![
                    "node-role.kubernetes.io/controlplane",
                    "node-role.kubernetes.io/leader",
                ],
            ),
            (
                NodeRoleProjection::ControlPlaneFollower,
                vec!["node-role.kubernetes.io/controlplane"],
            ),
            (
                NodeRoleProjection::Replica,
                vec!["node-role.kubernetes.io/replica"],
            ),
        ];
        for (projection, expected) in cases {
            assert_eq!(
                role_label_keys_for_projection(&role, Some(projection)),
                expected
            );
        }
    }

    #[test]
    fn standalone_and_worker_labels_remain_stable() {
        assert_eq!(
            role_label_keys_for_projection(
                &KubeletNodeRole::Leader,
                Some(NodeRoleProjection::StandaloneLeader),
            ),
            vec!["node-role.kubernetes.io/leader"]
        );
        assert_eq!(
            role_label_keys_for_projection(
                &KubeletNodeRole::Worker,
                Some(NodeRoleProjection::ControlPlaneLeader),
            ),
            vec!["node-role.kubernetes.io/worker"]
        );
    }

    #[test]
    fn missing_projection_keeps_single_node_fallback() {
        assert_eq!(
            role_label_keys_for_projection(&KubeletNodeRole::Leader, None),
            vec!["node-role.kubernetes.io/leader"]
        );
    }
}
