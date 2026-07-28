use crate::bootstrap::{NodeMode, NodeRole};
use crate::kubelet::node_config::{KubeletNodeRole, NodeRegistrationProfile};

/// Translate process-level role and mode flags into the kubelet-owned
/// registration contract at the composition root.
pub(crate) fn build(node_mode: &NodeMode, node_role: &NodeRole) -> NodeRegistrationProfile {
    let peer_mode = match node_mode {
        NodeMode::Root => klights_network_api::NodePeerMode::Root,
        NodeMode::Rootless { .. } => klights_network_api::NodePeerMode::Rootless,
    };
    let role = match node_role {
        NodeRole::Leader { .. } => KubeletNodeRole::Leader,
        NodeRole::Controlplane { as_learner, .. } => KubeletNodeRole::Controlplane {
            as_learner: *as_learner,
        },
        NodeRole::Worker { .. } => KubeletNodeRole::Worker,
    };
    let publish_external_ip = match node_role {
        NodeRole::Leader {
            bootstrap:
                crate::bootstrap::node_role::LeaderBootstrap::Seed
                | crate::bootstrap::node_role::LeaderBootstrap::Bootstrap { .. },
        } => false,
        NodeRole::Controlplane {
            leader_endpoints, ..
        } if leader_endpoints.is_empty() => false,
        NodeRole::Worker { .. }
        | NodeRole::Controlplane { .. }
        | NodeRole::Leader {
            bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Join { .. },
        } => true,
    };

    NodeRegistrationProfile::new(
        peer_mode,
        role,
        publish_external_ip,
        crate::version::build_identity(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn translates_worker_rootless_registration_at_the_root() {
        let role = NodeRole::Worker {
            leader_endpoints: vec!["https://leader:9443".to_string()],
            token: None,
            skip_ca: false,
        };
        let profile = build(
            &NodeMode::Rootless {
                rootlesskit_pid: 42,
                user_netns: std::path::PathBuf::from("/proc/42/ns/net"),
            },
            &role,
        );

        assert_eq!(
            profile.peer_mode(),
            klights_network_api::NodePeerMode::Rootless
        );
        assert_eq!(profile.role(), KubeletNodeRole::Worker);
        assert!(profile.publish_external_ip());
        assert_eq!(profile.kubelet_version(), crate::version::GIT_VERSION);
    }
}
