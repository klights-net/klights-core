use crate::node_config::{KubeletNodeRole, NodeRegistrationProfile};

/// Validated process facts supplied by the private root composition adapter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeRegistrationProcessInputs {
    peer_mode: klights_network_api::NodePeerMode,
    role: KubeletNodeRole,
    joins_existing_cluster: bool,
    build_identity: klights_types::BuildIdentity,
}

impl NodeRegistrationProcessInputs {
    pub fn new(
        peer_mode: klights_network_api::NodePeerMode,
        role: KubeletNodeRole,
        joins_existing_cluster: bool,
        build_identity: klights_types::BuildIdentity,
    ) -> Self {
        Self {
            peer_mode,
            role,
            joins_existing_cluster,
            build_identity,
        }
    }
}

/// Derive kubelet registration policy from root-provided process facts.
pub fn build_profile(inputs: NodeRegistrationProcessInputs) -> NodeRegistrationProfile {
    let publish_external_ip = match inputs.role {
        KubeletNodeRole::Worker => true,
        KubeletNodeRole::Leader | KubeletNodeRole::Controlplane { .. } => {
            inputs.joins_existing_cluster
        }
    };

    NodeRegistrationProfile::new(
        inputs.peer_mode,
        inputs.role,
        publish_external_ip,
        inputs.build_identity,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> klights_types::BuildIdentity {
        klights_types::BuildIdentity::new("v1.34.6+klights", "abcdef12")
    }

    #[test]
    fn profile_policy_preserves_role_and_join_semantics() {
        let cases = [
            (KubeletNodeRole::Leader, false, false),
            (KubeletNodeRole::Leader, true, true),
            (
                KubeletNodeRole::Controlplane { as_learner: false },
                false,
                false,
            ),
            (
                KubeletNodeRole::Controlplane { as_learner: true },
                true,
                true,
            ),
            (KubeletNodeRole::Worker, false, true),
        ];

        for (role, joins_existing_cluster, expected_external_ip) in cases {
            let profile = build_profile(NodeRegistrationProcessInputs::new(
                klights_network_api::NodePeerMode::Rootless,
                role,
                joins_existing_cluster,
                identity(),
            ));
            assert_eq!(profile.role(), role);
            assert_eq!(
                profile.peer_mode(),
                klights_network_api::NodePeerMode::Rootless
            );
            assert_eq!(profile.publish_external_ip(), expected_external_ip);
            assert_eq!(profile.kubelet_version(), "v1.34.6+klights");
            assert_eq!(profile.git_commit(), "abcdef12");
        }
    }
}
