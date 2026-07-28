#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KubeletNodeRole {
    Leader,
    Controlplane { as_learner: bool },
    Worker,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NodeRegistrationProfile {
    peer_mode: klights_network_api::NodePeerMode,
    role: KubeletNodeRole,
    publish_external_ip: bool,
    build_identity: klights_types::BuildIdentity,
}

impl NodeRegistrationProfile {
    pub(crate) fn new(
        peer_mode: klights_network_api::NodePeerMode,
        role: KubeletNodeRole,
        publish_external_ip: bool,
        build_identity: klights_types::BuildIdentity,
    ) -> Self {
        Self {
            peer_mode,
            role,
            publish_external_ip,
            build_identity,
        }
    }

    pub(crate) fn peer_mode(&self) -> klights_network_api::NodePeerMode {
        self.peer_mode
    }

    pub(crate) fn role(&self) -> KubeletNodeRole {
        self.role
    }

    pub(crate) fn publish_external_ip(&self) -> bool {
        self.publish_external_ip
    }

    pub(crate) fn kubelet_version(&self) -> &str {
        self.build_identity.kubelet_version()
    }

    pub(crate) fn git_commit(&self) -> &str {
        self.build_identity.git_commit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_preserves_root_translated_facts() {
        let profile = NodeRegistrationProfile::new(
            klights_network_api::NodePeerMode::Rootless,
            KubeletNodeRole::Controlplane { as_learner: true },
            true,
            klights_types::BuildIdentity::new("v1.34.6+klights0.9.14", "abcdef12"),
        );

        assert_eq!(
            profile.peer_mode(),
            klights_network_api::NodePeerMode::Rootless
        );
        assert_eq!(
            profile.role(),
            KubeletNodeRole::Controlplane { as_learner: true }
        );
        assert!(profile.publish_external_ip());
        assert_eq!(profile.kubelet_version(), "v1.34.6+klights0.9.14");
        assert_eq!(profile.git_commit(), "abcdef12");
    }
}
