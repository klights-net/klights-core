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
    kubelet_version: String,
}

impl NodeRegistrationProfile {
    pub(crate) fn new(
        peer_mode: klights_network_api::NodePeerMode,
        role: KubeletNodeRole,
        publish_external_ip: bool,
        kubelet_version: String,
    ) -> Self {
        Self {
            peer_mode,
            role,
            publish_external_ip,
            kubelet_version,
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
        &self.kubelet_version
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
            "v1.34.6+klights0.9.14".to_string(),
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
    }
}
