//! Mode + device wiring for service routing (F2-03).
//!
//! `KlightsTable` previously hardcoded the overlay forward rule's interface name
//! to the project default device. That breaks two ways:
//!   1. Rootless mode never owns a VXLAN device, so the rule matches an
//!      interface that does not exist in the user namespace.
//!   2. Test instances with custom bridge/table names must not accidentally
//!      pin the rule to an unrelated interface name.
//!
//! Both decisions belong in one config value handed down from the network
//! boot layer, not buried inside `nft_table.rs`.

use crate::bootstrap::NodeMode;

#[derive(Clone, Debug)]
pub struct ServiceRoutingMode {
    node_mode: NodeMode,
    vxlan_device: String,
}

impl ServiceRoutingMode {
    pub fn new(node_mode: NodeMode, vxlan_device: impl Into<String>) -> Self {
        Self {
            node_mode,
            vxlan_device: vxlan_device.into(),
        }
    }

    /// Convenience for tests and any cleanup path that doesn't depend on the
    /// mode's behavior. Pinned to root + the project default device name.
    #[cfg(test)]
    pub fn default_root_for_test() -> Self {
        Self::new(
            NodeMode::Root,
            crate::networking::DEFAULT_POD_OVERLAY_DEVICE,
        )
    }

    /// True when the forward chain should accept packets arriving on the
    /// VXLAN overlay device. Only root mode owns a VXLAN device.
    pub fn vxlan_rule_enabled(&self) -> bool {
        matches!(self.node_mode, NodeMode::Root)
    }

    /// VXLAN device name the forward-chain rule should match. Caller is
    /// expected to honor `vxlan_rule_enabled()`; the device name is still
    /// returned in rootless mode for diagnostics but should never be used.
    pub fn vxlan_device(&self) -> &str {
        &self.vxlan_device
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn vxlan_rule_enabled_in_root_mode() {
        let mode = ServiceRoutingMode::new(NodeMode::Root, "klights.vxlan");
        assert!(mode.vxlan_rule_enabled());
        assert_eq!(mode.vxlan_device(), "klights.vxlan");
    }

    #[test]
    fn vxlan_rule_disabled_in_rootless_mode() {
        let mode = ServiceRoutingMode::new(
            NodeMode::Rootless {
                rootlesskit_pid: 42,
                user_netns: PathBuf::from("/proc/42/ns/net"),
            },
            "klights.vxlan",
        );
        assert!(
            !mode.vxlan_rule_enabled(),
            "rootless never owns a VXLAN device — forward rule must be omitted"
        );
    }

    #[test]
    fn vxlan_device_carries_configured_name() {
        let mode = ServiceRoutingMode::new(NodeMode::Root, "tester1.vxlan");
        assert_eq!(
            mode.vxlan_device(),
            "tester1.vxlan",
            "configured routing mode device name must be honored"
        );
    }
}
