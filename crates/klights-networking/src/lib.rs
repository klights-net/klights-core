//! Container and service networking for klights.

mod cleanup;
mod cni;
pub mod cni_plugin;
pub mod dataplane_health;
mod device_state;
mod netfilter;
mod netns_sync;
mod network_boot;
mod network_composition;
mod network_plane;
mod peer_dataplane;
mod pod_endpoint_resolver;
mod pod_link;
mod pod_network_events;
mod root_datapath;
pub mod rootless;
pub mod service_routing;
mod subnet_allocator;
mod types;
pub mod wireguard;

pub use cleanup::{NetworkCleanup, NetworkCleanupArgs, NetworkCleanupKind};
pub use cni::{CniAddArgs, SandboxOperationGuard, SandboxOperationLocks, add, del};
pub use network_boot::{NetworkBoot, NetworkBootStores};
pub use network_composition::{
    Network, NetworkBootConfig, NetworkCleanupConfig, NetworkMode, POD_OVERLAY_MTU,
    pod_link_mtu_for_encryption,
};
pub use network_plane::NetworkPlane;
pub use peer_dataplane::{RootPeerDataplane, RootPeerDataplaneBoot};
pub use pod_endpoint_resolver::StorePodEndpointResolver;
pub use pod_network_events::PodNetworkAssignmentBus;
pub use root_datapath::RootDatapath;
pub use subnet_allocator::NodeSubnetAllocator;
pub use types::{BridgeName, PodLinkMtu};

#[cfg(test)]
mod bootstrap_plane_contract_tests {
    #[test]
    fn root_network_plane_is_owned_by_networking() {
        fn assert_datapath<T: klights_network_api::Datapath>() {}

        assert_datapath::<super::NetworkPlane>();
    }
}

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
