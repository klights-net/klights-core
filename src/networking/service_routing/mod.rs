mod coalescer;
mod hostport;
mod inventory;
mod mode;
mod network_policy;
mod nft_table;
mod planner;
mod prelude;
mod service_rules;
mod session_affinity;
mod state_source;

pub use inventory::{InventoryApply, ServiceRouteInventory};
pub use planner::RoutePlan;

pub use coalescer::{
    NftServiceRouter, NftServiceRouterBoot, NftServiceRouterDefaultBoot,
    NftServiceRouterNetworkConfig, NftServiceRouterRuntime, NftServiceRouterStores,
    NftServiceRouterTableConfig,
};
pub use mode::ServiceRoutingMode;
pub use network_policy::{
    Ipv4CidrMatch, NetworkPolicyDirection, NetworkPolicyFlow, NetworkPolicyPeerMatch,
    NetworkPolicyPlan, NetworkPolicyPortMatch,
};

const FILTER_FORWARD_CHAIN: &std::ffi::CStr = c"filter-forward";
const NAT_POSTROUTING_CHAIN: &std::ffi::CStr = c"nat-postrouting";
const NAT_PREROUTING_CHAIN: &std::ffi::CStr = c"nat-prerouting";
const NAT_OUTPUT_CHAIN: &std::ffi::CStr = c"nat-output";
const SERVICES_CHAIN: &std::ffi::CStr = c"services";
const SERVICE_CT_GUARD_CHAIN: &std::ffi::CStr = c"service_ct_guard";
const NETWORK_POLICY_CHAIN: &std::ffi::CStr = c"network-policy";
const HOSTPORTS_CHAIN: &std::ffi::CStr = c"hostports";
const REMOTE_POD_ENDPOINTS_CHAIN: &std::ffi::CStr = c"remote_pod_v4";

const PRIORITY_FILTER: i32 = 0;
const PRIORITY_NAT_SRC: i32 = 100;
const PRIORITY_NAT_DST: i32 = -100;

#[cfg(test)]
pub(crate) use hostport::HostPortSpec;
pub use nft_table::{KlightsTable, bootstrap_inventory_from_api, service_specs_from_api};
#[cfg(test)]
pub use service_rules::service_ct_guard_applies_to_forward_packet;
pub use service_rules::{
    PortSpec, Protocol, RemotePodEndpointSpec, ServiceCtGuardTransition, ServiceCtTuple,
    ServiceRuleSnapshot, ServiceSpec, legacy_unscoped_service_tables_to_cleanup,
    remote_pod_endpoint_specs_from_topology, service_ct_guard_transition, service_ct_guard_tuples,
};
#[cfg(test)]
pub(crate) use service_rules::{
    parse_port, parse_session_affinity, prefix_len_from_mask, probability_for_ladder_step,
};
pub use session_affinity::SessionAffinity;
pub use state_source::{
    NetworkPolicySnapshot, RoutingStateFuture, RoutingStateSource, ServiceRoutingResource,
    ServiceRoutingSnapshot,
};

#[cfg(test)]
mod tests;
