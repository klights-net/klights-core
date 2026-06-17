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

pub use inventory::{InventoryApply, ServiceRouteInventory};
pub use planner::RoutePlan;

pub use coalescer::{
    NftServiceRouter, NftServiceRouterBoot, NftServiceRouterDefaultBoot,
    NftServiceRouterNetworkConfig, NftServiceRouterRuntime, NftServiceRouterStores,
    NftServiceRouterTableConfig,
};
pub use mode::ServiceRoutingMode;
pub use network_policy::*;

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

pub use hostport::*;
pub use nft_table::*;
pub use service_rules::*;
pub use session_affinity::*;

#[cfg(test)]
mod tests;
