mod access;
mod apiextensions_v1;
mod apiservice_proxy;
mod apps_v1;
mod authorization_v1;
mod batch_v1;
mod certificates_v1;
mod coordination_v1;
mod core_v1;
mod custom_resource;
mod discovery_v1;
mod events_k8s_io_v1;
mod groups;
mod metrics_v1beta1;
mod networking_v1;
mod node_k8s_io_v1;
mod openapi;
mod policy_v1;
mod rbac_v1;
mod scheduling_v1;
mod shared;
mod storage_v1;

pub use self::apiextensions_v1::*;
pub use self::apps_v1::*;
pub use self::authorization_v1::*;
pub use self::batch_v1::*;
pub use self::certificates_v1::*;
pub use self::coordination_v1::*;
pub use self::core_v1::*;
pub use self::custom_resource::*;
pub use self::discovery_v1::*;
pub use self::events_k8s_io_v1::*;
pub use self::groups::*;
pub use self::metrics_v1beta1::*;
pub use self::networking_v1::*;
pub use self::node_k8s_io_v1::*;
pub use self::openapi::*;
pub use self::policy_v1::*;
pub use self::rbac_v1::*;
pub use self::scheduling_v1::*;
pub use self::shared::*;
pub use self::storage_v1::*;
pub use access::{
    DiscoveryAggregation, DiscoveryOperationalInputs, DiscoveryResourceQuery, DiscoveryState,
};
pub use apiservice_proxy::{
    ApiServiceProxyCache, invalidate_apiservice_proxy_cache_for_resource,
    load_apiservice_proxy_identity, proxy_apiservice_request, resolve_service_endpoint,
    resolve_service_proxy_target,
};

pub async fn register_crd_from_value(
    registry: &klights_leader_api::CrdRegistry,
    value: &serde_json::Value,
) -> Result<(), String> {
    for info in klights_leader_api::resource_infos_from_value(value)? {
        registry.register(info).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests_api_discovery_core;
#[cfg(test)]
mod tests_api_discovery_groups;
