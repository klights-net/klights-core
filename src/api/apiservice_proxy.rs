//! Root compatibility adapter for native APIService aggregation.

pub use k8s_native_service::discovery::{
    ApiServiceProxyCache, invalidate_apiservice_proxy_cache_for_resource,
    load_apiservice_proxy_identity, proxy_apiservice_request, resolve_service_endpoint,
    resolve_service_proxy_target,
};
