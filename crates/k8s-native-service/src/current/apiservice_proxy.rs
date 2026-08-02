//! Root compatibility adapter for native APIService aggregation.

pub use crate::discovery::{
    ApiServiceProxyCache, invalidate_apiservice_proxy_cache_for_resource, proxy_apiservice_request,
    resolve_service_proxy_target,
};
