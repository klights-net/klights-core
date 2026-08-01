use crate::ApiState;
use klights_leader_api::{CrdRegistry, LeaderResourceQuery};
use std::path::Path;
use tokio::sync::OnceCell;

use super::apiservice_proxy::ApiServiceProxyCache;

/// Focused query capability required by discovery and OpenAPI delivery.
pub trait DiscoveryResourceQuery: Send + Sync {
    fn resource_query(&self) -> &dyn LeaderResourceQuery;
}

/// Focused dynamic-discovery and API aggregation state.
pub trait DiscoveryAggregation: Send + Sync {
    fn crd_registry(&self) -> &CrdRegistry;
    fn apiservice_proxy_identity_cache(&self) -> &OnceCell<reqwest::Identity>;
    fn apiservice_proxy_cache(&self) -> &ApiServiceProxyCache;
}

/// Runtime inputs needed only by APIService client-certificate loading.
pub trait DiscoveryOperationalInputs: Send + Sync {
    fn apiservice_proxy_cert(&self) -> &Path;
    fn apiservice_proxy_key(&self) -> &Path;
    fn task_supervisor(&self) -> &klights_supervisor::TaskSupervisor;
}

/// Native discovery view of the existing six-family API state.
pub trait DiscoveryState: Send + Sync {
    fn resource_query(&self) -> &dyn LeaderResourceQuery;
    fn crd_registry(&self) -> &CrdRegistry;
    fn apiservice_proxy_identity_cache(&self) -> &OnceCell<reqwest::Identity>;
    fn apiservice_proxy_cache(&self) -> &ApiServiceProxyCache;
    fn apiservice_proxy_cert(&self) -> &Path;
    fn apiservice_proxy_key(&self) -> &Path;
    fn task_supervisor(&self) -> &klights_supervisor::TaskSupervisor;
}

impl<S: DiscoveryState + ?Sized> DiscoveryState for std::sync::Arc<S> {
    fn resource_query(&self) -> &dyn LeaderResourceQuery {
        self.as_ref().resource_query()
    }

    fn crd_registry(&self) -> &CrdRegistry {
        self.as_ref().crd_registry()
    }

    fn apiservice_proxy_identity_cache(&self) -> &OnceCell<reqwest::Identity> {
        self.as_ref().apiservice_proxy_identity_cache()
    }

    fn apiservice_proxy_cache(&self) -> &ApiServiceProxyCache {
        self.as_ref().apiservice_proxy_cache()
    }

    fn apiservice_proxy_cert(&self) -> &Path {
        self.as_ref().apiservice_proxy_cert()
    }

    fn apiservice_proxy_key(&self) -> &Path {
        self.as_ref().apiservice_proxy_key()
    }

    fn task_supervisor(&self) -> &klights_supervisor::TaskSupervisor {
        self.as_ref().task_supervisor()
    }
}

impl<Auth, Resource, Discovery, Reconcile, PodNode, Operational> DiscoveryState
    for ApiState<Auth, Resource, Discovery, Reconcile, PodNode, Operational>
where
    Auth: Send + Sync,
    Resource: DiscoveryResourceQuery,
    Discovery: DiscoveryAggregation,
    Reconcile: Send + Sync,
    PodNode: Send + Sync,
    Operational: DiscoveryOperationalInputs,
{
    fn resource_query(&self) -> &dyn LeaderResourceQuery {
        self.resource_mutation().resource_query()
    }

    fn crd_registry(&self) -> &CrdRegistry {
        self.discovery().crd_registry()
    }

    fn apiservice_proxy_identity_cache(&self) -> &OnceCell<reqwest::Identity> {
        self.discovery().apiservice_proxy_identity_cache()
    }

    fn apiservice_proxy_cache(&self) -> &ApiServiceProxyCache {
        self.discovery().apiservice_proxy_cache()
    }

    fn apiservice_proxy_cert(&self) -> &Path {
        self.operational().apiservice_proxy_cert()
    }

    fn apiservice_proxy_key(&self) -> &Path {
        self.operational().apiservice_proxy_key()
    }

    fn task_supervisor(&self) -> &klights_supervisor::TaskSupervisor {
        self.operational().task_supervisor()
    }
}
