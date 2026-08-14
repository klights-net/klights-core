use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_store::{
    ClusterResourceRead, ResourceCollectionScope, ResourceListOptions, ResourceListQuery,
    ResourceListRead, ResourceListRequest,
};

use crate::datastore::DatastoreBackend;
use klights_controllers::side_effects::service_pod::{ServiceEndpointState, ServicePodStore};

struct BorrowedServicePodStore<'a> {
    resource_reads: &'a dyn ClusterResourceRead,
}

pub(crate) fn borrowed_store(
    resource_reads: &dyn ClusterResourceRead,
) -> impl ServicePodStore + '_ {
    BorrowedServicePodStore { resource_reads }
}

#[async_trait]
impl ServicePodStore for BorrowedServicePodStore<'_> {
    async fn load_service_endpoint_state(&self, namespace: &str) -> Result<ServiceEndpointState> {
        let services = list_resources(self.resource_reads, "v1", "Service", namespace).await?;
        let endpoints = list_resources(self.resource_reads, "v1", "Endpoints", namespace).await?;
        let endpoint_slices = list_resources(
            self.resource_reads,
            "discovery.k8s.io/v1",
            "EndpointSlice",
            namespace,
        )
        .await?;
        Ok(ServiceEndpointState {
            services,
            endpoints,
            endpoint_slices,
        })
    }
}

async fn list_resources(
    resource_reads: &dyn ClusterResourceRead,
    api_version: &str,
    kind: &str,
    namespace: &str,
) -> Result<Vec<klights_cluster_core::Resource>> {
    match resource_reads
        .list_resources(ResourceListRequest::new(
            api_version,
            kind,
            ResourceCollectionScope::Namespace(namespace.to_string()),
            ResourceListQuery::all(),
        ))
        .await?
    {
        ResourceListRead::Current(page) | ResourceListRead::Historical(page) => {
            Ok(page.into_items())
        }
        ResourceListRead::Expired {
            requested,
            oldest_available,
            ..
        } => anyhow::bail!(
            "{api_version}/{kind} LIST at resourceVersion {requested} expired before {oldest_available}"
        ),
    }
}

#[async_trait]
impl ServicePodStore for dyn DatastoreBackend + '_ {
    async fn load_service_endpoint_state(&self, namespace: &str) -> Result<ServiceEndpointState> {
        let services = self
            .list_resources("v1", "Service", Some(namespace), ResourceListOptions::all())
            .await?;
        let endpoints = self
            .list_resources(
                "v1",
                "Endpoints",
                Some(namespace),
                ResourceListOptions::all(),
            )
            .await?;
        let endpoint_slices = self
            .list_resources(
                "discovery.k8s.io/v1",
                "EndpointSlice",
                Some(namespace),
                ResourceListOptions::all(),
            )
            .await?;
        Ok(ServiceEndpointState {
            services: services.items,
            endpoints: endpoints.items,
            endpoint_slices: endpoint_slices.items,
        })
    }
}
