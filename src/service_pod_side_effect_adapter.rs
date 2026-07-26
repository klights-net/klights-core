use anyhow::Result;
use async_trait::async_trait;

use crate::datastore::{DatastoreBackend, ResourceListQuery};
use crate::side_effects::service_pod::{ServiceEndpointState, ServicePodStore};

#[async_trait]
impl<T> ServicePodStore for T
where
    T: DatastoreBackend + ?Sized,
{
    async fn load_service_endpoint_state(&self, namespace: &str) -> Result<ServiceEndpointState> {
        let services = self
            .list_resources("v1", "Service", Some(namespace), ResourceListQuery::all())
            .await?;
        let endpoints = self
            .list_resources("v1", "Endpoints", Some(namespace), ResourceListQuery::all())
            .await?;
        let endpoint_slices = self
            .list_resources(
                "discovery.k8s.io/v1",
                "EndpointSlice",
                Some(namespace),
                ResourceListQuery::all(),
            )
            .await?;
        Ok(ServiceEndpointState {
            services: services.items,
            endpoints: endpoints.items,
            endpoint_slices: endpoint_slices.items,
        })
    }
}
