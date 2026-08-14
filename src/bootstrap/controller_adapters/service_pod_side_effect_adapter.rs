use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_store::ResourceListOptions;

use crate::datastore::DatastoreBackend;
use klights_controllers::side_effects::service_pod::{ServiceEndpointState, ServicePodStore};

struct BorrowedServicePodStore<'a> {
    db: &'a dyn DatastoreBackend,
}

pub(crate) fn borrowed_store(db: &dyn DatastoreBackend) -> impl ServicePodStore + '_ {
    BorrowedServicePodStore { db }
}

#[async_trait]
impl ServicePodStore for BorrowedServicePodStore<'_> {
    async fn load_service_endpoint_state(&self, namespace: &str) -> Result<ServiceEndpointState> {
        ServicePodStore::load_service_endpoint_state(self.db, namespace).await
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
