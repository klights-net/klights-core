use crate::datastore::{DatastoreBackend, Resource};
use klights_auth::projected_service_account_token::{
    ProjectedTokenResourceReader, ProjectedTokenStoredResource,
};
use klights_kubelet::pod_repository::store::PodStore;

/// Root-private adapter for the resources consumed by auth's projected-token policy.
pub(crate) struct ProjectedTokenResourceAdapter<'a> {
    db: &'a dyn DatastoreBackend,
    pod_store: &'a PodStore,
}

impl<'a> ProjectedTokenResourceAdapter<'a> {
    pub(crate) fn new(db: &'a dyn DatastoreBackend, pod_store: &'a PodStore) -> Self {
        Self { db, pod_store }
    }

    fn stored(resource: Resource) -> ProjectedTokenStoredResource {
        ProjectedTokenStoredResource::new(
            resource.api_version,
            resource.kind,
            resource.namespace,
            resource.name,
            resource.uid,
            resource.resource_version,
            resource.data,
        )
    }
}

#[async_trait::async_trait]
impl ProjectedTokenResourceReader for ProjectedTokenResourceAdapter<'_> {
    async fn get_service_account(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<ProjectedTokenStoredResource>, String> {
        self.db
            .get_resource("v1", "ServiceAccount", Some(namespace), name)
            .await
            .map(|resource| resource.map(Self::stored))
            .map_err(|error| error.to_string())
    }

    async fn get_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<ProjectedTokenStoredResource>, String> {
        self.pod_store
            .get(namespace, name)
            .await
            .map(|resource| resource.map(Self::stored))
            .map_err(|error| error.to_string())
    }

    async fn get_node(&self, name: &str) -> Result<Option<ProjectedTokenStoredResource>, String> {
        self.db
            .get_resource("v1", "Node", None, name)
            .await
            .map(|resource| resource.map(Self::stored))
            .map_err(|error| error.to_string())
    }
}
