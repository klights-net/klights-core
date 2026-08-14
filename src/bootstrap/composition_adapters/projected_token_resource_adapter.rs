use crate::datastore::Resource;
use klights_auth::projected_service_account_token::{
    ProjectedTokenResourceReader, ProjectedTokenStoredResource,
};

/// Root-private adapter for the resources consumed by auth's projected-token policy.
pub(crate) struct ProjectedTokenResourceAdapter<'a> {
    reads: Option<&'a dyn klights_cluster_store::ClusterResourceRead>,
    #[cfg(test)]
    db: Option<&'a dyn crate::datastore::DatastoreBackend>,
}

impl<'a> ProjectedTokenResourceAdapter<'a> {
    pub(crate) fn new(reads: &'a dyn klights_cluster_store::ClusterResourceRead) -> Self {
        Self {
            reads: Some(reads),
            #[cfg(test)]
            db: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(db: &'a dyn crate::datastore::DatastoreBackend) -> Self {
        Self {
            reads: None,
            db: Some(db),
        }
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
        #[cfg(test)]
        if let Some(db) = self.db {
            return db
                .get_resource("v1", "ServiceAccount", Some(namespace), name)
                .await
                .map(|resource| resource.map(Self::stored))
                .map_err(|error| error.to_string());
        }
        self.reads
            .expect("focused projected-token reads")
            .get_resource(klights_cluster_store::ResourceGetRequest::new(
                "v1",
                "ServiceAccount",
                Some(namespace.to_string()),
                name,
            ))
            .await
            .map(|resource| resource.map(Self::stored))
            .map_err(|error| error.to_string())
    }

    async fn get_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<ProjectedTokenStoredResource>, String> {
        #[cfg(test)]
        if let Some(db) = self.db {
            return db
                .get_resource("v1", "Pod", Some(namespace), name)
                .await
                .map(|resource| resource.map(Self::stored))
                .map_err(|error| error.to_string());
        }
        self.reads
            .expect("focused projected-token reads")
            .get_resource(klights_cluster_store::ResourceGetRequest::new(
                "v1",
                "Pod",
                Some(namespace.to_string()),
                name,
            ))
            .await
            .map(|resource| resource.map(Self::stored))
            .map_err(|error| error.to_string())
    }

    async fn get_node(&self, name: &str) -> Result<Option<ProjectedTokenStoredResource>, String> {
        #[cfg(test)]
        if let Some(db) = self.db {
            return db
                .get_resource("v1", "Node", None, name)
                .await
                .map(|resource| resource.map(Self::stored))
                .map_err(|error| error.to_string());
        }
        self.reads
            .expect("focused projected-token reads")
            .get_resource(klights_cluster_store::ResourceGetRequest::new(
                "v1", "Node", None, name,
            ))
            .await
            .map(|resource| resource.map(Self::stored))
            .map_err(|error| error.to_string())
    }
}
