//! Composition-root datastore adapter for focused Pod event ports.

#[async_trait::async_trait]
impl<T> crate::pod_events::PodEventQuery for T
where
    T: crate::datastore::DatastoreBackend + ?Sized,
{
    async fn namespace_eligibility(
        &self,
        namespace: &str,
    ) -> anyhow::Result<crate::namespace_admission::NamespaceCreateEligibility> {
        let resource = crate::datastore::DatastoreBackend::get_namespace(self, namespace).await?;
        Ok(crate::namespace_admission::classify_namespace(
            namespace,
            resource.as_ref().map(|resource| resource.data.as_ref()),
        ))
    }

    async fn list_events(
        &self,
        namespace: &str,
    ) -> anyhow::Result<Vec<klights_cluster_core::Resource>> {
        Ok(crate::datastore::DatastoreBackend::list_resources(
            self,
            "v1",
            "Event",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await?
        .items)
    }
}

#[async_trait::async_trait]
impl<T> crate::pod_events::PodEventEffect for T
where
    T: crate::datastore::DatastoreBackend + ?Sized,
{
    async fn create_event(
        &self,
        namespace: &str,
        name: &str,
        event: serde_json::Value,
    ) -> anyhow::Result<()> {
        crate::datastore::DatastoreBackend::create_resource(
            self,
            "v1",
            "Event",
            Some(namespace),
            name,
            event,
        )
        .await
        .map(|_| ())
    }
}
