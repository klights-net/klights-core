//! Composition-root datastore adapter for focused Pod event ports.

pub(crate) struct DatastorePodEventAdapter<'a> {
    db: &'a dyn crate::datastore::DatastoreBackend,
}

impl<'a> DatastorePodEventAdapter<'a> {
    pub(crate) const fn new(db: &'a dyn crate::datastore::DatastoreBackend) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl klights_kubelet::pod_events::PodEventQuery for DatastorePodEventAdapter<'_> {
    async fn namespace_eligibility(
        &self,
        namespace: &str,
    ) -> anyhow::Result<klights_kubelet::pod_events::PodEventNamespaceEligibility> {
        let resource = self.db.get_namespace(namespace).await?;
        Ok(map_namespace_eligibility(
            crate::namespace_admission::classify_namespace(
                namespace,
                resource.as_ref().map(|resource| resource.data.as_ref()),
            ),
        ))
    }

    async fn list_events(
        &self,
        namespace: &str,
    ) -> anyhow::Result<Vec<klights_cluster_core::Resource>> {
        Ok(self
            .db
            .list_resources(
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
impl klights_kubelet::pod_events::PodEventEffect for DatastorePodEventAdapter<'_> {
    async fn create_event(
        &self,
        namespace: &str,
        name: &str,
        event: serde_json::Value,
    ) -> anyhow::Result<()> {
        self.db
            .create_resource("v1", "Event", Some(namespace), name, event)
            .await
            .map(|_| ())
    }
}

pub(crate) struct LeaderPodEventQuery<'a> {
    query: &'a dyn klights_leader_api::LeaderResourceQuery,
}

impl<'a> LeaderPodEventQuery<'a> {
    pub(crate) const fn new(query: &'a dyn klights_leader_api::LeaderResourceQuery) -> Self {
        Self { query }
    }
}

#[async_trait::async_trait]
impl klights_kubelet::pod_events::PodEventQuery for LeaderPodEventQuery<'_> {
    async fn namespace_eligibility(
        &self,
        namespace: &str,
    ) -> anyhow::Result<klights_kubelet::pod_events::PodEventNamespaceEligibility> {
        let resource = self
            .query
            .get_resource(klights_leader_api::ResourceGetRequest::try_new(
                klights_types::ResourceKey::new("v1", "Namespace", None, namespace),
                klights_leader_api::ResourceQueryConsistency::LeaderFresh,
            )?)
            .await
            .map_err(anyhow::Error::new)?;
        Ok(map_namespace_eligibility(
            crate::namespace_admission::classify_namespace(
                namespace,
                resource.as_ref().map(|resource| resource.data.as_ref()),
            ),
        ))
    }

    async fn list_events(
        &self,
        namespace: &str,
    ) -> anyhow::Result<Vec<klights_cluster_core::Resource>> {
        self.query
            .list_resources(klights_leader_api::ResourceListRequest::try_new(
                "v1",
                "Event",
                Some(namespace.to_string()),
                None,
                None,
                None,
                None,
                klights_leader_api::ResourceQueryConsistency::LeaderFresh,
            )?)
            .await
            .map(|result| result.into_parts().0)
            .map_err(anyhow::Error::new)
    }
}

fn map_namespace_eligibility(
    eligibility: crate::namespace_admission::NamespaceCreateEligibility,
) -> klights_kubelet::pod_events::PodEventNamespaceEligibility {
    use crate::namespace_admission::NamespaceCreateEligibility;
    use klights_kubelet::pod_events::PodEventNamespaceEligibility;

    match eligibility {
        NamespaceCreateEligibility::Allowed => PodEventNamespaceEligibility::Allowed,
        NamespaceCreateEligibility::Missing => PodEventNamespaceEligibility::Missing,
        NamespaceCreateEligibility::Terminating => PodEventNamespaceEligibility::Terminating,
    }
}
