pub(crate) struct LeaderPodEventQuery<'a> {
    query: &'a dyn klights_leader_api::LeaderResourceQuery,
}

pub(crate) struct LeaderPodEventEffect {
    commands: std::sync::Arc<dyn klights_leader_api::LeaderResourceCommand>,
}

impl LeaderPodEventEffect {
    pub(crate) fn new(
        commands: std::sync::Arc<dyn klights_leader_api::LeaderResourceCommand>,
    ) -> Self {
        Self { commands }
    }
}

#[async_trait::async_trait]
impl klights_kubelet::pod_events::PodEventEffect for LeaderPodEventEffect {
    async fn create_event(
        &self,
        namespace: &str,
        name: &str,
        event: serde_json::Value,
    ) -> anyhow::Result<()> {
        let request = klights_leader_api::ResourceCommandRequest::try_new(
            klights_cluster_core::StorageCommand::CreateResource {
                api_version: "v1".to_string(),
                kind: "Event".to_string(),
                namespace: Some(namespace.to_string()),
                name: name.to_string(),
                data: event,
            },
        )?;
        self.commands
            .submit_resource_command(request)
            .await
            .map(|_| ())
            .map_err(anyhow::Error::new)
    }
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
            k8s_native_service::classify_namespace(
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
                klights_leader_api::ResourceListScope::Namespace(namespace.to_string()),
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
    eligibility: k8s_native_service::NamespaceCreateEligibility,
) -> klights_kubelet::pod_events::PodEventNamespaceEligibility {
    use k8s_native_service::NamespaceCreateEligibility;
    use klights_kubelet::pod_events::PodEventNamespaceEligibility;

    match eligibility {
        NamespaceCreateEligibility::Allowed => PodEventNamespaceEligibility::Allowed,
        NamespaceCreateEligibility::Missing => PodEventNamespaceEligibility::Missing,
        NamespaceCreateEligibility::Terminating => PodEventNamespaceEligibility::Terminating,
    }
}
