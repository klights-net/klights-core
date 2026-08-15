use std::sync::Arc;

use klights_leader_api::{
    LeaderResourceQuery, ResourceListRequest, ResourceListScope, ResourceQueryConsistency,
    ResourceQueryError,
};

use super::{
    NetworkPolicySnapshot, RoutingStateError, RoutingStateFuture, RoutingStateSource,
    ServiceRoutingResource, ServiceRoutingSnapshot,
};

/// The networking-owned fresh snapshot policy used by native service routing.
pub struct LeaderRoutingStateSource {
    query: Arc<dyn LeaderResourceQuery>,
}

impl LeaderRoutingStateSource {
    pub fn new(query: Arc<dyn LeaderResourceQuery>) -> Self {
        Self { query }
    }
}

fn fresh_list_request(
    api_version: &str,
    kind: &str,
    scope: ResourceListScope,
) -> Result<ResourceListRequest, ResourceQueryError> {
    ResourceListRequest::try_new(
        api_version,
        kind,
        scope,
        None,
        None,
        None,
        None,
        ResourceQueryConsistency::LeaderFresh,
    )
}

fn routing_resource(resource: &klights_cluster_core::Resource) -> ServiceRoutingResource {
    ServiceRoutingResource {
        api_version: resource.api_version.clone(),
        kind: resource.kind.clone(),
        namespace: resource.namespace.clone(),
        name: resource.name.clone(),
        resource_version: resource.resource_version,
        data: resource.data.clone(),
    }
}

fn routing_state_error(error: impl std::fmt::Display) -> RoutingStateError {
    RoutingStateError::unavailable(error.to_string())
}

impl RoutingStateSource for LeaderRoutingStateSource {
    fn service_routing_snapshot(&self) -> RoutingStateFuture<'_, ServiceRoutingSnapshot> {
        Box::pin(async move {
            let services = self
                .query
                .list_resources(
                    fresh_list_request("v1", "Service", ResourceListScope::AllNamespaces)
                        .map_err(routing_state_error)?,
                )
                .await
                .map_err(routing_state_error)?;
            let endpoints = self
                .query
                .list_resources(
                    fresh_list_request("v1", "Endpoints", ResourceListScope::AllNamespaces)
                        .map_err(routing_state_error)?,
                )
                .await
                .map_err(routing_state_error)?;
            let endpoint_slices = self
                .query
                .list_resources(
                    fresh_list_request(
                        "discovery.k8s.io/v1",
                        "EndpointSlice",
                        ResourceListScope::AllNamespaces,
                    )
                    .map_err(routing_state_error)?,
                )
                .await
                .map_err(routing_state_error)?;
            Ok(ServiceRoutingSnapshot {
                services: services.items().iter().map(routing_resource).collect(),
                endpoints: endpoints.items().iter().map(routing_resource).collect(),
                endpoint_slices: endpoint_slices
                    .items()
                    .iter()
                    .map(routing_resource)
                    .collect(),
            })
        })
    }

    fn network_policy_snapshot(&self) -> RoutingStateFuture<'_, NetworkPolicySnapshot> {
        Box::pin(async move {
            let policies = self
                .query
                .list_resources(
                    fresh_list_request(
                        "networking.k8s.io/v1",
                        "NetworkPolicy",
                        ResourceListScope::AllNamespaces,
                    )
                    .map_err(routing_state_error)?,
                )
                .await
                .map_err(routing_state_error)?;
            let pods = self
                .query
                .list_resources(
                    fresh_list_request("v1", "Pod", ResourceListScope::AllNamespaces)
                        .map_err(routing_state_error)?,
                )
                .await
                .map_err(routing_state_error)?;
            let namespaces = self
                .query
                .list_resources(
                    fresh_list_request("v1", "Namespace", ResourceListScope::Cluster)
                        .map_err(routing_state_error)?,
                )
                .await
                .map_err(routing_state_error)?;
            Ok(NetworkPolicySnapshot {
                policies: policies
                    .items()
                    .iter()
                    .map(|resource| resource.data.clone())
                    .collect(),
                pods: pods
                    .items()
                    .iter()
                    .map(|resource| resource.data.clone())
                    .collect(),
                namespaces: namespaces
                    .items()
                    .iter()
                    .map(|resource| resource.data.clone())
                    .collect(),
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct Query {
        requests: Mutex<Vec<ResourceQueryConsistency>>,
    }
    impl LeaderResourceQuery for Query {
        fn list_resources(
            &self,
            request: ResourceListRequest,
        ) -> klights_leader_api::ResourceQueryFuture<'_, klights_leader_api::ResourceListResult>
        {
            Box::pin(async move {
                self.requests.lock().unwrap().push(request.consistency());
                klights_leader_api::ResourceListResult::try_new(Vec::new(), 1, None, None, None)
            })
        }
        fn get_resource(
            &self,
            _: klights_leader_api::ResourceGetRequest,
        ) -> klights_leader_api::ResourceQueryFuture<'_, Option<klights_cluster_core::Resource>>
        {
            Box::pin(async { unreachable!() })
        }
    }

    #[tokio::test]
    async fn routing_snapshots_use_leader_fresh_for_every_collection() {
        let query = Arc::new(Query {
            requests: Mutex::new(Vec::new()),
        });
        let source = LeaderRoutingStateSource::new(query.clone());
        source.service_routing_snapshot().await.unwrap();
        source.network_policy_snapshot().await.unwrap();
        assert_eq!(
            query.requests.lock().unwrap().as_slice(),
            &[ResourceQueryConsistency::LeaderFresh; 6]
        );
    }
}
