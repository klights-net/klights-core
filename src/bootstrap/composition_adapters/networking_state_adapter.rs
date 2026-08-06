use std::sync::Arc;

use klights_leader_api::{
    LeaderResourceQuery, ResourceListRequest, ResourceQueryConsistency, ResourceQueryError,
};
use klights_networking::service_routing::{
    NetworkPolicySnapshot, RoutingStateError, RoutingStateFuture, RoutingStateSource,
    ServiceRoutingResource, ServiceRoutingSnapshot,
};

pub(crate) struct LeaderRoutingStateAdapter {
    query: Arc<dyn LeaderResourceQuery>,
}

impl LeaderRoutingStateAdapter {
    pub(crate) fn new(query: Arc<dyn LeaderResourceQuery>) -> Self {
        Self { query }
    }
}

fn fresh_list_request(
    api_version: &str,
    kind: &str,
) -> Result<ResourceListRequest, ResourceQueryError> {
    ResourceListRequest::try_new(
        api_version,
        kind,
        None,
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

impl RoutingStateSource for LeaderRoutingStateAdapter {
    fn service_routing_snapshot(&self) -> RoutingStateFuture<'_, ServiceRoutingSnapshot> {
        Box::pin(async move {
            let services = self
                .query
                .list_resources(fresh_list_request("v1", "Service").map_err(routing_state_error)?)
                .await
                .map_err(routing_state_error)?;
            let endpoints = self
                .query
                .list_resources(fresh_list_request("v1", "Endpoints").map_err(routing_state_error)?)
                .await
                .map_err(routing_state_error)?;
            let endpoint_slices = self
                .query
                .list_resources(
                    fresh_list_request("discovery.k8s.io/v1", "EndpointSlice")
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
                    fresh_list_request("networking.k8s.io/v1", "NetworkPolicy")
                        .map_err(routing_state_error)?,
                )
                .await
                .map_err(routing_state_error)?;
            let pods = self
                .query
                .list_resources(fresh_list_request("v1", "Pod").map_err(routing_state_error)?)
                .await
                .map_err(routing_state_error)?;
            let namespaces = self
                .query
                .list_resources(fresh_list_request("v1", "Namespace").map_err(routing_state_error)?)
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

    struct FakeQuery {
        requests: Mutex<Vec<(String, String, ResourceQueryConsistency)>>,
        service: klights_cluster_core::Resource,
    }

    impl LeaderResourceQuery for FakeQuery {
        fn list_resources(
            &self,
            request: ResourceListRequest,
        ) -> klights_leader_api::ResourceQueryFuture<'_, klights_leader_api::ResourceListResult>
        {
            Box::pin(async move {
                self.requests.lock().unwrap().push((
                    request.api_version().to_string(),
                    request.kind().to_string(),
                    request.consistency(),
                ));
                let items = if request.kind() == "Service" {
                    vec![self.service.clone()]
                } else {
                    Vec::new()
                };
                klights_leader_api::ResourceListResult::try_new(items, 91, None, None, None)
            })
        }

        fn get_resource(
            &self,
            _request: klights_leader_api::ResourceGetRequest,
        ) -> klights_leader_api::ResourceQueryFuture<'_, Option<klights_cluster_core::Resource>>
        {
            Box::pin(async { unreachable!("routing snapshots use LIST only") })
        }
    }

    #[tokio::test]
    async fn service_snapshot_preserves_identity_rv_status_and_arc_payload() {
        let data = Arc::new(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "namespace": "kube-system",
                "name": "dns",
                "uid": "uid-dns",
                "resourceVersion": "91"
            },
            "status": {"loadBalancer": {"ingress": [{"ip": "192.0.2.1"}]}}
        }));
        let query = Arc::new(FakeQuery {
            requests: Mutex::new(Vec::new()),
            service: klights_cluster_core::Resource {
                id: 1,
                api_version: "v1".to_string(),
                kind: "Service".to_string(),
                namespace: Some("kube-system".to_string()),
                name: "dns".to_string(),
                uid: "uid-dns".to_string(),
                resource_version: 91,
                data: data.clone(),
            },
        });
        let adapter = LeaderRoutingStateAdapter::new(query.clone());

        let snapshot = adapter.service_routing_snapshot().await.unwrap();
        let service = &snapshot.services[0];
        assert_eq!(service.api_version, "v1");
        assert_eq!(service.kind, "Service");
        assert_eq!(service.namespace.as_deref(), Some("kube-system"));
        assert_eq!(service.name, "dns");
        assert_eq!(service.resource_version, 91);
        assert_eq!(
            service.data.pointer("/status/loadBalancer/ingress/0/ip"),
            Some(&serde_json::json!("192.0.2.1"))
        );
        assert!(Arc::ptr_eq(&service.data, &data));
        assert!(
            query
                .requests
                .lock()
                .unwrap()
                .iter()
                .all(|(_, _, consistency)| *consistency == ResourceQueryConsistency::LeaderFresh)
        );
    }
}
