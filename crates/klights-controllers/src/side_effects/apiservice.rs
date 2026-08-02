//! Side effect to enqueue APIService availability reconciliation.

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;
use klights_reconcile_api::ReconcileKey;
use serde_json::Value;

#[async_trait]
pub trait ApiServiceSideEffectStore: Send + Sync {
    async fn list_apiservices(&self) -> Result<Vec<Resource>>;
}

pub async fn apiservice_reconcile_keys_for_resource<Store: ApiServiceSideEffectStore + ?Sized>(
    resource: &Value,
    store: &Store,
) -> Result<Vec<ReconcileKey>> {
    let api_version = resource
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let kind = resource.get("kind").and_then(|v| v.as_str()).unwrap_or("");
    if (api_version, kind) == ("apiregistration.k8s.io/v1", "APIService") {
        let Some(name) = resource.pointer("/metadata/name").and_then(|v| v.as_str()) else {
            return Ok(Vec::new());
        };
        return Ok(vec![ReconcileKey::cluster(
            "apiregistration.k8s.io/v1",
            "APIService",
            name,
        )]);
    }

    if !matches!(
        (api_version, kind),
        ("v1", "Service") | ("v1", "Endpoints") | ("discovery.k8s.io/v1", "EndpointSlice")
    ) {
        return Ok(Vec::new());
    }
    let namespace = resource
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .unwrap_or("default");
    let name = if (api_version, kind) == ("discovery.k8s.io/v1", "EndpointSlice") {
        resource
            .pointer("/metadata/labels/kubernetes.io~1service-name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
    } else {
        resource
            .pointer("/metadata/name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
    };
    if name.is_empty() {
        return Ok(Vec::new());
    }

    let apiservices = store.list_apiservices().await?;
    Ok(apiservices
        .into_iter()
        .filter(|apiservice| apiservice_targets_service(&apiservice.data, namespace, name))
        .map(|apiservice| {
            ReconcileKey::cluster("apiregistration.k8s.io/v1", "APIService", &apiservice.name)
        })
        .collect())
}

fn apiservice_targets_service(apiservice: &Value, namespace: &str, name: &str) -> bool {
    apiservice.pointer("/spec/service").is_some_and(|service| {
        service.get("name").and_then(|v| v.as_str()) == Some(name)
            && service
                .get("namespace")
                .and_then(|v| v.as_str())
                .unwrap_or("default")
                == namespace
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Arc;

    struct FakeApiServiceStore {
        apiservices: Vec<Resource>,
    }

    #[async_trait]
    impl ApiServiceSideEffectStore for FakeApiServiceStore {
        async fn list_apiservices(&self) -> Result<Vec<Resource>> {
            Ok(self.apiservices.clone())
        }
    }

    fn fixture() -> FakeApiServiceStore {
        FakeApiServiceStore {
            apiservices: [
                (
                    "v1alpha1.ready.example.com",
                    "ready.example.com",
                    "ready-service",
                ),
                (
                    "v1alpha1.other.example.com",
                    "other.example.com",
                    "other-service",
                ),
            ]
            .into_iter()
            .map(|(name, group, service)| {
                Resource::try_from_data(Arc::new(json!({
                    "apiVersion": "apiregistration.k8s.io/v1",
                    "kind": "APIService",
                    "metadata": {"name": name},
                    "spec": {
                        "group": group,
                        "version": "v1alpha1",
                        "service": {"namespace": "default", "name": service}
                    }
                })))
                .unwrap()
            })
            .collect(),
        }
    }

    #[tokio::test]
    async fn service_mutation_enqueues_matching_apiservice_only() {
        let store = fixture();

        let keys = apiservice_reconcile_keys_for_resource(
            &json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"namespace": "default", "name": "ready-service"}
            }),
            &store,
        )
        .await
        .unwrap();

        assert_eq!(
            keys,
            vec![ReconcileKey::cluster(
                "apiregistration.k8s.io/v1",
                "APIService",
                "v1alpha1.ready.example.com"
            )]
        );
    }

    #[tokio::test]
    async fn endpointslice_mutation_enqueues_matching_apiservice_only() {
        let store = fixture();

        let keys = apiservice_reconcile_keys_for_resource(
            &json!({
                "apiVersion": "discovery.k8s.io/v1",
                "kind": "EndpointSlice",
                "metadata": {
                    "name": "ready-service-abc",
                    "namespace": "default",
                    "labels": {"kubernetes.io/service-name": "ready-service"}
                }
            }),
            &store,
        )
        .await
        .unwrap();

        assert_eq!(
            keys,
            vec![ReconcileKey::cluster(
                "apiregistration.k8s.io/v1",
                "APIService",
                "v1alpha1.ready.example.com"
            )]
        );
    }
}
