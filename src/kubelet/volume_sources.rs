use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::control_plane::client::{LeaderApiClient, ResourceKey};
use crate::datastore::Resource;

pub use crate::control_plane::client::{
    ProjectedServiceAccountToken, ProjectedServiceAccountTokenRequest,
};

#[async_trait]
pub trait VolumeSourceReader: Send + Sync {
    async fn config_map(&self, namespace: &str, name: &str) -> Result<Option<Resource>>;
    async fn secret(&self, namespace: &str, name: &str) -> Result<Option<Resource>>;
    async fn service_account(&self, namespace: &str, name: &str) -> Result<Option<Resource>>;
    async fn pod(&self, namespace: &str, name: &str) -> Result<Option<Resource>>;
    async fn node(&self, name: &str) -> Result<Option<Resource>>;
    async fn persistent_volume_claim(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Resource>>;
    async fn persistent_volume(&self, name: &str) -> Result<Option<Resource>>;
    async fn projected_service_account_token(
        &self,
        request: ProjectedServiceAccountTokenRequest,
    ) -> Result<ProjectedServiceAccountToken> {
        let _ = request;
        anyhow::bail!("projected ServiceAccount token source is unavailable")
    }
}

pub struct LocalCacheVolumeSourceReader {
    cluster_api: Arc<dyn LeaderApiClient>,
}

impl LocalCacheVolumeSourceReader {
    pub fn new(cluster_api: Arc<dyn LeaderApiClient>) -> Self {
        Self { cluster_api }
    }

    async fn get(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        self.cluster_api
            .get_resource_fresh(ResourceKey {
                api_version: api_version.to_string(),
                kind: kind.to_string(),
                namespace: namespace.map(str::to_string),
                name: name.to_string(),
            })
            .await
    }
}

#[cfg(test)]
pub fn empty_volume_source_reader_for_tests() -> Arc<dyn VolumeSourceReader> {
    Arc::new(EmptyVolumeSourceReader)
}

#[cfg(test)]
struct EmptyVolumeSourceReader;

#[cfg(test)]
#[async_trait]
impl VolumeSourceReader for EmptyVolumeSourceReader {
    async fn config_map(&self, _namespace: &str, _name: &str) -> Result<Option<Resource>> {
        Ok(None)
    }

    async fn secret(&self, _namespace: &str, _name: &str) -> Result<Option<Resource>> {
        Ok(None)
    }

    async fn service_account(&self, _namespace: &str, _name: &str) -> Result<Option<Resource>> {
        Ok(None)
    }

    async fn pod(&self, _namespace: &str, _name: &str) -> Result<Option<Resource>> {
        Ok(None)
    }

    async fn node(&self, _name: &str) -> Result<Option<Resource>> {
        Ok(None)
    }

    async fn persistent_volume_claim(
        &self,
        _namespace: &str,
        _name: &str,
    ) -> Result<Option<Resource>> {
        Ok(None)
    }

    async fn persistent_volume(&self, _name: &str) -> Result<Option<Resource>> {
        Ok(None)
    }
}

#[async_trait]
impl VolumeSourceReader for LocalCacheVolumeSourceReader {
    async fn config_map(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.get("v1", "ConfigMap", Some(namespace), name).await
    }

    async fn secret(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.get("v1", "Secret", Some(namespace), name).await
    }

    async fn service_account(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.get("v1", "ServiceAccount", Some(namespace), name)
            .await
    }

    async fn pod(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.get("v1", "Pod", Some(namespace), name).await
    }

    async fn node(&self, name: &str) -> Result<Option<Resource>> {
        self.get("v1", "Node", None, name).await
    }

    async fn persistent_volume_claim(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Resource>> {
        self.get("v1", "PersistentVolumeClaim", Some(namespace), name)
            .await
    }

    async fn persistent_volume(&self, name: &str) -> Result<Option<Resource>> {
        self.get("v1", "PersistentVolume", None, name).await
    }

    async fn projected_service_account_token(
        &self,
        request: ProjectedServiceAccountTokenRequest,
    ) -> Result<ProjectedServiceAccountToken> {
        self.cluster_api
            .projected_service_account_token(request)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use anyhow::{Result, anyhow};
    use async_trait::async_trait;
    use bytes::Bytes;
    use serde_json::json;

    use crate::control_plane::client::{
        CacheScope, LeaderApiClient, ListRequest, ListResponse, Node, Pod, ResourceEvent,
        ResourceKey, Secret, WatchRequest, WatchStream,
    };
    use crate::datastore::{NodeSubnet, Resource, ResourceList};
    use crate::kubelet::outbox::payload::OutboxOperation;
    use crate::kubelet::outbox::{OutboxApplyError, OutboxApplyResult};
    use crate::networking::wireguard::DataplanePeerMetadata;

    use super::{LocalCacheVolumeSourceReader, VolumeSourceReader};

    struct ExactGetLeaderApiClient {
        resource: Resource,
        get_calls: AtomicUsize,
        fresh_get_calls: AtomicUsize,
        list_calls: AtomicUsize,
    }

    impl ExactGetLeaderApiClient {
        fn new(resource: Resource) -> Self {
            Self {
                resource,
                get_calls: AtomicUsize::new(0),
                fresh_get_calls: AtomicUsize::new(0),
                list_calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl LeaderApiClient for ExactGetLeaderApiClient {
        async fn get_resource(&self, key: ResourceKey) -> Result<Option<Resource>> {
            self.get_calls.fetch_add(1, Ordering::SeqCst);
            Err(anyhow!("unexpected cached get_resource for {key:?}"))
        }

        async fn get_resource_fresh(&self, key: ResourceKey) -> Result<Option<Resource>> {
            self.fresh_get_calls.fetch_add(1, Ordering::SeqCst);
            Ok((key.api_version == self.resource.api_version
                && key.kind == self.resource.kind
                && key.namespace.as_deref() == self.resource.namespace.as_deref()
                && key.name == self.resource.name)
                .then(|| self.resource.clone()))
        }

        async fn list_resources(&self, _req: ListRequest) -> Result<ListResponse> {
            self.list_calls.fetch_add(1, Ordering::SeqCst);
            Ok(ResourceList {
                items: Vec::new(),
                resource_version: 0,
                continue_token: None,
                remaining_item_count: None,
            })
        }

        async fn watch_resources(&self, req: WatchRequest) -> Result<WatchStream<ResourceEvent>> {
            Err(anyhow!("unexpected watch_resources for {req:?}"))
        }

        async fn wait_cache_ready(&self, scope: CacheScope) -> Result<()> {
            Err(anyhow!("unexpected wait_cache_ready for {scope:?}"))
        }

        async fn get_pod(&self, ns: &str, name: &str) -> Result<Option<Pod>> {
            Err(anyhow!("unexpected get_pod for {ns}/{name}"))
        }

        async fn get_pod_for_uid(&self, ns: &str, name: &str, uid: &str) -> Result<Option<Pod>> {
            Err(anyhow!("unexpected get_pod_for_uid for {ns}/{name}/{uid}"))
        }

        async fn watch_pods_on_node(&self, node_name: &str) -> Result<WatchStream<Pod>> {
            Err(anyhow!("unexpected watch_pods_on_node for {node_name}"))
        }

        async fn list_pods_on_node(&self, node_name: &str) -> Result<Vec<Pod>> {
            Err(anyhow!("unexpected list_pods_on_node for {node_name}"))
        }

        async fn get_configmap(&self, ns: &str, name: &str) -> Result<Option<Resource>> {
            Err(anyhow!("unexpected get_configmap for {ns}/{name}"))
        }

        async fn get_secret(&self, ns: &str, name: &str) -> Result<Option<Secret>> {
            Err(anyhow!("unexpected get_secret for {ns}/{name}"))
        }

        async fn get_node(&self, name: &str) -> Result<Node> {
            Err(anyhow!("unexpected get_node for {name}"))
        }

        async fn watch_node(&self, name: &str) -> Result<WatchStream<Node>> {
            Err(anyhow!("unexpected watch_node for {name}"))
        }

        async fn allocate_node_subnet(
            &self,
            node_name: &str,
            _cluster_cidr: &str,
            _node_ip: &str,
        ) -> Result<NodeSubnet> {
            Err(anyhow!("unexpected allocate_node_subnet for {node_name}"))
        }

        async fn get_node_subnet(&self, node_name: &str) -> Result<Option<NodeSubnet>> {
            Err(anyhow!("unexpected get_node_subnet for {node_name}"))
        }

        async fn list_peer_subnets(&self, my_node_name: &str) -> Result<Vec<NodeSubnet>> {
            Err(anyhow!("unexpected list_peer_subnets for {my_node_name}"))
        }

        async fn get_node_dataplane(
            &self,
            node_name: &str,
        ) -> Result<Option<DataplanePeerMetadata>> {
            Err(anyhow!("unexpected get_node_dataplane for {node_name}"))
        }

        async fn list_pod_cleanup_intents_for_node(
            &self,
            node_name: &str,
        ) -> Result<Vec<crate::datastore::PodCleanupIntent>> {
            Err(anyhow!(
                "unexpected list_pod_cleanup_intents_for_node for {node_name}"
            ))
        }

        async fn delete_pod_cleanup_intent(
            &self,
            node_name: &str,
            namespace: &str,
            pod_name: &str,
            pod_uid: &str,
            reason: &str,
        ) -> Result<()> {
            Err(anyhow!(
                "unexpected delete_pod_cleanup_intent for {node_name}/{namespace}/{pod_name}/{pod_uid}/{reason}"
            ))
        }

        async fn apply_outbox(
            &self,
            idempotency_key: &str,
            _operation: OutboxOperation,
            _payload: Bytes,
        ) -> std::result::Result<OutboxApplyResult, OutboxApplyError> {
            Err(OutboxApplyError::Retryable(format!(
                "unexpected apply_outbox for {idempotency_key}"
            )))
        }
    }

    #[tokio::test]
    async fn volume_reader_fetches_exact_namespaced_service_account_from_leader() {
        let service_account = Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "ServiceAccount".to_string(),
            namespace: Some("aggregator-test".to_string()),
            name: "sample-apiserver".to_string(),
            uid: "sa-uid-sample".to_string(),
            resource_version: 7,
            data: json!({
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": {
                    "namespace": "aggregator-test",
                    "name": "sample-apiserver",
                    "uid": "sa-uid-sample",
                    "resourceVersion": "7"
                }
            })
            .into(),
        };
        let client = Arc::new(ExactGetLeaderApiClient::new(service_account));
        let reader = LocalCacheVolumeSourceReader::new(client.clone());

        let found = reader
            .service_account("aggregator-test", "sample-apiserver")
            .await
            .expect("serviceaccount lookup should succeed");

        assert_eq!(
            found.as_ref().map(|resource| resource.uid.as_str()),
            Some("sa-uid-sample")
        );
        assert_eq!(client.fresh_get_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            client.get_calls.load(Ordering::SeqCst),
            0,
            "volume lookups must wait for the exact clusterdb client API response"
        );
        assert_eq!(
            client.list_calls.load(Ordering::SeqCst),
            0,
            "volume lookups must not rely on a stale primed list cache"
        );
    }
}
