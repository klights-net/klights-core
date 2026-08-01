use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use klights_cluster_core::Resource;
use klights_leader_api::{
    LeaderProjectedServiceAccountToken, LeaderResourceQuery, ResourceGetRequest,
    ResourceQueryConsistency,
};
use klights_types::ResourceKey;

pub use klights_leader_api::{ProjectedServiceAccountToken, ProjectedServiceAccountTokenRequest};

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
    resource_query: Arc<dyn LeaderResourceQuery>,
    projected_tokens: Arc<dyn LeaderProjectedServiceAccountToken>,
}

impl LocalCacheVolumeSourceReader {
    pub fn new(
        resource_query: Arc<dyn LeaderResourceQuery>,
        projected_tokens: Arc<dyn LeaderProjectedServiceAccountToken>,
    ) -> Self {
        Self {
            resource_query,
            projected_tokens,
        }
    }

    async fn get(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        self.resource_query
            .get_resource(ResourceGetRequest::try_new(
                ResourceKey {
                    api_version: api_version.to_string(),
                    kind: kind.to_string(),
                    namespace: namespace.map(str::to_string),
                    name: name.to_string(),
                },
                ResourceQueryConsistency::LeaderFresh,
            )?)
            .await
            .map_err(anyhow::Error::new)
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn empty_volume_source_reader_for_tests() -> Arc<dyn VolumeSourceReader> {
    Arc::new(EmptyVolumeSourceReader)
}

#[cfg(any(test, feature = "test-support"))]
struct EmptyVolumeSourceReader;

#[cfg(any(test, feature = "test-support"))]
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
        self.projected_tokens
            .issue_projected_service_account_token(request)
            .await
            .map_err(anyhow::Error::new)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use serde_json::json;

    use klights_cluster_core::Resource;
    use klights_leader_api::{
        LeaderProjectedServiceAccountToken, LeaderResourceQuery,
        ProjectedServiceAccountTokenFuture, ResourceGetRequest, ResourceListRequest,
        ResourceListResult, ResourceQueryConsistency, ResourceQueryError, ResourceQueryFuture,
    };

    use super::{
        LocalCacheVolumeSourceReader, ProjectedServiceAccountToken,
        ProjectedServiceAccountTokenRequest, VolumeSourceReader,
    };

    struct ExactGetLeaderResourceQuery {
        resource: Resource,
        get_calls: AtomicUsize,
        fresh_get_calls: AtomicUsize,
        list_calls: AtomicUsize,
    }

    impl ExactGetLeaderResourceQuery {
        fn new(resource: Resource) -> Self {
            Self {
                resource,
                get_calls: AtomicUsize::new(0),
                fresh_get_calls: AtomicUsize::new(0),
                list_calls: AtomicUsize::new(0),
            }
        }
    }

    impl LeaderResourceQuery for ExactGetLeaderResourceQuery {
        fn get_resource(
            &self,
            request: ResourceGetRequest,
        ) -> ResourceQueryFuture<'_, Option<Resource>> {
            Box::pin(async move {
                let consistency = request.consistency();
                let key = request.into_key();
                if consistency == ResourceQueryConsistency::Cached {
                    self.get_calls.fetch_add(1, Ordering::SeqCst);
                    return Err(ResourceQueryError::query_failed(format!(
                        "unexpected cached get_resource for {key:?}"
                    )));
                }
                self.fresh_get_calls.fetch_add(1, Ordering::SeqCst);
                Ok((key.api_version == self.resource.api_version
                    && key.kind == self.resource.kind
                    && key.namespace.as_deref() == self.resource.namespace.as_deref()
                    && key.name == self.resource.name)
                    .then(|| self.resource.clone()))
            })
        }

        fn list_resources(
            &self,
            _request: ResourceListRequest,
        ) -> ResourceQueryFuture<'_, ResourceListResult> {
            self.list_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { ResourceListResult::try_new(Vec::new(), 0, None, None, None) })
        }
    }

    #[derive(Default)]
    struct RecordingProjectedTokenIssuer {
        requests: Mutex<Vec<ProjectedServiceAccountTokenRequest>>,
    }

    impl RecordingProjectedTokenIssuer {
        fn requests(&self) -> Vec<ProjectedServiceAccountTokenRequest> {
            self.requests.lock().unwrap().clone()
        }
    }

    impl LeaderProjectedServiceAccountToken for RecordingProjectedTokenIssuer {
        fn issue_projected_service_account_token(
            &self,
            request: ProjectedServiceAccountTokenRequest,
        ) -> ProjectedServiceAccountTokenFuture<'_> {
            self.requests.lock().unwrap().push(request);
            Box::pin(async { ProjectedServiceAccountToken::try_new("focused-volume-token") })
        }
    }

    fn service_account_resource() -> Resource {
        Resource {
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
        }
    }

    #[tokio::test]
    async fn volume_reader_fetches_exact_namespaced_service_account_from_leader() {
        let query = Arc::new(ExactGetLeaderResourceQuery::new(service_account_resource()));
        let issuer = Arc::new(RecordingProjectedTokenIssuer::default());
        let reader = LocalCacheVolumeSourceReader::new(query.clone(), issuer);

        let found = reader
            .service_account("aggregator-test", "sample-apiserver")
            .await
            .expect("serviceaccount lookup should succeed");

        assert_eq!(
            found.as_ref().map(|resource| resource.uid.as_str()),
            Some("sa-uid-sample")
        );
        assert_eq!(query.fresh_get_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            query.get_calls.load(Ordering::SeqCst),
            0,
            "volume lookups must wait for the exact clusterdb client API response"
        );
        assert_eq!(
            query.list_calls.load(Ordering::SeqCst),
            0,
            "volume lookups must not rely on a stale primed list cache"
        );
    }

    #[tokio::test]
    async fn volume_reader_delegates_projected_token_to_focused_issuer() {
        let query = Arc::new(ExactGetLeaderResourceQuery::new(service_account_resource()));
        let issuer = Arc::new(RecordingProjectedTokenIssuer::default());
        let reader = LocalCacheVolumeSourceReader::new(query.clone(), issuer.clone());
        let request = ProjectedServiceAccountTokenRequest::try_new(
            "default",
            "default",
            vec!["api".to_string()],
            3_600,
            "web",
            "web-uid",
            "worker-1",
            None,
        )
        .unwrap();

        let token = reader
            .projected_service_account_token(request.clone())
            .await
            .expect("focused issuer should return a projected token");

        assert_eq!(token.token(), "focused-volume-token");
        assert_eq!(issuer.requests(), vec![request]);
        assert_eq!(query.fresh_get_calls.load(Ordering::SeqCst), 0);
        assert_eq!(query.get_calls.load(Ordering::SeqCst), 0);
        assert_eq!(query.list_calls.load(Ordering::SeqCst), 0);
    }
}
