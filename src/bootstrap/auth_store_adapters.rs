//! Root-composed storage adapters for auth policy ports.

use async_trait::async_trait;
use std::sync::Arc;

const RBAC_API_VERSION: &str = "rbac.authorization.k8s.io/v1";

pub fn rbac_policy_store(
    db: crate::datastore::backend::DatastoreHandle,
) -> Arc<dyn crate::auth::rbac_policy_store::RbacPolicyStore> {
    Arc::new(
        crate::auth::rbac_policy_store::ResourceRbacPolicyStore::new(Arc::new(
            DatastoreRbacResourceReader::new(db),
        )),
    )
}

pub fn node_policy_store(
    pods: Arc<dyn crate::kubelet::pod_repository::PodReader>,
) -> Arc<dyn crate::auth::node_policy_store::NodePolicyStore> {
    Arc::new(
        crate::auth::node_policy_store::PodSourceNodePolicyStore::new(Arc::new(
            PodRepositoryNodePodSource::new(pods),
        )),
    )
}

pub struct DatastoreRbacResourceReader {
    db: crate::datastore::backend::DatastoreHandle,
}

impl DatastoreRbacResourceReader {
    pub fn new(db: crate::datastore::backend::DatastoreHandle) -> Self {
        Self { db }
    }
}

#[async_trait]
impl crate::auth::rbac_policy_store::RbacResourceReader for DatastoreRbacResourceReader {
    async fn list_rbac_resources(
        &self,
        kind: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<serde_json::Value>, String> {
        let list = self
            .db
            .list_resources_page(
                RBAC_API_VERSION,
                kind,
                namespace,
                None,
                None,
                crate::datastore::types::ListPageRequest::unbounded(),
            )
            .await
            .map_err(|error| error.to_string())?;
        Ok(list
            .items
            .into_iter()
            .map(|resource| (*resource.data).clone())
            .collect())
    }
}

pub struct PodRepositoryNodePodSource {
    pods: Arc<dyn crate::kubelet::pod_repository::PodReader>,
}

impl PodRepositoryNodePodSource {
    pub fn new(pods: Arc<dyn crate::kubelet::pod_repository::PodReader>) -> Self {
        Self { pods }
    }
}

fn policy_pod(pod: crate::datastore::Resource) -> crate::auth::node_policy_store::NodePolicyPod {
    crate::auth::node_policy_store::NodePolicyPod {
        namespace: pod.namespace.unwrap_or_default(),
        name: pod.name,
        data: pod.data,
    }
}

#[async_trait]
impl crate::auth::node_policy_store::NodePodSource for PodRepositoryNodePodSource {
    async fn get_policy_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<crate::auth::node_policy_store::NodePolicyPod>, String> {
        self.pods
            .get_pod(namespace, name)
            .await
            .map(|pod| pod.map(policy_pod))
            .map_err(|error| error.to_string())
    }

    async fn list_policy_pods(
        &self,
    ) -> Result<Vec<crate::auth::node_policy_store::NodePolicyPod>, String> {
        self.pods
            .list_pods(None, None, None, None, None)
            .await
            .map(|pods| pods.items.into_iter().map(policy_pod).collect())
            .map_err(|error| error.to_string())
    }
}
