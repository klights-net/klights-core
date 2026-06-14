//! Node policy store trait and in-memory mock for Node authorizer.
//!
//! Production uses the datastore; tests use `InMemoryNodePolicyStore`.

use async_trait::async_trait;
use std::collections::{HashMap, HashSet};

/// Object-safe trait for node-scoped access decisions.
///
/// Returns the data needed by `NodeAccessPolicy` without embedding
/// authorization logic. Narrow and mockable by design.
#[async_trait]
pub trait NodePolicyStore: Send + Sync {
    /// Get the node name for a given pod, or None if the pod is not scheduled.
    async fn get_pod_node(&self, namespace: &str, name: &str) -> Option<String>;

    /// List all pods scheduled to a node, returning (namespace, name) pairs.
    async fn list_pods_on_node(&self, node_name: &str) -> Vec<(String, String)>;

    /// Get names of objects of `resource` kind referenced by a pod
    /// (e.g., secret names from volumes, envFrom, imagePullSecrets).
    async fn get_pod_referenced_objects(
        &self,
        namespace: &str,
        pod_name: &str,
        resource: &str,
    ) -> Vec<String>;
}

// --- In-memory implementation for tests ---

/// In-memory `NodePolicyStore` for unit tests.
///
/// Stores pod-to-node mappings and pod-to-referenced-object mappings.
/// Does not implement any authorization logic.
pub struct InMemoryNodePolicyStore {
    /// (namespace, pod_name) -> node_name
    pod_node: HashMap<(String, String), String>,
    /// (namespace, pod_name, resource_kind) -> object_names
    references: HashMap<(String, String, String), Vec<String>>,
}

impl InMemoryNodePolicyStore {
    pub fn new() -> Self {
        Self {
            pod_node: HashMap::new(),
            references: HashMap::new(),
        }
    }

    /// Schedule a pod on a node.
    pub fn add_pod(&mut self, namespace: &str, name: &str, node_name: &str) {
        self.pod_node.insert(
            (namespace.to_string(), name.to_string()),
            node_name.to_string(),
        );
    }

    /// Record that a pod references certain objects of a given resource kind.
    pub fn add_reference(
        &mut self,
        namespace: &str,
        pod_name: &str,
        resource_kind: &str,
        object_names: Vec<String>,
    ) {
        self.references.insert(
            (
                namespace.to_string(),
                pod_name.to_string(),
                resource_kind.to_string(),
            ),
            object_names,
        );
    }
}

impl Default for InMemoryNodePolicyStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl NodePolicyStore for InMemoryNodePolicyStore {
    async fn get_pod_node(&self, namespace: &str, name: &str) -> Option<String> {
        self.pod_node
            .get(&(namespace.to_string(), name.to_string()))
            .cloned()
    }

    async fn list_pods_on_node(&self, node_name: &str) -> Vec<(String, String)> {
        self.pod_node
            .iter()
            .filter(|(_, n)| *n == node_name)
            .map(|((ns, name), _)| (ns.clone(), name.clone()))
            .collect()
    }

    async fn get_pod_referenced_objects(
        &self,
        namespace: &str,
        pod_name: &str,
        resource: &str,
    ) -> Vec<String> {
        self.references
            .get(&(
                namespace.to_string(),
                pod_name.to_string(),
                resource.to_string(),
            ))
            .cloned()
            .unwrap_or_default()
    }
}

/// Production `NodePolicyStore` backed by the pod repository.
///
/// Queries pod resources to determine node assignments and referenced objects.
/// Fail-closed: errors produce empty results, which the NodeAuthorizer treats
/// as "no opinion" (defer to RBAC).
pub struct DatastoreNodePolicyStore {
    pods: std::sync::Arc<dyn crate::kubelet::pod_repository::PodReader>,
}

impl DatastoreNodePolicyStore {
    pub fn new(pods: std::sync::Arc<dyn crate::kubelet::pod_repository::PodReader>) -> Self {
        Self { pods }
    }
}

#[async_trait]
impl NodePolicyStore for DatastoreNodePolicyStore {
    async fn get_pod_node(&self, namespace: &str, name: &str) -> Option<String> {
        let pod = self.pods.get_pod(namespace, name).await.ok().flatten()?;
        pod.data
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    }

    async fn list_pods_on_node(&self, node_name: &str) -> Vec<(String, String)> {
        let Ok(pods) = self.pods.list_pods(None, None, None, None, None).await else {
            return Vec::new();
        };
        let mut result = Vec::new();
        for pod in &pods.items {
            let pod_node = pod.data.pointer("/spec/nodeName").and_then(|v| v.as_str());
            if pod_node == Some(node_name) {
                let ns = pod.namespace.clone().unwrap_or_default();
                result.push((ns, pod.name.clone()));
            }
        }
        result
    }

    async fn get_pod_referenced_objects(
        &self,
        namespace: &str,
        pod_name: &str,
        resource: &str,
    ) -> Vec<String> {
        let Ok(Some(pod)) = self.pods.get_pod(namespace, pod_name).await else {
            return Vec::new();
        };
        extract_referenced_objects(&pod.data, resource)
    }
}

/// Extract names of referenced objects from a pod spec for a given resource kind.
fn extract_referenced_objects(pod: &serde_json::Value, resource: &str) -> Vec<String> {
    let mut names = HashSet::new();

    match resource {
        "secrets" => {
            // volumes with secret
            if let Some(volumes) = pod.pointer("/spec/volumes").and_then(|v| v.as_array()) {
                for vol in volumes {
                    if let Some(secret) = vol.get("secret")
                        && let Some(name) = secret.get("secretName").and_then(|n| n.as_str())
                    {
                        names.insert(name.to_string());
                    }
                }
            }
            // envFrom with secretRef
            extract_env_from_refs(pod, "secretRef", &mut names);
            // imagePullSecrets
            if let Some(pull_secrets) = pod
                .pointer("/spec/imagePullSecrets")
                .and_then(|v| v.as_array())
            {
                for ps in pull_secrets {
                    if let Some(name) = ps.get("name").and_then(|n| n.as_str()) {
                        names.insert(name.to_string());
                    }
                }
            }
        }
        "configmaps" => {
            // volumes with configMap
            if let Some(volumes) = pod.pointer("/spec/volumes").and_then(|v| v.as_array()) {
                for vol in volumes {
                    if let Some(cm) = vol.get("configMap")
                        && let Some(name) = cm.get("name").and_then(|n| n.as_str())
                    {
                        names.insert(name.to_string());
                    }
                }
            }
            // envFrom with configMapRef
            extract_env_from_refs(pod, "configMapRef", &mut names);
        }
        "persistentvolumeclaims" => {
            if let Some(volumes) = pod.pointer("/spec/volumes").and_then(|v| v.as_array()) {
                for vol in volumes {
                    if let Some(pvc) = vol.get("persistentVolumeClaim")
                        && let Some(name) = pvc.get("claimName").and_then(|n| n.as_str())
                    {
                        names.insert(name.to_string());
                    }
                }
            }
        }
        "serviceaccounts" => {
            if let Some(name) = pod
                .pointer("/spec/serviceAccountName")
                .and_then(|v| v.as_str())
            {
                names.insert(name.to_string());
            }
        }
        _ => {}
    }

    names.into_iter().collect()
}

/// Extract envFrom secretRef or configMapRef references from all containers.
fn extract_env_from_refs(pod: &serde_json::Value, ref_key: &str, names: &mut HashSet<String>) {
    for container_path in &["/spec/containers", "/spec/initContainers"] {
        if let Some(containers) = pod.pointer(container_path).and_then(|v| v.as_array()) {
            for container in containers {
                if let Some(env_from) = container.get("envFrom").and_then(|v| v.as_array()) {
                    for ef in env_from {
                        if let Some(name) = ef
                            .get(ref_key)
                            .and_then(|r| r.get("name"))
                            .and_then(|n| n.as_str())
                        {
                            names.insert(name.to_string());
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pod_node_mapping_basics() {
        let mut store = InMemoryNodePolicyStore::new();
        store.add_pod("default", "pod-a", "tokyo");
        store.add_pod("default", "pod-b", "osaka");

        assert_eq!(
            store.get_pod_node("default", "pod-a").await,
            Some("tokyo".to_string())
        );
        assert_eq!(
            store.get_pod_node("default", "pod-b").await,
            Some("osaka".to_string())
        );
        assert_eq!(store.get_pod_node("default", "pod-c").await, None);
    }

    #[tokio::test]
    async fn list_pods_on_node_filters_correctly() {
        let mut store = InMemoryNodePolicyStore::new();
        store.add_pod("default", "pod-a", "tokyo");
        store.add_pod("kube-system", "coredns", "tokyo");
        store.add_pod("default", "pod-b", "osaka");

        let tokyo_pods = store.list_pods_on_node("tokyo").await;
        assert_eq!(tokyo_pods.len(), 2);
        assert!(tokyo_pods.contains(&("default".to_string(), "pod-a".to_string())));
        assert!(tokyo_pods.contains(&("kube-system".to_string(), "coredns".to_string())));

        let osaka_pods = store.list_pods_on_node("osaka").await;
        assert_eq!(osaka_pods.len(), 1);
        assert!(osaka_pods.contains(&("default".to_string(), "pod-b".to_string())));
    }

    #[tokio::test]
    async fn empty_store_returns_empty() {
        let store = InMemoryNodePolicyStore::new();
        assert!(store.get_pod_node("default", "any").await.is_none());
        assert!(store.list_pods_on_node("tokyo").await.is_empty());
        assert!(
            store
                .get_pod_referenced_objects("default", "any", "secrets")
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn referenced_objects_returns_registered() {
        let mut store = InMemoryNodePolicyStore::new();
        store.add_reference(
            "default",
            "pod-a",
            "secrets",
            vec!["my-secret".to_string(), "other-secret".to_string()],
        );
        store.add_reference(
            "default",
            "pod-a",
            "configmaps",
            vec!["my-config".to_string()],
        );

        let secrets = store
            .get_pod_referenced_objects("default", "pod-a", "secrets")
            .await;
        assert_eq!(secrets.len(), 2);
        assert!(secrets.contains(&"my-secret".to_string()));

        let configmaps = store
            .get_pod_referenced_objects("default", "pod-a", "configmaps")
            .await;
        assert_eq!(configmaps.len(), 1);

        let empty = store
            .get_pod_referenced_objects("default", "pod-a", "pvc")
            .await;
        assert!(empty.is_empty());
    }

    // --- DatastoreNodePolicyStore tests ---

    async fn setup_datastore_with_pods()
    -> std::sync::Arc<dyn crate::kubelet::pod_repository::PodReader> {
        let db = crate::datastore::test_support::in_memory().await;

        // Create namespaces
        db.create_resource(
            "v1",
            "Namespace",
            None,
            "default",
            serde_json::json!({"metadata": {"name": "default"}}),
        )
        .await
        .unwrap();
        db.create_resource(
            "v1",
            "Namespace",
            None,
            "kube-system",
            serde_json::json!({"metadata": {"name": "kube-system"}}),
        )
        .await
        .unwrap();

        // Create pods on different nodes
        let pod_a = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "pod-a", "namespace": "default"},
            "spec": {
                "nodeName": "tokyo",
                "serviceAccountName": "default-sa",
                "containers": [{
                    "name": "app",
                    "image": "nginx",
                    "envFrom": [
                        {"configMapRef": {"name": "app-config"}},
                        {"secretRef": {"name": "app-secret"}}
                    ]
                }],
                "volumes": [
                    {"name": "config", "configMap": {"name": "vol-config"}},
                    {"name": "secret", "secret": {"secretName": "vol-secret"}},
                    {"name": "data", "persistentVolumeClaim": {"claimName": "data-pvc"}}
                ],
                "imagePullSecrets": [{"name": "registry-secret"}]
            }
        });
        db.create_resource("v1", "Pod", Some("default"), "pod-a", pod_a)
            .await
            .unwrap();

        let pod_b = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "coredns", "namespace": "kube-system"},
            "spec": {
                "nodeName": "tokyo",
                "containers": [{"name": "coredns", "image": "coredns"}]
            }
        });
        db.create_resource("v1", "Pod", Some("kube-system"), "coredns", pod_b)
            .await
            .unwrap();

        let pod_c = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "pod-c", "namespace": "default"},
            "spec": {
                "nodeName": "osaka",
                "containers": [{"name": "app", "image": "nginx"}]
            }
        });
        db.create_resource("v1", "Pod", Some("default"), "pod-c", pod_c)
            .await
            .unwrap();

        let db_handle: std::sync::Arc<dyn crate::datastore::backend::DatastoreBackend> =
            std::sync::Arc::new(db);
        let ts = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let metrics = crate::side_effects::SideEffectMetrics::new();
        let side_effects = std::sync::Arc::new(crate::side_effects::default_registry(
            metrics.clone(),
            None,
            None,
            None,
        ));
        let pod_repo = std::sync::Arc::new(crate::kubelet::pod_repository::PodRepository::new(
            db_handle,
            ts,
            side_effects,
            metrics,
        ));
        pod_repo as std::sync::Arc<dyn crate::kubelet::pod_repository::PodReader>
    }

    #[tokio::test]
    async fn datastore_get_pod_node_returns_correct_node() {
        let db = setup_datastore_with_pods().await;
        let store = DatastoreNodePolicyStore::new(db);

        assert_eq!(
            store.get_pod_node("default", "pod-a").await,
            Some("tokyo".to_string())
        );
        assert_eq!(
            store.get_pod_node("default", "pod-c").await,
            Some("osaka".to_string())
        );
    }

    #[tokio::test]
    async fn datastore_get_pod_node_returns_none_for_nonexistent() {
        let db = setup_datastore_with_pods().await;
        let store = DatastoreNodePolicyStore::new(db);

        assert_eq!(store.get_pod_node("default", "ghost").await, None);
    }

    #[tokio::test]
    async fn datastore_list_pods_on_node_returns_correct_pods() {
        let db = setup_datastore_with_pods().await;
        let store = DatastoreNodePolicyStore::new(db);

        let tokyo_pods = store.list_pods_on_node("tokyo").await;
        assert_eq!(tokyo_pods.len(), 2);
        assert!(tokyo_pods.contains(&("default".to_string(), "pod-a".to_string())));
        assert!(tokyo_pods.contains(&("kube-system".to_string(), "coredns".to_string())));

        let osaka_pods = store.list_pods_on_node("osaka").await;
        assert_eq!(osaka_pods.len(), 1);
        assert!(osaka_pods.contains(&("default".to_string(), "pod-c".to_string())));

        let empty = store.list_pods_on_node("nagoya").await;
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn datastore_referenced_secrets_extracted_correctly() {
        let db = setup_datastore_with_pods().await;
        let store = DatastoreNodePolicyStore::new(db);

        let secrets = store
            .get_pod_referenced_objects("default", "pod-a", "secrets")
            .await;
        assert_eq!(secrets.len(), 3); // envFrom secretRef + volume secret + imagePullSecret
        assert!(secrets.contains(&"app-secret".to_string()));
        assert!(secrets.contains(&"vol-secret".to_string()));
        assert!(secrets.contains(&"registry-secret".to_string()));
    }

    #[tokio::test]
    async fn datastore_referenced_configmaps_extracted_correctly() {
        let db = setup_datastore_with_pods().await;
        let store = DatastoreNodePolicyStore::new(db);

        let cms = store
            .get_pod_referenced_objects("default", "pod-a", "configmaps")
            .await;
        assert_eq!(cms.len(), 2); // envFrom configMapRef + volume configMap
        assert!(cms.contains(&"app-config".to_string()));
        assert!(cms.contains(&"vol-config".to_string()));
    }

    #[tokio::test]
    async fn datastore_referenced_pvc_extracted_correctly() {
        let db = setup_datastore_with_pods().await;
        let store = DatastoreNodePolicyStore::new(db);

        let pvcs = store
            .get_pod_referenced_objects("default", "pod-a", "persistentvolumeclaims")
            .await;
        assert_eq!(pvcs.len(), 1);
        assert!(pvcs.contains(&"data-pvc".to_string()));
    }

    #[tokio::test]
    async fn datastore_referenced_service_account_extracted() {
        let db = setup_datastore_with_pods().await;
        let store = DatastoreNodePolicyStore::new(db);

        let sas = store
            .get_pod_referenced_objects("default", "pod-a", "serviceaccounts")
            .await;
        assert_eq!(sas.len(), 1);
        assert!(sas.contains(&"default-sa".to_string()));
    }

    #[tokio::test]
    async fn datastore_no_references_returns_empty() {
        let db = setup_datastore_with_pods().await;
        let store = DatastoreNodePolicyStore::new(db);

        let refs = store
            .get_pod_referenced_objects("kube-system", "coredns", "secrets")
            .await;
        assert!(refs.is_empty());
    }

    // --- extract_referenced_objects unit tests ---

    #[test]
    fn extract_referenced_objects_ignores_unknown_resource() {
        let pod = serde_json::json!({"spec": {"nodeName": "x"}});
        assert!(extract_referenced_objects(&pod, "unknown").is_empty());
    }
}
