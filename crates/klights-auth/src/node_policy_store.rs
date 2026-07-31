//! Focused node-policy store contract and in-memory policy fake.
//!
//! Concrete Pod repository adapters are composed by the root bootstrap owner.

use async_trait::async_trait;
#[cfg(test)]
use std::collections::HashMap;

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

/// In-memory `NodePolicyStore` for unit tests.
///
/// Stores pod-to-node mappings and pod-to-referenced-object mappings.
/// Does not implement any authorization logic.
#[cfg(test)]
pub(crate) struct InMemoryNodePolicyStore {
    /// (namespace, pod_name) -> node_name
    pod_node: HashMap<(String, String), String>,
    /// (namespace, pod_name, resource_kind) -> object_names
    references: HashMap<(String, String, String), Vec<String>>,
}

#[cfg(test)]
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

#[cfg(test)]
impl Default for InMemoryNodePolicyStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
#[cfg(test)]
impl NodePolicyStore for InMemoryNodePolicyStore {
    async fn get_pod_node(&self, namespace: &str, name: &str) -> Option<String> {
        self.pod_node
            .get(&(namespace.to_string(), name.to_string()))
            .cloned()
    }

    async fn list_pods_on_node(&self, node_name: &str) -> Vec<(String, String)> {
        self.pod_node
            .iter()
            .filter(|(_, node)| *node == node_name)
            .map(|((namespace, name), _)| (namespace.clone(), name.clone()))
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
}
