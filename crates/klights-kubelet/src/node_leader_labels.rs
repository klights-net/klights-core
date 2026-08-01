use anyhow::Result;
use klights_cluster_core::{Resource, ResourcePreconditions};
use std::sync::Arc;

#[async_trait::async_trait]
pub trait NodeLeaderLabelStore: Send + Sync {
    async fn list_nodes(&self) -> Result<Vec<Resource>>;

    async fn update_node_with_preconditions(
        &self,
        name: &str,
        data: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource>;
}

/// Remove stale `node-role.kubernetes.io/leader` labels from every node
/// except the current local node. The local leader election is responsible
/// for stamping its own leader label; this keeps old leader labels from a
/// previous leader visible only until the leader changes and the new leader
/// has observed that transition.
pub async fn clear_leader_label_from_other_nodes(
    store: &dyn NodeLeaderLabelStore,
    local_node_name: &str,
) -> Result<()> {
    for node in store.list_nodes().await? {
        if node.name == local_node_name {
            continue;
        }
        let mut data = Arc::unwrap_or_clone(node.data.clone());
        let Some(labels) = data
            .pointer_mut("/metadata/labels")
            .and_then(|labels| labels.as_object_mut())
        else {
            continue;
        };
        if labels.remove("node-role.kubernetes.io/leader").is_none() {
            continue;
        }
        if let Err(err) = store
            .update_node_with_preconditions(
                &node.name,
                data,
                ResourcePreconditions::from_resource(&node),
            )
            .await
        {
            tracing::warn!(
                error = %err,
                node_name = %node.name,
                local_node_name,
                "failed to clear stale node leader label"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestNodeLeaderLabelStore {
        nodes: tokio::sync::Mutex<Vec<Resource>>,
    }

    #[async_trait::async_trait]
    impl NodeLeaderLabelStore for TestNodeLeaderLabelStore {
        async fn list_nodes(&self) -> Result<Vec<Resource>> {
            Ok(self.nodes.lock().await.clone())
        }

        async fn update_node_with_preconditions(
            &self,
            name: &str,
            data: serde_json::Value,
            _preconditions: ResourcePreconditions,
        ) -> Result<Resource> {
            let mut nodes = self.nodes.lock().await;
            let node = nodes
                .iter_mut()
                .find(|node| node.name == name)
                .ok_or_else(|| anyhow::anyhow!("missing test Node {name}"))?;
            node.data = Arc::new(data);
            Ok(node.clone())
        }
    }

    fn node_with_labels(id: i64, name: &str, labels: serde_json::Value) -> Resource {
        Resource {
            id,
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: name.to_string(),
            uid: format!("uid-{name}"),
            resource_version: id,
            data: Arc::new(serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": name,
                    "labels": labels,
                }
            })),
        }
    }

    #[tokio::test]
    async fn clears_stale_leader_labels_except_local_node() {
        let store = TestNodeLeaderLabelStore {
            nodes: tokio::sync::Mutex::new(vec![
                node_with_labels(
                    1,
                    "local",
                    serde_json::json!({"node-role.kubernetes.io/leader": ""}),
                ),
                node_with_labels(
                    2,
                    "old-leader",
                    serde_json::json!({
                        "node-role.kubernetes.io/leader": "",
                        "kubernetes.io/os": "linux",
                    }),
                ),
            ]),
        };

        clear_leader_label_from_other_nodes(&store, "local")
            .await
            .expect("clear stale leader labels");

        let nodes = store.nodes.lock().await;
        let local = nodes.iter().find(|node| node.name == "local").unwrap();
        assert!(
            local
                .data
                .pointer("/metadata/labels/node-role.kubernetes.io~1leader")
                .is_some()
        );

        let old = nodes.iter().find(|node| node.name == "old-leader").unwrap();
        let labels = old
            .data
            .pointer("/metadata/labels")
            .and_then(|value| value.as_object())
            .expect("old labels");
        assert!(!labels.contains_key("node-role.kubernetes.io/leader"));
        assert_eq!(
            labels
                .get("kubernetes.io/os")
                .and_then(|value| value.as_str()),
            Some("linux")
        );
    }
}
