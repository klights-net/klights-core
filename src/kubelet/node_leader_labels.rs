use crate::datastore::{Resource, ResourceList, ResourcePreconditions};
use anyhow::Result;
use std::sync::Arc;

#[async_trait::async_trait]
pub(crate) trait NodeLeaderLabelStore: Send + Sync {
    async fn list_nodes(&self) -> Result<ResourceList>;

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
pub(crate) async fn clear_leader_label_from_other_nodes(
    store: &dyn NodeLeaderLabelStore,
    local_node_name: &str,
) -> Result<()> {
    let nodes = store.list_nodes().await?;
    for node in nodes.items {
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
        db: crate::datastore::sqlite::Datastore,
    }

    #[async_trait::async_trait]
    impl NodeLeaderLabelStore for TestNodeLeaderLabelStore {
        async fn list_nodes(&self) -> Result<ResourceList> {
            self.db
                .list_resources(
                    "v1",
                    "Node",
                    None,
                    crate::datastore::ResourceListQuery::all(),
                )
                .await
        }

        async fn update_node_with_preconditions(
            &self,
            name: &str,
            data: serde_json::Value,
            preconditions: ResourcePreconditions,
        ) -> Result<Resource> {
            self.db
                .update_resource_with_preconditions("v1", "Node", None, name, data, preconditions)
                .await
        }
    }

    async fn create_node_with_labels(
        db: &crate::datastore::sqlite::Datastore,
        name: &str,
        labels: serde_json::Value,
    ) {
        db.create_resource(
            "v1",
            "Node",
            None,
            name,
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": name,
                    "labels": labels,
                }
            }),
        )
        .await
        .expect("create node");
    }

    #[tokio::test]
    async fn clears_stale_leader_labels_except_local_node() {
        let db = crate::datastore::test_support::in_memory().await;
        let store = TestNodeLeaderLabelStore { db: db.clone() };
        create_node_with_labels(
            &db,
            "local",
            serde_json::json!({"node-role.kubernetes.io/leader": ""}),
        )
        .await;
        create_node_with_labels(
            &db,
            "old-leader",
            serde_json::json!({
                "node-role.kubernetes.io/leader": "",
                "kubernetes.io/os": "linux",
            }),
        )
        .await;

        clear_leader_label_from_other_nodes(&store, "local")
            .await
            .expect("clear stale leader labels");

        let local = db
            .get_resource("v1", "Node", None, "local")
            .await
            .expect("get local")
            .expect("local exists");
        assert!(
            local
                .data
                .pointer("/metadata/labels/node-role.kubernetes.io~1leader")
                .is_some()
        );

        let old = db
            .get_resource("v1", "Node", None, "old-leader")
            .await
            .expect("get old")
            .expect("old exists");
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
