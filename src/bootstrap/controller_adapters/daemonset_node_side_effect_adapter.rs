use klights_cluster_store::{
    ClusterResourceRead, ResourceCollectionScope, ResourceListQuery, ResourceListRead,
    ResourceListRequest,
};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;

use klights_controllers::side_effects::daemonset_node::DaemonSetNodeSideEffectStore;

struct RootDaemonSetNodeSideEffectStore {
    resource_reads: Arc<dyn ClusterResourceRead>,
}

#[async_trait]
impl DaemonSetNodeSideEffectStore for RootDaemonSetNodeSideEffectStore {
    async fn list_daemonsets(&self) -> Result<Vec<Resource>> {
        match self
            .resource_reads
            .list_resources(ResourceListRequest::new(
                "apps/v1",
                "DaemonSet",
                ResourceCollectionScope::AllNamespaces,
                ResourceListQuery::all(),
            ))
            .await?
        {
            ResourceListRead::Current(page) | ResourceListRead::Historical(page) => {
                Ok(page.into_items())
            }
            ResourceListRead::Expired {
                requested,
                oldest_available,
                ..
            } => anyhow::bail!(
                "DaemonSet LIST at resourceVersion {requested} expired before {oldest_available}"
            ),
        }
    }
}

pub(crate) fn port(
    resource_reads: Arc<dyn ClusterResourceRead>,
) -> Arc<dyn DaemonSetNodeSideEffectStore> {
    Arc::new(RootDaemonSetNodeSideEffectStore { resource_reads })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn node_label_change_enqueues_daemonsets_without_reconciling_inline() {
        let db = crate::bootstrap::composition::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
        let dispatcher = Arc::new(
            crate::bootstrap::composition_tests::recording_reconcile_sink::recording_reconcile_sink(
            ),
        );
        let slot = klights_controllers::side_effects::ControllerDispatcherSlot::new();
        slot.set(dispatcher.clone());

        let node = db
            .create_resource(
                "v1",
                "Node",
                None,
                "node-a",
                json!({"apiVersion": "v1", "kind": "Node", "metadata": {"name": "node-a", "labels": {}}}),
            )
            .await
            .unwrap();
        db.create_resource(
            "apps/v1",
            "DaemonSet",
            Some("default"),
            "daemon-set",
            json!({
                "apiVersion": "apps/v1",
                "kind": "DaemonSet",
                "metadata": {"name": "daemon-set", "namespace": "default", "uid": "ds-uid"},
                "spec": {
                    "selector": {"matchLabels": {"name": "daemon"}},
                    "template": {
                        "metadata": {"labels": {"name": "daemon"}},
                        "spec": {
                            "nodeSelector": {"daemonset-color": "blue"},
                            "containers": [{"name": "app", "image": "pause"}]
                        }
                    }
                }
            }),
        )
        .await
        .unwrap();

        let effect = klights_controllers::side_effects::daemonset_node::effect(
            port(db.focused_read_store()),
            slot,
        );
        effect.apply(&node.data).await.unwrap();
        assert_eq!(
            dispatcher.pending_keys().await,
            vec![klights_reconcile_api::ReconcileKey::namespaced(
                "apps/v1",
                "DaemonSet",
                "default",
                "daemon-set"
            )]
        );

        let mut labelled_node: serde_json::Value = (*node.data).clone();
        labelled_node["metadata"]["labels"] = json!({"daemonset-color": "blue"});
        let labelled_node = db
            .update_resource(
                "v1",
                "Node",
                None,
                "node-a",
                labelled_node,
                node.resource_version,
            )
            .await
            .unwrap();
        effect.apply(&labelled_node.data).await.unwrap();

        let pods = db
            .list_resources(
                "v1",
                "Pod",
                Some("default"),
                klights_cluster_store::ResourceListOptions::all(),
            )
            .await
            .unwrap();
        assert_eq!(pods.items.len(), 0);
        assert_eq!(
            dispatcher.pending_keys().await,
            vec![klights_reconcile_api::ReconcileKey::namespaced(
                "apps/v1",
                "DaemonSet",
                "default",
                "daemon-set"
            )],
            "repeated node mutations should deduplicate the same DaemonSet key"
        );
    }
}
