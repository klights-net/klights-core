use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;

use crate::datastore::{DatastoreHandle, ResourceListQuery};
use klights_controllers::side_effects::daemonset_node::DaemonSetNodeSideEffectStore;

struct RootDaemonSetNodeSideEffectStore {
    db: DatastoreHandle,
}

#[async_trait]
impl DaemonSetNodeSideEffectStore for RootDaemonSetNodeSideEffectStore {
    async fn list_daemonsets(&self) -> Result<Vec<Resource>> {
        self.db
            .list_resources("apps/v1", "DaemonSet", None, ResourceListQuery::all())
            .await
            .map(|listing| listing.items)
    }
}

pub(crate) fn port(db: DatastoreHandle) -> Arc<dyn DaemonSetNodeSideEffectStore> {
    Arc::new(RootDaemonSetNodeSideEffectStore { db })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn node_label_change_enqueues_daemonsets_without_reconciling_inline() {
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        let db_handle: crate::datastore::DatastoreHandle = Arc::new(db.clone());
        let service_ipam = Arc::new(klights_controllers::service::ServiceIpam::new(
            "10.43.128.0/17",
        ));
        let dispatcher =
            Arc::new(crate::controller_test_support::queue_only_dispatcher_for_test(service_ipam));
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
            port(db_handle.clone()),
            slot,
        );
        effect.apply(&node.data).await.unwrap();
        assert_eq!(
            dispatcher.queued_reconcile_keys_for_test().await,
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
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .unwrap();
        assert_eq!(pods.items.len(), 0);
        assert_eq!(
            dispatcher.queued_reconcile_keys_for_test().await,
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
