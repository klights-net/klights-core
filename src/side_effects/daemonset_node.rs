use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use crate::datastore::DatastoreBackend;
use crate::side_effects::{ControllerDispatcherSlot, SideEffect};

/// Cached fingerprint of scheduling-relevant node fields. When these don't
/// change, we skip the expensive "enqueue every DaemonSet" step.
#[derive(Clone, PartialEq)]
struct NodeSchedulingFingerprint {
    labels: Option<Value>,
    taints: Option<Value>,
    unschedulable: Option<Value>,
}

impl NodeSchedulingFingerprint {
    fn from_node(node: &Value) -> Self {
        Self {
            labels: node.pointer("/metadata/labels").cloned(),
            taints: node.pointer("/spec/taints").cloned(),
            unschedulable: node.pointer("/spec/unschedulable").cloned(),
        }
    }
}

struct DaemonSetNodeReconcile {
    controller_dispatcher: ControllerDispatcherSlot,
    last_fingerprint: Mutex<HashMap<String, NodeSchedulingFingerprint>>,
}

pub fn daemonset_node_reconcile(
    controller_dispatcher: ControllerDispatcherSlot,
) -> Arc<dyn SideEffect> {
    Arc::new(DaemonSetNodeReconcile {
        controller_dispatcher,
        last_fingerprint: Mutex::new(HashMap::new()),
    })
}

#[async_trait]
impl SideEffect for DaemonSetNodeReconcile {
    fn name(&self) -> &'static str {
        "daemonset_node_reconcile"
    }

    async fn apply(&self, node: &Value, db: &dyn DatastoreBackend) -> Result<()> {
        let Some(dispatcher) = self.controller_dispatcher.get() else {
            tracing::debug!("daemonset_node_reconcile: controller dispatcher is not bound yet");
            return Ok(());
        };

        // Only enqueue DaemonSets when scheduling-relevant node fields
        // (labels, taints, unschedulable) actually change. Routine kubelet
        // heartbeats update only status and must not trigger a DaemonSet
        // reconciliation storm.
        let node_name = node
            .pointer("/metadata/name")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if node_name.is_empty() {
            return Ok(());
        }

        let fingerprint = NodeSchedulingFingerprint::from_node(node);
        let changed = {
            let mut cache = self.last_fingerprint.lock().unwrap();
            let prev = cache.get(node_name);
            let changed = match prev {
                Some(prev) => *prev != fingerprint,
                None => true,
            };
            if changed {
                cache.insert(node_name.to_string(), fingerprint);
            }
            changed
        };

        if !changed {
            tracing::debug!(
                target: "klights::daemonset_node_reconcile",
                node = %node_name,
                "node scheduling fingerprint unchanged; skipping DaemonSet enqueue"
            );
            return Ok(());
        }

        tracing::info!(
            target: "klights::daemonset_node_reconcile",
            node = %node_name,
            "node labels/taints changed; enqueuing DaemonSets"
        );

        let daemonsets = db
            .list_resources(
                "apps/v1",
                "DaemonSet",
                None,
                crate::datastore::ResourceListQuery::all(),
            )
            .await?;
        for daemonset in daemonsets.items {
            let Some(namespace) = daemonset.namespace.as_deref() else {
                continue;
            };
            dispatcher
                .enqueue_reconcile_key(crate::controllers::workqueue::ReconcileKey::namespaced(
                    "apps/v1",
                    "DaemonSet",
                    namespace,
                    &daemonset.name,
                ))
                .await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn node_label_change_enqueues_daemonsets_without_reconciling_inline() {
        let db = crate::datastore::test_support::in_memory().await;
        let service_ipam = Arc::new(crate::controllers::service::ServiceIpam::new(
            "10.43.128.0/17",
        ));
        let dispatcher = Arc::new(crate::controller_dispatcher::ControllerDispatcher::new(
            service_ipam,
        ));
        let slot = ControllerDispatcherSlot::new();
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

        let effect = daemonset_node_reconcile(slot);
        effect.apply(&node.data, &db).await.unwrap();
        assert_eq!(
            dispatcher.queued_reconcile_keys_for_test().await,
            vec![crate::controllers::workqueue::ReconcileKey::namespaced(
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
        effect.apply(&labelled_node.data, &db).await.unwrap();

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
            vec![crate::controllers::workqueue::ReconcileKey::namespaced(
                "apps/v1",
                "DaemonSet",
                "default",
                "daemon-set"
            )],
            "repeated node mutations should deduplicate the same DaemonSet key"
        );
    }
}
