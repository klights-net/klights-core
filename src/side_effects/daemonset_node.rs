use std::collections::HashMap;
use std::sync::Mutex;

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;
use klights_reconcile_api::ReconcileKey;
use serde_json::Value;

#[async_trait]
pub(crate) trait DaemonSetNodeSideEffectStore: Send + Sync {
    async fn list_daemonsets(&self) -> Result<Vec<Resource>>;
}

/// Cached fingerprint of scheduling-relevant node fields. When these don't
/// change, we skip the expensive "enqueue every DaemonSet" step.
#[derive(Clone, PartialEq)]
pub(crate) struct NodeSchedulingFingerprint {
    labels: Option<Value>,
    taints: Option<Value>,
    unschedulable: Option<Value>,
}

impl NodeSchedulingFingerprint {
    pub(crate) fn from_node(node: &Value) -> Self {
        Self {
            labels: node.pointer("/metadata/labels").cloned(),
            taints: node.pointer("/spec/taints").cloned(),
            unschedulable: node.pointer("/spec/unschedulable").cloned(),
        }
    }
}

pub(crate) async fn reconcile_keys_for_node<Store: DaemonSetNodeSideEffectStore + ?Sized>(
    node: &Value,
    store: &Store,
    last_fingerprint: &Mutex<HashMap<String, NodeSchedulingFingerprint>>,
) -> Result<Vec<ReconcileKey>> {
    // Only enqueue DaemonSets when scheduling-relevant node fields
    // (labels, taints, unschedulable) actually change. Routine kubelet
    // heartbeats update only status and must not trigger a DaemonSet
    // reconciliation storm.
    let node_name = node
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if node_name.is_empty() {
        return Ok(Vec::new());
    }

    let fingerprint = NodeSchedulingFingerprint::from_node(node);
    let changed = {
        let mut cache = last_fingerprint.lock().unwrap();
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
        return Ok(Vec::new());
    }

    tracing::info!(
        target: "klights::daemonset_node_reconcile",
        node = %node_name,
        "node labels/taints changed; enqueuing DaemonSets"
    );

    let daemonsets = store.list_daemonsets().await?;
    let mut keys = Vec::with_capacity(daemonsets.len());
    for daemonset in daemonsets {
        let Some(namespace) = daemonset.namespace.as_deref() else {
            continue;
        };
        keys.push(ReconcileKey::namespaced(
            "apps/v1",
            "DaemonSet",
            namespace,
            &daemonset.name,
        ));
    }
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use crate::side_effects::ControllerDispatcherSlot;
    use serde_json::json;
    use std::sync::Arc;

    #[tokio::test]
    async fn node_label_change_enqueues_daemonsets_without_reconciling_inline() {
        let db = crate::datastore::test_support::in_memory().await;
        let db_handle: crate::datastore::DatastoreHandle = Arc::new(db.clone());
        let service_ipam = Arc::new(crate::controllers::service::ServiceIpam::new(
            "10.43.128.0/17",
        ));
        let dispatcher = Arc::new(crate::controllers::ControllerDispatcher::new(service_ipam));
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

        let effect = crate::daemonset_node_side_effect_adapter::effect(db_handle.clone(), slot);
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
