use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;
use klights_reconcile_api::ReconcileKey;
use serde_json::Value;

use super::{ControllerDispatcherSlot, SideEffect};

#[async_trait]
pub trait DaemonSetNodeSideEffectStore: Send + Sync {
    async fn list_daemonsets(&self) -> Result<Vec<Resource>>;
}

struct DaemonSetNodeReconcile {
    store: Arc<dyn DaemonSetNodeSideEffectStore>,
    controller_dispatcher: ControllerDispatcherSlot,
    last_fingerprint: Mutex<HashMap<String, NodeSchedulingFingerprint>>,
}

#[async_trait]
impl SideEffect for DaemonSetNodeReconcile {
    fn name(&self) -> &'static str {
        "daemonset_node_reconcile"
    }

    async fn apply(&self, node: &Value) -> Result<()> {
        let Some(dispatcher) = self.controller_dispatcher.get() else {
            tracing::debug!("daemonset_node_reconcile: controller dispatcher is not bound yet");
            return Ok(());
        };
        dispatcher
            .enqueue_reconcile_batch(
                reconcile_keys_for_node(node, self.store.as_ref(), &self.last_fingerprint).await?,
            )
            .await?;
        Ok(())
    }
}

pub fn effect(
    store: Arc<dyn DaemonSetNodeSideEffectStore>,
    controller_dispatcher: ControllerDispatcherSlot,
) -> Arc<dyn SideEffect> {
    Arc::new(DaemonSetNodeReconcile {
        store,
        controller_dispatcher,
        last_fingerprint: Mutex::new(HashMap::new()),
    })
}

/// Cached fingerprint of scheduling-relevant node fields. When these don't
/// change, we skip the expensive "enqueue every DaemonSet" step.
#[derive(Clone, PartialEq)]
pub struct NodeSchedulingFingerprint {
    labels: Option<Value>,
    taints: Option<Value>,
    unschedulable: Option<Value>,
}

impl NodeSchedulingFingerprint {
    pub fn from_node(node: &Value) -> Self {
        Self {
            labels: node.pointer("/metadata/labels").cloned(),
            taints: node.pointer("/spec/taints").cloned(),
            unschedulable: node.pointer("/spec/unschedulable").cloned(),
        }
    }
}

pub async fn reconcile_keys_for_node<Store: DaemonSetNodeSideEffectStore + ?Sized>(
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
