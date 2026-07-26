use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;
use serde_json::Value;

use crate::datastore::{DatastoreBackend, DatastoreHandle, ResourceListQuery};
use crate::side_effects::daemonset_node::{
    DaemonSetNodeSideEffectStore, NodeSchedulingFingerprint, reconcile_keys_for_node,
};
use crate::side_effects::{ControllerDispatcherSlot, SideEffect};

struct DaemonSetNodeReconcile {
    db: DatastoreHandle,
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
                reconcile_keys_for_node(node, self.db.as_ref(), &self.last_fingerprint).await?,
            )
            .await?;
        Ok(())
    }
}

#[async_trait]
impl<T> DaemonSetNodeSideEffectStore for T
where
    T: DatastoreBackend + ?Sized,
{
    async fn list_daemonsets(&self) -> Result<Vec<Resource>> {
        self.list_resources("apps/v1", "DaemonSet", None, ResourceListQuery::all())
            .await
            .map(|listing| listing.items)
    }
}

pub(crate) fn effect(
    db: DatastoreHandle,
    controller_dispatcher: ControllerDispatcherSlot,
) -> Arc<dyn SideEffect> {
    Arc::new(DaemonSetNodeReconcile {
        db,
        controller_dispatcher,
        last_fingerprint: Mutex::new(HashMap::new()),
    })
}
