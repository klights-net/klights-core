use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;
use serde_json::Value;

use crate::datastore::{DatastoreHandle, ResourceListQuery};
use klights_controllers::side_effects::hpa::{HpaSideEffectStore, hpa_reconcile_keys_for_resource};
use klights_controllers::side_effects::{ControllerDispatcherSlot, SideEffect};

struct HpaReconcileEffect {
    db: DatastoreHandle,
    controller_dispatcher: ControllerDispatcherSlot,
}

#[async_trait]
impl SideEffect for HpaReconcileEffect {
    fn name(&self) -> &'static str {
        "hpa_reconcile"
    }

    async fn apply(&self, resource: &Value) -> Result<()> {
        let Some(dispatcher) = self.controller_dispatcher.get() else {
            tracing::debug!("HpaReconcileEffect skipped: controller dispatcher not yet bound");
            return Ok(());
        };

        dispatcher
            .enqueue_reconcile_batch(hpa_reconcile_keys_for_resource(resource, self).await?)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl HpaSideEffectStore for HpaReconcileEffect {
    async fn list_hpas(&self, api_version: &'static str, namespace: &str) -> Result<Vec<Resource>> {
        self.db
            .list_resources(
                api_version,
                "HorizontalPodAutoscaler",
                Some(namespace),
                ResourceListQuery::all(),
            )
            .await
            .map(|listing| listing.items)
    }
}

pub(crate) fn effect(
    db: DatastoreHandle,
    controller_dispatcher: ControllerDispatcherSlot,
) -> Arc<dyn SideEffect> {
    Arc::new(HpaReconcileEffect {
        db,
        controller_dispatcher,
    })
}
