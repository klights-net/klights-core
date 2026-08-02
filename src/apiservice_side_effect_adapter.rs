use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;
use serde_json::Value;

use crate::datastore::{DatastoreHandle, ResourceListQuery};
use klights_controllers::side_effects::apiservice::{
    ApiServiceSideEffectStore, apiservice_reconcile_keys_for_resource,
};
use klights_controllers::side_effects::{ControllerDispatcherSlot, SideEffect};

struct ApiServiceReconcileEffect {
    db: DatastoreHandle,
    controller_dispatcher: ControllerDispatcherSlot,
}

#[async_trait]
impl SideEffect for ApiServiceReconcileEffect {
    fn name(&self) -> &'static str {
        "apiservice_reconcile"
    }

    async fn apply(&self, resource: &Value) -> Result<()> {
        let Some(dispatcher) = self.controller_dispatcher.get() else {
            tracing::debug!(
                "APIServiceReconcileEffect skipped: controller dispatcher not yet bound"
            );
            return Ok(());
        };
        dispatcher
            .enqueue_reconcile_batch(apiservice_reconcile_keys_for_resource(resource, self).await?)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl ApiServiceSideEffectStore for ApiServiceReconcileEffect {
    async fn list_apiservices(&self) -> Result<Vec<Resource>> {
        self.db
            .list_resources(
                "apiregistration.k8s.io/v1",
                "APIService",
                None,
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
    Arc::new(ApiServiceReconcileEffect {
        db,
        controller_dispatcher,
    })
}
