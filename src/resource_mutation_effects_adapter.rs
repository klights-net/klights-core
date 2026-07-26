use std::sync::Arc;

use klights_reconcile_api::{
    ResourceChange, ResourceMutationEffectsFuture, ResourceMutationEffectsPort,
    ResourceMutationEffectsRequest,
};

pub(crate) struct ResourceMutationEffectsAdapter {
    registry: Arc<crate::side_effects::SideEffectRegistry>,
    metrics: Arc<crate::side_effects::SideEffectMetrics>,
}

impl ResourceMutationEffectsAdapter {
    pub(crate) fn new(
        registry: Arc<crate::side_effects::SideEffectRegistry>,
        metrics: Arc<crate::side_effects::SideEffectMetrics>,
    ) -> Arc<Self> {
        Arc::new(Self { registry, metrics })
    }
}

impl ResourceMutationEffectsPort for ResourceMutationEffectsAdapter {
    fn dispatch_resource_mutation_effects<'a>(
        &'a self,
        request: ResourceMutationEffectsRequest<'a>,
    ) -> ResourceMutationEffectsFuture<'a> {
        Box::pin(async move {
            let _ = request.old_resource();
            match request.change() {
                ResourceChange::Deleted => {
                    crate::side_effect_registry_composition::run_delete_hooks_logged(
                        &self.registry,
                        request.resource(),
                        &self.metrics,
                        request.context(),
                    )
                    .await;
                }
                ResourceChange::Created | ResourceChange::Updated => {
                    crate::side_effect_registry_composition::run_hooks_logged(
                        &self.registry,
                        request.resource(),
                        &self.metrics,
                        request.context(),
                    )
                    .await;
                }
            }
        })
    }
}
