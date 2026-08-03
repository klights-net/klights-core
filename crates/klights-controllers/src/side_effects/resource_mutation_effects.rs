//! Adapter from mutation facts to the controller-owned side-effect runtime.

use std::sync::Arc;

use klights_reconcile_api::{
    ResourceChange, ResourceMutationEffectsFuture, ResourceMutationEffectsPort,
    ResourceMutationEffectsRequest,
};

use super::{SideEffectMetrics, SideEffectRegistry, run_delete_hooks_logged, run_hooks_logged};

pub struct ResourceMutationEffects {
    registry: Arc<SideEffectRegistry>,
    metrics: Arc<SideEffectMetrics>,
}

impl ResourceMutationEffects {
    pub fn new(registry: Arc<SideEffectRegistry>, metrics: Arc<SideEffectMetrics>) -> Arc<Self> {
        Arc::new(Self { registry, metrics })
    }
}

impl ResourceMutationEffectsPort for ResourceMutationEffects {
    fn dispatch_resource_mutation_effects<'a>(
        &'a self,
        request: ResourceMutationEffectsRequest<'a>,
    ) -> ResourceMutationEffectsFuture<'a> {
        Box::pin(async move {
            let _ = request.old_resource();
            match request.change() {
                ResourceChange::Deleted => {
                    run_delete_hooks_logged(
                        &self.registry,
                        request.resource(),
                        &self.metrics,
                        request.context(),
                    )
                    .await;
                }
                ResourceChange::Created | ResourceChange::Updated => {
                    run_hooks_logged(
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
