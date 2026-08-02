//! Applied-only command side-effect dispatch.

use serde_json::Value;

use super::DryRunMode;

pub struct MutationEvent<'a> {
    pub operation: klights_reconcile_api::MutationOperation,
    pub resource: &'a Value,
    pub old_resource: Option<&'a Value>,
    pub persisted: bool,
    pub dry_run: DryRunMode,
    pub context: &'static str,
}

pub async fn dispatch_mutation_event(
    effects: &dyn klights_reconcile_api::ResourceMutationEffectsPort,
    event: MutationEvent<'_>,
) {
    let facts = klights_reconcile_api::MutationFacts::new(
        event.operation,
        event.persisted,
        event.dry_run.is_all(),
    );
    let Some(change) = facts.change() else {
        return;
    };
    effects
        .dispatch_resource_mutation_effects(
            klights_reconcile_api::ResourceMutationEffectsRequest::new(
                change,
                event.resource,
                event.old_resource,
                event.context,
            ),
        )
        .await;
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use klights_reconcile_api::{
        MutationOperation, ResourceChange, ResourceMutationEffectsFuture,
        ResourceMutationEffectsPort, ResourceMutationEffectsRequest,
    };

    use super::*;

    struct CountingMutationEffects {
        apply_count: Arc<AtomicUsize>,
        delete_count: Arc<AtomicUsize>,
    }

    impl ResourceMutationEffectsPort for CountingMutationEffects {
        fn dispatch_resource_mutation_effects<'a>(
            &'a self,
            request: ResourceMutationEffectsRequest<'a>,
        ) -> ResourceMutationEffectsFuture<'a> {
            Box::pin(async move {
                match request.change() {
                    ResourceChange::Created | ResourceChange::Updated => {
                        self.apply_count.fetch_add(1, Ordering::Relaxed);
                    }
                    ResourceChange::Deleted => {
                        self.delete_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        }
    }

    #[tokio::test]
    async fn dispatch_is_applied_only_and_preserves_operation() {
        let apply_count = Arc::new(AtomicUsize::new(0));
        let delete_count = Arc::new(AtomicUsize::new(0));
        let effects = CountingMutationEffects {
            apply_count: apply_count.clone(),
            delete_count: delete_count.clone(),
        };
        let resource = serde_json::json!({"kind": "ConfigMap"});

        dispatch_mutation_event(
            &effects,
            MutationEvent {
                operation: MutationOperation::Create,
                resource: &resource,
                old_resource: None,
                persisted: false,
                dry_run: DryRunMode::All,
                context: "dry_run",
            },
        )
        .await;
        dispatch_mutation_event(
            &effects,
            MutationEvent {
                operation: MutationOperation::HardDelete,
                resource: &resource,
                old_resource: None,
                persisted: true,
                dry_run: DryRunMode::Live,
                context: "hard_delete",
            },
        )
        .await;

        assert_eq!(apply_count.load(Ordering::Relaxed), 0);
        assert_eq!(delete_count.load(Ordering::Relaxed), 1);
    }
}
