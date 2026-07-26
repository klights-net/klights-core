use serde_json::Value;

#[deprecated(
    note = "use klights_reconcile_api::MutationOperation; remove in Phase 18.2 compatibility cleanup"
)]
pub type MutationOperation = klights_reconcile_api::MutationOperation;

pub struct MutationEvent<'a> {
    pub operation: klights_reconcile_api::MutationOperation,
    pub resource: &'a Value,
    pub old_resource: Option<&'a Value>,
    pub persisted: bool,
    pub dry_run: crate::api::mutation::DryRunMode,
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
    use super::*;
    use klights_reconcile_api::{
        MutationOperation, ResourceChange, ResourceMutationEffectsFuture,
        ResourceMutationEffectsPort, ResourceMutationEffectsRequest,
    };
    use serde_json::json;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

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

    fn counting_effects() -> (CountingMutationEffects, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let apply_count = Arc::new(AtomicUsize::new(0));
        let delete_count = Arc::new(AtomicUsize::new(0));
        (
            CountingMutationEffects {
                apply_count: apply_count.clone(),
                delete_count: delete_count.clone(),
            },
            apply_count,
            delete_count,
        )
    }

    #[tokio::test]
    async fn mutation_event_dispatch_skips_dry_run() {
        let (effects, apply_count, delete_count) = counting_effects();
        let resource = json!({"apiVersion": "v1", "kind": "ConfigMap"});

        dispatch_mutation_event(
            &effects,
            MutationEvent {
                operation: MutationOperation::Create,
                resource: &resource,
                old_resource: None,
                persisted: false,
                dry_run: crate::api::mutation::DryRunMode::All,
                context: "test_dry_run",
            },
        )
        .await;

        assert_eq!(apply_count.load(Ordering::Relaxed), 0);
        assert_eq!(delete_count.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn mutation_event_dispatch_runs_once_for_persisted_update() {
        let (effects, apply_count, delete_count) = counting_effects();
        let resource = json!({"apiVersion": "v1", "kind": "ConfigMap"});

        dispatch_mutation_event(
            &effects,
            MutationEvent {
                operation: MutationOperation::Update,
                resource: &resource,
                old_resource: None,
                persisted: true,
                dry_run: crate::api::mutation::DryRunMode::Live,
                context: "test_update",
            },
        )
        .await;

        assert_eq!(apply_count.load(Ordering::Relaxed), 1);
        assert_eq!(delete_count.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn mutation_event_dispatch_uses_delete_hooks_for_hard_delete() {
        let (effects, apply_count, delete_count) = counting_effects();
        let resource = json!({"apiVersion": "v1", "kind": "ConfigMap"});

        dispatch_mutation_event(
            &effects,
            MutationEvent {
                operation: MutationOperation::HardDelete,
                resource: &resource,
                old_resource: None,
                persisted: true,
                dry_run: crate::api::mutation::DryRunMode::Live,
                context: "test_hard_delete",
            },
        )
        .await;

        assert_eq!(apply_count.load(Ordering::Relaxed), 0);
        assert_eq!(delete_count.load(Ordering::Relaxed), 1);
    }
}
