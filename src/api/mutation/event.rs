use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationOperation {
    Create,
    Update,
    Patch,
    DeleteMark,
    HardDelete,
}

pub struct MutationEvent<'a> {
    pub operation: MutationOperation,
    pub resource: &'a Value,
    pub old_resource: Option<&'a Value>,
    pub persisted: bool,
    pub dry_run: crate::api::mutation::DryRunMode,
    pub context: &'static str,
}

pub async fn dispatch_mutation_event(
    registry: &crate::side_effects::SideEffectRegistry,
    db: &dyn crate::datastore::DatastoreBackend,
    metrics: &crate::side_effects::SideEffectMetrics,
    event: MutationEvent<'_>,
) {
    if !event.persisted || event.dry_run.is_all() {
        return;
    }
    let _ = event.old_resource;
    match event.operation {
        MutationOperation::HardDelete => {
            crate::side_effects::run_delete_hooks_logged(
                registry,
                event.resource,
                db,
                metrics,
                event.context,
            )
            .await;
        }
        MutationOperation::Create
        | MutationOperation::Update
        | MutationOperation::Patch
        | MutationOperation::DeleteMark => {
            crate::side_effects::run_hooks_logged(
                registry,
                event.resource,
                db,
                metrics,
                event.context,
            )
            .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::side_effects::{ErrorPolicy, SideEffect, SideEffectRegistry};
    use anyhow::Result;
    use async_trait::async_trait;
    use serde_json::{Value, json};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingSideEffect {
        apply_count: Arc<AtomicUsize>,
        delete_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl SideEffect for CountingSideEffect {
        fn name(&self) -> &'static str {
            "counting"
        }

        async fn apply(
            &self,
            _resource: &Value,
            _db: &dyn crate::datastore::DatastoreBackend,
        ) -> Result<()> {
            self.apply_count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn apply_delete(
            &self,
            _resource: &Value,
            _db: &dyn crate::datastore::DatastoreBackend,
        ) -> Result<()> {
            self.delete_count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    fn register_counter(registry: &mut SideEffectRegistry) -> (Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let apply_count = Arc::new(AtomicUsize::new(0));
        let delete_count = Arc::new(AtomicUsize::new(0));
        registry.register(
            "v1",
            "ConfigMap",
            Arc::new(CountingSideEffect {
                apply_count: apply_count.clone(),
                delete_count: delete_count.clone(),
            }),
            ErrorPolicy::Warn,
        );
        (apply_count, delete_count)
    }

    #[tokio::test]
    async fn mutation_event_dispatch_skips_dry_run() {
        let (_db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
        let mut registry = SideEffectRegistry::new();
        let (apply_count, delete_count) = register_counter(&mut registry);
        let metrics = crate::side_effects::SideEffectMetrics::new();
        let resource = json!({"apiVersion": "v1", "kind": "ConfigMap"});

        dispatch_mutation_event(
            &registry,
            db_handle.as_ref(),
            &metrics,
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
        let (_db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
        let mut registry = SideEffectRegistry::new();
        let (apply_count, delete_count) = register_counter(&mut registry);
        let metrics = crate::side_effects::SideEffectMetrics::new();
        let resource = json!({"apiVersion": "v1", "kind": "ConfigMap"});

        dispatch_mutation_event(
            &registry,
            db_handle.as_ref(),
            &metrics,
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
        let (_db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
        let mut registry = SideEffectRegistry::new();
        let (apply_count, delete_count) = register_counter(&mut registry);
        let metrics = crate::side_effects::SideEffectMetrics::new();
        let resource = json!({"apiVersion": "v1", "kind": "ConfigMap"});

        dispatch_mutation_event(
            &registry,
            db_handle.as_ref(),
            &metrics,
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
