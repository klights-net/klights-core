//! Side-effect trait and registry for post-mutation hooks.

use super::policy::ErrorPolicy;
use crate::datastore::DatastoreBackend;
use crate::kubelet::pod_repository::PodRepository;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Shared, late-bound slot for the process-wide [`PodRepository`].
///
/// Constructed empty by `SideEffectRegistry::new` (and consequently by
/// `default_registry`); populated by `SideEffectRegistry::set_pod_repository`
/// once `PodRepository` exists. Side effects that need pod-scoped reads
/// (e.g. PDB status, ResourceQuota recount) hold a clone of this `Arc` and
/// resolve the repository at `apply` time so the registry-vs-repository
/// construction-order cycle stays manageable.
#[derive(Clone, Default)]
pub struct PodRepositorySlot {
    inner: Arc<RwLock<Option<Arc<PodRepository>>>>,
}

impl PodRepositorySlot {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(None)),
        }
    }

    pub fn set(&self, repository: Arc<PodRepository>) {
        if let Ok(mut guard) = self.inner.write() {
            *guard = Some(repository);
        }
    }

    pub fn get(&self) -> Option<Arc<PodRepository>> {
        self.inner.read().ok().and_then(|g| g.clone())
    }
}

/// Shared, late-bound slot for the process-wide controller dispatcher.
///
/// Controller-like side effects enqueue reconcile intents through this slot
/// instead of calling controllers inline from the mutation path.
#[derive(Clone, Default)]
pub struct ControllerDispatcherSlot {
    inner: Arc<RwLock<Option<Arc<crate::controller_dispatcher::ControllerDispatcher>>>>,
}

impl ControllerDispatcherSlot {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(None)),
        }
    }

    pub fn set(&self, dispatcher: Arc<crate::controller_dispatcher::ControllerDispatcher>) {
        if let Ok(mut guard) = self.inner.write() {
            *guard = Some(dispatcher);
        }
    }

    pub fn get(&self) -> Option<Arc<crate::controller_dispatcher::ControllerDispatcher>> {
        self.inner.read().ok().and_then(|g| g.clone())
    }
}

/// Trait for side-effect hooks that run after resource mutations.
#[async_trait]
pub trait SideEffect: Send + Sync {
    /// Returns a stable identifier for this side effect (used for metrics)
    fn name(&self) -> &'static str;

    /// Apply this side effect. Returns Ok(()) on success.
    ///
    /// Error is handled according to the hook's `ErrorPolicy`.
    async fn apply(&self, resource: &Value, db: &dyn DatastoreBackend) -> Result<()>;

    /// Apply delete-specific side effects for a resource that has already
    /// been hard-deleted from the datastore.
    async fn apply_delete(&self, _resource: &Value, _db: &dyn DatastoreBackend) -> Result<()> {
        Ok(())
    }
}

struct Hook {
    side_effect: Arc<dyn SideEffect>,
    policy: ErrorPolicy,
}

#[derive(Clone, Debug)]
pub struct SideEffectFailure {
    pub hook: &'static str,
    pub error: String,
}

/// Registry mapping (apiVersion, kind) to side-effect hooks.
pub struct SideEffectRegistry {
    hooks: HashMap<(&'static str, &'static str), Vec<Hook>>,
    pod_repository: PodRepositorySlot,
    controller_dispatcher: ControllerDispatcherSlot,
}

impl SideEffectRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            hooks: HashMap::new(),
            pod_repository: PodRepositorySlot::new(),
            controller_dispatcher: ControllerDispatcherSlot::new(),
        }
    }

    /// Borrow the (potentially-empty) shared `PodRepository` slot. Side
    /// effects clone this at construction time and resolve the repository
    /// at `apply` time — see `pdb.rs` and `resource_quota.rs`.
    pub fn pod_repository_slot(&self) -> PodRepositorySlot {
        self.pod_repository.clone()
    }

    /// Borrow the shared controller-dispatcher slot used by side effects that
    /// enqueue controller reconcile intents.
    pub fn controller_dispatcher_slot(&self) -> ControllerDispatcherSlot {
        self.controller_dispatcher.clone()
    }

    /// Late-bind the process-wide `PodRepository`. Bootstrap calls this
    /// once after `PodRepository::new` returns; before that, side effects
    /// that look up the slot will see `None` and degrade to a debug log.
    pub fn set_pod_repository(&self, repository: Arc<PodRepository>) {
        self.pod_repository.set(repository);
    }

    /// Late-bind the process-wide controller dispatcher. Bootstrap calls this
    /// after both the registry and dispatcher have been constructed.
    pub fn set_controller_dispatcher(
        &self,
        dispatcher: Arc<crate::controller_dispatcher::ControllerDispatcher>,
    ) {
        self.controller_dispatcher.set(dispatcher);
    }

    /// Register a side-effect hook for a specific resource type.
    pub fn register(
        &mut self,
        api_version: &'static str,
        kind: &'static str,
        side_effect: Arc<dyn SideEffect>,
        policy: ErrorPolicy,
    ) {
        let key = (api_version, kind);
        self.hooks.entry(key).or_default().push(Hook {
            side_effect,
            policy,
        });
    }

    /// Run all registered hooks for a resource type and collect failures.
    pub async fn run_hooks_collect_failures(
        &self,
        resource: &Value,
        db: &dyn DatastoreBackend,
    ) -> (Vec<SideEffectFailure>, bool) {
        self.run_hooks_collect_failures_matching(resource, db, |_| true)
            .await
    }

    /// Run one named hook for a resource type and collect failures.
    pub async fn run_named_hook_collect_failures(
        &self,
        resource: &Value,
        db: &dyn DatastoreBackend,
        hook_name: &'static str,
    ) -> (Vec<SideEffectFailure>, bool) {
        self.run_hooks_collect_failures_matching(resource, db, |hook| {
            hook.side_effect.name() == hook_name
        })
        .await
    }

    async fn run_hooks_collect_failures_matching(
        &self,
        resource: &Value,
        db: &dyn DatastoreBackend,
        should_run: impl Fn(&Hook) -> bool,
    ) -> (Vec<SideEffectFailure>, bool) {
        let api_version = resource
            .get("apiVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let kind = resource.get("kind").and_then(|v| v.as_str()).unwrap_or("");
        let key = (api_version, kind);
        let mut failures = Vec::new();
        let mut fatal = false;

        if let Some(hooks) = self.hooks.get(&key) {
            for hook in hooks {
                if !should_run(hook) {
                    continue;
                }
                match hook.side_effect.apply(resource, db).await {
                    Err(e) => {
                        failures.push(SideEffectFailure {
                            hook: hook.side_effect.name(),
                            error: e.to_string(),
                        });
                        match hook.policy {
                            ErrorPolicy::Ignore => {
                                tracing::debug!(
                                    side_effect = %hook.side_effect.name(),
                                    error = %e,
                                    "side-effect ignored"
                                );
                            }
                            ErrorPolicy::Warn => {
                                tracing::warn!(
                                    side_effect = %hook.side_effect.name(),
                                    error = %e,
                                    "side-effect failed"
                                );
                            }
                            ErrorPolicy::Fail => {
                                fatal = true;
                                break;
                            }
                        }
                    }
                    _ => {
                        tracing::debug!(
                            side_effect = %hook.side_effect.name(),
                            result = "ok"
                        );
                    }
                }
            }
        }

        (failures, fatal)
    }

    /// Run all registered hooks for a resource type.
    pub async fn run_hooks(&self, resource: &Value, db: &dyn DatastoreBackend) -> Result<()> {
        let (failures, fatal) = self.run_hooks_collect_failures(resource, db).await;
        if fatal {
            if let Some(error) = failures.into_iter().next() {
                anyhow::bail!(error.error)
            } else {
                anyhow::bail!("side-effect hook failed")
            }
        }
        Ok(())
    }

    /// Run all registered delete hooks for a resource type.
    pub async fn run_delete_hooks_collect_failures(
        &self,
        resource: &Value,
        db: &dyn DatastoreBackend,
    ) -> (Vec<SideEffectFailure>, bool) {
        let api_version = resource
            .get("apiVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let kind = resource.get("kind").and_then(|v| v.as_str()).unwrap_or("");
        let key = (api_version, kind);
        let mut failures = Vec::new();
        let mut fatal = false;
        if let Some(hooks) = self.hooks.get(&key) {
            for hook in hooks {
                match hook.side_effect.apply_delete(resource, db).await {
                    Ok(()) => {
                        tracing::debug!(
                            side_effect = %hook.side_effect.name(),
                            result = "delete-ok"
                        );
                    }
                    Err(error) => match hook.policy {
                        ErrorPolicy::Ignore => {
                            failures.push(SideEffectFailure {
                                hook: hook.side_effect.name(),
                                error: error.to_string(),
                            });
                            tracing::debug!(
                                side_effect = %hook.side_effect.name(),
                                error = %error,
                                "delete side-effect ignored"
                            );
                        }
                        ErrorPolicy::Warn => {
                            failures.push(SideEffectFailure {
                                hook: hook.side_effect.name(),
                                error: error.to_string(),
                            });
                            tracing::warn!(
                                side_effect = %hook.side_effect.name(),
                                error = %error,
                                "delete side-effect failed"
                            );
                        }
                        ErrorPolicy::Fail => {
                            failures.push(SideEffectFailure {
                                hook: hook.side_effect.name(),
                                error: error.to_string(),
                            });
                            fatal = true;
                            break;
                        }
                    },
                }
            }
        }
        (failures, fatal)
    }

    /// Run all registered delete hooks for a resource type.
    pub async fn run_delete_hooks(
        &self,
        resource: &Value,
        db: &dyn DatastoreBackend,
    ) -> Result<()> {
        let (failures, fatal) = self.run_delete_hooks_collect_failures(resource, db).await;
        if fatal {
            if let Some(error) = failures.into_iter().next() {
                anyhow::bail!(error.error)
            } else {
                anyhow::bail!("delete side-effect hook failed")
            }
        }
        Ok(())
    }
}

impl Default for SideEffectRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;
    use std::sync::Arc;

    struct TestSideEffect {
        name: &'static str,
    }

    #[async_trait]
    impl SideEffect for TestSideEffect {
        fn name(&self) -> &'static str {
            self.name
        }

        async fn apply(&self, _resource: &Value, _db: &dyn DatastoreBackend) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_registry_runs_hooks_in_order() {
        let (_db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
        let mut registry = SideEffectRegistry::new();

        registry.register(
            "v1",
            "Test",
            Arc::new(TestSideEffect { name: "hook1" }) as Arc<dyn SideEffect>,
            ErrorPolicy::Warn,
        );
        registry.register(
            "v1",
            "Test",
            Arc::new(TestSideEffect { name: "hook2" }) as Arc<dyn SideEffect>,
            ErrorPolicy::Warn,
        );

        let resource = json!({"apiVersion": "v1", "kind": "Test"});
        let result = registry.run_hooks(&resource, db_handle.as_ref()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_registry_ignores_unknown_resource_types() {
        let (_db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
        let mut registry = SideEffectRegistry::new();

        registry.register(
            "v1",
            "Test",
            Arc::new(TestSideEffect { name: "hook" }) as Arc<dyn SideEffect>,
            ErrorPolicy::Warn,
        );

        let unknown = json!({"apiVersion": "v1", "kind": "Unknown"});
        let result = registry.run_hooks(&unknown, db_handle.as_ref()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_registry_error_policy_warn_continues() {
        struct FailingSideEffect;

        #[async_trait]
        impl SideEffect for FailingSideEffect {
            fn name(&self) -> &'static str {
                "failing"
            }

            async fn apply(&self, _resource: &Value, _db: &dyn DatastoreBackend) -> Result<()> {
                anyhow::bail!("intentional failure")
            }
        }

        let (_db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
        let mut registry = SideEffectRegistry::new();

        registry.register(
            "v1",
            "Test",
            Arc::new(FailingSideEffect) as Arc<dyn SideEffect>,
            ErrorPolicy::Warn,
        );

        let resource = json!({"apiVersion": "v1", "kind": "Test"});
        let result = registry.run_hooks(&resource, db_handle.as_ref()).await;
        // Warn policy continues despite failure
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn node_side_effect_enqueues_daemonset_key_without_inline_reconcile() {
        let (db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
        let task_supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let service_ipam = Arc::new(crate::controllers::service::ServiceIpam::new(
            "10.43.128.0/17",
        ));
        let dispatcher = Arc::new(crate::controller_dispatcher::ControllerDispatcher::new(
            service_ipam,
        ));
        let metrics = crate::side_effects::SideEffectMetrics::new();
        let registry = crate::side_effects::default_registry(
            metrics,
            None,
            Some(task_supervisor.clone()),
            Some(db_handle.clone()),
        );
        registry.set_controller_dispatcher(dispatcher.clone());

        let node = db
            .create_resource(
                "v1",
                "Node",
                None,
                "node-a",
                json!({
                    "apiVersion": "v1",
                    "kind": "Node",
                    "metadata": {
                        "name": "node-a",
                        "labels": {"daemonset-color": "blue"}
                    }
                }),
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

        registry
            .run_hooks(&node.data, db_handle.as_ref())
            .await
            .unwrap();

        let keys = dispatcher.queued_reconcile_keys_for_test().await;
        assert_eq!(
            keys,
            vec![crate::controllers::workqueue::ReconcileKey::namespaced(
                "apps/v1",
                "DaemonSet",
                "default",
                "daemon-set"
            )],
            "node side effect should enqueue the affected daemonset"
        );

        let pods = db
            .list_resources(
                "v1",
                "Pod",
                Some("default"),
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .unwrap();
        assert!(
            pods.items.is_empty(),
            "node side effect must not run DaemonSet reconciliation inline"
        );
    }
}
