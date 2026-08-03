//! Observable execution policy for post-mutation side effects.

use serde_json::Value;

use super::{SideEffectFailureEntry, SideEffectMetrics, SideEffectRegistry};

fn resource_identity(resource: &Value) -> (String, String, Option<String>, String) {
    let api_version = resource
        .get("apiVersion")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let kind = resource
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let namespace = resource
        .pointer("/metadata/namespace")
        .and_then(Value::as_str)
        .map(str::to_string);
    let name = resource
        .pointer("/metadata/name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    (api_version, kind, namespace, name)
}

fn record_failures(
    metrics: &SideEffectMetrics,
    resource: &Value,
    context: &'static str,
    failures: &[super::SideEffectFailure],
) {
    let (api_version, kind, namespace, name) = resource_identity(resource);
    for failure in failures {
        metrics.record_recent_failure(SideEffectFailureEntry {
            api_version: api_version.clone(),
            kind: kind.clone(),
            namespace: namespace.clone(),
            name: name.clone(),
            hook: failure.hook.to_string(),
            context: context.to_string(),
            error: failure.error.clone(),
        });
    }
}

/// Run all registered hooks and make failures observable without changing the
/// already-successful mutation result.
pub async fn run_hooks_logged(
    registry: &SideEffectRegistry,
    resource: &Value,
    metrics: &SideEffectMetrics,
    context: &'static str,
) {
    let (failures, failed) = registry.run_hooks_collect_failures(resource).await;
    record_failures(metrics, resource, context, &failures);

    if failed {
        metrics
            .side_effect_failures_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Some(error) = failures.first() {
            tracing::error!(
                context,
                hook = %error.hook,
                error = %error.error,
                "side-effect hooks failed"
            );
        } else {
            tracing::error!(context, "side-effect hooks failed");
        }
    } else if !failures.is_empty() {
        tracing::warn!(
            context,
            side_effect_failures = failures.len(),
            "side-effect hooks failed with non-fatal policy"
        );
    }
}

/// Run all registered delete hooks and make failures observable without
/// changing the already-successful mutation result.
pub async fn run_delete_hooks_logged(
    registry: &SideEffectRegistry,
    resource: &Value,
    metrics: &SideEffectMetrics,
    context: &'static str,
) {
    let (failures, failed) = registry.run_delete_hooks_collect_failures(resource).await;
    record_failures(metrics, resource, context, &failures);

    if failed {
        metrics
            .side_effect_failures_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Some(error) = failures.first() {
            tracing::error!(
                context,
                hook = %error.hook,
                error = %error.error,
                "delete side-effect hooks failed"
            );
        } else {
            tracing::error!(context, "delete side-effect hooks failed");
        }
    } else if !failures.is_empty() {
        tracing::warn!(
            context,
            side_effect_failures = failures.len(),
            "delete side-effect hooks failed with non-fatal policy"
        );
    }
}

/// Run one registered hook by name with the same failure policy as the full
/// hook runner.
pub async fn run_named_hook_logged(
    registry: &SideEffectRegistry,
    resource: &Value,
    metrics: &SideEffectMetrics,
    hook_name: &'static str,
    context: &'static str,
) {
    let (failures, failed) = registry
        .run_named_hook_collect_failures(resource, hook_name)
        .await;
    record_failures(metrics, resource, context, &failures);

    if failed {
        metrics
            .side_effect_failures_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Some(error) = failures.first() {
            tracing::error!(
                context,
                hook = %error.hook,
                error = %error.error,
                "side-effect hook failed"
            );
        } else {
            tracing::error!(context, hook = hook_name, "side-effect hook failed");
        }
    } else if !failures.is_empty() {
        tracing::warn!(
            context,
            hook = hook_name,
            side_effect_failures = failures.len(),
            "side-effect hook failed with non-fatal policy"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use anyhow::Result;
    use async_trait::async_trait;
    use serde_json::{Value, json};

    use super::*;
    use crate::side_effects::{ErrorPolicy, SideEffect};

    struct FailingHook;

    #[async_trait]
    impl SideEffect for FailingHook {
        fn name(&self) -> &'static str {
            "failing_hook"
        }

        async fn apply(&self, _resource: &Value) -> Result<()> {
            anyhow::bail!("intentional failure")
        }
    }

    #[tokio::test]
    async fn run_hooks_logged_increments_counter_on_failure() {
        let mut registry = SideEffectRegistry::new();
        registry.register("v1", "Test", Arc::new(FailingHook), ErrorPolicy::Fail);
        let metrics = SideEffectMetrics::new();

        run_hooks_logged(
            &registry,
            &json!({"apiVersion": "v1", "kind": "Test"}),
            &metrics,
            "test",
        )
        .await;

        assert_eq!(
            metrics.side_effect_failures_total.load(Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn run_hooks_logged_does_not_panic_on_failure() {
        let mut registry = SideEffectRegistry::new();
        registry.register("v1", "Test", Arc::new(FailingHook), ErrorPolicy::Fail);

        run_hooks_logged(
            &registry,
            &json!({"apiVersion": "v1", "kind": "Test"}),
            &SideEffectMetrics::new(),
            "test",
        )
        .await;
    }

    #[tokio::test]
    async fn run_hooks_logged_no_increment_on_success() {
        let metrics = SideEffectMetrics::new();
        run_hooks_logged(
            &SideEffectRegistry::new(),
            &json!({"apiVersion": "v1", "kind": "Test"}),
            &metrics,
            "test",
        )
        .await;
        assert_eq!(
            metrics.side_effect_failures_total.load(Ordering::Relaxed),
            0
        );
    }
}
