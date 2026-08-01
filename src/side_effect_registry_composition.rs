//! Root composition for post-mutation side effects.

use klights_controllers::side_effects::{ErrorPolicy, SideEffectMetrics, SideEffectRegistry};

/// Run all registered hooks for `resource`, logging and counting any failure.
///
/// The HTTP handler must already have returned a success response — this
/// function never propagates the error to the caller; it only makes failures
/// observable via structured logs and the `metrics` counters.
pub async fn run_hooks_logged(
    registry: &SideEffectRegistry,
    resource: &serde_json::Value,
    metrics: &SideEffectMetrics,
    context: &'static str,
) {
    let api_version = resource
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let kind = resource
        .get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let namespace = resource
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .map(|value| value.to_string());
    let name = resource
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();

    let (failures, failed) = registry.run_hooks_collect_failures(resource).await;

    for failure in &failures {
        metrics.record_recent_failure(klights_controllers::side_effects::SideEffectFailureEntry {
            api_version: api_version.clone(),
            kind: kind.clone(),
            namespace: namespace.clone(),
            name: name.clone(),
            hook: failure.hook.to_string(),
            context: context.to_string(),
            error: failure.error.clone(),
        });
    }

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
        return;
    }

    if !failures.is_empty() {
        tracing::warn!(
            context,
            side_effect_failures = failures.len(),
            "side-effect hooks failed with non-fatal policy"
        );
    }
}

/// Run all registered delete hooks for `resource`, logging and counting any
/// failure without propagating it back to the already-successful mutation.
pub async fn run_delete_hooks_logged(
    registry: &SideEffectRegistry,
    resource: &serde_json::Value,
    metrics: &SideEffectMetrics,
    context: &'static str,
) {
    let api_version = resource
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let kind = resource
        .get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let namespace = resource
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .map(|value| value.to_string());
    let name = resource
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();

    let (failures, failed) = registry.run_delete_hooks_collect_failures(resource).await;

    for failure in &failures {
        metrics.record_recent_failure(klights_controllers::side_effects::SideEffectFailureEntry {
            api_version: api_version.clone(),
            kind: kind.clone(),
            namespace: namespace.clone(),
            name: name.clone(),
            hook: failure.hook.to_string(),
            context: context.to_string(),
            error: failure.error.clone(),
        });
    }

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
        return;
    }

    if !failures.is_empty() {
        tracing::warn!(
            context,
            side_effect_failures = failures.len(),
            "delete side-effect hooks failed with non-fatal policy"
        );
    }
}

/// Run one registered hook by name for `resource`, logging and counting any
/// failure with the same policy as `run_hooks_logged`.
pub async fn run_named_hook_logged(
    registry: &SideEffectRegistry,
    resource: &serde_json::Value,
    metrics: &SideEffectMetrics,
    hook_name: &'static str,
    context: &'static str,
) {
    let api_version = resource
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let kind = resource
        .get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let namespace = resource
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .map(|value| value.to_string());
    let name = resource
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();

    let (failures, failed) = registry
        .run_named_hook_collect_failures(resource, hook_name)
        .await;

    for failure in &failures {
        metrics.record_recent_failure(klights_controllers::side_effects::SideEffectFailureEntry {
            api_version: api_version.clone(),
            kind: kind.clone(),
            namespace: namespace.clone(),
            name: name.clone(),
            hook: failure.hook.to_string(),
            context: context.to_string(),
            error: failure.error.clone(),
        });
    }

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
        return;
    }

    if !failures.is_empty() {
        tracing::warn!(
            context,
            hook = hook_name,
            side_effect_failures = failures.len(),
            "side-effect hook failed with non-fatal policy"
        );
    }
}

/// Build the default side-effect registry used by both the production
/// bootstrap and integration test fixtures.  Keeping the registration set
/// in one place is what guarantees `cargo test` exercises the same hook
/// fan-out the running server does.
///
/// `metrics` is wired into hooks (currently `namespace_termination`) that
/// need to increment per-failure-mode counters from inside their `apply`
/// path. Test fixtures pass a fresh `SideEffectMetrics::new()`; production
/// passes the same `Arc<SideEffectMetrics>` that lives on `ApiState`.
pub fn default_registry(
    metrics: std::sync::Arc<SideEffectMetrics>,
    services: Option<std::sync::Arc<dyn klights_network_api::ServiceRouter>>,
    task_supervisor: Option<std::sync::Arc<klights_supervisor::TaskSupervisor>>,
    db: Option<crate::datastore::DatastoreHandle>,
) -> SideEffectRegistry {
    let db = db.expect("default side-effect registry requires a datastore handle");
    let mut registry = SideEffectRegistry::new();
    let pod_slot = registry.pod_ports_slot();
    let controller_slot = registry.controller_dispatcher_slot();
    registry.register(
        "v1",
        "Endpoints",
        crate::endpoint_mirror_side_effect_adapter::effect(db.clone()),
        ErrorPolicy::Warn,
    );
    registry.register(
        "discovery.k8s.io/v1",
        "EndpointSlice",
        crate::endpoint_slice_sync_side_effect_adapter::effect(services),
        ErrorPolicy::Warn,
    );
    let rq_effect = crate::resource_quota_side_effect_adapter::effect(db.clone(), pod_slot.clone());
    for (api_version, kind) in [
        ("v1", "Pod"),
        ("v1", "ConfigMap"),
        ("v1", "Secret"),
        ("v1", "PersistentVolumeClaim"),
        ("v1", "ServiceAccount"),
        ("v1", "Service"),
        ("v1", "ResourceQuota"),
        ("v1", "LimitRange"),
        ("v1", "ReplicationController"),
        ("apps/v1", "Deployment"),
        ("apps/v1", "ReplicaSet"),
        ("apps/v1", "StatefulSet"),
        ("apps/v1", "DaemonSet"),
        ("batch/v1", "Job"),
        ("batch/v1", "CronJob"),
        ("policy/v1", "PodDisruptionBudget"),
    ] {
        registry.register(api_version, kind, rq_effect.clone(), ErrorPolicy::Warn);
    }
    registry.register(
        "v1",
        "ServiceAccount",
        crate::service_account_defaults_side_effect_adapter::effect(db.clone()),
        ErrorPolicy::Warn,
    );
    registry.register(
        "v1",
        "Pod",
        crate::workload_pod_side_effect_adapter::effect(db.clone(), controller_slot.clone()),
        ErrorPolicy::Warn,
    );
    registry.register(
        "v1",
        "Pod",
        crate::job_side_effect_adapter::effect(db.clone(), controller_slot.clone()),
        ErrorPolicy::Warn,
    );
    registry.register(
        "v1",
        "Pod",
        crate::pdb_side_effect_adapter::effect(db.clone(), pod_slot.clone()),
        ErrorPolicy::Warn,
    );
    registry.register(
        "v1",
        "Pod",
        crate::namespace_termination_adapter::effect(db.clone(), metrics),
        ErrorPolicy::Warn,
    );
    let hpa_effect = crate::hpa_side_effect_adapter::effect(db.clone(), controller_slot.clone());
    for (api_version, kind) in [
        ("v1", "Pod"),
        ("v1", "ReplicationController"),
        ("apps/v1", "Deployment"),
        ("apps/v1", "ReplicaSet"),
        ("apps/v1", "StatefulSet"),
    ] {
        registry.register(api_version, kind, hpa_effect.clone(), ErrorPolicy::Warn);
    }
    registry.register(
        "v1",
        "Node",
        crate::daemonset_node_side_effect_adapter::effect(db.clone(), controller_slot),
        ErrorPolicy::Warn,
    );
    let apiservice_effect = crate::apiservice_side_effect_adapter::effect(
        db.clone(),
        registry.controller_dispatcher_slot(),
    );
    registry.register(
        "apiregistration.k8s.io/v1",
        "APIService",
        apiservice_effect.clone(),
        ErrorPolicy::Warn,
    );
    registry.register(
        "v1",
        "Service",
        apiservice_effect.clone(),
        ErrorPolicy::Warn,
    );
    registry.register(
        "v1",
        "Endpoints",
        apiservice_effect.clone(),
        ErrorPolicy::Warn,
    );
    registry.register(
        "discovery.k8s.io/v1",
        "EndpointSlice",
        apiservice_effect,
        ErrorPolicy::Warn,
    );
    registry.register(
        "v1",
        "Node",
        crate::node_taint_manager_side_effect_adapter::node_taint_manager(
            pod_slot,
            task_supervisor,
            Some(db),
        ),
        ErrorPolicy::Warn,
    );
    registry
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use klights_controllers::side_effects::SideEffect;
    use serde_json::json;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    struct FailingHook;

    #[async_trait]
    impl SideEffect for FailingHook {
        fn name(&self) -> &'static str {
            "failing_hook"
        }

        async fn apply(&self, _resource: &serde_json::Value) -> anyhow::Result<()> {
            anyhow::bail!("intentional failure")
        }
    }

    #[tokio::test]
    async fn node_side_effect_enqueues_daemonset_key_without_inline_reconcile() {
        let (db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
        let task_supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let service_ipam = Arc::new(crate::controllers::service::ServiceIpam::new(
            "10.43.128.0/17",
        ));
        let dispatcher = Arc::new(crate::controllers::ControllerDispatcher::new(service_ipam));
        let registry = default_registry(
            SideEffectMetrics::new(),
            None,
            Some(task_supervisor),
            Some(db_handle),
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

        registry.run_hooks(&node.data).await.unwrap();

        assert_eq!(
            dispatcher.queued_reconcile_keys_for_test().await,
            vec![klights_reconcile_api::ReconcileKey::namespaced(
                "apps/v1",
                "DaemonSet",
                "default",
                "daemon-set"
            )],
            "node side effect should enqueue the affected daemonset"
        );
        assert!(
            db.list_resources(
                "v1",
                "Pod",
                Some("default"),
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .unwrap()
            .items
            .is_empty(),
            "node side effect must not run DaemonSet reconciliation inline"
        );
    }

    #[tokio::test]
    async fn test_run_hooks_logged_increments_counter_on_failure() {
        let mut registry = SideEffectRegistry::new();
        registry.register(
            "v1",
            "Test",
            Arc::new(FailingHook) as Arc<dyn SideEffect>,
            ErrorPolicy::Fail,
        );

        let metrics = SideEffectMetrics::new();
        let resource = json!({"apiVersion": "v1", "kind": "Test"});

        run_hooks_logged(&registry, &resource, &metrics, "test").await;

        assert_eq!(
            metrics.side_effect_failures_total.load(Ordering::Relaxed),
            1,
            "counter must increment on hook failure"
        );
    }

    #[tokio::test]
    async fn test_run_hooks_logged_does_not_panic_on_failure() {
        let mut registry = SideEffectRegistry::new();
        registry.register(
            "v1",
            "Test",
            Arc::new(FailingHook) as Arc<dyn SideEffect>,
            ErrorPolicy::Fail,
        );

        let metrics = SideEffectMetrics::new();
        let resource = json!({"apiVersion": "v1", "kind": "Test"});

        // Must complete without panicking even when the hook fails.
        run_hooks_logged(&registry, &resource, &metrics, "test").await;
    }

    #[tokio::test]
    async fn test_run_hooks_logged_no_increment_on_success() {
        let registry = SideEffectRegistry::new(); // no hooks registered
        let metrics = SideEffectMetrics::new();
        let resource = json!({"apiVersion": "v1", "kind": "Test"});

        run_hooks_logged(&registry, &resource, &metrics, "test").await;

        assert_eq!(
            metrics.side_effect_failures_total.load(Ordering::Relaxed),
            0,
            "counter must stay zero when no hooks fail"
        );
    }

    #[tokio::test]
    async fn test_default_registry_enqueues_jobs_after_pod_mutation() {
        let (db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
        let metrics = SideEffectMetrics::new();
        let registry = default_registry(metrics.clone(), None, None, Some(db_handle.clone()));
        let service_ipam = Arc::new(crate::controllers::service::ServiceIpam::new(
            "10.43.128.0/17",
        ));
        let dispatcher = Arc::new(crate::controllers::ControllerDispatcher::new(service_ipam));
        registry.set_controller_dispatcher(dispatcher.clone());

        db.create_resource(
            "batch/v1",
            "Job",
            Some("default"),
            "adopt-release",
            json!({
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {
                    "name": "adopt-release",
                    "namespace": "default",
                    "uid": "job-uid"
                },
                "spec": {
                    "parallelism": 1,
                    "completions": 1,
                    "selector": {
                        "matchLabels": {
                            "job": "adopt-release"
                        }
                    },
                    "template": {
                        "metadata": {
                            "labels": {
                                "job": "adopt-release"
                            }
                        },
                        "spec": {
                            "restartPolicy": "Never",
                            "containers": [{
                                "name": "main",
                                "image": "registry.k8s.io/pause:3.10.1"
                            }]
                        }
                    }
                }
            }),
        )
        .await
        .unwrap();

        let pod = db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "adopt-release-orphan",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": "adopt-release-orphan",
                        "namespace": "default",
                        "labels": {
                            "job": "adopt-release"
                        }
                    },
                    "spec": {
                        "nodeName": "test-node",
                        "containers": [{
                            "name": "main",
                            "image": "registry.k8s.io/pause:3.10.1"
                        }]
                    },
                    "status": {
                        "phase": "Running"
                    }
                }),
            )
            .await
            .unwrap();

        registry.run_hooks(&pod.data).await.unwrap();

        assert_eq!(
            dispatcher.queued_reconcile_keys_for_test().await,
            vec![klights_reconcile_api::ReconcileKey::namespaced(
                "batch/v1",
                "Job",
                "default",
                "adopt-release"
            )],
            "pod mutation should enqueue the matching Job for later adoption"
        );

        let updated = db
            .get_resource("v1", "Pod", Some("default"), "adopt-release-orphan")
            .await
            .unwrap()
            .expect("pod should still exist");
        assert!(
            updated
                .data
                .pointer("/metadata/ownerReferences")
                .and_then(|v| v.as_array())
                .is_none(),
            "side effect must not adopt the pod inline"
        );
    }

    #[tokio::test]
    async fn service_pod_side_effect_not_registered_for_generic_pod_hook() {
        let (db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
        let metrics = SideEffectMetrics::new();
        let registry = default_registry(metrics.clone(), None, None, Some(db_handle.clone()));
        let service_ipam = Arc::new(crate::controllers::service::ServiceIpam::new(
            "10.43.128.0/17",
        ));
        let dispatcher = Arc::new(crate::controllers::ControllerDispatcher::new(service_ipam));
        registry.set_controller_dispatcher(dispatcher.clone());

        db.create_resource(
            "v1",
            "Service",
            Some("default"),
            "web",
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": "web", "namespace": "default"},
                "spec": {
                    "selector": {"app": "web"},
                    "ports": [{"port": 80, "targetPort": 8080}]
                }
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "v1",
            "Service",
            Some("default"),
            "api",
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": "api", "namespace": "default"},
                "spec": {
                    "selector": {"app": "api"},
                    "ports": [{"port": 80, "targetPort": 8080}]
                }
            }),
        )
        .await
        .unwrap();

        let pod = db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "web-pod",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": "web-pod",
                        "namespace": "default",
                        "labels": {"app": "web"}
                    },
                    "spec": {"containers": [{"name": "c", "image": "nginx"}]},
                    "status": {
                        "phase": "Running",
                        "podIP": "10.42.0.20",
                        "conditions": [{"type": "Ready", "status": "True"}]
                    }
                }),
            )
            .await
            .unwrap();

        registry.run_hooks(&pod.data).await.unwrap();

        let keys = dispatcher.queued_reconcile_keys_for_test().await;
        assert!(
            keys.is_empty(),
            "Pod generic side effects must not enqueue Service reconciles directly"
        );
    }

    #[tokio::test]
    async fn test_endpoint_hooks_do_not_enqueue_service_reconcile() {
        let (_db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
        let metrics = SideEffectMetrics::new();
        let registry = default_registry(metrics.clone(), None, None, Some(db_handle.clone()));
        let service_ipam = Arc::new(crate::controllers::service::ServiceIpam::new(
            "10.43.128.0/17",
        ));
        let dispatcher = Arc::new(crate::controllers::ControllerDispatcher::new(service_ipam));
        registry.set_controller_dispatcher(dispatcher.clone());

        let endpoints = json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "web", "namespace": "default"},
            "subsets": [{
                "addresses": [{
                    "ip": "10.42.0.20",
                    "targetRef": {
                        "kind": "Pod",
                        "namespace": "default",
                        "name": "web-pod",
                        "uid": "web-pod-uid"
                    }
                }],
                "ports": [{"port": 80, "protocol": "TCP"}]
            }]
        });

        registry.run_hooks(&endpoints).await.unwrap();

        assert!(
            dispatcher.queued_reconcile_keys_for_test().await.is_empty(),
            "Endpoints and EndpointSlice side effects must not feed back into Service reconcile"
        );
    }

    #[tokio::test]
    async fn test_default_registry_enqueues_replicationcontroller_owner_after_pod_mutation() {
        let (db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
        let metrics = SideEffectMetrics::new();
        let registry = default_registry(metrics.clone(), None, None, Some(db_handle.clone()));
        let service_ipam = Arc::new(crate::controllers::service::ServiceIpam::new(
            "10.43.128.0/17",
        ));
        let dispatcher = Arc::new(crate::controllers::ControllerDispatcher::new(service_ipam));
        registry.set_controller_dispatcher(dispatcher.clone());

        db.create_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "pod-release",
            json!({
                "apiVersion": "v1",
                "kind": "ReplicationController",
                "metadata": {
                    "name": "pod-release",
                    "namespace": "default",
                    "uid": "rc-uid"
                },
                "spec": {
                    "replicas": 1,
                    "selector": {
                        "name": "pod-release"
                    },
                    "template": {
                        "metadata": {
                            "labels": {
                                "name": "pod-release"
                            }
                        },
                        "spec": {
                            "containers": [{
                                "name": "main",
                                "image": "registry.k8s.io/pause:3.10.1"
                            }]
                        }
                    }
                }
            }),
        )
        .await
        .unwrap();

        let pod = db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "pod-release-owned",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": "pod-release-owned",
                        "namespace": "default",
                        "labels": {
                            "name": "no-longer-matches"
                        },
                        "ownerReferences": [{
                            "apiVersion": "v1",
                            "kind": "ReplicationController",
                            "name": "pod-release",
                            "uid": "rc-uid",
                            "controller": true
                        }]
                    },
                    "spec": {
                        "nodeName": "test-node",
                        "containers": [{
                            "name": "main",
                            "image": "registry.k8s.io/pause:3.10.1"
                        }]
                    },
                    "status": {
                        "phase": "Running"
                    }
                }),
            )
            .await
            .unwrap();

        registry.run_hooks(&pod.data).await.unwrap();

        assert_eq!(
            dispatcher.queued_reconcile_keys_for_test().await,
            vec![klights_reconcile_api::ReconcileKey::namespaced(
                "v1",
                "ReplicationController",
                "default",
                "pod-release"
            )],
            "pod label mutations must enqueue the owning RC so reconcile can release no-longer-matching pods"
        );

        let updated = db
            .get_resource("v1", "Pod", Some("default"), "pod-release-owned")
            .await
            .unwrap()
            .expect("pod should still exist");
        assert_eq!(
            updated
                .data
                .pointer("/metadata/ownerReferences/0/uid")
                .and_then(|v| v.as_str()),
            Some("rc-uid"),
            "side effect must not release the pod inline"
        );
    }

    #[tokio::test]
    async fn test_default_registry_enqueues_matching_replicaset_for_orphan_pod_mutation() {
        let (db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
        let metrics = SideEffectMetrics::new();
        let registry = default_registry(metrics.clone(), None, None, Some(db_handle.clone()));
        let service_ipam = Arc::new(crate::controllers::service::ServiceIpam::new(
            "10.43.128.0/17",
        ));
        let dispatcher = Arc::new(crate::controllers::ControllerDispatcher::new(service_ipam));
        registry.set_controller_dispatcher(dispatcher.clone());

        db.create_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "pod-adoption-release",
            json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "metadata": {
                    "name": "pod-adoption-release",
                    "namespace": "default",
                    "uid": "rs-uid"
                },
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"name": "pod-adoption-release"}},
                    "template": {
                        "metadata": {"labels": {"name": "pod-adoption-release"}},
                        "spec": {
                            "containers": [{
                                "name": "main",
                                "image": "registry.k8s.io/pause:3.10.1"
                            }]
                        }
                    }
                }
            }),
        )
        .await
        .unwrap();

        let pod = db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "pod-adoption-release",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": "pod-adoption-release",
                        "namespace": "default",
                        "labels": {"name": "pod-adoption-release"}
                    },
                    "spec": {
                        "nodeName": "test-node",
                        "containers": [{
                            "name": "main",
                            "image": "registry.k8s.io/pause:3.10.1"
                        }]
                    },
                    "status": {"phase": "Running"}
                }),
            )
            .await
            .unwrap();

        registry.run_hooks(&pod.data).await.unwrap();

        assert_eq!(
            dispatcher.queued_reconcile_keys_for_test().await,
            vec![klights_reconcile_api::ReconcileKey::namespaced(
                "apps/v1",
                "ReplicaSet",
                "default",
                "pod-adoption-release"
            )],
            "orphan pod label mutations must enqueue matching ReplicaSets for adoption"
        );
    }

    #[tokio::test]
    async fn test_default_registry_enqueues_replicaset_parent_deployment_after_pod_mutation() {
        let (db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
        let metrics = SideEffectMetrics::new();
        let registry = default_registry(metrics.clone(), None, None, Some(db_handle.clone()));
        let service_ipam = Arc::new(crate::controllers::service::ServiceIpam::new(
            "10.43.128.0/17",
        ));
        let dispatcher = Arc::new(crate::controllers::ControllerDispatcher::new(service_ipam));
        registry.set_controller_dispatcher(dispatcher.clone());

        db.create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "web-recreate",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "web-recreate",
                    "namespace": "default",
                    "uid": "deploy-recreate-uid"
                },
                "spec": {"replicas": 1}
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "web-rs",
            json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "metadata": {
                    "name": "web-rs",
                    "namespace": "default",
                    "uid": "rs-web-uid",
                    "ownerReferences": [{
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "name": "web-recreate",
                        "uid": "deploy-recreate-uid",
                        "controller": true
                    }]
                },
                "spec": {
                    "replicas": 0,
                    "selector": {"matchLabels": {"app": "web"}},
                    "template": {
                        "metadata": {"labels": {"app": "web"}},
                        "spec": {"containers": [{"name": "c", "image": "nginx"}]}
                    }
                }
            }),
        )
        .await
        .unwrap();

        let pod = db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "web-pod",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": "web-pod",
                        "namespace": "default",
                        "labels": {"app": "web"},
                        "ownerReferences": [{
                            "apiVersion": "apps/v1",
                            "kind": "ReplicaSet",
                            "name": "web-rs",
                            "uid": "rs-web-uid",
                            "controller": true
                        }]
                    },
                    "spec": {"containers": [{"name": "c", "image": "nginx"}]},
                    "status": {"phase": "Running"}
                }),
            )
            .await
            .unwrap();

        registry.run_hooks(&pod.data).await.unwrap();

        let keys = dispatcher.queued_reconcile_keys_for_test().await;
        assert!(
            keys.iter().any(|key| {
                key.api_version() == "apps/v1"
                    && key.kind() == "ReplicaSet"
                    && key.namespace() == Some("default")
                    && key.name() == "web-rs"
            }),
            "pod mutation must still enqueue the owning ReplicaSet"
        );
        assert!(
            keys.iter().any(|key| {
                key.api_version() == "apps/v1"
                    && key.kind() == "Deployment"
                    && key.namespace() == Some("default")
                    && key.name() == "web-recreate"
            }),
            "ReplicaSet-owned Pod mutations must enqueue the parent Deployment from the central workload side effect"
        );
    }

    #[tokio::test]
    async fn test_default_registry_enqueues_job_without_explicit_selector_after_pod_mutation() {
        let (db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
        let metrics = SideEffectMetrics::new();
        let registry = default_registry(metrics.clone(), None, None, Some(db_handle.clone()));
        let service_ipam = Arc::new(crate::controllers::service::ServiceIpam::new(
            "10.43.128.0/17",
        ));
        let dispatcher = Arc::new(crate::controllers::ControllerDispatcher::new(service_ipam));
        registry.set_controller_dispatcher(dispatcher.clone());

        db.create_resource(
            "batch/v1",
            "Job",
            Some("default"),
            "adopt-release",
            json!({
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {
                    "name": "adopt-release",
                    "namespace": "default",
                    "uid": "job-uid"
                },
                "spec": {
                    "parallelism": 1,
                    "completions": 2,
                    "template": {
                        "metadata": {"labels": {"job": "adopt-release"}},
                        "spec": {
                            "restartPolicy": "Never",
                            "containers": [{
                                "name": "main",
                                "image": "registry.k8s.io/pause:3.10.1"
                            }]
                        }
                    }
                }
            }),
        )
        .await
        .unwrap();

        let pod = db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "adopt-release-orphan",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": "adopt-release-orphan",
                        "namespace": "default",
                        "labels": {"job": "adopt-release"}
                    },
                    "spec": {
                        "nodeName": "test-node",
                        "containers": [{
                            "name": "main",
                            "image": "registry.k8s.io/pause:3.10.1"
                        }]
                    },
                    "status": {"phase": "Running"}
                }),
            )
            .await
            .unwrap();

        registry.run_hooks(&pod.data).await.unwrap();

        assert!(
            dispatcher.queued_reconcile_keys_for_test().await.contains(
                &klights_reconcile_api::ReconcileKey::namespaced(
                    "batch/v1",
                    "Job",
                    "default",
                    "adopt-release"
                )
            ),
            "orphan pod mutations must enqueue matching Jobs even when the Job relies on template labels for its selector"
        );
    }
}
