//! Root composition for post-mutation side effects.

use std::sync::Arc;

use klights_controllers::side_effects::{
    ControllerDispatcherSlot, DefaultSideEffects, PodSideEffectPortsSlot, SideEffectMetrics,
    SideEffectRegistry,
};

/// Construct the root-selected focused ports and hand the complete immutable
/// effect bundle to the controller-owned registration policy.
#[cfg(test)]
pub(crate) fn default_registry(
    metrics: Arc<SideEffectMetrics>,
    services: Option<Arc<dyn klights_network_api::ServiceRouter>>,
    task_supervisor: Option<Arc<klights_supervisor::TaskSupervisor>>,
    db: Option<crate::datastore::DatastoreHandle>,
    identity: Arc<dyn klights_controllers::ControllerIdentityGenerator>,
) -> SideEffectRegistry {
    let db = db.expect("default side-effect registry requires a datastore handle");
    let authority =
        crate::bootstrap::composition_adapters::authority_adapter::always_leader_authority();
    let query = crate::bootstrap::composition_adapters::resource_query_adapter::DatastoreResourceQueryAdapter::new(
        db.clone(), authority.clone(),
    );
    let resource_commands = Arc::new(
        klights_replication::leader_api::EmbeddedLeaderResourceCommand::new(
            Arc::new(
                crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(db.clone()),
            ),
            query,
            authority,
        ),
    );
    default_registry_with_commands(
        metrics,
        services,
        task_supervisor,
        db,
        resource_commands,
        identity,
    )
}

pub(crate) fn default_registry_with_commands(
    metrics: Arc<SideEffectMetrics>,
    services: Option<Arc<dyn klights_network_api::ServiceRouter>>,
    task_supervisor: Option<Arc<klights_supervisor::TaskSupervisor>>,
    db: crate::datastore::DatastoreHandle,
    resource_commands: Arc<dyn klights_leader_api::LeaderResourceCommand>,
    identity: Arc<dyn klights_controllers::ControllerIdentityGenerator>,
) -> SideEffectRegistry {
    let pod_slot = PodSideEffectPortsSlot::new();
    let controller_slot = ControllerDispatcherSlot::new();
    let controller_store = Arc::new(
        crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::new_with_commands(
            db.clone(),
            resource_commands.clone(),
        ),
    );
    let namespace_store = crate::bootstrap::composition_adapters::api_state_adapter::RootNamespaceTerminationStore::new_with_commands(
        db.clone(),
        resource_commands.clone(),
    );
    let namespace_reconciliation =
        crate::bootstrap::composition_adapters::api_state_adapter::RootNamespaceTerminationReconciler::new(namespace_store, metrics);
    let effects = DefaultSideEffects::new(
        klights_controllers::side_effects::apiservice::effect(
            crate::bootstrap::controller_adapters::apiservice_side_effect_adapter::port(db.clone()),
            controller_slot.clone(),
        ),
        klights_controllers::side_effects::daemonset_node::effect(
            crate::bootstrap::controller_adapters::daemonset_node_side_effect_adapter::port(db.clone()),
            controller_slot.clone(),
        ),
        klights_controllers::side_effects::endpoint_mirror::effect(
            crate::bootstrap::controller_adapters::endpoint_mirror_side_effect_adapter::port(controller_store.clone(), identity.clone()),
        ),
        klights_controllers::side_effects::endpoint_slice_sync::effect(services),
        klights_controllers::side_effects::hpa::effect(
            crate::bootstrap::controller_adapters::hpa_side_effect_adapter::port(db.clone()),
            controller_slot.clone(),
        ),
        klights_controllers::side_effects::job::effect(
            crate::bootstrap::controller_adapters::job_side_effect_adapter::port(db.clone()),
            controller_slot.clone(),
        ),
        klights_controllers::side_effects::namespace_termination::effect(namespace_reconciliation),
        klights_controllers::side_effects::node_taint_manager::effect(
            pod_slot.clone(),
            task_supervisor,
            Some(crate::bootstrap::controller_adapters::node_taint_manager_side_effect_adapter::port(
                db.clone(),
            )),
        ),
        klights_controllers::side_effects::pdb::effect(crate::bootstrap::controller_adapters::pdb_side_effect_adapter::port(
            controller_store.clone(),
            pod_slot.clone(),
        )),
        klights_controllers::side_effects::resource_quota::effect(
            crate::bootstrap::controller_adapters::resource_quota_side_effect_adapter::port(
                db.clone(),
                controller_store,
                pod_slot.clone(),
            ),
        ),
        klights_controllers::side_effects::service_account_defaults::effect(
            crate::bootstrap::controller_adapters::service_account_defaults_side_effect_adapter::port(
                db.clone(),
                resource_commands,
                identity,
            ),
        ),
        klights_controllers::side_effects::workload_pod::effect(
            crate::bootstrap::controller_adapters::workload_pod_side_effect_adapter::port(db),
            controller_slot.clone(),
        ),
    );
    klights_controllers::side_effects::default_registry(effects, pod_slot, controller_slot)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Arc;

    #[tokio::test]
    async fn node_side_effect_enqueues_daemonset_key_without_inline_reconcile() {
        let (db, db_handle) =
            crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let task_supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let dispatcher = Arc::new(
            crate::bootstrap::composition_tests::recording_reconcile_sink::recording_reconcile_sink(
            ),
        );
        let registry = default_registry(
            SideEffectMetrics::new(),
            None,
            Some(task_supervisor),
            Some(db_handle),
            crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
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
            dispatcher.pending_keys().await,
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
                klights_cluster_store::ResourceListOptions::all(),
            )
            .await
            .unwrap()
            .items
            .is_empty(),
            "node side effect must not run DaemonSet reconciliation inline"
        );
    }

    #[tokio::test]
    async fn test_default_registry_enqueues_jobs_after_pod_mutation() {
        let (db, db_handle) =
            crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let metrics = SideEffectMetrics::new();
        let registry = default_registry(
            metrics.clone(),
            None,
            None,
            Some(db_handle.clone()),
            crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
        );
        let dispatcher = Arc::new(
            crate::bootstrap::composition_tests::recording_reconcile_sink::recording_reconcile_sink(
            ),
        );
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
            dispatcher.pending_keys().await,
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
        let (db, db_handle) =
            crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let metrics = SideEffectMetrics::new();
        let registry = default_registry(
            metrics.clone(),
            None,
            None,
            Some(db_handle.clone()),
            crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
        );
        let dispatcher = Arc::new(
            crate::bootstrap::composition_tests::recording_reconcile_sink::recording_reconcile_sink(
            ),
        );
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

        let keys = dispatcher.pending_keys().await;
        assert!(
            keys.is_empty(),
            "Pod generic side effects must not enqueue Service reconciles directly"
        );
    }

    #[tokio::test]
    async fn test_endpoint_hooks_do_not_enqueue_service_reconcile() {
        let (_db, db_handle) =
            crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let metrics = SideEffectMetrics::new();
        let registry = default_registry(
            metrics.clone(),
            None,
            None,
            Some(db_handle.clone()),
            crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
        );
        let dispatcher = Arc::new(
            crate::bootstrap::composition_tests::recording_reconcile_sink::recording_reconcile_sink(
            ),
        );
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
            dispatcher.pending_keys().await.is_empty(),
            "Endpoints and EndpointSlice side effects must not feed back into Service reconcile"
        );
    }

    #[tokio::test]
    async fn test_default_registry_enqueues_replicationcontroller_owner_after_pod_mutation() {
        let (db, db_handle) =
            crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let metrics = SideEffectMetrics::new();
        let registry = default_registry(
            metrics.clone(),
            None,
            None,
            Some(db_handle.clone()),
            crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
        );
        let dispatcher = Arc::new(
            crate::bootstrap::composition_tests::recording_reconcile_sink::recording_reconcile_sink(
            ),
        );
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
            dispatcher.pending_keys().await,
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
        let (db, db_handle) =
            crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let metrics = SideEffectMetrics::new();
        let registry = default_registry(
            metrics.clone(),
            None,
            None,
            Some(db_handle.clone()),
            crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
        );
        let dispatcher = Arc::new(
            crate::bootstrap::composition_tests::recording_reconcile_sink::recording_reconcile_sink(
            ),
        );
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
            dispatcher.pending_keys().await,
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
        let (db, db_handle) =
            crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let metrics = SideEffectMetrics::new();
        let registry = default_registry(
            metrics.clone(),
            None,
            None,
            Some(db_handle.clone()),
            crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
        );
        let dispatcher = Arc::new(
            crate::bootstrap::composition_tests::recording_reconcile_sink::recording_reconcile_sink(
            ),
        );
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

        let keys = dispatcher.pending_keys().await;
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
        let (db, db_handle) =
            crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let metrics = SideEffectMetrics::new();
        let registry = default_registry(
            metrics.clone(),
            None,
            None,
            Some(db_handle.clone()),
            crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
        );
        let dispatcher = Arc::new(
            crate::bootstrap::composition_tests::recording_reconcile_sink::recording_reconcile_sink(
            ),
        );
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
            dispatcher.pending_keys().await.contains(
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
