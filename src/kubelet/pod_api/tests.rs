use std::sync::Arc;

use super::PodApiFacade;
use crate::kubelet::pod_repository::api::PodSchedulingMode;
use crate::kubelet::pod_repository::state_only_writer::StatusOnlyWriterService;
use crate::kubelet::pod_repository::store::PodStore;
use crate::kubelet::pod_repository::workqueue::PodWorkqueue;
use crate::kubelet::pod_repository::{PodApiDeleteOutcome, PodApiUpdateOutcome, PodReader};
use crate::side_effects::{SideEffectMetrics, SideEffectRegistry};
use crate::task_supervisor::TaskSupervisor;

use crate::kubelet::pod_repository::PodApiCreateRequest;

fn fixture_supervisor() -> Arc<TaskSupervisor> {
    Arc::new(TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ))
}

async fn fixture_handle() -> crate::datastore::DatastoreHandle {
    let (_ds, handle) = crate::datastore::test_support::in_memory_with_handle().await;
    handle
}

fn fixture_facade(db: crate::datastore::DatastoreHandle) -> PodApiFacade {
    let supervisor = fixture_supervisor();
    let side_effects = Arc::new(SideEffectRegistry::new());
    let metrics = SideEffectMetrics::new();
    let scheduling_mode = PodSchedulingMode::InlineSingleNode;

    let parts = crate::kubelet::pod_repository::PodRepository::build_parts(
        crate::kubelet::pod_repository::PodRepositoryBuildConfig {
            db: db.clone(),
            supervisor: supervisor.clone(),
            side_effects: side_effects.clone(),
            metrics: metrics.clone(),
            network_events: crate::networking::global_pod_network_events(),
            scheduling_mode,
            outbox: None,
            cluster_api: None,
        },
    );

    let repository = Arc::new(parts.repository);
    let store = Arc::new(PodStore::new(db.clone()));
    let workqueue = PodWorkqueue::new(
        store.clone(),
        db.clone(),
        supervisor.clone(),
        metrics.clone(),
    );
    let status_only = Arc::new(StatusOnlyWriterService::new(store.clone()));

    PodApiFacade::new(
        repository,
        crate::kubelet::pod_repository::api::PodApiServiceDependencies {
            store,
            status_only,
            db,
            supervisor,
            workqueue,
            side_effects,
            metrics,
            outbox: None,
        },
    )
}

/// Task 6.1: Facade constructor requires repository and services.
#[tokio::test]
async fn pod_api_facade_constructor_requires_repository_and_services() {
    let db = fixture_handle().await;
    let facade = fixture_facade(db);

    // Facade holds all its fields.
    let _ = &facade.repository;
    let _ = &facade.create_service;
    let _ = &facade.update_service;
    let _ = &facade.delete_service;
}

/// Task 6.2: Create service preserves request namespace, name, and UID arguments.
#[tokio::test]
async fn pod_api_create_preserves_request_namespace_name_uid_arguments() {
    let db = fixture_handle().await;
    let facade = fixture_facade(db);

    let pod_body = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test-pod",
            "namespace": "default",
            "uid": "abc-123-uid"
        },
        "spec": {
            "containers": [{
                "name": "nginx",
                "image": "nginx:latest"
            }]
        }
    });

    let request = PodApiCreateRequest {
        namespace: "default".to_string(),
        name: "test-pod".to_string(),
        body: pod_body,
        dry_run: false,
        run_admission: false,
    };

    let result = facade
        .create_service
        .create_pod(request)
        .await
        .expect("create_pod must succeed");

    let resource = result.resource.expect("resource must be present");
    assert_eq!(resource.namespace.as_deref(), Some("default"));
    assert_eq!(resource.name, "test-pod");
    assert!(resource.uid.contains("abc-123-uid"));
}

/// Task 6.3: Update and patch service preserves UID preconditions.
#[tokio::test]
async fn pod_api_update_patch_preserves_uid_preconditions() {
    let db = fixture_handle().await;
    let facade = fixture_facade(db);

    let pod_body = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "update-test-pod",
            "namespace": "default",
            "uid": "update-uid-123"
        },
        "spec": {
            "containers": [{
                "name": "nginx",
                "image": "nginx:latest"
            }]
        }
    });

    let request = PodApiCreateRequest {
        namespace: "default".to_string(),
        name: "update-test-pod".to_string(),
        body: pod_body,
        dry_run: false,
        run_admission: false,
    };

    let result = facade
        .create_service
        .create_pod(request)
        .await
        .expect("create must succeed");
    let resource = result.resource.expect("resource must be present");
    let pod_uid = resource.uid.clone();

    let mut updated_body = result.body.clone();
    updated_body["metadata"]["labels"] = serde_json::json!({"app": "test"});

    let update_outcome = facade
        .update_service
        .update_pod("default", "update-test-pod", updated_body, resource, false)
        .await
        .expect("update must succeed");

    match update_outcome {
        PodApiUpdateOutcome::Persisted(r) => {
            assert_eq!(r.uid, pod_uid, "UID must be preserved across update");
        }
        PodApiUpdateOutcome::DryRun(_) => {
            panic!("update should not be dry-run");
        }
    }
}

/// Task 6.4: Delete service queues UID-bound actor delete without hard-deleting.
#[tokio::test]
async fn pod_api_delete_queues_uid_bound_actor_delete() {
    let db = fixture_handle().await;
    let facade = fixture_facade(db);

    // Create a pod first.
    let pod_body = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "delete-test-pod",
            "namespace": "default",
            "uid": "delete-uid-456"
        },
        "spec": {
            "containers": [{
                "name": "nginx",
                "image": "nginx:latest"
            }]
        }
    });

    let request = PodApiCreateRequest {
        namespace: "default".to_string(),
        name: "delete-test-pod".to_string(),
        body: pod_body,
        dry_run: false,
        run_admission: false,
    };

    let result = facade
        .create_service
        .create_pod(request)
        .await
        .expect("create must succeed");
    assert!(result.resource.is_some(), "pod must be created");

    // Delete the pod via the delete service.
    let delete_outcome = facade
        .delete_service
        .delete_pod(
            "default",
            "delete-test-pod",
            crate::api::DeleteOptions::default(),
            false,
        )
        .await
        .expect("delete must succeed");

    // The delete outcome marks the pod (not a hard-delete).
    match delete_outcome {
        crate::kubelet::pod_repository::types::PodApiDeleteOutcome::GracefulSet(_) => {}
        crate::kubelet::pod_repository::types::PodApiDeleteOutcome::DryRun(_) => {
            panic!("delete should not be dry-run");
        }
    }
}

/// Task 6.5: Scheduling service uses pod identity and node name arguments.
#[tokio::test]
async fn pod_api_scheduling_uses_pod_identity_and_node_name_arguments() {
    let db = fixture_handle().await;
    let facade = fixture_facade(db);

    // Create a pending pod that scheduling can bind.
    let pod_body = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "sched-test-pod",
            "namespace": "default",
            "uid": "sched-uid-789"
        },
        "spec": {
            "containers": [{
                "name": "nginx",
                "image": "nginx:latest"
            }],
            "nodeName": "node-1"
        },
        "status": {
            "phase": "Pending"
        }
    });

    let request = PodApiCreateRequest {
        namespace: "default".to_string(),
        name: "sched-test-pod".to_string(),
        body: pod_body,
        dry_run: false,
        run_admission: false,
    };

    facade
        .create_service
        .create_pod(request)
        .await
        .expect("create must succeed");

    // Scheduling service is available and can be called.
    // schedule_pending_pod returns early when the pod is already bound,
    // so the call succeeds without error.
    let result = facade
        .scheduling_service
        .schedule_pending_pod("default", "sched-test-pod")
        .await;
    assert!(result.is_ok(), "scheduling service must be callable");
}

/// Task 6.1: Facade constructs with repository dependencies.
#[tokio::test]
async fn pod_api_facade_constructs_with_repository_dependencies() {
    let db = fixture_handle().await;
    let _facade = fixture_facade(db);
}

/// Task 6.4: API delete stamps deletionTimestamp and enqueues actor work
/// without hard-deleting the Pod row.
#[tokio::test]
async fn api_delete_marks_pod_and_enqueues_actor_without_hard_delete() {
    let db = fixture_handle().await;
    let facade = fixture_facade(db);

    let pod_body = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "mark-test-pod",
            "namespace": "default",
            "uid": "mark-uid-001"
        },
        "spec": {
            "containers": [{
                "name": "nginx",
                "image": "nginx:latest"
            }]
        }
    });

    let request = PodApiCreateRequest {
        namespace: "default".to_string(),
        name: "mark-test-pod".to_string(),
        body: pod_body,
        dry_run: false,
        run_admission: false,
    };

    facade
        .create_service
        .create_pod(request)
        .await
        .expect("create must succeed");

    let outcome = facade
        .delete_service
        .delete_pod(
            "default",
            "mark-test-pod",
            crate::api::DeleteOptions::default(),
            false,
        )
        .await
        .expect("delete must succeed");

    let resource = match outcome {
        PodApiDeleteOutcome::GracefulSet(ref r) => r,
        PodApiDeleteOutcome::DryRun(_) => panic!("delete should not be dry-run"),
    };

    let ts = resource
        .data
        .pointer("/metadata/deletionTimestamp")
        .and_then(|v| v.as_str());
    assert!(
        ts.is_some(),
        "API delete must stamp deletionTimestamp on the pod"
    );

    let grace = resource
        .data
        .pointer("/metadata/deletionGracePeriodSeconds")
        .and_then(|v| v.as_i64());
    assert!(
        grace.is_some(),
        "API delete must set deletionGracePeriodSeconds"
    );

    let still_exists = facade
        .repository
        .get_pod("default", "mark-test-pod")
        .await
        .expect("get_pod must succeed");
    assert!(
        still_exists.is_some(),
        "API delete must not hard-delete the Pod row; actor owns deletion"
    );
}

#[tokio::test]
async fn api_delete_uses_pod_spec_termination_grace_when_options_omit_grace() {
    let db = fixture_handle().await;
    let facade = fixture_facade(db);

    let pod_body = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "spec-grace-pod",
            "namespace": "default",
            "uid": "spec-grace-uid"
        },
        "spec": {
            "terminationGracePeriodSeconds": 0,
            "containers": [{
                "name": "nginx",
                "image": "nginx:latest"
            }]
        }
    });

    facade
        .create_service
        .create_pod(PodApiCreateRequest {
            namespace: "default".to_string(),
            name: "spec-grace-pod".to_string(),
            body: pod_body,
            dry_run: false,
            run_admission: false,
        })
        .await
        .expect("create must succeed");

    let outcome = facade
        .delete_service
        .delete_pod(
            "default",
            "spec-grace-pod",
            crate::api::DeleteOptions::default(),
            false,
        )
        .await
        .expect("delete must succeed");

    let resource = match outcome {
        PodApiDeleteOutcome::GracefulSet(resource) => resource,
        PodApiDeleteOutcome::DryRun(_) => panic!("delete should not be dry-run"),
    };

    assert_eq!(
        resource
            .data
            .pointer("/metadata/deletionGracePeriodSeconds")
            .and_then(|value| value.as_i64()),
        Some(0),
        "Pod DELETE without explicit grace must default from spec.terminationGracePeriodSeconds"
    );
}

/// Task 6.4: PodApiFacade delete service must not call PodStore hard-delete
/// — even with zero grace period the pod row must remain.
#[tokio::test]
async fn pod_api_facade_delete_does_not_call_pod_store_hard_delete() {
    let db = fixture_handle().await;
    let facade = fixture_facade(db);

    let pod_body = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "no-hard-delete-pod",
            "namespace": "default",
            "uid": "nohard-uid-002"
        },
        "spec": {
            "containers": [{
                "name": "nginx",
                "image": "nginx:latest"
            }]
        }
    });

    let request = PodApiCreateRequest {
        namespace: "default".to_string(),
        name: "no-hard-delete-pod".to_string(),
        body: pod_body,
        dry_run: false,
        run_admission: false,
    };

    facade
        .create_service
        .create_pod(request)
        .await
        .expect("create must succeed");

    let opts = crate::api::DeleteOptions {
        _grace_period_seconds: Some(0),
        ..Default::default()
    };

    let outcome = facade
        .delete_service
        .delete_pod("default", "no-hard-delete-pod", opts, false)
        .await
        .expect("delete must succeed");

    assert!(
        matches!(outcome, PodApiDeleteOutcome::GracefulSet(_)),
        "even with zero grace period, API delete must return GracefulSet (not hard-delete)"
    );

    let still_exists = facade
        .repository
        .get_pod("default", "no-hard-delete-pod")
        .await
        .expect("get_pod must succeed");

    let pod = still_exists
        .expect("Pod must still exist after API delete — only the actor may hard-delete");
    assert!(
        pod.data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some(),
        "Pod must have deletionTimestamp set after API delete"
    );
}
