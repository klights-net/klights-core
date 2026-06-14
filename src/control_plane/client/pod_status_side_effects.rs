//! Pod-status side-effect dispatch shared by every outbox apply path.
//!
//! Every successful UpdateStatus or DeleteResource apply for a `v1/Pod` must
//! enqueue workload-owner, Job, and Service reconcile keys onto the leader's
//! `ControllerDispatcher`. Without this the leader's controllers never see
//! the change — Endpoints/EndpointSlices stay empty, Deployment / StatefulSet
//! rollout never observes pod readiness, Job `.status.ready` stays stale, and
//! StatefulSet ordinal recreate stalls after a Pod is finalized off the worker.
//!
//! Two callers wire into this:
//!
//!  * `LocalApiClient::apply_outbox` — leader-bundled-worker writes through
//!    the in-process outbox.
//!  * `replication::grpc::server::Replication::apply_outbox` — remote worker
//!    writes forwarded over gRPC.
//!
//! Both paths converge on the same backend write (`apply_forwarded_command`),
//! so the side-effect dispatch logic is the same; sharing it here keeps the
//! two paths from drifting.

use std::sync::Arc;

use crate::controller_dispatcher::ControllerDispatcher;
use crate::controllers::workqueue::{QueuePriority, ReconcileKey};
use crate::datastore::DatastoreBackend;
use crate::datastore::command::StorageCommand;
use crate::replication::protocol::ForwardedResource;

pub async fn handle_applied_pod_side_effects(
    controller_dispatcher: Option<&Arc<ControllerDispatcher>>,
    command: &StorageCommand,
    resource: Option<&ForwardedResource>,
    db: &dyn DatastoreBackend,
) {
    enqueue_pod_status_side_effects(controller_dispatcher, command, resource, db).await;
    finalize_foreground_owners_after_pod_delete(controller_dispatcher, command, resource, db).await;
    reconcile_namespace_after_pod_delete(command, resource, db).await;
}

pub async fn enqueue_pod_status_side_effects(
    controller_dispatcher: Option<&Arc<ControllerDispatcher>>,
    command: &StorageCommand,
    resource: Option<&ForwardedResource>,
    db: &dyn DatastoreBackend,
) {
    let Some(controller_dispatcher) = controller_dispatcher else {
        return;
    };
    let is_pod_status_or_delete = matches!(
        command,
        StorageCommand::UpdateStatus { api_version, kind, .. }
            | StorageCommand::DeleteResource { api_version, kind, .. }
        if api_version == "v1" && kind == "Pod"
    );
    if !is_pod_status_or_delete {
        return;
    }
    let Some(resource) = resource else {
        return;
    };
    let namespace = resource
        .data
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if namespace.is_empty() {
        return;
    }
    let workload_keys = match crate::side_effects::workload_pod::workload_reconcile_keys_for_pod(
        &resource.data,
        db,
        namespace,
    )
    .await
    {
        Ok(keys) => keys,
        Err(err) => {
            tracing::warn!(
                error = %err,
                namespace,
                "failed to derive workload owner keys for pod status side effects"
            );
            Vec::new()
        }
    };
    let service_keys = match crate::side_effects::service_pod::service_reconcile_keys_for_pod(
        &resource.data,
        db,
        namespace,
    )
    .await
    {
        Ok(keys) => keys,
        Err(err) => {
            tracing::warn!(
                error = %err,
                namespace,
                "failed to derive Service keys for pod status side effects"
            );
            Vec::new()
        }
    };
    let job_keys =
        match crate::side_effects::job::job_reconcile_keys_for_pod(&resource.data, db, namespace)
            .await
        {
            Ok(keys) => keys,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    namespace,
                    "failed to derive Job keys for pod status side effects"
                );
                Vec::new()
            }
        };
    let pdb_keys = pdb_reconcile_keys_for_namespace(db, namespace).await;
    for key in workload_keys {
        controller_dispatcher
            .enqueue_reconcile_key_with_priority(key.clone(), pod_status_workload_priority(&key))
            .await;
    }
    for key in job_keys.into_iter().chain(service_keys).chain(pdb_keys) {
        controller_dispatcher.enqueue_reconcile_key(key).await;
    }
}

fn pod_status_workload_priority(key: &ReconcileKey) -> QueuePriority {
    if key.api_version == "apps/v1" && key.kind == "Deployment" {
        // Deployment rolling updates often enqueue during the create/adopt burst,
        // before replacement Pod readiness is visible. Preserve the readiness
        // event as a second reconcile so old ReplicaSets can be scaled down.
        QueuePriority::High
    } else {
        QueuePriority::Normal
    }
}

async fn finalize_foreground_owners_after_pod_delete(
    controller_dispatcher: Option<&Arc<ControllerDispatcher>>,
    command: &StorageCommand,
    resource: Option<&ForwardedResource>,
    db: &dyn DatastoreBackend,
) {
    if !matches!(
        command,
        StorageCommand::DeleteResource { api_version, kind, .. }
            if api_version == "v1" && kind == "Pod"
    ) {
        return;
    }

    let Some(resource) = resource else {
        return;
    };
    let Some(controller_dispatcher) = controller_dispatcher else {
        return;
    };
    let Some(pod_repository) = controller_dispatcher.current_pod_repository().await else {
        return;
    };

    let deleted_resource = resource.clone().into_resource();
    if let Err(err) = crate::controllers::gc::finalize_foreground_owners_after_dependent_delete(
        db,
        &deleted_resource,
        pod_repository.as_ref(),
    )
    .await
    {
        tracing::error!(
            namespace = ?deleted_resource.namespace,
            pod = %deleted_resource.name,
            uid = %deleted_resource.uid,
            error = %err,
            "leader outbox Pod delete foreground-owner check failed"
        );
    }
}

async fn reconcile_namespace_after_pod_delete(
    command: &StorageCommand,
    resource: Option<&ForwardedResource>,
    db: &dyn DatastoreBackend,
) {
    let StorageCommand::DeleteResource {
        api_version,
        kind,
        namespace,
        ..
    } = command
    else {
        return;
    };
    if api_version != "v1" || kind != "Pod" {
        return;
    }

    let namespace = namespace
        .as_deref()
        .or_else(|| {
            resource
                .and_then(|resource| resource.data.pointer("/metadata/namespace"))
                .and_then(|value| value.as_str())
        })
        .unwrap_or("default");
    if namespace.is_empty() {
        return;
    }

    let metrics = crate::side_effects::SideEffectMetrics::new();
    if let Err(err) = crate::api::reconcile_namespace_termination(db, namespace, &metrics).await {
        tracing::warn!(
            namespace,
            error = ?err,
            "leader outbox Pod delete namespace termination reconcile failed"
        );
    }
}

async fn pdb_reconcile_keys_for_namespace(
    db: &dyn DatastoreBackend,
    namespace: &str,
) -> Vec<ReconcileKey> {
    let pdbs = match db
        .list_resources(
            "policy/v1",
            "PodDisruptionBudget",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
    {
        Ok(pdbs) => pdbs,
        Err(err) => {
            tracing::warn!(
                error = %err,
                namespace,
                "failed to list PDBs for pod status side effects"
            );
            return Vec::new();
        }
    };

    pdbs.items
        .into_iter()
        .filter_map(|pdb| {
            pdb.data
                .pointer("/metadata/name")
                .and_then(|name| name.as_str())
                .map(|name| {
                    ReconcileKey::namespaced("policy/v1", "PodDisruptionBudget", namespace, name)
                })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::ResourcePreconditions;
    use crate::datastore::command::StorageCommand;
    use crate::replication::protocol::ForwardedResource;
    use serde_json::json;

    #[tokio::test]
    async fn outbox_pod_status_enqueues_pdb_reconcile_for_namespace() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "policy/v1",
            "PodDisruptionBudget",
            Some("default"),
            "pdb-ready",
            json!({
                "apiVersion": "policy/v1",
                "kind": "PodDisruptionBudget",
                "metadata": {"namespace": "default", "name": "pdb-ready"},
                "spec": {
                    "minAvailable": 1,
                    "selector": {"matchLabels": {"app": "x"}}
                }
            }),
        )
        .await
        .expect("create pdb");

        let dispatcher = Arc::new(ControllerDispatcher::default());
        let command = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "pdb-pod".to_string(),
            status: json!({"phase": "Running"}),
            expected_rv: None,
            preconditions: ResourcePreconditions {
                uid: Some("pod-uid".to_string()),
                resource_version: None,
            },
        };
        let resource = ForwardedResource {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "pdb-pod".to_string(),
            resource_version: 2,
            data: json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "pdb-pod",
                    "uid": "pod-uid",
                    "labels": {"app": "x"}
                },
                "spec": {"containers": [{"name": "c", "image": "pause"}]},
                "status": {"phase": "Running"}
            }),
        };

        enqueue_pod_status_side_effects(Some(&dispatcher), &command, Some(&resource), &db).await;

        let keys = dispatcher.queued_reconcile_keys_for_test().await;
        assert!(
            keys.iter().any(|key| {
                key.api_version == "policy/v1"
                    && key.kind == "PodDisruptionBudget"
                    && key.namespace.as_deref() == Some("default")
                    && key.name == "pdb-ready"
            }),
            "outbox Pod status applies must enqueue matching PDB reconciliation"
        );
    }

    #[tokio::test]
    async fn outbox_pod_status_enqueues_job_reconcile_for_owner_reference() {
        let db = crate::datastore::test_support::in_memory().await;
        let dispatcher = Arc::new(ControllerDispatcher::default());
        let command = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "job-pod".to_string(),
            status: json!({"phase": "Running"}),
            expected_rv: None,
            preconditions: ResourcePreconditions {
                uid: Some("pod-uid".to_string()),
                resource_version: None,
            },
        };
        let resource = ForwardedResource {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "job-pod".to_string(),
            resource_version: 2,
            data: json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "job-pod",
                    "uid": "pod-uid",
                    "ownerReferences": [{
                        "apiVersion": "batch/v1",
                        "kind": "Job",
                        "name": "ready-job",
                        "uid": "job-uid",
                        "controller": true
                    }]
                },
                "spec": {"containers": [{"name": "c", "image": "pause"}]},
                "status": {
                    "phase": "Running",
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        };

        enqueue_pod_status_side_effects(Some(&dispatcher), &command, Some(&resource), &db).await;

        let keys = dispatcher.queued_reconcile_keys_for_test().await;
        assert!(
            keys.iter().any(|key| {
                key.api_version == "batch/v1"
                    && key.kind == "Job"
                    && key.namespace.as_deref() == Some("default")
                    && key.name == "ready-job"
            }),
            "outbox Pod status applies must enqueue owning Job reconciliation"
        );
    }

    #[tokio::test]
    async fn outbox_ready_pod_status_preserves_followup_deployment_rollout_reconcile() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "web",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "namespace": "default",
                    "name": "web",
                    "uid": "deploy-web-uid"
                },
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "web"}},
                    "template": {
                        "metadata": {"labels": {"app": "web"}},
                        "spec": {"containers": [{"name": "web", "image": "agnhost"}]}
                    }
                }
            }),
        )
        .await
        .expect("create deployment");
        db.create_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "web-5812782185",
            json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "metadata": {
                    "namespace": "default",
                    "name": "web-5812782185",
                    "uid": "rs-web-uid",
                    "ownerReferences": [{
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "name": "web",
                        "uid": "deploy-web-uid",
                        "controller": true
                    }]
                },
                "spec": {"replicas": 1},
                "status": {"replicas": 1, "readyReplicas": 0, "availableReplicas": 0}
            }),
        )
        .await
        .expect("create replicaset");
        let dispatcher = Arc::new(ControllerDispatcher::default());
        let deployment_key = ReconcileKey::namespaced("apps/v1", "Deployment", "default", "web");
        dispatcher
            .enqueue_reconcile_key(deployment_key.clone())
            .await;

        let command = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web-pod".to_string(),
            status: json!({"phase": "Running"}),
            expected_rv: None,
            preconditions: ResourcePreconditions {
                uid: Some("pod-web-uid".to_string()),
                resource_version: None,
            },
        };
        let resource = ForwardedResource {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web-pod".to_string(),
            resource_version: 2,
            data: json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "web-pod",
                    "uid": "pod-web-uid",
                    "labels": {"app": "web"},
                    "ownerReferences": [{
                        "apiVersion": "apps/v1",
                        "kind": "ReplicaSet",
                        "name": "web-5812782185",
                        "uid": "rs-web-uid",
                        "controller": true
                    }]
                },
                "spec": {"containers": [{"name": "web", "image": "agnhost"}]},
                "status": {
                    "phase": "Running",
                    "conditions": [{"type": "Ready", "status": "True"}],
                    "containerStatuses": [{"name": "web", "ready": true, "restartCount": 0}]
                }
            }),
        };

        enqueue_pod_status_side_effects(Some(&dispatcher), &command, Some(&resource), &db).await;

        let mut deployment_reconciles = 0;
        for _ in 0..3 {
            let key = tokio::time::timeout(
                std::time::Duration::from_millis(100),
                dispatcher.take_reconcile_key_for_test(),
            )
            .await
            .expect("ready pod status must preserve the queued Deployment follow-up reconcile");
            if key == deployment_key {
                deployment_reconciles += 1;
            }
        }
        assert_eq!(
            deployment_reconciles, 2,
            "worker-applied pod readiness must not collapse into the stale Deployment rollout key"
        );
    }
}
