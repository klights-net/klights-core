//! Side effect to reconcile workload controllers after Pod metadata mutations.

use super::{ControllerDispatcherSlot, SideEffect};
use crate::controllers::workqueue::{ReconcileKey, controller_kind_static};
use crate::datastore::DatastoreBackend;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;

/// Enqueues the explicit controller owner of a mutated Pod.
///
/// This is intentionally narrow: Pod status writers do not run side effects,
/// and this hook only follows controller ownerReferences already present on
/// the Pod. The owning controller remains responsible for release/adoption
/// during its normal reconcile.
pub struct WorkloadPodReconcileEffect {
    controller_dispatcher: ControllerDispatcherSlot,
}

#[async_trait]
impl SideEffect for WorkloadPodReconcileEffect {
    fn name(&self) -> &'static str {
        "workload_pod_reconcile"
    }

    async fn apply(&self, resource: &Value, db: &dyn DatastoreBackend) -> Result<()> {
        let namespace = resource
            .pointer("/metadata/namespace")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if namespace.is_empty() {
            return Ok(());
        }

        let Some(dispatcher) = self.controller_dispatcher.get() else {
            tracing::debug!(
                "WorkloadPodReconcileEffect skipped for {}: controller dispatcher not yet bound",
                namespace
            );
            return Ok(());
        };

        for key in workload_reconcile_keys_for_pod(resource, db, namespace).await? {
            dispatcher.enqueue_reconcile_key(key).await;
        }

        Ok(())
    }
}

pub async fn workload_reconcile_keys_for_pod(
    pod: &Value,
    db: &dyn DatastoreBackend,
    namespace: &str,
) -> Result<Vec<ReconcileKey>> {
    let mut keys = workload_owner_keys_for_pod(pod, namespace);
    append_replicaset_parent_controller_keys(pod, db, namespace, &mut keys).await?;
    if keys.is_empty() && !pod_has_controller_owner(pod) && !pod_is_terminating(pod) {
        keys.extend(selector_matching_orphan_keys_for_pod(pod, db, namespace).await?);
    }
    Ok(keys)
}

fn workload_owner_keys_for_pod(pod: &Value, namespace: &str) -> Vec<ReconcileKey> {
    let Some(owner_refs) = pod
        .pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
    else {
        return Vec::new();
    };

    let mut seen = HashSet::new();
    let mut keys = Vec::new();
    for owner in owner_refs {
        if owner.get("controller").and_then(|v| v.as_bool()) != Some(true) {
            continue;
        }

        let Some(name) = owner.get("name").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some((api_version, kind)) = owner_ref_controller_kind(owner) else {
            continue;
        };

        let dedupe_key = (api_version, kind, name.to_string());
        if seen.insert(dedupe_key) {
            keys.push(ReconcileKey::namespaced(api_version, kind, namespace, name));
        }
    }

    keys
}

async fn append_replicaset_parent_controller_keys(
    pod: &Value,
    db: &dyn DatastoreBackend,
    namespace: &str,
    keys: &mut Vec<ReconcileKey>,
) -> Result<()> {
    let Some(owner_refs) = pod
        .pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
    else {
        return Ok(());
    };

    let mut seen: HashSet<ReconcileKey> = keys.iter().cloned().collect();
    for owner in owner_refs {
        if owner.get("controller").and_then(|v| v.as_bool()) != Some(true) {
            continue;
        }
        if owner.get("apiVersion").and_then(|v| v.as_str()) != Some("apps/v1")
            || owner.get("kind").and_then(|v| v.as_str()) != Some("ReplicaSet")
        {
            continue;
        }
        let Some(replica_set_name) = owner.get("name").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(replica_set) = db
            .get_resource("apps/v1", "ReplicaSet", Some(namespace), replica_set_name)
            .await?
        else {
            continue;
        };
        let Some(parent_refs) = replica_set
            .data
            .pointer("/metadata/ownerReferences")
            .and_then(|v| v.as_array())
        else {
            continue;
        };
        for parent_ref in parent_refs {
            if parent_ref.get("controller").and_then(|v| v.as_bool()) != Some(true) {
                continue;
            }
            let api_version = parent_ref
                .get("apiVersion")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let kind = parent_ref
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let Some((api_version, kind)) = controller_kind_static(api_version, kind) else {
                continue;
            };
            let Some(owner_name) = parent_ref.get("name").and_then(|v| v.as_str()) else {
                continue;
            };
            let key = ReconcileKey::namespaced(api_version, kind, namespace, owner_name);
            if seen.insert(key.clone()) {
                keys.push(key);
            }
        }
    }

    Ok(())
}

async fn selector_matching_orphan_keys_for_pod(
    pod: &Value,
    db: &dyn DatastoreBackend,
    namespace: &str,
) -> Result<Vec<ReconcileKey>> {
    let mut keys = Vec::new();
    let mut seen = HashSet::new();

    let replica_sets = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;
    for replica_set in replica_sets.items {
        let selector_matches = replica_set
            .data
            .pointer("/spec/selector")
            .and_then(|selector| {
                crate::label_selector::LabelSelector::from_k8s_selector(selector).ok()
            })
            .is_some_and(|selector| selector.matches_resource(pod));
        if selector_matches && seen.insert(("apps/v1", "ReplicaSet", replica_set.name.clone())) {
            keys.push(ReconcileKey::namespaced(
                "apps/v1",
                "ReplicaSet",
                namespace,
                &replica_set.name,
            ));
        }
    }

    let replication_controllers = db
        .list_resources(
            "v1",
            "ReplicationController",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;
    for rc in replication_controllers.items {
        let selector_matches = rc
            .data
            .pointer("/spec/selector")
            .and_then(|selector| {
                crate::label_selector::LabelSelector::from_flat_match_labels(selector).ok()
            })
            .is_some_and(|selector| {
                !selector.requirements().is_empty() && selector.matches_resource(pod)
            });
        if selector_matches && seen.insert(("v1", "ReplicationController", rc.name.clone())) {
            keys.push(ReconcileKey::namespaced(
                "v1",
                "ReplicationController",
                namespace,
                &rc.name,
            ));
        }
    }

    Ok(keys)
}

fn pod_has_controller_owner(pod: &Value) -> bool {
    pod.pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
        .is_some_and(|refs| {
            refs.iter()
                .any(|owner| owner.get("controller").and_then(|v| v.as_bool()) == Some(true))
        })
}

fn pod_is_terminating(pod: &Value) -> bool {
    pod.pointer("/metadata/deletionTimestamp")
        .is_some_and(|value| !value.is_null())
}

fn owner_ref_controller_kind(owner: &Value) -> Option<(&'static str, &'static str)> {
    let kind = owner.get("kind").and_then(|v| v.as_str())?;
    let api_version = owner.get("apiVersion").and_then(|v| v.as_str());
    match (api_version, kind) {
        (Some("v1") | None, "ReplicationController") => Some(("v1", "ReplicationController")),
        (Some("apps/v1"), "ReplicaSet") => Some(("apps/v1", "ReplicaSet")),
        (Some("apps/v1"), "StatefulSet") => Some(("apps/v1", "StatefulSet")),
        (Some("apps/v1"), "DaemonSet") => Some(("apps/v1", "DaemonSet")),
        _ => None,
    }
}

pub fn workload_pod_reconcile(
    controller_dispatcher: ControllerDispatcherSlot,
) -> Arc<dyn SideEffect> {
    Arc::new(WorkloadPodReconcileEffect {
        controller_dispatcher,
    })
}
