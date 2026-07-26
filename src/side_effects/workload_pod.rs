//! Side effect to reconcile workload controllers after Pod metadata mutations.

use crate::controllers::workqueue::controller_kind_static;
use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;
use klights_reconcile_api::ReconcileKey;
use serde_json::Value;
use std::collections::HashSet;

#[async_trait]
pub(crate) trait WorkloadPodStore: Send + Sync {
    async fn get_replica_set(&self, namespace: &str, name: &str) -> Result<Option<Resource>>;
    async fn list_replica_sets(&self, namespace: &str) -> Result<Vec<Resource>>;
    async fn list_replication_controllers(&self, namespace: &str) -> Result<Vec<Resource>>;
}

pub(crate) async fn workload_reconcile_keys_for_pod<Store: WorkloadPodStore + ?Sized>(
    pod: &Value,
    store: &Store,
    namespace: &str,
) -> Result<Vec<ReconcileKey>> {
    let mut keys = workload_owner_keys_for_pod(pod, namespace);
    append_replicaset_parent_controller_keys(pod, store, namespace, &mut keys).await?;
    if keys.is_empty() && !pod_has_controller_owner(pod) && !pod_is_terminating(pod) {
        keys.extend(selector_matching_orphan_keys_for_pod(pod, store, namespace).await?);
    }
    Ok(keys)
}

pub(crate) fn workload_owner_keys_for_pod(pod: &Value, namespace: &str) -> Vec<ReconcileKey> {
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

async fn append_replicaset_parent_controller_keys<Store: WorkloadPodStore + ?Sized>(
    pod: &Value,
    store: &Store,
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
        let Some(replica_set) = store.get_replica_set(namespace, replica_set_name).await? else {
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

async fn selector_matching_orphan_keys_for_pod<Store: WorkloadPodStore + ?Sized>(
    pod: &Value,
    store: &Store,
    namespace: &str,
) -> Result<Vec<ReconcileKey>> {
    let mut keys = Vec::new();
    let mut seen = HashSet::new();

    for replica_set in store.list_replica_sets(namespace).await? {
        let selector_matches = replica_set
            .data
            .pointer("/spec/selector")
            .and_then(|selector| klights_types::LabelSelector::from_k8s_selector(selector).ok())
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

    for rc in store.list_replication_controllers(namespace).await? {
        let selector_matches = rc
            .data
            .pointer("/spec/selector")
            .and_then(|selector| {
                klights_types::LabelSelector::from_flat_match_labels(selector).ok()
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
    let api_version = owner
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .or_else(|| (kind == "ReplicationController").then_some("v1"))?;
    controller_kind_static(api_version, kind)
}

#[cfg(test)]
mod tests {
    use super::workload_owner_keys_for_pod;
    use serde_json::json;

    #[test]
    fn workload_owner_keys_preserve_every_supported_workload_controller() {
        let cases = [
            ("apps/v1", "Deployment"),
            ("apps/v1", "ReplicaSet"),
            ("apps/v1", "StatefulSet"),
            ("apps/v1", "DaemonSet"),
            ("batch/v1", "Job"),
            ("v1", "ReplicationController"),
        ];

        for (api_version, kind) in cases {
            let pod = json!({
                "metadata": {
                    "ownerReferences": [{
                        "apiVersion": api_version,
                        "kind": kind,
                        "name": "owner",
                        "controller": true
                    }]
                }
            });
            let keys = workload_owner_keys_for_pod(&pod, "default");
            assert_eq!(keys.len(), 1, "missing owner key for {api_version}/{kind}");
            assert_eq!(
                keys.into_iter().next().unwrap().into_parts(),
                (
                    api_version,
                    kind,
                    Some("default".to_string()),
                    "owner".to_string()
                )
            );
        }
    }
}
