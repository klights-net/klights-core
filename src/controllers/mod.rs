pub mod annotations;
pub mod common;
pub mod coredns;
pub mod crd;
pub mod cronjob;
pub mod cronjob_scheduler;
pub mod csr_signer;
pub mod daemonset;
pub mod daemonset_controller;
pub mod deployment;
pub mod deployment_controller;
pub mod endpoints;
pub mod endpoints_controller;
pub mod gc;
pub mod job;
pub mod job_controller;
pub mod kube_service;
pub mod namespace;
pub mod node_lifecycle;
pub mod node_subnet;
pub mod pdb;
pub mod pdb_controller;
pub mod pvc;
pub mod pvc_controller;
pub mod rbac_reconcile;
pub mod replicaset;
pub mod replicaset_controller;
pub mod replication_controller_runner;
pub mod replicationcontroller;
pub mod resource_quota;
pub mod scheduler;
pub mod service;
pub mod service_controller;
pub mod statefulset;
pub mod statefulset_controller;
#[cfg(test)]
pub mod test_utils;
pub mod workqueue;

#[cfg(test)]
use crate::datastore::{DatastoreBackend, Resource};

/// Find all pods in a namespace owned by the resource identified by `owner_uid`.
///
/// Uses `list_resources_by_owner_uid`, which matches the UID across all
/// ownerReferences entries instead of assuming the controller owner is first.
///
/// Production code routes `v1/Pod` listings through
/// [`crate::kubelet::pod_repository::PodReader::list_pods_by_owner_uid`]; this
/// helper survives only as a test fixture so the existing
/// `test_find_owned_pods_filters_by_owner_uid` exercise of the underlying
/// `list_resources_by_owner_uid` plumbing keeps passing without growing
/// a parallel `PodReader` mock.
#[cfg(test)]
pub async fn find_owned_pods(
    ds: &dyn DatastoreBackend,
    namespace: &str,
    owner_uid: &str,
) -> anyhow::Result<Vec<Resource>> {
    ds.list_resources_by_owner_uid("v1", "Pod", Some(namespace), owner_uid)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    #[tokio::test]
    async fn test_find_owned_pods_filters_by_owner_uid() {
        let ds = crate::datastore::test_support::in_memory().await;

        // Create 3 pods: 2 owned by "abc", 1 owned by "xyz"
        let pod1 = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "pod1",
                "namespace": "default",
                "uid": "pod1-uid",
                "ownerReferences": [{
                    "uid": "abc",
                    "kind": "ReplicaSet",
                    "name": "rs1"
                }]
            },
            "spec": {
                "containers": [{"name": "c1", "image": "nginx"}]
            }
        });

        let pod2 = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "pod2",
                "namespace": "default",
                "uid": "pod2-uid",
                "ownerReferences": [{
                    "uid": "abc",
                    "kind": "ReplicaSet",
                    "name": "rs1"
                }]
            },
            "spec": {
                "containers": [{"name": "c1", "image": "nginx"}]
            }
        });

        let pod3 = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "pod3",
                "namespace": "default",
                "uid": "pod3-uid",
                "ownerReferences": [{
                    "uid": "xyz",
                    "kind": "ReplicaSet",
                    "name": "rs2"
                }]
            },
            "spec": {
                "containers": [{"name": "c1", "image": "nginx"}]
            }
        });

        ds.create_resource("v1", "Pod", Some("default"), "pod1", pod1)
            .await
            .unwrap();
        ds.create_resource("v1", "Pod", Some("default"), "pod2", pod2)
            .await
            .unwrap();
        ds.create_resource("v1", "Pod", Some("default"), "pod3", pod3)
            .await
            .unwrap();

        // Call find_owned_pods for UID "abc"
        let owned = find_owned_pods(&ds, "default", "abc").await.unwrap();

        // Should return 2 pods (pod1 and pod2)
        assert_eq!(owned.len(), 2);
        let names: Vec<_> = owned
            .iter()
            .map(|p| p.data["metadata"]["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"pod1"));
        assert!(names.contains(&"pod2"));
    }
}
