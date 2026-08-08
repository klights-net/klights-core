use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::datastore::DatastoreHandle;
use klights_controllers::side_effects::node_taint_manager::NodeTaintNodeStore;

struct RootNodeTaintNodeStore {
    db: DatastoreHandle,
}

#[async_trait]
impl NodeTaintNodeStore for RootNodeTaintNodeStore {
    async fn get_node(&self, name: &str) -> Result<Option<klights_cluster_core::Resource>> {
        self.db.get_resource("v1", "Node", None, name).await
    }
}

pub(crate) fn port(db: DatastoreHandle) -> Arc<dyn NodeTaintNodeStore> {
    Arc::new(RootNodeTaintNodeStore { db })
}

#[cfg(test)]
mod tests {
    use super::*;
    use klights_controllers::side_effects::PodSideEffectPortsSlot;
    use klights_controllers::side_effects::node_taint_manager::reconcile_node_noexecute_taints;
    use klights_supervisor::TaskSupervisor;
    use serde_json::Value;
    use serde_json::json;

    #[tokio::test]
    async fn node_noexecute_taint_deletes_untolerated_pod() {
        let (db, db_handle, slot, _supervisor) = fixture().await;
        let node = create_node(&db, vec![noexecute_taint()]).await;
        create_pod(&db, "untolerated", json!([])).await;

        reconcile_node_noexecute_taints(slot, None, Some(port(db_handle)), &node.data)
            .await
            .unwrap();

        let pod = db
            .get_resource("v1", "Pod", Some("default"), "untolerated")
            .await
            .unwrap();
        let pod = pod.expect("untolerated NoExecute pod row remains until actor finalization");
        assert!(
            pod.data.pointer("/metadata/deletionTimestamp").is_some(),
            "untolerated NoExecute pod must be marked terminating for actor-owned eviction"
        );
    }

    #[tokio::test]
    async fn node_ready_unknown_deletes_untolerated_pod() {
        let (db, db_handle, slot, _supervisor) = fixture().await;
        let node = create_node_with_status(
            &db,
            vec![],
            json!({
                "conditions": [{
                    "type": "Ready",
                    "status": "Unknown",
                    "reason": "NodeStatusUnknown",
                    "message": "Kubelet stopped posting node status.",
                    "lastHeartbeatTime": "2026-05-13T06:34:15Z",
                    "lastTransitionTime": "2026-05-13T06:34:15Z"
                }]
            }),
        )
        .await;
        create_pod(&db, "ready-unknown", json!([])).await;

        reconcile_node_noexecute_taints(slot, None, Some(port(db_handle)), &node.data)
            .await
            .unwrap();

        let pod = db
            .get_resource("v1", "Pod", Some("default"), "ready-unknown")
            .await
            .unwrap();
        let pod = pod.expect("ready-unknown pod row remains until actor finalization");
        assert!(
            pod.data.pointer("/metadata/deletionTimestamp").is_some(),
            "ready-unknown Node must evict untolerated pod"
        );
    }

    #[tokio::test]
    async fn delayed_noexecute_eviction_deletes_pod_when_taint_remains() {
        let (db, db_handle, slot, supervisor) = fixture().await;
        let node = create_node(&db, vec![noexecute_taint()]).await;
        create_pod(
            &db,
            "delayed-evict",
            json!([{
                "key": "kubernetes.io/e2e-evict-taint-key",
                "operator": "Equal",
                "value": "evictTaintVal",
                "effect": "NoExecute",
                "tolerationSeconds": 1
            }]),
        )
        .await;

        reconcile_node_noexecute_taints(
            slot,
            Some(supervisor.clone()),
            Some(port(db_handle)),
            &node.data,
        )
        .await
        .unwrap();

        supervisor
            .sleep(
                "node_taint_manager_test_wait",
                std::time::Duration::from_millis(1200),
            )
            .await
            .unwrap();
        let pod = db
            .get_resource("v1", "Pod", Some("default"), "delayed-evict")
            .await
            .unwrap();
        let pod = pod.expect("delayed NoExecute pod row remains until actor finalization");
        assert!(
            pod.data.pointer("/metadata/deletionTimestamp").is_some(),
            "pod must be marked terminating when NoExecute taint remains after tolerationSeconds"
        );
    }

    #[tokio::test]
    async fn delayed_noexecute_eviction_rechecks_removed_taint_before_delete() {
        let (db, db_handle, slot, supervisor) = fixture().await;
        let node = create_node(&db, vec![noexecute_taint()]).await;
        create_pod(
            &db,
            "delayed",
            json!([{
                "key": "kubernetes.io/e2e-evict-taint-key",
                "operator": "Equal",
                "value": "evictTaintVal",
                "effect": "NoExecute",
                "tolerationSeconds": 1
            }]),
        )
        .await;

        reconcile_node_noexecute_taints(
            slot,
            Some(supervisor.clone()),
            Some(port(db_handle)),
            &node.data,
        )
        .await
        .unwrap();

        let mut untainted_node: Value = Arc::unwrap_or_clone(node.data);
        untainted_node["spec"]["taints"] = json!([]);
        db.update_resource(
            "v1",
            "Node",
            None,
            "node-a",
            untainted_node,
            node.resource_version,
        )
        .await
        .unwrap();

        supervisor
            .sleep(
                "node_taint_manager_test_wait",
                std::time::Duration::from_millis(1200),
            )
            .await
            .unwrap();
        let pod = db
            .get_resource("v1", "Pod", Some("default"), "delayed")
            .await
            .unwrap();
        assert!(
            pod.is_some(),
            "pod must survive when NoExecute taint is removed before toleration expires"
        );
    }

    async fn fixture() -> (
        crate::datastore::sqlite::Datastore,
        crate::datastore::DatastoreHandle,
        PodSideEffectPortsSlot,
        Arc<TaskSupervisor>,
    ) {
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        let db_handle: crate::datastore::DatastoreHandle = Arc::new(db.clone());
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        // Built through the canonical kubelet test-support constructor and
        // decomposed immediately into the two focused ports this adapter's
        // tests actually need; nothing here names or stores the concrete
        // root repository type.
        let repository = crate::kubelet::pod_repository::pod_repository_for_test(&db);
        let pod_query: Arc<dyn klights_pod_api::PodQuery> = repository.clone();
        let pod_delete_sink: Arc<dyn klights_reconcile_api::GcPodDeleteSink> = repository;
        let slot = PodSideEffectPortsSlot::new();
        slot.set(pod_query, pod_delete_sink);
        (db, db_handle, slot, supervisor)
    }

    async fn create_node(
        db: &crate::datastore::sqlite::Datastore,
        taints: Vec<Value>,
    ) -> crate::datastore::Resource {
        create_node_with_status(
            db,
            taints,
            json!({
                "conditions": [{
                    "type": "Ready",
                    "status": "True",
                    "reason": "KubeletReady",
                    "message": "klights is ready",
                    "lastHeartbeatTime": "2026-05-13T06:34:15Z",
                    "lastTransitionTime": "2026-05-13T06:34:15Z"
                }]
            }),
        )
        .await
    }

    async fn create_node_with_status(
        db: &crate::datastore::sqlite::Datastore,
        taints: Vec<Value>,
        status: Value,
    ) -> crate::datastore::Resource {
        db.create_resource(
            "v1",
            "Node",
            None,
            "node-a",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "node-a"},
                "spec": {"taints": taints},
                "status": status
            }),
        )
        .await
        .unwrap()
    }

    async fn create_pod(db: &crate::datastore::sqlite::Datastore, name: &str, tolerations: Value) {
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            name,
            pod_with_tolerations(name, tolerations),
        )
        .await
        .unwrap();
    }

    fn noexecute_taint() -> Value {
        json!({
            "key": "kubernetes.io/e2e-evict-taint-key",
            "value": "evictTaintVal",
            "effect": "NoExecute"
        })
    }

    fn pod_with_tolerations(name: &str, tolerations: Value) -> Value {
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"namespace": "default", "name": name},
            "spec": {
                "nodeName": "node-a",
                "tolerations": tolerations,
                "containers": [{"name": "c", "image": "pause"}]
            }
        })
    }
}
