use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

#[cfg(test)]
use crate::datastore::DatastoreHandle;
use klights_cluster_store::{ClusterResourceRead, ResourceGetRequest};
use klights_controllers::side_effects::node_taint_manager::NodeTaintNodeStore;

struct RootNodeTaintNodeStore {
    resource_reads: Arc<dyn ClusterResourceRead>,
}

#[async_trait]
impl NodeTaintNodeStore for RootNodeTaintNodeStore {
    async fn get_node(&self, name: &str) -> Result<Option<klights_cluster_core::Resource>> {
        self.resource_reads
            .get_resource(ResourceGetRequest::new("v1", "Node", None, name))
            .await
            .map_err(Into::into)
    }
}

pub(crate) fn port(resource_reads: Arc<dyn ClusterResourceRead>) -> Arc<dyn NodeTaintNodeStore> {
    Arc::new(RootNodeTaintNodeStore { resource_reads })
}

#[cfg(test)]
struct DirectNodeTaintNodeStore {
    db: DatastoreHandle,
}

#[cfg(test)]
#[async_trait]
impl NodeTaintNodeStore for DirectNodeTaintNodeStore {
    async fn get_node(&self, name: &str) -> Result<Option<klights_cluster_core::Resource>> {
        self.db.get_resource("v1", "Node", None, name).await
    }
}

#[cfg(test)]
pub(crate) fn port_for_test(db: DatastoreHandle) -> Arc<dyn NodeTaintNodeStore> {
    Arc::new(DirectNodeTaintNodeStore { db })
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

        reconcile_node_noexecute_taints(slot, None, Some(port_for_test(db_handle)), &node.data)
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

        reconcile_node_noexecute_taints(slot, None, Some(port_for_test(db_handle)), &node.data)
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
            Some(port_for_test(db_handle)),
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
            Some(port_for_test(db_handle)),
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
        let authority =
            crate::bootstrap::composition_adapters::authority_adapter::always_leader_authority();
        let resource_query = crate::bootstrap::composition_adapters::resource_query_adapter::DatastoreResourceQueryAdapter::new(
            db_handle.clone(), authority.clone(),
        );
        let resource_commands = Arc::new(
            klights_replication::leader_api::EmbeddedLeaderResourceCommand::new(
                Arc::new(
                    crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(
                        db_handle.clone(),
                    ),
                ),
                resource_query.clone(),
                authority,
            ),
        );
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let (
            pod_query,
            _pod_snapshot,
            _pod_update,
            _pod_status_writer,
            _pod_workqueue,
            _pod_network_assignment,
            _pod_host_ip,
            _background,
            _deletion_finalizer,
            _dirty_counter,
            _mutation_reconcile,
            pod_delete_sink,
            _eviction_admission,
            _namespace_bootstrap,
            _namespace_termination_queue,
            _pod_api,
            _pod_subresource,
            _pod_scheduling,
            _watch_source,
            _bound_finalization,
            _deferred_runtime,
            _test_api,
            _test_subresource,
        ) = crate::bootstrap::pod_repository_composition::build_pod_repository_parts(
            crate::bootstrap::pod_repository_composition::PodRepositoryBuildConfig {
                resource_query,
                ownership_reads: db.focused_read_store(),
                resource_reads: db.focused_read_store(),
                namespace_content_reads: db.focused_read_store(),
                topology_reads: db.focused_read_store(),
                pod_workqueue_store: None,
                supervisor: supervisor.clone(),
                side_effects: Arc::new(klights_controllers::side_effects::SideEffectRegistry::new()),
                metrics: klights_controllers::side_effects::SideEffectMetrics::new(),
                pod_network_cache: crate::bootstrap::pod_repository_composition::empty_test_pod_network_cache(),
                assignment_waiter: crate::bootstrap::pod_repository_composition::test_assignment_bus(),
                scheduling_mode: crate::bootstrap::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
                outbox: None,
                cluster_api: None,
                resource_commands: Some(resource_commands),
                remote_delivery_required: false,
                controller_identity: crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
                api_identity: Arc::new(k8s_native_service::test_support::admission::DeterministicApiIdentity::default()),
                scheduler_bind_gate: None,
                post_write_maintenance_notify: None,
                gc_coordination: Arc::new(klights_controllers::ControllerCoordination::new()),
            },
            None,
        );
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
