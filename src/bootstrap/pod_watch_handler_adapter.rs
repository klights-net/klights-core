use klights_kubelet::pod_watch_handlers::PersistentVolumeEventHandler;
use klights_kubelet::pod_watch_source::PodWatchEvent;

pub(crate) struct LeaderPersistentVolumeEventHandler {
    resource_reads: std::sync::Arc<dyn klights_cluster_store::ClusterResourceRead>,
    controller_store: std::sync::Arc<
        crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort,
    >,
    is_leader_rx: tokio::sync::watch::Receiver<bool>,
    file_process: klights_supervisor::FileProcessExecutor,
    local_path_provisioner_root: std::path::PathBuf,
}

impl LeaderPersistentVolumeEventHandler {
    pub fn new(
        resource_reads: std::sync::Arc<dyn klights_cluster_store::ClusterResourceRead>,
        ownership_reads: std::sync::Arc<dyn klights_cluster_store::ClusterOwnershipRead>,
        resource_commands: std::sync::Arc<dyn klights_leader_api::LeaderResourceCommand>,
        is_leader_rx: tokio::sync::watch::Receiver<bool>,
        file_process: klights_supervisor::FileProcessExecutor,
        local_path_provisioner_root: std::path::PathBuf,
    ) -> Self {
        let controller_store = std::sync::Arc::new(
            crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::new_with_commands(
                resource_reads.clone(), ownership_reads,
                resource_commands,
            ),
        );
        Self {
            resource_reads,
            controller_store,
            is_leader_rx,
            file_process,
            local_path_provisioner_root,
        }
    }

    async fn reconcile_pvc(&self, resource: klights_cluster_core::Resource, event_name: &str) {
        use klights_reconcile_api::PvcReconcileSink;

        let reconcile = crate::bootstrap::controller_adapters::pod_reconcile_adapter::PersistentVolumeReconcileAdapter::new(
            self.controller_store.as_ref(),
            &self.file_process,
            &self.local_path_provisioner_root,
        );
        if let Err(error) = reconcile.reconcile_pvc(resource).await {
            tracing::error!(pvc = event_name, error = %error, "failed to reconcile PVC");
        }
    }
}

#[async_trait::async_trait]
impl PersistentVolumeEventHandler for LeaderPersistentVolumeEventHandler {
    async fn handle_pvc_event(&self, event: &PodWatchEvent, event_name: &str) {
        if !*self.is_leader_rx.borrow()
            || !matches!(
                event.event_type,
                klights_leader_api::WatchEventType::Added
                    | klights_leader_api::WatchEventType::Modified
            )
        {
            return;
        }
        let namespace = event
            .object
            .pointer("/metadata/namespace")
            .and_then(serde_json::Value::as_str);
        let resource = self
            .resource_reads
            .get_resource(klights_cluster_store::ResourceGetRequest::new(
                "v1",
                "PersistentVolumeClaim",
                namespace.map(ToOwned::to_owned),
                event_name,
            ))
            .await;
        if let Ok(Some(resource)) = resource {
            self.reconcile_pvc(resource, event_name).await;
        }
    }

    async fn handle_pv_event(&self, event: &PodWatchEvent, _event_name: &str) {
        if !*self.is_leader_rx.borrow()
            || event.event_type != klights_leader_api::WatchEventType::Added
        {
            return;
        }
        let listing = self
            .resource_reads
            .list_resources(klights_cluster_store::ResourceListRequest::new(
                "v1",
                "PersistentVolumeClaim",
                klights_cluster_store::ResourceCollectionScope::AllNamespaces,
                klights_cluster_store::ResourceListQuery::all(),
            ))
            .await
            .map(|read| match read {
                klights_cluster_store::ResourceListRead::Current(page)
                | klights_cluster_store::ResourceListRead::Historical(page) => page.into_items(),
                klights_cluster_store::ResourceListRead::Expired {
                    requested,
                    oldest_available,
                    ..
                } => {
                    tracing::warn!(requested, oldest_available, "PVC list expired");
                    Vec::new()
                }
            });
        let Ok(pvcs) = listing else {
            return;
        };
        for pvc in pvcs {
            if pvc
                .data
                .pointer("/status/phase")
                .and_then(serde_json::Value::as_str)
                != Some("Bound")
            {
                let name = pvc.name.clone();
                self.reconcile_pvc(pvc, &name).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn seed_matching_pv_and_pvc() -> (
        klights_cluster_datastore::sqlite::embedded::Datastore,
        klights_cluster_core::Resource,
        klights_cluster_core::Resource,
    ) {
        let sqlite = crate::bootstrap::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
        let pv = sqlite
            .create_resource(
                "v1",
                "PersistentVolume",
                None,
                "test-pv",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "PersistentVolume",
                    "metadata": {"name": "test-pv"},
                    "spec": {
                        "capacity": {"storage": "1Gi"},
                        "accessModes": ["ReadWriteOnce"]
                    },
                    "status": {"phase": "Available"}
                }),
            )
            .await
            .unwrap();
        let pvc = sqlite
            .create_resource(
                "v1",
                "PersistentVolumeClaim",
                Some("default"),
                "test-pvc",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "PersistentVolumeClaim",
                    "metadata": {"name": "test-pvc", "namespace": "default"},
                    "spec": {
                        "resources": {"requests": {"storage": "1Gi"}},
                        "accessModes": ["ReadWriteOnce"]
                    }
                }),
            )
            .await
            .unwrap();
        (sqlite, pv, pvc)
    }

    fn handler(
        sqlite: &klights_cluster_datastore::sqlite::embedded::Datastore,
        authority: tokio::sync::watch::Receiver<bool>,
    ) -> LeaderPersistentVolumeEventHandler {
        let ports = crate::bootstrap::cluster_store::selector::sqlite_opened_passive_store(sqlite);
        LeaderPersistentVolumeEventHandler::new(
            ports.read_ports.resource_reads(),
            ports.ownership_reads,
            crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::resource_commands_for_test(
                ports.applied_outbox,
                std::sync::Arc::new(sqlite.clone()),
                ports.read_ports.resource_reads(),
            ),
            authority,
            crate::bootstrap::file_blocking::test_file_process_executor(),
            crate::KlightsConfig::test_default()
                .data_root
                .join("local-path-provisioner"),
        )
    }

    #[tokio::test]
    async fn follower_pvc_event_does_not_reconcile_until_leadership_gained() {
        let (sqlite, _pv, pvc) = seed_matching_pv_and_pvc().await;
        let (leader_tx, leader_rx) = tokio::sync::watch::channel(false);
        let handler = handler(&sqlite, leader_rx);

        handler
            .handle_pvc_event(&PodWatchEvent::added((*pvc.data).clone()), "test-pvc")
            .await;
        let follower = sqlite
            .get_resource("v1", "PersistentVolumeClaim", Some("default"), "test-pvc")
            .await
            .unwrap()
            .unwrap();
        assert_ne!(
            follower.data.pointer("/status/phase"),
            Some(&serde_json::json!("Bound"))
        );

        leader_tx.send(true).unwrap();
        handler
            .handle_pvc_event(&PodWatchEvent::added((*pvc.data).clone()), "test-pvc")
            .await;
        let leader = sqlite
            .get_resource("v1", "PersistentVolumeClaim", Some("default"), "test-pvc")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            leader.data.pointer("/status/phase"),
            Some(&serde_json::json!("Bound"))
        );
    }

    #[tokio::test]
    async fn leader_pv_event_binds_pending_pvc_with_claimref_agreement() {
        let (sqlite, pv, _pvc) = seed_matching_pv_and_pvc().await;
        let (_leader_tx, leader_rx) = tokio::sync::watch::channel(true);
        handler(&sqlite, leader_rx)
            .handle_pv_event(&PodWatchEvent::added((*pv.data).clone()), "test-pv")
            .await;

        let pvc = sqlite
            .get_resource("v1", "PersistentVolumeClaim", Some("default"), "test-pvc")
            .await
            .unwrap()
            .unwrap();
        let pv = sqlite
            .get_resource("v1", "PersistentVolume", None, "test-pv")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            pvc.data.pointer("/status/volumeName"),
            Some(&serde_json::json!("test-pv"))
        );
        assert_eq!(
            pv.data.pointer("/spec/claimRef/name"),
            Some(&serde_json::json!("test-pvc"))
        );
    }

    #[tokio::test]
    async fn leader_pv_event_picks_smallest_sufficient_pv() {
        let (sqlite, small_pv, _pvc) = seed_matching_pv_and_pvc().await;
        sqlite
            .create_resource(
                "v1",
                "PersistentVolume",
                None,
                "big-pv",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "PersistentVolume",
                    "metadata": {"name": "big-pv"},
                    "spec": {"capacity": {"storage": "5Gi"}, "accessModes": ["ReadWriteOnce"]},
                    "status": {"phase": "Available"}
                }),
            )
            .await
            .unwrap();
        let (_leader_tx, leader_rx) = tokio::sync::watch::channel(true);
        handler(&sqlite, leader_rx)
            .handle_pv_event(&PodWatchEvent::added((*small_pv.data).clone()), "test-pv")
            .await;

        let pvc = sqlite
            .get_resource("v1", "PersistentVolumeClaim", Some("default"), "test-pvc")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            pvc.data.pointer("/status/volumeName"),
            Some(&serde_json::json!("test-pv"))
        );
    }
}
