use klights_kubelet::pod_watch_handlers::PersistentVolumeEventHandler;
use klights_kubelet::pod_watch_source::PodWatchEvent;

pub(crate) struct LeaderPersistentVolumeEventHandler {
    db: crate::datastore::DatastoreHandle,
    is_leader_rx: tokio::sync::watch::Receiver<bool>,
    file_process: klights_supervisor::FileProcessExecutor,
    local_path_provisioner_root: std::path::PathBuf,
}

impl LeaderPersistentVolumeEventHandler {
    pub fn new(
        db: crate::datastore::DatastoreHandle,
        is_leader_rx: tokio::sync::watch::Receiver<bool>,
        file_process: klights_supervisor::FileProcessExecutor,
        local_path_provisioner_root: std::path::PathBuf,
    ) -> Self {
        Self {
            db,
            is_leader_rx,
            file_process,
            local_path_provisioner_root,
        }
    }

    async fn reconcile_pvc(&self, resource: klights_cluster_core::Resource, event_name: &str) {
        use klights_reconcile_api::PvcReconcileSink;

        let reconcile = crate::bootstrap::controller_adapters::pod_reconcile_adapter::PersistentVolumeReconcileAdapter::new(
            self.db.as_ref(),
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
        if let Ok(Some(resource)) = self
            .db
            .get_resource("v1", "PersistentVolumeClaim", namespace, event_name)
            .await
        {
            self.reconcile_pvc(resource, event_name).await;
        }
    }

    async fn handle_pv_event(&self, event: &PodWatchEvent, _event_name: &str) {
        if !*self.is_leader_rx.borrow()
            || event.event_type != klights_leader_api::WatchEventType::Added
        {
            return;
        }
        let Ok(pvcs) = self
            .db
            .list_resources(
                "v1",
                "PersistentVolumeClaim",
                None,
                crate::datastore::ResourceListQuery::all(),
            )
            .await
        else {
            return;
        };
        for pvc in pvcs.items {
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
        crate::datastore::DatastoreHandle,
        klights_cluster_core::Resource,
        klights_cluster_core::Resource,
    ) {
        let db: crate::datastore::DatastoreHandle = std::sync::Arc::new(
            crate::datastore::sqlite::Datastore::new_in_memory()
                .await
                .unwrap(),
        );
        let pv = db
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
        let pvc = db
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
        (db, pv, pvc)
    }

    fn handler(
        db: crate::datastore::DatastoreHandle,
        authority: tokio::sync::watch::Receiver<bool>,
    ) -> LeaderPersistentVolumeEventHandler {
        LeaderPersistentVolumeEventHandler::new(
            db,
            authority,
            crate::bootstrap::file_blocking::test_file_process_executor(),
            crate::KlightsConfig::test_default()
                .data_root
                .join("local-path-provisioner"),
        )
    }

    #[tokio::test]
    async fn follower_pvc_event_does_not_reconcile_until_leadership_gained() {
        let (db, _pv, pvc) = seed_matching_pv_and_pvc().await;
        let (leader_tx, leader_rx) = tokio::sync::watch::channel(false);
        let handler = handler(db.clone(), leader_rx);

        handler
            .handle_pvc_event(&PodWatchEvent::added((*pvc.data).clone()), "test-pvc")
            .await;
        let follower = db
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
        let leader = db
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
        let (db, pv, _pvc) = seed_matching_pv_and_pvc().await;
        let (_leader_tx, leader_rx) = tokio::sync::watch::channel(true);
        handler(db.clone(), leader_rx)
            .handle_pv_event(&PodWatchEvent::added((*pv.data).clone()), "test-pv")
            .await;

        let pvc = db
            .get_resource("v1", "PersistentVolumeClaim", Some("default"), "test-pvc")
            .await
            .unwrap()
            .unwrap();
        let pv = db
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
        let (db, small_pv, _pvc) = seed_matching_pv_and_pvc().await;
        db.create_resource(
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
        handler(db.clone(), leader_rx)
            .handle_pv_event(&PodWatchEvent::added((*small_pv.data).clone()), "test-pv")
            .await;

        let pvc = db
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
