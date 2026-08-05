#[cfg(test)]
use crate::datastore::{DatastoreBackend, DatastoreHandle};
use crate::kubelet::pod_watch_source::PodWatchEvent;
#[cfg(test)]
use klights_leader_api::WatchEventType;
#[cfg(test)]
use klights_reconcile_api::PvcReconcileSink;

#[async_trait::async_trait]
pub trait PersistentVolumeEventHandler: Send + Sync {
    async fn handle_pvc_event(&self, event: &PodWatchEvent, event_name: &str);
    async fn handle_pv_event(&self, event: &PodWatchEvent, event_name: &str);
}

/// Leader-scoped handler that reconciles PersistentVolumeClaim binding against
/// the cluster datastore. Only the current raft leader may originate the
/// binding writes, so each event re-checks the live leadership authority
/// (a `watch::Receiver<bool>` driven by the raft role watcher) instead of
/// caching leadership at construction. A voter that loses leadership stops
/// originating writes immediately; a follower that gains leadership begins
/// reconciling on the next delivered event without restart.
#[cfg(test)]
pub struct DatastorePersistentVolumeEventHandler {
    db: DatastoreHandle,
    is_leader_rx: tokio::sync::watch::Receiver<bool>,
    file_process: klights_supervisor::FileProcessExecutor,
    local_path_provisioner_root: std::path::PathBuf,
}

#[cfg(test)]
impl DatastorePersistentVolumeEventHandler {
    pub fn new(
        db: DatastoreHandle,
        is_leader_rx: tokio::sync::watch::Receiver<bool>,
        file_process: klights_supervisor::FileProcessExecutor,
    ) -> Self {
        Self {
            db,
            is_leader_rx,
            file_process,
            local_path_provisioner_root: crate::KlightsConfig::test_default()
                .data_root
                .join("local-path-provisioner"),
        }
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl PersistentVolumeEventHandler for DatastorePersistentVolumeEventHandler {
    async fn handle_pvc_event(&self, event: &PodWatchEvent, event_name: &str) {
        // Re-check the live leadership authority for this event. Never cache
        // the result: a leadership transition between two events must be
        // honored without restart.
        if !*self.is_leader_rx.borrow() {
            tracing::debug!(
                "Skipping leader-owned PVC reconciliation for {event_name}: not current leader"
            );
            return;
        }
        handle_pvc_event(
            &self.file_process,
            &self.local_path_provisioner_root,
            self.db.as_ref(),
            event,
            event_name,
        )
        .await;
    }

    async fn handle_pv_event(&self, event: &PodWatchEvent, event_name: &str) {
        // Re-check the live leadership authority for this event. Never cache
        // the result: a leadership transition between two events must be
        // honored without restart.
        if !*self.is_leader_rx.borrow() {
            tracing::debug!(
                "Skipping leader-owned PV reconciliation for {event_name}: not current leader"
            );
            return;
        }
        handle_pv_event(
            &self.file_process,
            &self.local_path_provisioner_root,
            self.db.as_ref(),
            event,
            event_name,
        )
        .await;
    }
}

/// Worker-side no-op volume event handler. Workers must never originate
/// PV/PVC cluster reconciliation writes and therefore carry no cluster
/// datastore capability. Injected explicitly at the worker composition root so
/// the absence of reconciliation is expressed through behavior, not a boolean.
pub struct NoopPersistentVolumeEventHandler;

impl NoopPersistentVolumeEventHandler {
    pub fn new() -> Self {
        Self
    }
}

impl Default for NoopPersistentVolumeEventHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl PersistentVolumeEventHandler for NoopPersistentVolumeEventHandler {
    async fn handle_pvc_event(&self, _event: &PodWatchEvent, _event_name: &str) {}
    async fn handle_pv_event(&self, _event: &PodWatchEvent, _event_name: &str) {}
}

/// Handle PersistentVolumeClaim ADDED/MODIFIED events
#[cfg(test)]
pub async fn handle_pvc_event(
    file_process: &klights_supervisor::FileProcessExecutor,
    local_path_provisioner_root: &std::path::Path,
    db: &dyn DatastoreBackend,
    event: &PodWatchEvent,
    event_name: &str,
) {
    if event.event_type != WatchEventType::Added && event.event_type != WatchEventType::Modified {
        return;
    }

    tracing::info!(
        "Resource watcher received {} event for PVC {}",
        event.event_type.as_str(),
        event_name
    );

    if let Ok(Some(pvc_resource)) = db
        .get_resource(
            "v1",
            "PersistentVolumeClaim",
            event
                .object
                .pointer("/metadata/namespace")
                .and_then(|n| n.as_str())
                .map(String::from)
                .as_deref(),
            event_name,
        )
        .await
    {
        let reconcile = crate::bootstrap::controller_adapters::pod_reconcile_adapter::PersistentVolumeReconcileAdapter::new(
            db,
            file_process,
            local_path_provisioner_root,
        );
        match reconcile.reconcile_pvc(pvc_resource).await {
            Ok(outcome) => {
                if let Some(phase) = outcome.phase.as_deref() {
                    if phase == "Bound" {
                        let volume_name = outcome.volume_name.as_deref().unwrap_or("unknown");
                        tracing::info!("PVC {} bound to PV {}", event_name, volume_name);
                    } else {
                        tracing::info!("PVC {} remains Pending (no matching PV found)", event_name);
                    }
                }
            }
            Err(e) => {
                tracing::error!("Failed to reconcile PVC {}: {:#}", event_name, e);
            }
        }
    }
}

/// Handle PersistentVolume ADDED events
#[cfg(test)]
pub async fn handle_pv_event(
    file_process: &klights_supervisor::FileProcessExecutor,
    local_path_provisioner_root: &std::path::Path,
    db: &dyn DatastoreBackend,
    event: &PodWatchEvent,
    event_name: &str,
) {
    if event.event_type != WatchEventType::Added {
        return;
    }

    tracing::info!(
        "Resource watcher received ADDED event for PV {}",
        event_name
    );

    // When a new PV is created, scan for Pending PVCs and try to bind them
    match db
        .list_resources(
            "v1",
            "PersistentVolumeClaim",
            None,
            crate::datastore::ResourceListQuery::new(
                // all namespaces
                None, None, None, None,
            ),
        )
        .await
    {
        Ok(pvc_list) => {
            for pvc_resource in &pvc_list.items {
                // Only reconcile Pending PVCs
                let phase = pvc_resource
                    .data
                    .pointer("/status/phase")
                    .and_then(|p| p.as_str());

                if phase != Some("Bound") {
                    let reconcile =
                        crate::bootstrap::controller_adapters::pod_reconcile_adapter::PersistentVolumeReconcileAdapter::new(
                            db,
                            file_process,
                            local_path_provisioner_root,
                        );
                    if let Err(e) = reconcile.reconcile_pvc(pvc_resource.clone()).await {
                        let pvc_name = pvc_resource.name.as_str();
                        tracing::warn!(
                            "Failed to reconcile PVC {} after PV creation: {:#}",
                            pvc_name,
                            e
                        );
                    }
                }
            }
        }
        Err(e) => {
            tracing::error!("Failed to list PVCs for PV binding: {:#}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
                json!({
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
                json!({
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

    #[tokio::test]
    async fn follower_pvc_event_does_not_reconcile_until_leadership_gained() {
        let (db, _pv, pvc) = seed_matching_pv_and_pvc().await;
        let (leader_tx, leader_rx) = tokio::sync::watch::channel(false);
        let handler = DatastorePersistentVolumeEventHandler::new(
            db.clone(),
            leader_rx,
            crate::kubelet::file_blocking::test_file_process_executor(),
        );

        // While not leader, a PVC event must not originate a binding write.
        handler
            .handle_pvc_event(&PodWatchEvent::added((*pvc.data).clone()), "test-pvc")
            .await;
        let pvc_after = db
            .get_resource("v1", "PersistentVolumeClaim", Some("default"), "test-pvc")
            .await
            .unwrap()
            .unwrap();
        assert_ne!(
            pvc_after
                .data
                .pointer("/status/phase")
                .and_then(|phase| phase.as_str()),
            Some("Bound")
        );

        // After a live leadership transition, the same handler reconciles.
        leader_tx.send(true).unwrap();
        handler
            .handle_pvc_event(&PodWatchEvent::added((*pvc.data).clone()), "test-pvc")
            .await;
        let pvc_bound = db
            .get_resource("v1", "PersistentVolumeClaim", Some("default"), "test-pvc")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            pvc_bound
                .data
                .pointer("/status/phase")
                .and_then(|phase| phase.as_str()),
            Some("Bound")
        );
    }

    #[tokio::test]
    async fn leader_pv_event_binds_pending_pvc_with_claimref_agreement() {
        let (db, pv, _pvc) = seed_matching_pv_and_pvc().await;
        let (_leader_tx, leader_rx) = tokio::sync::watch::channel(true);
        let handler = DatastorePersistentVolumeEventHandler::new(
            db.clone(),
            leader_rx,
            crate::kubelet::file_blocking::test_file_process_executor(),
        );

        handler
            .handle_pv_event(&PodWatchEvent::added((*pv.data).clone()), "test-pv")
            .await;

        let pvc_after = db
            .get_resource("v1", "PersistentVolumeClaim", Some("default"), "test-pvc")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            pvc_after
                .data
                .pointer("/status/phase")
                .and_then(|phase| phase.as_str()),
            Some("Bound")
        );
        assert_eq!(
            pvc_after
                .data
                .pointer("/status/volumeName")
                .and_then(|v| v.as_str()),
            Some("test-pv")
        );
        let pv_after = db
            .get_resource("v1", "PersistentVolume", None, "test-pv")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            pv_after
                .data
                .pointer("/spec/claimRef/name")
                .and_then(|v| v.as_str()),
            Some("test-pvc")
        );
    }

    #[tokio::test]
    async fn leader_pv_event_picks_smallest_sufficient_pv() {
        let db: crate::datastore::DatastoreHandle = std::sync::Arc::new(
            crate::datastore::sqlite::Datastore::new_in_memory()
                .await
                .unwrap(),
        );
        db.create_resource(
            "v1",
            "PersistentVolume",
            None,
            "big-pv",
            json!({
                "apiVersion": "v1",
                "kind": "PersistentVolume",
                "metadata": {"name": "big-pv"},
                "spec": {"capacity": {"storage": "5Gi"}, "accessModes": ["ReadWriteOnce"]},
                "status": {"phase": "Available"}
            }),
        )
        .await
        .unwrap();
        let small_pv = db
            .create_resource(
                "v1",
                "PersistentVolume",
                None,
                "small-pv",
                json!({
                    "apiVersion": "v1",
                    "kind": "PersistentVolume",
                    "metadata": {"name": "small-pv"},
                    "spec": {"capacity": {"storage": "1Gi"}, "accessModes": ["ReadWriteOnce"]},
                    "status": {"phase": "Available"}
                }),
            )
            .await
            .unwrap();
        db.create_resource(
            "v1",
            "PersistentVolumeClaim",
            Some("default"),
            "claim",
            json!({
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": {"name": "claim", "namespace": "default"},
                "spec": {"resources": {"requests": {"storage": "1Gi"}}, "accessModes": ["ReadWriteOnce"]}
            }),
        )
        .await
        .unwrap();

        let (_leader_tx, leader_rx) = tokio::sync::watch::channel(true);
        let handler = DatastorePersistentVolumeEventHandler::new(
            db.clone(),
            leader_rx,
            crate::kubelet::file_blocking::test_file_process_executor(),
        );
        handler
            .handle_pv_event(&PodWatchEvent::added((*small_pv.data).clone()), "small-pv")
            .await;

        let pvc_after = db
            .get_resource("v1", "PersistentVolumeClaim", Some("default"), "claim")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            pvc_after
                .data
                .pointer("/status/volumeName")
                .and_then(|v| v.as_str()),
            Some("small-pv"),
            "deterministic ordering must select the smallest sufficient PV"
        );
    }

    #[tokio::test]
    async fn noop_handler_never_mutates_cluster_state() {
        let (db, pv, pvc) = seed_matching_pv_and_pvc().await;
        let handler = NoopPersistentVolumeEventHandler::new();
        handler
            .handle_pv_event(&PodWatchEvent::added((*pv.data).clone()), "test-pv")
            .await;
        handler
            .handle_pvc_event(&PodWatchEvent::added((*pvc.data).clone()), "test-pvc")
            .await;
        let pvc_after = db
            .get_resource("v1", "PersistentVolumeClaim", Some("default"), "test-pvc")
            .await
            .unwrap()
            .unwrap();
        assert_ne!(
            pvc_after
                .data
                .pointer("/status/phase")
                .and_then(|phase| phase.as_str()),
            Some("Bound")
        );
    }
}
