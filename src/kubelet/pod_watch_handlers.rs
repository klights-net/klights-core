use crate::datastore::DatastoreBackend;
use crate::watch::{EventType, WatchEvent};

/// Handle PersistentVolumeClaim ADDED/MODIFIED events
pub async fn handle_pvc_event(
    db: &dyn DatastoreBackend,
    event: &WatchEvent,
    event_name: &str,
    cluster_reconciliation_enabled: bool,
) {
    if event.event_type != EventType::Added && event.event_type != EventType::Modified {
        return;
    }

    if !cluster_reconciliation_enabled {
        tracing::debug!(
            "Skipping leader-owned PVC reconciliation for {} on non-leader kubelet context",
            event_name
        );
        return;
    }

    tracing::info!(
        "Resource watcher received {} event for PVC {}",
        event.event_type,
        event_name
    );

    // Inject resourceVersion for reconcile_pvc — needs owned Value for mutation
    let mut pvc_with_rv = (*event.object).clone();
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
        if let Some(meta) = pvc_with_rv
            .get_mut("metadata")
            .and_then(|m| m.as_object_mut())
        {
            meta.insert(
                "resourceVersion".to_string(),
                serde_json::json!(pvc_resource.resource_version.to_string()),
            );
        }

        // Reconcile PVC - bind to matching PV if available
        match crate::controllers::pvc::reconcile_pvc(db, &pvc_with_rv).await {
            Ok(updated_pvc) => {
                if let Some(phase) = updated_pvc
                    .pointer("/status/phase")
                    .and_then(|p| p.as_str())
                {
                    if phase == "Bound" {
                        let volume_name = updated_pvc
                            .pointer("/status/volumeName")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown");
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
pub async fn handle_pv_event(
    db: &dyn DatastoreBackend,
    event: &WatchEvent,
    event_name: &str,
    cluster_reconciliation_enabled: bool,
) {
    if event.event_type != EventType::Added {
        return;
    }

    if !cluster_reconciliation_enabled {
        tracing::debug!(
            "Skipping leader-owned PV reconciliation for {} on non-leader kubelet context",
            event_name
        );
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
                    // Inject resourceVersion
                    let mut pvc_with_rv: serde_json::Value = (*pvc_resource.data).clone();
                    if let Some(meta) = pvc_with_rv
                        .get_mut("metadata")
                        .and_then(|m| m.as_object_mut())
                    {
                        meta.insert(
                            "resourceVersion".to_string(),
                            serde_json::json!(pvc_resource.resource_version.to_string()),
                        );
                    }

                    if let Err(e) = crate::controllers::pvc::reconcile_pvc(db, &pvc_with_rv).await {
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
        crate::datastore::Resource,
        crate::datastore::Resource,
    ) {
        let db: crate::datastore::DatastoreHandle =
            std::sync::Arc::new(crate::datastore::test_support::in_memory().await);
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
    async fn pvc_event_does_not_reconcile_when_cluster_reconciliation_disabled() {
        let (db, _pv, pvc) = seed_matching_pv_and_pvc().await;
        handle_pvc_event(
            db.as_ref(),
            &WatchEvent::added((*pvc.data).clone()),
            "test-pvc",
            false,
        )
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

    #[tokio::test]
    async fn pv_event_does_not_reconcile_when_cluster_reconciliation_disabled() {
        let (db, pv, _pvc) = seed_matching_pv_and_pvc().await;
        handle_pv_event(
            db.as_ref(),
            &WatchEvent::added((*pv.data).clone()),
            "test-pv",
            false,
        )
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
