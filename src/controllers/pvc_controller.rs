//! `Controller` impl for `PersistentVolumeClaim`. Registered in `ControllerDispatcher`.

use crate::controller::controller_wrapper;
use crate::controllers::pvc as pvc_core;

controller_wrapper!(
    PVCController,
    "pvc",
    pvc_core::reconcile_pvc,
    no_node,
    discard
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::{Context, Controller};
    use crate::controllers::test_utils::store_and_prepare;
    use serde_json::json;

    #[test]
    fn test_pvc_controller_name() {
        assert_eq!(PVCController.name(), "pvc");
    }

    #[tokio::test]
    async fn test_pvc_controller_reconcile_binds_to_available_pv() {
        let db = crate::datastore::test_support::in_memory().await;
        let controller = PVCController;

        // Create an available PV
        let pv = json!({
            "apiVersion": "v1",
            "kind": "PersistentVolume",
            "metadata": {"name": "pv-1"},
            "spec": {
                "capacity": {"storage": "10Gi"},
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": "manual"
            },
            "status": {"phase": "Available"}
        });
        db.create_resource("v1", "PersistentVolume", None, "pv-1", pv)
            .await
            .unwrap();

        let pvc = store_and_prepare(
            &db,
            "v1",
            "PersistentVolumeClaim",
            Some("default"),
            "my-pvc",
            json!({
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": {"name": "my-pvc", "namespace": "default", "uid": "pvc-uid-1"},
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "storageClassName": "manual",
                    "resources": {"requests": {"storage": "10Gi"}}
                }
            }),
        )
        .await;

        let ctx = crate::datastore::test_support::test_context(&db);
        let result = controller.reconcile(pvc, ctx).await;
        assert!(result.is_ok(), "reconcile failed: {}", result.unwrap_err());
    }

    #[tokio::test]
    async fn test_pvc_controller_reconcile_missing_metadata_returns_error() {
        let db = crate::datastore::test_support::in_memory().await;
        let ctx = Context::new(std::sync::Arc::new(db), "test-node".to_string());
        let controller = PVCController;

        let bad = json!({"spec": {}});
        assert!(controller.reconcile(bad, ctx).await.is_err());
    }
}
