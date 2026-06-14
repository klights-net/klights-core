//! `Controller` impl for `DaemonSet`. Registered in `ControllerDispatcher`.

use crate::controller::controller_wrapper;
use crate::controllers::daemonset as daemonset_core;

controller_wrapper!(
    DaemonSetController,
    "daemonset",
    daemonset_core::reconcile_daemonset,
    no_node,
    with_pod_repository
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::Controller;
    use crate::controllers::test_utils::store_and_prepare;
    use serde_json::json;

    #[test]
    fn test_daemonset_controller_name() {
        assert_eq!(DaemonSetController.name(), "daemonset");
    }

    #[tokio::test]
    async fn test_daemonset_controller_reconcile_creates_pod_per_node() {
        let db = crate::datastore::test_support::in_memory().await;
        let controller = DaemonSetController;

        // DaemonSet reconcile lists nodes — create one
        let node = json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "test-node"},
            "status": {"conditions": [{"type": "Ready", "status": "True"}]}
        });
        db.create_resource("v1", "Node", None, "test-node", node)
            .await
            .unwrap();

        let ds = store_and_prepare(
            &db,
            "apps/v1",
            "DaemonSet",
            Some("default"),
            "fluentd",
            json!({
                "apiVersion": "apps/v1",
                "kind": "DaemonSet",
                "metadata": {"name": "fluentd", "namespace": "default", "uid": "ds-uid-1"},
                "spec": {
                    "selector": {"matchLabels": {"app": "fluentd"}},
                    "template": {
                        "metadata": {"labels": {"app": "fluentd"}},
                        "spec": {"containers": [{"name": "fluentd", "image": "fluentd:latest"}]}
                    }
                }
            }),
        )
        .await;

        let ctx = crate::datastore::test_support::test_context(&db)
            .with_pod_repository(crate::controllers::test_utils::pod_repository_for_test(&db));
        let result = controller.reconcile(ds, ctx).await;
        assert!(result.is_ok(), "reconcile failed: {}", result.unwrap_err());

        let pods = db
            .list_resources(
                "v1",
                "Pod",
                Some("default"),
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .unwrap();
        assert_eq!(pods.items.len(), 1);
        assert_eq!(
            pods.items[0].data["spec"]["nodeName"].as_str().unwrap(),
            "test-node"
        );
    }

    #[tokio::test]
    async fn test_daemonset_controller_reconcile_no_nodes_creates_no_pods() {
        let db = crate::datastore::test_support::in_memory().await;
        let controller = DaemonSetController;

        let ds = store_and_prepare(
            &db,
            "apps/v1",
            "DaemonSet",
            Some("default"),
            "fluentd",
            json!({
                "apiVersion": "apps/v1",
                "kind": "DaemonSet",
                "metadata": {"name": "fluentd", "namespace": "default", "uid": "ds-uid-2"},
                "spec": {
                    "selector": {"matchLabels": {"app": "fluentd"}},
                    "template": {
                        "metadata": {"labels": {"app": "fluentd"}},
                        "spec": {"containers": [{"name": "fluentd", "image": "fluentd:latest"}]}
                    }
                }
            }),
        )
        .await;

        let ctx = crate::datastore::test_support::test_context(&db)
            .with_pod_repository(crate::controllers::test_utils::pod_repository_for_test(&db));
        let result = controller.reconcile(ds, ctx).await;
        assert!(result.is_ok(), "reconcile failed: {}", result.unwrap_err());

        let pods = db
            .list_resources(
                "v1",
                "Pod",
                Some("default"),
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .unwrap();
        assert_eq!(pods.items.len(), 0);
    }
}
