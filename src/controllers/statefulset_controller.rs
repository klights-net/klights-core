//! `Controller` impl for `StatefulSet`. Registered in `ControllerDispatcher`.

use crate::controller::controller_wrapper;
use crate::controllers::statefulset as statefulset_core;

controller_wrapper!(
    StatefulSetController,
    "statefulset",
    statefulset_core::reconcile_statefulset,
    with_node,
    with_pod_repository
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::{Context, Controller};
    use crate::controllers::test_utils::store_and_prepare;
    use serde_json::json;

    #[test]
    fn test_statefulset_controller_name() {
        assert_eq!(StatefulSetController.name(), "statefulset");
    }

    #[tokio::test]
    async fn test_statefulset_controller_reconcile_creates_ordinal_pod() {
        let db = crate::datastore::test_support::in_memory().await;
        let controller = StatefulSetController;

        let sts = store_and_prepare(
            &db,
            "apps/v1",
            "StatefulSet",
            Some("default"),
            "web",
            json!({
                "apiVersion": "apps/v1",
                "kind": "StatefulSet",
                "metadata": {"name": "web", "namespace": "default", "uid": "sts-uid-1"},
                "spec": {
                    "replicas": 1,
                    "serviceName": "web-headless",
                    "selector": {"matchLabels": {"app": "web"}},
                    "template": {
                        "metadata": {"labels": {"app": "web"}},
                        "spec": {"containers": [{"name": "web", "image": "nginx:1.25"}]}
                    }
                }
            }),
        )
        .await;

        let ctx = crate::datastore::test_support::test_context(&db)
            .with_pod_repository(crate::controllers::test_utils::pod_repository_for_test(&db));
        let result = controller.reconcile(sts, ctx).await;
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
            pods.items[0].data["metadata"]["name"].as_str().unwrap(),
            "web-0"
        );
    }

    #[tokio::test]
    async fn test_statefulset_controller_reconcile_missing_metadata_returns_error() {
        let db = crate::datastore::test_support::in_memory().await;
        let ctx = Context::new(std::sync::Arc::new(db), "test-node".to_string());
        let controller = StatefulSetController;

        let bad = json!({"spec": {"replicas": 1}});
        assert!(controller.reconcile(bad, ctx).await.is_err());
    }
}
