//! `Controller` impl for `ReplicaSet`. Registered in `ControllerDispatcher`.

use crate::controller::controller_wrapper;
use crate::controllers::replicaset as replicaset_core;

controller_wrapper!(
    ReplicaSetController,
    "replicaset",
    replicaset_core::reconcile_replicaset,
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
    fn test_replicaset_controller_name() {
        assert_eq!(ReplicaSetController.name(), "replicaset");
    }

    #[tokio::test]
    async fn test_replicaset_controller_reconcile_creates_pods() {
        let db = crate::datastore::test_support::in_memory().await;
        let controller = ReplicaSetController;

        let rs = store_and_prepare(
            &db,
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "nginx-rs",
            json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "metadata": {"name": "nginx-rs", "namespace": "default", "uid": "rs-uid-1"},
                "spec": {
                    "replicas": 3,
                    "selector": {"matchLabels": {"app": "nginx"}},
                    "template": {
                        "metadata": {"labels": {"app": "nginx"}},
                        "spec": {"containers": [{"name": "nginx", "image": "nginx:1.25"}]}
                    }
                }
            }),
        )
        .await;

        let ctx = crate::datastore::test_support::test_context(&db)
            .with_pod_repository(crate::controllers::test_utils::pod_repository_for_test(&db));
        let result = controller.reconcile(rs, ctx).await;
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
        assert_eq!(pods.items.len(), 3);

        for pod in &pods.items {
            let owner_uid = pod.data["metadata"]["ownerReferences"][0]["uid"]
                .as_str()
                .unwrap();
            assert_eq!(owner_uid, "rs-uid-1");
        }
    }

    #[tokio::test]
    async fn test_replicaset_controller_reconcile_missing_spec_returns_error() {
        let db = crate::datastore::test_support::in_memory().await;
        let ctx = Context::new(std::sync::Arc::new(db), "test-node".to_string());
        let controller = ReplicaSetController;

        let bad = json!({"metadata": {"name": "x", "namespace": "default", "uid": "u"}});
        assert!(controller.reconcile(bad, ctx).await.is_err());
    }
}
