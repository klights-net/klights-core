//! `Controller` impl for `StatefulSet`. Registered in `ControllerDispatcher`.

use crate::controllers::controller_wrapper;
use klights_controllers::statefulset as statefulset_core;

controller_wrapper!(
    StatefulSetController,
    "statefulset",
    statefulset_core::reconcile_statefulset,
    with_node,
    with_pod_repository,
    store = statefulset_store,
    reader = pod_query,
    mutation = statefulset_mutation
);

#[cfg(test)]
use klights_controllers::statefulset::*;
#[cfg(test)]
#[path = "statefulset/tests/mod.rs"]
mod policy_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controllers::test_utils::store_and_prepare;
    use crate::controllers::{Context, Controller};
    use serde_json::json;

    #[test]
    fn test_statefulset_controller_name() {
        assert_eq!(
            StatefulSetController::new(
                crate::controllers::test_utils::deterministic_controller_identity()
            )
            .name(),
            "statefulset"
        );
    }

    #[tokio::test]
    async fn test_statefulset_controller_reconcile_creates_ordinal_pod() {
        let db = crate::datastore::test_support::in_memory().await;
        let controller = StatefulSetController::new(
            crate::controllers::test_utils::deterministic_controller_identity(),
        );

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
        let controller = StatefulSetController::new(
            crate::controllers::test_utils::deterministic_controller_identity(),
        );

        let bad = json!({"spec": {"replicas": 1}});
        assert!(controller.reconcile(bad, ctx).await.is_err());
    }
}
