//! `Controller` impl for `Deployment`. Registered in `ControllerDispatcher`.

use crate::controller::controller_wrapper;
use crate::controllers::deployment as deployment_core;

controller_wrapper!(
    DeploymentController,
    "deployment",
    deployment_core::reconcile_deployment,
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
    fn test_deployment_controller_name() {
        assert_eq!(DeploymentController.name(), "deployment");
    }

    #[tokio::test]
    async fn test_deployment_controller_reconcile_creates_replicaset() {
        let db = crate::datastore::test_support::in_memory().await;
        let controller = DeploymentController;

        let deployment = store_and_prepare(
            &db,
            "apps/v1",
            "Deployment",
            Some("default"),
            "nginx",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "nginx",
                    "namespace": "default",
                    "uid": "deploy-uid-1"
                },
                "spec": {
                    "replicas": 2,
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
        let result = controller.reconcile(deployment, ctx).await;
        assert!(result.is_ok(), "reconcile failed: {}", result.unwrap_err());

        let rs_list = db
            .list_resources(
                "apps/v1",
                "ReplicaSet",
                Some("default"),
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .unwrap();
        assert_eq!(rs_list.items.len(), 1);
        assert!(
            rs_list.items[0].data["metadata"]["name"]
                .as_str()
                .unwrap()
                .starts_with("nginx-")
        );
    }

    #[tokio::test]
    async fn test_deployment_controller_reconcile_missing_metadata_returns_error() {
        let db = crate::datastore::test_support::in_memory().await;
        let ctx = Context::new(std::sync::Arc::new(db), "test-node".to_string());
        let controller = DeploymentController;

        let bad_resource = json!({"spec": {}});
        assert!(controller.reconcile(bad_resource, ctx).await.is_err());
    }
}
