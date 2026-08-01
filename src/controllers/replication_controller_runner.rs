//! `Controller` impl for `ReplicationController`. Registered in `ControllerDispatcher`.

use crate::controllers::controller_wrapper;
use klights_controllers::replicationcontroller as rc_core;

controller_wrapper!(
    ReplicationControllerController,
    "replicationcontroller",
    rc_core::reconcile_replicationcontroller,
    with_node,
    with_pod_repository,
    store = replicationcontroller_store,
    reader = pod_query,
    mutation = replicationcontroller_mutation
);

#[cfg(test)]
use klights_controllers::replicationcontroller::*;
#[cfg(test)]
#[path = "replicationcontroller/condition_tests.rs"]
mod condition_policy_tests;
#[cfg(test)]
#[path = "replicationcontroller/tests.rs"]
mod policy_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controllers::Controller;
    use serde_json::json;

    #[test]
    fn test_replicationcontroller_controller_name() {
        assert_eq!(
            ReplicationControllerController::new(
                crate::controllers::test_utils::deterministic_controller_identity()
            )
            .name(),
            "replicationcontroller"
        );
    }

    #[tokio::test]
    async fn test_replicationcontroller_controller_creates_pods() {
        let db = crate::datastore::test_support::in_memory().await;
        let controller = ReplicationControllerController::new(
            crate::controllers::test_utils::deterministic_controller_identity(),
        );

        let rc = db
            .create_resource(
                "v1",
                "ReplicationController",
                Some("default"),
                "test-rc",
                json!({
                    "apiVersion": "v1",
                    "kind": "ReplicationController",
                    "metadata": {"name": "test-rc", "namespace": "default", "uid": "rc-uid-1"},
                    "spec": {
                        "replicas": 2,
                        "selector": {"app": "test"},
                        "template": {
                            "metadata": {"labels": {"app": "test"}},
                            "spec": {"containers": [{"name": "test", "image": "busybox"}]}
                        }
                    }
                }),
            )
            .await
            .unwrap();

        let ctx = crate::datastore::test_support::test_context(&db)
            .with_pod_repository(crate::controllers::test_utils::pod_repository_for_test(&db));
        let result = controller
            .reconcile(std::sync::Arc::unwrap_or_clone(rc.data), ctx)
            .await;
        assert!(
            result.is_ok(),
            "reconcile failed: {:?}",
            result.unwrap_err()
        );

        let pods = db
            .list_resources(
                "v1",
                "Pod",
                Some("default"),
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .unwrap();
        assert_eq!(pods.items.len(), 2, "should create 2 pods");
    }
}
