//! `Controller` impl for `Job`. Registered in `ControllerDispatcher`.

use crate::controller::controller_wrapper;
use crate::controllers::job as job_core;

controller_wrapper!(
    JobController,
    "job",
    job_core::reconcile_job,
    with_node,
    discard,
    with_pod_repository
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::{Context, Controller};
    use crate::controllers::test_utils::store_and_prepare;
    use serde_json::json;

    #[test]
    fn test_job_controller_name() {
        assert_eq!(JobController.name(), "job");
    }

    #[tokio::test]
    async fn test_job_controller_reconcile_creates_pod() {
        let db = crate::datastore::test_support::in_memory().await;
        let controller = JobController;

        let job = store_and_prepare(
            &db, "batch/v1", "Job", Some("default"), "pi",
            json!({
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {"name": "pi", "namespace": "default", "uid": "job-uid-1"},
                "spec": {
                    "completions": 1,
                    "parallelism": 1,
                    "template": {
                        "spec": {
                            "containers": [{"name": "pi", "image": "perl", "command": ["perl", "-e", "print 3.14"]}],
                            "restartPolicy": "Never"
                        }
                    }
                }
            }),
        ).await;

        let ctx = crate::datastore::test_support::test_context(&db)
            .with_pod_repository(crate::controllers::test_utils::pod_repository_for_test(&db));
        let result = controller.reconcile(job, ctx).await;
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
        let owner_uid = pods.items[0].data["metadata"]["ownerReferences"][0]["uid"]
            .as_str()
            .unwrap();
        assert_eq!(owner_uid, "job-uid-1");
    }

    #[tokio::test]
    async fn test_job_controller_reconcile_missing_template_returns_error() {
        let db = crate::datastore::test_support::in_memory().await;
        let ctx = Context::new(std::sync::Arc::new(db), "test-node".to_string());
        let controller = JobController;

        let bad = json!({
            "metadata": {"name": "x", "namespace": "default", "uid": "u"},
            "spec": {"completions": 1}
        });
        assert!(controller.reconcile(bad, ctx).await.is_err());
    }
}
