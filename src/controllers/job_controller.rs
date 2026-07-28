//! `Controller` impl for `Job`. Registered in `ControllerDispatcher`.

use crate::controllers::job as job_core;

pub struct JobController;

#[async_trait::async_trait]
impl crate::controllers::Controller for JobController {
    fn name(&self) -> &'static str {
        "job"
    }

    async fn reconcile(
        &self,
        resource: serde_json::Value,
        ctx: crate::controllers::Context,
    ) -> anyhow::Result<()> {
        job_core::reconcile_job(
            ctx.job_store(),
            ctx.pod_query(),
            ctx.job_mutation(),
            ctx.pod_delete_sink(),
            ctx.reconcile_port().non_pod_finalization(),
            &resource,
            crate::controllers::ControllerReconcileContext::at(
                ctx.coordination(),
                ctx.node_name(),
                ctx.reconcile_time(),
            ),
            ctx.reconcile_time(),
        )
        .await
        .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controllers::test_utils::store_and_prepare;
    use crate::controllers::{Context, Controller};
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
        let ctx = Context::new(std::sync::Arc::new(db.clone()), "test-node".to_string())
            .with_pod_repository(crate::controllers::test_utils::pod_repository_for_test(&db));
        let controller = JobController;

        let bad = store_and_prepare(
            &db,
            "batch/v1",
            "Job",
            Some("default"),
            "x",
            json!({
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {"name": "x", "namespace": "default", "uid": "u"},
                "spec": {"completions": 1}
            }),
        )
        .await;
        assert!(controller.reconcile(bad, ctx).await.is_err());
    }
}
