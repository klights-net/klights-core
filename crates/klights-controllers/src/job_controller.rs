//! `Controller` wrapper for `Job`.

pub struct JobController {
    identity: std::sync::Arc<dyn crate::ControllerIdentityGenerator>,
}

impl JobController {
    pub(crate) fn new(identity: std::sync::Arc<dyn crate::ControllerIdentityGenerator>) -> Self {
        Self { identity }
    }
}

#[async_trait::async_trait]
impl crate::Controller for JobController {
    fn name(&self) -> &'static str {
        "job"
    }

    async fn reconcile(
        &self,
        resource: serde_json::Value,
        context: crate::Context,
    ) -> anyhow::Result<()> {
        crate::job::reconcile_job(
            context.job_store(),
            context.pod_query(),
            context.job_mutation(),
            self.identity.as_ref(),
            context.pod_delete_sink(),
            context.reconcile_port().non_pod_finalization(),
            &resource,
            crate::ControllerReconcileContext::at(
                context.coordination(),
                context.node_name(),
                context.reconcile_time(),
            ),
            context.reconcile_time(),
        )
        .await
        .map(|_| ())
    }
}
