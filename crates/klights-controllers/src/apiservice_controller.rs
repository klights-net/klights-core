//! `Controller` wrapper for `APIService`.

pub struct APIServiceController;

#[async_trait::async_trait]
impl crate::Controller for APIServiceController {
    fn name(&self) -> &'static str {
        "apiservice"
    }

    async fn reconcile(
        &self,
        resource: serde_json::Value,
        context: crate::Context,
    ) -> anyhow::Result<()> {
        crate::apiservice::reconcile_apiservice(
            context.apiservice_store(),
            &resource,
            context.reconcile_time(),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::Controller;

    #[test]
    fn controller_name_is_stable() {
        assert_eq!(super::APIServiceController.name(), "apiservice");
    }
}
