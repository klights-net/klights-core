//! `Controller` impl for `APIService`. Registered in `ControllerDispatcher`.

use crate::controllers::apiservice as apiservice_core;
pub struct APIServiceController;

#[async_trait::async_trait]
impl crate::controllers::Controller for APIServiceController {
    fn name(&self) -> &'static str {
        "apiservice"
    }

    async fn reconcile(
        &self,
        resource: serde_json::Value,
        ctx: crate::controllers::Context,
    ) -> anyhow::Result<()> {
        apiservice_core::reconcile_apiservice(
            ctx.apiservice_store(),
            &resource,
            ctx.reconcile_time(),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controllers::Controller;

    #[test]
    fn test_apiservice_controller_name() {
        assert_eq!(APIServiceController.name(), "apiservice");
    }
}
