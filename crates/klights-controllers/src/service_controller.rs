//! `Controller` wrapper for `Service`.

use std::sync::Arc;

pub struct ServiceController {
    pub(crate) service_ipam: Arc<crate::service::ServiceIpam>,
    pub(crate) nodeport_alloc: Arc<crate::service::NodePortAllocator>,
    pub(crate) identity: Arc<dyn crate::ControllerIdentityGenerator>,
}

#[async_trait::async_trait]
impl crate::Controller for ServiceController {
    fn name(&self) -> &'static str {
        "service"
    }

    async fn reconcile(
        &self,
        resource: serde_json::Value,
        context: crate::Context,
    ) -> anyhow::Result<()> {
        crate::service::reconcile_service_with_nodeport_at(
            context.service_store(),
            context.pod_query(),
            &resource,
            &self.service_ipam,
            &self.nodeport_alloc,
            context.reconcile_time(),
            self.identity.as_ref(),
        )
        .await?;
        context.network().service_router().request_services_sync()?;
        Ok(())
    }
}
