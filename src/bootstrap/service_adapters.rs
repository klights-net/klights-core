use std::sync::Arc;

pub(crate) struct ApiServiceWriteAllocator {
    db: crate::datastore::DatastoreHandle,
    service_ipam: Arc<crate::controllers::service::ServiceIpam>,
    nodeport_alloc: Arc<crate::controllers::service::NodePortAllocator>,
}

impl ApiServiceWriteAllocator {
    pub(crate) fn new(
        db: crate::datastore::DatastoreHandle,
        service_ipam: Arc<crate::controllers::service::ServiceIpam>,
        nodeport_alloc: Arc<crate::controllers::service::NodePortAllocator>,
    ) -> Arc<Self> {
        Arc::new(Self {
            db,
            service_ipam,
            nodeport_alloc,
        })
    }
}

struct ApiServiceAllocationReservation {
    pending: crate::controllers::service::PendingServiceAllocations,
    service_ipam: Arc<crate::controllers::service::ServiceIpam>,
    nodeport_alloc: Arc<crate::controllers::service::NodePortAllocator>,
}

impl klights_reconcile_api::ServiceAllocationReservation for ApiServiceAllocationReservation {
    fn release(self: Box<Self>) {
        self.pending
            .release(&self.service_ipam, &self.nodeport_alloc);
    }
}

impl klights_reconcile_api::ServiceWriteAllocator for ApiServiceWriteAllocator {
    fn is_ready(&self) -> bool {
        self.nodeport_alloc.is_ready()
    }

    fn prepare_create<'a>(
        &'a self,
        service: &'a mut serde_json::Value,
    ) -> klights_reconcile_api::ServiceAllocationFuture<
        'a,
        Box<dyn klights_reconcile_api::ServiceAllocationReservation>,
    > {
        Box::pin(async move {
            let pending = crate::controllers::service::prepare_service_for_create(
                self.db.as_ref(),
                service,
                &self.service_ipam,
                &self.nodeport_alloc,
            )
            .await
            .map_err(|error| {
                klights_reconcile_api::ReconcileSinkError::unavailable(error.to_string())
            })?;
            Ok(Box::new(ApiServiceAllocationReservation {
                pending,
                service_ipam: self.service_ipam.clone(),
                nodeport_alloc: self.nodeport_alloc.clone(),
            })
                as Box<
                    dyn klights_reconcile_api::ServiceAllocationReservation,
                >)
        })
    }

    fn allocate_after_write<'a>(
        &'a self,
        service: &'a serde_json::Value,
    ) -> klights_reconcile_api::ServiceAllocationFuture<'a, Option<serde_json::Value>> {
        Box::pin(async move {
            crate::controllers::service::allocate_service_fields_for_api_write(
                self.db.as_ref(),
                service,
                &self.service_ipam,
                &self.nodeport_alloc,
            )
            .await
            .map_err(|error| {
                klights_reconcile_api::ReconcileSinkError::unavailable(error.to_string())
            })
        })
    }

    fn release_resource(&self, service: &serde_json::Value) {
        crate::controllers::service::release_service_allocations_from_resource(
            &self.service_ipam,
            &self.nodeport_alloc,
            service,
        );
    }
}
