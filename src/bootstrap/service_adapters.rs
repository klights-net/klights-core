use std::sync::Arc;

pub(crate) struct ApiServiceWriteAllocator {
    service_store: Arc<dyn klights_controllers::service::ServiceReconcileStore>,
    service_ipam: Arc<klights_controllers::service::ServiceIpam>,
    nodeport_alloc: Arc<klights_controllers::service::NodePortAllocator>,
    identity: Arc<dyn klights_controllers::ControllerIdentityGenerator>,
}

impl ApiServiceWriteAllocator {
    pub(crate) fn new(
        service_store: Arc<dyn klights_controllers::service::ServiceReconcileStore>,
        service_ipam: Arc<klights_controllers::service::ServiceIpam>,
        nodeport_alloc: Arc<klights_controllers::service::NodePortAllocator>,
        identity: Arc<dyn klights_controllers::ControllerIdentityGenerator>,
    ) -> Arc<Self> {
        Arc::new(Self {
            service_store,
            service_ipam,
            nodeport_alloc,
            identity,
        })
    }
}

struct ApiServiceAllocationReservation {
    pending: klights_controllers::service::PendingServiceAllocations,
    service_ipam: Arc<klights_controllers::service::ServiceIpam>,
    nodeport_alloc: Arc<klights_controllers::service::NodePortAllocator>,
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
            let pending = klights_controllers::service::prepare_service_for_create(
                self.service_store.as_ref(),
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
            klights_controllers::service::allocate_service_fields_for_api_write(
                self.service_store.as_ref(),
                service,
                &self.service_ipam,
                &self.nodeport_alloc,
                chrono::Utc::now(),
                self.identity.as_ref(),
            )
            .await
            .map_err(|error| {
                klights_reconcile_api::ReconcileSinkError::unavailable(error.to_string())
            })
        })
    }

    fn release_resource(&self, service: &serde_json::Value) {
        klights_controllers::service::release_service_allocations_from_resource(
            &self.service_ipam,
            &self.nodeport_alloc,
            service,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::ApiServiceWriteAllocator;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use klights_cluster_core::{Resource, ResourcePreconditions};
    use klights_controllers::service::ServiceReconcileStore;
    use klights_reconcile_api::{
        ControllerStoreError, ControllerStoreResult, ServiceWriteAllocator,
    };

    struct RejectingAuthorityStore {
        live: Resource,
        update_calls: AtomicUsize,
        observed_preconditions: std::sync::Mutex<Option<ResourcePreconditions>>,
    }

    struct FixedIdentity;

    impl klights_controllers::ControllerIdentityGenerator for FixedIdentity {
        fn generate_name(&self, prefix: &str) -> String {
            format!("{prefix}fixed")
        }

        fn new_uid(&self) -> String {
            "00000000-0000-4000-8000-000000000001".to_string()
        }
    }

    #[async_trait]
    impl ServiceReconcileStore for RejectingAuthorityStore {
        async fn list_services(&self) -> ControllerStoreResult<Vec<Resource>> {
            Ok(vec![self.live.clone()])
        }

        async fn get_service(
            &self,
            namespace: &str,
            name: &str,
        ) -> ControllerStoreResult<Option<Resource>> {
            assert_eq!(namespace, "services");
            assert_eq!(name, "externalname-service");
            Ok(Some(self.live.clone()))
        }

        async fn update_service(
            &self,
            namespace: &str,
            name: &str,
            _data: serde_json::Value,
            preconditions: ResourcePreconditions,
        ) -> ControllerStoreResult<Resource> {
            assert_eq!(namespace, "services");
            assert_eq!(name, "externalname-service");
            self.update_calls.fetch_add(1, Ordering::SeqCst);
            *self.observed_preconditions.lock().unwrap() = Some(preconditions);
            Err(ControllerStoreError::unavailable(
                "follower authority rejected Service allocation",
            ))
        }
    }

    #[tokio::test]
    async fn post_write_service_allocation_routes_through_authority_without_local_mutation() {
        let live_data = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "externalname-service",
                "namespace": "services",
                "uid": "service-uid",
                "resourceVersion": "41"
            },
            "spec": {
                "type": "ClusterIP",
                "ports": [{"port": 80}]
            }
        });
        let authority = std::sync::Arc::new(RejectingAuthorityStore {
            live: Resource {
                id: 1,
                api_version: "v1".to_string(),
                kind: "Service".to_string(),
                namespace: Some("services".to_string()),
                name: "externalname-service".to_string(),
                uid: "service-uid".to_string(),
                resource_version: 41,
                data: std::sync::Arc::new(live_data.clone()),
            },
            update_calls: AtomicUsize::new(0),
            observed_preconditions: std::sync::Mutex::new(None),
        });
        let allocator = ApiServiceWriteAllocator::new(
            authority.clone(),
            std::sync::Arc::new(klights_controllers::service::ServiceIpam::new(
                "10.51.0.0/16",
            )),
            std::sync::Arc::new(klights_controllers::service::NodePortAllocator::new()),
            std::sync::Arc::new(FixedIdentity),
        );

        let error = allocator
            .allocate_after_write(&live_data)
            .await
            .expect_err("follower authority must reject allocation");

        assert!(error.to_string().contains("follower authority rejected"));
        assert_eq!(authority.update_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            *authority.observed_preconditions.lock().unwrap(),
            Some(ResourcePreconditions::uid_and_resource_version(
                "service-uid",
                41,
            )),
            "post-write allocation must retain strict UID/RV CAS"
        );
        assert_eq!(
            authority.live.data.as_ref(),
            &live_data,
            "authority rejection must not mutate the locally observed Service"
        );
    }
}
