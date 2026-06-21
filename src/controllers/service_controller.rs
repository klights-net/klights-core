//! `Controller` impl for `Service`.

use crate::controller::{Context, Controller};
use crate::controllers::service as service_core;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

/// `Controller` impl for `Service`. Registered in `ControllerDispatcher`.
///
/// Holds `ServiceIpam` (ClusterIP allocator) and `NodePortAllocator`
/// because the `Controller` trait does not pass per-controller config
/// through `Context`.
pub struct ServiceController {
    pub service_ipam: Arc<service_core::ServiceIpam>,
    pub nodeport_alloc: Arc<service_core::NodePortAllocator>,
}

#[async_trait]
impl Controller for ServiceController {
    fn name(&self) -> &'static str {
        "service"
    }

    async fn reconcile(&self, resource: Value, ctx: Context) -> Result<()> {
        let pod_repository = ctx.pod_repository().ok_or_else(|| {
            anyhow::anyhow!(
                "service controller requires pod_repository in Context — wire it via \
                 ControllerDispatcher::set_pod_repository or Context::with_pod_repository"
            )
        })?;
        service_core::reconcile_service_with_nodeport(
            ctx.db_handle().as_ref(),
            pod_repository.as_ref(),
            &resource,
            &self.service_ipam,
            &self.nodeport_alloc,
        )
        .await?;
        if let Some(services) = ctx.services() {
            services.request_services_sync();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_service_controller_name() {
        let ipam = Arc::new(service_core::ServiceIpam::new("10.43.128.0/17"));
        let controller = ServiceController {
            service_ipam: ipam,
            nodeport_alloc: Arc::new(service_core::NodePortAllocator::new()),
        };
        assert_eq!(controller.name(), "service");
    }

    #[test]
    fn test_service_controller_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ServiceController>();
    }

    // Integration test — requires root for nftables/netlink (reconcile_service calls sync_service_rules)
    #[tokio::test]
    #[ignore]
    async fn test_service_controller_reconcile_allocates_cluster_ip() {
        let db = crate::datastore::test_support::in_memory().await;
        let ipam = Arc::new(service_core::ServiceIpam::new("10.43.128.0/17"));
        let controller = ServiceController {
            service_ipam: ipam,
            nodeport_alloc: Arc::new(service_core::NodePortAllocator::new()),
        };

        let service = json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "my-svc",
                "namespace": "default"
            },
            "spec": {
                "type": "ClusterIP",
                "selector": {"app": "web"},
                "ports": [{"port": 80, "targetPort": 8080, "protocol": "TCP"}]
            }
        });
        let created = db
            .create_resource("v1", "Service", Some("default"), "my-svc", service)
            .await
            .unwrap();
        let service_with_rv =
            crate::api::inject_resource_version(created.data, created.resource_version);

        let ctx = crate::datastore::test_support::test_context(&db);
        let result = controller.reconcile(service_with_rv, ctx).await;
        assert!(result.is_ok(), "reconcile failed: {}", result.unwrap_err());
    }

    #[tokio::test]
    async fn test_service_controller_reconcile_missing_metadata_returns_error() {
        let db = crate::datastore::test_support::in_memory().await;
        let ipam = Arc::new(service_core::ServiceIpam::new("10.43.128.0/17"));
        let ctx = Context::new(std::sync::Arc::new(db), "test-node".to_string());
        let controller = ServiceController {
            service_ipam: ipam,
            nodeport_alloc: Arc::new(service_core::NodePortAllocator::new()),
        };

        let bad = json!({"spec": {"type": "ClusterIP"}});
        assert!(controller.reconcile(bad, ctx).await.is_err());
    }

    #[tokio::test]
    async fn service_reconcile_requests_coalesced_service_sync_without_blocking_on_nft() {
        let db = crate::datastore::test_support::in_memory().await;
        let ipam = Arc::new(service_core::ServiceIpam::new("10.43.128.0/17"));
        let controller = ServiceController {
            service_ipam: ipam,
            nodeport_alloc: Arc::new(service_core::NodePortAllocator::new()),
        };
        let service = json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "my-svc",
                "namespace": "default"
            },
            "spec": {
                "type": "ClusterIP",
                "selector": {"app": "web"},
                "ports": [{"port": 80, "targetPort": 8080, "protocol": "TCP"}]
            }
        });
        let created = db
            .create_resource("v1", "Service", Some("default"), "my-svc", service)
            .await
            .unwrap();
        let service_with_rv =
            crate::api::inject_resource_version(created.data, created.resource_version);
        let services = Arc::new(crate::networking::test_support::MockServiceRouter::new());
        let ctx = Context::with_services(
            std::sync::Arc::new(db.clone()),
            "test-node".to_string(),
            services.clone(),
        )
        .with_pod_repository(crate::controllers::test_utils::pod_repository_for_test(&db));

        controller.reconcile(service_with_rv, ctx).await.unwrap();

        assert_eq!(
            services.sync_count(),
            1,
            "normal Service reconcile must enqueue a coalesced service sync"
        );
        assert_eq!(
            services.sync_now_count(),
            0,
            "normal Service reconcile must not block the API/workqueue on a full nft rebuild"
        );
    }
}
