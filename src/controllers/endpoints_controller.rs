//! Endpoints controller implementation using the Controller trait
//!
//! This module provides a trait-based implementation of the Endpoints controller,
//! wrapping the existing free-function reconcile logic.

use crate::controller::{Context, Controller};
use crate::controllers::endpoints as endpoints_core;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

/// Legacy `Controller` wrapper for the Endpoints reconciler.
///
/// Production Service reconciliation now owns both Endpoints and EndpointSlice
/// writes through the Service controller path, so real `v1/Endpoints` API
/// objects are not registered as dispatcher keys.
pub struct EndpointsController;

#[async_trait]
impl Controller for EndpointsController {
    fn name(&self) -> &'static str {
        "endpoints"
    }

    async fn reconcile(&self, resource: Value, ctx: Context) -> Result<()> {
        let meta = resource
            .get("metadata")
            .ok_or_else(|| anyhow::anyhow!("Missing metadata"))?;
        let name = meta
            .get("name")
            .and_then(|n| n.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing name"))?;
        let namespace = meta
            .get("namespace")
            .and_then(|n| n.as_str())
            .unwrap_or("default");

        let spec = resource
            .get("spec")
            .ok_or_else(|| anyhow::anyhow!("Missing spec"))?;
        let selector = spec.get("selector");
        let service_ports = spec.get("ports");

        // Legacy `service.alpha.kubernetes.io/tolerate-unready-endpoints`
        // annotation is still honored alongside `spec.publishNotReadyAddresses`.
        let publish_not_ready = spec
            .get("publishNotReadyAddresses")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
            || meta
                .get("annotations")
                .and_then(|a| a.get("service.alpha.kubernetes.io/tolerate-unready-endpoints"))
                .and_then(|v| v.as_str())
                .map(|v| v == "true")
                .unwrap_or(false);

        let pod_repository = ctx.pod_repository().ok_or_else(|| {
            anyhow::anyhow!(
                "endpoints controller requires pod_repository in Context — wire it via \
                 ControllerDispatcher::set_pod_repository or Context::with_pod_repository"
            )
        })?;

        endpoints_core::reconcile_endpoints(
            ctx.db_handle().as_ref(),
            pod_repository.as_ref(),
            name,
            namespace,
            selector,
            service_ports,
            publish_not_ready,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_endpoints_controller_name() {
        let controller = EndpointsController;
        assert_eq!(controller.name(), "endpoints");
    }

    #[test]
    fn test_endpoints_controller_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<EndpointsController>();
    }

    #[tokio::test]
    async fn test_endpoints_controller_reconcile_creates_endpoints() {
        let db = crate::datastore::test_support::in_memory().await;
        let ctx = crate::datastore::test_support::test_context(&db)
            .with_pod_repository(crate::controllers::test_utils::pod_repository_for_test(&db));
        let controller = EndpointsController;

        // Create a pod matching the selector
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "nginx-pod",
                "namespace": "default",
                "labels": {"app": "nginx"}
            },
            "spec": {"containers": [{"name": "nginx", "image": "nginx"}]},
            "status": {"phase": "Running", "podIP": "10.0.0.5",
                       "conditions": [{"type": "Ready", "status": "True"}]}
        });
        db.create_resource("v1", "Pod", Some("default"), "nginx-pod", pod)
            .await
            .unwrap();

        // Reconcile with a Service-shaped resource (the wrapper extracts fields)
        let service = json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "nginx-svc", "namespace": "default"},
            "spec": {
                "selector": {"app": "nginx"},
                "ports": [{"port": 80, "targetPort": 80, "protocol": "TCP"}]
            }
        });

        let result = controller.reconcile(service, ctx).await;
        assert!(result.is_ok());

        // Verify Endpoints were created
        let ep = db
            .get_resource("v1", "Endpoints", Some("default"), "nginx-svc")
            .await
            .unwrap();
        assert!(ep.is_some());
        let ep_data = ep.unwrap().data;
        let subsets = ep_data["subsets"].as_array().unwrap();
        assert_eq!(subsets.len(), 1);
        assert_eq!(
            subsets[0]["addresses"][0]["ip"].as_str().unwrap(),
            "10.0.0.5"
        );
    }

    #[tokio::test]
    async fn test_endpoints_controller_reconcile_missing_metadata_returns_error() {
        let db = crate::datastore::test_support::in_memory().await;
        let ctx = Context::new(std::sync::Arc::new(db), "test-node".to_string());
        let controller = EndpointsController;

        let bad = json!({"spec": {"selector": {"app": "x"}}});
        assert!(controller.reconcile(bad, ctx).await.is_err());
    }

    #[tokio::test]
    async fn test_endpoints_controller_reconcile_missing_spec_returns_error() {
        let db = crate::datastore::test_support::in_memory().await;
        let ctx = Context::new(std::sync::Arc::new(db), "test-node".to_string());
        let controller = EndpointsController;

        let bad = json!({"metadata": {"name": "svc", "namespace": "default"}});
        assert!(controller.reconcile(bad, ctx).await.is_err());
    }

    #[tokio::test]
    async fn test_endpoints_controller_reconcile_defaults_namespace_to_default() {
        let db = crate::datastore::test_support::in_memory().await;
        let ctx = crate::datastore::test_support::test_context(&db)
            .with_pod_repository(crate::controllers::test_utils::pod_repository_for_test(&db));
        let controller = EndpointsController;

        // Service without namespace in metadata — should default to "default"
        let service = json!({
            "metadata": {"name": "test-svc"},
            "spec": {
                "selector": {"app": "test"},
                "ports": [{"port": 80, "protocol": "TCP"}]
            }
        });

        let result = controller.reconcile(service, ctx).await;
        assert!(result.is_ok());

        let ep = db
            .get_resource("v1", "Endpoints", Some("default"), "test-svc")
            .await
            .unwrap();
        assert!(ep.is_some());
    }
}
