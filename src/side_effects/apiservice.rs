//! Side effect to enqueue APIService availability reconciliation.

use super::{ControllerDispatcherSlot, SideEffect};
use crate::controllers::workqueue::ReconcileKey;
use crate::datastore::{DatastoreBackend, ResourceListQuery};
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

pub struct APIServiceReconcileEffect {
    controller_dispatcher: ControllerDispatcherSlot,
}

#[async_trait]
impl SideEffect for APIServiceReconcileEffect {
    fn name(&self) -> &'static str {
        "apiservice_reconcile"
    }

    async fn apply(&self, resource: &Value, db: &dyn DatastoreBackend) -> Result<()> {
        let Some(dispatcher) = self.controller_dispatcher.get() else {
            tracing::debug!(
                "APIServiceReconcileEffect skipped: controller dispatcher not yet bound"
            );
            return Ok(());
        };

        for key in apiservice_reconcile_keys_for_resource(resource, db).await? {
            dispatcher.enqueue_reconcile_key(key).await;
        }
        Ok(())
    }
}

pub fn apiservice_reconcile(
    controller_dispatcher: ControllerDispatcherSlot,
) -> Arc<dyn SideEffect> {
    Arc::new(APIServiceReconcileEffect {
        controller_dispatcher,
    })
}

pub async fn apiservice_reconcile_keys_for_resource(
    resource: &Value,
    db: &dyn DatastoreBackend,
) -> Result<Vec<ReconcileKey>> {
    let api_version = resource
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let kind = resource.get("kind").and_then(|v| v.as_str()).unwrap_or("");
    if (api_version, kind) == ("apiregistration.k8s.io/v1", "APIService") {
        let Some(name) = resource.pointer("/metadata/name").and_then(|v| v.as_str()) else {
            return Ok(Vec::new());
        };
        return Ok(vec![ReconcileKey::cluster(
            "apiregistration.k8s.io/v1",
            "APIService",
            name,
        )]);
    }

    if !matches!(
        (api_version, kind),
        ("v1", "Service") | ("v1", "Endpoints") | ("discovery.k8s.io/v1", "EndpointSlice")
    ) {
        return Ok(Vec::new());
    }
    let namespace = resource
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .unwrap_or("default");
    let name = if (api_version, kind) == ("discovery.k8s.io/v1", "EndpointSlice") {
        resource
            .pointer("/metadata/labels/kubernetes.io~1service-name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
    } else {
        resource
            .pointer("/metadata/name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
    };
    if name.is_empty() {
        return Ok(Vec::new());
    }

    let apiservices = db
        .list_resources(
            "apiregistration.k8s.io/v1",
            "APIService",
            None,
            ResourceListQuery::all(),
        )
        .await?;
    Ok(apiservices
        .items
        .into_iter()
        .filter(|apiservice| apiservice_targets_service(&apiservice.data, namespace, name))
        .map(|apiservice| {
            ReconcileKey::cluster("apiregistration.k8s.io/v1", "APIService", &apiservice.name)
        })
        .collect())
}

fn apiservice_targets_service(apiservice: &Value, namespace: &str, name: &str) -> bool {
    apiservice.pointer("/spec/service").is_some_and(|service| {
        service.get("name").and_then(|v| v.as_str()) == Some(name)
            && service
                .get("namespace")
                .and_then(|v| v.as_str())
                .unwrap_or("default")
                == namespace
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn service_mutation_enqueues_matching_apiservice_only() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "apiregistration.k8s.io/v1",
            "APIService",
            None,
            "v1alpha1.ready.example.com",
            json!({
                "apiVersion": "apiregistration.k8s.io/v1",
                "kind": "APIService",
                "metadata": {"name": "v1alpha1.ready.example.com"},
                "spec": {
                    "group": "ready.example.com",
                    "version": "v1alpha1",
                    "service": {"namespace": "default", "name": "ready-service"}
                }
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "apiregistration.k8s.io/v1",
            "APIService",
            None,
            "v1alpha1.other.example.com",
            json!({
                "apiVersion": "apiregistration.k8s.io/v1",
                "kind": "APIService",
                "metadata": {"name": "v1alpha1.other.example.com"},
                "spec": {
                    "group": "other.example.com",
                    "version": "v1alpha1",
                    "service": {"namespace": "default", "name": "other-service"}
                }
            }),
        )
        .await
        .unwrap();

        let keys = apiservice_reconcile_keys_for_resource(
            &json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"namespace": "default", "name": "ready-service"}
            }),
            &db,
        )
        .await
        .unwrap();

        assert_eq!(
            keys,
            vec![ReconcileKey::cluster(
                "apiregistration.k8s.io/v1",
                "APIService",
                "v1alpha1.ready.example.com"
            )]
        );
    }

    #[tokio::test]
    async fn endpointslice_mutation_enqueues_matching_apiservice_only() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "apiregistration.k8s.io/v1",
            "APIService",
            None,
            "v1alpha1.ready.example.com",
            json!({
                "apiVersion": "apiregistration.k8s.io/v1",
                "kind": "APIService",
                "metadata": {"name": "v1alpha1.ready.example.com"},
                "spec": {
                    "group": "ready.example.com",
                    "version": "v1alpha1",
                    "service": {"namespace": "default", "name": "ready-service"}
                }
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "apiregistration.k8s.io/v1",
            "APIService",
            None,
            "v1alpha1.other.example.com",
            json!({
                "apiVersion": "apiregistration.k8s.io/v1",
                "kind": "APIService",
                "metadata": {"name": "v1alpha1.other.example.com"},
                "spec": {
                    "group": "other.example.com",
                    "version": "v1alpha1",
                    "service": {"namespace": "default", "name": "other-service"}
                }
            }),
        )
        .await
        .unwrap();

        let keys = apiservice_reconcile_keys_for_resource(
            &json!({
                "apiVersion": "discovery.k8s.io/v1",
                "kind": "EndpointSlice",
                "metadata": {
                    "name": "ready-service-abc",
                    "namespace": "default",
                    "labels": {"kubernetes.io/service-name": "ready-service"}
                }
            }),
            &db,
        )
        .await
        .unwrap();

        assert_eq!(
            keys,
            vec![ReconcileKey::cluster(
                "apiregistration.k8s.io/v1",
                "APIService",
                "v1alpha1.ready.example.com"
            )]
        );
    }
}
