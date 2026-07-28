//! APIService availability controller.
//!
//! Keeps `.status.conditions[Available]` aligned with the backing Service and
//! Endpoints objects so the apiregistration API behaves like a small
//! kube-aggregator control plane instead of a passive proxy registry.

use anyhow::{Context as _, Result};
use async_trait::async_trait;
use klights_cluster_core::Resource;
use klights_reconcile_api::{ControllerStoreError, ControllerStoreResult};
use serde_json::{Value, json};

const MAX_RETRIES: u32 = 5;

#[async_trait]
pub(crate) trait ApiServiceStore: Send + Sync {
    async fn get_apiservice(&self, name: &str) -> ControllerStoreResult<Option<Resource>>;

    async fn service_exists(&self, namespace: &str, name: &str) -> ControllerStoreResult<bool>;

    async fn list_endpoint_slices(
        &self,
        namespace: &str,
        service_name: &str,
    ) -> ControllerStoreResult<Vec<Resource>>;

    async fn get_endpoints(
        &self,
        namespace: &str,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>>;

    async fn update_apiservice_status(
        &self,
        current: &Resource,
        status: Value,
    ) -> ControllerStoreResult<()>;
}

pub(crate) async fn reconcile_apiservice<S: ApiServiceStore + ?Sized>(
    store: &S,
    apiservice: &Value,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let name = apiservice
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .context("APIService missing metadata.name")?;

    let mut last_conflict = None;
    for _ in 0..MAX_RETRIES {
        let current = store
            .get_apiservice(name)
            .await?
            .context("APIService not found")?;
        let status = evaluate_apiservice_status(store, &current.data, now).await?;
        if current.data.get("status") == Some(&status) {
            return Ok(());
        }

        match store.update_apiservice_status(&current, status).await {
            Ok(_) => return Ok(()),
            Err(err @ ControllerStoreError::Conflict(_)) => {
                last_conflict = Some(err);
                continue;
            }
            Err(err) => return Err(err.into()),
        }
    }

    match last_conflict {
        Some(err) => Err(err).context("APIService status update conflict retries exhausted"),
        None => Ok(()),
    }
}

async fn evaluate_apiservice_status<S: ApiServiceStore + ?Sized>(
    store: &S,
    apiservice: &Value,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Value> {
    let Some(service) = apiservice
        .pointer("/spec/service")
        .and_then(|v| v.as_object())
    else {
        return Ok(status_with_available(
            apiservice,
            now,
            "True",
            "Local",
            "APIService is handled locally",
        ));
    };
    let namespace = service
        .get("namespace")
        .and_then(|v| v.as_str())
        .unwrap_or("default");
    let Some(name) = service.get("name").and_then(|v| v.as_str()) else {
        return Ok(status_with_available(
            apiservice,
            now,
            "False",
            "ServiceNotFound",
            "APIService spec.service.name is missing",
        ));
    };

    if !store.service_exists(namespace, name).await? {
        return Ok(status_with_available(
            apiservice,
            now,
            "False",
            "ServiceNotFound",
            format!("APIService backend Service {namespace}/{name} not found"),
        ));
    }

    let endpoint_slices = store.list_endpoint_slices(namespace, name).await?;
    if !endpoint_slices.is_empty() {
        let slice_refs: Vec<&Value> = endpoint_slices
            .iter()
            .map(|slice| slice.data.as_ref())
            .collect();
        if endpointslices_have_ready_address(&slice_refs) {
            return Ok(status_with_available(
                apiservice,
                now,
                "True",
                "Passed",
                "all checks passed",
            ));
        }

        return Ok(status_with_available(
            apiservice,
            now,
            "False",
            "MissingEndpoints",
            format!("APIService backend EndpointSlice {namespace}/{name} has no ready addresses"),
        ));
    }

    let Some(endpoints) = store.get_endpoints(namespace, name).await? else {
        return Ok(status_with_available(
            apiservice,
            now,
            "False",
            "EndpointsNotFound",
            format!("APIService backend Endpoints {namespace}/{name} not found"),
        ));
    };

    if !endpoints_have_ready_address(&endpoints.data) {
        return Ok(status_with_available(
            apiservice,
            now,
            "False",
            "MissingEndpoints",
            format!("APIService backend Endpoints {namespace}/{name} has no ready addresses"),
        ));
    }

    Ok(status_with_available(
        apiservice,
        now,
        "True",
        "Passed",
        "all checks passed",
    ))
}

fn endpointslices_have_ready_address(slices: &[&Value]) -> bool {
    slices.iter().any(|slice| {
        slice
            .get("endpoints")
            .and_then(|v| v.as_array())
            .is_some_and(|endpoints| {
                endpoints.iter().any(|endpoint| {
                    let ready = endpoint
                        .pointer("/conditions/ready")
                        .and_then(Value::as_bool)
                        .unwrap_or(true);
                    ready
                        && endpoint
                            .get("addresses")
                            .and_then(Value::as_array)
                            .is_some_and(|addresses| {
                                addresses.iter().any(|address| {
                                    address.as_str().is_some_and(|value| !value.is_empty())
                                })
                            })
                })
            })
    })
}

fn endpoints_have_ready_address(endpoints: &Value) -> bool {
    endpoints
        .get("subsets")
        .and_then(|v| v.as_array())
        .is_some_and(|subsets| {
            subsets.iter().any(|subset| {
                subset
                    .get("addresses")
                    .and_then(|v| v.as_array())
                    .is_some_and(|addresses| !addresses.is_empty())
            })
        })
}

fn status_with_available(
    apiservice: &Value,
    now: chrono::DateTime<chrono::Utc>,
    status: &'static str,
    reason: &'static str,
    message: impl Into<String>,
) -> Value {
    let mut conditions = apiservice
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|condition| condition.get("type").and_then(|v| v.as_str()) != Some("Available"))
        .collect::<Vec<_>>();
    let last_transition_time = existing_available_transition_time(apiservice, status)
        .unwrap_or_else(|| crate::k8s_time::format_legacy_timestamp(now));
    conditions.push(json!({
        "type": "Available",
        "status": status,
        "reason": reason,
        "message": message.into(),
        "lastTransitionTime": last_transition_time
    }));
    json!({ "conditions": conditions })
}

fn existing_available_transition_time(apiservice: &Value, status: &str) -> Option<String> {
    apiservice
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .and_then(|conditions| {
            conditions.iter().find(|condition| {
                condition.get("type").and_then(|v| v.as_str()) == Some("Available")
                    && condition.get("status").and_then(|v| v.as_str()) == Some(status)
            })
        })
        .and_then(|condition| condition.get("lastTransitionTime"))
        .and_then(|v| v.as_str())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn apiservice_available_when_ready_endpointslice_exists_without_legacy_endpoints() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Service",
            Some("default"),
            "wardle-service",
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": "wardle-service", "namespace": "default"},
                "spec": {"ports": [{"name": "https", "port": 443, "targetPort": 8443, "protocol": "TCP"}]}
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            "wardle-service-abc",
            json!({
                "apiVersion": "discovery.k8s.io/v1",
                "kind": "EndpointSlice",
                "metadata": {
                    "name": "wardle-service-abc",
                    "namespace": "default",
                    "labels": {"kubernetes.io/service-name": "wardle-service"}
                },
                "addressType": "IPv4",
                "ports": [{"name": "https", "port": 8443, "protocol": "TCP"}],
                "endpoints": [{"addresses": ["10.42.0.25"], "conditions": {"ready": true}}]
            }),
        )
        .await
        .unwrap();

        let apiservice = json!({
            "apiVersion": "apiregistration.k8s.io/v1",
            "kind": "APIService",
            "metadata": {"name": "v1alpha1.wardle.example.com"},
            "spec": {
                "group": "wardle.example.com",
                "version": "v1alpha1",
                "service": {"namespace": "default", "name": "wardle-service"}
            }
        });

        let status = evaluate_apiservice_status(&db, &apiservice, chrono::Utc::now())
            .await
            .unwrap();
        assert_eq!(available_condition_status(&status), Some("True"));
    }

    #[tokio::test]
    async fn apiservice_unavailable_when_endpointslice_has_no_ready_addresses() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Service",
            Some("default"),
            "wardle-service",
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": "wardle-service", "namespace": "default"},
                "spec": {"ports": [{"name": "https", "port": 443, "targetPort": 8443, "protocol": "TCP"}]}
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            "wardle-service-empty",
            json!({
                "apiVersion": "discovery.k8s.io/v1",
                "kind": "EndpointSlice",
                "metadata": {
                    "name": "wardle-service-empty",
                    "namespace": "default",
                    "labels": {"kubernetes.io/service-name": "wardle-service"}
                },
                "addressType": "IPv4",
                "ports": [{"name": "https", "port": 8443, "protocol": "TCP"}],
                "endpoints": [{"addresses": ["10.42.0.25"], "conditions": {"ready": false}}]
            }),
        )
        .await
        .unwrap();

        let apiservice = json!({
            "apiVersion": "apiregistration.k8s.io/v1",
            "kind": "APIService",
            "metadata": {"name": "v1alpha1.wardle.example.com"},
            "spec": {"service": {"namespace": "default", "name": "wardle-service"}}
        });

        let status = evaluate_apiservice_status(&db, &apiservice, chrono::Utc::now())
            .await
            .unwrap();
        assert_eq!(available_condition_status(&status), Some("False"));
        assert_eq!(
            available_condition_reason(&status),
            Some("MissingEndpoints")
        );
    }

    fn available_condition_status(status: &Value) -> Option<&str> {
        status
            .pointer("/conditions")
            .and_then(Value::as_array)?
            .iter()
            .find(|condition| {
                condition.pointer("/type").and_then(Value::as_str) == Some("Available")
            })
            .and_then(|condition| condition.pointer("/status").and_then(Value::as_str))
    }

    fn available_condition_reason(status: &Value) -> Option<&str> {
        status
            .pointer("/conditions")
            .and_then(Value::as_array)?
            .iter()
            .find(|condition| {
                condition.pointer("/type").and_then(Value::as_str) == Some("Available")
            })
            .and_then(|condition| condition.pointer("/reason").and_then(Value::as_str))
    }
}
