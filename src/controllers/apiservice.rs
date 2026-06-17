//! APIService availability controller.
//!
//! Keeps `.status.conditions[Available]` aligned with the backing Service and
//! Endpoints objects so the apiregistration API behaves like a small
//! kube-aggregator control plane instead of a passive proxy registry.

use crate::datastore::{DatastoreBackend, ResourcePreconditions};
use anyhow::{Context as _, Result};
use serde_json::{Value, json};

const MAX_RETRIES: u32 = 5;

pub async fn reconcile_apiservice(db: &dyn DatastoreBackend, apiservice: &Value) -> Result<()> {
    let name = apiservice
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .context("APIService missing metadata.name")?;

    let mut last_conflict = None;
    for _ in 0..MAX_RETRIES {
        let current = db
            .get_resource("apiregistration.k8s.io/v1", "APIService", None, name)
            .await?
            .context("APIService not found")?;
        let status = evaluate_apiservice_status(db, &current.data).await?;
        if current.data.get("status") == Some(&status) {
            return Ok(());
        }

        match db
            .update_status_only_with_preconditions(
                "apiregistration.k8s.io/v1",
                "APIService",
                None,
                name,
                status,
                ResourcePreconditions::from_resource(&current),
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(err) if crate::datastore::errors::is_conflict_error(&err) => {
                last_conflict = Some(err);
                continue;
            }
            Err(err) => return Err(err),
        }
    }

    match last_conflict {
        Some(err) => Err(err).context("APIService status update conflict retries exhausted"),
        None => Ok(()),
    }
}

async fn evaluate_apiservice_status(
    db: &dyn DatastoreBackend,
    apiservice: &Value,
) -> Result<Value> {
    let Some(service) = apiservice
        .pointer("/spec/service")
        .and_then(|v| v.as_object())
    else {
        return Ok(status_with_available(
            apiservice,
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
            "False",
            "ServiceNotFound",
            "APIService spec.service.name is missing",
        ));
    };

    if db
        .get_resource("v1", "Service", Some(namespace), name)
        .await?
        .is_none()
    {
        return Ok(status_with_available(
            apiservice,
            "False",
            "ServiceNotFound",
            format!("APIService backend Service {namespace}/{name} not found"),
        ));
    }

    let Some(endpoints) = db
        .get_resource("v1", "Endpoints", Some(namespace), name)
        .await?
    else {
        return Ok(status_with_available(
            apiservice,
            "False",
            "EndpointsNotFound",
            format!("APIService backend Endpoints {namespace}/{name} not found"),
        ));
    };

    if !endpoints_have_ready_address(&endpoints.data) {
        return Ok(status_with_available(
            apiservice,
            "False",
            "MissingEndpoints",
            format!("APIService backend Endpoints {namespace}/{name} has no ready addresses"),
        ));
    }

    Ok(status_with_available(
        apiservice,
        "True",
        "Passed",
        "all checks passed",
    ))
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
        .unwrap_or_else(crate::utils::k8s_timestamp);
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
