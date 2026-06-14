//! Service-namespace helpers for reconciling Services after endpoint-affecting
//! Pod mutations.

use super::ControllerDispatcherSlot;
use crate::controllers::workqueue::ReconcileKey;
use crate::datastore::DatastoreBackend;
use anyhow::Result;
use serde_json::Value;
use std::collections::HashSet;

pub async fn service_reconcile_keys_for_pod(
    pod: &Value,
    db: &dyn DatastoreBackend,
    namespace: &str,
) -> Result<Vec<ReconcileKey>> {
    let services = db
        .list_resources(
            "v1",
            "Service",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;
    let mut keys = Vec::new();
    let mut seen = HashSet::new();

    for service in services.items {
        if service.data.pointer("/spec/type").and_then(|v| v.as_str()) == Some("ExternalName") {
            continue;
        }
        let Some(selector) = crate::controllers::endpoints::endpoints_selector(
            service.data.pointer("/spec/selector"),
        ) else {
            continue;
        };
        let selected = selector.matches_resource(pod);
        if !selected && !endpoints_reference_pod(db, namespace, &service.name, pod).await? {
            continue;
        }
        let key = ReconcileKey::namespaced("v1", "Service", namespace, &service.name);
        if seen.insert(key.clone()) {
            keys.push(key);
        }
    }

    Ok(keys)
}

pub async fn enqueue_services_after_pod_create(
    pod: &Value,
    db: &dyn DatastoreBackend,
    controller_dispatcher: &ControllerDispatcherSlot,
) -> Result<()> {
    if !pod_endpoint_relevant(pod) {
        return Ok(());
    }
    let Some(dispatcher) = controller_dispatcher.get() else {
        return Ok(());
    };
    let namespace = pod
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .unwrap_or("default");
    for key in service_reconcile_keys_for_pod(pod, db, namespace).await? {
        dispatcher.enqueue_reconcile_key(key).await;
    }
    Ok(())
}

pub async fn enqueue_services_after_pod_update(
    previous: &Value,
    updated: &Value,
    db: &dyn DatastoreBackend,
    controller_dispatcher: &ControllerDispatcherSlot,
) -> Result<()> {
    if !pod_endpoint_state_changed(previous, updated) {
        return Ok(());
    }
    let Some(dispatcher) = controller_dispatcher.get() else {
        return Ok(());
    };
    let namespace = updated
        .pointer("/metadata/namespace")
        .or_else(|| previous.pointer("/metadata/namespace"))
        .and_then(|v| v.as_str())
        .unwrap_or("default");
    let mut seen = HashSet::new();
    for pod in [previous, updated] {
        for key in service_reconcile_keys_for_pod(pod, db, namespace).await? {
            if seen.insert(key.clone()) {
                dispatcher.enqueue_reconcile_key(key).await;
            }
        }
    }
    Ok(())
}

pub async fn enqueue_services_after_pod_delete(
    deleted: &Value,
    db: &dyn DatastoreBackend,
    controller_dispatcher: &ControllerDispatcherSlot,
) -> Result<()> {
    if !pod_endpoint_relevant(deleted) {
        return Ok(());
    }
    let Some(dispatcher) = controller_dispatcher.get() else {
        return Ok(());
    };
    let namespace = deleted
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .unwrap_or("default");
    for key in service_reconcile_keys_for_pod(deleted, db, namespace).await? {
        dispatcher.enqueue_reconcile_key(key).await;
    }
    Ok(())
}

pub fn pod_endpoint_state_changed(previous: &Value, updated: &Value) -> bool {
    pod_endpoint_relevant_status(previous) != pod_endpoint_relevant_status(updated)
        || previous.pointer("/status/podIP") != updated.pointer("/status/podIP")
        || previous.pointer("/status/podIPs") != updated.pointer("/status/podIPs")
        || previous.pointer("/metadata/labels") != updated.pointer("/metadata/labels")
        || previous.pointer("/metadata/deletionTimestamp")
            != updated.pointer("/metadata/deletionTimestamp")
        || pod_terminal_phase(previous) != pod_terminal_phase(updated)
}

fn pod_endpoint_relevant(pod: &Value) -> bool {
    pod_terminal_phase(pod).is_some()
        || has_pod_ip(pod)
        || pod
            .pointer("/metadata/deletionTimestamp")
            .is_some_and(|ts| !ts.is_null())
}

fn has_pod_ip(pod: &Value) -> bool {
    let pod_ip = pod
        .pointer("/status/podIP")
        .and_then(|v| v.as_str())
        .filter(|ip| !ip.is_empty() && *ip != "0.0.0.0")
        .is_some();
    pod_ip
        || pod
            .pointer("/status/podIPs")
            .and_then(|v| v.as_array())
            .is_some_and(|ips| {
                ips.iter().any(|ip| {
                    ip.get("ip")
                        .and_then(|value| value.as_str())
                        .filter(|value| !value.is_empty() && *value != "0.0.0.0")
                        .is_some()
                })
            })
}

fn pod_endpoint_relevant_status(pod: &Value) -> bool {
    crate::controllers::common::is_pod_ready_value(pod)
}

fn pod_terminal_phase(pod: &Value) -> Option<&str> {
    match pod.pointer("/status/phase").and_then(|v| v.as_str()) {
        Some("Failed") => Some("Failed"),
        Some("Succeeded") => Some("Succeeded"),
        _ => None,
    }
}

async fn endpoints_reference_pod(
    db: &dyn DatastoreBackend,
    namespace: &str,
    service_name: &str,
    pod: &Value,
) -> Result<bool> {
    let Some(endpoints) = db
        .get_resource("v1", "Endpoints", Some(namespace), service_name)
        .await?
    else {
        return Ok(false);
    };
    let pod_name = pod
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let pod_uid = pod.pointer("/metadata/uid").and_then(|v| v.as_str());
    Ok(endpoint_addresses_reference_pod(
        endpoints.data.pointer("/subsets"),
        namespace,
        pod_name,
        pod_uid,
    ))
}

fn endpoint_addresses_reference_pod(
    subsets: Option<&Value>,
    namespace: &str,
    pod_name: &str,
    pod_uid: Option<&str>,
) -> bool {
    let Some(subsets) = subsets.and_then(|v| v.as_array()) else {
        return false;
    };
    subsets.iter().any(|subset| {
        ["addresses", "notReadyAddresses"].iter().any(|field| {
            subset
                .get(*field)
                .and_then(|v| v.as_array())
                .is_some_and(|addresses| {
                    addresses.iter().any(|address| {
                        let Some(target_ref) = address.get("targetRef") else {
                            return false;
                        };
                        if target_ref.get("kind").and_then(|v| v.as_str()) != Some("Pod") {
                            return false;
                        }
                        let target_namespace = target_ref
                            .get("namespace")
                            .and_then(|v| v.as_str())
                            .unwrap_or(namespace);
                        if target_namespace != namespace {
                            return false;
                        }
                        let target_name = target_ref.get("name").and_then(|v| v.as_str());
                        let target_uid = target_ref.get("uid").and_then(|v| v.as_str());
                        target_name == Some(pod_name)
                            || (pod_uid.is_some() && target_uid == pod_uid)
                    })
                })
        })
    })
}
