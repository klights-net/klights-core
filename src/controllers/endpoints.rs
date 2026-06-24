use crate::datastore::{
    DatastoreBackend, Resource, ResourceBatchOperation, ResourceBatchPutMode, ResourcePreconditions,
};
use crate::kubelet::pod_repository::PodReader;
use anyhow::Result;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

use crate::controllers::common::is_pod_ready_value as is_pod_ready;

async fn namespace_is_terminating(db: &dyn DatastoreBackend, namespace: &str) -> Result<bool> {
    let Some(ns) = db.get_namespace(namespace).await? else {
        return Ok(false);
    };
    Ok(ns
        .data
        .pointer("/metadata/deletionTimestamp")
        .and_then(|v| v.as_str())
        .is_some())
}

/// Resolve a service port's targetPort to the numeric container port.
///
/// K8s allows targetPort to be either a number or a named port string.
/// When it's a named string (e.g. "http"), we find the matching container port
/// by name across the provided pods and return its numeric containerPort.
/// Numeric strings (e.g. "8080") are treated as numeric targetPorts.
/// If a named targetPort cannot be resolved, returns None so the caller can
/// skip that port until matching endpoints exist.
fn resolve_target_port(service_port: &Value, pod_data_list: &[&Value]) -> Option<u64> {
    if let Some(target_port) = service_port.get("targetPort") {
        // Internal resilience: accept IntOrString object form in addition to
        // canonical JSON scalar representation.
        let target_port = match target_port.as_object() {
            Some(obj) => {
                let ty = obj.get("type").and_then(|t| t.as_i64());
                if ty == Some(1) {
                    obj.get("strVal")
                        .or_else(|| obj.get("strval"))
                        .or_else(|| obj.get("string"))
                        .unwrap_or(target_port)
                } else {
                    obj.get("intVal")
                        .or_else(|| obj.get("intval"))
                        .or_else(|| obj.get("int"))
                        .or_else(|| obj.get("strVal"))
                        .unwrap_or(target_port)
                }
            }
            None => target_port,
        };

        // Integer targetPort — use directly, but treat 0 as absent.
        // K8s client-go sends targetPort=0 (Go int32 zero value) when not explicitly set;
        // that must fall back to the service's own port number, not be used as a target port.
        if let Some(n) = target_port.as_u64()
            && n > 0
        {
            return Some(n);
        }
        // n == 0: fall through to service port fallback below

        // String targetPort may be a numeric string or a named targetPort.
        if let Some(port_name) = target_port.as_str() {
            if let Ok(parsed) = port_name.parse::<u64>()
                && parsed > 0
            {
                return Some(parsed);
            }
            // "0" falls through to service-port fallback below.

            // Named targetPort — resolve against pod container ports.
            for pod_data in pod_data_list {
                if let Some(containers) = pod_data
                    .pointer("/spec/containers")
                    .and_then(|c| c.as_array())
                {
                    for container in containers {
                        if let Some(ports) = container.get("ports").and_then(|p| p.as_array()) {
                            for port in ports {
                                if port.get("name").and_then(|n| n.as_str()) == Some(port_name)
                                    && let Some(n) =
                                        port.get("containerPort").and_then(|p| p.as_u64())
                                {
                                    return Some(n);
                                }
                            }
                        }
                    }
                }
            }

            // Named targetPort did not resolve for any matched pod.
            return None;
        }
    }

    // No targetPort — fall back to the service port number.
    service_port.get("port").and_then(|p| p.as_u64())
}

fn build_endpoints_port(service_port: &Value, pod_data_list: &[&Value]) -> Option<Value> {
    let target_port = resolve_target_port(service_port, pod_data_list)?;
    let name = service_port
        .get("name")
        .and_then(|n| n.as_str())
        .filter(|s| !s.is_empty());
    let protocol = service_port
        .get("protocol")
        .and_then(|proto| proto.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("TCP");
    let mut endpoint_port = json!({
        "port": target_port,
        "protocol": protocol,
    });
    if let Some(name) = name {
        endpoint_port["name"] = json!(name);
    }
    Some(endpoint_port)
}

fn build_endpointslice_port(service_port: &Value, pod_data_list: &[&Value]) -> Option<Value> {
    let name = service_port
        .get("name")
        .and_then(|n| n.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("");
    let port = resolve_target_port(service_port, pod_data_list)?;
    let protocol = service_port
        .get("protocol")
        .and_then(|proto| proto.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("TCP");

    Some(json!({
        "name": name,
        "port": port,
        "protocol": protocol
    }))
}

fn build_endpoints_ports_for_pod(service_ports: Option<&Value>, pod_data: &Value) -> Vec<Value> {
    let pod_refs = [pod_data];
    service_ports
        .and_then(|p| p.as_array())
        .map(|ports| {
            ports
                .iter()
                .filter_map(|p| build_endpoints_port(p, &pod_refs))
                .collect()
        })
        .unwrap_or_default()
}

fn build_endpointslice_ports_for_pod(
    service_ports: Option<&Value>,
    pod_data: &Value,
) -> Vec<Value> {
    let pod_refs = [pod_data];
    service_ports
        .and_then(|p| p.as_array())
        .map(|ports| {
            ports
                .iter()
                .filter_map(|p| build_endpointslice_port(p, &pod_refs))
                .collect()
        })
        .unwrap_or_default()
}

fn ports_signature(ports: &[Value]) -> String {
    serde_json::to_string(ports).unwrap_or_else(|_| "[]".to_string())
}

fn pod_is_terminating(pod: &Value) -> bool {
    pod.pointer("/metadata/deletionTimestamp")
        .is_some_and(|value| !value.is_null())
}

/// Build a `LabelSelector` for endpoints matching when the Service has a
/// non-empty selector. K8s behaviour: Services without a selector (or with
/// an empty `matchLabels: {}`) do not get controller-managed Endpoints —
/// the user provides them manually for headless / external services.
pub fn endpoints_selector(
    selector: Option<&Value>,
) -> Option<crate::label_selector::LabelSelector> {
    let selector = selector?;
    let has_match_labels = selector
        .pointer("/matchLabels")
        .and_then(|m| m.as_object())
        .is_some_and(|m| !m.is_empty());
    let has_match_expressions = selector
        .pointer("/matchExpressions")
        .and_then(|m| m.as_array())
        .is_some_and(|a| !a.is_empty());
    // Some Service callers pass the labels map directly (legacy shape) —
    // honour it as a flat matchLabels equivalent.
    let flat_labels = selector.as_object().filter(|m| {
        !m.is_empty() && !m.contains_key("matchLabels") && !m.contains_key("matchExpressions")
    });

    if !has_match_labels && !has_match_expressions && flat_labels.is_none() {
        return None;
    }

    let canonical = if let Some(flat) = flat_labels {
        json!({ "matchLabels": flat })
    } else {
        selector.clone()
    };
    crate::label_selector::LabelSelector::from_k8s_selector(&canonical).ok()
}

fn endpoints_desired_state_matches(current: &Value, desired: &Value) -> bool {
    current.get("subsets") == desired.get("subsets")
}

fn endpointslice_desired_state_matches(current: &Value, desired: &Value) -> bool {
    current.get("endpoints") == desired.get("endpoints")
        && current.get("ports") == desired.get("ports")
}

struct EndpointSubsetGroup {
    ports: Vec<Value>,
    addresses: Vec<Value>,
    not_ready_addresses: Vec<Value>,
}

struct EndpointSliceGroup {
    ports: Vec<Value>,
    endpoints: Vec<Value>,
}

pub struct ServiceEndpointBatchReconcileRequest<'a> {
    pub service_name: &'a str,
    pub service_uid: &'a str,
    pub namespace: &'a str,
    pub selector: Option<&'a Value>,
    pub service_ports: Option<&'a Value>,
    pub publish_not_ready: bool,
}

fn build_desired_endpoints(
    service_name: &str,
    namespace: &str,
    service_ports: Option<&Value>,
    publish_not_ready: bool,
    pods: &[Resource],
    selector: &crate::label_selector::LabelSelector,
) -> Value {
    let mut subset_groups: Vec<EndpointSubsetGroup> = Vec::new();
    let mut subset_group_indexes: BTreeMap<String, usize> = BTreeMap::new();

    for pod_resource in pods {
        if pod_is_terminating(&pod_resource.data) {
            continue;
        }
        if !selector.matches_resource(&pod_resource.data) {
            continue;
        }

        if let Some(pod_ip) = pod_resource
            .data
            .get("status")
            .and_then(|s| s.get("podIP"))
            .and_then(|ip| ip.as_str())
            && pod_ip != "0.0.0.0"
            && !pod_ip.is_empty()
            && let Some(pod_name) = pod_resource
                .data
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
        {
            let mut target_ref = json!({
                "kind": "Pod",
                "namespace": namespace,
                "name": pod_name
            });
            if let Some(pod_uid) = pod_resource
                .data
                .get("metadata")
                .and_then(|m| m.get("uid"))
                .and_then(|u| u.as_str())
            {
                target_ref["uid"] = json!(pod_uid);
            }

            let mut addr = json!({
                "ip": pod_ip,
                "targetRef": target_ref
            });

            let pod_hostname = pod_resource
                .data
                .pointer("/spec/hostname")
                .and_then(|v| v.as_str());
            let pod_subdomain = pod_resource
                .data
                .pointer("/spec/subdomain")
                .and_then(|v| v.as_str());
            if let (Some(hostname), Some(subdomain)) = (pod_hostname, pod_subdomain)
                && subdomain == service_name
            {
                addr.as_object_mut()
                    .unwrap()
                    .insert("hostname".to_string(), json!(hostname));
            }

            let ports = build_endpoints_ports_for_pod(service_ports, &pod_resource.data);
            let group_key = ports_signature(&ports);
            let group_idx = if let Some(idx) = subset_group_indexes.get(&group_key) {
                *idx
            } else {
                let idx = subset_groups.len();
                subset_groups.push(EndpointSubsetGroup {
                    ports,
                    addresses: Vec::new(),
                    not_ready_addresses: Vec::new(),
                });
                subset_group_indexes.insert(group_key, idx);
                idx
            };

            if publish_not_ready || is_pod_ready(&pod_resource.data) {
                subset_groups[group_idx].addresses.push(addr);
            } else {
                subset_groups[group_idx].not_ready_addresses.push(addr);
            }
        }
    }

    let mut subsets = Vec::with_capacity(subset_groups.len());
    for group in subset_groups {
        if group.addresses.is_empty() && group.not_ready_addresses.is_empty() {
            continue;
        }
        let mut subset = json!({
            "addresses": group.addresses,
            "ports": group.ports
        });

        if !group.not_ready_addresses.is_empty() {
            subset.as_object_mut().unwrap().insert(
                "notReadyAddresses".to_string(),
                json!(group.not_ready_addresses),
            );
        }

        subsets.push(subset);
    }

    json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {
            "name": service_name,
            "namespace": namespace
        },
        "subsets": subsets
    })
}

fn build_desired_endpointslices(
    service_name: &str,
    service_uid: &str,
    namespace: &str,
    service_ports: Option<&Value>,
    pods: &[Resource],
    selector: &crate::label_selector::LabelSelector,
) -> Vec<(String, Value)> {
    let mut slice_groups: Vec<EndpointSliceGroup> = Vec::new();
    let mut slice_group_indexes: BTreeMap<String, usize> = BTreeMap::new();

    for pod_resource in pods {
        if pod_is_terminating(&pod_resource.data) {
            continue;
        }
        if !selector.matches_resource(&pod_resource.data) {
            continue;
        }

        if let Some(pod_ip) = pod_resource
            .data
            .get("status")
            .and_then(|s| s.get("podIP"))
            .and_then(|ip| ip.as_str())
            && pod_ip != "0.0.0.0"
            && !pod_ip.is_empty()
            && let Some(pod_name) = pod_resource
                .data
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
        {
            let is_ready = is_pod_ready(&pod_resource.data);
            let mut target_ref = json!({
                "kind": "Pod",
                "namespace": namespace,
                "name": pod_name
            });
            if let Some(pod_uid) = pod_resource
                .data
                .get("metadata")
                .and_then(|m| m.get("uid"))
                .and_then(|u| u.as_str())
            {
                target_ref["uid"] = json!(pod_uid);
            }

            let mut ep_entry = json!({
                "addresses": [pod_ip],
                "conditions": {
                    "ready": is_ready,
                    "serving": is_ready,
                    "terminating": false
                },
                "targetRef": target_ref
            });

            let pod_hostname = pod_resource
                .data
                .pointer("/spec/hostname")
                .and_then(|v| v.as_str());
            let pod_subdomain = pod_resource
                .data
                .pointer("/spec/subdomain")
                .and_then(|v| v.as_str());
            if let (Some(hostname), Some(subdomain)) = (pod_hostname, pod_subdomain)
                && subdomain == service_name
            {
                ep_entry["hostname"] = json!(hostname);
            }

            let ports = build_endpointslice_ports_for_pod(service_ports, &pod_resource.data);
            let group_key = ports_signature(&ports);
            let group_idx = if let Some(idx) = slice_group_indexes.get(&group_key) {
                *idx
            } else {
                let idx = slice_groups.len();
                slice_groups.push(EndpointSliceGroup {
                    ports,
                    endpoints: Vec::new(),
                });
                slice_group_indexes.insert(group_key, idx);
                idx
            };

            slice_groups[group_idx].endpoints.push(ep_entry);
        }
    }

    if slice_groups.is_empty() {
        slice_groups.push(EndpointSliceGroup {
            ports: Vec::new(),
            endpoints: Vec::new(),
        });
    }

    slice_groups
        .into_iter()
        .enumerate()
        .map(|(idx, group)| {
            let endpointslice_name = if idx == 0 {
                format!("{}-{}", service_name, "klights")
            } else {
                format!("{}-{}-{}", service_name, "klights", idx)
            };
            let endpointslice = json!({
                "apiVersion": "discovery.k8s.io/v1",
                "kind": "EndpointSlice",
                "metadata": {
                    "name": endpointslice_name.clone(),
                    "namespace": namespace,
                    "labels": {
                        "kubernetes.io/service-name": service_name,
                        "endpointslice.kubernetes.io/managed-by": "endpointslice-controller.k8s.io"
                    },
                    "ownerReferences": [{
                        "apiVersion": "v1",
                        "kind": "Service",
                        "name": service_name,
                        "uid": service_uid,
                        "controller": false,
                        "blockOwnerDeletion": true
                    }]
                },
                "addressType": "IPv4",
                "endpoints": group.endpoints,
                "ports": group.ports
            });
            (endpointslice_name, endpointslice)
        })
        .collect()
}

async fn update_endpoints_with_retry(
    db: &dyn DatastoreBackend,
    namespace: &str,
    service_name: &str,
    endpoints: Value,
    existing_resource: Resource,
) -> Result<()> {
    let mut preconditions = ResourcePreconditions::from_resource(&existing_resource);
    let max_attempts = 4;
    for attempt in 1..=max_attempts {
        match db
            .update_resource_with_preconditions(
                "v1",
                "Endpoints",
                Some(namespace),
                service_name,
                endpoints.clone(),
                preconditions.clone(),
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(e) if crate::datastore::errors::is_conflict_error(&e) && attempt < max_attempts => {
                let refreshed = db
                    .get_resource("v1", "Endpoints", Some(namespace), service_name)
                    .await?;
                let Some(resource) = refreshed else {
                    match db
                        .create_resource(
                            "v1",
                            "Endpoints",
                            Some(namespace),
                            service_name,
                            endpoints.clone(),
                        )
                        .await
                    {
                        Ok(_) => return Ok(()),
                        Err(create_err) if create_err.to_string().contains("exists") => continue,
                        Err(create_err) => return Err(create_err),
                    }
                };
                if endpoints_desired_state_matches(&resource.data, &endpoints) {
                    tracing::debug!(
                        namespace = %namespace,
                        service = %service_name,
                        "Endpoints reconcile conflict already converged"
                    );
                    return Ok(());
                }
                preconditions = ResourcePreconditions::from_resource(&resource);
            }
            Err(e) if crate::datastore::errors::is_conflict_error(&e) => {
                anyhow::bail!(
                    "failed to update Endpoints {}/{} after retries: {}",
                    namespace,
                    service_name,
                    e
                );
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

async fn update_endpointslice_with_retry(
    db: &dyn DatastoreBackend,
    namespace: &str,
    service_name: &str,
    endpointslice_name: &str,
    endpointslice: Value,
    existing_resource: Resource,
) -> Result<()> {
    let mut preconditions = ResourcePreconditions::from_resource(&existing_resource);
    let max_attempts = 4;
    for attempt in 1..=max_attempts {
        match db
            .update_resource_with_preconditions(
                "discovery.k8s.io/v1",
                "EndpointSlice",
                Some(namespace),
                endpointslice_name,
                endpointslice.clone(),
                preconditions.clone(),
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(e) if crate::datastore::errors::is_conflict_error(&e) && attempt < max_attempts => {
                let refreshed = db
                    .get_resource(
                        "discovery.k8s.io/v1",
                        "EndpointSlice",
                        Some(namespace),
                        endpointslice_name,
                    )
                    .await?;
                let Some(resource) = refreshed else {
                    match db
                        .create_resource(
                            "discovery.k8s.io/v1",
                            "EndpointSlice",
                            Some(namespace),
                            endpointslice_name,
                            endpointslice.clone(),
                        )
                        .await
                    {
                        Ok(_) => return Ok(()),
                        Err(create_err) if create_err.to_string().contains("exists") => continue,
                        Err(create_err) => return Err(create_err),
                    }
                };
                if endpointslice_desired_state_matches(&resource.data, &endpointslice) {
                    tracing::debug!(
                        namespace = %namespace,
                        service = %service_name,
                        endpointslice = %endpointslice_name,
                        "EndpointSlice reconcile conflict already converged"
                    );
                    return Ok(());
                }
                preconditions = ResourcePreconditions::from_resource(&resource);
            }
            Err(e) if crate::datastore::errors::is_conflict_error(&e) => {
                anyhow::bail!(
                    "failed to update EndpointSlice {}/{} for Service {} after retries: {}",
                    namespace,
                    endpointslice_name,
                    service_name,
                    e
                );
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

async fn create_or_update_endpointslice(
    db: &dyn DatastoreBackend,
    namespace: &str,
    service_name: &str,
    endpointslice_name: &str,
    endpointslice: Value,
) -> Result<()> {
    match db
        .create_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some(namespace),
            endpointslice_name,
            endpointslice.clone(),
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(create_err) if crate::datastore::errors::is_conflict_error(&create_err) => {
            let refreshed = db
                .get_resource(
                    "discovery.k8s.io/v1",
                    "EndpointSlice",
                    Some(namespace),
                    endpointslice_name,
                )
                .await?;
            let Some(refreshed_resource) = refreshed else {
                return Err(create_err);
            };

            if endpointslice_desired_state_matches(&refreshed_resource.data, &endpointslice) {
                tracing::debug!(
                    namespace = %namespace,
                    service = %service_name,
                    endpointslice = %endpointslice_name,
                    "EndpointSlice reconcile create conflict already converged"
                );
                return Ok(());
            }

            update_endpointslice_with_retry(
                db,
                namespace,
                service_name,
                endpointslice_name,
                endpointslice,
                refreshed_resource,
            )
            .await
        }
        Err(create_err) => Err(create_err),
    }
}

pub async fn reconcile_endpoints(
    db: &dyn DatastoreBackend,
    pod_reader: &dyn PodReader,
    service_name: &str,
    namespace: &str,
    selector: Option<&Value>,
    service_ports: Option<&Value>,
    publish_not_ready: bool,
) -> Result<()> {
    let Some(parsed_selector) = endpoints_selector(selector) else {
        return Ok(());
    };

    if namespace_is_terminating(db, namespace).await? {
        tracing::debug!(
            namespace = %namespace,
            service = %service_name,
            "skipping Endpoints reconcile in terminating namespace"
        );
        return Ok(());
    }

    let existing = db
        .get_resource("v1", "Endpoints", Some(namespace), service_name)
        .await?;
    let pod_list = pod_reader
        .list_pods(Some(namespace), None, None, None, None)
        .await?;
    let endpoints = build_desired_endpoints(
        service_name,
        namespace,
        service_ports,
        publish_not_ready,
        &pod_list.items,
        &parsed_selector,
    );

    if let Some(existing_resource) = existing {
        if endpoints_desired_state_matches(&existing_resource.data, &endpoints) {
            tracing::debug!(
                "Skipping endpoint reconciliation for {}/{} - no changes",
                namespace,
                service_name
            );
            return Ok(());
        }

        update_endpoints_with_retry(db, namespace, service_name, endpoints, existing_resource)
            .await?;
    } else {
        match db
            .create_resource(
                "v1",
                "Endpoints",
                Some(namespace),
                service_name,
                endpoints.clone(),
            )
            .await
        {
            Ok(_) => {}
            Err(e) if e.to_string().contains("exists") => {
                if let Some(existing) = db
                    .get_resource("v1", "Endpoints", Some(namespace), service_name)
                    .await?
                {
                    update_endpoints_with_retry(db, namespace, service_name, endpoints, existing)
                        .await?;
                }
            }
            Err(e) => return Err(e),
        }
    }

    Ok(())
}

/// Generate EndpointSlice resource for a Service.
/// EndpointSlice is the newer alternative to Endpoints (discovery.k8s.io/v1).
/// Creates one EndpointSlice per service with `<service-name>-<hash>` naming.
pub async fn reconcile_endpointslice(
    db: &dyn DatastoreBackend,
    pod_reader: &dyn PodReader,
    service_name: &str,
    service_uid: &str,
    namespace: &str,
    selector: Option<&Value>,
    service_ports: Option<&Value>,
) -> Result<()> {
    let Some(parsed_selector) = endpoints_selector(selector) else {
        return Ok(());
    };

    if namespace_is_terminating(db, namespace).await? {
        tracing::debug!(
            namespace = %namespace,
            service = %service_name,
            "skipping EndpointSlice reconcile in terminating namespace"
        );
        return Ok(());
    }

    let pod_list = pod_reader
        .list_pods(Some(namespace), None, None, None, None)
        .await?;

    let existing_slices = db
        .list_resources(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some(namespace),
            crate::datastore::ResourceListQuery::new(
                Some(&format!("kubernetes.io/service-name={service_name}")),
                None,
                None,
                None,
            ),
        )
        .await?;

    let mut desired_names = BTreeSet::new();
    for (endpointslice_name, endpointslice) in build_desired_endpointslices(
        service_name,
        service_uid,
        namespace,
        service_ports,
        &pod_list.items,
        &parsed_selector,
    ) {
        desired_names.insert(endpointslice_name.clone());

        let existing = existing_slices
            .items
            .iter()
            .find(|resource| resource.name == endpointslice_name);

        if let Some(existing_resource) = existing {
            if endpointslice_desired_state_matches(&existing_resource.data, &endpointslice) {
                tracing::debug!(
                    "Skipping endpointslice reconciliation for {}/{} - no changes",
                    namespace,
                    service_name
                );
                continue;
            }

            update_endpointslice_with_retry(
                db,
                namespace,
                service_name,
                &endpointslice_name,
                endpointslice,
                existing_resource.clone(),
            )
            .await?;
        } else {
            create_or_update_endpointslice(
                db,
                namespace,
                service_name,
                &endpointslice_name,
                endpointslice,
            )
            .await?;
        }
    }

    for existing in existing_slices.items {
        if !desired_names.contains(&existing.name) {
            db.delete_resource_with_preconditions(
                "discovery.k8s.io/v1",
                "EndpointSlice",
                Some(namespace),
                &existing.name,
                ResourcePreconditions::uid(existing.uid),
            )
            .await?;
        }
    }

    Ok(())
}

pub async fn reconcile_service_endpoints_batch(
    db: &dyn DatastoreBackend,
    pod_reader: &dyn PodReader,
    request: ServiceEndpointBatchReconcileRequest<'_>,
) -> Result<()> {
    let ServiceEndpointBatchReconcileRequest {
        service_name,
        service_uid,
        namespace,
        selector,
        service_ports,
        publish_not_ready,
    } = request;
    let Some(parsed_selector) = endpoints_selector(selector) else {
        return Ok(());
    };

    if namespace_is_terminating(db, namespace).await? {
        tracing::debug!(
            namespace = %namespace,
            service = %service_name,
            "skipping batched endpoint reconcile in terminating namespace"
        );
        return Ok(());
    }

    let pod_list = pod_reader
        .list_pods(Some(namespace), None, None, None, None)
        .await?;
    let desired_endpoints = build_desired_endpoints(
        service_name,
        namespace,
        service_ports,
        publish_not_ready,
        &pod_list.items,
        &parsed_selector,
    );
    let desired_slices = build_desired_endpointslices(
        service_name,
        service_uid,
        namespace,
        service_ports,
        &pod_list.items,
        &parsed_selector,
    );

    let existing_endpoints = db
        .get_resource("v1", "Endpoints", Some(namespace), service_name)
        .await?;
    let existing_slices = db
        .list_resources(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some(namespace),
            crate::datastore::ResourceListQuery::new(
                Some(&format!("kubernetes.io/service-name={service_name}")),
                None,
                None,
                None,
            ),
        )
        .await?;

    let mut operations = Vec::new();
    match existing_endpoints {
        Some(existing) if endpoints_desired_state_matches(&existing.data, &desired_endpoints) => {}
        Some(existing) => operations.push(ResourceBatchOperation::Put {
            api_version: "v1".to_string(),
            kind: "Endpoints".to_string(),
            namespace: Some(namespace.to_string()),
            name: service_name.to_string(),
            data: desired_endpoints,
            mode: ResourceBatchPutMode::Update,
            preconditions: ResourcePreconditions::from_resource(&existing),
        }),
        None => operations.push(ResourceBatchOperation::Put {
            api_version: "v1".to_string(),
            kind: "Endpoints".to_string(),
            namespace: Some(namespace.to_string()),
            name: service_name.to_string(),
            data: desired_endpoints,
            mode: ResourceBatchPutMode::Create,
            preconditions: ResourcePreconditions::default(),
        }),
    }

    let mut desired_names = BTreeSet::new();
    for (slice_name, desired_slice) in desired_slices {
        desired_names.insert(slice_name.clone());
        match existing_slices
            .items
            .iter()
            .find(|resource| resource.name == slice_name)
        {
            Some(existing)
                if endpointslice_desired_state_matches(&existing.data, &desired_slice) => {}
            Some(existing) => operations.push(ResourceBatchOperation::Put {
                api_version: "discovery.k8s.io/v1".to_string(),
                kind: "EndpointSlice".to_string(),
                namespace: Some(namespace.to_string()),
                name: slice_name,
                data: desired_slice,
                mode: ResourceBatchPutMode::Update,
                preconditions: ResourcePreconditions::from_resource(existing),
            }),
            None => operations.push(ResourceBatchOperation::Put {
                api_version: "discovery.k8s.io/v1".to_string(),
                kind: "EndpointSlice".to_string(),
                namespace: Some(namespace.to_string()),
                name: slice_name,
                data: desired_slice,
                mode: ResourceBatchPutMode::Create,
                preconditions: ResourcePreconditions::default(),
            }),
        }
    }

    for existing in existing_slices.items {
        if !desired_names.contains(&existing.name) {
            operations.push(ResourceBatchOperation::Delete {
                api_version: "discovery.k8s.io/v1".to_string(),
                kind: "EndpointSlice".to_string(),
                namespace: Some(namespace.to_string()),
                name: existing.name,
                preconditions: ResourcePreconditions::uid(existing.uid),
            });
        }
    }

    if operations.is_empty() {
        return Ok(());
    }
    db.apply_resource_batch(operations).await
}

/// Mirror manually-created Endpoints to EndpointSlice.
/// K8s has an endpointslice-mirroring-controller that watches for Endpoints
/// (not created by a Service) and creates matching EndpointSlices.
/// This enables EndpointSlice consumers to work with manually-created Endpoints.
pub async fn mirror_endpoints_to_endpointslice(
    db: &dyn DatastoreBackend,
    endpoints: &Value,
) -> Result<()> {
    let input_metadata = endpoints
        .get("metadata")
        .ok_or_else(|| anyhow::anyhow!("Missing metadata"))?;
    let name = input_metadata
        .get("name")
        .and_then(|n| n.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing name"))?;
    let namespace = input_metadata
        .get("namespace")
        .and_then(|n| n.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing namespace"))?;

    if namespace_is_terminating(db, namespace).await? {
        tracing::debug!(
            namespace = %namespace,
            "skipping EndpointSlice mirror in terminating namespace"
        );
        return Ok(());
    }

    let Some(live_endpoints) = db
        .get_resource("v1", "Endpoints", Some(namespace), name)
        .await?
    else {
        return Ok(());
    };
    let endpoints =
        crate::api::inject_resource_version(live_endpoints.data, live_endpoints.resource_version);
    let metadata = endpoints
        .get("metadata")
        .ok_or_else(|| anyhow::anyhow!("Missing metadata"))?;
    if metadata.get("deletionTimestamp").is_some() {
        return Ok(());
    }

    // Skip if Endpoints has endpointslice.kubernetes.io/skip-mirror: "true" label
    if let Some(skip_mirror) = endpoints
        .pointer("/metadata/labels/endpointslice.kubernetes.io~1skip-mirror")
        .and_then(|v| v.as_str())
        && skip_mirror == "true"
    {
        return Ok(());
    }

    // Skip if this Endpoints is managed by a Service controller
    // (Service controller already creates EndpointSlices via reconcile_endpointslice)
    if let Some(managed_by) = metadata
        .pointer("/labels/endpointslice.kubernetes.io~1managed-by")
        .and_then(|v| v.as_str())
        && managed_by == "endpointslice-controller.k8s.io"
    {
        return Ok(());
    }

    // Generate EndpointSlice name with hash suffix (mirror Endpoints name exactly)
    let endpointslice_name = format!("{}-{}", name, "mirror");

    // Convert Endpoints subsets to EndpointSlice endpoints format
    let subsets = endpoints.get("subsets").and_then(|s| s.as_array());
    let mut ep_endpoints = vec![];
    let mut ep_ports = vec![];

    if let Some(subsets_array) = subsets {
        for subset in subsets_array {
            // Process addresses
            let addresses = subset.get("addresses").and_then(|a| a.as_array());
            let not_ready_addresses = subset.get("notReadyAddresses").and_then(|a| a.as_array());

            if let Some(addrs) = addresses {
                for addr in addrs {
                    if let Some(ip) = addr.get("ip").and_then(|ip| ip.as_str()) {
                        let mut ep_entry = json!({
                            "addresses": [ip],
                            "conditions": {
                                "ready": true,
                                "serving": true,
                                "terminating": false
                            }
                        });

                        // Copy targetRef if present
                        if let Some(target_ref) = addr.get("targetRef") {
                            ep_entry["targetRef"] = target_ref.clone();
                        }

                        // Copy hostname if present (for StatefulSet pod DNS)
                        if let Some(hostname) = addr.get("hostname") {
                            ep_entry["hostname"] = hostname.clone();
                        }

                        ep_endpoints.push(ep_entry);
                    }
                }
            }

            if let Some(addrs) = not_ready_addresses {
                for addr in addrs {
                    if let Some(ip) = addr.get("ip").and_then(|ip| ip.as_str()) {
                        let mut ep_entry = json!({
                            "addresses": [ip],
                            "conditions": {
                                "ready": false,
                                "serving": false,
                                "terminating": false
                            }
                        });

                        if let Some(target_ref) = addr.get("targetRef") {
                            ep_entry["targetRef"] = target_ref.clone();
                        }

                        if let Some(hostname) = addr.get("hostname") {
                            ep_entry["hostname"] = hostname.clone();
                        }

                        ep_endpoints.push(ep_entry);
                    }
                }
            }

            // Process ports (Endpoints format: {port: int, protocol: str})
            if let Some(ports) = subset.get("ports").and_then(|p| p.as_array()) {
                for port in ports {
                    if let Some(port_num) = port.get("port").and_then(|p| p.as_u64()) {
                        let name = port
                            .get("name")
                            .and_then(|n| n.as_str())
                            .filter(|s| !s.is_empty())
                            .unwrap_or("");
                        let protocol = port
                            .get("protocol")
                            .and_then(|p| p.as_str())
                            .filter(|s| !s.is_empty())
                            .unwrap_or("TCP");

                        let port_obj = json!({
                            "name": name,
                            "port": port_num,
                            "protocol": protocol
                        });

                        ep_ports.push(port_obj);
                    }
                }
            }
        }
    }

    // Build ownerReference pointing back to the Endpoints object so GC
    // automatically deletes this mirror EndpointSlice when the Endpoints is
    // deleted (P0-E2E-20260423-09 delete lifecycle requirement).
    let ep_uid = metadata.get("uid").and_then(|u| u.as_str()).unwrap_or("");
    let ep_rv = metadata
        .get("resourceVersion")
        .and_then(|r| r.as_str())
        .unwrap_or("0");
    let owner_refs = if !ep_uid.is_empty() {
        serde_json::json!([{
            "apiVersion": "v1",
            "kind": "Endpoints",
            "name": name,
            "uid": ep_uid,
            "resourceVersion": ep_rv,
            "blockOwnerDeletion": true,
            "controller": false
        }])
    } else {
        serde_json::json!([])
    };

    // Create EndpointSlice
    let endpointslice = json!({
        "apiVersion": "discovery.k8s.io/v1",
        "kind": "EndpointSlice",
        "metadata": {
            "name": endpointslice_name.clone(),
            "namespace": namespace,
            "labels": {
                "kubernetes.io/service-name": name,
                "endpointslice.kubernetes.io/managed-by": "endpointslicemirroring-controller.k8s.io"
            },
            "ownerReferences": owner_refs
        },
        "addressType": "IPv4",
        "endpoints": ep_endpoints,
        "ports": ep_ports
    });

    // Check if EndpointSlice already exists
    let existing = db
        .get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some(namespace),
            &endpointslice_name,
        )
        .await?;

    if let Some(existing_resource) = existing {
        // Update existing EndpointSlice. Retry conflicts because EndpointSlice
        // metadata can change concurrently (resourceVersion churn) between
        // get_resource and update_resource.
        let mut preconditions = ResourcePreconditions::from_resource(&existing_resource);
        let mut last_conflict = None;
        let max_attempts = 4;
        for attempt in 1..=max_attempts {
            match db
                .update_resource_with_preconditions(
                    "discovery.k8s.io/v1",
                    "EndpointSlice",
                    Some(namespace),
                    &endpointslice_name,
                    endpointslice.clone(),
                    preconditions.clone(),
                )
                .await
            {
                Ok(_) => {
                    last_conflict = None;
                    break;
                }
                Err(e)
                    if crate::datastore::errors::is_conflict_error(&e)
                        && attempt < max_attempts =>
                {
                    last_conflict = Some(e.to_string());
                    let refreshed = db
                        .get_resource(
                            "discovery.k8s.io/v1",
                            "EndpointSlice",
                            Some(namespace),
                            &endpointslice_name,
                        )
                        .await?;
                    if let Some(resource) = refreshed {
                        preconditions = ResourcePreconditions::from_resource(&resource);
                    } else {
                        break;
                    }
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        if let Some(conflict) = last_conflict {
            return Err(anyhow::anyhow!(
                "failed to update mirrored EndpointSlice {} after retries: {}",
                endpointslice_name,
                conflict
            ));
        }
    } else {
        // Create new EndpointSlice — handle concurrent create race.
        match db
            .create_resource(
                "discovery.k8s.io/v1",
                "EndpointSlice",
                Some(namespace),
                &endpointslice_name,
                endpointslice.clone(),
            )
            .await
        {
            Ok(_) => {}
            Err(e) if e.to_string().contains("exists") => {
                // Concurrent create won — update instead.
                if let Some(existing) = db
                    .get_resource(
                        "discovery.k8s.io/v1",
                        "EndpointSlice",
                        Some(namespace),
                        &endpointslice_name,
                    )
                    .await?
                {
                    let _ = db
                        .update_resource_with_preconditions(
                            "discovery.k8s.io/v1",
                            "EndpointSlice",
                            Some(namespace),
                            &endpointslice_name,
                            endpointslice,
                            ResourcePreconditions::from_resource(&existing),
                        )
                        .await;
                }
            }
            Err(e) => return Err(e),
        }
    }

    Ok(())
}

pub async fn delete_mirrored_endpointslice_for_endpoints(
    db: &dyn DatastoreBackend,
    endpoints: &Value,
) -> Result<()> {
    let metadata = endpoints
        .get("metadata")
        .ok_or_else(|| anyhow::anyhow!("Missing metadata"))?;
    let name = metadata
        .get("name")
        .and_then(|n| n.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing name"))?;
    let namespace = metadata
        .get("namespace")
        .and_then(|n| n.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing namespace"))?;
    let endpointslice_name = format!("{name}-mirror");

    let Some(existing) = db
        .get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some(namespace),
            &endpointslice_name,
        )
        .await?
    else {
        return Ok(());
    };

    let _ = db
        .delete_resource_with_preconditions(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some(namespace),
            &endpointslice_name,
            ResourcePreconditions::uid(existing.uid),
        )
        .await;
    Ok(())
}

#[cfg(test)]
mod tests;
