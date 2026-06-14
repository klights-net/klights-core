use crate::datastore::{DatastoreBackend, ResourcePreconditions};
use anyhow::Result;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::Mutex;

/// Readiness state for the NodePort allocator.
/// Used to ensure the allocator is bootstrapped before allowing allocations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllocatorState {
    /// Allocator has not been rebuilt from existing services yet.
    NotReady,
    /// Allocator has been rebuilt and is ready to allocate.
    Ready,
    /// Rebuild failed - allocator should not be used.
    Failed,
}

pub struct NodePortAllocator {
    allocated: Mutex<HashSet<u32>>,
    state: Mutex<AllocatorState>,
}

impl NodePortAllocator {
    pub fn new() -> Self {
        Self {
            allocated: Mutex::new(HashSet::new()),
            state: Mutex::new(AllocatorState::NotReady),
        }
    }

    /// Mark a port as already in use.
    #[cfg(test)]
    pub fn mark_used(&self, port: u32) {
        self.allocated.lock().unwrap().insert(port);
    }

    pub fn release(&self, port: u32) {
        self.allocated.lock().unwrap().remove(&port);
    }

    /// Returns true if the allocator is ready for allocations.
    pub fn is_ready(&self) -> bool {
        *self.state.lock().unwrap() == AllocatorState::Ready
    }

    /// Mark the allocator as ready after bootstrap rebuild.
    pub fn set_ready(&self) {
        *self.state.lock().unwrap() = AllocatorState::Ready;
    }

    /// Mark the allocator as failed.
    pub fn set_failed(&self) {
        *self.state.lock().unwrap() = AllocatorState::Failed;
    }

    /// Get the current allocator state.
    pub fn state(&self) -> AllocatorState {
        *self.state.lock().unwrap()
    }

    /// Allocate the next free port in the NodePort range 30000-32767.
    /// Returns an error if the allocator is not ready or the range is
    /// exhausted.
    pub fn allocate(&self) -> Result<u32, &'static str> {
        let state = self.state.lock().unwrap();
        if *state != AllocatorState::Ready {
            return Err("NodePort allocator is not ready");
        }
        drop(state);

        let mut allocated = self.allocated.lock().unwrap();
        let mut candidate = 30000u32;
        while candidate <= 32767 && allocated.contains(&candidate) {
            candidate += 1;
        }
        if candidate > 32767 {
            return Err("NodePort range 30000-32767 exhausted");
        }
        allocated.insert(candidate);
        Ok(candidate)
    }

    fn replace_allocated(&self, ports: HashSet<u32>) {
        *self.allocated.lock().unwrap() = ports;
    }
}

impl Default for NodePortAllocator {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ServiceIpam {
    // Store allocated IPs as u32 in network byte order
    allocated: Mutex<HashSet<u32>>,
    allocation_lock: tokio::sync::Mutex<()>,
    // Starting IP for allocation (network_addr + 2)
    start_ip: u32,
    // Last allocatable IP (broadcast - 1), or 0 when CIDR parsing failed
    end_ip: u32,
}

impl ServiceIpam {
    pub fn new(service_cidr: &str) -> Self {
        // Start at network_addr + 2 (skip .0 and .1; .1 is reserved for the
        // kubernetes service ClusterIP). Falls back to 0 on an unparseable
        // CIDR to preserve the previous tolerant behavior — config validation
        // happens at startup in `KlightsConfig::from_env`.
        let (start_ip, end_ip) = crate::networking::ClusterCidr::parse(service_cidr)
            .map(|c| {
                let net = c.network();
                let broadcast = net | !c.mask();
                // skip .0, .1; skip broadcast address
                (net + 2, broadcast.saturating_sub(1))
            })
            .unwrap_or((0, 0));
        Self {
            allocated: Mutex::new(HashSet::new()),
            allocation_lock: tokio::sync::Mutex::new(()),
            start_ip,
            end_ip,
        }
    }

    pub async fn allocation_guard(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.allocation_lock.lock().await
    }

    /// Allocate the next free ClusterIP in the service CIDR.
    /// Returns an error string when the range is exhausted.
    pub fn allocate(&self) -> Result<String, &'static str> {
        let mut allocated = self.allocated.lock().unwrap();

        let mut candidate = self.start_ip;
        while candidate <= self.end_ip && allocated.contains(&candidate) {
            candidate += 1;
        }
        if candidate > self.end_ip {
            return Err("Service ClusterIP range exhausted");
        }

        allocated.insert(candidate);
        Ok(crate::utils::ip_u32_to_string(candidate))
    }

    pub fn release(&self, ip: &str) {
        // Parse IP string to u32
        if let Some(ip_num) = parse_ip_to_u32(ip) {
            let mut allocated = self.allocated.lock().unwrap();
            allocated.remove(&ip_num);
        }
        // If parsing fails or IP wasn't allocated, this is a no-op
    }

    fn replace_allocated(&self, ips: HashSet<u32>) {
        *self.allocated.lock().unwrap() = ips;
    }
}

fn service_allocated_cluster_ips(service: &Value, allocated: &mut HashSet<u32>) {
    let Some(spec) = service.get("spec") else {
        return;
    };

    if let Some(cluster_ip) = spec.get("clusterIP").and_then(|ip| ip.as_str())
        && !cluster_ip.is_empty()
        && cluster_ip != "None"
        && let Some(ip_num) = parse_ip_to_u32(cluster_ip)
    {
        allocated.insert(ip_num);
    }

    if let Some(cluster_ips) = spec.get("clusterIPs").and_then(|ips| ips.as_array()) {
        for ip in cluster_ips {
            let Some(cluster_ip) = ip.as_str() else {
                continue;
            };
            if cluster_ip.is_empty() || cluster_ip == "None" {
                continue;
            }
            if let Some(ip_num) = parse_ip_to_u32(cluster_ip) {
                allocated.insert(ip_num);
            }
        }
    }
}

pub async fn rebuild_service_ipam_from_services(
    db: &dyn DatastoreBackend,
    ipam: &ServiceIpam,
) -> Result<()> {
    let svc_list = db
        .list_resources(
            "v1",
            "Service",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;
    let mut allocated = HashSet::new();
    for svc in &svc_list.items {
        service_allocated_cluster_ips(&svc.data, &mut allocated);
    }
    ipam.replace_allocated(allocated);
    Ok(())
}

async fn allocate_service_cluster_ip(
    db: &dyn DatastoreBackend,
    service_ipam: &ServiceIpam,
) -> Result<String> {
    match service_ipam.allocate() {
        Ok(ip) => Ok(ip),
        Err(err) if err.contains("exhausted") => {
            rebuild_service_ipam_from_services(db, service_ipam).await?;
            service_ipam
                .allocate()
                .map_err(|e| anyhow::anyhow!("ClusterIP allocation failed: {}", e))
        }
        Err(err) => Err(anyhow::anyhow!("ClusterIP allocation failed: {}", err)),
    }
}

fn release_pending_service_allocations(
    service_ipam: &ServiceIpam,
    nodeport_alloc: &NodePortAllocator,
    cluster_ip: Option<&str>,
    node_ports: &[u32],
) {
    if let Some(cluster_ip) = cluster_ip {
        service_ipam.release(cluster_ip);
    }
    for node_port in node_ports {
        nodeport_alloc.release(*node_port);
    }
}

pub fn release_service_allocations_from_resource(
    service_ipam: &ServiceIpam,
    nodeport_alloc: &NodePortAllocator,
    service: &Value,
) {
    let mut cluster_ips = HashSet::new();
    service_allocated_cluster_ips(service, &mut cluster_ips);
    for cluster_ip in cluster_ips {
        service_ipam.release(&crate::utils::ip_u32_to_string(cluster_ip));
    }

    let Some(ports) = service
        .pointer("/spec/ports")
        .and_then(|ports| ports.as_array())
    else {
        return;
    };
    for port in ports {
        if let Some(node_port) = port
            .get("nodePort")
            .and_then(|node_port| node_port.as_u64())
            && (30000..=32767).contains(&node_port)
        {
            nodeport_alloc.release(node_port as u32);
        }
    }
}

/// Rebuild the NodePort allocator from existing services in the datastore.
///
/// This function scans all existing services in the database, extracts their
/// allocated NodePorts, and marks them as used in the allocator. After a
/// successful rebuild, the allocator is marked as ready.
///
/// Used during:
/// - Bootstrap initialization
/// - Leader promotion (when this instance becomes the primary)
pub async fn rebuild_nodeport_allocator_from_services(
    db: &dyn DatastoreBackend,
    alloc: &std::sync::Arc<NodePortAllocator>,
) -> Result<()> {
    // Scan all services in all namespaces to mark already-allocated NodePorts
    let mut allocated = HashSet::new();
    if let Ok(svc_list) = db
        .list_resources(
            "v1",
            "Service",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await
    {
        for svc in &svc_list.items {
            if let Some(ports) = svc.data.pointer("/spec/ports").and_then(|p| p.as_array()) {
                for port in ports {
                    if let Some(np) = port.get("nodePort").and_then(|n| n.as_u64())
                        && (30000..=32767).contains(&np)
                    {
                        allocated.insert(np as u32);
                    }
                }
            }
        }
    }
    alloc.replace_allocated(allocated);
    // Mark allocator as ready after successful rebuild
    alloc.set_ready();
    Ok(())
}

fn parse_ip_to_u32(ip: &str) -> Option<u32> {
    let parts: Vec<u32> = ip.split('.').filter_map(|s| s.parse().ok()).collect();
    if parts.len() == 4 {
        Some((parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3])
    } else {
        None
    }
}

/// Clear clusterIP and clusterIPs for ExternalName services.
///
/// Normalize spec.type: default to "ClusterIP" if missing or empty.
/// K8s spec: if a service has no type, it defaults to ClusterIP.
/// This handles ExternalName→ClusterIP patch where type field may be absent/empty.
pub fn normalize_service_type(spec: &mut serde_json::Map<String, Value>) {
    let current_type = spec.get("type").and_then(|t| t.as_str()).unwrap_or("");
    if current_type.is_empty() {
        spec.insert("type".to_string(), json!("ClusterIP"));
    }
}

/// K8s Service default: sessionAffinity defaults to "None" when unset.
fn normalize_service_session_affinity(spec: &mut serde_json::Map<String, Value>) {
    let needs_default = spec
        .get("sessionAffinity")
        .is_none_or(|v| v.is_null() || v.as_str().is_some_and(str::is_empty));
    if needs_default {
        spec.insert("sessionAffinity".to_string(), json!("None"));
    }
}

/// Apply K8s ServicePort defaults.
/// - protocol defaults to TCP
/// - targetPort defaults to the same value as port when omitted/empty/0
fn normalize_service_ports(spec: &mut serde_json::Map<String, Value>) {
    let Some(ports) = spec.get_mut("ports").and_then(|p| p.as_array_mut()) else {
        return;
    };

    for port in ports {
        let Some(port_obj) = port.as_object_mut() else {
            continue;
        };

        let protocol_missing_or_empty = port_obj
            .get("protocol")
            .and_then(|v| v.as_str())
            .map(|s| s.is_empty())
            .unwrap_or(true);
        if protocol_missing_or_empty {
            port_obj.insert("protocol".to_string(), json!("TCP"));
        }

        let target_port_missing_or_empty = match port_obj.get("targetPort") {
            None => true,
            Some(v) if v.is_null() => true,
            Some(v) => v.as_str().map(|s| s.is_empty()).unwrap_or(false) || v.as_i64() == Some(0),
        };
        if target_port_missing_or_empty && let Some(service_port) = port_obj.get("port").cloned() {
            port_obj.insert("targetPort".to_string(), service_port);
        }
    }
}

/// K8s spec: ExternalName services return CNAME DNS records and must not
/// carry any cluster-routing fields — `clusterIP`/`clusterIPs` are emptied
/// and per-port `nodePort` values are removed so a Service transitioned
/// from NodePort/LoadBalancer to ExternalName presents the same shape an
/// ExternalName-from-creation would.
///
/// P0-E2E-20260423-07 regression: the conformance test that switches an
/// existing NodePort Service to type=ExternalName asserts
/// `Spec.Ports[0].NodePort` is unset on the persisted object; the previous
/// implementation only cleared `clusterIP`/`clusterIPs`, leaving stale
/// NodePort allocations visible.
fn clear_externalname_invalid_fields(spec: &mut serde_json::Map<String, Value>) {
    let is_external_name = spec.get("type").and_then(|t| t.as_str()) == Some("ExternalName");
    if !is_external_name {
        return;
    }
    spec.insert("clusterIP".to_string(), json!(""));
    spec.insert("clusterIPs".to_string(), json!([]));
    if let Some(ports) = spec.get_mut("ports").and_then(|p| p.as_array_mut()) {
        for port in ports {
            if let Some(port_obj) = port.as_object_mut() {
                port_obj.remove("nodePort");
            }
        }
    }
}

#[cfg(test)]
pub async fn reconcile_service(
    db: &dyn DatastoreBackend,
    pod_reader: &dyn crate::kubelet::pod_repository::PodReader,
    service: &Value,
    service_ipam: &ServiceIpam,
) -> Result<Value> {
    let default_alloc = NodePortAllocator::new();
    reconcile_service_with_nodeport(db, pod_reader, service, service_ipam, &default_alloc).await
}

pub async fn reconcile_service_with_nodeport(
    db: &dyn DatastoreBackend,
    pod_reader: &dyn crate::kubelet::pod_repository::PodReader,
    service: &Value,
    service_ipam: &ServiceIpam,
    nodeport_alloc: &NodePortAllocator,
) -> Result<Value> {
    let input_metadata = service
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

    let Some(live_service) = db
        .get_resource("v1", "Service", Some(namespace), name)
        .await?
    else {
        return Ok(service.clone());
    };
    let service =
        crate::api::inject_resource_version(live_service.data, live_service.resource_version);
    let metadata = service
        .get("metadata")
        .ok_or_else(|| anyhow::anyhow!("Missing metadata"))?;
    if metadata.get("deletionTimestamp").is_some() {
        return Ok(service);
    }
    let current_rv = crate::utils::extract_resource_version(metadata);
    let update_preconditions = ResourcePreconditions::from_metadata(metadata, current_rv)?;

    let mut updated_service = service.clone();
    let spec_mut = updated_service
        .get_mut("spec")
        .and_then(|s| s.as_object_mut())
        .ok_or_else(|| anyhow::anyhow!("Invalid spec"))?;

    // Normalize spec.type: default to "ClusterIP" if missing or empty
    normalize_service_type(spec_mut);
    normalize_service_session_affinity(spec_mut);
    // Default ServicePort fields to Kubernetes behavior used by kubectl describe.
    normalize_service_ports(spec_mut);

    // ExternalName services must not have a clusterIP — clear it if type changed to ExternalName
    clear_externalname_invalid_fields(spec_mut);

    // Allocate ClusterIP if not set (but preserve "None" for headless services)
    let cluster_ip_value = spec_mut.get("clusterIP").and_then(|v| v.as_str());

    // Check if clusterIP is missing, empty, or None (headless)
    let needs_cluster_ip = cluster_ip_value.is_none()
        || cluster_ip_value.map(|s| s.is_empty()).unwrap_or(false)
        || cluster_ip_value.map(|s| s == "None").unwrap_or(false);

    let service_type = spec_mut
        .get("type")
        .and_then(|t| t.as_str())
        .unwrap_or("ClusterIP")
        .to_string();
    let is_headless = cluster_ip_value.map(|s| s == "None").unwrap_or(false);
    let should_allocate_cluster_ip =
        needs_cluster_ip && service_type != "ExternalName" && !is_headless;
    let cluster_ip_guard = if should_allocate_cluster_ip {
        Some(service_ipam.allocation_guard().await)
    } else {
        None
    };
    let mut allocated_cluster_ip: Option<String> = None;
    let mut allocated_node_ports: Vec<u32> = Vec::new();

    if needs_cluster_ip {
        // No clusterIP specified - allocate one unless service is ExternalName
        if service_type != "ExternalName" {
            // Only allocate if not explicitly "None" (headless)
            if !is_headless {
                let cluster_ip = allocate_service_cluster_ip(db, service_ipam).await?;

                spec_mut.insert("clusterIP".to_string(), json!(cluster_ip.clone()));
                spec_mut.insert("clusterIPs".to_string(), json!([cluster_ip.clone()]));
                allocated_cluster_ip = Some(cluster_ip);
            }
        }
    }
    // If clusterIP is explicitly set to "None", it's a headless service - don't allocate

    // Allocate NodePort if type is NodePort or LoadBalancer
    if (service_type == "NodePort" || service_type == "LoadBalancer")
        && let Some(ports) = spec_mut.get_mut("ports").and_then(|p| p.as_array_mut())
    {
        for port in ports {
            if let Some(port_obj) = port.as_object_mut() {
                // Allocate NodePort if not set or set to 0
                let needs_nodeport = port_obj
                    .get("nodePort")
                    .and_then(|np| np.as_u64())
                    .map(|np| np == 0)
                    .unwrap_or(true); // true if key doesn't exist

                if needs_nodeport {
                    let node_port = match nodeport_alloc.allocate() {
                        Ok(node_port) => node_port,
                        Err(e) => {
                            release_pending_service_allocations(
                                service_ipam,
                                nodeport_alloc,
                                allocated_cluster_ip.as_deref(),
                                &allocated_node_ports,
                            );
                            return Err(anyhow::anyhow!("NodePort allocation failed: {}", e));
                        }
                    };
                    port_obj.insert("nodePort".to_string(), json!(node_port));
                    allocated_node_ports.push(node_port);
                }
            }
        }
    }

    // Only persist when the normalized object differs from the stored object.
    // Endpoint-only reconcile events must not churn Service resourceVersions.
    let needs_update = updated_service != service;
    let (updated_data, updated_rv) = if needs_update {
        let update_result = db
            .update_resource_with_preconditions(
                "v1",
                "Service",
                Some(namespace),
                name,
                updated_service.clone(),
                update_preconditions,
            )
            .await;
        match update_result {
            Ok(updated) => (updated.data, updated.resource_version),
            Err(err) => {
                release_pending_service_allocations(
                    service_ipam,
                    nodeport_alloc,
                    allocated_cluster_ip.as_deref(),
                    &allocated_node_ports,
                );
                return Err(err);
            }
        }
    } else {
        (std::sync::Arc::new(service.clone()), current_rv)
    };
    drop(cluster_ip_guard);

    // Skip Endpoints and EndpointSlice for ExternalName services
    // ExternalName services return CNAME DNS records, not pod IPs
    let updated_metadata = updated_data
        .get("metadata")
        .ok_or_else(|| anyhow::anyhow!("Missing updated metadata"))?;
    let updated_spec = updated_data
        .get("spec")
        .ok_or_else(|| anyhow::anyhow!("Missing updated spec"))?;
    let service_type = updated_spec
        .get("type")
        .and_then(|t| t.as_str())
        .unwrap_or("ClusterIP");

    if service_type != "ExternalName" {
        // Check publishNotReadyAddresses (spec field or legacy annotation)
        let publish_not_ready = updated_spec
            .get("publishNotReadyAddresses")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
            || updated_metadata
                .get("annotations")
                .and_then(|a| a.get("service.alpha.kubernetes.io/tolerate-unready-endpoints"))
                .and_then(|v| v.as_str())
                .map(|v| v == "true")
                .unwrap_or(false);

        // Create or update Endpoints
        crate::controllers::endpoints::reconcile_endpoints(
            db,
            pod_reader,
            name,
            namespace,
            updated_spec.get("selector"),
            updated_spec.get("ports"),
            publish_not_ready,
        )
        .await?;

        // Create or update EndpointSlice (discovery.k8s.io/v1)
        let service_uid = updated_metadata
            .get("uid")
            .and_then(|u| u.as_str())
            .unwrap_or("");
        crate::controllers::endpoints::reconcile_endpointslice(
            db,
            pod_reader,
            name,
            service_uid,
            namespace,
            updated_spec.get("selector"),
            updated_spec.get("ports"),
        )
        .await?;
    }

    // The synchronous nft rebuild used to live here; it's been moved to
    // `ServiceController::reconcile` (Task 5 of the network refactor) so
    // this DB-only path stays callable without a live ServiceRouter.

    let service_with_rv = crate::api::inject_resource_version(updated_data.clone(), updated_rv);
    Ok(service_with_rv)
}

#[cfg(test)]
mod tests;
