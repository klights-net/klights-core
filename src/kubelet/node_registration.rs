use crate::kubelet::node;
use crate::kubelet::node_config::{KubeletNodeRole, NodeRegistrationProfile};
use crate::node_outbox::payload::OutboxOperation;
use crate::node_outbox::{Outbox, OutboxSendRoute};
use crate::utils::k8s_time_now;
use anyhow::{Context, Result};
use klights_cluster_core::ResourcePreconditions;
use klights_cluster_core::StorageCommand;

/// Focused persistence capability required by Node registration.
///
/// Concrete datastore adaptation is owned by bootstrap composition; kubelet
/// owns only the registration transaction it needs.
#[async_trait::async_trait]
pub(crate) trait NodeRegistrationStore: Send + Sync {
    async fn get_node(&self, node_name: &str) -> Result<Option<klights_cluster_core::Resource>>;

    async fn stamp_routing_metadata(
        &self,
        node_name: &str,
        node: &mut serde_json::Value,
    ) -> Result<bool>;

    async fn update_node(
        &self,
        node_name: &str,
        node: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> Result<()>;

    async fn create_node(&self, node_name: &str, node: serde_json::Value) -> Result<()>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NodeRegistrationAddresses {
    internal_ip: String,
    external_ip: Option<String>,
}

impl NodeRegistrationAddresses {
    pub fn new(internal_ip: String, external_ip: Option<String>) -> Self {
        let external_ip = external_ip
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        Self {
            internal_ip,
            external_ip,
        }
    }

    pub fn internal_ip(&self) -> &str {
        &self.internal_ip
    }

    pub fn external_ip(&self) -> Option<&str> {
        self.external_ip.as_deref()
    }
}

/// Host facts owned by the node being registered. Remote registration must
/// receive these from the joiner; the leader must never substitute its own
/// capacity, architecture, kernel, runtime, or build identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NodeRegistrationHostFacts {
    pub cpu_count: u32,
    pub memory_ki: u64,
    pub architecture: String,
    pub operating_system: String,
    pub os_image: String,
    pub kernel_version: String,
    pub container_runtime_version: String,
    pub kubelet_version: String,
    pub git_commit: String,
}

impl NodeRegistrationHostFacts {
    pub fn node_capacity(&self) -> crate::kubelet::node::NodeCapacity {
        crate::kubelet::node::NodeCapacity::new(self.memory_ki, u64::from(self.cpu_count))
    }

    pub async fn capture_local(
        file_process: &klights_supervisor::FileProcessExecutor,
        kubelet_version: &str,
    ) -> Self {
        let (host_info, memory_info) = tokio::join!(
            host_node_info(file_process),
            crate::utils::read_utf8_file_async(file_process, "/proc/meminfo")
        );
        let memory_ki = memory_info
            .ok()
            .and_then(|content| node::parse_memory_ki(&content))
            .unwrap_or(8 * 1024 * 1024);
        Self {
            cpu_count: u32::try_from(num_cpus()).unwrap_or(u32::MAX),
            memory_ki,
            architecture: std::env::consts::ARCH.to_string(),
            operating_system: "linux".to_string(),
            os_image: host_info.os_image,
            kernel_version: host_info.kernel_version,
            container_runtime_version: "containerd://1.7.0".to_string(),
            kubelet_version: kubelet_version.to_string(),
            git_commit: crate::version::GIT_COMMIT_SHORT.to_string(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            self.cpu_count > 0,
            "node registration cpu_count must be positive"
        );
        anyhow::ensure!(
            self.memory_ki > 0,
            "node registration memory_ki must be positive"
        );
        validate_node_registration_text("architecture", &self.architecture, 63)?;
        validate_node_registration_text("operating_system", &self.operating_system, 63)?;
        anyhow::ensure!(
            self.operating_system == "linux",
            "node registration operating_system must be 'linux'"
        );
        validate_node_registration_text("os_image", &self.os_image, 256)?;
        validate_node_registration_text("kernel_version", &self.kernel_version, 256)?;
        validate_node_registration_text(
            "container_runtime_version",
            &self.container_runtime_version,
            256,
        )?;
        validate_node_registration_text("kubelet_version", &self.kubelet_version, 256)?;
        validate_node_registration_text("git_commit", &self.git_commit, 128)?;
        Ok(())
    }

    /// Rehydrate facts for an old persisted control-plane member whose legacy
    /// JoinAsControlplane request predates the typed snapshot. This reads only
    /// the member's existing Node row; it never samples the leader host.
    pub fn from_existing_node(
        node: &serde_json::Value,
        joiner_git_commit: Option<&str>,
    ) -> Result<Self> {
        let cpu_count = node
            .pointer("/status/capacity/cpu")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| value.parse::<u32>().ok())
            .context("persisted Node capacity.cpu is not a positive integer")?;
        let memory_ki = node
            .pointer("/status/capacity/memory")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| value.strip_suffix("Ki"))
            .and_then(|value| value.parse::<u64>().ok())
            .context("persisted Node capacity.memory is not expressed in Ki")?;
        let text = |pointer: &str, field: &str| -> Result<String> {
            node.pointer(pointer)
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
                .with_context(|| format!("persisted Node is missing {field}"))
        };
        let git_commit = joiner_git_commit
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .or_else(|| {
                node.pointer("/metadata/annotations/klights.io~1git-commit")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
            .context("persisted Node is missing joiner-owned git commit")?;
        let facts = Self {
            cpu_count,
            memory_ki,
            architecture: text("/status/nodeInfo/architecture", "nodeInfo.architecture")?,
            operating_system: text(
                "/status/nodeInfo/operatingSystem",
                "nodeInfo.operatingSystem",
            )?,
            os_image: text("/status/nodeInfo/osImage", "nodeInfo.osImage")?,
            kernel_version: text("/status/nodeInfo/kernelVersion", "nodeInfo.kernelVersion")?,
            container_runtime_version: text(
                "/status/nodeInfo/containerRuntimeVersion",
                "nodeInfo.containerRuntimeVersion",
            )?,
            kubelet_version: text("/status/nodeInfo/kubeletVersion", "nodeInfo.kubeletVersion")?,
            git_commit,
        };
        facts.validate()?;
        Ok(facts)
    }
}

/// Complete typed input for constructing one Kubernetes Node registration.
/// Registration metadata and host facts travel together so a remote writer
/// cannot accidentally combine joiner identity with leader-local host data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NodeRegistrationSnapshot {
    pub node_name: String,
    pub node_mode: klights_network_api::NodePeerMode,
    pub node_role: KubeletNodeRole,
    pub publish_external_ip: bool,
    pub addresses: NodeRegistrationAddresses,
    pub raft_shape: Option<klights_cluster_core::RaftShape>,
    pub grpc_port: Option<u16>,
    pub host: NodeRegistrationHostFacts,
}

impl NodeRegistrationSnapshot {
    pub async fn capture_local(
        file_process: &klights_supervisor::FileProcessExecutor,
        node_name: &str,
        profile: &NodeRegistrationProfile,
        addresses: NodeRegistrationAddresses,
        raft_shape: Option<&klights_cluster_core::RaftShape>,
        grpc_port: Option<u16>,
    ) -> Self {
        Self {
            node_name: node_name.to_string(),
            node_mode: profile.peer_mode(),
            node_role: profile.role(),
            publish_external_ip: profile.publish_external_ip(),
            addresses,
            raft_shape: raft_shape.cloned(),
            grpc_port,
            host: NodeRegistrationHostFacts::capture_local(file_process, profile.kubelet_version())
                .await,
        }
    }

    pub fn validate(&self) -> Result<()> {
        validate_node_registration_text("node_name", &self.node_name, 253)?;
        validate_node_registration_text("internal_ip", self.addresses.internal_ip(), 64)?;
        self.addresses
            .internal_ip()
            .parse::<std::net::IpAddr>()
            .context("node registration internal_ip is invalid")?;
        if let Some(external_ip) = self.addresses.external_ip() {
            validate_node_registration_text("external_ip", external_ip, 64)?;
        }
        self.host.validate()
    }

    pub fn controlplane_as_learner(&self) -> Option<bool> {
        match &self.node_role {
            KubeletNodeRole::Controlplane { as_learner } => Some(*as_learner),
            _ => None,
        }
    }
}

fn validate_node_registration_text(field: &str, value: &str, max_len: usize) -> Result<()> {
    anyhow::ensure!(
        !value.is_empty(),
        "node registration {field} must not be empty"
    );
    anyhow::ensure!(
        value == value.trim(),
        "node registration {field} must not have surrounding whitespace"
    );
    anyhow::ensure!(
        value.len() <= max_len,
        "node registration {field} exceeds {max_len} bytes"
    );
    Ok(())
}

fn registration_mode_annotation(mode: &klights_network_api::NodePeerMode) -> &'static str {
    match mode {
        klights_network_api::NodePeerMode::Root => "root",
        klights_network_api::NodePeerMode::Rootless => "rootless",
    }
}

fn registration_hostport_range(mode: &klights_network_api::NodePeerMode) -> &'static str {
    match mode {
        klights_network_api::NodePeerMode::Root => "",
        klights_network_api::NodePeerMode::Rootless => klights_network_api::DEFAULT_HOSTPORT_RANGE,
    }
}

/// Get number of CPUs from system
fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

struct HostNodeInfo {
    os_image: String,
    kernel_version: String,
}

async fn host_node_info(file_process: &klights_supervisor::FileProcessExecutor) -> HostNodeInfo {
    let os_image = crate::utils::read_utf8_file_async(file_process, "/etc/os-release")
        .await
        .ok()
        .and_then(|content| os_release_pretty_name(&content))
        .unwrap_or_else(|| "Linux".to_string());
    let kernel_version =
        crate::utils::read_utf8_file_async(file_process, "/proc/sys/kernel/osrelease")
            .await
            .ok()
            .map(|content| content.trim().to_string())
            .filter(|content| !content.is_empty())
            .unwrap_or_else(|| "<unknown>".to_string());

    HostNodeInfo {
        os_image,
        kernel_version,
    }
}

fn os_release_pretty_name(content: &str) -> Option<String> {
    content.lines().find_map(|line| {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return None;
        }

        let value = line.strip_prefix("PRETTY_NAME=")?;
        let value = unquote_os_release_value(value.trim());
        if value.is_empty() { None } else { Some(value) }
    })
}

fn unquote_os_release_value(value: &str) -> String {
    let Some(quote) = value.chars().next().filter(|ch| *ch == '"' || *ch == '\'') else {
        return value.to_string();
    };
    if !value.ends_with(quote) || value.len() < 2 {
        return value.to_string();
    }

    let inner = &value[1..value.len() - 1];
    let mut out = String::with_capacity(inner.len());
    let mut escaped = false;
    for ch in inner.chars() {
        if escaped {
            out.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else {
            out.push(ch);
        }
    }
    if escaped {
        out.push('\\');
    }
    out
}

/// Register Node resource on startup. F2-05: publishes the
/// `klights.io/mode` and `klights.io/hostport-range` annotations so peers
/// (root + rootless + hybrid) can discover each other's mode through Node
/// metadata.
#[cfg(test)]
pub(crate) async fn register_node(
    file_process: &klights_supervisor::FileProcessExecutor,
    db: &dyn crate::datastore::DatastoreBackend,
    node_name: &str,
    profile: &NodeRegistrationProfile,
    dataplane_health: Option<&klights_network_api::DataplaneHealthSnapshot>,
    dataplane_external_ip: Option<&str>,
) -> Result<()> {
    let snapshot = capture_node_registration_snapshot(
        file_process,
        node_name,
        profile,
        dataplane_external_ip,
        None,
    )
    .await;
    crate::bootstrap::node_registration_adapter::register_node_snapshot(
        db,
        None,
        dataplane_health,
        &snapshot,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(crate) async fn register_node_with_outbox(
    file_process: &klights_supervisor::FileProcessExecutor,
    db: &dyn crate::datastore::DatastoreBackend,
    outbox: &Outbox,
    node_name: &str,
    profile: &NodeRegistrationProfile,
    dataplane_health: Option<&klights_network_api::DataplaneHealthSnapshot>,
    dataplane_external_ip: Option<&str>,
) -> Result<()> {
    let snapshot = capture_node_registration_snapshot(
        file_process,
        node_name,
        profile,
        dataplane_external_ip,
        None,
    )
    .await;
    crate::bootstrap::node_registration_adapter::register_node_snapshot(
        db,
        Some(outbox),
        dataplane_health,
        &snapshot,
    )
    .await
}

#[cfg(test)]
pub(crate) async fn register_node_at_addresses(
    file_process: &klights_supervisor::FileProcessExecutor,
    db: &dyn crate::datastore::DatastoreBackend,
    node_name: &str,
    profile: &NodeRegistrationProfile,
    dataplane_health: Option<&klights_network_api::DataplaneHealthSnapshot>,
    addresses: &NodeRegistrationAddresses,
) -> Result<()> {
    let snapshot = NodeRegistrationSnapshot::capture_local(
        file_process,
        node_name,
        profile,
        addresses.clone(),
        None,
        None,
    )
    .await;
    crate::bootstrap::node_registration_adapter::register_node_snapshot(
        db,
        None,
        dataplane_health,
        &snapshot,
    )
    .await
}

#[cfg(test)]
async fn capture_node_registration_snapshot(
    file_process: &klights_supervisor::FileProcessExecutor,
    node_name: &str,
    profile: &NodeRegistrationProfile,
    dataplane_external_ip: Option<&str>,
    grpc_port: Option<u16>,
) -> NodeRegistrationSnapshot {
    let node_ip = crate::kubelet::node_ip::resolve_node_ip(node_name).await;
    NodeRegistrationSnapshot::capture_local(
        file_process,
        node_name,
        profile,
        NodeRegistrationAddresses::new(node_ip, dataplane_external_ip.map(str::to_string)),
        None,
        grpc_port,
    )
    .await
}

pub(crate) async fn register_node_snapshot(
    db: &dyn NodeRegistrationStore,
    outbox: Option<&Outbox>,
    dataplane_health: Option<&klights_network_api::DataplaneHealthSnapshot>,
    snapshot: &NodeRegistrationSnapshot,
) -> Result<()> {
    use klights_network_api::{
        GIT_COMMIT_ANNOTATION, GRPC_PORT_ANNOTATION, HOSTPORT_RANGE_ANNOTATION,
        NODE_MODE_ANNOTATION,
    };
    snapshot.validate()?;
    let node_name = &snapshot.node_name;
    let node_role = &snapshot.node_role;
    tracing::info!("Registering node: {}", node_name);
    let host = &snapshot.host;
    let node_ip = snapshot.addresses.internal_ip();

    let conditions = node::NodeNetworkConditions::from_health(dataplane_health);
    let node::NodeNetworkConditions {
        ready_status,
        ready_reason,
        ready_message,
        net_unavail_status,
        net_unavail_reason,
        net_unavail_message,
    } = &conditions;

    let mut addresses = vec![
        serde_json::json!({"type": "Hostname", "address": node_name}),
        serde_json::json!({"type": "InternalIP", "address": node_ip}),
    ];
    if snapshot.publish_external_ip
        && let Some(external_ip) = snapshot
            .addresses
            .external_ip()
            .map(str::trim)
            .filter(|value| !value.is_empty())
    {
        addresses.push(node_address_json("ExternalIP", external_ip));
    }

    let mut node = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": node_name,
            "creationTimestamp": k8s_time_now(),
            "labels": {
                "kubernetes.io/hostname": node_name,
                "kubernetes.io/os": host.operating_system,
                "kubernetes.io/arch": host.architecture,
                "node.kubernetes.io/instance-type": "klights",
            },
            "annotations": {
                NODE_MODE_ANNOTATION: registration_mode_annotation(&snapshot.node_mode),
                HOSTPORT_RANGE_ANNOTATION: registration_hostport_range(&snapshot.node_mode),
                GIT_COMMIT_ANNOTATION: host.git_commit,
            }
        },
        "spec": {
            "unschedulable": false
        },
        "status": {
            "capacity": {
                "cpu": host.cpu_count.to_string(),
                "memory": format!("{}Ki", host.memory_ki),
                "pods": "110"
            },
            "allocatable": {
                "cpu": host.cpu_count.to_string(),
                "memory": format!("{}Ki", host.memory_ki),
                "pods": "110"
            },
            "conditions": [
                {
                    "type": "Ready",
                    "status": ready_status,
                    "reason": ready_reason,
                    "message": ready_message,
                    "lastTransitionTime": k8s_time_now()
                },
                {
                    "type": "MemoryPressure",
                    "status": "False",
                    "reason": "KubeletHasSufficientMemory",
                    "message": "kubelet has sufficient memory available",
                    "lastTransitionTime": k8s_time_now()
                },
                {
                    "type": "DiskPressure",
                    "status": "False",
                    "reason": "KubeletHasNoDiskPressure",
                    "message": "kubelet has no disk pressure",
                    "lastTransitionTime": k8s_time_now()
                },
                {
                    "type": "PIDPressure",
                    "status": "False",
                    "reason": "KubeletHasSufficientPID",
                    "message": "kubelet has sufficient PID available",
                    "lastTransitionTime": k8s_time_now()
                },
                {
                    "type": "NetworkUnavailable",
                    "status": net_unavail_status,
                    "reason": net_unavail_reason,
                    "message": net_unavail_message,
                    "lastTransitionTime": k8s_time_now()
                }
            ],
            "addresses": addresses,
            "daemonEndpoints": {
                "kubeletEndpoint": {
                    "Port": 10250
                }
            },
            "nodeInfo": {
                "kubeletVersion": host.kubelet_version,
                "operatingSystem": host.operating_system,
                "architecture": host.architecture,
                "osImage": host.os_image,
                "kernelVersion": host.kernel_version,
                "containerRuntimeVersion": host.container_runtime_version,
            }
        }
    });
    if let Some(labels) = node
        .pointer_mut("/metadata/labels")
        .and_then(|labels| labels.as_object_mut())
    {
        // P3-11d: stamp the shape-driven role label set. With no raft_shape
        // the helper falls back to the static `node_role_label_key`, so
        // legacy LeaderFollower mode keeps the same wire bytes.
        for key in node::role_label_keys_for_shape(node_role, snapshot.raft_shape.as_ref()) {
            labels.insert(key.to_string(), serde_json::json!(""));
        }
    }
    // Publish grpc-port annotation for controlplane nodes so workers can
    // discover all controlplane endpoints from Node watch.
    if let Some(port) = snapshot.grpc_port
        && let Some(annotations) = node
            .pointer_mut("/metadata/annotations")
            .and_then(|a| a.as_object_mut())
    {
        annotations.insert(
            GRPC_PORT_ANNOTATION.to_string(),
            serde_json::json!(port.to_string()),
        );
    }
    db.stamp_routing_metadata(node_name, &mut node)
        .await
        .context("Failed to stamp Node routing metadata")?;

    if let Some(existing) = db
        .get_node(node_name)
        .await
        .context("Failed to read existing Node resource")?
    {
        node::preserve_existing_network_conditions(&mut node, &existing.data);
        node::merge_existing_node_mutable_fields(&mut node, &existing.data);
        if let Some(outbox) = outbox {
            let route = node::send_node_command(
                Some(outbox),
                OutboxOperation::NodeRegistration,
                node_name,
                existing.uid.as_str(),
                StorageCommand::UpdateResource {
                    api_version: "v1".to_string(),
                    kind: "Node".to_string(),
                    namespace: None,
                    name: node_name.to_string(),
                    data: node.clone(),
                    expected_rv: existing.resource_version,
                    preconditions: ResourcePreconditions::from_resource(&existing),
                },
            )
            .await
            .context("Failed to send Node status refresh")?;
            if matches!(route, OutboxSendRoute::Enqueued) {
                tracing::info!("Node {} registration refresh enqueued", node_name);
            }
            return Ok(());
        }

        db.update_node(
            node_name,
            node,
            ResourcePreconditions::from_resource(&existing),
        )
        .await
        .context("Failed to update Node resource")?;
        return Ok(());
    }

    if let Some(outbox) = outbox {
        let create_command = StorageCommand::CreateResource {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: node_name.to_string(),
            data: node.clone(),
        };

        let route = node::send_node_command(
            Some(outbox),
            OutboxOperation::NodeRegistration,
            node_name,
            "",
            create_command,
        )
        .await
        .context("Failed to send Node registration")?;
        if matches!(route, OutboxSendRoute::Enqueued) {
            tracing::info!("Node {} registration enqueued", node_name);
        }
        return Ok(());
    }

    db.create_node(node_name, node)
        .await
        .context("Failed to create Node resource")?;
    Ok(())
}

fn node_address_json(address_type: &str, address: &str) -> serde_json::Value {
    serde_json::json!({"type": address_type, "address": address})
}
