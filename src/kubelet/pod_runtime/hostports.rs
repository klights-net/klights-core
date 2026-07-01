use std::sync::Arc;

use crate::kubelet::pod_repository::PodReader;
use crate::kubelet::pod_runtime::service::PodRuntimeKey;
use serde_json::Value;

/// HostPort/service-routing port used by runtime start/stop flows.
#[async_trait::async_trait]
pub trait HostPortRuntime: Send + Sync {
    /// Add hostPort rules for a pod.
    async fn add_host_ports(
        &self,
        key: &PodRuntimeKey,
        pod: &serde_json::Value,
    ) -> anyhow::Result<()>;

    /// Remove hostPort rules for a pod.
    async fn remove_host_ports(
        &self,
        key: &PodRuntimeKey,
        pod: &serde_json::Value,
    ) -> anyhow::Result<()>;

    /// Check hostPort admission for a pod.
    async fn check_host_port_admission(
        &self,
        key: &PodRuntimeKey,
        pod: &serde_json::Value,
    ) -> anyhow::Result<()>;
}

// --- Production adapter ---

/// Production hostPort adapter over ServiceRouter + PodRepository.
pub struct RealHostPortRuntime {
    service_router: Arc<dyn crate::networking::ServiceRouter>,
    repository: Arc<dyn PodReader>,
    node_name: String,
}

impl RealHostPortRuntime {
    pub fn new(
        service_router: Arc<dyn crate::networking::ServiceRouter>,
        repository: Arc<dyn PodReader>,
        node_name: String,
    ) -> Self {
        Self {
            service_router,
            repository,
            node_name,
        }
    }
}

#[async_trait::async_trait]
impl HostPortRuntime for RealHostPortRuntime {
    async fn add_host_ports(
        &self,
        key: &PodRuntimeKey,
        pod: &serde_json::Value,
    ) -> anyhow::Result<()> {
        let pod_ip: std::net::Ipv4Addr = match pod
            .pointer("/status/podIP")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok())
        {
            Some(ip) => ip,
            None => {
                tracing::debug!(
                    namespace = key.namespace,
                    name = key.name,
                    "no podIP available, skipping hostPort rule add"
                );
                return Ok(());
            }
        };
        self.service_router.add_hostport_rules(pod, pod_ip).await
    }

    async fn remove_host_ports(
        &self,
        _key: &PodRuntimeKey,
        pod: &serde_json::Value,
    ) -> anyhow::Result<()> {
        self.service_router.remove_hostport_rules(pod).await
    }

    async fn check_host_port_admission(
        &self,
        _key: &PodRuntimeKey,
        pod: &serde_json::Value,
    ) -> anyhow::Result<()> {
        reject_hostport_conflicts(self.repository.as_ref(), pod, &self.node_name).await
    }
}

fn hostport_protocol_name(protocol: crate::networking::service_routing::Protocol) -> &'static str {
    match protocol {
        crate::networking::service_routing::Protocol::Tcp => "TCP",
        crate::networking::service_routing::Protocol::Udp => "UDP",
        crate::networking::service_routing::Protocol::Sctp => "SCTP",
    }
}

fn hostport_bindings_conflict(
    left: &crate::networking::service_routing::HostPortSpec,
    right: &crate::networking::service_routing::HostPortSpec,
) -> bool {
    left.host_port == right.host_port
        && left.protocol == right.protocol
        && (left.host_ip.is_none() || right.host_ip.is_none() || left.host_ip == right.host_ip)
}

fn pod_is_active_for_hostport_admission(pod: &Value) -> bool {
    if pod.pointer("/metadata/deletionTimestamp").is_some() {
        return false;
    }
    !matches!(
        pod.pointer("/status/phase").and_then(|v| v.as_str()),
        Some("Succeeded" | "Failed")
    )
}

pub async fn reject_hostport_conflicts(
    pod_reader: &dyn PodReader,
    pod: &Value,
    node_name: &str,
) -> anyhow::Result<()> {
    let requested = crate::networking::service_routing::HostPortSpec::from_pod(pod);
    if requested.is_empty() {
        return Ok(());
    }

    let namespace = pod.pointer("/metadata/namespace").and_then(|v| v.as_str());
    let name = pod.pointer("/metadata/name").and_then(|v| v.as_str());
    let uid = pod.pointer("/metadata/uid").and_then(|v| v.as_str());
    let pods = pod_reader.list_pods(None, None, None, None, None).await?;

    for existing in pods.items {
        let existing_pod = &existing.data;
        if !pod_is_active_for_hostport_admission(existing_pod) {
            continue;
        }
        if existing_pod
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str())
            != Some(node_name)
        {
            continue;
        }

        let existing_namespace = existing_pod
            .pointer("/metadata/namespace")
            .and_then(|v| v.as_str());
        let existing_name = existing_pod
            .pointer("/metadata/name")
            .and_then(|v| v.as_str());
        let existing_uid = existing_pod
            .pointer("/metadata/uid")
            .and_then(|v| v.as_str());
        if uid.is_some() && uid == existing_uid {
            continue;
        }
        if namespace == existing_namespace && name == existing_name {
            continue;
        }

        let existing_ports =
            crate::networking::service_routing::HostPortSpec::from_pod(existing_pod);
        for requested_port in &requested {
            if existing_ports
                .iter()
                .any(|existing_port| hostport_bindings_conflict(requested_port, existing_port))
            {
                return Err(anyhow::anyhow!(
                    "hostPort {}/{} is already allocated on node {} by pod {}/{}",
                    requested_port.host_port,
                    hostport_protocol_name(requested_port.protocol),
                    node_name,
                    existing_namespace.unwrap_or("default"),
                    existing_name.unwrap_or("<unknown>")
                ));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn hostport_admission_allows_same_name_recreate_with_different_uid() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("test-ns"),
            "ss-0",
            json!({
                "metadata": {"name": "ss-0", "namespace": "test-ns", "uid": "old-uid"},
                "spec": {
                    "nodeName": "test-node",
                    "containers": [{"name": "web", "ports": [{"hostPort": 21017, "containerPort": 21017, "protocol": ""}]}]
                },
                "status": {"phase": "Running"}
            }),
        )
        .await
        .unwrap();
        let repo = crate::controllers::test_utils::pod_repository_for_test(&db);
        let recreated = json!({
            "metadata": {"name": "ss-0", "namespace": "test-ns", "uid": "new-uid"},
            "spec": {
                "nodeName": "test-node",
                "containers": [{"name": "web", "ports": [{"hostPort": 21017, "containerPort": 21017, "protocol": ""}]}]
            }
        });

        reject_hostport_conflicts(repo.as_ref(), &recreated, "test-node")
            .await
            .expect("same-name replacement must not fail hostPort admission");
    }

    #[tokio::test]
    async fn hostport_admission_rejects_different_name_same_node_same_port() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("test-ns"),
            "holder",
            json!({
                "metadata": {"name": "holder", "namespace": "test-ns", "uid": "holder-uid"},
                "spec": {
                    "nodeName": "test-node",
                    "containers": [{"name": "web", "ports": [{"hostPort": 21017, "containerPort": 21017, "protocol": ""}]}]
                },
                "status": {"phase": "Running"}
            }),
        )
        .await
        .unwrap();
        let repo = crate::controllers::test_utils::pod_repository_for_test(&db);
        let claimant = json!({
            "metadata": {"name": "claimant", "namespace": "test-ns", "uid": "claimant-uid"},
            "spec": {
                "nodeName": "test-node",
                "containers": [{"name": "web", "ports": [{"hostPort": 21017, "containerPort": 21017, "protocol": ""}]}]
            }
        });

        let err = reject_hostport_conflicts(repo.as_ref(), &claimant, "test-node")
            .await
            .expect_err(
                "different pod binding the same hostPort on the same node must be rejected",
            );
        assert!(format!("{err:#}").contains("hostPort 21017/TCP is already allocated"));
    }
}
