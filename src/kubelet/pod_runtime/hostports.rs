use std::sync::Arc;

use crate::kubelet::pod_repository::PodReader;
use crate::kubelet::pod_runtime::service::PodRuntimeKey;
use serde_json::Value;

pub fn pod_host_ports_from_resource(
    key: &PodRuntimeKey,
    pod: &Value,
) -> Result<klights_network_api::PodHostPorts, klights_network_api::ServiceRouterError> {
    let pod_ip = pod
        .pointer("/status/podIP")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .and_then(|value| value.parse().ok());
    klights_network_api::PodHostPorts::try_new(
        klights_types::PodIdentity::new(&key.namespace, &key.name, &key.uid),
        pod_ip,
        klights_network_api::host_port_bindings_from_specs(klights_types::pod_host_port_specs(pod)),
    )
}

/// HostPort/service-routing port used by runtime start/stop flows.
#[async_trait::async_trait]
pub trait HostPortRuntime: Send + Sync {
    /// Add hostPort rules for a pod.
    async fn add_host_ports(&self, pod: &klights_network_api::PodHostPorts) -> anyhow::Result<()>;

    /// Remove hostPort rules for a pod.
    async fn remove_host_ports(
        &self,
        pod: &klights_network_api::PodHostPorts,
    ) -> anyhow::Result<()>;

    /// Check hostPort admission for a pod.
    async fn check_host_port_admission(
        &self,
        pod: &klights_network_api::PodHostPorts,
    ) -> anyhow::Result<()>;
}

// --- Production adapter ---

/// Production hostPort adapter over ServiceRouter + PodRepository.
pub struct RealHostPortRuntime {
    service_router: Arc<dyn klights_network_api::ServiceRouter>,
    repository: Arc<dyn PodReader>,
    node_name: String,
}

impl RealHostPortRuntime {
    pub fn new(
        service_router: Arc<dyn klights_network_api::ServiceRouter>,
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
    async fn add_host_ports(&self, pod: &klights_network_api::PodHostPorts) -> anyhow::Result<()> {
        let pod_ip = match pod.pod_ip() {
            Some(ip) => ip,
            None => {
                tracing::debug!(
                    namespace = pod.pod().namespace,
                    name = pod.pod().name,
                    "no podIP available, skipping hostPort rule add"
                );
                return Ok(());
            }
        };
        if pod.bindings().is_empty() {
            return Ok(());
        }
        let request = klights_network_api::HostPortRules::try_new(pod_ip, pod.bindings().to_vec())?;
        self.service_router
            .add_hostport_rules(request)
            .await
            .map_err(anyhow::Error::from)
    }

    async fn remove_host_ports(
        &self,
        pod: &klights_network_api::PodHostPorts,
    ) -> anyhow::Result<()> {
        if pod.bindings().is_empty() {
            return Ok(());
        }
        let pod_ip = match pod.pod_ip() {
            Some(value) => value,
            _ => return Ok(()),
        };
        let request = klights_network_api::HostPortRemoval::new(pod_ip);
        self.service_router
            .remove_hostport_rules(request)
            .await
            .map_err(anyhow::Error::from)
    }

    async fn check_host_port_admission(
        &self,
        pod: &klights_network_api::PodHostPorts,
    ) -> anyhow::Result<()> {
        reject_hostport_conflicts(self.repository.as_ref(), pod, &self.node_name).await
    }
}

fn hostport_protocol_name(protocol: klights_network_api::HostPortProtocol) -> &'static str {
    match protocol {
        klights_network_api::HostPortProtocol::Tcp => "TCP",
        klights_network_api::HostPortProtocol::Udp => "UDP",
        klights_network_api::HostPortProtocol::Sctp => "SCTP",
    }
}

fn hostport_bindings_conflict(
    left: &klights_network_api::HostPortBinding,
    right: &klights_network_api::HostPortBinding,
) -> bool {
    left.host_port() == right.host_port()
        && left.protocol() == right.protocol()
        && (left.host_ip().is_none()
            || right.host_ip().is_none()
            || left.host_ip() == right.host_ip())
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
    requested_pod: &klights_network_api::PodHostPorts,
    node_name: &str,
) -> anyhow::Result<()> {
    let requested = requested_pod.bindings();
    if requested.is_empty() {
        return Ok(());
    }

    let identity = requested_pod.pod();
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
        if existing_uid == Some(identity.uid.as_str()) {
            continue;
        }
        if existing_namespace == Some(identity.namespace.as_str())
            && existing_name == Some(identity.name.as_str())
        {
            continue;
        }

        let existing_ports = klights_network_api::host_port_bindings_from_specs(
            klights_types::pod_host_port_specs(existing_pod),
        );
        for requested_port in requested {
            if existing_ports
                .iter()
                .any(|existing_port| hostport_bindings_conflict(requested_port, existing_port))
            {
                return Err(anyhow::anyhow!(
                    "hostPort {}/{} is already allocated on node {} by pod {}/{}",
                    requested_port.host_port(),
                    hostport_protocol_name(requested_port.protocol()),
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

    fn adapted_pod(pod: &Value) -> klights_network_api::PodHostPorts {
        let metadata = pod.get("metadata").expect("pod metadata");
        let key = PodRuntimeKey::new(
            metadata
                .get("namespace")
                .and_then(Value::as_str)
                .unwrap_or("default"),
            metadata.get("name").and_then(Value::as_str).unwrap(),
            metadata.get("uid").and_then(Value::as_str).unwrap(),
        );
        pod_host_ports_from_resource(&key, pod).unwrap()
    }

    #[tokio::test]
    async fn hostport_admission_allows_same_name_recreate_with_different_uid() {
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
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
        let repo = crate::kubelet::pod_repository::pod_repository_for_test(&db);
        let recreated = json!({
            "metadata": {"name": "ss-0", "namespace": "test-ns", "uid": "new-uid"},
            "spec": {
                "nodeName": "test-node",
                "containers": [{"name": "web", "ports": [{"hostPort": 21017, "containerPort": 21017, "protocol": ""}]}]
            }
        });

        reject_hostport_conflicts(repo.as_ref(), &adapted_pod(&recreated), "test-node")
            .await
            .expect("same-name replacement must not fail hostPort admission");
    }

    #[tokio::test]
    async fn hostport_admission_rejects_different_name_same_node_same_port() {
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
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
        let repo = crate::kubelet::pod_repository::pod_repository_for_test(&db);
        let claimant = json!({
            "metadata": {"name": "claimant", "namespace": "test-ns", "uid": "claimant-uid"},
            "spec": {
                "nodeName": "test-node",
                "containers": [{"name": "web", "ports": [{"hostPort": 21017, "containerPort": 21017, "protocol": ""}]}]
            }
        });

        let err = reject_hostport_conflicts(repo.as_ref(), &adapted_pod(&claimant), "test-node")
            .await
            .expect_err(
                "different pod binding the same hostPort on the same node must be rejected",
            );
        assert!(format!("{err:#}").contains("hostPort 21017/TCP is already allocated"));
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Mutex;

    // --- MockHostPortRuntime ---

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) enum MockHostPortOp {
        Add {
            namespace: String,
            name: String,
            uid: String,
        },
        Remove {
            namespace: String,
            name: String,
            uid: String,
        },
        Check {
            namespace: String,
            name: String,
            uid: String,
        },
    }

    pub(crate) struct MockHostPortRuntime {
        calls: Mutex<Vec<MockHostPortOp>>,
        check_error: Mutex<Option<String>>,
        add_error: Mutex<Option<String>>,
        hang_add: Mutex<bool>,
    }

    impl Default for MockHostPortRuntime {
        fn default() -> Self {
            Self::new()
        }
    }

    impl MockHostPortRuntime {
        pub(crate) fn new() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                check_error: Mutex::new(None),
                add_error: Mutex::new(None),
                hang_add: Mutex::new(false),
            }
        }

        pub(crate) fn clear_calls(&self) {
            self.calls.lock().unwrap().clear();
        }

        pub(crate) fn recorded_calls(&self) -> Vec<MockHostPortOp> {
            self.calls.lock().unwrap().clone()
        }

        pub(crate) fn reject_next_check(&self, message: &str) {
            *self.check_error.lock().unwrap() = Some(message.to_string());
        }

        pub(crate) fn hang_add_host_ports(&self) {
            *self.hang_add.lock().unwrap() = true;
        }
    }

    #[async_trait::async_trait]
    impl crate::kubelet::pod_runtime::hostports::HostPortRuntime for MockHostPortRuntime {
        async fn add_host_ports(
            &self,
            pod: &klights_network_api::PodHostPorts,
        ) -> anyhow::Result<()> {
            let key = pod.pod();
            self.calls.lock().unwrap().push(MockHostPortOp::Add {
                namespace: key.namespace.clone(),
                name: key.name.clone(),
                uid: key.uid.clone(),
            });
            if let Some(message) = self.add_error.lock().unwrap().take() {
                anyhow::bail!("{message}");
            }
            let should_hang = *self.hang_add.lock().unwrap();
            if should_hang {
                std::future::pending::<()>().await;
            }
            Ok(())
        }

        async fn remove_host_ports(
            &self,
            pod: &klights_network_api::PodHostPorts,
        ) -> anyhow::Result<()> {
            let key = pod.pod();
            self.calls.lock().unwrap().push(MockHostPortOp::Remove {
                namespace: key.namespace.clone(),
                name: key.name.clone(),
                uid: key.uid.clone(),
            });
            Ok(())
        }

        async fn check_host_port_admission(
            &self,
            pod: &klights_network_api::PodHostPorts,
        ) -> anyhow::Result<()> {
            let key = pod.pod();
            self.calls.lock().unwrap().push(MockHostPortOp::Check {
                namespace: key.namespace.clone(),
                name: key.name.clone(),
                uid: key.uid.clone(),
            });
            if let Some(message) = self.check_error.lock().unwrap().take() {
                anyhow::bail!("{message}");
            }
            Ok(())
        }
    }
}
