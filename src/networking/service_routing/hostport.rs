use super::prelude::*;
use super::service_rules::Protocol;
use klights_network_api::{HostPortBinding, HostPortProtocol};

// ============ HostPortSpec ===============================================
// Per-pod hostport mapping. Typed data extracted from a Pod's container
// ports so the nft rule builder can consume it directly.

/// One hostPort declared on a container port.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HostPortSpec {
    /// `containerPort.hostIP` — `None` means "any host destination IP"
    /// (i.e. omit the `ip daddr` match in the rule).
    pub(crate) host_ip: Option<Ipv4Addr>,
    /// `containerPort.hostPort` — the port the host listens on.
    pub(crate) host_port: u16,
    /// `containerPort.containerPort` — the pod-side port the rule
    /// DNATs to.
    pub(crate) container_port: u16,
    pub(crate) protocol: Protocol,
}

impl HostPortSpec {
    /// Walk a Pod's `spec.containers[].ports[]` and return every port
    /// with a non-zero `hostPort` declaration. hostPort=0 (or missing)
    /// is skipped; protocol defaults to TCP; `0.0.0.0`/empty hostIP is
    /// treated as "any IP".
    #[cfg(test)]
    pub(crate) fn from_pod(pod: &serde_json::Value) -> Vec<HostPortSpec> {
        crate::networking::hostport_resource::bindings_from_pod(pod)
            .into_iter()
            .map(Into::into)
            .collect()
    }
}

#[cfg(test)]
impl From<HostPortSpec> for HostPortBinding {
    fn from(spec: HostPortSpec) -> Self {
        let protocol = match spec.protocol {
            Protocol::Tcp => HostPortProtocol::Tcp,
            Protocol::Udp => HostPortProtocol::Udp,
            Protocol::Sctp => HostPortProtocol::Sctp,
        };
        HostPortBinding::try_new(spec.host_ip, spec.host_port, spec.container_port, protocol)
            .expect("HostPortSpec only contains validated non-zero ports")
    }
}

impl From<HostPortBinding> for HostPortSpec {
    fn from(binding: HostPortBinding) -> Self {
        let protocol = match binding.protocol() {
            HostPortProtocol::Tcp => Protocol::Tcp,
            HostPortProtocol::Udp => Protocol::Udp,
            HostPortProtocol::Sctp => Protocol::Sctp,
        };
        Self {
            host_ip: binding.host_ip(),
            host_port: binding.host_port(),
            container_port: binding.container_port(),
            protocol,
        }
    }
}
