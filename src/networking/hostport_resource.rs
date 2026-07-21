use klights_network_api::{HostPortBinding, HostPortProtocol};

/// Adapt Kubernetes container-port JSON into the validated transport-neutral
/// hostPort values consumed by kubelet/runtime and the concrete nft adapter.
pub(crate) fn bindings_from_pod(pod: &serde_json::Value) -> Vec<HostPortBinding> {
    let mut bindings = Vec::new();
    let Some(containers) = pod
        .pointer("/spec/containers")
        .and_then(|value| value.as_array())
    else {
        return bindings;
    };
    for container in containers {
        let Some(ports) = container.get("ports").and_then(|value| value.as_array()) else {
            continue;
        };
        for port in ports {
            let Some(host_port) = parse_port(port.get("hostPort")) else {
                continue;
            };
            let Some(container_port) = parse_port(port.get("containerPort")) else {
                continue;
            };
            let Some(protocol) =
                parse_protocol(port.get("protocol").and_then(|value| value.as_str()))
            else {
                continue;
            };
            let host_ip = port
                .get("hostIP")
                .and_then(|value| value.as_str())
                .filter(|value| !value.is_empty() && *value != "0.0.0.0")
                .and_then(|value| value.parse().ok());
            bindings.push(
                HostPortBinding::try_new(host_ip, host_port, container_port, protocol)
                    .expect("resource adapter validates both hostPort values"),
            );
        }
    }
    bindings
}

fn parse_port(value: Option<&serde_json::Value>) -> Option<u16> {
    let value = value?.as_i64()?;
    u16::try_from(value).ok().filter(|port| *port != 0)
}

fn parse_protocol(value: Option<&str>) -> Option<HostPortProtocol> {
    match value
        .filter(|value| !value.is_empty())
        .unwrap_or("TCP")
        .to_ascii_uppercase()
        .as_str()
    {
        "TCP" => Some(HostPortProtocol::Tcp),
        "UDP" => Some(HostPortProtocol::Udp),
        "SCTP" => Some(HostPortProtocol::Sctp),
        _ => None,
    }
}
