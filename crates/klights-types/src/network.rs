//! Transport- and implementation-neutral network identity values.

use std::fmt;
use std::net::Ipv4Addr;

pub const DATAPLANE_ENDPOINT_ANNOTATION: &str = "klights.io/dataplane-endpoint";
pub const DATAPLANE_PORT_ANNOTATION: &str = "klights.io/dataplane-port";
pub const DATAPLANE_MODE_ANNOTATION: &str = "klights.io/dataplane-mode";
pub const DATAPLANE_ENCRYPTION_ANNOTATION: &str = "klights.io/dataplane-encryption";
pub const DATAPLANE_PUBLIC_KEY_ANNOTATION: &str = "klights.io/dataplane-public-key";

pub fn set_node_dataplane_annotations(
    node: &mut serde_json::Value,
    endpoint: &str,
    mode: &str,
    encryption: &str,
    public_key: Option<&str>,
    port: Option<u16>,
) -> bool {
    let Some(node_object) = node.as_object_mut() else {
        return false;
    };
    let metadata = node_object
        .entry("metadata")
        .or_insert_with(|| serde_json::json!({}));
    if !metadata.is_object() {
        *metadata = serde_json::json!({});
    }
    let Some(metadata) = metadata.as_object_mut() else {
        return false;
    };
    let annotations = metadata
        .entry("annotations")
        .or_insert_with(|| serde_json::json!({}));
    if !annotations.is_object() {
        *annotations = serde_json::json!({});
    }
    let Some(annotations) = annotations.as_object_mut() else {
        return false;
    };

    let mut changed = false;
    changed |= set_json_string_field(annotations, DATAPLANE_ENDPOINT_ANNOTATION, endpoint);
    changed |= set_json_string_field(annotations, DATAPLANE_MODE_ANNOTATION, mode);
    changed |= set_json_string_field(annotations, DATAPLANE_ENCRYPTION_ANNOTATION, encryption);
    changed |= match port {
        Some(port) => {
            set_json_string_field(annotations, DATAPLANE_PORT_ANNOTATION, &port.to_string())
        }
        None => annotations.remove(DATAPLANE_PORT_ANNOTATION).is_some(),
    };
    changed |= match public_key {
        Some(public_key) => {
            set_json_string_field(annotations, DATAPLANE_PUBLIC_KEY_ANNOTATION, public_key)
        }
        None => annotations
            .remove(DATAPLANE_PUBLIC_KEY_ANNOTATION)
            .is_some(),
    };
    changed
}

fn set_json_string_field(
    object: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: &str,
) -> bool {
    if object.get(key).and_then(serde_json::Value::as_str) == Some(value) {
        return false;
    }
    object.insert(
        key.to_string(),
        serde_json::Value::String(value.to_string()),
    );
    true
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodePeerMode {
    Root,
    Rootless,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodePeerModeParseError(String);

impl fmt::Display for NodePeerModeParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "annotation 'klights.io/mode' has invalid value '{}'; expected 'root' or 'rootless'",
            self.0
        )
    }
}

impl std::error::Error for NodePeerModeParseError {}

pub fn parse_node_peer_mode(value: Option<&str>) -> Result<NodePeerMode, NodePeerModeParseError> {
    match value {
        None | Some("root") => Ok(NodePeerMode::Root),
        Some("rootless") => Ok(NodePeerMode::Rootless),
        Some(other) => Err(NodePeerModeParseError(other.to_owned())),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostPortRange {
    pub start: u16,
    pub end: u16,
}

impl HostPortRange {
    pub fn parse(raw: &str) -> Result<Self, String> {
        let trimmed = raw.trim();
        let (start, end) = trimmed
            .split_once('-')
            .ok_or_else(|| format!("HostPortRange must be 'start-end', got '{trimmed}'"))?;
        let start = start
            .parse::<u16>()
            .map_err(|error| format!("HostPortRange start '{start}' invalid: {error}"))?;
        let end = end
            .parse::<u16>()
            .map_err(|error| format!("HostPortRange end '{end}' invalid: {error}"))?;
        if start == 0 || end == 0 || start > end {
            return Err(format!(
                "HostPortRange '{trimmed}' must be non-zero and start <= end"
            ));
        }
        Ok(Self { start, end })
    }
}

impl fmt::Display for HostPortRange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}-{}", self.start, self.end)
    }
}

/// Kubernetes container-port protocol before adaptation to a network backend.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PodHostPortProtocol {
    Tcp,
    Udp,
    Sctp,
}

/// Resource-level hostPort facts extracted from one Pod container port.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodHostPortSpec {
    pub host_ip: Option<Ipv4Addr>,
    pub host_port: u16,
    pub container_port: u16,
    pub protocol: PodHostPortProtocol,
}

/// Parse Kubernetes Pod JSON at the shared resource-value boundary.
///
/// Network capability crates consume only these typed facts and therefore do
/// not depend on Kubernetes JSON representation.
pub fn pod_host_port_specs(pod: &serde_json::Value) -> Vec<PodHostPortSpec> {
    let Some(containers) = pod
        .pointer("/spec/containers")
        .and_then(serde_json::Value::as_array)
    else {
        return Vec::new();
    };
    containers
        .iter()
        .filter_map(|container| container.get("ports").and_then(serde_json::Value::as_array))
        .flatten()
        .filter_map(|port| {
            let host_port = json_port(port.get("hostPort"))?;
            let container_port = json_port(port.get("containerPort"))?;
            let protocol = parse_pod_host_port_protocol(
                port.get("protocol").and_then(serde_json::Value::as_str),
            )?;
            let host_ip = port
                .get("hostIP")
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.is_empty() && *value != "0.0.0.0")
                .and_then(|value| value.parse().ok());
            Some(PodHostPortSpec {
                host_ip,
                host_port,
                container_port,
                protocol,
            })
        })
        .collect()
}

fn json_port(value: Option<&serde_json::Value>) -> Option<u16> {
    u16::try_from(value?.as_i64()?)
        .ok()
        .filter(|port| *port != 0)
}

fn parse_pod_host_port_protocol(value: Option<&str>) -> Option<PodHostPortProtocol> {
    match value
        .filter(|value| !value.is_empty())
        .unwrap_or("TCP")
        .to_ascii_uppercase()
        .as_str()
    {
        "TCP" => Some(PodHostPortProtocol::Tcp),
        "UDP" => Some(PodHostPortProtocol::Udp),
        "SCTP" => Some(PodHostPortProtocol::Sctp),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct PodSubnet {
    base: u32,
    prefix: u8,
}

impl PodSubnet {
    pub fn parse(cidr: &str) -> Result<Self, String> {
        let (base, prefix) = parse_cidr_components(cidr)?;
        if !(1..=30).contains(&prefix) {
            return Err(format!(
                "PodSubnet prefix must be in /1..=/30, got /{prefix} in {cidr}"
            ));
        }
        Ok(Self {
            base: base & mask_for_prefix(prefix),
            prefix,
        })
    }

    #[cfg(test)]
    pub fn from_parts(base: u32, prefix: u8) -> Self {
        Self {
            base: base & mask_for_prefix(prefix),
            prefix,
        }
    }

    pub fn bridge_ip(self) -> Ipv4Addr {
        Ipv4Addr::from(self.base + 1)
    }

    pub const fn size(self) -> u32 {
        1_u32 << (32 - self.prefix as u32)
    }

    pub const fn mask(self) -> u32 {
        mask_for_prefix(self.prefix)
    }

    pub const fn prefix(self) -> u8 {
        self.prefix
    }

    pub const fn base(self) -> u32 {
        self.base
    }

    pub fn base_ip(self) -> Ipv4Addr {
        Ipv4Addr::from(self.base)
    }

    pub fn pod_ip_range(self) -> (u32, u32) {
        (self.base + 2, self.base + self.size() - 2)
    }
}

impl fmt::Display for PodSubnet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", Ipv4Addr::from(self.base), self.prefix)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ClusterCidr {
    base: u32,
    prefix: u8,
}

impl ClusterCidr {
    pub fn parse(cidr: &str) -> Result<Self, String> {
        let (base, prefix) = parse_cidr_components(cidr)?;
        Ok(Self { base, prefix })
    }

    #[cfg(test)]
    pub const fn from_parts(base: u32, prefix: u8) -> Self {
        Self { base, prefix }
    }

    pub const fn base(self) -> u32 {
        self.base
    }

    pub const fn network(self) -> u32 {
        self.base & self.mask()
    }

    pub fn network_ip(self) -> Ipv4Addr {
        Ipv4Addr::from(self.network())
    }

    pub const fn prefix(self) -> u8 {
        self.prefix
    }

    pub const fn mask(self) -> u32 {
        mask_for_prefix(self.prefix)
    }
}

impl fmt::Display for ClusterCidr {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", Ipv4Addr::from(self.base), self.prefix)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct NodeName(String);

impl NodeName {
    pub fn parse(name: &str) -> Result<Self, String> {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return Err("Node name cannot be empty".to_string());
        }
        if trimmed.len() > 253 {
            return Err(format!(
                "Node name too long (max 253 chars), got {}",
                trimmed.len()
            ));
        }
        if !trimmed
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'.')
        {
            return Err(format!(
                "Node name must be alphanumeric, hyphen, or dot, got: {trimmed}"
            ));
        }
        if trimmed.starts_with('-') || trimmed.ends_with('-') {
            return Err("Node name cannot start or end with hyphen".to_string());
        }
        Ok(Self(trimmed.to_string()))
    }

    #[cfg(test)]
    pub fn new_unchecked(name: &str) -> Self {
        Self(name.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl AsRef<str> for NodeName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

const fn mask_for_prefix(prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else if prefix >= 32 {
        u32::MAX
    } else {
        u32::MAX << (32 - prefix as u32)
    }
}

fn parse_cidr_components(cidr: &str) -> Result<(u32, u8), String> {
    let (address, prefix) = cidr
        .split_once('/')
        .ok_or_else(|| format!("CIDR must be in the form a.b.c.d/prefix, got: {cidr}"))?;
    let prefix = prefix
        .parse::<u8>()
        .map_err(|_| format!("Invalid prefix length in CIDR: {cidr}"))?;
    if prefix > 32 {
        return Err(format!("IPv4 CIDR prefix must be in /0..=/32: {cidr}"));
    }
    let address = address
        .parse::<Ipv4Addr>()
        .map_err(|_| format!("Invalid IPv4 address in CIDR: {cidr}"))?;
    Ok((u32::from(address), prefix))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_values_preserve_existing_validation_contracts() {
        assert_eq!(
            HostPortRange::parse("20000-20010").unwrap(),
            HostPortRange {
                start: 20_000,
                end: 20_010,
            }
        );
        assert_eq!(
            PodSubnet::parse("10.42.1.99/24").unwrap().to_string(),
            "10.42.1.0/24"
        );
        assert_eq!(
            ClusterCidr::parse("10.42.0.0/16").unwrap().network_ip(),
            Ipv4Addr::new(10, 42, 0, 0)
        );
        assert_eq!(
            NodeName::parse("cp-1.local").unwrap().as_str(),
            "cp-1.local"
        );
        assert_eq!(parse_node_peer_mode(None).unwrap(), NodePeerMode::Root);
        assert_eq!(
            parse_node_peer_mode(Some("rootless")).unwrap(),
            NodePeerMode::Rootless
        );
    }

    #[test]
    fn dataplane_annotations_are_idempotent_and_clear_absent_optional_values() {
        let mut node = serde_json::json!({
            "metadata": {
                "annotations": {
                    DATAPLANE_PORT_ANNOTATION: "51820",
                    DATAPLANE_PUBLIC_KEY_ANNOTATION: "stale"
                }
            }
        });
        assert!(set_node_dataplane_annotations(
            &mut node,
            "192.0.2.15",
            "root",
            "disabled",
            None,
            None,
        ));
        assert!(!set_node_dataplane_annotations(
            &mut node,
            "192.0.2.15",
            "root",
            "disabled",
            None,
            None,
        ));
        let annotations = node
            .pointer("/metadata/annotations")
            .and_then(serde_json::Value::as_object)
            .expect("annotations");
        assert_eq!(
            annotations
                .get(DATAPLANE_ENDPOINT_ANNOTATION)
                .and_then(serde_json::Value::as_str),
            Some("192.0.2.15")
        );
        assert!(!annotations.contains_key(DATAPLANE_PORT_ANNOTATION));
        assert!(!annotations.contains_key(DATAPLANE_PUBLIC_KEY_ANNOTATION));
    }
}
