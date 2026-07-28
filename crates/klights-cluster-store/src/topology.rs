//! Persistence-facing cluster topology values.

use std::fmt;
use std::future::Future;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::pin::Pin;

use base64::Engine;
use klights_types::{HostPortRange, NodeName, NodePeerMode, PodSubnet};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataplaneMetadataError(String);

impl DataplaneMetadataError {
    fn invalid(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for DataplaneMetadataError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for DataplaneMetadataError {}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DataplaneEncryption {
    #[default]
    Enabled,
    Disabled,
}

impl DataplaneEncryption {
    pub fn parse(raw: Option<&str>) -> Result<Self, DataplaneMetadataError> {
        match raw.map(str::trim).filter(|value| !value.is_empty()) {
            None | Some("enabled") => Ok(Self::Enabled),
            Some("disabled") => Ok(Self::Disabled),
            Some(other) => Err(DataplaneMetadataError::invalid(format!(
                "invalid dataplane encryption mode '{other}', expected enabled or disabled"
            ))),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataplaneMode {
    Root,
    Rootless,
}

impl DataplaneMode {
    pub fn parse(raw: &str) -> Result<Self, DataplaneMetadataError> {
        match raw.trim() {
            "root" => Ok(Self::Root),
            "rootless" => Ok(Self::Rootless),
            other => Err(DataplaneMetadataError::invalid(format!(
                "invalid dataplane mode '{other}', expected root or rootless"
            ))),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Root => "root",
            Self::Rootless => "rootless",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WireGuardPublicKey(String);

impl WireGuardPublicKey {
    pub fn parse(raw: &str) -> Result<Self, DataplaneMetadataError> {
        let trimmed = raw.trim();
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(trimmed)
            .map_err(|_| DataplaneMetadataError::invalid("WireGuard public key must be base64"))?;
        if bytes.len() != 32 {
            return Err(DataplaneMetadataError::invalid(format!(
                "WireGuard public key must decode to 32 bytes, got {}",
                bytes.len()
            )));
        }
        Ok(Self(trimmed.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for WireGuardPublicKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataplanePeerMetadata {
    pub node_name: String,
    pub mode: DataplaneMode,
    pub encryption: DataplaneEncryption,
    pub public_key: Option<WireGuardPublicKey>,
    pub endpoint: IpAddr,
    pub port: Option<u16>,
}

impl DataplanePeerMetadata {
    pub fn try_new(
        node_name: String,
        mode: DataplaneMode,
        encryption: DataplaneEncryption,
        public_key: Option<String>,
        endpoint: Option<String>,
        port: Option<u16>,
    ) -> Result<Self, DataplaneMetadataError> {
        if node_name.trim().is_empty() {
            return Err(DataplaneMetadataError::invalid(
                "dataplane peer node_name is required",
            ));
        }
        let endpoint = endpoint
            .as_deref()
            .ok_or_else(|| DataplaneMetadataError::invalid("dataplane peer endpoint is required"))?
            .parse::<IpAddr>()
            .map_err(|_| {
                DataplaneMetadataError::invalid("dataplane peer endpoint must be an IP address")
            })?;
        let public_key = match encryption {
            DataplaneEncryption::Enabled => {
                let raw = public_key.as_deref().ok_or_else(|| {
                    DataplaneMetadataError::invalid(
                        "WireGuard public key is required when encryption is enabled",
                    )
                })?;
                let port = port.ok_or_else(|| {
                    DataplaneMetadataError::invalid(
                        "WireGuard listen port is required when encryption is enabled",
                    )
                })?;
                if port == 0 {
                    return Err(DataplaneMetadataError::invalid(
                        "WireGuard listen port must be non-zero",
                    ));
                }
                Some(WireGuardPublicKey::parse(raw)?)
            }
            DataplaneEncryption::Disabled => None,
        };
        Ok(Self {
            node_name,
            mode,
            encryption,
            public_key,
            endpoint,
            port: port.filter(|value| *value != 0),
        })
    }
}

/// One exact persisted `node_subnets` row.
///
/// This persistence DTO deliberately does not create a broadly shared
/// `klights-types::NodeSubnet`: leader-facing routing projections remain owned
/// by their transport contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredNodeSubnet {
    pub node_name: NodeName,
    pub subnet: PodSubnet,
    pub subnet_base_int: u32,
    pub gateway_ip: Ipv4Addr,
    pub node_ip: Ipv4Addr,
    pub mode: NodePeerMode,
    pub hostport_range: Option<HostPortRange>,
}

impl StoredNodeSubnet {
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        node_name: NodeName,
        subnet: PodSubnet,
        subnet_base_int: u32,
        gateway_ip: Ipv4Addr,
        node_ip: Ipv4Addr,
        mode: NodePeerMode,
        hostport_range: Option<HostPortRange>,
    ) -> Self {
        Self {
            node_name,
            subnet,
            subnet_base_int,
            gateway_ip,
            node_ip,
            mode,
            hostport_range,
        }
    }

    pub const fn node_name(&self) -> &NodeName {
        &self.node_name
    }

    pub const fn subnet(&self) -> &PodSubnet {
        &self.subnet
    }

    pub const fn subnet_base_int(&self) -> u32 {
        self.subnet_base_int
    }

    pub const fn gateway_ip(&self) -> Ipv4Addr {
        self.gateway_ip
    }

    pub const fn node_ip(&self) -> Ipv4Addr {
        self.node_ip
    }

    pub const fn mode(&self) -> NodePeerMode {
        self.mode
    }

    pub const fn hostport_range(&self) -> Option<HostPortRange> {
        self.hostport_range
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeTopologyRequest {
    node_name: NodeName,
}

impl NodeTopologyRequest {
    pub fn try_new(node_name: impl AsRef<str>) -> Result<Self, ClusterTopologyReadError> {
        Ok(Self {
            node_name: NodeName::parse(node_name.as_ref())
                .map_err(ClusterTopologyReadError::invalid_request)?,
        })
    }

    pub const fn node_name(&self) -> &NodeName {
        &self.node_name
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PeerTopologyRequest {
    All,
    Excluding(NodeName),
}

impl PeerTopologyRequest {
    pub const fn all() -> Self {
        Self::All
    }

    pub fn excluding(node_name: impl AsRef<str>) -> Result<Self, ClusterTopologyReadError> {
        Ok(Self::Excluding(
            NodeName::parse(node_name.as_ref())
                .map_err(ClusterTopologyReadError::invalid_request)?,
        ))
    }

    pub const fn excluded_node_name(&self) -> Option<&NodeName> {
        match self {
            Self::All => None,
            Self::Excluding(node_name) => Some(node_name),
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterTopologyReadError {
    InvalidRequest { message: String },
    CorruptData { message: String },
    Retryable { message: String },
    Timeout,
    Cancelled,
}

impl ClusterTopologyReadError {
    fn invalid_request(message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            message: message.into(),
        }
    }

    pub fn corrupt_data(message: impl Into<String>) -> Self {
        Self::CorruptData {
            message: message.into(),
        }
    }

    pub fn retryable(message: impl Into<String>) -> Self {
        Self::Retryable {
            message: message.into(),
        }
    }
}

impl fmt::Display for ClusterTopologyReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { message }
            | Self::CorruptData { message }
            | Self::Retryable { message } => formatter.write_str(message),
            Self::Timeout => formatter.write_str("cluster topology read timed out"),
            Self::Cancelled => formatter.write_str("cluster topology read was cancelled"),
        }
    }
}

impl std::error::Error for ClusterTopologyReadError {}

pub type ClusterTopologyFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ClusterTopologyReadError>> + Send + 'a>>;

pub trait ClusterTopologyRead: Send + Sync {
    fn get_node_dataplane(
        &self,
        request: NodeTopologyRequest,
    ) -> ClusterTopologyFuture<'_, Option<DataplanePeerMetadata>>;

    fn get_node_subnet(
        &self,
        request: NodeTopologyRequest,
    ) -> ClusterTopologyFuture<'_, Option<StoredNodeSubnet>>;

    fn list_peer_subnets(
        &self,
        request: PeerTopologyRequest,
    ) -> ClusterTopologyFuture<'_, Vec<StoredNodeSubnet>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persistence_topology_validates_wireguard_shape_without_network_runtime() {
        let key = base64::engine::general_purpose::STANDARD.encode([7_u8; 32]);
        let metadata = DataplanePeerMetadata::try_new(
            "cp-2".to_string(),
            DataplaneMode::Root,
            DataplaneEncryption::Enabled,
            Some(key.clone()),
            Some("192.0.2.2".to_string()),
            Some(7679),
        )
        .unwrap();
        assert_eq!(metadata.public_key.unwrap().as_str(), key);
        assert!(
            DataplanePeerMetadata::try_new(
                "cp-2".to_string(),
                DataplaneMode::Root,
                DataplaneEncryption::Enabled,
                Some("invalid".to_string()),
                Some("192.0.2.2".to_string()),
                Some(7679),
            )
            .is_err()
        );
    }
}
