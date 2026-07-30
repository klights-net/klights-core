use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};

use crate::networking::BridgeName;
use klights_networking::wireguard::DataplaneEncryption;
use klights_types::{ClusterCidr, NodeName};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetworkMode {
    Root,
    Rootless,
}

/// Validated immutable input for root and rootless network-plane boot.
#[derive(Clone, Debug)]
pub struct NetworkBootConfig {
    mode: NetworkMode,
    bridge: BridgeName,
    node: NodeName,
    cluster_cidr: ClusterCidr,
    host_ip: Ipv4Addr,
    encryption: DataplaneEncryption,
    wireguard_device: String,
    wireguard_key_path: PathBuf,
    wireguard_port: u16,
}

impl NetworkBootConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        mode: NetworkMode,
        bridge_name: &str,
        node_name: &str,
        cluster_cidr: &str,
        host_ip: &str,
        encryption: DataplaneEncryption,
        wireguard_device: impl Into<String>,
        wireguard_key_path: impl Into<PathBuf>,
        wireguard_port: u16,
    ) -> Result<Self, String> {
        let wireguard_device = wireguard_device.into();
        if wireguard_device.is_empty() || wireguard_device.len() > 15 {
            return Err("wireguard device must contain 1..=15 bytes".to_string());
        }
        if wireguard_port == 0 {
            return Err("wireguard port must be non-zero".to_string());
        }
        let wireguard_key_path = wireguard_key_path.into();
        if wireguard_key_path.as_os_str().is_empty() {
            return Err("wireguard key path must not be empty".to_string());
        }
        Ok(Self {
            mode,
            bridge: BridgeName::parse(bridge_name)?,
            node: NodeName::parse(node_name)?,
            cluster_cidr: ClusterCidr::parse(cluster_cidr)?,
            host_ip: host_ip
                .parse::<Ipv4Addr>()
                .map_err(|error| format!("invalid node IPv4 address: {error}"))?,
            encryption,
            wireguard_device,
            wireguard_key_path,
            wireguard_port,
        })
    }

    pub const fn mode(&self) -> NetworkMode {
        self.mode
    }
    pub const fn bridge(&self) -> &BridgeName {
        &self.bridge
    }
    pub const fn node(&self) -> &NodeName {
        &self.node
    }
    pub const fn cluster_cidr(&self) -> &ClusterCidr {
        &self.cluster_cidr
    }
    pub const fn host_ip(&self) -> Ipv4Addr {
        self.host_ip
    }
    pub const fn encryption(&self) -> DataplaneEncryption {
        self.encryption
    }
    pub fn wireguard_device(&self) -> &str {
        &self.wireguard_device
    }
    pub fn wireguard_key_path(&self) -> &Path {
        &self.wireguard_key_path
    }
    pub const fn wireguard_port(&self) -> u16 {
        self.wireguard_port
    }
}

/// Validated static scope for best-effort network cleanup.
#[derive(Clone, Debug)]
pub struct NetworkCleanupConfig {
    mode: NetworkMode,
    bridge_name: String,
    wireguard_device: String,
    nft_table_name: String,
    inside_rootlesskit: bool,
}

impl NetworkCleanupConfig {
    pub fn try_new(
        mode: NetworkMode,
        bridge_name: impl Into<String>,
        wireguard_device: impl Into<String>,
        nft_table_name: impl Into<String>,
        inside_rootlesskit: bool,
    ) -> Result<Self, String> {
        let bridge_name = bridge_name.into();
        BridgeName::parse(&bridge_name)?;
        let wireguard_device = wireguard_device.into();
        if wireguard_device.is_empty() || wireguard_device.len() > 15 {
            return Err("wireguard device must contain 1..=15 bytes".to_string());
        }
        let nft_table_name = nft_table_name.into();
        if nft_table_name.trim().is_empty() {
            return Err("nft table name must not be empty".to_string());
        }
        Ok(Self {
            mode,
            bridge_name,
            wireguard_device,
            nft_table_name,
            inside_rootlesskit,
        })
    }

    pub const fn mode(&self) -> NetworkMode {
        self.mode
    }
    pub fn bridge_name(&self) -> &str {
        &self.bridge_name
    }
    pub fn wireguard_device(&self) -> &str {
        &self.wireguard_device
    }
    pub fn nft_table_name(&self) -> &str {
        &self.nft_table_name
    }
    pub const fn inside_rootlesskit(&self) -> bool {
        self.inside_rootlesskit
    }

    pub fn build_cleanup(
        &self,
        file_process: klights_supervisor::FileProcessExecutor,
    ) -> klights_networking::NetworkCleanup {
        let kind = match self.mode {
            NetworkMode::Root => klights_networking::NetworkCleanupKind::Root,
            NetworkMode::Rootless => klights_networking::NetworkCleanupKind::Rootless,
        };
        let args = klights_networking::NetworkCleanupArgs::try_new(
            kind,
            self.bridge_name.clone(),
            self.wireguard_device.clone(),
            self.nft_table_name.clone(),
            self.inside_rootlesskit,
        )
        .expect("NetworkCleanupConfig already validated destination cleanup arguments");
        klights_networking::NetworkCleanup::new(args, file_process)
    }
}
