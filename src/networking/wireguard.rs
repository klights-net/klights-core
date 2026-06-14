use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use futures::StreamExt;
use netlink_packet_core_08::{NLM_F_ACK, NLM_F_REQUEST, NetlinkMessage, NetlinkPayload};
use netlink_packet_generic_04::GenlMessage;
use netlink_packet_wireguard::{
    WireguardAddressFamily, WireguardAllowedIp, WireguardAllowedIpAttr, WireguardAttribute,
    WireguardCmd, WireguardMessage, WireguardPeer, WireguardPeerAttribute,
};
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;

use crate::task_supervisor::{SupervisedJoinHandle, TaskCategory, TaskSupervisor};

pub const DEFAULT_WIREGUARD_DEVICE: &str = "klights.wg";
pub const DEFAULT_WIREGUARD_PORT: u16 = 7_679;
pub const WIREGUARD_PERSISTENT_KEEPALIVE_SECS: u16 = 25;
/// Conservative MTU for the encrypted pod dataplane.
///
/// Multinode dev clusters commonly run WireGuard over public/NATed paths
/// whose usable PMTU is lower than Ethernet's 1500. 1280 keeps TCP MSS below
/// the IPv6 minimum MTU and prevents larger webhook AdmissionReview POSTs from
/// blackholing when a node is behind consumer or cloud edge networking.
pub const WIREGUARD_MTU: u32 = 1280;
pub const WGPEER_F_REMOVE_ME: u32 = 1 << 0;
pub const WGPEER_F_REPLACE_ALLOWEDIPS: u32 = 1 << 1;
const IFNAMSIZ: usize = 15;

pub fn wireguard_device_config_retry_delays() -> [Duration; 4] {
    [
        Duration::from_millis(25),
        Duration::from_millis(75),
        Duration::from_millis(150),
        Duration::from_millis(300),
    ]
}

type WireGuardNetlinkMessage = NetlinkMessage<GenlMessage<WireguardMessage>>;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DataplaneEncryption {
    #[default]
    Enabled,
    Disabled,
}

impl DataplaneEncryption {
    pub fn parse(raw: Option<&str>) -> Result<Self> {
        match raw.map(str::trim).filter(|value| !value.is_empty()) {
            None | Some("enabled") => Ok(Self::Enabled),
            Some("disabled") => Ok(Self::Disabled),
            Some(other) => Err(anyhow!(
                "invalid dataplane encryption mode '{other}', expected enabled or disabled"
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataplaneMode {
    Root,
    Rootless,
}

impl DataplaneMode {
    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim() {
            "root" => Ok(Self::Root),
            "rootless" => Ok(Self::Rootless),
            other => Err(anyhow!(
                "invalid dataplane mode '{other}', expected root or rootless"
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Root => "root",
            Self::Rootless => "rootless",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WireGuardPublicKey(String);

impl WireGuardPublicKey {
    pub fn parse(raw: &str) -> Result<Self> {
        let trimmed = raw.trim();
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(trimmed)
            .with_context(|| "WireGuard public key must be base64")?;
        if bytes.len() != 32 {
            return Err(anyhow!(
                "WireGuard public key must decode to 32 bytes, got {}",
                bytes.len()
            ));
        }
        Ok(Self(trimmed.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn to_bytes(&self) -> Result<[u8; 32]> {
        base64::engine::general_purpose::STANDARD
            .decode(&self.0)
            .with_context(|| "WireGuard public key must be base64")?
            .try_into()
            .map_err(|bytes: Vec<u8>| {
                anyhow!(
                    "WireGuard public key must decode to 32 bytes, got {}",
                    bytes.len()
                )
            })
    }
}

impl fmt::Display for WireGuardPublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct WireGuardPrivateKey([u8; 32]);

impl WireGuardPrivateKey {
    #[cfg(test)]
    pub fn parse_base64(raw: &str) -> Result<Self> {
        decode_private_key(raw)
    }

    pub fn generate() -> Self {
        let secret = x25519_dalek::StaticSecret::random_from_rng(rand_core::OsRng);
        Self(secret.to_bytes())
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    fn to_base64(&self) -> String {
        base64::engine::general_purpose::STANDARD.encode(self.0)
    }
}

impl fmt::Debug for WireGuardPrivateKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WireGuardPrivateKey(REDACTED)")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
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
    ) -> Result<Self> {
        if node_name.trim().is_empty() {
            return Err(anyhow!("dataplane peer node_name is required"));
        }
        let endpoint = endpoint
            .as_deref()
            .ok_or_else(|| anyhow!("dataplane peer endpoint is required"))?
            .parse::<IpAddr>()
            .with_context(|| "dataplane peer endpoint must be an IP address")?;
        let public_key = match encryption {
            DataplaneEncryption::Enabled => {
                let raw = public_key.as_deref().ok_or_else(|| {
                    anyhow!("WireGuard public key is required when encryption is enabled")
                })?;
                let port = port.ok_or_else(|| {
                    anyhow!("WireGuard listen port is required when encryption is enabled")
                })?;
                if port == 0 {
                    return Err(anyhow!("WireGuard listen port must be non-zero"));
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WireGuardPeerPlan {
    pub node_name: String,
    pub mode: DataplaneMode,
    pub public_key: WireGuardPublicKey,
    pub endpoint: SocketAddr,
    pub allowed_pod_cidr: String,
}

impl WireGuardPeerPlan {
    pub fn try_new(metadata: DataplanePeerMetadata, peer_pod_cidr: &str) -> Result<Self> {
        if metadata.encryption != DataplaneEncryption::Enabled {
            return Err(anyhow!(
                "WireGuard peer plan requires dataplane encryption enabled for {}",
                metadata.node_name
            ));
        }
        let public_key = metadata.public_key.ok_or_else(|| {
            anyhow!(
                "WireGuard peer plan requires a public key for {}",
                metadata.node_name
            )
        })?;
        let port = metadata.port.ok_or_else(|| {
            anyhow!(
                "WireGuard peer plan requires a listen port for {}",
                metadata.node_name
            )
        })?;
        let allowed_pod_cidr = crate::networking::PodSubnet::parse(peer_pod_cidr)
            .map_err(|err| anyhow!("invalid WireGuard peer pod CIDR '{peer_pod_cidr}': {err}"))?
            .to_string();

        Ok(Self {
            node_name: metadata.node_name,
            mode: metadata.mode,
            public_key,
            endpoint: SocketAddr::new(metadata.endpoint, port),
            allowed_pod_cidr,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnencryptedPeerPlan {
    pub node_name: String,
    pub mode: DataplaneMode,
    pub endpoint: IpAddr,
    pub allowed_pod_cidr: String,
}

impl UnencryptedPeerPlan {
    pub fn try_new(metadata: DataplanePeerMetadata, peer_pod_cidr: &str) -> Result<Self> {
        if metadata.encryption != DataplaneEncryption::Disabled {
            return Err(anyhow!(
                "unencrypted peer plan requires dataplane encryption disabled for {}",
                metadata.node_name
            ));
        }
        let allowed_pod_cidr = crate::networking::PodSubnet::parse(peer_pod_cidr)
            .map_err(|err| anyhow!("invalid unencrypted peer pod CIDR '{peer_pod_cidr}': {err}"))?
            .to_string();

        Ok(Self {
            node_name: metadata.node_name,
            mode: metadata.mode,
            endpoint: metadata.endpoint,
            allowed_pod_cidr,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WireGuardIdentity {
    private_key: WireGuardPrivateKey,
    public_key: WireGuardPublicKey,
}

impl WireGuardIdentity {
    pub async fn load_or_create(path: &Path, supervisor: &TaskSupervisor) -> Result<Self> {
        let path = path.to_path_buf();
        supervisor
            .run_blocking_file_keyed(
                "wireguard_identity_load_or_create",
                path_key(&path),
                move || load_or_create_sync(path),
            )
            .await?
    }

    pub fn public_key(&self) -> &WireGuardPublicKey {
        &self.public_key
    }

    pub fn private_key(&self) -> &WireGuardPrivateKey {
        &self.private_key
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WireGuardDeviceConfig {
    pub device: String,
    pub private_key: WireGuardPrivateKey,
    pub listen_port: u16,
}

impl WireGuardDeviceConfig {
    pub fn try_new(
        device: String,
        private_key: WireGuardPrivateKey,
        listen_port: u16,
    ) -> Result<Self> {
        let device = parse_wireguard_device_name(&device)?;
        if listen_port == 0 {
            return Err(anyhow!("WireGuard listen port must be non-zero"));
        }
        Ok(Self {
            device,
            private_key,
            listen_port,
        })
    }
}

pub fn parse_wireguard_device_name(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("WireGuard device name must not be empty"));
    }
    if trimmed.len() > IFNAMSIZ {
        return Err(anyhow!(
            "WireGuard device name '{}' exceeds Linux IFNAMSIZ (15 chars)",
            trimmed
        ));
    }
    if trimmed.contains('/') || trimmed.chars().any(char::is_whitespace) {
        return Err(anyhow!(
            "WireGuard device name '{}' must not contain '/' or whitespace",
            trimmed
        ));
    }
    Ok(trimmed.to_string())
}

pub struct WireGuardController {
    device: String,
    handle: AsyncMutex<genetlink::GenetlinkHandle>,
    _conn: SupervisedJoinHandle<()>,
}

impl WireGuardController {
    pub async fn open(
        config: WireGuardDeviceConfig,
        supervisor: &TaskSupervisor,
        cancel: CancellationToken,
    ) -> Result<Self> {
        let (conn, handle, _) =
            genetlink::new_connection().context("failed to open WireGuard generic netlink")?;
        let conn = supervisor
            .spawn_async(
                TaskCategory::Network,
                "wireguard_genetlink_connection",
                async move {
                    tokio::select! {
                        _ = conn => {}
                        _ = cancel.cancelled() => {}
                    }
                },
            )
            .await
            .context("failed to spawn WireGuard generic netlink connection task")?;
        let controller = Self {
            device: config.device.clone(),
            handle: AsyncMutex::new(handle),
            _conn: conn,
        };
        controller
            .set_device_with_retry(&config, supervisor)
            .await
            .with_context(|| format!("configure WireGuard device {}", config.device))?;
        Ok(controller)
    }

    pub async fn apply_peer(&self, plan: &WireGuardPeerPlan) -> Result<()> {
        let message = build_set_peer_message(&self.device, plan)?;
        self.send(message)
            .await
            .with_context(|| format!("apply WireGuard peer {}", plan.node_name))
    }

    pub async fn remove_peer(&self, public_key: &WireGuardPublicKey) -> Result<()> {
        let message = build_remove_peer_message(&self.device, public_key)?;
        self.send(message)
            .await
            .with_context(|| format!("remove WireGuard peer {}", public_key))
    }

    async fn set_device(&self, config: &WireGuardDeviceConfig) -> Result<()> {
        self.send(build_set_device_message(config)).await
    }

    async fn set_device_with_retry(
        &self,
        config: &WireGuardDeviceConfig,
        supervisor: &TaskSupervisor,
    ) -> Result<()> {
        let retry_delays = wireguard_device_config_retry_delays();
        for attempt in 0..=retry_delays.len() {
            match self.set_device(config).await {
                Ok(()) => return Ok(()),
                Err(err) if attempt < retry_delays.len() => {
                    let delay = retry_delays[attempt];
                    tracing::warn!(
                        device = %config.device,
                        attempt = attempt + 1,
                        retry_delay_ms = delay.as_millis(),
                        error = %err,
                        "WireGuard device configure failed; retrying"
                    );
                    supervisor
                        .sleep("wireguard_device_config_retry", delay)
                        .await
                        .context("WireGuard device configure retry timer failed")?;
                }
                Err(err) => return Err(err),
            }
        }
        unreachable!("bounded WireGuard configure retry loop always returns")
    }

    async fn send(&self, message: WireGuardNetlinkMessage) -> Result<()> {
        let mut handle = self.handle.lock().await;
        let mut responses = handle
            .request(message)
            .await
            .context("WireGuard generic netlink request failed")?;
        while let Some(response) = responses.next().await {
            let response = response.context("decode WireGuard generic netlink response")?;
            if let NetlinkPayload::Error(err) = response.payload
                && err.code.is_some()
            {
                bail!("WireGuard generic netlink error: {}", err.to_io());
            }
        }
        Ok(())
    }
}

pub fn build_set_device_message(config: &WireGuardDeviceConfig) -> WireGuardNetlinkMessage {
    wireguard_request(vec![
        WireguardAttribute::IfName(config.device.clone()),
        WireguardAttribute::PrivateKey(*config.private_key.as_bytes()),
        WireguardAttribute::ListenPort(config.listen_port),
        WireguardAttribute::Fwmark(0),
    ])
}

pub fn build_set_peer_message(
    device: &str,
    plan: &WireGuardPeerPlan,
) -> Result<WireGuardNetlinkMessage> {
    let subnet = crate::networking::PodSubnet::parse(&plan.allowed_pod_cidr).map_err(|err| {
        anyhow!(
            "invalid WireGuard peer pod CIDR '{}': {err}",
            plan.allowed_pod_cidr
        )
    })?;
    Ok(wireguard_request(vec![
        WireguardAttribute::IfName(parse_wireguard_device_name(device)?),
        WireguardAttribute::Peers(vec![WireguardPeer(vec![
            WireguardPeerAttribute::PublicKey(plan.public_key.to_bytes()?),
            WireguardPeerAttribute::Endpoint(plan.endpoint),
            WireguardPeerAttribute::PersistentKeepalive(WIREGUARD_PERSISTENT_KEEPALIVE_SECS),
            WireguardPeerAttribute::Flags(WGPEER_F_REPLACE_ALLOWEDIPS),
            WireguardPeerAttribute::AllowedIps(vec![WireguardAllowedIp(vec![
                WireguardAllowedIpAttr::Family(WireguardAddressFamily::Ipv4),
                WireguardAllowedIpAttr::IpAddr(IpAddr::V4(subnet.base_ip())),
                WireguardAllowedIpAttr::Cidr(subnet.prefix()),
            ])]),
        ])]),
    ]))
}

pub fn build_remove_peer_message(
    device: &str,
    public_key: &WireGuardPublicKey,
) -> Result<WireGuardNetlinkMessage> {
    Ok(wireguard_request(vec![
        WireguardAttribute::IfName(parse_wireguard_device_name(device)?),
        WireguardAttribute::Peers(vec![WireguardPeer(vec![
            WireguardPeerAttribute::PublicKey(public_key.to_bytes()?),
            WireguardPeerAttribute::Flags(WGPEER_F_REMOVE_ME),
        ])]),
    ]))
}

pub async fn apply_wireguard_pod_route(
    handle: &rtnetlink::Handle,
    wireguard_idx: u32,
    plan: &WireGuardPeerPlan,
    preferred_source: Ipv4Addr,
) -> Result<()> {
    let subnet = parse_plan_subnet(&plan.allowed_pod_cidr)?;
    handle
        .route()
        .add()
        .v4()
        .destination_prefix(subnet.base_ip(), subnet.prefix())
        .output_interface(wireguard_idx)
        .pref_source(preferred_source)
        .replace()
        .execute()
        .await
        .with_context(|| {
            format!(
                "failed to install WireGuard route for peer {} subnet {}",
                plan.node_name, plan.allowed_pod_cidr
            )
        })
}

pub async fn remove_wireguard_pod_route(
    handle: &rtnetlink::Handle,
    wireguard_idx: u32,
    plan: &WireGuardPeerPlan,
    preferred_source: Ipv4Addr,
) -> Result<()> {
    let subnet = parse_plan_subnet(&plan.allowed_pod_cidr)?;
    let message = wireguard_route_message(wireguard_idx, subnet, preferred_source);
    if let Err(err) = handle.route().del(message).execute().await {
        tracing::warn!(
            peer = %plan.node_name,
            subnet = %plan.allowed_pod_cidr,
            error = %err,
            "failed to remove WireGuard pod route"
        );
    }
    Ok(())
}

pub async fn apply_unencrypted_direct_route(
    handle: &rtnetlink::Handle,
    plan: &UnencryptedPeerPlan,
) -> Result<()> {
    let subnet = parse_plan_subnet(&plan.allowed_pod_cidr)?;
    let IpAddr::V4(gateway) = plan.endpoint else {
        bail!(
            "unencrypted direct route for {} requires IPv4 endpoint, got {}",
            plan.node_name,
            plan.endpoint
        );
    };
    handle
        .route()
        .add()
        .v4()
        .destination_prefix(subnet.base_ip(), subnet.prefix())
        .gateway(gateway)
        .replace()
        .execute()
        .await
        .with_context(|| {
            format!(
                "failed to install unencrypted direct route for peer {} subnet {} via {}",
                plan.node_name, plan.allowed_pod_cidr, gateway
            )
        })
}

pub async fn remove_unencrypted_direct_route(
    handle: &rtnetlink::Handle,
    plan: &UnencryptedPeerPlan,
) -> Result<()> {
    let subnet = parse_plan_subnet(&plan.allowed_pod_cidr)?;
    let IpAddr::V4(gateway) = plan.endpoint else {
        return Ok(());
    };
    let message = unencrypted_direct_route_message(subnet, gateway);
    if let Err(err) = handle.route().del(message).execute().await {
        tracing::warn!(
            peer = %plan.node_name,
            subnet = %plan.allowed_pod_cidr,
            gateway = %gateway,
            error = %err,
            "failed to remove unencrypted direct pod route"
        );
    }
    Ok(())
}

fn parse_plan_subnet(cidr: &str) -> Result<crate::networking::PodSubnet> {
    crate::networking::PodSubnet::parse(cidr)
        .map_err(|err| anyhow!("invalid peer pod CIDR '{cidr}': {err}"))
}

fn wireguard_route_message(
    wireguard_idx: u32,
    subnet: crate::networking::PodSubnet,
    preferred_source: Ipv4Addr,
) -> netlink_packet_route::route::RouteMessage {
    use netlink_packet_route::route::{
        RouteAddress, RouteAttribute, RouteMessage, RouteProtocol, RouteScope, RouteType,
    };

    let mut route_msg = RouteMessage::default();
    route_msg.header.address_family = netlink_packet_route::AddressFamily::Inet;
    route_msg.header.destination_prefix_length = subnet.prefix();
    route_msg.header.protocol = RouteProtocol::Static;
    route_msg.header.scope = RouteScope::Universe;
    route_msg.header.kind = RouteType::Unicast;
    route_msg
        .attributes
        .push(RouteAttribute::Destination(RouteAddress::Inet(
            subnet.base_ip(),
        )));
    route_msg
        .attributes
        .push(RouteAttribute::Oif(wireguard_idx));
    route_msg
        .attributes
        .push(RouteAttribute::PrefSource(RouteAddress::Inet(
            preferred_source,
        )));
    route_msg
}

fn unencrypted_direct_route_message(
    subnet: crate::networking::PodSubnet,
    gateway: std::net::Ipv4Addr,
) -> netlink_packet_route::route::RouteMessage {
    use netlink_packet_route::route::{
        RouteAddress, RouteAttribute, RouteMessage, RouteProtocol, RouteScope, RouteType,
    };

    let mut route_msg = RouteMessage::default();
    route_msg.header.address_family = netlink_packet_route::AddressFamily::Inet;
    route_msg.header.destination_prefix_length = subnet.prefix();
    route_msg.header.protocol = RouteProtocol::Static;
    route_msg.header.scope = RouteScope::Universe;
    route_msg.header.kind = RouteType::Unicast;
    route_msg
        .attributes
        .push(RouteAttribute::Destination(RouteAddress::Inet(
            subnet.base_ip(),
        )));
    route_msg
        .attributes
        .push(RouteAttribute::Gateway(RouteAddress::Inet(gateway)));
    route_msg
}

fn wireguard_request(attributes: Vec<WireguardAttribute>) -> WireGuardNetlinkMessage {
    let genlmsg: GenlMessage<WireguardMessage> = GenlMessage::from_payload(WireguardMessage {
        cmd: WireguardCmd::SetDevice,
        attributes,
    });
    let mut nlmsg = NetlinkMessage::from(genlmsg);
    nlmsg.header.flags = NLM_F_REQUEST | NLM_F_ACK;
    nlmsg
}

#[cfg(test)]
fn wireguard_message_attrs(message: &WireGuardNetlinkMessage) -> &[WireguardAttribute] {
    match &message.payload {
        NetlinkPayload::InnerMessage(genlmsg) => &genlmsg.payload.attributes,
        _ => &[],
    }
}

fn path_key(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn load_or_create_sync(path: PathBuf) -> Result<WireGuardIdentity> {
    use std::fs as blocking_fs;
    use std::io::{Read, Write};
    use std::os::unix::prelude::{OpenOptionsExt, PermissionsExt};

    if let Some(parent) = path.parent() {
        blocking_fs::create_dir_all(parent)
            .with_context(|| format!("create WireGuard key directory {}", parent.display()))?;
    }

    let mut file = blocking_fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(&path)
        .with_context(|| format!("open WireGuard private key {}", path.display()))?;

    file.set_permissions(blocking_fs::Permissions::from_mode(0o600))
        .with_context(|| format!("chmod 0600 {}", path.display()))?;

    let mut existing = String::new();
    file.read_to_string(&mut existing)
        .with_context(|| format!("read WireGuard private key {}", path.display()))?;
    let private_key = if existing.trim().is_empty() {
        let key = generate_private_key();
        let encoded = key.to_base64();
        file.set_len(0)
            .with_context(|| format!("truncate WireGuard private key {}", path.display()))?;
        file.write_all(encoded.as_bytes())
            .with_context(|| format!("write WireGuard private key {}", path.display()))?;
        file.write_all(b"\n")
            .with_context(|| format!("finish WireGuard private key {}", path.display()))?;
        key
    } else {
        decode_private_key(existing.trim())?
    };

    Ok(WireGuardIdentity {
        public_key: public_key_from_private(&private_key)?,
        private_key,
    })
}

fn generate_private_key() -> WireGuardPrivateKey {
    WireGuardPrivateKey::generate()
}

fn decode_private_key(encoded: &str) -> Result<WireGuardPrivateKey> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .with_context(|| "WireGuard private key must be base64")?;
    bytes
        .try_into()
        .map(WireGuardPrivateKey)
        .map_err(|bytes: Vec<u8>| {
            anyhow!(
                "WireGuard private key must decode to 32 bytes, got {}",
                bytes.len()
            )
        })
}

fn public_key_from_private(private_key: &WireGuardPrivateKey) -> Result<WireGuardPublicKey> {
    let secret = x25519_dalek::StaticSecret::from(*private_key.as_bytes());
    let public = x25519_dalek::PublicKey::from(&secret);
    WireGuardPublicKey::parse(&base64::engine::general_purpose::STANDARD.encode(public.as_bytes()))
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::{
        WGPEER_F_REMOVE_ME, WGPEER_F_REPLACE_ALLOWEDIPS, WireGuardDeviceConfig,
        WireGuardPrivateKey, build_remove_peer_message, build_set_device_message,
        build_set_peer_message, unencrypted_direct_route_message,
        wireguard_device_config_retry_delays, wireguard_message_attrs, wireguard_route_message,
    };
    use crate::networking::wireguard::{
        DataplaneEncryption, DataplaneMode, DataplanePeerMetadata, WireGuardIdentity,
        WireGuardPeerPlan, WireGuardPublicKey,
    };
    use crate::task_supervisor::{TaskCategoryConfig, TaskSupervisor};

    #[test]
    fn wireguard_mtu_is_safe_for_public_internet_overlay() {
        assert_eq!(
            super::WIREGUARD_MTU,
            1280,
            "WireGuard carries pod traffic across public/NATed paths; keep the tunnel MTU at the IPv6 minimum so webhook-sized TLS POSTs do not blackhole"
        );
    }

    #[test]
    fn default_wireguard_port_matches_release_dataplane_port() {
        assert_eq!(
            super::DEFAULT_WIREGUARD_PORT,
            7_679,
            "default multinode WireGuard dataplane traffic must listen on UDP 7679"
        );
    }

    #[test]
    fn dataplane_encryption_defaults_to_enabled_and_preserves_disabled() {
        assert_eq!(
            DataplaneEncryption::parse(None).unwrap(),
            DataplaneEncryption::Enabled
        );
        assert_eq!(
            DataplaneEncryption::parse(Some("")).unwrap(),
            DataplaneEncryption::Enabled
        );
        assert_eq!(
            DataplaneEncryption::parse(Some("disabled")).unwrap(),
            DataplaneEncryption::Disabled
        );
        assert!(DataplaneEncryption::parse(Some("plaintext")).is_err());
    }

    #[test]
    fn peer_metadata_validates_key_endpoint_port_and_mode() {
        let public_key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
        assert!(WireGuardPublicKey::parse(public_key).is_ok());
        assert!(WireGuardPublicKey::parse("not-a-wireguard-key").is_err());
        assert!(DataplaneMode::parse("root").is_ok());
        assert!(DataplaneMode::parse("rootless").is_ok());
        assert!(DataplaneMode::parse("vxlan").is_err());

        let enabled = DataplanePeerMetadata::try_new(
            "worker-1".to_string(),
            DataplaneMode::Root,
            DataplaneEncryption::Enabled,
            Some(public_key.to_string()),
            Some("192.0.2.10".to_string()),
            Some(51_820),
        )
        .unwrap();
        assert_eq!(enabled.encryption, DataplaneEncryption::Enabled);
        assert!(enabled.public_key.is_some());

        assert!(
            DataplanePeerMetadata::try_new(
                "worker-1".to_string(),
                DataplaneMode::Rootless,
                DataplaneEncryption::Enabled,
                None,
                Some("192.0.2.10".to_string()),
                Some(51_820),
            )
            .is_err()
        );
        assert!(
            DataplanePeerMetadata::try_new(
                "worker-1".to_string(),
                DataplaneMode::Root,
                DataplaneEncryption::Enabled,
                Some(public_key.to_string()),
                None,
                Some(51_820),
            )
            .is_err()
        );
        assert!(
            DataplanePeerMetadata::try_new(
                "worker-1".to_string(),
                DataplaneMode::Root,
                DataplaneEncryption::Enabled,
                Some(public_key.to_string()),
                Some("192.0.2.10".to_string()),
                Some(0),
            )
            .is_err()
        );

        let disabled = DataplanePeerMetadata::try_new(
            "worker-1".to_string(),
            DataplaneMode::Rootless,
            DataplaneEncryption::Disabled,
            None,
            Some("192.0.2.10".to_string()),
            None,
        )
        .unwrap();
        assert_eq!(disabled.encryption, DataplaneEncryption::Disabled);
        assert!(disabled.public_key.is_none());
    }

    #[tokio::test]
    async fn wireguard_identity_persists_private_key_0600_and_reuses_public_key() {
        let dir = tempfile::tempdir().unwrap();
        let key_path = dir.path().join("wg_private_key");
        let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());

        let first = WireGuardIdentity::load_or_create(&key_path, &supervisor)
            .await
            .unwrap();
        let second = WireGuardIdentity::load_or_create(&key_path, &supervisor)
            .await
            .unwrap();

        assert_eq!(first.public_key(), second.public_key());
        let metadata = std::fs::metadata(&key_path).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);

        let private_key = std::fs::read_to_string(&key_path).unwrap();
        assert_ne!(private_key.trim(), first.public_key().as_str());
    }

    #[tokio::test]
    async fn wireguard_identity_exposes_private_key_without_leaking_debug() {
        let dir = tempfile::tempdir().unwrap();
        let key_path = dir.path().join("wg_private_key");
        let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());

        let identity = WireGuardIdentity::load_or_create(&key_path, &supervisor)
            .await
            .unwrap();
        let encoded = std::fs::read_to_string(&key_path).unwrap();
        let debug = format!("{identity:?}");

        assert_eq!(identity.private_key().as_bytes().len(), 32);
        assert!(
            !debug.contains(encoded.trim()),
            "private key material must not appear in Debug output: {debug}"
        );
        assert!(
            debug.contains("REDACTED"),
            "Debug output must make redaction explicit: {debug}"
        );
    }

    #[test]
    fn wireguard_peer_plan_requires_enabled_encryption_and_uses_peer_pod_cidr() {
        let metadata = DataplanePeerMetadata::try_new(
            "worker-1".to_string(),
            DataplaneMode::Rootless,
            DataplaneEncryption::Enabled,
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
            Some("192.0.2.10".to_string()),
            Some(51_820),
        )
        .unwrap();

        let plan = WireGuardPeerPlan::try_new(metadata, "10.42.7.0/24").unwrap();

        assert_eq!(plan.node_name, "worker-1");
        assert_eq!(plan.allowed_pod_cidr, "10.42.7.0/24");
        assert_eq!(plan.endpoint.to_string(), "192.0.2.10:51820");
        assert_eq!(plan.mode, DataplaneMode::Rootless);
    }

    #[test]
    fn wireguard_peer_plan_rejects_disabled_encryption_and_malformed_cidr() {
        let disabled = DataplanePeerMetadata::try_new(
            "worker-1".to_string(),
            DataplaneMode::Root,
            DataplaneEncryption::Disabled,
            None,
            Some("192.0.2.10".to_string()),
            None,
        )
        .unwrap();
        assert!(WireGuardPeerPlan::try_new(disabled, "10.42.7.0/24").is_err());

        let enabled = DataplanePeerMetadata::try_new(
            "worker-2".to_string(),
            DataplaneMode::Root,
            DataplaneEncryption::Enabled,
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
            Some("192.0.2.11".to_string()),
            Some(51_820),
        )
        .unwrap();
        assert!(WireGuardPeerPlan::try_new(enabled, "not-a-cidr").is_err());
    }

    #[test]
    fn wireguard_device_config_builds_set_device_netlink_payload() {
        use netlink_packet_wireguard::WireguardAttribute;

        let private_key =
            WireGuardPrivateKey::parse_base64("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
                .unwrap();
        let config =
            WireGuardDeviceConfig::try_new("klights.wg".to_string(), private_key, 51_820).unwrap();
        let message = build_set_device_message(&config);

        let attrs = wireguard_message_attrs(&message);
        assert!(
            attrs.iter().any(
                |attr| matches!(attr, WireguardAttribute::IfName(name) if name == "klights.wg")
            )
        );
        assert!(attrs.iter().any(
            |attr| matches!(attr, WireguardAttribute::PrivateKey(bytes) if *bytes == [0u8; 32])
        ));
        assert!(
            attrs
                .iter()
                .any(|attr| matches!(attr, WireguardAttribute::ListenPort(51_820)))
        );
    }

    #[test]
    fn wireguard_device_config_retry_delays_are_bounded_for_startup_races() {
        let delays = wireguard_device_config_retry_delays();

        assert_eq!(
            delays.len(),
            4,
            "WireGuard startup races must be retried before marking a rootless node NotReady"
        );
        assert!(
            delays.iter().all(|delay| !delay.is_zero()),
            "retry delays must use real supervised timer waits"
        );
        assert!(
            delays.iter().sum::<std::time::Duration>() <= std::time::Duration::from_secs(1),
            "startup retry budget must stay bounded"
        );
    }

    #[test]
    fn wireguard_peer_config_replaces_allowed_ips_for_peer_pod_cidr() {
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        use netlink_packet_wireguard::{
            WireguardAddressFamily, WireguardAllowedIpAttr, WireguardAttribute,
            WireguardPeerAttribute,
        };

        let metadata = DataplanePeerMetadata::try_new(
            "node-b".to_string(),
            DataplaneMode::Root,
            DataplaneEncryption::Enabled,
            Some("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=".to_string()),
            Some("192.0.2.10".to_string()),
            Some(51_821),
        )
        .unwrap();
        let plan = WireGuardPeerPlan::try_new(metadata, "10.42.7.0/24").unwrap();
        let message = build_set_peer_message("klights.wg", &plan).unwrap();
        let attrs = wireguard_message_attrs(&message);
        let peer_attrs = attrs
            .iter()
            .find_map(|attr| match attr {
                WireguardAttribute::Peers(peers) => peers.first().map(|peer| peer.0.as_slice()),
                _ => None,
            })
            .expect("peer attributes must be present");

        assert!(peer_attrs.iter().any(|attr| matches!(
            attr,
            WireguardPeerAttribute::Endpoint(endpoint)
                if *endpoint == SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)), 51_821)
        )));
        assert!(peer_attrs.iter().any(|attr| matches!(
            attr,
            WireguardPeerAttribute::Flags(flags)
                if flags & WGPEER_F_REPLACE_ALLOWEDIPS == WGPEER_F_REPLACE_ALLOWEDIPS
        )));
        assert!(peer_attrs.iter().any(|attr| matches!(
            attr,
            WireguardPeerAttribute::PersistentKeepalive(value)
                if *value == super::WIREGUARD_PERSISTENT_KEEPALIVE_SECS
        )));
        let allowed = peer_attrs
            .iter()
            .find_map(|attr| match attr {
                WireguardPeerAttribute::AllowedIps(allowed) => allowed.first(),
                _ => None,
            })
            .expect("allowed IP must be present");
        assert!(allowed.0.iter().any(|attr| {
            matches!(
                attr,
                WireguardAllowedIpAttr::Family(WireguardAddressFamily::Ipv4)
            )
        }));
        assert!(allowed.0.iter().any(|attr| matches!(
            attr,
            WireguardAllowedIpAttr::IpAddr(IpAddr::V4(addr))
                if *addr == Ipv4Addr::new(10, 42, 7, 0)
        )));
        assert!(
            allowed
                .0
                .iter()
                .any(|attr| matches!(attr, WireguardAllowedIpAttr::Cidr(24)))
        );
    }

    #[test]
    fn wireguard_remove_peer_marks_peer_for_kernel_removal() {
        use netlink_packet_wireguard::{WireguardAttribute, WireguardPeerAttribute};

        let public_key =
            WireGuardPublicKey::parse("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=").unwrap();
        let message = build_remove_peer_message("klights.wg", &public_key).unwrap();
        let attrs = wireguard_message_attrs(&message);
        let peer_attrs = attrs
            .iter()
            .find_map(|attr| match attr {
                WireguardAttribute::Peers(peers) => peers.first().map(|peer| peer.0.as_slice()),
                _ => None,
            })
            .expect("peer attributes must be present");

        assert!(peer_attrs.iter().any(|attr| matches!(
            attr,
            WireguardPeerAttribute::Flags(flags)
                if flags & WGPEER_F_REMOVE_ME == WGPEER_F_REMOVE_ME
        )));
    }

    #[test]
    fn wireguard_route_message_routes_peer_cidr_over_wireguard_oif() {
        use std::net::Ipv4Addr;

        use netlink_packet_route::route::{RouteAddress, RouteAttribute};

        let subnet = crate::networking::PodSubnet::parse("10.42.7.0/24").unwrap();
        let message = wireguard_route_message(77, subnet, Ipv4Addr::new(10, 42, 0, 1));

        assert_eq!(message.header.destination_prefix_length, 24);
        assert!(message.attributes.iter().any(|attr| matches!(
            attr,
            RouteAttribute::Destination(RouteAddress::Inet(addr))
                if *addr == Ipv4Addr::new(10, 42, 7, 0)
        )));
        assert!(
            message
                .attributes
                .iter()
                .any(|attr| matches!(attr, RouteAttribute::Oif(77)))
        );
        assert!(
            !message
                .attributes
                .iter()
                .any(|attr| matches!(attr, RouteAttribute::Gateway(_))),
            "WireGuard route must use the WG output interface, not a plaintext gateway"
        );
        assert!(message.attributes.iter().any(|attr| matches!(
            attr,
            RouteAttribute::PrefSource(RouteAddress::Inet(addr))
                if *addr == Ipv4Addr::new(10, 42, 0, 1)
        )));
    }

    #[test]
    fn unencrypted_route_message_is_explicit_plaintext_gateway_route() {
        use std::net::Ipv4Addr;

        use netlink_packet_route::route::{RouteAddress, RouteAttribute};

        let subnet = crate::networking::PodSubnet::parse("10.42.8.0/24").unwrap();
        let message = unencrypted_direct_route_message(subnet, Ipv4Addr::new(192, 0, 2, 44));

        assert_eq!(message.header.destination_prefix_length, 24);
        assert!(message.attributes.iter().any(|attr| matches!(
            attr,
            RouteAttribute::Destination(RouteAddress::Inet(addr))
                if *addr == Ipv4Addr::new(10, 42, 8, 0)
        )));
        assert!(message.attributes.iter().any(|attr| matches!(
            attr,
            RouteAttribute::Gateway(RouteAddress::Inet(addr))
                if *addr == Ipv4Addr::new(192, 0, 2, 44)
        )));
        assert!(
            !message
                .attributes
                .iter()
                .any(|attr| matches!(attr, RouteAttribute::Oif(_))),
            "explicit plaintext route must not masquerade as a WireGuard or VXLAN device route"
        );
    }
}
