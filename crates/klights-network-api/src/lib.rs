//! Transport-neutral runtime networking contracts for klights.

#[cfg(feature = "conformance")]
#[doc(hidden)]
pub mod conformance;

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};

use klights_types::PodIdentity;

/// Object-safe future used by one complete datapath operation.
///
/// Datapath calls are coarse sandbox lifecycle operations, so dynamic dispatch
/// and one boxed future do not add a per-packet or per-resource hot-loop
/// allocation.
pub type DatapathFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, DatapathError>> + Send + 'a>>;

fn require_nonempty(value: &str, field: &'static str) -> Result<(), DatapathError> {
    if value.trim().is_empty() {
        Err(DatapathError::invalid(field, "must not be empty"))
    } else {
        Ok(())
    }
}

/// Validated container-runtime sandbox identity used by CNI ADD and DEL.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SandboxId(String);

impl SandboxId {
    pub fn try_new(value: impl Into<String>) -> Result<Self, DatapathError> {
        let value = value.into();
        require_nonempty(&value, "datapath.sandbox_id")?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SandboxId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Validated namespace path used either for `setns` or durable allocation
/// bookkeeping.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct NetworkNamespacePath(String);

impl NetworkNamespacePath {
    fn try_new(value: impl Into<String>, field: &'static str) -> Result<Self, DatapathError> {
        let value = value.into();
        require_nonempty(&value, field)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Validated runtime request for attaching one sandbox to the pod datapath.
///
/// Persistence allocation and endpoint DTOs remain owned by
/// `klights-node-store`; this value carries only the runtime facts needed by a
/// datapath implementation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CniAddRequest {
    sandbox_id: SandboxId,
    pod: PodIdentity,
    netns_setns_path: NetworkNamespacePath,
    netns_record_path: NetworkNamespacePath,
    host_network: bool,
}

impl CniAddRequest {
    pub fn try_new(
        sandbox_id: impl Into<String>,
        pod: PodIdentity,
        netns_setns_path: impl Into<String>,
        netns_record_path: impl Into<String>,
        host_network: bool,
    ) -> Result<Self, DatapathError> {
        require_nonempty(&pod.namespace, "datapath.pod.namespace")?;
        require_nonempty(&pod.name, "datapath.pod.name")?;
        require_nonempty(&pod.uid, "datapath.pod.uid")?;

        Ok(Self {
            sandbox_id: SandboxId::try_new(sandbox_id)?,
            pod,
            netns_setns_path: NetworkNamespacePath::try_new(
                netns_setns_path,
                "datapath.netns_setns_path",
            )?,
            netns_record_path: NetworkNamespacePath::try_new(
                netns_record_path,
                "datapath.netns_record_path",
            )?,
            host_network,
        })
    }

    pub fn sandbox_id(&self) -> &SandboxId {
        &self.sandbox_id
    }

    pub fn pod(&self) -> &PodIdentity {
        &self.pod
    }

    pub fn netns_setns_path(&self) -> &NetworkNamespacePath {
        &self.netns_setns_path
    }

    pub fn netns_record_path(&self) -> &NetworkNamespacePath {
        &self.netns_record_path
    }

    pub const fn host_network(&self) -> bool {
        self.host_network
    }

    pub fn into_parts(
        self,
    ) -> (
        SandboxId,
        PodIdentity,
        NetworkNamespacePath,
        NetworkNamespacePath,
        bool,
    ) {
        (
            self.sandbox_id,
            self.pod,
            self.netns_setns_path,
            self.netns_record_path,
            self.host_network,
        )
    }
}

/// Runtime result of a successful datapath attachment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PodNetwork {
    ip_addr: IpAddr,
}

impl PodNetwork {
    pub const fn new(ip_addr: IpAddr) -> Self {
        Self { ip_addr }
    }

    pub const fn ip_addr(self) -> IpAddr {
        self.ip_addr
    }
}

/// Typed failure categories for the runtime datapath boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DatapathError {
    InvalidRequest {
        field: &'static str,
        message: String,
    },
    Setup {
        message: String,
    },
    Teardown {
        message: String,
    },
    Address {
        message: String,
    },
    Shutdown {
        message: String,
    },
}

impl DatapathError {
    fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            field,
            message: message.into(),
        }
    }

    pub fn setup(message: impl Into<String>) -> Self {
        Self::Setup {
            message: message.into(),
        }
    }

    pub fn teardown(message: impl Into<String>) -> Self {
        Self::Teardown {
            message: message.into(),
        }
    }

    pub fn address(message: impl Into<String>) -> Self {
        Self::Address {
            message: message.into(),
        }
    }

    pub fn shutdown(message: impl Into<String>) -> Self {
        Self::Shutdown {
            message: message.into(),
        }
    }

    fn message(&self) -> &str {
        match self {
            Self::InvalidRequest { message, .. }
            | Self::Setup { message }
            | Self::Teardown { message }
            | Self::Address { message }
            | Self::Shutdown { message } => message,
        }
    }
}

impl fmt::Display for DatapathError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message())
    }
}

impl Error for DatapathError {}

/// Runtime pod-network setup, teardown, address, and shutdown capability.
///
/// Implementations live in the networking adapter. The contract deliberately
/// carries no persistence, Kubernetes object, wire, task-runtime, filesystem,
/// process, or concrete Linux-networking type.
pub trait Datapath: Send + Sync + 'static {
    fn cni_add(&self, request: CniAddRequest) -> DatapathFuture<'_, PodNetwork>;

    fn cni_del<'a>(&'a self, sandbox_id: &'a SandboxId) -> DatapathFuture<'a, ()>;

    fn host_ip(&self) -> DatapathFuture<'_, IpAddr>;

    fn pod_gateway_ip(&self) -> DatapathFuture<'_, IpAddr>;

    fn shutdown(&self) -> DatapathFuture<'_, ()>;
}

/// Validated IPv4 pod CIDR programmed as one peer's reachable prefix.
///
/// Peer routing currently owns IPv4 pod prefixes. Construction preserves the
/// existing `/1..=/30` range and normalizes host bits before the value crosses
/// into a concrete networking adapter.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PeerPodCidr {
    network: Ipv4Addr,
    prefix: u8,
}

impl PeerPodCidr {
    pub fn try_new(value: &str) -> Result<Self, PeerRouterError> {
        let (address, prefix) = value.split_once('/').ok_or_else(|| {
            PeerRouterError::invalid(
                "peer.pod_cidr",
                "must be an IPv4 CIDR in address/prefix form",
            )
        })?;
        if prefix.contains('/') {
            return Err(PeerRouterError::invalid(
                "peer.pod_cidr",
                "must contain exactly one prefix separator",
            ));
        }
        let address = address
            .parse::<Ipv4Addr>()
            .map_err(|_| PeerRouterError::invalid("peer.pod_cidr", "address must be IPv4"))?;
        let prefix = prefix
            .parse::<u8>()
            .map_err(|_| PeerRouterError::invalid("peer.pod_cidr", "prefix must be an integer"))?;
        if !(1..=30).contains(&prefix) {
            return Err(PeerRouterError::invalid(
                "peer.pod_cidr",
                "prefix must be in /1..=/30",
            ));
        }

        let mask = u32::MAX << (32 - u32::from(prefix));
        Ok(Self {
            network: Ipv4Addr::from(u32::from(address) & mask),
            prefix,
        })
    }

    pub const fn network(self) -> Ipv4Addr {
        self.network
    }

    pub const fn prefix(self) -> u8 {
        self.prefix
    }
}

impl fmt::Display for PeerPodCidr {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.network, self.prefix)
    }
}

/// WireGuard public-key bytes required to program one peer.
///
/// Base64 is a topology/wire representation and is decoded by the outer
/// adapter before constructing this transport-neutral runtime value.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct WireGuardPeerKey([u8; 32]);

impl WireGuardPeerKey {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Complete validated request for encrypted WireGuard pod-CIDR routing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WireGuardPeerRoute {
    node_name: String,
    public_key: WireGuardPeerKey,
    endpoint: SocketAddr,
    allowed_pod_cidr: PeerPodCidr,
}

impl WireGuardPeerRoute {
    pub fn try_new(
        node_name: impl Into<String>,
        public_key: WireGuardPeerKey,
        endpoint: SocketAddr,
        allowed_pod_cidr: &str,
    ) -> Result<Self, PeerRouterError> {
        let node_name = validate_peer_node_name(node_name)?;
        if endpoint.port() == 0 {
            return Err(PeerRouterError::invalid(
                "peer.wireguard.endpoint",
                "port must be non-zero",
            ));
        }

        Ok(Self {
            node_name,
            public_key,
            endpoint,
            allowed_pod_cidr: PeerPodCidr::try_new(allowed_pod_cidr)?,
        })
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub const fn public_key(&self) -> &WireGuardPeerKey {
        &self.public_key
    }

    pub const fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }

    pub const fn allowed_pod_cidr(&self) -> PeerPodCidr {
        self.allowed_pod_cidr
    }
}

/// Complete validated request for explicit unencrypted direct pod-CIDR
/// routing. Direct routing uses the peer's IPv4 node address as its gateway and
/// never creates an overlay device.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectPeerRoute {
    node_name: String,
    gateway: Ipv4Addr,
    allowed_pod_cidr: PeerPodCidr,
}

impl DirectPeerRoute {
    pub fn try_new(
        node_name: impl Into<String>,
        gateway: IpAddr,
        allowed_pod_cidr: &str,
    ) -> Result<Self, PeerRouterError> {
        let node_name = validate_peer_node_name(node_name)?;
        let IpAddr::V4(gateway) = gateway else {
            return Err(PeerRouterError::invalid(
                "peer.direct.gateway",
                "must be IPv4",
            ));
        };

        Ok(Self {
            node_name,
            gateway,
            allowed_pod_cidr: PeerPodCidr::try_new(allowed_pod_cidr)?,
        })
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub const fn gateway(&self) -> Ipv4Addr {
        self.gateway
    }

    pub const fn allowed_pod_cidr(&self) -> PeerPodCidr {
        self.allowed_pod_cidr
    }
}

fn validate_peer_node_name(value: impl Into<String>) -> Result<String, PeerRouterError> {
    let value = value.into();
    if value.trim().is_empty() {
        Err(PeerRouterError::invalid(
            "peer.node_name",
            "must not be empty",
        ))
    } else {
        Ok(value)
    }
}

/// Exact peer-route shape applied and later removed by a [`PeerRouter`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PeerRoute {
    WireGuard(WireGuardPeerRoute),
    Direct(DirectPeerRoute),
}

/// Typed failures for validation and coarse peer-route operations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PeerRouterError {
    InvalidRequest {
        field: &'static str,
        message: String,
    },
    Apply {
        message: String,
    },
    Remove {
        message: String,
    },
}

impl PeerRouterError {
    fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            field,
            message: message.into(),
        }
    }

    pub fn apply(message: impl Into<String>) -> Self {
        Self::Apply {
            message: message.into(),
        }
    }

    pub fn remove(message: impl Into<String>) -> Self {
        Self::Remove {
            message: message.into(),
        }
    }

    fn message(&self) -> &str {
        match self {
            Self::InvalidRequest { message, .. }
            | Self::Apply { message }
            | Self::Remove { message } => message,
        }
    }
}

impl fmt::Display for PeerRouterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message())
    }
}

impl Error for PeerRouterError {}

/// Object-safe future for one complete route-programming operation.
///
/// Peer route calls happen only when an event changes the desired peer state;
/// one boxed future is outside the packet path and is dominated by the
/// corresponding kernel networking operation.
pub type PeerRouterFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), PeerRouterError>> + Send + 'a>>;

/// Cross-node pod-CIDR route programming capability.
///
/// Implementations live in the networking adapter. The contract contains no
/// topology persistence DTO, Kubernetes object, generated wire, async runtime,
/// framework, filesystem/process, or concrete Linux networking type.
pub trait PeerRouter: Send + Sync + 'static {
    fn apply_peer_route<'a>(&'a self, route: &'a PeerRoute) -> PeerRouterFuture<'a>;

    fn remove_peer_route<'a>(&'a self, route: &'a PeerRoute) -> PeerRouterFuture<'a>;
}

/// L4 protocol for one pod hostPort mapping.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum HostPortProtocol {
    Tcp,
    Udp,
    Sctp,
}

/// Validated transport-neutral hostPort mapping for one container port.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostPortBinding {
    host_ip: Option<Ipv4Addr>,
    host_port: u16,
    container_port: u16,
    protocol: HostPortProtocol,
}

impl HostPortBinding {
    pub fn try_new(
        host_ip: Option<Ipv4Addr>,
        host_port: u16,
        container_port: u16,
        protocol: HostPortProtocol,
    ) -> Result<Self, ServiceRouterError> {
        if host_port == 0 {
            return Err(ServiceRouterError::invalid(
                "service.hostport.host_port",
                "must be non-zero",
            ));
        }
        if container_port == 0 {
            return Err(ServiceRouterError::invalid(
                "service.hostport.container_port",
                "must be non-zero",
            ));
        }
        Ok(Self {
            host_ip,
            host_port,
            container_port,
            protocol,
        })
    }

    pub const fn host_ip(&self) -> Option<Ipv4Addr> {
        self.host_ip
    }

    pub const fn host_port(&self) -> u16 {
        self.host_port
    }

    pub const fn container_port(&self) -> u16 {
        self.container_port
    }

    pub const fn protocol(&self) -> HostPortProtocol {
        self.protocol
    }
}

/// Validated hostPort facts adapted from one Pod resource at the kubelet
/// boundary.
///
/// The optional pod IP preserves lifecycle timing: admission happens before an
/// address exists, while add/remove routing runs after status publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodHostPorts {
    pod: PodIdentity,
    pod_ip: Option<Ipv4Addr>,
    bindings: Vec<HostPortBinding>,
}

impl PodHostPorts {
    pub fn try_new(
        pod: PodIdentity,
        pod_ip: Option<Ipv4Addr>,
        bindings: Vec<HostPortBinding>,
    ) -> Result<Self, ServiceRouterError> {
        for (field, value) in [
            ("service.hostport.pod.namespace", pod.namespace.as_str()),
            ("service.hostport.pod.name", pod.name.as_str()),
            ("service.hostport.pod.uid", pod.uid.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(ServiceRouterError::invalid(field, "must not be empty"));
            }
        }
        Ok(Self {
            pod,
            pod_ip,
            bindings,
        })
    }

    pub const fn pod(&self) -> &PodIdentity {
        &self.pod
    }

    pub const fn pod_ip(&self) -> Option<Ipv4Addr> {
        self.pod_ip
    }

    pub fn bindings(&self) -> &[HostPortBinding] {
        &self.bindings
    }
}

/// Complete hostPort rule set for one pod.
///
/// The owned vector is allocated once per pod lifecycle event, outside every
/// packet and service-reconcile hot path, and lets adapters hand rule ownership
/// to the concrete router without cloning each mapping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostPortRules {
    pod_ip: Ipv4Addr,
    bindings: Vec<HostPortBinding>,
}

impl HostPortRules {
    pub fn try_new(
        pod_ip: Ipv4Addr,
        bindings: Vec<HostPortBinding>,
    ) -> Result<Self, ServiceRouterError> {
        if bindings.is_empty() {
            return Err(ServiceRouterError::invalid(
                "service.hostport.bindings",
                "must contain at least one binding",
            ));
        }
        Ok(Self { pod_ip, bindings })
    }

    pub const fn pod_ip(&self) -> Ipv4Addr {
        self.pod_ip
    }

    pub fn bindings(&self) -> &[HostPortBinding] {
        &self.bindings
    }

    pub fn into_parts(self) -> (Ipv4Addr, Vec<HostPortBinding>) {
        (self.pod_ip, self.bindings)
    }
}

/// Idempotent request to remove every hostPort rule owned by one pod IP.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HostPortRemoval {
    pod_ip: Ipv4Addr,
}

impl HostPortRemoval {
    pub const fn new(pod_ip: Ipv4Addr) -> Self {
        Self { pod_ip }
    }

    pub const fn pod_ip(self) -> Ipv4Addr {
        self.pod_ip
    }
}

/// Typed failures for validation and coarse service-routing operations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServiceRouterError {
    InvalidRequest {
        field: &'static str,
        message: String,
    },
    Sync {
        message: String,
    },
    HostPort {
        message: String,
    },
    Cleanup {
        message: String,
    },
}

impl ServiceRouterError {
    fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            field,
            message: message.into(),
        }
    }

    pub fn sync(message: impl Into<String>) -> Self {
        Self::Sync {
            message: message.into(),
        }
    }

    pub fn hostport(message: impl Into<String>) -> Self {
        Self::HostPort {
            message: message.into(),
        }
    }

    pub fn cleanup(message: impl Into<String>) -> Self {
        Self::Cleanup {
            message: message.into(),
        }
    }

    fn message(&self) -> &str {
        match self {
            Self::InvalidRequest { message, .. }
            | Self::Sync { message }
            | Self::HostPort { message }
            | Self::Cleanup { message } => message,
        }
    }
}

impl fmt::Display for ServiceRouterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message())
    }
}

impl Error for ServiceRouterError {}

/// Object-safe future for one complete service-routing operation.
///
/// Calls are coarse reconcile, pod-lifecycle, or shutdown operations; the one
/// boxed future is outside packet and per-endpoint loops and is dominated by
/// the corresponding API or kernel networking work.
pub type ServiceRouterFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), ServiceRouterError>> + Send + 'a>>;

/// Service and hostPort routing capability.
///
/// Implementations own API reconciliation and concrete nft state. This port
/// accepts only validated runtime values and exposes no Kubernetes object,
/// datastore, generated wire, async-runtime, framework, filesystem/process,
/// or concrete Linux networking type.
pub trait ServiceRouter: Send + Sync + 'static {
    fn request_services_sync(&self) -> Result<(), ServiceRouterError>;

    fn sync_services_now(&self) -> ServiceRouterFuture<'_>;

    fn add_hostport_rules(&self, request: HostPortRules) -> ServiceRouterFuture<'_>;

    fn remove_hostport_rules(&self, request: HostPortRemoval) -> ServiceRouterFuture<'_>;

    fn cleanup(&self) -> ServiceRouterFuture<'_>;
}

/// Identity shared by direct pod-endpoint topology and resolved reachability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectPodEndpoint {
    pod_ip: Ipv4Addr,
    node_name: String,
}

impl DirectPodEndpoint {
    pub fn try_new(
        pod_ip: Ipv4Addr,
        node_name: impl Into<String>,
    ) -> Result<Self, PodEndpointError> {
        validate_endpoint_pod_ip(pod_ip)?;
        let node_name = validate_endpoint_node_name(node_name)?;
        Ok(Self { pod_ip, node_name })
    }

    pub const fn pod_ip(&self) -> Ipv4Addr {
        self.pod_ip
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }
}

/// Rootless/hybrid endpoint topology with every published L4 host port.
///
/// Both ports may be absent for a persisted endpoint that currently publishes
/// no host mapping. A present port is always non-zero. Keeping both mappings
/// prevents event consumers from silently dropping one protocol.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostPortPodEndpoint {
    pod_ip: Ipv4Addr,
    node_name: String,
    node_ip: Ipv4Addr,
    host_port_tcp: Option<u16>,
    host_port_udp: Option<u16>,
}

impl HostPortPodEndpoint {
    pub fn try_new(
        pod_ip: Ipv4Addr,
        node_name: impl Into<String>,
        node_ip: Ipv4Addr,
        host_port_tcp: Option<u16>,
        host_port_udp: Option<u16>,
    ) -> Result<Self, PodEndpointError> {
        validate_endpoint_pod_ip(pod_ip)?;
        let node_name = validate_endpoint_node_name(node_name)?;
        validate_endpoint_port(host_port_tcp, "endpoint.host_port_tcp")?;
        validate_endpoint_port(host_port_udp, "endpoint.host_port_udp")?;
        Ok(Self {
            pod_ip,
            node_name,
            node_ip,
            host_port_tcp,
            host_port_udp,
        })
    }

    pub const fn pod_ip(&self) -> Ipv4Addr {
        self.pod_ip
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub const fn node_ip(&self) -> Ipv4Addr {
        self.node_ip
    }

    pub const fn host_port_tcp(&self) -> Option<u16> {
        self.host_port_tcp
    }

    pub const fn host_port_udp(&self) -> Option<u16> {
        self.host_port_udp
    }
}

fn validate_endpoint_pod_ip(pod_ip: Ipv4Addr) -> Result<(), PodEndpointError> {
    if pod_ip.is_unspecified() {
        Err(PodEndpointError::invalid(
            "endpoint.pod_ip",
            "must not be the unspecified address",
        ))
    } else {
        Ok(())
    }
}

fn validate_endpoint_node_name(node_name: impl Into<String>) -> Result<String, PodEndpointError> {
    let node_name = node_name.into();
    if node_name.trim().is_empty() {
        Err(PodEndpointError::invalid(
            "endpoint.node_name",
            "must not be empty",
        ))
    } else {
        Ok(node_name)
    }
}

fn validate_endpoint_port(port: Option<u16>, field: &'static str) -> Result<(), PodEndpointError> {
    if port == Some(0) {
        Err(PodEndpointError::invalid(field, "must be non-zero"))
    } else {
        Ok(())
    }
}

/// Effective runtime reachability returned by a [`PodEndpointResolver`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodEndpoint {
    EncryptedDirect(DirectPodEndpoint),
    UnencryptedDirect(DirectPodEndpoint),
    HostPort(HostPortPodEndpoint),
}

impl PodEndpoint {
    pub const fn pod_ip(&self) -> Ipv4Addr {
        match self {
            Self::EncryptedDirect(endpoint) | Self::UnencryptedDirect(endpoint) => {
                endpoint.pod_ip()
            }
            Self::HostPort(endpoint) => endpoint.pod_ip(),
        }
    }
}

/// Persisted runtime topology carried by endpoint events.
///
/// Direct topology deliberately does not claim an encryption mode. Resolution
/// consults current node dataplane metadata before returning encrypted versus
/// explicit direct reachability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodEndpointTopology {
    Direct(DirectPodEndpoint),
    HostPort(HostPortPodEndpoint),
}

impl PodEndpointTopology {
    pub const fn pod_ip(&self) -> Ipv4Addr {
        match self {
            Self::Direct(endpoint) => endpoint.pod_ip(),
            Self::HostPort(endpoint) => endpoint.pod_ip(),
        }
    }
}

/// Ordered node-local endpoint change delivered to networking consumers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodEndpointEvent {
    Upsert(PodEndpointTopology),
    Delete(Ipv4Addr),
    /// Complete recovery snapshot after the underlying bounded source reports
    /// loss. This vector is allocated only on coarse lag recovery, never per
    /// live endpoint event.
    Resync(Vec<PodEndpointTopology>),
}

/// Typed failures for endpoint validation, resolution, and source recovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodEndpointError {
    InvalidEndpoint {
        field: &'static str,
        message: String,
    },
    Resolve {
        message: String,
    },
    EventSource {
        message: String,
    },
}

impl PodEndpointError {
    fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidEndpoint {
            field,
            message: message.into(),
        }
    }

    pub fn resolve(message: impl Into<String>) -> Self {
        Self::Resolve {
            message: message.into(),
        }
    }

    pub fn event_source(message: impl Into<String>) -> Self {
        Self::EventSource {
            message: message.into(),
        }
    }

    fn message(&self) -> &str {
        match self {
            Self::InvalidEndpoint { message, .. }
            | Self::Resolve { message }
            | Self::EventSource { message } => message,
        }
    }
}

impl fmt::Display for PodEndpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message())
    }
}

impl Error for PodEndpointError {}

/// Heap-erased future for one coarse endpoint lookup or recovery snapshot.
pub type PodEndpointFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, PodEndpointError>> + Send + 'a>>;

/// Pull-based endpoint subscription without a Tokio or futures dependency.
///
/// Implementations allocate once when the subscription is created. Polling an
/// item uses the caller-provided task context and does not box each event.
pub trait PodEndpointEventSubscription: Send {
    fn poll_next(
        self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<PodEndpointEvent, PodEndpointError>>>;
}

pub type PodEndpointEventStream = Pin<Box<dyn PodEndpointEventSubscription + 'static>>;

/// Atomic authoritative snapshot and ordered-change capability for pod
/// endpoint topology.
///
/// A successful subscription emits [`PodEndpointEvent::Resync`] first. The
/// source establishes its receiver before reading that snapshot, so callers
/// cannot create a LIST-to-subscribe gap. A typed stream error means delivery
/// is no longer authoritative and the caller must establish a fresh
/// subscription.
pub trait PodEndpointEventSource: Send + Sync + 'static {
    fn subscribe(&self) -> PodEndpointFuture<'_, PodEndpointEventStream>;
}

/// Effective pod reachability lookup capability.
pub trait PodEndpointResolver: Send + Sync + 'static {
    fn resolve(&self, pod_ip: Ipv4Addr) -> PodEndpointFuture<'_, Option<PodEndpoint>>;
}
