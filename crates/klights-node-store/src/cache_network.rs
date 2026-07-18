//! Node-local pod-network allocation cache and endpoint-row persistence.
//!
//! This module owns persistence state only. Datapath behavior, endpoint event
//! delivery, and resolver translation belong to `klights-network-api`.

use std::fmt;
use std::future::Future;
use std::net::Ipv4Addr;
use std::pin::Pin;

use klights_types::PodIdentity;

/// Failure returned by node-local cache/network persistence.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CacheNetworkError {
    InvalidInput {
        field: &'static str,
        message: String,
    },
    PersistenceFailed {
        message: String,
    },
    CorruptData {
        message: String,
    },
    Retryable {
        message: String,
    },
    AddressExhausted {
        subnet_base_int: u32,
        subnet_size: u32,
    },
    IdentityConflict {
        sandbox_id: String,
    },
    Timeout,
    Cancelled,
}

impl CacheNetworkError {
    pub fn persistence_failed(message: impl Into<String>) -> Self {
        Self::PersistenceFailed {
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

    fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidInput {
            field,
            message: message.into(),
        }
    }
}

impl fmt::Display for CacheNetworkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput { field, message } => {
                write!(formatter, "invalid {field}: {message}")
            }
            Self::PersistenceFailed { message }
            | Self::CorruptData { message }
            | Self::Retryable { message } => formatter.write_str(message),
            Self::AddressExhausted {
                subnet_base_int,
                subnet_size,
            } => write!(
                formatter,
                "pod address range {subnet_base_int}/{subnet_size} is exhausted"
            ),
            Self::IdentityConflict { sandbox_id } => {
                write!(
                    formatter,
                    "sandbox {sandbox_id} already has a different identity"
                )
            }
            Self::Timeout => formatter.write_str("node cache/network persistence timed out"),
            Self::Cancelled => formatter.write_str("node cache/network persistence was cancelled"),
        }
    }
}

impl std::error::Error for CacheNetworkError {}

fn require_nonempty(value: &str, field: &'static str) -> Result<(), CacheNetworkError> {
    if value.is_empty() {
        Err(CacheNetworkError::invalid(field, "must not be empty"))
    } else {
        Ok(())
    }
}

fn validate_pod_identity(pod: &PodIdentity) -> Result<(), CacheNetworkError> {
    require_nonempty(&pod.namespace, "pod.namespace")?;
    require_nonempty(&pod.name, "pod.name")?;
    require_nonempty(&pod.uid, "pod.uid")
}

macro_rules! string_key {
    ($name:ident, $field:literal) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl $name {
            pub fn try_new(value: impl Into<String>) -> Result<Self, CacheNetworkError> {
                let value = value.into();
                require_nonempty(&value, $field)?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_inner(self) -> String {
                self.0
            }
        }
    };
}

string_key!(SandboxKey, "sandbox_id");
string_key!(PodUidKey, "pod_uid");
string_key!(NodeKey, "node_name");

/// Owned request for one atomic, idempotent pod-IP reservation and cache row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodNetworkAllocationRequest {
    sandbox_id: SandboxKey,
    pod: PodIdentity,
    subnet_base_int: u32,
    subnet_size: u32,
    veth_host: String,
    netns_path: String,
}

impl PodNetworkAllocationRequest {
    pub fn try_new(
        sandbox_id: impl Into<String>,
        pod: PodIdentity,
        subnet_base_int: u32,
        subnet_size: u32,
        veth_host: impl Into<String>,
        netns_path: impl Into<String>,
    ) -> Result<Self, CacheNetworkError> {
        let sandbox_id = SandboxKey::try_new(sandbox_id)?;
        let veth_host = veth_host.into();
        let netns_path = netns_path.into();
        validate_pod_identity(&pod)?;
        require_nonempty(&veth_host, "veth_host")?;
        require_nonempty(&netns_path, "netns_path")?;
        if subnet_size < 4 {
            return Err(CacheNetworkError::invalid(
                "subnet_size",
                "must contain network, gateway, pod, and broadcast addresses",
            ));
        }
        if subnet_base_int.checked_add(subnet_size - 1).is_none() {
            return Err(CacheNetworkError::invalid(
                "subnet",
                "address range exceeds IPv4 space",
            ));
        }
        Ok(Self {
            sandbox_id,
            pod,
            subnet_base_int,
            subnet_size,
            veth_host,
            netns_path,
        })
    }

    pub fn sandbox_id(&self) -> &str {
        self.sandbox_id.as_str()
    }

    pub const fn sandbox_key(&self) -> &SandboxKey {
        &self.sandbox_id
    }

    pub const fn pod(&self) -> &PodIdentity {
        &self.pod
    }

    pub const fn subnet_base_int(&self) -> u32 {
        self.subnet_base_int
    }

    pub const fn subnet_size(&self) -> u32 {
        self.subnet_size
    }

    pub fn veth_host(&self) -> &str {
        &self.veth_host
    }

    pub fn netns_path(&self) -> &str {
        &self.netns_path
    }

    pub fn into_parts(self) -> (String, PodIdentity, u32, u32, String, String) {
        (
            self.sandbox_id.into_inner(),
            self.pod,
            self.subnet_base_int,
            self.subnet_size,
            self.veth_host,
            self.netns_path,
        )
    }
}

/// Result of an atomic pod-IP reservation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodNetworkAllocation {
    ip_addr: String,
    ip_int: u32,
}

impl PodNetworkAllocation {
    pub fn try_new(ip_addr: impl Into<String>, ip_int: u32) -> Result<Self, CacheNetworkError> {
        let ip_addr = ip_addr.into();
        let parsed = ip_addr
            .parse::<Ipv4Addr>()
            .map_err(|error| CacheNetworkError::invalid("ip_addr", error.to_string()))?;
        if u32::from(parsed) != ip_int {
            return Err(CacheNetworkError::invalid(
                "ip_int",
                "does not match ip_addr",
            ));
        }
        Ok(Self { ip_addr, ip_int })
    }

    pub fn ip_addr(&self) -> &str {
        &self.ip_addr
    }

    pub const fn ip_int(&self) -> u32 {
        self.ip_int
    }

    pub fn into_parts(self) -> (String, u32) {
        (self.ip_addr, self.ip_int)
    }
}

/// Cached pod network state addressed by either Pod UID or sandbox ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodNetworkEndpoint {
    ip_addr: String,
    veth_host: String,
    netns_path: String,
}

impl PodNetworkEndpoint {
    pub fn try_new(
        ip_addr: impl Into<String>,
        veth_host: impl Into<String>,
        netns_path: impl Into<String>,
    ) -> Result<Self, CacheNetworkError> {
        let ip_addr = ip_addr.into();
        let veth_host = veth_host.into();
        let netns_path = netns_path.into();
        ip_addr
            .parse::<Ipv4Addr>()
            .map_err(|error| CacheNetworkError::invalid("ip_addr", error.to_string()))?;
        require_nonempty(&veth_host, "veth_host")?;
        require_nonempty(&netns_path, "netns_path")?;
        Ok(Self {
            ip_addr,
            veth_host,
            netns_path,
        })
    }

    pub fn ip_addr(&self) -> &str {
        &self.ip_addr
    }

    pub fn veth_host(&self) -> &str {
        &self.veth_host
    }

    pub fn netns_path(&self) -> &str {
        &self.netns_path
    }

    pub fn into_parts(self) -> (String, String, String) {
        (self.ip_addr, self.veth_host, self.netns_path)
    }
}

/// Persisted reachability mode. This is storage state, not runtime behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PodEndpointMode {
    EncryptedDirect,
    Hostport,
}

/// One persisted pod endpoint row with exact identity and generation state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodEndpointRecord {
    pod: PodIdentity,
    node_name: String,
    mode: PodEndpointMode,
    pod_ip: Ipv4Addr,
    node_ip: Ipv4Addr,
    host_port_tcp: Option<u16>,
    host_port_udp: Option<u16>,
    generation: i64,
    updated_at_ms: i64,
}

impl PodEndpointRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        pod: PodIdentity,
        node_name: impl Into<String>,
        mode: PodEndpointMode,
        pod_ip: Ipv4Addr,
        node_ip: Ipv4Addr,
        host_port_tcp: Option<u16>,
        host_port_udp: Option<u16>,
        generation: i64,
        updated_at_ms: i64,
    ) -> Result<Self, CacheNetworkError> {
        let node_name = node_name.into();
        validate_pod_identity(&pod)?;
        require_nonempty(&node_name, "node_name")?;
        if pod_ip.is_unspecified() {
            return Err(CacheNetworkError::invalid(
                "pod_ip",
                "must not be the unspecified address",
            ));
        }
        if generation < 0 {
            return Err(CacheNetworkError::invalid(
                "generation",
                "must be non-negative",
            ));
        }
        if updated_at_ms < 0 {
            return Err(CacheNetworkError::invalid(
                "updated_at_ms",
                "must be non-negative",
            ));
        }
        Ok(Self {
            pod,
            node_name,
            mode,
            pod_ip,
            node_ip,
            host_port_tcp,
            host_port_udp,
            generation,
            updated_at_ms,
        })
    }

    /// Validates signed host-port values read from a durable backend.
    /// An unspecified `node_ip` is retained for legacy direct-mode rows.
    #[allow(clippy::too_many_arguments)]
    pub fn try_from_persisted(
        pod: PodIdentity,
        node_name: impl Into<String>,
        mode: PodEndpointMode,
        pod_ip: Ipv4Addr,
        node_ip: Ipv4Addr,
        host_port_tcp: Option<i64>,
        host_port_udp: Option<i64>,
        generation: i64,
        updated_at_ms: i64,
    ) -> Result<Self, CacheNetworkError> {
        fn port(value: Option<i64>, field: &'static str) -> Result<Option<u16>, CacheNetworkError> {
            value
                .map(|value| {
                    u16::try_from(value).map_err(|_| {
                        CacheNetworkError::invalid(field, "must fit an unsigned 16-bit port")
                    })
                })
                .transpose()
        }
        Self::try_new(
            pod,
            node_name,
            mode,
            pod_ip,
            node_ip,
            port(host_port_tcp, "host_port_tcp")?,
            port(host_port_udp, "host_port_udp")?,
            generation,
            updated_at_ms,
        )
    }

    pub const fn pod(&self) -> &PodIdentity {
        &self.pod
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub const fn mode(&self) -> PodEndpointMode {
        self.mode
    }

    pub const fn pod_ip(&self) -> Ipv4Addr {
        self.pod_ip
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

    pub const fn generation(&self) -> i64 {
        self.generation
    }

    pub const fn updated_at_ms(&self) -> i64 {
        self.updated_at_ms
    }

    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        PodIdentity,
        String,
        PodEndpointMode,
        Ipv4Addr,
        Ipv4Addr,
        Option<u16>,
        Option<u16>,
        i64,
        i64,
    ) {
        (
            self.pod,
            self.node_name,
            self.mode,
            self.pod_ip,
            self.node_ip,
            self.host_port_tcp,
            self.host_port_udp,
            self.generation,
            self.updated_at_ms,
        )
    }
}

/// Atomic facts produced by one committed endpoint upsert.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EndpointUpsertOutcome {
    previous: Option<PodEndpointRecord>,
    current: PodEndpointRecord,
}

impl EndpointUpsertOutcome {
    pub fn new(previous: Option<PodEndpointRecord>, current: PodEndpointRecord) -> Self {
        Self { previous, current }
    }

    pub const fn previous(&self) -> Option<&PodEndpointRecord> {
        self.previous.as_ref()
    }

    pub const fn current(&self) -> &PodEndpointRecord {
        &self.current
    }

    pub fn into_parts(self) -> (Option<PodEndpointRecord>, PodEndpointRecord) {
        (self.previous, self.current)
    }
}

/// Atomic facts produced by one committed endpoint delete.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EndpointDeleteOutcome {
    removed: Option<PodEndpointRecord>,
}

impl EndpointDeleteOutcome {
    pub const fn new(removed: Option<PodEndpointRecord>) -> Self {
        Self { removed }
    }

    pub const fn removed(&self) -> Option<&PodEndpointRecord> {
        self.removed.as_ref()
    }

    pub fn into_removed(self) -> Option<PodEndpointRecord> {
        self.removed
    }
}

/// Heap-erased future used at the coarse node-persistence boundary.
pub type CacheNetworkFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, CacheNetworkError>> + Send + 'a>>;

/// Read/delete access to persisted pod-network allocation cache rows.
pub trait PodNetworkCache: Send + Sync {
    fn get_network_for_uid(
        &self,
        pod_uid: PodUidKey,
    ) -> CacheNetworkFuture<'_, Option<PodNetworkEndpoint>>;
    fn get_network_for_sandbox(
        &self,
        sandbox_id: SandboxKey,
    ) -> CacheNetworkFuture<'_, Option<PodNetworkEndpoint>>;
    fn delete_network_for_sandbox(&self, sandbox_id: SandboxKey) -> CacheNetworkFuture<'_, ()>;
    fn list_network_sandbox_ids(&self) -> CacheNetworkFuture<'_, Vec<String>>;
}

/// Atomic pod-IP reservation and allocation-cache insertion capability.
pub trait PodIpamStore: Send + Sync {
    /// Atomically reserves from `base + 2 .. base + size - 1`.
    /// Repeating the exact sandbox identity returns its allocation; reusing a
    /// sandbox with different Pod/subnet/link identity returns
    /// [`CacheNetworkError::IdentityConflict`]. Exhaustion is typed.
    fn reserve_ip_and_insert_network(
        &self,
        request: PodNetworkAllocationRequest,
    ) -> CacheNetworkFuture<'_, PodNetworkAllocation>;
}

/// Mutation and query access to persisted pod endpoint rows.
pub trait PodEndpointStore: Send + Sync {
    /// Returns old and current facts from the same committed mutation.
    fn upsert_endpoint(
        &self,
        record: PodEndpointRecord,
    ) -> CacheNetworkFuture<'_, EndpointUpsertOutcome>;
    /// Returns the removed facts from the same committed mutation.
    fn delete_endpoint_for_uid(
        &self,
        pod_uid: PodUidKey,
    ) -> CacheNetworkFuture<'_, EndpointDeleteOutcome>;
    fn get_endpoint_by_pod_ip(
        &self,
        pod_ip: Ipv4Addr,
    ) -> CacheNetworkFuture<'_, Option<PodEndpointRecord>>;
    fn list_endpoints_all(&self) -> CacheNetworkFuture<'_, Vec<PodEndpointRecord>>;
    fn list_endpoints_for_node(
        &self,
        node_name: NodeKey,
    ) -> CacheNetworkFuture<'_, Vec<PodEndpointRecord>>;
}
