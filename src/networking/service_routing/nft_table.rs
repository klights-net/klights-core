use super::hostport::HostPortSpec;
use super::mode::ServiceRoutingMode;
use super::network_policy::{
    Ipv4CidrMatch, NetworkPolicyDirection, NetworkPolicyPeerMatch, NetworkPolicyPlan,
};
use super::prelude::*;
use super::service_rules::select_authoritative_service_spec;
use super::*;
use crate::utils::lock_recover;
use klights_cluster_core::Resource;
use klights_leader_api::{ResourceListRequest, ResourceQueryConsistency, ResourceQueryError};
use nftnl::expr::Expression;
use nftnl::nftnl_sys as sys;
use std::ptr;

const IP_CT_DIR_ORIGINAL: u32 = 0;
const NFT_CT_PROTOCOL: u32 = 10;
const NFT_CT_PROTO_DST: u32 = 12;
const NFT_CT_DST_IP: u32 = 20;
const LINUX_IFNAMSIZ: usize = 16;
const HOST_FORWARD_COMPAT_FAMILY: &str = "ip";
const HOST_FORWARD_COMPAT_TABLE: &str = "filter";
const HOST_FORWARD_COMPAT_CHAIN: &str = "FORWARD";
const HOST_FORWARD_COMPAT_COMMENT: &str = "klights-forward-compat";

fn fresh_list_request(
    api_version: &str,
    kind: &str,
) -> std::result::Result<ResourceListRequest, ResourceQueryError> {
    ResourceListRequest::try_new(
        api_version,
        kind,
        None,
        None,
        None,
        None,
        None,
        ResourceQueryConsistency::LeaderFresh,
    )
}

#[derive(Clone, Copy)]
enum CtOriginalKey {
    DstIp,
    Protocol,
    ProtoDst,
}

impl CtOriginalKey {
    fn raw(self) -> u32 {
        match self {
            CtOriginalKey::Protocol => NFT_CT_PROTOCOL,
            CtOriginalKey::ProtoDst => NFT_CT_PROTO_DST,
            CtOriginalKey::DstIp => NFT_CT_DST_IP,
        }
    }
}

struct CtOriginal {
    key: CtOriginalKey,
}

impl CtOriginal {
    fn new(key: CtOriginalKey) -> Self {
        Self { key }
    }
}

impl Expression for CtOriginal {
    fn to_expr(&self, _rule: &Rule) -> ptr::NonNull<sys::nftnl_expr> {
        // SAFETY: libnftnl returns either a valid owned expression pointer or
        // null on allocation failure. We abort on null to match nftnl's
        // internal expression wrappers.
        let expr = unsafe { sys::nftnl_expr_alloc(c"ct".as_ptr()) };
        let Some(expr) = ptr::NonNull::new(expr) else {
            std::process::abort();
        };

        // SAFETY: `expr` is a live `ct` expression allocated above. These
        // setters only write scalar attributes for destination register, key,
        // and direction; libnftnl takes no borrowed Rust references.
        //
        // Direction uses the raw `nftnl_expr_set` instead of `_u32` because
        // libnftnl's `_u32` path silently drops the CT direction attribute
        // from the serialized netlink message (likely a "value == 0 means
        // unset" optimisation).  On kernel ≤6.x the missing attribute was
        // tolerated because the kernel defaulted to ORIGINAL (0).  Kernel
        // 7.x (Ubuntu 24.10+) rejects the rule with EINVAL when the
        // direction is absent for IP-address keys.  Passing the single byte
        // via `nftnl_expr_set` forces libnftnl to emit the NLA unconditionally
        // — matching what `nft add rule ... ct original ...` does.
        unsafe {
            sys::nftnl_expr_set_u32(
                expr.as_ptr(),
                sys::NFTNL_EXPR_CT_DREG as u16,
                libc::NFT_REG_1 as u32,
            );
            sys::nftnl_expr_set_u32(expr.as_ptr(), sys::NFTNL_EXPR_CT_KEY as u16, self.key.raw());
            let dir: u8 = IP_CT_DIR_ORIGINAL as u8;
            sys::nftnl_expr_set(
                expr.as_ptr(),
                sys::NFTNL_EXPR_CT_DIR as u16,
                &dir as *const u8 as *const std::ffi::c_void,
                1,
            );
        }

        expr
    }
}

pub struct KlightsTable {
    nf: Netfilter,
    table_name: CString,
    bridge_ifname: CString,
    pod_subnet_ip: Ipv4Addr,
    pod_subnet_mask: Ipv4Addr,
    pod_gateway_ip: Ipv4Addr,
    cluster_cidr_ip: Ipv4Addr,
    cluster_cidr_mask: Ipv4Addr,
    service_cidr_ip: Ipv4Addr,
    service_cidr_mask: Ipv4Addr,
    /// F2-03: mode + selected route device flow in from the network boot
    /// boundary. Decides whether the forward chain needs an explicit
    /// cross-node route-device accept rule and which interface it matches.
    mode: ServiceRoutingMode,
    /// Per-table hostport mappings — one entry per pod that has at least
    /// one hostPort declared. Re-emitting the entire `hostports` chain
    /// from this snapshot avoids reading kernel state back to compute
    /// deltas. Lives on the table instance so the OnceLock global
    /// registry can be deleted (Task 5 of the network refactor).
    hostport_registry: std::sync::Mutex<std::collections::HashMap<Ipv4Addr, Vec<HostPortSpec>>>,
    host_forward_compat_enabled: bool,
    /// Remote rootless pod endpoints keyed by pod IP. Root nodes use this
    /// registry to rebuild `remote_pod_v4` from `pod_endpoints` without
    /// reading kernel state back.
    remote_pod_registry:
        std::sync::Mutex<std::collections::HashMap<Ipv4Addr, Vec<RemotePodEndpointSpec>>>,
    /// Async lock serializing `(mutate registry + snapshot + send batch)`
    /// per table. See [`add_hostports_for_pod`] for the race scenario
    /// this prevents.
    hostport_lock: std::sync::Arc<tokio::sync::Mutex<()>>,
    remote_pod_lock: std::sync::Arc<tokio::sync::Mutex<()>>,
    /// Last successfully installed service DNAT inventory. Used to avoid
    /// rewriting nft chains when repeated Service/Endpoints watch events
    /// produce identical routing semantics.
    service_snapshot: std::sync::Mutex<Option<ServiceRuleSnapshot>>,
    /// Last successfully applied semantic ServiceSpec set. This is the planner
    /// input used by watch-fed cached syncs to avoid nft rewrites for no-op
    /// inventory events.
    applied_service_specs: std::sync::Mutex<Vec<ServiceSpec>>,
    /// Cached watch-fed Service route inventory. Populated on the first
    /// `sync_services_from_api` so subsequent watch events can apply diffs
    /// from cached state without re-listing the entire cluster API.
    /// `None` means the cache must be re-bootstrapped from the API
    /// (initial sync or after watch compaction / inventory corruption).
    service_route_inventory: std::sync::Mutex<Option<super::inventory::ServiceRouteInventory>>,
    network_policy_snapshot: std::sync::Mutex<Option<NetworkPolicyPlan>>,
    #[cfg(test)]
    hostport_flush_results: std::sync::Mutex<std::collections::VecDeque<Result<()>>>,
    #[cfg(test)]
    remote_flush_results: std::sync::Mutex<std::collections::VecDeque<Result<()>>>,
    #[cfg(test)]
    hostport_flush_attempts: std::sync::atomic::AtomicUsize,
}

#[derive(Clone, Copy)]
struct DnatServicePort {
    protocol: Protocol,
    service_port: u16,
}

impl DnatServicePort {
    fn new(protocol: Protocol, service_port: u16) -> Self {
        Self {
            protocol,
            service_port,
        }
    }
}

#[derive(Clone, Copy)]
struct DnatEndpointTarget {
    ip: Ipv4Addr,
    port: u16,
}

impl DnatEndpointTarget {
    fn new(ip: Ipv4Addr, port: u16) -> Self {
        Self { ip, port }
    }
}

#[derive(Clone, Copy)]
struct DnatEndpointRuleSpec {
    match_daddr: Option<Ipv4Addr>,
    service: DnatServicePort,
    endpoint: DnatEndpointTarget,
    probability: Option<u32>,
}

impl DnatEndpointRuleSpec {
    fn new(
        match_daddr: Option<Ipv4Addr>,
        service: DnatServicePort,
        endpoint: DnatEndpointTarget,
        probability: Option<u32>,
    ) -> Self {
        Self {
            match_daddr,
            service,
            endpoint,
            probability,
        }
    }
}

fn nft_args<const N: usize>(args: [&str; N]) -> Vec<String> {
    args.into_iter().map(str::to_string).collect()
}

fn ipv4_prefix_len(mask: Ipv4Addr) -> u32 {
    u32::from(mask).count_ones()
}

fn forward_compat_rule_handles(listing: &[u8], comment: &str) -> Result<Vec<u64>> {
    let value: serde_json::Value =
        serde_json::from_slice(listing).context("parse nft JSON listing")?;
    let handles = value
        .get("nftables")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.get("rule"))
        .filter(|rule| rule_has_comment(rule, comment))
        .filter_map(|rule| rule.get("handle").and_then(serde_json::Value::as_u64))
        .collect();
    Ok(handles)
}

fn rule_has_comment(rule: &serde_json::Value, comment: &str) -> bool {
    if rule.get("comment").and_then(serde_json::Value::as_str) == Some(comment) {
        return true;
    }
    rule.get("expr")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .any(|expr| expr.get("comment").and_then(serde_json::Value::as_str) == Some(comment))
}

/// Bulk-load the cluster Service / Endpoints / EndpointSlice state into a
/// fresh [`ServiceRouteInventory`]. Used by `sync_services_from_api` as the
/// initial-snapshot path and for recovery after watch compaction.
pub async fn bootstrap_inventory_from_api(
    api: &dyn LeaderResourceQuery,
) -> Result<super::inventory::ServiceRouteInventory> {
    let services_list = api
        .list_resources(fresh_list_request("v1", "Service")?)
        .await
        .context("list Services through focused leader query")?;

    let endpoints_list = api
        .list_resources(fresh_list_request("v1", "Endpoints")?)
        .await
        .context("list Endpoints through focused leader query")?;

    let endpoint_slices_list = api
        .list_resources(fresh_list_request("discovery.k8s.io/v1", "EndpointSlice")?)
        .await
        .context("list EndpointSlices through focused leader query")?;

    let services = services_list.items().iter().filter_map(|r| {
        let ns = r.namespace.clone()?;
        Some((
            ns,
            r.name.clone(),
            r.resource_version,
            r.data.as_ref().clone(),
        ))
    });
    let endpoints = endpoints_list.items().iter().filter_map(|r| {
        let ns = r.namespace.clone()?;
        Some((
            ns,
            r.name.clone(),
            r.resource_version,
            r.data.as_ref().clone(),
        ))
    });
    let endpoint_slices = endpoint_slices_list.items().iter().filter_map(|r| {
        let ns = r.namespace.clone()?;
        let (_, service_name) = endpoint_slice_service_key(r)?;
        Some((
            ns,
            service_name.to_string(),
            r.name.clone(),
            r.resource_version,
            r.data.as_ref().clone(),
        ))
    });

    let mut inv = super::inventory::ServiceRouteInventory::new();
    inv.replace_from_snapshot(services, endpoints, endpoint_slices);
    Ok(inv)
}

pub async fn service_specs_from_api(api: &dyn LeaderResourceQuery) -> Result<Vec<ServiceSpec>> {
    let services_list = api
        .list_resources(fresh_list_request("v1", "Service")?)
        .await
        .context("list Services through focused leader query")?;

    let endpoints_list = api
        .list_resources(fresh_list_request("v1", "Endpoints")?)
        .await
        .context("list Endpoints through focused leader query")?;

    let endpoint_slices_list = api
        .list_resources(fresh_list_request("discovery.k8s.io/v1", "EndpointSlice")?)
        .await
        .context("list EndpointSlices through focused leader query")?;

    let mut endpoints_by_service: std::collections::HashMap<(String, String), &Resource> =
        std::collections::HashMap::with_capacity(endpoints_list.items().len());
    for endpoints in endpoints_list.items() {
        if let Some((namespace, name)) = resource_namespace_name(endpoints) {
            endpoints_by_service.insert((namespace.to_string(), name.to_string()), endpoints);
        }
    }

    let mut endpoint_slices_by_service: std::collections::HashMap<
        (String, String),
        Vec<&Resource>,
    > = std::collections::HashMap::new();
    for slice in endpoint_slices_list.items() {
        if let Some((namespace, service_name)) = endpoint_slice_service_key(slice) {
            endpoint_slices_by_service
                .entry((namespace.to_string(), service_name.to_string()))
                .or_default()
                .push(slice);
        }
    }

    let mut specs: Vec<ServiceSpec> = Vec::with_capacity(services_list.items().len());
    for svc_resource in services_list.items() {
        let svc = &svc_resource.data;
        let metadata = match svc.get("metadata") {
            Some(m) => m,
            None => continue,
        };
        let svc_name = metadata.get("name").and_then(|n| n.as_str());
        let namespace = metadata.get("namespace").and_then(|n| n.as_str());
        let (svc_name, namespace) = match (svc_name, namespace) {
            (Some(n), Some(ns)) => (n, ns),
            _ => continue,
        };

        let key = (namespace.to_string(), svc_name.to_string());
        let endpoints = endpoints_by_service.get(&key).copied();
        let slice_items = endpoint_slices_by_service
            .get(&key)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        if let Some(spec) =
            service_spec_from_endpoint_inventory(svc, endpoints, slice_items.iter().copied())
        {
            specs.push(spec);
        }
    }

    Ok(specs)
}

fn resource_namespace_name(resource: &Resource) -> Option<(&str, &str)> {
    let metadata = resource.data.get("metadata");
    let namespace = metadata
        .and_then(|m| m.get("namespace"))
        .and_then(|v| v.as_str())
        .or(resource.namespace.as_deref())?;
    let name = metadata
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or(resource.name.as_str());
    Some((namespace, name))
}

fn endpoint_slice_service_key(resource: &Resource) -> Option<(&str, &str)> {
    let namespace = resource_namespace_name(resource)?.0;
    let service_name = resource
        .data
        .get("metadata")
        .and_then(|m| m.get("labels"))
        .and_then(|l| l.get("kubernetes.io/service-name"))
        .and_then(|v| v.as_str())?;
    Some((namespace, service_name))
}

fn service_spec_from_endpoint_inventory<'a, I>(
    service: &serde_json::Value,
    endpoints: Option<&Resource>,
    endpoint_slices: I,
) -> Option<ServiceSpec>
where
    I: IntoIterator<Item = &'a Resource>,
{
    let slice_refs: Vec<&serde_json::Value> = endpoint_slices
        .into_iter()
        .map(|r| r.data.as_ref())
        .collect();
    let endpointslice_spec = if slice_refs.is_empty() {
        None
    } else {
        ServiceSpec::from_service_and_endpointslices(service, &slice_refs)
    };
    let legacy_endpoints_spec =
        endpoints.and_then(|eps| ServiceSpec::from_service_and_endpoints(service, Some(&eps.data)));
    select_authoritative_service_spec(endpointslice_spec, legacy_endpoints_spec)
}

impl KlightsTable {
    /// Construct against an explicit table name.
    ///
    /// **Always pass `config.containerd_namespace`** in production code paths
    /// — never `bridge_name`, which gets truncated to the Linux 15-char
    /// interface-name limit. The nft table name has no length limit, so
    /// using the full, un-truncated namespace identifier keeps the table
    /// name aligned with what test harnesses, operators, and the chainsaw
    /// `nft-foundation` test expect (see `tests/run_tests.sh` `--set
    /// klights_ns=$KLIGHTS_NS`).
    ///
    /// There is intentionally no `production()` shortcut hardcoding
    /// `"klights"` — that constructor existed briefly during early
    /// development and was an easy way to silently reintroduce the
    /// truncation bug fixed in 969d8c3. If you find yourself reaching
    /// for a default, plumb the actual namespace through instead.
    #[cfg(test)]
    pub fn with_name(
        nf: Netfilter,
        table_name: &str,
        pod_subnet: PodSubnet,
        cluster_cidr: ClusterCidr,
        service_cidr: ClusterCidr,
        mode: ServiceRoutingMode,
    ) -> Result<Self> {
        Self::with_name_and_bridge(
            nf,
            table_name,
            "klights0",
            pod_subnet,
            cluster_cidr,
            service_cidr,
            mode,
        )
    }

    pub fn with_name_and_bridge(
        nf: Netfilter,
        table_name: &str,
        bridge_ifname: &str,
        pod_subnet: PodSubnet,
        cluster_cidr: ClusterCidr,
        service_cidr: ClusterCidr,
        mode: ServiceRoutingMode,
    ) -> Result<Self> {
        let pod_subnet_ip = Ipv4Addr::from(pod_subnet.base());
        let pod_subnet_mask = Ipv4Addr::from(pod_subnet.mask());
        let pod_gateway_ip = pod_subnet.bridge_ip();
        let cluster_cidr_ip = Ipv4Addr::from(cluster_cidr.network());
        let cluster_cidr_mask = Ipv4Addr::from(cluster_cidr.mask());
        let service_cidr_ip = Ipv4Addr::from(service_cidr.network());
        let service_cidr_mask = Ipv4Addr::from(service_cidr.mask());
        let table_name = CString::new(table_name)
            .with_context(|| format!("invalid table name {table_name:?}"))?;
        let bridge_ifname = CString::new(bridge_ifname)
            .with_context(|| format!("invalid bridge interface name {bridge_ifname:?}"))?;
        validate_linux_ifname(&bridge_ifname)?;
        Ok(Self {
            nf,
            table_name,
            bridge_ifname,
            pod_subnet_ip,
            pod_subnet_mask,
            pod_gateway_ip,
            cluster_cidr_ip,
            cluster_cidr_mask,
            service_cidr_ip,
            service_cidr_mask,
            mode,
            hostport_registry: std::sync::Mutex::new(std::collections::HashMap::new()),
            host_forward_compat_enabled: !cfg!(test),
            remote_pod_registry: std::sync::Mutex::new(std::collections::HashMap::new()),
            hostport_lock: std::sync::Arc::new(tokio::sync::Mutex::new(())),
            remote_pod_lock: std::sync::Arc::new(tokio::sync::Mutex::new(())),
            service_snapshot: std::sync::Mutex::new(None),
            applied_service_specs: std::sync::Mutex::new(Vec::new()),
            service_route_inventory: std::sync::Mutex::new(None),
            network_policy_snapshot: std::sync::Mutex::new(None),
            #[cfg(test)]
            hostport_flush_results: std::sync::Mutex::new(std::collections::VecDeque::new()),
            #[cfg(test)]
            remote_flush_results: std::sync::Mutex::new(std::collections::VecDeque::new()),
            #[cfg(test)]
            hostport_flush_attempts: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    /// Borrow the routing mode (test inspection + Phase-2 future use).
    pub fn mode(&self) -> &ServiceRoutingMode {
        &self.mode
    }

    /// Snapshot every (pod_ip, specs) entry in the per-instance registry.
    fn hostport_snapshot(&self) -> Vec<(Ipv4Addr, Vec<HostPortSpec>)> {
        lock_recover(&self.hostport_registry)
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }

    /// Insert/replace one pod's hostport specs, retaining the exact prior
    /// entry so a failed kernel flush can be rolled back without cloning it.
    fn hostport_insert(
        &self,
        pod_ip: Ipv4Addr,
        specs: Vec<HostPortSpec>,
    ) -> Option<Option<Vec<HostPortSpec>>> {
        let mut reg = lock_recover(&self.hostport_registry);
        if reg.get(&pod_ip) == Some(&specs) {
            return None;
        }
        Some(reg.insert(pod_ip, specs))
    }

    fn hostport_restore(&self, pod_ip: Ipv4Addr, prior: Option<Vec<HostPortSpec>>) {
        let mut reg = lock_recover(&self.hostport_registry);
        match prior {
            Some(specs) => {
                reg.insert(pod_ip, specs);
            }
            None => {
                reg.remove(&pod_ip);
            }
        }
    }

    fn remote_pod_snapshot(&self) -> Vec<RemotePodEndpointSpec> {
        let mut snapshot: Vec<_> = lock_recover(&self.remote_pod_registry)
            .values()
            .flat_map(|specs| specs.iter().copied())
            .collect();
        snapshot.sort_by_key(|spec| {
            (
                u32::from(spec.pod_ip),
                match spec.protocol {
                    Protocol::Tcp => 0u8,
                    Protocol::Udp => 1u8,
                    Protocol::Sctp => 2u8,
                },
                spec.host_port,
            )
        });
        snapshot
    }

    fn remote_pod_restore(
        &self,
        prior: std::collections::HashMap<Ipv4Addr, Vec<RemotePodEndpointSpec>>,
    ) {
        *lock_recover(&self.remote_pod_registry) = prior;
    }

    fn finish_remote_pod_flush<T>(
        &self,
        prior: std::collections::HashMap<Ipv4Addr, Vec<RemotePodEndpointSpec>>,
        result: Result<T>,
    ) -> Result<T> {
        if result.is_err() {
            self.remote_pod_restore(prior);
        }
        result
    }

    fn remote_pod_replace_all(
        &self,
        specs: &[RemotePodEndpointSpec],
    ) -> std::collections::HashMap<Ipv4Addr, Vec<RemotePodEndpointSpec>> {
        let mut replacement = std::collections::HashMap::new();
        for spec in specs {
            replacement
                .entry(spec.pod_ip)
                .or_insert_with(Vec::new)
                .push(*spec);
        }
        std::mem::replace(&mut *lock_recover(&self.remote_pod_registry), replacement)
    }

    fn remote_pod_upsert(
        &self,
        pod_ip: Ipv4Addr,
        specs: Vec<RemotePodEndpointSpec>,
    ) -> Option<Option<Vec<RemotePodEndpointSpec>>> {
        let mut reg = lock_recover(&self.remote_pod_registry);
        if specs.is_empty() {
            return reg.remove(&pod_ip).map(Some);
        }
        if reg.get(&pod_ip) == Some(&specs) {
            return None;
        }
        Some(reg.insert(pod_ip, specs))
    }

    fn remote_pod_restore_entry(
        &self,
        pod_ip: Ipv4Addr,
        prior: Option<Vec<RemotePodEndpointSpec>>,
    ) {
        let mut reg = lock_recover(&self.remote_pod_registry);
        match prior {
            Some(specs) => {
                reg.insert(pod_ip, specs);
            }
            None => {
                reg.remove(&pod_ip);
            }
        }
    }

    #[cfg(test)]
    fn test_queue_hostport_flush_result(&self, result: Result<()>) {
        lock_recover(&self.hostport_flush_results).push_back(result);
    }

    #[cfg(test)]
    fn test_hostport_flush_attempts(&self) -> usize {
        self.hostport_flush_attempts
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    #[cfg(test)]
    fn test_queue_remote_flush_result(&self, result: Result<()>) {
        lock_recover(&self.remote_flush_results).push_back(result);
    }

    #[cfg(test)]
    fn test_remote_entry_ptr(&self, pod_ip: Ipv4Addr) -> Option<usize> {
        lock_recover(&self.remote_pod_registry)
            .get(&pod_ip)
            .map(|specs| specs.as_ptr() as usize)
    }

    /// Build the table with all foundation + service-routing chains.
    /// Idempotent: re-running rebuilds the chains atomically without
    /// touching unrelated tables. The `services` chain is created
    /// empty here; [`replace_services`] populates it.
    pub async fn init(&self) -> Result<()> {
        self.cleanup_legacy_unscoped_tables().await;
        self.nf
            .ensure_table(ProtoFamily::Inet, self.table_name())
            .await
            .context("ensure table")?;

        let table = self.table();

        // ---- Service conntrack guard chain ----
        // Regular chain used by filter-forward. It must exist before the
        // forward chain installs a jump to it.
        let service_ct_guard = Chain::new(SERVICE_CT_GUARD_CHAIN, &table);
        let mut service_ct_guard_create = Batch::new();
        service_ct_guard_create.add(&service_ct_guard, MsgType::Add);
        self.nf
            .send(service_ct_guard_create)
            .await
            .context("ensure service_ct_guard chain exists")?;
        self.replace_service_ct_guard(&[]).await?;

        // ---- NetworkPolicy chain ----
        // Regular chain jumped from filter-forward. Starts empty and returns
        // by default, so clusters with no policies keep the existing allow
        // behavior.
        let network_policy = Chain::new(NETWORK_POLICY_CHAIN, &table);
        let mut network_policy_create = Batch::new();
        network_policy_create.add(&network_policy, MsgType::Add);
        self.nf
            .send(network_policy_create)
            .await
            .context("ensure network-policy chain exists")?;
        self.replace_network_policies(&NetworkPolicyPlan::default())
            .await?;

        // ---- Filter forward ----
        let mut forward = Chain::new(FILTER_FORWARD_CHAIN, &table);
        forward.set_type(ChainType::Filter);
        forward.set_hook(Hook::Forward, PRIORITY_FILTER);
        // Drop INVALID conntrack state at the very top of the chain
        // before any accept rules. Catches packets that don't fit any
        // tracked connection (forged sequence numbers, packets after a
        // RST, fragments outside a flow, etc.) — defense-in-depth that
        // costs one extra rule evaluation per forwarded packet. Standard
        // kube-proxy / Calico / firewall pattern.
        let forward_rules = vec![
            self.rule_ct_invalid_drop(&forward),
            self.rule_jump_service_ct_guard_for_iif(&forward),
            self.rule_jump_service_ct_guard_for_oif(&forward),
            self.rule_ct_established_accept(&forward),
            self.rule_jump_to_chain(&forward, NETWORK_POLICY_CHAIN),
            self.rule_ip_in_subnet(&forward, Ipv4HeaderField::Saddr, CmpOp::Eq, Verdict::Accept),
            self.rule_ip_in_subnet(&forward, Ipv4HeaderField::Daddr, CmpOp::Eq, Verdict::Accept),
        ];
        self.nf
            .replace_chain(&forward, &forward_rules)
            .await
            .context("replace filter-forward chain")?;

        self.reconcile_host_forward_compat()
            .await
            .context("reconcile host FORWARD compatibility rules")?;

        // ---- NAT postrouting ----
        let mut postrouting = Chain::new(NAT_POSTROUTING_CHAIN, &table);
        postrouting.set_type(ChainType::Nat);
        postrouting.set_hook(Hook::PostRouting, PRIORITY_NAT_SRC);
        let postrouting_rules = vec![
            self.rule_service_dnat_hairpin_masquerade(&postrouting),
            self.rule_service_dnat_node_snat(&postrouting),
            self.rule_pod_to_external_masquerade(&postrouting),
        ];
        self.nf
            .replace_chain(&postrouting, &postrouting_rules)
            .await
            .context("replace nat-postrouting chain")?;

        // ---- Services chain ----
        // Regular chain (no hook). Created BEFORE nat-prerouting and
        // nat-output because they include `jump services` verdicts —
        // the kernel rejects a jump at chain-add time if the target
        // chain doesn't exist yet.
        //
        // Uses ensure_table-style add (idempotent NEWCHAIN, no FLUSH)
        // rather than replace_chain because once nat-prerouting/output
        // exist, they hold a `jump services` reference and the kernel
        // refuses to DEL the services chain (EBUSY). On subsequent
        // init() runs this is a no-op for the chain itself; the chain's
        // rule contents are managed exclusively by replace_services.
        let services = Chain::new(SERVICES_CHAIN, &table);
        let mut services_create = Batch::new();
        services_create.add(&services, MsgType::Add);
        self.nf
            .send(services_create)
            .await
            .context("ensure services chain exists")?;

        // ---- Hostports chain ----
        // Same idempotent-add pattern as services. Per-pod contents
        // managed by add_hostports_for_pod / remove_hostports_for_pod.
        let hostports = Chain::new(HOSTPORTS_CHAIN, &table);
        let mut hostports_create = Batch::new();
        hostports_create.add(&hostports, MsgType::Add);
        self.nf
            .send(hostports_create)
            .await
            .context("ensure hostports chain exists")?;

        // ---- Remote rootless pod endpoints chain ----
        // Hybrid root nodes DNAT remote rootless pod IPs to the peer node's
        // published per-pod hostport. Contents are managed from the
        // pod_endpoints resolver stream.
        let remote_pods = Chain::new(REMOTE_POD_ENDPOINTS_CHAIN, &table);
        let mut remote_pods_create = Batch::new();
        remote_pods_create.add(&remote_pods, MsgType::Add);
        self.nf
            .send(remote_pods_create)
            .await
            .context("ensure remote_pod_v4 chain exists")?;

        // ---- NAT prerouting ----
        // Hooked at dstnat priority (-100). Two jumps in order:
        // hostports first, then services. hostports must run first so
        // host-port DNAT preempts any service-vip rule that might also
        // match the same packet on a NodePort overlap.
        let mut prerouting = Chain::new(NAT_PREROUTING_CHAIN, &table);
        prerouting.set_type(ChainType::Nat);
        prerouting.set_hook(Hook::PreRouting, PRIORITY_NAT_DST);
        let prerouting_rules = vec![
            self.rule_jump_to_chain(&prerouting, HOSTPORTS_CHAIN),
            self.rule_jump_to_chain(&prerouting, REMOTE_POD_ENDPOINTS_CHAIN),
            self.rule_jump_to_chain(&prerouting, SERVICES_CHAIN),
        ];
        self.nf
            .replace_chain(&prerouting, &prerouting_rules)
            .await
            .context("replace nat-prerouting chain")?;

        // ---- NAT output ----
        let mut output = Chain::new(NAT_OUTPUT_CHAIN, &table);
        output.set_type(ChainType::Nat);
        output.set_hook(Hook::Out, PRIORITY_NAT_DST);
        let output_rules = vec![
            self.rule_jump_to_chain(&output, HOSTPORTS_CHAIN),
            self.rule_jump_to_chain(&output, REMOTE_POD_ENDPOINTS_CHAIN),
            self.rule_jump_to_chain(&output, SERVICES_CHAIN),
        ];
        self.nf
            .replace_chain(&output, &output_rules)
            .await
            .context("replace nat-output chain")?;

        tracing::info!(
            "Initialized inet {} table (pod subnet: {}/{})",
            self.table_name.to_string_lossy(),
            self.pod_subnet_ip,
            super::service_rules::prefix_len_from_mask(self.pod_subnet_mask),
        );
        Ok(())
    }

    pub async fn sync_network_policies_from_api(
        &self,
        api: &dyn LeaderResourceQuery,
    ) -> Result<usize> {
        let policies = api
            .list_resources(fresh_list_request("networking.k8s.io/v1", "NetworkPolicy")?)
            .await
            .context("list NetworkPolicies through focused leader query")?;
        let pods = api
            .list_resources(fresh_list_request("v1", "Pod")?)
            .await
            .context("list Pods through focused leader query for NetworkPolicy")?;
        let namespaces = api
            .list_resources(fresh_list_request("v1", "Namespace")?)
            .await
            .context("list Namespaces through focused leader query for NetworkPolicy")?;

        let policy_values: Vec<serde_json::Value> = policies
            .items()
            .iter()
            .map(|resource| resource.data.as_ref().clone())
            .collect();
        let pod_values: Vec<serde_json::Value> = pods
            .items()
            .iter()
            .map(|resource| resource.data.as_ref().clone())
            .collect();
        let namespace_values: Vec<serde_json::Value> = namespaces
            .items()
            .iter()
            .map(|resource| resource.data.as_ref().clone())
            .collect();
        let plan =
            NetworkPolicyPlan::from_resources(&policy_values, &pod_values, &namespace_values)?;
        let isolated = plan.isolated_ingress.len().max(plan.isolated_egress.len());
        self.replace_network_policies(&plan).await?;
        Ok(isolated)
    }

    pub async fn sync_services_from_api(&self, api: &dyn LeaderResourceQuery) -> Result<usize> {
        let inventory = bootstrap_inventory_from_api(api).await?;
        let specs = inventory.to_specs();
        let svc_count = specs.len();
        self.replace_services(&specs).await?;
        *lock_recover(&self.service_route_inventory) = Some(inventory);
        Ok(svc_count)
    }

    /// Re-emit the services chain from the cached
    /// [`ServiceRouteInventory`] without touching the cluster API. The
    /// coalescer calls this for every watch event after the initial
    /// snapshot is established. Returns `Err` if no inventory has been
    /// bootstrapped — the caller must fall back to
    /// `sync_services_from_api` for re-bootstrap.
    pub async fn sync_services_from_cached_inventory(&self) -> Result<usize> {
        let Some(specs_with_count) = ({
            let guard = lock_recover(&self.service_route_inventory);
            guard.as_ref().map(|inv| {
                let specs = inv.to_specs();
                let len = specs.len();
                (specs, len)
            })
        }) else {
            anyhow::bail!("sync_services_from_cached_inventory: inventory not bootstrapped yet");
        };
        let (specs, svc_count) = specs_with_count;
        let previous_specs = lock_recover(&self.applied_service_specs).clone();
        let plan = super::RoutePlan::diff(&previous_specs, &specs);
        if plan.is_empty() {
            tracing::debug!(
                "nft service route plan empty: {} services; skipping cached inventory render",
                svc_count
            );
            return Ok(svc_count);
        }
        self.replace_services(&specs).await?;
        Ok(svc_count)
    }

    /// Apply a single Service watch event to the cached inventory. Returns
    /// whether the inventory changed (so the coalescer can skip the nft
    /// rewrite entirely when nothing changed).
    pub fn apply_service_event_to_inventory(
        &self,
        namespace: &str,
        name: &str,
        resource_version: i64,
        deleted: bool,
        data: Option<serde_json::Value>,
    ) -> Option<super::inventory::InventoryApply> {
        let mut guard = lock_recover(&self.service_route_inventory);
        let inv = guard.as_mut()?;
        Some(inv.apply_service_event(namespace, name, resource_version, deleted, data))
    }

    /// Apply a single Endpoints watch event to the cached inventory.
    pub fn apply_endpoints_event_to_inventory(
        &self,
        namespace: &str,
        name: &str,
        resource_version: i64,
        deleted: bool,
        data: Option<serde_json::Value>,
    ) -> Option<super::inventory::InventoryApply> {
        let mut guard = lock_recover(&self.service_route_inventory);
        let inv = guard.as_mut()?;
        Some(inv.apply_endpoints_event(namespace, name, resource_version, deleted, data))
    }

    /// Apply a single EndpointSlice watch event to the cached inventory.
    pub fn apply_endpoint_slice_event_to_inventory(
        &self,
        namespace: &str,
        service_name: &str,
        slice_name: &str,
        resource_version: i64,
        deleted: bool,
        data: Option<serde_json::Value>,
    ) -> Option<super::inventory::InventoryApply> {
        let mut guard = lock_recover(&self.service_route_inventory);
        let inv = guard.as_mut()?;
        Some(inv.apply_endpoint_slice_event(
            namespace,
            service_name,
            slice_name,
            resource_version,
            deleted,
            data,
        ))
    }

    /// Rebuild the `services` chain from the supplied
    /// service inventory. Each [`ServiceSpec`] contributes one or more
    /// DNAT rules — one for each endpoint of each service port, plus a
    /// NodePort variant if the service exposes one.
    ///
    /// Empty `services` is valid: the chain is flushed and left empty,
    /// which means no service-VIP DNAT happens until the next sync.
    pub async fn replace_services(&self, services: &[ServiceSpec]) -> Result<()> {
        let snapshot = ServiceRuleSnapshot::from_services(services);
        if lock_recover(&self.service_snapshot).as_ref() == Some(&snapshot) {
            tracing::debug!(
                "nft services chain unchanged: {} services; skipping replace",
                services.len()
            );
            return Ok(());
        }

        let table = self.table();
        let services_chain = Chain::new(SERVICES_CHAIN, &table);
        let service_ct_guard_chain = Chain::new(SERVICE_CT_GUARD_CHAIN, &table);

        let mut rules: Vec<Rule<'_>> = Vec::new();
        for svc in services {
            self.append_service_rules(&services_chain, svc, &mut rules);
        }
        let previous_specs = lock_recover(&self.applied_service_specs).clone();
        let previous_tuples = service_ct_guard_tuples(&previous_specs);
        let guard_transition = service_ct_guard_transition(&previous_specs, services);

        let rule_count = rules.len();
        // The services chain is referenced by `jump services` from
        // nat-prerouting and nat-output, while service_ct_guard is referenced
        // from filter-forward. We can't DEL+ADD either chain (EBUSY), so both
        // chains are flushed in place. Stage the union of old and new guard
        // tuples before switching DNAT, then remove obsolete tuples after the
        // DNAT batch succeeds. Every partial outcome therefore remains safe:
        // old DNAT + union and new DNAT + union both preserve connectivity,
        // and a retry can finish the same idempotent transition.
        if guard_transition.staged != previous_tuples {
            let staged_rules = self.service_ct_guard_rules_for_tuples(
                &service_ct_guard_chain,
                &guard_transition.staged,
            );
            let mut guard_batch = Batch::new();
            guard_batch.replace_chain_rules(&service_ct_guard_chain, &staged_rules);
            self.nf
                .send(guard_batch)
                .await
                .context("stage service conntrack guard rules")?;
        }

        let mut batch = Batch::new();
        batch.replace_chain_rules(&services_chain, &rules);
        self.nf
            .send(batch)
            .await
            .context("replace services chain rules")?;

        if guard_transition.finalized != guard_transition.staged {
            let finalized_rules = self.service_ct_guard_rules_for_tuples(
                &service_ct_guard_chain,
                &guard_transition.finalized,
            );
            let mut guard_batch = Batch::new();
            guard_batch.replace_chain_rules(&service_ct_guard_chain, &finalized_rules);
            self.nf
                .send(guard_batch)
                .await
                .context("finalize service conntrack guard rules")?;
        }

        tracing::debug!(
            "nft services chain replaced: {} services, {} rules",
            services.len(),
            rule_count
        );
        *lock_recover(&self.service_snapshot) = Some(snapshot);
        *lock_recover(&self.applied_service_specs) = services.to_vec();
        Ok(())
    }

    async fn replace_service_ct_guard(&self, services: &[ServiceSpec]) -> Result<()> {
        let table = self.table();
        let chain = Chain::new(SERVICE_CT_GUARD_CHAIN, &table);
        let rules = self.service_ct_guard_rules(&chain, services);
        self.nf
            .replace_chain_rules(&chain, &rules)
            .await
            .context("replace service conntrack guard rules")?;
        Ok(())
    }

    pub async fn replace_network_policies(&self, plan: &NetworkPolicyPlan) -> Result<()> {
        if lock_recover(&self.network_policy_snapshot).as_ref() == Some(plan) {
            tracing::debug!("nft network-policy chain unchanged; skipping replace");
            return Ok(());
        }

        let table = self.table();
        let chain = Chain::new(NETWORK_POLICY_CHAIN, &table);
        let rules = self.network_policy_rules(&chain, plan);
        let rule_count = rules.len();
        self.nf
            .replace_chain_rules(&chain, &rules)
            .await
            .context("replace network-policy chain rules")?;
        tracing::debug!(
            "nft network-policy chain replaced: {} isolated ingress pods, {} isolated egress pods, {} allowed flows, {} rules",
            plan.isolated_ingress.len(),
            plan.isolated_egress.len(),
            plan.allowed_flows.len(),
            rule_count
        );
        *lock_recover(&self.network_policy_snapshot) = Some(plan.clone());
        Ok(())
    }

    /// Drop the entire table in one syscall. Best-effort: a missing
    /// table is logged as a warning but not propagated as an error,
    /// because shutdown should never fail just because cleanup state
    /// is already gone.
    pub async fn cleanup(&self) -> Result<()> {
        let table = self.table();
        let mut batch = Batch::new();
        batch.add(&table, MsgType::Del);
        match self.nf.send(batch).await {
            Ok(()) => {
                tracing::info!(
                    "Dropped inet {} nftables table",
                    self.table_name.to_string_lossy()
                );
                Ok(())
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to drop inet {} table (may not exist): {}",
                    self.table_name.to_string_lossy(),
                    e
                );
                Ok(())
            }
        }
    }

    /// The table name as a `CStr`, for callers that want to issue
    /// custom batches against this table.
    pub fn table_name(&self) -> &CStr {
        &self.table_name
    }

    async fn cleanup_legacy_unscoped_tables(&self) {
        let current_table = self.table_name.to_string_lossy();
        for stale_table in
            super::service_rules::legacy_unscoped_service_tables_to_cleanup(&current_table)
        {
            let Ok(stale_name) = CString::new(stale_table) else {
                continue;
            };
            let table = Table::new(stale_name.as_c_str(), ProtoFamily::Inet);
            let mut batch = Batch::new();
            batch.add(&table, MsgType::Del);
            match self.nf.send(batch).await {
                Ok(()) => tracing::info!(
                    table = stale_table,
                    current = %current_table,
                    "dropped legacy unscoped service-routing nft table"
                ),
                Err(e) => tracing::debug!(
                    table = stale_table,
                    current = %current_table,
                    "legacy unscoped service-routing nft table not removed: {e:#}"
                ),
            }
        }
    }

    async fn reconcile_host_forward_compat(&self) -> Result<()> {
        if !self.host_forward_compat_enabled {
            return Ok(());
        }
        self.reconcile_forward_compat_chain(
            HOST_FORWARD_COMPAT_FAMILY,
            HOST_FORWARD_COMPAT_TABLE,
            HOST_FORWARD_COMPAT_CHAIN,
            HOST_FORWARD_COMPAT_COMMENT,
        )
        .await
    }

    pub(crate) async fn reconcile_forward_compat_chain(
        &self,
        family: &str,
        table: &str,
        chain: &str,
        comment: &str,
    ) -> Result<()> {
        let list = self
            .run_nft(
                "nft_forward_compat_list",
                nft_args(["-j", "-a", "list", "chain", family, table, chain]),
            )
            .await?;
        if !list.status.success() {
            let stderr = String::from_utf8_lossy(&list.stderr);
            tracing::debug!(
                family,
                table,
                chain,
                stderr = %stderr.trim(),
                "host FORWARD compatibility chain absent; skipping"
            );
            return Ok(());
        }

        let handles = forward_compat_rule_handles(&list.stdout, comment)
            .context("parse host FORWARD compatibility rules")?;
        for handle in handles {
            let handle_arg = handle.to_string();
            let delete = self
                .run_nft(
                    "nft_forward_compat_delete",
                    nft_args([
                        "delete",
                        "rule",
                        family,
                        table,
                        chain,
                        "handle",
                        &handle_arg,
                    ]),
                )
                .await?;
            if !delete.status.success() {
                let stderr = String::from_utf8_lossy(&delete.stderr);
                tracing::debug!(
                    family,
                    table,
                    chain,
                    handle,
                    stderr = %stderr.trim(),
                    "stale host FORWARD compatibility rule already absent"
                );
            }
        }

        let bridge = self
            .bridge_ifname
            .to_str()
            .context("bridge interface name is not valid UTF-8")?;
        let pod_cidr = format!(
            "{}/{}",
            self.pod_subnet_ip,
            ipv4_prefix_len(self.pod_subnet_mask)
        );

        self.add_forward_compat_rule(
            family,
            table,
            chain,
            ["iifname", bridge, "ip", "saddr", &pod_cidr],
            comment,
        )
        .await?;
        self.add_forward_compat_rule(
            family,
            table,
            chain,
            ["oifname", bridge, "ip", "daddr", &pod_cidr],
            comment,
        )
        .await?;

        Ok(())
    }

    async fn add_forward_compat_rule<const N: usize>(
        &self,
        family: &str,
        table: &str,
        chain: &str,
        matches: [&str; N],
        comment: &str,
    ) -> Result<()> {
        let mut args = vec![
            "add".to_string(),
            "rule".to_string(),
            family.to_string(),
            table.to_string(),
            chain.to_string(),
        ];
        args.extend(matches.into_iter().map(str::to_string));
        args.extend([
            "accept".to_string(),
            "comment".to_string(),
            comment.to_string(),
        ]);

        let output = self.run_nft("nft_forward_compat_add", args).await?;
        if output.status.success() {
            return Ok(());
        }

        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "add host FORWARD compatibility rule to {family} {table} {chain} failed: {}",
            stderr.trim()
        );
    }

    async fn run_nft(&self, name: &'static str, args: Vec<String>) -> Result<std::process::Output> {
        self.nf.nft_output(name, args).await
    }

    fn table(&self) -> Table {
        Table::new(self.table_name(), ProtoFamily::Inet)
    }

    // ---- Rule builders ------------------------------------------------

    fn rule_ct_established_accept<'a>(&self, chain: &'a Chain<'a>) -> Rule<'a> {
        let mut rule = Rule::new(chain);
        rule.add_expr(&nft_expr!(ct state));
        let mask: u32 = States::ESTABLISHED.bits() | States::RELATED.bits();
        rule.add_expr(&Bitwise::new(mask, 0u32));
        rule.add_expr(&Cmp::new(CmpOp::Neq, 0u32));
        rule.add_expr(&nft_expr!(verdict accept));
        rule
    }

    /// `ct state invalid drop` — drops packets that don't fit any
    /// tracked conntrack entry. Forged sequence numbers, packets after
    /// RST, fragments outside an established flow, etc. Standard
    /// firewall hygiene; runs before any accept rules so invalid
    /// packets can't sneak through on the back of a saddr/daddr match.
    fn rule_ct_invalid_drop<'a>(&self, chain: &'a Chain<'a>) -> Rule<'a> {
        let mut rule = Rule::new(chain);
        rule.add_expr(&nft_expr!(ct state));
        // States::INVALID is a single bit; mask + neq 0 == "invalid set".
        rule.add_expr(&Bitwise::new(States::INVALID.bits(), 0u32));
        rule.add_expr(&Cmp::new(CmpOp::Neq, 0u32));
        rule.add_expr(&nft_expr!(verdict drop));
        rule
    }

    /// Generic "ip {s,d}addr {==,!=} pod-subnet" match — used by both
    /// the forward-chain accept rules and (with `CmpOp::Neq`) the
    /// postrouting masquerade rule. Single source of truth for
    /// pod-subnet matching across the table.
    fn rule_ip_in_subnet<'a>(
        &self,
        chain: &'a Chain<'a>,
        field: Ipv4HeaderField,
        op: CmpOp,
        verdict: Verdict,
    ) -> Rule<'a> {
        let mut rule = Rule::new(chain);
        self.add_subnet_match(&mut rule, field, op);
        verdict.append_to(&mut rule);
        rule
    }

    fn rule_pod_to_external_masquerade<'a>(&self, chain: &'a Chain<'a>) -> Rule<'a> {
        let mut rule = Rule::new(chain);
        // saddr in pod subnet (this is a local pod packet)
        self.add_subnet_match(&mut rule, Ipv4HeaderField::Saddr, CmpOp::Eq);
        // daddr NOT in cluster CIDR — prevents masquerading cross-node VXLAN traffic
        // (remote pod subnets are in cluster_cidr but not in this node's pod_subnet)
        self.add_cidr_match(&mut rule, Ipv4HeaderField::Daddr, CmpOp::Neq);
        rule.add_expr(&Masquerade);
        rule
    }

    /// SNAT service-DNATed node/external traffic to pod endpoints.
    ///
    /// Host-originated ClusterIP calls and external NodePort/hostPort calls
    /// can DNAT to a remote pod endpoint. Without SNAT, the backend pod replies
    /// directly to the node's host address instead of back through the overlay
    /// route, so the original connection times out. This rule is limited to
    /// flows that actually went through DNAT and whose post-DNAT destination is
    /// still inside the cluster pod CIDR, leaving direct pod-to-pod traffic
    /// untouched.
    fn rule_service_dnat_node_snat<'a>(&self, chain: &'a Chain<'a>) -> Rule<'a> {
        let mut rule = Rule::new(chain);
        // Source is not a local pod. Local pod service hairpin traffic is
        // handled by the more specific hairpin rule above.
        self.add_subnet_match(&mut rule, Ipv4HeaderField::Saddr, CmpOp::Neq);
        // Destination after DNAT is a pod endpoint in the cluster CIDR.
        self.add_cidr_match(&mut rule, Ipv4HeaderField::Daddr, CmpOp::Eq);
        Self::add_ct_status_dnat_match(&mut rule);
        rule.add_expr(&Immediate::new(
            self.pod_gateway_ip.octets(),
            Register::Reg1,
        ));
        rule.add_expr(&Nat {
            nat_type: NatType::SNat,
            family: ProtoFamily::Ipv4,
            ip_register: Register::Reg1,
            port_register: None,
        });
        rule
    }

    /// Hairpin SNAT for service-DNATed pod traffic.
    ///
    /// Without this, a pod querying a ClusterIP service whose backend is also in
    /// the pod subnet can receive replies directly from backend PodIP (source
    /// mismatch) instead of the ClusterIP, which breaks DNS resolvers and
    /// service proxy conformance.
    fn rule_service_dnat_hairpin_masquerade<'a>(&self, chain: &'a Chain<'a>) -> Rule<'a> {
        let mut rule = Rule::new(chain);
        // Source pod is local.
        self.add_subnet_match(&mut rule, Ipv4HeaderField::Saddr, CmpOp::Eq);
        // Destination after DNAT is also a local pod.
        self.add_subnet_match(&mut rule, Ipv4HeaderField::Daddr, CmpOp::Eq);
        // Only masquerade flows that went through DNAT (service VIP/NodePort path),
        // not direct pod-to-pod traffic.
        Self::add_ct_status_dnat_match(&mut rule);
        rule.add_expr(&Masquerade);
        rule
    }

    fn add_ct_status_dnat_match(rule: &mut Rule<'_>) {
        rule.add_expr(&nft_expr!(ct status));
        // Match nft(8)'s `ct status dnat` encoding exactly. Newer kernels
        // reject adding IPS_DST_NAT_DONE to this mask in forward-chain rules.
        rule.add_expr(&Bitwise::new(ConntrackStatus::DST_NAT.bits(), 0u32));
        rule.add_expr(&Cmp::new(CmpOp::Neq, 0u32));
    }

    /// Adds the `payload + bitwise + cmp` triplet for matching one IP
    /// header field against the configured pod subnet. Reused by every
    /// chain in this module (and future per-service rules that need
    /// "source is in pod subnet" predicates).
    fn add_subnet_match(&self, rule: &mut Rule, field: Ipv4HeaderField, op: CmpOp) {
        rule.add_expr(&Payload::Network(NetworkHeaderField::Ipv4(field)));
        rule.add_expr(&Bitwise::new(self.pod_subnet_mask, 0u32));
        rule.add_expr(&Cmp::new(op, self.pod_subnet_ip));
    }

    /// Same as `add_subnet_match` but uses cluster CIDR instead of pod subnet.
    /// Used by the masquerade rule to exclude ALL cross-node pod traffic, not just
    /// traffic within this node's /24. Without this, traffic from a local pod
    /// (saddr in pod_subnet) to a remote pod (daddr in cluster_cidr but NOT in
    /// pod_subnet) would be incorrectly masqueraded, breaking VXLAN forwarding.
    fn add_cidr_match(&self, rule: &mut Rule, field: Ipv4HeaderField, op: CmpOp) {
        rule.add_expr(&Payload::Network(NetworkHeaderField::Ipv4(field)));
        rule.add_expr(&Bitwise::new(self.cluster_cidr_mask, 0u32));
        rule.add_expr(&Cmp::new(op, self.cluster_cidr_ip));
    }

    fn add_exact_ip_match(rule: &mut Rule, field: Ipv4HeaderField, ip: Ipv4Addr) {
        rule.add_expr(&Payload::Network(NetworkHeaderField::Ipv4(field)));
        rule.add_expr(&Cmp::new(CmpOp::Eq, ip));
    }

    fn add_policy_cidr_match(
        rule: &mut Rule,
        field: Ipv4HeaderField,
        cidr: Ipv4CidrMatch,
        op: CmpOp,
    ) {
        rule.add_expr(&Payload::Network(NetworkHeaderField::Ipv4(field)));
        rule.add_expr(&Bitwise::new(cidr.mask, 0u32));
        rule.add_expr(&Cmp::new(op, cidr.network));
    }

    fn network_policy_rules<'a>(
        &self,
        chain: &'a Chain<'a>,
        plan: &NetworkPolicyPlan,
    ) -> Vec<Rule<'a>> {
        let mut rules = Vec::new();
        for flow in &plan.allowed_flows {
            rules.push(self.rule_network_policy_allow(chain, flow));
        }
        for pod_ip in &plan.isolated_ingress {
            rules.push(self.rule_network_policy_drop(
                chain,
                NetworkPolicyDirection::Ingress,
                *pod_ip,
            ));
        }
        for pod_ip in &plan.isolated_egress {
            rules.push(self.rule_network_policy_drop(
                chain,
                NetworkPolicyDirection::Egress,
                *pod_ip,
            ));
        }
        let mut return_rule = Rule::new(chain);
        return_rule.add_expr(&nft_expr!(verdict return));
        rules.push(return_rule);
        rules
    }

    fn rule_network_policy_allow<'a>(
        &self,
        chain: &'a Chain<'a>,
        flow: &super::network_policy::NetworkPolicyFlow,
    ) -> Rule<'a> {
        let mut rule = Rule::new(chain);
        match flow.direction {
            NetworkPolicyDirection::Ingress => {
                Self::add_exact_ip_match(&mut rule, Ipv4HeaderField::Daddr, flow.pod_ip);
                self.add_network_policy_peer_match(&mut rule, Ipv4HeaderField::Saddr, &flow.peer);
            }
            NetworkPolicyDirection::Egress => {
                Self::add_exact_ip_match(&mut rule, Ipv4HeaderField::Saddr, flow.pod_ip);
                self.add_network_policy_peer_match(&mut rule, Ipv4HeaderField::Daddr, &flow.peer);
            }
        }
        if let Some(port) = flow.port {
            rule.add_expr(&Payload::Network(NetworkHeaderField::Ipv4(
                Ipv4HeaderField::Protocol,
            )));
            rule.add_expr(&Cmp::new(CmpOp::Eq, port.protocol.ip_proto()));
            Self::add_transport_port_match(&mut rule, port.protocol, port.port, port.end_port);
        }
        rule.add_expr(&nft_expr!(verdict accept));
        rule
    }

    fn add_transport_port_match(
        rule: &mut Rule,
        protocol: Protocol,
        start_port: u16,
        end_port: u16,
    ) {
        rule.add_expr(&Payload::Transport(protocol.dport_field()));
        if start_port == end_port {
            rule.add_expr(&Cmp::new(CmpOp::Eq, start_port.to_be()));
            return;
        }
        rule.add_expr(&Cmp::new(CmpOp::Gte, start_port.to_be()));
        rule.add_expr(&Payload::Transport(protocol.dport_field()));
        rule.add_expr(&Cmp::new(CmpOp::Lte, end_port.to_be()));
    }

    fn add_network_policy_peer_match(
        &self,
        rule: &mut Rule,
        field: Ipv4HeaderField,
        peer: &NetworkPolicyPeerMatch,
    ) {
        match peer {
            NetworkPolicyPeerMatch::Any => {}
            NetworkPolicyPeerMatch::IpBlock { cidr, except } => {
                Self::add_policy_cidr_match(rule, field, *cidr, CmpOp::Eq);
                for excluded in except {
                    Self::add_policy_cidr_match(rule, field, *excluded, CmpOp::Neq);
                }
            }
        }
    }

    fn rule_network_policy_drop<'a>(
        &self,
        chain: &'a Chain<'a>,
        direction: NetworkPolicyDirection,
        pod_ip: Ipv4Addr,
    ) -> Rule<'a> {
        let mut rule = Rule::new(chain);
        match direction {
            NetworkPolicyDirection::Ingress => {
                Self::add_exact_ip_match(&mut rule, Ipv4HeaderField::Daddr, pod_ip);
            }
            NetworkPolicyDirection::Egress => {
                Self::add_exact_ip_match(&mut rule, Ipv4HeaderField::Saddr, pod_ip);
            }
        }
        rule.add_expr(&nft_expr!(verdict drop));
        rule
    }

    // ---- Service routing helpers --------------------------------------

    /// `jump <target>` — used by nat-prerouting and nat-output base
    /// chains to dispatch into the regular `services` and `hostports`
    /// chains. One rule shape, parameterized by target chain name.
    fn rule_jump_to_chain<'a>(&self, chain: &'a Chain<'a>, target: &CStr) -> Rule<'a> {
        let mut rule = Rule::new(chain);
        // nft_expr!(verdict jump $expr) wants `Into<CString>`. CStr
        // implements that via .into() copying the bytes.
        let target: CString = target.into();
        rule.add_expr(&nft_expr!(verdict jump target));
        rule
    }

    fn rule_jump_service_ct_guard_for_iif<'a>(&self, chain: &'a Chain<'a>) -> Rule<'a> {
        let mut rule = Rule::new(chain);
        Self::add_ct_status_dnat_match(&mut rule);
        rule.add_expr(&Meta::IifName);
        rule.add_expr(&Cmp::new(
            CmpOp::Eq,
            InterfaceName::Exact(self.bridge_ifname.clone()),
        ));
        let target: CString = SERVICE_CT_GUARD_CHAIN.into();
        rule.add_expr(&nft_expr!(verdict jump target));
        rule
    }

    fn rule_jump_service_ct_guard_for_oif<'a>(&self, chain: &'a Chain<'a>) -> Rule<'a> {
        let mut rule = Rule::new(chain);
        Self::add_ct_status_dnat_match(&mut rule);
        rule.add_expr(&Meta::OifName);
        rule.add_expr(&Cmp::new(
            CmpOp::Eq,
            InterfaceName::Exact(self.bridge_ifname.clone()),
        ));
        let target: CString = SERVICE_CT_GUARD_CHAIN.into();
        rule.add_expr(&nft_expr!(verdict jump target));
        rule
    }

    fn service_ct_guard_rules<'a>(
        &self,
        chain: &'a Chain<'a>,
        services: &[ServiceSpec],
    ) -> Vec<Rule<'a>> {
        self.service_ct_guard_rules_for_tuples(chain, &service_ct_guard_tuples(services))
    }

    fn service_ct_guard_rules_for_tuples<'a>(
        &self,
        chain: &'a Chain<'a>,
        tuples: &[ServiceCtTuple],
    ) -> Vec<Rule<'a>> {
        let mut rules = Vec::new();
        for tuple in tuples {
            rules.push(self.rule_return_active_service_ct_tuple(chain, *tuple));
        }
        rules.push(self.rule_drop_stale_service_ct_mapping(chain));
        rules.push(self.rule_return_from_service_ct_guard(chain));
        rules
    }

    fn rule_return_active_service_ct_tuple<'a>(
        &self,
        chain: &'a Chain<'a>,
        tuple: ServiceCtTuple,
    ) -> Rule<'a> {
        let mut rule = Rule::new(chain);
        rule.add_expr(&CtOriginal::new(CtOriginalKey::DstIp));
        rule.add_expr(&Cmp::new(CmpOp::Eq, tuple.cluster_ip));
        rule.add_expr(&CtOriginal::new(CtOriginalKey::Protocol));
        rule.add_expr(&Cmp::new(CmpOp::Eq, tuple.protocol.ip_proto()));
        rule.add_expr(&CtOriginal::new(CtOriginalKey::ProtoDst));
        rule.add_expr(&Cmp::new(CmpOp::Eq, tuple.service_port.to_be()));
        rule.add_expr(&nft_expr!(verdict return));
        rule
    }

    fn rule_drop_stale_service_ct_mapping<'a>(&self, chain: &'a Chain<'a>) -> Rule<'a> {
        let mut rule = Rule::new(chain);
        rule.add_expr(&CtOriginal::new(CtOriginalKey::DstIp));
        rule.add_expr(&Bitwise::new(self.service_cidr_mask, 0u32));
        rule.add_expr(&Cmp::new(CmpOp::Eq, self.service_cidr_ip));
        rule.add_expr(&nft_expr!(verdict drop));
        rule
    }

    fn rule_return_from_service_ct_guard<'a>(&self, chain: &'a Chain<'a>) -> Rule<'a> {
        let mut rule = Rule::new(chain);
        rule.add_expr(&nft_expr!(verdict return));
        rule
    }

    // ---- Hostport helpers ---------------------------------------------

    /// Add (or replace) a pod's hostport rules in the per-table registry,
    /// then re-emit the entire hostports chain. Idempotent for a given
    /// pod_ip + specs combination.
    ///
    /// Holds the per-table hostport lock across the `(mutate registry +
    /// snapshot + send batch)` sequence so concurrent callers can't
    /// observe each other's intermediate state and emit chains based on
    /// stale snapshots. See [`hostport_lock_for`] for the race scenario.
    pub(crate) async fn add_hostports_for_pod(
        &self,
        pod_ip: Ipv4Addr,
        specs: Vec<HostPortSpec>,
    ) -> Result<()> {
        if specs.is_empty() {
            // Treat empty list as a remove — keeps the registry tidy
            // and avoids leaving entries that produce no rules.
            return self.remove_hostports_for_pod(pod_ip).await;
        }
        let lock = self.hostport_lock.clone();
        let _guard = lock.lock().await;
        let Some(prior) = self.hostport_insert(pod_ip, specs) else {
            return Ok(());
        };
        let result = self.flush_hostports_chain().await;
        if result.is_err() {
            self.hostport_restore(pod_ip, prior);
        }
        result
    }

    /// Remove all hostport rules for a pod and re-emit the chain.
    /// Idempotent — a remove of an unknown pod is a no-op (no chain
    /// rewrite, since nothing changed).
    ///
    /// Holds the per-table hostport lock for the same reason
    /// [`add_hostports_for_pod`] does — the (mutate + snapshot + send)
    /// sequence is the critical section.
    pub(crate) async fn remove_hostports_for_pod(&self, pod_ip: Ipv4Addr) -> Result<()> {
        let lock = self.hostport_lock.clone();
        let _guard = lock.lock().await;
        let prior = lock_recover(&self.hostport_registry).remove(&pod_ip);
        let Some(prior) = prior else {
            return Ok(());
        };
        let result = self.flush_hostports_chain().await;
        if result.is_err() {
            self.hostport_restore(pod_ip, Some(prior));
        }
        result
    }

    /// Re-emit the hostports chain from the current registry contents
    /// for THIS table. Used by add/remove and by the integration tests
    /// directly. Atomic via replace_chain_rules (handleless DELRULE
    /// flush + new rules in one batch) so observers never see a partial
    /// chain.
    async fn flush_hostports_chain(&self) -> Result<()> {
        #[cfg(test)]
        {
            self.hostport_flush_attempts
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if let Some(result) = lock_recover(&self.hostport_flush_results).pop_front() {
                return result;
            }
        }
        let table = self.table();
        let chain = Chain::new(HOSTPORTS_CHAIN, &table);

        let snapshot = self.hostport_snapshot();

        let mut rules: Vec<Rule<'_>> = Vec::new();
        for (pod_ip, specs) in &snapshot {
            for spec in specs {
                rules.push(self.rule_dnat_hostport(&chain, *pod_ip, spec));
            }
        }

        let count = rules.len();
        self.nf
            .replace_chain_rules(&chain, &rules)
            .await
            .context("replace hostports chain rules")?;

        tracing::debug!(
            "nft hostports chain replaced: {} pods, {} rules",
            snapshot.len(),
            count
        );
        Ok(())
    }

    /// Build one hostport DNAT rule. Same field-by-field shape as the
    /// service-routing DNAT (rule_dnat_endpoint), differing only in:
    ///   - dport matched is the host_port (not service_port)
    ///   - daddr match is host_ip (the host's address, not the cluster IP)
    ///   - destination is pod_ip:container_port
    ///   - no probability ladder (one rule per pod hostport mapping)
    fn rule_dnat_hostport<'a>(
        &self,
        chain: &'a Chain<'a>,
        pod_ip: Ipv4Addr,
        spec: &HostPortSpec,
    ) -> Rule<'a> {
        let mut rule = Rule::new(chain);

        // (1) Optional ip daddr match — only present if hostIP is set
        //     to a specific IP. Absent (None) means match any
        //     destination on this hostPort.
        if let Some(host_ip) = spec.host_ip {
            rule.add_expr(&Payload::Network(NetworkHeaderField::Ipv4(
                Ipv4HeaderField::Daddr,
            )));
            rule.add_expr(&Cmp::new(CmpOp::Eq, host_ip));
        }

        // (2) Match L4 protocol.
        rule.add_expr(&Payload::Network(NetworkHeaderField::Ipv4(
            Ipv4HeaderField::Protocol,
        )));
        rule.add_expr(&Cmp::new(CmpOp::Eq, spec.protocol.ip_proto()));

        // (3) Match destination port = host_port.
        rule.add_expr(&Payload::Transport(spec.protocol.dport_field()));
        rule.add_expr(&Cmp::new(CmpOp::Eq, spec.host_port.to_be()));

        // (4) DNAT to pod_ip:container_port.
        Self::append_dnat_tail(&mut rule, pod_ip, spec.container_port);

        rule
    }

    // ---- Remote rootless pod endpoint helpers -------------------------

    pub async fn replace_remote_pod_endpoints(
        &self,
        specs: &[RemotePodEndpointSpec],
    ) -> Result<()> {
        let lock = self.remote_pod_lock.clone();
        let _guard = lock.lock().await;
        let prior = self.remote_pod_replace_all(specs);
        let result = self.flush_remote_pod_endpoints_chain().await;
        self.finish_remote_pod_flush(prior, result)
    }

    pub async fn sync_remote_pod_endpoints_from_topology(
        &self,
        local_node_name: &str,
        endpoints: &[klights_network_api::PodEndpointTopology],
    ) -> Result<usize> {
        let specs = remote_pod_endpoint_specs_from_topology(local_node_name, endpoints);
        let count = specs.len();
        self.replace_remote_pod_endpoints(&specs).await?;
        Ok(count)
    }

    pub async fn upsert_remote_pod_endpoint(
        &self,
        local_node_name: &str,
        endpoint: klights_network_api::PodEndpointTopology,
    ) -> Result<()> {
        let pod_ip = endpoint.pod_ip();
        let specs = remote_pod_endpoint_specs_from_topology(local_node_name, &[endpoint]);
        let lock = self.remote_pod_lock.clone();
        let _guard = lock.lock().await;
        if let Some(prior) = self.remote_pod_upsert(pod_ip, specs) {
            let result = self.flush_remote_pod_endpoints_chain().await;
            if result.is_err() {
                self.remote_pod_restore_entry(pod_ip, prior);
            }
            result?;
        }
        Ok(())
    }

    pub async fn remove_remote_pod_endpoint(&self, pod_ip: Ipv4Addr) -> Result<()> {
        let lock = self.remote_pod_lock.clone();
        let _guard = lock.lock().await;
        if let Some(prior) = self.remote_pod_upsert(pod_ip, Vec::new()) {
            let result = self.flush_remote_pod_endpoints_chain().await;
            if result.is_err() {
                self.remote_pod_restore_entry(pod_ip, prior);
            }
            result?;
        }
        Ok(())
    }

    async fn flush_remote_pod_endpoints_chain(&self) -> Result<()> {
        #[cfg(test)]
        if let Some(result) = lock_recover(&self.remote_flush_results).pop_front() {
            return result;
        }
        let table = self.table();
        let chain = Chain::new(REMOTE_POD_ENDPOINTS_CHAIN, &table);
        let snapshot = self.remote_pod_snapshot();
        let rules: Vec<Rule<'_>> = snapshot
            .iter()
            .map(|spec| self.rule_dnat_remote_pod_endpoint(&chain, spec))
            .collect();
        let count = rules.len();
        self.nf
            .replace_chain_rules(&chain, &rules)
            .await
            .context("replace remote_pod_v4 chain rules")?;
        tracing::debug!("nft remote_pod_v4 chain replaced: {} endpoint rules", count);
        Ok(())
    }

    fn rule_dnat_remote_pod_endpoint<'a>(
        &self,
        chain: &'a Chain<'a>,
        spec: &RemotePodEndpointSpec,
    ) -> Rule<'a> {
        let mut rule = Rule::new(chain);

        rule.add_expr(&Payload::Network(NetworkHeaderField::Ipv4(
            Ipv4HeaderField::Daddr,
        )));
        rule.add_expr(&Cmp::new(CmpOp::Eq, spec.pod_ip));

        rule.add_expr(&Payload::Network(NetworkHeaderField::Ipv4(
            Ipv4HeaderField::Protocol,
        )));
        rule.add_expr(&Cmp::new(CmpOp::Eq, spec.protocol.ip_proto()));

        Self::append_dnat_tail(&mut rule, spec.node_ip, spec.host_port);

        rule
    }

    /// Generate all rules for a single service into the `services` chain.
    /// Each (port, endpoints) pair contributes:
    ///   - One ClusterIP DNAT rule per endpoint (with probability ladder
    ///     for multi-endpoint load balancing via `meta random`)
    ///   - If NodePort/LoadBalancer: same shape but without `ip daddr`
    ///     match (matches any destination on the NodePort)
    ///
    /// Skipped silently:
    ///   - Headless services (cluster_ip = None)
    ///   - ExternalName (no cluster_ip)
    ///   - Ports with no ready endpoints (no DNAT to write)
    fn append_service_rules<'a>(
        &self,
        chain: &'a Chain<'a>,
        svc: &ServiceSpec,
        out: &mut Vec<Rule<'a>>,
    ) {
        match svc.session_affinity {
            SessionAffinity::None => self.append_random_rules(chain, svc, out),
            SessionAffinity::ClientIp => self.append_jhash_rules(chain, svc, out),
        }
    }

    /// Random load balancing via probability ladder (default, no affinity).
    fn append_random_rules<'a>(
        &self,
        chain: &'a Chain<'a>,
        svc: &ServiceSpec,
        out: &mut Vec<Rule<'a>>,
    ) {
        for port in &svc.ports {
            let n = port.endpoints.len();
            if n == 0 {
                continue;
            }
            for (i, ep) in port.endpoints.iter().enumerate() {
                let probability = if n > 1 && i + 1 < n {
                    Some(super::service_rules::probability_for_ladder_step(n - i))
                } else {
                    None
                };
                let endpoint = DnatEndpointTarget::new(*ep, port.target_port);
                out.push(self.rule_dnat_endpoint(
                    chain,
                    DnatEndpointRuleSpec::new(
                        Some(svc.cluster_ip),
                        DnatServicePort::new(port.protocol, port.service_port),
                        endpoint,
                        probability,
                    ),
                ));
                if let Some(node_port) = port.node_port {
                    out.push(self.rule_dnat_endpoint(
                        chain,
                        DnatEndpointRuleSpec::new(
                            None,
                            DnatServicePort::new(port.protocol, node_port),
                            endpoint,
                            probability,
                        ),
                    ));
                }
            }
        }
    }

    /// ClientIP session affinity via `jhash ip saddr mod N` — each client
    /// IP always selects the same backend index deterministically. No
    /// conntrack state needed; the jhash is stateless and consistent.
    ///
    /// nft expression: `jhash ip saddr size 4 seed 0xcafe mod <N>` produces
    /// a u32 in [0, N). Each endpoint i gets rule `<result> == i → dnat`.
    fn append_jhash_rules<'a>(
        &self,
        chain: &'a Chain<'a>,
        svc: &ServiceSpec,
        out: &mut Vec<Rule<'a>>,
    ) {
        for port in &svc.ports {
            let n = port.endpoints.len();
            if n == 0 {
                continue;
            }
            for (i, ep) in port.endpoints.iter().enumerate() {
                // jhash ip saddr size 4 seed 0xcafe mod N → register 1
                let mut rule = Rule::new(chain);

                // Match ClusterIP destination
                rule.add_expr(&Payload::Network(NetworkHeaderField::Ipv4(
                    Ipv4HeaderField::Daddr,
                )));
                rule.add_expr(&Cmp::new(CmpOp::Eq, svc.cluster_ip));

                // Match L4 protocol
                rule.add_expr(&Payload::Network(NetworkHeaderField::Ipv4(
                    Ipv4HeaderField::Protocol,
                )));
                rule.add_expr(&Cmp::new(CmpOp::Eq, port.protocol.ip_proto()));

                // Match service port
                rule.add_expr(&Payload::Transport(port.protocol.dport_field()));
                rule.add_expr(&Cmp::new(CmpOp::Eq, port.service_port.to_be()));

                // Load source IP into Reg1 first, then jhash it.
                // NFT registers: 1 = NFT_REG32_01.
                // The hash expression reads `len` bytes from the payload
                // starting at `offset` within the packet (IP header saddr = offset 12).
                // It stores result in `dreg`.
                // We use Reg1 as both source (payload load) and dest (hash result).
                rule.add_expr(&Payload::Network(NetworkHeaderField::Ipv4(
                    Ipv4HeaderField::Saddr,
                )));
                rule.add_expr(&JhashExpr {
                    sreg: 1, // NFT_REG32_01: source register (payload already loaded here)
                    dreg: 1, // NFT_REG32_01: overwrite with hash result
                    len: 4,  // IPv4 address = 4 bytes
                    modulus: n as u32,
                    seed: 0xcafe_u32,
                    offset: 0, // offset within the loaded data (we just loaded saddr)
                });

                // Compare hash output (endpoint index) to `i`.
                // Jhash result is a register value in host/native integer order;
                // byte-swapping here would make buckets >0 unreachable.
                rule.add_expr(&Cmp::new(CmpOp::Eq, i as u32));

                // DNAT to this endpoint
                Self::append_dnat_tail(&mut rule, *ep, port.target_port);

                out.push(rule);

                // NodePort rules use the same jhash approach
                if let Some(node_port) = port.node_port {
                    let mut np_rule = Rule::new(chain);
                    np_rule.add_expr(&Payload::Network(NetworkHeaderField::Ipv4(
                        Ipv4HeaderField::Protocol,
                    )));
                    np_rule.add_expr(&Cmp::new(CmpOp::Eq, port.protocol.ip_proto()));
                    np_rule.add_expr(&Payload::Transport(port.protocol.dport_field()));
                    np_rule.add_expr(&Cmp::new(CmpOp::Eq, node_port.to_be()));
                    np_rule.add_expr(&Payload::Network(NetworkHeaderField::Ipv4(
                        Ipv4HeaderField::Saddr,
                    )));
                    np_rule.add_expr(&JhashExpr {
                        sreg: 1,
                        dreg: 1,
                        len: 4,
                        modulus: n as u32,
                        seed: 0xcafe_u32,
                        offset: 0,
                    });
                    np_rule.add_expr(&Cmp::new(CmpOp::Eq, i as u32));
                    Self::append_dnat_tail(&mut np_rule, *ep, port.target_port);
                    out.push(np_rule);
                }
            }
        }
    }

    /// Build one DNAT rule. Field-by-field construction is centralized
    /// here so the wire format is in exactly one place — every
    /// service/endpoint variation goes through this single builder.
    ///
    /// `match_daddr` is `Some(cluster_ip)` for ClusterIP rules and
    /// `None` for NodePort rules (which match on the port alone).
    /// `probability` is `Some(threshold)` for non-final endpoints in a
    /// multi-endpoint ladder, `None` for single-endpoint or final-step.
    fn rule_dnat_endpoint<'a>(&self, chain: &'a Chain<'a>, spec: DnatEndpointRuleSpec) -> Rule<'a> {
        let mut rule = Rule::new(chain);

        // (1) Match destination IP if applicable (ClusterIP only; NodePort
        //     deliberately omits this so any destination on the node port
        //     matches).
        if let Some(daddr) = spec.match_daddr {
            rule.add_expr(&Payload::Network(NetworkHeaderField::Ipv4(
                Ipv4HeaderField::Daddr,
            )));
            rule.add_expr(&Cmp::new(CmpOp::Eq, daddr));
        }

        // (2) Match L4 protocol (tcp / udp).
        rule.add_expr(&Payload::Network(NetworkHeaderField::Ipv4(
            Ipv4HeaderField::Protocol,
        )));
        rule.add_expr(&Cmp::new(CmpOp::Eq, spec.service.protocol.ip_proto()));

        // (3) Match destination port (transport-header dport, in network
        //     byte order). u16::to_be on little-endian flips bytes; on
        //     big-endian it's identity. Cmp::new -> ToSlice -> to_ne_bytes
        //     then writes those bytes as-is, so the result on the wire
        //     is the original big-endian representation. Portable.
        rule.add_expr(&Payload::Transport(spec.service.protocol.dport_field()));
        rule.add_expr(&Cmp::new(CmpOp::Eq, spec.service.service_port.to_be()));

        // (4) Probability ladder for multi-endpoint LB. Each non-final
        //     endpoint's threshold is UINT32_MAX/(N-i) — same statistical
        //     pattern kube-proxy uses (cf. iptables `-m statistic --mode
        //     random`). If the cmp fails, the whole rule is skipped and
        //     the next rule (next endpoint) is evaluated.
        if let Some(threshold) = spec.probability {
            rule.add_expr(&nft_expr!(meta random));
            rule.add_expr(&Cmp::new(CmpOp::Lt, threshold));
        }

        // (5) DNAT verdict tail, reading addr from Reg1 and port from Reg2.
        Self::append_dnat_tail(&mut rule, spec.endpoint.ip, spec.endpoint.port);

        rule
    }

    fn append_dnat_tail(rule: &mut Rule<'_>, target_ip: Ipv4Addr, target_port: u16) {
        rule.add_expr(&Immediate::new(target_ip.octets(), Register::Reg1));
        rule.add_expr(&Immediate::new(target_port.to_be_bytes(), Register::Reg2));
        rule.add_expr(&Nat {
            nat_type: NatType::DNat,
            family: ProtoFamily::Ipv4,
            ip_register: Register::Reg1,
            port_register: Some(Register::Reg2),
        });
    }
}

fn validate_linux_ifname(ifname: &CStr) -> Result<()> {
    if ifname.to_bytes().len() >= LINUX_IFNAMSIZ {
        anyhow::bail!(
            "bridge interface name {:?} exceeds Linux IFNAMSIZ-1 ({})",
            ifname,
            LINUX_IFNAMSIZ - 1
        );
    }
    Ok(())
}

#[cfg(test)]
mod remote_endpoint_failure_tests {
    use super::*;

    fn table() -> KlightsTable {
        let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        KlightsTable::with_name(
            Netfilter::new(supervisor).unwrap(),
            "klights-remote-rollback-test",
            PodSubnet::parse("10.42.0.0/24").unwrap(),
            ClusterCidr::parse("10.42.0.0/16").unwrap(),
            ClusterCidr::parse("10.43.0.0/16").unwrap(),
            ServiceRoutingMode::default_root_for_test(),
        )
        .unwrap()
    }

    #[test]
    fn failed_remote_endpoint_flush_restores_prior_registry() {
        let table = table();
        let prior_spec = RemotePodEndpointSpec {
            pod_ip: Ipv4Addr::new(10, 42, 1, 10),
            node_ip: Ipv4Addr::new(192, 0, 2, 10),
            host_port: 31_010,
            protocol: Protocol::Tcp,
        };
        let failed_spec = RemotePodEndpointSpec {
            pod_ip: Ipv4Addr::new(10, 42, 2, 10),
            node_ip: Ipv4Addr::new(192, 0, 2, 20),
            host_port: 31_020,
            protocol: Protocol::Udp,
        };
        table.remote_pod_replace_all(&[prior_spec]);
        let prior = table.remote_pod_replace_all(&[failed_spec]);

        table
            .finish_remote_pod_flush::<()>(prior, Err(anyhow::anyhow!("nft flush failed")))
            .expect_err("deterministic nft failure must remain visible to the worker");

        assert_eq!(table.remote_pod_snapshot(), vec![prior_spec]);
    }

    #[tokio::test]
    async fn failed_hostport_delete_restores_registry_for_identical_retry() {
        let table = table();
        let pod_ip = Ipv4Addr::new(10, 42, 1, 10);
        let spec = HostPortSpec {
            host_ip: None,
            host_port: 30_080,
            container_port: 80,
            protocol: Protocol::Tcp,
        };
        assert!(table.hostport_insert(pod_ip, vec![spec.clone()]).is_some());
        table
            .test_queue_hostport_flush_result(Err(anyhow::anyhow!("injected first flush failure")));
        table.test_queue_hostport_flush_result(Ok(()));

        table
            .remove_hostports_for_pod(pod_ip)
            .await
            .expect_err("first delete must expose the nft flush failure");
        assert_eq!(table.hostport_snapshot(), vec![(pod_ip, vec![spec])]);

        table
            .remove_hostports_for_pod(pod_ip)
            .await
            .expect("identical retry must flush and commit the delete");
        assert!(table.hostport_snapshot().is_empty());
        assert_eq!(table.test_hostport_flush_attempts(), 2);
    }

    #[tokio::test]
    async fn incremental_remote_mutations_rollback_only_the_affected_key() {
        let table = table();
        let unrelated = RemotePodEndpointSpec {
            pod_ip: Ipv4Addr::new(10, 42, 1, 10),
            node_ip: Ipv4Addr::new(192, 0, 2, 10),
            host_port: 31_010,
            protocol: Protocol::Tcp,
        };
        let prior = RemotePodEndpointSpec {
            pod_ip: Ipv4Addr::new(10, 42, 2, 10),
            node_ip: Ipv4Addr::new(192, 0, 2, 20),
            host_port: 31_020,
            protocol: Protocol::Udp,
        };
        let replacement = RemotePodEndpointSpec {
            host_port: 31_021,
            ..prior
        };
        table.remote_pod_replace_all(&[unrelated, prior]);
        let unrelated_ptr = table.test_remote_entry_ptr(unrelated.pod_ip);

        for result in [
            Err(anyhow::anyhow!("injected upsert failure")),
            Ok(()),
            Err(anyhow::anyhow!("injected removal failure")),
            Ok(()),
        ] {
            table.test_queue_remote_flush_result(result);
        }

        table
            .upsert_remote_pod_endpoint(
                "local",
                klights_network_api::PodEndpointTopology::HostPort(
                    klights_network_api::HostPortPodEndpoint::try_new(
                        replacement.pod_ip,
                        "remote",
                        replacement.node_ip,
                        None,
                        Some(replacement.host_port),
                    )
                    .unwrap(),
                ),
            )
            .await
            .expect_err("first incremental upsert must expose flush failure");
        assert_eq!(table.remote_pod_snapshot(), vec![unrelated, prior]);
        assert_eq!(
            table.test_remote_entry_ptr(unrelated.pod_ip),
            unrelated_ptr,
            "rollback must not clone or replace an unrelated registry vector"
        );

        table
            .upsert_remote_pod_endpoint(
                "local",
                klights_network_api::PodEndpointTopology::HostPort(
                    klights_network_api::HostPortPodEndpoint::try_new(
                        replacement.pod_ip,
                        "remote",
                        replacement.node_ip,
                        None,
                        Some(replacement.host_port),
                    )
                    .unwrap(),
                ),
            )
            .await
            .expect("identical upsert retry must commit");
        assert_eq!(table.test_remote_entry_ptr(unrelated.pod_ip), unrelated_ptr);

        table
            .remove_remote_pod_endpoint(replacement.pod_ip)
            .await
            .expect_err("first incremental removal must expose flush failure");
        assert!(
            table
                .remote_pod_snapshot()
                .iter()
                .any(|spec| *spec == replacement)
        );
        assert_eq!(table.test_remote_entry_ptr(unrelated.pod_ip), unrelated_ptr);

        table
            .remove_remote_pod_endpoint(replacement.pod_ip)
            .await
            .expect("identical removal retry must commit");
        assert_eq!(table.remote_pod_snapshot(), vec![unrelated]);
        assert_eq!(table.test_remote_entry_ptr(unrelated.pod_ip), unrelated_ptr);
    }
}

/// The verdict applied at the end of a rule.
#[derive(Copy, Clone)]
enum Verdict {
    Accept,
}

impl Verdict {
    fn append_to(self, rule: &mut Rule) {
        match self {
            Verdict::Accept => rule.add_expr(&nft_expr!(verdict accept)),
        }
    }
}

#[cfg(test)]
mod kernel_compat_tests {
    use super::*;
    use crate::networking::netfilter::{Batch, Netfilter};
    use crate::networking::{ClusterCidr, PodSubnet};
    use std::process::Command;
    use std::sync::Arc;

    struct TableGuard {
        name: String,
    }

    impl Drop for TableGuard {
        fn drop(&mut self) {
            let _ = Command::new("nft")
                .args(["delete", "table", "inet", &self.name])
                .status();
        }
    }

    fn unique_name(prefix: &str) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        format!("{prefix}_{}_{}", std::process::id(), nanos)
    }

    fn test_task_supervisor() -> Arc<klights_supervisor::TaskSupervisor> {
        Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ))
    }

    fn build(name: &str) -> KlightsTable {
        let nf = Netfilter::new(test_task_supervisor()).expect("Netfilter::new");
        KlightsTable::with_name(
            nf,
            name,
            PodSubnet::parse("10.99.0.0/24").expect("static pod cidr"),
            ClusterCidr::parse("10.99.0.0/16").expect("static cluster cidr"),
            ClusterCidr::parse("10.99.128.0/17").expect("static service cidr"),
            ServiceRoutingMode::default_root_for_test(),
        )
        .expect("build table handle")
    }

    #[test]
    fn bridge_ifname_validation_rejects_linux_overlong_names() {
        let valid = CString::new("klights0").unwrap();
        validate_linux_ifname(&valid).expect("short Linux ifname accepted");

        let overlong = CString::new("1234567890abcdef").unwrap();
        assert!(
            validate_linux_ifname(&overlong).is_err(),
            "Linux IFNAMSIZ allows at most 15 visible bytes"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn filter_forward_rules_are_accepted_individually() {
        let name = unique_name("klights_sr_fwd_rules");
        let _guard = TableGuard { name: name.clone() };
        let table_handle = build(&name);
        table_handle
            .nf
            .ensure_table(ProtoFamily::Inet, table_handle.table_name())
            .await
            .expect("ensure table");

        let table = Table::new(table_handle.table_name(), ProtoFamily::Inet);
        let service_ct_guard = Chain::new(SERVICE_CT_GUARD_CHAIN, &table);
        let mut service_ct_guard_batch = Batch::new();
        service_ct_guard_batch.add(&service_ct_guard, MsgType::Add);
        table_handle
            .nf
            .send(service_ct_guard_batch)
            .await
            .expect("create service_ct_guard chain");

        let mut chain = Chain::new(c"filter-forward", &table);
        chain.set_type(ChainType::Filter);
        chain.set_hook(Hook::Forward, PRIORITY_FILTER);

        let mut batch = Batch::new();
        batch.add(&chain, MsgType::Add);
        table_handle.nf.send(batch).await.expect("create chain");

        let cases = [
            ("ct-invalid-drop", table_handle.rule_ct_invalid_drop(&chain)),
            ("ct-status-dnat-drop", {
                let mut rule = Rule::new(&chain);
                KlightsTable::add_ct_status_dnat_match(&mut rule);
                rule.add_expr(&nft_expr!(verdict drop));
                rule
            }),
            ("iifname-accept", {
                let mut rule = Rule::new(&chain);
                rule.add_expr(&Meta::IifName);
                rule.add_expr(&Cmp::new(
                    CmpOp::Eq,
                    InterfaceName::Exact(table_handle.bridge_ifname.clone()),
                ));
                rule.add_expr(&nft_expr!(verdict accept));
                rule
            }),
            ("jump-service-ct-guard", {
                let mut rule = Rule::new(&chain);
                let target: CString = SERVICE_CT_GUARD_CHAIN.into();
                rule.add_expr(&nft_expr!(verdict jump target));
                rule
            }),
            ("ct-status-dnat-iifname-accept", {
                let mut rule = Rule::new(&chain);
                KlightsTable::add_ct_status_dnat_match(&mut rule);
                rule.add_expr(&Meta::IifName);
                rule.add_expr(&Cmp::new(
                    CmpOp::Eq,
                    InterfaceName::Exact(table_handle.bridge_ifname.clone()),
                ));
                rule.add_expr(&nft_expr!(verdict accept));
                rule
            }),
            (
                "jump-service-ct-guard-iif",
                table_handle.rule_jump_service_ct_guard_for_iif(&chain),
            ),
            (
                "jump-service-ct-guard-oif",
                table_handle.rule_jump_service_ct_guard_for_oif(&chain),
            ),
            (
                "ct-established-accept",
                table_handle.rule_ct_established_accept(&chain),
            ),
            (
                "pod-saddr-accept",
                table_handle.rule_ip_in_subnet(
                    &chain,
                    Ipv4HeaderField::Saddr,
                    CmpOp::Eq,
                    Verdict::Accept,
                ),
            ),
            (
                "pod-daddr-accept",
                table_handle.rule_ip_in_subnet(
                    &chain,
                    Ipv4HeaderField::Daddr,
                    CmpOp::Eq,
                    Verdict::Accept,
                ),
            ),
        ];

        for (label, rule) in cases {
            table_handle
                .nf
                .replace_chain_rules(&chain, &[rule])
                .await
                .unwrap_or_else(|err| panic!("{label} rejected by kernel: {err:#}"));
        }
    }
}
