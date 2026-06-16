use crate::control_plane::client::LeaderApiClient;
use crate::datastore::node_local::NodeLocalHandle;
use crate::networking::dataplane_health::DataplaneHealth;
use crate::networking::device_state::{self, LinkKind, LinkState};
use crate::networking::{BridgeName, NodeName, PodSubnet};
use anyhow::{Context, Result};

use futures::stream::TryStreamExt;
use netlink_packet_route::{
    AddressFamily,
    address::{AddressAttribute, AddressMessage},
    link::State as LinkOperState,
};
use std::net::{IpAddr, Ipv4Addr};
use std::str::FromStr;
use std::sync::{Arc, OnceLock};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LinkIpv4Address {
    local: Ipv4Addr,
    prefix_len: u8,
}

fn address_message_ipv4(addr_msg: &AddressMessage) -> Option<LinkIpv4Address> {
    if addr_msg.header.family != AddressFamily::Inet {
        return None;
    }

    let mut local = None;
    for attr in &addr_msg.attributes {
        match attr {
            AddressAttribute::Local(IpAddr::V4(addr))
            | AddressAttribute::Address(IpAddr::V4(addr)) => {
                local.get_or_insert(*addr);
            }
            _ => {}
        }
    }

    local.map(|local| LinkIpv4Address {
        local,
        prefix_len: addr_msg.header.prefix_len,
    })
}

fn stale_down_bridge_pod_subnet_addr_candidate(
    state: &LinkState,
    current_bridge_idx: u32,
    bridge_ip: Ipv4Addr,
    prefix_len: u8,
    addresses: &[LinkIpv4Address],
) -> bool {
    state.ifindex != current_bridge_idx
        && matches!(state.kind, LinkKind::Bridge)
        && link_state_is_down_for_stale_cleanup(state)
        && addresses
            .iter()
            .any(|addr| addr.local == bridge_ip && addr.prefix_len == prefix_len)
}

fn link_state_is_down_for_stale_cleanup(state: &LinkState) -> bool {
    !state.up
        || matches!(
            state.operstate,
            Some(LinkOperState::Down | LinkOperState::LowerLayerDown | LinkOperState::NotPresent)
        )
}

fn is_nl_absent_error(err: &rtnetlink::Error) -> bool {
    match err {
        rtnetlink::Error::NetlinkError(e) => e.code.is_some_and(|code| {
            let code = code.get().abs();
            code == libc::ENODEV || code == libc::ENOENT || code == libc::EADDRNOTAVAIL
        }),
        _ => false,
    }
}

/// Concrete root-mode networking implementation used by klights runtime.
pub struct NetworkPlane {
    rt: rtnetlink::Handle,
    _rt_conn: crate::task_supervisor::SupervisedJoinHandle<()>,
    node_local: NodeLocalHandle,
    bridge: BridgeName,
    pod_subnet: PodSubnet,
    pod_link_mtu: u32,
    my_node: NodeName,
    host_ip: Ipv4Addr,
    _vxlan_vni: u32,
    _vxlan_port: u16,
    wireguard_device: String,
    bridge_idx: OnceLock<u32>,
    wireguard_idx: OnceLock<u32>,
    wireguard: OnceLock<Arc<crate::networking::wireguard::WireGuardController>>,
    health: DataplaneHealth,
    task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
}

impl NetworkPlane {
    /// Boot the shared networking plane. Opens one rtnetlink connection,
    /// prepares the local bridge/CNI datapath, and initializes the selected
    /// cross-node dataplane. WireGuard is the default encrypted dataplane;
    /// VXLAN is not created unless an explicit VXLAN route mode is added and
    /// selected.
    pub async fn boot(
        cfg: &crate::KlightsConfig,
        cluster_api: Arc<dyn LeaderApiClient>,
        node_local: NodeLocalHandle,
        node_ip: &str,
        cancel: tokio_util::sync::CancellationToken,
        task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    ) -> Result<Arc<Self>> {
        let bridge = BridgeName::parse(&cfg.bridge_name)
            .map_err(anyhow::Error::msg)
            .with_context(|| format!("invalid bridge name {}", cfg.bridge_name))?;
        let my_node = NodeName::parse(&cfg.node_name)
            .map_err(anyhow::Error::msg)
            .with_context(|| format!("invalid node name {}", cfg.node_name))?;
        let host_ip =
            Ipv4Addr::from_str(node_ip).with_context(|| format!("invalid node ip {}", node_ip))?;

        let local_subnet = cluster_api
            .allocate_node_subnet(&cfg.node_name, &cfg.cluster_cidr, node_ip)
            .await
            .with_context(|| {
                format!(
                    "failed to allocate local node subnet for {} at {}",
                    cfg.node_name, node_ip
                )
            })?;

        let (conn, handle, _) =
            rtnetlink::new_connection().context("failed to open rtnetlink for network plane")?;
        let rt_cancel = cancel.clone();
        let rt_conn = task_supervisor
            .spawn_async(
                crate::task_supervisor::TaskCategory::Network,
                "network_plane_rtnetlink_connection",
                async move {
                    tokio::select! {
                        _ = conn => {}
                        _ = rt_cancel.cancelled() => {}
                    }
                },
            )
            .await
            .context("failed to spawn network plane rtnetlink connection task")?;

        let plane = Arc::new(Self {
            rt: handle,
            _rt_conn: rt_conn,
            node_local,
            bridge,
            pod_subnet: local_subnet.subnet,
            pod_link_mtu: crate::networking::pod_link_mtu_for_encryption(cfg.dataplane_encryption),
            my_node,
            host_ip,
            _vxlan_vni: cfg.vxlan_vni,
            _vxlan_port: cfg.vxlan_port,
            wireguard_device: cfg.wireguard_device.clone(),
            bridge_idx: OnceLock::new(),
            wireguard_idx: OnceLock::new(),
            wireguard: OnceLock::new(),
            health: DataplaneHealth::new_healthy(),
            task_supervisor: task_supervisor.clone(),
        });

        plane.ensure_bridge_once().await?;

        plane
            .validate_boot_bridge()
            .await
            .context("boot-time networking validation failed")?;

        if cfg.dataplane_encryption == crate::networking::wireguard::DataplaneEncryption::Enabled
            && let Err(err) = plane.ensure_wireguard_enabled(cfg, cancel).await
        {
            plane
                .health
                .set_unavailable(format!("WireGuard dataplane: {err:#}"));
            tracing::error!(
                error = %err,
                "root WireGuard dataplane setup failed; node will report NotReady"
            );
        }

        Ok(plane)
    }

    pub fn local_pod_subnet(&self) -> PodSubnet {
        self.pod_subnet
    }

    async fn link_index_cached(&self, name: &str, cache: &OnceLock<u32>) -> Result<u32> {
        if let Some(idx) = cache.get() {
            return Ok(*idx);
        }
        let idx = self.link_index(name).await?;
        let _ = cache.set(idx);
        Ok(idx)
    }

    async fn link_index(&self, name: &str) -> Result<u32> {
        use futures::stream::TryStreamExt;

        let mut links = self.rt.link().get().match_name(name.to_owned()).execute();
        if let Some(link) = links
            .try_next()
            .await
            .context("rtnl list-link failed while resolving interface index")?
        {
            Ok(link.header.index)
        } else {
            anyhow::bail!("interface {} not found", name)
        }
    }

    async fn link_message(&self, name: &str) -> Result<netlink_packet_route::link::LinkMessage> {
        let mut links = self.rt.link().get().match_name(name.to_owned()).execute();
        links
            .try_next()
            .await
            .context("rtnl list-link failed while resolving link")?
            .with_context(|| format!("interface {} not found", name))
    }

    async fn ensure_link_up_and_mtu(&self, idx: u32, expected_mtu: u32) -> Result<()> {
        self.rt
            .link()
            .set(idx)
            .mtu(expected_mtu)
            .execute()
            .await
            .context("failed to set interface MTU")?;
        self.rt
            .link()
            .set(idx)
            .up()
            .execute()
            .await
            .context("failed to bring interface up")?;
        Ok(())
    }

    async fn ensure_ipv4_link_address(
        &self,
        idx: u32,
        expected: Ipv4Addr,
        prefix_len: u8,
    ) -> Result<()> {
        let mut stale_addrs = Vec::<AddressMessage>::new();
        let mut has_expected = false;

        let mut addrs = self.rt.address().get().set_link_index_filter(idx).execute();
        while let Some(addr_msg) = addrs
            .try_next()
            .await
            .context("failed to query link addresses while validating networking")?
        {
            if addr_msg.header.family != AddressFamily::Inet {
                continue;
            }

            let mut is_exact_expected = false;
            let mut is_ipv4_addr_attr = false;
            for attr in &addr_msg.attributes {
                match attr {
                    AddressAttribute::Address(IpAddr::V4(addr))
                    | AddressAttribute::Local(IpAddr::V4(addr)) => {
                        is_ipv4_addr_attr = true;
                        if *addr == IpAddr::V4(expected) && addr_msg.header.prefix_len == prefix_len
                        {
                            is_exact_expected = true;
                        }
                    }
                    _ => {}
                }
            }

            if is_exact_expected {
                has_expected = true;
                continue;
            }

            if is_ipv4_addr_attr {
                stale_addrs.push(addr_msg);
            }
        }

        for addr_msg in stale_addrs {
            self.rt
                .address()
                .del(addr_msg)
                .execute()
                .await
                .context("failed to remove unexpected IPv4 address from link")?;
        }

        if !has_expected {
            let add_result = self
                .rt
                .address()
                .add(idx, IpAddr::V4(expected), prefix_len)
                .execute()
                .await;
            if let Err(err) = add_result
                && !crate::networking::is_nl_eexist_error(&err)
            {
                return Err(err).context(format!("failed to add {}", expected));
            }
        }

        Ok(())
    }

    fn ignore_eexist<T>(res: std::result::Result<T, rtnetlink::Error>) -> Result<()> {
        match res {
            Ok(_) => Ok(()),
            Err(err) if crate::networking::is_nl_eexist_error(&err) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    async fn ensure_bridge_once(&self) -> Result<()> {
        if self.link_index(self.bridge.as_ref()).await.is_err() {
            self.rt
                .link()
                .add()
                .bridge(self.bridge.as_ref().to_string())
                .execute()
                .await
                .with_context(|| format!("failed to create bridge {}", self.bridge))?;
            tracing::info!(bridge = %self.bridge, "created bridge");
        }

        let idx = self
            .link_index(self.bridge.as_ref())
            .await
            .with_context(|| format!("bridge {} not found after creation", self.bridge))?;

        // Cache so cni_add skips the RTM_GETLINK round-trip on every pod ADD.
        let _ = self.bridge_idx.set(idx);

        Self::ignore_eexist(
            self.rt
                .address()
                .add(
                    idx,
                    IpAddr::V4(self.pod_subnet.bridge_ip()),
                    self.pod_subnet.prefix(),
                )
                .execute()
                .await,
        )?;

        self.ensure_link_up_and_mtu(idx, self.pod_link_mtu).await?;

        Ok(())
    }

    /// Dataplane health snapshot. WireGuard failures are recorded here
    /// so callers can set `NetworkUnavailable=True` on the Node.
    pub fn health(&self) -> &DataplaneHealth {
        &self.health
    }

    async fn ensure_wireguard_once(&self) -> Result<u32> {
        match self.link_index(&self.wireguard_device).await {
            Ok(idx) => {
                let _ = self.wireguard_idx.set(idx);
            }
            Err(_) => {
                Self::ignore_eexist(
                    self.rt
                        .link()
                        .add()
                        .wireguard(self.wireguard_device.clone())
                        .execute()
                        .await,
                )
                .with_context(|| format!("failed to create {}", self.wireguard_device))?;
            }
        }

        let msg = self
            .link_message(&self.wireguard_device)
            .await
            .with_context(|| format!("{} not found after creation", self.wireguard_device))?;
        let state = device_state::parse_link_state(&msg);
        if !matches!(state.kind, LinkKind::Wireguard) {
            anyhow::bail!(
                "expected interface {} to be wireguard kind, got {:?}",
                self.wireguard_device,
                state.kind
            );
        }
        self.ensure_link_up_and_mtu(state.ifindex, crate::networking::wireguard::WIREGUARD_MTU)
            .await
            .with_context(|| format!("failed to bring up {}", self.wireguard_device))?;
        let _ = self.wireguard_idx.set(state.ifindex);
        Ok(state.ifindex)
    }

    async fn ensure_wireguard_enabled(
        &self,
        cfg: &crate::KlightsConfig,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<()> {
        self.ensure_wireguard_once().await?;
        let key_path =
            crate::paths::etc_dir_path(&cfg.containerd_namespace).join("wireguard-private.key");
        let identity = crate::networking::wireguard::WireGuardIdentity::load_or_create(
            &key_path,
            self.task_supervisor.as_ref(),
        )
        .await?;
        let config = crate::networking::wireguard::WireGuardDeviceConfig::try_new(
            self.wireguard_device.clone(),
            identity.private_key().clone(),
            cfg.wireguard_port,
        )?;
        let controller = Arc::new(
            crate::networking::wireguard::WireGuardController::open(
                config,
                self.task_supervisor.as_ref(),
                cancel,
            )
            .await?,
        );
        let _ = self.wireguard.set(controller);
        Ok(())
    }

    async fn validate_boot_bridge(&self) -> Result<()> {
        let bridge_msg = self
            .link_message(self.bridge.as_ref())
            .await
            .with_context(|| format!("bridge {} not found during boot validation", self.bridge))?;
        let bridge_state = device_state::parse_link_state(&bridge_msg);

        if !matches!(bridge_state.kind, LinkKind::Bridge) {
            anyhow::bail!(
                "expected {} to be bridge kind, got {:?}",
                self.bridge,
                bridge_state.kind
            );
        }

        self.ensure_link_up_and_mtu(bridge_state.ifindex, self.pod_link_mtu)
            .await
            .context("failed to repair bridge interface state")?;
        self.ensure_ipv4_link_address(
            bridge_state.ifindex,
            self.pod_subnet.bridge_ip(),
            self.pod_subnet.prefix(),
        )
        .await
        .context("failed to repair bridge interface address")?;
        self.remove_stale_down_bridge_pod_subnet_addresses(bridge_state.ifindex)
            .await
            .context("failed to remove stale duplicate pod-subnet bridge addresses")?;

        let _ = self.bridge_idx.set(bridge_state.ifindex);

        Ok(())
    }

    async fn ipv4_addresses_for_link(&self, ifindex: u32) -> Result<Vec<AddressMessage>> {
        let mut out = Vec::new();
        let mut addrs = self
            .rt
            .address()
            .get()
            .set_link_index_filter(ifindex)
            .execute();
        while let Some(addr_msg) = addrs
            .try_next()
            .await
            .context("failed to query link addresses while scanning stale pod-subnet routes")?
        {
            if address_message_ipv4(&addr_msg).is_some() {
                out.push(addr_msg);
            }
        }
        Ok(out)
    }

    async fn remove_stale_down_bridge_pod_subnet_addresses(
        &self,
        current_bridge_idx: u32,
    ) -> Result<()> {
        let bridge_ip = self.pod_subnet.bridge_ip();
        let prefix_len = self.pod_subnet.prefix();
        let mut links = self.rt.link().get().execute();

        while let Some(link_msg) = links
            .try_next()
            .await
            .context("failed to list links while scanning stale pod-subnet routes")?
        {
            let state = device_state::parse_link_state(&link_msg);
            if state.ifindex == current_bridge_idx || !matches!(state.kind, LinkKind::Bridge) {
                continue;
            }

            let addr_msgs = self.ipv4_addresses_for_link(state.ifindex).await?;
            let addresses = addr_msgs
                .iter()
                .filter_map(address_message_ipv4)
                .collect::<Vec<_>>();

            if !stale_down_bridge_pod_subnet_addr_candidate(
                &state,
                current_bridge_idx,
                bridge_ip,
                prefix_len,
                &addresses,
            ) {
                if !link_state_is_down_for_stale_cleanup(&state)
                    && addresses
                        .iter()
                        .any(|addr| addr.local == bridge_ip && addr.prefix_len == prefix_len)
                {
                    tracing::warn!(
                        bridge = %state.name,
                        ifindex = state.ifindex,
                        pod_subnet = %self.pod_subnet,
                        current_bridge = %self.bridge,
                        "duplicate pod-subnet address exists on another UP bridge; leaving it untouched"
                    );
                }
                continue;
            }

            for addr_msg in addr_msgs {
                let Some(addr) = address_message_ipv4(&addr_msg) else {
                    continue;
                };
                if addr.local != bridge_ip || addr.prefix_len != prefix_len {
                    continue;
                }

                match self.rt.address().del(addr_msg).execute().await {
                    Ok(()) => {
                        tracing::warn!(
                            bridge = %state.name,
                            ifindex = state.ifindex,
                            pod_subnet = %self.pod_subnet,
                            current_bridge = %self.bridge,
                            "removed stale duplicate pod-subnet address from down bridge"
                        );
                    }
                    Err(err) if is_nl_absent_error(&err) => {}
                    Err(err) => {
                        return Err(err).with_context(|| {
                            format!(
                                "failed to remove stale pod-subnet address {bridge_ip}/{prefix_len} from {}",
                                state.name
                            )
                        });
                    }
                }
            }
        }

        Ok(())
    }

    async fn cni_add(
        &self,
        request: crate::networking::provider::CniAddRequest,
    ) -> Result<crate::networking::cni::PodNetwork> {
        let bridge_idx = self
            .link_index_cached(self.bridge.as_ref(), &self.bridge_idx)
            .await
            .with_context(|| format!("bridge {} not found", self.bridge))?;
        crate::networking::cni::add(crate::networking::cni::CniAddArgs {
            store: self.node_local.as_ref(),
            handle: &self.rt,
            sandbox_id: &request.sandbox_id,
            pod: crate::pod_identity::PodIdentity::new(
                &request.namespace,
                &request.pod_name,
                &request.pod_uid,
            ),
            bridge_name: &self.bridge,
            bridge_idx,
            netns_setns_path: &request.netns_setns_path,
            netns_record_path: &request.netns_record_path,
            pod_subnet: &self.pod_subnet,
            pod_link_mtu: self.pod_link_mtu,
            host_network: request.host_network,
            host_ip: &self.host_ip.to_string(),
            _node_name: &self.my_node,
            task_supervisor: self.task_supervisor.clone(),
        })
        .await
    }

    async fn cni_del(&self, sandbox_id: &str) -> Result<()> {
        let bridge_idx = self
            .link_index_cached(self.bridge.as_ref(), &self.bridge_idx)
            .await
            .with_context(|| format!("bridge {} not found", self.bridge))?;
        crate::networking::cni::del(self.node_local.as_ref(), &self.rt, sandbox_id, bridge_idx)
            .await
    }

    async fn apply_peer_endpoint(
        &self,
        peer: &crate::networking::types::NodeEndpoint,
    ) -> Result<()> {
        use crate::networking::types::NodeEndpoint;
        match peer {
            NodeEndpoint::WireGuard(plan) => {
                let controller = self
                    .wireguard
                    .get()
                    .context("WireGuard dataplane is not initialized")?;
                let idx = self
                    .link_index_cached(&self.wireguard_device, &self.wireguard_idx)
                    .await?;
                controller.apply_peer(plan).await?;
                crate::networking::wireguard::apply_wireguard_pod_route(
                    &self.rt,
                    idx,
                    plan,
                    self.pod_subnet.bridge_ip(),
                )
                .await
            }
            NodeEndpoint::UnencryptedDirect(plan) => {
                crate::networking::wireguard::apply_unencrypted_direct_route(&self.rt, plan).await
            }
            NodeEndpoint::Rootless { .. } => Ok(()),
        }
    }

    async fn remove_peer_endpoint(
        &self,
        peer: &crate::networking::types::NodeEndpoint,
    ) -> Result<()> {
        use crate::networking::types::NodeEndpoint;
        match peer {
            NodeEndpoint::WireGuard(plan) => {
                let idx = self
                    .link_index_cached(&self.wireguard_device, &self.wireguard_idx)
                    .await?;
                crate::networking::wireguard::remove_wireguard_pod_route(
                    &self.rt,
                    idx,
                    plan,
                    self.pod_subnet.bridge_ip(),
                )
                .await?;
                if let Some(controller) = self.wireguard.get() {
                    controller.remove_peer(&plan.public_key).await?;
                }
                Ok(())
            }
            NodeEndpoint::UnencryptedDirect(plan) => {
                crate::networking::wireguard::remove_unencrypted_direct_route(&self.rt, plan).await
            }
            NodeEndpoint::Rootless { .. } => Ok(()),
        }
    }

    async fn shutdown_impl(&self) -> Result<()> {
        self._rt_conn.abort();
        Ok(())
    }
}

#[async_trait::async_trait]
impl crate::networking::datapath::Datapath for NetworkPlane {
    async fn cni_add(
        &self,
        request: crate::networking::provider::CniAddRequest,
    ) -> Result<crate::networking::cni::PodNetwork> {
        Self::cni_add(self, request).await
    }

    async fn cni_del(&self, sandbox_id: &str) -> Result<()> {
        Self::cni_del(self, sandbox_id).await
    }

    async fn host_ip(&self) -> Result<std::net::IpAddr> {
        Ok(std::net::IpAddr::V4(self.host_ip))
    }

    async fn pod_gateway_ip(&self) -> Result<std::net::IpAddr> {
        Ok(std::net::IpAddr::V4(self.pod_subnet.bridge_ip()))
    }

    async fn shutdown(&self) -> Result<()> {
        self.shutdown_impl().await
    }
}

#[async_trait::async_trait]
impl crate::networking::peer_router::PeerRouter for NetworkPlane {
    async fn apply_peer_endpoint(
        &self,
        peer: &crate::networking::types::NodeEndpoint,
    ) -> Result<()> {
        Self::apply_peer_endpoint(self, peer).await
    }

    async fn remove_peer_endpoint(
        &self,
        peer: &crate::networking::types::NodeEndpoint,
    ) -> Result<()> {
        Self::remove_peer_endpoint(self, peer).await
    }
}

// Hybrid peer boot invariants (boot ordering, VXLAN avoidance,
// peer-endpoint arms) are enforced by `scripts/check_networking_invariants.sh`,
// run as part of `./build.sh`.

#[cfg(test)]
mod stale_route_tests {
    use super::*;
    use crate::networking::device_state::{LinkKind, LinkState};

    fn link_state(
        name: &str,
        ifindex: u32,
        kind: LinkKind,
        up: bool,
        operstate: Option<LinkOperState>,
    ) -> LinkState {
        LinkState {
            name: name.to_string(),
            ifindex,
            kind,
            mtu: None,
            up,
            operstate,
            master: None,
        }
    }

    #[test]
    fn stale_down_bridge_with_same_local_pod_subnet_is_cleanup_candidate() {
        let bridge_ip = Ipv4Addr::new(10, 43, 1, 1);
        let current_bridge_idx = 20;
        let exact_addr = vec![LinkIpv4Address {
            local: bridge_ip,
            prefix_len: 24,
        }];

        assert!(stale_down_bridge_pod_subnet_addr_candidate(
            &link_state("klights", 10, LinkKind::Bridge, false, None),
            current_bridge_idx,
            bridge_ip,
            24,
            &exact_addr,
        ));
        assert!(!stale_down_bridge_pod_subnet_addr_candidate(
            &link_state(
                "klights-worker",
                current_bridge_idx,
                LinkKind::Bridge,
                true,
                Some(LinkOperState::Up),
            ),
            current_bridge_idx,
            bridge_ip,
            24,
            &exact_addr,
        ));
        assert!(!stale_down_bridge_pod_subnet_addr_candidate(
            &link_state("other", 11, LinkKind::Bridge, false, None),
            current_bridge_idx,
            bridge_ip,
            24,
            &[LinkIpv4Address {
                local: Ipv4Addr::new(10, 43, 2, 1),
                prefix_len: 24,
            }],
        ));
        assert!(stale_down_bridge_pod_subnet_addr_candidate(
            &link_state(
                "admin-up-but-linkdown",
                12,
                LinkKind::Bridge,
                true,
                Some(LinkOperState::Down),
            ),
            current_bridge_idx,
            bridge_ip,
            24,
            &exact_addr,
        ));
        assert!(!stale_down_bridge_pod_subnet_addr_candidate(
            &link_state(
                "live-other",
                12,
                LinkKind::Bridge,
                true,
                Some(LinkOperState::Up),
            ),
            current_bridge_idx,
            bridge_ip,
            24,
            &exact_addr,
        ));
        assert!(!stale_down_bridge_pod_subnet_addr_candidate(
            &link_state("wg", 13, LinkKind::Wireguard, false, None),
            current_bridge_idx,
            bridge_ip,
            24,
            &exact_addr,
        ));
    }
}
