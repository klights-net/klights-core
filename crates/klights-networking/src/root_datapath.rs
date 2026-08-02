//! Root-mode bridge and Linux link lifecycle.

use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, OnceLock};

use anyhow::{Context, Result};
use futures::stream::TryStreamExt;
use klights_types::PodSubnet;
use netlink_packet_route::{
    AddressFamily,
    address::{AddressAttribute, AddressMessage},
    link::State as LinkOperState,
};
use tokio_util::sync::CancellationToken;

use crate::device_state::{self, LinkKind, LinkState};
use crate::types::{BridgeName, PodLinkMtu};

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
        rtnetlink::Error::NetlinkError(error) => error.code.is_some_and(|code| {
            let code = code.get().abs();
            code == libc::ENODEV || code == libc::ENOENT || code == libc::EADDRNOTAVAIL
        }),
        _ => false,
    }
}

/// Root-mode owner of the rtnetlink connection, bridge and Pod CIDR link state.
///
/// Root composition combines this Linux link owner with CNI state and the
/// selected peer dataplane behind the focused networking traits.
pub struct RootDatapath {
    handle: rtnetlink::Handle,
    connection: klights_supervisor::SupervisedJoinHandle<()>,
    bridge: BridgeName,
    pod_subnet: PodSubnet,
    pod_link_mtu: u32,
    bridge_index: OnceLock<u32>,
}

impl RootDatapath {
    fn from_open_connection_inner(
        handle: rtnetlink::Handle,
        connection: klights_supervisor::SupervisedJoinHandle<()>,
        bridge: BridgeName,
        pod_subnet: PodSubnet,
        pod_link_mtu: PodLinkMtu,
    ) -> Self {
        Self {
            handle,
            connection,
            bridge,
            pod_subnet,
            pod_link_mtu: pod_link_mtu.get(),
            bridge_index: OnceLock::new(),
        }
    }

    #[cfg(feature = "test-support")]
    pub fn from_open_connection(
        handle: rtnetlink::Handle,
        connection: klights_supervisor::SupervisedJoinHandle<()>,
        bridge: BridgeName,
        pod_subnet: PodSubnet,
        pod_link_mtu: PodLinkMtu,
    ) -> Self {
        Self::from_open_connection_inner(handle, connection, bridge, pod_subnet, pod_link_mtu)
    }

    pub async fn boot(
        bridge: BridgeName,
        pod_subnet: PodSubnet,
        pod_link_mtu: PodLinkMtu,
        cancel: CancellationToken,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Result<Self> {
        let (connection, handle, _) =
            rtnetlink::new_connection().context("failed to open rtnetlink for root datapath")?;
        let connection_cancel = cancel.clone();
        let connection = supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Network,
                "root_datapath_rtnetlink_connection",
                async move {
                    tokio::select! {
                        _ = connection => {}
                        _ = connection_cancel.cancelled() => {}
                    }
                },
            )
            .await
            .context("failed to spawn root datapath rtnetlink connection task")?;

        let datapath =
            Self::from_open_connection_inner(handle, connection, bridge, pod_subnet, pod_link_mtu);
        datapath.ensure_bridge_once().await?;
        datapath
            .validate_boot_bridge()
            .await
            .context("boot-time root datapath validation failed")?;
        Ok(datapath)
    }

    pub fn handle(&self) -> &rtnetlink::Handle {
        &self.handle
    }

    pub fn bridge(&self) -> &BridgeName {
        &self.bridge
    }

    pub const fn pod_subnet(&self) -> PodSubnet {
        self.pod_subnet
    }

    pub const fn pod_link_mtu(&self) -> u32 {
        self.pod_link_mtu
    }

    pub async fn bridge_index(&self) -> Result<u32> {
        self.link_index_cached(self.bridge.as_ref(), &self.bridge_index)
            .await
    }

    pub async fn link_index_cached(&self, name: &str, cache: &OnceLock<u32>) -> Result<u32> {
        if let Some(index) = cache.get() {
            return Ok(*index);
        }
        let index = self.link_index(name).await?;
        let _ = cache.set(index);
        Ok(index)
    }

    pub async fn link_index(&self, name: &str) -> Result<u32> {
        get_link_index(&self.handle, name).await
    }

    pub async fn link_message(
        &self,
        name: &str,
    ) -> Result<netlink_packet_route::link::LinkMessage> {
        let mut links = self
            .handle
            .link()
            .get()
            .match_name(name.to_owned())
            .execute();
        links
            .try_next()
            .await
            .context("rtnl list-link failed while resolving link")?
            .with_context(|| format!("interface {name} not found"))
    }

    pub async fn ensure_link_up_and_mtu(&self, index: u32, expected_mtu: u32) -> Result<()> {
        self.handle
            .link()
            .set(index)
            .mtu(expected_mtu)
            .execute()
            .await
            .context("failed to set interface MTU")?;
        self.handle
            .link()
            .set(index)
            .up()
            .execute()
            .await
            .context("failed to bring interface up")?;
        Ok(())
    }

    pub fn ignore_eexist<T>(result: std::result::Result<T, rtnetlink::Error>) -> Result<()> {
        match result {
            Ok(_) => Ok(()),
            Err(error) if is_nl_eexist_error(&error) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub fn shutdown(&self) {
        self.connection.abort();
    }

    async fn ensure_ipv4_link_address(
        &self,
        index: u32,
        expected: Ipv4Addr,
        prefix_len: u8,
    ) -> Result<()> {
        let mut stale_addresses = Vec::<AddressMessage>::new();
        let mut has_expected = false;
        let mut addresses = self
            .handle
            .address()
            .get()
            .set_link_index_filter(index)
            .execute();

        while let Some(message) = addresses
            .try_next()
            .await
            .context("failed to query link addresses while validating networking")?
        {
            if message.header.family != AddressFamily::Inet {
                continue;
            }
            let mut exact = false;
            let mut ipv4 = false;
            for attribute in &message.attributes {
                match attribute {
                    AddressAttribute::Address(IpAddr::V4(address))
                    | AddressAttribute::Local(IpAddr::V4(address)) => {
                        ipv4 = true;
                        if *address == expected && message.header.prefix_len == prefix_len {
                            exact = true;
                        }
                    }
                    _ => {}
                }
            }
            if exact {
                has_expected = true;
            } else if ipv4 {
                stale_addresses.push(message);
            }
        }

        for message in stale_addresses {
            self.handle
                .address()
                .del(message)
                .execute()
                .await
                .context("failed to remove unexpected IPv4 address from link")?;
        }
        if !has_expected {
            let result = self
                .handle
                .address()
                .add(index, IpAddr::V4(expected), prefix_len)
                .execute()
                .await;
            if let Err(error) = result
                && !is_nl_eexist_error(&error)
            {
                return Err(error).with_context(|| format!("failed to add {expected}"));
            }
        }
        Ok(())
    }

    async fn ensure_bridge_once(&self) -> Result<()> {
        if self.link_index(self.bridge.as_ref()).await.is_err() {
            self.handle
                .link()
                .add()
                .bridge(self.bridge.as_ref().to_string())
                .execute()
                .await
                .with_context(|| format!("failed to create bridge {}", self.bridge))?;
            tracing::info!(bridge = %self.bridge, "created bridge");
        }

        let index = self
            .link_index(self.bridge.as_ref())
            .await
            .with_context(|| format!("bridge {} not found after creation", self.bridge))?;
        let _ = self.bridge_index.set(index);
        Self::ignore_eexist(
            self.handle
                .address()
                .add(
                    index,
                    IpAddr::V4(self.pod_subnet.bridge_ip()),
                    self.pod_subnet.prefix(),
                )
                .execute()
                .await,
        )?;
        self.ensure_link_up_and_mtu(index, self.pod_link_mtu).await
    }

    async fn validate_boot_bridge(&self) -> Result<()> {
        let message = self
            .link_message(self.bridge.as_ref())
            .await
            .with_context(|| format!("bridge {} not found during boot validation", self.bridge))?;
        let state = device_state::parse_link_state(&message);
        if !matches!(state.kind, LinkKind::Bridge) {
            anyhow::bail!(
                "expected {} to be bridge kind, got {:?}",
                self.bridge,
                state.kind
            );
        }

        self.ensure_link_up_and_mtu(state.ifindex, self.pod_link_mtu)
            .await
            .context("failed to repair bridge interface state")?;
        self.ensure_ipv4_link_address(
            state.ifindex,
            self.pod_subnet.bridge_ip(),
            self.pod_subnet.prefix(),
        )
        .await
        .context("failed to repair bridge interface address")?;
        self.remove_stale_down_bridge_pod_subnet_addresses(state.ifindex)
            .await
            .context("failed to remove stale duplicate pod-subnet bridge addresses")?;
        let _ = self.bridge_index.set(state.ifindex);
        Ok(())
    }

    async fn ipv4_addresses_for_link(&self, ifindex: u32) -> Result<Vec<AddressMessage>> {
        let mut output = Vec::new();
        let mut addresses = self
            .handle
            .address()
            .get()
            .set_link_index_filter(ifindex)
            .execute();
        while let Some(message) = addresses
            .try_next()
            .await
            .context("failed to query link addresses while scanning stale pod-subnet routes")?
        {
            if address_message_ipv4(&message).is_some() {
                output.push(message);
            }
        }
        Ok(output)
    }

    async fn remove_stale_down_bridge_pod_subnet_addresses(
        &self,
        current_bridge_index: u32,
    ) -> Result<()> {
        let bridge_ip = self.pod_subnet.bridge_ip();
        let prefix_len = self.pod_subnet.prefix();
        let mut links = self.handle.link().get().execute();

        while let Some(message) = links
            .try_next()
            .await
            .context("failed to list links while scanning stale pod-subnet routes")?
        {
            let state = device_state::parse_link_state(&message);
            if state.ifindex == current_bridge_index || !matches!(state.kind, LinkKind::Bridge) {
                continue;
            }
            let messages = self.ipv4_addresses_for_link(state.ifindex).await?;
            let addresses = messages
                .iter()
                .filter_map(address_message_ipv4)
                .collect::<Vec<_>>();

            if !stale_down_bridge_pod_subnet_addr_candidate(
                &state,
                current_bridge_index,
                bridge_ip,
                prefix_len,
                &addresses,
            ) {
                if !link_state_is_down_for_stale_cleanup(&state)
                    && addresses.iter().any(|address| {
                        address.local == bridge_ip && address.prefix_len == prefix_len
                    })
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

            for message in messages {
                let Some(address) = address_message_ipv4(&message) else {
                    continue;
                };
                if address.local != bridge_ip || address.prefix_len != prefix_len {
                    continue;
                }
                match self.handle.address().del(message).execute().await {
                    Ok(()) => tracing::warn!(
                        bridge = %state.name,
                        ifindex = state.ifindex,
                        pod_subnet = %self.pod_subnet,
                        current_bridge = %self.bridge,
                        "removed stale duplicate pod-subnet address from down bridge"
                    ),
                    Err(error) if is_nl_absent_error(&error) => {}
                    Err(error) => {
                        return Err(error).with_context(|| {
                            format!(
                                "failed to remove stale pod-subnet address \
                                 {bridge_ip}/{prefix_len} from {}",
                                state.name
                            )
                        });
                    }
                }
            }
        }
        Ok(())
    }
}

pub async fn get_link_index(handle: &rtnetlink::Handle, name: &str) -> Result<u32> {
    let mut links = handle.link().get().match_name(name.to_owned()).execute();
    if let Some(link) = links
        .try_next()
        .await
        .context("failed to list links while resolving interface index")?
    {
        Ok(link.header.index)
    } else {
        anyhow::bail!("Interface '{name}' not found")
    }
}

pub fn is_nl_eexist_error(error: &rtnetlink::Error) -> bool {
    match error {
        rtnetlink::Error::NetlinkError(error) => error.code.is_some_and(|code| {
            let code = code.get();
            code == libc::EEXIST || code == -libc::EEXIST
        }),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let current_bridge_index = 20;
        let exact_address = vec![LinkIpv4Address {
            local: bridge_ip,
            prefix_len: 24,
        }];

        assert!(stale_down_bridge_pod_subnet_addr_candidate(
            &link_state("klights", 10, LinkKind::Bridge, false, None),
            current_bridge_index,
            bridge_ip,
            24,
            &exact_address,
        ));
        assert!(!stale_down_bridge_pod_subnet_addr_candidate(
            &link_state(
                "klights-worker",
                current_bridge_index,
                LinkKind::Bridge,
                true,
                Some(LinkOperState::Up),
            ),
            current_bridge_index,
            bridge_ip,
            24,
            &exact_address,
        ));
        assert!(!stale_down_bridge_pod_subnet_addr_candidate(
            &link_state("other", 11, LinkKind::Bridge, false, None),
            current_bridge_index,
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
            current_bridge_index,
            bridge_ip,
            24,
            &exact_address,
        ));
        assert!(!stale_down_bridge_pod_subnet_addr_candidate(
            &link_state(
                "live-other",
                12,
                LinkKind::Bridge,
                true,
                Some(LinkOperState::Up),
            ),
            current_bridge_index,
            bridge_ip,
            24,
            &exact_address,
        ));
        assert!(!stale_down_bridge_pod_subnet_addr_candidate(
            &link_state("wg", 13, LinkKind::Wireguard, false, None),
            current_bridge_index,
            bridge_ip,
            24,
            &exact_address,
        ));
    }
}
