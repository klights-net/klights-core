//! Sync netlink helpers used to configure a pod netns without tokio runtime.
//!
//! These helpers build and send `netlink-sys` requests directly using
//! `NETLINK_ROUTE` and parse acknowledgements with matching sequence numbers.

use anyhow::{Context, Result, anyhow};
use netlink_packet_core::{
    NetlinkMessage, NetlinkPayload,
    constants::{NLM_F_ACK, NLM_F_CREATE, NLM_F_EXCL, NLM_F_REQUEST},
};
use netlink_packet_route::{
    RouteNetlinkMessage,
    address::{AddressAttribute, AddressMessage},
    link::{LinkAttribute, LinkFlag, LinkMessage},
    route::{
        RouteAddress, RouteAttribute, RouteHeader, RouteMessage, RouteProtocol, RouteScope,
        RouteType,
    },
};
use netlink_sys::{Socket, SocketAddr, protocols::NETLINK_ROUTE};
use std::io;
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicU32, Ordering};

use netlink_packet_route::AddressFamily;

static NEXT_SEQUENCE: AtomicU32 = AtomicU32::new(1);

fn next_sequence() -> u32 {
    NEXT_SEQUENCE.fetch_add(1, Ordering::SeqCst)
}

fn build_request(
    mut message: NetlinkMessage<RouteNetlinkMessage>,
    extra_flags: u16,
) -> Result<(NetlinkMessage<RouteNetlinkMessage>, u32)> {
    let sequence = next_sequence();
    message.header.sequence_number = sequence;
    message.header.flags |= NLM_F_REQUEST | NLM_F_ACK | extra_flags;
    message.finalize();
    Ok((message, sequence))
}

fn send_and_collect(
    socket: &mut Socket,
    message: NetlinkMessage<RouteNetlinkMessage>,
    expect_multipart: bool,
    extra_flags: u16,
) -> Result<Vec<NetlinkMessage<RouteNetlinkMessage>>> {
    let (request, seq) = build_request(message, extra_flags)?;

    let request_buf_len = request.buffer_len();
    let mut request_buf = vec![0u8; request_buf_len];
    request.serialize(&mut request_buf);

    socket
        .send(&request_buf[..], 0)
        .context("send netlink request")?;

    let mut responses = Vec::new();

    loop {
        let mut response_buf = vec![0u8; 16 * 1024];
        let response_len = socket.recv(&mut &mut response_buf[..], 0)?;
        if response_len == 0 {
            continue;
        }

        let response =
            NetlinkMessage::<RouteNetlinkMessage>::deserialize(&response_buf[..response_len])
                .context("deserialize netlink response")?;

        // Ignore late/unrelated replies from earlier requests.
        if response.header.sequence_number != seq {
            continue;
        }

        let payload_is_done = matches!(&response.payload, NetlinkPayload::Done(_));

        let payload_is_inner = matches!(&response.payload, NetlinkPayload::InnerMessage(_));

        responses.push(response);

        if let Some(err) = responses.last().and_then(|m| {
            if let NetlinkPayload::Error(err) = &m.payload {
                Some(err)
            } else {
                None
            }
        }) {
            if let Some(code) = err.code {
                return Err(io::Error::from_raw_os_error(code.get().abs()).into());
            }
            // ACK for ACK-only requests.
            return Ok(responses);
        }

        if payload_is_done || (payload_is_inner && !expect_multipart) {
            return Ok(responses);
        }
    }
}

pub fn new_route_socket() -> Result<Socket> {
    let mut socket = Socket::new(NETLINK_ROUTE).context("create NETLINK_ROUTE socket")?;
    socket.bind_auto().context("bind NETLINK_ROUTE socket")?;
    socket
        .connect(&SocketAddr::new(0, 0))
        .context("connect NETLINK_ROUTE socket")?;
    Ok(socket)
}

/// Resolve an interface index by name using rtnetlink sync request/response.
pub fn link_index_by_name(socket: &mut Socket, name: &str) -> Result<u32> {
    let mut message = LinkMessage::default();
    message
        .attributes
        .push(LinkAttribute::IfName(name.to_string()));

    let request = NetlinkMessage::from(RouteNetlinkMessage::GetLink(message));
    // rtnetlink uses RTM_GETLINK with filter by name in message attributes.
    let responses = send_and_collect(socket, request, false, 0)?;

    for message in responses {
        if let NetlinkPayload::InnerMessage(RouteNetlinkMessage::NewLink(link)) = message.payload {
            return Ok(link.header.index);
        }
    }

    Err(anyhow!("interface '{}' not found", name))
}

/// Rename a link by index.
pub fn link_rename(socket: &mut Socket, index: u32, new_name: &str) -> Result<()> {
    let mut message = LinkMessage::default();
    message.header.index = index;
    message
        .attributes
        .push(LinkAttribute::IfName(new_name.to_string()));

    let request = NetlinkMessage::from(RouteNetlinkMessage::SetLink(message));
    send_and_collect(socket, request, false, 0)?;
    Ok(())
}

/// Set link MTU by index.
pub fn link_set_mtu(socket: &mut Socket, index: u32, mtu: u32) -> Result<()> {
    let mut message = LinkMessage::default();
    message.header.index = index;
    message.attributes.push(LinkAttribute::Mtu(mtu));

    let request = NetlinkMessage::from(RouteNetlinkMessage::SetLink(message));
    send_and_collect(socket, request, false, 0)?;
    Ok(())
}

/// Bring link up by index.
pub fn link_up(socket: &mut Socket, index: u32) -> Result<()> {
    let mut message = LinkMessage::default();
    message.header.index = index;
    message.header.flags.push(LinkFlag::Up);
    message.header.change_mask.push(LinkFlag::Up);

    let request = NetlinkMessage::from(RouteNetlinkMessage::SetLink(message));
    send_and_collect(socket, request, false, 0)?;
    Ok(())
}

fn ipv4_broadcast(addr: Ipv4Addr, prefix_len: u8) -> Ipv4Addr {
    if prefix_len >= 32 {
        return addr;
    }

    let mask = if prefix_len == 0 {
        0u32
    } else {
        (!0u32) << (32 - prefix_len)
    };
    Ipv4Addr::from(u32::from(addr) | (!mask))
}

/// Add IPv4 address with netmask to a link.
pub fn addr_add_v4(socket: &mut Socket, index: u32, address: Ipv4Addr, prefix: u8) -> Result<()> {
    let mut message = AddressMessage::default();
    message.header.family = AddressFamily::Inet;
    message.header.index = index;
    message.header.prefix_len = prefix;
    message
        .attributes
        .push(AddressAttribute::Address(std::net::IpAddr::V4(address)));
    message
        .attributes
        .push(AddressAttribute::Local(std::net::IpAddr::V4(address)));
    message
        .attributes
        .push(AddressAttribute::Broadcast(ipv4_broadcast(address, prefix)));

    let request = NetlinkMessage::from(RouteNetlinkMessage::NewAddress(message));
    send_and_collect(socket, request, false, NLM_F_EXCL | NLM_F_CREATE)?;
    Ok(())
}

/// Add default IPv4 route through `gateway` via output interface `oif_index`.
pub fn route_add_default_v4(socket: &mut Socket, gateway: Ipv4Addr, oif_index: u32) -> Result<()> {
    let mut message = RouteMessage::default();
    message.header.address_family = AddressFamily::Inet;
    message.header.table = RouteHeader::RT_TABLE_MAIN;
    message.header.protocol = RouteProtocol::Static;
    message.header.scope = RouteScope::Universe;
    message.header.kind = RouteType::Unicast;
    message.attributes.push(RouteAttribute::Oif(oif_index));
    message
        .attributes
        .push(RouteAttribute::Gateway(RouteAddress::Inet(gateway)));

    let request = NetlinkMessage::from(RouteNetlinkMessage::NewRoute(message));
    send_and_collect(socket, request, false, NLM_F_EXCL | NLM_F_CREATE)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_next_sequence_produces_monotonic_values() {
        let first = next_sequence();
        let second = next_sequence();

        assert!(second > first);
    }

    #[test]
    fn test_build_request_adds_required_flags() {
        let message = NetlinkMessage::from(RouteNetlinkMessage::NewLink(LinkMessage::default()));
        let (request, sequence) = build_request(message, 0).unwrap();

        assert_eq!(request.header.sequence_number, sequence);
        assert!(request.header.flags & NLM_F_REQUEST != 0);
        assert!(request.header.flags & NLM_F_ACK != 0);
    }

    #[test]
    fn test_build_request_appends_extra_flags() {
        let message = NetlinkMessage::from(RouteNetlinkMessage::NewRoute(RouteMessage::default()));
        let (request, _) = build_request(message, NLM_F_EXCL).unwrap();

        assert!(request.header.flags & NLM_F_EXCL != 0);
    }
}
