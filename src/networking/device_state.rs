use std::net::Ipv4Addr;

use netlink_packet_route::link::{
    InfoData, InfoKind, LinkAttribute, LinkFlag, LinkInfo, LinkMessage, State as LinkOperState,
};

/// Parsed link-kind taxonomy for parsed rtnetlink `LinkMessage` records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinkKind {
    /// A Linux bridge (`bridge` link kind).
    Bridge,
    /// A VXLAN tunnel interface.
    Vxlan,
    /// A WireGuard tunnel interface.
    Wireguard,
    /// A dummy link.
    Dummy,
    /// A veth interface.
    Veth,
    /// Link kind parsed successfully but not one of the recognized core types.
    Other(String),
    /// Kind missing from the received netlink attributes.
    Unknown,
}

/// Small parsed view of a link used for boot-time validation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkState {
    pub name: String,
    pub ifindex: u32,
    pub kind: LinkKind,
    pub mtu: Option<u32>,
    pub up: bool,
    pub operstate: Option<LinkOperState>,
    pub master: Option<u32>,
}

/// Parsed VXLAN-specific configuration extracted from rtnetlink link info attributes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VxlanState {
    pub vni: Option<u32>,
    pub port: Option<u16>,
    pub local: Option<Ipv4Addr>,
    pub learning: Option<bool>,
}

/// Parse a `LinkMessage` into the validation-focused [`LinkState`].
pub fn parse_link_state(msg: &LinkMessage) -> LinkState {
    let mut state = LinkState {
        name: String::new(),
        ifindex: msg.header.index,
        kind: LinkKind::Unknown,
        mtu: None,
        up: msg.header.flags.contains(&LinkFlag::Up),
        operstate: None,
        master: None,
    };

    for attr in &msg.attributes {
        match attr {
            LinkAttribute::IfName(n) => state.name = n.clone(),
            LinkAttribute::Mtu(v) => state.mtu = Some(*v),
            LinkAttribute::OperState(operstate) => state.operstate = Some(*operstate),
            LinkAttribute::Controller(idx) => {
                state.master.get_or_insert(*idx);
            }
            LinkAttribute::LinkInfo(infos) => {
                for info in infos {
                    if let LinkInfo::Kind(kind) = info {
                        state.kind = parse_link_kind(kind);
                    }
                }
            }
            _ => {}
        }
    }

    state
}

/// Parse VXLAN fields (`vni`, `port`, `local`, `learning`) from a `LinkMessage`.
pub fn parse_vxlan_state(msg: &LinkMessage) -> VxlanState {
    let mut out = VxlanState {
        vni: None,
        port: None,
        local: None,
        learning: None,
    };

    for attr in &msg.attributes {
        let LinkAttribute::LinkInfo(infos) = attr else {
            continue;
        };

        for info in infos {
            let LinkInfo::Data(InfoData::Vxlan(vxlan_attrs)) = info else {
                continue;
            };

            for vxlan_attr in vxlan_attrs {
                match vxlan_attr {
                    netlink_packet_route::link::InfoVxlan::Id(vni) => out.vni = Some(*vni),
                    netlink_packet_route::link::InfoVxlan::Port(port) => out.port = Some(*port),
                    netlink_packet_route::link::InfoVxlan::Local(raw) => {
                        let raw = raw.as_slice();
                        if let [a, b, c, d] = raw {
                            out.local = Some(Ipv4Addr::new(*a, *b, *c, *d));
                        }
                    }
                    netlink_packet_route::link::InfoVxlan::Learning(learning) => {
                        out.learning = Some(*learning)
                    }
                    _ => {}
                }
            }
        }
    }

    out
}

fn parse_link_kind(kind: &InfoKind) -> LinkKind {
    match kind {
        InfoKind::Bridge => LinkKind::Bridge,
        InfoKind::Vxlan => LinkKind::Vxlan,
        InfoKind::Wireguard => LinkKind::Wireguard,
        InfoKind::Dummy => LinkKind::Dummy,
        InfoKind::Veth => LinkKind::Veth,
        InfoKind::Other(other) => LinkKind::Other(other.to_string()),
        _ => LinkKind::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use netlink_packet_route::link::{
        InfoData, InfoKind, InfoVxlan, LinkAttribute, LinkFlag, LinkInfo,
    };

    #[test]
    fn parses_bridge_link_state() {
        let mut msg = LinkMessage::default();
        msg.attributes = vec![
            LinkAttribute::IfName("klights0".to_string()),
            LinkAttribute::Mtu(1450),
            LinkAttribute::Controller(11),
            LinkAttribute::LinkInfo(vec![
                LinkInfo::Kind(InfoKind::Bridge),
                LinkInfo::Data(InfoData::Vxlan(vec![InfoVxlan::Id(1228)])),
            ]),
        ];

        let state = parse_link_state(&msg);
        assert_eq!(
            state,
            LinkState {
                name: "klights0".to_string(),
                ifindex: 0,
                kind: LinkKind::Bridge,
                mtu: Some(1450),
                up: false,
                operstate: None,
                master: Some(11),
            }
        );
    }

    #[test]
    fn parses_vxlan_state() {
        let mut msg = LinkMessage::default();
        msg.attributes = vec![LinkAttribute::LinkInfo(vec![
            LinkInfo::Kind(InfoKind::Vxlan),
            LinkInfo::Data(InfoData::Vxlan(vec![
                InfoVxlan::Id(1228),
                InfoVxlan::Port(8472),
                InfoVxlan::Learning(false),
                InfoVxlan::Local(vec![10, 43, 0, 1]),
            ])),
        ])];

        let state = parse_vxlan_state(&msg);
        assert_eq!(
            state,
            VxlanState {
                vni: Some(1228),
                port: Some(8472),
                local: Some(Ipv4Addr::new(10, 43, 0, 1)),
                learning: Some(false),
            }
        );
    }

    #[test]
    fn bridge_marked_up_when_flag_present() {
        let mut msg = LinkMessage::default();
        msg.attributes = vec![
            LinkAttribute::IfName("up".to_string()),
            LinkAttribute::LinkInfo(vec![LinkInfo::Kind(InfoKind::Bridge)]),
        ];
        msg.header.flags = vec![LinkFlag::Up];

        let state = parse_link_state(&msg);
        assert!(state.up);
    }

    #[test]
    fn parses_operstate_down_independent_of_admin_up_flag() {
        let mut msg = LinkMessage::default();
        msg.attributes = vec![
            LinkAttribute::IfName("admin-up-linkdown".to_string()),
            LinkAttribute::OperState(LinkOperState::Down),
            LinkAttribute::LinkInfo(vec![LinkInfo::Kind(InfoKind::Bridge)]),
        ];
        msg.header.flags = vec![LinkFlag::Up];

        let state = parse_link_state(&msg);
        assert!(state.up);
        assert_eq!(state.operstate, Some(LinkOperState::Down));
    }
}
