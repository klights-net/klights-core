use netlink_packet_route::link::{
    InfoKind, LinkAttribute, LinkFlag, LinkInfo, LinkMessage, State as LinkOperState,
};

/// Parsed link-kind taxonomy for parsed rtnetlink `LinkMessage` records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinkKind {
    /// A Linux bridge (`bridge` link kind).
    Bridge,
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

fn parse_link_kind(kind: &InfoKind) -> LinkKind {
    match kind {
        InfoKind::Bridge => LinkKind::Bridge,
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
    use netlink_packet_route::link::{InfoKind, LinkAttribute, LinkFlag, LinkInfo};

    #[test]
    fn parses_bridge_link_state() {
        let mut msg = LinkMessage::default();
        msg.attributes = vec![
            LinkAttribute::IfName("klights0".to_string()),
            LinkAttribute::Mtu(1450),
            LinkAttribute::Controller(11),
            LinkAttribute::LinkInfo(vec![LinkInfo::Kind(InfoKind::Bridge)]),
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
