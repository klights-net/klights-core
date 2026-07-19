use std::net::{IpAddr, Ipv4Addr};

use klights_leader_api::{
    DataplaneEncryption, HostPortRange, LeaderNetworkTopologyQuery, LeaderNodeSubnetAllocation,
    NetworkDataplane, NetworkNodeMode, NetworkTopologyError, NetworkTopologyFuture,
    NodeDataplaneQuery, NodeDataplaneResult, NodeSubnet, NodeSubnetAllocationError,
    NodeSubnetAllocationFuture, NodeSubnetAllocationRequest, NodeSubnetAllocationResult,
    NodeSubnetQuery, NodeSubnetResult, PeerSubnetsQuery, PeerSubnetsResult,
};

const VALID_WIREGUARD_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

fn root_subnet(node: &str, subnet: &str, node_ip: Ipv4Addr) -> NodeSubnet {
    let base = subnet
        .split_once('/')
        .expect("CIDR")
        .0
        .parse::<Ipv4Addr>()
        .expect("IPv4");
    NodeSubnet::try_new(
        node,
        subnet,
        u32::from(base),
        base,
        node_ip,
        NetworkNodeMode::Root,
        None,
    )
    .expect("valid root subnet")
}

#[test]
fn allocation_request_requires_canonical_ipv4_network_inputs() {
    let request = NodeSubnetAllocationRequest::try_new("worker-a", "10.42.0.0/16", "192.0.2.10")
        .expect("canonical allocation request");
    assert_eq!(request.node_name(), "worker-a");
    assert_eq!(request.cluster_cidr(), "10.42.0.0/16");
    assert_eq!(request.node_ip(), Ipv4Addr::new(192, 0, 2, 10));

    for (node, cidr, ip, field) in [
        ("", "10.42.0.0/16", "192.0.2.10", "node_name"),
        ("worker-a", "10.42.0.1/16", "192.0.2.10", "cluster_cidr"),
        ("worker-a", "10.42.0.0/25", "192.0.2.10", "cluster_cidr"),
        ("worker-a", "2001:db8::/64", "192.0.2.10", "cluster_cidr"),
        ("worker-a", "10.42.0.0/16", "not-an-ip", "node_ip"),
    ] {
        assert!(matches!(
            NodeSubnetAllocationRequest::try_new(node, cidr, ip),
            Err(NodeSubnetAllocationError::InvalidRequest {
                field: actual,
                ..
            }) if actual == field
        ));
    }
}

#[test]
fn subnet_dto_validates_24_redundancy_and_mode_range_shape() {
    let root = root_subnet("root-a", "10.42.1.0/24", Ipv4Addr::new(192, 0, 2, 10));
    assert_eq!(root.node_name(), "root-a");
    assert_eq!(root.subnet(), "10.42.1.0/24");
    assert_eq!(
        root.subnet_base_int(),
        u32::from(Ipv4Addr::new(10, 42, 1, 0))
    );
    assert_eq!(root.gateway_ip(), Ipv4Addr::new(10, 42, 1, 0));
    assert_eq!(root.mode(), NetworkNodeMode::Root);
    assert_eq!(root.hostport_range(), None);

    let rootless_range = HostPortRange::try_new(20_000, 20_999).expect("valid range");
    let rootless = NodeSubnet::try_new(
        "rootless-a",
        "10.42.2.0/24",
        u32::from(Ipv4Addr::new(10, 42, 2, 0)),
        Ipv4Addr::new(10, 42, 2, 0),
        Ipv4Addr::new(192, 0, 2, 11),
        NetworkNodeMode::Rootless,
        Some(rootless_range),
    )
    .expect("valid rootless subnet");
    assert_eq!(rootless.hostport_range(), Some(rootless_range));

    for invalid in [
        NodeSubnet::try_new(
            "node-a",
            "10.42.1.0/25",
            u32::from(Ipv4Addr::new(10, 42, 1, 0)),
            Ipv4Addr::new(10, 42, 1, 0),
            Ipv4Addr::new(192, 0, 2, 10),
            NetworkNodeMode::Root,
            None,
        ),
        NodeSubnet::try_new(
            "node-a",
            "10.42.1.0/24",
            u32::from(Ipv4Addr::new(10, 42, 9, 0)),
            Ipv4Addr::new(10, 42, 1, 0),
            Ipv4Addr::new(192, 0, 2, 10),
            NetworkNodeMode::Root,
            None,
        ),
        NodeSubnet::try_new(
            "node-a",
            "10.42.1.0/24",
            u32::from(Ipv4Addr::new(10, 42, 1, 0)),
            Ipv4Addr::new(10, 42, 1, 2),
            Ipv4Addr::new(192, 0, 2, 10),
            NetworkNodeMode::Root,
            None,
        ),
        NodeSubnet::try_new(
            "node-a",
            "10.42.1.0/24",
            u32::from(Ipv4Addr::new(10, 42, 1, 0)),
            Ipv4Addr::new(10, 42, 1, 0),
            Ipv4Addr::new(192, 0, 2, 10),
            NetworkNodeMode::Rootless,
            None,
        ),
    ] {
        assert!(matches!(
            invalid,
            Err(NetworkTopologyError::CorruptResponse { .. })
        ));
    }
}

#[test]
fn topology_results_reject_flag_mismatch_duplicate_nodes_and_overlap() {
    let subnet_a = root_subnet("node-a", "10.42.1.0/24", Ipv4Addr::new(192, 0, 2, 10));
    let subnet_b = root_subnet("node-b", "10.42.2.0/24", Ipv4Addr::new(192, 0, 2, 11));

    assert!(matches!(
        NodeSubnetResult::try_from_wire("node-a", false, Some(subnet_a.clone())),
        Err(NetworkTopologyError::CorruptResponse { .. })
    ));
    assert!(matches!(
        NodeSubnetResult::try_from_wire("node-a", true, None),
        Err(NetworkTopologyError::CorruptResponse { .. })
    ));
    assert_eq!(
        NodeSubnetResult::try_from_wire("node-a", true, Some(subnet_a.clone()))
            .expect("matching payload")
            .into_option(),
        Some(subnet_a.clone())
    );

    PeerSubnetsResult::try_new("local-node", vec![subnet_a.clone(), subnet_b])
        .expect("unique non-overlapping peers");
    assert!(matches!(
        PeerSubnetsResult::try_new("local-node", vec![subnet_a.clone(), subnet_a.clone()]),
        Err(NetworkTopologyError::CorruptResponse { .. })
    ));
    let overlapping_node = root_subnet("node-c", "10.42.1.0/24", Ipv4Addr::new(192, 0, 2, 12));
    assert!(matches!(
        PeerSubnetsResult::try_new("local-node", vec![subnet_a, overlapping_node]),
        Err(NetworkTopologyError::CorruptResponse { .. })
    ));
}

#[test]
fn dataplane_dto_is_strict_and_keeps_mode_orthogonal_to_encryption() {
    for mode in [NetworkNodeMode::Root, NetworkNodeMode::Rootless] {
        let encrypted = NetworkDataplane::try_new(
            "node-a",
            mode,
            DataplaneEncryption::WireGuard,
            Some(VALID_WIREGUARD_KEY),
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)),
            Some(7_679),
        )
        .expect("encrypted mode is valid for root and rootless nodes");
        assert_eq!(encrypted.mode(), mode);
        assert_eq!(encrypted.encryption(), DataplaneEncryption::WireGuard);

        let direct = NetworkDataplane::try_new(
            "node-a",
            mode,
            DataplaneEncryption::Direct,
            None,
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)),
            None,
        )
        .expect("direct mode is valid for root and rootless nodes");
        assert_eq!(direct.encryption(), DataplaneEncryption::Direct);
    }

    for invalid in [
        NetworkDataplane::try_new(
            "node-a",
            NetworkNodeMode::Root,
            DataplaneEncryption::WireGuard,
            None,
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            Some(7_679),
        ),
        NetworkDataplane::try_new(
            "node-a",
            NetworkNodeMode::Root,
            DataplaneEncryption::WireGuard,
            Some("not-a-wireguard-key"),
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            Some(7_679),
        ),
        NetworkDataplane::try_new(
            "node-a",
            NetworkNodeMode::Root,
            DataplaneEncryption::WireGuard,
            Some(VALID_WIREGUARD_KEY),
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            None,
        ),
        NetworkDataplane::try_new(
            "node-a",
            NetworkNodeMode::Root,
            DataplaneEncryption::Direct,
            Some(VALID_WIREGUARD_KEY),
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            None,
        ),
        NetworkDataplane::try_new(
            "node-a",
            NetworkNodeMode::Root,
            DataplaneEncryption::Direct,
            None,
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            Some(7_679),
        ),
    ] {
        assert!(matches!(
            invalid,
            Err(NetworkTopologyError::CorruptResponse { .. })
        ));
    }

    let metadata = NetworkDataplane::try_new(
        "node-a",
        NetworkNodeMode::Root,
        DataplaneEncryption::WireGuard,
        Some(VALID_WIREGUARD_KEY),
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        Some(7_679),
    )
    .expect("valid metadata");
    assert!(matches!(
        NodeDataplaneResult::try_from_wire("node-a", false, Some(metadata)),
        Err(NetworkTopologyError::CorruptResponse { .. })
    ));
    assert!(matches!(
        NodeDataplaneResult::try_from_wire("node-a", true, None),
        Err(NetworkTopologyError::CorruptResponse { .. })
    ));
}

struct ObjectSafeNetwork;

impl LeaderNodeSubnetAllocation for ObjectSafeNetwork {
    fn allocate_node_subnet(
        &self,
        request: NodeSubnetAllocationRequest,
    ) -> NodeSubnetAllocationFuture<'_, NodeSubnetAllocationResult> {
        Box::pin(async move {
            Ok(NodeSubnetAllocationResult::Allocated(root_subnet(
                request.node_name(),
                "10.42.1.0/24",
                request.node_ip(),
            )))
        })
    }
}

impl LeaderNetworkTopologyQuery for ObjectSafeNetwork {
    fn get_node_subnet(
        &self,
        _request: NodeSubnetQuery,
    ) -> NetworkTopologyFuture<'_, NodeSubnetResult> {
        Box::pin(async { NodeSubnetResult::try_from_wire("node-a", false, None) })
    }

    fn list_peer_subnets(
        &self,
        _request: PeerSubnetsQuery,
    ) -> NetworkTopologyFuture<'_, PeerSubnetsResult> {
        Box::pin(async { PeerSubnetsResult::try_new("node-a", Vec::new()) })
    }

    fn get_node_dataplane(
        &self,
        _request: NodeDataplaneQuery,
    ) -> NetworkTopologyFuture<'_, NodeDataplaneResult> {
        Box::pin(async { NodeDataplaneResult::try_from_wire("node-a", false, None) })
    }
}

#[test]
fn focused_network_capabilities_are_independently_object_safe() {
    let allocation: &dyn LeaderNodeSubnetAllocation = &ObjectSafeNetwork;
    drop(
        allocation.allocate_node_subnet(
            NodeSubnetAllocationRequest::try_new("node-a", "10.42.0.0/16", "192.0.2.10")
                .expect("request"),
        ),
    );

    let topology: &dyn LeaderNetworkTopologyQuery = &ObjectSafeNetwork;
    drop(topology.get_node_subnet(NodeSubnetQuery::try_new("node-a").expect("query")));
    drop(topology.list_peer_subnets(PeerSubnetsQuery::try_new("node-a").expect("query")));
    drop(topology.get_node_dataplane(NodeDataplaneQuery::try_new("node-a").expect("query")));

    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<NodeSubnetAllocationRequest>();
    assert_send_sync::<NodeSubnetAllocationResult>();
    assert_send_sync::<NodeSubnetAllocationError>();
    assert_send_sync::<NetworkTopologyError>();
}

#[test]
fn allocation_failures_are_typed_for_conflict_exhaustion_and_retry() {
    let failures = [
        NodeSubnetAllocationError::Conflict {
            message: "node already owns a different subnet".to_string(),
        },
        NodeSubnetAllocationError::Exhausted {
            cluster_cidr: "10.42.0.0/24".to_string(),
        },
        NodeSubnetAllocationError::Retryable {
            message: "leader changed".to_string(),
        },
    ];
    assert!(matches!(
        failures[0],
        NodeSubnetAllocationError::Conflict { .. }
    ));
    assert!(matches!(
        failures[1],
        NodeSubnetAllocationError::Exhausted { .. }
    ));
    assert!(matches!(
        failures[2],
        NodeSubnetAllocationError::Retryable { .. }
    ));
}
