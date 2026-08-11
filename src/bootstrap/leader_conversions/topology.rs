use klights_leader_api::{
    DataplaneEncryption, HostPortRange as LeaderHostPortRange, NetworkDataplane, NetworkNodeMode,
    NetworkTopologyError,
};

pub(crate) fn focused_node_subnet(
    subnet: klights_cluster_store::StoredNodeSubnet,
) -> std::result::Result<klights_leader_api::NodeSubnet, NetworkTopologyError> {
    let mode = match subnet.mode {
        klights_controllers::annotations::NodePeerMode::Root => NetworkNodeMode::Root,
        klights_controllers::annotations::NodePeerMode::Rootless => NetworkNodeMode::Rootless,
    };
    let hostport_range = subnet
        .hostport_range
        .map(|range| LeaderHostPortRange::try_new(range.start, range.end))
        .transpose()?;
    klights_leader_api::NodeSubnet::try_new(
        subnet.node_name.into_string(),
        subnet.subnet.to_string(),
        subnet.subnet_base_int,
        subnet.gateway_ip,
        subnet.node_ip,
        mode,
        hostport_range,
    )
}

pub(crate) fn focused_dataplane(
    metadata: klights_cluster_store::DataplanePeerMetadata,
) -> std::result::Result<NetworkDataplane, NetworkTopologyError> {
    NetworkDataplane::try_new(
        metadata.node_name,
        match metadata.mode {
            klights_cluster_store::DataplaneMode::Root => NetworkNodeMode::Root,
            klights_cluster_store::DataplaneMode::Rootless => NetworkNodeMode::Rootless,
        },
        match metadata.encryption {
            klights_cluster_store::DataplaneEncryption::Enabled => DataplaneEncryption::WireGuard,
            klights_cluster_store::DataplaneEncryption::Disabled => DataplaneEncryption::Direct,
        },
        metadata.public_key.as_ref().map(|key| key.as_str()),
        metadata.endpoint,
        metadata.port,
    )
}

pub(crate) fn legacy_dataplane(
    metadata: NetworkDataplane,
) -> std::result::Result<klights_cluster_store::DataplanePeerMetadata, NetworkTopologyError> {
    let mode = match metadata.mode() {
        NetworkNodeMode::Root => klights_cluster_store::DataplaneMode::Root,
        NetworkNodeMode::Rootless => klights_cluster_store::DataplaneMode::Rootless,
    };
    let encryption = match metadata.encryption() {
        DataplaneEncryption::WireGuard => klights_cluster_store::DataplaneEncryption::Enabled,
        DataplaneEncryption::Direct => klights_cluster_store::DataplaneEncryption::Disabled,
    };
    klights_cluster_store::DataplanePeerMetadata::try_new(
        metadata.node_name().to_string(),
        mode,
        encryption,
        metadata.public_key().map(str::to_owned),
        Some(metadata.endpoint().to_string()),
        metadata.port(),
    )
    .map_err(|error| NetworkTopologyError::corrupt_response(error.to_string()))
}

pub(crate) fn node_subnet_allocation_is_exhausted(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("exhaust") || lower.contains("query returned no rows")
}
