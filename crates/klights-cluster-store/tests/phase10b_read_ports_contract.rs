use std::net::Ipv4Addr;

use bytes::Bytes;
use klights_cluster_core::{Resource, WatchReplayPosition};
use klights_cluster_store::{
    ClusterOwnershipRead, ClusterResourceScopeRead, ClusterTopologyFuture, ClusterTopologyRead,
    DurableRawWatchEvent, DurableRawWatchHistoryRead, DurableWatchRangeRead,
    ModifiedClusterResourcesRequest, ModifiedResourcesRequest, NamespaceContentFuture,
    NamespaceContentRead, NamespaceKindRequest, NamespaceRequest, NodeTopologyRequest,
    OwnedKindRequest, OwnerNameKindRequest, OwnerUidRequest, OwnershipReadFuture,
    PeerTopologyRequest, PositionedRawWatchHistoryRead, RawWatchEventsAfterPositionRequest,
    RawWatchEventsSinceRequest, RawWatchHistoryFuture, RawWatchHistoryPage, RawWatchHistoryRead,
    ResourceKeyScopeRequest, ResourceReadFuture, ResourceScopeSnapshot,
    ResourceSnapshotAtPositionRequest, ResourceSnapshotRead, ResourceWatchTargetsRequest,
    StoredNodeSubnet, WatchEventsSinceRequest, WatchRangeFuture, WatchRangeStart,
};
use klights_cluster_store::{
    DataplaneEncryption, DataplaneMode, DataplanePeerMetadata, DurableWatchTarget,
};
use klights_types::{HostPortRange, NodeName, NodePeerMode, PodSubnet};

struct FakeReadPorts;

impl ClusterResourceScopeRead for FakeReadPorts {
    fn list_resources_for_watch_targets(
        &self,
        _request: ResourceWatchTargetsRequest,
    ) -> ResourceReadFuture<'_, ResourceScopeSnapshot> {
        Box::pin(async move {
            ResourceScopeSnapshot::try_new(Vec::new(), WatchReplayPosition::default())
        })
    }

    fn list_resource_keys_for_scope(
        &self,
        _request: ResourceKeyScopeRequest,
    ) -> ResourceReadFuture<'_, Vec<klights_cluster_store::ResourceCollectionKey>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn list_cluster_resources(&self) -> ResourceReadFuture<'_, Vec<Resource>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn snapshot_resources_at_position(
        &self,
        _request: ResourceSnapshotAtPositionRequest,
    ) -> ResourceReadFuture<'_, ResourceSnapshotRead> {
        Box::pin(async { Ok(ResourceSnapshotRead::Expired) })
    }
}

impl ClusterOwnershipRead for FakeReadPorts {
    fn find_owned_resources(
        &self,
        _request: OwnerUidRequest,
    ) -> OwnershipReadFuture<'_, Vec<Resource>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn list_resources_by_owner_uid(
        &self,
        _request: OwnedKindRequest,
    ) -> OwnershipReadFuture<'_, Vec<Resource>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn find_owned_by_name_kind_empty_uid(
        &self,
        _request: OwnerNameKindRequest,
    ) -> OwnershipReadFuture<'_, Vec<Resource>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

impl NamespaceContentRead for FakeReadPorts {
    fn list_namespace_resources(
        &self,
        _request: NamespaceRequest,
    ) -> NamespaceContentFuture<'_, Vec<Resource>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn list_namespace_resources_of_kind(
        &self,
        _request: NamespaceKindRequest,
    ) -> NamespaceContentFuture<'_, Vec<Resource>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn list_namespace_resources_excluding_kind(
        &self,
        _request: NamespaceKindRequest,
    ) -> NamespaceContentFuture<'_, Vec<Resource>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn count_namespace_resources(
        &self,
        _request: NamespaceRequest,
    ) -> NamespaceContentFuture<'_, i64> {
        Box::pin(async { Ok(0) })
    }
}

impl DurableWatchRangeRead for FakeReadPorts {
    fn list_cluster_resources_modified_since(
        &self,
        _request: ModifiedClusterResourcesRequest,
    ) -> WatchRangeFuture<'_, Vec<klights_cluster_store::DurableWatchEvent>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn list_resources_modified_since(
        &self,
        _request: ModifiedResourcesRequest,
    ) -> WatchRangeFuture<'_, Vec<klights_cluster_store::DurableWatchEvent>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn list_watch_events_since(
        &self,
        _request: WatchEventsSinceRequest,
    ) -> WatchRangeFuture<'_, Vec<klights_cluster_store::DurableWatchEvent>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn earliest_watch_event_rv(&self) -> WatchRangeFuture<'_, Option<i64>> {
        Box::pin(async { Ok(None) })
    }

    fn list_all_watch_events_since(
        &self,
        _request: WatchRangeStart,
    ) -> WatchRangeFuture<'_, Vec<klights_cluster_store::DurableWatchEvent>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn list_deleted_watch_events_since(
        &self,
        _request: WatchRangeStart,
    ) -> WatchRangeFuture<'_, Vec<klights_cluster_store::DurableWatchEvent>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

impl DurableRawWatchHistoryRead for FakeReadPorts {
    fn list_raw_watch_events_since_checked_bounded(
        &self,
        _request: RawWatchEventsSinceRequest,
    ) -> RawWatchHistoryFuture<'_, RawWatchHistoryRead> {
        Box::pin(async { Ok(RawWatchHistoryRead::Events(RawWatchHistoryPage::empty())) })
    }

    fn list_raw_watch_events_after_position_checked_bounded(
        &self,
        request: RawWatchEventsAfterPositionRequest,
    ) -> RawWatchHistoryFuture<'_, PositionedRawWatchHistoryRead> {
        Box::pin(async move {
            Ok(PositionedRawWatchHistoryRead::Events(
                klights_cluster_store::PositionedRawWatchHistoryPage::try_new(
                    Vec::new(),
                    request.position(),
                )?,
            ))
        })
    }
}

impl ClusterTopologyRead for FakeReadPorts {
    fn get_node_dataplane(
        &self,
        _request: NodeTopologyRequest,
    ) -> ClusterTopologyFuture<'_, Option<DataplanePeerMetadata>> {
        Box::pin(async { Ok(None) })
    }

    fn get_node_subnet(
        &self,
        _request: NodeTopologyRequest,
    ) -> ClusterTopologyFuture<'_, Option<StoredNodeSubnet>> {
        Box::pin(async { Ok(None) })
    }

    fn list_peer_subnets(
        &self,
        _request: PeerTopologyRequest,
    ) -> ClusterTopologyFuture<'_, Vec<StoredNodeSubnet>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

fn assert_resource_scope_object_safe(_: &dyn ClusterResourceScopeRead) {}
fn assert_ownership_object_safe(_: &dyn ClusterOwnershipRead) {}
fn assert_namespace_object_safe(_: &dyn NamespaceContentRead) {}
fn assert_watch_range_object_safe(_: &dyn DurableWatchRangeRead) {}
fn assert_raw_watch_object_safe(_: &dyn DurableRawWatchHistoryRead) {}
fn assert_topology_object_safe(_: &dyn ClusterTopologyRead) {}

#[test]
fn phase10b_read_ports_are_independently_object_safe() {
    let store = FakeReadPorts;
    assert_resource_scope_object_safe(&store);
    assert_ownership_object_safe(&store);
    assert_namespace_object_safe(&store);
    assert_watch_range_object_safe(&store);
    assert_raw_watch_object_safe(&store);
    assert_topology_object_safe(&store);
}

#[test]
fn request_values_preserve_legacy_empty_and_duplicate_target_domains() {
    let pod = DurableWatchTarget::namespaced("v1", "Pod");
    assert!(ResourceWatchTargetsRequest::try_new(Vec::new(), None).is_ok());
    let duplicate =
        ResourceWatchTargetsRequest::try_new(vec![pod.clone(), pod.clone()], None).unwrap();
    assert_eq!(duplicate.targets(), &[pod.clone(), pod.clone()]);
    assert!(
        ResourceSnapshotAtPositionRequest::try_new(
            Vec::new(),
            None,
            None,
            WatchReplayPosition::default(),
        )
        .is_ok()
    );
    assert!(WatchEventsSinceRequest::try_new(Vec::new(), 0).is_ok());
    let duplicate_watch =
        WatchEventsSinceRequest::try_new(vec![pod.clone(), pod.clone()], 0).unwrap();
    assert_eq!(duplicate_watch.targets(), &[pod.clone(), pod]);
    assert!(ResourceKeyScopeRequest::try_new("", "Pod", true).is_err());
    assert!(OwnerUidRequest::try_new("", None).is_err());
    assert!(OwnedKindRequest::try_new("v1", "", None, "uid-1").is_err());
    assert!(OwnerNameKindRequest::try_new("v1", "owner", "", None).is_err());
    assert!(NamespaceRequest::try_new("").is_err());
    assert!(NamespaceKindRequest::try_new("default", "").is_err());
    assert!(ModifiedClusterResourcesRequest::try_new("v1", "Pod", -1).is_err());
    assert!(ModifiedResourcesRequest::try_new("v1", "Pod", None, -1).is_err());
    assert!(WatchRangeStart::try_new(-1).is_err());
    assert!(NodeTopologyRequest::try_new("").is_err());
    assert!(PeerTopologyRequest::excluding("-bad").is_err());
}

#[test]
fn raw_watch_row_preserves_routing_identity_event_and_original_bytes() {
    let original = Bytes::from_static(
        br#"{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p","namespace":"ns","resourceVersion":"9"}}"#,
    );
    let row = DurableRawWatchEvent::try_new(
        "v1",
        "Pod",
        Some("ns".to_string()),
        "p",
        9,
        "MODIFIED",
        original.clone(),
    )
    .unwrap();

    assert_eq!(row.api_version(), "v1");
    assert_eq!(row.kind(), "Pod");
    assert_eq!(row.namespace(), Some("ns"));
    assert_eq!(row.name(), "p");
    assert_eq!(row.resource_version(), 9);
    assert_eq!(row.event_type(), "MODIFIED");
    assert_eq!(row.object_json(), &original);
    assert_eq!(row.into_object_json(), original);

    let legacy =
        DurableRawWatchEvent::try_new("v1", "Pod", None, "", 0, "", Bytes::from_static(b"null"))
            .expect("Redb rows with absent name and event type remain representable");
    assert_eq!(legacy.name(), "");
    assert_eq!(legacy.event_type(), "");
    assert_eq!(legacy.object_json(), &Bytes::from_static(b"null"));

    assert!(
        DurableRawWatchEvent::try_new(
            "v1",
            "Pod",
            Some("ns".to_string()),
            "p",
            -1,
            "MODIFIED",
            Bytes::new(),
        )
        .is_err()
    );
}

#[test]
fn raw_watch_requests_are_bounded_and_position_validated() {
    let target = vec![DurableWatchTarget::namespaced("v1", "Pod")];
    assert!(RawWatchEventsSinceRequest::try_new(Vec::new(), 0, 1).is_ok());
    assert!(
        RawWatchEventsSinceRequest::try_new(vec![target[0].clone(), target[0].clone()], 0, 1,)
            .is_ok()
    );
    assert!(RawWatchEventsSinceRequest::try_new(target.clone(), 0, 0).is_err());
    assert!(
        RawWatchEventsSinceRequest::try_new(
            target.clone(),
            0,
            klights_cluster_store::MAX_WATCH_HISTORY_PAGE + 1,
        )
        .is_err()
    );
    assert!(
        RawWatchEventsAfterPositionRequest::try_new(
            target,
            WatchReplayPosition {
                resource_version: 1,
                event_id: -1,
                resource_version_filter_through_event_id: 0,
            },
            1,
        )
        .is_err()
    );
}

#[test]
fn stored_node_subnet_preserves_the_persistence_row_shape() {
    let row = StoredNodeSubnet::new(
        NodeName::parse("cp-2").unwrap(),
        PodSubnet::parse("10.42.2.0/24").unwrap(),
        7,
        Ipv4Addr::new(10, 42, 2, 0),
        Ipv4Addr::new(192, 0, 2, 2),
        NodePeerMode::Rootless,
        None,
    );
    assert_eq!(row.node_name().as_str(), "cp-2");
    assert_eq!(row.subnet().to_string(), "10.42.2.0/24");
    assert_eq!(row.subnet_base_int(), 7);
    assert_eq!(row.gateway_ip(), Ipv4Addr::new(10, 42, 2, 0));
    assert_eq!(row.node_ip(), Ipv4Addr::new(192, 0, 2, 2));
    assert_eq!(row.mode(), NodePeerMode::Rootless);
    assert_eq!(row.hostport_range(), None);

    let legacy = StoredNodeSubnet::new(
        NodeName::parse("legacy").unwrap(),
        PodSubnet::parse("10.0.0.0/16").unwrap(),
        1,
        Ipv4Addr::new(192, 0, 2, 99),
        Ipv4Addr::new(192, 0, 2, 3),
        NodePeerMode::Root,
        Some(HostPortRange {
            start: 30_100,
            end: 30_000,
        }),
    );
    assert_eq!(legacy.subnet().prefix(), 16);
    assert_eq!(legacy.subnet_base_int(), 1);
    assert_eq!(legacy.gateway_ip(), Ipv4Addr::new(192, 0, 2, 99));
    assert!(legacy.hostport_range().is_some());
}

#[test]
fn peer_topology_request_models_all_and_excluding_without_empty_sentinel() {
    assert_eq!(PeerTopologyRequest::all().excluded_node_name(), None);
    let excluding = PeerTopologyRequest::excluding("cp-2").unwrap();
    assert_eq!(
        excluding.excluded_node_name().map(NodeName::as_str),
        Some("cp-2")
    );
}

#[test]
fn topology_dto_remains_persistence_owned_and_wireguard_validated() {
    let key = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [3_u8; 32]);
    let metadata = DataplanePeerMetadata::try_new(
        "cp-2".to_string(),
        DataplaneMode::Root,
        DataplaneEncryption::Enabled,
        Some(key),
        Some("192.0.2.2".to_string()),
        Some(7679),
    )
    .unwrap();
    assert_eq!(metadata.node_name, "cp-2");
}
