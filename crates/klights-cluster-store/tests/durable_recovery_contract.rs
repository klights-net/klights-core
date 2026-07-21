use klights_cluster_core::{
    ClusterMembership, ClusterMetadata, LogApplyCommit, ResourceVersionAssignment,
    WatchReplayPosition,
};
use klights_cluster_store::{
    AllocatorStateError, AllocatorStateFuture, AuthoritativeSnapshot, AuthoritativeSnapshotCapture,
    AuthoritativeSnapshotPersistence, ClusterMetadataFuture, ClusterMetadataRead,
    ClusterMetadataStoreError, DurableAllocatorRead, DurableAllocatorState, DurableReplayFloor,
    DurableReplayTarget, DurableWatchHistoryRead, DurableWatchScope, DurableWatchTarget,
    MAX_SNAPSHOT_CAPTURE_PAGE, MAX_WATCH_HISTORY_PAGE, PersistedClusterMetadata,
    SnapshotCaptureHeader, SnapshotCapturePage, SnapshotCaptureSink, SnapshotMembership,
    SnapshotPersistenceError, SnapshotPersistenceFuture, WatchHistoryError, WatchHistoryFuture,
    WatchHistoryPage, WatchHistoryRead, WatchHistoryRequest,
};

struct FakeRecoveryStore;

impl DurableWatchHistoryRead for FakeRecoveryStore {
    fn replay_watch_history(
        &self,
        request: WatchHistoryRequest,
    ) -> WatchHistoryFuture<'_, WatchHistoryRead> {
        Box::pin(async move {
            Ok(WatchHistoryRead::Events(WatchHistoryPage::try_new(
                Vec::new(),
                request.position(),
            )?))
        })
    }

    fn list_replay_floors(&self) -> WatchHistoryFuture<'_, Vec<DurableReplayFloor>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

impl DurableAllocatorRead for FakeRecoveryStore {
    fn read_allocator_state(&self) -> AllocatorStateFuture<'_, DurableAllocatorState> {
        Box::pin(async {
            DurableAllocatorState::try_new(
                ResourceVersionAssignment::CommittedApplyV1,
                WatchReplayPosition {
                    resource_version: 17,
                    event_id: 23,
                    resource_version_filter_through_event_id: 0,
                },
            )
        })
    }
}

impl AuthoritativeSnapshotPersistence for FakeRecoveryStore {
    fn restore_authoritative_snapshot(
        &self,
        snapshot: AuthoritativeSnapshot,
    ) -> SnapshotPersistenceFuture<'_> {
        Box::pin(async move {
            assert_eq!(
                snapshot.position().map(|position| position.event_id),
                Some(23)
            );
            Ok(())
        })
    }
}

impl AuthoritativeSnapshotCapture for FakeRecoveryStore {
    fn capture_authoritative_snapshot<'a>(
        &'a self,
        _sink: &'a mut dyn SnapshotCaptureSink,
    ) -> SnapshotPersistenceFuture<'a, SnapshotCaptureHeader> {
        Box::pin(async {
            SnapshotCaptureHeader::try_new(
                Some(ResourceVersionAssignment::CommittedApplyV1),
                WatchReplayPosition {
                    resource_version: 17,
                    event_id: 23,
                    resource_version_filter_through_event_id: 0,
                },
                metadata(17),
                SnapshotMembership::AuthoritativeAbsent,
            )
        })
    }
}

impl ClusterMetadataRead for FakeRecoveryStore {
    fn read_cluster_metadata(&self) -> ClusterMetadataFuture<'_, PersistedClusterMetadata> {
        Box::pin(async {
            Ok(PersistedClusterMetadata::new(
                ClusterMetadata {
                    cluster_id: "cluster-a".to_string(),
                    leader_epoch: 3,
                    current_rv: 17,
                },
                SnapshotMembership::AuthoritativeAbsent,
            ))
        })
    }
}

fn assert_history_object_safe(_: &dyn DurableWatchHistoryRead) {}
fn assert_allocator_object_safe(_: &dyn DurableAllocatorRead) {}
fn assert_snapshot_object_safe(_: &dyn AuthoritativeSnapshotPersistence) {}
fn assert_capture_object_safe(_: &dyn AuthoritativeSnapshotCapture) {}
fn assert_metadata_object_safe(_: &dyn ClusterMetadataRead) {}

fn metadata(current_rv: i64) -> ClusterMetadata {
    ClusterMetadata {
        cluster_id: "cluster-a".to_string(),
        leader_epoch: 3,
        current_rv,
    }
}

fn membership() -> ClusterMembership {
    ClusterMembership {
        cluster_id: "cluster-a".to_string(),
        voters: vec!["cp-1".to_string(), "cp-2".to_string()],
        term: 9,
        leader_hint: Some("https://cp-1:7446".to_string()),
    }
}

#[test]
fn recovery_capabilities_are_distinct_and_object_safe() {
    let store = FakeRecoveryStore;
    assert_history_object_safe(&store);
    assert_allocator_object_safe(&store);
    assert_snapshot_object_safe(&store);
    assert_capture_object_safe(&store);
    assert_metadata_object_safe(&store);
}

#[test]
fn watch_history_values_preserve_position_scope_and_floor_exactly() {
    let position = WatchReplayPosition {
        resource_version: 17,
        event_id: 13,
        resource_version_filter_through_event_id: 19,
    };
    let targets = vec![
        DurableWatchTarget::cluster("v1", "Namespace"),
        DurableWatchTarget::namespaced("v1", "Pod"),
        DurableWatchTarget::namespaced_in_namespace("v1", "ConfigMap", "default"),
    ];
    let request = WatchHistoryRequest::new(targets.clone(), position, 64).unwrap();
    assert_eq!(request.targets(), targets.as_slice());
    assert_eq!(request.position(), position);
    assert_eq!(request.limit().get(), 64);
    assert_eq!(targets[0].scope(), &DurableWatchScope::Cluster);
    assert_eq!(targets[1].scope(), &DurableWatchScope::Namespaced(None));
    assert_eq!(
        targets[2].scope(),
        &DurableWatchScope::Namespaced(Some("default".to_string()))
    );

    let floor = DurableReplayFloor::namespaced("v1", "ConfigMap", "default", 11, 13, true).unwrap();
    assert_eq!(
        floor.target(),
        &DurableReplayTarget::Namespaced {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: "default".to_string(),
        }
    );
    assert_eq!(floor.resource_version(), 11);
    assert_eq!(floor.event_id(), 13);
    assert!(floor.position_is_exact());
    assert_eq!(
        DurableReplayFloor::cluster("v1", "Namespace", 7, 9, true)
            .unwrap()
            .target(),
        &DurableReplayTarget::Cluster {
            api_version: "v1".to_string(),
            kind: "Namespace".to_string(),
        }
    );
    assert_eq!(
        DurableReplayFloor::all(5, 6, false).unwrap().target(),
        &DurableReplayTarget::All
    );
}

#[test]
fn allocator_state_preserves_canonical_mode_position_and_exact_next_values() {
    let position = WatchReplayPosition {
        resource_version: 17,
        event_id: 23,
        resource_version_filter_through_event_id: 0,
    };
    let state =
        DurableAllocatorState::try_new(ResourceVersionAssignment::CommittedApplyV1, position)
            .unwrap();
    assert_eq!(
        state.resource_version_assignment(),
        ResourceVersionAssignment::CommittedApplyV1
    );
    assert_eq!(state.position(), position);
    assert_eq!(state.next_resource_version(), 18);
    assert_eq!(state.next_event_id(), 24);
}

#[test]
fn authoritative_snapshot_preserves_canonical_state_without_shadow_values() {
    let commits = vec![LogApplyCommit::new(17, Vec::new())];
    let position = WatchReplayPosition {
        resource_version: 17,
        event_id: 23,
        resource_version_filter_through_event_id: 0,
    };
    let floors =
        vec![DurableReplayFloor::namespaced("v1", "ConfigMap", "default", 11, 13, true).unwrap()];
    let snapshot = AuthoritativeSnapshot::try_new(
        commits.clone(),
        Some(ResourceVersionAssignment::CommittedApplyV1),
        Some(position),
        Some(floors.clone()),
        metadata(17),
        SnapshotMembership::Present(membership()),
    )
    .unwrap();

    assert_eq!(snapshot.commits(), commits.as_slice());
    assert_eq!(
        snapshot.resource_version_assignment(),
        Some(ResourceVersionAssignment::CommittedApplyV1)
    );
    assert_eq!(snapshot.position(), Some(position));
    assert_eq!(snapshot.replay_floors(), Some(floors.as_slice()));
    assert_eq!(snapshot.metadata(), &metadata(17));
    assert_eq!(
        snapshot.membership(),
        &SnapshotMembership::Present(membership())
    );
}

#[test]
fn persisted_cluster_metadata_keeps_canonical_values_in_one_observation() {
    let state =
        PersistedClusterMetadata::new(metadata(17), SnapshotMembership::Present(membership()));
    assert_eq!(state.metadata(), &metadata(17));
    assert_eq!(
        state.membership(),
        &SnapshotMembership::Present(membership())
    );
    assert_eq!(
        state.into_parts(),
        (metadata(17), SnapshotMembership::Present(membership()))
    );
}

#[test]
fn snapshot_presence_distinguishes_legacy_absence_from_explicit_empty_state() {
    let absent = AuthoritativeSnapshot::try_new(
        Vec::new(),
        None,
        None,
        None,
        metadata(0),
        SnapshotMembership::LegacyOmitted,
    )
    .unwrap();
    assert_eq!(absent.resource_version_assignment(), None);
    assert_eq!(absent.position(), None);
    assert_eq!(absent.replay_floors(), None);

    let explicit = AuthoritativeSnapshot::try_new(
        Vec::new(),
        Some(ResourceVersionAssignment::LegacyLeaderAssigned),
        Some(WatchReplayPosition::default()),
        Some(Vec::new()),
        metadata(0),
        SnapshotMembership::AuthoritativeAbsent,
    )
    .unwrap();
    assert_eq!(
        explicit.resource_version_assignment(),
        Some(ResourceVersionAssignment::LegacyLeaderAssigned)
    );
    assert_eq!(explicit.position(), Some(WatchReplayPosition::default()));
    assert_eq!(explicit.replay_floors(), Some([].as_slice()));
}

#[test]
fn recovery_values_reject_inexact_or_internally_inconsistent_state() {
    assert_eq!(
        WatchHistoryRequest::new(Vec::new(), WatchReplayPosition::default(), 0),
        Err(WatchHistoryError::InvalidLimit { limit: 0 })
    );
    assert!(matches!(
        DurableReplayFloor::namespaced("v1", "Pod", "default", -1, 0, false),
        Err(WatchHistoryError::InvalidReplayFloor { .. })
    ));
    assert_eq!(
        DurableAllocatorState::try_new(
            ResourceVersionAssignment::LegacyLeaderAssigned,
            WatchReplayPosition {
                resource_version: i64::MAX,
                event_id: 0,
                resource_version_filter_through_event_id: 0,
            }
        ),
        Err(AllocatorStateError::AllocatorExhausted {
            allocator: "resourceVersion",
            current: i64::MAX,
        })
    );

    let mismatch = AuthoritativeSnapshot::try_new(
        Vec::new(),
        Some(ResourceVersionAssignment::LegacyLeaderAssigned),
        Some(WatchReplayPosition::from_resource_version(9)),
        Some(Vec::new()),
        metadata(8),
        SnapshotMembership::AuthoritativeAbsent,
    );
    assert!(matches!(
        mismatch,
        Err(SnapshotPersistenceError::InvalidSnapshot { .. })
    ));

    let invalid_capture = SnapshotCaptureHeader::try_new(
        Some(ResourceVersionAssignment::CommittedApplyV1),
        WatchReplayPosition {
            resource_version: 1,
            event_id: 1,
            resource_version_filter_through_event_id: 0,
        },
        ClusterMetadata {
            cluster_id: "cluster-a".into(),
            leader_epoch: -1,
            current_rv: 1,
        },
        SnapshotMembership::Present(ClusterMembership {
            cluster_id: "cluster-a".into(),
            voters: Vec::new(),
            term: 0,
            leader_hint: None,
        }),
    );
    assert!(matches!(
        invalid_capture,
        Err(SnapshotPersistenceError::InvalidSnapshot { .. })
    ));
}

#[test]
fn watch_history_rejects_hostile_limits_and_invalid_positions_before_io() {
    let target = vec![DurableWatchTarget::namespaced("v1", "Pod")];
    assert!(
        WatchHistoryRequest::new(
            target.clone(),
            WatchReplayPosition::from_resource_version_through_event_id(1, 3),
            1,
        )
        .is_ok()
    );
    assert!(matches!(
        WatchHistoryRequest::new(
            target.clone(),
            WatchReplayPosition::default(),
            MAX_WATCH_HISTORY_PAGE + 1
        ),
        Err(WatchHistoryError::LimitTooLarge { .. })
    ));
    assert!(matches!(
        WatchHistoryRequest::new(
            target,
            WatchReplayPosition {
                resource_version: 1,
                event_id: 3,
                resource_version_filter_through_event_id: 3,
            },
            1
        ),
        Err(WatchHistoryError::InvalidPosition { .. })
    ));
}

#[test]
fn watch_history_rejects_empty_or_reserved_target_identities_before_io() {
    let invalid_targets = [
        DurableWatchTarget::cluster("", "Namespace"),
        DurableWatchTarget::cluster("v1", ""),
        DurableWatchTarget::cluster("*", "Namespace"),
        DurableWatchTarget::namespaced_in_namespace("v1", "Pod", ""),
        DurableWatchTarget::namespaced_in_namespace("v1", "Pod", "#cluster"),
    ];

    for target in invalid_targets {
        assert!(matches!(
            WatchHistoryRequest::new(vec![target], WatchReplayPosition::default(), 1),
            Err(WatchHistoryError::InvalidTarget { .. })
        ));
    }
}

#[test]
fn watch_history_rejects_empty_malformed_and_reserved_targets_before_io() {
    let invalid = [
        DurableWatchTarget::cluster("", "Pod"),
        DurableWatchTarget::cluster("v1", ""),
        DurableWatchTarget::cluster("*", "Pod"),
        DurableWatchTarget::cluster("v1", "#cluster"),
        DurableWatchTarget::namespaced_in_namespace("v1", "Pod", ""),
        DurableWatchTarget::namespaced_in_namespace("v1", "Pod", "*"),
        DurableWatchTarget::namespaced_in_namespace("v1", "Pod", "#cluster"),
        DurableWatchTarget::cluster("v1/", "Pod"),
        DurableWatchTarget::cluster("v1", "Pod/List"),
        DurableWatchTarget::namespaced_in_namespace("v1", "Pod", "bad/name"),
    ];

    for target in invalid {
        assert!(matches!(
            WatchHistoryRequest::new(vec![target], WatchReplayPosition::default(), 1),
            Err(WatchHistoryError::InvalidTarget { .. })
        ));
    }
}

#[test]
fn replay_floor_preserves_legacy_false_with_positive_stored_event_id() {
    let floor = DurableReplayFloor::namespaced("v1", "Pod", "default", 7, 99, false).unwrap();
    assert!(!floor.position_is_exact());
    assert_eq!(floor.event_id(), 99);
    assert!(matches!(
        floor.boundary(),
        klights_cluster_store::DurableReplayBoundary::LegacyResourceVersion {
            resource_version: 7,
            stored_event_id: 99,
        }
    ));
    assert!(matches!(
        DurableReplayFloor::namespaced("*", "Pod", "default", 1, 0, false),
        Err(WatchHistoryError::InvalidTarget { .. })
    ));
}

#[test]
fn exact_floor_requires_snapshot_position_and_duplicate_targets_fail_closed() {
    let exact = DurableReplayFloor::cluster("v1", "Namespace", 0, 0, true).unwrap();
    let missing_position = AuthoritativeSnapshot::try_new(
        Vec::new(),
        None,
        None,
        Some(vec![exact]),
        metadata(0),
        SnapshotMembership::AuthoritativeAbsent,
    );
    assert!(matches!(
        missing_position,
        Err(SnapshotPersistenceError::InvalidSnapshot { .. })
    ));

    let floor = DurableReplayFloor::all(0, 0, false).unwrap();
    let duplicate = AuthoritativeSnapshot::try_new(
        Vec::new(),
        None,
        None,
        Some(vec![floor.clone(), floor]),
        metadata(0),
        SnapshotMembership::AuthoritativeAbsent,
    );
    assert!(matches!(
        duplicate,
        Err(SnapshotPersistenceError::InvalidSnapshot { .. })
    ));
}

#[test]
fn capture_pages_are_nonempty_and_strictly_bounded() {
    assert!(SnapshotCapturePage::try_commits(Vec::new()).is_err());
    assert!(
        SnapshotCapturePage::try_commits(
            (0..=MAX_SNAPSHOT_CAPTURE_PAGE)
                .map(|index| LogApplyCommit::new(index as i64 + 1, Vec::new()))
                .collect()
        )
        .is_err()
    );
    assert_eq!(
        SnapshotCapturePage::try_commits(vec![LogApplyCommit::new(1, Vec::new())])
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn capture_pages_offer_narrow_consuming_handoff_by_family() {
    let commits = vec![LogApplyCommit::new(1, Vec::new())];
    let page = SnapshotCapturePage::try_commits(commits.clone()).unwrap();
    assert_eq!(page.into_commits(), Some(commits));

    let floor = DurableReplayFloor::all(0, 0, false).unwrap();
    let page = SnapshotCapturePage::try_replay_floors(vec![floor.clone()]).unwrap();
    assert_eq!(page.into_replay_floors(), Some(vec![floor]));
}

#[test]
fn snapshot_parts_expose_consuming_accessors_not_public_fields() {
    let mut parts = AuthoritativeSnapshot::try_new(
        Vec::new(),
        None,
        None,
        None,
        metadata(0),
        SnapshotMembership::LegacyOmitted,
    )
    .unwrap()
    .into_parts();
    assert_eq!(parts.metadata(), &metadata(0));
    assert_eq!(parts.membership(), &SnapshotMembership::LegacyOmitted);
    let commits = parts.take_commits();
    let (_, membership) = parts.into_metadata_and_membership();
    assert!(commits.is_empty());
    assert_eq!(membership, SnapshotMembership::LegacyOmitted);
}

#[test]
fn recovery_errors_are_typed_and_adapter_neutral() {
    assert_eq!(
        WatchHistoryError::persistence_failed("history failed").to_string(),
        "history failed"
    );
    assert_eq!(
        AllocatorStateError::persistence_failed("allocator failed").to_string(),
        "allocator failed"
    );
    assert_eq!(
        SnapshotPersistenceError::persistence_failed("restore failed").to_string(),
        "restore failed"
    );
    assert_eq!(
        ClusterMetadataStoreError::persistence_failed("metadata failed").to_string(),
        "metadata failed"
    );
}
