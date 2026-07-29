use std::num::NonZeroUsize;

use klights_cluster_core::WatchReplayPosition;
use klights_cluster_store::{
    ClusterResourceRead, ClusterResourceScopeRead, DurableAllocatorRead, DurableWatchHistoryRead,
    DurableWatchRangeRead, DurableWatchTarget, ResourceCollectionScope, ResourceContinuation,
    ResourceListQuery as FocusedListQuery, ResourceListRead, ResourceListRequest,
    ResourceSnapshotAtPositionRequest, ResourceSnapshotRead, ResourceVersionMatch,
    ResourceWatchTargetsRequest, WatchEventsSinceRequest, WatchHistoryRead, WatchHistoryRequest,
};
use serde_json::json;

use crate::datastore::backend::DatastoreBackend;
use crate::datastore::redb::RedbDatastore;
use crate::datastore::types::{PositionedWatchReplayRead, ResourceListQuery, WatchTarget};
use klights_cluster_datastore::redb::tables;

async fn redb_db() -> RedbDatastore {
    RedbDatastore::new_in_memory().await.unwrap()
}

async fn create_config_map(db: &RedbDatastore, namespace: &str, name: &str, tracked: bool) {
    DatastoreBackend::create_resource(
        db,
        "v1",
        "ConfigMap",
        Some(namespace),
        name,
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "namespace": namespace,
                "name": name,
                "uid": format!("{namespace}-{name}"),
                "labels": {"tracked": tracked.to_string()},
            },
        }),
    )
    .await
    .unwrap();
}

fn page(read: ResourceListRead) -> klights_cluster_store::ResourceListPage {
    match read {
        ResourceListRead::Current(page) | ResourceListRead::Historical(page) => page,
        ResourceListRead::Expired { .. } => panic!("resource list unexpectedly expired"),
    }
}

#[tokio::test]
async fn focused_resource_read_preserves_legacy_selector_paging_and_remaining_count() {
    let db = redb_db().await;
    create_config_map(&db, "default", "alpha", true).await;
    create_config_map(&db, "default", "bravo", false).await;
    create_config_map(&db, "default", "charlie", true).await;

    let legacy = DatastoreBackend::list_resources(
        &db,
        "v1",
        "ConfigMap",
        Some("default"),
        ResourceListQuery::new(Some("tracked=true"), None, Some(1), None),
    )
    .await
    .unwrap();
    let focused = page(
        ClusterResourceRead::list_resources(
            db.focused_read_store().as_ref(),
            ResourceListRequest::new(
                "v1",
                "ConfigMap",
                ResourceCollectionScope::Namespace("default".to_string()),
                FocusedListQuery::try_new_borrowed(
                    Some("tracked=true"),
                    None,
                    Some(1),
                    None,
                    ResourceVersionMatch::Any,
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap(),
    );

    assert_eq!(
        focused
            .items()
            .iter()
            .map(|resource| resource.name.as_str())
            .collect::<Vec<_>>(),
        legacy
            .items
            .iter()
            .map(|resource| resource.name.as_str())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        focused.continuation().map(|cursor| cursor.after().name()),
        legacy.continue_token.as_deref()
    );
    assert_eq!(
        focused.remaining_item_count(),
        legacy.remaining_item_count,
        "selector pages omit an expensive exact remaining count"
    );

    let unfiltered = page(
        ClusterResourceRead::list_resources(
            db.focused_read_store().as_ref(),
            ResourceListRequest::new(
                "v1",
                "ConfigMap",
                ResourceCollectionScope::Namespace("default".to_string()),
                FocusedListQuery::try_new_borrowed(
                    None,
                    None,
                    Some(1),
                    None,
                    ResourceVersionMatch::Any,
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap(),
    );
    assert_eq!(unfiltered.remaining_item_count(), Some(2));
}

#[tokio::test]
async fn focused_all_namespaces_continuation_keeps_equal_names_in_namespace_order() {
    let db = redb_db().await;
    create_config_map(&db, "zeta", "same", true).await;
    create_config_map(&db, "alpha", "same", true).await;
    create_config_map(&db, "zeta", "tail", true).await;

    let first = page(
        ClusterResourceRead::list_resources(
            db.focused_read_store().as_ref(),
            ResourceListRequest::new(
                "v1",
                "ConfigMap",
                ResourceCollectionScope::AllNamespaces,
                FocusedListQuery::try_new_borrowed(
                    None,
                    None,
                    Some(1),
                    None,
                    ResourceVersionMatch::Any,
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap(),
    );
    assert_eq!(
        (
            first.items()[0].namespace.as_deref(),
            first.items()[0].name.as_str()
        ),
        (Some("alpha"), "same")
    );
    let continuation: ResourceContinuation = first.continuation().unwrap().clone();

    let second = page(
        ClusterResourceRead::list_resources(
            db.focused_read_store().as_ref(),
            ResourceListRequest::new(
                "v1",
                "ConfigMap",
                ResourceCollectionScope::AllNamespaces,
                FocusedListQuery::try_new(
                    None,
                    None,
                    Some(1),
                    Some(continuation),
                    ResourceVersionMatch::Any,
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap(),
    );
    assert_eq!(
        (
            second.items()[0].namespace.as_deref(),
            second.items()[0].name.as_str()
        ),
        (Some("zeta"), "same"),
        "a name-only cursor must not skip the same name in another namespace"
    );
}

#[tokio::test]
async fn focused_watch_history_honors_the_legacy_wildcard_position_floor() {
    let db = redb_db().await;
    create_config_map(&db, "default", "one", true).await;
    create_config_map(&db, "default", "two", true).await;
    create_config_map(&db, "default", "three", true).await;

    db.accessor
        .call("test:install-wildcard-replay-floor", |database| {
            let write = database.begin_write()?;
            {
                let mut rv_floors = write.open_table(tables::WATCH_REPLAY_FLOORS)?;
                rv_floors.insert(b"*\0*\0*".as_slice(), 2)?;
                let mut floors = write.open_table(tables::WATCH_REPLAY_POSITION_FLOORS)?;
                let mut encoded = [0_u8; 16];
                encoded[..8].copy_from_slice(&2_u64.to_be_bytes());
                encoded[8..].copy_from_slice(&2_u64.to_be_bytes());
                floors.insert(b"*\0*\0*".as_slice(), encoded.as_slice())?;
            }
            write.commit()?;
            Ok(())
        })
        .await
        .unwrap();

    let position = WatchReplayPosition {
        resource_version: 1,
        event_id: 1,
        resource_version_filter_through_event_id: 0,
    };
    let target = DurableWatchTarget::namespaced_in_namespace("v1", "ConfigMap", "default");
    let focused = DurableWatchHistoryRead::replay_watch_history(
        db.focused_read_store().as_ref(),
        WatchHistoryRequest::new(vec![target], position, 16).unwrap(),
    )
    .await
    .unwrap();
    assert!(matches!(focused, WatchHistoryRead::Expired));

    let legacy = DatastoreBackend::list_watch_events_after_position_checked_bounded(
        &db,
        &[WatchTarget::namespaced_in_namespace(
            "v1",
            "ConfigMap",
            "default",
        )],
        position,
        NonZeroUsize::new(16).unwrap(),
    )
    .await
    .unwrap();
    assert!(matches!(legacy, PositionedWatchReplayRead::Expired));
}

#[tokio::test]
async fn focused_watch_range_preserves_legacy_event_order_and_payloads() {
    let db = redb_db().await;
    create_config_map(&db, "default", "one", true).await;
    create_config_map(&db, "default", "two", true).await;

    let legacy = DatastoreBackend::list_watch_events_since(
        &db,
        &[WatchTarget::namespaced_in_namespace(
            "v1",
            "ConfigMap",
            "default",
        )],
        0,
    )
    .await
    .unwrap();
    let focused = DurableWatchRangeRead::list_watch_events_since(
        db.focused_read_store().as_ref(),
        WatchEventsSinceRequest::try_new(
            vec![DurableWatchTarget::namespaced_in_namespace(
                "v1",
                "ConfigMap",
                "default",
            )],
            0,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    assert_eq!(focused.len(), legacy.len());
    for (focused, legacy) in focused.iter().zip(&legacy) {
        assert_eq!(focused.event_type(), legacy.event_type.as_ref());
        assert_eq!(focused.resource().name, legacy.resource.name);
        assert_eq!(
            focused.resource().resource_version,
            legacy.resource.resource_version
        );
        assert_eq!(focused.resource().data, legacy.resource.data);
    }
}

#[tokio::test]
async fn focused_scope_and_positioned_snapshot_reuse_legacy_membership_reconstruction() {
    let db = redb_db().await;
    create_config_map(&db, "default", "before", true).await;
    let position = DatastoreBackend::current_watch_replay_position(&db)
        .await
        .unwrap();
    create_config_map(&db, "default", "after", true).await;

    let target = DurableWatchTarget::namespaced_in_namespace("v1", "ConfigMap", "default");
    let current = ClusterResourceScopeRead::list_resources_for_watch_targets(
        db.focused_read_store().as_ref(),
        ResourceWatchTargetsRequest::try_new(vec![target.clone()], Some("tracked=true".into()))
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        current
            .items()
            .iter()
            .map(|resource| resource.name.as_str())
            .collect::<Vec<_>>(),
        vec!["after", "before"]
    );

    let focused = ClusterResourceScopeRead::snapshot_resources_at_position(
        db.focused_read_store().as_ref(),
        ResourceSnapshotAtPositionRequest::try_new(
            vec![target],
            Some("tracked=true".into()),
            None,
            position,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let ResourceSnapshotRead::Historical(focused) = focused else {
        panic!("positioned focused snapshot was not historical");
    };

    let legacy = DatastoreBackend::snapshot_resources_at_position(
        &db,
        &[WatchTarget::namespaced_in_namespace(
            "v1",
            "ConfigMap",
            "default",
        )],
        Some("tracked=true"),
        None,
        position,
    )
    .await
    .unwrap();
    let crate::datastore::SnapshotAtRv::List(legacy) = legacy else {
        panic!("positioned legacy snapshot was not reconstructed");
    };

    assert_eq!(
        focused
            .items()
            .iter()
            .map(|resource| resource.name.as_str())
            .collect::<Vec<_>>(),
        legacy
            .items
            .iter()
            .map(|resource| resource.name.as_str())
            .collect::<Vec<_>>()
    );
    assert_eq!(focused.snapshot().position(), position);

    let allocator = DurableAllocatorRead::read_allocator_state(db.focused_read_store().as_ref())
        .await
        .unwrap();
    assert_eq!(
        allocator.position(),
        DatastoreBackend::current_watch_replay_position(&db)
            .await
            .unwrap()
    );
}
