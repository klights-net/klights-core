use std::sync::{Arc, Mutex};

use futures::StreamExt as _;
use klights_cluster_core::{PositionedWatchEvent, Resource, WatchReplayPosition};
use klights_cluster_store::{
    AllocatorStateFuture, DurableAllocatorRead, DurableAllocatorState, DurableWatchEvent,
    DurableWatchHistoryRead, ResourceCollectionKey, ResourceCollectionScope, ResourceContinuation,
    ResourceGetRequest, ResourceListPage, ResourceListRead, ResourceListRequest,
    ResourceListSnapshot, ResourceReadFuture, ResourceVersionMatch, WatchHistoryFuture,
    WatchHistoryPage, WatchHistoryRead, WatchHistoryRequest,
};
use klights_leader_api::{
    CacheReadinessRequest, LeaderWatch, ResourceListRequest as LeaderListRequest,
    ResourceListScope, ResourceQueryConsistency, WatchEventType, WatchRequest,
};
use klights_types::ResourceKey;
use klights_watch::{
    PositionedWatchService, WatchCache, WatchResourceScope, WatchScopeResolver, WatchSignalHub,
};

fn position(resource_version: i64, event_id: i64) -> WatchReplayPosition {
    WatchReplayPosition {
        resource_version,
        event_id,
        resource_version_filter_through_event_id: 0,
    }
}

fn selected_resource(resource_version: i64, selected: bool) -> Resource {
    Resource::try_from_data(Arc::new(serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "namespace": "default",
            "name": "moving",
            "uid": "uid-moving",
            "labels": {"track": if selected { "yes" } else { "no" }},
            "resourceVersion": resource_version.to_string()
        },
        "data": {"visible": selected.to_string()}
    })))
    .expect("valid ConfigMap")
}

struct ExactBaseline {
    expected: WatchReplayPosition,
    observed: Arc<Mutex<Vec<WatchReplayPosition>>>,
}

#[derive(Clone, Copy)]
enum InvalidBaselineCase {
    Current,
    MismatchedSnapshot,
    Continued,
    Duplicate,
    OutOfScope,
    SelectorMismatch,
}

struct InvalidBaseline {
    case: InvalidBaselineCase,
    requested: WatchReplayPosition,
}

impl klights_cluster_store::ClusterResourceRead for InvalidBaseline {
    fn get_resource(
        &self,
        _request: ResourceGetRequest,
    ) -> ResourceReadFuture<'_, Option<Resource>> {
        Box::pin(async { panic!("watch baseline uses LIST") })
    }

    fn list_resources(
        &self,
        _request: ResourceListRequest,
    ) -> ResourceReadFuture<'_, ResourceListRead> {
        let snapshot_position = match self.case {
            InvalidBaselineCase::MismatchedSnapshot => position(9, 19),
            _ => self.requested,
        };
        let snapshot = ResourceListSnapshot::try_new(snapshot_position).unwrap();
        let mut resources = match self.case {
            InvalidBaselineCase::Duplicate => {
                vec![selected_resource(10, true), selected_resource(10, true)]
            }
            InvalidBaselineCase::OutOfScope => {
                let mut resource = selected_resource(10, true);
                Arc::make_mut(&mut resource.data)["metadata"]["namespace"] =
                    serde_json::Value::String("other".to_string());
                vec![Resource::try_from_data(resource.data).unwrap()]
            }
            InvalidBaselineCase::SelectorMismatch => vec![selected_resource(10, false)],
            _ => vec![selected_resource(10, true)],
        };
        let continuation = matches!(self.case, InvalidBaselineCase::Continued).then(|| {
            ResourceContinuation::new(
                ResourceCollectionKey::new(Some("default".to_string()), "moving"),
                snapshot,
            )
        });
        let page =
            ResourceListPage::try_new(std::mem::take(&mut resources), snapshot, continuation, None)
                .unwrap();
        let read = if matches!(self.case, InvalidBaselineCase::Current) {
            ResourceListRead::Current(page)
        } else {
            ResourceListRead::Historical(page)
        };
        Box::pin(async move { Ok(read) })
    }
}

impl klights_cluster_store::ClusterResourceRead for ExactBaseline {
    fn get_resource(
        &self,
        _request: ResourceGetRequest,
    ) -> ResourceReadFuture<'_, Option<Resource>> {
        Box::pin(async { panic!("watch baseline uses LIST") })
    }

    fn list_resources(
        &self,
        request: ResourceListRequest,
    ) -> ResourceReadFuture<'_, ResourceListRead> {
        assert_eq!(request.api_version(), "v1");
        assert_eq!(request.kind(), "ConfigMap");
        assert_eq!(
            request.scope(),
            &ResourceCollectionScope::Namespace("default".to_string())
        );
        assert_eq!(request.query().label_selector(), Some("track=yes"));
        let ResourceVersionMatch::AtPosition(observed) = request.query().resource_version_match()
        else {
            panic!("selector baseline must use the exact positioned handoff");
        };
        assert_eq!(observed, self.expected);
        self.observed.lock().expect("observed lock").push(observed);
        let page = ResourceListPage::try_new(
            vec![selected_resource(10, true)],
            ResourceListSnapshot::try_new(self.expected).expect("exact snapshot"),
            None,
            None,
        )
        .expect("baseline page");
        Box::pin(async move { Ok(ResourceListRead::Historical(page)) })
    }
}

struct FixedAllocator(DurableAllocatorState);

impl DurableAllocatorRead for FixedAllocator {
    fn read_allocator_state(&self) -> AllocatorStateFuture<'_, DurableAllocatorState> {
        let state = self.0;
        Box::pin(async move { Ok(state) })
    }
}

struct LeaveHistory {
    delivered: Mutex<bool>,
    position: WatchReplayPosition,
}

impl DurableWatchHistoryRead for LeaveHistory {
    fn replay_watch_history(
        &self,
        request: WatchHistoryRequest,
    ) -> WatchHistoryFuture<'_, WatchHistoryRead> {
        let mut delivered = self.delivered.lock().expect("history lock");
        let page = if std::mem::replace(&mut *delivered, true) {
            WatchHistoryPage::try_new(Vec::new(), self.position).expect("empty history tail")
        } else {
            assert_eq!(request.position(), position(10, 20));
            WatchHistoryPage::try_new(
                vec![PositionedWatchEvent {
                    position: self.position,
                    event: DurableWatchEvent::new("MODIFIED", selected_resource(11, false)),
                }],
                self.position,
            )
            .expect("leave page")
        };
        Box::pin(async move { Ok(WatchHistoryRead::Events(page)) })
    }

    fn list_replay_floors(
        &self,
    ) -> WatchHistoryFuture<'_, Vec<klights_cluster_store::DurableReplayFloor>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

struct NamespacedScopes;

impl WatchScopeResolver for NamespacedScopes {
    fn resource_scope<'a>(
        &'a self,
        _api_version: &'a str,
        _kind: &'a str,
    ) -> futures::future::BoxFuture<
        'a,
        Result<WatchResourceScope, klights_leader_api::LeaderWatchError>,
    > {
        Box::pin(async { Ok(WatchResourceScope::Namespaced) })
    }
}

#[tokio::test]
async fn selector_baseline_is_positioned_and_leave_reuses_the_cached_matching_object() {
    let baseline_position = position(10, 20);
    let leave_position = position(11, 21);
    let observed = Arc::new(Mutex::new(Vec::new()));
    let service = PositionedWatchService::new(
        Arc::new(ExactBaseline {
            expected: baseline_position,
            observed: observed.clone(),
        }),
        Arc::new(LeaveHistory {
            delivered: Mutex::new(false),
            position: leave_position,
        }),
        Arc::new(FixedAllocator(
            DurableAllocatorState::try_new(baseline_position).expect("allocator state"),
        )),
        Arc::new(WatchSignalHub::new(1)),
        Arc::new(NamespacedScopes),
    );
    let mut stream = service
        .watch_resources(
            WatchRequest::try_new(
                "v1",
                "ConfigMap",
                Some("default".to_string()),
                Some("track=yes".to_string()),
                None,
                Some(10),
                Some(baseline_position),
            )
            .expect("selected watch"),
        )
        .await
        .expect("watch opens");

    let leave = stream
        .next()
        .await
        .expect("selector leave")
        .expect("valid selector leave");
    assert_eq!(leave.event_type(), WatchEventType::Deleted);
    assert_eq!(leave.resource().data["metadata"]["labels"]["track"], "yes");
    assert_eq!(leave.resource().resource_version, 10);
    assert_eq!(leave.resume_position(), Some(leave_position));
    assert_eq!(
        observed.lock().expect("observed lock").as_slice(),
        &[baseline_position]
    );
}

#[tokio::test]
async fn selector_baseline_requires_the_exact_complete_unique_matching_snapshot() {
    let requested = position(10, 20);
    for (name, case) in [
        ("current sentinel", InvalidBaselineCase::Current),
        (
            "mismatched snapshot",
            InvalidBaselineCase::MismatchedSnapshot,
        ),
        ("continued page", InvalidBaselineCase::Continued),
        ("duplicate identity", InvalidBaselineCase::Duplicate),
        ("out of scope", InvalidBaselineCase::OutOfScope),
        ("selector mismatch", InvalidBaselineCase::SelectorMismatch),
    ] {
        let service = PositionedWatchService::new(
            Arc::new(InvalidBaseline { case, requested }),
            Arc::new(LeaveHistory {
                delivered: Mutex::new(true),
                position: requested,
            }),
            Arc::new(FixedAllocator(
                DurableAllocatorState::try_new(requested).unwrap(),
            )),
            Arc::new(WatchSignalHub::new(1)),
            Arc::new(NamespacedScopes),
        );
        let result = service
            .watch_resources(
                WatchRequest::try_new(
                    "v1",
                    "ConfigMap",
                    Some("default".to_string()),
                    Some("track=yes".to_string()),
                    None,
                    Some(10),
                    Some(requested),
                )
                .unwrap(),
            )
            .await;
        assert!(result.is_err(), "invalid baseline accepted: {name}");
    }
}

#[tokio::test]
async fn watch_cache_coordinates_filtered_scope_readiness_and_event_application() {
    let cache = WatchCache::new();
    let list_request = LeaderListRequest::try_new(
        "v1",
        "ConfigMap",
        ResourceListScope::Namespace("default".to_string()),
        Some("track=yes".to_string()),
        None,
        None,
        None,
        ResourceQueryConsistency::Cached,
    )
    .expect("cache request");
    let readiness = CacheReadinessRequest::try_new(
        "v1",
        "ConfigMap",
        Some("default".to_string()),
        Some("track=yes".to_string()),
        None,
    )
    .expect("readiness scope");
    cache
        .replace_scope(
            &list_request,
            vec![selected_resource(10, true)],
            position(10, 20),
        )
        .await
        .expect("prime cache");
    cache
        .mark_ready(readiness.clone())
        .await
        .expect("baseline may become ready");
    assert!(cache.is_ready(&readiness).await);
    cache.wait_ready(readiness).await;

    let key = ResourceKey::new("v1", "ConfigMap", Some("default".to_string()), "moving");
    assert_eq!(cache.get(&key).await.unwrap().resource_version, 10);
    let listed = cache
        .list(&list_request)
        .await
        .expect("filtered cache LIST");
    assert_eq!(listed.items().len(), 1);
    assert_eq!(listed.watch_replay_position(), Some(position(10, 20)));
}

#[tokio::test]
async fn watch_cache_matches_omitted_node_unschedulable_as_false() {
    let cache = WatchCache::new();
    let request = LeaderListRequest::try_new(
        "v1",
        "Node",
        ResourceListScope::Cluster,
        None,
        Some("spec.unschedulable=false".to_string()),
        None,
        None,
        ResourceQueryConsistency::Cached,
    )
    .expect("cache request");
    let node = Resource::try_from_data(Arc::new(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": "node-a",
            "uid": "uid-node-a",
            "resourceVersion": "10"
        },
        "spec": {}
    })))
    .expect("valid Node");
    cache
        .replace_scope(&request, vec![node], position(10, 20))
        .await
        .expect("omitted unschedulable uses the Node default");
    let readiness = CacheReadinessRequest::try_new(
        "v1",
        "Node",
        None,
        None,
        Some("spec.unschedulable=false".to_string()),
    )
    .expect("readiness scope");
    cache.mark_ready(readiness).await.expect("baseline ready");

    assert_eq!(cache.list(&request).await.unwrap().items().len(), 1);
}

#[tokio::test]
async fn cache_rejects_unready_reads() {
    let cache = WatchCache::new();
    let request = LeaderListRequest::try_new(
        "v1",
        "ConfigMap",
        ResourceListScope::Namespace("default".to_string()),
        None,
        None,
        None,
        None,
        ResourceQueryConsistency::Cached,
    )
    .expect("cache request");
    cache
        .replace_scope(
            &request,
            vec![selected_resource(10, true)],
            position(10, 20),
        )
        .await
        .expect("prime cache");
    assert!(
        cache.list(&request).await.is_err(),
        "unready cache reads must fail"
    );
}

#[tokio::test]
async fn readiness_requires_a_scope_baseline_and_never_exposes_global_entries() {
    let cache = WatchCache::new();
    cache.insert(selected_resource(10, true)).await;
    let request = LeaderListRequest::try_new(
        "v1",
        "ConfigMap",
        ResourceListScope::Namespace("default".to_string()),
        Some("track=yes".to_string()),
        None,
        None,
        None,
        ResourceQueryConsistency::Cached,
    )
    .expect("cache request");
    let readiness = CacheReadinessRequest::try_new(
        "v1",
        "ConfigMap",
        Some("default".to_string()),
        Some("track=yes".to_string()),
        None,
    )
    .expect("readiness");
    assert!(cache.mark_ready(readiness).await.is_err());
    assert!(cache.list(&request).await.is_err());
}

#[tokio::test]
async fn replacement_rejects_duplicate_keys_and_bodies_newer_than_the_position() {
    let cache = WatchCache::new();
    let request = LeaderListRequest::try_new(
        "v1",
        "ConfigMap",
        ResourceListScope::Namespace("default".to_string()),
        None,
        None,
        None,
        None,
        ResourceQueryConsistency::Cached,
    )
    .expect("cache request");
    assert!(
        cache
            .replace_scope(
                &request,
                vec![selected_resource(10, true), selected_resource(10, true)],
                position(10, 20),
            )
            .await
            .is_err()
    );
    assert!(
        cache
            .replace_scope(
                &request,
                vec![selected_resource(11, true)],
                position(10, 20)
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn equal_event_id_with_changed_cursor_cannot_replace_a_ready_snapshot() {
    let cache = WatchCache::new();
    let request = LeaderListRequest::try_new(
        "v1",
        "ConfigMap",
        ResourceListScope::Namespace("default".to_string()),
        None,
        None,
        None,
        None,
        ResourceQueryConsistency::Cached,
    )
    .expect("cache request");
    cache
        .replace_scope(
            &request,
            vec![selected_resource(20, true)],
            position(20, 30),
        )
        .await
        .expect("initial snapshot");
    cache
        .replace_scope(&request, Vec::new(), position(21, 30))
        .await
        .expect("stale replacement is ignored");
    let readiness =
        CacheReadinessRequest::try_new("v1", "ConfigMap", Some("default".to_string()), None, None)
            .expect("readiness");
    cache.mark_ready(readiness).await.expect("baseline ready");
    let list = cache.list(&request).await.expect("ready cache");
    assert_eq!(list.items().len(), 1);
    assert_eq!(list.watch_replay_position(), Some(position(20, 30)));
}

#[tokio::test]
async fn cache_advances_the_scope_position_after_apply() {
    let cache = WatchCache::new();
    let request = LeaderListRequest::try_new(
        "v1",
        "ConfigMap",
        ResourceListScope::Namespace("default".to_string()),
        None,
        None,
        None,
        None,
        ResourceQueryConsistency::Cached,
    )
    .expect("cache request");
    cache
        .replace_scope(
            &request,
            vec![selected_resource(10, true)],
            position(10, 20),
        )
        .await
        .expect("prime cache");
    let readiness =
        CacheReadinessRequest::try_new("v1", "ConfigMap", Some("default".to_string()), None, None)
            .expect("readiness scope");
    cache
        .mark_ready(readiness)
        .await
        .expect("baseline may become ready");
    let event_position = position(11, 21);
    let event = klights_leader_api::ResourceEvent::try_new(
        WatchEventType::Modified,
        selected_resource(11, true),
        Some(event_position),
    )
    .expect("positioned event");
    assert!(cache.apply_event(&event).await.is_some());
    let list = cache.list(&request).await.expect("ready cache LIST");
    assert_eq!(list.watch_replay_position(), Some(event_position));
}

#[tokio::test]
async fn replacing_an_older_overlapping_scope_does_not_evict_newer_scope_state() {
    let cache = WatchCache::new();
    let selected = LeaderListRequest::try_new(
        "v1",
        "ConfigMap",
        ResourceListScope::Namespace("default".to_string()),
        Some("track=yes".to_string()),
        None,
        None,
        None,
        ResourceQueryConsistency::Cached,
    )
    .expect("selected request");
    let unfiltered = LeaderListRequest::try_new(
        "v1",
        "ConfigMap",
        ResourceListScope::Namespace("default".to_string()),
        None,
        None,
        None,
        None,
        ResourceQueryConsistency::Cached,
    )
    .expect("unfiltered request");
    cache
        .replace_scope(
            &selected,
            vec![selected_resource(20, true)],
            position(20, 30),
        )
        .await
        .expect("new selected snapshot");
    cache
        .replace_scope(&unfiltered, Vec::new(), position(10, 15))
        .await
        .expect("older overlapping snapshot");
    cache
        .mark_ready(
            CacheReadinessRequest::try_new(
                "v1",
                "ConfigMap",
                Some("default".to_string()),
                Some("track=yes".to_string()),
                None,
            )
            .expect("readiness"),
        )
        .await
        .expect("baseline may become ready");

    let list = cache.list(&selected).await.expect("selected cache LIST");
    assert_eq!(list.items().len(), 1);
    assert_eq!(list.items()[0].resource_version, 20);
}

#[tokio::test]
async fn overlapping_scopes_keep_atomic_bodies_at_their_own_frontiers_in_both_orders() {
    for newer_selected in [false, true] {
        let cache = WatchCache::new();
        let selected = LeaderListRequest::try_new(
            "v1",
            "ConfigMap",
            ResourceListScope::Namespace("default".to_string()),
            Some("track=yes".to_string()),
            None,
            None,
            None,
            ResourceQueryConsistency::Cached,
        )
        .unwrap();
        let broad = LeaderListRequest::try_new(
            "v1",
            "ConfigMap",
            ResourceListScope::Namespace("default".to_string()),
            None,
            None,
            None,
            None,
            ResourceQueryConsistency::Cached,
        )
        .unwrap();
        let (selected_rv, broad_rv) = if newer_selected { (20, 10) } else { (10, 20) };
        let selected_body = selected_resource(selected_rv, true);
        let broad_body = selected_resource(broad_rv, false);
        if newer_selected {
            cache
                .replace_scope(&broad, vec![broad_body], position(broad_rv, broad_rv + 10))
                .await
                .unwrap();
            cache
                .replace_scope(
                    &selected,
                    vec![selected_body],
                    position(selected_rv, selected_rv + 10),
                )
                .await
                .unwrap();
        } else {
            cache
                .replace_scope(
                    &selected,
                    vec![selected_body],
                    position(selected_rv, selected_rv + 10),
                )
                .await
                .unwrap();
            cache
                .replace_scope(&broad, vec![broad_body], position(broad_rv, broad_rv + 10))
                .await
                .unwrap();
        }
        for readiness in [
            CacheReadinessRequest::try_new(
                "v1",
                "ConfigMap",
                Some("default".to_string()),
                Some("track=yes".to_string()),
                None,
            )
            .unwrap(),
            CacheReadinessRequest::try_new(
                "v1",
                "ConfigMap",
                Some("default".to_string()),
                None,
                None,
            )
            .unwrap(),
        ] {
            cache.mark_ready(readiness).await.unwrap();
        }
        let selected_list = cache.list(&selected).await.unwrap();
        let broad_list = cache.list(&broad).await.unwrap();
        assert_eq!(
            selected_list.items().len(),
            1,
            "newer_selected={newer_selected}"
        );
        assert_eq!(selected_list.items()[0].resource_version, selected_rv);
        assert_eq!(
            selected_list.items()[0].data["metadata"]["labels"]["track"],
            "yes"
        );
        assert_eq!(broad_list.items().len(), 1);
        assert_eq!(broad_list.items()[0].resource_version, broad_rv);
        assert_eq!(
            broad_list.items()[0].data["metadata"]["labels"]["track"],
            "no"
        );
    }
}

#[tokio::test]
async fn delete_frontier_tombstone_prevents_stale_resurrection_in_every_scope() {
    let cache = WatchCache::new();
    let selected = LeaderListRequest::try_new(
        "v1",
        "ConfigMap",
        ResourceListScope::Namespace("default".to_string()),
        Some("track=yes".to_string()),
        None,
        None,
        None,
        ResourceQueryConsistency::Cached,
    )
    .unwrap();
    let broad = LeaderListRequest::try_new(
        "v1",
        "ConfigMap",
        ResourceListScope::Namespace("default".to_string()),
        None,
        None,
        None,
        None,
        ResourceQueryConsistency::Cached,
    )
    .unwrap();
    for request in [&selected, &broad] {
        cache
            .replace_scope(request, vec![selected_resource(20, true)], position(20, 30))
            .await
            .unwrap();
        cache
            .mark_ready(
                CacheReadinessRequest::try_new(
                    "v1",
                    "ConfigMap",
                    Some("default".to_string()),
                    request.label_selector().map(str::to_owned),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();
    }
    let deleted = klights_leader_api::ResourceEvent::try_new(
        WatchEventType::Deleted,
        selected_resource(30, true),
        Some(position(30, 40)),
    )
    .unwrap();
    assert!(cache.apply_event(&deleted).await.is_some());
    let stale = klights_leader_api::ResourceEvent::try_new(
        WatchEventType::Added,
        selected_resource(25, true),
        Some(position(25, 35)),
    )
    .unwrap();
    assert!(cache.apply_event(&stale).await.is_none());
    for request in [&selected, &broad] {
        let list = cache.list(request).await.unwrap();
        assert!(list.items().is_empty());
        assert_eq!(list.watch_replay_position(), Some(position(30, 40)));
    }
}

#[tokio::test]
async fn delete_tombstone_prevents_an_older_new_scope_snapshot_from_resurrecting_global_get() {
    let cache = WatchCache::new();
    let key = ResourceKey::new("v1", "ConfigMap", Some("default".to_string()), "moving");
    let deleted = klights_leader_api::ResourceEvent::try_new(
        WatchEventType::Deleted,
        selected_resource(30, true),
        Some(position(30, 40)),
    )
    .unwrap();
    assert!(cache.apply_event(&deleted).await.is_some());

    let stale_scope = LeaderListRequest::try_new(
        "v1",
        "ConfigMap",
        ResourceListScope::Namespace("default".to_string()),
        Some("track=yes".to_string()),
        None,
        None,
        None,
        ResourceQueryConsistency::Cached,
    )
    .unwrap();
    cache
        .replace_scope(
            &stale_scope,
            vec![selected_resource(20, true)],
            position(20, 30),
        )
        .await
        .unwrap();

    assert!(
        cache.get(&key).await.is_none(),
        "an older scope baseline must not overwrite a newer global delete tombstone"
    );

    cache
        .replace_scope(
            &stale_scope,
            vec![selected_resource(50, true)],
            position(50, 60),
        )
        .await
        .unwrap();
    assert_eq!(
        cache.get(&key).await.unwrap().resource_version,
        50,
        "a genuinely newer authoritative baseline must supersede the tombstone"
    );
}
