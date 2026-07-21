use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use futures::StreamExt as _;
use klights_cluster_core::{
    PositionedWatchEvent, Resource, ResourceVersionAssignment, WatchReplayPosition,
};
use klights_cluster_store::{
    AllocatorStateError, AllocatorStateFuture, DurableAllocatorRead, DurableAllocatorState,
    DurableWatchEvent, DurableWatchHistoryRead, DurableWatchTarget, ResourceGetRequest,
    ResourceListPage, ResourceListRead, ResourceListRequest, ResourceListSnapshot,
    ResourceReadFuture, WatchHistoryFuture, WatchHistoryPage, WatchHistoryRead,
    WatchHistoryRequest,
};
use klights_leader_api::{LeaderWatch, WatchEventType, WatchRequest};
use klights_watch::{
    PositionedWatchService, ProjectedWatchBaselineRead, ProjectedWatchBaselineRequest,
    ProjectedWatchPlan, WatchResourceProjection, WatchResourceScope, WatchScopeResolver,
    WatchSignalReceiver, WatchSignalSubscribe, WatchTopic,
};

fn position(resource_version: i64, event_id: i64) -> WatchReplayPosition {
    WatchReplayPosition {
        resource_version,
        event_id,
        resource_version_filter_through_event_id: 0,
    }
}

fn resource(resource_version: i64, name: &str) -> Resource {
    Resource::try_from_data(Arc::new(serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "namespace": "default",
            "name": name,
            "uid": format!("uid-{name}"),
            "resourceVersion": resource_version.to_string()
        }
    })))
    .expect("valid ConfigMap")
}

struct NoListReads;

impl klights_cluster_store::ClusterResourceRead for NoListReads {
    fn get_resource(
        &self,
        _request: ResourceGetRequest,
    ) -> ResourceReadFuture<'_, Option<Resource>> {
        Box::pin(async { panic!("unselected watch must not establish a baseline") })
    }

    fn list_resources(
        &self,
        _request: ResourceListRequest,
    ) -> ResourceReadFuture<'_, ResourceListRead> {
        Box::pin(async { panic!("unselected watch must not establish a baseline") })
    }
}

struct OrderedAllocator {
    subscribed: Arc<AtomicBool>,
    state: DurableAllocatorState,
}

impl DurableAllocatorRead for OrderedAllocator {
    fn read_allocator_state(&self) -> AllocatorStateFuture<'_, DurableAllocatorState> {
        assert!(
            self.subscribed.load(Ordering::Acquire),
            "the live subscriber must exist before the first awaited handoff read"
        );
        let state = self.state;
        Box::pin(async move { Ok(state) })
    }
}

struct RecordingHistory {
    requested: Arc<Mutex<Vec<WatchReplayPosition>>>,
    pages: Mutex<VecDeque<WatchHistoryRead>>,
}

impl DurableWatchHistoryRead for RecordingHistory {
    fn replay_watch_history(
        &self,
        request: WatchHistoryRequest,
    ) -> WatchHistoryFuture<'_, WatchHistoryRead> {
        self.requested
            .lock()
            .expect("request log lock")
            .push(request.position());
        let page = self
            .pages
            .lock()
            .expect("page lock")
            .pop_front()
            .unwrap_or_else(|| {
                WatchHistoryRead::Events(
                    WatchHistoryPage::try_new(Vec::new(), request.position()).expect("empty page"),
                )
            });
        Box::pin(async move { Ok(page) })
    }

    fn list_replay_floors(
        &self,
    ) -> WatchHistoryFuture<'_, Vec<klights_cluster_store::DurableReplayFloor>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

struct OrderedSignals {
    subscribed: Arc<AtomicBool>,
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

impl WatchSignalSubscribe for OrderedSignals {
    fn subscribe(&self, _topic: WatchTopic) -> WatchSignalReceiver {
        self.subscribed.store(true, Ordering::Release);
        WatchSignalReceiver::closed()
    }
}

#[tokio::test]
async fn omitted_cursor_subscribes_before_atomic_anchor_and_replays_the_gap() {
    let subscribed = Arc::new(AtomicBool::new(false));
    let requested = Arc::new(Mutex::new(Vec::new()));
    let anchor = position(7, 10);
    let delivered_position = position(8, 11);
    let history = RecordingHistory {
        requested: requested.clone(),
        pages: Mutex::new(VecDeque::from([
            WatchHistoryRead::Events(
                WatchHistoryPage::try_new(
                    vec![PositionedWatchEvent {
                        position: delivered_position,
                        event: DurableWatchEvent::new("ADDED", resource(8, "created-in-gap")),
                    }],
                    delivered_position,
                )
                .expect("positioned page"),
            ),
            WatchHistoryRead::Events(
                WatchHistoryPage::try_new(Vec::new(), delivered_position).expect("empty tail"),
            ),
        ])),
    };
    let service = PositionedWatchService::new(
        Arc::new(NoListReads),
        Arc::new(history),
        Arc::new(OrderedAllocator {
            subscribed: subscribed.clone(),
            state: DurableAllocatorState::try_new(
                ResourceVersionAssignment::CommittedApplyV1,
                anchor,
            )
            .expect("allocator state"),
        }),
        Arc::new(OrderedSignals {
            subscribed: subscribed.clone(),
        }),
        Arc::new(NamespacedScopes),
    );

    let mut stream = service
        .watch_resources(
            WatchRequest::try_new(
                "v1",
                "ConfigMap",
                Some("default".to_string()),
                None,
                None,
                None,
                None,
            )
            .expect("watch request"),
        )
        .await
        .expect("positioned watch");

    let event = stream
        .next()
        .await
        .expect("one gap event")
        .expect("valid gap event");
    assert_eq!(event.event_type(), WatchEventType::Added);
    assert_eq!(event.resource().name, "created-in-gap");
    assert_eq!(event.resume_position(), Some(delivered_position));
    assert_eq!(requested.lock().expect("request log").as_slice(), &[anchor]);
}

struct ForbiddenAllocator;

impl DurableAllocatorRead for ForbiddenAllocator {
    fn read_allocator_state(&self) -> AllocatorStateFuture<'_, DurableAllocatorState> {
        Box::pin(async {
            Err(AllocatorStateError::CorruptData {
                message: "exact LIST handoff must not read a replacement anchor".to_string(),
            })
        })
    }
}

#[tokio::test]
async fn exact_list_position_is_the_authoritative_watch_cursor() {
    let subscribed = Arc::new(AtomicBool::new(false));
    let requested = Arc::new(Mutex::new(Vec::new()));
    let list_position = position(41, 91);
    let history = RecordingHistory {
        requested: requested.clone(),
        pages: Mutex::new(VecDeque::from([WatchHistoryRead::Events(
            WatchHistoryPage::try_new(Vec::new(), list_position).expect("empty current page"),
        )])),
    };
    let service = PositionedWatchService::new(
        Arc::new(NoListReads),
        Arc::new(history),
        Arc::new(ForbiddenAllocator),
        Arc::new(OrderedSignals {
            subscribed: subscribed.clone(),
        }),
        Arc::new(NamespacedScopes),
    );

    let _stream = service
        .watch_resources(
            WatchRequest::try_new(
                "v1",
                "ConfigMap",
                Some("default".to_string()),
                None,
                None,
                Some(41),
                Some(list_position),
            )
            .expect("positioned watch request"),
        )
        .await
        .expect("exact handoff opens");

    assert!(subscribed.load(Ordering::Acquire));
    // Opening is lazy with respect to durable replay, so the first request is
    // made only once the pull-based stream is polled.
    assert!(requested.lock().expect("request log").is_empty());
}

fn custom_resource(api_version: &str, resource_version: i64, selected: bool) -> Resource {
    Resource::try_from_data(Arc::new(serde_json::json!({
        "apiVersion": api_version,
        "kind": "Widget",
        "metadata": {
            "namespace": "default",
            "name": "same-widget",
            "uid": "uid-same-widget",
            "resourceVersion": resource_version.to_string(),
            "labels": { "selected": if selected { "yes" } else { "no" } }
        }
    })))
    .expect("valid custom resource")
}

struct CountingSignals(Arc<AtomicUsize>);

impl WatchSignalSubscribe for CountingSignals {
    fn subscribe(&self, _topic: WatchTopic) -> WatchSignalReceiver {
        self.0.fetch_add(1, Ordering::AcqRel);
        WatchSignalReceiver::closed()
    }
}

struct EmptyProjectedBaseline {
    expected: WatchReplayPosition,
    subscriptions: Arc<AtomicUsize>,
}

impl ProjectedWatchBaselineRead for EmptyProjectedBaseline {
    fn read_baseline(
        &self,
        request: ProjectedWatchBaselineRequest,
    ) -> futures::future::BoxFuture<
        '_,
        Result<ResourceListRead, klights_leader_api::LeaderWatchError>,
    > {
        assert_eq!(self.subscriptions.load(Ordering::Acquire), 2);
        assert_eq!(request.targets().len(), 2);
        assert_eq!(request.position(), self.expected);
        let page = ResourceListPage::try_new(
            Vec::new(),
            ResourceListSnapshot::try_new(self.expected).expect("exact snapshot"),
            None,
            None,
        )
        .expect("empty baseline");
        Box::pin(async move { Ok(ResourceListRead::Historical(page)) })
    }
}

struct ToServedVersion;

impl WatchResourceProjection for ToServedVersion {
    fn project_resources(
        &self,
        resources: Vec<Resource>,
    ) -> futures::future::BoxFuture<'_, Result<Vec<Resource>, klights_leader_api::LeaderWatchError>>
    {
        Box::pin(async move {
            resources
                .into_iter()
                .map(|resource| {
                    let mut data = (*resource.data).clone();
                    data["apiVersion"] = serde_json::Value::String("example.io/v2".to_string());
                    Resource::try_from_data(Arc::new(data)).map_err(|error| {
                        klights_leader_api::LeaderWatchError::malformed_event(error.to_string())
                    })
                })
                .collect()
        })
    }
}

#[tokio::test]
async fn projected_multi_target_session_uses_one_membership_and_replay_state_machine() {
    let start = position(10, 20);
    let first = position(11, 21);
    let second = position(12, 22);
    let requested = Arc::new(Mutex::new(Vec::new()));
    let history = RecordingHistory {
        requested: requested.clone(),
        pages: Mutex::new(VecDeque::from([
            WatchHistoryRead::Events(
                WatchHistoryPage::try_new(
                    vec![
                        PositionedWatchEvent {
                            position: first,
                            event: DurableWatchEvent::new(
                                "ADDED",
                                custom_resource("example.io/v1", 11, false),
                            ),
                        },
                        PositionedWatchEvent {
                            position: second,
                            event: DurableWatchEvent::new(
                                "MODIFIED",
                                custom_resource("example.io/v1beta1", 12, true),
                            ),
                        },
                    ],
                    second,
                )
                .expect("two-version replay page"),
            ),
            WatchHistoryRead::Events(
                WatchHistoryPage::try_new(Vec::new(), second).expect("empty tail"),
            ),
        ])),
    };
    let subscriptions = Arc::new(AtomicUsize::new(0));
    let service = PositionedWatchService::new(
        Arc::new(NoListReads),
        Arc::new(history),
        Arc::new(ForbiddenAllocator),
        Arc::new(CountingSignals(subscriptions.clone())),
        Arc::new(NamespacedScopes),
    );
    let request = WatchRequest::try_new(
        "example.io/v2",
        "Widget",
        Some("default".to_string()),
        Some("selected=yes".to_string()),
        None,
        Some(10),
        Some(start),
    )
    .expect("served-version request");
    let plan = ProjectedWatchPlan::try_new(
        request,
        vec![
            DurableWatchTarget::namespaced_in_namespace("example.io/v1", "Widget", "default"),
            DurableWatchTarget::namespaced_in_namespace("example.io/v1beta1", "Widget", "default"),
        ],
        vec![
            WatchTopic::new("example.io/v1", "Widget"),
            WatchTopic::new("example.io/v1beta1", "Widget"),
        ],
        WatchResourceScope::Namespaced,
        Arc::new(EmptyProjectedBaseline {
            expected: start,
            subscriptions: subscriptions.clone(),
        }),
        Arc::new(ToServedVersion),
    )
    .expect("projected plan");

    let mut stream = service
        .watch_projected_resources(plan)
        .await
        .expect("projected watch");
    let event = stream
        .next()
        .await
        .expect("one membership transition")
        .expect("valid projected event");

    assert_eq!(event.event_type(), WatchEventType::Added);
    assert_eq!(event.resource().api_version, "example.io/v2");
    assert_eq!(event.resource().name, "same-widget");
    assert_eq!(event.resume_position(), Some(second));
    assert_eq!(subscriptions.load(Ordering::Acquire), 2);
    assert_eq!(requested.lock().expect("request log").as_slice(), &[start]);
}
