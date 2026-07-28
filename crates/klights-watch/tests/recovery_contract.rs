use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::{FutureExt as _, StreamExt as _, pin_mut, poll};
use klights_cluster_core::{PositionedWatchEvent, Resource, WatchReplayPosition};
use klights_cluster_store::{
    AllocatorStateFuture, DurableAllocatorRead, DurableAllocatorState, DurableWatchEvent,
    DurableWatchHistoryRead, MAX_WATCH_HISTORY_PAGE, ResourceGetRequest, ResourceListRead,
    ResourceListRequest, ResourceReadFuture, WatchHistoryFuture, WatchHistoryPage,
    WatchHistoryRead, WatchHistoryRequest,
};
use klights_leader_api::{LeaderWatch, WatchRequest};
use klights_watch::{
    PositionedWatchService, WatchAdvance, WatchResourceScope, WatchScopeResolver, WatchSignal,
    WatchSignalHub, WatchTopic,
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

fn resource_with_namespace(resource_version: i64, name: &str, namespace: Option<&str>) -> Resource {
    let mut value = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "uid": format!("uid-{name}"),
            "resourceVersion": resource_version.to_string()
        }
    });
    if let Some(namespace) = namespace {
        value["metadata"]["namespace"] = serde_json::json!(namespace);
    }
    Resource::try_from_data(Arc::new(value)).expect("valid scoped resource")
}

struct NoLists;

impl klights_cluster_store::ClusterResourceRead for NoLists {
    fn get_resource(
        &self,
        _request: ResourceGetRequest,
    ) -> ResourceReadFuture<'_, Option<Resource>> {
        Box::pin(async { panic!("no GET") })
    }

    fn list_resources(
        &self,
        _request: ResourceListRequest,
    ) -> ResourceReadFuture<'_, ResourceListRead> {
        Box::pin(async { panic!("unselected watch has no baseline") })
    }
}

struct FixedAllocator(DurableAllocatorState);

impl DurableAllocatorRead for FixedAllocator {
    fn read_allocator_state(&self) -> AllocatorStateFuture<'_, DurableAllocatorState> {
        let state = self.0;
        Box::pin(async move { Ok(state) })
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

struct FixedScope(WatchResourceScope);

impl WatchScopeResolver for FixedScope {
    fn resource_scope<'a>(
        &'a self,
        _api_version: &'a str,
        _kind: &'a str,
    ) -> futures::future::BoxFuture<
        'a,
        Result<WatchResourceScope, klights_leader_api::LeaderWatchError>,
    > {
        let scope = self.0;
        Box::pin(async move { Ok(scope) })
    }
}

struct QueueHistory {
    requests: Arc<Mutex<Vec<WatchReplayPosition>>>,
    pages: Mutex<VecDeque<WatchHistoryRead>>,
}

impl DurableWatchHistoryRead for QueueHistory {
    fn replay_watch_history(
        &self,
        request: WatchHistoryRequest,
    ) -> WatchHistoryFuture<'_, WatchHistoryRead> {
        assert_eq!(
            request.limit().get(),
            MAX_WATCH_HISTORY_PAGE,
            "positioned sessions must read bounded canonical history pages"
        );
        self.requests
            .lock()
            .expect("requests lock")
            .push(request.position());
        let page = self
            .pages
            .lock()
            .expect("pages lock")
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

#[tokio::test]
async fn short_nonempty_pages_preserve_same_rv_event_order_before_waiting_for_signal() {
    let anchor = position(10, 20);
    let first_position = position(11, 21);
    let second_position = position(11, 22);
    let requests = Arc::new(Mutex::new(Vec::new()));
    let history = QueueHistory {
        requests: requests.clone(),
        pages: Mutex::new(VecDeque::from([
            WatchHistoryRead::Events(
                WatchHistoryPage::try_new(
                    vec![PositionedWatchEvent {
                        position: first_position,
                        event: DurableWatchEvent::new("ADDED", resource(11, "first")),
                    }],
                    first_position,
                )
                .expect("first short page"),
            ),
            WatchHistoryRead::Events(
                WatchHistoryPage::try_new(
                    vec![PositionedWatchEvent {
                        position: second_position,
                        event: DurableWatchEvent::new("ADDED", resource(11, "second")),
                    }],
                    second_position,
                )
                .expect("second short page"),
            ),
        ])),
    };
    let service = PositionedWatchService::new(
        Arc::new(NoLists),
        Arc::new(history),
        Arc::new(FixedAllocator(
            DurableAllocatorState::try_new(anchor).expect("allocator"),
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
                None,
                None,
                Some(10),
                Some(anchor),
            )
            .expect("watch"),
        )
        .await
        .expect("watch opens");

    let first = stream
        .next()
        .await
        .expect("first event")
        .expect("first event is valid");
    let second = stream
        .next()
        .await
        .expect("second event")
        .expect("second event is valid");

    assert_eq!(first.resource().name, "first");
    assert_eq!(first.resume_position(), Some(first_position));
    assert_eq!(second.resource().name, "second");
    assert_eq!(second.resume_position(), Some(second_position));
    assert_eq!(
        requests.lock().expect("requests lock").as_slice(),
        &[anchor, first_position],
        "a short nonempty page must advance directly into the next bounded read"
    );
}

#[tokio::test]
async fn expired_history_returns_typed_relist_error() {
    let anchor = position(14, 30);
    let history = QueueHistory {
        requests: Arc::new(Mutex::new(Vec::new())),
        pages: Mutex::new(VecDeque::from([WatchHistoryRead::Expired])),
    };
    let service = PositionedWatchService::new(
        Arc::new(NoLists),
        Arc::new(history),
        Arc::new(FixedAllocator(
            DurableAllocatorState::try_new(anchor).expect("allocator"),
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
                None,
                None,
                Some(14),
                Some(anchor),
            )
            .expect("watch"),
        )
        .await
        .expect("watch opens");

    assert!(matches!(
        stream.next().await.expect("terminal replay error"),
        Err(klights_leader_api::LeaderWatchError::ReplayExpired {
            accepted_resource_version: 14
        })
    ));
}

#[tokio::test]
async fn lagged_bounded_subscriber_recovers_exclusively_from_durable_position() {
    let anchor = position(5, 10);
    let recovered = position(6, 11);
    let requests = Arc::new(Mutex::new(Vec::new()));
    let history = QueueHistory {
        requests: requests.clone(),
        pages: Mutex::new(VecDeque::from([
            WatchHistoryRead::Events(
                WatchHistoryPage::try_new(Vec::new(), anchor).expect("initially caught up"),
            ),
            WatchHistoryRead::Events(
                WatchHistoryPage::try_new(
                    vec![PositionedWatchEvent {
                        position: recovered,
                        event: DurableWatchEvent::new("ADDED", resource(6, "recovered")),
                    }],
                    recovered,
                )
                .expect("recovery page"),
            ),
            WatchHistoryRead::Events(
                WatchHistoryPage::try_new(Vec::new(), recovered).expect("recovery tail"),
            ),
        ])),
    };
    let hub = Arc::new(WatchSignalHub::new(1));
    let service = PositionedWatchService::new(
        Arc::new(NoLists),
        Arc::new(history),
        Arc::new(FixedAllocator(
            DurableAllocatorState::try_new(anchor).expect("allocator"),
        )),
        hub.clone(),
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
                Some(5),
                Some(anchor),
            )
            .expect("watch"),
        )
        .await
        .expect("watch opens");
    let topic = WatchTopic::new("v1", "ConfigMap");
    for high_rv in [6, 7] {
        hub.publish(WatchSignal {
            topic: topic.clone(),
            advances: vec![WatchAdvance {
                namespace: Some("default".to_string()),
                low_rv: high_rv,
                high_rv,
            }],
        });
    }

    let event = stream
        .next()
        .await
        .expect("recovered event")
        .expect("valid recovered event");
    assert_eq!(event.resource().name, "recovered");
    assert_eq!(event.resume_position(), Some(recovered));
    assert_eq!(
        requests.lock().expect("requests lock").as_slice(),
        &[anchor, anchor]
    );
}

#[tokio::test]
async fn scalar_resource_version_uses_the_atomic_event_high_water_filter() {
    let anchor = position(12, 20);
    let requests = Arc::new(Mutex::new(Vec::new()));
    let history = QueueHistory {
        requests: requests.clone(),
        pages: Mutex::new(VecDeque::from([WatchHistoryRead::Events(
            WatchHistoryPage::try_new(Vec::new(), anchor).expect("caught up"),
        )])),
    };
    let service = PositionedWatchService::new(
        Arc::new(NoLists),
        Arc::new(history),
        Arc::new(FixedAllocator(
            DurableAllocatorState::try_new(anchor).expect("allocator"),
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
                None,
                None,
                Some(10),
                None,
            )
            .expect("scalar watch"),
        )
        .await
        .expect("watch opens");
    let next = stream.next();
    pin_mut!(next);
    assert!(matches!(poll!(next.as_mut()), Poll::Pending));
    assert_eq!(
        requests.lock().expect("requests lock").as_slice(),
        &[WatchReplayPosition::from_resource_version_through_event_id(
            10, 20
        )]
    );
}

struct PendingReplay {
    polled: Arc<AtomicBool>,
    dropped: Arc<AtomicBool>,
}

impl Future for PendingReplay {
    type Output = Result<WatchHistoryRead, klights_cluster_store::WatchHistoryError>;

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        self.polled.store(true, Ordering::Release);
        Poll::Pending
    }
}

impl Drop for PendingReplay {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}

struct PendingHistory {
    polled: Arc<AtomicBool>,
    dropped: Arc<AtomicBool>,
}

impl DurableWatchHistoryRead for PendingHistory {
    fn replay_watch_history(
        &self,
        _request: WatchHistoryRequest,
    ) -> WatchHistoryFuture<'_, WatchHistoryRead> {
        Box::pin(PendingReplay {
            polled: self.polled.clone(),
            dropped: self.dropped.clone(),
        })
    }

    fn list_replay_floors(
        &self,
    ) -> WatchHistoryFuture<'_, Vec<klights_cluster_store::DurableReplayFloor>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

#[tokio::test]
async fn dropping_stream_cancels_in_flight_replay_without_a_background_task() {
    let anchor = position(5, 10);
    let polled = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicBool::new(false));
    let service = PositionedWatchService::new(
        Arc::new(NoLists),
        Arc::new(PendingHistory {
            polled: polled.clone(),
            dropped: dropped.clone(),
        }),
        Arc::new(FixedAllocator(
            DurableAllocatorState::try_new(anchor).expect("allocator"),
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
                None,
                None,
                Some(5),
                Some(anchor),
            )
            .expect("watch"),
        )
        .await
        .expect("watch opens");
    {
        let next = stream.next();
        pin_mut!(next);
        assert!(matches!(poll!(next.as_mut()), Poll::Pending));
        assert!(polled.load(Ordering::Acquire));
    }
    assert!(!dropped.load(Ordering::Acquire));
    drop(stream);
    assert!(dropped.load(Ordering::Acquire));
}

#[tokio::test]
async fn regressing_history_page_is_rejected_before_waiting_for_a_live_signal() {
    let anchor = position(8, 20);
    let history = QueueHistory {
        requests: Arc::new(Mutex::new(Vec::new())),
        pages: Mutex::new(VecDeque::from([WatchHistoryRead::Events(
            WatchHistoryPage::try_new(Vec::new(), position(7, 19))
                .expect("structurally valid but regressing page"),
        )])),
    };
    let service = PositionedWatchService::new(
        Arc::new(NoLists),
        Arc::new(history),
        Arc::new(FixedAllocator(
            DurableAllocatorState::try_new(anchor).expect("allocator"),
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
                None,
                None,
                Some(8),
                Some(anchor),
            )
            .expect("watch"),
        )
        .await
        .expect("watch opens");

    let error = stream
        .next()
        .now_or_never()
        .expect("regression must fail synchronously")
        .expect("terminal error")
        .expect_err("regression must be rejected");
    assert!(matches!(
        error,
        klights_leader_api::LeaderWatchError::MalformedEvent { .. }
    ));
}

#[tokio::test]
async fn malformed_equal_cursor_and_non_exact_event_pages_are_rejected() {
    let anchor = position(8, 20);
    let malformed_pages = [
        WatchHistoryPage::try_new(Vec::new(), position(9, 20))
            .expect("standalone equal-ID cursor is structurally valid"),
        WatchHistoryPage::try_new(
            vec![PositionedWatchEvent {
                position: WatchReplayPosition {
                    resource_version: 9,
                    event_id: 21,
                    resource_version_filter_through_event_id: 30,
                },
                event: DurableWatchEvent::new("ADDED", resource(9, "non-exact")),
            }],
            WatchReplayPosition {
                resource_version: 9,
                event_id: 21,
                resource_version_filter_through_event_id: 30,
            },
        )
        .expect("standalone composite event page is structurally valid"),
    ];
    for page in malformed_pages {
        let history = QueueHistory {
            requests: Arc::new(Mutex::new(Vec::new())),
            pages: Mutex::new(VecDeque::from([WatchHistoryRead::Events(page)])),
        };
        let service = PositionedWatchService::new(
            Arc::new(NoLists),
            Arc::new(history),
            Arc::new(FixedAllocator(
                DurableAllocatorState::try_new(anchor).expect("allocator"),
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
                    None,
                    None,
                    Some(8),
                    Some(anchor),
                )
                .expect("watch"),
            )
            .await
            .expect("watch opens");
        assert!(matches!(
            stream.next().await.expect("terminal malformed page"),
            Err(klights_leader_api::LeaderWatchError::MalformedEvent { .. })
        ));
    }
}

#[tokio::test]
async fn replay_rejects_cluster_and_all_namespaces_scope_mismatches_before_yield() {
    for (scope, event_namespace) in [
        (WatchResourceScope::Cluster, Some("default")),
        (WatchResourceScope::Namespaced, None),
    ] {
        let anchor = position(8, 20);
        let delivered = position(9, 21);
        let history = QueueHistory {
            requests: Arc::new(Mutex::new(Vec::new())),
            pages: Mutex::new(VecDeque::from([WatchHistoryRead::Events(
                WatchHistoryPage::try_new(
                    vec![PositionedWatchEvent {
                        position: delivered,
                        event: DurableWatchEvent::new(
                            "ADDED",
                            resource_with_namespace(9, "wrong-scope", event_namespace),
                        ),
                    }],
                    delivered,
                )
                .expect("structurally valid mismatched page"),
            )])),
        };
        let service = PositionedWatchService::new(
            Arc::new(NoLists),
            Arc::new(history),
            Arc::new(FixedAllocator(
                DurableAllocatorState::try_new(anchor).expect("allocator"),
            )),
            Arc::new(WatchSignalHub::new(1)),
            Arc::new(FixedScope(scope)),
        );
        let mut stream = service
            .watch_resources(
                WatchRequest::try_new("v1", "ConfigMap", None, None, None, Some(8), Some(anchor))
                    .expect("watch"),
            )
            .await
            .expect("watch opens");
        let error = stream
            .next()
            .await
            .expect("terminal mismatch")
            .expect_err("scope mismatch must not yield a resource");
        assert!(matches!(
            error,
            klights_leader_api::LeaderWatchError::MismatchedEvent { .. }
        ));
    }
}
