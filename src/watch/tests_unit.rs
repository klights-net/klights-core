use super::*;

fn test_task_supervisor() -> crate::task_supervisor::TaskSupervisor {
    crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    )
}

#[test]
fn window_policy_default_watch_delivery_is_sliding_three() {
    assert_eq!(
        crate::watch::WindowPolicy::default_watch_delivery()
            .limit()
            .get(),
        3
    );
}

#[test]
fn window_policy_stop_and_wait_reports_one() {
    assert_eq!(crate::watch::WindowPolicy::StopAndWait.limit().get(), 1);
}

#[test]
fn test_watch_event_added() {
    let obj = serde_json::json!({"kind": "Pod", "metadata": {"name": "test"}});
    let event = WatchEvent::added(obj.clone());
    assert_eq!(event.event_type, EventType::Added);
    assert_eq!(*event.object, obj);
}

#[test]
fn test_watch_event_bookmark() {
    let event = WatchEvent::bookmark(123);
    assert_eq!(event.event_type, EventType::Bookmark);
    assert_eq!(
        event
            .object
            .get("metadata")
            .unwrap()
            .get("resourceVersion")
            .unwrap(),
        "123"
    );
}

#[test]
fn test_matches_filter_kind() {
    let pod = serde_json::json!({"kind": "Pod", "metadata": {"name": "test"}});
    let event = WatchEvent::added(pod);

    assert!(event.matches_filter("Pod", None, None));
    assert!(!event.matches_filter("Service", None, None));
}

#[test]
fn test_matches_filter_namespace() {
    let pod = serde_json::json!({
        "kind": "Pod",
        "metadata": {"name": "test", "namespace": "default"}
    });
    let event = WatchEvent::added(pod);

    assert!(event.matches_filter("Pod", Some("default"), None));
    assert!(!event.matches_filter("Pod", Some("kube-system"), None));
}

#[test]
fn test_bookmark_always_matches() {
    let event = WatchEvent::bookmark(123);
    assert!(event.matches_filter("Pod", Some("default"), None));
    assert!(event.matches_filter("Service", None, None));
}

#[test]
fn test_watch_event_modified() {
    let obj = serde_json::json!({"kind": "Pod", "metadata": {"name": "test"}});
    let event = WatchEvent::modified(obj.clone());
    assert_eq!(event.event_type, EventType::Modified);
    assert_eq!(*event.object, obj);
}

#[test]
fn test_watch_event_deleted() {
    let obj = serde_json::json!({"kind": "Pod", "metadata": {"name": "test"}});
    let event = WatchEvent::deleted(obj.clone());
    assert_eq!(event.event_type, EventType::Deleted);
    assert_eq!(*event.object, obj);
}

#[test]
fn test_bookmark_typed_includes_api_version_and_kind() {
    let event = WatchEvent::bookmark_typed(456, "v1", "Pod");
    assert_eq!(event.event_type, EventType::Bookmark);
    assert_eq!(event.object["apiVersion"], "v1");
    assert_eq!(event.object["kind"], "Pod");
    assert_eq!(event.object["metadata"]["resourceVersion"], "456");
}

#[test]
fn test_matches_filter_missing_kind_returns_false() {
    let obj = serde_json::json!({"metadata": {"name": "test"}});
    let event = WatchEvent::added(obj);
    assert!(!event.matches_filter("Pod", None, None));
}

#[test]
fn test_matches_filter_namespaced_event_without_namespace_returns_false() {
    let obj = serde_json::json!({"kind": "Pod", "metadata": {"name": "test"}});
    let event = WatchEvent::added(obj);
    // Filtering for a specific namespace but event has no namespace
    assert!(!event.matches_filter("Pod", Some("default"), None));
}

#[tokio::test]
async fn test_broadcast_lag_recovers_remaining_messages() {
    use tokio::sync::broadcast;

    // Create small channel to force lagging
    let (tx, mut rx) = broadcast::channel::<i32>(2);

    // Send 3 messages (capacity is 2, so receiver will lag)
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    tx.send(3).unwrap(); // This overwrites message 1, causing lag

    // First recv should return Lagged error
    match rx.recv().await {
        Err(broadcast::error::RecvError::Lagged(n)) => {
            assert_eq!(n, 1, "Should have lagged by 1 message");
            // After lag, recv returns the oldest retained message (2),
            // then the next recv returns 3
            let msg2 = rx.recv().await.unwrap();
            assert_eq!(msg2, 2, "Should receive oldest retained message after lag");
            let msg3 = rx.recv().await.unwrap();
            assert_eq!(msg3, 3, "Should receive next retained message");
        }
        Ok(val) => panic!("Expected Lagged error, got Ok({})", val),
        Err(e) => panic!("Expected Lagged error, got {:?}", e),
    }
}

struct FixedReplaySource {
    events: Vec<WatchEvent>,
}

impl FixedReplaySource {
    fn new(events: Vec<WatchEvent>) -> Self {
        Self { events }
    }
}

#[async_trait::async_trait]
impl WatchReplaySource for FixedReplaySource {
    async fn replay_since(&self, since_rv: i64) -> anyhow::Result<Vec<WatchEvent>> {
        Ok(self
            .events
            .iter()
            .filter(|event| event.resource_version().unwrap_or_default() > since_rv)
            .cloned()
            .collect())
    }
}

#[derive(Clone)]
struct SignalCursorReplaySource {
    events: std::sync::Arc<Vec<WatchEvent>>,
    expired: bool,
    calls: std::sync::Arc<std::sync::Mutex<Vec<(i64, usize)>>>,
}

impl SignalCursorReplaySource {
    fn events(events: Vec<WatchEvent>) -> Self {
        Self {
            events: std::sync::Arc::new(events),
            expired: false,
            calls: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    fn expired() -> Self {
        Self {
            events: std::sync::Arc::new(Vec::new()),
            expired: true,
            calls: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    fn calls(&self) -> Vec<(i64, usize)> {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

#[async_trait::async_trait]
impl WatchReplaySource for SignalCursorReplaySource {
    async fn replay_since(&self, since_rv: i64) -> anyhow::Result<Vec<WatchEvent>> {
        Ok(self
            .events
            .iter()
            .filter(|event| event.resource_version().unwrap_or_default() > since_rv)
            .cloned()
            .collect())
    }

    async fn replay_since_checked(
        &self,
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> anyhow::Result<crate::datastore::WatchReplayRead<WatchEvent>> {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((since_rv, limit.get()));
        if self.expired {
            return Ok(crate::datastore::WatchReplayRead::Expired);
        }
        Ok(crate::datastore::WatchReplayRead::Events(
            self.events
                .iter()
                .filter(|event| event.resource_version().unwrap_or_default() > since_rv)
                .take(limit.get())
                .cloned()
                .collect(),
        ))
    }
}

fn signal_cursor_topic() -> WatchTopic {
    WatchTopic::new("v1", "Pod")
}

fn signal_cursor_pod(namespace: &str, name: &str, rv: i64) -> WatchEvent {
    WatchEvent::modified(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": namespace,
            "name": name,
            "resourceVersion": rv.to_string(),
        }
    }))
}

fn signal_cursor_signal(namespace: Option<&str>, high_rv: i64) -> WatchSignal {
    WatchSignal {
        topic: signal_cursor_topic(),
        advances: vec![WatchAdvance {
            namespace: namespace.map(str::to_string),
            high_rv,
        }],
    }
}

#[tokio::test]
async fn signal_cursor_default_window_three_delivers_single_event_without_waiting_for_three() {
    let (tx, rx) = broadcast::channel(4);
    let source = SignalCursorReplaySource::events(vec![signal_cursor_pod("default", "pod-11", 11)]);
    let mut cursor = SignalWatchCursor::new(
        rx,
        source.clone(),
        signal_cursor_topic(),
        WatchDeliveryScope::Namespaced("default".to_string()),
        10,
        WindowPolicy::default_watch_delivery(),
    );

    tx.send(signal_cursor_signal(Some("default"), 11)).unwrap();

    let event = cursor.next_event().await.unwrap();
    assert_eq!(event.resource_version(), Some(11));
    assert_eq!(cursor.accepted_rv(), 10);
    cursor.accept_event(11);
    assert_eq!(cursor.accepted_rv(), 11);
    assert_eq!(source.calls(), vec![(10, 3)]);
}

#[tokio::test]
async fn signal_cursor_default_window_three_delivers_two_events_without_waiting_for_three() {
    let (tx, rx) = broadcast::channel(4);
    let source = SignalCursorReplaySource::events(vec![
        signal_cursor_pod("default", "pod-11", 11),
        signal_cursor_pod("default", "pod-12", 12),
    ]);
    let mut cursor = SignalWatchCursor::new(
        rx,
        source.clone(),
        signal_cursor_topic(),
        WatchDeliveryScope::Namespaced("default".to_string()),
        10,
        WindowPolicy::default_watch_delivery(),
    );

    tx.send(signal_cursor_signal(Some("default"), 12)).unwrap();

    let first = cursor.next_event().await.unwrap();
    assert_eq!(first.resource_version(), Some(11));
    assert_eq!(cursor.accepted_rv(), 10);
    cursor.accept_event(11);
    let second = cursor.next_event().await.unwrap();
    assert_eq!(second.resource_version(), Some(12));
    assert_eq!(cursor.accepted_rv(), 11);
    cursor.accept_event(12);
    assert_eq!(cursor.accepted_rv(), 12);
    assert_eq!(source.calls(), vec![(10, 3)]);
}

#[tokio::test]
async fn signal_cursor_returned_event_does_not_advance_until_explicit_accept() {
    let (tx, rx) = broadcast::channel(4);
    let source = SignalCursorReplaySource::events(vec![signal_cursor_pod("default", "pod-11", 11)]);
    let mut cursor = SignalWatchCursor::new(
        rx,
        source.clone(),
        signal_cursor_topic(),
        WatchDeliveryScope::Namespaced("default".to_string()),
        10,
        WindowPolicy::default_watch_delivery(),
    );

    tx.send(signal_cursor_signal(Some("default"), 11)).unwrap();

    let event = cursor.next_event().await.unwrap();
    assert_eq!(event.resource_version(), Some(11));
    assert_eq!(
        cursor.accepted_rv(),
        10,
        "returned events must not advance accepted_rv before stream acceptance"
    );

    cursor.accept_event(11);
    assert_eq!(cursor.accepted_rv(), 11);
    assert_eq!(source.calls(), vec![(10, 3)]);
}

#[tokio::test]
async fn signal_cursor_skipped_out_of_scope_event_advances_replay_safe_frontier() {
    let (tx, rx) = broadcast::channel(4);
    let source = SignalCursorReplaySource::events(vec![
        signal_cursor_pod("other", "pod-11", 11),
        signal_cursor_pod("default", "pod-12", 12),
    ]);
    let mut cursor = SignalWatchCursor::new(
        rx,
        source.clone(),
        signal_cursor_topic(),
        WatchDeliveryScope::Namespaced("default".to_string()),
        10,
        WindowPolicy::default_watch_delivery(),
    );

    tx.send(signal_cursor_signal(Some("default"), 12)).unwrap();

    let event = cursor.next_event().await.unwrap();
    assert_eq!(event.resource_version(), Some(12));
    assert_eq!(
        cursor.accepted_rv(),
        11,
        "out-of-scope replay entries can be processed without caller acceptance"
    );

    cursor.accept_event(12);
    assert_eq!(cursor.accepted_rv(), 12);
    assert_eq!(source.calls(), vec![(10, 3)]);
}

#[tokio::test]
async fn signal_cursor_lost_signal_replays_all_missing_events_on_next_signal() {
    let (tx, rx) = broadcast::channel(4);
    let source = SignalCursorReplaySource::events(vec![
        signal_cursor_pod("default", "pod-11", 11),
        signal_cursor_pod("default", "pod-12", 12),
        signal_cursor_pod("default", "pod-13", 13),
    ]);
    let mut cursor = SignalWatchCursor::new(
        rx,
        source.clone(),
        signal_cursor_topic(),
        WatchDeliveryScope::Namespaced("default".to_string()),
        10,
        WindowPolicy::default_watch_delivery(),
    );

    tx.send(signal_cursor_signal(Some("default"), 13)).unwrap();

    let delivered = vec![
        cursor.next_event().await.unwrap().resource_version(),
        cursor.next_event().await.unwrap().resource_version(),
        cursor.next_event().await.unwrap().resource_version(),
    ];
    assert_eq!(delivered, vec![Some(11), Some(12), Some(13)]);
    assert_eq!(source.calls(), vec![(10, 3)]);
}

#[tokio::test]
async fn signal_cursor_lagged_signal_receiver_uses_replay_instead_of_failing() {
    let (tx, rx) = broadcast::channel(1);
    let source = SignalCursorReplaySource::events(vec![
        signal_cursor_pod("default", "pod-11", 11),
        signal_cursor_pod("default", "pod-12", 12),
        signal_cursor_pod("default", "pod-13", 13),
    ]);
    let mut cursor = SignalWatchCursor::new(
        rx,
        source.clone(),
        signal_cursor_topic(),
        WatchDeliveryScope::Namespaced("default".to_string()),
        10,
        WindowPolicy::default_watch_delivery(),
    );

    tx.send(signal_cursor_signal(Some("default"), 11)).unwrap();
    tx.send(signal_cursor_signal(Some("default"), 12)).unwrap();
    tx.send(signal_cursor_signal(Some("default"), 13)).unwrap();

    let event = cursor.next_event().await.unwrap();
    assert_eq!(event.resource_version(), Some(11));
    assert_eq!(source.calls(), vec![(10, 3)]);
}

#[tokio::test]
async fn signal_cursor_expired_replay_returns_expired() {
    let (tx, rx) = broadcast::channel(4);
    let source = SignalCursorReplaySource::expired();
    let mut cursor = SignalWatchCursor::new(
        rx,
        source,
        signal_cursor_topic(),
        WatchDeliveryScope::Namespaced("default".to_string()),
        10,
        WindowPolicy::default_watch_delivery(),
    );

    tx.send(signal_cursor_signal(Some("default"), 11)).unwrap();

    match cursor.next_event().await {
        Err(WatchCursorError::Expired) => {}
        other => panic!("expected Expired, got {other:?}"),
    }
}

#[tokio::test]
async fn signal_cursor_namespaced_all_does_not_match_cluster_scoped_signal() {
    assert!(!WatchDeliveryScope::NamespacedAll.matches_namespace(None));
    assert!(WatchDeliveryScope::NamespacedAll.matches_namespace(Some("default")));
}

#[tokio::test]
async fn signal_cursor_cluster_does_not_match_namespaced_signal() {
    assert!(WatchDeliveryScope::Cluster.matches_namespace(None));
    assert!(!WatchDeliveryScope::Cluster.matches_namespace(Some("default")));
}

#[tokio::test]
async fn signal_cursor_empty_namespace_is_cluster_scoped() {
    assert!(WatchDeliveryScope::Cluster.matches_namespace(Some("")));
    assert!(!WatchDeliveryScope::NamespacedAll.matches_namespace(Some("")));
    assert!(!WatchDeliveryScope::Namespaced("default".to_string()).matches_namespace(Some("")));
}

#[tokio::test]
async fn signal_cursor_all_matches_cluster_and_namespaced_signals() {
    assert!(WatchDeliveryScope::All.matches_namespace(None));
    assert!(WatchDeliveryScope::All.matches_namespace(Some("default")));
}

#[tokio::test]
async fn watch_replay_source_checked_replay_respects_limit() {
    let source = FixedReplaySource::new(vec![
        WatchEvent::added(serde_json::json!({
            "kind": "Pod",
            "metadata": {"name": "p1", "resourceVersion": "11"}
        })),
        WatchEvent::added(serde_json::json!({
            "kind": "Pod",
            "metadata": {"name": "p2", "resourceVersion": "12"}
        })),
        WatchEvent::added(serde_json::json!({
            "kind": "Pod",
            "metadata": {"name": "p3", "resourceVersion": "13"}
        })),
    ]);

    let replay = source
        .replay_since_checked(10, std::num::NonZeroUsize::new(2).unwrap())
        .await
        .unwrap();

    match replay {
        crate::datastore::WatchReplayRead::Events(events) => {
            assert_eq!(events.len(), 2);
            assert_eq!(events[0].resource_version(), Some(11));
            assert_eq!(events[1].resource_version(), Some(12));
        }
        crate::datastore::WatchReplayRead::Expired => panic!("fixed source should not expire"),
    }
}

/// Replay source that models a trimmed durable window: events older than
/// `earliest` have been garbage-collected, so a resume from before `earliest`
/// cannot be satisfied.
struct WindowedReplaySource {
    earliest: Option<i64>,
}

#[async_trait::async_trait]
impl WatchReplaySource for WindowedReplaySource {
    async fn replay_since(&self, _since_rv: i64) -> anyhow::Result<Vec<WatchEvent>> {
        Ok(Vec::new())
    }

    async fn earliest_retained_rv(&self) -> anyhow::Result<Option<i64>> {
        Ok(self.earliest)
    }
}

#[tokio::test]
async fn test_prime_replay_expires_when_resume_predates_window() {
    // Oldest retained event is RV=50; client resumes from RV=10 → the
    // 11..=49 gap is gone, so the watch must report Expired (→ 410 Gone).
    let (_tx, rx) = broadcast::channel(4);
    let replay_source = WindowedReplaySource { earliest: Some(50) };
    let mut cursor = WatchCursor::new(rx, replay_source, 10);
    match cursor.prime_replay_or_expired().await {
        Err(WatchCursorError::Expired) => {}
        other => panic!("expected Expired for resume before window, got {other:?}"),
    }
}

#[tokio::test]
async fn test_prime_replay_ok_when_resume_within_window() {
    // Oldest retained event is RV=50; resuming from RV=49 keeps continuity
    // (the next needed event, RV=50, is still retained) → no expiry.
    let (_tx, rx) = broadcast::channel(4);
    let replay_source = WindowedReplaySource { earliest: Some(50) };
    let mut cursor = WatchCursor::new(rx, replay_source, 49);
    cursor
        .prime_replay_or_expired()
        .await
        .expect("resume at window edge must not expire");
}

#[tokio::test]
async fn test_prime_replay_ok_when_window_empty() {
    // Empty window (no retained events) must never expire a watch.
    let (_tx, rx) = broadcast::channel(4);
    let replay_source = WindowedReplaySource { earliest: None };
    let mut cursor = WatchCursor::new(rx, replay_source, 10);
    cursor
        .prime_replay_or_expired()
        .await
        .expect("empty window must not expire");
}

#[tokio::test]
async fn test_watch_cursor_recovers_missed_events_and_skips_live_duplicates() {
    let (tx, rx) = broadcast::channel(2);
    let replay_source = FixedReplaySource::new(vec![
        WatchEvent::added(serde_json::json!({
            "kind": "Pod",
            "metadata": {"name": "pod-a", "resourceVersion": "2"}
        })),
        WatchEvent::modified(serde_json::json!({
            "kind": "Pod",
            "metadata": {"name": "pod-a", "resourceVersion": "3"}
        })),
    ]);
    let mut cursor = WatchCursor::new(rx, replay_source, 1);

    tx.send(WatchEvent::added(serde_json::json!({
        "kind": "Pod",
        "metadata": {"name": "stale-a", "resourceVersion": "2"}
    })))
    .unwrap();
    tx.send(WatchEvent::modified(serde_json::json!({
        "kind": "Pod",
        "metadata": {"name": "stale-b", "resourceVersion": "3"}
    })))
    .unwrap();
    tx.send(WatchEvent::deleted(serde_json::json!({
        "kind": "Pod",
        "metadata": {"name": "pod-b", "resourceVersion": "4"}
    })))
    .unwrap();

    let supervisor = test_task_supervisor();
    let first = cursor.next_event(&supervisor).await.unwrap();
    assert_eq!(first.resource_version(), Some(2));
    let second = cursor.next_event(&supervisor).await.unwrap();
    assert_eq!(second.resource_version(), Some(3));
    let third = cursor.next_event(&supervisor).await.unwrap();
    assert_eq!(third.resource_version(), Some(4));
    assert_eq!(third.event_type, EventType::Deleted);
}

#[tokio::test]
async fn test_watch_cursor_replay_keeps_lower_late_rv_after_observed_higher_rv() {
    let (tx, rx) = broadcast::channel(2);
    let late_matching = WatchEvent::added(serde_json::json!({
        "kind": "ConfigMap",
        "metadata": {"name": "late-match", "resourceVersion": "10"}
    }));
    let replay_source = FixedReplaySource::new(vec![late_matching.clone()]);
    let mut cursor = WatchCursor::new(rx, replay_source, 0);

    tx.send(WatchEvent::added(serde_json::json!({
        "kind": "Pod",
        "metadata": {"name": "higher-first", "resourceVersion": "11"}
    })))
    .unwrap();

    let supervisor = test_task_supervisor();
    let first = cursor.next_event(&supervisor).await.unwrap();
    assert_eq!(first.resource_version(), Some(11));

    tx.send(late_matching).unwrap();
    tx.send(WatchEvent::added(serde_json::json!({
        "kind": "Pod",
        "metadata": {"name": "later-a", "resourceVersion": "12"}
    })))
    .unwrap();
    tx.send(WatchEvent::added(serde_json::json!({
        "kind": "Pod",
        "metadata": {"name": "later-b", "resourceVersion": "13"}
    })))
    .unwrap();

    let recovered = cursor.next_event(&supervisor).await.unwrap();
    assert_eq!(
        recovered.resource_version(),
        Some(10),
        "replay must include a lower-RV event that was published after a higher-RV event"
    );
    assert_eq!(recovered.object["metadata"]["name"], "late-match");
}

#[tokio::test]
async fn watch_cursor_ordered_replay_drains_gap_before_live_jump() {
    let (tx, rx) = broadcast::channel(8);
    let replay_source = FixedReplaySource::new(vec![
        WatchEvent::modified(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "frontend",
                "namespace": "default",
                "resourceVersion": "11"
            },
            "status": {"phase": "Pending"}
        })),
        WatchEvent::modified(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "frontend",
                "namespace": "default",
                "resourceVersion": "12"
            },
            "status": {"phase": "Running"}
        })),
    ]);
    let mut cursor = WatchCursor::new(rx, replay_source, 10).with_ordered_replay();

    tx.send(WatchEvent::modified(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "frontend",
            "namespace": "default",
            "resourceVersion": "12"
        },
        "status": {"phase": "Running"}
    })))
    .unwrap();

    let supervisor = test_task_supervisor();
    let first = cursor.next_event(&supervisor).await.unwrap();
    assert_eq!(first.resource_version(), Some(11));
    assert_eq!(first.object["status"]["phase"], "Pending");

    let second = cursor.next_event(&supervisor).await.unwrap();
    assert_eq!(second.resource_version(), Some(12));
    assert_eq!(second.object["status"]["phase"], "Running");

    tx.send(WatchEvent::modified(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "frontend",
            "namespace": "default",
            "resourceVersion": "11"
        },
        "status": {"phase": "Pending"}
    })))
    .unwrap();
    tx.send(WatchEvent::modified(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "frontend",
            "namespace": "default",
            "resourceVersion": "13"
        },
        "status": {"phase": "Running"}
    })))
    .unwrap();

    let third = tokio::time::timeout(Duration::from_secs(1), cursor.next_event(&supervisor))
        .await
        .expect("ordered cursor should skip the late stale live event")
        .unwrap();
    assert_eq!(third.resource_version(), Some(13));
    assert_eq!(third.object["status"]["phase"], "Running");
}

#[tokio::test]
async fn watch_cursor_ordered_replay_keeps_live_jump_when_replay_omits_it() {
    let (tx, rx) = broadcast::channel(8);
    let replay_source = FixedReplaySource::new(vec![WatchEvent::modified(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "frontend",
            "namespace": "default",
            "resourceVersion": "11"
        },
        "status": {"phase": "Pending"}
    }))]);
    let mut cursor = WatchCursor::new(rx, replay_source, 10).with_ordered_replay();

    tx.send(WatchEvent::modified(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "frontend",
            "namespace": "default",
            "resourceVersion": "12"
        },
        "status": {"phase": "Running"}
    })))
    .unwrap();

    let supervisor = test_task_supervisor();
    let first = cursor.next_event(&supervisor).await.unwrap();
    assert_eq!(first.resource_version(), Some(11));
    assert_eq!(first.object["status"]["phase"], "Pending");

    let second = cursor.next_event(&supervisor).await.unwrap();
    assert_eq!(second.resource_version(), Some(12));
    assert_eq!(second.object["status"]["phase"], "Running");
}

/// Durable window that is momentarily unreadable under transport stress
/// (packet loss / raft-apply read contention on the watch_events table): the
/// retained-edge read fails and the gap replay returns nothing. Models the
/// lossy-netns path where `replay_gap_detected` hits its `Err(_) => false`
/// fail-open arm, so a real gap is treated as recoverable when it is not.
struct TransportFlakyReplaySource;

#[async_trait::async_trait]
impl WatchReplaySource for TransportFlakyReplaySource {
    async fn replay_since(&self, _since_rv: i64) -> anyhow::Result<Vec<WatchEvent>> {
        Ok(Vec::new())
    }

    async fn earliest_retained_rv(&self) -> anyhow::Result<Option<i64>> {
        Err(anyhow::Error::msg("window read failed (transport stress)"))
    }
}

#[tokio::test]
async fn watch_cursor_ordered_replay_expires_when_gap_unfillable_under_transport_stress() {
    // Scoped Pod watch resumed at floor_rv=10. Under packet loss a live
    // readiness transition jumps to RV=12 while the intervening in-scope event
    // at RV=11 must be replayed. The durable window read fails transiently
    // (`earliest_retained_rv -> Err`, the fail-open arm of
    // `replay_gap_detected`) and `replay_since` returns nothing. The cursor must
    // NOT silently advance `floor_rv` past the unfilled gap -- that would drop
    // the late RV=11 readiness MODIFIED via `should_skip` and hang the
    // reflector cache until the readiness wait times out. It must surface
    // `Expired` so the HTTP watch returns 410 Gone and the client relists with
    // fresh data.
    let (tx, rx) = broadcast::channel(8);
    let replay_source = TransportFlakyReplaySource;
    let mut cursor = WatchCursor::new(rx, replay_source, 10).with_ordered_replay();

    tx.send(WatchEvent::modified(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "frontend",
            "namespace": "default",
            "resourceVersion": "12"
        },
        "status": {"phase": "Running"}
    })))
    .unwrap();

    let supervisor = test_task_supervisor();
    let result = tokio::time::timeout(Duration::from_secs(2), cursor.next_event(&supervisor))
        .await
        .expect("cursor must not block on an unfillable gap");
    match result {
        Err(WatchCursorError::Expired) => {}
        other => panic!(
            "expected Expired (410) for an unfillable ordered gap under transport stress, got {other:?}"
        ),
    }
}

#[tokio::test]
async fn watch_cursor_allowlist_allows_scoped_transition_from_floor_with_no_regression() {
    let (tx, rx) = broadcast::channel(8);
    let replay_source = FixedReplaySource::new(Vec::new());
    let mut cursor = WatchCursor::new(rx, replay_source, 20);
    cursor.allow_low_rv_for_key(Some("default".into()), "resume-pod".into(), 15);

    tx.send(WatchEvent::added(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "resume-pod",
            "namespace": "default",
            "resourceVersion": "12"
        }
    })))
    .unwrap();
    tx.send(WatchEvent::modified(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "resume-pod",
            "namespace": "default",
            "resourceVersion": "16"
        }
    })))
    .unwrap();
    tx.send(WatchEvent::modified(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "other-pod",
            "namespace": "default",
            "resourceVersion": "16"
        }
    })))
    .unwrap();

    let supervisor = test_task_supervisor();
    let first = tokio::time::timeout(Duration::from_secs(1), cursor.next_event(&supervisor))
        .await
        .expect("scoped cursor should emit first floor-eligible event")
        .unwrap();
    assert_eq!(first.resource_version(), Some(16));
    assert_eq!(
        first.object.get("metadata").unwrap().get("name"),
        Some(&serde_json::Value::String("resume-pod".into()))
    );

    let timed =
        tokio::time::timeout(Duration::from_millis(200), cursor.next_event(&supervisor)).await;
    assert!(
        timed.is_err(),
        "events below scoped floor or transition threshold must be suppressed"
    );
}

#[tokio::test]
async fn watch_cursor_allowlist_skips_non_matching_floor_events() {
    let (tx, rx) = broadcast::channel(8);
    let replay_source = FixedReplaySource::new(Vec::new());
    let mut cursor = WatchCursor::new(rx, replay_source, 20);
    cursor.allow_low_rv_for_key(Some("default".into()), "resume-pod".into(), 15);

    tx.send(WatchEvent::modified(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "resume-pod",
            "namespace": "default",
            "resourceVersion": "12"
        }
    })))
    .unwrap();
    tx.send(WatchEvent::modified(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "resume-pod",
            "namespace": "default",
            "resourceVersion": "16"
        }
    })))
    .unwrap();
    tx.send(WatchEvent::modified(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "out-of-scope",
            "namespace": "default",
            "resourceVersion": "18"
        }
    })))
    .unwrap();
    tx.send(WatchEvent::modified(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "resume-pod",
            "namespace": "default",
            "resourceVersion": "21"
        }
    })))
    .unwrap();

    let supervisor = test_task_supervisor();
    let first = tokio::time::timeout(Duration::from_secs(1), cursor.next_event(&supervisor))
        .await
        .expect("cursor must emit allowed floor-crossing transition event")
        .unwrap();
    assert_eq!(first.resource_version(), Some(16));
    assert_eq!(
        first.object.get("metadata").unwrap().get("name"),
        Some(&serde_json::Value::String("resume-pod".into()))
    );

    let second = tokio::time::timeout(Duration::from_secs(1), cursor.next_event(&supervisor))
        .await
        .expect("cursor should skip floor-only events and emit the next resumed event")
        .unwrap();
    assert_eq!(second.resource_version(), Some(21));
    assert_eq!(
        second.object.get("metadata").unwrap().get("name"),
        Some(&serde_json::Value::String("resume-pod".into()))
    );

    let timed =
        tokio::time::timeout(Duration::from_millis(200), cursor.next_event(&supervisor)).await;
    assert!(
        timed.is_err(),
        "cursor must not emit events that remain below scoped floor/allowlist threshold"
    );
}

#[tokio::test]
async fn watch_cursor_ordered_replay_does_not_expire_rv_less_watch() {
    let (tx, rx) = broadcast::channel(8);
    let replay_source = WindowedReplaySource { earliest: Some(6) };
    let mut cursor = WatchCursor::new(rx, replay_source, 0).with_ordered_replay();

    tx.send(WatchEvent::modified(serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "frontend",
            "namespace": "default",
            "resourceVersion": "12"
        }
    })))
    .unwrap();

    let supervisor = test_task_supervisor();
    let event = cursor
        .next_event(&supervisor)
        .await
        .expect("rv-less watches must not expire from the retained history floor");
    assert_eq!(event.resource_version(), Some(12));
}

#[tokio::test]
async fn watch_cursor_target_field_selector_skips_nonmatching_pods_only() {
    let (tx, rx) = broadcast::channel(8);
    let replay_source = FixedReplaySource::new(Vec::new());
    let mut cursor = WatchCursor::new(rx, replay_source, 0).with_event_filter(
        WatchEventFilter::new().with_field_selector("v1", "Pod", "spec.nodeName=node-a"),
    );

    tx.send(WatchEvent::added(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "other-pod", "namespace": "default", "resourceVersion": "1"},
        "spec": {"nodeName": "node-b"}
    })))
    .unwrap();
    tx.send(WatchEvent::modified(serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "mounted-config", "namespace": "default", "resourceVersion": "2"}
    })))
    .unwrap();
    tx.send(WatchEvent::added(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "local-pod", "namespace": "default", "resourceVersion": "3"},
        "spec": {"nodeName": "node-a"}
    })))
    .unwrap();

    let supervisor = test_task_supervisor();
    let first = tokio::time::timeout(Duration::from_secs(1), cursor.next_event(&supervisor))
        .await
        .expect("configmap event timed out")
        .unwrap();
    assert_eq!(first.object["kind"], "ConfigMap");
    assert_eq!(first.resource_version(), Some(2));

    let second = tokio::time::timeout(Duration::from_secs(1), cursor.next_event(&supervisor))
        .await
        .expect("local pod event timed out")
        .unwrap();
    assert_eq!(second.object["metadata"]["name"], "local-pod");
    assert_eq!(second.resource_version(), Some(3));
}

#[tokio::test]
async fn watch_cursor_recovering_target_field_selector_skips_live_nonmatching_pods() {
    let (tx, rx) = broadcast::channel(8);
    let replay_source = FixedReplaySource::new(Vec::new());
    let mut cursor = WatchCursor::new(rx, replay_source, 0).with_event_filter(
        WatchEventFilter::new().with_field_selector("v1", "Pod", "spec.nodeName=node-a"),
    );

    tx.send(WatchEvent::added(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "other-pod", "namespace": "default", "resourceVersion": "1"},
        "spec": {"nodeName": "node-b"}
    })))
    .unwrap();
    tx.send(WatchEvent::added(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "local-pod", "namespace": "default", "resourceVersion": "2"},
        "spec": {"nodeName": "node-a"}
    })))
    .unwrap();

    let supervisor = test_task_supervisor();
    let cancel = tokio_util::sync::CancellationToken::new();
    let event = tokio::time::timeout(
        Duration::from_secs(1),
        cursor.next_event_recovering(&cancel, &supervisor),
    )
    .await
    .expect("local pod event timed out")
    .unwrap()
    .expect("cursor must not be cancelled");
    assert_eq!(event.object["metadata"]["name"], "local-pod");
    assert_eq!(event.resource_version(), Some(2));
}

#[tokio::test]
async fn watch_cursor_recovering_delivers_late_unseen_matching_event_after_higher_rv() {
    let (tx, rx) = broadcast::channel(8);
    let replay_source = FixedReplaySource::new(Vec::new());
    let mut cursor = WatchCursor::new(rx, replay_source, 0).with_event_filter(
        WatchEventFilter::new().with_field_selector("v1", "Pod", "spec.nodeName=node-a"),
    );

    tx.send(WatchEvent::modified(serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "later-config", "namespace": "default", "resourceVersion": "12"}
    })))
    .unwrap();
    tx.send(WatchEvent::added(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "late-local-pod", "namespace": "default", "resourceVersion": "10"},
        "spec": {"nodeName": "node-a"}
    })))
    .unwrap();

    let supervisor = test_task_supervisor();
    let cancel = tokio_util::sync::CancellationToken::new();
    let first = tokio::time::timeout(
        Duration::from_secs(1),
        cursor.next_event_recovering(&cancel, &supervisor),
    )
    .await
    .expect("higher-rv configmap event timed out")
    .unwrap()
    .expect("cursor must not be cancelled");
    assert_eq!(first.object["kind"], "ConfigMap");
    assert_eq!(first.resource_version(), Some(12));

    let second = tokio::time::timeout(
        Duration::from_secs(1),
        cursor.next_event_recovering(&cancel, &supervisor),
    )
    .await
    .expect("late local pod event timed out")
    .unwrap()
    .expect("cursor must not be cancelled");
    assert_eq!(second.object["metadata"]["name"], "late-local-pod");
    assert_eq!(second.resource_version(), Some(10));
}

#[tokio::test]
async fn test_watch_cursor_recovers_namespace_events_from_datastore_replay_source() {
    let watch_bus = WatchBus::new(1);
    let db = crate::datastore::test_support::in_memory().await;
    let start_rv = db.get_current_resource_version().await.unwrap();
    let rx = watch_bus.subscribe(WatchTopic::new("v1", "Namespace"));
    let replay_source = DatastoreWatchReplaySource::new(
        std::sync::Arc::new(db.clone()) as crate::datastore::DatastoreHandle,
        vec![WatchTarget::cluster("v1", "Namespace")],
    );
    let mut cursor = WatchCursor::new(rx, replay_source, start_rv);

    db.create_namespace(
        "lag-a",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": "lag-a"}
        }),
    )
    .await
    .unwrap();
    db.create_namespace(
        "lag-b",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": "lag-b"}
        }),
    )
    .await
    .unwrap();

    watch_bus.publish(WatchEvent::added(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": "stale-a", "resourceVersion": "1"}
    })));
    watch_bus.publish(WatchEvent::added(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": "stale-b", "resourceVersion": "2"}
    })));

    let supervisor = test_task_supervisor();
    let first = tokio::time::timeout(Duration::from_secs(1), cursor.next_event(&supervisor))
        .await
        .expect("first namespace event timed out")
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(1), cursor.next_event(&supervisor))
        .await
        .expect("second namespace event timed out")
        .unwrap();

    assert_eq!(first.object["metadata"]["name"], "lag-a");
    assert_eq!(first.event_type, EventType::Added);
    assert_eq!(second.object["metadata"]["name"], "lag-b");
    assert_eq!(second.event_type, EventType::Added);
}

#[tokio::test]
async fn test_watch_bootstrap_preserves_pre_recovery_snapshot() {
    let db = crate::datastore::test_support::in_memory().await;
    let start_rv = db.get_current_resource_version().await.unwrap();
    let bootstrap = WatchBootstrap::new(
        broadcast::channel(128).0.subscribe(),
        DatastoreWatchReplaySource::new(
            std::sync::Arc::new(db.clone()) as crate::datastore::DatastoreHandle,
            vec![WatchTarget::namespaced("v1", "Pod")],
        ),
        start_rv,
    );

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "bootstrap-gap",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "bootstrap-gap",
                "namespace": "default"
            },
            "spec": {
                "containers": [{"name": "pause", "image": "registry.k8s.io/pause:3.9"}],
                "nodeName": "test-node"
            },
            "status": {"phase": "Pending"}
        }),
    )
    .await
    .unwrap();
    let mut cursor = bootstrap.into_cursor();
    assert_eq!(cursor.prime_replay().await.unwrap(), 1);
    let supervisor = test_task_supervisor();
    let event = tokio::time::timeout(Duration::from_secs(1), cursor.next_event(&supervisor))
        .await
        .expect("bootstrapped cursor timed out")
        .unwrap();

    assert_eq!(event.event_type, EventType::Added);
    assert_eq!(event.object["metadata"]["name"], "bootstrap-gap");
}

#[test]
fn test_matches_filter_cluster_scoped_no_namespace_filter() {
    let obj = serde_json::json!({"kind": "Node", "metadata": {"name": "node1"}});
    let event = WatchEvent::added(obj);
    // No namespace filter = matches any cluster-scoped resource
    assert!(event.matches_filter("Node", None, None));
}

#[test]
fn test_watch_filters_by_label_selector() {
    // Resource with matching labels
    let matching = serde_json::json!({
        "kind": "ConfigMap",
        "metadata": {
            "name": "cm-a",
            "namespace": "default",
            "labels": {
                "app": "test",
                "env": "prod"
            }
        }
    });
    let event_matching = WatchEvent::added(matching);

    // Resource with non-matching labels
    let non_matching = serde_json::json!({
        "kind": "ConfigMap",
        "metadata": {
            "name": "cm-b",
            "namespace": "default",
            "labels": {
                "app": "other",
                "env": "dev"
            }
        }
    });
    let event_non_matching = WatchEvent::added(non_matching);

    // Resource with no labels
    let no_labels = serde_json::json!({
        "kind": "ConfigMap",
        "metadata": {
            "name": "cm-c",
            "namespace": "default"
        }
    });
    let event_no_labels = WatchEvent::added(no_labels);

    // Test: selector "app=test" should match only cm-a
    assert!(event_matching.matches_filter("ConfigMap", Some("default"), Some("app=test")));
    assert!(!event_non_matching.matches_filter("ConfigMap", Some("default"), Some("app=test")));
    assert!(!event_no_labels.matches_filter("ConfigMap", Some("default"), Some("app=test")));

    // Test: selector "app=test,env=prod" should match only cm-a
    assert!(event_matching.matches_filter("ConfigMap", Some("default"), Some("app=test,env=prod")));
    assert!(!event_non_matching.matches_filter(
        "ConfigMap",
        Some("default"),
        Some("app=test,env=prod")
    ));

    // Test: no selector should match all
    assert!(event_matching.matches_filter("ConfigMap", Some("default"), None));
    assert!(event_non_matching.matches_filter("ConfigMap", Some("default"), None));
    assert!(event_no_labels.matches_filter("ConfigMap", Some("default"), None));
}

#[tokio::test]
async fn test_concurrent_watchers_receive_events_in_same_order() {
    // Simulates the Sonobuoy test: multiple concurrent broadcast receivers
    // must see events in the same order (guaranteed by tokio broadcast)
    let (tx, _) = broadcast::channel::<WatchEvent>(128);

    // Create 5 concurrent receivers
    let mut receivers: Vec<_> = (0..5).map(|_| tx.subscribe()).collect();

    // Send 10 events with monotonically increasing resourceVersions
    for i in 1..=10 {
        let event = WatchEvent::modified(serde_json::json!({
            "kind": "ConfigMap",
            "metadata": {
                "name": format!("cm-{}", i),
                "namespace": "default",
                "resourceVersion": i.to_string()
            }
        }));
        tx.send(event).unwrap();
    }

    // Verify all receivers see events in the same order
    let mut all_rvs: Vec<Vec<String>> = Vec::new();
    for rx in &mut receivers {
        let mut rvs = Vec::new();
        for _ in 0..10 {
            let event = rx.recv().await.unwrap();
            let rv = event.object["metadata"]["resourceVersion"]
                .as_str()
                .unwrap()
                .to_string();
            rvs.push(rv);
        }
        all_rvs.push(rvs);
    }

    // All receivers must have identical RV sequences
    let first = &all_rvs[0];
    for (i, rvs) in all_rvs.iter().enumerate().skip(1) {
        assert_eq!(
            first, rvs,
            "Receiver {} got different order than receiver 0: {:?} vs {:?}",
            i, rvs, first
        );
    }

    // Verify ordering is monotonically increasing
    for i in 1..first.len() {
        let prev: i64 = first[i - 1].parse().unwrap();
        let curr: i64 = first[i].parse().unwrap();
        assert!(
            curr > prev,
            "Events not in RV order: rv {} followed by rv {}",
            prev,
            curr
        );
    }
}

#[tokio::test]
async fn test_delete_without_finalizers_emits_single_deleted_event() {
    // Regression test for watch RV off-by-one: deleting a resource without
    // finalizers must emit a single DELETED event, not MODIFIED+DELETED.
    //
    // Before the fix, the API delete handler called update_resource (set
    // deletionTimestamp, MODIFIED at RV=N) then delete_resource (DELETED at
    // RV=N+1) for ALL resources. A watcher started at RV=N would skip the
    // MODIFIED (event_rv <= N) and get DELETED at N+1, while an older watcher
    // would get MODIFIED at N. Different events → "resource version mismatch".
    //
    // The fix: only emit MODIFIED+DELETED for resources with finalizers.
    // Without finalizers, emit only DELETED.
    let (tx, _) = broadcast::channel::<WatchEvent>(128);
    let mut rx = tx.subscribe();

    // Simulate the FIXED delete handler for a resource WITHOUT finalizers:
    // only one DELETED event, no preceding MODIFIED.
    tx.send(WatchEvent::deleted(serde_json::json!({
        "kind": "ConfigMap",
        "metadata": {"name": "test-cm", "namespace": "default", "resourceVersion": "731"}
    })))
    .unwrap();

    let e = rx.recv().await.unwrap();
    assert_eq!(e.event_type, EventType::Deleted);
    assert_eq!(e.object["metadata"]["resourceVersion"], "731");

    // Verify no extra events
    assert!(rx.try_recv().is_err());

    // Now simulate a watcher consistency scenario:
    // Two watchers — one at RV=730 (older), one at RV=731 (newer).
    // With single DELETED event at RV=731:
    //   older watcher (rv > 730): sees DELETED at 731 ✓
    //   newer watcher (rv > 731): skips 731, waits for next event ✓
    // This is correct because the newer watcher was created AFTER the event.

    // The BUG was: two events (MODIFIED at 731, DELETED at 732).
    //   older watcher: sees MODIFIED at 731
    //   newer watcher (rv > 731): skips 731, sees DELETED at 732
    //   → "resource version mismatch, expected 731 but got 732"
}

#[test]
fn test_watch_event_rv_at_or_before_initial_list_should_be_skipped() {
    // Sonobuoy: "Unexpected watch notification observed"
    // Race condition: when rv=0 watch starts, it:
    //   1. subscribes to broadcast (buffered channel)
    //   2. queries initial list (gets resources up to last_rv=5)
    //   3. sends ADDED events from initial list
    //   4. starts broadcast loop — may replay events already covered by initial list
    //
    // Events with rv <= last_rv from the initial list must be skipped in
    // the broadcast loop to avoid duplicate ADDED events.
    //
    // WatchEvent::event_rv_at_or_before(threshold) returns true if the
    // event's resourceVersion <= threshold and should be skipped.

    let event_rv_5 = WatchEvent::added(serde_json::json!({
        "kind": "ConfigMap",
        "metadata": {
            "name": "cm-b",
            "namespace": "default",
            "resourceVersion": "5",
            "labels": {"testing.k8s.io/other": "true"}
        }
    }));
    let event_rv_6 = WatchEvent::modified(serde_json::json!({
        "kind": "ConfigMap",
        "metadata": {
            "name": "cm-b",
            "namespace": "default",
            "resourceVersion": "6",
            "labels": {"testing.k8s.io/other": "true"}
        }
    }));

    // After initial list with last_rv=5, events at rv=5 must be filtered
    // (they were already sent via the initial list path).
    assert!(
        event_rv_5.event_rv_at_or_before(5),
        "rv=5 event must be skipped when initial list had last_rv=5"
    );
    // Events at rv=6 must pass through (new since initial list).
    assert!(
        !event_rv_6.event_rv_at_or_before(5),
        "rv=6 event must pass through when initial list had last_rv=5"
    );
    // When requested_rv=0 (initial list+watch), events at rv <= last_rv
    // must still be filtered using last_rv (not requested_rv=0).
    assert!(
        event_rv_5.event_rv_at_or_before(5),
        "Must use last_rv (5) as threshold, not requested_rv (0)"
    );
}
