use super::*;

fn test_task_supervisor() -> crate::task_supervisor::TaskSupervisor {
    crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    )
}

#[tokio::test]
async fn test_concurrent_watchers_with_different_label_selectors_only_receive_matching_events() {
    // Simulates the Sonobuoy test:
    //   - Watcher A: labelSelector=watch-this-configmap=multiple-watchers-A
    //   - Watcher B: labelSelector=watch-this-configmap=multiple-watchers-B
    //   - Watcher C: no labelSelector (receives all)
    //
    // configmap-A (label: watch-this-configmap=multiple-watchers-A) is created.
    // configmap-B (label: watch-this-configmap=multiple-watchers-B) is created.
    //
    // Expected:
    //   - Watcher A sees ONLY configmap-A (not configmap-B)
    //   - Watcher B sees ONLY configmap-B (not configmap-A)
    //   - Watcher C sees BOTH configmap-A and configmap-B
    let (tx, _) = broadcast::channel::<WatchEvent>(128);

    let mut rx_a = tx.subscribe();
    let mut rx_b = tx.subscribe();
    let mut rx_c = tx.subscribe();

    let selector_a = "watch-this-configmap=multiple-watchers-A";
    let selector_b = "watch-this-configmap=multiple-watchers-B";

    // Broadcast configmap-A (label matches selector A, not B)
    let configmap_a = WatchEvent::added(serde_json::json!({
        "kind": "ConfigMap",
        "metadata": {
            "name": "configmap-a",
            "namespace": "default",
            "resourceVersion": "1",
            "labels": {
                "watch-this-configmap": "multiple-watchers-A"
            }
        }
    }));
    tx.send(configmap_a).unwrap();

    // Broadcast configmap-B (label matches selector B, not A)
    let configmap_b = WatchEvent::added(serde_json::json!({
        "kind": "ConfigMap",
        "metadata": {
            "name": "configmap-b",
            "namespace": "default",
            "resourceVersion": "2",
            "labels": {
                "watch-this-configmap": "multiple-watchers-B"
            }
        }
    }));
    tx.send(configmap_b).unwrap();

    // Collect events from each watcher, applying label selector filter
    let mut events_a: Vec<String> = Vec::new();
    let mut events_b: Vec<String> = Vec::new();
    let mut events_c: Vec<String> = Vec::new();

    for _ in 0..2 {
        let event = rx_a.recv().await.unwrap();
        if event.matches_filter("ConfigMap", Some("default"), Some(selector_a)) {
            events_a.push(
                event.object["metadata"]["name"]
                    .as_str()
                    .unwrap()
                    .to_string(),
            );
        }
    }
    for _ in 0..2 {
        let event = rx_b.recv().await.unwrap();
        if event.matches_filter("ConfigMap", Some("default"), Some(selector_b)) {
            events_b.push(
                event.object["metadata"]["name"]
                    .as_str()
                    .unwrap()
                    .to_string(),
            );
        }
    }
    for _ in 0..2 {
        let event = rx_c.recv().await.unwrap();
        if event.matches_filter("ConfigMap", Some("default"), None) {
            events_c.push(
                event.object["metadata"]["name"]
                    .as_str()
                    .unwrap()
                    .to_string(),
            );
        }
    }

    // Watcher A: only configmap-a
    assert_eq!(
        events_a,
        vec!["configmap-a"],
        "Watcher A should only receive configmap-a, got: {:?}",
        events_a
    );

    // Watcher B: only configmap-b
    assert_eq!(
        events_b,
        vec!["configmap-b"],
        "Watcher B should only receive configmap-b, got: {:?}",
        events_b
    );

    // Watcher C: both
    assert_eq!(
        events_c,
        vec!["configmap-a", "configmap-b"],
        "Watcher C (no selector) should receive both, got: {:?}",
        events_c
    );
}

#[test]
fn test_matches_label_selector_set_based_in_operator() {
    // K8s e2e Watchers test uses FormatLabelSelector with MatchExpressions{In},
    // which formats as "watch-this-configmap in (multiple-watchers-A)".
    // matches_label_selector must handle this syntax or it passes all events.
    let cm_a = serde_json::json!({
        "kind": "ConfigMap",
        "metadata": {
            "name": "e2e-watch-test-configmap-a",
            "namespace": "test-ns",
            "labels": { "watch-this-configmap": "multiple-watchers-A" }
        }
    });
    let cm_b = serde_json::json!({
        "kind": "ConfigMap",
        "metadata": {
            "name": "e2e-watch-test-configmap-b",
            "namespace": "test-ns",
            "labels": { "watch-this-configmap": "multiple-watchers-B" }
        }
    });
    let cm_ab = serde_json::json!({
        "kind": "ConfigMap",
        "metadata": {
            "name": "e2e-watch-test-configmap-ab",
            "namespace": "test-ns",
            "labels": { "watch-this-configmap": "multiple-watchers-A" }
        }
    });

    let event_a = WatchEvent::added(cm_a);
    let event_b = WatchEvent::added(cm_b);
    let event_ab = WatchEvent::added(cm_ab);

    // Watcher A: "watch-this-configmap in (multiple-watchers-A)"
    let sel_a = "watch-this-configmap in (multiple-watchers-A)";
    assert!(
        event_a.matches_filter("ConfigMap", Some("test-ns"), Some(sel_a)),
        "cm-a should match watcher A selector"
    );
    assert!(
        !event_b.matches_filter("ConfigMap", Some("test-ns"), Some(sel_a)),
        "cm-b should NOT match watcher A selector"
    );

    // Watcher B: "watch-this-configmap in (multiple-watchers-B)"
    let sel_b = "watch-this-configmap in (multiple-watchers-B)";
    assert!(
        !event_a.matches_filter("ConfigMap", Some("test-ns"), Some(sel_b)),
        "cm-a should NOT match watcher B selector"
    );
    assert!(
        event_b.matches_filter("ConfigMap", Some("test-ns"), Some(sel_b)),
        "cm-b should match watcher B selector"
    );

    // Watcher AB: "watch-this-configmap in (multiple-watchers-A,multiple-watchers-B)"
    let sel_ab = "watch-this-configmap in (multiple-watchers-A,multiple-watchers-B)";
    assert!(
        event_a.matches_filter("ConfigMap", Some("test-ns"), Some(sel_ab)),
        "cm-a should match watcher AB selector"
    );
    assert!(
        event_b.matches_filter("ConfigMap", Some("test-ns"), Some(sel_ab)),
        "cm-b should match watcher AB selector"
    );
    assert!(
        event_ab.matches_filter("ConfigMap", Some("test-ns"), Some(sel_ab)),
        "cm-ab (label A) should match watcher AB selector"
    );

    // notin operator
    let sel_notin_a = "watch-this-configmap notin (multiple-watchers-A)";
    assert!(
        !event_a.matches_filter("ConfigMap", Some("test-ns"), Some(sel_notin_a)),
        "cm-a should NOT match notin-A selector"
    );
    assert!(
        event_b.matches_filter("ConfigMap", Some("test-ns"), Some(sel_notin_a)),
        "cm-b should match notin-A selector"
    );
}

#[test]
fn test_matches_label_selector_inequality_not_equal() {
    // K8s supports != operator: "key!=value" means exclude resources where key==value
    let obj_a = serde_json::json!({
        "kind": "ConfigMap",
        "metadata": {
            "name": "cm-a",
            "namespace": "default",
            "labels": {
                "env": "prod"
            }
        }
    });
    let obj_b = serde_json::json!({
        "kind": "ConfigMap",
        "metadata": {
            "name": "cm-b",
            "namespace": "default",
            "labels": {
                "env": "dev"
            }
        }
    });
    let obj_no_label = serde_json::json!({
        "kind": "ConfigMap",
        "metadata": {
            "name": "cm-c",
            "namespace": "default"
        }
    });

    let event_a = WatchEvent::added(obj_a);
    let event_b = WatchEvent::added(obj_b);
    let event_no_label = WatchEvent::added(obj_no_label);

    // "env!=prod": matches cm-b (env=dev) and cm-c (no label), NOT cm-a (env=prod)
    assert!(
        !event_a.matches_filter("ConfigMap", Some("default"), Some("env!=prod")),
        "env=prod should NOT match selector env!=prod"
    );
    assert!(
        event_b.matches_filter("ConfigMap", Some("default"), Some("env!=prod")),
        "env=dev should match selector env!=prod"
    );
    assert!(
        event_no_label.matches_filter("ConfigMap", Some("default"), Some("env!=prod")),
        "no env label should match selector env!=prod"
    );
}

#[test]
fn test_watch_label_selector_exists_and_not_exists_match_list_semantics() {
    let exists_selector = crate::label_selector::LabelSelector::parse("has-gpu").unwrap();
    let not_exists_selector = crate::label_selector::LabelSelector::parse("!deprecated").unwrap();

    let events = vec![
        WatchEvent::added(serde_json::json!({
            "kind": "ConfigMap",
            "metadata": {
                "name": "cm-gpu",
                "namespace": "default",
                "labels": {
                    "has-gpu": "true"
                }
            }
        })),
        WatchEvent::added(serde_json::json!({
            "kind": "ConfigMap",
            "metadata": {
                "name": "cm-deprecated",
                "namespace": "default",
                "labels": {
                    "deprecated": "true"
                }
            }
        })),
        WatchEvent::added(serde_json::json!({
            "kind": "ConfigMap",
            "metadata": {
                "name": "cm-gpu-deprecated",
                "namespace": "default",
                "labels": {
                    "has-gpu": "true",
                    "deprecated": "true"
                }
            }
        })),
        WatchEvent::added(serde_json::json!({
            "kind": "ConfigMap",
            "metadata": {
                "name": "cm-unlabeled",
                "namespace": "default"
            }
        })),
    ];

    for event in events {
        let list_match_exists = exists_selector.matches_resource(&event.object);
        let watch_match_exists =
            event.matches_filter_parsed("ConfigMap", Some("default"), Some(&exists_selector));
        assert_eq!(
            watch_match_exists, list_match_exists,
            "watch exists-selector evaluation must match list semantics for {:?}",
            event.object
        );

        let list_match_not_exists = not_exists_selector.matches_resource(&event.object);
        let watch_match_not_exists =
            event.matches_filter_parsed("ConfigMap", Some("default"), Some(&not_exists_selector));
        assert_eq!(
            watch_match_not_exists, list_match_not_exists,
            "watch not-exists-selector evaluation must match list semantics for {:?}",
            event.object
        );
    }
}

#[test]
fn test_matches_field_selector_metadata_name_and_namespace() {
    let obj = serde_json::json!({
        "kind": "NoxuType",
        "metadata": {
            "name": "cr-a",
            "namespace": "default"
        }
    });
    let event = WatchEvent::added(obj);
    assert!(event.matches_field_selector(Some("metadata.name=cr-a")));
    assert!(event.matches_field_selector(Some("metadata.namespace=default")));
    assert!(!event.matches_field_selector(Some("metadata.name=cr-b")));
    assert!(!event.matches_field_selector(Some("metadata.namespace=other")));
}

#[test]
fn test_matches_field_selector_inequality() {
    let obj = serde_json::json!({
        "kind": "NoxuType",
        "metadata": {
            "name": "cr-a",
            "namespace": "default"
        }
    });
    let event = WatchEvent::added(obj);
    assert!(event.matches_field_selector(Some("metadata.name!=cr-b")));
    assert!(!event.matches_field_selector(Some("metadata.name!=cr-a")));
}

#[test]
fn test_matches_field_selector_event_source_alias() {
    let obj = serde_json::json!({
        "kind": "Event",
        "metadata": {
            "name": "ev-a",
            "namespace": "default"
        },
        "source": {
            "component": "event-test"
        }
    });
    let event = WatchEvent::added(obj);
    assert!(event.matches_field_selector(Some("source=event-test")));
    assert!(!event.matches_field_selector(Some("source=other")));
}

#[test]
fn test_matches_field_selector_event_source_alias_events_v1_shape() {
    let obj = serde_json::json!({
        "apiVersion": "events.k8s.io/v1",
        "kind": "Event",
        "metadata": {
            "name": "ev-a",
            "namespace": "default"
        },
        "reportingController": "event-test"
    });
    let event = WatchEvent::added(obj);
    assert!(event.matches_field_selector(Some("source=event-test")));
    assert!(!event.matches_field_selector(Some("source=other")));
}

#[test]
fn test_matches_field_selector_event_source_alias_ignores_empty_deprecated_source() {
    let event = WatchEvent::added(serde_json::json!({
        "apiVersion": "events.k8s.io/v1",
        "kind": "Event",
        "metadata": {
            "name": "ev-a",
            "namespace": "default"
        },
        "deprecatedSource": {
            "component": ""
        },
        "reportingController": "event-test"
    }));

    assert!(event.matches_field_selector(Some("source=event-test")));
    assert!(!event.matches_field_selector(Some("source=other")));
}

#[test]
fn test_matches_field_selector_event_source_alias_reporting_component() {
    let event = WatchEvent::added(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": {"name": "event-a", "namespace": "default"},
        "reportingComponent": "event-test"
    }));

    assert!(event.matches_field_selector(Some("source=event-test")));
    assert!(!event.matches_field_selector(Some("source=other")));
}

#[test]
fn test_event_type_serde_wire_format() {
    assert_eq!(
        serde_json::to_string(&EventType::Added).unwrap(),
        "\"ADDED\""
    );
    assert_eq!(
        serde_json::to_string(&EventType::Modified).unwrap(),
        "\"MODIFIED\""
    );
    assert_eq!(
        serde_json::to_string(&EventType::Deleted).unwrap(),
        "\"DELETED\""
    );
    assert_eq!(
        serde_json::to_string(&EventType::Bookmark).unwrap(),
        "\"BOOKMARK\""
    );
    assert_eq!(
        serde_json::from_str::<EventType>("\"ADDED\"").unwrap(),
        EventType::Added
    );
    assert_eq!(
        serde_json::from_str::<EventType>("\"MODIFIED\"").unwrap(),
        EventType::Modified
    );
    assert_eq!(
        serde_json::from_str::<EventType>("\"DELETED\"").unwrap(),
        EventType::Deleted
    );
    assert_eq!(
        serde_json::from_str::<EventType>("\"BOOKMARK\"").unwrap(),
        EventType::Bookmark
    );
}

#[test]
fn test_watch_event_object_arc_serde_roundtrip() {
    let obj = serde_json::json!({"kind": "Pod", "metadata": {"name": "arc-test"}});
    let event = WatchEvent::added(obj.clone());
    let json = serde_json::to_string(&event).unwrap();
    assert!(json.contains("\"ADDED\""));
    assert!(json.contains("\"arc-test\""));
    let decoded: WatchEvent = serde_json::from_str(&json).unwrap();
    assert_eq!(*decoded.object, obj);
}

/// Fake replay source that fails for the first N calls, then succeeds.
pub struct FailingReplaySource {
    events: Vec<WatchEvent>,
    attempts_until_success: usize,
    attempt_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    first_attempt: std::sync::Arc<tokio::sync::Notify>,
}

impl FailingReplaySource {
    pub fn new(events: Vec<WatchEvent>, attempts_until_success: usize) -> Self {
        Self {
            events,
            attempts_until_success,
            attempt_count: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            first_attempt: std::sync::Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn attempt_count(&self) -> usize {
        self.attempt_count.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn first_attempt_notify(&self) -> std::sync::Arc<tokio::sync::Notify> {
        self.first_attempt.clone()
    }
}

#[async_trait::async_trait]
impl WatchReplaySource for FailingReplaySource {
    async fn replay_since(&self, since_rv: i64) -> anyhow::Result<Vec<WatchEvent>> {
        let prev = self
            .attempt_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if prev == 0 {
            self.first_attempt.notify_one();
        }
        if self.attempt_count() < self.attempts_until_success {
            Err(anyhow::anyhow!("simulated replay failure"))
        } else {
            Ok(self
                .events
                .iter()
                .filter(|event| event.resource_version().unwrap_or_default() > since_rv)
                .cloned()
                .collect())
        }
    }
}

#[tokio::test]
async fn test_next_event_recovering_retries_on_replay_failure() {
    // Test that replay failures are retried until success
    let cancel = CancellationToken::new();
    let (tx, rx) = broadcast::channel(2);

    // Replay source fails twice, then succeeds on third attempt
    let replay_source = FailingReplaySource::new(
        vec![WatchEvent::added(serde_json::json!({
            "kind": "Pod",
            "metadata": {"name": "missed-pod", "resourceVersion": "5"}
        }))],
        3, // attempts_until_success
    );

    let mut cursor = WatchCursor::new(rx, replay_source, 1);

    // Force broadcast lag to trigger replay
    tx.send(WatchEvent::added(serde_json::json!({
        "kind": "Pod",
        "metadata": {"name": "stale", "resourceVersion": "2"}
    })))
    .unwrap();
    tx.send(WatchEvent::added(serde_json::json!({
        "kind": "Pod",
        "metadata": {"name": "stale-2", "resourceVersion": "3"}
    })))
    .unwrap();
    tx.send(WatchEvent::added(serde_json::json!({
        "kind": "Pod",
        "metadata": {"name": "stale-3", "resourceVersion": "4"}
    })))
    .unwrap();

    // First call to next_event_recovering should:
    // 1. Detect lag (replay_required = true)
    // 2. Retry replay twice (fail, fail)
    // 3. Succeed on third attempt
    let event = cursor
        .next_event_recovering(&cancel, &test_task_supervisor())
        .await
        .unwrap()
        .expect("should return event after replay retry");

    // Verify we got the replayed event
    assert_eq!(event.object["metadata"]["name"], "missed-pod");
    assert_eq!(event.resource_version(), Some(5));
}

#[tokio::test]
async fn test_next_event_recovering_holds_live_events_during_replay_failure() {
    // Test that live events arriving during replay failure are NOT consumed
    // until replay succeeds.
    let cancel = CancellationToken::new();
    let (tx, rx) = broadcast::channel(2);

    // Replay source fails twice, then succeeds
    let replay_source = FailingReplaySource::new(
        vec![WatchEvent::added(serde_json::json!({
            "kind": "Pod",
            "metadata": {"name": "replay-pod", "resourceVersion": "100"}
        }))],
        3, // attempts_until_success
    );

    let first_attempt = replay_source.first_attempt_notify();
    let mut cursor = WatchCursor::new(rx, replay_source, 1);

    // Send the live event only after lag has been detected and the first replay
    // attempt has started — no timing assumption.
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        first_attempt.notified().await;
        tx_clone
            .send(WatchEvent::added(serde_json::json!({
                "kind": "Pod",
                "metadata": {"name": "live-pod", "resourceVersion": "200"}
            })))
            .unwrap();
    });

    // Force broadcast lag to trigger replay BEFORE the live event arrives
    tx.send(WatchEvent::added(serde_json::json!({
        "kind": "Pod",
        "metadata": {"name": "stale", "resourceVersion": "2"}
    })))
    .unwrap();
    tx.send(WatchEvent::added(serde_json::json!({
        "kind": "Pod",
        "metadata": {"name": "stale-2", "resourceVersion": "3"}
    })))
    .unwrap();
    tx.send(WatchEvent::added(serde_json::json!({
        "kind": "Pod",
        "metadata": {"name": "stale-3", "resourceVersion": "4"}
    })))
    .unwrap();

    // First call should get the replayed event (RV=100) after retries
    // The live event (RV=200) should not be returned before replay succeeds
    let event = cursor
        .next_event_recovering(&cancel, &test_task_supervisor())
        .await
        .unwrap()
        .expect("should return replayed event");
    assert_eq!(event.object["metadata"]["name"], "replay-pod");
    assert_eq!(event.resource_version(), Some(100));

    // Second call should get the live event (RV=200)
    let event = cursor
        .next_event_recovering(&cancel, &test_task_supervisor())
        .await
        .unwrap()
        .expect("should return live event");
    assert_eq!(event.object["metadata"]["name"], "live-pod");
    assert_eq!(event.resource_version(), Some(200));
}

// ---------------------------------------------------------------------------
// Multinode scoped-watch delivery mock (Guestbook readiness stall).
// ---------------------------------------------------------------------------

fn scoped_watch_event_at(rv: i64, name: &str) -> WatchEvent {
    WatchEvent::modified(serde_json::json!({
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": {
            "name": name,
            "namespace": "kubectl-gb",
            "resourceVersion": rv.to_string(),
            "labels": {"app": "guestbook", "tier": "frontend"}
        }
    }))
}

/// Mock replay source that models the multinode raft/outbox apply inconsistency
/// behind the Guestbook scoped-watch stall: the durable `watch_events` window is
/// NON-DENSE -- a committed, broadcast RV is absent (its row was lost or lagged
/// under transport stress). The cursor's replay then advances its delivery
/// floor past the missing RV, and the live broadcast copy is silently dropped
/// (the `klights::watch_diag` "cursor dropped an undelivered event (floor
/// advanced past it)" canary). `fills_on_retry` simulates the late-landing row:
/// a later replay recovers the event.
struct NonDenseReplaySource {
    events: Vec<WatchEvent>,
    earliest: i64,
    fills_on_retry: bool,
    calls: std::sync::Mutex<u32>,
}

impl NonDenseReplaySource {
    fn new(events: Vec<WatchEvent>, earliest: i64, fills_on_retry: bool) -> Self {
        Self {
            events,
            earliest,
            fills_on_retry,
            calls: std::sync::Mutex::new(0),
        }
    }
}

#[async_trait::async_trait]
impl WatchReplaySource for NonDenseReplaySource {
    async fn replay_since(&self, since_rv: i64) -> anyhow::Result<Vec<WatchEvent>> {
        let calls = {
            let mut c = self.calls.lock().unwrap();
            *c += 1;
            *c
        };
        // First replay returns the non-dense window exactly as the failing run
        // served it. If `fills_on_retry`, a subsequent replay (after the
        // raft-apply lag clears) returns the full dense window.
        let base: Vec<WatchEvent> = if calls > 1 && self.fills_on_retry {
            self.events.clone()
        } else {
            self.events
                .iter()
                .filter(|e| e.resource_version().unwrap_or(0) > since_rv)
                .cloned()
                .collect()
        };
        if calls > 1 && self.fills_on_retry {
            // Insert the previously-missing rv=12 row (now landed).
            let mut v: Vec<WatchEvent> = base
                .into_iter()
                .filter(|e| e.resource_version().unwrap_or(0) != 12)
                .collect();
            v.push(scoped_watch_event_at(12, "frontend-ready"));
            v.sort_by_key(|e| e.resource_version().unwrap_or(0));
            Ok(v.into_iter()
                .filter(|e| e.resource_version().unwrap_or(0) > since_rv)
                .collect())
        } else {
            Ok(base)
        }
    }

    async fn earliest_retained_rv(&self) -> anyhow::Result<Option<i64>> {
        Ok(Some(self.earliest))
    }
}

/// RED regression: a broadcast in-scope event whose durable row is absent from
/// the (non-dense) replay must not be silently dropped. The cursor must either
/// recover it (re-replay once the row lands) or surface Expired so the HTTP
/// watch returns 410 and the client relists -- never the quiet bookmark-only
/// stall that timed out `kubectl wait` in the failing run.
#[tokio::test]
async fn test_cursor_recovers_broadcast_event_missing_from_non_dense_replay() {
    let (tx, rx) = broadcast::channel::<WatchEvent>(16);
    // Durable window retains rv=11 and rv=13 but NOT rv=12 (the in-scope
    // frontend Ready event whose row was lost/lagged). earliest=11.
    let replay = NonDenseReplaySource::new(
        vec![
            scoped_watch_event_at(11, "pod-a"),
            scoped_watch_event_at(13, "pod-c"),
        ],
        11,
        true,
    );
    let mut cursor = WatchCursor::new(rx, replay, 10).with_ordered_replay();
    cursor
        .prime_replay_or_expired()
        .await
        .expect("prime replay must succeed (boundary gap check passes: 10+1 < 11 is false)");

    // rv=12 (missing from the durable window) arrives via live broadcast, then
    // rv=14 follows so the cursor has a forward event to return.
    tx.send(scoped_watch_event_at(12, "frontend-ready"))
        .unwrap();
    tx.send(scoped_watch_event_at(14, "pod-d")).unwrap();

    let mut delivered: Vec<i64> = Vec::new();
    let mut expired = false;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    while delivered.iter().all(|&rv| rv != 12) && !expired && tokio::time::Instant::now() < deadline
    {
        let next = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            cursor.next_event(&test_task_supervisor()),
        )
        .await;
        match next {
            Ok(Ok(event)) => {
                if let Some(rv) = event.resource_version() {
                    delivered.push(rv);
                }
            }
            Ok(Err(WatchCursorError::Expired)) => expired = true,
            Ok(Err(WatchCursorError::Closed)) => break,
            Ok(Err(WatchCursorError::Replay(_))) => continue,
            Err(_) => break, // timeout: no more events
        }
    }

    assert!(
        delivered.iter().any(|&rv| rv == 12) || expired,
        "scoped watch cursor must recover the in-scope broadcast event rv=12 \
         (whose durable row lagged) via re-replay, or surface Expired (410) -- \
         silently dropping it is the Guestbook readiness stall. \
         delivered={delivered:?} expired={expired}"
    );
}

/// Companion to the recovery test: when the broadcast event's durable row is
/// GENUINELY absent (never lands -- a permanent broadcast/watch_events
/// inconsistency), the cursor must surface `Expired` so the HTTP watch returns
/// 410 and the client relists, rather than silently dropping the event and
/// stalling. This is the fail-safe branch of the fix.
#[tokio::test]
async fn test_cursor_expires_when_broadcast_event_never_persisted() {
    let (tx, rx) = broadcast::channel::<WatchEvent>(16);
    // Durable window retains rv=11 and rv=13 but rv=12 is permanently absent
    // (fills_on_retry=false): the broadcast row was lost, not merely lagged.
    let replay = NonDenseReplaySource::new(
        vec![
            scoped_watch_event_at(11, "pod-a"),
            scoped_watch_event_at(13, "pod-c"),
        ],
        11,
        false,
    );
    let mut cursor = WatchCursor::new(rx, replay, 10).with_ordered_replay();
    cursor
        .prime_replay_or_expired()
        .await
        .expect("prime replay");

    tx.send(scoped_watch_event_at(12, "frontend-ready-never-persisted"))
        .unwrap();

    let mut saw_expired = false;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    while !saw_expired && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(
            std::time::Duration::from_millis(500),
            cursor.next_event(&test_task_supervisor()),
        )
        .await
        {
            Ok(Err(WatchCursorError::Expired)) => saw_expired = true,
            Ok(Ok(_))
            | Ok(Err(WatchCursorError::Replay(_)))
            | Ok(Err(WatchCursorError::Closed)) => continue,
            Err(_) => break,
        }
    }
    assert!(
        saw_expired,
        "a broadcast event whose durable row is permanently absent must surface Expired (410), \
         not be silently dropped"
    );
}
