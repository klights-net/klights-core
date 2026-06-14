use super::tests_replay::FailingReplaySource;
use super::*;

fn test_task_supervisor() -> crate::task_supervisor::TaskSupervisor {
    crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    )
}

#[tokio::test]
async fn test_next_event_recovering_cancels_during_replay_backoff() {
    // Test that cancellation during replay backoff returns Ok(None) quickly
    let cancel = CancellationToken::new();
    let (tx, rx) = broadcast::channel(2);
    let tx_clone = tx.clone();
    let cancel_clone = cancel.clone();

    // Replay source that always fails
    let replay_source = FailingReplaySource::new(
        vec![WatchEvent::added(serde_json::json!({
            "kind": "Pod",
            "metadata": {"name": "never-seen", "resourceVersion": "5"}
        }))],
        999, // always fail
    );

    let mut cursor = WatchCursor::new(rx, replay_source, 1);

    // Spawn a task to cancel after a short delay and trigger lag
    tokio::spawn(async move {
        // Give the main task time to start, then trigger lag
        tokio::time::sleep(Duration::from_millis(10)).await;
        // Send enough messages to cause lag (channel capacity is 2)
        for i in 0..5 {
            tx_clone
                .send(WatchEvent::added(serde_json::json!({
                    "kind": "Pod",
                    "metadata": {"name": format!("stale-{}", i), "resourceVersion": (i + 2)}
                })))
                .unwrap();
        }
        // Cancel shortly after lag is triggered
        tokio::time::sleep(Duration::from_millis(30)).await;
        cancel_clone.cancel();
    });

    // Should return Ok(None) within 100ms of cancellation
    let start = std::time::Instant::now();
    let result = cursor
        .next_event_recovering(&cancel, &test_task_supervisor())
        .await
        .unwrap();
    let elapsed = start.elapsed();

    assert!(result.is_none(), "should return None on cancellation");
    assert!(
        elapsed < Duration::from_millis(100),
        "should exit within 100ms of cancellation, took {:?}",
        elapsed
    );
}

#[tokio::test]
async fn test_next_event_recovering_exponential_backoff() {
    // Test that exponential backoff is applied on consecutive failures
    let cancel = CancellationToken::new();
    let (tx, rx) = broadcast::channel(2);
    let tx_clone = tx.clone();
    let cancel_clone = cancel.clone();

    // Replay source that fails many times
    let replay_source = FailingReplaySource::new(vec![], 999);

    let mut cursor = WatchCursor::new(rx, replay_source, 1);

    // Spawn a task to trigger lag and cancel after some retries
    tokio::spawn(async move {
        // Give main task time to start
        tokio::time::sleep(Duration::from_millis(5)).await;
        // Trigger lag
        for i in 0..5 {
            tx_clone
                .send(WatchEvent::added(serde_json::json!({
                    "kind": "Pod",
                    "metadata": {"name": format!("stale-{}", i), "resourceVersion": (i + 2)}
                })))
                .unwrap();
        }
        // Let it retry for a while (200ms should be enough for several retries)
        tokio::time::sleep(Duration::from_millis(200)).await;
        cancel_clone.cancel();
    });

    let _ = cursor
        .next_event_recovering(&cancel, &test_task_supervisor())
        .await;

    // Check that backoff increased
    // The cursor should have retried at least once, doubling the backoff
    assert!(
        cursor.replay_backoff() >= INITIAL_REPLAY_BACKOFF * 2,
        "backoff should have increased to at least {:?}, was {:?}",
        INITIAL_REPLAY_BACKOFF * 2,
        cursor.replay_backoff()
    );
}
