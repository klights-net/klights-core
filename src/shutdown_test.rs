#[cfg(test)]
mod tests {
    use tokio::time::Duration;

    /// Test that spawned tasks can be cancelled via CancellationToken
    #[tokio::test]
    async fn test_cancellation_token_stops_infinite_loop() {
        let cancel_token = tokio_util::sync::CancellationToken::new();
        let cancel_clone = cancel_token.clone();

        // Spawn a task with an infinite loop (like heartbeat)
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(10));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Simulate work
                    }
                    _ = cancel_clone.cancelled() => {
                        // Shutdown signal received
                        break;
                    }
                }
            }
        });

        // Let it run for a bit
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Cancel the task
        cancel_token.cancel();

        // Task should complete within 100ms
        let result = tokio::time::timeout(Duration::from_millis(100), handle).await;
        assert!(result.is_ok(), "Task should complete when cancelled");
    }

    /// Test that multiple tasks can be cancelled simultaneously
    #[tokio::test]
    async fn test_multiple_tasks_cancel() {
        let cancel_token = tokio_util::sync::CancellationToken::new();

        // Spawn multiple long-running tasks
        let mut handles = vec![];
        for _ in 0..3 {
            let cancel_clone = cancel_token.clone();
            let handle = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(10)) => {}
                        _ = cancel_clone.cancelled() => {
                            break;
                        }
                    }
                }
            });
            handles.push(handle);
        }

        // Cancel all tasks
        cancel_token.cancel();

        // All tasks should complete within 100ms
        for handle in handles {
            let result = tokio::time::timeout(Duration::from_millis(100), handle).await;
            assert!(result.is_ok(), "All tasks should complete when cancelled");
        }
    }
}
