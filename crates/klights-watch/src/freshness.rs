use std::time::Duration;

use klights_cluster_store::DurableAllocatorRead;
use klights_supervisor::TaskSupervisor;

use crate::{WatchSignalReceiveError, WatchSignalSubscribe, WatchTopic};

/// Waits for a local watch source to reach the requested public resource
/// version. The subscription is established before the initial allocator read
/// so an intervening committed update cannot be lost.
pub async fn wait_until_resource_version_fresh(
    allocator_reads: &dyn DurableAllocatorRead,
    signals: &dyn WatchSignalSubscribe,
    target_rv: i64,
    topic: WatchTopic,
    timeout: Duration,
    task_supervisor: &TaskSupervisor,
) {
    if target_rv <= 0 {
        return;
    }
    let mut fresh_rx = signals.subscribe(topic);
    if current_resource_version(allocator_reads).await >= target_rv {
        return;
    }
    let sleep = task_supervisor.sleep("watch_read_freshness_wait", timeout);
    tokio::pin!(sleep);
    loop {
        tokio::select! {
            _ = &mut sleep => return,
            recv = fresh_rx.recv() => match recv {
                Ok(signal) if signal.advances.iter().any(|advance| advance.high_rv >= target_rv) => return,
                Ok(_) => {}
                Err(WatchSignalReceiveError::Lagged(_)) if current_resource_version(allocator_reads).await >= target_rv => return,
                Err(WatchSignalReceiveError::Lagged(_)) | Err(WatchSignalReceiveError::Closed) => return,
            },
        }
    }
}

async fn current_resource_version(allocator_reads: &dyn DurableAllocatorRead) -> i64 {
    allocator_reads
        .read_allocator_state()
        .await
        .map(|state| state.position().resource_version)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use klights_cluster_core::WatchReplayPosition;
    use klights_cluster_store::{
        AllocatorStateFuture, DurableAllocatorRead, DurableAllocatorState,
    };
    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};

    use super::*;
    use crate::{WatchAdvance, WatchSignal, WatchSignalHub};

    struct Allocator(i64);
    impl DurableAllocatorRead for Allocator {
        fn read_allocator_state(&self) -> AllocatorStateFuture<'_, DurableAllocatorState> {
            Box::pin(async move {
                DurableAllocatorState::try_new(
                    WatchReplayPosition::from_resource_version_through_event_id(self.0, self.0),
                )
                .map_err(|error| {
                    klights_cluster_store::AllocatorStateError::CorruptData {
                        message: error.to_string(),
                    }
                })
            })
        }
    }

    fn supervisor() -> Arc<TaskSupervisor> {
        Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()))
    }

    #[tokio::test]
    async fn returns_immediately_for_fresh_or_nonpositive_positions() {
        let supervisor = supervisor();
        let signals = WatchSignalHub::new(1);
        wait_until_resource_version_fresh(
            &Allocator(8),
            &signals,
            8,
            WatchTopic::new("v1", "Pod"),
            Duration::from_secs(1),
            supervisor.as_ref(),
        )
        .await;
        wait_until_resource_version_fresh(
            &Allocator(0),
            &signals,
            0,
            WatchTopic::new("v1", "Pod"),
            Duration::from_secs(1),
            supervisor.as_ref(),
        )
        .await;
        let _ = supervisor.shutdown(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn stale_wait_returns_for_matching_signal_and_closed_source() {
        let supervisor = supervisor();
        let signals = Arc::new(WatchSignalHub::new(1));
        let topic = WatchTopic::new("v1", "Pod");
        let waiter = {
            let signals = signals.clone();
            let topic = topic.clone();
            let supervisor = supervisor.clone();
            tokio::spawn(async move {
                wait_until_resource_version_fresh(
                    &Allocator(1),
                    signals.as_ref(),
                    2,
                    topic,
                    Duration::from_secs(1),
                    supervisor.as_ref(),
                )
                .await
            })
        };
        tokio::task::yield_now().await;
        signals.publish(WatchSignal {
            topic,
            advances: vec![WatchAdvance {
                namespace: None,
                low_rv: 2,
                high_rv: 2,
            }],
        });
        waiter.await.expect("matching signal wakes waiter");
        struct Closed;
        impl WatchSignalSubscribe for Closed {
            fn subscribe(&self, _: WatchTopic) -> crate::WatchSignalReceiver {
                crate::WatchSignalReceiver::closed()
            }
        }
        wait_until_resource_version_fresh(
            &Allocator(1),
            &Closed,
            2,
            WatchTopic::new("v1", "Pod"),
            Duration::from_secs(1),
            supervisor.as_ref(),
        )
        .await;
        let _ = supervisor.shutdown(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn stale_wait_returns_when_timeout_or_shutdown_completes_sleep() {
        let supervisor = supervisor();
        let signals = WatchSignalHub::new(1);
        wait_until_resource_version_fresh(
            &Allocator(1),
            &signals,
            2,
            WatchTopic::new("v1", "Pod"),
            Duration::ZERO,
            supervisor.as_ref(),
        )
        .await;
        let _ = supervisor.shutdown(Duration::from_secs(1)).await;
    }
}
