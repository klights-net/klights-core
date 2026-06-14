//! Centralized garbage-collection scheduler primitive.
//!
//! GC tasks are grouped by cadence behind supervised schedulers. Concentrating
//! periodic timers behind this primitive keeps idle CPU and timer auditing in
//! one place and prevents tasks in the same cadence from racing for SQLite/CRI
//! bandwidth.

use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

pub mod applied_outbox_gc;
pub mod sandbox_gc;
pub mod watch_events_gc;

#[async_trait]
pub trait GcTask: Send + Sync {
    fn name(&self) -> &'static str;
    async fn run(&self) -> Result<()>;
}

pub struct GcScheduler {
    tasks: Vec<Arc<dyn GcTask>>,
    interval: Duration,
}

impl GcScheduler {
    pub fn new(interval: Duration) -> Self {
        Self {
            tasks: Vec::new(),
            interval,
        }
    }

    pub fn register(&mut self, task: Arc<dyn GcTask>) {
        self.tasks.push(task);
    }

    /// Drives the scheduler. Each interval tick runs every registered task
    /// to completion in registration order, so concurrent GC workloads never
    /// compete for CPU/IO at the same instant.
    pub async fn run(
        self,
        task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
        cancel: tokio_util::sync::CancellationToken,
    ) {
        let (tick_tx, mut tick_rx) = tokio::sync::mpsc::channel::<()>(1);
        // JUSTIFY: external-state drift sweep for surfaces that don't emit events
        // (sandbox GC, watch_events GC, hourly applied_outbox GC). No producer
        // to subscribe to.
        if let Err(err) = task_supervisor
            .spawn_interval("gc_scheduler_tick", self.interval, move |tick| {
                let tick_tx = tick_tx.clone();
                async move {
                    // Skip the first immediate tick — first sweep happens after one full interval.
                    if tick == 0 {
                        return;
                    }
                    let _ = tick_tx.send(()).await;
                }
            })
            .await
        {
            tracing::warn!("Failed to spawn GC scheduler timer: {}", err);
            return;
        }

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!("GcScheduler shutting down");
                    return;
                }
                Some(()) = tick_rx.recv() => {
                    for task in &self.tasks {
                        let name = task.name();
                        let started = std::time::Instant::now();
                        match task.run().await {
                            Ok(()) => tracing::debug!(
                                gc_task = %name,
                                elapsed_ms = started.elapsed().as_millis() as u64,
                                "GC task completed"
                            ),
                            Err(e) => tracing::warn!(
                                gc_task = %name,
                                error = %e,
                                "GC task failed"
                            ),
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingTask {
        name: &'static str,
        count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl GcTask for CountingTask {
        fn name(&self) -> &'static str {
            self.name
        }
        async fn run(&self) -> Result<()> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct FailingTask;

    #[async_trait]
    impl GcTask for FailingTask {
        fn name(&self) -> &'static str {
            "failing"
        }
        async fn run(&self) -> Result<()> {
            Err(anyhow::anyhow!("synthetic failure"))
        }
    }

    #[tokio::test]
    async fn scheduler_runs_each_registered_task_per_tick() {
        let count_a = Arc::new(AtomicUsize::new(0));
        let count_b = Arc::new(AtomicUsize::new(0));
        let mut scheduler = GcScheduler::new(Duration::from_millis(50));
        scheduler.register(Arc::new(CountingTask {
            name: "a",
            count: count_a.clone(),
        }));
        scheduler.register(Arc::new(CountingTask {
            name: "b",
            count: count_b.clone(),
        }));

        let cancel = tokio_util::sync::CancellationToken::new();
        let cancel_for_run = cancel.clone();
        let task_supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let handle =
            tokio::spawn(async move { scheduler.run(task_supervisor, cancel_for_run).await });

        tokio::time::sleep(Duration::from_millis(180)).await;
        cancel.cancel();
        handle.await.unwrap();

        let a = count_a.load(Ordering::SeqCst);
        let b = count_b.load(Ordering::SeqCst);
        assert!(
            a >= 2,
            "task a should have ticked at least twice, got {}",
            a
        );
        assert_eq!(a, b, "both tasks must run on every tick");
    }

    #[tokio::test]
    async fn applied_outbox_gc_prunes_all_rows_older_than_twelve_hours() {
        let db = std::sync::Arc::new(crate::datastore::test_support::in_memory().await);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let expired_ms = now_ms - applied_outbox_gc::APPLIED_OUTBOX_GC_TTL_MS - 1;
        let recent_ms = now_ms - applied_outbox_gc::APPLIED_OUTBOX_GC_TTL_MS + 1;

        db.insert_applied_outbox(crate::datastore::AppliedOutboxRecord {
            idempotency_key: "expired-pod-status".to_string(),
            subject_key: "v1/Pod/default/expired/uid-expired".to_string(),
            operation: "PodStatus".to_string(),
            first_seen_ms: expired_ms,
            applied_rv: Some(1),
            result_proto: vec![],
        })
        .await
        .unwrap();
        db.insert_applied_outbox(crate::datastore::AppliedOutboxRecord {
            idempotency_key: "recent-pod-status".to_string(),
            subject_key: "v1/Pod/default/recent/uid-recent".to_string(),
            operation: "PodStatus".to_string(),
            first_seen_ms: recent_ms,
            applied_rv: Some(2),
            result_proto: vec![],
        })
        .await
        .unwrap();
        db.insert_applied_outbox(crate::datastore::AppliedOutboxRecord {
            idempotency_key: "expired-event-create".to_string(),
            subject_key: "events.k8s.io/v1/Event/default/event/uid-event".to_string(),
            operation: "EventCreate".to_string(),
            first_seen_ms: expired_ms,
            applied_rv: Some(3),
            result_proto: vec![],
        })
        .await
        .unwrap();

        let task = applied_outbox_gc::AppliedOutboxGc::new(db.clone());
        task.run().await.unwrap();

        assert!(
            db.get_applied_outbox("expired-pod-status")
                .await
                .unwrap()
                .is_none(),
            "rows older than the twelve-hour global TTL must be pruned"
        );
        assert!(
            db.get_applied_outbox("recent-pod-status")
                .await
                .unwrap()
                .is_some(),
            "recent rows must stay inside the twelve-hour TTL"
        );
        assert!(
            db.get_applied_outbox("expired-event-create")
                .await
                .unwrap()
                .is_none(),
            "EventCreate rows older than the twelve-hour TTL must be pruned"
        );
    }

    #[tokio::test]
    async fn scheduler_continues_after_task_failure() {
        let count = Arc::new(AtomicUsize::new(0));
        let mut scheduler = GcScheduler::new(Duration::from_millis(50));
        scheduler.register(Arc::new(FailingTask));
        scheduler.register(Arc::new(CountingTask {
            name: "after_fail",
            count: count.clone(),
        }));

        let cancel = tokio_util::sync::CancellationToken::new();
        let cancel_for_run = cancel.clone();
        let task_supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let handle =
            tokio::spawn(async move { scheduler.run(task_supervisor, cancel_for_run).await });

        tokio::time::sleep(Duration::from_millis(150)).await;
        cancel.cancel();
        handle.await.unwrap();

        assert!(
            count.load(Ordering::SeqCst) >= 1,
            "task following a failing task must still run"
        );
    }

    #[tokio::test]
    async fn scheduler_cancellation_returns_promptly() {
        let scheduler = GcScheduler::new(Duration::from_secs(60));
        let cancel = tokio_util::sync::CancellationToken::new();
        let cancel_for_run = cancel.clone();
        let task_supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let handle =
            tokio::spawn(async move { scheduler.run(task_supervisor, cancel_for_run).await });

        tokio::time::sleep(Duration::from_millis(20)).await;
        cancel.cancel();
        let res = tokio::time::timeout(Duration::from_secs(1), handle).await;
        assert!(res.is_ok(), "scheduler must stop within 1s of cancellation");
    }
}
