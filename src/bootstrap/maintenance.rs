//! Private root composition for periodic external-state maintenance.
//!
//! Feature behavior stays with its canonical owner: kubelet owns sandbox
//! cleanup and cluster-store owns watch-history retention. Root only composes
//! those focused capabilities with supervised
//! timer lifecycle.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_store::ClusterWatchMaintenance;
use tokio_util::sync::CancellationToken;

const WATCH_EVENT_BATCH_CAP: i64 = 5_000;

#[async_trait]
trait SandboxMaintenance: Send + Sync {
    async fn run_if_dirty(&self) -> Result<()>;
}

#[async_trait]
impl SandboxMaintenance for klights_kubelet::sandbox_gc::SandboxGc {
    async fn run_if_dirty(&self) -> Result<()> {
        self.run_if_dirty().await
    }
}

pub(crate) struct MaintenanceRunner {
    watch: Arc<dyn ClusterWatchMaintenance>,
    sandbox: Option<Arc<dyn SandboxMaintenance>>,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
    interval: Duration,
    max_watch_events: i64,
}

impl MaintenanceRunner {
    pub(crate) fn new(
        watch: Arc<dyn ClusterWatchMaintenance>,
        sandbox: Option<Arc<klights_kubelet::sandbox_gc::SandboxGc>>,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
        interval: Duration,
        max_watch_events: i64,
    ) -> Result<Self> {
        anyhow::ensure!(
            max_watch_events > 0,
            "watch event retention limit must be positive"
        );
        let sandbox = sandbox.map(|sandbox| -> Arc<dyn SandboxMaintenance> { sandbox });
        Ok(Self {
            watch,
            sandbox,
            supervisor,
            interval,
            max_watch_events,
        })
    }

    pub(crate) async fn run(self, cancel: CancellationToken) {
        let (tick_tx, tick_rx) = tokio::sync::mpsc::channel::<()>(1);
        // JUSTIFY: CRI sandbox state and retained durable watch history have no
        // shared event source that can prove external-state drift is absent.
        let timer = match self
            .supervisor
            .spawn_interval("root_maintenance_tick", self.interval, move |tick| {
                let tick_tx = tick_tx.clone();
                async move {
                    // Tokio intervals tick immediately; retain the established
                    // contract that the first sweep follows one full cadence.
                    if tick > 0 {
                        let _ = tick_tx.send(()).await;
                    }
                }
            })
            .await
        {
            Ok(timer) => timer,
            Err(error) => {
                tracing::warn!(%error, "failed to start root maintenance timer");
                return;
            }
        };

        self.run_with_ticks(tick_rx, cancel).await;
        // A leader lease can be lost without shutting down the application
        // supervisor. Abort and join the cadence task so reacquisition cannot
        // accumulate detached interval producers.
        timer.abort();
        let _ = timer.join().await;
    }

    async fn run_with_ticks(
        &self,
        mut ticks: tokio::sync::mpsc::Receiver<()>,
        cancel: CancellationToken,
    ) {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                tick = ticks.recv() => {
                    let Some(()) = tick else { return; };
                    self.run_sweeps().await;
                }
            }
        }
    }

    async fn run_sweeps(&self) {
        if let Some(sandbox) = &self.sandbox {
            let started = Instant::now();
            match sandbox.run_if_dirty().await {
                Ok(()) => tracing::debug!(
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "sandbox maintenance completed"
                ),
                Err(error) => tracing::warn!(%error, "sandbox maintenance failed"),
            }
        }

        let started = Instant::now();
        match ClusterWatchMaintenance::gc_watch_events(
            self.watch.as_ref(),
            self.max_watch_events,
            WATCH_EVENT_BATCH_CAP,
        )
        .await
        {
            Ok(removed) => tracing::debug!(
                removed,
                max_rows = self.max_watch_events,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "watch-history maintenance completed"
            ),
            Err(error) => tracing::warn!(%error, "watch-history maintenance failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

    use super::*;

    struct RecordingWatchMaintenance {
        sweeps: AtomicUsize,
        last_max_rows: AtomicI64,
        last_batch_cap: AtomicI64,
        called: tokio::sync::Notify,
    }

    impl RecordingWatchMaintenance {
        fn new() -> Self {
            Self {
                sweeps: AtomicUsize::new(0),
                last_max_rows: AtomicI64::new(0),
                last_batch_cap: AtomicI64::new(0),
                called: tokio::sync::Notify::new(),
            }
        }
    }

    #[async_trait]
    impl ClusterWatchMaintenance for RecordingWatchMaintenance {
        async fn advance_resource_version_after(
            &self,
            min_rv: i64,
        ) -> klights_cluster_store::ClusterStoreResult<i64> {
            Ok(min_rv)
        }

        async fn watch_events_gc_prunable_count(
            &self,
            _max_rows: i64,
            _batch_cap: i64,
        ) -> klights_cluster_store::ClusterStoreResult<usize> {
            Ok(0)
        }

        async fn gc_watch_events(
            &self,
            max_rows: i64,
            batch_cap: i64,
        ) -> klights_cluster_store::ClusterStoreResult<usize> {
            self.sweeps.fetch_add(1, Ordering::SeqCst);
            self.last_max_rows.store(max_rows, Ordering::SeqCst);
            self.last_batch_cap.store(batch_cap, Ordering::SeqCst);
            self.called.notify_one();
            Ok(0)
        }
    }

    struct FailingSandbox;

    #[async_trait]
    impl SandboxMaintenance for FailingSandbox {
        async fn run_if_dirty(&self) -> Result<()> {
            anyhow::bail!("synthetic sandbox failure")
        }
    }

    fn test_runner(
        watch: Arc<RecordingWatchMaintenance>,
        sandbox: Option<Arc<dyn SandboxMaintenance>>,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> MaintenanceRunner {
        MaintenanceRunner {
            watch,
            sandbox,
            supervisor,
            interval: Duration::from_millis(50),
            max_watch_events: 100,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn lease_reacquisition_does_not_retain_duplicate_interval_callbacks() {
        let watch = Arc::new(RecordingWatchMaintenance::new());
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));

        let cancel = CancellationToken::new();
        let first_call = watch.called.notified();
        let first =
            tokio::spawn(test_runner(watch.clone(), None, supervisor.clone()).run(cancel.clone()));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(50)).await;
        first_call.await;
        cancel.cancel();
        first.await.unwrap();
        assert_eq!(watch.sweeps.load(Ordering::SeqCst), 1);
        assert_eq!(watch.last_max_rows.load(Ordering::SeqCst), 100);
        assert_eq!(
            watch.last_batch_cap.load(Ordering::SeqCst),
            WATCH_EVENT_BATCH_CAP,
            "each cadence must request exactly one bounded retention batch"
        );

        let no_detached_call = watch.called.notified();
        tokio::time::advance(Duration::from_millis(150)).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(1), no_detached_call)
                .await
                .is_err(),
            "lease loss must abort the old interval producer"
        );

        let reacquired_cancel = CancellationToken::new();
        let reacquired_call = watch.called.notified();
        let reacquired = tokio::spawn(
            test_runner(watch.clone(), None, supervisor).run(reacquired_cancel.clone()),
        );
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(50)).await;
        reacquired_call.await;
        reacquired_cancel.cancel();
        reacquired.await.unwrap();
        assert_eq!(watch.sweeps.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn maintenance_rejects_nonpositive_watch_retention() {
        let watch = Arc::new(RecordingWatchMaintenance::new());
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));

        assert!(
            MaintenanceRunner::new(
                watch.clone(),
                None,
                supervisor.clone(),
                Duration::from_secs(1),
                0,
            )
            .is_err()
        );
        assert!(
            MaintenanceRunner::new(watch, None, supervisor, Duration::from_secs(1), -1).is_err()
        );
    }

    #[tokio::test]
    async fn watch_maintenance_continues_after_sandbox_failure() {
        let watch = Arc::new(RecordingWatchMaintenance::new());
        let runner = test_runner(
            watch.clone(),
            Some(Arc::new(FailingSandbox)),
            Arc::new(klights_supervisor::TaskSupervisor::new(
                klights_supervisor::TaskCategoryConfig::default(),
            )),
        );

        runner.run_sweeps().await;

        assert_eq!(watch.sweeps.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn maintenance_cancellation_returns_promptly_without_a_tick() {
        let watch = Arc::new(RecordingWatchMaintenance::new());
        let runner = test_runner(
            watch,
            None,
            Arc::new(klights_supervisor::TaskSupervisor::new(
                klights_supervisor::TaskCategoryConfig::default(),
            )),
        );
        let (_tick_tx, tick_rx) = tokio::sync::mpsc::channel(1);
        let cancel = CancellationToken::new();
        cancel.cancel();

        tokio::time::timeout(
            Duration::from_secs(1),
            runner.run_with_ticks(tick_rx, cancel),
        )
        .await
        .expect("cancelled maintenance must return promptly");
    }
}
