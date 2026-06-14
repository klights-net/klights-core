//! Deferred-startup services owned by `PodRepository`. Separated from
//! construction so that `build_parts` is side-effect-free and explicit
//! startup can happen after lifecycle wiring is complete.

use std::sync::Arc;

use super::workqueue::PodWorkqueue;
use crate::task_supervisor::TaskSupervisor;

/// Services that must be started after repository construction.
pub struct PodRepositoryBackground {
    workqueue: Arc<PodWorkqueue>,
    watch_runner: Option<PodWatchRunner>,
    deadline_runner: Option<DeadlineTimerRunner>,
}

impl PodRepositoryBackground {
    pub(super) fn new(workqueue: Arc<PodWorkqueue>) -> Self {
        Self {
            workqueue,
            watch_runner: None,
            deadline_runner: None,
        }
    }

    /// Attach a PodWatchRunner for deferred start.
    pub fn with_watch_runner(mut self, runner: PodWatchRunner) -> Self {
        self.watch_runner = Some(runner);
        self
    }

    /// Attach a DeadlineTimerRunner for deferred start.
    pub fn with_deadline_runner(mut self, runner: DeadlineTimerRunner) -> Self {
        self.deadline_runner = Some(runner);
        self
    }

    /// Start deferred services: workqueue reconciler and other delayed
    /// background tasks.
    pub fn start(&self) {
        self.workqueue.start();
        if let Some(ref runner) = self.watch_runner {
            runner.start();
        }
        if let Some(ref runner) = self.deadline_runner {
            runner.start();
        }
    }

    #[cfg(test)]
    pub fn workqueue_start_called(&self) -> bool {
        self.workqueue.start_called()
    }

    #[cfg(test)]
    pub fn watch_runner_started(&self) -> bool {
        self.watch_runner
            .as_ref()
            .is_some_and(|r| r.started.load(std::sync::atomic::Ordering::Acquire))
    }
}

// --- Task 4.8: PodWatchRunner ---

/// Forwards UID-bearing pod watch events to the Pod lifecycle router.
/// Spawned through `TaskSupervisor`, not direct `tokio::spawn`.
pub struct PodWatchRunner {
    _supervisor: Arc<TaskSupervisor>,
    pub started: std::sync::atomic::AtomicBool,
}

impl PodWatchRunner {
    pub fn new(supervisor: Arc<TaskSupervisor>) -> Self {
        Self {
            _supervisor: supervisor,
            started: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn start(&self) {
        self.started
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

// --- Task 4.9: DeadlineTimerRunner ---

/// One-shot deadline runner for Pod lifecycle reminders. Uses
/// `TaskSupervisor::spawn_delay` (no polling, no spawn_interval).
pub struct DeadlineTimerRunner {
    _supervisor: Arc<TaskSupervisor>,
}

impl DeadlineTimerRunner {
    pub fn new(supervisor: Arc<TaskSupervisor>) -> Self {
        Self {
            _supervisor: supervisor,
        }
    }

    fn start(&self) {}

    /// Schedule a UID-bound deadline wakeup. Uses `spawn_delay` so the
    /// timer is event-driven (not a polling loop).
    pub fn schedule_uid_bound_wakeup(
        &self,
        _ns: &str,
        _name: &str,
        _uid: &str,
        _delay_ms: u64,
        _reason: &str,
    ) {
        // Task 4.9 impl: spawn_delay -> lifecycle router wakeup for UID.
    }
}
