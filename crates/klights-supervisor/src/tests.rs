use crate::supervisor::{ProcessError, ProcessShutdownPolicy};
use crate::{TaskAdmissionError, TaskCategory, TaskCategoryConfig, TaskOutcome, TaskSupervisor};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::Poll;
use std::time::{Duration, Instant};
use tracing::field::{Field, Visit};
use tracing::subscriber::Interest;
use tracing::{Event, Id, Metadata, Subscriber};

#[test]
fn defaults_match_p0_category_limits() {
    let cfg = TaskCategoryConfig::default();
    assert_eq!(cfg.background, 0);
    assert_eq!(cfg.file, 3);
    assert_eq!(cfg.db, 1);
    assert_eq!(cfg.db_read, 0);
    assert_eq!(cfg.timer, 0);
    assert_eq!(cfg.network, 256);
    assert_eq!(cfg.pod_delete_workqueue, 10);
    assert_eq!(cfg.pod_lifecycle_actor, 0);
    assert_eq!(cfg.pod_lifecycle_work, 16);
    assert_eq!(cfg.pod_probe, 64);
    assert_eq!(cfg.others, 0);
}

#[test]
fn task_category_serializes_to_kebab_case() {
    assert_eq!(
        serde_json::to_string(&TaskCategory::Background).unwrap(),
        "\"background\""
    );
    assert_eq!(
        serde_json::to_string(&TaskCategory::File).unwrap(),
        "\"file\""
    );
    assert_eq!(serde_json::to_string(&TaskCategory::Db).unwrap(), "\"db\"");
    assert_eq!(
        serde_json::to_string(&TaskCategory::DbRead).unwrap(),
        "\"db-read\""
    );
    assert_eq!(
        serde_json::to_string(&TaskCategory::Timer).unwrap(),
        "\"timer\""
    );
    assert_eq!(
        serde_json::to_string(&TaskCategory::Network).unwrap(),
        "\"network\""
    );
    assert_eq!(
        serde_json::to_string(&TaskCategory::PodDeleteWorkqueue).unwrap(),
        "\"pod-delete-workqueue\""
    );
    assert_eq!(
        serde_json::to_string(&TaskCategory::PodLifecycleActor).unwrap(),
        "\"pod-lifecycle-actor\""
    );
    assert_eq!(
        serde_json::to_string(&TaskCategory::PodLifecycleWork).unwrap(),
        "\"pod-lifecycle-work\""
    );
    assert_eq!(
        serde_json::to_string(&TaskCategory::PodProbe).unwrap(),
        "\"pod-probe\""
    );
    assert_eq!(
        serde_json::to_string(&TaskCategory::Others).unwrap(),
        "\"others\""
    );
}

#[test]
fn semaphore_presence_matches_category_limits() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());

    assert_eq!(supervisor.semaphore_limit(TaskCategory::Background), None);
    assert_eq!(supervisor.semaphore_limit(TaskCategory::Timer), None);
    assert_eq!(supervisor.semaphore_limit(TaskCategory::Network), Some(256));
    assert_eq!(
        supervisor.semaphore_limit(TaskCategory::PodDeleteWorkqueue),
        Some(10)
    );
    assert_eq!(
        supervisor.semaphore_limit(TaskCategory::PodLifecycleActor),
        None
    );
    assert_eq!(
        supervisor.semaphore_limit(TaskCategory::PodLifecycleWork),
        Some(16)
    );
    assert_eq!(supervisor.semaphore_limit(TaskCategory::PodProbe), Some(64));
    assert_eq!(supervisor.semaphore_limit(TaskCategory::Others), None);
    assert_eq!(supervisor.semaphore_limit(TaskCategory::File), Some(3));
    assert_eq!(supervisor.semaphore_limit(TaskCategory::Db), Some(1));
    assert_eq!(supervisor.semaphore_limit(TaskCategory::DbRead), None);
}

#[tokio::test]
async fn pod_delete_workqueue_limit_serializes_tasks() {
    // Construct a config with a small limit (2) so we can deterministically
    // observe queueing without spinning up 11 concurrent tasks.
    let cfg = TaskCategoryConfig {
        pod_delete_workqueue: 2,
        ..TaskCategoryConfig::default()
    };
    let supervisor = Arc::new(TaskSupervisor::new(cfg));
    let started = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new((Mutex::new(0usize), Condvar::new()));

    let mut joins = Vec::new();
    for index in 0..4 {
        let supervisor = supervisor.clone();
        let started = started.clone();
        let gate = gate.clone();
        joins.push(tokio::spawn(async move {
            supervisor
                .run_blocking(
                    TaskCategory::PodDeleteWorkqueue,
                    format!("pdwq-{index}"),
                    move || {
                        started.fetch_add(1, Ordering::SeqCst);
                        wait_on_gate(&gate);
                    },
                )
                .await
                .unwrap();
        }));
    }

    wait_for(
        || started.load(Ordering::SeqCst) == 2,
        Duration::from_secs(2),
    )
    .await;
    assert_eq!(
        category_status(&supervisor, TaskCategory::PodDeleteWorkqueue).queued,
        2
    );

    release_gate(&gate, 4);
    for join in joins {
        join.await.unwrap();
    }
}

#[tokio::test]
async fn category_free_notify_fires_when_slot_releases() {
    let cfg = TaskCategoryConfig {
        background: 1,
        ..TaskCategoryConfig::default()
    };
    let supervisor = Arc::new(TaskSupervisor::new(cfg));

    let gate = Arc::new((Mutex::new(0usize), Condvar::new()));
    let started = Arc::new(AtomicUsize::new(0));

    let task = {
        let supervisor = supervisor.clone();
        let gate = gate.clone();
        let started = started.clone();
        tokio::spawn(async move {
            supervisor
                .run_blocking(TaskCategory::Background, "hold-bg", move || {
                    started.store(1, Ordering::SeqCst);
                    wait_on_gate(&gate);
                })
                .await
                .unwrap();
        })
    };

    wait_for(
        || started.load(Ordering::SeqCst) == 1,
        Duration::from_secs(2),
    )
    .await;
    assert!(!supervisor.is_category_free(TaskCategory::Background));

    let free = supervisor.category_free_notify(TaskCategory::Background);
    release_gate(&gate, 1);
    tokio::time::timeout(Duration::from_secs(2), free.notified())
        .await
        .expect("free-slot notify must fire when permit releases");
    task.await.unwrap();
    assert!(supervisor.is_category_free(TaskCategory::Background));
}

#[tokio::test]
async fn active_task_tracking_adds_and_removes_entries() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    assert!(supervisor.active_tasks(None).is_empty());
    let release_a = Arc::new(tokio::sync::Notify::new());
    let release_b = Arc::new(tokio::sync::Notify::new());
    let wait_a = release_a.clone();
    let wait_b = release_b.clone();
    let guard_a = supervisor
        .spawn_async(TaskCategory::Background, "worker-a", async move {
            wait_a.notified().await
        })
        .await
        .unwrap();
    let guard_b = supervisor
        .spawn_async(TaskCategory::File, "render-volume", async move {
            wait_b.notified().await
        })
        .await
        .unwrap();

    let all = supervisor.active_tasks(None);
    assert_eq!(all.len(), 2);
    assert_eq!(
        supervisor.active_tasks(Some(TaskCategory::File))[0].name,
        "render-volume"
    );

    release_b.notify_one();
    guard_b.join().await.unwrap();
    let all_after = supervisor.active_tasks(None);
    assert_eq!(all_after.len(), 1);
    assert_eq!(all_after[0].name, "worker-a");
    release_a.notify_one();
    guard_a.join().await.unwrap();
}

#[tokio::test]
async fn file_limit_queues_fourth_blocking_task() {
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let started = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new((Mutex::new(0usize), Condvar::new()));

    let mut joins = Vec::new();
    for index in 0..4 {
        let supervisor = supervisor.clone();
        let started = started.clone();
        let gate = gate.clone();
        joins.push(tokio::spawn(async move {
            supervisor
                .run_blocking_file(format!("file-task-{index}"), move || {
                    started.fetch_add(1, Ordering::SeqCst);
                    wait_on_gate(&gate);
                })
                .await
                .unwrap();
        }));
    }

    wait_for(
        || started.load(Ordering::SeqCst) == 3,
        Duration::from_secs(2),
    )
    .await;

    let file_status = category_status(&supervisor, TaskCategory::File);
    assert_eq!(file_status.queued, 1);

    release_gate(&gate, 1);
    wait_for(
        || started.load(Ordering::SeqCst) == 4,
        Duration::from_secs(2),
    )
    .await;

    release_gate(&gate, 3);
    for join in joins {
        join.await.unwrap();
    }
}

#[tokio::test]
async fn unlimited_category_does_not_queue() {
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let gate = Arc::new((Mutex::new(0usize), Condvar::new()));
    let started = Arc::new(AtomicUsize::new(0));

    let mut joins = Vec::new();
    for index in 0..6 {
        let supervisor = supervisor.clone();
        let started = started.clone();
        let gate = gate.clone();
        joins.push(tokio::spawn(async move {
            supervisor
                .run_blocking(TaskCategory::Background, format!("bg-{index}"), move || {
                    started.fetch_add(1, Ordering::SeqCst);
                    wait_on_gate(&gate);
                })
                .await
                .unwrap();
        }));
    }

    wait_for(
        || started.load(Ordering::SeqCst) >= 1,
        Duration::from_secs(2),
    )
    .await;

    let status = category_status(&supervisor, TaskCategory::Background);
    assert_eq!(status.limit, 0);
    assert_eq!(status.queued, 0);

    release_gate(&gate, 6);
    for join in joins {
        join.await.unwrap();
    }
}

#[tokio::test]
async fn same_key_file_tasks_serialize() {
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let gate = Arc::new((Mutex::new(0usize), Condvar::new()));
    let running_same_key = Arc::new(AtomicUsize::new(0));
    let max_running_same_key = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(AtomicUsize::new(0));

    let mut joins = Vec::new();
    for index in 0..2 {
        let supervisor = supervisor.clone();
        let gate = gate.clone();
        let running_same_key = running_same_key.clone();
        let max_running_same_key = max_running_same_key.clone();
        let started = started.clone();
        joins.push(tokio::spawn(async move {
            supervisor
                .run_blocking_file_keyed(format!("same-key-{index}"), "volume/a", move || {
                    started.fetch_add(1, Ordering::SeqCst);
                    let current = running_same_key.fetch_add(1, Ordering::SeqCst) + 1;
                    update_max(&max_running_same_key, current);
                    wait_on_gate(&gate);
                    running_same_key.fetch_sub(1, Ordering::SeqCst);
                })
                .await
                .unwrap();
        }));
    }

    wait_for(
        || started.load(Ordering::SeqCst) == 1,
        Duration::from_secs(2),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(started.load(Ordering::SeqCst), 1);

    release_gate(&gate, 1);
    wait_for(
        || started.load(Ordering::SeqCst) == 2,
        Duration::from_secs(2),
    )
    .await;
    release_gate(&gate, 1);

    for join in joins {
        join.await.unwrap();
    }
    assert_eq!(max_running_same_key.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn same_key_waiters_do_not_occupy_global_file_permits() {
    let cfg = TaskCategoryConfig {
        file: 2,
        ..TaskCategoryConfig::default()
    };
    let supervisor = Arc::new(TaskSupervisor::new(cfg));
    let gate = Arc::new((Mutex::new(0usize), Condvar::new()));

    let started_same = Arc::new(AtomicUsize::new(0));
    let started_other = Arc::new(AtomicUsize::new(0));

    let task_same_a = {
        let supervisor = supervisor.clone();
        let gate = gate.clone();
        let started_same = started_same.clone();
        tokio::spawn(async move {
            supervisor
                .run_blocking_file_keyed("same-a", "volume/same", move || {
                    started_same.fetch_add(1, Ordering::SeqCst);
                    wait_on_gate(&gate);
                })
                .await
                .unwrap();
        })
    };

    wait_for(
        || started_same.load(Ordering::SeqCst) == 1,
        Duration::from_secs(2),
    )
    .await;

    let task_same_b = {
        let supervisor = supervisor.clone();
        let gate = gate.clone();
        let started_same = started_same.clone();
        tokio::spawn(async move {
            supervisor
                .run_blocking_file_keyed("same-b", "volume/same", move || {
                    started_same.fetch_add(1, Ordering::SeqCst);
                    wait_on_gate(&gate);
                })
                .await
                .unwrap();
        })
    };

    let task_other = {
        let supervisor = supervisor.clone();
        let gate = gate.clone();
        let started_other = started_other.clone();
        tokio::spawn(async move {
            supervisor
                .run_blocking_file_keyed("other", "volume/other", move || {
                    started_other.fetch_add(1, Ordering::SeqCst);
                    wait_on_gate(&gate);
                })
                .await
                .unwrap();
        })
    };

    wait_for(
        || started_other.load(Ordering::SeqCst) == 1,
        Duration::from_secs(2),
    )
    .await;

    release_gate(&gate, 3);
    task_same_a.await.unwrap();
    task_same_b.await.unwrap();
    task_other.await.unwrap();
}

fn category_status(
    supervisor: &TaskSupervisor,
    category: TaskCategory,
) -> crate::TaskCategoryStatus {
    supervisor
        .category_statuses()
        .into_iter()
        .find(|entry| entry.category == category)
        .expect("category status must exist")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_process_executor_uses_injected_supervisor_and_isolates_shutdown() {
    let first_supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let second_supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let first_executor = crate::FileProcessExecutor::new(first_supervisor.clone());
    let second_executor = crate::FileProcessExecutor::new(second_supervisor.clone());
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();

    let first_work = tokio::spawn(async move {
        first_executor
            .run_blocking_file("injected-file-executor", move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok::<_, anyhow::Error>(7)
            })
            .await
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("injected file work did not start");

    assert_eq!(
        category_status(&first_supervisor, TaskCategory::File).active,
        1
    );
    assert_eq!(
        category_status(&second_supervisor, TaskCategory::File).active,
        0
    );

    second_supervisor.shutdown(std::time::Duration::ZERO).await;
    assert!(
        second_executor
            .run_blocking_file("rejected-after-second-shutdown", || Ok::<_, anyhow::Error>(
                ()
            ))
            .await
            .is_err()
    );
    assert_eq!(
        category_status(&first_supervisor, TaskCategory::File).active,
        1,
        "shutting down another injected supervisor must not cancel this executor"
    );

    release_tx.send(()).unwrap();
    assert_eq!(first_work.await.unwrap().unwrap(), 7);
    assert_eq!(
        category_status(&first_supervisor, TaskCategory::File).active,
        0
    );
    first_supervisor.shutdown(std::time::Duration::ZERO).await;
}

fn wait_on_gate(gate: &Arc<(Mutex<usize>, Condvar)>) {
    let (lock, condvar) = &**gate;
    let mut permits = lock.lock().expect("gate lock poisoned");
    while *permits == 0 {
        permits = condvar.wait(permits).expect("gate wait poisoned");
    }
    *permits -= 1;
}

fn release_gate(gate: &Arc<(Mutex<usize>, Condvar)>, count: usize) {
    let (lock, condvar) = &**gate;
    let mut permits = lock.lock().expect("gate lock poisoned");
    *permits += count;
    condvar.notify_all();
}

fn update_max(max: &AtomicUsize, value: usize) {
    let _ = max.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
        if value > current { Some(value) } else { None }
    });
}

async fn wait_for<F>(predicate: F, timeout: Duration)
where
    F: Fn() -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        if predicate() {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "condition was not met before timeout"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn shutdown_root_cancellation_wakes_managed_tasks() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let token = supervisor.root_cancellation_token();
    let woke = Arc::new(AtomicUsize::new(0));
    let woke_for_task = woke.clone();

    let _task = supervisor
        .spawn_async(TaskCategory::Background, "cancellable", async move {
            token.cancelled().await;
            woke_for_task.store(1, Ordering::SeqCst);
        })
        .await
        .unwrap();

    let report = supervisor.shutdown(Duration::from_secs(1)).await;
    assert!(report.joined >= 1);
    assert_eq!(woke.load(Ordering::SeqCst), 1);
    assert!(supervisor.active_tasks(None).is_empty());
}

async fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
    std::future::poll_fn(move |context| Poll::Ready(future.as_mut().poll(context))).await
}

#[tokio::test(start_paused = true)]
async fn shutdown_completion_is_event_driven_without_timer_advance() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let cancellation = supervisor.root_cancellation_token();
    let cancellation_seen = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let cancellation_seen_for_task = cancellation_seen.clone();
    let release_for_task = release.clone();
    let task = supervisor
        .spawn_async(
            TaskCategory::Background,
            "event-driven-shutdown",
            async move {
                cancellation.cancelled().await;
                cancellation_seen_for_task.notify_one();
                release_for_task.notified().await;
            },
        )
        .await
        .unwrap();

    let started_at = tokio::time::Instant::now();
    let mut shutdown = Box::pin(supervisor.shutdown(Duration::from_secs(30)));
    assert!(matches!(poll_once(shutdown.as_mut()).await, Poll::Pending));
    cancellation_seen.notified().await;
    release.notify_one();
    task.join().await.unwrap();
    assert_eq!(tokio::time::Instant::now(), started_at);

    let Poll::Ready(report) = poll_once(shutdown.as_mut()).await else {
        panic!("shutdown waited for a polling timer after managed task completion");
    };
    assert_eq!(report.total_managed, 1);
    assert_eq!(report.joined, 1);
    assert_eq!(report.aborted, 0);
    assert!(!report.timed_out);
    assert_eq!(report.remaining_active, 0);
}

#[tokio::test]
async fn shutdown_joins_completed_tasks() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let handle = supervisor
        .spawn_async(TaskCategory::Background, "quick", async {})
        .await
        .unwrap();
    handle.join().await.unwrap();

    // After completion the ManagedTaskGuard drops, removing the entry from
    // the managed_tasks registry before shutdown observes it. So shutdown
    // sees zero managed tasks, not one — this is the new (correct) behavior:
    // the registry only tracks live work.
    let report = supervisor.shutdown(Duration::from_secs(1)).await;
    assert_eq!(report.total_managed, 0);
    assert_eq!(report.joined, 0);
    assert_eq!(report.aborted, 0);
    assert!(!report.timed_out);
}

#[tokio::test]
async fn shutdown_timeout_aborts_cancellation_ignoring_tasks() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let _task = supervisor
        .spawn_async(TaskCategory::Background, "ignores-cancel", async move {
            loop {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();

    let report = supervisor.shutdown(Duration::from_millis(100)).await;
    assert_eq!(report.total_managed, 1);
    assert_eq!(report.aborted, 1);
    assert!(report.timed_out);
}

#[tokio::test]
async fn shutdown_leaves_no_managed_tasks_active() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let token = supervisor.root_cancellation_token();
    let _task = supervisor
        .spawn_async(TaskCategory::Background, "worker", async move {
            token.cancelled().await;
        })
        .await
        .unwrap();

    let report = supervisor.shutdown(Duration::from_secs(1)).await;
    assert_eq!(report.remaining_active, 0);
    assert_eq!(supervisor.active_tasks(None).len(), 0);
    assert!(supervisor.active_tasks(None).is_empty());
}

#[tokio::test(start_paused = true)]
async fn spawn_after_shutdown_is_rejected_and_future_never_runs() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    supervisor.shutdown(Duration::ZERO).await;
    let ran = Arc::new(AtomicBool::new(false));
    let ran_in_task = ran.clone();

    let error = supervisor
        .spawn_async(TaskCategory::File, "late", async move {
            ran_in_task.store(true, Ordering::SeqCst);
        })
        .await
        .expect_err("shutdown must close task admission");

    assert_eq!(error, TaskAdmissionError::ShuttingDown);
    assert!(!ran.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn unlimited_category_cannot_start_after_shutdown() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    supervisor.shutdown(Duration::ZERO).await;

    let error = supervisor
        .spawn_async(TaskCategory::Background, "late-unlimited", async {})
        .await
        .expect_err("unlimited categories must share the admission gate");
    assert_eq!(error, TaskAdmissionError::ShuttingDown);
}

#[tokio::test(start_paused = true)]
async fn queued_spawn_is_rejected_when_shutdown_precedes_permit_release() {
    let config = TaskCategoryConfig {
        file: 1,
        ..TaskCategoryConfig::default()
    };
    let supervisor = Arc::new(TaskSupervisor::new(config));
    let release = Arc::new(tokio::sync::Notify::new());
    let release_in_task = release.clone();
    let first = supervisor
        .spawn_async(TaskCategory::File, "holder", async move {
            release_in_task.notified().await;
        })
        .await
        .unwrap();
    let ran = Arc::new(AtomicBool::new(false));
    let ran_in_task = ran.clone();
    let queued_supervisor = supervisor.clone();
    let queued = tokio::spawn(async move {
        queued_supervisor
            .spawn_async(TaskCategory::File, "queued", async move {
                ran_in_task.store(true, Ordering::SeqCst);
            })
            .await
    });
    tokio::task::yield_now().await;
    assert_eq!(category_status(&supervisor, TaskCategory::File).queued, 1);

    let report = supervisor.shutdown(Duration::ZERO).await;
    release.notify_one();
    let error = queued
        .await
        .unwrap()
        .expect_err("queued admission must close");
    assert_eq!(error, TaskAdmissionError::ShuttingDown);
    assert_eq!(category_status(&supervisor, TaskCategory::File).queued, 0);
    assert!(!ran.load(Ordering::SeqCst));
    assert_eq!(report.abort_confirmed, 1);
    assert!(first.join().await.unwrap_err().is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn queued_accounting_returns_to_zero_when_shutdown_cancels_admission() {
    let config = TaskCategoryConfig {
        file: 1,
        ..TaskCategoryConfig::default()
    };
    let supervisor = Arc::new(TaskSupervisor::new(config));
    let holder = supervisor
        .spawn_async(
            TaskCategory::File,
            "queued-counter-holder",
            std::future::pending::<()>(),
        )
        .await
        .unwrap();
    let queued_supervisor = supervisor.clone();
    let queued = tokio::spawn(async move {
        queued_supervisor
            .spawn_async(TaskCategory::File, "queued-counter-waiter", async {})
            .await
    });
    tokio::task::yield_now().await;
    assert_eq!(category_status(&supervisor, TaskCategory::File).queued, 1);

    supervisor.shutdown(Duration::ZERO).await;
    assert_eq!(
        queued.await.unwrap().unwrap_err(),
        TaskAdmissionError::ShuttingDown
    );
    assert_eq!(category_status(&supervisor, TaskCategory::File).queued, 0);
    assert!(holder.join().await.unwrap_err().is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn caller_and_shutdown_abort_are_attributed_exactly_once() {
    let caller = TaskSupervisor::new(TaskCategoryConfig::default());
    let caller_handle = caller
        .spawn_async(
            TaskCategory::Background,
            "caller-abort",
            std::future::pending::<()>(),
        )
        .await
        .unwrap();
    caller_handle.abort();
    assert!(caller_handle.join().await.unwrap_err().is_cancelled());
    assert_eq!(
        caller
            .recent_task_outcomes()
            .iter()
            .filter(
                |entry| entry.name == "caller-abort" && entry.outcome == TaskOutcome::CallerAborted
            )
            .count(),
        1
    );

    let shutdown = TaskSupervisor::new(TaskCategoryConfig::default());
    let _handle = shutdown
        .spawn_async(
            TaskCategory::Background,
            "shutdown-abort",
            std::future::pending::<()>(),
        )
        .await
        .unwrap();
    let report = shutdown.shutdown(Duration::ZERO).await;
    assert_eq!(report.aborted, 1);
    assert_eq!(report.abort_confirmed, 1);
    assert_eq!(
        shutdown
            .recent_task_outcomes()
            .iter()
            .filter(|entry| entry.name == "shutdown-abort"
                && entry.outcome == TaskOutcome::ShutdownAborted)
            .count(),
        1
    );
}

#[tokio::test(start_paused = true)]
async fn registration_and_shutdown_snapshot_are_linearizable() {
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let task_supervisor = supervisor.clone();
    let handle = supervisor
        .spawn_async(
            TaskCategory::Background,
            "registered-before-run",
            async move {
                assert!(
                    task_supervisor
                        .active_tasks(None)
                        .iter()
                        .any(|task| task.name == "registered-before-run")
                );
            },
        )
        .await
        .unwrap();
    handle.join().await.unwrap();
    let report = supervisor.shutdown(Duration::ZERO).await;
    assert_eq!(report.remaining_active, 0);
}

#[tokio::test(start_paused = true)]
async fn dropped_handle_panic_remains_attributable() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let entered_in_task = entered.clone();
    let release_in_task = release.clone();
    let handle = supervisor
        .spawn_async(TaskCategory::Background, "dropped-panic", async move {
            entered_in_task.notify_one();
            release_in_task.notified().await;
            panic!("intentional dropped-handle panic");
        })
        .await
        .unwrap();
    entered.notified().await;
    drop(handle);
    release.notify_one();
    let report = supervisor.shutdown(Duration::from_secs(30)).await;

    assert_eq!(report.joined, 0);
    assert_eq!(
        supervisor
            .recent_task_outcomes()
            .iter()
            .filter(|entry| entry.name == "dropped-panic" && entry.outcome == TaskOutcome::Panicked)
            .count(),
        1
    );
    assert!(supervisor.active_tasks(None).is_empty());
}

#[tokio::test(start_paused = true)]
async fn panic_during_shutdown_is_not_a_successful_join() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let cancellation = supervisor.root_cancellation_token();
    let _handle = supervisor
        .spawn_async(TaskCategory::Background, "shutdown-panic", async move {
            cancellation.cancelled().await;
            panic!("panic after observing shutdown");
        })
        .await
        .unwrap();

    let report = supervisor.shutdown(Duration::from_secs(30)).await;
    assert_eq!(report.joined, 0);
    assert_eq!(report.aborted, 0);
    assert!(
        supervisor.recent_task_outcomes().iter().any(|entry| {
            entry.name == "shutdown-panic" && entry.outcome == TaskOutcome::Panicked
        })
    );
    assert!(supervisor.active_tasks(None).is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_cleanup_cannot_publish_a_terminal_outcome() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let cleanup = supervisor.pause_next_terminal_cleanup();
    let handle = supervisor
        .spawn_async(TaskCategory::Background, "blocked-cleanup", async {})
        .await
        .unwrap();
    cleanup.wait_until_entered().await;

    assert!(
        supervisor
            .active_tasks(None)
            .iter()
            .any(|task| task.name == "blocked-cleanup")
    );
    assert!(
        !supervisor
            .recent_task_outcomes()
            .iter()
            .any(|task| task.name == "blocked-cleanup")
    );

    cleanup.release();
    handle.join().await.unwrap();
    assert!(supervisor.active_tasks(None).is_empty());
    assert!(
        supervisor.recent_task_outcomes().iter().any(|task| {
            task.name == "blocked-cleanup" && task.outcome == TaskOutcome::Completed
        })
    );
}

#[tokio::test(start_paused = true)]
async fn recent_task_outcome_history_is_bounded() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    for index in 0..257 {
        supervisor
            .spawn_async(
                TaskCategory::Background,
                format!("bounded-{index}"),
                async {},
            )
            .await
            .unwrap()
            .join()
            .await
            .unwrap();
    }

    let outcomes = supervisor.recent_task_outcomes();
    assert_eq!(outcomes.len(), 256);
    assert_eq!(outcomes.first().unwrap().name, "bounded-1");
    assert_eq!(outcomes.last().unwrap().name, "bounded-256");
}

#[tokio::test(start_paused = true)]
async fn multiple_coalesced_completions_need_no_time_advance() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let release = Arc::new(tokio::sync::Notify::new());
    let cancellation_count = Arc::new(AtomicUsize::new(0));
    let all_cancelled = Arc::new(tokio::sync::Notify::new());
    let mut handles = Vec::new();
    for name in ["coalesced-a", "coalesced-b"] {
        let cancellation = supervisor.root_cancellation_token();
        let release_in_task = release.clone();
        let cancellation_count = cancellation_count.clone();
        let all_cancelled = all_cancelled.clone();
        handles.push(
            supervisor
                .spawn_async(TaskCategory::Background, name, async move {
                    cancellation.cancelled().await;
                    if cancellation_count.fetch_add(1, Ordering::SeqCst) == 1 {
                        all_cancelled.notify_one();
                    }
                    release_in_task.notified().await;
                })
                .await
                .unwrap(),
        );
    }
    let started_at = tokio::time::Instant::now();
    let mut shutdown = Box::pin(supervisor.shutdown(Duration::from_secs(30)));
    assert!(matches!(poll_once(shutdown.as_mut()).await, Poll::Pending));
    all_cancelled.notified().await;
    release.notify_waiters();
    for handle in handles {
        handle.join().await.unwrap();
    }
    let report = shutdown.await;
    assert_eq!(tokio::time::Instant::now(), started_at);
    assert_eq!(report.joined, 2);
    assert_eq!(report.aborted, 0);
}

#[tokio::test(start_paused = true)]
async fn completion_just_before_abort_is_not_aborted() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let cancellation = supervisor.root_cancellation_token();
    let cancellation_seen = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let seen_in_task = cancellation_seen.clone();
    let release_in_task = release.clone();
    let handle = supervisor
        .spawn_async(
            TaskCategory::Background,
            "pre-abort-completion",
            async move {
                cancellation.cancelled().await;
                seen_in_task.notify_one();
                release_in_task.notified().await;
            },
        )
        .await
        .unwrap();
    let mut shutdown = Box::pin(supervisor.shutdown(Duration::from_secs(30)));
    assert!(matches!(poll_once(shutdown.as_mut()).await, Poll::Pending));
    cancellation_seen.notified().await;
    release.notify_one();
    handle.join().await.unwrap();
    let report = shutdown.await;
    assert_eq!(report.joined, 1);
    assert_eq!(report.aborted, 0);
    assert_eq!(report.abort_confirmed, 0);
}

#[tokio::test]
async fn timer_default_is_unlimited() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    assert_eq!(supervisor.semaphore_limit(TaskCategory::Timer), None);
}

#[tokio::test]
async fn timer_sleep_status_appears_and_disappears() {
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let sleeper = {
        let supervisor = supervisor.clone();
        tokio::spawn(async move {
            supervisor
                .sleep("timer-sleep", Duration::from_millis(120))
                .await
                .unwrap();
        })
    };

    wait_for(
        || {
            supervisor
                .active_tasks(Some(TaskCategory::Timer))
                .iter()
                .any(|task| task.name == "timer-sleep")
        },
        Duration::from_secs(2),
    )
    .await;
    sleeper.await.unwrap();

    wait_for(
        || {
            supervisor
                .active_tasks(Some(TaskCategory::Timer))
                .is_empty()
        },
        Duration::from_secs(2),
    )
    .await;
}

#[tokio::test]
async fn spawn_async_removes_completed_task_from_managed_registry() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());

    let handle = supervisor
        .spawn_async(TaskCategory::Background, "quick-cleanup", async {})
        .await
        .unwrap();
    handle.join().await.unwrap();

    // Give the drop guard a moment to release the lock if needed.
    assert!(supervisor.active_tasks(None).is_empty());
}

#[tokio::test]
async fn spawn_async_removes_panicked_task_from_managed_registry() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());

    let handle = supervisor
        .spawn_async(TaskCategory::Background, "panic-cleanup", async {
            panic!("intentional supervised task panic");
        })
        .await
        .unwrap();
    assert!(handle.join().await.is_err());

    assert!(supervisor.active_tasks(None).is_empty());
}

#[tokio::test]
async fn spawn_delay_does_not_run_future_after_root_cancellation() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let fired = Arc::new(AtomicUsize::new(0));
    let fired_for_task = fired.clone();

    let _handle = supervisor
        .spawn_delay("cancelled-delay", Duration::from_secs(60), async move {
            fired_for_task.fetch_add(1, Ordering::SeqCst);
        })
        .await
        .unwrap();

    let report = supervisor.shutdown(Duration::from_secs(1)).await;

    assert_eq!(
        fired.load(Ordering::SeqCst),
        0,
        "delayed future must not run when root cancellation wins the race"
    );
    assert_eq!(report.remaining_active, 0);
    assert!(supervisor.active_tasks(None).is_empty());
}

#[tokio::test]
async fn timer_spawn_delay_fires_once() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let fired = Arc::new(AtomicUsize::new(0));
    let fired_for_task = fired.clone();

    let handle = supervisor
        .spawn_delay("delay-once", Duration::from_millis(40), async move {
            fired_for_task.fetch_add(1, Ordering::SeqCst);
        })
        .await
        .unwrap();
    handle.join().await.unwrap();

    assert_eq!(fired.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn timer_spawn_interval_stops_on_root_cancellation() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let ticks = Arc::new(AtomicUsize::new(0));
    let ticks_for_task = ticks.clone();

    let handle = supervisor
        .spawn_interval("interval", Duration::from_millis(20), move |_| {
            let ticks_for_task = ticks_for_task.clone();
            async move {
                ticks_for_task.fetch_add(1, Ordering::SeqCst);
            }
        })
        .await
        .unwrap();

    wait_for(|| ticks.load(Ordering::SeqCst) >= 2, Duration::from_secs(2)).await;

    let cancel = supervisor.root_cancellation_token();
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(1), handle.join())
        .await
        .expect("interval task should stop after cancellation")
        .unwrap();

    let value_after = ticks.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(ticks.load(Ordering::SeqCst), value_after);
}

#[tokio::test]
async fn db_limit_queues_second_call() {
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let conn = tokio_rusqlite::Connection::open_in_memory().await.unwrap();
    let gate = Arc::new((Mutex::new(0usize), Condvar::new()));
    let first_started = Arc::new(AtomicUsize::new(0));
    let second_started = Arc::new(AtomicUsize::new(0));

    let first = {
        let supervisor = supervisor.clone();
        let conn = conn.clone();
        let gate = gate.clone();
        let first_started = first_started.clone();
        tokio::spawn(async move {
            supervisor
                .call_db("first", "conn-a", conn, move |_conn| {
                    first_started.store(1, Ordering::SeqCst);
                    wait_on_gate(&gate);
                    Ok(())
                })
                .await
                .unwrap();
        })
    };

    wait_for(
        || first_started.load(Ordering::SeqCst) == 1,
        Duration::from_secs(2),
    )
    .await;

    let second = {
        let supervisor = supervisor.clone();
        let conn = conn.clone();
        let second_started = second_started.clone();
        tokio::spawn(async move {
            supervisor
                .call_db("second", "conn-a", conn, move |_conn| {
                    second_started.store(1, Ordering::SeqCst);
                    Ok(())
                })
                .await
                .unwrap();
        })
    };

    wait_for(
        || category_status(&supervisor, TaskCategory::Db).queued == 1,
        Duration::from_secs(2),
    )
    .await;
    release_gate(&gate, 1);
    first.await.unwrap();
    second.await.unwrap();
    assert_eq!(second_started.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn db_active_status_only_while_call_in_flight() {
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let conn = tokio_rusqlite::Connection::open_in_memory().await.unwrap();
    let gate = Arc::new((Mutex::new(0usize), Condvar::new()));

    let task = {
        let supervisor = supervisor.clone();
        let conn = conn.clone();
        let gate = gate.clone();
        tokio::spawn(async move {
            supervisor
                .call_db("active-check", "conn-a", conn, move |_conn| {
                    wait_on_gate(&gate);
                    Ok(())
                })
                .await
                .unwrap();
        })
    };

    wait_for(
        || category_status(&supervisor, TaskCategory::Db).active == 1,
        Duration::from_secs(2),
    )
    .await;
    release_gate(&gate, 1);
    task.await.unwrap();
    wait_for(
        || category_status(&supervisor, TaskCategory::Db).active == 0,
        Duration::from_secs(2),
    )
    .await;
}

#[tokio::test]
async fn default_db_read_admission_defers_serialization_to_the_connection_actor() {
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let conn = tokio_rusqlite::Connection::open_in_memory().await.unwrap();
    let gate = Arc::new((Mutex::new(0usize), Condvar::new()));
    let mut tasks = Vec::new();

    for index in 0..3 {
        let supervisor = supervisor.clone();
        let conn = conn.clone();
        let gate = gate.clone();
        tasks.push(tokio::spawn(async move {
            supervisor
                .call_db_read(format!("read-{index}"), "conn-read", conn, move |_conn| {
                    if index == 0 {
                        wait_on_gate(&gate);
                    }
                    Ok(())
                })
                .await
                .unwrap();
        }));
    }

    wait_for(
        || {
            let status = category_status(&supervisor, TaskCategory::DbRead);
            status.active + status.queued == 3
        },
        Duration::from_secs(2),
    )
    .await;
    let status_while_actor_blocked = category_status(&supervisor, TaskCategory::DbRead);
    release_gate(&gate, 1);
    for task in tasks {
        task.await.unwrap();
    }

    assert_eq!(
        status_while_actor_blocked.active, 3,
        "the supervised read boundary must admit every call while the connection actor owns serialization"
    );
    assert_eq!(status_while_actor_blocked.queued, 0);
}

#[tokio::test]
async fn db_query_logging_default_off_and_toggle() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let conn = tokio_rusqlite::Connection::open_in_memory().await.unwrap();
    let capture = Arc::new(TraceCapture::default());
    let _default = tracing::subscriber::set_default(capture.clone());

    assert!(!(supervisor.db_query_logging_status().enabled));
    supervisor
        .call_db("no-log", "conn-a", conn.clone(), move |_conn| {
            Ok::<_, tokio_rusqlite::Error>(())
        })
        .await
        .unwrap();
    assert!(capture.events().is_empty());

    assert!(supervisor.set_db_query_logging(true).enabled);
    supervisor
        .call_db("metadata-only", "conn-a", conn, move |_conn| {
            Ok::<_, tokio_rusqlite::Error>(())
        })
        .await
        .unwrap();
    let events = capture.events();
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].get("query_name").map(String::as_str),
        Some("metadata-only")
    );
    assert_eq!(
        events[0].get("connection_key").map(String::as_str),
        Some("conn-a")
    );
}

#[tokio::test]
async fn db_query_logging_entries_never_include_payload_values() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let conn = tokio_rusqlite::Connection::open_in_memory().await.unwrap();
    supervisor.set_db_query_logging(true);
    let capture = Arc::new(TraceCapture::default());
    let _default = tracing::subscriber::set_default(capture.clone());

    let secret_payload = "top-secret-value";
    supervisor
        .call_db("payload-check", "conn-a", conn, move |_conn| {
            let _ignored = secret_payload;
            Ok::<_, tokio_rusqlite::Error>(())
        })
        .await
        .unwrap();

    let serialized = serde_json::to_string(&capture.events()).unwrap();
    assert!(serialized.contains("payload-check"));
    assert!(!serialized.contains(secret_payload));
}

#[derive(Default)]
struct TraceCapture {
    events: Mutex<Vec<BTreeMap<String, String>>>,
}

impl TraceCapture {
    fn events(&self) -> Vec<BTreeMap<String, String>> {
        self.events.lock().unwrap().clone()
    }
}

#[derive(Default)]
struct FieldCapture(BTreeMap<String, String>);

impl Visit for FieldCapture {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().to_owned(), value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl Subscriber for TraceCapture {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target() == "klights::task_supervisor::db"
    }

    fn new_span(&self, _attributes: &tracing::span::Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut capture = FieldCapture::default();
        event.record(&mut capture);
        self.events.lock().unwrap().push(capture.0);
    }
    fn enter(&self, _span: &Id) {}
    fn exit(&self, _span: &Id) {}

    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        if self.enabled(metadata) {
            Interest::always()
        } else {
            Interest::never()
        }
    }

    fn clone_span(&self, id: &Id) -> Id {
        id.clone()
    }
    fn try_close(&self, _id: Id) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// DSB-HA-03: run_db_blocking tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn run_db_blocking_uses_db_category_semaphore() {
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let gate = Arc::new((Mutex::new(0usize), Condvar::new()));
    let started = Arc::new(AtomicUsize::new(0));

    // First task holds the DB slot
    let first = {
        let supervisor = supervisor.clone();
        let gate = gate.clone();
        let started = started.clone();
        tokio::spawn(async move {
            supervisor
                .run_db_blocking("db-blocking-first", "test-backend", move || {
                    started.fetch_add(1, Ordering::SeqCst);
                    wait_on_gate(&gate);
                })
                .await
                .unwrap();
        })
    };

    wait_for(
        || started.load(Ordering::SeqCst) == 1,
        Duration::from_secs(2),
    )
    .await;

    // Second task must queue because DB limit is 1
    let second_started = Arc::new(AtomicUsize::new(0));
    let second = {
        let supervisor = supervisor.clone();
        let second_started = second_started.clone();
        tokio::spawn(async move {
            supervisor
                .run_db_blocking("db-blocking-second", "test-backend", move || {
                    second_started.fetch_add(1, Ordering::SeqCst);
                })
                .await
                .unwrap();
        })
    };

    // Second should be queued, not running
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        category_status(&supervisor, TaskCategory::Db).queued,
        1,
        "second run_db_blocking task should be queued behind first"
    );
    assert_eq!(second_started.load(Ordering::SeqCst), 0);

    // Release first
    release_gate(&gate, 1);
    first.await.unwrap();
    second.await.unwrap();
    assert_eq!(second_started.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn run_db_blocking_returns_result() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let result: i64 = supervisor
        .run_db_blocking("db-blocking-result", "test-backend", || 42i64)
        .await
        .unwrap();
    assert_eq!(result, 42);
}

#[tokio::test]
async fn run_db_blocking_propagates_panic() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let result = supervisor
        .run_db_blocking::<(), _>("db-blocking-panic", "test-backend", || {
            panic!("intentional db blocking panic");
        })
        .await;
    assert!(
        result.is_err(),
        "panicked run_db_blocking should return error"
    );
}

/// When a `run_blocking` future is cancelled (dropped) while the blocking
/// work is still in flight, the RAII guard must still finalise the active
/// task and release the semaphore permit.
#[tokio::test]
async fn run_blocking_cancellation_cleans_up_active_task() {
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let gate = Arc::new((Mutex::new(0usize), Condvar::new()));
    let entered = Arc::new(AtomicUsize::new(0));
    let cancel = Arc::new(tokio::sync::Notify::new());
    let cancelled_flag = Arc::new(AtomicUsize::new(0));

    let task = {
        let supervisor = supervisor.clone();
        let gate = gate.clone();
        let entered = entered.clone();
        let cancel = cancel.clone();
        let cancelled_flag = cancelled_flag.clone();
        tokio::spawn(async move {
            tokio::select! {
                result = supervisor.run_blocking(
                    TaskCategory::Background,
                    "cancellable-blocking",
                    move || {
                        entered.store(1, Ordering::SeqCst);
                        wait_on_gate(&gate);
                        42usize
                    },
                ) => { let _ = result; }
                _ = cancel.notified() => { cancelled_flag.store(1, Ordering::SeqCst); }
            }
        })
    };

    wait_for(
        || entered.load(Ordering::SeqCst) == 1,
        Duration::from_secs(2),
    )
    .await;
    assert!(
        supervisor
            .active_tasks(Some(TaskCategory::Background))
            .iter()
            .any(|t| t.name == "cancellable-blocking"),
        "active task must appear while blocking work is in flight"
    );

    cancel.notify_one();
    task.await.unwrap();
    assert_eq!(cancelled_flag.load(Ordering::SeqCst), 1);

    // After cancellation the active task must STILL be present because the
    // underlying spawn_blocking work is uncancellable and the detached
    // wrapper task holds the permit + active-task entry until completion.
    assert!(
        supervisor
            .active_tasks(Some(TaskCategory::Background))
            .iter()
            .any(|t| t.name == "cancellable-blocking"),
        "active task must persist after caller cancellation — blocking work still in flight"
    );

    // Release the gate so the blocking work can finish. The wrapper task
    // then drops its guard, which removes the active-task entry.
    release_gate(&gate, 1);
    wait_for(
        || {
            supervisor
                .active_tasks(Some(TaskCategory::Background))
                .iter()
                .all(|t| t.name != "cancellable-blocking")
        },
        Duration::from_secs(2),
    )
    .await;
}

/// Same cancellation-safety guarantee for `call_db`.
#[tokio::test]
async fn call_db_cancellation_cleans_up_active_task() {
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let conn = tokio_rusqlite::Connection::open_in_memory().await.unwrap();
    let gate = Arc::new((Mutex::new(0usize), Condvar::new()));
    let entered = Arc::new(AtomicUsize::new(0));
    let cancel = Arc::new(tokio::sync::Notify::new());
    let cancelled_flag = Arc::new(AtomicUsize::new(0));

    let task = {
        let supervisor = supervisor.clone();
        let conn = conn.clone();
        let gate = gate.clone();
        let entered = entered.clone();
        let cancel = cancel.clone();
        let cancelled_flag = cancelled_flag.clone();
        tokio::spawn(async move {
            tokio::select! {
                result = supervisor.call_db(
                    "cancellable-db", "conn-test", conn,
                    move |_conn| {
                        entered.store(1, Ordering::SeqCst);
                        wait_on_gate(&gate);
                        Ok(())
                    },
                ) => { let _ = result; }
                _ = cancel.notified() => { cancelled_flag.store(1, Ordering::SeqCst); }
            }
        })
    };

    wait_for(
        || entered.load(Ordering::SeqCst) == 1,
        Duration::from_secs(2),
    )
    .await;
    assert!(
        supervisor
            .active_tasks(Some(TaskCategory::Db))
            .iter()
            .any(|t| t.name == "cancellable-db"),
        "active DB task must appear while call is in flight"
    );

    cancel.notify_one();
    task.await.unwrap();
    assert_eq!(cancelled_flag.load(Ordering::SeqCst), 1);

    // After cancellation the active task must STILL be present because the
    // underlying DB work (connection.call) is uncancellable and the detached
    // wrapper task holds the permit + active-task entry until completion.
    assert!(
        supervisor
            .active_tasks(Some(TaskCategory::Db))
            .iter()
            .any(|t| t.name == "cancellable-db"),
        "active DB task must persist after caller cancellation — DB work still in flight"
    );

    // Release the gate so the DB work can finish. The wrapper task then
    // drops its guard, which removes the active-task entry.
    release_gate(&gate, 1);
    wait_for(
        || {
            supervisor
                .active_tasks(Some(TaskCategory::Db))
                .iter()
                .all(|t| t.name != "cancellable-db")
        },
        Duration::from_secs(2),
    )
    .await;
}

/// A caller that is cancelled *while still queued* (waiting to acquire the
/// category permit) must not leak the `queued` gauge. The counter is bumped
/// +1 before awaiting the semaphore and -1 after; if the await is cancelled
/// the decrement must still run, or `category_status(..).queued` drifts
/// upward forever. Regression for the observed steady `db queued=22,
/// active=0` on the live leader.
#[tokio::test]
async fn queued_gauge_recovers_when_waiting_caller_is_cancelled() {
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let conn = tokio_rusqlite::Connection::open_in_memory().await.unwrap();
    let gate = Arc::new((Mutex::new(0usize), Condvar::new()));
    let first_started = Arc::new(AtomicUsize::new(0));

    // First caller holds the single DB slot on the gate.
    let first = {
        let supervisor = supervisor.clone();
        let conn = conn.clone();
        let gate = gate.clone();
        let first_started = first_started.clone();
        tokio::spawn(async move {
            supervisor
                .call_db("holder", "conn-a", conn, move |_conn| {
                    first_started.store(1, Ordering::SeqCst);
                    wait_on_gate(&gate);
                    Ok(())
                })
                .await
                .unwrap();
        })
    };
    wait_for(
        || first_started.load(Ordering::SeqCst) == 1,
        Duration::from_secs(2),
    )
    .await;

    // Second caller cannot acquire the slot; it parks in acquire_permit.
    let cancel = Arc::new(tokio::sync::Notify::new());
    let waiter = {
        let supervisor = supervisor.clone();
        let conn = conn.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            tokio::select! {
                r = supervisor.call_db("waiter", "conn-a", conn, move |_conn| Ok(())) => { let _ = r; }
                _ = cancel.notified() => {}
            }
        })
    };

    // The waiter is now queued.
    wait_for(
        || category_status(&supervisor, TaskCategory::Db).queued == 1,
        Duration::from_secs(2),
    )
    .await;

    // Cancel the waiter while it is still queued (before it ever acquires).
    cancel.notify_one();
    waiter.await.unwrap();

    // The queued gauge must return to 0 — not stay stuck at 1.
    wait_for(
        || category_status(&supervisor, TaskCategory::Db).queued == 0,
        Duration::from_secs(2),
    )
    .await;
    assert_eq!(
        category_status(&supervisor, TaskCategory::Db).queued,
        0,
        "queued gauge must not leak when a waiting caller is cancelled"
    );

    // Cleanup: release the holder.
    release_gate(&gate, 1);
    first.await.unwrap();
}

/// When the caller future is cancelled (e.g. by timeout or select!), the
/// semaphore permit must remain held until the underlying spawn_blocking
/// work completes. Releasing the permit early over-admits blocking work
/// past the configured category cap.
#[tokio::test]
async fn blocking_permit_held_during_caller_cancellation() {
    let config = TaskCategoryConfig {
        file: 1,
        ..TaskCategoryConfig::default()
    };
    let supervisor = Arc::new(TaskSupervisor::new(config));
    let gate = Arc::new((Mutex::new(0usize), Condvar::new()));
    let entered = Arc::new(AtomicBool::new(false));
    let first_done = Arc::new(AtomicBool::new(false));

    // Spawn a task that runs a blocking operation holding the File permit.
    // We cancel it before the blocking work completes.
    let gate_c = gate.clone();
    let entered_c = entered.clone();
    let first_done_c = first_done.clone();
    let sup_c = supervisor.clone();
    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();

    let first_task = tokio::spawn(async move {
        tokio::select! {
            r = sup_c.run_blocking_file("test-blocking", move || {
                entered_c.store(true, Ordering::SeqCst);
                wait_on_gate(&gate_c);
                first_done_c.store(true, Ordering::SeqCst);
                42u32
            }) => {
                let _ = r;
            }
            _ = cancel_rx => {
                // caller cancelled — drop the run_blocking_file future
            }
        }
    });

    // Wait until the blocking closure has entered.
    let entered_ok = wait_for_bool(&entered, Duration::from_secs(5)).await;
    assert!(entered_ok, "blocking closure must enter");

    // Cancel the first caller. The spawn_blocking work continues (uncancellable).
    let _ = cancel_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), first_task).await;

    // Sanity: the first blocking work is still running on the gate.
    assert!(!first_done.load(Ordering::SeqCst));

    // The permit must still be held — a second caller should be queued.
    let second_entered = Arc::new(AtomicBool::new(false));
    let second_entered_c = second_entered.clone();
    let gate_c2 = gate.clone();
    let sup_c2 = supervisor.clone();
    let second_task = tokio::spawn(async move {
        sup_c2
            .run_blocking_file("test-blocking-2", move || {
                second_entered_c.store(true, Ordering::SeqCst);
                wait_on_gate(&gate_c2);
                99u32
            })
            .await
    });

    // Give the second task time to try to acquire the permit.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !second_entered.load(Ordering::SeqCst),
        "second call must be queued; permit must still be held by the first (cancelled) caller"
    );
    assert_eq!(
        category_status(&supervisor, TaskCategory::File).queued,
        1,
        "exactly one caller should be queued for the file category permit"
    );

    // Release the gate so the first blocking work finishes. The permit is
    // then released and the second call proceeds.
    release_gate(&gate, 1);
    wait_for(
        || second_entered.load(Ordering::SeqCst),
        Duration::from_secs(5),
    )
    .await;

    // Clean up: release the gate for the second call.
    release_gate(&gate, 1);
    let result = second_task.await.unwrap();
    assert_eq!(result.unwrap(), 99);
}

#[tokio::test]
async fn process_output_runs_through_the_selected_supervised_category() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let command = process_fixture_command("success");

    let output = supervisor
        .run_process_output(TaskCategory::Network, "process-output", command)
        .await
        .expect("supervised process output");

    assert!(output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("supervised-process"),
        "fixture output must be captured"
    );
    assert!(
        supervisor
            .active_tasks(Some(TaskCategory::Network))
            .is_empty(),
        "completed process output must leave no active task registration"
    );
}

#[tokio::test]
async fn process_spawn_is_admitted_through_the_selected_supervised_category() {
    let config = TaskCategoryConfig {
        others: 1,
        ..TaskCategoryConfig::default()
    };
    let supervisor = TaskSupervisor::new(config);
    let command = process_fixture_command("park");

    let mut child = supervisor
        .spawn_process(
            TaskCategory::Others,
            "process-spawn",
            command,
            ProcessShutdownPolicy::KillAndReap,
        )
        .await
        .expect("supervised process spawn");

    assert_eq!(
        category_status(&supervisor, TaskCategory::Others).active,
        1,
        "long-lived child must retain its active registration and permit"
    );
    child.kill().await.expect("kill supervised child");
    let status = child.wait().await.expect("wait for killed process");
    assert!(!status.success());
    assert!(
        supervisor
            .active_tasks(Some(TaskCategory::Others))
            .is_empty(),
        "completed process admission must leave no active task registration"
    );
}

#[tokio::test]
async fn unexpected_process_exit_is_reaped_and_releases_accounting() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let mut child = supervisor
        .spawn_process(
            TaskCategory::Others,
            "process-unexpected-exit",
            process_fixture_command("success"),
            ProcessShutdownPolicy::KillAndReap,
        )
        .await
        .expect("spawn short-lived process");

    let status = child.wait().await.expect("reaped process status");

    assert!(status.success());
    assert_eq!(category_status(&supervisor, TaskCategory::Others).active, 0);
}

#[tokio::test]
async fn dropping_kill_policy_handle_kills_and_reaps_process() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let child = supervisor
        .spawn_process(
            TaskCategory::Others,
            "process-handle-drop",
            process_fixture_command("park"),
            ProcessShutdownPolicy::KillAndReap,
        )
        .await
        .expect("spawn parked process");
    let pid = child.id().expect("child pid");
    assert!(process_fixture_is_running(pid));

    drop(child);
    wait_for(
        || category_status(&supervisor, TaskCategory::Others).active == 0,
        Duration::from_secs(2),
    )
    .await;

    assert!(
        !process_fixture_is_running(pid),
        "dropping KillAndReap handle must not leave a live child"
    );
}

#[tokio::test]
async fn dropping_preserve_handle_without_root_shutdown_kills_and_reaps() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let child = supervisor
        .spawn_process(
            TaskCategory::Others,
            "process-preserve-handle-drop",
            process_fixture_command("park"),
            ProcessShutdownPolicy::Preserve,
        )
        .await
        .expect("spawn parked process");
    let pid = child.id().expect("child pid");

    drop(child);
    wait_for(
        || category_status(&supervisor, TaskCategory::Others).active == 0,
        Duration::from_secs(2),
    )
    .await;
    let still_running = process_fixture_is_running(pid);
    if still_running {
        terminate_and_reap_fixture(pid);
    }

    assert!(
        !still_running,
        "Preserve is a root-shutdown policy, not permission to orphan a dropped handle"
    );
}

#[tokio::test]
async fn cancelled_preserve_spawn_before_handoff_kills_and_reaps() {
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let startup = supervisor.pause_next_process_startup_handoff();
    let spawn = {
        let supervisor = supervisor.clone();
        tokio::spawn(async move {
            supervisor
                .spawn_process(
                    TaskCategory::Others,
                    "process-cancelled-before-handoff",
                    process_fixture_command("park"),
                    ProcessShutdownPolicy::Preserve,
                )
                .await
        })
    };
    startup.wait_until_entered().await;
    let pid = startup.pid().expect("paused child pid");

    spawn.abort();
    assert!(
        spawn
            .await
            .expect_err("spawn caller cancelled")
            .is_cancelled()
    );
    startup.release();
    wait_for(
        || category_status(&supervisor, TaskCategory::Others).active == 0,
        Duration::from_secs(2),
    )
    .await;
    let still_running = process_fixture_is_running(pid);
    if still_running {
        terminate_and_reap_fixture(pid);
    }

    assert!(
        !still_running,
        "a process whose ownership handoff failed must be killed and reaped"
    );
}

#[tokio::test]
async fn process_output_preserves_nonzero_status_and_stderr() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let output = supervisor
        .run_process_output(
            TaskCategory::Network,
            "process-nonzero",
            process_fixture_command("nonzero"),
        )
        .await
        .expect("nonzero exit is a normal process output");

    assert_eq!(output.status.code(), Some(23));
    assert!(String::from_utf8_lossy(&output.stderr).contains("fixture-stderr"));
}

#[tokio::test]
async fn process_output_drains_large_stdout_and_stderr_without_deadlock() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let output = supervisor
        .run_process_output(
            TaskCategory::Network,
            "process-large-dual-pipe",
            process_fixture_command("large-dual-pipe"),
        )
        .await
        .expect("large stdout/stderr process");

    assert!(output.status.success());
    assert!(output.stdout.iter().filter(|byte| **byte == b'o').count() >= 256 * 1024);
    assert!(output.stderr.iter().filter(|byte| **byte == b'e').count() >= 256 * 1024);
}

#[tokio::test]
async fn process_spawn_failure_cleans_registration_and_permit() {
    let config = TaskCategoryConfig {
        others: 1,
        ..TaskCategoryConfig::default()
    };
    let supervisor = TaskSupervisor::new(config);
    let command = std::process::Command::new("/definitely/missing/klights-process-fixture");

    let error = supervisor
        .spawn_process(
            TaskCategory::Others,
            "process-missing",
            command,
            ProcessShutdownPolicy::KillAndReap,
        )
        .await
        .expect_err("missing executable must fail at spawn");

    assert!(matches!(error, ProcessError::Spawn(_)));
    assert_eq!(category_status(&supervisor, TaskCategory::Others).active, 0);
    assert_eq!(category_status(&supervisor, TaskCategory::Others).queued, 0);
}

#[tokio::test]
async fn running_output_process_is_killed_and_reaped_on_shutdown() {
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let output_task = {
        let supervisor = supervisor.clone();
        tokio::spawn(async move {
            supervisor
                .run_process_output(
                    TaskCategory::Network,
                    "process-output-shutdown",
                    process_fixture_command("park"),
                )
                .await
        })
    };
    wait_for(
        || category_status(&supervisor, TaskCategory::Network).active == 1,
        Duration::from_secs(2),
    )
    .await;

    let report = supervisor.shutdown(Duration::from_secs(2)).await;
    let result = output_task.await.expect("output caller task");

    assert!(matches!(result, Err(ProcessError::Cancelled)));
    assert!(!report.timed_out);
    assert_eq!(report.remaining_active, 0);
    assert_eq!(
        category_status(&supervisor, TaskCategory::Network).active,
        0
    );
}

#[tokio::test]
async fn zero_timeout_shutdown_does_not_abort_process_cleanup_actor() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let mut child = supervisor
        .spawn_process(
            TaskCategory::Others,
            "process-zero-timeout-shutdown",
            process_fixture_command("park"),
            ProcessShutdownPolicy::KillAndReap,
        )
        .await
        .expect("spawn parked process");
    let pid = child.id().expect("child pid");

    let report = supervisor.shutdown(Duration::ZERO).await;
    let status = child.wait().await.expect("cleanup actor must reap child");

    assert!(report.timed_out);
    assert_eq!(report.aborted, 0, "cleanup-critical actor is not abortable");
    assert!(!status.success());
    assert!(!process_fixture_is_running(pid));
    assert!(supervisor.active_tasks(None).is_empty());
}

#[tokio::test]
async fn cancelling_output_caller_keeps_managed_process_accounted_until_shutdown() {
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let caller = {
        let supervisor = supervisor.clone();
        tokio::spawn(async move {
            supervisor
                .run_process_output(
                    TaskCategory::Network,
                    "process-cancelled-caller",
                    process_fixture_command("park"),
                )
                .await
        })
    };
    wait_for(
        || category_status(&supervisor, TaskCategory::Network).active == 1,
        Duration::from_secs(2),
    )
    .await;

    caller.abort();
    assert!(caller.await.expect_err("caller cancelled").is_cancelled());
    assert_eq!(
        category_status(&supervisor, TaskCategory::Network).active,
        1,
        "caller cancellation must not release accounting while its process runs"
    );

    let report = supervisor.shutdown(Duration::from_secs(2)).await;
    assert!(!report.timed_out);
    assert!(supervisor.active_tasks(None).is_empty());
}

#[tokio::test]
async fn queued_process_spawn_is_rejected_when_shutdown_starts() {
    let config = TaskCategoryConfig {
        others: 1,
        ..TaskCategoryConfig::default()
    };
    let supervisor = Arc::new(TaskSupervisor::new(config));
    let first = supervisor
        .spawn_process(
            TaskCategory::Others,
            "process-holder",
            process_fixture_command("park"),
            ProcessShutdownPolicy::KillAndReap,
        )
        .await
        .expect("first process");
    let queued = {
        let supervisor = supervisor.clone();
        tokio::spawn(async move {
            supervisor
                .spawn_process(
                    TaskCategory::Others,
                    "process-queued",
                    process_fixture_command("success"),
                    ProcessShutdownPolicy::KillAndReap,
                )
                .await
        })
    };
    wait_for(
        || category_status(&supervisor, TaskCategory::Others).queued == 1,
        Duration::from_secs(2),
    )
    .await;

    let report = supervisor.shutdown(Duration::from_secs(2)).await;
    let queued_error = queued
        .await
        .expect("queued caller task")
        .expect_err("queued process must be rejected");

    assert!(matches!(
        queued_error,
        ProcessError::Admission(TaskAdmissionError::ShuttingDown)
    ));
    assert!(!report.timed_out);
    drop(first);
}

#[tokio::test]
async fn cancelling_queued_process_spawn_recovers_queue_without_spawning() {
    let config = TaskCategoryConfig {
        others: 1,
        ..TaskCategoryConfig::default()
    };
    let supervisor = Arc::new(TaskSupervisor::new(config));
    let mut holder = supervisor
        .spawn_process(
            TaskCategory::Others,
            "process-queue-holder",
            process_fixture_command("park"),
            ProcessShutdownPolicy::KillAndReap,
        )
        .await
        .expect("holder process");
    let queued = {
        let supervisor = supervisor.clone();
        tokio::spawn(async move {
            supervisor
                .spawn_process(
                    TaskCategory::Others,
                    "process-queued-caller-cancelled",
                    process_fixture_command("success"),
                    ProcessShutdownPolicy::KillAndReap,
                )
                .await
        })
    };
    wait_for(
        || category_status(&supervisor, TaskCategory::Others).queued == 1,
        Duration::from_secs(2),
    )
    .await;

    queued.abort();
    assert!(
        queued
            .await
            .expect_err("queued caller cancelled")
            .is_cancelled()
    );
    assert_eq!(category_status(&supervisor, TaskCategory::Others).queued, 0);
    assert_eq!(
        category_status(&supervisor, TaskCategory::Others).active,
        1,
        "only the holder process may exist"
    );

    holder.kill().await.expect("kill holder");
    holder.wait().await.expect("reap holder");
}

#[tokio::test]
async fn output_process_is_rejected_after_shutdown_without_spawning() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    supervisor.shutdown(Duration::from_secs(1)).await;

    let error = supervisor
        .run_process_output(
            TaskCategory::Network,
            "process-output-after-shutdown",
            process_fixture_command("success"),
        )
        .await
        .expect_err("shutdown must reject output process admission");

    assert!(matches!(
        error,
        ProcessError::Admission(TaskAdmissionError::ShuttingDown)
    ));
    assert!(supervisor.active_tasks(None).is_empty());
}

#[tokio::test]
async fn preserve_policy_is_explicit_and_releases_supervisor_accounting() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let mut child = supervisor
        .spawn_process(
            TaskCategory::Others,
            "process-preserved",
            process_fixture_command("park"),
            ProcessShutdownPolicy::Preserve,
        )
        .await
        .expect("preserved child");
    let pid = child.id().expect("child pid");

    let report = supervisor.shutdown(Duration::from_secs(2)).await;
    let wait_error = child
        .wait()
        .await
        .expect_err("preserved process is intentionally detached");

    assert!(matches!(wait_error, ProcessError::Preserved));
    assert!(!report.timed_out);
    assert_eq!(report.remaining_active, 0);

    terminate_and_reap_fixture(pid);
}

fn process_fixture_command(mode: &str) -> std::process::Command {
    let mut command = std::process::Command::new(std::env::current_exe().expect("test executable"));
    command
        .arg("--ignored")
        .arg("--exact")
        .arg("tests::process_test_fixture")
        .arg("--nocapture")
        .env("KLIGHTS_PROCESS_TEST_MODE", mode);
    command
}

fn terminate_and_reap_fixture(pid: u32) {
    unsafe extern "C" {
        fn kill(pid: i32, signal: i32) -> i32;
        fn waitpid(pid: i32, status: *mut i32, options: i32) -> i32;
    }
    const SIGKILL: i32 = 9;
    // SAFETY: the pid belongs to the dedicated test fixture child, and the
    // status pointer remains valid for the duration of waitpid.
    unsafe {
        let _ = kill(pid as i32, SIGKILL);
        let mut status = 0;
        let _ = waitpid(pid as i32, &mut status, 0);
    }
}

fn process_fixture_is_running(pid: u32) -> bool {
    unsafe extern "C" {
        fn kill(pid: i32, signal: i32) -> i32;
    }
    // SAFETY: signal 0 performs existence/permission checking only and does
    // not mutate the process.
    unsafe { kill(pid as i32, 0) == 0 }
}

#[test]
#[ignore]
fn process_test_fixture() {
    use std::io::Write;

    match std::env::var("KLIGHTS_PROCESS_TEST_MODE").as_deref() {
        Ok("success") => print!("supervised-process"),
        Ok("nonzero") => {
            eprint!("fixture-stderr");
            std::process::exit(23);
        }
        Ok("large-dual-pipe") => {
            let stdout = std::thread::spawn(|| {
                std::io::stdout()
                    .write_all(&vec![b'o'; 256 * 1024])
                    .unwrap();
            });
            let stderr = std::thread::spawn(|| {
                std::io::stderr()
                    .write_all(&vec![b'e'; 256 * 1024])
                    .unwrap();
            });
            stdout.join().unwrap();
            stderr.join().unwrap();
        }
        Ok("park") => loop {
            std::thread::park();
        },
        mode => panic!("unexpected process fixture mode: {mode:?}"),
    }
}

async fn wait_for_bool(flag: &AtomicBool, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if flag.load(Ordering::SeqCst) {
            return true;
        }
        if Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
