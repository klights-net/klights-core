use super::category::{TaskCategory, TaskCategoryConfig};
use super::supervisor::TaskSupervisor;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

#[test]
fn defaults_match_p0_category_limits() {
    let cfg = TaskCategoryConfig::default();
    assert_eq!(cfg.background, 0);
    assert_eq!(cfg.file, 3);
    assert_eq!(cfg.db, 1);
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

#[test]
fn active_task_tracking_adds_and_removes_entries() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    assert!(supervisor.active_tasks(None).is_empty());

    let _guard_a = supervisor.start_task_for_test(TaskCategory::Background, "worker-a");
    let guard_b = supervisor.start_task_for_test(TaskCategory::File, "render-volume");

    let all = supervisor.active_tasks(None);
    assert_eq!(all.len(), 2);
    assert_eq!(
        supervisor.active_tasks(Some(TaskCategory::File))[0].name,
        "render-volume"
    );

    drop(guard_b);
    let all_after = supervisor.active_tasks(None);
    assert_eq!(all_after.len(), 1);
    assert_eq!(all_after[0].name, "worker-a");
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
) -> super::task::TaskCategoryStatus {
    supervisor
        .category_statuses()
        .into_iter()
        .find(|entry| entry.category == category)
        .expect("category status must exist")
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
    assert_eq!(supervisor.managed_task_count(), 0);
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
    assert_eq!(supervisor.managed_task_count(), 0);
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
    wait_for(
        || supervisor.managed_task_count() == 0,
        Duration::from_secs(2),
    )
    .await;
    assert_eq!(
        supervisor.managed_task_count(),
        0,
        "completed supervised tasks must not remain in the managed task registry"
    );
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

    wait_for(
        || supervisor.managed_task_count() == 0,
        Duration::from_secs(2),
    )
    .await;
    assert_eq!(
        supervisor.managed_task_count(),
        0,
        "panicked supervised tasks must be removed by drop cleanup"
    );
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
    assert_eq!(supervisor.managed_task_count(), 0);
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
async fn db_query_logging_default_off_and_toggle() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let conn = tokio_rusqlite::Connection::open_in_memory().await.unwrap();

    assert!(!(supervisor.db_query_logging_status().enabled));
    supervisor
        .call_db("no-log", "conn-a", conn.clone(), move |_conn| {
            Ok::<_, tokio_rusqlite::Error>(())
        })
        .await
        .unwrap();
    assert!(supervisor.db_query_logs_for_test().is_empty());

    assert!(supervisor.set_db_query_logging(true).enabled);
    supervisor
        .call_db("metadata-only", "conn-a", conn, move |_conn| {
            Ok::<_, tokio_rusqlite::Error>(())
        })
        .await
        .unwrap();
    let logs = supervisor.db_query_logs_for_test();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].query_name, "metadata-only");
    assert_eq!(logs[0].connection_key, "conn-a");
}

#[tokio::test]
async fn db_query_logging_entries_never_include_payload_values() {
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
    let conn = tokio_rusqlite::Connection::open_in_memory().await.unwrap();
    supervisor.set_db_query_logging(true);

    let secret_payload = "top-secret-value";
    supervisor
        .call_db("payload-check", "conn-a", conn, move |_conn| {
            let _ignored = secret_payload;
            Ok::<_, tokio_rusqlite::Error>(())
        })
        .await
        .unwrap();

    let serialized = serde_json::to_string(&supervisor.db_query_logs_for_test()).unwrap();
    assert!(!serialized.contains("top-secret-value"));
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
