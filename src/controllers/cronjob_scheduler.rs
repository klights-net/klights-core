//! Per-CronJob event-driven scheduler (T13).
//!
//! Replaces the legacy 30s `runtime_cronjob_scheduler` periodic scan with
//! one wall-clock `spawn_delay` timer per active CronJob, keyed by UID.
//! Idle CPU on a quiet cluster is zero — no scan runs unless a CronJob is
//! actually due to fire.
//!
//! Lifecycle:
//! 1. `start` is called once at bootstrap. It subscribes to the datastore
//!    watch *before* the startup walk so no event is missed.
//! 2. The startup walk lists every CronJob and arms a per-UID timer.
//! 3. The watch loop arms / re-arms / cancels timers in response to
//!    create / update / delete events on `batch/v1` `CronJob`.
//! 4. Each fire closure: re-reads the CronJob, invokes the existing
//!    reconcile path (which applies the concurrency policy and creates
//!    the Job), then re-arms its own timer for the next fire time.
//!
//! HA forward-compat: this scheduler holds a `DatastoreHandle` and never
//! depends on consensus mode. Under future Raft mode the scheduler is
//! started fresh by the elected leader (every leadership acquisition
//! re-runs `start`); the per-UID timer map is process-local and is
//! rebuilt from `cluster.db` state — there is no shared state between
//! leaders. The fire closure re-reads the CronJob through
//! `DatastoreBackend` so a stale follower-turned-leader sees the latest
//! spec.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::controller_dispatcher::ControllerDispatcher;
use crate::datastore::sqlite::DatastoreWatchReplaySource;
use crate::datastore::{DatastoreHandle, WatchTarget};
use crate::task_supervisor::{SupervisedJoinHandle, TaskSupervisor};
use crate::watch::{
    EventType, SignalWatchCursor, WatchCursorError, WatchDeliveryScope, WatchEvent, WatchTopic,
    WindowPolicy,
};

/// Maximum delay we ever pass to `spawn_delay` for a single arm. Long
/// delays still work (Tokio's timer wheel handles years); this cap is a
/// belt-and-braces against pathological cron expressions and lets the
/// fire-closure self-rearm at most once per cap so a clock jump that
/// "skips" the original wake gets resynced within the cap.
const MAX_ARM_DELAY: Duration = Duration::from_secs(24 * 60 * 60);

pub struct CronJobScheduler {
    db: DatastoreHandle,
    dispatcher: Arc<ControllerDispatcher>,
    supervisor: Arc<TaskSupervisor>,
    timers: Mutex<HashMap<String /* uid */, SupervisedJoinHandle<()>>>,
}

impl CronJobScheduler {
    pub fn new(
        db: DatastoreHandle,
        dispatcher: Arc<ControllerDispatcher>,
        supervisor: Arc<TaskSupervisor>,
    ) -> Arc<Self> {
        Arc::new(Self {
            db,
            dispatcher,
            supervisor,
            timers: Mutex::new(HashMap::new()),
        })
    }

    /// Test-only accessor for the inner controller dispatcher reference.
    /// Lets test code drive `reconcile_cronjob_one` with the same
    /// dispatcher the scheduler would pass.
    #[cfg(test)]
    pub fn dispatcher_for_test(&self) -> &ControllerDispatcher {
        self.dispatcher.as_ref()
    }

    /// Number of currently-armed UID timers (test/observability only).
    pub async fn armed_count(&self) -> usize {
        self.timers.lock().await.len()
    }

    /// True if a timer is currently armed for this UID.
    pub async fn is_armed(&self, uid: &str) -> bool {
        self.timers.lock().await.contains_key(uid)
    }

    /// Cancel the timer for `uid` if any. Idempotent. Uses a tokio
    /// `Mutex` whose guard is `Send`, so nothing is held across an
    /// implicit boundary even when called from async code.
    pub async fn cancel(&self, uid: &str) {
        let prev = self.timers.lock().await.remove(uid);
        if let Some(handle) = prev {
            handle.abort();
        }
    }

    /// One-shot startup walk. Lists every CronJob and arms a per-UID
    /// timer. Returns once arming is complete; the armed timers fire
    /// asynchronously in the background.
    pub async fn startup_walk(self: &Arc<Self>) -> Result<()> {
        let listing = self
            .db
            .list_resources(
                "batch/v1",
                "CronJob",
                None,
                crate::datastore::ResourceListQuery::all(),
            )
            .await?;
        for resource in listing.items {
            Arc::clone(self).arm(&resource.data).await;
        }
        Ok(())
    }

    /// Arm a CronJob: cancel any prior timer for the same UID, compute
    /// the next fire time from the spec, then `spawn_delay` for that
    /// duration. If the CronJob is suspended, deleted, or has no
    /// computable next fire, no timer is armed.
    ///
    /// Takes `Arc<Self>` (not `&Arc<Self>`) so the returned future is
    /// `'static`, which is what `spawn_delay`'s 'static + Send bound on
    /// the caller chain requires when arm is reached transitively from
    /// `fire_and_rearm`.
    pub async fn arm(self: Arc<Self>, cj: &Value) {
        let Some(uid) = cj
            .pointer("/metadata/uid")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
        else {
            return;
        };

        // Always cancel first so re-arms after spec/suspend updates are
        // clean. `cancel` is idempotent if no prior timer exists.
        self.cancel(&uid).await;

        // Suspended: do not arm.
        if cj
            .pointer("/spec/suspend")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            return;
        }

        // Deletion: do not arm.
        if cj.pointer("/metadata/deletionTimestamp").is_some() {
            return;
        }

        let now = Utc::now();
        let next = match compute_next_fire(cj, now) {
            Ok(Some(t)) => t,
            Ok(None) => {
                // No future fire (invalid schedule, MissedSchedule beyond
                // startingDeadlineSeconds, etc.). Nothing to arm.
                return;
            }
            Err(e) => {
                tracing::warn!("CronJob {}: cannot compute next fire: {:#}", uid, e);
                return;
            }
        };

        let delay = if next <= now {
            Duration::from_secs(0)
        } else {
            (next - now).to_std().unwrap_or(Duration::from_secs(0))
        };
        let delay = std::cmp::min(delay, MAX_ARM_DELAY);

        let weak = Arc::downgrade(&self);
        let uid_for_closure = uid.clone();
        let handle = match self
            .supervisor
            .spawn_delay(format!("cronjob_fire:{}", uid), delay, async move {
                let Some(scheduler) = weak.upgrade() else {
                    return;
                };
                scheduler.fire_and_rearm(&uid_for_closure).await;
            })
            .await
        {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!(
                    "CronJob {}: failed to arm fire timer for {:?}: {:#}",
                    uid,
                    delay,
                    e
                );
                return;
            }
        };

        let displaced = self.timers.lock().await.insert(uid, handle);
        if let Some(prev) = displaced {
            // A racing arm() (or a stale entry) inserted under the same
            // UID; cancel the loser outside the lock.
            prev.abort();
        }
    }

    /// Fire closure body: re-read the CronJob (might have been suspended
    /// or deleted between schedule and fire), invoke the per-CronJob
    /// reconcile (creates Job + applies concurrency policy + updates
    /// status), then re-arm for the next fire time.
    async fn fire_and_rearm(self: Arc<Self>, uid: &str) {
        // Drop the timer entry before doing work — we own the wakeup, not
        // the entry. A subsequent watch update is allowed to re-arm us.
        self.timers.lock().await.remove(uid);

        // The CronJob may have been deleted/suspended during the wait.
        // We re-discover it by listing — UID lookup needs no special
        // index, the CronJob population per cluster is small.
        let listing = match self
            .db
            .list_resources(
                "batch/v1",
                "CronJob",
                None,
                crate::datastore::ResourceListQuery::all(),
            )
            .await
        {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!("CronJob fire {}: list failed: {:#}", uid, e);
                return;
            }
        };
        let Some(resource) = listing
            .items
            .into_iter()
            .find(|r| r.data.pointer("/metadata/uid").and_then(|v| v.as_str()) == Some(uid))
        else {
            // CronJob is gone — orphan fire suppressed.
            return;
        };

        // Suspended? Skip the fire but DO NOT re-arm. A future watch
        // event with suspend=false will re-arm.
        if resource
            .data
            .pointer("/spec/suspend")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            return;
        }

        // DeletionTimestamp set? Skip the fire and don't re-arm.
        if resource
            .data
            .pointer("/metadata/deletionTimestamp")
            .is_some()
        {
            return;
        }

        if let Err(e) = crate::controllers::cronjob::reconcile_cronjob_one(
            self.db.as_ref(),
            Some(self.dispatcher.as_ref()),
            &resource.data,
            resource.resource_version,
        )
        .await
        {
            tracing::warn!("CronJob fire {}: reconcile failed: {:#}", uid, e);
        }

        // Re-arm for the next fire. Re-read the CronJob via list again so
        // we pick up any status mutation our own reconcile produced
        // (lastScheduleTime moves forward).
        let refreshed = match self
            .db
            .list_resources(
                "batch/v1",
                "CronJob",
                None,
                crate::datastore::ResourceListQuery::all(),
            )
            .await
        {
            Ok(l) => l
                .items
                .into_iter()
                .find(|r| r.data.pointer("/metadata/uid").and_then(|v| v.as_str()) == Some(uid)),
            Err(e) => {
                tracing::warn!("CronJob fire {}: list-after failed: {:#}", uid, e);
                None
            }
        };
        if let Some(r) = refreshed {
            // arm_owned returns a pre-boxed Send future to break the
            // mutually-recursive async type chain
            // arm -> spawn_delay -> fire_and_rearm -> arm.
            Arc::clone(&self).arm_owned(r.data).await;
        }
    }

    /// Owned-data variant of `arm` for the recursive re-arm path. Takes
    /// `Arc<Value>` so the returned future is `'static` and can be boxed.
    /// Returns a boxed pinned future so the caller can `.await` it
    /// without re-introducing the mutually-recursive async type chain
    /// `arm -> spawn_delay -> fire_and_rearm -> arm`.
    fn arm_owned(
        self: Arc<Self>,
        cj: Arc<Value>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        Box::pin(async move {
            // Re-use the borrow-based arm for the actual logic.
            self.arm(&cj).await;
        })
    }

    /// Watch loop. Subscribes to the datastore watch, filters to
    /// `batch/v1`/`CronJob`, and arms / cancels timers in response.
    /// Runs until `cancel` fires.
    pub async fn run_watch_loop(self: Arc<Self>, cancel: CancellationToken) {
        let topic = WatchTopic::new("batch/v1", "CronJob");
        let mut cursor = SignalWatchCursor::new(
            self.db.subscribe_watch_signals(topic.clone()),
            DatastoreWatchReplaySource::new(
                self.db.clone(),
                vec![WatchTarget::namespaced("batch/v1", "CronJob")],
            ),
            topic,
            WatchDeliveryScope::NamespacedAll,
            self.db.get_current_resource_version().await.unwrap_or(0),
            WindowPolicy::default_watch_delivery(),
        );
        if let Err(e) = self.startup_walk().await {
            tracing::warn!("cronjob_scheduler: startup walk after watch subscribe failed: {e:#}");
        }
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                msg = cursor.next_event() => {
                    match msg {
                        Ok(event) => {
                            if !is_cronjob_event(&event) {
                                continue;
                            }
                            self.handle_watch_event(event).await;
                        }
                        Err(WatchCursorError::Expired) => {
                            tracing::warn!(
                                "cronjob_scheduler: replay window expired; \
                                 reconciling all CronJobs to recover state"
                            );
                            if let Err(e) = self.startup_walk().await {
                                tracing::warn!(
                                    "cronjob_scheduler: re-walk after expired replay failed: {:#}",
                                    e
                                );
                            }
                        }
                        Err(WatchCursorError::Replay(err)) => {
                            tracing::warn!("cronjob_scheduler: watch replay failed: {err:#}");
                        }
                        Err(WatchCursorError::Closed) => break,
                    }
                }
            }
        }
    }

    async fn handle_watch_event(self: &Arc<Self>, event: WatchEvent) {
        let uid_opt = event
            .object
            .pointer("/metadata/uid")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        match event.event_type {
            EventType::Deleted => {
                if let Some(uid) = uid_opt {
                    self.cancel(&uid).await;
                }
            }
            EventType::Added | EventType::Modified => {
                // arm() handles uid extraction + suspension + deletionTimestamp.
                Arc::clone(self).arm(&event.object).await;
            }
            EventType::Bookmark | EventType::Error => {}
        }
    }
}

fn is_cronjob_event(event: &WatchEvent) -> bool {
    let api = event
        .object
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let kind = event
        .object
        .get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    api == "batch/v1" && kind == "CronJob"
}

/// Compute the next fire time for a CronJob given `now`.
///
/// Returns:
/// - `Ok(Some(t))` for a future or immediately-due fire.
/// - `Ok(None)` if there is no fire to schedule (invalid schedule;
///   missed-fire window exceeded `startingDeadlineSeconds`).
/// - `Err(_)` only on programmer errors (currently unreachable; reserved
///   for future stricter parsing).
pub fn compute_next_fire(cj: &Value, now: DateTime<Utc>) -> Result<Option<DateTime<Utc>>> {
    let schedule_str = match cj.pointer("/spec/schedule").and_then(|v| v.as_str()) {
        Some(s) => s,
        None => return Ok(None),
    };

    let schedule = match crate::controllers::cronjob::parse_cron_schedule(schedule_str) {
        Ok(s) => s,
        Err(_) => return Ok(None),
    };

    if let Some(due) =
        crate::controllers::cronjob::most_recent_cronjob_schedule_time(cj, now, &schedule, true)?
    {
        return Ok(Some(if due <= now { now } else { due }));
    }

    let reference = match crate::controllers::cronjob::most_recent_cronjob_schedule_time(
        cj, now, &schedule, false,
    )? {
        Some(most_recent_without_deadline) => most_recent_without_deadline,
        None => crate::controllers::cronjob::cronjob_schedule_lower_bound(cj, now, false),
    };

    Ok(schedule.after(&reference).next())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cj(uid: &str, schedule: &str) -> Value {
        json!({
            "apiVersion": "batch/v1",
            "kind": "CronJob",
            "metadata": {"name": uid, "namespace": "default", "uid": uid},
            "spec": {
                "schedule": schedule,
                "jobTemplate": {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [{"name": "c", "image": "busybox"}],
                                "restartPolicy": "Never"
                            }
                        }
                    }
                }
            }
        })
    }

    #[test]
    fn compute_next_fire_basic_every_minute() {
        let value = cj("u", "* * * * *");
        let now = Utc::now();
        let next = compute_next_fire(&value, now).unwrap().unwrap();
        // Next fire is within ~60s of now.
        assert!((next - now).num_seconds() <= 60);
    }

    #[test]
    fn compute_next_fire_invalid_schedule_returns_none() {
        let value = cj("u", "not a cron expr");
        let next = compute_next_fire(&value, Utc::now()).unwrap();
        assert!(next.is_none());
    }

    #[test]
    fn compute_next_fire_missed_within_starting_deadline_fires_immediately() {
        // last_schedule_time was 5 minutes ago; schedule fires every minute.
        // startingDeadlineSeconds=600 means a missed fire is recoverable.
        let mut value = cj("u", "* * * * *");
        let last = Utc::now() - chrono::Duration::minutes(5);
        value["status"] = json!({ "lastScheduleTime": last.to_rfc3339() });
        value["spec"]["startingDeadlineSeconds"] = json!(600);

        let now = Utc::now();
        let next = compute_next_fire(&value, now).unwrap().unwrap();
        assert!(next <= now, "missed fire within deadline must fire now");
    }

    #[test]
    fn compute_next_fire_outside_deadline_does_not_backfire_past_schedules() {
        // last_schedule_time was 1 hour ago; schedule fires every minute.
        // startingDeadlineSeconds=10 means missed fires older than 10s
        // are skipped — but the NEXT future fire still arms normally.
        let mut value = cj("u", "* * * * *");
        let last = Utc::now() - chrono::Duration::hours(1);
        value["status"] = json!({ "lastScheduleTime": last.to_rfc3339() });
        value["spec"]["startingDeadlineSeconds"] = json!(10);

        let now = Utc::now();
        let next = compute_next_fire(&value, now).unwrap().unwrap();
        // The next fire MUST be in the future or within the 10s deadline
        // window — it must NOT be the original 1h-ago candidate.
        assert!(
            (next - now).num_seconds() >= -10,
            "outside-deadline catch-up suppressed: returned {:?}, expected within ±60s of now",
            next
        );
        assert!(
            (next - now).num_seconds() <= 60,
            "next future fire is at most one schedule-period away"
        );
    }

    #[test]
    fn compute_next_fire_no_status_arms_within_one_period() {
        // Brand-new CronJob with no status. compute_next_fire either
        // returns the next minute boundary (future) or `now` (when the
        // most recent schedule match is in the past and there is no
        // startingDeadlineSeconds — the controller will then run the
        // catch-up reconcile inline, which is idempotent and safe).
        let value = cj("u", "* * * * *");
        let now = Utc::now();
        let next = compute_next_fire(&value, now).unwrap().unwrap();
        let delta = (next - now).num_seconds();
        assert!(
            (-1..=60).contains(&delta),
            "next fire must be within ±60s of now (period of `* * * * *`); got delta={delta}s"
        );
    }

    #[test]
    fn compute_next_fire_respects_creation_timestamp_for_new_cronjob() {
        let mut value = cj("u", "* * * * *");
        let now = chrono::DateTime::parse_from_rfc3339("2026-05-16T19:22:10Z")
            .unwrap()
            .with_timezone(&Utc);
        value["metadata"]["creationTimestamp"] = json!("2026-05-16T19:22:10Z");

        let next = compute_next_fire(&value, now).unwrap().unwrap();

        assert_eq!(
            next,
            chrono::DateTime::parse_from_rfc3339("2026-05-16T19:23:00Z")
                .unwrap()
                .with_timezone(&Utc),
            "a new CronJob must not backfill a schedule before creationTimestamp"
        );
    }
}
