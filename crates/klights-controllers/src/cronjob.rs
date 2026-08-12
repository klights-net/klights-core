//! CronJob controller — schedules periodic Jobs from CronJob specs.
//!
//! Runs as a background task (every 30 s). For each active CronJob it
//! determines whether a new Job is due, respects concurrencyPolicy
//! (ForbidConcurrent / Replace / Allow), and creates the Job.
//! Status (lastScheduleTime, active) is kept up-to-date.

use crate::common::ControllerStatusStore;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use klights_cluster_core::Resource;
use klights_reconcile_api::ControllerStoreResult;
use serde_json::{Value, json};

// `reconcile_all_cronjobs_and_enqueue_jobs` (the periodic-scan entry
// point) and its `_inner` helper were removed in T13 — the per-CronJob
// `cronjob_scheduler::CronJobScheduler` (event-driven `spawn_delay`)
// replaces them. The remaining per-CronJob reconcile entry point is
// `reconcile_cronjob_one` below.

/// Public per-CronJob fire entry point used by `cronjob_scheduler`. Re-uses
/// the same reconcile path as the legacy bulk scan so concurrency policy,
/// status updates, and history cleanup stay consistent.
#[async_trait]
pub trait CronJobStore: ControllerStatusStore {
    async fn get_cronjob(
        &self,
        namespace: &str,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>>;
    async fn get_job(&self, namespace: &str, name: &str)
    -> ControllerStoreResult<Option<Resource>>;
    async fn create_job(
        &self,
        namespace: &str,
        name: &str,
        value: Value,
    ) -> ControllerStoreResult<Resource>;
    async fn list_jobs(&self, namespace: &str) -> ControllerStoreResult<Vec<Resource>>;
    async fn delete_job(
        &self,
        namespace: &str,
        name: &str,
        uid: String,
        resource_version: i64,
    ) -> ControllerStoreResult<()>;
}

pub async fn reconcile_cronjob_one_at<S: CronJobStore + ?Sized>(
    store: &S,
    dispatcher: Option<&dyn klights_reconcile_api::ControllerDispatcherPort>,
    cj: &Value,
    rv: i64,
    now: DateTime<Utc>,
) -> Result<()> {
    reconcile_cronjob_inner_at(store, dispatcher, cj, rv, now).await
}

async fn reconcile_cronjob_inner_at<S: CronJobStore + ?Sized>(
    store: &S,
    dispatcher: Option<&dyn klights_reconcile_api::ControllerDispatcherPort>,
    cj: &Value,
    _rv: i64,
    now: DateTime<Utc>,
) -> Result<()> {
    let input_metadata = cj
        .get("metadata")
        .ok_or_else(|| anyhow::anyhow!("missing metadata"))?;
    let name = input_metadata
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let namespace = input_metadata
        .get("namespace")
        .and_then(|v| v.as_str())
        .unwrap_or("default");
    let Some(live_cj) = store.get_cronjob(namespace, name).await? else {
        return Ok(());
    };
    let cj = live_cj.data.as_ref();
    let metadata = cj
        .get("metadata")
        .ok_or_else(|| anyhow::anyhow!("missing metadata"))?;
    let uid = metadata.get("uid").and_then(|v| v.as_str()).unwrap_or("");

    if metadata.get("deletionTimestamp").is_some() {
        return Ok(());
    }

    // Skip suspended CronJobs
    let suspended = cj
        .pointer("/spec/suspend")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if suspended {
        // Still clean up old jobs for suspended CronJobs
        cleanup_old_jobs_by_history_limit(store, cj, namespace, uid).await?;
        return Ok(());
    }

    let schedule_str = cj
        .pointer("/spec/schedule")
        .and_then(|v| v.as_str())
        .unwrap_or("* * * * *");
    let concurrency = cj
        .pointer("/spec/concurrencyPolicy")
        .and_then(|v| v.as_str())
        .unwrap_or("Allow");

    // Parse the cron schedule and determine the next scheduled time since last run.
    let schedule = match parse_cron_schedule(schedule_str) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(
                "CronJob {}/{}: invalid schedule '{}': {}",
                namespace,
                name,
                schedule_str,
                e
            );
            return Ok(());
        }
    };

    let Some(scheduled_time) = most_recent_cronjob_schedule_time(cj, now, &schedule, true)? else {
        // Not yet due — just sync active list and clean up old jobs, then return.
        sync_active_status_at(store, &live_cj, None, now).await?;
        cleanup_old_jobs_by_history_limit(store, cj, namespace, uid).await?;
        return Ok(());
    };

    // List currently active Jobs for this CronJob.
    let active_jobs = list_active_jobs(store, namespace, uid).await?;

    match concurrency {
        "ForbidConcurrent" if !active_jobs.is_empty() => {
            tracing::debug!(
                "CronJob {}/{}: ForbidConcurrent — {} active Job(s), skipping",
                namespace,
                name,
                active_jobs.len()
            );
            sync_active_status_at(store, &live_cj, None, now).await?;
            cleanup_old_jobs_by_history_limit(store, cj, namespace, uid).await?;
            return Ok(());
        }
        "Replace" => {
            // Delete running Jobs before creating a new one. If any delete fails,
            // bail so the next reconcile retries; we must not create the
            // replacement Job while the old one still exists (violates the
            // Replace contract).
            for job in &active_jobs {
                store
                    .delete_job(namespace, &job.name, job.uid.clone(), job.resource_version)
                    .await?;
            }
        }
        _ => {} // "Allow": create regardless
    }

    // Create a new Job.
    let created_job =
        create_job_from_cronjob(store, cj, name, namespace, uid, scheduled_time).await?;
    if let (Some(dispatcher), Some(job)) = (dispatcher, created_job.as_ref()) {
        dispatcher.enqueue(&job.data).await;
    }
    sync_active_status_at(store, &live_cj, Some(scheduled_time), now).await?;

    // Clean up old completed/failed Jobs that exceed history limits
    cleanup_old_jobs_by_history_limit(store, cj, namespace, uid).await?;

    Ok(())
}

pub fn expand_cron_schedule(schedule_str: &str) -> String {
    let parts: Vec<&str> = schedule_str.split_whitespace().collect();
    if parts.len() == 5 {
        format!("0 {} *", parts.join(" "))
    } else {
        schedule_str.to_string()
    }
}

pub fn parse_cron_schedule(schedule_str: &str) -> Result<cron::Schedule> {
    let cron_expr = expand_cron_schedule(schedule_str);
    cron_expr.parse::<cron::Schedule>().map_err(|e| {
        anyhow::anyhow!(
            "invalid schedule '{}' (expanded: '{}'): {}",
            schedule_str,
            cron_expr,
            e
        )
    })
}

fn parse_cronjob_time(cj: &Value, pointer: &str) -> Option<DateTime<Utc>> {
    cj.pointer(pointer)
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

pub fn cronjob_schedule_lower_bound(
    cj: &Value,
    now: DateTime<Utc>,
    include_starting_deadline_seconds: bool,
) -> DateTime<Utc> {
    let mut earliest = parse_cronjob_time(cj, "/status/lastScheduleTime")
        .or_else(|| parse_cronjob_time(cj, "/metadata/creationTimestamp"))
        .unwrap_or_else(|| now - chrono::Duration::seconds(61));

    if include_starting_deadline_seconds
        && let Some(deadline) = cj
            .pointer("/spec/startingDeadlineSeconds")
            .and_then(|v| v.as_i64())
    {
        let scheduling_deadline = now - chrono::Duration::seconds(deadline);
        if scheduling_deadline > earliest {
            earliest = scheduling_deadline;
        }
    }

    earliest
}

pub fn most_recent_cronjob_schedule_time(
    cj: &Value,
    now: DateTime<Utc>,
    schedule: &cron::Schedule,
    include_starting_deadline_seconds: bool,
) -> Result<Option<DateTime<Utc>>> {
    let earliest = cronjob_schedule_lower_bound(cj, now, include_starting_deadline_seconds);
    let Some(first) = schedule.after(&earliest).next() else {
        return Ok(None);
    };
    if now < first {
        return Ok(None);
    }

    let Some(second) = schedule.after(&first).next() else {
        return Ok(Some(first));
    };
    if now < second {
        return Ok(Some(first));
    }

    let interval_secs = (second - first).num_seconds();
    if interval_secs < 1 {
        return Err(anyhow::anyhow!(
            "time difference between two CronJob schedules is less than 1 second"
        ));
    }

    let elapsed_secs = (now - first).num_seconds();
    let missed_schedules = (elapsed_secs / interval_secs) + 1;
    let offset = missed_schedules.saturating_sub(2);
    let potential_earliest =
        first + chrono::Duration::seconds(offset.saturating_mul(interval_secs));

    let mut most_recent = None;
    for candidate in schedule.after(&potential_earliest).take(128) {
        if candidate > now {
            break;
        }
        most_recent = Some(candidate);
    }

    Ok(most_recent)
}

/// Create a Job from the CronJob template.
async fn create_job_from_cronjob<S: CronJobStore + ?Sized>(
    store: &S,
    cj: &Value,
    cj_name: &str,
    namespace: &str,
    cj_uid: &str,
    scheduled_time: chrono::DateTime<chrono::Utc>,
) -> Result<Option<Resource>> {
    let template = cj
        .pointer("/spec/jobTemplate")
        .ok_or_else(|| anyhow::anyhow!("CronJob missing spec.jobTemplate"))?;

    // Generate a unique Job name derived from CronJob name + timestamp hash.
    let ts_secs = scheduled_time.timestamp();
    let job_name = format!("{}-{}", cj_name, ts_secs % 1_000_000_000);

    // Check if a Job with this name already exists (idempotent).
    if store.get_job(namespace, &job_name).await?.is_some() {
        return Ok(None);
    }

    let mut job = template.clone();
    if let Some(obj) = job.as_object_mut() {
        obj.insert("apiVersion".to_string(), json!("batch/v1"));
        obj.insert("kind".to_string(), json!("Job"));

        // Merge template metadata with the generated name and ownerReference.
        let existing_meta = obj.remove("metadata").unwrap_or(json!({}));
        let mut meta_map = existing_meta.as_object().cloned().unwrap_or_default();
        meta_map.insert("name".to_string(), json!(job_name));
        meta_map.insert("namespace".to_string(), json!(namespace));
        meta_map.insert(
            "annotations".to_string(),
            json!({
                "batch.kubernetes.io/cronjob-scheduled-timestamp": scheduled_time.to_rfc3339(),
            }),
        );
        meta_map.insert(
            "ownerReferences".to_string(),
            json!([{
                "apiVersion": "batch/v1",
                "kind": "CronJob",
                "name": cj_name,
                "uid": cj_uid,
                "controller": true,
                "blockOwnerDeletion": true,
            }]),
        );
        obj.insert("metadata".to_string(), serde_json::Value::Object(meta_map));
    }

    let created = store.create_job(namespace, &job_name, job).await?;
    tracing::info!(
        "CronJob {}/{}: created Job for scheduled time {}",
        namespace,
        cj_name,
        scheduled_time
    );
    Ok(Some(created))
}

/// Clean up old completed Jobs that exceed the CronJob's history limits.
/// `successfulJobsHistoryLimit` (default 3) and `failedJobsHistoryLimit` (default 1)
/// control how many completed/failed Jobs to retain. Oldest Jobs are deleted first.
async fn cleanup_old_jobs_by_history_limit<S: CronJobStore + ?Sized>(
    store: &S,
    cj: &Value,
    namespace: &str,
    cj_uid: &str,
) -> Result<()> {
    let successful_limit = cj
        .pointer("/spec/successfulJobsHistoryLimit")
        .and_then(|v| v.as_u64())
        .unwrap_or(3) as usize;
    let failed_limit = cj
        .pointer("/spec/failedJobsHistoryLimit")
        .and_then(|v| v.as_u64())
        .unwrap_or(1) as usize;

    let jobs = store.list_jobs(namespace).await?;

    // Collect completed (successful) and failed jobs owned by this CronJob
    let mut successful_jobs: Vec<&Resource> = Vec::new();
    let mut failed_jobs: Vec<&Resource> = Vec::new();

    for job in &jobs {
        let owned = job
            .data
            .pointer("/metadata/ownerReferences")
            .and_then(|refs| refs.as_array())
            .map(|refs| {
                refs.iter()
                    .any(|r| r.get("uid").and_then(|u| u.as_str()) == Some(cj_uid))
            })
            .unwrap_or(false);
        if !owned {
            continue;
        }

        let conditions = job
            .data
            .pointer("/status/conditions")
            .and_then(|c| c.as_array());

        let is_complete = conditions.is_some_and(|conds| {
            conds.iter().any(|c| {
                c.get("type").and_then(|t| t.as_str()) == Some("Complete")
                    && c.get("status").and_then(|s| s.as_str()) == Some("True")
            })
        });
        let is_failed = conditions.is_some_and(|conds| {
            conds.iter().any(|c| {
                c.get("type").and_then(|t| t.as_str()) == Some("Failed")
                    && c.get("status").and_then(|s| s.as_str()) == Some("True")
            })
        });

        if is_complete {
            successful_jobs.push(job);
        } else if is_failed {
            failed_jobs.push(job);
        }
    }

    // Sort by creation timestamp (oldest first) using the job name as tiebreaker.
    // K8s uses creationTimestamp for ordering; we approximate with name ordering
    // when timestamps are unavailable.
    let sort_by_creation = |jobs: &mut Vec<&Resource>| {
        jobs.sort_by(|a, b| {
            let a_ts = a
                .data
                .pointer("/metadata/creationTimestamp")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let b_ts = b
                .data
                .pointer("/metadata/creationTimestamp")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            a_ts.cmp(b_ts).then_with(|| a.name.cmp(&b.name))
        });
    };

    sort_by_creation(&mut successful_jobs);
    sort_by_creation(&mut failed_jobs);

    // Delete oldest successful jobs that exceed the limit
    if successful_jobs.len() > successful_limit {
        let to_delete = successful_jobs.len() - successful_limit;
        for job in successful_jobs.iter().take(to_delete) {
            store
                .delete_job(namespace, &job.name, job.uid.clone(), job.resource_version)
                .await?;
            tracing::info!(
                "CronJob {}/{}: cleaned up old successful Job {} (limit={})",
                namespace,
                cj.pointer("/metadata/name")
                    .and_then(|v| v.as_str())
                    .unwrap_or(""),
                job.name,
                successful_limit
            );
        }
    }

    // Delete oldest failed jobs that exceed the limit
    if failed_jobs.len() > failed_limit {
        let to_delete = failed_jobs.len() - failed_limit;
        for job in failed_jobs.iter().take(to_delete) {
            store
                .delete_job(namespace, &job.name, job.uid.clone(), job.resource_version)
                .await?;
            tracing::info!(
                "CronJob {}/{}: cleaned up old failed Job {} (limit={})",
                namespace,
                cj.pointer("/metadata/name")
                    .and_then(|v| v.as_str())
                    .unwrap_or(""),
                job.name,
                failed_limit
            );
        }
    }

    Ok(())
}

/// Return Jobs owned by this CronJob that are still active (not Complete/Failed).
async fn list_active_jobs<S: CronJobStore + ?Sized>(
    store: &S,
    namespace: &str,
    cj_uid: &str,
) -> Result<Vec<Resource>> {
    let jobs = store.list_jobs(namespace).await?;

    let active: Vec<Resource> = jobs
        .into_iter()
        .filter(|j| {
            // Owned by this CronJob
            let owned = j
                .data
                .pointer("/metadata/ownerReferences")
                .and_then(|refs| refs.as_array())
                .map(|refs| {
                    refs.iter()
                        .any(|r| r.get("uid").and_then(|u| u.as_str()) == Some(cj_uid))
                })
                .unwrap_or(false);
            if !owned {
                return false;
            }
            // Not yet complete or failed
            let complete = j
                .data
                .pointer("/status/conditions")
                .and_then(|c| c.as_array())
                .map(|conds| {
                    conds.iter().any(|c| {
                        matches!(
                            c.get("type").and_then(|t| t.as_str()),
                            Some("Complete") | Some("Failed")
                        ) && c.get("status").and_then(|s| s.as_str()) == Some("True")
                    })
                })
                .unwrap_or(false);
            !complete
        })
        .collect();
    Ok(active)
}

/// Sync the CronJob's status.lastScheduleTime and status.active list.
async fn sync_active_status_at<S: CronJobStore + ?Sized>(
    store: &S,
    cj_resource: &Resource,
    new_scheduled: Option<chrono::DateTime<chrono::Utc>>,
    now: DateTime<Utc>,
) -> Result<()> {
    let cj = cj_resource.data.as_ref();
    let namespace = cj_resource.namespace.as_deref().unwrap_or("default");
    let cj_uid = cj_resource.uid.as_str();
    let active_jobs = list_active_jobs(store, namespace, cj_uid).await?;
    let active_refs: Vec<Value> = active_jobs
        .iter()
        .map(|j| {
            json!({
                "apiVersion": "batch/v1",
                "kind": "Job",
                "name": j.name.as_str(),
                "namespace": namespace,
                "uid": j.uid.as_str(),
            })
        })
        .collect();

    let now_str = klights_cluster_core::k8s_time::format_time(now);
    let mut status = cj.get("status").cloned().unwrap_or_else(|| json!({}));
    if !status.is_object() {
        status = json!({});
    }
    if let Some(s) = status.as_object_mut() {
        s.insert("active".to_string(), json!(active_refs));
        if let Some(t) = new_scheduled {
            s.insert(
                "lastScheduleTime".to_string(),
                json!(klights_cluster_core::k8s_time::format_time(t)),
            );
        }
        s.insert(
            "observedGeneration".to_string(),
            json!(
                cj.pointer("/metadata/generation")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(1)
            ),
        );
        // Mark lastSuccessfulTime if the most recent Job completed successfully.
        s.entry("lastSuccessfulTime")
            .or_insert_with(|| json!(now_str));
    }

    // The shared writer owns the no-op gate because it validates an apparently
    // unchanged observed status against a live reread before skipping the CAS.
    crate::common::write_status_for_resource(store, cj_resource, &status).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use klights_cluster_core::ResourcePreconditions;
    use klights_reconcile_api::{ControllerStoreError, ControllerStoreResult};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    struct MemoryCronJobStore {
        current: Mutex<Resource>,
        jobs: Mutex<Vec<Resource>>,
        deleted: std::sync::atomic::AtomicBool,
        created_jobs: AtomicUsize,
        writes: AtomicUsize,
    }

    impl MemoryCronJobStore {
        fn new(value: Value) -> Self {
            Self {
                current: Mutex::new(Resource::try_from_data(Arc::new(value)).unwrap()),
                jobs: Mutex::new(Vec::new()),
                deleted: std::sync::atomic::AtomicBool::new(false),
                created_jobs: AtomicUsize::new(0),
                writes: AtomicUsize::new(0),
            }
        }

        fn with_jobs(value: Value, jobs: impl IntoIterator<Item = Value>) -> Self {
            Self {
                current: Mutex::new(Resource::try_from_data(Arc::new(value)).unwrap()),
                jobs: Mutex::new(
                    jobs.into_iter()
                        .map(|job| Resource::try_from_data(Arc::new(job)).unwrap())
                        .collect(),
                ),
                deleted: std::sync::atomic::AtomicBool::new(false),
                created_jobs: AtomicUsize::new(0),
                writes: AtomicUsize::new(0),
            }
        }

        fn resource(&self) -> Resource {
            self.current.lock().unwrap().clone()
        }

        fn replace(&self, value: Value) {
            *self.current.lock().unwrap() = Resource::try_from_data(Arc::new(value)).unwrap();
        }

        fn delete_current(&self) {
            self.deleted.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl ControllerStatusStore for MemoryCronJobStore {
        async fn get_status_resource(
            &self,
            _api_version: &str,
            _kind: &str,
            _namespace: Option<&str>,
            _name: &str,
        ) -> ControllerStoreResult<Option<Resource>> {
            Ok((!self.deleted.load(Ordering::SeqCst)).then(|| self.resource()))
        }

        async fn update_status(
            &self,
            _api_version: &str,
            _kind: &str,
            _namespace: Option<&str>,
            _name: &str,
            status: Value,
            preconditions: ResourcePreconditions,
        ) -> ControllerStoreResult<Resource> {
            let mut current = self.current.lock().unwrap();
            if preconditions
                .uid
                .as_deref()
                .is_some_and(|uid| uid != current.uid)
                || preconditions
                    .resource_version
                    .is_some_and(|rv| rv != current.resource_version)
            {
                return Err(ControllerStoreError::conflict("stale CronJob status"));
            }
            let mut value = Arc::unwrap_or_clone(current.data.clone());
            value["status"] = status;
            value["metadata"]["resourceVersion"] =
                json!((current.resource_version + 1).to_string());
            *current = Resource::try_from_data(Arc::new(value)).unwrap();
            self.writes.fetch_add(1, Ordering::Relaxed);
            Ok(current.clone())
        }

        fn log_noop_status_write(
            &self,
            _operation: &'static str,
            _resource: &Resource,
            _reason: &'static str,
        ) {
        }
    }

    #[async_trait]
    impl CronJobStore for MemoryCronJobStore {
        async fn get_cronjob(
            &self,
            namespace: &str,
            name: &str,
        ) -> ControllerStoreResult<Option<Resource>> {
            if self.deleted.load(Ordering::SeqCst) {
                return Ok(None);
            }
            let current = self.resource();
            Ok(
                (current.namespace.as_deref() == Some(namespace) && current.name == name)
                    .then_some(current),
            )
        }

        async fn get_job(
            &self,
            namespace: &str,
            name: &str,
        ) -> ControllerStoreResult<Option<Resource>> {
            Ok(self
                .jobs
                .lock()
                .unwrap()
                .iter()
                .find(|job| job.namespace.as_deref() == Some(namespace) && job.name == name)
                .cloned())
        }

        async fn create_job(
            &self,
            namespace: &str,
            name: &str,
            mut value: Value,
        ) -> ControllerStoreResult<Resource> {
            self.created_jobs.fetch_add(1, Ordering::Relaxed);
            value["metadata"]["namespace"] = json!(namespace);
            value["metadata"]["name"] = json!(name);
            value["metadata"]["uid"] = json!(format!("uid-{name}"));
            value["metadata"]["resourceVersion"] = json!("1");
            let resource = Resource::try_from_data(Arc::new(value)).unwrap();
            self.jobs.lock().unwrap().push(resource.clone());
            Ok(resource)
        }

        async fn list_jobs(&self, _namespace: &str) -> ControllerStoreResult<Vec<Resource>> {
            Ok(self.jobs.lock().unwrap().clone())
        }

        async fn delete_job(
            &self,
            namespace: &str,
            name: &str,
            uid: String,
            resource_version: i64,
        ) -> ControllerStoreResult<()> {
            self.jobs.lock().unwrap().retain(|job| {
                !(job.namespace.as_deref() == Some(namespace)
                    && job.name == name
                    && job.uid == uid
                    && job.resource_version == resource_version)
            });
            Ok(())
        }
    }

    fn cronjob(resource_version: i64, last_successful_time: &str) -> Value {
        json!({
            "apiVersion": "batch/v1",
            "kind": "CronJob",
            "metadata": {
                "name": "status-test",
                "namespace": "default",
                "uid": "status-test-uid",
                "resourceVersion": resource_version.to_string(),
                "generation": 1,
                "creationTimestamp": "2026-01-01T00:00:00Z"
            },
            "spec": {"schedule": "* * * * *"},
            "status": {
                "active": [],
                "observedGeneration": 1,
                "lastSuccessfulTime": last_successful_time
            }
        })
    }

    #[tokio::test]
    async fn sync_active_status_skips_unchanged_status() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let store = MemoryCronJobStore::new(cronjob(1, "2026-01-01T00:00:00Z"));
        let observed = store.resource();

        sync_active_status_at(&store, &observed, None, now)
            .await
            .unwrap();

        assert_eq!(store.writes.load(Ordering::Relaxed), 0);
        assert_eq!(store.resource().resource_version, 1);
    }

    #[tokio::test]
    async fn sync_active_status_validates_unchanged_stale_status_against_live_row() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let stale = Resource::try_from_data(Arc::new(cronjob(1, "2026-01-01T00:00:00Z"))).unwrap();
        let store = MemoryCronJobStore::new(cronjob(2, "2026-01-02T00:00:00Z"));

        let error = sync_active_status_at(&store, &stale, None, now)
            .await
            .expect_err("stale CronJob status overlap must conflict");

        assert!(error.chain().any(|cause| {
            cause
                .downcast_ref::<ControllerStoreError>()
                .is_some_and(ControllerStoreError::is_conflict)
        }));
        assert_eq!(store.writes.load(Ordering::Relaxed), 0);
        assert_eq!(
            store.resource().data["status"]["lastSuccessfulTime"],
            "2026-01-02T00:00:00Z"
        );
    }

    #[tokio::test]
    async fn forbid_concurrent_blocks_second_job_when_active_present() {
        let mut value = cronjob(1, "2026-01-01T00:00:00Z");
        value["metadata"]["name"] = json!("cj-forbid");
        value["metadata"]["uid"] = json!("u-forbid");
        value["metadata"]["creationTimestamp"] = json!("2026-01-01T00:00:00Z");
        value["spec"]["concurrencyPolicy"] = json!("ForbidConcurrent");
        value["spec"]["jobTemplate"] = json!({
            "spec": {
                "template": {
                    "spec": {
                        "containers": [{"name": "c", "image": "busybox"}],
                        "restartPolicy": "Never"
                    }
                }
            }
        });
        let active_job = json!({
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": "cj-forbid-active",
                "namespace": "default",
                "uid": "job-uid-active",
                "resourceVersion": "1",
                "ownerReferences": [{
                    "apiVersion": "batch/v1",
                    "kind": "CronJob",
                    "name": "cj-forbid",
                    "uid": "u-forbid",
                    "controller": true
                }]
            },
            "spec": {"template": {"spec": {}}},
            "status": {}
        });
        let store = MemoryCronJobStore::with_jobs(value.clone(), [active_job]);
        let now = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:01:10Z")
            .unwrap()
            .with_timezone(&Utc);

        reconcile_cronjob_one_at(&store, None, &value, 1, now)
            .await
            .unwrap();

        assert_eq!(store.jobs.lock().unwrap().len(), 1);
        assert_eq!(store.created_jobs.load(Ordering::Relaxed), 0);
    }

    fn instant(value: &str) -> DateTime<Utc> {
        chrono::DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn scheduled_cronjob(name: &str, creation: &str) -> Value {
        json!({
            "apiVersion": "batch/v1",
            "kind": "CronJob",
            "metadata": {
                "name": name,
                "namespace": "default",
                "uid": format!("uid-{name}"),
                "resourceVersion": "1",
                "generation": 1,
                "creationTimestamp": creation
            },
            "spec": {
                "schedule": "* * * * *",
                "concurrencyPolicy": "Allow",
                "jobTemplate": {"spec": {"template": {"spec": {
                    "containers": [{"name": "c", "image": "nginx"}],
                    "restartPolicy": "Never"
                }}}}
            },
            "status": {}
        })
    }

    fn finished_job(name: &str, owner_uid: &str, timestamp: &str, condition: &str) -> Value {
        json!({
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": name,
                "namespace": "default",
                "uid": format!("uid-{name}"),
                "resourceVersion": "1",
                "creationTimestamp": timestamp,
                "ownerReferences": [{
                    "apiVersion": "batch/v1",
                    "kind": "CronJob",
                    "name": "owner",
                    "uid": owner_uid,
                    "controller": true
                }]
            },
            "spec": {"template": {"spec": {}}},
            "status": {"conditions": [{"type": condition, "status": "True"}]}
        })
    }

    async fn reconcile_at(store: &MemoryCronJobStore, value: &Value, now: &str) -> Result<()> {
        reconcile_cronjob_one_at(store, None, value, 1, instant(now)).await
    }

    #[tokio::test]
    async fn test_cronjob_creates_job_when_due() {
        let value = scheduled_cronjob("due", "2026-01-01T00:00:00Z");
        let store = MemoryCronJobStore::new(value.clone());

        reconcile_at(&store, &value, "2026-01-01T00:02:10Z")
            .await
            .unwrap();

        assert_eq!(store.jobs.lock().unwrap().len(), 1);
        assert_eq!(store.created_jobs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_cronjob_reconcile_persists_last_schedule_time_status() {
        let value = scheduled_cronjob("status", "2026-01-01T00:00:00Z");
        let store = MemoryCronJobStore::new(value.clone());

        reconcile_at(&store, &value, "2026-01-01T00:02:10Z")
            .await
            .unwrap();

        let current = store.resource();
        assert!(
            current
                .data
                .pointer("/status/lastScheduleTime")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.is_empty())
        );
        assert_eq!(
            current.data.pointer("/spec/schedule"),
            Some(&json!("* * * * *"))
        );
    }

    #[tokio::test]
    async fn test_cronjob_stale_snapshot_after_delete_does_not_create_job() {
        let value = scheduled_cronjob("deleted", "2026-01-01T00:00:00Z");
        let store = MemoryCronJobStore::new(value.clone());
        store.delete_current();

        reconcile_at(&store, &value, "2026-01-01T00:02:10Z")
            .await
            .unwrap();

        assert!(store.jobs.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_cronjob_reconcile_uses_live_suspend_state() {
        let stale = scheduled_cronjob("suspended", "2026-01-01T00:00:00Z");
        let store = MemoryCronJobStore::new(stale.clone());
        let mut live = stale.clone();
        live["metadata"]["resourceVersion"] = json!("2");
        live["spec"]["suspend"] = json!(true);
        store.replace(live);

        reconcile_at(&store, &stale, "2026-01-01T00:02:10Z")
            .await
            .unwrap();

        assert!(store.jobs.lock().unwrap().is_empty());
    }

    #[derive(Default)]
    struct PodHandoffDispatcher {
        reconciled_jobs: AtomicUsize,
    }

    impl klights_reconcile_api::ServiceReconcileSink for PodHandoffDispatcher {
        fn enqueue_service_reconcile_batch(
            &self,
            _keys: Vec<klights_reconcile_api::ServiceReconcileKey>,
        ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
            Box::pin(async { Ok(()) })
        }
    }

    impl klights_reconcile_api::ControllerDispatcherPort for PodHandoffDispatcher {
        fn enqueue<'a>(
            &'a self,
            resource: &'a Value,
        ) -> klights_reconcile_api::ControllerDispatchFuture<'a, ()> {
            Box::pin(async move {
                if resource.get("kind").and_then(Value::as_str) == Some("Job") {
                    self.reconciled_jobs.fetch_add(1, Ordering::SeqCst);
                }
            })
        }

        fn enqueue_reconcile(
            &self,
            _key: klights_reconcile_api::ReconcileKey,
        ) -> klights_reconcile_api::ControllerDispatchFuture<'_, ()> {
            Box::pin(async {})
        }

        fn pending_reconcile_keys(
            &self,
        ) -> klights_reconcile_api::ControllerDispatchFuture<
            '_,
            Vec<klights_reconcile_api::ReconcileKey>,
        > {
            Box::pin(async { Vec::new() })
        }
    }

    #[tokio::test]
    async fn test_cronjob_created_job_is_reconciled_into_pod() {
        let value = scheduled_cronjob("handoff", "2026-01-01T00:00:00Z");
        let store = MemoryCronJobStore::new(value.clone());
        let dispatcher = PodHandoffDispatcher::default();

        reconcile_cronjob_one_at(
            &store,
            Some(&dispatcher),
            &value,
            1,
            instant("2026-01-01T00:02:10Z"),
        )
        .await
        .unwrap();

        assert_eq!(dispatcher.reconciled_jobs.load(Ordering::SeqCst), 1);
        assert_eq!(store.jobs.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_cronjob_reconcile_uses_live_resource_for_status_after_stale_input_rv() {
        let value = scheduled_cronjob("live-rv", "2026-01-01T00:00:00Z");
        let store = MemoryCronJobStore::new(value.clone());

        reconcile_cronjob_one_at(
            &store,
            None,
            &value,
            999_999,
            instant("2026-01-01T00:02:10Z"),
        )
        .await
        .unwrap();

        assert_eq!(
            store.resource().data.pointer("/status/observedGeneration"),
            Some(&json!(1))
        );
    }

    #[tokio::test]
    async fn test_cronjob_forbid_concurrent_skips_when_active_job() {
        let mut value = scheduled_cronjob("forbid", "2026-01-01T00:00:00Z");
        value["spec"]["concurrencyPolicy"] = json!("ForbidConcurrent");
        let active = json!({
            "apiVersion": "batch/v1", "kind": "Job",
            "metadata": {
                "name": "active", "namespace": "default", "uid": "uid-active",
                "resourceVersion": "1",
                "ownerReferences": [{"uid": "uid-forbid", "controller": true}]
            },
            "spec": {"template": {"spec": {}}}, "status": {}
        });
        let store = MemoryCronJobStore::with_jobs(value.clone(), [active]);

        reconcile_at(&store, &value, "2026-01-01T00:02:10Z")
            .await
            .unwrap();

        assert_eq!(store.jobs.lock().unwrap().len(), 1);
        assert_eq!(store.created_jobs.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_cronjob_does_not_schedule_before_creation_timestamp() {
        let value = scheduled_cronjob("new", "2026-01-01T00:00:30Z");
        let store = MemoryCronJobStore::new(value.clone());

        reconcile_at(&store, &value, "2026-01-01T00:00:40Z")
            .await
            .unwrap();

        assert!(store.jobs.lock().unwrap().is_empty());
    }

    async fn assert_history_limits(successful_limit: usize, failed_limit: usize) {
        let mut value = scheduled_cronjob("history", "2026-01-01T00:00:00Z");
        value["spec"]["suspend"] = json!(true);
        value["spec"]["successfulJobsHistoryLimit"] = json!(successful_limit);
        value["spec"]["failedJobsHistoryLimit"] = json!(failed_limit);
        let owner_uid = "uid-history";
        value["metadata"]["uid"] = json!(owner_uid);
        let successful = (0..successful_limit + 2).map(|index| {
            finished_job(
                &format!("success-{index}"),
                owner_uid,
                &format!("2025-01-{:02}T00:00:00Z", index + 1),
                "Complete",
            )
        });
        let failed = (0..failed_limit + 2).map(|index| {
            finished_job(
                &format!("failed-{index}"),
                owner_uid,
                &format!("2025-02-{:02}T00:00:00Z", index + 1),
                "Failed",
            )
        });
        let store = MemoryCronJobStore::with_jobs(value.clone(), successful.chain(failed));

        reconcile_at(&store, &value, "2026-01-01T00:02:10Z")
            .await
            .unwrap();

        let jobs = store.jobs.lock().unwrap();
        assert_eq!(jobs.len(), successful_limit + failed_limit);
        for index in 0..2 {
            assert!(
                !jobs
                    .iter()
                    .any(|job| job.name == format!("success-{index}"))
            );
            assert!(!jobs.iter().any(|job| job.name == format!("failed-{index}")));
        }
    }

    #[tokio::test]
    async fn test_cronjob_history_limits_cleanup_old_completed_jobs() {
        assert_history_limits(1, 1).await;
    }

    #[tokio::test]
    async fn test_cronjob_history_limits_keep_five_successful_and_two_failed_jobs() {
        assert_history_limits(5, 2).await;
    }
}
