//! CronJob controller — schedules periodic Jobs from CronJob specs.
//!
//! Runs as a background task (every 30 s). For each active CronJob it
//! determines whether a new Job is due, respects concurrencyPolicy
//! (ForbidConcurrent / Replace / Allow), and creates the Job.
//! Status (lastScheduleTime, active) is kept up-to-date.

use crate::controller_dispatcher::ControllerDispatcher;
use crate::datastore::{DatastoreBackend, Resource, ResourcePreconditions};
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

// `reconcile_all_cronjobs_and_enqueue_jobs` (the periodic-scan entry
// point) and its `_inner` helper were removed in T13 — the per-CronJob
// `cronjob_scheduler::CronJobScheduler` (event-driven `spawn_delay`)
// replaces them. The remaining per-CronJob reconcile entry point is
// `reconcile_cronjob_one` below.

/// Public per-CronJob fire entry point used by `cronjob_scheduler`. Re-uses
/// the same reconcile path as the legacy bulk scan so concurrency policy,
/// status updates, and history cleanup stay consistent.
pub async fn reconcile_cronjob_one(
    db: &dyn DatastoreBackend,
    dispatcher: Option<&ControllerDispatcher>,
    cj: &Value,
    rv: i64,
) -> Result<()> {
    reconcile_cronjob_inner(db, dispatcher, cj, rv).await
}

async fn reconcile_cronjob_inner(
    db: &dyn DatastoreBackend,
    dispatcher: Option<&ControllerDispatcher>,
    cj: &Value,
    rv: i64,
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
    let Some(live_cj) = db
        .get_resource("batch/v1", "CronJob", Some(namespace), name)
        .await?
    else {
        return Ok(());
    };
    let cj = &live_cj.data;
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
        cleanup_old_jobs_by_history_limit(db, cj, namespace, uid).await?;
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
    let now = chrono::Utc::now();
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
        sync_active_status(db, cj, name, namespace, uid, rv, None).await?;
        cleanup_old_jobs_by_history_limit(db, cj, namespace, uid).await?;
        return Ok(());
    };

    // List currently active Jobs for this CronJob.
    let active_jobs = list_active_jobs(db, namespace, uid).await?;

    match concurrency {
        "ForbidConcurrent" if !active_jobs.is_empty() => {
            tracing::debug!(
                "CronJob {}/{}: ForbidConcurrent — {} active Job(s), skipping",
                namespace,
                name,
                active_jobs.len()
            );
            sync_active_status(db, cj, name, namespace, uid, rv, None).await?;
            cleanup_old_jobs_by_history_limit(db, cj, namespace, uid).await?;
            return Ok(());
        }
        "Replace" => {
            // Delete running Jobs before creating a new one. If any delete fails,
            // bail so the next reconcile retries; we must not create the
            // replacement Job while the old one still exists (violates the
            // Replace contract).
            for job in &active_jobs {
                db.delete_resource_with_preconditions(
                    "batch/v1",
                    "Job",
                    Some(namespace),
                    &job.name,
                    ResourcePreconditions::uid(job.uid.clone()),
                )
                .await?;
            }
        }
        _ => {} // "Allow": create regardless
    }

    // Create a new Job.
    let created_job = create_job_from_cronjob(db, cj, name, namespace, uid, scheduled_time).await?;
    if let (Some(dispatcher), Some(job)) = (dispatcher, created_job.as_ref()) {
        dispatcher.enqueue(&job.data).await;
    }
    sync_active_status(db, cj, name, namespace, uid, rv, Some(scheduled_time)).await?;

    // Clean up old completed/failed Jobs that exceed history limits
    cleanup_old_jobs_by_history_limit(db, cj, namespace, uid).await?;

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
async fn create_job_from_cronjob(
    db: &dyn DatastoreBackend,
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
    if db
        .get_resource("batch/v1", "Job", Some(namespace), &job_name)
        .await?
        .is_some()
    {
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

    let created = db
        .create_resource("batch/v1", "Job", Some(namespace), &job_name, job)
        .await?;
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
async fn cleanup_old_jobs_by_history_limit(
    db: &dyn DatastoreBackend,
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

    let jobs = db
        .list_resources(
            "batch/v1",
            "Job",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;

    // Collect completed (successful) and failed jobs owned by this CronJob
    let mut successful_jobs: Vec<&Resource> = Vec::new();
    let mut failed_jobs: Vec<&Resource> = Vec::new();

    for job in &jobs.items {
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
            db.delete_resource_with_preconditions(
                "batch/v1",
                "Job",
                Some(namespace),
                &job.name,
                ResourcePreconditions::uid(job.uid.clone()),
            )
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
            db.delete_resource_with_preconditions(
                "batch/v1",
                "Job",
                Some(namespace),
                &job.name,
                ResourcePreconditions::uid(job.uid.clone()),
            )
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
async fn list_active_jobs(
    db: &dyn DatastoreBackend,
    namespace: &str,
    cj_uid: &str,
) -> Result<Vec<Resource>> {
    let jobs = db
        .list_resources(
            "batch/v1",
            "Job",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;

    let active: Vec<Resource> = jobs
        .items
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
async fn sync_active_status(
    db: &dyn DatastoreBackend,
    cj: &Value,
    name: &str,
    namespace: &str,
    cj_uid: &str,
    rv: i64,
    new_scheduled: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<()> {
    let active_jobs = list_active_jobs(db, namespace, cj_uid).await?;
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

    let now_str = crate::utils::k8s_time_now();
    let mut status = cj.get("status").cloned().unwrap_or_else(|| json!({}));
    if !status.is_object() {
        status = json!({});
    }
    if let Some(s) = status.as_object_mut() {
        s.insert("active".to_string(), json!(active_refs));
        if let Some(t) = new_scheduled {
            s.insert(
                "lastScheduleTime".to_string(),
                json!(crate::utils::k8s_time_format(t)),
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

    db.update_status_only_with_preconditions(
        "batch/v1",
        "CronJob",
        Some(namespace),
        name,
        status,
        ResourcePreconditions::uid_and_resource_version(cj_uid.to_string(), rv),
    )
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::Arc;

    async fn make_raft_cronjob_datastore() -> crate::datastore::replicated::ReplicatedDatastore {
        use crate::datastore::backend::DatastoreHandle;
        use crate::datastore::command::StorageCommand;
        use crate::kubelet::outbox::payload::{OutboxOperation, OutboxPayload};

        struct InlineProposer {
            inner: DatastoreHandle,
        }

        #[async_trait]
        impl crate::datastore::replicated::RaftProposer for InlineProposer {
            async fn propose_command(&self, command: StorageCommand) -> anyhow::Result<()> {
                let payload = OutboxPayload::from_command(command).encode_protobuf()?;
                let key = format!("cronjob-inline-{}", uuid::Uuid::new_v4());
                crate::datastore::raft::state_machine::propose_outbox_on_backend(
                    self.inner.as_ref(),
                    &key,
                    OutboxOperation::PodStatus,
                    bytes::Bytes::from(payload),
                    "cronjob-inline-proposer",
                )
                .await
                .map_err(|err| anyhow::anyhow!("inline cronjob propose: {err}"))?;
                Ok(())
            }

            async fn propose_outbox_command(
                &self,
                idempotency_key: &str,
                operation: &str,
                command: StorageCommand,
                authoring_node: &str,
            ) -> std::result::Result<
                crate::kubelet::outbox::OutboxApplyResult,
                crate::kubelet::outbox::OutboxApplyError,
            > {
                let payload = OutboxPayload::from_command(command)
                    .encode_protobuf()
                    .map_err(|err| {
                        crate::kubelet::outbox::OutboxApplyError::Retryable(err.to_string())
                    })?;
                let outcome = crate::datastore::raft::state_machine::propose_outbox_on_backend(
                    self.inner.as_ref(),
                    idempotency_key,
                    OutboxOperation::try_from(operation).map_err(|err| {
                        crate::kubelet::outbox::OutboxApplyError::Retryable(err.to_string())
                    })?,
                    bytes::Bytes::from(payload),
                    authoring_node,
                )
                .await?;
                Ok(outcome.result)
            }
        }

        let inner = crate::datastore::test_support::in_memory().await;
        let handle: DatastoreHandle = Arc::new(inner);
        let ds = crate::datastore::replicated::ReplicatedDatastore::new(
            handle.clone(),
            crate::datastore::replicated::ReplicationMode::Raft {
                node_name: "cronjob-test-node".to_string(),
            },
        );
        ds.set_raft_proposer(Arc::new(InlineProposer { inner: handle }));
        ds
    }

    #[tokio::test]
    async fn test_cronjob_creates_job_when_due() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Namespace",
            None,
            "default",
            json!({"metadata": {"name": "default"}}),
        )
        .await
        .unwrap();
        let old_creation =
            crate::utils::k8s_time_format(chrono::Utc::now() - chrono::Duration::minutes(2));

        // CronJob with every-minute schedule and no lastScheduleTime
        // (so it's immediately due)
        let cj = json!({
            "apiVersion": "batch/v1",
            "kind": "CronJob",
            "metadata": {
                "name": "test-cj",
                "namespace": "default",
                "uid": "cj-uid-1",
                "creationTimestamp": old_creation
            },
            "spec": {
                "schedule": "* * * * *",
                "concurrencyPolicy": "Allow",
                "jobTemplate": {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [{"name": "c", "image": "nginx"}],
                                "restartPolicy": "Never"
                            }
                        }
                    }
                }
            }
        });

        let created = db
            .create_resource(
                "batch/v1",
                "CronJob",
                Some("default"),
                "test-cj",
                cj.clone(),
            )
            .await
            .unwrap();

        // Reconcile — should create a Job
        reconcile_cronjob_inner(&db, None, &cj, created.resource_version)
            .await
            .unwrap();

        let jobs = db
            .list_resources(
                "batch/v1",
                "Job",
                Some("default"),
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .unwrap();
        assert!(
            !jobs.items.is_empty(),
            "CronJob reconcile should create at least one Job"
        );
    }

    #[tokio::test]
    async fn test_cronjob_reconcile_persists_last_schedule_time_status() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Namespace",
            None,
            "default",
            json!({"metadata": {"name": "default"}}),
        )
        .await
        .unwrap();
        let old_creation =
            crate::utils::k8s_time_format(chrono::Utc::now() - chrono::Duration::minutes(2));

        let cj = json!({
            "apiVersion": "batch/v1",
            "kind": "CronJob",
            "metadata": {
                "name": "test-cj-status",
                "namespace": "default",
                "uid": "cj-uid-status",
                "creationTimestamp": old_creation
            },
            "spec": {
                "schedule": "* * * * *",
                "concurrencyPolicy": "Allow",
                "jobTemplate": {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [{"name": "c", "image": "nginx"}],
                                "restartPolicy": "Never"
                            }
                        }
                    }
                }
            },
            "status": {}
        });

        let created = db
            .create_resource(
                "batch/v1",
                "CronJob",
                Some("default"),
                "test-cj-status",
                cj.clone(),
            )
            .await
            .unwrap();

        reconcile_cronjob_inner(&db, None, &cj, created.resource_version)
            .await
            .unwrap();

        let updated = db
            .get_resource("batch/v1", "CronJob", Some("default"), "test-cj-status")
            .await
            .unwrap()
            .unwrap();
        let last_schedule_time = updated
            .data
            .pointer("/status/lastScheduleTime")
            .and_then(|v| v.as_str());
        assert!(
            last_schedule_time.is_some_and(|value| !value.is_empty()),
            "CronJob reconcile must persist status.lastScheduleTime so the event-driven scheduler does not re-fire the same schedule: {:?}",
            updated.data
        );
        assert_eq!(
            updated
                .data
                .pointer("/spec/schedule")
                .and_then(|v| v.as_str()),
            Some("* * * * *"),
            "status write must preserve CronJob spec"
        );
    }

    #[tokio::test]
    async fn test_cronjob_reconcile_persists_last_schedule_time_through_raft_status_path() {
        let db = make_raft_cronjob_datastore().await;
        let old_creation =
            crate::utils::k8s_time_format(chrono::Utc::now() - chrono::Duration::minutes(2));

        let cj = json!({
            "apiVersion": "batch/v1",
            "kind": "CronJob",
            "metadata": {
                "name": "test-cj-raft-status",
                "namespace": "default",
                "uid": "cj-uid-raft-status",
                "creationTimestamp": old_creation
            },
            "spec": {
                "schedule": "* * * * *",
                "concurrencyPolicy": "Allow",
                "jobTemplate": {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [{"name": "c", "image": "nginx"}],
                                "restartPolicy": "Never"
                            }
                        }
                    }
                }
            },
            "status": {}
        });

        let created = db
            .create_resource(
                "batch/v1",
                "CronJob",
                Some("default"),
                "test-cj-raft-status",
                cj.clone(),
            )
            .await
            .unwrap();

        reconcile_cronjob_inner(&db, None, &cj, created.resource_version)
            .await
            .unwrap();

        let updated = db
            .get_resource(
                "batch/v1",
                "CronJob",
                Some("default"),
                "test-cj-raft-status",
            )
            .await
            .unwrap()
            .unwrap();
        assert!(
            updated
                .data
                .pointer("/status/lastScheduleTime")
                .and_then(|v| v.as_str())
                .is_some_and(|value| !value.is_empty()),
            "raft-routed CronJob reconcile must persist status.lastScheduleTime: {:?}",
            updated.data
        );
    }

    #[tokio::test]
    async fn test_cronjob_stale_snapshot_after_delete_does_not_create_job() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Namespace",
            None,
            "default",
            json!({"metadata": {"name": "default"}}),
        )
        .await
        .unwrap();

        let cj = json!({
            "apiVersion": "batch/v1",
            "kind": "CronJob",
            "metadata": {
                "name": "stale-cj",
                "namespace": "default",
                "uid": "stale-cj-uid"
            },
            "spec": {
                "schedule": "* * * * *",
                "concurrencyPolicy": "Allow",
                "jobTemplate": {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [{"name": "c", "image": "nginx"}],
                                "restartPolicy": "Never"
                            }
                        }
                    }
                }
            }
        });

        let created = db
            .create_resource("batch/v1", "CronJob", Some("default"), "stale-cj", cj)
            .await
            .unwrap();
        let stale_snapshot = created.data.clone();

        db.delete_resource("batch/v1", "CronJob", Some("default"), "stale-cj")
            .await
            .unwrap();

        reconcile_cronjob_inner(&db, None, &stale_snapshot, created.resource_version)
            .await
            .unwrap();

        let jobs = db
            .list_resources(
                "batch/v1",
                "Job",
                Some("default"),
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .unwrap();
        assert!(
            jobs.items.is_empty(),
            "stale CronJob reconcile after delete must not create Jobs"
        );
    }

    #[tokio::test]
    async fn test_cronjob_reconcile_uses_live_suspend_state() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Namespace",
            None,
            "default",
            json!({"metadata": {"name": "default"}}),
        )
        .await
        .unwrap();

        let cj = json!({
            "apiVersion": "batch/v1",
            "kind": "CronJob",
            "metadata": {
                "name": "suspend-cj",
                "namespace": "default",
                "uid": "suspend-cj-uid"
            },
            "spec": {
                "schedule": "* * * * *",
                "concurrencyPolicy": "Allow",
                "jobTemplate": {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [{"name": "c", "image": "nginx"}],
                                "restartPolicy": "Never"
                            }
                        }
                    }
                }
            }
        });

        let created = db
            .create_resource("batch/v1", "CronJob", Some("default"), "suspend-cj", cj)
            .await
            .unwrap();
        let stale_snapshot = created.data.clone();
        let mut suspended: serde_json::Value = (*created.data).clone();
        suspended["spec"]["suspend"] = json!(true);
        db.update_resource(
            "batch/v1",
            "CronJob",
            Some("default"),
            "suspend-cj",
            suspended,
            created.resource_version,
        )
        .await
        .unwrap();

        reconcile_cronjob_inner(&db, None, &stale_snapshot, created.resource_version)
            .await
            .unwrap();

        let jobs = db
            .list_resources(
                "batch/v1",
                "Job",
                Some("default"),
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .unwrap();
        assert!(
            jobs.items.is_empty(),
            "CronJob reconcile must observe live spec.suspend before creating Jobs"
        );
    }

    #[tokio::test]
    async fn test_cronjob_created_job_is_reconciled_into_pod() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Namespace",
            None,
            "default",
            json!({"metadata": {"name": "default"}}),
        )
        .await
        .unwrap();

        let dispatcher = std::sync::Arc::new(
            crate::controller_dispatcher::ControllerDispatcher::new(std::sync::Arc::new(
                crate::controllers::service::ServiceIpam::new("10.43.128.0/17"),
            )),
        );
        dispatcher
            .set_sync_context(std::sync::Arc::new(db.clone()), "test-node".to_string())
            .await;
        dispatcher
            .set_pod_repository(crate::controllers::test_utils::pod_repository_for_test(&db))
            .await;
        let old_creation =
            crate::utils::k8s_time_format(chrono::Utc::now() - chrono::Duration::minutes(2));

        let cj = json!({
            "apiVersion": "batch/v1",
            "kind": "CronJob",
            "metadata": {
                "name": "test-cj-reconcile",
                "namespace": "default",
                "uid": "cj-uid-reconcile",
                "creationTimestamp": old_creation
            },
            "spec": {
                "schedule": "* * * * *",
                "concurrencyPolicy": "Allow",
                "jobTemplate": {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [{"name": "c", "image": "nginx"}],
                                "restartPolicy": "Never"
                            }
                        }
                    }
                }
            }
        });

        let created = db
            .create_resource(
                "batch/v1",
                "CronJob",
                Some("default"),
                "test-cj-reconcile",
                cj.clone(),
            )
            .await
            .unwrap();

        reconcile_cronjob_inner(
            &db,
            Some(dispatcher.as_ref()),
            &cj,
            created.resource_version,
        )
        .await
        .unwrap();

        let pods = db
            .list_resources(
                "v1",
                "Pod",
                Some("default"),
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .unwrap();
        assert_eq!(
            pods.items.len(),
            1,
            "CronJob-created Job must be enqueued so the Job controller creates its Pod"
        );
    }

    #[tokio::test]
    async fn test_cronjob_reconcile_propagates_status_update_error() {
        // Status-write failures (e.g., resourceVersion conflict from a concurrent
        // update) must surface to the caller so the controller's outer loop retries
        // instead of silently treating the reconcile as successful.
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Namespace",
            None,
            "default",
            json!({"metadata": {"name": "default"}}),
        )
        .await
        .unwrap();

        let cj = json!({
            "apiVersion": "batch/v1",
            "kind": "CronJob",
            "metadata": {"name": "test-cj-prop", "namespace": "default", "uid": "cj-uid-prop"},
            "spec": {
                "schedule": "* * * * *",
                "concurrencyPolicy": "Allow",
                "jobTemplate": {"spec": {"template": {"spec": {
                    "containers": [{"name": "c", "image": "nginx"}],
                    "restartPolicy": "Never"
                }}}}
            }
        });
        db.create_resource(
            "batch/v1",
            "CronJob",
            Some("default"),
            "test-cj-prop",
            cj.clone(),
        )
        .await
        .unwrap();

        // Pass a stale resource_version that cannot match the row's current rv;
        // the eventual status update inside sync_active_status must fail.
        let stale_rv: i64 = 999_999;
        let result = reconcile_cronjob_inner(&db, None, &cj, stale_rv).await;
        assert!(
            result.is_err(),
            "reconcile_cronjob must propagate status-update conflict, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_cronjob_forbid_concurrent_skips_when_active_job() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Namespace",
            None,
            "default",
            json!({"metadata": {"name": "default"}}),
        )
        .await
        .unwrap();

        // Create an existing active Job owned by the CronJob
        let existing_job = json!({
            "apiVersion": "batch/v1", "kind": "Job",
            "metadata": {
                "name": "test-cj-existing",
                "namespace": "default",
                "uid": "job-uid-1",
                "ownerReferences": [{"apiVersion": "batch/v1", "kind": "CronJob", "name": "test-cj2", "uid": "cj-uid-2", "controller": true}]
            },
            "spec": {"template": {"spec": {"containers": [{"name": "c", "image": "nginx"}], "restartPolicy": "Never"}}}
        });
        db.create_resource(
            "batch/v1",
            "Job",
            Some("default"),
            "test-cj-existing",
            existing_job,
        )
        .await
        .unwrap();

        let cj = json!({
            "apiVersion": "batch/v1", "kind": "CronJob",
            "metadata": {"name": "test-cj2", "namespace": "default", "uid": "cj-uid-2"},
            "spec": {
                "schedule": "* * * * *",
                "concurrencyPolicy": "ForbidConcurrent",
                "jobTemplate": {"spec": {"template": {"spec": {"containers": [{"name": "c", "image": "nginx"}], "restartPolicy": "Never"}}}}
            }
        });
        let created = db
            .create_resource(
                "batch/v1",
                "CronJob",
                Some("default"),
                "test-cj2",
                cj.clone(),
            )
            .await
            .unwrap();

        reconcile_cronjob_inner(&db, None, &cj, created.resource_version)
            .await
            .unwrap();

        let jobs = db
            .list_resources(
                "batch/v1",
                "Job",
                Some("default"),
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .unwrap();
        // Only the pre-existing job; ForbidConcurrent should not have created a new one
        assert_eq!(
            jobs.items.len(),
            1,
            "ForbidConcurrent should not create additional Jobs when one is active"
        );
    }

    #[tokio::test]
    async fn test_cronjob_does_not_schedule_before_creation_timestamp() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Namespace",
            None,
            "default",
            json!({"metadata": {"name": "default"}}),
        )
        .await
        .unwrap();

        let creation_timestamp = crate::utils::k8s_time_format(chrono::Utc::now());
        let cj = json!({
            "apiVersion": "batch/v1",
            "kind": "CronJob",
            "metadata": {
                "name": "test-cj-new",
                "namespace": "default",
                "uid": "cj-uid-new",
                "creationTimestamp": creation_timestamp
            },
            "spec": {
                "schedule": "* * * * *",
                "concurrencyPolicy": "ForbidConcurrent",
                "jobTemplate": {"spec": {"template": {"spec": {
                    "containers": [{"name": "c", "image": "nginx"}],
                    "restartPolicy": "Never"
                }}}}
            }
        });
        let created = db
            .create_resource(
                "batch/v1",
                "CronJob",
                Some("default"),
                "test-cj-new",
                cj.clone(),
            )
            .await
            .unwrap();

        reconcile_cronjob_inner(&db, None, &cj, created.resource_version)
            .await
            .unwrap();

        let jobs = db
            .list_resources(
                "batch/v1",
                "Job",
                Some("default"),
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .unwrap();
        assert!(
            jobs.items.is_empty(),
            "CronJob reconcile must not create a Job for a schedule before creationTimestamp"
        );
    }

    #[tokio::test]
    async fn test_cronjob_history_limits_cleanup_old_completed_jobs() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Namespace",
            None,
            "default",
            json!({"metadata": {"name": "default"}}),
        )
        .await
        .unwrap();

        let cj_uid = "cj-uid-history";
        let cj = json!({
            "apiVersion": "batch/v1",
            "kind": "CronJob",
            "metadata": {"name": "test-cj-history", "namespace": "default", "uid": cj_uid},
            "spec": {
                "schedule": "0 0 1 1 *",
                "concurrencyPolicy": "Allow",
                "successfulJobsHistoryLimit": 1,
                "failedJobsHistoryLimit": 1,
                "jobTemplate": {"spec": {"template": {"spec": {
                    "containers": [{"name": "c", "image": "nginx"}],
                    "restartPolicy": "Never"
                }}}}
            }
        });

        let created = db
            .create_resource(
                "batch/v1",
                "CronJob",
                Some("default"),
                "test-cj-history",
                cj.clone(),
            )
            .await
            .unwrap();

        // Create 3 old completed successful jobs
        for i in 0..3 {
            let job = json!({
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {
                    "name": format!("test-cj-history-{}", 1000 + i),
                    "namespace": "default",
                    "uid": format!("job-success-{}", i),
                    "creationTimestamp": format!("2025-01-0{}T00:00:00Z", i + 1),
                    "ownerReferences": [{
                        "apiVersion": "batch/v1", "kind": "CronJob",
                        "name": "test-cj-history", "uid": cj_uid, "controller": true
                    }]
                },
                "spec": {"template": {"spec": {"containers": [{"name": "c", "image": "nginx"}], "restartPolicy": "Never"}}},
                "status": {"conditions": [{"type": "Complete", "status": "True"}]}
            });
            db.create_resource(
                "batch/v1",
                "Job",
                Some("default"),
                &format!("test-cj-history-{}", 1000 + i),
                job,
            )
            .await
            .unwrap();
        }

        // Create 2 old completed failed jobs
        for i in 0..2 {
            let job = json!({
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {
                    "name": format!("test-cj-history-{}", 2000 + i),
                    "namespace": "default",
                    "uid": format!("job-failed-{}", i),
                    "creationTimestamp": format!("2025-01-0{}T00:00:00Z", i + 1),
                    "ownerReferences": [{
                        "apiVersion": "batch/v1", "kind": "CronJob",
                        "name": "test-cj-history", "uid": cj_uid, "controller": true
                    }]
                },
                "spec": {"template": {"spec": {"containers": [{"name": "c", "image": "nginx"}], "restartPolicy": "Never"}}},
                "status": {"conditions": [{"type": "Failed", "status": "True"}]}
            });
            db.create_resource(
                "batch/v1",
                "Job",
                Some("default"),
                &format!("test-cj-history-{}", 2000 + i),
                job,
            )
            .await
            .unwrap();
        }

        // Reconcile — should clean up old jobs exceeding limits
        reconcile_cronjob_inner(&db, None, &cj, created.resource_version)
            .await
            .unwrap();

        let jobs = db
            .list_resources(
                "batch/v1",
                "Job",
                Some("default"),
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .unwrap();

        // Should have 1 successful + 1 failed = 2 jobs remaining
        // (oldest ones deleted: success-0, success-1, failed-0 deleted)
        assert_eq!(
            jobs.items.len(),
            2,
            "Should keep only 1 successful + 1 failed job"
        );

        // Verify the remaining jobs are the newest ones
        let job_names: Vec<String> = jobs
            .items
            .iter()
            .map(|j| j.data["metadata"]["name"].as_str().unwrap().to_string())
            .collect();
        assert!(
            job_names.contains(&"test-cj-history-1002".to_string()),
            "Should keep newest successful job"
        );
        assert!(
            job_names.contains(&"test-cj-history-2001".to_string()),
            "Should keep newest failed job"
        );
        assert!(
            !job_names.contains(&"test-cj-history-1000".to_string()),
            "Should delete oldest successful job"
        );
        assert!(
            !job_names.contains(&"test-cj-history-1001".to_string()),
            "Should delete oldest successful job"
        );
        assert!(
            !job_names.contains(&"test-cj-history-2000".to_string()),
            "Should delete oldest failed job"
        );
    }

    #[tokio::test]
    async fn test_cronjob_history_limits_keep_five_successful_and_two_failed_jobs() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Namespace",
            None,
            "default",
            json!({"metadata": {"name": "default"}}),
        )
        .await
        .unwrap();

        let cj_uid = "cj-uid-history-5-2";
        let cj = json!({
            "apiVersion": "batch/v1",
            "kind": "CronJob",
            "metadata": {"name": "test-cj-history-5-2", "namespace": "default", "uid": cj_uid},
            "spec": {
                "schedule": "0 0 1 1 *",
                "suspend": true,
                "successfulJobsHistoryLimit": 5,
                "failedJobsHistoryLimit": 2,
                "jobTemplate": {"spec": {"template": {"spec": {
                    "containers": [{"name": "c", "image": "nginx"}],
                    "restartPolicy": "Never"
                }}}}
            }
        });

        let created = db
            .create_resource(
                "batch/v1",
                "CronJob",
                Some("default"),
                "test-cj-history-5-2",
                cj.clone(),
            )
            .await
            .unwrap();

        for i in 0..7 {
            let name = format!("test-cj-history-5-2-success-{i}");
            db.create_resource(
                "batch/v1",
                "Job",
                Some("default"),
                &name,
                json!({
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "metadata": {
                        "name": name,
                        "namespace": "default",
                        "uid": format!("job-success-5-2-{i}"),
                        "creationTimestamp": format!("2025-01-{:02}T00:00:00Z", i + 1),
                        "ownerReferences": [{
                            "apiVersion": "batch/v1",
                            "kind": "CronJob",
                            "name": "test-cj-history-5-2",
                            "uid": cj_uid,
                            "controller": true
                        }]
                    },
                    "spec": {"template": {"spec": {
                        "containers": [{"name": "c", "image": "nginx"}],
                        "restartPolicy": "Never"
                    }}},
                    "status": {"conditions": [{"type": "Complete", "status": "True"}]}
                }),
            )
            .await
            .unwrap();
        }

        for i in 0..4 {
            let name = format!("test-cj-history-5-2-failed-{i}");
            db.create_resource(
                "batch/v1",
                "Job",
                Some("default"),
                &name,
                json!({
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "metadata": {
                        "name": name,
                        "namespace": "default",
                        "uid": format!("job-failed-5-2-{i}"),
                        "creationTimestamp": format!("2025-02-{:02}T00:00:00Z", i + 1),
                        "ownerReferences": [{
                            "apiVersion": "batch/v1",
                            "kind": "CronJob",
                            "name": "test-cj-history-5-2",
                            "uid": cj_uid,
                            "controller": true
                        }]
                    },
                    "spec": {"template": {"spec": {
                        "containers": [{"name": "c", "image": "nginx"}],
                        "restartPolicy": "Never"
                    }}},
                    "status": {"conditions": [{"type": "Failed", "status": "True"}]}
                }),
            )
            .await
            .unwrap();
        }

        reconcile_cronjob_inner(&db, None, &cj, created.resource_version)
            .await
            .unwrap();

        let jobs = db
            .list_resources(
                "batch/v1",
                "Job",
                Some("default"),
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .unwrap();
        let job_names: std::collections::HashSet<String> =
            jobs.items.iter().map(|job| job.name.clone()).collect();

        assert_eq!(
            job_names.len(),
            7,
            "CronJob should retain 5 successful and 2 failed Jobs"
        );
        for i in 0..2 {
            assert!(
                !job_names.contains(&format!("test-cj-history-5-2-success-{i}")),
                "oldest successful Jobs above successfulJobsHistoryLimit must be deleted"
            );
        }
        for i in 2..7 {
            assert!(
                job_names.contains(&format!("test-cj-history-5-2-success-{i}")),
                "newest successful Jobs within successfulJobsHistoryLimit must be retained"
            );
        }
        for i in 0..2 {
            assert!(
                !job_names.contains(&format!("test-cj-history-5-2-failed-{i}")),
                "oldest failed Jobs above failedJobsHistoryLimit must be deleted"
            );
        }
        for i in 2..4 {
            assert!(
                job_names.contains(&format!("test-cj-history-5-2-failed-{i}")),
                "newest failed Jobs within failedJobsHistoryLimit must be retained"
            );
        }
    }
}
