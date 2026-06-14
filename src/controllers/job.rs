use crate::datastore::{DatastoreBackend, Resource, ResourcePreconditions};
use crate::kubelet::pod_repository::{PodObjectWriter, PodReader};
use anyhow::Result;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

type JobReconcileLocks = HashMap<String, Arc<tokio::sync::Mutex<()>>>;

static JOB_RECONCILE_LOCKS: LazyLock<tokio::sync::Mutex<JobReconcileLocks>> =
    LazyLock::new(|| tokio::sync::Mutex::new(HashMap::new()));

async fn job_reconcile_lock(namespace: &str, name: &str) -> Arc<tokio::sync::Mutex<()>> {
    let key = format!("{namespace}/{name}");
    let mut locks = JOB_RECONCILE_LOCKS.lock().await;
    locks
        .entry(key)
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}

/// Parse a succeededIndexes string like "0,2,4-6" into a set of individual indexes.
fn parse_succeeded_indexes(indexes_str: &str) -> std::collections::HashSet<i64> {
    let mut result = std::collections::HashSet::new();
    for part in indexes_str.split(',') {
        let part = part.trim();
        if let Some((start, end)) = part.split_once('-') {
            if let (Ok(s), Ok(e)) = (start.trim().parse::<i64>(), end.trim().parse::<i64>()) {
                for i in s..=e {
                    result.insert(i);
                }
            }
        } else if let Ok(n) = part.parse::<i64>() {
            result.insert(n);
        }
    }
    result
}

fn format_indexes(indexes: &std::collections::HashSet<i64>) -> Option<String> {
    if indexes.is_empty() {
        return None;
    }
    let mut vals: Vec<i64> = indexes.iter().copied().collect();
    vals.sort_unstable();

    let mut out: Vec<String> = Vec::new();
    let mut i = 0usize;
    while i < vals.len() {
        let start = vals[i];
        let mut end = start;
        while i + 1 < vals.len() && vals[i + 1] == end + 1 {
            i += 1;
            end = vals[i];
        }
        if start == end {
            out.push(start.to_string());
        } else {
            out.push(format!("{start}-{end}"));
        }
        i += 1;
    }
    Some(out.join(","))
}

fn pod_completion_index(pod: &crate::datastore::Resource) -> Option<i64> {
    pod.data
        .pointer("/metadata/annotations/batch.kubernetes.io~1job-completion-index")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i64>().ok())
}

fn pod_is_terminal(pod: &Value) -> bool {
    matches!(
        pod.pointer("/status/phase")
            .and_then(|phase| phase.as_str()),
        Some("Succeeded") | Some("Failed")
    )
}

fn pod_is_terminating(pod: &Value) -> bool {
    pod.pointer("/metadata/deletionTimestamp").is_some() && !pod_is_terminal(pod)
}

struct LiveJobCreateState {
    parallelism: i64,
    completions: i64,
    suspended: bool,
}

async fn live_job_create_state(
    db: &dyn DatastoreBackend,
    namespace: &str,
    name: &str,
) -> Result<Option<LiveJobCreateState>> {
    let Some(resource) = db
        .get_resource("batch/v1", "Job", Some(namespace), name)
        .await?
    else {
        return Ok(None);
    };
    if resource
        .data
        .pointer("/metadata/deletionTimestamp")
        .is_some()
    {
        return Ok(None);
    }
    let spec = resource.data.get("spec").unwrap_or(&Value::Null);
    Ok(Some(LiveJobCreateState {
        parallelism: spec
            .get("parallelism")
            .and_then(|value| value.as_i64())
            .unwrap_or(1),
        completions: spec
            .get("completions")
            .and_then(|value| value.as_i64())
            .unwrap_or(1),
        suspended: spec
            .get("suspend")
            .and_then(|value| value.as_bool())
            .unwrap_or(false),
    }))
}

fn finished_job_transition_time(job: &Value) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
    let Some(conditions) = job
        .pointer("/status/conditions")
        .and_then(|value| value.as_array())
    else {
        return Ok(None);
    };

    for condition in conditions {
        let condition_type = condition.get("type").and_then(|value| value.as_str());
        let status = condition.get("status").and_then(|value| value.as_str());
        if !matches!(condition_type, Some("Complete") | Some("Failed")) || status != Some("True") {
            continue;
        }

        let Some(raw_time) = condition
            .get("lastTransitionTime")
            .and_then(|value| value.as_str())
            .filter(|value| !value.is_empty())
        else {
            anyhow::bail!("finished Job condition missing lastTransitionTime");
        };

        let parsed = chrono::DateTime::parse_from_rfc3339(raw_time)
            .map_err(|err| anyhow::anyhow!("invalid Job finish time {raw_time:?}: {err}"))?
            .with_timezone(&chrono::Utc);
        return Ok(Some(parsed));
    }

    Ok(None)
}

pub fn job_ttl_cleanup_delay_at(
    job: &Value,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Option<Duration>> {
    let Some(ttl_seconds) = job
        .pointer("/spec/ttlSecondsAfterFinished")
        .and_then(|value| value.as_i64())
    else {
        return Ok(None);
    };
    if ttl_seconds < 0 {
        anyhow::bail!("ttlSecondsAfterFinished must be non-negative");
    }

    let Some(finish_time) = finished_job_transition_time(job)? else {
        return Ok(None);
    };

    let expires_at = finish_time + chrono::Duration::seconds(ttl_seconds);
    let remaining = expires_at.signed_duration_since(now);
    if remaining <= chrono::Duration::zero() {
        return Ok(Some(Duration::ZERO));
    }

    Ok(Some(remaining.to_std().map_err(|err| {
        anyhow::anyhow!("failed to convert Job TTL delay to std duration: {err}")
    })?))
}

pub fn job_ttl_cleanup_delay(job: &Value) -> Result<Option<Duration>> {
    job_ttl_cleanup_delay_at(job, chrono::Utc::now())
}

async fn mark_job_foreground_deleting(
    db: &dyn DatastoreBackend,
    resource: &Resource,
) -> Result<Resource> {
    let mut data: Value = (*resource.data).clone();
    let metadata = data
        .get_mut("metadata")
        .and_then(|value| value.as_object_mut())
        .ok_or_else(|| anyhow::anyhow!("Job missing metadata"))?;
    if !metadata.contains_key("deletionTimestamp")
        || metadata
            .get("deletionTimestamp")
            .is_some_and(|value| value.is_null())
    {
        metadata.insert(
            "deletionTimestamp".to_string(),
            json!(crate::utils::k8s_timestamp()),
        );
    }
    let finalizers = metadata
        .entry("finalizers".to_string())
        .or_insert_with(|| json!([]));
    if let Some(finalizers) = finalizers.as_array_mut()
        && !finalizers
            .iter()
            .any(|value| value.as_str() == Some("foregroundDeletion"))
    {
        finalizers.push(json!("foregroundDeletion"));
    }

    db.update_resource_with_preconditions(
        "batch/v1",
        "Job",
        resource.namespace.as_deref(),
        &resource.name,
        data,
        ResourcePreconditions::from_resource(resource),
    )
    .await
}

async fn delete_finished_job_for_ttl(
    db: &dyn DatastoreBackend,
    resource: &Resource,
    pod_delete_sink: &dyn crate::controllers::gc::GcPodDeleteSink,
) -> Result<()> {
    if resource
        .data
        .pointer("/metadata/deletionTimestamp")
        .is_some()
    {
        return Ok(());
    }

    let marked = mark_job_foreground_deleting(db, resource).await?;
    let _ =
        crate::controllers::gc::finalize_foreground_owner_if_ready(db, &marked, pod_delete_sink)
            .await?;
    Ok(())
}

/// Evaluate successPolicy rules. Returns true if any rule is satisfied.
fn check_success_policy(
    spec: &Value,
    active_owned_pods: &[&crate::datastore::Resource],
    succeeded_count: i64,
    historical_succeeded_indexes: &std::collections::HashSet<i64>,
) -> bool {
    let rules = match spec
        .get("successPolicy")
        .and_then(|sp| sp.get("rules"))
        .and_then(|r| r.as_array())
    {
        Some(r) if !r.is_empty() => r,
        _ => return false,
    };

    for rule in rules {
        // Check succeededCount rule
        if let Some(required_count) = rule.get("succeededCount").and_then(|c| c.as_i64()) {
            let effective_succeeded_count =
                succeeded_count.max(historical_succeeded_indexes.len() as i64);
            if effective_succeeded_count >= required_count {
                return true;
            }
        }

        // Check succeededIndexes rule
        if let Some(indexes_str) = rule.get("succeededIndexes").and_then(|s| s.as_str()) {
            let required_indexes = parse_succeeded_indexes(indexes_str);
            if required_indexes.is_empty() {
                continue;
            }
            // Collect indexes of succeeded pods and merge indexes already
            // recorded in Job status. Indexed Job status is the controller's
            // durable terminal-index history; terminal Pods may be deleted
            // before a later reconcile observes them again.
            let mut succeeded_indexes = historical_succeeded_indexes.clone();
            succeeded_indexes.extend(active_owned_pods.iter().filter_map(|pod| {
                let phase = pod
                    .data
                    .get("status")
                    .and_then(|s| s.get("phase"))
                    .and_then(|p| p.as_str());
                if phase != Some("Succeeded") {
                    return None;
                }
                pod.data
                    .pointer("/metadata/annotations/batch.kubernetes.io~1job-completion-index")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<i64>().ok())
            }));
            // All required indexes must be present in succeeded_indexes
            if required_indexes.is_subset(&succeeded_indexes) {
                return true;
            }
        }
    }

    false
}

fn pod_restart_count_sum(pod: &Value) -> i64 {
    pod.pointer("/status/containerStatuses")
        .and_then(|statuses| statuses.as_array())
        .map(|statuses| {
            statuses
                .iter()
                .filter_map(|status| status.get("restartCount").and_then(|v| v.as_i64()))
                .sum()
        })
        .unwrap_or(0)
}

fn pod_matches_exit_codes(pod: &Value, on_exit_codes: &Value) -> bool {
    let operator = on_exit_codes
        .get("operator")
        .and_then(|o| o.as_str())
        .unwrap_or("In");
    let values: Vec<i64> = on_exit_codes
        .get("values")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_i64()).collect())
        .unwrap_or_default();
    let container_name_filter = on_exit_codes.get("containerName").and_then(|n| n.as_str());

    let Some(statuses) = pod
        .pointer("/status/containerStatuses")
        .and_then(|cs| cs.as_array())
    else {
        return false;
    };

    statuses.iter().any(|cs| {
        if let Some(filter) = container_name_filter {
            let cs_name = cs.get("name").and_then(|n| n.as_str()).unwrap_or("");
            if cs_name != filter {
                return false;
            }
        }

        let Some(code) = cs
            .pointer("/state/terminated/exitCode")
            .and_then(|c| c.as_i64())
        else {
            return false;
        };
        match operator {
            "In" => values.contains(&code),
            "NotIn" => !values.contains(&code),
            _ => false,
        }
    })
}

fn pod_matches_conditions(pod: &Value, on_pod_conditions: &Value) -> bool {
    let Some(patterns) = on_pod_conditions.as_array() else {
        return false;
    };
    let Some(conditions) = pod
        .pointer("/status/conditions")
        .and_then(|conditions| conditions.as_array())
    else {
        return false;
    };

    patterns.iter().any(|pattern| {
        let Some(pattern_type) = pattern.get("type").and_then(|v| v.as_str()) else {
            return false;
        };
        let pattern_status = pattern
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("True");
        conditions.iter().any(|condition| {
            condition.get("type").and_then(|v| v.as_str()) == Some(pattern_type)
                && condition.get("status").and_then(|v| v.as_str()) == Some(pattern_status)
        })
    })
}

fn pod_matches_failure_policy_rule(pod: &Value, rule: &Value) -> bool {
    let mut has_matcher = false;

    if let Some(on_exit_codes) = rule.get("onExitCodes") {
        has_matcher = true;
        if pod_matches_exit_codes(pod, on_exit_codes) {
            return true;
        }
    }

    if let Some(on_pod_conditions) = rule.get("onPodConditions") {
        has_matcher = true;
        if pod_matches_conditions(pod, on_pod_conditions) {
            return true;
        }
    }

    !has_matcher
}

fn pod_failure_policy_action_for_pod<'a>(spec: &'a Value, pod: &Value) -> Option<&'a str> {
    let rules = match spec
        .pointer("/podFailurePolicy/rules")
        .and_then(|r| r.as_array())
    {
        Some(r) if !r.is_empty() => r,
        _ => return None,
    };

    for rule in rules {
        if pod_matches_failure_policy_rule(pod, rule) {
            return Some(
                rule.get("action")
                    .and_then(|a| a.as_str())
                    .unwrap_or("Count"),
            );
        }
    }

    None
}

fn pod_failure_ignored_by_policy(spec: &Value, pod: &Value) -> bool {
    pod_failure_policy_action_for_pod(spec, pod) == Some("Ignore")
}

/// Check podFailurePolicy rules against failed pods.
/// Returns true if any rule with action=FailJob matches.
fn check_pod_failure_policy(
    spec: &Value,
    active_owned_pods: &[&crate::datastore::Resource],
) -> bool {
    active_owned_pods.iter().copied().any(|pod| {
        pod.data.pointer("/status/phase").and_then(|p| p.as_str()) == Some("Failed")
            && pod_failure_policy_action_for_pod(spec, &pod.data) == Some("FailJob")
    })
}

fn fail_index_policy_matches(
    spec: &Value,
    active_owned_pods: &[&crate::datastore::Resource],
) -> std::collections::HashSet<i64> {
    let failed_pods: Vec<&crate::datastore::Resource> = active_owned_pods
        .iter()
        .copied()
        .filter(|pod| pod.data.pointer("/status/phase").and_then(|p| p.as_str()) == Some("Failed"))
        .collect();
    if failed_pods.is_empty() {
        return std::collections::HashSet::new();
    }

    let mut failed_indexes = std::collections::HashSet::new();
    for pod in &failed_pods {
        if pod_failure_policy_action_for_pod(spec, &pod.data) != Some("FailIndex") {
            continue;
        }
        if let Some(index) = pod_completion_index(pod) {
            failed_indexes.insert(index);
        }
    }
    failed_indexes
}

fn existing_condition_transition_time(
    existing_conditions: &[Value],
    condition_type: &str,
    status: &str,
) -> Option<Value> {
    existing_conditions.iter().find_map(|condition| {
        let same_type =
            condition.get("type").and_then(|value| value.as_str()) == Some(condition_type);
        let same_status = condition.get("status").and_then(|value| value.as_str()) == Some(status);
        if same_type && same_status {
            condition.get("lastTransitionTime").cloned()
        } else {
            None
        }
    })
}

/// Derive the Job `.status` subtree from the current Job object and its owned
/// Pods without creating or deleting Pods. This is the shared status owner used
/// by full Job reconciliation and by bottom-up Pod status refreshes.
pub fn derive_job_status_from_owned_pods(job: &Value, owned_pods: &[Resource]) -> Value {
    let common = crate::controllers::common::controller_common();
    let metadata = job.get("metadata").unwrap_or(&Value::Null);
    let spec = job.get("spec").unwrap_or(&Value::Null);

    let completions = spec
        .get("completions")
        .and_then(|c| c.as_i64())
        .unwrap_or(1);
    let backoff_limit = spec
        .get("backoffLimit")
        .and_then(|b| b.as_i64())
        .unwrap_or(6);

    let active_owned_pods: Vec<&Resource> = owned_pods
        .iter()
        .filter(|pod| {
            pod.data
                .get("metadata")
                .and_then(|m| m.get("deletionTimestamp"))
                .is_none()
        })
        .collect();

    let restart_policy = spec
        .pointer("/template/spec/restartPolicy")
        .and_then(|p| p.as_str())
        .unwrap_or("Never");

    let mut succeeded_count = 0i64;
    let mut failed_count = 0i64;
    let mut restart_failure_count = 0i64;
    let mut active_count = 0i64;
    let mut ready_count = 0i64;
    let terminating_count = owned_pods
        .iter()
        .filter(|pod| pod_is_terminating(&pod.data))
        .count() as i64;

    for pod in &active_owned_pods {
        if let Some(phase) = pod
            .data
            .get("status")
            .and_then(|s| s.get("phase"))
            .and_then(|p| p.as_str())
        {
            match phase {
                "Succeeded" => succeeded_count += 1,
                "Failed" if !pod_failure_ignored_by_policy(spec, &pod.data) => failed_count += 1,
                "Failed" => {}
                "Pending" | "Running" => {
                    active_count += 1;
                    if restart_policy == "OnFailure" {
                        restart_failure_count += pod_restart_count_sum(&pod.data);
                    }
                }
                _ => {}
            }
        } else {
            active_count += 1;
            if restart_policy == "OnFailure" {
                restart_failure_count += pod_restart_count_sum(&pod.data);
            }
        }

        if matches!(
            pod.data
                .get("status")
                .and_then(|s| s.get("phase"))
                .and_then(|p| p.as_str()),
            Some("Pending") | Some("Running") | None
        ) && pod_is_ready(&pod.data)
        {
            ready_count += 1;
        }
    }

    let is_indexed = spec
        .get("completionMode")
        .and_then(|m| m.as_str())
        .unwrap_or("NonIndexed")
        == "Indexed";

    let previous_completed_indexes = if is_indexed {
        job.pointer("/status/completedIndexes")
            .and_then(|v| v.as_str())
            .map(parse_succeeded_indexes)
            .unwrap_or_default()
    } else {
        std::collections::HashSet::new()
    };

    let mut is_complete = !is_indexed && succeeded_count >= completions;
    // For Job pods with restartPolicy=OnFailure, Kubernetes counts container
    // restarts toward backoffLimit even while the pod remains Running.
    let effective_failed_attempts = failed_count + restart_failure_count;
    let mut is_failed = effective_failed_attempts > backoff_limit;
    let mut failure_reason = format!(
        "Job has reached the specified backoff limit ({})",
        backoff_limit
    );
    let mut failure_condition_reason = "BackoffLimitExceeded".to_string();

    if !is_failed && check_pod_failure_policy(spec, &active_owned_pods) {
        is_failed = true;
        failure_condition_reason = "PodFailurePolicy".to_string();
        failure_reason = "Job failed due to pod failure policy".to_string();
    }

    let is_success_criteria_met = check_success_policy(
        spec,
        &active_owned_pods,
        succeeded_count,
        &previous_completed_indexes,
    );

    let is_suspended = spec
        .get("suspend")
        .and_then(|s| s.as_bool())
        .unwrap_or(false);

    let mut failed_indexes: std::collections::HashSet<i64> = std::collections::HashSet::new();
    let mut failed_count_by_index: std::collections::HashMap<i64, i64> =
        std::collections::HashMap::new();
    if is_indexed {
        for pod in &active_owned_pods {
            if pod.data.pointer("/status/phase").and_then(|p| p.as_str()) != Some("Failed") {
                continue;
            }
            if pod_failure_ignored_by_policy(spec, &pod.data) {
                continue;
            }
            if let Some(idx) = pod_completion_index(pod) {
                *failed_count_by_index.entry(idx).or_insert(0) += 1;
            }
        }

        failed_indexes.extend(fail_index_policy_matches(spec, &active_owned_pods));

        if let Some(backoff_per_index) = spec.get("backoffLimitPerIndex").and_then(|b| b.as_i64()) {
            for (idx, count) in &failed_count_by_index {
                if *count > backoff_per_index {
                    failed_indexes.insert(*idx);
                }
            }
        }
    }

    if !is_failed && let Some(max_failed) = spec.get("maxFailedIndexes").and_then(|m| m.as_i64()) {
        let failed_index_count = if is_indexed {
            let indexed = failed_indexes.len() as i64;
            if indexed > 0 { indexed } else { failed_count }
        } else {
            failed_count
        };
        if failed_index_count > max_failed {
            is_failed = true;
            failure_condition_reason = "MaxFailedIndexesExceeded".to_string();
            failure_reason = format!(
                "Job has exceeded the maximum number of failed indexes ({})",
                max_failed
            );
        }
    }

    let succeeded_indexes: std::collections::HashSet<i64> = if is_indexed {
        let mut indexes = previous_completed_indexes.clone();
        indexes.extend(active_owned_pods.iter().filter_map(|pod| {
            let phase = pod
                .data
                .pointer("/status/phase")
                .and_then(|p| p.as_str())
                .unwrap_or("");
            if phase == "Succeeded" {
                pod.data
                    .pointer("/metadata/annotations/batch.kubernetes.io~1job-completion-index")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<i64>().ok())
            } else {
                None
            }
        }));
        indexes
    } else {
        std::collections::HashSet::new()
    };

    if is_indexed {
        succeeded_count = succeeded_indexes.len() as i64;
    }

    if !is_complete && is_indexed && (succeeded_indexes.len() as i64) >= completions {
        is_complete = true;
    }

    if !is_failed && is_indexed && completions > 0 {
        let terminal_indexes = succeeded_indexes.len() + failed_indexes.len();
        if terminal_indexes as i64 >= completions && (succeeded_indexes.len() as i64) < completions
        {
            is_failed = true;
            failure_condition_reason = "FailedIndexes".to_string();
            failure_reason = "Job has failed indexes for all completions".to_string();
        }
    }

    let existing_conditions = job
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let mut conditions = Vec::new();

    let success_policy_ready_for_complete =
        is_success_criteria_met && active_count == 0 && terminating_count == 0;
    let should_set_complete = is_complete || success_policy_ready_for_complete;

    if should_set_complete {
        let (reason, message) = if is_success_criteria_met {
            ("SuccessPolicy", "Matched success policy rule")
        } else {
            (
                "CompletionsReached",
                "Reached expected number of succeeded pods",
            )
        };
        let mut condition = common.build_condition("Complete", "True", reason, message);
        condition["lastProbeTime"] = Value::Null;
        condition["lastTransitionTime"] =
            existing_condition_transition_time(&existing_conditions, "Complete", "True")
                .unwrap_or_else(|| json!(crate::utils::k8s_timestamp()));
        conditions.push(condition);
    }

    if is_failed {
        let mut condition =
            common.build_condition("Failed", "True", &failure_condition_reason, &failure_reason);
        condition["lastProbeTime"] = Value::Null;
        condition["lastTransitionTime"] =
            existing_condition_transition_time(&existing_conditions, "Failed", "True")
                .unwrap_or_else(|| json!(crate::utils::k8s_timestamp()));
        conditions.push(condition);
    }

    if is_success_criteria_met {
        let mut condition = common.build_condition(
            "SuccessCriteriaMet",
            "True",
            "SuccessPolicy",
            "Matched success policy rule",
        );
        condition["lastProbeTime"] = Value::Null;
        condition["lastTransitionTime"] =
            existing_condition_transition_time(&existing_conditions, "SuccessCriteriaMet", "True")
                .unwrap_or_else(|| json!(crate::utils::k8s_timestamp()));
        conditions.push(condition);
    }

    if is_suspended {
        let mut condition =
            common.build_condition("Suspended", "True", "JobSuspended", "Job is suspended");
        condition["lastProbeTime"] = Value::Null;
        condition["lastTransitionTime"] =
            existing_condition_transition_time(&existing_conditions, "Suspended", "True")
                .unwrap_or_else(|| json!(crate::utils::k8s_timestamp()));
        conditions.push(condition);
    }

    let controller_owned_condition_types: std::collections::HashSet<&str> =
        ["Complete", "Failed", "SuccessCriteriaMet", "Suspended"]
            .into_iter()
            .collect();
    for existing in existing_conditions {
        let cond_type = existing.get("type").and_then(|v| v.as_str()).unwrap_or("");
        if cond_type.is_empty() || controller_owned_condition_types.contains(cond_type) {
            continue;
        }
        let already_present = conditions
            .iter()
            .any(|c| c.get("type").and_then(|v| v.as_str()) == Some(cond_type));
        if !already_present {
            conditions.push(existing);
        }
    }

    let start_time = metadata
        .get("creationTimestamp")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(crate::utils::k8s_time_now);
    let mut status = json!({
        "active": active_count,
        "ready": ready_count,
        "succeeded": succeeded_count,
        "failed": failed_count,
        "conditions": conditions,
        "startTime": start_time,
        "terminating": terminating_count,
    });
    if is_indexed {
        if let Some(completed_indexes) = format_indexes(&succeeded_indexes) {
            status["completedIndexes"] = json!(completed_indexes);
        }
        if let Some(failed_indexes_str) = format_indexes(&failed_indexes) {
            status["failedIndexes"] = json!(failed_indexes_str);
        }
    }
    if (should_set_complete || is_failed)
        && let Some(s) = status.as_object_mut()
    {
        let completion_time = job
            .pointer("/status/completionTime")
            .cloned()
            .unwrap_or_else(|| json!(crate::utils::k8s_time_now()));
        s.insert("completionTime".into(), completion_time);
    }

    status
}

/// Reconcile a Job: manage pod creation/deletion against `completions`,
/// `parallelism`, and `backoffLimit`. Returns the updated Job resource.
pub async fn reconcile_job(
    db: &dyn DatastoreBackend,
    pod_reader: &dyn PodReader,
    pod_writer: &dyn PodObjectWriter,
    pod_delete_sink: &dyn crate::controllers::gc::GcPodDeleteSink,
    job: &Value,
    node_name: &str,
) -> Result<Value> {
    let initial_metadata = job
        .get("metadata")
        .ok_or_else(|| anyhow::anyhow!("Missing metadata"))?;
    let initial_name = initial_metadata
        .get("name")
        .and_then(|n| n.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing name"))?;
    let initial_namespace = initial_metadata
        .get("namespace")
        .and_then(|n| n.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing namespace"))?;

    let reconcile_lock = job_reconcile_lock(initial_namespace, initial_name).await;
    let _reconcile_guard = reconcile_lock.lock().await;

    let latest_job = db
        .get_resource("batch/v1", "Job", Some(initial_namespace), initial_name)
        .await?;
    let latest_job = match latest_job {
        Some(resource) => resource,
        None => return Ok(job.clone()),
    };
    let job = &latest_job.data;

    let common = crate::controllers::common::controller_common();
    let metadata = job
        .get("metadata")
        .ok_or_else(|| anyhow::anyhow!("Missing metadata"))?;

    // Skip reconciliation if the resource is being deleted
    if metadata.get("deletionTimestamp").is_some() {
        return Ok(std::sync::Arc::unwrap_or_clone(latest_job.data.clone()));
    }

    if job_ttl_cleanup_delay(&latest_job.data)?.is_some_and(|delay| delay.is_zero()) {
        delete_finished_job_for_ttl(db, &latest_job, pod_delete_sink).await?;
        return Ok(std::sync::Arc::unwrap_or_clone(latest_job.data.clone()));
    }

    let spec = job
        .get("spec")
        .ok_or_else(|| anyhow::anyhow!("Missing spec"))?;

    let name = metadata
        .get("name")
        .and_then(|n| n.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing name"))?;
    let namespace = metadata
        .get("namespace")
        .and_then(|n| n.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing namespace"))?;
    let uid = metadata
        .get("uid")
        .and_then(|u| u.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing uid"))?;

    let completions = spec
        .get("completions")
        .and_then(|c| c.as_i64())
        .unwrap_or(1);
    let parallelism = spec
        .get("parallelism")
        .and_then(|p| p.as_i64())
        .unwrap_or(1);
    let backoff_limit = spec
        .get("backoffLimit")
        .and_then(|b| b.as_i64())
        .unwrap_or(6);
    let template = spec
        .get("template")
        .ok_or_else(|| anyhow::anyhow!("Missing template"))?;

    // List existing pods owned by this Job, then apply Kubernetes controller
    // adoption/release semantics before computing status or creating pods.
    let mut owned_pods = pod_reader.list_pods_by_owner_uid(namespace, uid).await?;
    if let Some(selector) = job_pod_selector(spec, template) {
        let mut retained_owned_pods = Vec::new();
        for pod in owned_pods {
            if pod_matches_job_selector(&pod.data, &selector) {
                retained_owned_pods.push(pod);
            } else {
                let mut released_pod: serde_json::Value = (*pod.data).clone();
                if crate::controllers::common::remove_owner_reference_by_uid(
                    &mut released_pod,
                    "Job",
                    uid,
                ) {
                    let owner_refs = released_pod
                        .pointer("/metadata/ownerReferences")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();
                    pod_writer
                        .update_pod_owner_references(namespace, &pod.name, owner_refs)
                        .await?;
                }
            }
        }

        let all_pods = pod_reader
            .list_pods(Some(namespace), None, None, None, None)
            .await?
            .items;
        for pod in all_pods {
            if pod_owned_by_job(&pod.data, uid) {
                continue;
            }
            if pod_matches_job_selector(&pod.data, &selector)
                && !pod_has_controller_owner(&pod.data)
            {
                let mut adopted_pod: serde_json::Value = (*pod.data).clone();
                crate::controllers::common::append_owner_reference(
                    &mut adopted_pod,
                    common.build_owner_ref("batch/v1", "Job", name, uid),
                );
                let owner_refs = adopted_pod
                    .pointer("/metadata/ownerReferences")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                pod_writer
                    .update_pod_owner_references(namespace, &pod.name, owner_refs)
                    .await?;
                retained_owned_pods.push(pod);
            }
        }
        owned_pods = retained_owned_pods;
    }

    // Count succeeded and failed pods
    // Filter out pods with deletionTimestamp before counting
    let active_owned_pods: Vec<&Resource> = owned_pods
        .iter()
        .filter(|pod| {
            pod.data
                .get("metadata")
                .and_then(|m| m.get("deletionTimestamp"))
                .is_none()
        })
        .collect();

    let restart_policy = spec
        .pointer("/template/spec/restartPolicy")
        .and_then(|p| p.as_str())
        .unwrap_or("Never");

    let mut succeeded_count = 0i64;
    let mut failed_count = 0i64;
    let mut restart_failure_count = 0i64;
    let mut active_count = 0i64;
    let mut ready_count = 0i64;

    for pod in &active_owned_pods {
        if let Some(phase) = pod
            .data
            .get("status")
            .and_then(|s| s.get("phase"))
            .and_then(|p| p.as_str())
        {
            match phase {
                "Succeeded" => succeeded_count += 1,
                "Failed" if !pod_failure_ignored_by_policy(spec, &pod.data) => failed_count += 1,
                "Failed" => {}
                "Pending" | "Running" => {
                    active_count += 1;
                    if restart_policy == "OnFailure" {
                        restart_failure_count += pod_restart_count_sum(&pod.data);
                    }
                }
                _ => {}
            }
        } else {
            // No phase yet - consider it active (Pending)
            active_count += 1;
            if restart_policy == "OnFailure" {
                restart_failure_count += pod_restart_count_sum(&pod.data);
            }
        }

        if matches!(
            pod.data
                .get("status")
                .and_then(|s| s.get("phase"))
                .and_then(|p| p.as_str()),
            Some("Pending") | Some("Running") | None
        ) && pod_is_ready(&pod.data)
        {
            ready_count += 1;
        }
    }

    // Detect completionMode: "Indexed" assigns completion indexes to pods.
    let is_indexed = spec
        .get("completionMode")
        .and_then(|m| m.as_str())
        .unwrap_or("NonIndexed")
        == "Indexed";

    let previous_completed_indexes = if is_indexed {
        job.pointer("/status/completedIndexes")
            .and_then(|v| v.as_str())
            .map(parse_succeeded_indexes)
            .unwrap_or_default()
    } else {
        std::collections::HashSet::new()
    };

    // NonIndexed Jobs complete by succeeded pod count. Indexed Jobs complete by
    // unique succeeded completion indexes (computed below), not by raw pod
    // count, because retries/duplicates for one index must not satisfy another.
    let mut is_complete = !is_indexed && succeeded_count >= completions;

    // Check if Job has failed (exceeded backoffLimit). For OnFailure Jobs,
    // container restarts count toward the backoff limit while the pod remains Running.
    let effective_failed_attempts = failed_count + restart_failure_count;
    let mut is_failed = effective_failed_attempts > backoff_limit;

    // Check podFailurePolicy: fail job immediately if a matching rule fires
    if !is_failed && check_pod_failure_policy(spec, &active_owned_pods) {
        is_failed = true;
    }

    // Check successPolicy rules — any rule satisfied → SuccessCriteriaMet
    let is_success_criteria_met = check_success_policy(
        spec,
        &active_owned_pods,
        succeeded_count,
        &previous_completed_indexes,
    );

    // spec.suspend: when true, stop creating new pods and mark as Suspended.
    let is_suspended = spec
        .get("suspend")
        .and_then(|s| s.as_bool())
        .unwrap_or(false);

    if is_success_criteria_met && !is_failed {
        for pod in &active_owned_pods {
            let phase = pod.data.pointer("/status/phase").and_then(|p| p.as_str());
            if matches!(phase, Some("Pending") | Some("Running") | None) {
                pod_writer.delete_pod(namespace, &pod.name).await?;
            }
        }
        active_count = 0;
        ready_count = 0;
        is_complete = true;
    }

    let mut deleted_active_pod_names = std::collections::HashSet::new();
    if !is_complete && !is_failed && active_count > parallelism.max(0) {
        let excess = active_count - parallelism.max(0);
        let mut active_pods_for_removal: Vec<&Resource> = active_owned_pods
            .iter()
            .copied()
            .filter(|pod| {
                let phase = pod.data.pointer("/status/phase").and_then(|p| p.as_str());
                matches!(phase, Some("Pending") | Some("Running") | None)
            })
            .collect();
        active_pods_for_removal.sort_by(|a, b| a.name.cmp(&b.name));
        for pod in active_pods_for_removal
            .into_iter()
            .rev()
            .take(excess as usize)
        {
            pod_writer.delete_pod(namespace, &pod.name).await?;
            deleted_active_pod_names.insert(pod.name.clone());
            active_count -= 1;
            if pod_is_ready(&pod.data) {
                ready_count = ready_count.saturating_sub(1);
            }
        }
    }

    // For Indexed jobs, compute completed/failed indexes.
    let mut failed_indexes: std::collections::HashSet<i64> = std::collections::HashSet::new();
    let mut failed_count_by_index: std::collections::HashMap<i64, i64> =
        std::collections::HashMap::new();
    if is_indexed {
        for pod in &active_owned_pods {
            if pod.data.pointer("/status/phase").and_then(|p| p.as_str()) != Some("Failed") {
                continue;
            }
            if pod_failure_ignored_by_policy(spec, &pod.data) {
                continue;
            }
            if let Some(idx) = pod_completion_index(pod) {
                *failed_count_by_index.entry(idx).or_insert(0) += 1;
            }
        }

        // podFailurePolicy action=FailIndex marks an index terminally failed.
        failed_indexes.extend(fail_index_policy_matches(spec, &active_owned_pods));

        // backoffLimitPerIndex marks index failed once retries exceeded.
        if let Some(backoff_per_index) = spec.get("backoffLimitPerIndex").and_then(|b| b.as_i64()) {
            for (idx, count) in &failed_count_by_index {
                if *count > backoff_per_index {
                    failed_indexes.insert(*idx);
                }
            }
        }
    }

    // Check maxFailedIndexes: indexed Jobs fail early when failed indexes exceed limit.
    if !is_failed && let Some(max_failed) = spec.get("maxFailedIndexes").and_then(|m| m.as_i64()) {
        let failed_index_count = if is_indexed {
            let indexed = failed_indexes.len() as i64;
            if indexed > 0 { indexed } else { failed_count }
        } else {
            failed_count
        };
        if failed_index_count > max_failed {
            is_failed = true;
        }
    }

    // For indexed jobs, track which indexes are already succeeded/active so we only
    // create pods for unstarted indexes.
    let succeeded_indexes: std::collections::HashSet<i64> = if is_indexed {
        let mut indexes = previous_completed_indexes.clone();
        indexes.extend(active_owned_pods.iter().filter_map(|pod| {
            let phase = pod
                .data
                .pointer("/status/phase")
                .and_then(|p| p.as_str())
                .unwrap_or("");
            if phase == "Succeeded" {
                pod.data
                    .pointer("/metadata/annotations/batch.kubernetes.io~1job-completion-index")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<i64>().ok())
            } else {
                None
            }
        }));
        indexes
    } else {
        std::collections::HashSet::new()
    };

    if is_indexed {
        succeeded_count = succeeded_indexes.len() as i64;
    }

    if !is_complete && is_indexed && (succeeded_indexes.len() as i64) >= completions {
        is_complete = true;
    }

    // If all indexes are terminal (succeeded or failed), indexed Job is failed when not complete.
    if !is_failed && is_indexed && completions > 0 {
        let terminal_indexes = succeeded_indexes.len() + failed_indexes.len();
        if terminal_indexes as i64 >= completions && (succeeded_indexes.len() as i64) < completions
        {
            is_failed = true;
        }
    }

    let active_indexes: std::collections::HashSet<i64> = if is_indexed {
        active_owned_pods
            .iter()
            .filter(|pod| !deleted_active_pod_names.contains(&pod.name))
            .filter_map(|pod| {
                let phase = pod
                    .data
                    .pointer("/status/phase")
                    .and_then(|p| p.as_str())
                    .unwrap_or("");
                if phase == "Pending" || phase == "Running" || phase.is_empty() {
                    pod.data
                        .pointer("/metadata/annotations/batch.kubernetes.io~1job-completion-index")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<i64>().ok())
                } else {
                    None
                }
            })
            .collect()
    } else {
        std::collections::HashSet::new()
    };

    // Create new pods if needed (not complete, not failed, not suspended)
    if !is_complete && !is_failed && !is_suspended && !is_success_criteria_met {
        if is_indexed {
            // For Indexed jobs: create one pod per unstarted index, up to parallelism.
            // Calculate the slots available once before the loop (active_count grows as we create).
            let slots_available = (parallelism - active_count).max(0);
            let mut created = 0i64;
            for idx in 0..completions {
                if created >= slots_available {
                    break;
                }
                let Some(live_state) = live_job_create_state(db, namespace, name).await? else {
                    return Ok(std::sync::Arc::unwrap_or_clone(latest_job.data.clone()));
                };
                if live_state.suspended {
                    break;
                }
                if idx >= live_state.completions {
                    break;
                }
                if active_count + created >= live_state.parallelism.max(0) {
                    break;
                }
                if succeeded_indexes.contains(&idx)
                    || active_indexes.contains(&idx)
                    || failed_indexes.contains(&idx)
                {
                    continue;
                }
                let pod_name = format!(
                    "{}-{}-{}",
                    name,
                    idx,
                    uuid::Uuid::new_v4()
                        .to_string()
                        .chars()
                        .take(5)
                        .collect::<String>()
                );

                let idx_str = idx.to_string();
                let mut pod = crate::controllers::common::build_child_pod(
                    template,
                    &pod_name,
                    namespace,
                    node_name,
                    crate::controllers::common::OwnerInfo {
                        api_version: "batch/v1",
                        kind: "Job",
                        name,
                        uid,
                    },
                    &[("batch.kubernetes.io/job-completion-index", idx_str.as_str())],
                    &[("batch.kubernetes.io/job-completion-index", idx_str.as_str())],
                )?;

                // Inject JOB_COMPLETION_INDEX env var into all containers
                // (Job-specific contract beyond the canonical pod template).
                let indexed_hostname = format!("{name}-{idx}");
                if let Some(spec_obj) = pod.pointer_mut("/spec").and_then(|s| s.as_object_mut()) {
                    spec_obj.insert("hostname".to_string(), json!(indexed_hostname));
                }
                if let Some(containers) = pod
                    .pointer_mut("/spec/containers")
                    .and_then(|c| c.as_array_mut())
                {
                    for container in containers.iter_mut() {
                        if let Some(c) = container.as_object_mut() {
                            let env = c.entry("env").or_insert_with(|| json!([]));
                            if let Some(env_arr) = env.as_array_mut() {
                                env_arr.push(json!({
                                    "name": "JOB_COMPLETION_INDEX",
                                    "value": idx_str
                                }));
                            }
                        }
                    }
                }

                pod_writer
                    .create_controller_pod(namespace, &pod_name, node_name, pod)
                    .await?;
                created += 1;
            }
        } else {
            // NonIndexed: create pods up to parallelism
            let remaining_completions = completions - succeeded_count;
            let desired_active = std::cmp::min(parallelism, remaining_completions).max(0);
            let pods_to_create = (desired_active - active_count).max(0);

            for created in 0..pods_to_create {
                let Some(live_state) = live_job_create_state(db, namespace, name).await? else {
                    return Ok(std::sync::Arc::unwrap_or_clone(latest_job.data.clone()));
                };
                if live_state.suspended {
                    break;
                }
                let live_remaining_completions = (live_state.completions - succeeded_count).max(0);
                let live_desired_active =
                    std::cmp::min(live_state.parallelism, live_remaining_completions).max(0);
                if active_count + created >= live_desired_active {
                    break;
                }
                let pod_name = format!(
                    "{}-{}",
                    name,
                    uuid::Uuid::new_v4()
                        .to_string()
                        .chars()
                        .take(5)
                        .collect::<String>()
                );

                let pod = crate::controllers::common::build_child_pod(
                    template,
                    &pod_name,
                    namespace,
                    node_name,
                    crate::controllers::common::OwnerInfo {
                        api_version: "batch/v1",
                        kind: "Job",
                        name,
                        uid,
                    },
                    &[],
                    &[],
                )?;

                pod_writer
                    .create_controller_pod(namespace, &pod_name, node_name, pod)
                    .await?;
            }
        }
    }

    let final_owned_pods = pod_reader.list_pods_by_owner_uid(namespace, uid).await?;
    let Some(status_job_resource) = db
        .get_resource("batch/v1", "Job", Some(namespace), name)
        .await?
    else {
        return Ok(std::sync::Arc::unwrap_or_clone(latest_job.data.clone()));
    };
    if status_job_resource
        .data
        .pointer("/metadata/deletionTimestamp")
        .is_some()
    {
        return Ok(std::sync::Arc::unwrap_or_clone(
            status_job_resource.data.clone(),
        ));
    }
    let status = derive_job_status_from_owned_pods(&status_job_resource.data, &final_owned_pods);

    let updated_resource =
        crate::controllers::common::write_status_for_resource(db, &status_job_resource, &status)
            .await?;

    if job_ttl_cleanup_delay(&updated_resource.data)?.is_some_and(|delay| delay.is_zero()) {
        delete_finished_job_for_ttl(db, &updated_resource, pod_delete_sink).await?;
    }

    Ok(std::sync::Arc::unwrap_or_clone(updated_resource.data))
}

fn job_pod_selector(spec: &Value, template: &Value) -> Option<Value> {
    if let Some(selector) = spec.get("selector") {
        return Some(selector.clone());
    }
    let labels = template
        .pointer("/metadata/labels")
        .and_then(|v| v.as_object())?;
    if labels.is_empty() {
        None
    } else {
        Some(json!({ "matchLabels": labels }))
    }
}

fn pod_matches_job_selector(pod: &Value, selector: &Value) -> bool {
    let parsed = match crate::label_selector::LabelSelector::from_k8s_selector(selector) {
        Ok(parsed) => parsed,
        Err(_) => return false,
    };
    parsed.matches_resource(pod)
}

fn pod_owned_by_job(pod: &Value, job_uid: &str) -> bool {
    pod.pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
        .is_some_and(|refs| {
            refs.iter().any(|owner| {
                owner.get("kind").and_then(|v| v.as_str()) == Some("Job")
                    && owner.get("uid").and_then(|v| v.as_str()) == Some(job_uid)
            })
        })
}

fn pod_has_controller_owner(pod: &Value) -> bool {
    pod.pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
        .is_some_and(|refs| {
            refs.iter()
                .any(|owner| owner.get("controller").and_then(|v| v.as_bool()) == Some(true))
        })
}

fn pod_is_ready(pod: &Value) -> bool {
    pod.pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .is_some_and(|conditions| {
            conditions.iter().any(|condition| {
                condition.get("type").and_then(|v| v.as_str()) == Some("Ready")
                    && condition.get("status").and_then(|v| v.as_str()) == Some("True")
            })
        })
}

#[cfg(test)]
mod tests;
