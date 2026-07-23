use crate::protobuf::*;
pb_decode!(
    pb_job_to_json,
    k8s_pb::api::batch::v1::Job,
    job,
    "batch/v1",
    "Job",
    obj,
    {
        if let Some(spec) = &job.spec {
            let mut spec_obj = json!({});
            if let Some(template) = &spec.template {
                spec_obj["template"] = pb_pod_template_spec_to_json(template);
            }
            if let Some(completions) = spec.completions {
                spec_obj["completions"] = json!(completions);
            }
            if let Some(parallelism) = spec.parallelism {
                spec_obj["parallelism"] = json!(parallelism);
            }
            if let Some(backoff_limit) = spec.backoff_limit {
                spec_obj["backoffLimit"] = json!(backoff_limit);
            }
            if let Some(backoff_per_index) = spec.backoff_limit_per_index {
                spec_obj["backoffLimitPerIndex"] = json!(backoff_per_index);
            }
            if let Some(max_failed) = spec.max_failed_indexes {
                spec_obj["maxFailedIndexes"] = json!(max_failed);
            }
            if let Some(completion_mode) = &spec.completion_mode {
                spec_obj["completionMode"] = json!(completion_mode);
            }
            if let Some(suspend) = spec.suspend {
                spec_obj["suspend"] = json!(suspend);
            }
            if let Some(success_policy) = &spec.success_policy {
                let rules: Vec<Value> = success_policy
                    .rules
                    .iter()
                    .map(|rule| {
                        let mut obj = json!({});
                        if let Some(indexes) = &rule.succeeded_indexes {
                            obj["succeededIndexes"] = json!(indexes);
                        }
                        if let Some(count) = rule.succeeded_count {
                            obj["succeededCount"] = json!(count);
                        }
                        obj
                    })
                    .collect();
                spec_obj["successPolicy"] = json!({ "rules": rules });
            }
            if let Some(pfp) = &spec.pod_failure_policy {
                let rules: Vec<Value> = pfp
                    .rules
                    .iter()
                    .map(|rule| {
                        let mut obj = json!({});
                        if let Some(action) = &rule.action {
                            obj["action"] = json!(action);
                        }
                        if let Some(on_exit) = &rule.on_exit_codes {
                            let mut on_exit_obj = json!({});
                            if let Some(container_name) = &on_exit.container_name {
                                on_exit_obj["containerName"] = json!(container_name);
                            }
                            if let Some(operator) = &on_exit.operator {
                                on_exit_obj["operator"] = json!(operator);
                            }
                            if !on_exit.values.is_empty() {
                                on_exit_obj["values"] = json!(on_exit.values);
                            }
                            obj["onExitCodes"] = on_exit_obj;
                        }
                        if !rule.on_pod_conditions.is_empty() {
                            let conds: Vec<Value> = rule
                                .on_pod_conditions
                                .iter()
                                .map(|c| {
                                    let mut cobj = json!({});
                                    if let Some(t) = &c.r#type {
                                        cobj["type"] = json!(t);
                                    }
                                    if let Some(status) = &c.status {
                                        cobj["status"] = json!(status);
                                    }
                                    cobj
                                })
                                .collect();
                            obj["onPodConditions"] = json!(conds);
                        }
                        obj
                    })
                    .collect();
                spec_obj["podFailurePolicy"] = json!({ "rules": rules });
            }
            obj["spec"] = spec_obj;
        }
        if let Some(status) = &job.status {
            let mut status_obj = json!({});
            if let Some(v) = status.active {
                status_obj["active"] = json!(v);
            }
            if let Some(v) = status.succeeded {
                status_obj["succeeded"] = json!(v);
            }
            if let Some(v) = status.failed {
                status_obj["failed"] = json!(v);
            }
            if let Some(v) = status.ready {
                status_obj["ready"] = json!(v);
            }
            if let Some(v) = status.terminating {
                status_obj["terminating"] = json!(v);
            }
            if let Some(v) = &status.completed_indexes {
                status_obj["completedIndexes"] = json!(v);
            }
            if let Some(v) = &status.failed_indexes {
                status_obj["failedIndexes"] = json!(v);
            }
            if let Some(t) = &status.start_time {
                status_obj["startTime"] = pb_time_to_json(t);
            }
            if let Some(t) = &status.completion_time {
                status_obj["completionTime"] = pb_time_to_json(t);
            }
            if !status.conditions.is_empty() {
                let conditions: Vec<Value> = status
                    .conditions
                    .iter()
                    .map(|c| {
                        let mut cond = json!({
                            "type": c.r#type.as_deref().unwrap_or(""),
                            "status": c.status.as_deref().unwrap_or("")
                        });
                        if let Some(reason) = &c.reason {
                            cond["reason"] = json!(reason);
                        }
                        if let Some(message) = &c.message {
                            cond["message"] = json!(message);
                        }
                        if let Some(t) = &c.last_probe_time {
                            cond["lastProbeTime"] = pb_time_to_json(t);
                        }
                        if let Some(t) = &c.last_transition_time {
                            cond["lastTransitionTime"] = pb_time_to_json(t);
                        }
                        cond
                    })
                    .collect();
                status_obj["conditions"] = json!(conditions);
            }
            obj["status"] = status_obj;
        }
    }
);

pb_decode!(
    pb_cronjob_to_json,
    k8s_pb::api::batch::v1::CronJob,
    cronjob,
    "batch/v1",
    "CronJob",
    obj,
    {
        if let Some(spec) = &cronjob.spec {
            let mut spec_obj = json!({});
            if let Some(schedule) = &spec.schedule {
                spec_obj["schedule"] = json!(schedule);
            }
            if let Some(job_template) = &spec.job_template {
                let mut jt_obj = json!({});
                if let Some(jt_meta) = &job_template.metadata {
                    jt_obj["metadata"] = meta_to_json(jt_meta);
                }
                if let Some(jt_spec) = &job_template.spec {
                    let mut jt_spec_obj = json!({});
                    if let Some(template) = &jt_spec.template {
                        jt_spec_obj["template"] = pb_pod_template_spec_to_json(template);
                    }
                    if let Some(completions) = jt_spec.completions {
                        jt_spec_obj["completions"] = json!(completions);
                    }
                    if let Some(parallelism) = jt_spec.parallelism {
                        jt_spec_obj["parallelism"] = json!(parallelism);
                    }
                    if let Some(backoff_limit) = jt_spec.backoff_limit {
                        jt_spec_obj["backoffLimit"] = json!(backoff_limit);
                    }
                    jt_obj["spec"] = jt_spec_obj;
                }
                spec_obj["jobTemplate"] = jt_obj;
            }
            if let Some(concurrency_policy) = &spec.concurrency_policy {
                spec_obj["concurrencyPolicy"] = json!(concurrency_policy);
            }
            if let Some(suspend) = spec.suspend {
                spec_obj["suspend"] = json!(suspend);
            }
            if let Some(v) = spec.successful_jobs_history_limit {
                spec_obj["successfulJobsHistoryLimit"] = json!(v);
            }
            if let Some(v) = spec.failed_jobs_history_limit {
                spec_obj["failedJobsHistoryLimit"] = json!(v);
            }
            obj["spec"] = spec_obj;
        }
        if let Some(status) = &cronjob.status {
            let mut status_obj = json!({});
            if !status.active.is_empty() {
                let active_jobs: Vec<Value> = status
                    .active
                    .iter()
                    .map(|obj_ref| {
                        let mut ref_obj = json!({});
                        if let Some(name) = &obj_ref.name {
                            ref_obj["name"] = json!(name);
                        }
                        if let Some(namespace) = &obj_ref.namespace {
                            ref_obj["namespace"] = json!(namespace);
                        }
                        if let Some(kind) = &obj_ref.kind {
                            ref_obj["kind"] = json!(kind);
                        }
                        if let Some(api_version) = &obj_ref.api_version {
                            ref_obj["apiVersion"] = json!(api_version);
                        }
                        ref_obj
                    })
                    .collect();
                status_obj["active"] = json!(active_jobs);
            }
            if let Some(last_schedule_time) = &status.last_schedule_time
                && let Some(seconds) = last_schedule_time.seconds
            {
                let ts = chrono::DateTime::from_timestamp(
                    seconds,
                    last_schedule_time.nanos.unwrap_or(0) as u32,
                )
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_default();
                status_obj["lastScheduleTime"] = json!(ts);
            }
            if let Some(last_successful_time) = &status.last_successful_time
                && let Some(seconds) = last_successful_time.seconds
            {
                let ts = chrono::DateTime::from_timestamp(
                    seconds,
                    last_successful_time.nanos.unwrap_or(0) as u32,
                )
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_default();
                status_obj["lastSuccessfulTime"] = json!(ts);
            }
            obj["status"] = status_obj;
        }
    }
);
