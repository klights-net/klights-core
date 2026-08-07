//! Pure Kubernetes Pod graceful-delete deadline planning.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PodDeleteDeadlineDisposition {
    Initialize,
    Shorten,
    Unchanged,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PodDeleteDeadlinePlan {
    pub disposition: PodDeleteDeadlineDisposition,
    pub body: Value,
    pub remaining_delay: Duration,
    pub queue_actor_reminder: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodDeleteDeadlineError {
    message: String,
}

impl std::fmt::Display for PodDeleteDeadlineError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for PodDeleteDeadlineError {}

pub fn plan_pod_delete_deadline(
    pod: &Value,
    requested_grace_period_seconds: Option<i64>,
    operation_now: DateTime<Utc>,
) -> Result<PodDeleteDeadlinePlan, PodDeleteDeadlineError> {
    let metadata = pod
        .get("metadata")
        .and_then(Value::as_object)
        .ok_or_else(|| PodDeleteDeadlineError {
            message: "Pod metadata must be an object".to_string(),
        })?;
    let existing_timestamp = metadata
        .get("deletionTimestamp")
        .filter(|timestamp| !timestamp.is_null());

    if let Some(existing_timestamp) = existing_timestamp {
        let deadline = existing_timestamp
            .as_str()
            .ok_or_else(|| PodDeleteDeadlineError {
                message: "Pod deletionTimestamp must be a string".to_string(),
            })
            .and_then(|raw| {
                DateTime::parse_from_rfc3339(raw)
                    .map(|parsed| parsed.with_timezone(&Utc))
                    .map_err(|error| PodDeleteDeadlineError {
                        message: format!("invalid Pod deletionTimestamp: {error}"),
                    })
            })?;
        let existing_grace = metadata
            .get("deletionGracePeriodSeconds")
            .and_then(Value::as_i64);
        let Some(existing_grace) = existing_grace.filter(|grace| *grace > 0) else {
            return Ok(PodDeleteDeadlinePlan {
                disposition: PodDeleteDeadlineDisposition::Unchanged,
                body: pod.clone(),
                remaining_delay: Duration::ZERO,
                queue_actor_reminder: true,
            });
        };
        let remaining_delay = duration_until(deadline, operation_now);
        let Some(requested_grace) = requested_grace_period_seconds.map(normalize_grace) else {
            return Ok(PodDeleteDeadlinePlan {
                disposition: PodDeleteDeadlineDisposition::Unchanged,
                body: pod.clone(),
                remaining_delay,
                queue_actor_reminder: false,
            });
        };
        if requested_grace >= existing_grace {
            return Ok(PodDeleteDeadlinePlan {
                disposition: PodDeleteDeadlineDisposition::Unchanged,
                body: pod.clone(),
                remaining_delay,
                queue_actor_reminder: false,
            });
        }

        let original_start = deadline - chrono::Duration::seconds(existing_grace);
        let requested_deadline = original_start + chrono::Duration::seconds(requested_grace);
        let (deadline, stored_grace) = if requested_deadline < operation_now {
            (operation_now, i64::from(requested_grace != 0))
        } else {
            (requested_deadline, requested_grace)
        };
        let mut body = pod.clone();
        let metadata = body
            .get_mut("metadata")
            .and_then(Value::as_object_mut)
            .expect("validated Pod metadata remains an object");
        metadata.insert(
            "deletionTimestamp".to_string(),
            Value::String(klights_cluster_core::k8s_time::format_legacy_timestamp(
                deadline,
            )),
        );
        metadata.insert(
            "deletionGracePeriodSeconds".to_string(),
            json!(stored_grace),
        );
        mark_terminating_unready(&mut body, operation_now);
        return Ok(PodDeleteDeadlinePlan {
            disposition: PodDeleteDeadlineDisposition::Shorten,
            body,
            remaining_delay: duration_until(deadline, operation_now),
            queue_actor_reminder: true,
        });
    }

    let grace = normalize_grace(requested_grace_period_seconds.unwrap_or_else(|| {
        pod.pointer("/spec/terminationGracePeriodSeconds")
            .and_then(Value::as_i64)
            .unwrap_or(30)
    }));
    let deadline = operation_now + chrono::Duration::seconds(grace);
    let mut body = pod.clone();
    let metadata = body
        .get_mut("metadata")
        .and_then(Value::as_object_mut)
        .expect("validated Pod metadata remains an object");
    metadata.insert(
        "deletionTimestamp".to_string(),
        Value::String(klights_cluster_core::k8s_time::format_legacy_timestamp(
            deadline,
        )),
    );
    metadata.insert("deletionGracePeriodSeconds".to_string(), json!(grace));
    if let Some(generation) = metadata.get("generation").and_then(Value::as_i64)
        && generation > 0
    {
        metadata.insert("generation".to_string(), json!(generation + 1));
    }
    mark_terminating_unready(&mut body, operation_now);
    Ok(PodDeleteDeadlinePlan {
        disposition: PodDeleteDeadlineDisposition::Initialize,
        body,
        remaining_delay: duration_until(deadline, operation_now),
        queue_actor_reminder: true,
    })
}

fn normalize_grace(grace_period_seconds: i64) -> i64 {
    if grace_period_seconds < 0 {
        1
    } else {
        grace_period_seconds
    }
}

fn duration_until(deadline: DateTime<Utc>, operation_now: DateTime<Utc>) -> Duration {
    deadline
        .signed_duration_since(operation_now)
        .to_std()
        .unwrap_or(Duration::ZERO)
}

fn mark_terminating_unready(body: &mut Value, operation_now: DateTime<Utc>) {
    let transition_time = klights_cluster_core::k8s_time::format_legacy_timestamp(operation_now);
    klights_types::mark_terminating_pod_unready_at(body, &transition_time);
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 8, 12, 0, 0)
            .single()
            .expect("fixed operation time")
    }

    fn pod(spec_grace: Option<i64>, generation: Option<i64>) -> Value {
        let mut pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "pod-a",
                "namespace": "default",
                "uid": "uid-a",
                "resourceVersion": "99",
                "finalizers": ["example.test/hold"]
            },
            "spec": {"nodeName": "node-a", "containers": [{"name": "app", "image": "busybox"}]},
            "status": {"phase": "Running"}
        });
        if let Some(grace) = spec_grace {
            pod["spec"]["terminationGracePeriodSeconds"] = json!(grace);
        }
        if let Some(generation) = generation {
            pod["metadata"]["generation"] = json!(generation);
        }
        pod
    }

    fn timestamp(body: &Value) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(
            body.pointer("/metadata/deletionTimestamp")
                .and_then(Value::as_str)
                .expect("deletion timestamp"),
        )
        .expect("RFC3339 timestamp")
        .with_timezone(&Utc)
    }

    fn terminating(deadline_offset_seconds: i64, stored_grace: Option<i64>) -> Value {
        let mut pod = pod(Some(30), Some(7));
        pod["metadata"]["deletionTimestamp"] =
            json!((now() + chrono::Duration::seconds(deadline_offset_seconds)).to_rfc3339());
        if let Some(grace) = stored_grace {
            pod["metadata"]["deletionGracePeriodSeconds"] = json!(grace);
        }
        pod
    }

    #[test]
    fn kubernetes_v1_34_deadline_planner_table() {
        struct Case {
            name: &'static str,
            pod: Value,
            requested: Option<i64>,
            disposition: PodDeleteDeadlineDisposition,
            deadline_offset: i64,
            stored_grace: Option<i64>,
            remaining: u64,
            generation: Option<i64>,
        }

        let cases = [
            Case {
                name: "first_spec_30",
                pod: pod(Some(30), Some(7)),
                requested: None,
                disposition: PodDeleteDeadlineDisposition::Initialize,
                deadline_offset: 30,
                stored_grace: Some(30),
                remaining: 30,
                generation: Some(8),
            },
            Case {
                name: "request_over_spec",
                pod: pod(Some(30), Some(7)),
                requested: Some(5),
                disposition: PodDeleteDeadlineDisposition::Initialize,
                deadline_offset: 5,
                stored_grace: Some(5),
                remaining: 5,
                generation: Some(8),
            },
            Case {
                name: "negative_request_normalizes_to_one",
                pod: pod(Some(30), Some(7)),
                requested: Some(-9),
                disposition: PodDeleteDeadlineDisposition::Initialize,
                deadline_offset: 1,
                stored_grace: Some(1),
                remaining: 1,
                generation: Some(8),
            },
            Case {
                name: "missing_spec_defaults_30",
                pod: pod(None, None),
                requested: None,
                disposition: PodDeleteDeadlineDisposition::Initialize,
                deadline_offset: 30,
                stored_grace: Some(30),
                remaining: 30,
                generation: None,
            },
            Case {
                name: "zero_generation_not_incremented",
                pod: pod(Some(0), Some(0)),
                requested: None,
                disposition: PodDeleteDeadlineDisposition::Initialize,
                deadline_offset: 0,
                stored_grace: Some(0),
                remaining: 0,
                generation: Some(0),
            },
            Case {
                name: "repeat_absent_request",
                pod: terminating(30, Some(30)),
                requested: None,
                disposition: PodDeleteDeadlineDisposition::Unchanged,
                deadline_offset: 30,
                stored_grace: Some(30),
                remaining: 30,
                generation: Some(7),
            },
            Case {
                name: "repeat_equal",
                pod: terminating(30, Some(30)),
                requested: Some(30),
                disposition: PodDeleteDeadlineDisposition::Unchanged,
                deadline_offset: 30,
                stored_grace: Some(30),
                remaining: 30,
                generation: Some(7),
            },
            Case {
                name: "repeat_longer",
                pod: terminating(30, Some(30)),
                requested: Some(60),
                disposition: PodDeleteDeadlineDisposition::Unchanged,
                deadline_offset: 30,
                stored_grace: Some(30),
                remaining: 30,
                generation: Some(7),
            },
            Case {
                name: "shorten_future",
                pod: terminating(30, Some(30)),
                requested: Some(5),
                disposition: PodDeleteDeadlineDisposition::Shorten,
                deadline_offset: 5,
                stored_grace: Some(5),
                remaining: 5,
                generation: Some(7),
            },
            Case {
                name: "shorten_expired_to_zero",
                pod: terminating(-5, Some(30)),
                requested: Some(0),
                disposition: PodDeleteDeadlineDisposition::Shorten,
                deadline_offset: 0,
                stored_grace: Some(0),
                remaining: 0,
                generation: Some(7),
            },
            Case {
                name: "shorten_expired_positive_clamps_grace_one",
                pod: terminating(-5, Some(30)),
                requested: Some(5),
                disposition: PodDeleteDeadlineDisposition::Shorten,
                deadline_offset: 0,
                stored_grace: Some(1),
                remaining: 0,
                generation: Some(7),
            },
            Case {
                name: "existing_nil_grace_is_immediate_unchanged",
                pod: terminating(-5, None),
                requested: Some(0),
                disposition: PodDeleteDeadlineDisposition::Unchanged,
                deadline_offset: -5,
                stored_grace: None,
                remaining: 0,
                generation: Some(7),
            },
            Case {
                name: "existing_zero_grace_is_immediate_unchanged",
                pod: terminating(-5, Some(0)),
                requested: Some(0),
                disposition: PodDeleteDeadlineDisposition::Unchanged,
                deadline_offset: -5,
                stored_grace: Some(0),
                remaining: 0,
                generation: Some(7),
            },
        ];

        for case in cases {
            let original = case.pod.clone();
            let plan = plan_pod_delete_deadline(&case.pod, case.requested, now())
                .expect("valid Pod deadline plan");
            assert_eq!(
                plan.disposition, case.disposition,
                "{} disposition",
                case.name
            );
            assert_eq!(
                timestamp(&plan.body),
                now() + chrono::Duration::seconds(case.deadline_offset),
                "{} deadline",
                case.name
            );
            assert_eq!(
                plan.body
                    .pointer("/metadata/deletionGracePeriodSeconds")
                    .and_then(Value::as_i64),
                case.stored_grace,
                "{} stored grace",
                case.name
            );
            assert_eq!(
                plan.remaining_delay,
                Duration::from_secs(case.remaining),
                "{} remaining delay",
                case.name
            );
            assert_eq!(
                plan.body
                    .pointer("/metadata/generation")
                    .and_then(Value::as_i64),
                case.generation,
                "{} generation",
                case.name
            );
            assert_eq!(
                plan.body.pointer("/metadata/uid"),
                original.pointer("/metadata/uid"),
                "{} UID",
                case.name
            );
            assert_eq!(
                plan.body.pointer("/metadata/resourceVersion"),
                original.pointer("/metadata/resourceVersion"),
                "{} resourceVersion",
                case.name
            );
            assert_eq!(
                plan.body.pointer("/metadata/finalizers"),
                original.pointer("/metadata/finalizers"),
                "{} finalizers",
                case.name
            );
            let existing_positive_grace = original
                .pointer("/metadata/deletionGracePeriodSeconds")
                .and_then(Value::as_i64)
                .is_some_and(|grace| grace > 0);
            assert_eq!(
                plan.queue_actor_reminder,
                case.disposition != PodDeleteDeadlineDisposition::Unchanged
                    || !existing_positive_grace,
                "{} actor reminder",
                case.name
            );
            if case.disposition == PodDeleteDeadlineDisposition::Unchanged {
                assert_eq!(
                    plan.body, original,
                    "{} must preserve the body byte-semantically",
                    case.name
                );
            }
        }
    }
}
