/// Convert k8s-openapi Job to k8s-pb Job
use crate::protobuf::*;
pub fn json_job_to_pb(
    job: &k8s_openapi::api::batch::v1::Job,
) -> anyhow::Result<k8s_pb::api::batch::v1::Job> {
    Ok(k8s_pb::api::batch::v1::Job {
        metadata: Some(json_meta_to_pb(&job.metadata)),
        spec: job
            .spec
            .as_ref()
            .map(|spec| k8s_pb::api::batch::v1::JobSpec {
                parallelism: spec.parallelism,
                completions: spec.completions,
                active_deadline_seconds: spec.active_deadline_seconds,
                backoff_limit: spec.backoff_limit,
                backoff_limit_per_index: spec.backoff_limit_per_index,
                max_failed_indexes: spec.max_failed_indexes,
                selector: spec.selector.as_ref().map(|sel| {
                    k8s_pb::apimachinery::pkg::apis::meta::v1::LabelSelector {
                        match_labels: sel
                            .match_labels
                            .as_ref()
                            .map(|labels| {
                                labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
                            })
                            .unwrap_or_default(),
                        ..Default::default()
                    }
                }),
                manual_selector: spec.manual_selector,
                ttl_seconds_after_finished: spec.ttl_seconds_after_finished,
                completion_mode: spec.completion_mode.clone(),
                suspend: spec.suspend,
                success_policy: spec.success_policy.as_ref().map(|success_policy| {
                    k8s_pb::api::batch::v1::SuccessPolicy {
                        rules: success_policy
                            .rules
                            .iter()
                            .map(|rule| k8s_pb::api::batch::v1::SuccessPolicyRule {
                                succeeded_indexes: rule.succeeded_indexes.clone(),
                                succeeded_count: rule.succeeded_count,
                            })
                            .collect(),
                    }
                }),
                pod_failure_policy: spec.pod_failure_policy.as_ref().map(|pfp| {
                    k8s_pb::api::batch::v1::PodFailurePolicy {
                        rules: pfp
                            .rules
                            .iter()
                            .map(|rule| k8s_pb::api::batch::v1::PodFailurePolicyRule {
                                action: Some(rule.action.clone()),
                                on_exit_codes: rule.on_exit_codes.as_ref().map(|req| {
                                    k8s_pb::api::batch::v1::PodFailurePolicyOnExitCodesRequirement {
                                        container_name: req.container_name.clone(),
                                        operator: Some(req.operator.clone()),
                                        values: req.values.to_vec(),
                                    }
                                }),
                                on_pod_conditions: rule
                                    .on_pod_conditions
                                    .as_ref()
                                    .map(|conds| {
                                        conds
                                            .iter()
                                            .map(|c| k8s_pb::api::batch::v1::PodFailurePolicyOnPodConditionsPattern {
                                                r#type: Some(c.type_.clone()),
                                                status: Some(c.status.clone()),
                                            })
                                            .collect()
                                    })
                                    .unwrap_or_default(),
                            })
                            .collect(),
                    }
                }),
                template: Some(json_pod_template_spec_to_pb_encode(&spec.template)),
                ..Default::default()
            }),
        status: job
            .status
            .as_ref()
            .map(|status| k8s_pb::api::batch::v1::JobStatus {
                start_time: status.start_time.as_ref().map(json_time_to_pb),
                completion_time: status.completion_time.as_ref().map(json_time_to_pb),
                active: status.active,
                succeeded: status.succeeded,
                failed: status.failed,
                terminating: status.terminating,
                completed_indexes: status.completed_indexes.clone(),
                failed_indexes: status.failed_indexes.clone(),
                ready: status.ready,
                conditions: status
                    .conditions
                    .as_ref()
                    .map(|conds| {
                        conds
                            .iter()
                            .map(|c| k8s_pb::api::batch::v1::JobCondition {
                                r#type: Some(c.type_.clone()),
                                status: Some(c.status.clone()),
                                last_probe_time: c.last_probe_time.as_ref().map(json_time_to_pb),
                                last_transition_time: c
                                    .last_transition_time
                                    .as_ref()
                                    .map(json_time_to_pb),
                                reason: c.reason.clone(),
                                message: c.message.clone(),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                ..Default::default()
            }),
    })
}
