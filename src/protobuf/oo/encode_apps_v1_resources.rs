/// Convert k8s-openapi Deployment to k8s-pb Deployment
use crate::protobuf::*;
pub fn json_deployment_to_pb(
    dep: &k8s_openapi::api::apps::v1::Deployment,
) -> anyhow::Result<k8s_pb::api::apps::v1::Deployment> {
    Ok(k8s_pb::api::apps::v1::Deployment {
        metadata: Some(json_meta_to_pb(&dep.metadata)),
        spec: dep
            .spec
            .as_ref()
            .map(|spec| k8s_pb::api::apps::v1::DeploymentSpec {
                replicas: spec.replicas,
                selector: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::LabelSelector {
                    match_labels: spec
                        .selector
                        .match_labels
                        .as_ref()
                        .map(|labels| labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                        .unwrap_or_default(),
                    ..Default::default()
                }),
                strategy: spec.strategy.as_ref().map(|strat| {
                    k8s_pb::api::apps::v1::DeploymentStrategy {
                        r#type: strat.type_.clone(),
                        rolling_update: strat.rolling_update.as_ref().map(|ru| {
                            k8s_pb::api::apps::v1::RollingUpdateDeployment {
                                max_unavailable: ru
                                    .max_unavailable
                                    .as_ref()
                                    .map(json_intorstring_to_pb),
                                max_surge: ru.max_surge.as_ref().map(json_intorstring_to_pb),
                            }
                        }),
                    }
                }),
                min_ready_seconds: spec.min_ready_seconds,
                revision_history_limit: spec.revision_history_limit,
                paused: spec.paused,
                progress_deadline_seconds: spec.progress_deadline_seconds,
                template: Some(json_pod_template_spec_to_pb_encode(&spec.template)),
            }),
        status: dep
            .status
            .as_ref()
            .map(|status| k8s_pb::api::apps::v1::DeploymentStatus {
                observed_generation: status.observed_generation,
                replicas: status.replicas,
                updated_replicas: status.updated_replicas,
                ready_replicas: status.ready_replicas,
                available_replicas: status.available_replicas,
                unavailable_replicas: status.unavailable_replicas,
                collision_count: status.collision_count,
                conditions: status
                    .conditions
                    .as_ref()
                    .map(|conditions| {
                        conditions
                            .iter()
                            .map(|condition| k8s_pb::api::apps::v1::DeploymentCondition {
                                r#type: Some(condition.type_.clone()),
                                status: Some(condition.status.clone()),
                                last_update_time: condition
                                    .last_update_time
                                    .as_ref()
                                    .map(json_time_to_pb),
                                last_transition_time: condition
                                    .last_transition_time
                                    .as_ref()
                                    .map(json_time_to_pb),
                                reason: condition.reason.clone(),
                                message: condition.message.clone(),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                terminating_replicas: None,
            }),
    })
}

/// Convert k8s-openapi ReplicaSet to k8s-pb ReplicaSet
pub fn json_replicaset_to_pb(
    rs: &k8s_openapi::api::apps::v1::ReplicaSet,
) -> anyhow::Result<k8s_pb::api::apps::v1::ReplicaSet> {
    Ok(k8s_pb::api::apps::v1::ReplicaSet {
        metadata: Some(json_meta_to_pb(&rs.metadata)),
        spec: rs
            .spec
            .as_ref()
            .map(|spec| k8s_pb::api::apps::v1::ReplicaSetSpec {
                replicas: spec.replicas,
                min_ready_seconds: spec.min_ready_seconds,
                selector: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::LabelSelector {
                    match_labels: spec
                        .selector
                        .match_labels
                        .as_ref()
                        .map(|labels| labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                        .unwrap_or_default(),
                    ..Default::default()
                }),
                template: spec
                    .template
                    .as_ref()
                    .map(json_pod_template_spec_to_pb_encode),
            }),
        status: rs
            .status
            .as_ref()
            .map(|status| k8s_pb::api::apps::v1::ReplicaSetStatus {
                replicas: Some(status.replicas),
                fully_labeled_replicas: status.fully_labeled_replicas,
                ready_replicas: status.ready_replicas,
                available_replicas: status.available_replicas,
                observed_generation: status.observed_generation,
                ..Default::default()
            }),
    })
}

/// Convert k8s-openapi StatefulSet to k8s-pb StatefulSet
pub fn json_statefulset_to_pb(
    ss: &k8s_openapi::api::apps::v1::StatefulSet,
) -> anyhow::Result<k8s_pb::api::apps::v1::StatefulSet> {
    Ok(k8s_pb::api::apps::v1::StatefulSet {
        metadata: Some(json_meta_to_pb(&ss.metadata)),
        spec: ss
            .spec
            .as_ref()
            .map(|spec| k8s_pb::api::apps::v1::StatefulSetSpec {
                replicas: spec.replicas,
                selector: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::LabelSelector {
                    match_labels: spec
                        .selector
                        .match_labels
                        .as_ref()
                        .map(|labels| labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                        .unwrap_or_default(),
                    ..Default::default()
                }),
                service_name: Some(spec.service_name.clone()),
                pod_management_policy: spec.pod_management_policy.clone(),
                update_strategy: spec.update_strategy.as_ref().map(|strat| {
                    k8s_pb::api::apps::v1::StatefulSetUpdateStrategy {
                        r#type: strat.type_.clone(),
                        rolling_update: strat.rolling_update.as_ref().map(|r| {
                            k8s_pb::api::apps::v1::RollingUpdateStatefulSetStrategy {
                                partition: r.partition,
                                max_unavailable: r
                                    .max_unavailable
                                    .as_ref()
                                    .map(json_intorstring_to_pb),
                            }
                        }),
                    }
                }),
                revision_history_limit: spec.revision_history_limit,
                min_ready_seconds: spec.min_ready_seconds,
                template: Some(json_pod_template_spec_to_pb_encode(&spec.template)),
                ..Default::default()
            }),
        status: ss
            .status
            .as_ref()
            .map(|status| k8s_pb::api::apps::v1::StatefulSetStatus {
                observed_generation: status.observed_generation,
                replicas: Some(status.replicas),
                ready_replicas: status.ready_replicas,
                current_replicas: status.current_replicas,
                updated_replicas: status.updated_replicas,
                current_revision: status.current_revision.clone(),
                update_revision: status.update_revision.clone(),
                collision_count: status.collision_count,
                available_replicas: status.available_replicas,
                ..Default::default()
            }),
    })
}

/// Convert k8s-openapi DaemonSet to k8s-pb DaemonSet
pub fn json_daemonset_to_pb(
    ds: &k8s_openapi::api::apps::v1::DaemonSet,
) -> anyhow::Result<k8s_pb::api::apps::v1::DaemonSet> {
    Ok(k8s_pb::api::apps::v1::DaemonSet {
        metadata: Some(json_meta_to_pb(&ds.metadata)),
        spec: ds
            .spec
            .as_ref()
            .map(|spec| k8s_pb::api::apps::v1::DaemonSetSpec {
                selector: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::LabelSelector {
                    match_labels: spec
                        .selector
                        .match_labels
                        .as_ref()
                        .map(|labels| labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                        .unwrap_or_default(),
                    ..Default::default()
                }),
                update_strategy: spec.update_strategy.as_ref().map(|strat| {
                    k8s_pb::api::apps::v1::DaemonSetUpdateStrategy {
                        r#type: strat.type_.clone(),
                        ..Default::default()
                    }
                }),
                min_ready_seconds: spec.min_ready_seconds,
                revision_history_limit: spec.revision_history_limit,
                template: Some(json_pod_template_spec_to_pb_encode(&spec.template)),
            }),
        status: ds
            .status
            .as_ref()
            .map(|status| k8s_pb::api::apps::v1::DaemonSetStatus {
                current_number_scheduled: Some(status.current_number_scheduled),
                number_misscheduled: Some(status.number_misscheduled),
                desired_number_scheduled: Some(status.desired_number_scheduled),
                number_ready: Some(status.number_ready),
                observed_generation: status.observed_generation,
                updated_number_scheduled: status.updated_number_scheduled,
                number_available: status.number_available,
                number_unavailable: status.number_unavailable,
                collision_count: status.collision_count,
                conditions: status
                    .conditions
                    .as_ref()
                    .map(|conditions| {
                        conditions
                            .iter()
                            .map(|condition| k8s_pb::api::apps::v1::DaemonSetCondition {
                                r#type: Some(condition.type_.clone()),
                                status: Some(condition.status.clone()),
                                last_transition_time: condition
                                    .last_transition_time
                                    .as_ref()
                                    .map(json_time_to_pb),
                                reason: condition.reason.clone(),
                                message: condition.message.clone(),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            }),
    })
}
