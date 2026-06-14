use crate::protobuf::*;
pb_decode!(
    pb_statefulset_to_json,
    k8s_pb::api::apps::v1::StatefulSet,
    sts,
    "apps/v1",
    "StatefulSet",
    obj,
    {
        if let Some(spec) = &sts.spec {
            let mut spec_obj = json!({});
            if let Some(replicas) = spec.replicas {
                spec_obj["replicas"] = json!(replicas);
            }
            if let Some(service_name) = &spec.service_name {
                spec_obj["serviceName"] = json!(service_name);
            }
            if let Some(policy) = &spec.pod_management_policy {
                spec_obj["podManagementPolicy"] = json!(policy);
            }
            if let Some(selector) = &spec.selector {
                let mut sel = json!({});
                if !selector.match_labels.is_empty() {
                    sel["matchLabels"] = json!(selector.match_labels);
                }
                spec_obj["selector"] = sel;
            }
            if let Some(update) = &spec.update_strategy {
                let mut update_obj = json!({});
                if let Some(t) = &update.r#type {
                    update_obj["type"] = json!(t);
                }
                if let Some(rolling) = &update.rolling_update {
                    let mut rolling_obj = json!({});
                    if let Some(partition) = rolling.partition {
                        rolling_obj["partition"] = json!(partition);
                    }
                    if let Some(max_unavailable) = &rolling.max_unavailable {
                        rolling_obj["maxUnavailable"] = intorstring_to_json(max_unavailable);
                    }
                    update_obj["rollingUpdate"] = rolling_obj;
                }
                spec_obj["updateStrategy"] = update_obj;
            }
            if let Some(limit) = spec.revision_history_limit {
                spec_obj["revisionHistoryLimit"] = json!(limit);
            }
            if let Some(min_ready) = spec.min_ready_seconds {
                spec_obj["minReadySeconds"] = json!(min_ready);
            }
            if let Some(template) = &spec.template {
                spec_obj["template"] = pb_pod_template_spec_to_json(template);
            }
            obj["spec"] = spec_obj;
        }
        if let Some(status) = &sts.status {
            let mut status_obj = json!({});
            if let Some(v) = status.observed_generation {
                status_obj["observedGeneration"] = json!(v);
            }
            if let Some(v) = status.replicas {
                status_obj["replicas"] = json!(v);
            }
            if let Some(v) = status.ready_replicas {
                status_obj["readyReplicas"] = json!(v);
            }
            if let Some(v) = status.current_replicas {
                status_obj["currentReplicas"] = json!(v);
            }
            if let Some(v) = status.updated_replicas {
                status_obj["updatedReplicas"] = json!(v);
            }
            if let Some(v) = &status.current_revision {
                status_obj["currentRevision"] = json!(v);
            }
            if let Some(v) = &status.update_revision {
                status_obj["updateRevision"] = json!(v);
            }
            if let Some(v) = status.collision_count {
                status_obj["collisionCount"] = json!(v);
            }
            if let Some(v) = status.available_replicas {
                status_obj["availableReplicas"] = json!(v);
            }
            if !status.conditions.is_empty() {
                status_obj["conditions"] = json!(
                    status
                        .conditions
                        .iter()
                        .map(|c| {
                            let mut cond = json!({
                                "type": c.r#type,
                                "status": c.status
                            });
                            if let Some(t) = &c.last_transition_time {
                                cond["lastTransitionTime"] = pb_time_to_json(t);
                            }
                            if let Some(reason) = &c.reason {
                                cond["reason"] = json!(reason);
                            }
                            if let Some(message) = &c.message {
                                cond["message"] = json!(message);
                            }
                            cond
                        })
                        .collect::<Vec<_>>()
                );
            }
            if !status_obj.as_object().is_some_and(|o| o.is_empty()) {
                obj["status"] = status_obj;
            }
        }
    }
);

pb_decode!(
    pb_daemonset_to_json,
    k8s_pb::api::apps::v1::DaemonSet,
    ds,
    "apps/v1",
    "DaemonSet",
    obj,
    {
        if let Some(spec) = &ds.spec {
            let mut spec_obj = json!({});
            if let Some(selector) = &spec.selector {
                let mut sel = json!({});
                if !selector.match_labels.is_empty() {
                    sel["matchLabels"] = json!(selector.match_labels);
                }
                spec_obj["selector"] = sel;
            }
            if let Some(template) = &spec.template {
                spec_obj["template"] = pb_pod_template_spec_to_json(template);
            }
            obj["spec"] = spec_obj;
        }
        if let Some(status) = &ds.status {
            let mut status_obj = json!({});
            if let Some(v) = status.current_number_scheduled {
                status_obj["currentNumberScheduled"] = json!(v);
            }
            if let Some(v) = status.number_misscheduled {
                status_obj["numberMisscheduled"] = json!(v);
            }
            if let Some(v) = status.desired_number_scheduled {
                status_obj["desiredNumberScheduled"] = json!(v);
            }
            if let Some(v) = status.number_ready {
                status_obj["numberReady"] = json!(v);
            }
            if let Some(v) = status.observed_generation {
                status_obj["observedGeneration"] = json!(v);
            }
            if let Some(v) = status.updated_number_scheduled {
                status_obj["updatedNumberScheduled"] = json!(v);
            }
            if let Some(v) = status.number_available {
                status_obj["numberAvailable"] = json!(v);
            }
            if let Some(v) = status.number_unavailable {
                status_obj["numberUnavailable"] = json!(v);
            }
            if let Some(v) = status.collision_count {
                status_obj["collisionCount"] = json!(v);
            }
            if !status.conditions.is_empty() {
                status_obj["conditions"] = json!(
                    status
                        .conditions
                        .iter()
                        .map(|c| {
                            let mut cond = json!({});
                            if let Some(t) = &c.r#type {
                                cond["type"] = json!(t);
                            }
                            if let Some(s) = &c.status {
                                cond["status"] = json!(s);
                            }
                            if let Some(t) = &c.last_transition_time {
                                cond["lastTransitionTime"] = pb_time_to_json(t);
                            }
                            if let Some(reason) = &c.reason {
                                cond["reason"] = json!(reason);
                            }
                            if let Some(message) = &c.message {
                                cond["message"] = json!(message);
                            }
                            cond
                        })
                        .collect::<Vec<_>>()
                );
            }
            if !status_obj.as_object().is_some_and(|o| o.is_empty()) {
                obj["status"] = status_obj;
            }
        }
    }
);
