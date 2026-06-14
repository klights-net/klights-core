use crate::protobuf::*;
pb_decode!(
    pb_poddisruptionbudget_to_json,
    k8s_pb::api::policy::v1::PodDisruptionBudget,
    pdb,
    "policy/v1",
    "PodDisruptionBudget",
    obj,
    {
        if let Some(spec) = &pdb.spec {
            let mut spec_obj = json!({});
            if let Some(min_available) = &spec.min_available {
                spec_obj["minAvailable"] = intorstring_to_json(min_available);
            }
            if let Some(max_unavailable) = &spec.max_unavailable {
                spec_obj["maxUnavailable"] = intorstring_to_json(max_unavailable);
            }
            if let Some(selector) = &spec.selector {
                let mut sel = json!({});
                if !selector.match_labels.is_empty() {
                    sel["matchLabels"] = json!(selector.match_labels);
                }
                if !selector.match_expressions.is_empty() {
                    let exprs: Vec<Value> = selector.match_expressions.iter().map(|expr| {
                    json!({"key": expr.key, "operator": expr.operator, "values": expr.values})
                }).collect();
                    sel["matchExpressions"] = json!(exprs);
                }
                spec_obj["selector"] = sel;
            }
            if let Some(policy) = &spec.unhealthy_pod_eviction_policy {
                spec_obj["unhealthyPodEvictionPolicy"] = json!(policy);
            }
            obj["spec"] = spec_obj;
        }
        if let Some(status) = &pdb.status {
            let mut status_obj = json!({});
            if let Some(v) = status.observed_generation {
                status_obj["observedGeneration"] = json!(v);
            }
            if let Some(v) = status.disruptions_allowed {
                status_obj["disruptionsAllowed"] = json!(v);
            }
            if let Some(v) = status.current_healthy {
                status_obj["currentHealthy"] = json!(v);
            }
            if let Some(v) = status.desired_healthy {
                status_obj["desiredHealthy"] = json!(v);
            }
            if let Some(v) = status.expected_pods {
                status_obj["expectedPods"] = json!(v);
            }
            if !status.disrupted_pods.is_empty() {
                let mut disrupted = json!({});
                for (name, time) in &status.disrupted_pods {
                    let ts = time
                        .seconds
                        .map(|s| {
                            chrono::DateTime::from_timestamp(s, 0)
                                .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                                .unwrap_or_default()
                        })
                        .unwrap_or_default();
                    disrupted[name] = json!(ts);
                }
                status_obj["disruptedPods"] = disrupted;
            }
            if !status.conditions.is_empty() {
                let conditions: Vec<Value> = status
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
                        if let Some(r) = &c.reason {
                            cond["reason"] = json!(r);
                        }
                        if let Some(m) = &c.message {
                            cond["message"] = json!(m);
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
