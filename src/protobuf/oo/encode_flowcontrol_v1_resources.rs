use crate::protobuf::*;
pub fn json_flowschema_subject_to_pb(subj: &Value) -> k8s_pb::api::flowcontrol::v1::Subject {
    use k8s_pb::api::flowcontrol::v1 as fcv1;
    fcv1::Subject {
        kind: subj.get("kind").and_then(|v| v.as_str()).map(String::from),
        user: subj.get("user").map(|u| fcv1::UserSubject {
            name: u.get("name").and_then(|n| n.as_str()).map(String::from),
        }),
        group: subj.get("group").map(|g| fcv1::GroupSubject {
            name: g.get("name").and_then(|n| n.as_str()).map(String::from),
        }),
        service_account: subj
            .get("serviceAccount")
            .map(|sa| fcv1::ServiceAccountSubject {
                namespace: sa
                    .get("namespace")
                    .and_then(|n| n.as_str())
                    .map(String::from),
                name: sa.get("name").and_then(|n| n.as_str()).map(String::from),
            }),
    }
}

pub fn json_flowschema_to_pb(value: &Value) -> k8s_pb::api::flowcontrol::v1::FlowSchema {
    use k8s_pb::api::flowcontrol::v1 as fcv1;

    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta::deserialize(m).ok()?;
        Some(json_meta_to_pb(&openapi))
    });

    let spec = value.get("spec").map(|s| {
        let priority_level_configuration = s.get("priorityLevelConfiguration").map(|plc| {
            fcv1::PriorityLevelConfigurationReference {
                name: plc.get("name").and_then(|n| n.as_str()).map(String::from),
            }
        });

        let rules = s
            .get("rules")
            .and_then(|r| r.as_array())
            .map(|rules| {
                rules
                    .iter()
                    .map(|rule| fcv1::PolicyRulesWithSubjects {
                        subjects: rule
                            .get("subjects")
                            .and_then(|ss| ss.as_array())
                            .map(|ss| ss.iter().map(json_flowschema_subject_to_pb).collect())
                            .unwrap_or_default(),
                        resource_rules: rule
                            .get("resourceRules")
                            .and_then(|rr| rr.as_array())
                            .map(|rr| {
                                rr.iter()
                                    .map(|r| fcv1::ResourcePolicyRule {
                                        verbs: r
                                            .get("verbs")
                                            .and_then(|v| v.as_array())
                                            .map(|v| {
                                                v.iter()
                                                    .filter_map(|s| s.as_str().map(String::from))
                                                    .collect()
                                            })
                                            .unwrap_or_default(),
                                        api_groups: r
                                            .get("apiGroups")
                                            .and_then(|v| v.as_array())
                                            .map(|v| {
                                                v.iter()
                                                    .filter_map(|s| s.as_str().map(String::from))
                                                    .collect()
                                            })
                                            .unwrap_or_default(),
                                        resources: r
                                            .get("resources")
                                            .and_then(|v| v.as_array())
                                            .map(|v| {
                                                v.iter()
                                                    .filter_map(|s| s.as_str().map(String::from))
                                                    .collect()
                                            })
                                            .unwrap_or_default(),
                                        cluster_scope: r
                                            .get("clusterScope")
                                            .and_then(|v| v.as_bool()),
                                        namespaces: r
                                            .get("namespaces")
                                            .and_then(|v| v.as_array())
                                            .map(|v| {
                                                v.iter()
                                                    .filter_map(|s| s.as_str().map(String::from))
                                                    .collect()
                                            })
                                            .unwrap_or_default(),
                                    })
                                    .collect()
                            })
                            .unwrap_or_default(),
                        non_resource_rules: rule
                            .get("nonResourceRules")
                            .and_then(|nr| nr.as_array())
                            .map(|nr| {
                                nr.iter()
                                    .map(|r| fcv1::NonResourcePolicyRule {
                                        verbs: r
                                            .get("verbs")
                                            .and_then(|v| v.as_array())
                                            .map(|v| {
                                                v.iter()
                                                    .filter_map(|s| s.as_str().map(String::from))
                                                    .collect()
                                            })
                                            .unwrap_or_default(),
                                        non_resource_ur_ls: r
                                            .get("nonResourceURLs")
                                            .and_then(|v| v.as_array())
                                            .map(|v| {
                                                v.iter()
                                                    .filter_map(|s| s.as_str().map(String::from))
                                                    .collect()
                                            })
                                            .unwrap_or_default(),
                                    })
                                    .collect()
                            })
                            .unwrap_or_default(),
                    })
                    .collect()
            })
            .unwrap_or_default();

        fcv1::FlowSchemaSpec {
            priority_level_configuration,
            matching_precedence: s
                .get("matchingPrecedence")
                .and_then(|v| v.as_i64())
                .map(|v| v as i32),
            distinguisher_method: s.get("distinguisherMethod").map(|dm| {
                fcv1::FlowDistinguisherMethod {
                    r#type: dm.get("type").and_then(|t| t.as_str()).map(String::from),
                }
            }),
            rules,
        }
    });

    let status = value.get("status").map(|st| {
        let conditions = st
            .get("conditions")
            .and_then(|c| c.as_array())
            .map(|conds| {
                conds
                    .iter()
                    .map(|c| fcv1::FlowSchemaCondition {
                        r#type: c.get("type").and_then(|t| t.as_str()).map(String::from),
                        status: c.get("status").and_then(|s| s.as_str()).map(String::from),
                        last_transition_time: None,
                        reason: c.get("reason").and_then(|r| r.as_str()).map(String::from),
                        message: c.get("message").and_then(|m| m.as_str()).map(String::from),
                    })
                    .collect()
            })
            .unwrap_or_default();
        fcv1::FlowSchemaStatus { conditions }
    });

    fcv1::FlowSchema {
        metadata,
        spec,
        status,
    }
}

pub fn json_flowschemalist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::flowcontrol::v1::FlowSchemaList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("FlowSchemaList missing items array"))?
        .iter()
        .map(json_flowschema_to_pb)
        .collect();

    Ok(k8s_pb::api::flowcontrol::v1::FlowSchemaList { metadata, items })
}

pub fn json_prioritylevelconfiguration_to_pb(
    value: &Value,
) -> k8s_pb::api::flowcontrol::v1::PriorityLevelConfiguration {
    use k8s_pb::api::flowcontrol::v1 as fcv1;

    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta::deserialize(m).ok()?;
        Some(json_meta_to_pb(&openapi))
    });

    let spec = value.get("spec").map(|s| {
        let limited = s
            .get("limited")
            .map(|lim| fcv1::LimitedPriorityLevelConfiguration {
                nominal_concurrency_shares: lim
                    .get("nominalConcurrencyShares")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                limit_response: lim.get("limitResponse").map(|lr| fcv1::LimitResponse {
                    r#type: lr.get("type").and_then(|t| t.as_str()).map(String::from),
                    queuing: lr.get("queuing").map(|q| fcv1::QueuingConfiguration {
                        queues: q.get("queues").and_then(|v| v.as_i64()).map(|v| v as i32),
                        hand_size: q.get("handSize").and_then(|v| v.as_i64()).map(|v| v as i32),
                        queue_length_limit: q
                            .get("queueLengthLimit")
                            .and_then(|v| v.as_i64())
                            .map(|v| v as i32),
                    }),
                }),
                lendable_percent: lim
                    .get("lendablePercent")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                borrowing_limit_percent: lim
                    .get("borrowingLimitPercent")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
            });

        let exempt = s
            .get("exempt")
            .map(|ex| fcv1::ExemptPriorityLevelConfiguration {
                nominal_concurrency_shares: ex
                    .get("nominalConcurrencyShares")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                lendable_percent: ex
                    .get("lendablePercent")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
            });

        fcv1::PriorityLevelConfigurationSpec {
            r#type: s.get("type").and_then(|t| t.as_str()).map(String::from),
            limited,
            exempt,
        }
    });

    let status = value.get("status").map(|st| {
        let conditions = st
            .get("conditions")
            .and_then(|c| c.as_array())
            .map(|conds| {
                conds
                    .iter()
                    .map(|c| fcv1::PriorityLevelConfigurationCondition {
                        r#type: c.get("type").and_then(|t| t.as_str()).map(String::from),
                        status: c.get("status").and_then(|s| s.as_str()).map(String::from),
                        last_transition_time: None,
                        reason: c.get("reason").and_then(|r| r.as_str()).map(String::from),
                        message: c.get("message").and_then(|m| m.as_str()).map(String::from),
                    })
                    .collect()
            })
            .unwrap_or_default();
        fcv1::PriorityLevelConfigurationStatus { conditions }
    });

    fcv1::PriorityLevelConfiguration {
        metadata,
        spec,
        status,
    }
}

pub fn json_prioritylevelconfigurationlist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::flowcontrol::v1::PriorityLevelConfigurationList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("PriorityLevelConfigurationList missing items array"))?
        .iter()
        .map(json_prioritylevelconfiguration_to_pb)
        .collect();

    Ok(k8s_pb::api::flowcontrol::v1::PriorityLevelConfigurationList { metadata, items })
}
