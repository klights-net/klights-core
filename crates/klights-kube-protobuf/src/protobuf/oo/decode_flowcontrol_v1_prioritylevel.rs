use crate::protobuf::*;
pub fn pb_prioritylevelconfiguration_to_json(
    plc: &k8s_pb::api::flowcontrol::v1::PriorityLevelConfiguration,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "flowcontrol.apiserver.k8s.io/v1", "kind": "PriorityLevelConfiguration"});
    if let Some(metadata) = &plc.metadata {
        obj["metadata"] = meta_to_json(metadata);
    }
    if let Some(spec) = &plc.spec {
        let mut spec_obj = json!({});
        if let Some(t) = &spec.r#type {
            spec_obj["type"] = json!(t);
        }
        if let Some(lim) = &spec.limited {
            let mut lim_obj = json!({});
            if let Some(ncs) = lim.nominal_concurrency_shares {
                lim_obj["nominalConcurrencyShares"] = json!(ncs);
            }
            if let Some(lr) = &lim.limit_response {
                let mut lr_obj = json!({});
                if let Some(t) = &lr.r#type {
                    lr_obj["type"] = json!(t);
                }
                if let Some(q) = &lr.queuing {
                    lr_obj["queuing"] = json!({
                        "queues": q.queues,
                        "handSize": q.hand_size,
                        "queueLengthLimit": q.queue_length_limit,
                    });
                }
                lim_obj["limitResponse"] = lr_obj;
            }
            if let Some(lp) = lim.lendable_percent {
                lim_obj["lendablePercent"] = json!(lp);
            }
            if let Some(blp) = lim.borrowing_limit_percent {
                lim_obj["borrowingLimitPercent"] = json!(blp);
            }
            spec_obj["limited"] = lim_obj;
        }
        if let Some(ex) = &spec.exempt {
            let mut ex_obj = json!({});
            if let Some(ncs) = ex.nominal_concurrency_shares {
                ex_obj["nominalConcurrencyShares"] = json!(ncs);
            }
            if let Some(lp) = ex.lendable_percent {
                ex_obj["lendablePercent"] = json!(lp);
            }
            spec_obj["exempt"] = ex_obj;
        }
        obj["spec"] = spec_obj;
    }
    if let Some(status) = &plc.status
        && !status.conditions.is_empty()
    {
        let conds: Vec<Value> = status
            .conditions
            .iter()
            .map(|c| {
                json!({
                    "type": c.r#type,
                    "status": c.status,
                    "reason": c.reason,
                    "message": c.message,
                })
            })
            .collect();
        obj["status"] = json!({"conditions": conds});
    }
    Ok(obj)
}

pub fn pb_prioritylevelconfigurationlist_to_json(
    list: &k8s_pb::api::flowcontrol::v1::PriorityLevelConfigurationList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "flowcontrol.apiserver.k8s.io/v1", "kind": "PriorityLevelConfigurationList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_prioritylevelconfiguration_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}
