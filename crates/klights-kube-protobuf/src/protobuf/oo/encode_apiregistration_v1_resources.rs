use crate::protobuf::*;
pub fn json_apiservice_condition_to_pb(
    cond: &k8s_openapi::kube_aggregator::pkg::apis::apiregistration::v1::APIServiceCondition,
) -> k8s_pb::kube_aggregator::pkg::apis::apiregistration::v1::APIServiceCondition {
    k8s_pb::kube_aggregator::pkg::apis::apiregistration::v1::APIServiceCondition {
        r#type: Some(cond.type_.clone()),
        status: Some(cond.status.clone()),
        last_transition_time: cond.last_transition_time.as_ref().map(json_time_to_pb),
        reason: cond.reason.clone(),
        message: cond.message.clone(),
    }
}

pub fn json_apiservice_to_pb(
    apiservice: &k8s_openapi::kube_aggregator::pkg::apis::apiregistration::v1::APIService,
) -> anyhow::Result<k8s_pb::kube_aggregator::pkg::apis::apiregistration::v1::APIService> {
    use k8s_pb::kube_aggregator::pkg::apis::apiregistration::v1 as apiregistrationv1;

    Ok(apiregistrationv1::APIService {
        metadata: Some(json_meta_to_pb(&apiservice.metadata)),
        spec: apiservice
            .spec
            .as_ref()
            .map(|spec| apiregistrationv1::APIServiceSpec {
                service: spec
                    .service
                    .as_ref()
                    .map(|svc| apiregistrationv1::ServiceReference {
                        namespace: svc.namespace.clone(),
                        name: svc.name.clone(),
                        port: svc.port,
                    }),
                group: spec.group.clone(),
                version: spec.version.clone(),
                insecure_skip_tls_verify: spec.insecure_skip_tls_verify,
                ca_bundle: spec.ca_bundle.as_ref().map(|b| b.0.clone()),
                group_priority_minimum: Some(spec.group_priority_minimum),
                version_priority: Some(spec.version_priority),
            }),
        status: apiservice
            .status
            .as_ref()
            .map(|status| apiregistrationv1::APIServiceStatus {
                conditions: status
                    .conditions
                    .as_ref()
                    .map(|conds| conds.iter().map(json_apiservice_condition_to_pb).collect())
                    .unwrap_or_default(),
            }),
    })
}

pub fn json_apiservicelist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::kube_aggregator::pkg::apis::apiregistration::v1::APIServiceList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("APIServiceList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::kube_aggregator::pkg::apis::apiregistration::v1::APIService::deserialize(item)?;
            json_apiservice_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(
        k8s_pb::kube_aggregator::pkg::apis::apiregistration::v1::APIServiceList {
            metadata,
            items: pb_items,
        },
    )
}
