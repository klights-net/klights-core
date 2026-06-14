use crate::protobuf::*;
pub fn json_runtimeclass_to_pb(
    rc: &k8s_openapi::api::node::v1::RuntimeClass,
) -> k8s_pb::api::node::v1::RuntimeClass {
    k8s_pb::api::node::v1::RuntimeClass {
        metadata: Some(json_meta_to_pb(&rc.metadata)),
        handler: Some(rc.handler.clone()),
        overhead: rc
            .overhead
            .as_ref()
            .map(|o| k8s_pb::api::node::v1::Overhead {
                pod_fixed: o
                    .pod_fixed
                    .as_ref()
                    .map(|pf| {
                        pf.iter()
                            .map(|(k, v)| {
                                (
                                    k.clone(),
                                    k8s_pb::apimachinery::pkg::api::resource::Quantity {
                                        string: Some(v.0.clone()),
                                    },
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            }),
        scheduling: rc
            .scheduling
            .as_ref()
            .map(|s| k8s_pb::api::node::v1::Scheduling {
                node_selector: s
                    .node_selector
                    .as_ref()
                    .map(|ns| ns.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                    .unwrap_or_default(),
                tolerations: s
                    .tolerations
                    .as_ref()
                    .map(|tols| {
                        tols.iter()
                            .map(|t| k8s_pb::api::core::v1::Toleration {
                                key: t.key.clone(),
                                operator: t.operator.clone(),
                                value: t.value.clone(),
                                effect: t.effect.clone(),
                                toleration_seconds: t.toleration_seconds,
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            }),
    }
}
