use crate::protobuf::*;
pub fn json_priorityclass_to_pb(
    pc: &k8s_openapi::api::scheduling::v1::PriorityClass,
) -> k8s_pb::api::scheduling::v1::PriorityClass {
    k8s_pb::api::scheduling::v1::PriorityClass {
        metadata: Some(json_meta_to_pb(&pc.metadata)),
        value: Some(pc.value),
        global_default: pc.global_default,
        description: pc.description.clone(),
        preemption_policy: pc.preemption_policy.clone(),
    }
}
