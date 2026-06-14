use crate::protobuf::*;
pub fn json_scale_to_pb(value: &Value) -> k8s_pb::api::autoscaling::v1::Scale {
    use k8s_pb::api::autoscaling::v1 as autoscalingv1;

    let metadata = value.get("metadata").map(|m| {
        let openapi = k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta::deserialize(m)
            .unwrap_or_default();
        json_meta_to_pb(&openapi)
    });

    let spec = value.get("spec").map(|s| autoscalingv1::ScaleSpec {
        replicas: s.get("replicas").and_then(|r| r.as_i64()).map(|r| r as i32),
    });

    let status = value.get("status").map(|s| autoscalingv1::ScaleStatus {
        replicas: s.get("replicas").and_then(|r| r.as_i64()).map(|r| r as i32),
        selector: s.get("selector").and_then(|s| s.as_str()).map(String::from),
    });

    autoscalingv1::Scale {
        metadata,
        spec,
        status,
    }
}
