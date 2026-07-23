use crate::protobuf::*;
pb_decode!(
    pb_scale_to_json,
    k8s_pb::api::autoscaling::v1::Scale,
    scale,
    "autoscaling/v1",
    "Scale",
    obj,
    {
        if let Some(spec) = &scale.spec {
            let mut spec_obj = json!({});
            if let Some(replicas) = spec.replicas {
                spec_obj["replicas"] = json!(replicas);
            }
            obj["spec"] = spec_obj;
        }
        if let Some(status) = &scale.status {
            let mut status_obj = json!({});
            if let Some(replicas) = status.replicas {
                status_obj["replicas"] = json!(replicas);
            }
            if let Some(selector) = &status.selector {
                status_obj["selector"] = json!(selector);
            }
            obj["status"] = status_obj;
        }
    }
);
