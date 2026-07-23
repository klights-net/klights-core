use crate::protobuf::*;
pub fn pb_joblist_to_json(list: &k8s_pb::api::batch::v1::JobList) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "batch/v1", "kind": "JobList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_job_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}

/// CronJobList decoder
pub fn pb_cronjoblist_to_json(list: &k8s_pb::api::batch::v1::CronJobList) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "batch/v1", "kind": "CronJobList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_cronjob_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}
