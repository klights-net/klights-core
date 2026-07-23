use crate::protobuf::*;
pub fn pb_csr_condition_to_json(
    cond: &k8s_pb::api::certificates::v1::CertificateSigningRequestCondition,
) -> Value {
    use serde_json::json;
    let mut obj = json!({});
    if let Some(t) = &cond.r#type {
        obj["type"] = json!(t);
    }
    if let Some(status) = &cond.status {
        obj["status"] = json!(status);
    }
    if let Some(reason) = &cond.reason {
        obj["reason"] = json!(reason);
    }
    if let Some(message) = &cond.message {
        obj["message"] = json!(message);
    }
    if let Some(last_update) = &cond.last_update_time {
        let ts = pb_time_to_json(last_update);
        if !ts.is_null() {
            obj["lastUpdateTime"] = ts;
        }
    }
    if let Some(last_transition) = &cond.last_transition_time {
        let ts = pb_time_to_json(last_transition);
        if !ts.is_null() {
            obj["lastTransitionTime"] = ts;
        }
    }
    obj
}

pub fn pb_csr_to_json(
    csr: &k8s_pb::api::certificates::v1::CertificateSigningRequest,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj =
        json!({"apiVersion": "certificates.k8s.io/v1", "kind": "CertificateSigningRequest"});

    if let Some(metadata) = &csr.metadata {
        obj["metadata"] = meta_to_json(metadata);
    }

    if let Some(spec) = &csr.spec {
        let mut spec_obj = json!({});
        if let Some(request) = &spec.request
            && !request.is_empty()
        {
            spec_obj["request"] = json!(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                request
            ));
        }
        if let Some(signer_name) = &spec.signer_name {
            spec_obj["signerName"] = json!(signer_name);
        }
        if let Some(expiration_seconds) = spec.expiration_seconds {
            spec_obj["expirationSeconds"] = json!(expiration_seconds);
        }
        if !spec.usages.is_empty() {
            spec_obj["usages"] = json!(spec.usages);
        }
        if let Some(username) = &spec.username {
            spec_obj["username"] = json!(username);
        }
        if let Some(uid) = &spec.uid {
            spec_obj["uid"] = json!(uid);
        }
        if !spec.groups.is_empty() {
            spec_obj["groups"] = json!(spec.groups);
        }
        if !spec.extra.is_empty() {
            let extra: serde_json::Map<String, Value> = spec
                .extra
                .iter()
                .map(|(k, v)| (k.clone(), json!(v.items)))
                .collect();
            spec_obj["extra"] = Value::Object(extra);
        }
        if spec_obj.as_object().is_some_and(|o| !o.is_empty()) {
            obj["spec"] = spec_obj;
        }
    }

    if let Some(status) = &csr.status {
        let mut status_obj = json!({});
        if !status.conditions.is_empty() {
            status_obj["conditions"] = json!(
                status
                    .conditions
                    .iter()
                    .map(pb_csr_condition_to_json)
                    .collect::<Vec<_>>()
            );
        }
        if let Some(certificate) = &status.certificate
            && !certificate.is_empty()
        {
            status_obj["certificate"] = json!(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                certificate
            ));
        }
        if status_obj.as_object().is_some_and(|o| !o.is_empty()) {
            obj["status"] = status_obj;
        }
    }

    Ok(obj)
}

pub fn pb_csrlist_to_json(
    list: &k8s_pb::api::certificates::v1::CertificateSigningRequestList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj =
        json!({"apiVersion": "certificates.k8s.io/v1", "kind": "CertificateSigningRequestList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .map(pb_csr_to_json)
        .collect::<Result<Vec<_>, _>>()?;
    obj["items"] = json!(items);
    Ok(obj)
}
