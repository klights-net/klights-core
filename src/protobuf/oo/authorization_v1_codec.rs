//! AuthorizationV1Codec: OO protobuf codec for authorization.k8s.io/v1 resources.
//!
//! Handles round-trip encode/decode for SubjectAccessReview, SelfSubjectAccessReview,
//! LocalSubjectAccessReview, and SelfSubjectRulesReview.
//!
//! Dispatch is owned by the global OO protobuf registry.

use crate::protobuf::ResourceProtoCodec;
use crate::protobuf::*;
use anyhow::Context;
use serde_json::Value;

/// (api_version_prefix, kind) entries for authorization.k8s.io resources.
const AUTHZ_ENTRIES: &[(&str, &str)] = &[
    ("authorization.k8s.io", "SubjectAccessReview"),
    ("authorization.k8s.io", "SelfSubjectAccessReview"),
    ("authorization.k8s.io", "LocalSubjectAccessReview"),
    ("authorization.k8s.io", "SelfSubjectRulesReview"),
];

/// Codec for authorization.k8s.io/v1 resources.
pub struct AuthorizationV1Codec;

impl ResourceProtoCodec for AuthorizationV1Codec {
    fn entry_keys(&self) -> &'static [(&'static str, &'static str)] {
        AUTHZ_ENTRIES
    }

    fn decode_to_json(&self, _api_version: &str, kind: &str, data: &[u8]) -> anyhow::Result<Value> {
        use prost::Message;
        match kind {
            "SubjectAccessReview" => {
                let pb = k8s_pb::api::authorization::v1::SubjectAccessReview::decode(data)
                    .context("failed to decode SubjectAccessReview protobuf")?;
                pb_subject_access_review_to_json(&pb)
            }
            "SelfSubjectAccessReview" => {
                let pb = k8s_pb::api::authorization::v1::SelfSubjectAccessReview::decode(data)
                    .context("failed to decode SelfSubjectAccessReview protobuf")?;
                pb_self_subject_access_review_to_json(&pb)
            }
            "LocalSubjectAccessReview" => {
                let pb = k8s_pb::api::authorization::v1::LocalSubjectAccessReview::decode(data)
                    .context("failed to decode LocalSubjectAccessReview protobuf")?;
                pb_local_subject_access_review_to_json(&pb)
            }
            "SelfSubjectRulesReview" => {
                let pb = k8s_pb::api::authorization::v1::SelfSubjectRulesReview::decode(data)
                    .context("failed to decode SelfSubjectRulesReview protobuf")?;
                pb_self_subject_rules_review_to_json(&pb)
            }
            _ => anyhow::bail!("AuthorizationV1Codec: unknown kind {kind}"),
        }
    }

    fn encode_from_json(
        &self,
        _api_version: &str,
        kind: &str,
        value: &Value,
    ) -> anyhow::Result<Vec<u8>> {
        match kind {
            "SubjectAccessReview" => {
                let pb = json_subject_access_review_to_pb(value)?;
                encode_message_to_vec(&pb)
            }
            "SelfSubjectAccessReview" => {
                let pb = json_self_subject_access_review_to_pb(value)?;
                encode_message_to_vec(&pb)
            }
            "LocalSubjectAccessReview" => {
                let pb = json_local_subject_access_review_to_pb(value)?;
                encode_message_to_vec(&pb)
            }
            "SelfSubjectRulesReview" => {
                let pb = json_self_subject_rules_review_to_pb(value)?;
                encode_message_to_vec(&pb)
            }
            _ => anyhow::bail!("AuthorizationV1Codec: unknown kind {kind}"),
        }
    }
}

#[cfg(test)]
impl AuthorizationV1Codec {
    fn decode_to_json(&self, kind: &str, data: &[u8]) -> anyhow::Result<Value> {
        <Self as ResourceProtoCodec>::decode_to_json(self, "authorization.k8s.io/v1", kind, data)
    }

    fn encode_from_json(&self, kind: &str, value: &Value) -> anyhow::Result<Vec<u8>> {
        <Self as ResourceProtoCodec>::encode_from_json(self, "authorization.k8s.io/v1", kind, value)
    }
}

// ---------------------------------------------------------------------------
// Decode: protobuf → JSON
// ---------------------------------------------------------------------------

fn pb_resource_attributes_to_json(
    attrs: &k8s_pb::api::authorization::v1::ResourceAttributes,
) -> Value {
    let mut obj = serde_json::Map::new();
    if let Some(v) = &attrs.namespace {
        obj.insert("namespace".into(), Value::String(v.clone()));
    }
    if let Some(v) = &attrs.verb {
        obj.insert("verb".into(), Value::String(v.clone()));
    }
    if let Some(v) = &attrs.group {
        obj.insert("group".into(), Value::String(v.clone()));
    }
    if let Some(v) = &attrs.version {
        obj.insert("version".into(), Value::String(v.clone()));
    }
    if let Some(v) = &attrs.resource {
        obj.insert("resource".into(), Value::String(v.clone()));
    }
    if let Some(v) = &attrs.subresource {
        obj.insert("subresource".into(), Value::String(v.clone()));
    }
    if let Some(v) = &attrs.name {
        obj.insert("name".into(), Value::String(v.clone()));
    }
    Value::Object(obj)
}

fn pb_non_resource_attributes_to_json(
    attrs: &k8s_pb::api::authorization::v1::NonResourceAttributes,
) -> Value {
    let mut obj = serde_json::Map::new();
    if let Some(v) = &attrs.path {
        obj.insert("path".into(), Value::String(v.clone()));
    }
    if let Some(v) = &attrs.verb {
        obj.insert("verb".into(), Value::String(v.clone()));
    }
    Value::Object(obj)
}

fn pb_spec_to_json(
    resource_attributes: &Option<k8s_pb::api::authorization::v1::ResourceAttributes>,
    non_resource_attributes: &Option<k8s_pb::api::authorization::v1::NonResourceAttributes>,
    user: &Option<String>,
    groups: &[String],
    extra: &std::collections::BTreeMap<String, k8s_pb::api::authorization::v1::ExtraValue>,
    uid: &Option<String>,
) -> Value {
    let mut spec = serde_json::Map::new();
    if let Some(ra) = resource_attributes {
        spec.insert(
            "resourceAttributes".into(),
            pb_resource_attributes_to_json(ra),
        );
    }
    if let Some(nra) = non_resource_attributes {
        spec.insert(
            "nonResourceAttributes".into(),
            pb_non_resource_attributes_to_json(nra),
        );
    }
    if let Some(u) = user {
        spec.insert("user".into(), Value::String(u.clone()));
    }
    if !groups.is_empty() {
        spec.insert("groups".into(), serde_json::json!(groups));
    }
    if !extra.is_empty() {
        let mut extra_obj = serde_json::Map::new();
        for (k, v) in extra {
            extra_obj.insert(k.clone(), serde_json::json!(v.items));
        }
        spec.insert("extra".into(), Value::Object(extra_obj));
    }
    if let Some(u) = uid {
        spec.insert("uid".into(), Value::String(u.clone()));
    }
    Value::Object(spec)
}

fn pb_status_to_json(status: &k8s_pb::api::authorization::v1::SubjectAccessReviewStatus) -> Value {
    let mut obj = serde_json::Map::new();
    if let Some(v) = status.allowed {
        obj.insert("allowed".into(), serde_json::json!(v));
    }
    if let Some(v) = status.denied {
        obj.insert("denied".into(), serde_json::json!(v));
    }
    if let Some(v) = &status.reason {
        obj.insert("reason".into(), Value::String(v.clone()));
    }
    if let Some(v) = &status.evaluation_error {
        obj.insert("evaluationError".into(), Value::String(v.clone()));
    }
    Value::Object(obj)
}

fn pb_resource_rule_to_json(rule: &k8s_pb::api::authorization::v1::ResourceRule) -> Value {
    let mut obj = serde_json::Map::new();
    if !rule.verbs.is_empty() {
        obj.insert("verbs".into(), serde_json::json!(rule.verbs));
    }
    if !rule.api_groups.is_empty() {
        obj.insert("apiGroups".into(), serde_json::json!(rule.api_groups));
    }
    if !rule.resources.is_empty() {
        obj.insert("resources".into(), serde_json::json!(rule.resources));
    }
    if !rule.resource_names.is_empty() {
        obj.insert(
            "resourceNames".into(),
            serde_json::json!(rule.resource_names),
        );
    }
    Value::Object(obj)
}

fn pb_non_resource_rule_to_json(rule: &k8s_pb::api::authorization::v1::NonResourceRule) -> Value {
    let mut obj = serde_json::Map::new();
    if !rule.verbs.is_empty() {
        obj.insert("verbs".into(), serde_json::json!(rule.verbs));
    }
    if !rule.non_resource_ur_ls.is_empty() {
        obj.insert(
            "nonResourceURLs".into(),
            serde_json::json!(rule.non_resource_ur_ls),
        );
    }
    Value::Object(obj)
}

fn pb_subject_access_review_to_json(
    pb: &k8s_pb::api::authorization::v1::SubjectAccessReview,
) -> anyhow::Result<Value> {
    let mut obj = serde_json::Map::new();
    obj.insert(
        "apiVersion".into(),
        Value::String("authorization.k8s.io/v1".into()),
    );
    obj.insert("kind".into(), Value::String("SubjectAccessReview".into()));
    if let Some(meta) = &pb.metadata {
        obj.insert("metadata".into(), pb_object_meta_to_json(meta));
    } else {
        obj.insert("metadata".into(), serde_json::json!({}));
    }
    if let Some(spec) = &pb.spec {
        obj.insert(
            "spec".into(),
            pb_spec_to_json(
                &spec.resource_attributes,
                &spec.non_resource_attributes,
                &spec.user,
                &spec.groups,
                &spec.extra,
                &spec.uid,
            ),
        );
    }
    if let Some(status) = &pb.status {
        obj.insert("status".into(), pb_status_to_json(status));
    }
    Ok(Value::Object(obj))
}

fn pb_self_subject_access_review_to_json(
    pb: &k8s_pb::api::authorization::v1::SelfSubjectAccessReview,
) -> anyhow::Result<Value> {
    let mut obj = serde_json::Map::new();
    obj.insert(
        "apiVersion".into(),
        Value::String("authorization.k8s.io/v1".into()),
    );
    obj.insert(
        "kind".into(),
        Value::String("SelfSubjectAccessReview".into()),
    );
    if let Some(meta) = &pb.metadata {
        obj.insert("metadata".into(), pb_object_meta_to_json(meta));
    } else {
        obj.insert("metadata".into(), serde_json::json!({}));
    }
    if let Some(spec) = &pb.spec {
        let mut spec_obj = serde_json::Map::new();
        if let Some(ra) = &spec.resource_attributes {
            spec_obj.insert(
                "resourceAttributes".into(),
                pb_resource_attributes_to_json(ra),
            );
        }
        if let Some(nra) = &spec.non_resource_attributes {
            spec_obj.insert(
                "nonResourceAttributes".into(),
                pb_non_resource_attributes_to_json(nra),
            );
        }
        obj.insert("spec".into(), Value::Object(spec_obj));
    }
    if let Some(status) = &pb.status {
        obj.insert("status".into(), pb_status_to_json(status));
    }
    Ok(Value::Object(obj))
}

fn pb_local_subject_access_review_to_json(
    pb: &k8s_pb::api::authorization::v1::LocalSubjectAccessReview,
) -> anyhow::Result<Value> {
    let mut obj = serde_json::Map::new();
    obj.insert(
        "apiVersion".into(),
        Value::String("authorization.k8s.io/v1".into()),
    );
    obj.insert(
        "kind".into(),
        Value::String("LocalSubjectAccessReview".into()),
    );
    if let Some(meta) = &pb.metadata {
        obj.insert("metadata".into(), pb_object_meta_to_json(meta));
    } else {
        obj.insert("metadata".into(), serde_json::json!({}));
    }
    if let Some(spec) = &pb.spec {
        obj.insert(
            "spec".into(),
            pb_spec_to_json(
                &spec.resource_attributes,
                &spec.non_resource_attributes,
                &spec.user,
                &spec.groups,
                &spec.extra,
                &spec.uid,
            ),
        );
    }
    if let Some(status) = &pb.status {
        obj.insert("status".into(), pb_status_to_json(status));
    }
    Ok(Value::Object(obj))
}

fn pb_self_subject_rules_review_to_json(
    pb: &k8s_pb::api::authorization::v1::SelfSubjectRulesReview,
) -> anyhow::Result<Value> {
    let mut obj = serde_json::Map::new();
    obj.insert(
        "apiVersion".into(),
        Value::String("authorization.k8s.io/v1".into()),
    );
    obj.insert(
        "kind".into(),
        Value::String("SelfSubjectRulesReview".into()),
    );
    if let Some(meta) = &pb.metadata {
        obj.insert("metadata".into(), pb_object_meta_to_json(meta));
    } else {
        obj.insert("metadata".into(), serde_json::json!({}));
    }
    if let Some(spec) = &pb.spec {
        let mut spec_obj = serde_json::Map::new();
        if let Some(ns) = &spec.namespace {
            spec_obj.insert("namespace".into(), Value::String(ns.clone()));
        }
        obj.insert("spec".into(), Value::Object(spec_obj));
    }
    if let Some(status) = &pb.status {
        let mut status_obj = serde_json::Map::new();
        let resource_rules: Vec<Value> = status
            .resource_rules
            .iter()
            .map(pb_resource_rule_to_json)
            .collect();
        let non_resource_rules: Vec<Value> = status
            .non_resource_rules
            .iter()
            .map(pb_non_resource_rule_to_json)
            .collect();
        if !resource_rules.is_empty() {
            status_obj.insert("resourceRules".into(), Value::Array(resource_rules));
        }
        if !non_resource_rules.is_empty() {
            status_obj.insert("nonResourceRules".into(), Value::Array(non_resource_rules));
        }
        if let Some(v) = status.incomplete {
            status_obj.insert("incomplete".into(), serde_json::json!(v));
        }
        if let Some(v) = &status.evaluation_error {
            status_obj.insert("evaluationError".into(), Value::String(v.clone()));
        }
        obj.insert("status".into(), Value::Object(status_obj));
    }
    Ok(Value::Object(obj))
}

/// Minimal ObjectMeta decode for authorization types.
fn pb_object_meta_to_json(meta: &k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta) -> Value {
    let mut obj = serde_json::Map::new();
    if let Some(v) = &meta.name {
        obj.insert("name".into(), Value::String(v.clone()));
    }
    if let Some(v) = &meta.namespace {
        obj.insert("namespace".into(), Value::String(v.clone()));
    }
    if let Some(v) = &meta.uid {
        obj.insert("uid".into(), Value::String(v.clone()));
    }
    if !meta.labels.is_empty() {
        let labels: serde_json::Map<String, Value> = meta
            .labels
            .iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect();
        obj.insert("labels".into(), Value::Object(labels));
    }
    if !meta.annotations.is_empty() {
        let anns: serde_json::Map<String, Value> = meta
            .annotations
            .iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect();
        obj.insert("annotations".into(), Value::Object(anns));
    }
    Value::Object(obj)
}

// ---------------------------------------------------------------------------
// Encode: JSON → protobuf
// ---------------------------------------------------------------------------

fn json_resource_attributes_to_pb(
    attrs: &Value,
) -> k8s_pb::api::authorization::v1::ResourceAttributes {
    k8s_pb::api::authorization::v1::ResourceAttributes {
        namespace: attrs
            .get("namespace")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        verb: attrs
            .get("verb")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        group: attrs
            .get("group")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        version: attrs
            .get("version")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        resource: attrs
            .get("resource")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        subresource: attrs
            .get("subresource")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        name: attrs
            .get("name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        ..Default::default()
    }
}

fn json_non_resource_attributes_to_pb(
    attrs: &Value,
) -> k8s_pb::api::authorization::v1::NonResourceAttributes {
    k8s_pb::api::authorization::v1::NonResourceAttributes {
        path: attrs
            .get("path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        verb: attrs
            .get("verb")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
    }
}

fn json_extra_to_pb(
    extra: &Value,
) -> std::collections::BTreeMap<String, k8s_pb::api::authorization::v1::ExtraValue> {
    extra
        .as_object()
        .map(|obj| {
            obj.iter()
                .map(|(k, v)| {
                    let items = v
                        .as_array()
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|sv| sv.as_str().map(|s| s.to_string()))
                                .collect()
                        })
                        .unwrap_or_default();
                    (
                        k.clone(),
                        k8s_pb::api::authorization::v1::ExtraValue { items },
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

fn json_spec_to_pb(spec: &Value) -> k8s_pb::api::authorization::v1::SubjectAccessReviewSpec {
    let resource_attributes = spec
        .get("resourceAttributes")
        .map(json_resource_attributes_to_pb);
    let non_resource_attributes = spec
        .get("nonResourceAttributes")
        .map(json_non_resource_attributes_to_pb);
    let user = spec
        .get("user")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let groups: Vec<String> = spec
        .get("groups")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let extra = spec.get("extra").map(json_extra_to_pb).unwrap_or_default();
    let uid = spec
        .get("uid")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    k8s_pb::api::authorization::v1::SubjectAccessReviewSpec {
        resource_attributes,
        non_resource_attributes,
        user,
        groups,
        extra,
        uid,
    }
}

fn json_status_to_pb(status: &Value) -> k8s_pb::api::authorization::v1::SubjectAccessReviewStatus {
    k8s_pb::api::authorization::v1::SubjectAccessReviewStatus {
        allowed: status.get("allowed").and_then(|v| v.as_bool()),
        denied: status.get("denied").and_then(|v| v.as_bool()),
        reason: status
            .get("reason")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        evaluation_error: status
            .get("evaluationError")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
    }
}

fn json_meta_to_pb_minimal(
    meta: &Value,
) -> Option<k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta> {
    let openapi_meta =
        k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta::deserialize(meta).ok()?;
    Some(json_meta_to_pb(&openapi_meta))
}

fn json_subject_access_review_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::authorization::v1::SubjectAccessReview> {
    let metadata = value.get("metadata").and_then(json_meta_to_pb_minimal);
    let spec = value.get("spec").map(json_spec_to_pb);
    let status = value.get("status").map(json_status_to_pb);
    Ok(k8s_pb::api::authorization::v1::SubjectAccessReview {
        metadata,
        spec,
        status,
    })
}

fn json_self_subject_access_review_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::authorization::v1::SelfSubjectAccessReview> {
    let metadata = value.get("metadata").and_then(json_meta_to_pb_minimal);
    let spec = value.get("spec").map(|s| {
        let resource_attributes = s
            .get("resourceAttributes")
            .map(json_resource_attributes_to_pb);
        let non_resource_attributes = s
            .get("nonResourceAttributes")
            .map(json_non_resource_attributes_to_pb);
        k8s_pb::api::authorization::v1::SelfSubjectAccessReviewSpec {
            resource_attributes,
            non_resource_attributes,
        }
    });
    let status = value.get("status").map(json_status_to_pb);
    Ok(k8s_pb::api::authorization::v1::SelfSubjectAccessReview {
        metadata,
        spec,
        status,
    })
}

fn json_local_subject_access_review_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::authorization::v1::LocalSubjectAccessReview> {
    let metadata = value.get("metadata").and_then(json_meta_to_pb_minimal);
    let spec = value.get("spec").map(json_spec_to_pb);
    let status = value.get("status").map(json_status_to_pb);
    Ok(k8s_pb::api::authorization::v1::LocalSubjectAccessReview {
        metadata,
        spec,
        status,
    })
}

fn json_self_subject_rules_review_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::authorization::v1::SelfSubjectRulesReview> {
    let metadata = value.get("metadata").and_then(json_meta_to_pb_minimal);
    let spec = value.get("spec").map(|s| {
        let namespace = s
            .get("namespace")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        k8s_pb::api::authorization::v1::SelfSubjectRulesReviewSpec { namespace }
    });
    let status = value.get("status").map(|s| {
        let resource_rules: Vec<k8s_pb::api::authorization::v1::ResourceRule> = s
            .get("resourceRules")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().map(json_resource_rule_to_pb).collect())
            .unwrap_or_default();
        let non_resource_rules: Vec<k8s_pb::api::authorization::v1::NonResourceRule> = s
            .get("nonResourceRules")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().map(json_non_resource_rule_to_pb).collect())
            .unwrap_or_default();
        let incomplete = s.get("incomplete").and_then(|v| v.as_bool());
        let evaluation_error = s
            .get("evaluationError")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        k8s_pb::api::authorization::v1::SubjectRulesReviewStatus {
            resource_rules,
            non_resource_rules,
            incomplete,
            evaluation_error,
        }
    });
    Ok(k8s_pb::api::authorization::v1::SelfSubjectRulesReview {
        metadata,
        spec,
        status,
    })
}

fn json_resource_rule_to_pb(rule: &Value) -> k8s_pb::api::authorization::v1::ResourceRule {
    k8s_pb::api::authorization::v1::ResourceRule {
        verbs: rule
            .get("verbs")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default(),
        api_groups: rule
            .get("apiGroups")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default(),
        resources: rule
            .get("resources")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default(),
        resource_names: rule
            .get("resourceNames")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default(),
    }
}

fn json_non_resource_rule_to_pb(rule: &Value) -> k8s_pb::api::authorization::v1::NonResourceRule {
    k8s_pb::api::authorization::v1::NonResourceRule {
        verbs: rule
            .get("verbs")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default(),
        non_resource_ur_ls: rule
            .get("nonResourceURLs")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protobuf::OoCodecRegistry;
    use serde_json::json;

    fn sar_fixture() -> Value {
        json!({
            "apiVersion": "authorization.k8s.io/v1",
            "kind": "SubjectAccessReview",
            "metadata": {"name": "sar-1"},
            "spec": {
                "resourceAttributes": {
                    "namespace": "default",
                    "verb": "get",
                    "group": "",
                    "resource": "pods",
                    "name": "my-pod"
                },
                "user": "alice",
                "groups": ["developers", "system:authenticated"],
                "uid": "user-uid-1",
                "extra": {"source": ["cli", "web"]}
            },
            "status": {
                "allowed": true,
                "denied": false,
                "reason": "RBAC allowed"
            }
        })
    }

    fn ssar_fixture() -> Value {
        json!({
            "apiVersion": "authorization.k8s.io/v1",
            "kind": "SelfSubjectAccessReview",
            "metadata": {"name": "ssar-1"},
            "spec": {
                "resourceAttributes": {
                    "namespace": "kube-system",
                    "verb": "list",
                    "resource": "secrets"
                }
            },
            "status": {
                "allowed": false,
                "reason": "no RBAC binding"
            }
        })
    }

    fn lsar_fixture() -> Value {
        json!({
            "apiVersion": "authorization.k8s.io/v1",
            "kind": "LocalSubjectAccessReview",
            "metadata": {"name": "lsar-1", "namespace": "production"},
            "spec": {
                "resourceAttributes": {
                    "namespace": "production",
                    "verb": "delete",
                    "resource": "configmaps",
                    "name": "critical-cm"
                },
                "user": "bob",
                "groups": ["admins"]
            },
            "status": {
                "allowed": true,
                "denied": false,
                "reason": "cluster-admin"
            }
        })
    }

    fn ssrr_fixture() -> Value {
        json!({
            "apiVersion": "authorization.k8s.io/v1",
            "kind": "SelfSubjectRulesReview",
            "metadata": {"name": "ssrr-1"},
            "spec": {
                "namespace": "default"
            },
            "status": {
                "resourceRules": [{
                    "verbs": ["get", "list"],
                    "apiGroups": [""],
                    "resources": ["pods", "configmaps"]
                }],
                "nonResourceRules": [{
                    "verbs": ["get"],
                    "nonResourceURLs": ["/healthz", "/livez"]
                }],
                "incomplete": false
            }
        })
    }

    // === Round-trip tests ===

    #[test]
    fn subject_access_review_round_trips() {
        let original = sar_fixture();
        let encoded = AuthorizationV1Codec
            .encode_from_json("SubjectAccessReview", &original)
            .unwrap();
        let decoded = AuthorizationV1Codec
            .decode_to_json("SubjectAccessReview", &encoded)
            .unwrap();

        assert_eq!(decoded["kind"], "SubjectAccessReview");
        assert_eq!(decoded["spec"]["user"], "alice");
        assert_eq!(decoded["spec"]["groups"][0], "developers");
        assert_eq!(decoded["spec"]["uid"], "user-uid-1");
        assert_eq!(decoded["spec"]["resourceAttributes"]["verb"], "get");
        assert_eq!(decoded["spec"]["resourceAttributes"]["resource"], "pods");
        assert_eq!(decoded["status"]["allowed"], true);
        assert_eq!(decoded["status"]["reason"], "RBAC allowed");
    }

    #[test]
    fn self_subject_access_review_round_trips() {
        let original = ssar_fixture();
        let encoded = AuthorizationV1Codec
            .encode_from_json("SelfSubjectAccessReview", &original)
            .unwrap();
        let decoded = AuthorizationV1Codec
            .decode_to_json("SelfSubjectAccessReview", &encoded)
            .unwrap();

        assert_eq!(decoded["kind"], "SelfSubjectAccessReview");
        assert_eq!(
            decoded["spec"]["resourceAttributes"]["namespace"],
            "kube-system"
        );
        assert_eq!(decoded["spec"]["resourceAttributes"]["verb"], "list");
        assert_eq!(decoded["status"]["allowed"], false);
        assert_eq!(decoded["status"]["reason"], "no RBAC binding");
    }

    #[test]
    fn local_subject_access_review_round_trips() {
        let original = lsar_fixture();
        let encoded = AuthorizationV1Codec
            .encode_from_json("LocalSubjectAccessReview", &original)
            .unwrap();
        let decoded = AuthorizationV1Codec
            .decode_to_json("LocalSubjectAccessReview", &encoded)
            .unwrap();

        assert_eq!(decoded["kind"], "LocalSubjectAccessReview");
        assert_eq!(decoded["spec"]["user"], "bob");
        assert_eq!(
            decoded["spec"]["resourceAttributes"]["namespace"],
            "production"
        );
        assert_eq!(decoded["spec"]["resourceAttributes"]["name"], "critical-cm");
        assert_eq!(decoded["status"]["allowed"], true);
    }

    #[test]
    fn self_subject_rules_review_round_trips() {
        let original = ssrr_fixture();
        let encoded = AuthorizationV1Codec
            .encode_from_json("SelfSubjectRulesReview", &original)
            .unwrap();
        let decoded = AuthorizationV1Codec
            .decode_to_json("SelfSubjectRulesReview", &encoded)
            .unwrap();

        assert_eq!(decoded["kind"], "SelfSubjectRulesReview");
        assert_eq!(decoded["spec"]["namespace"], "default");
        let resource_rules = decoded["status"]["resourceRules"]
            .as_array()
            .expect("resourceRules array");
        assert_eq!(resource_rules.len(), 1);
        assert_eq!(resource_rules[0]["verbs"][0], "get");
        assert_eq!(resource_rules[0]["resources"][1], "configmaps");
        let non_resource_rules = decoded["status"]["nonResourceRules"]
            .as_array()
            .expect("nonResourceRules array");
        assert_eq!(non_resource_rules[0]["nonResourceURLs"][0], "/healthz");
        assert_eq!(decoded["status"]["incomplete"], false);
    }

    // === Field preservation tests ===

    #[test]
    fn sar_preserves_extra_fields() {
        let mut original = sar_fixture();
        original["spec"]["extra"] = json!({
            "source": ["cli"],
            "tenant": ["acme"]
        });
        let encoded = AuthorizationV1Codec
            .encode_from_json("SubjectAccessReview", &original)
            .unwrap();
        let decoded = AuthorizationV1Codec
            .decode_to_json("SubjectAccessReview", &encoded)
            .unwrap();
        let extra = decoded["spec"]["extra"]
            .as_object()
            .expect("extra must be object");
        assert_eq!(extra["source"][0], "cli");
        assert_eq!(extra["tenant"][0], "acme");
    }

    #[test]
    fn sar_preserves_non_resource_attributes() {
        let original = json!({
            "apiVersion": "authorization.k8s.io/v1",
            "kind": "SubjectAccessReview",
            "metadata": {},
            "spec": {
                "nonResourceAttributes": {
                    "path": "/api/v1/pods",
                    "verb": "get"
                },
                "user": "test-user"
            }
        });
        let encoded = AuthorizationV1Codec
            .encode_from_json("SubjectAccessReview", &original)
            .unwrap();
        let decoded = AuthorizationV1Codec
            .decode_to_json("SubjectAccessReview", &encoded)
            .unwrap();
        assert_eq!(
            decoded["spec"]["nonResourceAttributes"]["path"],
            "/api/v1/pods"
        );
        assert_eq!(decoded["spec"]["nonResourceAttributes"]["verb"], "get");
    }

    #[test]
    fn sar_status_evaluation_error_preserved() {
        let original = json!({
            "apiVersion": "authorization.k8s.io/v1",
            "kind": "SubjectAccessReview",
            "metadata": {},
            "spec": {"resourceAttributes": {"verb": "get", "resource": "pods"}},
            "status": {
                "allowed": false,
                "evaluationError": "webhook timeout"
            }
        });
        let encoded = AuthorizationV1Codec
            .encode_from_json("SubjectAccessReview", &original)
            .unwrap();
        let decoded = AuthorizationV1Codec
            .decode_to_json("SubjectAccessReview", &encoded)
            .unwrap();
        assert_eq!(decoded["status"]["evaluationError"], "webhook timeout");
    }

    // === Registry tests ===

    #[test]
    fn registry_dispatches_authorization_kinds() {
        let registry = OoCodecRegistry::new(vec![Box::new(AuthorizationV1Codec)]);
        for (_, kind) in AUTHZ_ENTRIES {
            assert!(
                registry.handles("authorization.k8s.io/v1", kind),
                "should handle {kind}"
            );
        }
        assert!(!registry.handles("v1", "Pod"));
        assert!(!registry.handles("rbac.authorization.k8s.io/v1", "ClusterRole"));
    }

    #[test]
    fn registry_round_trip_through_dispatch() {
        let registry = OoCodecRegistry::new(vec![Box::new(AuthorizationV1Codec)]);
        let original = ssar_fixture();
        let encoded = registry
            .encode(
                "authorization.k8s.io/v1",
                "SelfSubjectAccessReview",
                &original,
            )
            .unwrap();
        let decoded = registry
            .decode(
                "authorization.k8s.io/v1",
                "SelfSubjectAccessReview",
                &encoded,
            )
            .unwrap();
        assert_eq!(decoded["kind"], "SelfSubjectAccessReview");
        assert_eq!(decoded["status"]["allowed"], false);
    }
}
