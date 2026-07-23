/// Encode CertificateSigningRequest to protobuf (minimal implementation)
use crate::protobuf::*;
pub fn json_csr_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::certificates::v1::CertificateSigningRequest> {
    use k8s_pb::api::certificates::v1 as certsv1;

    let metadata = if let Some(m) = value.get("metadata") {
        let openapi_meta =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta::deserialize(m)?;
        Some(json_meta_to_pb(&openapi_meta))
    } else {
        None
    };

    let spec = value
        .get("spec")
        .and_then(|s| s.as_object())
        .map(|spec_obj| {
            let request = spec_obj.get("request").and_then(|r| r.as_str()).map(|s| {
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s)
                    .unwrap_or_default()
            });
            let signer_name = spec_obj
                .get("signerName")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let expiration_seconds = spec_obj
                .get("expirationSeconds")
                .and_then(|v| v.as_i64())
                .map(|v| v as i32);
            let usages = spec_obj
                .get("usages")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|u| u.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            let username = spec_obj
                .get("username")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let uid = spec_obj
                .get("uid")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let groups = spec_obj
                .get("groups")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|g| g.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();

            certsv1::CertificateSigningRequestSpec {
                request,
                signer_name,
                expiration_seconds,
                usages,
                username,
                uid,
                groups,
                extra: Default::default(),
            }
        });

    let status = value
        .get("status")
        .and_then(|s| s.as_object())
        .map(|status_obj| {
            let certificate = status_obj
                .get("certificate")
                .and_then(|v| v.as_str())
                .map(|s| {
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s)
                        .unwrap_or_default()
                });

            let conditions = status_obj
                .get("conditions")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|c| c.as_object())
                        .map(|c| certsv1::CertificateSigningRequestCondition {
                            r#type: c.get("type").and_then(|v| v.as_str()).map(str::to_string),
                            status: c.get("status").and_then(|v| v.as_str()).map(str::to_string),
                            reason: c.get("reason").and_then(|v| v.as_str()).map(str::to_string),
                            message: c
                                .get("message")
                                .and_then(|v| v.as_str())
                                .map(str::to_string),
                            ..Default::default()
                        })
                        .collect()
                })
                .unwrap_or_default();

            certsv1::CertificateSigningRequestStatus {
                conditions,
                certificate,
            }
        });

    Ok(certsv1::CertificateSigningRequest {
        metadata,
        spec,
        status,
    })
}

/// Encode CertificateSigningRequestList to protobuf (minimal implementation)
pub fn json_csrlist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::certificates::v1::CertificateSigningRequestList> {
    use k8s_pb::api::certificates::v1 as certsv1;

    let metadata = if let Some(m) = value.get("metadata") {
        let openapi_meta =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m)?;
        Some(json_listmeta_to_pb(&openapi_meta))
    } else {
        None
    };

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("CertificateSigningRequestList missing items array"))?;

    let pb_items = items
        .iter()
        .map(json_csr_to_pb)
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(certsv1::CertificateSigningRequestList {
        metadata,
        items: pb_items,
    })
}
