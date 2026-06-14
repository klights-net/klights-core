//! CertificatesV1Codec: OO protobuf codec for certificates.k8s.io/v1 resources.
//!
//! Handles round-trip encode/decode for CertificateSigningRequest and
//! CertificateSigningRequestList. Uses strict base64 validation — malformed
//! request/certificate base64 returns an error instead of silently defaulting.
//!
//! Dispatch is owned by the global OO protobuf registry.

use crate::protobuf::ResourceProtoCodec;
use crate::protobuf::*;
use anyhow::Context;
use base64::Engine;
use serde_json::Value;

/// (api_version_prefix, kind) entries for certificates.k8s.io resources.
const CSR_ENTRIES: &[(&str, &str)] = &[
    ("certificates.k8s.io", "CertificateSigningRequest"),
    ("certificates.k8s.io", "CertificateSigningRequestList"),
];

/// Codec for certificates.k8s.io/v1 resources.
pub struct CertificatesV1Codec;

impl ResourceProtoCodec for CertificatesV1Codec {
    fn entry_keys(&self) -> &'static [(&'static str, &'static str)] {
        CSR_ENTRIES
    }

    fn decode_to_json(&self, _api_version: &str, kind: &str, data: &[u8]) -> anyhow::Result<Value> {
        use prost::Message;
        match kind {
            "CertificateSigningRequest" => {
                let pb = k8s_pb::api::certificates::v1::CertificateSigningRequest::decode(data)
                    .context("failed to decode CertificateSigningRequest protobuf")?;
                pb_csr_to_json(&pb)
            }
            "CertificateSigningRequestList" => {
                let pb = k8s_pb::api::certificates::v1::CertificateSigningRequestList::decode(data)
                    .context("failed to decode CertificateSigningRequestList protobuf")?;
                pb_csrlist_to_json(&pb)
            }
            _ => anyhow::bail!("CertificatesV1Codec: unknown kind {kind}"),
        }
    }

    fn encode_from_json(
        &self,
        _api_version: &str,
        kind: &str,
        value: &Value,
    ) -> anyhow::Result<Vec<u8>> {
        match kind {
            "CertificateSigningRequest" => {
                let pb = json_csr_to_pb_strict(value)
                    .context("failed to encode CertificateSigningRequest")?;
                encode_message_to_vec(&pb)
            }
            "CertificateSigningRequestList" => {
                let pb = json_csrlist_to_pb(value)
                    .context("failed to encode CertificateSigningRequestList")?;
                encode_message_to_vec(&pb)
            }
            _ => anyhow::bail!("CertificatesV1Codec: unknown kind {kind}"),
        }
    }
}

#[cfg(test)]
impl CertificatesV1Codec {
    fn decode_to_json(&self, kind: &str, data: &[u8]) -> anyhow::Result<Value> {
        <Self as ResourceProtoCodec>::decode_to_json(self, "certificates.k8s.io/v1", kind, data)
    }

    fn encode_from_json(&self, kind: &str, value: &Value) -> anyhow::Result<Vec<u8>> {
        <Self as ResourceProtoCodec>::encode_from_json(self, "certificates.k8s.io/v1", kind, value)
    }
}

/// Strict base64 decode helper — returns an error on malformed input instead
/// of silently producing empty bytes.
fn decode_base64_strict(input: &str, field_name: &'static str) -> anyhow::Result<Vec<u8>> {
    use base64::engine::general_purpose::STANDARD;
    if input.is_empty() {
        return Ok(Vec::new());
    }
    STANDARD
        .decode(input)
        .with_context(|| format!("invalid base64 in {field_name}"))
}

/// Encode a CertificateSigningRequest from JSON with strict base64 validation.
///
/// Unlike `json_csr_to_pb` which uses `unwrap_or_default()` for base64 decode,
/// this rejects malformed `spec.request` and `status.certificate` fields.
fn json_csr_to_pb_strict(
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
            let request = spec_obj
                .get("request")
                .and_then(|r| r.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| decode_base64_strict(s, "spec.request"))
                .transpose()?;

            let signer_name = spec_obj
                .get("signerName")
                .and_then(|s| s.as_str())
                .map(|s| s.to_string());

            let usages: Vec<String> = spec_obj
                .get("usages")
                .and_then(|u| u.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();

            // Preserve CSR identity fields
            let username = spec_obj
                .get("username")
                .and_then(|u| u.as_str())
                .map(|s| s.to_string());
            let groups: Vec<String> = spec_obj
                .get("groups")
                .and_then(|g| g.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();
            let uid = spec_obj
                .get("uid")
                .and_then(|u| u.as_str())
                .map(|s| s.to_string());
            let extra: std::collections::BTreeMap<String, certsv1::ExtraValue> = spec_obj
                .get("extra")
                .and_then(|e| e.as_object())
                .map(|obj| {
                    obj.iter()
                        .filter_map(|(k, v)| {
                            let values = v
                                .as_array()?
                                .iter()
                                .filter_map(|sv| sv.as_str().map(|s| s.to_string()))
                                .collect();
                            Some((k.clone(), certsv1::ExtraValue { items: values }))
                        })
                        .collect()
                })
                .unwrap_or_default();

            let expiration_seconds = spec_obj
                .get("expirationSeconds")
                .and_then(|v| v.as_i64())
                .map(|v| v as i32);

            Ok::<_, anyhow::Error>(certsv1::CertificateSigningRequestSpec {
                request,
                signer_name,
                usages,
                username,
                groups,
                uid,
                extra,
                expiration_seconds,
            })
        })
        .transpose()?;

    let status = value
        .get("status")
        .and_then(|s| s.as_object())
        .map(|status_obj| {
            let certificate = status_obj
                .get("certificate")
                .and_then(|c| c.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| decode_base64_strict(s, "status.certificate"))
                .transpose()?;

            let conditions: Vec<certsv1::CertificateSigningRequestCondition> = status_obj
                .get("conditions")
                .and_then(|c| c.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|cond| {
                            let condition_type = cond.get("type")?.as_str().map(|s| s.to_string());
                            let status_str = cond.get("status")?.as_str().map(|s| s.to_string());
                            let reason = cond
                                .get("reason")
                                .and_then(|r| r.as_str())
                                .map(|s| s.to_string());
                            let message = cond
                                .get("message")
                                .and_then(|m| m.as_str())
                                .map(|s| s.to_string());
                            Some(certsv1::CertificateSigningRequestCondition {
                                r#type: condition_type,
                                status: status_str,
                                reason,
                                message,
                                ..Default::default()
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();

            Ok::<_, anyhow::Error>(certsv1::CertificateSigningRequestStatus {
                certificate,
                conditions,
            })
        })
        .transpose()?;

    Ok(certsv1::CertificateSigningRequest {
        metadata,
        spec,
        status,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protobuf::OoCodecRegistry;
    use serde_json::json;

    /// Build a minimal CSR JSON fixture with valid base64-encoded request.
    fn csr_fixture(name: &str) -> Value {
        let request_pem =
            "-----BEGIN CERTIFICATE REQUEST-----\nMIH...\n-----END CERTIFICATE REQUEST-----";
        let request_b64 = base64::engine::general_purpose::STANDARD.encode(request_pem);
        json!({
            "apiVersion": "certificates.k8s.io/v1",
            "kind": "CertificateSigningRequest",
            "metadata": {
                "name": name,
                "uid": "csr-uid-1"
            },
            "spec": {
                "request": request_b64,
                "signerName": "kubernetes.io/kube-apiserver-client-kubelet",
                "usages": ["client auth", "digital signature"],
                "username": "system:node:tokyo",
                "groups": ["system:nodes", "system:authenticated"],
                "uid": "node-uid-tokyo"
            }
        })
    }

    /// Build a CSR with status (approved + certificate).
    fn csr_fixture_approved(name: &str) -> Value {
        let mut csr = csr_fixture(name);
        let cert_pem = "-----BEGIN CERTIFICATE-----\nMIID...\n-----END CERTIFICATE-----";
        let cert_b64 = base64::engine::general_purpose::STANDARD.encode(cert_pem);
        csr["status"] = json!({
            "certificate": cert_b64,
            "conditions": [{
                "type": "Approved",
                "status": "True",
                "reason": "AutoApproved",
                "message": "Auto-approved by klights CSR controller",
                "lastUpdateTime": "2025-01-01T00:00:00Z"
            }]
        });
        csr
    }

    // === Round-trip tests ===

    #[test]
    fn csr_round_trips() {
        let original = csr_fixture("test-csr");
        let encoded = CertificatesV1Codec
            .encode_from_json("CertificateSigningRequest", &original)
            .unwrap();
        let decoded = CertificatesV1Codec
            .decode_to_json("CertificateSigningRequest", &encoded)
            .unwrap();

        assert_eq!(decoded["kind"], "CertificateSigningRequest");
        assert_eq!(decoded["metadata"]["name"], "test-csr");
        assert_eq!(
            decoded["spec"]["signerName"],
            "kubernetes.io/kube-apiserver-client-kubelet"
        );
        // Request bytes should round-trip (decoded → encoded → decoded)
        assert!(decoded["spec"]["request"].as_str().is_some());
        let usages = decoded["spec"]["usages"].as_array().expect("usages array");
        assert!(usages.iter().any(|u| u == "client auth"));
    }

    #[test]
    fn csr_with_status_round_trips() {
        let original = csr_fixture_approved("approved-csr");
        let encoded = CertificatesV1Codec
            .encode_from_json("CertificateSigningRequest", &original)
            .unwrap();
        let decoded = CertificatesV1Codec
            .decode_to_json("CertificateSigningRequest", &encoded)
            .unwrap();

        assert_eq!(decoded["kind"], "CertificateSigningRequest");
        // Certificate should survive round-trip
        assert!(
            decoded["status"]["certificate"].as_str().is_some(),
            "status.certificate should be present"
        );
        // Conditions should survive
        let conditions = decoded["status"]["conditions"]
            .as_array()
            .expect("conditions array");
        assert_eq!(conditions[0]["type"], "Approved");
        assert_eq!(conditions[0]["status"], "True");
    }

    #[test]
    fn csrlist_round_trips() {
        let original = json!({
            "apiVersion": "certificates.k8s.io/v1",
            "kind": "CertificateSigningRequestList",
            "metadata": {},
            "items": [
                csr_fixture("csr1"),
                csr_fixture("csr2")
            ]
        });
        let encoded = CertificatesV1Codec
            .encode_from_json("CertificateSigningRequestList", &original)
            .unwrap();
        let decoded = CertificatesV1Codec
            .decode_to_json("CertificateSigningRequestList", &encoded)
            .unwrap();

        assert_eq!(decoded["kind"], "CertificateSigningRequestList");
        let items = decoded["items"].as_array().expect("items must be array");
        assert_eq!(items.len(), 2);
    }

    // === Strict base64 tests ===

    #[test]
    fn malformed_request_base64_rejected() {
        let bad_csr = json!({
            "apiVersion": "certificates.k8s.io/v1",
            "kind": "CertificateSigningRequest",
            "metadata": {"name": "bad-csr", "uid": "bad-uid"},
            "spec": {
                "request": "!!!NOT_BASE64!!!",
                "signerName": "kubernetes.io/kube-apiserver-client-kubelet",
                "usages": ["client auth"]
            }
        });
        let result = CertificatesV1Codec.encode_from_json("CertificateSigningRequest", &bad_csr);
        assert!(
            result.is_err(),
            "malformed base64 in spec.request should be rejected, got: {result:?}"
        );
        let err_msg = format!("{:#}", result.unwrap_err());
        assert!(
            err_msg.contains("request")
                || err_msg.contains("base64")
                || err_msg.contains("invalid"),
            "error should mention base64/spec.request, got: {err_msg}"
        );
    }

    #[test]
    fn malformed_certificate_base64_rejected() {
        let bad_csr = json!({
            "apiVersion": "certificates.k8s.io/v1",
            "kind": "CertificateSigningRequest",
            "metadata": {"name": "bad-cert", "uid": "bad-cert-uid"},
            "spec": {
                "request": "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURSBSRVFVRVNULS0tLS0=",
                "signerName": "kubernetes.io/kube-apiserver-client-kubelet",
                "usages": ["client auth"]
            },
            "status": {
                "certificate": "!!!NOT_BASE64!!!"
            }
        });
        let result = CertificatesV1Codec.encode_from_json("CertificateSigningRequest", &bad_csr);
        assert!(
            result.is_err(),
            "malformed base64 in status.certificate should be rejected, got: {result:?}"
        );
        let err_msg = format!("{:#}", result.unwrap_err());
        assert!(
            err_msg.contains("certificate")
                || err_msg.contains("base64")
                || err_msg.contains("invalid"),
            "error should mention base64/status.certificate, got: {err_msg}"
        );
    }

    #[test]
    fn empty_request_base64_is_accepted() {
        let csr = json!({
            "apiVersion": "certificates.k8s.io/v1",
            "kind": "CertificateSigningRequest",
            "metadata": {"name": "empty-req", "uid": "e-uid"},
            "spec": {
                "request": "",
                "signerName": "kubernetes.io/kube-apiserver-client-kubelet",
                "usages": []
            }
        });
        // Empty string should not trigger base64 decode error
        let result = CertificatesV1Codec.encode_from_json("CertificateSigningRequest", &csr);
        assert!(result.is_ok(), "empty request should be accepted");
    }

    // === Registry tests ===

    #[test]
    fn registry_dispatches_csr_kinds() {
        let registry = OoCodecRegistry::new(vec![Box::new(CertificatesV1Codec)]);

        assert!(registry.handles("certificates.k8s.io/v1", "CertificateSigningRequest"));
        assert!(registry.handles("certificates.k8s.io/v1", "CertificateSigningRequestList"));
        assert!(!registry.handles("v1", "Pod"));
    }

    #[test]
    fn registry_csr_round_trip_through_dispatch() {
        let registry = OoCodecRegistry::new(vec![Box::new(CertificatesV1Codec)]);
        let original = csr_fixture("dispatched-csr");

        let encoded = registry
            .encode(
                "certificates.k8s.io/v1",
                "CertificateSigningRequest",
                &original,
            )
            .unwrap();
        let decoded = registry
            .decode(
                "certificates.k8s.io/v1",
                "CertificateSigningRequest",
                &encoded,
            )
            .unwrap();

        assert_eq!(decoded["kind"], "CertificateSigningRequest");
        assert_eq!(decoded["metadata"]["name"], "dispatched-csr");
    }
}
