use crate::protobuf::*;
use serde_json::Value;

/// K8s protobuf Unknown envelope message
#[derive(Clone, PartialEq, prost::Message)]
pub struct Unknown {
    #[prost(message, tag = "1")]
    pub type_meta: Option<TypeMeta>,

    #[prost(bytes, tag = "2")]
    pub raw: Vec<u8>,

    #[prost(string, tag = "3")]
    pub content_encoding: String,

    #[prost(string, tag = "4")]
    pub content_type: String,
}

#[cfg(test)]
mod apiservice_protobuf_tests {
    use crate::protobuf::*;
    use prost::Message;
    use serde_json::json;

    #[test]
    pub fn test_decode_apiservice_protobuf_preserves_spec_and_status_fields() {
        let pb = k8s_pb::kube_aggregator::pkg::apis::apiregistration::v1::APIService {
            metadata: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta {
                name: Some("v1alpha1.wardle.example.com".to_string()),
                ..Default::default()
            }),
            spec: Some(
                k8s_pb::kube_aggregator::pkg::apis::apiregistration::v1::APIServiceSpec {
                    service: Some(
                        k8s_pb::kube_aggregator::pkg::apis::apiregistration::v1::ServiceReference {
                            namespace: Some("aggregator-1781".to_string()),
                            name: Some("sample-api".to_string()),
                            port: Some(443),
                        },
                    ),
                    group: Some("wardle.example.com".to_string()),
                    version: Some("v1alpha1".to_string()),
                    insecure_skip_tls_verify: Some(false),
                    ca_bundle: Some(b"ca".to_vec()),
                    group_priority_minimum: Some(2000),
                    version_priority: Some(15),
                },
            ),
            status: Some(
                k8s_pb::kube_aggregator::pkg::apis::apiregistration::v1::APIServiceStatus {
                    conditions: vec![
                        k8s_pb::kube_aggregator::pkg::apis::apiregistration::v1::APIServiceCondition {
                            r#type: Some("Available".to_string()),
                            status: Some("True".to_string()),
                            last_transition_time: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::Time {
                                seconds: Some(1713705600),
                                nanos: Some(0),
                            }),
                            reason: Some("Passed".to_string()),
                            message: Some("passed checks".to_string()),
                        },
                    ],
                },
            ),
        };

        let mut raw = Vec::new();
        pb.encode(&mut raw).expect("encode APIService protobuf");

        let unknown = Unknown {
            type_meta: Some(TypeMeta {
                api_version: "apiregistration.k8s.io/v1".to_string(),
                kind: "APIService".to_string(),
            }),
            raw,
            content_encoding: String::new(),
            content_type: String::new(),
        };

        let mut wire = vec![0x6b, 0x38, 0x73, 0x00];
        unknown.encode(&mut wire).expect("encode Unknown envelope");

        let decoded = decode_protobuf(&wire).expect("decode APIService envelope");

        assert_eq!(decoded["apiVersion"], "apiregistration.k8s.io/v1");
        assert_eq!(decoded["kind"], "APIService");
        assert_eq!(decoded["metadata"]["name"], "v1alpha1.wardle.example.com");
        assert_eq!(
            decoded["spec"]["service"]["namespace"], "aggregator-1781",
            "protobuf decode must preserve APIService.spec.service.namespace"
        );
        assert_eq!(decoded["spec"]["service"]["name"], "sample-api");
        assert_eq!(decoded["spec"]["group"], "wardle.example.com");
        assert_eq!(decoded["spec"]["version"], "v1alpha1");
        assert_eq!(decoded["spec"]["groupPriorityMinimum"], 2000);
        assert_eq!(decoded["spec"]["versionPriority"], 15);
        assert_eq!(
            decoded["spec"]["caBundle"], "Y2E=",
            "caBundle must remain base64 string in JSON form"
        );
        assert_eq!(decoded["status"]["conditions"][0]["type"], "Available");
        assert_eq!(decoded["status"]["conditions"][0]["status"], "True");
        assert_eq!(decoded["status"]["conditions"][0]["reason"], "Passed");
    }

    #[test]
    pub fn test_encode_apiservice_protobuf_resource_produces_typed_bytes() {
        let apiservice_json = json!({
            "apiVersion": "apiregistration.k8s.io/v1",
            "kind": "APIService",
            "metadata": {"name": "v1alpha1.wardle.example.com"},
            "spec": {
                "service": {
                    "namespace": "aggregator-1781",
                    "name": "sample-api",
                    "port": 443
                },
                "group": "wardle.example.com",
                "version": "v1alpha1",
                "groupPriorityMinimum": 2000,
                "versionPriority": 15,
                "caBundle": "Y2E="
            }
        });

        let bytes = encode_protobuf_resource("APIService", &apiservice_json)
            .expect("APIService must have typed protobuf encoder");

        let decoded_pb =
            k8s_pb::kube_aggregator::pkg::apis::apiregistration::v1::APIService::decode(&bytes[..])
                .expect("typed APIService protobuf bytes must decode");

        let spec = decoded_pb.spec.expect("spec must be present");
        let service = spec.service.expect("spec.service must be present");
        assert_eq!(service.namespace.as_deref(), Some("aggregator-1781"));
        assert_eq!(service.name.as_deref(), Some("sample-api"));
        assert_eq!(spec.group.as_deref(), Some("wardle.example.com"));
        assert_eq!(spec.version.as_deref(), Some("v1alpha1"));
        assert_eq!(spec.group_priority_minimum, Some(2000));
        assert_eq!(spec.version_priority, Some(15));
        assert_eq!(spec.ca_bundle, Some(b"ca".to_vec()));
    }
}

#[cfg(test)]
mod csr_protobuf_tests {
    use crate::protobuf::*;
    use prost::Message;
    use serde_json::json;

    #[test]
    pub fn test_certificate_signing_request_protobuf_roundtrip_preserves_spec_and_status() {
        let csr_json = json!({
            "apiVersion": "certificates.k8s.io/v1",
            "kind": "CertificateSigningRequest",
            "metadata": {"name": "csr-test"},
            "spec": {
                "request": "Y3NyLWJ5dGVz",
                "signerName": "kubernetes.io/kube-apiserver-client",
                "expirationSeconds": 3600,
                "usages": ["client auth"],
                "username": "tester",
                "uid": "uid-1",
                "groups": ["system:authenticated"],
                "extra": {"example.com/key": ["value"]}
            },
            "status": {
                "conditions": [{
                    "type": "Approved",
                    "status": "True",
                    "reason": "UnitTest",
                    "message": "approved"
                }],
                "certificate": "Y2VydC1ieXRlcw=="
            }
        });

        let raw = encode_protobuf_resource("CertificateSigningRequest", &csr_json)
            .expect("CSR must have typed protobuf encoder");
        let decoded_pb = k8s_pb::api::certificates::v1::CertificateSigningRequest::decode(&raw[..])
            .expect("typed CSR bytes must decode");
        let spec = decoded_pb.spec.expect("spec must be encoded");
        assert_eq!(
            spec.signer_name.as_deref(),
            Some("kubernetes.io/kube-apiserver-client")
        );
        assert_eq!(spec.request.as_deref(), Some(&b"csr-bytes"[..]));
        assert_eq!(spec.usages, vec!["client auth"]);
        assert_eq!(spec.username.as_deref(), Some("tester"));
        assert_eq!(spec.groups, vec!["system:authenticated"]);

        let decoded_json =
            decode_protobuf_resource("certificates.k8s.io/v1", "CertificateSigningRequest", &raw)
                .expect("CSR protobuf decode must produce JSON");
        assert_eq!(decoded_json["metadata"]["name"], "csr-test");
        assert_eq!(
            decoded_json["spec"]["signerName"],
            "kubernetes.io/kube-apiserver-client"
        );
        assert_eq!(decoded_json["spec"]["request"], "Y3NyLWJ5dGVz");
        assert_eq!(decoded_json["status"]["conditions"][0]["type"], "Approved");
        assert_eq!(decoded_json["status"]["certificate"], "Y2VydC1ieXRlcw==");
    }

    #[test]
    pub fn test_certificate_signing_request_list_protobuf_encodes_typed_items() {
        let list_json = json!({
            "apiVersion": "certificates.k8s.io/v1",
            "kind": "CertificateSigningRequestList",
            "metadata": {"resourceVersion": "10"},
            "items": [{
                "apiVersion": "certificates.k8s.io/v1",
                "kind": "CertificateSigningRequest",
                "metadata": {"name": "csr-test"},
                "spec": {
                    "request": "Y3NyLWJ5dGVz",
                    "signerName": "kubernetes.io/kube-apiserver-client",
                    "usages": ["client auth"]
                }
            }]
        });

        let raw = encode_protobuf_resource("CertificateSigningRequestList", &list_json)
            .expect("CSR list must have typed protobuf encoder");
        let decoded_pb =
            k8s_pb::api::certificates::v1::CertificateSigningRequestList::decode(&raw[..])
                .expect("typed CSR list bytes must decode");
        assert_eq!(decoded_pb.items.len(), 1);
        assert_eq!(
            decoded_pb.items[0]
                .spec
                .as_ref()
                .and_then(|spec| spec.signer_name.as_deref()),
            Some("kubernetes.io/kube-apiserver-client")
        );
    }
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct TypeMeta {
    #[prost(string, tag = "1")]
    pub api_version: String,

    #[prost(string, tag = "2")]
    pub kind: String,
}

pub fn decode_protobuf(data: &[u8]) -> anyhow::Result<Value> {
    use prost::Message;

    // Check if data starts with "k8s\0" magic prefix (Unknown envelope)
    let has_magic_prefix = data.len() >= 4 && data[0..4] == [0x6b, 0x38, 0x73, 0x00];

    if has_magic_prefix {
        // Decode the Unknown envelope (for single resources)
        let unknown = Unknown::decode(&data[4..])
            .map_err(|e| anyhow::anyhow!("Failed to decode Unknown envelope: {}", e))?;

        // The raw field contains the actual resource data
        // Try JSON first (common case for kubectl with --v=8)
        if let Ok(json_value) = serde_json::from_slice::<Value>(&unknown.raw) {
            return Ok(json_value);
        }

        // If JSON parsing failed, decode as protobuf based on apiVersion + kind
        let (api_version, kind) = unknown
            .type_meta
            .as_ref()
            .map(|tm| (tm.api_version.as_str(), tm.kind.as_str()))
            .unwrap_or(("", ""));

        decode_protobuf_resource(api_version, kind, &unknown.raw)
    } else {
        // No magic prefix — this data does NOT come from klights (which always wraps in k8s\0).
        // This path handles incoming PUT/PATCH bodies or test data that uses bare/legacy format.
        // Try Unknown envelope first (legacy format without magic prefix)
        match Unknown::decode(data) {
            Ok(unknown) => {
                // Check if type_meta is present
                if let Some(type_meta) = &unknown.type_meta {
                    let kind = &type_meta.kind;

                    // Check if kind looks valid (starts with uppercase letter, contains alphanumeric)
                    // If kind is just a number or doesn't look like a K8s kind, it's likely bare list being mis-decoded
                    let looks_valid_kind = kind
                        .chars()
                        .next()
                        .map(|c| c.is_ascii_uppercase())
                        .unwrap_or(false)
                        && kind.chars().all(|c| c.is_alphanumeric() || c == '_');

                    if !looks_valid_kind {
                        // kind doesn't look valid - try bare list decode
                        return bare_list_protobuf_to_json(data);
                    }

                    // kind looks valid - use Unknown envelope
                    // Try JSON first
                    if let Ok(json_value) = serde_json::from_slice::<Value>(&unknown.raw) {
                        return Ok(json_value);
                    }

                    // Decode as protobuf based on apiVersion + kind
                    decode_protobuf_resource(&type_meta.api_version, kind, &unknown.raw)
                } else {
                    // Unknown had no type_meta - try bare list protobuf
                    bare_list_protobuf_to_json(data)
                }
            }
            Err(e) => {
                // Unknown decode failed - preserve error message for malformed data test
                // But first try bare list decode for actual list types
                match bare_list_protobuf_to_json(data) {
                    Ok(list_json) => Ok(list_json),
                    Err(_) => Err(anyhow::anyhow!("Failed to decode Unknown envelope: {}", e)),
                }
            }
        }
    }
}

/// Fallback decoder for bare protobuf bytes (no k8s\0 magic prefix, no Unknown wrapper).
///
/// NOTE: klights always *encodes* responses using k8s\0 + Unknown envelope — the K8s Go client
/// requires this format for ALL types and rejects bare bytes with "expected prefix [107 56 115 0]".
/// This function handles the *decode* direction only: incoming PUT/PATCH requests or legacy test
/// data that arrives without the magic prefix. The S8-API-1 "unexpected EOF" was caused by
/// broken list encoding bytes (fixed in commit d501560), not by the wrapper format.
pub fn bare_list_protobuf_to_json(data: &[u8]) -> anyhow::Result<Value> {
    for (api_version, kind) in bare_list_decode_candidates() {
        if let Ok(decoded) = global_oo_registry().decode(api_version, kind, data) {
            return Ok(decoded);
        }
    }

    Err(anyhow::anyhow!(
        "Failed to decode as bare protobuf list type - unknown format"
    ))
}
