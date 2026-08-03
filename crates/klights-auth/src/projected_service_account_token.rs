//! Authorization and signing policy for bound projected ServiceAccount tokens.
//!
//! Concrete resource reads remain adapter-owned. This module consumes only a
//! focused reader and neutral stored-resource snapshots.

use std::sync::Arc;

use async_trait::async_trait;
use klights_leader_api::{
    ProjectedServiceAccountToken, ProjectedServiceAccountTokenError,
    ProjectedServiceAccountTokenRequest,
};
use serde_json::Value;

/// Neutral stored-resource snapshot needed by projected-token authorization.
#[derive(Clone, Debug, PartialEq)]
pub struct ProjectedTokenStoredResource {
    api_version: String,
    kind: String,
    namespace: Option<String>,
    name: String,
    uid: String,
    resource_version: i64,
    data: Arc<Value>,
}

impl ProjectedTokenStoredResource {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        api_version: String,
        kind: String,
        namespace: Option<String>,
        name: String,
        uid: String,
        resource_version: i64,
        data: Arc<Value>,
    ) -> Self {
        Self {
            api_version,
            kind,
            namespace,
            name,
            uid,
            resource_version,
            data,
        }
    }
}

/// Focused resource capability consumed by projected-token authorization.
///
/// Production adapters keep Pod reads behind their Pod repository boundary;
/// auth never receives a concrete datastore or repository.
#[async_trait]
pub trait ProjectedTokenResourceReader: Send + Sync {
    async fn get_service_account(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<ProjectedTokenStoredResource>, String>;

    async fn get_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<ProjectedTokenStoredResource>, String>;

    async fn get_node(&self, name: &str) -> Result<Option<ProjectedTokenStoredResource>, String>;
}

/// Claims approved by resource-identity and Pod/Node binding policy.
#[derive(Debug, Eq, PartialEq)]
pub struct AuthorizedProjectedServiceAccountTokenClaims {
    service_account_name: String,
    namespace: String,
    audiences: Vec<String>,
    expiration_seconds: i64,
    service_account_uid: String,
    bound_pod_name: String,
    bound_pod_uid: String,
    bound_node_name: String,
    bound_node_uid: String,
}

/// Resolve and authorize the resource identities bound into a projected token.
pub async fn authorize_projected_service_account_token(
    resources: &dyn ProjectedTokenResourceReader,
    request: &ProjectedServiceAccountTokenRequest,
) -> Result<AuthorizedProjectedServiceAccountTokenClaims, ProjectedServiceAccountTokenError> {
    let service_account = resources
        .get_service_account(request.namespace(), request.service_account_name())
        .await
        .map_err(ProjectedServiceAccountTokenError::unavailable)?
        .ok_or(ProjectedServiceAccountTokenError::ServiceAccountNotFound)?;
    validate_resource_identity(
        &service_account,
        "v1",
        "ServiceAccount",
        Some(request.namespace()),
        request.service_account_name(),
        None,
    )?;
    let (bound_pod_name, bound_pod_uid, bound_node_name, bound_node_uid) =
        resolve_bound_pod_and_node(resources, request).await?;

    Ok(AuthorizedProjectedServiceAccountTokenClaims {
        service_account_name: request.service_account_name().to_string(),
        namespace: request.namespace().to_string(),
        audiences: request.audiences().to_vec(),
        expiration_seconds: klights_types::normalize_service_account_token_expiration_seconds(
            Some(request.expiration_seconds()),
        ),
        service_account_uid: service_account.uid,
        bound_pod_name,
        bound_pod_uid,
        bound_node_name,
        bound_node_uid,
    })
}

/// Sign claims that have already passed projected-token authorization policy.
pub fn sign_authorized_projected_service_account_token(
    signing_key_pem: &str,
    claims: AuthorizedProjectedServiceAccountTokenClaims,
    clock: &dyn crate::clock::Clock,
) -> Result<ProjectedServiceAccountToken, ProjectedServiceAccountTokenError> {
    let audience_refs: Vec<&str> = claims.audiences.iter().map(String::as_str).collect();
    let token = crate::generate_sa_token_with_bound_pod_and_clock(
        crate::ServiceAccountTokenRequest {
            ca_key_pem: signing_key_pem,
            service_account: &claims.service_account_name,
            namespace: &claims.namespace,
            audiences: &audience_refs,
            expiration_seconds: Some(claims.expiration_seconds),
            bound: crate::BoundServiceAccountToken {
                pod_name: Some(&claims.bound_pod_name),
                pod_uid: Some(&claims.bound_pod_uid),
                node_name: Some(&claims.bound_node_name),
                node_uid: Some(&claims.bound_node_uid),
                secret_name: None,
                secret_uid: None,
                sa_uid: Some(&claims.service_account_uid),
            },
        },
        clock,
    )
    .map_err(|error| ProjectedServiceAccountTokenError::signing_failed(error.to_string()))?;

    ProjectedServiceAccountToken::try_new(token)
}

async fn resolve_bound_pod_and_node(
    resources: &dyn ProjectedTokenResourceReader,
    request: &ProjectedServiceAccountTokenRequest,
) -> Result<(String, String, String, String), ProjectedServiceAccountTokenError> {
    let pod = resources
        .get_pod(request.namespace(), request.bound_pod_name())
        .await
        .map_err(ProjectedServiceAccountTokenError::unavailable)?
        .ok_or(ProjectedServiceAccountTokenError::BoundPodNotFound)?;
    validate_resource_identity(
        &pod,
        "v1",
        "Pod",
        Some(request.namespace()),
        request.bound_pod_name(),
        Some(request.bound_pod_uid()),
    )?;

    let pod_service_account = pod
        .data
        .pointer("/spec/serviceAccountName")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .unwrap_or("default");
    if pod_service_account != request.service_account_name() {
        return Err(ProjectedServiceAccountTokenError::binding_mismatch(
            format!(
                "bound Pod {}/{} uses ServiceAccount {}, not {}",
                request.namespace(),
                request.bound_pod_name(),
                pod_service_account,
                request.service_account_name()
            ),
        ));
    }

    let pod_node_name = pod
        .data
        .pointer("/spec/nodeName")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            ProjectedServiceAccountTokenError::binding_mismatch(format!(
                "bound Pod {}/{} is not assigned to a node",
                request.namespace(),
                request.bound_pod_name()
            ))
        })?;
    if pod_node_name != request.bound_node_name() {
        return Err(ProjectedServiceAccountTokenError::binding_mismatch(
            format!(
                "bound Pod {}/{} is not assigned to node {}",
                request.namespace(),
                request.bound_pod_name(),
                request.bound_node_name()
            ),
        ));
    }

    let node = resources
        .get_node(request.bound_node_name())
        .await
        .map_err(ProjectedServiceAccountTokenError::unavailable)?
        .ok_or(ProjectedServiceAccountTokenError::BoundNodeNotFound)?;
    validate_resource_identity(
        &node,
        "v1",
        "Node",
        None,
        request.bound_node_name(),
        request.bound_node_uid(),
    )?;

    Ok((
        request.bound_pod_name().to_string(),
        request.bound_pod_uid().to_string(),
        pod_node_name.to_string(),
        node.uid,
    ))
}

fn validate_resource_identity(
    resource: &ProjectedTokenStoredResource,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    expected_uid: Option<&str>,
) -> Result<(), ProjectedServiceAccountTokenError> {
    let canonical_api_version = resource.data.get("apiVersion").and_then(Value::as_str);
    let canonical_kind = resource.data.get("kind").and_then(Value::as_str);
    let canonical_name = resource
        .data
        .pointer("/metadata/name")
        .and_then(Value::as_str);
    let canonical_namespace = resource
        .data
        .pointer("/metadata/namespace")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty());
    let canonical_uid = resource
        .data
        .pointer("/metadata/uid")
        .and_then(Value::as_str)
        .unwrap_or_default();
    for (value, missing) in [
        (canonical_api_version, "resource missing apiVersion"),
        (canonical_kind, "resource missing kind"),
        (canonical_name, "resource missing metadata.name"),
    ] {
        if value.is_none() {
            return Err(ProjectedServiceAccountTokenError::corrupt_resource(
                format!("{kind} {namespace:?}/{name} has invalid identity: {missing}"),
            ));
        }
    }

    if canonical_api_version != Some(resource.api_version.as_str())
        || canonical_kind != Some(resource.kind.as_str())
        || canonical_namespace != resource.namespace.as_deref()
        || canonical_name != Some(resource.name.as_str())
        || canonical_uid != resource.uid
        || canonical_api_version != Some(api_version)
        || canonical_kind != Some(kind)
        || canonical_namespace != namespace
        || canonical_name != Some(name)
        || canonical_uid.trim().is_empty()
        || resource.resource_version <= 0
    {
        return Err(ProjectedServiceAccountTokenError::corrupt_resource(
            format!("{kind} {namespace:?}/{name} does not match its canonical stored identity"),
        ));
    }
    if expected_uid.is_some_and(|expected| canonical_uid != expected) {
        return Err(ProjectedServiceAccountTokenError::binding_mismatch(
            format!("{kind} {namespace:?}/{name} UID does not match the requested binding"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use base64::Engine;
    use rand_core::OsRng;
    use rsa::RsaPrivateKey;
    use rsa::pkcs8::EncodePrivateKey;
    use serde_json::{Value, json};

    use super::*;

    struct BoundTokenResources {
        service_account: ProjectedTokenStoredResource,
        pod: ProjectedTokenStoredResource,
        node: ProjectedTokenStoredResource,
    }

    #[async_trait]
    impl ProjectedTokenResourceReader for BoundTokenResources {
        async fn get_service_account(
            &self,
            _namespace: &str,
            _name: &str,
        ) -> Result<Option<ProjectedTokenStoredResource>, String> {
            Ok(Some(self.service_account.clone()))
        }

        async fn get_pod(
            &self,
            _namespace: &str,
            _name: &str,
        ) -> Result<Option<ProjectedTokenStoredResource>, String> {
            Ok(Some(self.pod.clone()))
        }

        async fn get_node(
            &self,
            _name: &str,
        ) -> Result<Option<ProjectedTokenStoredResource>, String> {
            Ok(Some(self.node.clone()))
        }
    }

    fn stored(data: Value, resource_version: i64) -> ProjectedTokenStoredResource {
        let api_version = data["apiVersion"].as_str().unwrap().to_string();
        let kind = data["kind"].as_str().unwrap().to_string();
        let namespace = data["metadata"]["namespace"].as_str().map(str::to_string);
        let name = data["metadata"]["name"].as_str().unwrap().to_string();
        let uid = data["metadata"]["uid"].as_str().unwrap().to_string();
        let mut data = data;
        data["metadata"]["resourceVersion"] = resource_version.to_string().into();
        ProjectedTokenStoredResource::new(
            api_version,
            kind,
            namespace,
            name,
            uid,
            resource_version,
            Arc::new(data),
        )
    }

    fn bound_resources() -> BoundTokenResources {
        BoundTokenResources {
            service_account: stored(
                json!({
                    "apiVersion": "v1",
                    "kind": "ServiceAccount",
                    "metadata": {
                        "name": "default", "namespace": "default", "uid": "sa-uid-a"
                    }
                }),
                1,
            ),
            node: stored(
                json!({
                    "apiVersion": "v1",
                    "kind": "Node",
                    "metadata": {"name": "node-a", "uid": "node-uid-a"}
                }),
                2,
            ),
            pod: stored(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": "pod-a", "namespace": "default", "uid": "pod-uid-a"
                    },
                    "spec": {"serviceAccountName": "default", "nodeName": "node-a"}
                }),
                3,
            ),
        }
    }

    fn request(node_name: &str) -> ProjectedServiceAccountTokenRequest {
        ProjectedServiceAccountTokenRequest::try_new(
            "default",
            "default",
            vec!["oidc-discovery-test".to_string()],
            7200,
            "pod-a",
            "pod-uid-a",
            node_name,
            Some("node-uid-a".to_string()),
        )
        .unwrap()
    }

    fn signing_key() -> String {
        RsaPrivateKey::new(&mut OsRng, 2048)
            .unwrap()
            .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
            .unwrap()
            .to_string()
    }

    fn jwt_claims(token: &str) -> Value {
        let payload_b64 = token.split('.').nth(1).expect("JWT payload segment");
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(payload_b64)
            .expect("decode payload");
        serde_json::from_slice(&payload).expect("claims JSON")
    }

    #[tokio::test]
    async fn issuer_signs_bound_projected_token_with_leader_state_claims() {
        let claims =
            authorize_projected_service_account_token(&bound_resources(), &request("node-a"))
                .await
                .expect("resource binding should authorize");
        let token = sign_authorized_projected_service_account_token(
            &signing_key(),
            claims,
            &crate::clock::SystemClock,
        )
        .expect("authorized claims should sign");

        let claims = jwt_claims(token.token());
        assert_eq!(claims["sub"], "system:serviceaccount:default:default");
        assert_eq!(claims["aud"][0], "oidc-discovery-test");
        assert_eq!(claims["kubernetes.io"]["serviceaccount"]["uid"], "sa-uid-a");
        assert_eq!(claims["kubernetes.io"]["pod"]["name"], "pod-a");
        assert_eq!(claims["kubernetes.io"]["pod"]["uid"], "pod-uid-a");
        assert_eq!(claims["kubernetes.io"]["node"]["name"], "node-a");
        assert_eq!(claims["kubernetes.io"]["node"]["uid"], "node-uid-a");
    }

    #[tokio::test]
    async fn issuer_rejects_projected_token_for_wrong_node() {
        let error =
            authorize_projected_service_account_token(&bound_resources(), &request("node-b"))
                .await
                .expect_err("a Pod on a different node must be rejected");

        assert!(
            error.to_string().contains("not assigned to node node-b"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn projected_token_resource_identity_matches_legacy_normalization_table() {
        struct Case {
            name: &'static str,
            data: Value,
            stored_namespace: Option<&'static str>,
            stored_name: &'static str,
            stored_uid: &'static str,
            stored_resource_version: i64,
            expected_error: Option<&'static str>,
        }

        let canonical = || {
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "pod-a",
                    "namespace": "default",
                    "uid": "pod-uid-a",
                    "resourceVersion": "17"
                }
            })
        };
        let cases = [
            Case {
                name: "canonical identity",
                data: canonical(),
                stored_namespace: Some("default"),
                stored_name: "pod-a",
                stored_uid: "pod-uid-a",
                stored_resource_version: 17,
                expected_error: None,
            },
            Case {
                name: "non-object metadata",
                data: json!({"apiVersion": "v1", "kind": "Pod", "metadata": []}),
                stored_namespace: Some("default"),
                stored_name: "pod-a",
                stored_uid: "pod-uid-a",
                stored_resource_version: 17,
                expected_error: Some("resource missing metadata.name"),
            },
            Case {
                name: "missing apiVersion",
                data: json!({
                    "kind": "Pod",
                    "metadata": {"name": "pod-a", "namespace": "default", "uid": "pod-uid-a"}
                }),
                stored_namespace: Some("default"),
                stored_name: "pod-a",
                stored_uid: "pod-uid-a",
                stored_resource_version: 17,
                expected_error: Some("resource missing apiVersion"),
            },
            Case {
                name: "non-string apiVersion",
                data: json!({
                    "apiVersion": 1,
                    "kind": "Pod",
                    "metadata": {"name": "pod-a", "namespace": "default", "uid": "pod-uid-a"}
                }),
                stored_namespace: Some("default"),
                stored_name: "pod-a",
                stored_uid: "pod-uid-a",
                stored_resource_version: 17,
                expected_error: Some("resource missing apiVersion"),
            },
            Case {
                name: "missing kind",
                data: json!({
                    "apiVersion": "v1",
                    "metadata": {"name": "pod-a", "namespace": "default", "uid": "pod-uid-a"}
                }),
                stored_namespace: Some("default"),
                stored_name: "pod-a",
                stored_uid: "pod-uid-a",
                stored_resource_version: 17,
                expected_error: Some("resource missing kind"),
            },
            Case {
                name: "non-string kind",
                data: json!({
                    "apiVersion": "v1",
                    "kind": {},
                    "metadata": {"name": "pod-a", "namespace": "default", "uid": "pod-uid-a"}
                }),
                stored_namespace: Some("default"),
                stored_name: "pod-a",
                stored_uid: "pod-uid-a",
                stored_resource_version: 17,
                expected_error: Some("resource missing kind"),
            },
            Case {
                name: "missing name",
                data: json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"namespace": "default", "uid": "pod-uid-a"}
                }),
                stored_namespace: Some("default"),
                stored_name: "pod-a",
                stored_uid: "pod-uid-a",
                stored_resource_version: 17,
                expected_error: Some("resource missing metadata.name"),
            },
            Case {
                name: "non-string name",
                data: json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": 7, "namespace": "default", "uid": "pod-uid-a"}
                }),
                stored_namespace: Some("default"),
                stored_name: "pod-a",
                stored_uid: "pod-uid-a",
                stored_resource_version: 17,
                expected_error: Some("resource missing metadata.name"),
            },
            Case {
                name: "empty namespace normalizes to absent",
                data: json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "pod-a", "namespace": "", "uid": "pod-uid-a"}
                }),
                stored_namespace: None,
                stored_name: "pod-a",
                stored_uid: "pod-uid-a",
                stored_resource_version: 17,
                expected_error: None,
            },
            Case {
                name: "empty stored namespace does not equal normalized absent",
                data: json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "pod-a", "namespace": "", "uid": "pod-uid-a"}
                }),
                stored_namespace: Some(""),
                stored_name: "pod-a",
                stored_uid: "pod-uid-a",
                stored_resource_version: 17,
                expected_error: Some("does not match its canonical stored identity"),
            },
            Case {
                name: "non-string namespace normalizes to absent",
                data: json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "pod-a", "namespace": 3, "uid": "pod-uid-a"}
                }),
                stored_namespace: None,
                stored_name: "pod-a",
                stored_uid: "pod-uid-a",
                stored_resource_version: 17,
                expected_error: None,
            },
            Case {
                name: "non-string namespace does not match namespaced storage",
                data: json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "pod-a", "namespace": 3, "uid": "pod-uid-a"}
                }),
                stored_namespace: Some("default"),
                stored_name: "pod-a",
                stored_uid: "pod-uid-a",
                stored_resource_version: 17,
                expected_error: Some("does not match its canonical stored identity"),
            },
            Case {
                name: "missing uid normalizes empty and is rejected",
                data: json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "pod-a", "namespace": "default"}
                }),
                stored_namespace: Some("default"),
                stored_name: "pod-a",
                stored_uid: "",
                stored_resource_version: 17,
                expected_error: Some("does not match its canonical stored identity"),
            },
            Case {
                name: "non-string uid normalizes empty and is rejected",
                data: json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "pod-a", "namespace": "default", "uid": 9}
                }),
                stored_namespace: Some("default"),
                stored_name: "pod-a",
                stored_uid: "",
                stored_resource_version: 17,
                expected_error: Some("does not match its canonical stored identity"),
            },
            Case {
                name: "invalid JSON resourceVersion remains ignored",
                data: {
                    let mut data = canonical();
                    data["metadata"]["resourceVersion"] = "not-an-rv".into();
                    data
                },
                stored_namespace: Some("default"),
                stored_name: "pod-a",
                stored_uid: "pod-uid-a",
                stored_resource_version: 17,
                expected_error: None,
            },
            Case {
                name: "non-string JSON resourceVersion remains ignored",
                data: {
                    let mut data = canonical();
                    data["metadata"]["resourceVersion"] = 17.into();
                    data
                },
                stored_namespace: Some("default"),
                stored_name: "pod-a",
                stored_uid: "pod-uid-a",
                stored_resource_version: 17,
                expected_error: None,
            },
            Case {
                name: "non-positive stored resourceVersion is rejected",
                data: canonical(),
                stored_namespace: Some("default"),
                stored_name: "pod-a",
                stored_uid: "pod-uid-a",
                stored_resource_version: 0,
                expected_error: Some("does not match its canonical stored identity"),
            },
        ];

        for case in cases {
            let resource = ProjectedTokenStoredResource::new(
                "v1".to_string(),
                "Pod".to_string(),
                case.stored_namespace.map(str::to_string),
                case.stored_name.to_string(),
                case.stored_uid.to_string(),
                case.stored_resource_version,
                Arc::new(case.data),
            );
            let result = validate_resource_identity(
                &resource,
                "v1",
                "Pod",
                case.stored_namespace,
                "pod-a",
                Some("pod-uid-a"),
            );
            match case.expected_error {
                None => assert!(result.is_ok(), "{}: {result:?}", case.name),
                Some(expected) => {
                    let error = result.expect_err(case.name);
                    assert!(
                        error.to_string().contains(expected),
                        "{}: expected {expected:?}, got {error}",
                        case.name
                    );
                }
            }
        }
    }

    #[test]
    fn projected_token_resource_reader_is_object_safe() {
        fn assert_object_safe(_: &dyn ProjectedTokenResourceReader) {}
        assert_object_safe(&bound_resources());
    }
}
