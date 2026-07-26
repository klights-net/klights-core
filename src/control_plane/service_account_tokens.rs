use crate::datastore::{Resource, backend::DatastoreBackend};
use crate::kubelet::pod_repository::store::PodStore;
use klights_leader_api::{
    ProjectedServiceAccountToken, ProjectedServiceAccountTokenError,
    ProjectedServiceAccountTokenRequest,
};

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct AuthorizedProjectedServiceAccountTokenClaims {
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

pub(crate) async fn authorize_projected_service_account_token(
    db: &dyn DatastoreBackend,
    pod_store: &PodStore,
    request: &ProjectedServiceAccountTokenRequest,
) -> Result<AuthorizedProjectedServiceAccountTokenClaims, ProjectedServiceAccountTokenError> {
    let service_account = db
        .get_resource(
            "v1",
            "ServiceAccount",
            Some(request.namespace()),
            request.service_account_name(),
        )
        .await
        .map_err(|error| ProjectedServiceAccountTokenError::unavailable(error.to_string()))?
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
        resolve_bound_pod_and_node(db, pod_store, request).await?;

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

pub(crate) fn sign_authorized_projected_service_account_token(
    signing_key_pem: &str,
    claims: AuthorizedProjectedServiceAccountTokenClaims,
    clock: &dyn crate::auth::clock::Clock,
) -> Result<ProjectedServiceAccountToken, ProjectedServiceAccountTokenError> {
    let audience_refs: Vec<&str> = claims.audiences.iter().map(String::as_str).collect();
    let token = crate::auth::generate_sa_token_with_bound_pod_and_clock(
        crate::auth::ServiceAccountTokenRequest {
            ca_key_pem: signing_key_pem,
            service_account: &claims.service_account_name,
            namespace: &claims.namespace,
            audiences: &audience_refs,
            expiration_seconds: Some(claims.expiration_seconds),
            bound: crate::auth::BoundServiceAccountToken {
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

#[cfg(test)]
async fn issue_projected_service_account_token(
    db: &dyn DatastoreBackend,
    pod_store: &PodStore,
    signing_key_pem: &str,
    request: &ProjectedServiceAccountTokenRequest,
) -> Result<ProjectedServiceAccountToken, ProjectedServiceAccountTokenError> {
    let claims = authorize_projected_service_account_token(db, pod_store, request).await?;
    sign_authorized_projected_service_account_token(
        signing_key_pem,
        claims,
        &crate::auth::clock::SystemClock,
    )
}

async fn resolve_bound_pod_and_node(
    db: &dyn DatastoreBackend,
    pod_store: &PodStore,
    request: &ProjectedServiceAccountTokenRequest,
) -> Result<(String, String, String, String), ProjectedServiceAccountTokenError> {
    let pod = pod_store
        .get(request.namespace(), request.bound_pod_name())
        .await
        .map_err(|error| ProjectedServiceAccountTokenError::unavailable(error.to_string()))?
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
        .and_then(|v| v.as_str())
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
        .and_then(|v| v.as_str())
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

    let node = db
        .get_resource("v1", "Node", None, request.bound_node_name())
        .await
        .map_err(|error| ProjectedServiceAccountTokenError::unavailable(error.to_string()))?
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
    resource: &Resource,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    expected_uid: Option<&str>,
) -> Result<(), ProjectedServiceAccountTokenError> {
    let canonical = Resource::try_from_data(resource.data.clone()).map_err(|error| {
        ProjectedServiceAccountTokenError::corrupt_resource(format!(
            "{kind} {namespace:?}/{name} has invalid identity: {error}"
        ))
    })?;
    if canonical.api_version != resource.api_version
        || canonical.kind != resource.kind
        || canonical.namespace != resource.namespace
        || canonical.name != resource.name
        || canonical.uid != resource.uid
        || canonical.api_version != api_version
        || canonical.kind != kind
        || canonical.namespace.as_deref() != namespace
        || canonical.name != name
        || canonical.uid.trim().is_empty()
        || resource.resource_version <= 0
    {
        return Err(ProjectedServiceAccountTokenError::corrupt_resource(
            format!("{kind} {namespace:?}/{name} does not match its canonical stored identity"),
        ));
    }
    if expected_uid.is_some_and(|expected| canonical.uid != expected) {
        return Err(ProjectedServiceAccountTokenError::binding_mismatch(
            format!("{kind} {namespace:?}/{name} UID does not match the requested binding"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use base64::Engine;
    use rand_core::OsRng;
    use rsa::RsaPrivateKey;
    use rsa::pkcs8::EncodePrivateKey;
    use serde_json::{Value, json};

    use super::*;
    use crate::datastore::backend::DatastoreBackend;

    fn signing_key() -> String {
        RsaPrivateKey::new(&mut OsRng, 2048)
            .unwrap()
            .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
            .unwrap()
            .to_string()
    }

    async fn seed_bound_token_resources(db: &dyn DatastoreBackend) {
        db.create_resource(
            "v1",
            "ServiceAccount",
            Some("default"),
            "default",
            json!({
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": {"name": "default", "namespace": "default", "uid": "sa-uid-a"}
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "v1",
            "Node",
            None,
            "node-a",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "node-a", "uid": "node-uid-a"}
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "pod-a",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "pod-a", "namespace": "default", "uid": "pod-uid-a"},
                "spec": {"serviceAccountName": "default", "nodeName": "node-a"}
            }),
        )
        .await
        .unwrap();
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
        let db: crate::datastore::DatastoreHandle =
            Arc::new(crate::datastore::test_support::in_memory().await);
        seed_bound_token_resources(db.as_ref()).await;
        let pod_store = PodStore::new(db.clone());

        let token = issue_projected_service_account_token(
            db.as_ref(),
            &pod_store,
            &signing_key(),
            &ProjectedServiceAccountTokenRequest::try_new(
                "default",
                "default",
                vec!["oidc-discovery-test".to_string()],
                7200,
                "pod-a",
                "pod-uid-a",
                "node-a",
                Some("node-uid-a".to_string()),
            )
            .unwrap(),
        )
        .await
        .expect("leader should issue projected token");

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
        let db: crate::datastore::DatastoreHandle =
            Arc::new(crate::datastore::test_support::in_memory().await);
        seed_bound_token_resources(db.as_ref()).await;
        let pod_store = PodStore::new(db.clone());

        let err = issue_projected_service_account_token(
            db.as_ref(),
            &pod_store,
            &signing_key(),
            &ProjectedServiceAccountTokenRequest::try_new(
                "default",
                "default",
                vec!["api".to_string()],
                3600,
                "pod-a",
                "pod-uid-a",
                "node-b",
                None,
            )
            .unwrap(),
        )
        .await
        .expect_err("leader must reject a token request for a pod on a different node");

        assert!(
            err.to_string().contains("not assigned to node node-b"),
            "unexpected error: {err:#}"
        );
    }
}
