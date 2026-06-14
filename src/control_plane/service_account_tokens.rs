use anyhow::{Context, Result, bail};

use crate::control_plane::client::{
    ProjectedServiceAccountToken, ProjectedServiceAccountTokenRequest,
};
use crate::datastore::{Resource, backend::DatastoreBackend};

pub async fn issue_projected_service_account_token(
    db: &dyn DatastoreBackend,
    signing_key_pem: &str,
    request: &ProjectedServiceAccountTokenRequest,
    bound_pod: Option<&Resource>,
) -> Result<ProjectedServiceAccountToken> {
    let service_account = db
        .get_resource(
            "v1",
            "ServiceAccount",
            Some(&request.namespace),
            &request.service_account_name,
        )
        .await?
        .with_context(|| {
            format!(
                "ServiceAccount {}/{} not found",
                request.namespace, request.service_account_name
            )
        })?;
    let service_account_uid = service_account
        .data
        .pointer("/metadata/uid")
        .and_then(|v| v.as_str())
        .filter(|uid| !uid.is_empty())
        .map(str::to_string);

    let (bound_pod_name, bound_pod_uid, bound_node_name, bound_node_uid) =
        resolve_bound_pod_and_node(db, request, bound_pod).await?;

    let audiences = if request.audiences.is_empty() {
        vec!["https://kubernetes.default.svc.cluster.local".to_string()]
    } else {
        request.audiences.clone()
    };
    let audience_refs: Vec<&str> = audiences.iter().map(String::as_str).collect();
    let expiration_seconds = crate::auth::normalize_service_account_token_expiration_seconds(Some(
        request.expiration_seconds,
    ));

    let token =
        crate::auth::generate_sa_token_with_bound_pod(crate::auth::ServiceAccountTokenRequest {
            ca_key_pem: signing_key_pem,
            service_account: &request.service_account_name,
            namespace: &request.namespace,
            audiences: &audience_refs,
            expiration_seconds: Some(expiration_seconds),
            bound: crate::auth::BoundServiceAccountToken {
                pod_name: bound_pod_name.as_deref(),
                pod_uid: bound_pod_uid.as_deref(),
                node_name: bound_node_name.as_deref(),
                node_uid: bound_node_uid.as_deref(),
                secret_name: None,
                secret_uid: None,
                sa_uid: service_account_uid.as_deref(),
            },
        })
        .context("Failed to generate projected ServiceAccount token")?;

    Ok(ProjectedServiceAccountToken { token })
}

async fn resolve_bound_pod_and_node(
    db: &dyn DatastoreBackend,
    request: &ProjectedServiceAccountTokenRequest,
    bound_pod: Option<&Resource>,
) -> Result<(
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
)> {
    let Some(pod_name) = request.bound_pod_name.as_deref() else {
        return Ok((None, None, None, None));
    };

    let pod = bound_pod
        .with_context(|| format!("bound Pod {}/{} not found", request.namespace, pod_name))?;
    if let Some(expected_uid) = request.bound_pod_uid.as_deref() {
        let actual_uid = pod
            .data
            .pointer("/metadata/uid")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if actual_uid != expected_uid {
            bail!(
                "bound Pod {}/{} UID mismatch: expected {}, got {}",
                request.namespace,
                pod_name,
                expected_uid,
                actual_uid
            );
        }
    }

    let pod_service_account = pod
        .data
        .pointer("/spec/serviceAccountName")
        .and_then(|v| v.as_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("default");
    if pod_service_account != request.service_account_name {
        bail!(
            "bound Pod {}/{} uses ServiceAccount {}, not {}",
            request.namespace,
            pod_name,
            pod_service_account,
            request.service_account_name
        );
    }

    let pod_node_name = pod
        .data
        .pointer("/spec/nodeName")
        .and_then(|v| v.as_str())
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    if let Some(expected_node) = request.bound_node_name.as_deref()
        && pod_node_name.as_deref() != Some(expected_node)
    {
        bail!(
            "bound Pod {}/{} is not assigned to node {}",
            request.namespace,
            pod_name,
            expected_node
        );
    }

    let node_uid = match pod_node_name.as_deref() {
        Some(node_name) => {
            let node = db
                .get_resource("v1", "Node", None, node_name)
                .await?
                .with_context(|| format!("bound node {node_name} not found"))?;
            let stored_uid = node
                .data
                .pointer("/metadata/uid")
                .and_then(|v| v.as_str())
                .filter(|uid| !uid.is_empty())
                .map(str::to_string);
            if let Some(expected) = request.bound_node_uid.as_deref()
                && stored_uid.as_deref() != Some(expected)
            {
                bail!(
                    "bound node {} UID mismatch: expected {}, got {}",
                    node_name,
                    expected,
                    stored_uid.as_deref().unwrap_or("")
                );
            }
            stored_uid
        }
        None => None,
    };

    Ok((
        Some(pod_name.to_string()),
        request.bound_pod_uid.clone(),
        pod_node_name,
        node_uid,
    ))
}

#[cfg(test)]
mod tests {
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
        let db = crate::datastore::test_support::in_memory().await;
        seed_bound_token_resources(&db).await;
        let bound_pod = db
            .get_resource("v1", "Pod", Some("default"), "pod-a")
            .await
            .unwrap();

        let token = issue_projected_service_account_token(
            &db,
            &signing_key(),
            &ProjectedServiceAccountTokenRequest {
                namespace: "default".to_string(),
                service_account_name: "default".to_string(),
                audiences: vec!["oidc-discovery-test".to_string()],
                expiration_seconds: 7200,
                bound_pod_name: Some("pod-a".to_string()),
                bound_pod_uid: Some("pod-uid-a".to_string()),
                bound_node_name: Some("node-a".to_string()),
                bound_node_uid: Some("node-uid-a".to_string()),
            },
            bound_pod.as_ref(),
        )
        .await
        .expect("leader should issue projected token");

        let claims = jwt_claims(&token.token);
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
        let db = crate::datastore::test_support::in_memory().await;
        seed_bound_token_resources(&db).await;
        let bound_pod = db
            .get_resource("v1", "Pod", Some("default"), "pod-a")
            .await
            .unwrap();

        let err = issue_projected_service_account_token(
            &db,
            &signing_key(),
            &ProjectedServiceAccountTokenRequest {
                namespace: "default".to_string(),
                service_account_name: "default".to_string(),
                audiences: vec!["api".to_string()],
                expiration_seconds: 3600,
                bound_pod_name: Some("pod-a".to_string()),
                bound_pod_uid: Some("pod-uid-a".to_string()),
                bound_node_name: Some("node-b".to_string()),
                bound_node_uid: None,
            },
            bound_pod.as_ref(),
        )
        .await
        .expect_err("leader must reject a token request for a pod on a different node");

        assert!(
            err.to_string().contains("not assigned to node node-b"),
            "unexpected error: {err:#}"
        );
    }
}
