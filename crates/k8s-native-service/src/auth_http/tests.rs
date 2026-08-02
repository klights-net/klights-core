use axum::http::{HeaderMap, HeaderValue};
use base64::Engine as _;
use klights_auth::ImpersonationError;

use super::*;

#[test]
fn impersonation_header_extraction_is_api_owned_and_strict() {
    let mut headers = HeaderMap::new();
    headers.append(IMPERSONATE_GROUP, HeaderValue::from_static("developers"));
    assert!(matches!(
        parse_impersonation_headers(&headers),
        Err(ImpersonationError::InvalidRequest { .. })
    ));

    headers.insert(IMPERSONATE_USER, HeaderValue::from_static("alice"));
    headers.insert("impersonate-extra-scopes", HeaderValue::from_static("view"));
    let request = parse_impersonation_headers(&headers)
        .expect("valid headers")
        .expect("impersonation present");
    assert_eq!(request.username, "alice");
    assert_eq!(request.groups, vec!["developers"]);
    assert_eq!(
        request.extra,
        vec![("scopes".to_string(), "view".to_string())]
    );
}

#[test]
fn requestheader_identity_cannot_assert_system_masters() {
    let mut headers = HeaderMap::new();
    headers.insert("x-remote-user", HeaderValue::from_static("alice"));
    headers.append("x-remote-group", HeaderValue::from_static("developers"));
    headers.append("x-remote-group", HeaderValue::from_static("system:masters"));
    let identity = requestheader_identity_from_headers(&headers).unwrap();
    assert_eq!(identity.groups, vec!["developers"]);
}

#[test]
fn forwarded_client_cert_header_roundtrips_base64_der() {
    let der = vec![0x30, 0x82, 0x01, 0x02, 0x03];
    let encoded = base64::engine::general_purpose::STANDARD.encode(&der);
    let mut headers = HeaderMap::new();
    headers.insert(
        FORWARDED_CLIENT_CERT_HEADER,
        HeaderValue::from_str(&encoded).unwrap(),
    );
    assert_eq!(forwarded_client_cert_from_headers(&headers), Some(der));

    assert_eq!(forwarded_client_cert_from_headers(&HeaderMap::new()), None);
    headers.insert(
        FORWARDED_CLIENT_CERT_HEADER,
        HeaderValue::from_static("not base64!!!"),
    );
    assert_eq!(forwarded_client_cert_from_headers(&headers), None);
}

struct RejectBootstrap;

impl klights_leader_api::LeaderBootstrapTokenAuthentication for RejectBootstrap {
    fn authenticate_bootstrap_token<'a>(
        &'a self,
        _token: &'a str,
    ) -> klights_leader_api::ClusterIdentityFuture<'a, klights_leader_api::BootstrapTokenIdentity>
    {
        Box::pin(async { Err(klights_leader_api::ClusterIdentityError::rejected("unused")) })
    }
}

struct UnavailableSigningKeys;

impl klights_leader_api::LeaderServiceAccountSigningKeyState for UnavailableSigningKeys {
    fn service_account_signing_key_pem(
        &self,
    ) -> klights_leader_api::ClusterIdentityFuture<
        '_,
        klights_leader_api::ServiceAccountSigningKeyPem,
    > {
        Box::pin(async {
            Err(klights_leader_api::ClusterIdentityError::dependency_failure("unused signing key"))
        })
    }
}

struct EmptySubjects;

impl klights_leader_api::LeaderBoundTokenSubjectLookup for EmptySubjects {
    fn service_account_uid<'a>(
        &'a self,
        _namespace: &'a str,
        _name: &'a str,
    ) -> klights_leader_api::ClusterIdentityFuture<'a, Option<String>> {
        Box::pin(async { Ok(None) })
    }

    fn pod_uid<'a>(
        &'a self,
        _namespace: &'a str,
        _name: &'a str,
    ) -> klights_leader_api::ClusterIdentityFuture<'a, Option<String>> {
        Box::pin(async { Ok(None) })
    }

    fn node_uid<'a>(
        &'a self,
        _name: &'a str,
    ) -> klights_leader_api::ClusterIdentityFuture<'a, Option<String>> {
        Box::pin(async { Ok(None) })
    }

    fn secret_uid<'a>(
        &'a self,
        _namespace: &'a str,
        _name: &'a str,
    ) -> klights_leader_api::ClusterIdentityFuture<'a, Option<String>> {
        Box::pin(async { Ok(None) })
    }
}

#[tokio::test]
async fn anonymous_auth_false_rejects_unauthenticated_requests_before_authorization() {
    use axum::body::Body;
    use axum::http::Request;
    use axum::{Router, middleware, routing::get};
    use std::sync::Arc;
    use tower::ServiceExt;

    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(Default::default()));
    let inputs = Arc::new(crate::policy_inputs::AuthenticationHttpInputs::new(
        crate::policy_inputs::AuthenticationPolicyInputs::new(
            Arc::new(klights_auth::authorizer::DenyAuthorizer),
            Arc::new(RejectBootstrap),
            None,
            None,
            None,
            false,
        ),
        crate::policy_inputs::AuthenticationRuntimeInputs::new(
            Arc::new(EmptySubjects),
            Arc::new(UnavailableSigningKeys),
            Arc::new(klights_auth::clock::SystemClock),
            supervisor,
        ),
    ));
    let app = Router::new()
        .route("/api", get(|| async { "must not run" }))
        .layer(middleware::from_fn(move |request, next| {
            authenticate_request(inputs.clone(), request, next)
        }));
    let response = app
        .oneshot(Request::builder().uri("/api").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::UNAUTHORIZED);
}
