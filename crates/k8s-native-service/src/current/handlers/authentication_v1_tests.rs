use std::sync::Arc;

use klights_leader_api::{
    BootstrapTokenIdentity, ClusterIdentityError, ClusterIdentityFuture,
    LeaderBootstrapTokenAuthentication, LeaderBoundTokenSubjectLookup,
    LeaderServiceAccountSigningKeyState, ServiceAccountSigningKeyPem,
};

use super::authentication_v1::token_review_response;

struct FailingBootstrapStore;

impl LeaderBootstrapTokenAuthentication for FailingBootstrapStore {
    fn authenticate_bootstrap_token<'a>(
        &'a self,
        _token: &'a str,
    ) -> ClusterIdentityFuture<'a, BootstrapTokenIdentity> {
        Box::pin(async {
            Err(ClusterIdentityError::dependency_failure(
                "bootstrap datastore unavailable",
            ))
        })
    }
}

struct UnusedSigningKeys;

impl LeaderServiceAccountSigningKeyState for UnusedSigningKeys {
    fn service_account_signing_key_pem(
        &self,
    ) -> ClusterIdentityFuture<'_, ServiceAccountSigningKeyPem> {
        Box::pin(async { Err(ClusterIdentityError::internal_failure("unused signing key")) })
    }
}

struct EmptyBoundSubjects;

impl LeaderBoundTokenSubjectLookup for EmptyBoundSubjects {
    fn service_account_uid<'a>(
        &'a self,
        _namespace: &'a str,
        _name: &'a str,
    ) -> ClusterIdentityFuture<'a, Option<String>> {
        Box::pin(async { Ok(None) })
    }

    fn pod_uid<'a>(
        &'a self,
        _namespace: &'a str,
        _name: &'a str,
    ) -> ClusterIdentityFuture<'a, Option<String>> {
        Box::pin(async { Ok(None) })
    }

    fn node_uid<'a>(&'a self, _name: &'a str) -> ClusterIdentityFuture<'a, Option<String>> {
        Box::pin(async { Ok(None) })
    }

    fn secret_uid<'a>(
        &'a self,
        _namespace: &'a str,
        _name: &'a str,
    ) -> ClusterIdentityFuture<'a, Option<String>> {
        Box::pin(async { Ok(None) })
    }
}

#[tokio::test]
async fn tokenreview_formats_bootstrap_dependency_failure_in_status() {
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(Default::default()));
    let inputs = crate::policy_inputs::AuthenticationHttpInputs::new(
        crate::policy_inputs::AuthenticationPolicyInputs::new(
            Arc::new(klights_auth::authorizer::DenyAuthorizer),
            Arc::new(FailingBootstrapStore),
            None,
            None,
            None,
            false,
        ),
        crate::policy_inputs::AuthenticationRuntimeInputs::new(
            Arc::new(EmptyBoundSubjects),
            Arc::new(UnusedSigningKeys),
            Arc::new(klights_auth::clock::SystemClock),
            supervisor,
        ),
    );
    let request = serde_json::json!({
        "apiVersion": "authentication.k8s.io/v1",
        "kind": "TokenReview",
        "spec": {"token": "abcdef.0123456789abcdef"}
    });

    let error = match crate::auth_http::authenticate_token_for_review(
        &inputs,
        "abcdef.0123456789abcdef",
        &[],
    )
    .await
    {
        Ok(_) => {
            panic!("bootstrap dependency failure must remain operational, not unauthenticated")
        }
        Err(error) => error,
    };
    assert!(matches!(
        error,
        klights_auth::AuthenticationError::DependencyFailure { .. }
    ));

    let response = token_review_response(
        request,
        serde_json::json!({"authenticated": false, "error": error.to_string()}),
    );
    assert_eq!(response["status"]["authenticated"], false);
    assert_eq!(
        response["status"]["error"],
        "bootstrap datastore unavailable"
    );
}
