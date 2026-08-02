use std::sync::Arc;

use k8s_native_service::policy_inputs::{
    AuthenticationHttpInputs, AuthenticationPolicyInputs, AuthenticationRuntimeInputs,
    AuthorizationHttpInputs, PriorityFairnessHttpInputs,
};
use klights_auth::{
    AuthenticatedIdentity, Authorizer, authorizer::AuthorizationDecision, clock::Clock,
    request_attributes::AuthorizationRequest,
};
use klights_cluster_core::Resource;
use klights_leader_api::{
    BootstrapTokenIdentity, ClusterIdentityFuture, LeaderBootstrapTokenAuthentication,
    LeaderBoundTokenSubjectLookup, LeaderResourceQuery, LeaderServiceAccountSigningKeyState,
    ResourceGetRequest, ResourceListRequest, ResourceListResult, ResourceQueryFuture,
    ServiceAccountSigningKeyPem,
};
use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};
use time::OffsetDateTime;

struct FakeAuthorizer;

#[async_trait::async_trait]
impl Authorizer for FakeAuthorizer {
    async fn authorize(
        &self,
        _identity: &AuthenticatedIdentity,
        _request: &AuthorizationRequest,
    ) -> AuthorizationDecision {
        AuthorizationDecision::allow("compiler contract")
    }
}

struct FakeBootstrapTokens;

impl LeaderBootstrapTokenAuthentication for FakeBootstrapTokens {
    fn authenticate_bootstrap_token<'a>(
        &'a self,
        _token: &'a str,
    ) -> ClusterIdentityFuture<'a, BootstrapTokenIdentity> {
        panic!("compiler-only fake")
    }
}

struct FakeBoundSubjects;

impl LeaderBoundTokenSubjectLookup for FakeBoundSubjects {
    fn service_account_uid<'a>(
        &'a self,
        _namespace: &'a str,
        _name: &'a str,
    ) -> ClusterIdentityFuture<'a, Option<String>> {
        panic!("compiler-only fake")
    }

    fn pod_uid<'a>(
        &'a self,
        _namespace: &'a str,
        _name: &'a str,
    ) -> ClusterIdentityFuture<'a, Option<String>> {
        panic!("compiler-only fake")
    }

    fn node_uid<'a>(&'a self, _name: &'a str) -> ClusterIdentityFuture<'a, Option<String>> {
        panic!("compiler-only fake")
    }

    fn secret_uid<'a>(
        &'a self,
        _namespace: &'a str,
        _name: &'a str,
    ) -> ClusterIdentityFuture<'a, Option<String>> {
        panic!("compiler-only fake")
    }
}

struct FakeSigningKeys;

impl LeaderServiceAccountSigningKeyState for FakeSigningKeys {
    fn service_account_signing_key_pem(
        &self,
    ) -> ClusterIdentityFuture<'_, ServiceAccountSigningKeyPem> {
        panic!("compiler-only fake")
    }
}

struct FakeClock;

impl Clock for FakeClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::UNIX_EPOCH
    }
}

struct FakeAudit;
struct FakePriorityFairness;
struct FakeResourceQuery;

impl LeaderResourceQuery for FakeResourceQuery {
    fn get_resource(
        &self,
        _request: ResourceGetRequest,
    ) -> ResourceQueryFuture<'_, Option<Resource>> {
        panic!("compiler-only fake")
    }

    fn list_resources(
        &self,
        _request: ResourceListRequest,
    ) -> ResourceQueryFuture<'_, ResourceListResult> {
        panic!("compiler-only fake")
    }
}

#[test]
fn focused_policy_input_constructors_accept_only_canonical_capabilities() {
    let authorizer: Arc<dyn Authorizer> = Arc::new(FakeAuthorizer);
    let clock: Arc<dyn Clock> = Arc::new(FakeClock);
    let policy = AuthenticationPolicyInputs::new(
        Arc::clone(&authorizer),
        Arc::new(FakeBootstrapTokens),
        None,
        None,
        Some(Arc::new("test cluster CA".to_string())),
        false,
    );
    let runtime = AuthenticationRuntimeInputs::new(
        Arc::new(FakeBoundSubjects),
        Arc::new(FakeSigningKeys),
        Arc::clone(&clock),
        Arc::new(TaskSupervisor::new(TaskCategoryConfig::default())),
    );

    let authentication = AuthenticationHttpInputs::new(policy, runtime);
    let audit = Arc::new(FakeAudit);
    let authorization = AuthorizationHttpInputs::new(
        Arc::clone(&authorizer),
        Arc::clone(&audit),
        Arc::clone(&clock),
    );
    let priority_fairness = PriorityFairnessHttpInputs::new(
        Arc::new(FakePriorityFairness),
        Arc::new(FakeResourceQuery),
    );

    assert!(!authentication.policy().anonymous_auth());
    assert!(authentication.policy().oidc_authenticator().is_none());
    assert!(authentication.policy().webhook_authenticator().is_none());
    assert!(authentication.policy().cluster_ca_pem().is_some());
    let _ = authentication.policy().authorizer();
    let _ = authentication.policy().bootstrap_token_authenticator();
    let _ = authentication.runtime().bound_token_subjects();
    let _ = authentication.runtime().signing_keys();
    let _ = authentication.runtime().clock();
    let _ = authentication.runtime().task_supervisor();
    assert!(Arc::ptr_eq(authorization.authorizer(), &authorizer));
    assert!(Arc::ptr_eq(authorization.audit(), &audit));
    assert!(Arc::ptr_eq(authorization.clock(), &clock));
    let _ = priority_fairness.policy();
    let _ = priority_fairness.resource_query();
}
