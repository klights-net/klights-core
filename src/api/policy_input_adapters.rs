//! Root composition adapters for focused native policy HTTP inputs.

use std::sync::Arc;

use k8s_native_service::policy_inputs::{
    AuthenticationHttpInputs, AuthenticationPolicyInputs, AuthenticationRuntimeInputs,
    AuthorizationHttpInputs, PriorityFairnessHttpInputs,
};
use klights_leader_api::{
    ClusterIdentityError, ClusterIdentityFuture, LeaderBoundTokenSubjectLookup,
};

use super::ApiState;

struct ApiBoundTokenSubjects<PodQuery: ?Sized> {
    resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    pod_query: Arc<PodQuery>,
}

impl<PodQuery> ApiBoundTokenSubjects<PodQuery>
where
    PodQuery: klights_pod_api::PodQuery + ?Sized,
{
    async fn resource_uid(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<String>, ClusterIdentityError> {
        crate::api::resource_query_ports::get_resource(
            self.resource_query.as_ref(),
            api_version,
            kind,
            namespace,
            name,
        )
        .await
        .map_err(|error| {
            ClusterIdentityError::dependency_failure(format!(
                "credential subject lookup failed: {error:?}"
            ))
        })
        .map(|resource| {
            resource.and_then(|resource| {
                resource
                    .data
                    .pointer("/metadata/uid")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
        })
    }
}

impl<PodQuery> LeaderBoundTokenSubjectLookup for ApiBoundTokenSubjects<PodQuery>
where
    PodQuery: klights_pod_api::PodQuery + ?Sized + 'static,
{
    fn service_account_uid<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
    ) -> ClusterIdentityFuture<'a, Option<String>> {
        Box::pin(async move {
            self.resource_uid("v1", "ServiceAccount", Some(namespace), name)
                .await
        })
    }

    fn pod_uid<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
    ) -> ClusterIdentityFuture<'a, Option<String>> {
        Box::pin(async move {
            let request =
                klights_pod_api::PodGetRequest::try_by_name(namespace, name).map_err(|error| {
                    ClusterIdentityError::dependency_failure(format!(
                        "bound Pod lookup failed: {error}"
                    ))
                })?;
            self.pod_query
                .get_pod(request)
                .await
                .map(|pod| pod.map(|pod| pod.uid))
                .map_err(|error| {
                    ClusterIdentityError::dependency_failure(format!(
                        "bound Pod lookup failed: {error}"
                    ))
                })
        })
    }

    fn node_uid<'a>(&'a self, name: &'a str) -> ClusterIdentityFuture<'a, Option<String>> {
        Box::pin(async move { self.resource_uid("v1", "Node", None, name).await })
    }

    fn secret_uid<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
    ) -> ClusterIdentityFuture<'a, Option<String>> {
        Box::pin(async move {
            self.resource_uid("v1", "Secret", Some(namespace), name)
                .await
        })
    }
}

fn bound_token_subjects<PodQuery>(
    resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    pod_query: Arc<PodQuery>,
) -> Arc<dyn LeaderBoundTokenSubjectLookup>
where
    PodQuery: klights_pod_api::PodQuery + ?Sized + 'static,
{
    Arc::new(ApiBoundTokenSubjects {
        resource_query,
        pod_query,
    })
}

pub(crate) fn authentication_http_inputs(state: &ApiState) -> AuthenticationHttpInputs {
    let policy = state.auth_policy();
    let operational = state.operational();
    let resources = state.resource_mutation();
    let policy = AuthenticationPolicyInputs::new(
        policy.authorizer.clone(),
        policy.bootstrap_token_authenticator.clone(),
        policy.oidc_authenticator.clone(),
        policy.webhook_authenticator.clone(),
        policy.cluster_ca_pem.clone(),
        operational.config.anonymous_auth,
    );
    let runtime = AuthenticationRuntimeInputs::new(
        bound_token_subjects(
            resources.resource_query.clone(),
            resources.pod_repository.clone(),
        ),
        operational.signing_keys.clone(),
        operational.clock.clone(),
        operational.task_supervisor.clone(),
    );
    AuthenticationHttpInputs::new(policy, runtime)
}

pub(crate) type ApiAuthorizationHttpInputs = AuthorizationHttpInputs<dyn crate::audit::AuditSink>;

pub(crate) fn authorization_http_inputs(state: &ApiState) -> ApiAuthorizationHttpInputs {
    AuthorizationHttpInputs::new(
        state.auth_policy().authorizer.clone(),
        state.auth_policy().audit_sink.clone(),
        state.operational().clock.clone(),
    )
}

pub(crate) type ApiPriorityFairnessHttpInputs =
    PriorityFairnessHttpInputs<crate::api::priority_fairness::ApiPriorityFairness>;

pub(crate) fn priority_fairness_http_inputs(state: &ApiState) -> ApiPriorityFairnessHttpInputs {
    PriorityFairnessHttpInputs::new(
        state.auth_policy().api_priority_fairness.clone(),
        state.resource_mutation().resource_query.clone(),
    )
}
