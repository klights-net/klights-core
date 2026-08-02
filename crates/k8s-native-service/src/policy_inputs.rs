//! Focused dependency values for Kubernetes-native policy HTTP adapters.
//!
//! Root composition constructs these values from canonical policy ports.  The
//! native service consumes them without gaining access to the root API state or
//! to a concrete datastore/transport owner.

use std::sync::Arc;

use klights_auth::{
    Authorizer, clock::Clock, oidc::OidcValidator, webhook_auth::WebhookAuthenticator,
};
use klights_leader_api::{
    LeaderBootstrapTokenAuthentication, LeaderBoundTokenSubjectLookup, LeaderResourceQuery,
    LeaderServiceAccountSigningKeyState,
};
use klights_supervisor::TaskSupervisor;

/// Stable policy inputs used by authentication HTTP handling.
#[derive(Clone)]
pub struct AuthenticationPolicyInputs {
    authorizer: Arc<dyn Authorizer>,
    bootstrap_token_authenticator: Arc<dyn LeaderBootstrapTokenAuthentication>,
    oidc_authenticator: Option<Arc<dyn OidcValidator>>,
    webhook_authenticator: Option<Arc<dyn WebhookAuthenticator>>,
    cluster_ca_pem: Option<Arc<String>>,
    anonymous_auth: bool,
}

impl AuthenticationPolicyInputs {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        authorizer: Arc<dyn Authorizer>,
        bootstrap_token_authenticator: Arc<dyn LeaderBootstrapTokenAuthentication>,
        oidc_authenticator: Option<Arc<dyn OidcValidator>>,
        webhook_authenticator: Option<Arc<dyn WebhookAuthenticator>>,
        cluster_ca_pem: Option<Arc<String>>,
        anonymous_auth: bool,
    ) -> Self {
        Self {
            authorizer,
            bootstrap_token_authenticator,
            oidc_authenticator,
            webhook_authenticator,
            cluster_ca_pem,
            anonymous_auth,
        }
    }

    pub fn authorizer(&self) -> &Arc<dyn Authorizer> {
        &self.authorizer
    }

    pub fn bootstrap_token_authenticator(&self) -> &Arc<dyn LeaderBootstrapTokenAuthentication> {
        &self.bootstrap_token_authenticator
    }

    pub fn oidc_authenticator(&self) -> Option<&Arc<dyn OidcValidator>> {
        self.oidc_authenticator.as_ref()
    }

    pub fn webhook_authenticator(&self) -> Option<&Arc<dyn WebhookAuthenticator>> {
        self.webhook_authenticator.as_ref()
    }

    pub fn cluster_ca_pem(&self) -> Option<&Arc<String>> {
        self.cluster_ca_pem.as_ref()
    }

    pub fn anonymous_auth(&self) -> bool {
        self.anonymous_auth
    }
}

/// Operation/runtime inputs used by authentication HTTP handling.
#[derive(Clone)]
pub struct AuthenticationRuntimeInputs {
    bound_token_subjects: Arc<dyn LeaderBoundTokenSubjectLookup>,
    signing_keys: Arc<dyn LeaderServiceAccountSigningKeyState>,
    clock: Arc<dyn Clock>,
    task_supervisor: Arc<TaskSupervisor>,
}

impl AuthenticationRuntimeInputs {
    pub fn new(
        bound_token_subjects: Arc<dyn LeaderBoundTokenSubjectLookup>,
        signing_keys: Arc<dyn LeaderServiceAccountSigningKeyState>,
        clock: Arc<dyn Clock>,
        task_supervisor: Arc<TaskSupervisor>,
    ) -> Self {
        Self {
            bound_token_subjects,
            signing_keys,
            clock,
            task_supervisor,
        }
    }

    pub fn bound_token_subjects(&self) -> &Arc<dyn LeaderBoundTokenSubjectLookup> {
        &self.bound_token_subjects
    }

    pub fn signing_keys(&self) -> &Arc<dyn LeaderServiceAccountSigningKeyState> {
        &self.signing_keys
    }

    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    pub fn task_supervisor(&self) -> &Arc<TaskSupervisor> {
        &self.task_supervisor
    }
}

/// Complete focused input for the authentication middleware.
#[derive(Clone)]
pub struct AuthenticationHttpInputs {
    policy: AuthenticationPolicyInputs,
    runtime: AuthenticationRuntimeInputs,
}

impl AuthenticationHttpInputs {
    pub fn new(policy: AuthenticationPolicyInputs, runtime: AuthenticationRuntimeInputs) -> Self {
        Self { policy, runtime }
    }

    pub fn policy(&self) -> &AuthenticationPolicyInputs {
        &self.policy
    }

    pub fn runtime(&self) -> &AuthenticationRuntimeInputs {
        &self.runtime
    }
}

/// Focused input for authorization middleware.
#[derive(Clone)]
pub struct AuthorizationHttpInputs<Audit: ?Sized> {
    authorizer: Arc<dyn Authorizer>,
    audit: Arc<Audit>,
    clock: Arc<dyn Clock>,
}

impl<Audit: ?Sized> AuthorizationHttpInputs<Audit> {
    pub fn new(authorizer: Arc<dyn Authorizer>, audit: Arc<Audit>, clock: Arc<dyn Clock>) -> Self {
        Self {
            authorizer,
            audit,
            clock,
        }
    }

    pub fn authorizer(&self) -> &Arc<dyn Authorizer> {
        &self.authorizer
    }

    pub fn audit(&self) -> &Arc<Audit> {
        &self.audit
    }

    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }
}

/// Focused input for API priority-and-fairness admission.
#[derive(Clone)]
pub struct PriorityFairnessHttpInputs<Policy: ?Sized> {
    policy: Arc<Policy>,
    resource_query: Arc<dyn LeaderResourceQuery>,
}

impl<Policy: ?Sized> PriorityFairnessHttpInputs<Policy> {
    pub fn new(policy: Arc<Policy>, resource_query: Arc<dyn LeaderResourceQuery>) -> Self {
        Self {
            policy,
            resource_query,
        }
    }

    pub fn policy(&self) -> &Arc<Policy> {
        &self.policy
    }

    pub fn resource_query(&self) -> &Arc<dyn LeaderResourceQuery> {
        &self.resource_query
    }
}
