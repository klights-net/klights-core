//! Authorization trait, decision type, and authorizer chain.
//!
//! Defines the object-safe `Authorizer` trait, `AuthorizationDecision` matching
//! Kubernetes SubjectAccessReview semantics, and an `AuthorizerChain` that
//! composes multiple authorizers in order.

use crate::auth::identity::AuthenticatedIdentity;
use crate::auth::node_authorizer::NodeAuthorizer;
use crate::auth::request_attributes::AuthorizationRequest;
use async_trait::async_trait;

/// Kubernetes-compatible authorization decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorizationDecision {
    pub allowed: bool,
    pub denied: bool,
    pub reason: String,
    pub evaluation_error: Option<String>,
}

impl AuthorizationDecision {
    /// Explicit allow.
    pub fn allow(reason: &str) -> Self {
        Self {
            allowed: true,
            denied: false,
            reason: reason.to_string(),
            evaluation_error: None,
        }
    }

    /// Explicit deny.
    pub fn deny(reason: &str) -> Self {
        Self {
            allowed: false,
            denied: true,
            reason: reason.to_string(),
            evaluation_error: None,
        }
    }

    /// No opinion — continue to next authorizer in chain.
    pub fn no_opinion() -> Self {
        Self {
            allowed: false,
            denied: false,
            reason: String::new(),
            evaluation_error: None,
        }
    }

    /// Decision from an evaluation error.
    pub fn evaluation_error(error: &str) -> Self {
        Self {
            allowed: false,
            denied: false,
            reason: String::new(),
            evaluation_error: Some(error.to_string()),
        }
    }
}

/// Object-safe authorizer trait.
#[async_trait]
pub trait Authorizer: Send + Sync {
    async fn authorize(
        &self,
        identity: &AuthenticatedIdentity,
        request: &AuthorizationRequest,
    ) -> AuthorizationDecision;
}

/// Chain of authorizers evaluated in order.
///
/// Stops on the first explicit allow or deny. No-opinion continues to the next.
/// If all return no-opinion, the final result is deny.
pub struct AuthorizerChain {
    authorizers: Vec<Box<dyn Authorizer>>,
}

impl AuthorizerChain {
    pub fn new(authorizers: Vec<Box<dyn Authorizer>>) -> Self {
        Self { authorizers }
    }

    /// Create a chain with just the system:masters bypass and a final deny.
    pub fn default_chain() -> Self {
        Self::new(vec![
            Box::new(SystemMastersAuthorizer),
            Box::new(DenyAuthorizer),
        ])
    }

    /// Create a permissive chain for tests (allow everything).
    pub fn test_allow_all() -> Self {
        Self::new(vec![Box::new(AllowAllAuthorizer)])
    }

    /// Create the full production chain with RBAC.
    ///
    /// Order: system:masters bypass → bootstrap CSR → RBAC → Node authorizer → deny.
    pub fn default_chain_with_rbac(
        db: crate::datastore::backend::DatastoreHandle,
        pod_repository: std::sync::Arc<dyn crate::kubelet::pod_repository::PodReader>,
    ) -> Self {
        use crate::auth::bootstrap_authorizer::BootstrapCsrAuthorizer;
        use crate::auth::node_policy_store::DatastoreNodePolicyStore;
        use crate::auth::rbac_authorizer::RbacAuthorizer;
        use crate::auth::rbac_policy_store::DatastoreRbacPolicyStore;
        Self::new(vec![
            Box::new(SystemMastersAuthorizer),
            Box::new(BootstrapCsrAuthorizer),
            Box::new(RbacAuthorizer::new(std::sync::Arc::new(
                DatastoreRbacPolicyStore::new(db),
            ))),
            Box::new(NodeAuthorizer::new(Some(std::sync::Arc::new(
                DatastoreNodePolicyStore::new(pod_repository),
            )))),
            Box::new(DenyAuthorizer),
        ])
    }
}

#[async_trait]
impl Authorizer for AuthorizerChain {
    async fn authorize(
        &self,
        identity: &AuthenticatedIdentity,
        request: &AuthorizationRequest,
    ) -> AuthorizationDecision {
        let mut last_error = None;
        for authorizer in &self.authorizers {
            let decision = authorizer.authorize(identity, request).await;
            if decision.allowed {
                return decision;
            }
            if decision.denied {
                return decision;
            }
            if decision.evaluation_error.is_some() {
                last_error = decision.evaluation_error;
            }
        }
        // All no-opinion — deny with any evaluation error from the chain.
        if let Some(err) = last_error {
            AuthorizationDecision::deny(&format!("no authorizer allowed: {err}"))
        } else {
            AuthorizationDecision::deny("no authorizer allowed")
        }
    }
}

/// Allows everything for identities in the `system:masters` group.
pub struct SystemMastersAuthorizer;

#[async_trait]
impl Authorizer for SystemMastersAuthorizer {
    async fn authorize(
        &self,
        identity: &AuthenticatedIdentity,
        _request: &AuthorizationRequest,
    ) -> AuthorizationDecision {
        if identity.is_admin() {
            AuthorizationDecision::allow("system:masters bypass")
        } else {
            AuthorizationDecision::no_opinion()
        }
    }
}

/// Final deny authorizer — denies everything.
pub struct DenyAuthorizer;

#[async_trait]
impl Authorizer for DenyAuthorizer {
    async fn authorize(
        &self,
        _identity: &AuthenticatedIdentity,
        _request: &AuthorizationRequest,
    ) -> AuthorizationDecision {
        AuthorizationDecision::deny("RBAC denial")
    }
}

/// Allow-all authorizer for tests.
pub struct AllowAllAuthorizer;

#[async_trait]
impl Authorizer for AllowAllAuthorizer {
    async fn authorize(
        &self,
        _identity: &AuthenticatedIdentity,
        _request: &AuthorizationRequest,
    ) -> AuthorizationDecision {
        AuthorizationDecision::allow("test allow-all")
    }
}

/// Recording authorizer for tests — records every authorization request and returns a
/// configurable decision.
pub struct RecordingAuthorizer {
    pub requests: tokio::sync::Mutex<Vec<(AuthenticatedIdentity, AuthorizationRequest)>>,
    pub decision: tokio::sync::Mutex<AuthorizationDecision>,
}

impl RecordingAuthorizer {
    pub fn new(decision: AuthorizationDecision) -> Self {
        Self {
            requests: tokio::sync::Mutex::new(Vec::new()),
            decision: tokio::sync::Mutex::new(decision),
        }
    }

    pub fn allow() -> Self {
        Self::new(AuthorizationDecision::allow("recording allow"))
    }

    pub fn deny(reason: &str) -> Self {
        Self::new(AuthorizationDecision::deny(reason))
    }

    pub async fn set_decision(&self, decision: AuthorizationDecision) {
        *self.decision.lock().await = decision;
    }

    pub async fn take_requests(&self) -> Vec<(AuthenticatedIdentity, AuthorizationRequest)> {
        std::mem::take(&mut *self.requests.lock().await)
    }
}

#[async_trait]
impl Authorizer for RecordingAuthorizer {
    async fn authorize(
        &self,
        identity: &AuthenticatedIdentity,
        request: &AuthorizationRequest,
    ) -> AuthorizationDecision {
        self.requests
            .lock()
            .await
            .push((identity.clone(), request.clone()));
        self.decision.lock().await.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::identity::AuthenticatedIdentity;

    fn admin_identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::client_cert("admin".to_string(), vec!["system:masters".to_string()])
    }

    fn user_identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::client_cert("user".to_string(), vec![])
    }

    fn sample_request() -> AuthorizationRequest {
        AuthorizationRequest::resource("get", "", "v1", "pods", None, None, None)
    }

    #[tokio::test]
    async fn system_masters_allows_everything() {
        let authorizer = SystemMastersAuthorizer;
        let id = admin_identity();
        let req = sample_request();
        let decision = authorizer.authorize(&id, &req).await;
        assert!(decision.allowed);
        assert!(!decision.denied);
    }

    #[tokio::test]
    async fn system_masters_no_opinion_for_non_admin() {
        let authorizer = SystemMastersAuthorizer;
        let id = user_identity();
        let req = sample_request();
        let decision = authorizer.authorize(&id, &req).await;
        assert!(!decision.allowed);
        assert!(!decision.denied);
    }

    #[tokio::test]
    async fn deny_authorizer_always_denies() {
        let authorizer = DenyAuthorizer;
        let id = admin_identity();
        let req = sample_request();
        let decision = authorizer.authorize(&id, &req).await;
        assert!(!decision.allowed);
        assert!(decision.denied);
    }

    #[tokio::test]
    async fn chain_all_short_circuits_on_allow() {
        let chain = AuthorizerChain::new(vec![
            Box::new(SystemMastersAuthorizer),
            Box::new(DenyAuthorizer),
        ]);
        let id = admin_identity();
        let req = sample_request();
        let decision = chain.authorize(&id, &req).await;
        assert!(decision.allowed);
    }

    #[tokio::test]
    async fn chain_deny_short_circuits() {
        struct AlwaysDeny;
        #[async_trait]
        impl Authorizer for AlwaysDeny {
            async fn authorize(
                &self,
                _identity: &AuthenticatedIdentity,
                _request: &AuthorizationRequest,
            ) -> AuthorizationDecision {
                AuthorizationDecision::deny("test deny")
            }
        }
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        struct AllowCounter(std::sync::Arc<std::sync::atomic::AtomicBool>);
        #[async_trait]
        impl Authorizer for AllowCounter {
            async fn authorize(
                &self,
                _identity: &AuthenticatedIdentity,
                _request: &AuthorizationRequest,
            ) -> AuthorizationDecision {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
                AuthorizationDecision::allow("should not be reached")
            }
        }
        let chain = AuthorizerChain::new(vec![
            Box::new(AlwaysDeny),
            Box::new(AllowCounter(counter.clone())),
        ]);
        let id = user_identity();
        let req = sample_request();
        let decision = chain.authorize(&id, &req).await;
        assert!(decision.denied);
        assert_eq!(decision.reason, "test deny");
        assert!(!counter.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn chain_no_opinion_falls_through_to_deny() {
        let chain = AuthorizerChain::new(vec![
            Box::new(SystemMastersAuthorizer),
            Box::new(DenyAuthorizer),
        ]);
        let id = user_identity();
        let req = sample_request();
        let decision = chain.authorize(&id, &req).await;
        assert!(!decision.allowed);
        assert!(decision.denied);
    }

    #[tokio::test]
    async fn chain_evaluation_error_propagated() {
        struct ErrorAuthorizer;
        #[async_trait]
        impl Authorizer for ErrorAuthorizer {
            async fn authorize(
                &self,
                _identity: &AuthenticatedIdentity,
                _request: &AuthorizationRequest,
            ) -> AuthorizationDecision {
                AuthorizationDecision::evaluation_error("test error")
            }
        }
        let chain = AuthorizerChain::new(vec![Box::new(ErrorAuthorizer)]);
        let id = user_identity();
        let req = sample_request();
        let decision = chain.authorize(&id, &req).await;
        assert!(decision.denied);
        assert!(decision.reason.contains("test error"));
    }

    #[tokio::test]
    async fn default_chain_allows_admin_denies_user() {
        let chain = AuthorizerChain::default_chain();
        let req = sample_request();

        let admin = admin_identity();
        assert!(chain.authorize(&admin, &req).await.allowed);

        let user = user_identity();
        assert!(chain.authorize(&user, &req).await.denied);
    }
}
