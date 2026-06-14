//! Bootstrap token CSR authorizer: defense-in-depth restriction for bootstrap identities.
//!
//! This authorizer ensures that identities in the `system:bootstrappers` group
//! can ONLY access CSR resources and discovery endpoints, regardless of what
//! RBAC rules might grant. It acts as a safety net in the authorizer chain.

use crate::auth::authorizer::{AuthorizationDecision, Authorizer};
use crate::auth::identity::AuthenticatedIdentity;
use crate::auth::request_attributes::{AuthorizationRequest, RequestKind};
use async_trait::async_trait;

/// Authorizer that restricts bootstrap token identities to CSR operations only.
///
/// This must appear before the RBAC authorizer in the chain so that bootstrap
/// tokens are constrained even if RBAC rules are overly broad.
pub struct BootstrapCsrAuthorizer;

/// Check if the identity is a bootstrap token (system:bootstrappers group).
fn is_bootstrap_identity(identity: &AuthenticatedIdentity) -> bool {
    identity
        .groups
        .contains(&"system:bootstrappers".to_string())
}

/// Check if the request targets CSR resources.
fn is_csr_resource(request: &AuthorizationRequest) -> bool {
    if request.kind != RequestKind::Resource {
        return false;
    }
    request.resource.as_deref() == Some("certificatesigningrequests")
        && request.api_group.as_deref() == Some("certificates.k8s.io")
}

/// Check if the verb is allowed for CSR operations.
fn is_allowed_csr_verb(request: &AuthorizationRequest) -> bool {
    matches!(request.verb.as_str(), "create" | "get" | "list" | "watch")
}

/// Check if the request targets a discovery non-resource URL.
fn is_discovery_url(request: &AuthorizationRequest) -> bool {
    if request.kind != RequestKind::NonResource {
        return false;
    }
    if request.verb != "get" {
        return false;
    }
    let url = match request.non_resource_url.as_deref() {
        Some(u) => u,
        None => return false,
    };
    matches!(
        url,
        "/api"
            | "/api/"
            | "/apis"
            | "/apis/"
            | "/healthz"
            | "/livez"
            | "/readyz"
            | "/openapi"
            | "/openapi/*"
            | "/version"
            | "/version/"
    ) || url.starts_with("/api/")
        || url.starts_with("/apis/")
        || url.starts_with("/openapi/")
}

#[async_trait]
impl Authorizer for BootstrapCsrAuthorizer {
    async fn authorize(
        &self,
        identity: &AuthenticatedIdentity,
        request: &AuthorizationRequest,
    ) -> AuthorizationDecision {
        if !is_bootstrap_identity(identity) {
            return AuthorizationDecision::no_opinion();
        }

        // Bootstrap tokens can create/get/list/watch CSRs
        if is_csr_resource(request) && is_allowed_csr_verb(request) {
            return AuthorizationDecision::allow("bootstrap CSR access");
        }

        // Bootstrap tokens can access discovery endpoints
        if is_discovery_url(request) {
            return AuthorizationDecision::allow("bootstrap discovery access");
        }

        // Everything else is explicitly denied for bootstrap tokens
        AuthorizationDecision::deny("bootstrap tokens may only access CSR resources and discovery")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bootstrap_id() -> AuthenticatedIdentity {
        AuthenticatedIdentity::bootstrap("abcdef", &[])
    }

    fn bootstrap_id_with_extra_group() -> AuthenticatedIdentity {
        AuthenticatedIdentity::bootstrap("abcdef", &["system:bootstrappers:nodes".to_string()])
    }

    fn user_id() -> AuthenticatedIdentity {
        AuthenticatedIdentity::client_cert("user".to_string(), vec![])
    }

    fn node_id() -> AuthenticatedIdentity {
        AuthenticatedIdentity::client_cert(
            "system:node:tokyo".to_string(),
            vec!["system:nodes".to_string()],
        )
    }

    fn admin_id() -> AuthenticatedIdentity {
        AuthenticatedIdentity::client_cert("admin".to_string(), vec!["system:masters".to_string()])
    }

    fn authorizer() -> BootstrapCsrAuthorizer {
        BootstrapCsrAuthorizer
    }

    // === Bootstrap CSR access ===

    #[tokio::test]
    async fn bootstrap_can_create_csr() {
        let authz = authorizer();
        let req = AuthorizationRequest::resource(
            "create",
            "certificates.k8s.io",
            "certificates.k8s.io/v1",
            "certificatesigningrequests",
            None,
            None,
            None,
        );
        let decision = authz.authorize(&bootstrap_id(), &req).await;
        assert!(decision.allowed);
    }

    #[tokio::test]
    async fn bootstrap_can_get_csr() {
        let authz = authorizer();
        let req = AuthorizationRequest::resource(
            "get",
            "certificates.k8s.io",
            "certificates.k8s.io/v1",
            "certificatesigningrequests",
            None,
            None,
            Some("my-csr"),
        );
        let decision = authz.authorize(&bootstrap_id(), &req).await;
        assert!(decision.allowed);
    }

    #[tokio::test]
    async fn bootstrap_can_list_csr() {
        let authz = authorizer();
        let req = AuthorizationRequest::resource(
            "list",
            "certificates.k8s.io",
            "certificates.k8s.io/v1",
            "certificatesigningrequests",
            None,
            None,
            None,
        );
        let decision = authz.authorize(&bootstrap_id(), &req).await;
        assert!(decision.allowed);
    }

    #[tokio::test]
    async fn bootstrap_can_watch_csr() {
        let authz = authorizer();
        let req = AuthorizationRequest::resource(
            "watch",
            "certificates.k8s.io",
            "certificates.k8s.io/v1",
            "certificatesigningrequests",
            None,
            None,
            None,
        );
        let decision = authz.authorize(&bootstrap_id(), &req).await;
        assert!(decision.allowed);
    }

    // === Bootstrap CSR denial ===

    #[tokio::test]
    async fn bootstrap_cannot_update_csr() {
        let authz = authorizer();
        let req = AuthorizationRequest::resource(
            "update",
            "certificates.k8s.io",
            "certificates.k8s.io/v1",
            "certificatesigningrequests",
            None,
            None,
            Some("my-csr"),
        );
        let decision = authz.authorize(&bootstrap_id(), &req).await;
        assert!(decision.denied);
    }

    #[tokio::test]
    async fn bootstrap_cannot_update_csr_status() {
        let authz = authorizer();
        let req = AuthorizationRequest::resource(
            "update",
            "certificates.k8s.io",
            "certificates.k8s.io/v1",
            "certificatesigningrequests",
            Some("status"),
            None,
            Some("my-csr"),
        );
        let decision = authz.authorize(&bootstrap_id(), &req).await;
        assert!(decision.denied);
    }

    #[tokio::test]
    async fn bootstrap_cannot_update_csr_approval() {
        let authz = authorizer();
        let req = AuthorizationRequest::resource(
            "update",
            "certificates.k8s.io",
            "certificates.k8s.io/v1",
            "certificatesigningrequests",
            Some("approval"),
            None,
            Some("my-csr"),
        );
        let decision = authz.authorize(&bootstrap_id(), &req).await;
        assert!(decision.denied);
    }

    #[tokio::test]
    async fn bootstrap_cannot_delete_csr() {
        let authz = authorizer();
        let req = AuthorizationRequest::resource(
            "delete",
            "certificates.k8s.io",
            "certificates.k8s.io/v1",
            "certificatesigningrequests",
            None,
            None,
            Some("my-csr"),
        );
        let decision = authz.authorize(&bootstrap_id(), &req).await;
        assert!(decision.denied);
    }

    // === Bootstrap non-CSR denial ===

    #[tokio::test]
    async fn bootstrap_cannot_renew_lease() {
        let authz = authorizer();
        let req = AuthorizationRequest::resource(
            "update",
            "coordination.k8s.io",
            "coordination.k8s.io/v1",
            "leases",
            None,
            Some("kube-node-lease"),
            Some("tokyo"),
        );
        let decision = authz.authorize(&bootstrap_id(), &req).await;
        assert!(decision.denied);
    }

    #[tokio::test]
    async fn bootstrap_cannot_update_node_status() {
        let authz = authorizer();
        let req = AuthorizationRequest::resource(
            "update",
            "",
            "v1",
            "nodes",
            Some("status"),
            None,
            Some("tokyo"),
        );
        let decision = authz.authorize(&bootstrap_id(), &req).await;
        assert!(decision.denied);
    }

    #[tokio::test]
    async fn bootstrap_cannot_update_pod_status() {
        let authz = authorizer();
        let req = AuthorizationRequest::resource(
            "update",
            "",
            "v1",
            "pods",
            Some("status"),
            Some("default"),
            Some("my-pod"),
        );
        let decision = authz.authorize(&bootstrap_id(), &req).await;
        assert!(decision.denied);
    }

    #[tokio::test]
    async fn bootstrap_cannot_read_secrets() {
        let authz = authorizer();
        let req = AuthorizationRequest::resource(
            "get",
            "",
            "v1",
            "secrets",
            None,
            Some("default"),
            Some("my-secret"),
        );
        let decision = authz.authorize(&bootstrap_id(), &req).await;
        assert!(decision.denied);
    }

    #[tokio::test]
    async fn bootstrap_cannot_list_pods() {
        let authz = authorizer();
        let req =
            AuthorizationRequest::resource("list", "", "v1", "pods", None, Some("default"), None);
        let decision = authz.authorize(&bootstrap_id(), &req).await;
        assert!(decision.denied);
    }

    // === Bootstrap discovery access ===

    #[tokio::test]
    async fn bootstrap_can_access_discovery_endpoints() {
        let authz = authorizer();
        for url in &["/api", "/apis", "/healthz", "/livez", "/version"] {
            let req = AuthorizationRequest::non_resource("get", url);
            let decision = authz.authorize(&bootstrap_id(), &req).await;
            assert!(decision.allowed, "bootstrap should access {url}");
        }
    }

    #[tokio::test]
    async fn bootstrap_cannot_post_to_discovery() {
        let authz = authorizer();
        let req = AuthorizationRequest::non_resource("post", "/api");
        let decision = authz.authorize(&bootstrap_id(), &req).await;
        assert!(decision.denied);
    }

    // === Non-bootstrap identities get no-opinion ===

    #[tokio::test]
    async fn regular_user_gets_no_opinion() {
        let authz = authorizer();
        let req = AuthorizationRequest::resource("get", "", "v1", "pods", None, None, None);
        let decision = authz.authorize(&user_id(), &req).await;
        assert!(!decision.allowed);
        assert!(!decision.denied);
    }

    #[tokio::test]
    async fn node_identity_gets_no_opinion() {
        let authz = authorizer();
        let req = AuthorizationRequest::resource(
            "update",
            "",
            "v1",
            "nodes",
            Some("status"),
            None,
            Some("tokyo"),
        );
        let decision = authz.authorize(&node_id(), &req).await;
        assert!(!decision.allowed);
        assert!(!decision.denied);
    }

    #[tokio::test]
    async fn admin_identity_gets_no_opinion() {
        let authz = authorizer();
        let req = AuthorizationRequest::resource("get", "", "v1", "pods", None, None, None);
        let decision = authz.authorize(&admin_id(), &req).await;
        assert!(!decision.allowed);
        assert!(!decision.denied);
    }

    // === Extra bootstrap groups still restricted ===

    #[tokio::test]
    async fn bootstrap_with_extra_group_still_denied_non_csr() {
        let authz = authorizer();
        let req = AuthorizationRequest::resource(
            "get",
            "",
            "v1",
            "secrets",
            None,
            Some("default"),
            Some("my-secret"),
        );
        let decision = authz
            .authorize(&bootstrap_id_with_extra_group(), &req)
            .await;
        assert!(decision.denied);
    }
}
