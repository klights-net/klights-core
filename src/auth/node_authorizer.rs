//! Node authorizer: validates system:node:<nodeName> scoped access.
//!
//! Implements the Kubernetes Node authorizer subset for klights workers.
//! Uses `NodePolicyStore` for pod-scoped access decisions — pure OO,
//! mockable, no datastore/network/filesystem dependency.

use crate::auth::authorizer::{AuthorizationDecision, Authorizer};
use crate::auth::identity::AuthenticatedIdentity;
use crate::auth::node_policy_store::NodePolicyStore;
use crate::auth::request_attributes::{AuthorizationRequest, RequestKind};
use async_trait::async_trait;
use std::sync::Arc;

/// Parse node name from `system:node:<nodeName>` identity.
fn parse_node_name(identity: &AuthenticatedIdentity) -> Option<String> {
    if !identity.groups.contains(&"system:nodes".to_string()) {
        return None;
    }
    identity
        .username
        .strip_prefix("system:node:")
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

/// Node authorizer: allows node-scoped access for `system:node:<nodeName>`.
///
/// For username-based checks (own node, own lease), the store is not required.
/// For pod-scoped checks (pod status, pod reads, referenced objects), the
/// store is used to verify the node relationship.
pub struct NodeAuthorizer {
    store: Option<Arc<dyn NodePolicyStore>>,
}

impl NodeAuthorizer {
    pub fn new(store: Option<Arc<dyn NodePolicyStore>>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl Authorizer for NodeAuthorizer {
    async fn authorize(
        &self,
        identity: &AuthenticatedIdentity,
        request: &AuthorizationRequest,
    ) -> AuthorizationDecision {
        let Some(node_name) = parse_node_name(identity) else {
            return AuthorizationDecision::no_opinion();
        };

        // Non-resource URLs: no opinion (RBAC handles discovery)
        if request.kind == RequestKind::NonResource {
            return AuthorizationDecision::no_opinion();
        }

        let resource = match request.resource.as_deref() {
            Some(r) => r,
            None => return AuthorizationDecision::no_opinion(),
        };

        match resource {
            "nodes" => self.check_node_access(&node_name, request),
            "leases" => self.check_lease_access(&node_name, request),
            "pods" => self.check_pod_access(&node_name, request).await,
            "secrets"
            | "configmaps"
            | "persistentvolumeclaims"
            | "persistentvolumes"
            | "serviceaccounts" => {
                self.check_referenced_object_read(&node_name, resource, request)
                    .await
            }
            _ => AuthorizationDecision::no_opinion(),
        }
    }
}

impl NodeAuthorizer {
    fn relationship_store(&self) -> Option<&Arc<dyn NodePolicyStore>> {
        self.store.as_ref()
    }

    fn check_node_access(
        &self,
        node_name: &str,
        request: &AuthorizationRequest,
    ) -> AuthorizationDecision {
        let subresource = request.subresource.as_deref();
        let name = request.name.as_deref();

        match request.verb.as_str() {
            "get" if name == Some(node_name) => {
                AuthorizationDecision::allow("node can get own node")
            }
            "update" | "patch" if subresource == Some("status") && name == Some(node_name) => {
                AuthorizationDecision::allow("node can update own node status")
            }
            _ => AuthorizationDecision::no_opinion(),
        }
    }

    fn check_lease_access(
        &self,
        node_name: &str,
        request: &AuthorizationRequest,
    ) -> AuthorizationDecision {
        let namespace = request.namespace.as_deref();
        let name = request.name.as_deref();

        match request.verb.as_str() {
            "get" | "update" | "patch" | "create"
                if namespace == Some("kube-node-lease") && name == Some(node_name) =>
            {
                AuthorizationDecision::allow("node can manage own lease")
            }
            _ => AuthorizationDecision::no_opinion(),
        }
    }

    async fn check_pod_access(
        &self,
        node_name: &str,
        request: &AuthorizationRequest,
    ) -> AuthorizationDecision {
        let subresource = request.subresource.as_deref();

        match request.verb.as_str() {
            "get" | "list" | "watch" => {
                let Some(store) = self.relationship_store() else {
                    return AuthorizationDecision::no_opinion();
                };

                // List/watch must be constrained to this node.
                if request.verb == "list" || request.verb == "watch" {
                    let field_selector = request.field_selector.as_deref().unwrap_or("");
                    if !field_selector_matches_node(field_selector, node_name) {
                        return AuthorizationDecision::no_opinion();
                    }
                }
                // Get must be for a pod on this node.
                if request.verb == "get" {
                    if let (Some(ns), Some(name)) =
                        (request.namespace.as_deref(), request.name.as_deref())
                    {
                        let pod_node = store.get_pod_node(ns, name).await;
                        if pod_node.as_deref() != Some(node_name) {
                            return AuthorizationDecision::no_opinion();
                        }
                    } else {
                        return AuthorizationDecision::no_opinion();
                    }
                }
                AuthorizationDecision::allow("node can read pods")
            }
            "update" | "patch" if subresource == Some("status") => {
                let Some(store) = self.relationship_store() else {
                    return AuthorizationDecision::no_opinion();
                };

                // Pod status update: pod must be bound to this node
                if let (Some(ns), Some(name)) =
                    (request.namespace.as_deref(), request.name.as_deref())
                {
                    let pod_node = store.get_pod_node(ns, name).await;
                    if pod_node.as_deref() != Some(node_name) {
                        return AuthorizationDecision::no_opinion();
                    }
                } else {
                    return AuthorizationDecision::no_opinion();
                }
                AuthorizationDecision::allow("node can update pod status")
            }
            _ => AuthorizationDecision::no_opinion(),
        }
    }

    async fn check_referenced_object_read(
        &self,
        node_name: &str,
        resource_kind: &str,
        request: &AuthorizationRequest,
    ) -> AuthorizationDecision {
        match request.verb.as_str() {
            "get" | "list" | "watch" => {
                // If store is available, check that the object is referenced
                // by a pod on this node.
                if let Some(ref store) = self.store {
                    if let (Some(ns), Some(name)) =
                        (request.namespace.as_deref(), request.name.as_deref())
                    {
                        // Check all pods on this node to see if any reference this object
                        let pods = store.list_pods_on_node(node_name).await;
                        for (pod_ns, pod_name) in &pods {
                            if pod_ns != ns {
                                continue;
                            }
                            let refs = store
                                .get_pod_referenced_objects(pod_ns, pod_name, resource_kind)
                                .await;
                            if refs.contains(&name.to_string()) {
                                return AuthorizationDecision::allow(
                                    "node can read object referenced by own pod",
                                );
                            }
                        }
                    }
                    return AuthorizationDecision::no_opinion();
                }
                // Without store, no opinion — defer to RBAC
                AuthorizationDecision::no_opinion()
            }
            _ => AuthorizationDecision::no_opinion(),
        }
    }
}

fn field_selector_matches_node(field_selector: &str, node_name: &str) -> bool {
    field_selector.split(',').any(|part| {
        let trimmed = part.trim();
        let Some((field, value)) = trimmed.split_once('=') else {
            return false;
        };
        field.trim() == "spec.nodeName" && value.trim() == node_name
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::node_policy_store::InMemoryNodePolicyStore;

    fn node_identity(name: &str) -> AuthenticatedIdentity {
        AuthenticatedIdentity::client_cert(
            format!("system:node:{name}"),
            vec!["system:nodes".to_string()],
        )
    }

    fn other_identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::client_cert("user".to_string(), vec![])
    }

    fn bootstrap_identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::bootstrap("abcdef", &[])
    }

    fn authorizer() -> NodeAuthorizer {
        NodeAuthorizer::new(None)
    }

    /// Build a store with pre-seeded pod data and return authorizer + store.
    fn seeded_store() -> InMemoryNodePolicyStore {
        let mut store = InMemoryNodePolicyStore::new();
        store.add_pod("default", "my-pod", "tokyo");
        store.add_pod("kube-system", "coredns", "tokyo");
        store.add_pod("default", "other-pod", "osaka");
        store.add_reference(
            "default",
            "my-pod",
            "secrets",
            vec!["my-secret".to_string()],
        );
        store.add_reference(
            "default",
            "my-pod",
            "configmaps",
            vec!["my-config".to_string()],
        );
        store
    }

    // --- Node access tests (username-based, no store needed) ---

    #[tokio::test]
    async fn node_can_get_own_node() {
        let authorizer = authorizer();
        let id = node_identity("tokyo");
        let req =
            AuthorizationRequest::resource("get", "", "v1", "nodes", None, None, Some("tokyo"));
        let decision = authorizer.authorize(&id, &req).await;
        assert!(decision.allowed);
    }

    #[tokio::test]
    async fn node_cannot_get_other_node() {
        let authorizer = authorizer();
        let id = node_identity("tokyo");
        let req =
            AuthorizationRequest::resource("get", "", "v1", "nodes", None, None, Some("osaka"));
        let decision = authorizer.authorize(&id, &req).await;
        assert!(!decision.allowed);
    }

    #[tokio::test]
    async fn node_can_update_own_node_status() {
        let authorizer = authorizer();
        let id = node_identity("tokyo");
        let req = AuthorizationRequest::resource(
            "update",
            "",
            "v1",
            "nodes",
            Some("status"),
            None,
            Some("tokyo"),
        );
        let decision = authorizer.authorize(&id, &req).await;
        assert!(decision.allowed);
    }

    #[tokio::test]
    async fn node_cannot_update_other_node_status() {
        let authorizer = authorizer();
        let id = node_identity("tokyo");
        let req = AuthorizationRequest::resource(
            "update",
            "",
            "v1",
            "nodes",
            Some("status"),
            None,
            Some("osaka"),
        );
        let decision = authorizer.authorize(&id, &req).await;
        assert!(!decision.allowed);
    }

    // --- Lease access tests (username-based, no store needed) ---

    #[tokio::test]
    async fn node_can_renew_own_lease() {
        let authorizer = authorizer();
        let id = node_identity("tokyo");
        let req = AuthorizationRequest::resource(
            "update",
            "coordination.k8s.io",
            "v1",
            "leases",
            None,
            Some("kube-node-lease"),
            Some("tokyo"),
        );
        let decision = authorizer.authorize(&id, &req).await;
        assert!(decision.allowed);
    }

    #[tokio::test]
    async fn node_cannot_renew_other_lease() {
        let authorizer = authorizer();
        let id = node_identity("tokyo");
        let req = AuthorizationRequest::resource(
            "update",
            "coordination.k8s.io",
            "v1",
            "leases",
            None,
            Some("kube-node-lease"),
            Some("osaka"),
        );
        let decision = authorizer.authorize(&id, &req).await;
        assert!(!decision.allowed);
    }

    // --- Pod access tests (store-based) ---

    #[tokio::test]
    async fn node_can_update_pod_status_for_own_pod() {
        let mut store = seeded_store();
        store.add_pod("default", "my-pod", "tokyo");
        let authorizer = NodeAuthorizer::new(Some(Arc::new(store)));
        let id = node_identity("tokyo");
        let req = AuthorizationRequest::resource(
            "update",
            "",
            "v1",
            "pods",
            Some("status"),
            Some("default"),
            Some("my-pod"),
        );
        let decision = authorizer.authorize(&id, &req).await;
        assert!(decision.allowed);
    }

    #[tokio::test]
    async fn node_cannot_update_pod_status_for_other_node_pod() {
        let store = seeded_store();
        let authorizer = NodeAuthorizer::new(Some(Arc::new(store)));
        let id = node_identity("tokyo");
        let req = AuthorizationRequest::resource(
            "update",
            "",
            "v1",
            "pods",
            Some("status"),
            Some("default"),
            Some("other-pod"), // bound to osaka
        );
        let decision = authorizer.authorize(&id, &req).await;
        assert!(!decision.allowed);
    }

    #[tokio::test]
    async fn node_cannot_update_pod_status_for_nonexistent_pod() {
        let store = seeded_store();
        let authorizer = NodeAuthorizer::new(Some(Arc::new(store)));
        let id = node_identity("tokyo");
        let req = AuthorizationRequest::resource(
            "update",
            "",
            "v1",
            "pods",
            Some("status"),
            Some("default"),
            Some("ghost-pod"),
        );
        let decision = authorizer.authorize(&id, &req).await;
        assert!(!decision.allowed);
    }

    #[tokio::test]
    async fn node_cannot_update_pod_spec() {
        let store = seeded_store();
        let authorizer = NodeAuthorizer::new(Some(Arc::new(store)));
        let id = node_identity("tokyo");
        let req = AuthorizationRequest::resource(
            "update",
            "",
            "v1",
            "pods",
            None, // no subresource = spec update
            Some("default"),
            Some("my-pod"),
        );
        let decision = authorizer.authorize(&id, &req).await;
        assert!(!decision.allowed);
    }

    #[tokio::test]
    async fn node_can_list_pods_with_field_selector() {
        let store = seeded_store();
        let authorizer = NodeAuthorizer::new(Some(Arc::new(store)));
        let id = node_identity("tokyo");
        let req =
            AuthorizationRequest::resource("list", "", "v1", "pods", None, Some("default"), None)
                .with_field_selector(Some("spec.nodeName=tokyo".to_string()));
        let decision = authorizer.authorize(&id, &req).await;
        assert!(decision.allowed);
    }

    #[tokio::test]
    async fn node_can_list_pods_with_node_field_selector_among_other_terms() {
        let store = seeded_store();
        let authorizer = NodeAuthorizer::new(Some(Arc::new(store)));
        let id = node_identity("tokyo");
        let req =
            AuthorizationRequest::resource("list", "", "v1", "pods", None, Some("default"), None)
                .with_field_selector(Some(
                    "metadata.namespace=default,spec.nodeName=tokyo".to_string(),
                ));
        let decision = authorizer.authorize(&id, &req).await;
        assert!(decision.allowed);
    }

    #[tokio::test]
    async fn node_list_pods_without_field_selector_denied() {
        let store = seeded_store();
        let authorizer = NodeAuthorizer::new(Some(Arc::new(store)));
        let id = node_identity("tokyo");
        let req =
            AuthorizationRequest::resource("list", "", "v1", "pods", None, Some("default"), None);
        let decision = authorizer.authorize(&id, &req).await;
        // Without field selector, no opinion (falls through to RBAC/deny)
        assert!(!decision.allowed);
    }

    #[tokio::test]
    async fn node_authorizer_without_store_does_not_allow_pod_relationship_access() {
        let authorizer = authorizer();
        let id = node_identity("tokyo");

        let get_pod = AuthorizationRequest::resource(
            "get",
            "",
            "v1",
            "pods",
            None,
            Some("default"),
            Some("my-pod"),
        );
        assert!(!authorizer.authorize(&id, &get_pod).await.allowed);

        let list_pods =
            AuthorizationRequest::resource("list", "", "v1", "pods", None, Some("default"), None)
                .with_field_selector(Some("spec.nodeName=tokyo".to_string()));
        assert!(!authorizer.authorize(&id, &list_pods).await.allowed);

        let pod_status = AuthorizationRequest::resource(
            "update",
            "",
            "v1",
            "pods",
            Some("status"),
            Some("default"),
            Some("my-pod"),
        );
        assert!(!authorizer.authorize(&id, &pod_status).await.allowed);
    }

    #[tokio::test]
    async fn node_can_get_own_pod() {
        let store = seeded_store();
        let authorizer = NodeAuthorizer::new(Some(Arc::new(store)));
        let id = node_identity("tokyo");
        let req = AuthorizationRequest::resource(
            "get",
            "",
            "v1",
            "pods",
            None,
            Some("default"),
            Some("my-pod"),
        );
        let decision = authorizer.authorize(&id, &req).await;
        assert!(decision.allowed);
    }

    #[tokio::test]
    async fn node_cannot_get_other_node_pod() {
        let store = seeded_store();
        let authorizer = NodeAuthorizer::new(Some(Arc::new(store)));
        let id = node_identity("tokyo");
        let req = AuthorizationRequest::resource(
            "get",
            "",
            "v1",
            "pods",
            None,
            Some("default"),
            Some("other-pod"),
        );
        let decision = authorizer.authorize(&id, &req).await;
        assert!(!decision.allowed);
    }

    // --- Referenced object access tests ---

    #[tokio::test]
    async fn node_can_read_secret_referenced_by_own_pod() {
        let store = seeded_store();
        let authorizer = NodeAuthorizer::new(Some(Arc::new(store)));
        let id = node_identity("tokyo");
        let req = AuthorizationRequest::resource(
            "get",
            "",
            "v1",
            "secrets",
            None,
            Some("default"),
            Some("my-secret"),
        );
        let decision = authorizer.authorize(&id, &req).await;
        assert!(decision.allowed);
    }

    #[tokio::test]
    async fn node_cannot_read_secret_not_referenced_by_own_pod() {
        let store = seeded_store();
        let authorizer = NodeAuthorizer::new(Some(Arc::new(store)));
        let id = node_identity("tokyo");
        let req = AuthorizationRequest::resource(
            "get",
            "",
            "v1",
            "secrets",
            None,
            Some("default"),
            Some("other-secret"),
        );
        let decision = authorizer.authorize(&id, &req).await;
        assert!(!decision.allowed);
    }

    #[tokio::test]
    async fn node_can_read_configmap_referenced_by_own_pod() {
        let store = seeded_store();
        let authorizer = NodeAuthorizer::new(Some(Arc::new(store)));
        let id = node_identity("tokyo");
        let req = AuthorizationRequest::resource(
            "get",
            "",
            "v1",
            "configmaps",
            None,
            Some("default"),
            Some("my-config"),
        );
        let decision = authorizer.authorize(&id, &req).await;
        assert!(decision.allowed);
    }

    // --- Identity tests (no store needed) ---

    #[tokio::test]
    async fn non_node_identity_gets_no_opinion() {
        let authorizer = authorizer();
        let id = other_identity();
        let req =
            AuthorizationRequest::resource("get", "", "v1", "nodes", None, None, Some("tokyo"));
        let decision = authorizer.authorize(&id, &req).await;
        assert!(!decision.allowed && !decision.denied); // no opinion
    }

    #[tokio::test]
    async fn bootstrap_identity_gets_no_opinion() {
        let authorizer = authorizer();
        let id = bootstrap_identity();
        let req =
            AuthorizationRequest::resource("get", "", "v1", "nodes", None, None, Some("tokyo"));
        let decision = authorizer.authorize(&id, &req).await;
        assert!(!decision.allowed && !decision.denied); // no opinion
    }

    #[tokio::test]
    async fn node_without_system_nodes_group_gets_no_opinion() {
        let authorizer = authorizer();
        let id = AuthenticatedIdentity::client_cert(
            "system:node:tokyo".to_string(),
            vec!["some-other-group".to_string()],
        );
        let req =
            AuthorizationRequest::resource("get", "", "v1", "nodes", None, None, Some("tokyo"));
        let decision = authorizer.authorize(&id, &req).await;
        assert!(!decision.allowed && !decision.denied);
    }
}
