//! RBAC authorizer: evaluates RBAC rules from a policy store.
//!
//! Uses `RbacRuleEvaluator` for pure rule matching and `RbacPolicyStore`
//! for loading bindings. No datastore/network/filesystem dependency.

use crate::auth::authorizer::{AuthorizationDecision, Authorizer};
use crate::auth::identity::AuthenticatedIdentity;
use crate::auth::rbac_policy_store::RbacPolicyStore;
use crate::auth::rbac_rule_evaluator::{rule_matches, subject_matches};
use crate::auth::request_attributes::{AuthorizationRequest, RequestKind};
use async_trait::async_trait;
use std::sync::Arc;

/// RBAC authorizer: checks the request against all matching RBAC bindings.
pub struct RbacAuthorizer {
    store: Arc<dyn RbacPolicyStore>,
}

impl RbacAuthorizer {
    pub fn new(store: Arc<dyn RbacPolicyStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl Authorizer for RbacAuthorizer {
    async fn authorize(
        &self,
        identity: &AuthenticatedIdentity,
        request: &AuthorizationRequest,
    ) -> AuthorizationDecision {
        let namespace = request.namespace.as_deref();
        let bindings = self.store.list_bindings_for_namespace(namespace).await;

        for binding in &bindings {
            // Check if any subject in this binding matches the identity
            let subject_match = binding
                .subjects
                .iter()
                .any(|s| subject_matches(s, &identity.username, &identity.groups));
            if !subject_match {
                continue;
            }

            // Check if any rule in this binding matches the request
            let api_group = request.api_group.as_deref();
            let resource = request.resource.as_deref();
            let subresource = request.subresource.as_deref();
            let name = request.name.as_deref();
            let non_resource_url = request.non_resource_url.as_deref();

            for rule in &binding.rules {
                let field_selector = request.field_selector.as_deref();
                if rule_matches(
                    rule,
                    crate::auth::rbac_rule_evaluator::RuleMatchRequest {
                        verb: &request.verb,
                        api_group,
                        resource,
                        subresource,
                        resource_name: name,
                        non_resource_url,
                        field_selector,
                    },
                ) {
                    // nonResourceURLs are cluster-scoped: only ClusterRoleBindings
                    // (namespace == None) grant them.
                    if request.kind == RequestKind::NonResource && binding.namespace.is_some() {
                        continue;
                    }
                    return AuthorizationDecision::allow("RBAC: allowed by rule");
                }
            }
        }

        AuthorizationDecision::no_opinion()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::rbac_policy_store::{InMemoryRbacPolicyStore, ResolvedBinding};
    use crate::auth::rbac_rule_evaluator::{PolicyRule, Subject, SubjectKind};

    fn bootstrap_store() -> InMemoryRbacPolicyStore {
        InMemoryRbacPolicyStore::new(vec![
            // system:node-bootstrapper -> worker bootstrap tokens only.
            ResolvedBinding {
                subjects: vec![Subject {
                    kind: SubjectKind::Group,
                    name: "system:bootstrappers:klights:worker".to_string(),
                    namespace: None,
                }],
                rules: vec![
                    PolicyRule {
                        verbs: vec!["create".to_string()],
                        api_groups: vec!["certificates.k8s.io".to_string()],
                        resources: vec!["certificatesigningrequests".to_string()],
                        resource_names: vec![],
                        non_resource_urls: vec![],
                    },
                    PolicyRule {
                        verbs: vec!["get".to_string(), "list".to_string(), "watch".to_string()],
                        api_groups: vec!["certificates.k8s.io".to_string()],
                        resources: vec!["certificatesigningrequests".to_string()],
                        resource_names: vec![],
                        non_resource_urls: vec![],
                    },
                ],
                namespace: None,
            },
            // Discovery: authenticated users can read API discovery
            ResolvedBinding {
                subjects: vec![Subject {
                    kind: SubjectKind::Group,
                    name: "system:authenticated".to_string(),
                    namespace: None,
                }],
                rules: vec![PolicyRule {
                    verbs: vec!["get".to_string()],
                    api_groups: vec!["".to_string(), "apis".to_string()],
                    resources: vec![],
                    resource_names: vec![],
                    non_resource_urls: vec![
                        "/api".to_string(),
                        "/api/*".to_string(),
                        "/apis".to_string(),
                        "/apis/*".to_string(),
                        "/healthz".to_string(),
                        "/livez".to_string(),
                        "/openapi".to_string(),
                        "/openapi/*".to_string(),
                        "/version".to_string(),
                        "/version/".to_string(),
                    ],
                }],
                namespace: None,
            },
        ])
    }

    fn bootstrap_identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::bootstrap(
            "abcdef",
            &["system:bootstrappers:klights:worker".to_string()],
        )
    }

    fn user_identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::client_cert("user".to_string(), vec![])
    }

    fn default_sa_identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::service_account(
            "system:serviceaccount:default:default".to_string(),
            vec![
                "system:serviceaccounts".to_string(),
                "system:serviceaccounts:default".to_string(),
            ],
            Some("sa-uid-123".to_string()),
        )
    }

    fn discovery_only_store() -> InMemoryRbacPolicyStore {
        InMemoryRbacPolicyStore::new(vec![ResolvedBinding {
            subjects: vec![Subject {
                kind: SubjectKind::Group,
                name: "system:authenticated".to_string(),
                namespace: None,
            }],
            rules: vec![PolicyRule {
                verbs: vec!["get".to_string()],
                api_groups: vec!["".to_string()],
                resources: vec![],
                resource_names: vec![],
                non_resource_urls: vec![
                    "/api".to_string(),
                    "/api/*".to_string(),
                    "/apis".to_string(),
                    "/apis/*".to_string(),
                    "/healthz".to_string(),
                    "/livez".to_string(),
                    "/openapi".to_string(),
                    "/openapi/*".to_string(),
                    "/version".to_string(),
                    "/version/".to_string(),
                ],
            }],
            namespace: None,
        }])
    }

    #[tokio::test]
    async fn bootstrap_can_create_csr() {
        let store = Arc::new(bootstrap_store());
        let authorizer = RbacAuthorizer::new(store);
        let id = bootstrap_identity();
        let req = AuthorizationRequest::resource(
            "create",
            "certificates.k8s.io",
            "v1",
            "certificatesigningrequests",
            None,
            None,
            None,
        );
        let decision = authorizer.authorize(&id, &req).await;
        assert!(decision.allowed);
    }

    #[tokio::test]
    async fn bootstrap_can_list_csr() {
        let store = Arc::new(bootstrap_store());
        let authorizer = RbacAuthorizer::new(store);
        let id = bootstrap_identity();
        let req = AuthorizationRequest::resource(
            "list",
            "certificates.k8s.io",
            "v1",
            "certificatesigningrequests",
            None,
            None,
            None,
        );
        let decision = authorizer.authorize(&id, &req).await;
        assert!(decision.allowed);
    }

    #[tokio::test]
    async fn bootstrap_cannot_update_csr_status() {
        let store = Arc::new(bootstrap_store());
        let authorizer = RbacAuthorizer::new(store);
        let id = bootstrap_identity();
        let req = AuthorizationRequest::resource(
            "update",
            "certificates.k8s.io",
            "v1",
            "certificatesigningrequests",
            Some("status"),
            None,
            None,
        );
        let decision = authorizer.authorize(&id, &req).await;
        assert!(!decision.allowed);
    }

    #[tokio::test]
    async fn bootstrap_cannot_renew_lease() {
        let store = Arc::new(bootstrap_store());
        let authorizer = RbacAuthorizer::new(store);
        let id = bootstrap_identity();
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
        assert!(!decision.allowed);
    }

    #[tokio::test]
    async fn bootstrap_cannot_update_node_status() {
        let store = Arc::new(bootstrap_store());
        let authorizer = RbacAuthorizer::new(store);
        let id = bootstrap_identity();
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
        assert!(!decision.allowed);
    }

    #[tokio::test]
    async fn bootstrap_cannot_update_pod_status() {
        let store = Arc::new(bootstrap_store());
        let authorizer = RbacAuthorizer::new(store);
        let id = bootstrap_identity();
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
        assert!(!decision.allowed);
    }

    #[tokio::test]
    async fn bootstrap_cannot_read_secrets() {
        let store = Arc::new(bootstrap_store());
        let authorizer = RbacAuthorizer::new(store);
        let id = bootstrap_identity();
        for name in [
            "worker-bootstrap-token",
            "controlplane-bootstrap-token",
            "my-secret",
        ] {
            let req = AuthorizationRequest::resource(
                "get",
                "",
                "v1",
                "secrets",
                None,
                Some("kube-system"),
                Some(name),
            );
            let decision = authorizer.authorize(&id, &req).await;
            assert!(!decision.allowed, "bootstrap identity must not read {name}");
        }
    }

    #[tokio::test]
    async fn bootstrap_cannot_list_pods() {
        let store = Arc::new(bootstrap_store());
        let authorizer = RbacAuthorizer::new(store);
        let id = bootstrap_identity();
        let req =
            AuthorizationRequest::resource("list", "", "v1", "pods", None, Some("default"), None);
        let decision = authorizer.authorize(&id, &req).await;
        assert!(!decision.allowed);
    }

    #[tokio::test]
    async fn authenticated_user_can_discover_apis() {
        let store = Arc::new(bootstrap_store());
        let authorizer = RbacAuthorizer::new(store);
        let id = user_identity();
        let req = AuthorizationRequest::non_resource("get", "/apis");
        let decision = authorizer.authorize(&id, &req).await;
        assert!(decision.allowed);
    }

    #[tokio::test]
    async fn empty_store_denies_everything() {
        let store = Arc::new(InMemoryRbacPolicyStore::empty());
        let authorizer = RbacAuthorizer::new(store);
        let id = user_identity();
        let req = AuthorizationRequest::resource("get", "", "v1", "pods", None, None, None);
        let decision = authorizer.authorize(&id, &req).await;
        assert!(!decision.allowed);
        assert!(!decision.denied); // no opinion
    }

    // === Task 2.2: Default SA Has No Workload Permissions ===

    #[tokio::test]
    async fn default_sa_can_access_discovery_endpoints() {
        let store = Arc::new(discovery_only_store());
        let authorizer = RbacAuthorizer::new(store);
        let id = default_sa_identity();

        for url in &["/api", "/apis", "/healthz", "/version"] {
            let req = AuthorizationRequest::non_resource("get", url);
            let decision = authorizer.authorize(&id, &req).await;
            assert!(decision.allowed, "default SA should access {url}");
        }
    }

    #[tokio::test]
    async fn default_sa_cannot_list_pods_without_rolebinding() {
        let store = Arc::new(discovery_only_store());
        let authorizer = RbacAuthorizer::new(store);
        let id = default_sa_identity();
        let req =
            AuthorizationRequest::resource("list", "", "v1", "pods", None, Some("default"), None);
        let decision = authorizer.authorize(&id, &req).await;
        assert!(!decision.allowed);
    }

    #[tokio::test]
    async fn default_sa_cannot_read_secrets_without_rolebinding() {
        let store = Arc::new(discovery_only_store());
        let authorizer = RbacAuthorizer::new(store);
        let id = default_sa_identity();
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
        assert!(!decision.allowed);
    }

    #[tokio::test]
    async fn default_sa_cannot_update_pod_status() {
        let store = Arc::new(discovery_only_store());
        let authorizer = RbacAuthorizer::new(store);
        let id = default_sa_identity();
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
        assert!(!decision.allowed);
    }

    #[tokio::test]
    async fn rolebinding_to_default_sa_grants_namespace_permission() {
        // Seeded discovery + a RoleBinding granting pod list to default SA in default ns
        let store = InMemoryRbacPolicyStore::new(vec![
            // Discovery for authenticated users (cluster-scoped)
            ResolvedBinding {
                subjects: vec![Subject {
                    kind: SubjectKind::Group,
                    name: "system:authenticated".to_string(),
                    namespace: None,
                }],
                rules: vec![PolicyRule {
                    verbs: vec!["get".to_string()],
                    api_groups: vec!["".to_string()],
                    resources: vec![],
                    resource_names: vec![],
                    non_resource_urls: vec![
                        "/api".to_string(),
                        "/api/*".to_string(),
                        "/apis".to_string(),
                        "/apis/*".to_string(),
                        "/healthz".to_string(),
                        "/livez".to_string(),
                        "/openapi".to_string(),
                        "/openapi/*".to_string(),
                        "/version".to_string(),
                        "/version/".to_string(),
                    ],
                }],
                namespace: None,
            },
            // RoleBinding in default ns: default SA → pod reader
            ResolvedBinding {
                subjects: vec![Subject {
                    kind: SubjectKind::ServiceAccount,
                    name: "default".to_string(),
                    namespace: Some("default".to_string()),
                }],
                rules: vec![PolicyRule {
                    verbs: vec!["get".to_string(), "list".to_string()],
                    api_groups: vec!["".to_string()],
                    resources: vec!["pods".to_string()],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                }],
                namespace: Some("default".to_string()),
            },
        ]);
        let authorizer = RbacAuthorizer::new(Arc::new(store));
        let id = default_sa_identity();

        // Can list pods in default namespace
        let req =
            AuthorizationRequest::resource("list", "", "v1", "pods", None, Some("default"), None);
        let decision = authorizer.authorize(&id, &req).await;
        assert!(decision.allowed, "should be allowed after RoleBinding");

        // Cannot list pods in other namespace (RoleBinding is namespace-scoped)
        let req = AuthorizationRequest::resource(
            "list",
            "",
            "v1",
            "pods",
            None,
            Some("kube-system"),
            None,
        );
        let decision = authorizer.authorize(&id, &req).await;
        assert!(!decision.allowed, "should be denied in other namespace");
    }

    #[tokio::test]
    async fn clusterrolebinding_to_serviceaccounts_grants_cluster_wide() {
        let store = InMemoryRbacPolicyStore::new(vec![
            // Discovery
            ResolvedBinding {
                subjects: vec![Subject {
                    kind: SubjectKind::Group,
                    name: "system:authenticated".to_string(),
                    namespace: None,
                }],
                rules: vec![PolicyRule {
                    verbs: vec!["get".to_string()],
                    api_groups: vec!["".to_string()],
                    resources: vec![],
                    resource_names: vec![],
                    non_resource_urls: vec![
                        "/api".to_string(),
                        "/api/*".to_string(),
                        "/apis".to_string(),
                        "/apis/*".to_string(),
                        "/healthz".to_string(),
                        "/livez".to_string(),
                        "/version".to_string(),
                    ],
                }],
                namespace: None,
            },
            // ClusterRoleBinding: system:serviceaccounts group → read pods cluster-wide
            ResolvedBinding {
                subjects: vec![Subject {
                    kind: SubjectKind::Group,
                    name: "system:serviceaccounts".to_string(),
                    namespace: None,
                }],
                rules: vec![PolicyRule {
                    verbs: vec!["get".to_string(), "list".to_string()],
                    api_groups: vec!["".to_string()],
                    resources: vec!["pods".to_string()],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                }],
                namespace: None,
            },
        ]);
        let authorizer = RbacAuthorizer::new(Arc::new(store));
        let id = default_sa_identity();

        // Can list pods in any namespace (cluster-wide grant via group)
        assert!(
            authorizer
                .authorize(
                    &id,
                    &AuthorizationRequest::resource(
                        "list",
                        "",
                        "v1",
                        "pods",
                        None,
                        Some("default"),
                        None
                    )
                )
                .await
                .allowed
        );
        assert!(
            authorizer
                .authorize(
                    &id,
                    &AuthorizationRequest::resource(
                        "list",
                        "",
                        "v1",
                        "pods",
                        None,
                        Some("kube-system"),
                        None
                    )
                )
                .await
                .allowed
        );
    }

    #[tokio::test]
    async fn clusterrolebinding_to_namespace_sa_group_scoped_to_that_namespace() {
        let kube_system_sa = AuthenticatedIdentity::service_account(
            "system:serviceaccount:kube-system:coredns".to_string(),
            vec![
                "system:serviceaccounts".to_string(),
                "system:serviceaccounts:kube-system".to_string(),
            ],
            None,
        );

        let store = InMemoryRbacPolicyStore::new(vec![
            // ClusterRoleBinding: system:serviceaccounts:kube-system → read secrets
            ResolvedBinding {
                subjects: vec![Subject {
                    kind: SubjectKind::Group,
                    name: "system:serviceaccounts:kube-system".to_string(),
                    namespace: None,
                }],
                rules: vec![PolicyRule {
                    verbs: vec!["get".to_string()],
                    api_groups: vec!["".to_string()],
                    resources: vec!["secrets".to_string()],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                }],
                namespace: None,
            },
        ]);
        let authorizer = RbacAuthorizer::new(Arc::new(store));

        // kube-system SA can read secrets in kube-system (via namespace group)
        assert!(
            authorizer
                .authorize(
                    &kube_system_sa,
                    &AuthorizationRequest::resource(
                        "get",
                        "",
                        "v1",
                        "secrets",
                        None,
                        Some("kube-system"),
                        Some("my-secret")
                    )
                )
                .await
                .allowed
        );

        // default SA cannot (not in the kube-system group)
        let default_sa = default_sa_identity();
        assert!(
            !authorizer
                .authorize(
                    &default_sa,
                    &AuthorizationRequest::resource(
                        "get",
                        "",
                        "v1",
                        "secrets",
                        None,
                        Some("kube-system"),
                        Some("my-secret")
                    )
                )
                .await
                .allowed
        );
    }

    #[tokio::test]
    async fn rolebinding_sa_subject_defaults_to_binding_namespace() {
        // Phase 2B: When a RoleBinding has a ServiceAccount subject without
        // namespace, the store should default it to the binding's namespace.
        let sa_identity = AuthenticatedIdentity::service_account(
            "system:serviceaccount:default:my-sa".to_string(),
            vec![
                "system:serviceaccounts".to_string(),
                "system:serviceaccounts:default".to_string(),
            ],
            None,
        );

        // RoleBinding in "default" namespace with SA subject "my-sa" (no namespace)
        // The store should default namespace to "default"
        let store = InMemoryRbacPolicyStore::new(vec![ResolvedBinding {
            subjects: vec![Subject {
                kind: SubjectKind::ServiceAccount,
                name: "my-sa".to_string(),
                namespace: None, // omitted — should be defaulted by store
            }],
            rules: vec![PolicyRule {
                verbs: vec!["get".to_string()],
                api_groups: vec!["".to_string()],
                resources: vec!["configmaps".to_string()],
                resource_names: vec![],
                non_resource_urls: vec![],
            }],
            namespace: Some("default".to_string()),
        }]);
        let authorizer = RbacAuthorizer::new(Arc::new(store));

        // SA with exact username should match (namespace defaulted)
        assert!(
            authorizer
                .authorize(
                    &sa_identity,
                    &AuthorizationRequest::resource(
                        "get",
                        "",
                        "v1",
                        "configmaps",
                        None,
                        Some("default"),
                        Some("my-config")
                    )
                )
                .await
                .allowed
        );
    }

    #[tokio::test]
    async fn clusterrolebinding_sa_subject_not_defaulted() {
        // Phase 2B: ClusterRoleBindings do NOT default SA subject namespaces.
        // A ServiceAccount subject without namespace in a ClusterRoleBinding
        // cannot match via exact username (namespace required).
        let sa_identity = AuthenticatedIdentity::service_account(
            "system:serviceaccount:default:my-sa".to_string(),
            vec![
                "system:serviceaccounts".to_string(),
                "system:serviceaccounts:default".to_string(),
            ],
            None,
        );

        // ClusterRoleBinding with SA subject "my-sa" (no namespace)
        // The namespace should NOT be defaulted — stays None
        let store = InMemoryRbacPolicyStore::new(vec![ResolvedBinding {
            subjects: vec![Subject {
                kind: SubjectKind::ServiceAccount,
                name: "my-sa".to_string(),
                namespace: None, // omitted in ClusterRoleBinding — stays None
            }],
            rules: vec![PolicyRule {
                verbs: vec!["get".to_string()],
                api_groups: vec!["".to_string()],
                resources: vec!["pods".to_string()],
                resource_names: vec![],
                non_resource_urls: vec![],
            }],
            namespace: None, // ClusterRoleBinding
        }]);
        let authorizer = RbacAuthorizer::new(Arc::new(store));

        // Exact SA username match should fail because namespace is None
        // (subject_matches requires namespace for SA exact match)
        // And the subject kind is ServiceAccount, not Group, so the
        // system:serviceaccounts group check doesn't apply here.
        assert!(
            !authorizer
                .authorize(
                    &sa_identity,
                    &AuthorizationRequest::resource(
                        "get",
                        "",
                        "v1",
                        "pods",
                        None,
                        Some("default"),
                        Some("my-pod")
                    )
                )
                .await
                .allowed
        );
    }
}
