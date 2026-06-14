//! Immutable authenticated identity type used across all authenticators.
//!
//! Every authenticator (client cert, bootstrap token, ServiceAccount token,
//! extension-injected) produces an `AuthenticatedIdentity`. Authorizers consume
//! it without knowing which authenticator produced it.

/// Immutable identity for an authenticated (or anonymous) request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthenticatedIdentity {
    pub username: String,
    pub groups: Vec<String>,
    pub uid: Option<String>,
    pub extra: Vec<(String, String)>,
}

impl AuthenticatedIdentity {
    /// Build a client-certificate identity, appending `system:authenticated`.
    pub fn client_cert(username: String, mut groups: Vec<String>) -> Self {
        append_authenticated(&mut groups);
        Self {
            username,
            groups,
            uid: None,
            extra: Vec::new(),
        }
    }

    /// Build a bootstrap-token identity.
    ///
    /// Username: `system:bootstrap:<token-id>`.
    /// Groups: `system:bootstrappers`, `system:authenticated`, and any
    /// `auth-extra-groups` that pass Kubernetes validation.
    pub fn bootstrap(token_id: &str, auth_extra_groups: &[String]) -> Self {
        let mut groups = vec!["system:bootstrappers".to_string()];
        // Phase 2B: invalid groups are now rejected at validation time
        // (validate_bootstrap_token). All groups here are pre-validated.
        for g in auth_extra_groups {
            debug_assert!(
                g.starts_with("system:bootstrappers:"),
                "invalid extra group should have been rejected at token validation"
            );
            groups.push(g.clone());
        }
        append_authenticated(&mut groups);
        Self {
            username: format!("system:bootstrap:{token_id}"),
            groups,
            uid: None,
            extra: Vec::new(),
        }
    }

    /// Build a ServiceAccount token identity (groups already shaped by caller).
    pub fn service_account(username: String, mut groups: Vec<String>, uid: Option<String>) -> Self {
        append_authenticated(&mut groups);
        Self {
            username,
            groups,
            uid,
            extra: Vec::new(),
        }
    }

    /// Anonymous identity: `system:anonymous` / `system:unauthenticated`.
    pub fn anonymous() -> Self {
        Self {
            username: "system:anonymous".to_string(),
            groups: vec!["system:unauthenticated".to_string()],
            uid: None,
            extra: Vec::new(),
        }
    }

    /// Build an OIDC-authenticated identity with groups.
    pub fn oidc(username: String, mut groups: Vec<String>, uid: Option<String>) -> Self {
        append_authenticated(&mut groups);
        Self {
            username,
            groups,
            uid,
            extra: Vec::new(),
        }
    }

    /// Build a webhook-authenticated identity with groups and extra fields.
    pub fn webhook(
        username: String,
        mut groups: Vec<String>,
        uid: Option<String>,
        extra: Vec<(String, String)>,
    ) -> Self {
        append_authenticated(&mut groups);
        Self {
            username,
            groups,
            uid,
            extra,
        }
    }

    /// Build an admin identity for tests (system:masters group).
    #[cfg(test)]
    pub fn admin(username: impl Into<String>) -> Self {
        Self::client_cert(username.into(), vec!["system:masters".to_string()])
    }

    /// Check if this identity has the `system:masters` group.
    pub fn is_admin(&self) -> bool {
        self.groups.contains(&"system:masters".to_string())
    }
}

/// Append `system:authenticated` exactly once.
fn append_authenticated(groups: &mut Vec<String>) {
    if !groups.contains(&"system:authenticated".to_string()) {
        groups.push("system:authenticated".to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_cert_identity_adds_system_authenticated() {
        let id = AuthenticatedIdentity::client_cert(
            "klights-admin".to_string(),
            vec!["system:masters".to_string()],
        );
        assert_eq!(id.username, "klights-admin");
        assert!(id.groups.contains(&"system:masters".to_string()));
        assert!(id.groups.contains(&"system:authenticated".to_string()));
    }

    #[test]
    fn client_cert_identity_no_duplicate_authenticated() {
        let id = AuthenticatedIdentity::client_cert(
            "test".to_string(),
            vec!["system:authenticated".to_string()],
        );
        let count = id
            .groups
            .iter()
            .filter(|g| *g == "system:authenticated")
            .count();
        assert_eq!(count, 1);
    }

    #[test]
    fn bootstrap_identity_has_correct_groups() {
        let id = AuthenticatedIdentity::bootstrap("abcdef", &[]);
        assert_eq!(id.username, "system:bootstrap:abcdef");
        assert!(id.groups.contains(&"system:bootstrappers".to_string()));
        assert!(id.groups.contains(&"system:authenticated".to_string()));
    }

    #[test]
    fn bootstrap_identity_accepts_valid_extra_groups() {
        // Phase 2B: only groups with system:bootstrappers: prefix are valid.
        let id = AuthenticatedIdentity::bootstrap(
            "abcdef",
            &[
                "system:bootstrappers:nodes".to_string(),
                "system:bootstrappers:workers".to_string(),
            ],
        );
        assert!(
            id.groups
                .contains(&"system:bootstrappers:nodes".to_string())
        );
        assert!(
            id.groups
                .contains(&"system:bootstrappers:workers".to_string())
        );
        assert!(id.groups.contains(&"system:bootstrappers".to_string()));
        assert!(id.groups.contains(&"system:authenticated".to_string()));
    }

    #[test]
    fn service_account_identity_preserves_groups() {
        let id = AuthenticatedIdentity::service_account(
            "system:serviceaccount:default:my-sa".to_string(),
            vec![
                "system:serviceaccounts".to_string(),
                "system:serviceaccounts:default".to_string(),
                "system:authenticated".to_string(),
            ],
            None,
        );
        assert_eq!(id.username, "system:serviceaccount:default:my-sa");
        assert!(id.groups.contains(&"system:serviceaccounts".to_string()));
        assert!(
            id.groups
                .contains(&"system:serviceaccounts:default".to_string())
        );
        assert!(id.groups.contains(&"system:authenticated".to_string()));
        let count = id
            .groups
            .iter()
            .filter(|g| *g == "system:authenticated")
            .count();
        assert_eq!(count, 1, "no duplicate system:authenticated");
    }

    #[test]
    fn service_account_identity_stores_uid_when_present() {
        let id = AuthenticatedIdentity::service_account(
            "system:serviceaccount:default:my-sa".to_string(),
            vec![],
            Some("abc-123-uid".to_string()),
        );
        assert_eq!(id.uid.as_deref(), Some("abc-123-uid"));
    }

    #[test]
    fn anonymous_identity_is_unauthenticated() {
        let id = AuthenticatedIdentity::anonymous();
        assert_eq!(id.username, "system:anonymous");
        assert_eq!(id.groups, vec!["system:unauthenticated".to_string()]);
        assert!(!id.groups.contains(&"system:authenticated".to_string()));
    }

    #[test]
    fn is_admin_true_for_system_masters() {
        let id = AuthenticatedIdentity::client_cert(
            "admin".to_string(),
            vec!["system:masters".to_string()],
        );
        assert!(id.is_admin());
    }

    #[test]
    fn is_admin_false_for_regular_user() {
        let id = AuthenticatedIdentity::client_cert("user".to_string(), vec![]);
        assert!(!id.is_admin());
    }

    #[test]
    fn client_cert_identity_with_multiple_orgs() {
        let id = AuthenticatedIdentity::client_cert(
            "multi-org-user".to_string(),
            vec![
                "system:masters".to_string(),
                "developers".to_string(),
                "system:authenticated".to_string(),
            ],
        );
        assert_eq!(id.username, "multi-org-user");
        assert!(
            id.groups.contains(&"system:masters".to_string()),
            "should have system:masters"
        );
        assert!(
            id.groups.contains(&"developers".to_string()),
            "should have developers"
        );
        let auth_count = id
            .groups
            .iter()
            .filter(|g| *g == "system:authenticated")
            .count();
        assert_eq!(auth_count, 1, "system:authenticated appears exactly once");
        assert_eq!(id.groups.len(), 3, "should have exactly 3 groups total");
    }
}
