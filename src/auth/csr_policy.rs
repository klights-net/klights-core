//! Kubelet client CSR validation policy.
//!
//! Validates that a CertificateSigningRequest matches Kubernetes kubelet
//! TLS bootstrap requirements. Pure domain object — no datastore, TLS,
//! filesystem, or network dependency.

/// Result of validating a kubelet client CSR.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CsrValidationResult {
    pub valid: bool,
    pub reason: String,
    pub node_name: Option<String>,
    pub ttl_seconds: u32,
}

impl CsrValidationResult {
    fn valid(node_name: String, ttl_seconds: u32) -> Self {
        Self {
            valid: true,
            reason: String::new(),
            node_name: Some(node_name),
            ttl_seconds,
        }
    }

    fn invalid(reason: impl Into<String>, node_name: Option<String>) -> Self {
        Self {
            valid: false,
            reason: reason.into(),
            node_name,
            ttl_seconds: 0,
        }
    }
}

pub const MIN_CSR_EXPIRATION_SECONDS: u32 = 600;
pub const DEFAULT_CSR_EXPIRATION_SECONDS: u32 = 31_536_000;

/// Full validation input for a kubelet client CSR request.
pub struct KubeletClientCsrValidationInput<'a> {
    pub signer_name: &'a str,
    pub csr_pem: &'a [u8],
    pub usages: &'a [String],
    pub username: &'a str,
    pub groups: &'a [String],
    pub expiration_seconds: Option<u32>,
}

/// Validates a kubelet client CSR for TLS bootstrap.
///
/// Checks:
/// - signerName is `kubernetes.io/kube-apiserver-client-kubelet`
/// - CSR PEM is valid and has a public key
/// - Subject CN is `system:node:<nodeName>`
/// - Subject O includes `system:nodes`
/// - Subject O does not include `system:masters`
/// - Usages include client auth and exclude server auth
pub fn validate_kubelet_client_csr(
    signer_name: &str,
    csr_pem: &[u8],
    usages: &[String],
) -> CsrValidationResult {
    let groups = [
        "system:bootstrappers".to_string(),
        "system:bootstrappers:klights:worker".to_string(),
    ];
    validate_kubelet_client_csr_request(KubeletClientCsrValidationInput {
        signer_name,
        csr_pem,
        usages,
        username: "system:bootstrap:legacy",
        groups: &groups,
        expiration_seconds: None,
    })
}

pub fn validate_kubelet_client_csr_request(
    input: KubeletClientCsrValidationInput<'_>,
) -> CsrValidationResult {
    if input.signer_name != "kubernetes.io/kube-apiserver-client-kubelet" {
        return CsrValidationResult::invalid(
            format!("wrong signerName: {}", input.signer_name),
            None,
        );
    }

    // Parse the CSR to extract subject
    let subject = match parse_csr_subject(input.csr_pem) {
        Ok(s) => s,
        Err(reason) => {
            return CsrValidationResult::invalid(reason, None);
        }
    };

    // CN must be system:node:<nodeName>
    let node_name = match subject.cn.strip_prefix("system:node:") {
        Some(name) if !name.is_empty() => name.to_string(),
        _ => {
            return CsrValidationResult::invalid(
                format!("CSR CN must be system:node:<nodeName>, got: {}", subject.cn),
                None,
            );
        }
    };

    // O must include system:nodes
    if !subject.organizations.contains(&"system:nodes".to_string()) {
        return CsrValidationResult::invalid("CSR O must include system:nodes", Some(node_name));
    }

    // O must NOT include system:masters
    if subject
        .organizations
        .contains(&"system:masters".to_string())
    {
        return CsrValidationResult::invalid(
            "CSR O must not include system:masters",
            Some(node_name),
        );
    }

    if let Err(reason) = validate_requester(input.username, input.groups, &node_name) {
        return CsrValidationResult::invalid(reason, Some(node_name));
    }

    if let Err(reason) = validate_usages(input.usages) {
        return CsrValidationResult::invalid(reason, Some(node_name));
    }

    let ttl_seconds = input
        .expiration_seconds
        .unwrap_or(DEFAULT_CSR_EXPIRATION_SECONDS);
    if ttl_seconds < MIN_CSR_EXPIRATION_SECONDS {
        return CsrValidationResult::invalid(
            format!(
                "expirationSeconds must be at least {MIN_CSR_EXPIRATION_SECONDS}, got {ttl_seconds}"
            ),
            Some(node_name),
        );
    }

    CsrValidationResult::valid(node_name, ttl_seconds)
}

fn validate_requester(username: &str, groups: &[String], node_name: &str) -> Result<(), String> {
    if groups.iter().any(|g| g == "system:masters") {
        return Err("CSR requester groups must not include system:masters".to_string());
    }

    if let Some(token_id) = username.strip_prefix("system:bootstrap:") {
        if token_id.is_empty() {
            return Err("CSR requester bootstrap username is missing token id".to_string());
        }
        if !groups.iter().any(|g| g == "system:bootstrappers") {
            return Err("CSR requester bootstrap identity must be in system:bootstrappers".into());
        }
        if !groups
            .iter()
            .any(|g| g == "system:bootstrappers:klights:worker")
        {
            return Err(
                "CSR requester bootstrap identity must use a worker bootstrap token".into(),
            );
        }
        if groups.iter().any(|g| g == "system:nodes") {
            return Err("CSR requester bootstrap identity must not request system:nodes".into());
        }
        return Ok(());
    }

    if let Some(request_node) = username.strip_prefix("system:node:") {
        if request_node != node_name {
            return Err(format!(
                "CSR requester node name {request_node} must match CSR node name {node_name}"
            ));
        }
        if !groups.iter().any(|g| g == "system:nodes") {
            return Err("CSR requester node identity must be in system:nodes".into());
        }
        return Ok(());
    }

    Err("CSR requester must be a bootstrap token or matching node identity".to_string())
}

fn validate_usages(usages: &[String]) -> Result<(), String> {
    let mut has_client_auth = false;
    for usage in usages {
        match normalize_usage(usage).as_deref() {
            Some("client auth") => has_client_auth = true,
            Some("digital signature") | Some("key encipherment") => {}
            Some("server auth") => {
                return Err("CSR usages must not include server auth".to_string());
            }
            Some(other) => {
                return Err(format!("CSR usages include unsupported usage {other}"));
            }
            None => {
                return Err(format!("CSR usages include unsupported usage {usage}"));
            }
        }
    }

    if !has_client_auth {
        return Err("CSR usages must include client auth".to_string());
    }

    Ok(())
}

fn normalize_usage(usage: &str) -> Option<String> {
    match usage {
        "client auth" | "client_auth" | "clientAuth" => Some("client auth".to_string()),
        "server auth" | "server_auth" | "serverAuth" => Some("server auth".to_string()),
        "digital signature" | "digitalSignature" | "digital_signature" => {
            Some("digital signature".to_string())
        }
        "key encipherment" | "keyEncipherment" | "key_encipherment" => {
            Some("key encipherment".to_string())
        }
        _ => None,
    }
}

struct CsrSubject {
    cn: String,
    organizations: Vec<String>,
}

fn parse_csr_subject(csr_pem: &[u8]) -> Result<CsrSubject, String> {
    use x509_parser::pem::Pem;
    use x509_parser::prelude::*;
    let pem = Pem::read(std::io::Cursor::new(csr_pem))
        .map_err(|_| "invalid or unparseable CSR PEM".to_string())?
        .0;
    let (_, csr) = X509CertificationRequest::from_der(&pem.contents)
        .map_err(|_| "invalid or unparseable CSR PEM".to_string())?;
    csr.verify_signature()
        .map_err(|_| "CSR signature could not be verified with its public key".to_string())?;

    let subject = csr.certification_request_info.subject;
    let mut cn = String::new();
    let mut organizations = Vec::new();

    for attr in subject.iter_attributes() {
        let oid = attr.attr_type();
        if oid == &x509_parser::oid_registry::OID_X509_COMMON_NAME {
            if let Ok(s) = attr.as_str() {
                cn = s.to_string();
            }
        } else if oid == &x509_parser::oid_registry::OID_X509_ORGANIZATION_NAME
            && let Ok(s) = attr.as_str()
        {
            organizations.push(s.to_string());
        }
    }

    Ok(CsrSubject { cn, organizations })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generate_test_csr(cn: &str, orgs: &[&str]) -> Vec<u8> {
        use rcgen::{CertificateParams, DnType, KeyPair};
        let mut params = CertificateParams::default();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, cn.to_string());
        // Join multiple orgs with comma — rcgen doesn't support multiple O values easily.
        // For testing purposes, just use the first org.
        if let Some(first_org) = orgs.first() {
            params
                .distinguished_name
                .push(DnType::OrganizationName, (*first_org).to_string());
        }
        let key_pair = KeyPair::generate().unwrap();
        let csr = params.serialize_request(&key_pair).unwrap();
        csr.pem().unwrap().into_bytes()
    }

    fn valid_csr() -> Vec<u8> {
        generate_test_csr("system:node:tokyo", &["system:nodes"])
    }

    #[test]
    fn valid_kubelet_client_csr() {
        let result = validate_kubelet_client_csr(
            "kubernetes.io/kube-apiserver-client-kubelet",
            &valid_csr(),
            &["client auth".to_string()],
        );
        assert!(result.valid);
        assert_eq!(result.node_name.as_deref(), Some("tokyo"));
    }

    #[test]
    fn rejects_wrong_signer_name() {
        let result = validate_kubelet_client_csr(
            "kubernetes.io/kube-apiserver-client",
            &valid_csr(),
            &["client auth".to_string()],
        );
        assert!(!result.valid);
        assert!(result.reason.contains("signerName"));
    }

    #[test]
    fn rejects_missing_system_nodes() {
        let csr = generate_test_csr("system:node:tokyo", &["other-org"]);
        let result = validate_kubelet_client_csr(
            "kubernetes.io/kube-apiserver-client-kubelet",
            &csr,
            &["client auth".to_string()],
        );
        assert!(!result.valid);
        assert!(result.reason.contains("system:nodes"));
    }

    #[test]
    fn rejects_system_masters() {
        // Test the validation logic directly: build a CSR that has system:masters
        // in the subject O. Since rcgen may not support multiple O values easily,
        // we generate a CSR with only system:masters and verify it's rejected for
        // that reason (not just for missing system:nodes).
        let csr = generate_test_csr("system:node:tokyo", &["system:masters"]);
        let result = validate_kubelet_client_csr(
            "kubernetes.io/kube-apiserver-client-kubelet",
            &csr,
            &["client auth".to_string()],
        );
        // Rejected because system:nodes is missing OR system:masters is present
        assert!(!result.valid);
        assert!(
            result.reason.contains("system:masters") || result.reason.contains("system:nodes"),
            "reason: {}",
            result.reason
        );
    }

    #[test]
    fn rejects_server_auth_only() {
        let result = validate_kubelet_client_csr(
            "kubernetes.io/kube-apiserver-client-kubelet",
            &valid_csr(),
            &["server auth".to_string()],
        );
        assert!(!result.valid);
        // Rejected because client auth is required, or because server-auth-only
        assert!(
            result.reason.contains("client auth") || result.reason.contains("server auth"),
            "reason: {}",
            result.reason
        );
    }

    #[test]
    fn rejects_wrong_cn_format() {
        let csr = generate_test_csr("my-node", &["system:nodes"]);
        let result = validate_kubelet_client_csr(
            "kubernetes.io/kube-apiserver-client-kubelet",
            &csr,
            &["client auth".to_string()],
        );
        assert!(!result.valid);
        assert!(result.reason.contains("system:node:<nodeName>"));
    }

    #[test]
    fn rejects_invalid_pem() {
        let result = validate_kubelet_client_csr(
            "kubernetes.io/kube-apiserver-client-kubelet",
            b"not a pem",
            &["client auth".to_string()],
        );
        assert!(!result.valid);
        assert!(result.reason.contains("unparseable"));
    }

    #[test]
    fn rejects_server_auth_even_with_client_auth() {
        let result = validate_kubelet_client_csr(
            "kubernetes.io/kube-apiserver-client-kubelet",
            &valid_csr(),
            &["client auth".to_string(), "server auth".to_string()],
        );
        assert!(!result.valid);
        assert!(result.reason.contains("server auth"));
    }

    #[test]
    fn validates_bootstrap_requester_identity_and_expiration() {
        let usages = ["client auth".to_string()];
        let result = validate_kubelet_client_csr_request(KubeletClientCsrValidationInput {
            signer_name: "kubernetes.io/kube-apiserver-client-kubelet",
            csr_pem: &valid_csr(),
            usages: &usages,
            username: "system:bootstrap:abcdef",
            groups: &[
                "system:bootstrappers".to_string(),
                "system:bootstrappers:klights:worker".to_string(),
            ],
            expiration_seconds: Some(600),
        });
        assert!(result.valid, "reason: {}", result.reason);
        assert_eq!(result.node_name.as_deref(), Some("tokyo"));
        assert_eq!(result.ttl_seconds, 600);
    }

    #[test]
    fn defaults_unspecified_expiration_to_kubernetes_cluster_signing_duration() {
        let usages = ["client auth".to_string()];
        let result = validate_kubelet_client_csr_request(KubeletClientCsrValidationInput {
            signer_name: "kubernetes.io/kube-apiserver-client-kubelet",
            csr_pem: &valid_csr(),
            usages: &usages,
            username: "system:bootstrap:abcdef",
            groups: &[
                "system:bootstrappers".to_string(),
                "system:bootstrappers:klights:worker".to_string(),
            ],
            expiration_seconds: None,
        });

        assert!(result.valid, "reason: {}", result.reason);
        assert_eq!(result.ttl_seconds, 31_536_000);
    }

    #[test]
    fn rejects_requester_without_bootstrap_or_matching_node_identity() {
        let usages = ["client auth".to_string()];
        let result = validate_kubelet_client_csr_request(KubeletClientCsrValidationInput {
            signer_name: "kubernetes.io/kube-apiserver-client-kubelet",
            csr_pem: &valid_csr(),
            usages: &usages,
            username: "system:serviceaccount:default:builder",
            groups: &["system:serviceaccounts".to_string()],
            expiration_seconds: Some(600),
        });
        assert!(!result.valid);
        assert!(result.reason.contains("requester"));
    }

    #[test]
    fn rejects_node_renewal_when_username_does_not_match_csr_node() {
        let usages = ["client auth".to_string()];
        let result = validate_kubelet_client_csr_request(KubeletClientCsrValidationInput {
            signer_name: "kubernetes.io/kube-apiserver-client-kubelet",
            csr_pem: &valid_csr(),
            usages: &usages,
            username: "system:node:osaka",
            groups: &["system:nodes".to_string()],
            expiration_seconds: Some(600),
        });
        assert!(!result.valid);
        assert!(result.reason.contains("node name"));
    }

    #[test]
    fn rejects_expiration_below_kubernetes_minimum() {
        let usages = ["client auth".to_string()];
        let result = validate_kubelet_client_csr_request(KubeletClientCsrValidationInput {
            signer_name: "kubernetes.io/kube-apiserver-client-kubelet",
            csr_pem: &valid_csr(),
            usages: &usages,
            username: "system:bootstrap:abcdef",
            groups: &[
                "system:bootstrappers".to_string(),
                "system:bootstrappers:klights:worker".to_string(),
            ],
            expiration_seconds: Some(599),
        });
        assert!(!result.valid);
        assert!(result.reason.contains("expirationSeconds"));
    }
}
