//! User identity extraction from client certificates.
//!
//! Provides the `User` struct and `user_from_cert` function for extracting
//! user identity from X.509 certificate CN and O fields.

use anyhow::Result;
use x509_parser::prelude::*;

/// User identity extracted from client cert or bearer token.
#[derive(Clone, Debug)]
pub struct User {
    pub username: String,
    pub groups: Vec<String>,
}

impl User {
    /// Check if this user has admin privileges.
    ///
    /// Admin users have the `system:masters` group.
    pub fn is_admin(&self) -> bool {
        self.groups.contains(&"system:masters".to_string())
    }

    /// Create an anonymous user.
    pub fn anonymous() -> Self {
        User {
            username: "system:anonymous".to_string(),
            groups: vec!["system:unauthenticated".to_string()],
        }
    }
}

/// Extract user from client certificate CN and O fields.
pub fn user_from_cert(cert_der: &[u8]) -> Result<User> {
    let (_, cert) = X509Certificate::from_der(cert_der)?;
    user_from_x509(&cert)
}

/// Extract the K8s user (CN → username, O → groups) from a parsed certificate.
fn user_from_x509(cert: &X509Certificate) -> Result<User> {
    let subject = cert.subject();

    let mut username = None;
    let mut groups = Vec::new();

    for attr in subject.iter_attributes() {
        let oid = attr.attr_type();
        if oid == &oid_registry::OID_X509_COMMON_NAME {
            if let Ok(s) = attr.as_str() {
                username = Some(s.to_string());
            }
        } else if oid == &oid_registry::OID_X509_ORGANIZATION_NAME
            && let Ok(s) = attr.as_str()
        {
            // A single O attribute may carry several groups comma-joined: rcgen's
            // DistinguishedName is keyed by DnType and cannot emit two separate O
            // RDNs, so the CA signer (`CaCsrSigner::sign`) joins multiple groups
            // (e.g. `system:nodes,system:controlplanes` for control-plane node
            // certs) into one O. Split them back into individual groups. klights
            // only signs comma-free group names, so a plain single-group O (the
            // common case) round-trips unchanged.
            for group in s.split(',') {
                let group = group.trim();
                if !group.is_empty() {
                    groups.push(group.to_string());
                }
            }
        }
    }

    // A CA-signed client certificate with no CommonName carries no identity.
    // Defaulting it to a placeholder username ("unknown") would let any such
    // cert authenticate under a shared, attacker-knowable name; reject instead.
    let username = username
        .filter(|cn| !cn.is_empty())
        .ok_or_else(|| anyhow::anyhow!("client certificate has no CommonName (CN)"))?;

    Ok(User { username, groups })
}

/// Authenticate a client certificate that arrived out-of-band (i.e. not
/// validated by the TLS stack) by *cryptographically* verifying it against the
/// cluster CA before trusting its subject.
///
/// This is the trust anchor for the follower→leader API proxy: a non-leader
/// control plane forwards the end user's actual client certificate to the
/// leader, and the leader re-authenticates it here. Because the leaf must carry
/// a valid signature from the cluster CA, a compromised or over-broad proxy
/// cannot mint `system:masters` (or any other identity) by fabricating a
/// certificate — it would need the CA private key. The proxy's *own* credential
/// is therefore never presented as the end-user identity; only a CA-signed end
/// user certificate is honored.
///
/// Verification performed:
/// - the leaf's signature validates against the CA's public key, and
/// - the leaf is within its validity window (not expired / not yet valid).
///
/// On success the identity is derived from the leaf's CN/O exactly as for a
/// directly-presented TLS client certificate.
pub fn verify_client_cert_signed_by_ca(cert_der: &[u8], ca_pem: &str) -> Result<User> {
    let ca_der = first_pem_certificate(ca_pem)?;
    let (_, ca_cert) = X509Certificate::from_der(&ca_der)
        .map_err(|e| anyhow::anyhow!("failed to parse cluster CA certificate: {e}"))?;
    let (_, leaf) = X509Certificate::from_der(cert_der)
        .map_err(|e| anyhow::anyhow!("failed to parse forwarded client certificate: {e}"))?;

    leaf.verify_signature(Some(ca_cert.public_key()))
        .map_err(|e| anyhow::anyhow!("forwarded client certificate is not CA-signed: {e}"))?;

    if !leaf.validity().is_valid() {
        anyhow::bail!("forwarded client certificate is expired or not yet valid");
    }

    user_from_x509(&leaf)
}

/// Decode the first certificate from a PEM bundle into DER bytes.
fn first_pem_certificate(pem: &str) -> Result<Vec<u8>> {
    use x509_parser::pem::Pem;
    let (parsed, _) = Pem::read(std::io::Cursor::new(pem.as_bytes()))
        .map_err(|e| anyhow::anyhow!("failed to read CA PEM: {e}"))?;
    Ok(parsed.contents)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================
    // User tests
    // ========================

    #[test]
    fn test_user_is_admin_with_system_masters_group() {
        let user = User {
            username: "klights-admin".to_string(),
            groups: vec!["system:masters".to_string()],
        };
        assert!(user.is_admin());
    }

    #[test]
    fn test_user_is_admin_false_without_system_masters() {
        let user = User {
            username: "developer".to_string(),
            groups: vec!["system:authenticated".to_string()],
        };
        assert!(!user.is_admin());
    }

    #[test]
    fn test_user_is_admin_false_with_empty_groups() {
        let user = User {
            username: "nobody".to_string(),
            groups: vec![],
        };
        assert!(!user.is_admin());
    }

    #[test]
    fn test_user_is_admin_with_multiple_groups() {
        let user = User {
            username: "admin".to_string(),
            groups: vec![
                "system:authenticated".to_string(),
                "system:masters".to_string(),
                "developers".to_string(),
            ],
        };
        assert!(user.is_admin());
    }

    #[test]
    fn test_user_anonymous_has_correct_identity() {
        let user = User::anonymous();
        assert_eq!(user.username, "system:anonymous");
        assert_eq!(user.groups, vec!["system:unauthenticated"]);
        assert!(!user.is_admin());
    }

    // ========================
    // user_from_cert tests
    // ========================

    fn pem_to_der(pem_str: &str) -> Vec<u8> {
        use x509_parser::pem::Pem;
        let (pem, _) = Pem::read(std::io::Cursor::new(pem_str.as_bytes())).unwrap();
        pem.contents
    }

    #[test]
    fn test_user_from_cert_extracts_cn_and_org() {
        let (ca_cert, ca_key, _, _) = super::super::cert::generate_ca_full().unwrap();
        let (admin_cert_pem, _) =
            super::super::cert::generate_admin_cert(&ca_cert, &ca_key).unwrap();

        let der = pem_to_der(&admin_cert_pem);
        let user = user_from_cert(&der).unwrap();

        assert_eq!(user.username, "klights-admin");
        assert!(user.groups.contains(&"system:masters".to_string()));
        assert!(user.is_admin());
    }

    #[test]
    fn ca_signed_multi_group_cert_round_trips_to_separate_groups() {
        // A control-plane node cert carries two groups (`system:nodes` and
        // `system:controlplanes`). rcgen cannot emit two O RDNs, so the signer
        // comma-joins them into one O; `user_from_cert` must split them back into
        // two distinct groups (otherwise raft auth and NodeRestriction both
        // break).
        use super::super::csr_signer::{CaCsrSigner, CsrSigner, SignRequest};

        let (_, _, ca_cert_pem, ca_key_pem) = super::super::cert::generate_ca_full().unwrap();
        let csr = super::super::kubelet_client_cert::generate_kubelet_client_csr("cp-1").unwrap();
        let signer = CaCsrSigner::new(ca_cert_pem, ca_key_pem);
        let signed = signer
            .sign(SignRequest {
                csr_pem: csr.csr_pem,
                common_name: "system:node:cp-1".to_string(),
                organizations: vec![
                    "system:nodes".to_string(),
                    "system:controlplanes".to_string(),
                ],
                usages: vec!["client auth".to_string()],
                ttl_seconds: 3600,
            })
            .unwrap();

        let der = pem_to_der(&signed.certificate_pem);
        let user = user_from_cert(&der).unwrap();
        assert_eq!(user.username, "system:node:cp-1");
        assert!(
            user.groups.contains(&"system:nodes".to_string()),
            "node group must survive comma-join round-trip, got {:?}",
            user.groups
        );
        assert!(
            user.groups.contains(&"system:controlplanes".to_string()),
            "controlplane group must survive comma-join round-trip, got {:?}",
            user.groups
        );
    }

    #[test]
    fn test_user_from_cert_server_cert_not_admin() {
        let (ca_cert, ca_key, _, _) = super::super::cert::generate_ca_full().unwrap();
        let (server_cert_pem, _) =
            super::super::cert::generate_server_cert(&ca_cert, &ca_key).unwrap();

        let der = pem_to_der(&server_cert_pem);
        let user = user_from_cert(&der).unwrap();

        assert_eq!(user.username, "klights-server");
        assert!(!user.is_admin());
        assert!(user.groups.is_empty());
    }

    #[test]
    fn verify_forwarded_admin_cert_against_ca_preserves_system_masters() {
        // The follower forwards the kubectl admin's actual client cert; the
        // leader re-authenticates it natively and must derive system:masters
        // from the cert's O (so cluster-admin access survives the proxy hop).
        let (ca_cert, ca_key, ca_pem, _) = super::super::cert::generate_ca_full().unwrap();
        let (admin_cert_pem, _) =
            super::super::cert::generate_admin_cert(&ca_cert, &ca_key).unwrap();
        let der = pem_to_der(&admin_cert_pem);

        let user = verify_client_cert_signed_by_ca(&der, &ca_pem).unwrap();
        assert_eq!(user.username, "klights-admin");
        assert!(
            user.is_admin(),
            "CA-verified admin cert must keep system:masters"
        );
    }

    #[test]
    fn verify_forwarded_cert_rejects_cert_not_signed_by_cluster_ca() {
        // A cert signed by a *different* CA (i.e. forged by a proxy that lacks
        // the cluster CA key) must be rejected even if its O claims
        // system:masters.
        let (_, _, cluster_ca_pem, _) = super::super::cert::generate_ca_full().unwrap();

        // Independent CA + admin cert claiming system:masters.
        let (rogue_ca_cert, rogue_ca_key, _, _) = super::super::cert::generate_ca_full().unwrap();
        let (rogue_admin_pem, _) =
            super::super::cert::generate_admin_cert(&rogue_ca_cert, &rogue_ca_key).unwrap();
        let der = pem_to_der(&rogue_admin_pem);

        let result = verify_client_cert_signed_by_ca(&der, &cluster_ca_pem);
        assert!(
            result.is_err(),
            "a cert not signed by the cluster CA must be rejected, got {:?}",
            result.map(|u| u.username)
        );
    }

    #[test]
    fn test_user_from_cert_rejects_certificate_without_common_name() {
        // A cert with only an Organization (group) and no CommonName must not
        // authenticate as a placeholder username — it must be rejected outright.
        use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair};

        let mut params = CertificateParams::default();
        let mut dn = DistinguishedName::new();
        dn.push(DnType::OrganizationName, "system:masters");
        params.distinguished_name = dn;
        let key = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        let der = cert.der().to_vec();

        let result = user_from_cert(&der);
        assert!(
            result.is_err(),
            "CN-less client cert must be rejected, got {:?}",
            result.map(|u| u.username)
        );
    }
}
