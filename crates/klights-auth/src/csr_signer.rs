//! CSR signing abstraction.
//!
//! Production signs with the cluster client CA. Tests use `RecordingCsrSigner`.

use crate::clock::Clock;
use klights_supervisor::{TaskCategory, TaskSupervisor};
use std::sync::Arc;

/// A signing request captured by the signer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignRequest {
    pub csr_pem: Vec<u8>,
    pub common_name: String,
    pub organizations: Vec<String>,
    pub usages: Vec<String>,
    pub ttl_seconds: u32,
}

/// Result of signing a CSR.
#[derive(Clone, Debug)]
pub struct SignResult {
    pub certificate_pem: String,
}

/// Object-safe CSR signer trait.
pub trait CsrSigner: Send + Sync {
    fn sign(&self, request: SignRequest) -> Result<SignResult, crate::CredentialOperationError>;
}

/// Auth-owned kubelet CSR policy. Validation, subject construction, TTL
/// selection, and signing all execute behind one supervised capability.
pub struct KubeletCredentialPolicy {
    signer: Arc<dyn CsrSigner>,
    clock: Arc<dyn Clock>,
    supervisor: Arc<TaskSupervisor>,
}

impl KubeletCredentialPolicy {
    pub fn new(
        signer: Arc<dyn CsrSigner>,
        clock: Arc<dyn Clock>,
        supervisor: Arc<TaskSupervisor>,
    ) -> Self {
        Self {
            signer,
            clock,
            supervisor,
        }
    }

    pub async fn issue(
        &self,
        request: crate::KubeletCertificateRequest,
    ) -> Result<crate::KubeletCertificateOutcome, crate::CredentialOperationError> {
        let signer = self.signer.clone();
        let issued_at = self.clock.now();
        self.supervisor
            .run_blocking(
                TaskCategory::Others,
                "issue-kubelet-client-certificate",
                move || {
                    let validation = crate::csr_policy::validate_kubelet_client_csr_request(
                        crate::csr_policy::KubeletClientCsrValidationInput {
                            signer_name: &request.signer_name,
                            csr_pem: &request.csr_pem,
                            usages: &request.usages,
                            username: &request.username,
                            groups: &request.groups,
                            expiration_seconds: request.expiration_seconds,
                        },
                    );
                    if !validation.valid {
                        return Ok(crate::KubeletCertificateOutcome::Rejected {
                            reason: validation.reason,
                        });
                    }
                    let node_name = validation.node_name.ok_or_else(|| {
                        crate::CredentialOperationError::internal_failure(
                            "CSR policy did not resolve a node identity",
                        )
                    })?;
                    let signed = signer.sign(SignRequest {
                        csr_pem: request.csr_pem,
                        common_name: format!("system:node:{node_name}"),
                        organizations: vec!["system:nodes".to_string()],
                        usages: request.usages,
                        ttl_seconds: validation.ttl_seconds,
                    })?;
                    Ok(crate::KubeletCertificateOutcome::Issued {
                        node_name,
                        certificate_pem: signed.certificate_pem,
                        issued_at_unix_seconds: issued_at.unix_timestamp(),
                    })
                },
            )
            .await
            .map_err(|error| {
                crate::CredentialOperationError::internal_failure(format!(
                    "supervised kubelet certificate issuance failed: {error}"
                ))
            })?
    }
}

pub struct PeerCertificatePolicy {
    supervisor: Arc<TaskSupervisor>,
}

impl PeerCertificatePolicy {
    pub fn new(supervisor: Arc<TaskSupervisor>) -> Self {
        Self { supervisor }
    }

    pub async fn authenticate(
        &self,
        certificate: klights_types::TlsClientCertificate,
    ) -> Result<crate::PeerCertificateIdentity, crate::AuthenticationError> {
        self.supervisor
            .run_blocking(
                TaskCategory::Others,
                "authenticate-replication-peer",
                move || {
                    crate::user::user_from_cert(&certificate.0).map(|user| {
                        crate::PeerCertificateIdentity {
                            username: user.username,
                            groups: user.groups,
                        }
                    })
                },
            )
            .await
            .map_err(|error| {
                crate::AuthenticationError::internal_failure(format!(
                    "supervised peer authentication failed: {error}"
                ))
            })?
            .map_err(|error| crate::AuthenticationError::unauthenticated(error.to_string()))
    }
}

pub struct ControlplaneCredentialPolicy {
    clock: Arc<dyn Clock>,
    supervisor: Arc<TaskSupervisor>,
}

impl ControlplaneCredentialPolicy {
    pub fn new(clock: Arc<dyn Clock>, supervisor: Arc<TaskSupervisor>) -> Self {
        Self { clock, supervisor }
    }

    pub async fn sign_server_csr(
        &self,
        ca_cert_pem: String,
        ca_key_pem: String,
        csr_pem: Vec<u8>,
    ) -> Result<String, crate::CredentialOperationError> {
        let clock = self.clock.clone();
        self.supervisor
            .run_blocking(
                TaskCategory::Others,
                "issue-controlplane-server-certificate",
                move || {
                    CaCsrSigner::new(ca_cert_pem, ca_key_pem, clock)
                        .sign(SignRequest {
                            csr_pem,
                            common_name: "klights-server".to_string(),
                            organizations: vec![],
                            usages: vec!["server auth".to_string()],
                            ttl_seconds: 86_400 * 365 * 10,
                        })
                        .map(|result| result.certificate_pem)
                },
            )
            .await
            .map_err(|error| {
                crate::CredentialOperationError::internal_failure(format!(
                    "supervised control-plane certificate issuance failed: {error}"
                ))
            })?
    }

    pub async fn encrypt_key_material(
        &self,
        join_token: String,
        plaintext: Vec<u8>,
    ) -> Result<(Vec<u8>, Vec<u8>), crate::CredentialOperationError> {
        self.supervisor
            .run_blocking(
                TaskCategory::Others,
                "encrypt-controlplane-key-material",
                move || {
                    crate::ca_transport::encrypt_ca_key(&join_token, &plaintext)
                        .map(|(ciphertext, nonce)| (ciphertext, nonce.to_vec()))
                        .map_err(|error| {
                            crate::CredentialOperationError::internal_failure(error.to_string())
                        })
                },
            )
            .await
            .map_err(|error| {
                crate::CredentialOperationError::internal_failure(format!(
                    "supervised control-plane key encryption failed: {error}"
                ))
            })?
    }
}

/// Recording signer for tests. Records all sign requests and returns
/// a fixed certificate. Never calls real TLS.
#[cfg(test)]
pub(crate) struct RecordingCsrSigner {
    requests: std::sync::Mutex<Vec<SignRequest>>,
}

#[cfg(test)]
impl RecordingCsrSigner {
    pub fn new() -> Self {
        Self {
            requests: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn take_requests(&self) -> Vec<SignRequest> {
        std::mem::take(&mut self.requests.lock().unwrap())
    }

    pub fn request_count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

#[cfg(test)]
impl Default for RecordingCsrSigner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl CsrSigner for RecordingCsrSigner {
    fn sign(&self, request: SignRequest) -> Result<SignResult, crate::CredentialOperationError> {
        self.requests.lock().unwrap().push(request);
        // Return a fake certificate
        Ok(SignResult {
            certificate_pem: "-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----\n"
                .to_string(),
        })
    }
}

/// Production CSR signer using a CA certificate and key.
///
/// Parses the CSR PEM, builds a leaf certificate with the requested
/// subject/O/usages, signs with the CA key, and returns the PEM.
pub struct CaCsrSigner {
    ca_cert_pem: String,
    ca_key_pem: String,
    clock: Arc<dyn Clock>,
}

impl CaCsrSigner {
    pub fn new(ca_cert_pem: String, ca_key_pem: String, clock: Arc<dyn Clock>) -> Self {
        Self {
            ca_cert_pem,
            ca_key_pem,
            clock,
        }
    }
}

impl CsrSigner for CaCsrSigner {
    fn sign(&self, request: SignRequest) -> Result<SignResult, crate::CredentialOperationError> {
        use rcgen::{CertificateParams, DnType, KeyPair};
        use time::Duration;

        // Parse the CSR to extract the public key
        let csr_str = std::str::from_utf8(&request.csr_pem).map_err(|e| {
            crate::CredentialOperationError::rejected(format!("CSR is not valid UTF-8: {e}"))
        })?;
        let csr_params =
            rcgen::CertificateSigningRequestParams::from_pem(csr_str).map_err(|e| {
                crate::CredentialOperationError::rejected(format!("failed to parse CSR PEM: {e}"))
            })?;

        // Build certificate parameters from the request
        let mut params = CertificateParams::default();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, &request.common_name);

        // Encode all organizations as a single comma-joined O attribute. rcgen's
        // DistinguishedName is keyed by DnType, so it cannot hold two separate O
        // RDNs (a second push would overwrite the first). `user_from_cert`
        // splits this value back into one group per comma, so a control-plane
        // node cert can carry both `system:nodes` and `system:controlplanes`.
        // klights only ever signs comma-free group names, so the join/split
        // round-trips losslessly.
        if !request.organizations.is_empty() {
            params
                .distinguished_name
                .push(DnType::OrganizationName, request.organizations.join(","));
        }

        // Preserve SANs from the CSR (e.g. server cert IPs/DNS names).
        params.subject_alt_names = csr_params.params.subject_alt_names;

        // Set validity window from the CSR policy TTL.
        let now = self.clock.now();
        params.not_before = now;
        params.not_after = now + Duration::seconds(request.ttl_seconds as i64);

        // Map K8s usages to rcgen extended key usage
        for usage in &request.usages {
            match usage.as_str() {
                "client auth" | "clientAuth" => {
                    params
                        .extended_key_usages
                        .push(rcgen::ExtendedKeyUsagePurpose::ClientAuth);
                }
                "server auth" | "serverAuth" => {
                    params
                        .extended_key_usages
                        .push(rcgen::ExtendedKeyUsagePurpose::ServerAuth);
                }
                _ => {}
            }
        }

        // Parse CA key and build CA certificate for signing
        let ca_key = KeyPair::from_pem(&self.ca_key_pem).map_err(|e| {
            crate::CredentialOperationError::dependency_failure(format!(
                "failed to parse CA key: {e}"
            ))
        })?;

        // Reconstruct CA CertificateParams from PEM, then self-sign to get a
        // Certificate object suitable for use as an issuer in signed_by.
        let ca_params = CertificateParams::from_ca_cert_pem(&self.ca_cert_pem).map_err(|e| {
            crate::CredentialOperationError::dependency_failure(format!(
                "failed to parse CA cert: {e}"
            ))
        })?;
        let ca_cert = ca_params.self_signed(&ca_key).map_err(|e| {
            crate::CredentialOperationError::dependency_failure(format!(
                "failed to reconstruct CA cert: {e}"
            ))
        })?;

        // Sign the leaf certificate with the CA
        let cert = params
            .signed_by(&csr_params.public_key, &ca_cert, &ca_key)
            .map_err(|e| {
                crate::CredentialOperationError::internal_failure(format!(
                    "failed to sign certificate: {e}"
                ))
            })?;

        let cert_pem = cert.pem();
        Ok(SignResult {
            certificate_pem: cert_pem,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use x509_parser::prelude::FromDer;

    fn system_clock() -> Arc<dyn Clock> {
        Arc::new(crate::clock::SystemClock)
    }

    #[test]
    fn recording_signer_captures_request() {
        let signer = RecordingCsrSigner::new();
        let req = SignRequest {
            csr_pem: vec![1, 2, 3],
            common_name: "system:node:tokyo".to_string(),
            organizations: vec!["system:nodes".to_string()],
            usages: vec!["client auth".to_string()],
            ttl_seconds: 3600,
        };
        let result = signer.sign(req.clone()).unwrap();
        assert!(result.certificate_pem.contains("CERTIFICATE"));

        let requests = signer.take_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].common_name, "system:node:tokyo");
        assert_eq!(signer.request_count(), 0); // cleared
    }

    #[test]
    fn recording_signer_records_multiple() {
        let signer = RecordingCsrSigner::new();
        signer
            .sign(SignRequest {
                csr_pem: vec![],
                common_name: "a".to_string(),
                organizations: vec![],
                usages: vec![],
                ttl_seconds: 0,
            })
            .unwrap();
        signer
            .sign(SignRequest {
                csr_pem: vec![],
                common_name: "b".to_string(),
                organizations: vec![],
                usages: vec![],
                ttl_seconds: 0,
            })
            .unwrap();
        assert_eq!(signer.request_count(), 2);
    }

    #[tokio::test]
    async fn kubelet_issuance_policy_owns_subject_groups_usages_and_ttl() {
        let signer = Arc::new(RecordingCsrSigner::new());
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(Default::default()));
        let policy = super::KubeletCredentialPolicy::new(
            signer.clone(),
            Arc::new(crate::clock::FixedClock {
                now: time::OffsetDateTime::UNIX_EPOCH,
            }),
            supervisor,
        );
        let csr_pem = generate_csr_pem("system:node:tokyo", &["system:nodes"]);
        let outcome = policy
            .issue(crate::KubeletCertificateRequest {
                signer_name: "kubernetes.io/kube-apiserver-client-kubelet".into(),
                csr_pem: csr_pem.clone(),
                usages: vec!["client auth".into()],
                username: "system:bootstrap:abcdef".into(),
                groups: vec![
                    "system:bootstrappers".into(),
                    "system:bootstrappers:klights:worker".into(),
                ],
                expiration_seconds: Some(600),
            })
            .await
            .expect("policy should run");
        assert!(matches!(
            outcome,
            crate::KubeletCertificateOutcome::Issued { .. }
        ));
        assert_eq!(
            signer.take_requests(),
            vec![SignRequest {
                csr_pem,
                common_name: "system:node:tokyo".into(),
                organizations: vec!["system:nodes".into()],
                usages: vec!["client auth".into()],
                ttl_seconds: 600,
            }]
        );
    }

    fn generate_ca() -> (String, String) {
        use rcgen::{CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose};
        let mut params = CertificateParams::default();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, "klights-test-ca");
        params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.key_usages.push(KeyUsagePurpose::KeyCertSign);
        params.key_usages.push(KeyUsagePurpose::CrlSign);
        let key = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        (cert.pem(), key.serialize_pem())
    }

    fn generate_csr_pem(cn: &str, orgs: &[&str]) -> Vec<u8> {
        use rcgen::{CertificateParams, DnType, KeyPair};
        let mut params = CertificateParams::default();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params.distinguished_name.push(DnType::CommonName, cn);
        if !orgs.is_empty() {
            params
                .distinguished_name
                .push(DnType::OrganizationName, orgs.join(","));
        }
        let key = KeyPair::generate().unwrap();
        let csr = params.serialize_request(&key).unwrap();
        csr.pem().unwrap().into_bytes()
    }

    #[test]
    fn ca_signer_produces_valid_certificate() {
        let (ca_cert, ca_key) = generate_ca();
        let signer = CaCsrSigner::new(ca_cert, ca_key, system_clock());

        let csr_pem = generate_csr_pem("system:node:tokyo", &["system:nodes"]);
        let result = signer
            .sign(SignRequest {
                csr_pem,
                common_name: "system:node:tokyo".to_string(),
                organizations: vec!["system:nodes".to_string()],
                usages: vec!["client auth".to_string()],
                ttl_seconds: 3600,
            })
            .unwrap();

        assert!(
            result.certificate_pem.contains("BEGIN CERTIFICATE"),
            "result should be a PEM certificate"
        );
    }

    #[test]
    fn ca_signer_preserves_san_from_csr() {
        let (ca_cert, ca_key) = generate_ca();
        let signer = CaCsrSigner::new(ca_cert, ca_key, system_clock());

        // Generate a CSR with SANs (IP + DNS)
        let mut params = rcgen::CertificateParams::default();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "klights-server");
        params.subject_alt_names = vec![
            rcgen::SanType::IpAddress(std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 99, 0, 14))),
            rcgen::SanType::DnsName(rcgen::Ia5String::try_from("api.klights.net").unwrap()),
            rcgen::SanType::DnsName(rcgen::Ia5String::try_from("localhost").unwrap()),
        ];
        let key = rcgen::KeyPair::generate().unwrap();
        let csr = params.serialize_request(&key).unwrap();
        let csr_pem = csr.pem().unwrap().into_bytes();

        let result = signer
            .sign(SignRequest {
                csr_pem,
                common_name: "klights-server".to_string(),
                organizations: vec![],
                usages: vec!["server auth".to_string()],
                ttl_seconds: 3600,
            })
            .unwrap();

        // Verify the signed cert contains the SANs from the CSR
        let (_, pem) = x509_parser::pem::parse_x509_pem(result.certificate_pem.as_bytes()).unwrap();
        let (_, cert) = x509_parser::parse_x509_certificate(&pem.contents).unwrap();
        let mut dns_names = Vec::new();
        let mut ip_addrs: Vec<Vec<u8>> = Vec::new();
        for ext in cert.extensions() {
            if ext.oid == x509_parser::oid_registry::OID_X509_EXT_SUBJECT_ALT_NAME
                && let x509_parser::extensions::ParsedExtension::SubjectAlternativeName(san) =
                    ext.parsed_extension()
            {
                for gn in &san.general_names {
                    match gn {
                        x509_parser::extensions::GeneralName::DNSName(s) => {
                            dns_names.push(s.to_string())
                        }
                        x509_parser::extensions::GeneralName::IPAddress(ip) => {
                            ip_addrs.push(ip.to_vec())
                        }
                        _ => {}
                    }
                }
            }
        }
        assert!(
            dns_names.contains(&"api.klights.net".to_string()),
            "signed cert must preserve DNS SAN from CSR, got: {dns_names:?}"
        );
        assert!(
            dns_names.contains(&"localhost".to_string()),
            "signed cert must preserve localhost DNS SAN from CSR, got: {dns_names:?}"
        );
        assert!(
            ip_addrs.contains(&vec![10, 99, 0, 14]),
            "signed cert must preserve IP SAN from CSR, got: {ip_addrs:?}"
        );
    }

    #[test]
    fn ca_signer_rejects_invalid_csr() {
        let (ca_cert, ca_key) = generate_ca();
        let signer = CaCsrSigner::new(ca_cert, ca_key, system_clock());

        let result = signer.sign(SignRequest {
            csr_pem: b"not a valid CSR".to_vec(),
            common_name: "test".to_string(),
            organizations: vec![],
            usages: vec![],
            ttl_seconds: 3600,
        });
        assert!(result.is_err(), "invalid CSR should be rejected");
    }

    #[test]
    fn ca_signer_rejects_invalid_ca_key() {
        let signer = CaCsrSigner::new(
            "not a cert".to_string(),
            "not a key".to_string(),
            system_clock(),
        );
        let csr_pem = generate_csr_pem("test", &[]);
        let result = signer.sign(SignRequest {
            csr_pem,
            common_name: "test".to_string(),
            organizations: vec![],
            usages: vec![],
            ttl_seconds: 3600,
        });
        assert!(result.is_err(), "invalid CA key should be rejected");
    }

    #[test]
    fn ca_signer_uses_injected_clock_for_certificate_validity() {
        let (ca_cert, ca_key) = generate_ca();
        let fixed_now =
            time::OffsetDateTime::from_unix_timestamp(1_704_067_200).expect("valid timestamp");
        let signer = CaCsrSigner::new(
            ca_cert,
            ca_key,
            std::sync::Arc::new(crate::clock::FixedClock { now: fixed_now }),
        );

        let result = signer
            .sign(SignRequest {
                csr_pem: generate_csr_pem("system:node:tokyo", &["system:nodes"]),
                common_name: "system:node:tokyo".to_string(),
                organizations: vec!["system:nodes".to_string()],
                usages: vec!["client auth".to_string()],
                ttl_seconds: 600,
            })
            .unwrap();

        let pem =
            x509_parser::pem::Pem::read(std::io::Cursor::new(result.certificate_pem.as_bytes()))
                .expect("certificate PEM should parse")
                .0;
        let (_, cert) = x509_parser::prelude::X509Certificate::from_der(&pem.contents)
            .expect("certificate DER should parse");
        let validity = cert.validity();

        assert_eq!(
            validity.not_before.timestamp(),
            fixed_now.unix_timestamp(),
            "certificate not_before must come from injected clock"
        );
        assert_eq!(
            validity.not_after.timestamp(),
            (fixed_now + time::Duration::seconds(600)).unix_timestamp(),
            "certificate not_after must use injected clock plus requested TTL"
        );
    }
}
