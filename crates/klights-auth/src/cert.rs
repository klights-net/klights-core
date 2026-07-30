//! Certificate generation for klights CA, server, and admin certs.
//!
//! Provides RSA key pair generation, CA certificate creation, and signed certificates
//! for server and admin use.

use anyhow::Result;
use rand_core::OsRng;
use rcgen::{Certificate, CertificateParams, DistinguishedName, DnType, IsCa, KeyPair, SanType};
use rsa::{RsaPrivateKey, pkcs8::EncodePrivateKey};
use std::sync::Arc;
use time::{Duration, OffsetDateTime};

const CERTIFICATE_VALIDITY_YEARS: i64 = 10;
pub const API_PROXY_COMMON_NAME_PREFIX: &str = "system:klights:api-proxy:";
pub const APISERVICE_PROXY_GROUP: &str = "system:klights:apiservice-proxies";

/// Standard node group carried by every node (control-plane and worker) client
/// certificate.
pub const NODES_GROUP: &str = "system:nodes";

/// Group carried — in addition to [`NODES_GROUP`] — only by control-plane node
/// client certificates, i.e. those minted through the controlplane-token-gated
/// bootstrap (`ensure_local_node_client_certificate`). It is the authorization
/// signal for raft consensus RPCs (vote / append-entries / install-snapshot): a
/// worker's node certificate, signed via the Kubernetes CSR API, carries only
/// `system:nodes` and is therefore barred from driving raft consensus. Anchoring
/// the gate on the certificate (rather than the node's own raft membership view)
/// lets a freshly-joining control-plane authorize immediately, before it has
/// caught up enough to learn cluster membership.
pub const CONTROLPLANE_NODES_GROUP: &str = "system:controlplanes";

/// In-memory certificate-signing capability built from caller-owned CA
/// material.
///
/// The auth layer owns certificate policy and cryptography only. Bootstrap
/// decides where the PEM material comes from and whether/how it is persisted,
/// then injects it through this capability.
#[derive(Clone)]
pub struct CertificateAuthority {
    certificate: Arc<Certificate>,
    private_key: Arc<KeyPair>,
    certificate_pem: Arc<str>,
    private_key_pem: Arc<str>,
}

impl CertificateAuthority {
    pub fn from_pem(
        certificate_pem: String,
        private_key_pem: String,
        valid_at: OffsetDateTime,
    ) -> Result<Self> {
        let private_key = KeyPair::from_pem(&private_key_pem)?;
        let params = generate_ca_params_at(valid_at);
        let certificate = params.self_signed(&private_key)?;
        Ok(Self {
            certificate: Arc::new(certificate),
            private_key: Arc::new(private_key),
            certificate_pem: Arc::from(certificate_pem),
            private_key_pem: Arc::from(private_key_pem),
        })
    }

    pub fn generate(valid_at: OffsetDateTime) -> Result<Self> {
        let (certificate, private_key, certificate_pem, private_key_pem) =
            generate_ca_full_at(valid_at)?;
        Ok(Self {
            certificate: Arc::new(certificate),
            private_key: Arc::new(private_key),
            certificate_pem: Arc::from(certificate_pem),
            private_key_pem: Arc::from(private_key_pem),
        })
    }

    pub fn certificate_pem(&self) -> &str {
        &self.certificate_pem
    }

    pub fn private_key_pem(&self) -> &str {
        &self.private_key_pem
    }

    #[allow(clippy::too_many_arguments)]
    pub fn issue_server_certificate(
        &self,
        service_cidr: &str,
        pod_subnet: &str,
        host_ip: Option<&str>,
        node_name: &str,
        api_fqdn: Option<&str>,
        valid_at: OffsetDateTime,
    ) -> Result<(String, String)> {
        generate_server_cert_from_config(
            &self.certificate,
            &self.private_key,
            ServerCertGenerationConfig {
                service_cidr,
                pod_subnet,
                host_ip,
                node_name,
                api_fqdn,
                valid_at,
            },
        )
    }

    pub fn issue_admin_certificate(&self, valid_at: OffsetDateTime) -> Result<(String, String)> {
        generate_admin_cert_at(&self.certificate, &self.private_key, valid_at)
    }

    pub fn issue_api_proxy_certificate(
        &self,
        node_name: &str,
        valid_at: OffsetDateTime,
    ) -> Result<(String, String)> {
        generate_api_proxy_cert(&self.certificate, &self.private_key, node_name, valid_at)
    }

    pub fn issue_apiservice_proxy_certificate(
        &self,
        valid_at: OffsetDateTime,
    ) -> Result<(String, String)> {
        generate_apiservice_proxy_cert(&self.certificate, &self.private_key, valid_at)
    }
}

pub fn api_proxy_common_name(node_name: &str) -> String {
    format!("{API_PROXY_COMMON_NAME_PREFIX}{node_name}")
}

fn set_certificate_validity(params: &mut CertificateParams, valid_at: OffsetDateTime) {
    params.not_before = valid_at;
    params.not_after = valid_at + Duration::days(CERTIFICATE_VALIDITY_YEARS * 365);
}

/// Generate CA certificate parameters.
fn generate_ca_params_at(valid_at: OffsetDateTime) -> CertificateParams {
    let mut params = CertificateParams::default();

    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "klights-ca");
    params.distinguished_name = dn;

    params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    set_certificate_validity(&mut params, valid_at);
    params
}

/// Generate an RSA-2048 key pair and import it into rcgen.
///
/// Uses the `rsa` crate for generation (pure Rust, no aws-lc-rs needed).
/// ring backend handles RSA signing for externally-generated keys.
fn generate_rsa_key_pair() -> Result<KeyPair> {
    let private_key = RsaPrivateKey::new(&mut OsRng, 2048)
        .map_err(|e| anyhow::anyhow!("RSA key generation failed: {}", e))?;
    let der = private_key
        .to_pkcs8_der()
        .map_err(|e| anyhow::anyhow!("RSA PKCS#8 serialization failed: {}", e))?;
    KeyPair::try_from(der.as_bytes()).map_err(|e| anyhow::anyhow!("rcgen key import failed: {}", e))
}

/// Generate a CA certificate and return both PEM representations.
///
/// Returns: (cert, key, cert_pem, key_pem)
/// - `cert` and `key`: rcgen objects for signing
/// - `cert_pem` and `key_pem`: PEM strings for file I/O
pub fn generate_ca_full_at(
    valid_at: OffsetDateTime,
) -> Result<(rcgen::Certificate, KeyPair, String, String)> {
    let params = generate_ca_params_at(valid_at);
    let key_pair = generate_rsa_key_pair()?;
    let cert = params.self_signed(&key_pair)?;
    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();

    Ok((cert, key_pair, cert_pem, key_pem))
}

#[cfg(test)]
pub fn generate_ca_full() -> Result<(rcgen::Certificate, KeyPair, String, String)> {
    generate_ca_full_at(OffsetDateTime::now_utc())
}

/// Generate a server certificate with dynamic service CIDR and optional host IP.
///
/// This version allows configuration for testing and multi-namespace deployments.
struct ServerCertGenerationConfig<'a> {
    service_cidr: &'a str,
    pod_subnet: &'a str,
    host_ip: Option<&'a str>,
    node_name: &'a str,
    api_fqdn: Option<&'a str>,
    valid_at: OffsetDateTime,
}

fn generate_server_cert_from_config(
    ca_cert: &rcgen::Certificate,
    ca_key: &KeyPair,
    config: ServerCertGenerationConfig<'_>,
) -> Result<(String, String)> {
    let mut params = CertificateParams::default();

    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "klights-server");
    params.distinguished_name = dn;

    params.subject_alt_names = server_cert_san_types(
        config.service_cidr,
        config.pod_subnet,
        config.host_ip,
        config.node_name,
        config.api_fqdn,
    );

    set_certificate_validity(&mut params, config.valid_at);

    let key_pair = generate_rsa_key_pair()?;
    let cert = params.signed_by(&key_pair, ca_cert, ca_key)?;

    Ok((cert.pem(), key_pair.serialize_pem()))
}

#[allow(clippy::too_many_arguments)]
pub fn generate_server_cert_with_config_at(
    ca_cert: &rcgen::Certificate,
    ca_key: &KeyPair,
    service_cidr: &str,
    pod_subnet: &str,
    host_ip: Option<String>,
    node_name: &str,
    api_fqdn: Option<&str>,
    valid_at: OffsetDateTime,
) -> Result<(String, String)> {
    generate_server_cert_from_config(
        ca_cert,
        ca_key,
        ServerCertGenerationConfig {
            service_cidr,
            pod_subnet,
            host_ip: host_ip.as_deref(),
            node_name,
            api_fqdn,
            valid_at,
        },
    )
}

#[cfg(test)]
pub fn generate_server_cert_with_config(
    ca_cert: &rcgen::Certificate,
    ca_key: &KeyPair,
    service_cidr: &str,
    pod_subnet: &str,
    host_ip: Option<String>,
    node_name: &str,
    api_fqdn: Option<&str>,
) -> Result<(String, String)> {
    generate_server_cert_with_config_at(
        ca_cert,
        ca_key,
        service_cidr,
        pod_subnet,
        host_ip,
        node_name,
        api_fqdn,
        OffsetDateTime::now_utc(),
    )
}

/// Generate a server certificate with deterministic fixture defaults at a
/// caller-selected time.
pub fn generate_server_cert_at(
    ca_cert: &rcgen::Certificate,
    ca_key: &KeyPair,
    valid_at: OffsetDateTime,
) -> Result<(String, String)> {
    generate_server_cert_from_config(
        ca_cert,
        ca_key,
        ServerCertGenerationConfig {
            service_cidr: "10.43.128.0/17",
            pod_subnet: "10.43.0.0/17",
            host_ip: None,
            node_name: "test-node",
            api_fqdn: None,
            valid_at,
        },
    )
}

#[cfg(test)]
pub fn generate_server_cert(
    ca_cert: &rcgen::Certificate,
    ca_key: &KeyPair,
) -> Result<(String, String)> {
    generate_server_cert_at(ca_cert, ca_key, OffsetDateTime::now_utc())
}

/// Generate an admin certificate signed by the CA.
pub fn generate_admin_cert_at(
    ca_cert: &rcgen::Certificate,
    ca_key: &KeyPair,
    valid_at: OffsetDateTime,
) -> Result<(String, String)> {
    let mut params = CertificateParams::default();

    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "klights-admin");
    dn.push(DnType::OrganizationName, "system:masters");
    params.distinguished_name = dn;

    set_certificate_validity(&mut params, valid_at);

    let key_pair = generate_rsa_key_pair()?;
    let cert = params.signed_by(&key_pair, ca_cert, ca_key)?;

    Ok((cert.pem(), key_pair.serialize_pem()))
}

#[cfg(test)]
pub fn generate_admin_cert(
    ca_cert: &rcgen::Certificate,
    ca_key: &KeyPair,
) -> Result<(String, String)> {
    generate_admin_cert_at(ca_cert, ca_key, OffsetDateTime::now_utc())
}

/// Generate the dedicated follower API-proxy client certificate.
///
/// This credential proves "trusted follower proxy" to a leader. It is not an
/// admin credential and is not valid as an API server serving certificate.
pub fn generate_api_proxy_cert(
    ca_cert: &rcgen::Certificate,
    ca_key: &KeyPair,
    node_name: &str,
    valid_at: OffsetDateTime,
) -> Result<(String, String)> {
    let mut params = CertificateParams::default();

    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, api_proxy_common_name(node_name));
    params.distinguished_name = dn;
    params.key_usages = vec![
        rcgen::KeyUsagePurpose::DigitalSignature,
        rcgen::KeyUsagePurpose::KeyEncipherment,
    ];
    params
        .extended_key_usages
        .push(rcgen::ExtendedKeyUsagePurpose::ClientAuth);

    set_certificate_validity(&mut params, valid_at);

    let key_pair = generate_rsa_key_pair()?;
    let cert = params.signed_by(&key_pair, ca_cert, ca_key)?;

    Ok((cert.pem(), key_pair.serialize_pem()))
}

/// Generate the dedicated API aggregation proxy client certificate.
///
/// This credential authenticates klights to aggregated APIService backends so
/// they can trust sanitized requestheader identity. It is not an admin
/// credential and is not valid as an API server serving certificate.
pub fn generate_apiservice_proxy_cert(
    ca_cert: &rcgen::Certificate,
    ca_key: &KeyPair,
    valid_at: OffsetDateTime,
) -> Result<(String, String)> {
    let mut params = CertificateParams::default();

    let mut dn = DistinguishedName::new();
    dn.push(
        DnType::CommonName,
        klights_types::APISERVICE_PROXY_COMMON_NAME,
    );
    dn.push(DnType::OrganizationName, APISERVICE_PROXY_GROUP);
    params.distinguished_name = dn;
    params.key_usages = vec![
        rcgen::KeyUsagePurpose::DigitalSignature,
        rcgen::KeyUsagePurpose::KeyEncipherment,
    ];
    params
        .extended_key_usages
        .push(rcgen::ExtendedKeyUsagePurpose::ClientAuth);

    set_certificate_validity(&mut params, valid_at);

    let key_pair = generate_rsa_key_pair()?;
    let cert = params.signed_by(&key_pair, ca_cert, ca_key)?;

    Ok((cert.pem(), key_pair.serialize_pem()))
}

pub fn api_proxy_cert_and_key_match_config(cert_pem: &str, key_pem: &str, node_name: &str) -> bool {
    let der = match first_pem_cert_der(cert_pem) {
        Some(der) => der,
        None => return false,
    };
    let Ok(user) = super::user::user_from_cert(&der) else {
        return false;
    };
    if user.username != api_proxy_common_name(node_name)
        || user.groups.iter().any(|group| group == "system:masters")
    {
        return false;
    }
    if !matches!(
        parse_certificate_extended_key_usage(cert_pem),
        Some((false, true))
    ) {
        return false;
    }
    certificate_key_pair_matches(cert_pem, key_pem)
}

pub fn apiservice_proxy_cert_and_key_match_config(cert_pem: &str, key_pem: &str) -> bool {
    let der = match first_pem_cert_der(cert_pem) {
        Some(der) => der,
        None => return false,
    };
    let Ok(user) = super::user::user_from_cert(&der) else {
        return false;
    };
    if user.username != klights_types::APISERVICE_PROXY_COMMON_NAME
        || user.groups != [APISERVICE_PROXY_GROUP.to_string()]
        || user.groups.iter().any(|group| group == "system:masters")
    {
        return false;
    }
    if !matches!(
        parse_certificate_extended_key_usage(cert_pem),
        Some((false, true))
    ) {
        return false;
    }
    certificate_key_pair_matches(cert_pem, key_pem)
}

fn certificate_key_pair_matches(cert_pem: &str, key_pem: &str) -> bool {
    let Ok(key_pair) = KeyPair::from_pem(key_pem) else {
        return false;
    };
    let Some(cert_public_key_der) = certificate_subject_public_key_info_der(cert_pem) else {
        return false;
    };
    cert_public_key_der == key_pair.public_key_der()
}

fn first_pem_cert_der(cert_pem: &str) -> Option<Vec<u8>> {
    rustls_pemfile::certs(&mut cert_pem.as_bytes())
        .next()?
        .ok()
        .map(|cert| cert.as_ref().to_vec())
}

fn certificate_subject_public_key_info_der(cert_pem: &str) -> Option<Vec<u8>> {
    with_parsed_certificate(cert_pem, |cert| {
        cert.tbs_certificate.subject_pki.raw.to_vec()
    })
}

pub fn parse_certificate_extended_key_usage(cert_pem: &str) -> Option<(bool, bool)> {
    use x509_parser::prelude::*;
    with_parsed_certificate(cert_pem, |cert| {
        cert.extensions().iter().find_map(|ext| {
            if ext.oid == x509_parser::oid_registry::OID_X509_EXT_EXTENDED_KEY_USAGE
                && let ParsedExtension::ExtendedKeyUsage(eku) = ext.parsed_extension()
            {
                return Some((eku.server_auth, eku.client_auth));
            }
            None
        })
    })?
}

fn with_parsed_certificate<T>(
    cert_pem: &str,
    f: impl for<'a> FnOnce(&x509_parser::certificate::X509Certificate<'a>) -> T,
) -> Option<T> {
    let der = first_pem_cert_der(cert_pem)?;
    let (_, cert) = x509_parser::parse_x509_certificate(&der).ok()?;
    Some(f(&cert))
}

/// Derive the bridge gateway IP from the pod subnet.
///
/// The gateway is always the first IP (network address + 1).
/// Example: "10.43.0.0/17" -> "10.43.0.1"
pub fn derive_gateway_ip(pod_subnet: &str) -> String {
    klights_types::first_usable_ipv4(pod_subnet)
}

/// Build the SAN list shared between server cert generation and CSR generation.
fn server_cert_san_types(
    service_cidr: &str,
    pod_subnet: &str,
    host_ip: Option<&str>,
    node_name: &str,
    api_fqdn: Option<&str>,
) -> Vec<SanType> {
    let mut sans = vec![
        SanType::DnsName(rcgen::Ia5String::try_from("localhost").unwrap()),
        SanType::IpAddress(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1))),
        SanType::DnsName(rcgen::Ia5String::try_from("kubernetes").unwrap()),
        SanType::DnsName(rcgen::Ia5String::try_from("kubernetes.default").unwrap()),
        SanType::DnsName(rcgen::Ia5String::try_from("kubernetes.default.svc").unwrap()),
        SanType::DnsName(
            rcgen::Ia5String::try_from("kubernetes.default.svc.cluster.local").unwrap(),
        ),
    ];

    let kubernetes_service_ip = klights_types::first_usable_ipv4(service_cidr);
    if let Ok(ip_addr) = kubernetes_service_ip.parse::<std::net::IpAddr>() {
        sans.push(SanType::IpAddress(ip_addr));
    }

    let gateway_ip = derive_gateway_ip(pod_subnet);
    if let Ok(ip_addr) = gateway_ip.parse::<std::net::IpAddr>() {
        sans.push(SanType::IpAddress(ip_addr));
    }

    if let Some(ip_str) = host_ip
        && let Ok(ip_addr) = ip_str.parse::<std::net::IpAddr>()
    {
        sans.push(SanType::IpAddress(ip_addr));
    }

    if let Ok(ia5_hostname) = rcgen::Ia5String::try_from(node_name) {
        sans.push(SanType::DnsName(ia5_hostname));
    }

    if let Some(fqdn) = api_fqdn
        && let Ok(ia5) = rcgen::Ia5String::try_from(fqdn)
    {
        sans.push(SanType::DnsName(ia5));
    }

    sans
}

pub fn server_cert_matches_config(
    cert_pem: &str,
    service_cidr: &str,
    pod_subnet: &str,
    host_ip: Option<&str>,
    node_name: &str,
    api_fqdn: Option<&str>,
) -> bool {
    let Ok(actual_sans) = parse_certificate_sans(cert_pem) else {
        return false;
    };
    server_cert_san_types(service_cidr, pod_subnet, host_ip, node_name, api_fqdn)
        .into_iter()
        .all(|desired| match desired {
            SanType::DnsName(name) => actual_sans.dns_names.contains(name.as_ref()),
            SanType::IpAddress(addr) => actual_sans.ip_addrs.contains(&addr),
            _ => true,
        })
}

struct ParsedCertificateSans {
    dns_names: std::collections::HashSet<String>,
    ip_addrs: std::collections::HashSet<std::net::IpAddr>,
}

fn parse_certificate_sans(cert_pem: &str) -> Result<ParsedCertificateSans> {
    use x509_parser::prelude::*;

    let (_, pem) = x509_parser::pem::parse_x509_pem(cert_pem.as_bytes())
        .map_err(|e| anyhow::anyhow!("failed to parse server certificate PEM: {e}"))?;
    let (_, cert) = x509_parser::parse_x509_certificate(&pem.contents)
        .map_err(|e| anyhow::anyhow!("failed to parse server certificate DER: {e}"))?;
    let mut dns_names = std::collections::HashSet::new();
    let mut ip_addrs = std::collections::HashSet::new();
    for ext in cert.extensions() {
        if ext.oid != x509_parser::oid_registry::OID_X509_EXT_SUBJECT_ALT_NAME {
            continue;
        }
        let ParsedExtension::SubjectAlternativeName(san) = ext.parsed_extension() else {
            continue;
        };
        for name in &san.general_names {
            match name {
                GeneralName::DNSName(value) => {
                    dns_names.insert((*value).to_string());
                }
                GeneralName::IPAddress(bytes) => {
                    if let Some(addr) = ip_addr_from_san_bytes(bytes) {
                        ip_addrs.insert(addr);
                    }
                }
                _ => {}
            }
        }
    }
    Ok(ParsedCertificateSans {
        dns_names,
        ip_addrs,
    })
}

fn ip_addr_from_san_bytes(bytes: &[u8]) -> Option<std::net::IpAddr> {
    match bytes.len() {
        4 => Some(std::net::IpAddr::V4(std::net::Ipv4Addr::new(
            bytes[0], bytes[1], bytes[2], bytes[3],
        ))),
        16 => {
            let mut octets = [0u8; 16];
            octets.copy_from_slice(bytes);
            Some(std::net::IpAddr::V6(std::net::Ipv6Addr::from(octets)))
        }
        _ => None,
    }
}

/// Generate a server key pair and CSR for server auth.
/// Used by joining controlplanes that don't have the CA key yet.
/// Returns (server_key_pem, csr_pem_bytes).
pub fn generate_server_csr(
    service_cidr: &str,
    pod_subnet: &str,
    host_ip: Option<&str>,
    node_name: &str,
    api_fqdn: Option<&str>,
    valid_at: OffsetDateTime,
) -> Result<(String, Vec<u8>)> {
    let mut params = CertificateParams::default();
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "klights-server");
    params.distinguished_name = dn;
    params.subject_alt_names =
        server_cert_san_types(service_cidr, pod_subnet, host_ip, node_name, api_fqdn);
    set_certificate_validity(&mut params, valid_at);

    let key_pair = generate_rsa_key_pair()?;
    let csr = params.serialize_request(&key_pair)?;
    let csr_pem = csr
        .pem()
        .map_err(|e| anyhow::anyhow!("CSR PEM encoding failed: {e}"))?;

    Ok((key_pair.serialize_pem(), csr_pem.into_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_ca_params_is_ca() {
        let valid_at = OffsetDateTime::UNIX_EPOCH + Duration::days(20_000);
        let params = generate_ca_params_at(valid_at);
        assert!(matches!(params.is_ca, IsCa::Ca(_)));
        assert_eq!(params.not_before, valid_at);
        assert_eq!(
            params.not_after,
            valid_at + Duration::days(CERTIFICATE_VALIDITY_YEARS * 365)
        );
    }

    #[test]
    fn test_generate_ca_full_produces_valid_keypair() {
        let (cert, key, cert_pem, key_pem) = generate_ca_full().unwrap();
        assert!(cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(key_pem.contains("BEGIN"));

        // Verify that cert can sign other certs
        let (server_cert_pem, server_key_pem) = generate_server_cert(&cert, &key).unwrap();
        assert!(server_cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(server_key_pem.contains("BEGIN"));
    }

    #[test]
    fn test_generate_ca_full_returns_matching_certificate_object_and_pem() {
        let (cert, _, cert_pem, _) = generate_ca_full().unwrap();
        assert_eq!(cert.pem(), cert_pem);
    }

    #[test]
    fn injected_ca_material_is_sufficient_for_certificate_policy() {
        let valid_at = OffsetDateTime::UNIX_EPOCH + Duration::days(20_000);
        let (_, _, cert_pem, key_pem) = generate_ca_full_at(valid_at).unwrap();
        let authority =
            CertificateAuthority::from_pem(cert_pem.clone(), key_pem, valid_at).unwrap();

        let (admin_cert_pem, admin_key_pem) = authority.issue_admin_certificate(valid_at).unwrap();

        assert_eq!(authority.certificate_pem(), cert_pem);
        assert!(admin_cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(admin_key_pem.contains("BEGIN"));
    }

    #[test]
    fn test_generate_server_cert_signature_verifies_against_generated_ca() {
        let (ca_cert, ca_key, ca_pem, _) = generate_ca_full().unwrap();
        let (server_cert_pem, _) = generate_server_cert(&ca_cert, &ca_key).unwrap();

        let (_, ca_pem) = x509_parser::pem::parse_x509_pem(ca_pem.as_bytes()).unwrap();
        let (_, ca_x509) = x509_parser::parse_x509_certificate(&ca_pem.contents).unwrap();
        let (_, server_pem) = x509_parser::pem::parse_x509_pem(server_cert_pem.as_bytes()).unwrap();
        let (_, server_x509) = x509_parser::parse_x509_certificate(&server_pem.contents).unwrap();

        ca_x509
            .verify_signature(None)
            .expect("generated CA self-signature must verify");
        server_x509
            .verify_signature(Some(ca_x509.public_key()))
            .expect("server certificate signature must verify against generated CA");
    }

    #[test]
    fn test_generate_server_cert_has_localhost_cn() {
        let (ca_cert, ca_key, _, _) = generate_ca_full().unwrap();
        let (server_cert_pem, _) = generate_server_cert(&ca_cert, &ca_key).unwrap();

        let der = pem_to_der(&server_cert_pem);
        // user_from_cert extracts CN — verify it's klights-server
        let user = super::super::user::user_from_cert(&der).unwrap();
        assert_eq!(user.username, "klights-server");
    }

    #[test]
    fn test_generate_admin_cert_has_system_masters_org() {
        let (ca_cert, ca_key, _, _) = generate_ca_full().unwrap();
        let (admin_cert_pem, _) = generate_admin_cert(&ca_cert, &ca_key).unwrap();

        let der = pem_to_der(&admin_cert_pem);
        let user = super::super::user::user_from_cert(&der).unwrap();
        assert_eq!(user.username, "klights-admin");
        assert!(user.groups.contains(&"system:masters".to_string()));
    }

    // Helper for tests
    fn pem_to_der(pem_str: &str) -> Vec<u8> {
        use x509_parser::pem::Pem;
        let (pem, _) = Pem::read(std::io::Cursor::new(pem_str.as_bytes())).unwrap();
        pem.contents
    }

    fn extract_dns_sans(cert_pem: &str) -> Vec<String> {
        use x509_parser::prelude::*;
        let (_, pem) = x509_parser::pem::parse_x509_pem(cert_pem.as_bytes()).unwrap();
        let (_, cert) = x509_parser::parse_x509_certificate(&pem.contents).unwrap();
        let mut dns_names = Vec::new();
        for ext in cert.extensions() {
            if ext.oid == x509_parser::oid_registry::OID_X509_EXT_SUBJECT_ALT_NAME
                && let ParsedExtension::SubjectAlternativeName(san) = ext.parsed_extension()
            {
                for gn in &san.general_names {
                    if let GeneralName::DNSName(s) = gn {
                        dns_names.push(s.to_string());
                    }
                }
            }
        }
        dns_names
    }

    #[test]
    fn test_server_cert_includes_api_fqdn_san() {
        let (ca_cert, ca_key, _, _) = generate_ca_full().unwrap();
        let (cert_pem, _) = generate_server_cert_with_config(
            &ca_cert,
            &ca_key,
            "10.43.128.0/17",
            "10.43.0.0/17",
            None,
            "test-node",
            Some("klights.example.com"),
        )
        .unwrap();

        let dns_sans = extract_dns_sans(&cert_pem);
        assert!(
            dns_sans.contains(&"klights.example.com".to_string()),
            "api_fqdn should appear in DNS SANs, got: {dns_sans:?}"
        );
    }

    #[test]
    fn test_server_cert_without_api_fqdn_san_unchanged() {
        let (ca_cert, ca_key, _, _) = generate_ca_full().unwrap();
        let (cert_pem, _) = generate_server_cert_with_config(
            &ca_cert,
            &ca_key,
            "10.43.128.0/17",
            "10.43.0.0/17",
            None,
            "test-node",
            None,
        )
        .unwrap();

        let dns_sans = extract_dns_sans(&cert_pem);
        // Standard K8s DNS names + hostname should still be present
        assert!(dns_sans.contains(&"localhost".to_string()));
        assert!(dns_sans.contains(&"kubernetes".to_string()));
        assert!(dns_sans.contains(&"test-node".to_string()));
        // No extra FQDN
        assert!(!dns_sans.iter().any(|s| s.contains("example.com")));
    }
}
