//! Certificate generation for klights CA, server, and admin certs.
//!
//! Provides RSA key pair generation, CA certificate creation, and signed certificates
//! for server and admin use.

use anyhow::Result;
use rand_core::OsRng;
use rcgen::{CertificateParams, DistinguishedName, DnType, IsCa, KeyPair, SanType};
use rsa::{RsaPrivateKey, pkcs8::EncodePrivateKey};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use time::{Duration, OffsetDateTime};

const CERTIFICATE_VALIDITY_YEARS: i64 = 10;
pub const API_PROXY_COMMON_NAME_PREFIX: &str = "system:klights:api-proxy:";
pub const APISERVICE_PROXY_COMMON_NAME: &str = "system:klights:apiservice-proxy";
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

pub fn api_proxy_common_name(node_name: &str) -> String {
    format!("{API_PROXY_COMMON_NAME_PREFIX}{node_name}")
}

/// Certificate paths returned by initialization.
#[derive(Clone, Debug)]
pub struct CertPaths {
    pub ca_cert: String,
    pub server_cert: String,
    pub server_key: String,
}

/// Result of certificate initialization.
#[derive(Clone, Debug)]
pub enum CertInitResult {
    /// All certificates are ready.
    Complete(CertPaths),
    /// CA cert exists but CA key is missing. The caller must get the CSR
    /// signed by the leader (e.g. via SignControlplaneCsr RPC), write
    /// ca.crt/ca.key/server.crt to disk, then the node can proceed.
    NeedsCsrSign(PendingCsr),
}

/// A pending CSR that needs leader signing.
#[derive(Clone, Debug)]
pub struct PendingCsr {
    /// Path where the server private key was written.
    pub server_key_path: std::path::PathBuf,
    /// PEM-encoded CSR to send to the leader.
    pub server_csr_pem: Vec<u8>,
    /// Path to the etc directory (for writing signed cert later).
    pub etc_dir: std::path::PathBuf,
}

#[derive(Clone, Debug)]
pub struct InitCertificateRequest<'a> {
    pub tls_port: u16,
    pub context_name: &'a str,
    pub service_cidr: &'a str,
    pub pod_subnet: &'a str,
    pub etc_dir_path: &'a str,
    pub node_name: &'a str,
    pub host_ip: Option<String>,
    /// Optional FQDN included as a DNS SAN in the server certificate.
    pub api_fqdn: Option<&'a str>,
    /// Whether this node may create a new local cluster CA when no CA
    /// material exists. Seed leaders/controlplanes set this; joining
    /// controlplanes must use leader CSR signing instead.
    pub allow_local_ca_generation: bool,
}

/// Initialize CA and server/client certificates. Creates etc directory if needed.
///
/// `host_ip` is supplied by the caller (bootstrap discovers it once per
/// process and passes the same value to NetworkPlane). Pass `None` to
/// skip the host IP SAN entry — the cert is still valid, just without
/// the additional IP claim.
pub async fn init_certificates(
    request: InitCertificateRequest<'_>,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) -> Result<CertInitResult> {
    let InitCertificateRequest {
        tls_port,
        context_name,
        service_cidr,
        pod_subnet,
        etc_dir_path,
        node_name,
        host_ip,
        api_fqdn,
        allow_local_ca_generation,
    } = request;

    tracing::info!("Initializing certificates...");
    let etc_dir = Path::new(etc_dir_path);
    let etc_dir_for_create = etc_dir.to_path_buf();
    task_supervisor
        .run_blocking_file_keyed("cert_create_etc_dir", etc_dir_path.to_string(), move || {
            fs::create_dir_all(&etc_dir_for_create)?;
            // The etc dir holds private keys (ca.key/server.key/admin.key) and
            // the SA signing key — restrict to the owner (0700) so co-located
            // local users cannot read key material.
            fs::set_permissions(&etc_dir_for_create, PermissionsExt::from_mode(0o700))
        })
        .await??;
    tracing::info!("Etc directory created/verified: {}", etc_dir_path);

    let ca_cert_path = etc_dir.join("ca.crt");
    let ca_key_path = etc_dir.join("ca.key");
    let server_cert_path = etc_dir.join("server.crt");
    let server_key_path = etc_dir.join("server.key");
    let admin_cert_path = etc_dir.join("admin.crt");
    let admin_key_path = etc_dir.join("admin.key");
    let service_account_signing_key_path = etc_dir.join("service-account-signing.key");

    // Load or generate CA
    let ca_cert_exists =
        path_exists_keyed(task_supervisor, &ca_cert_path, "cert_check_ca_cert").await?;
    let ca_key_exists =
        path_exists_keyed(task_supervisor, &ca_key_path, "cert_check_ca_key").await?;
    let (ca_cert_pem, ca_key_pem, ca_cert, ca_key) = if ca_cert_exists && ca_key_exists {
        tracing::info!("Loading existing CA certificates");
        let cert_pem =
            read_utf8_file_keyed(task_supervisor, &ca_cert_path, "cert_read_ca_cert").await?;
        let key_pem =
            read_utf8_file_keyed(task_supervisor, &ca_key_path, "cert_read_ca_key").await?;
        let key = KeyPair::from_pem(&key_pem)?;
        let params = generate_ca_params();
        let cert = params.self_signed(&key)?;
        (cert_pem, key_pem, cert, key)
    } else if ca_cert_exists || !allow_local_ca_generation {
        // CA cert exists but CA key is missing (e.g. joining controlplane
        // with only ca.crt seeded), or neither exists (e.g. joining with
        // --skip-ca).  Generate a server key + CSR.  The caller must get
        // the CSR signed by the leader via SignControlplaneCsr RPC, then
        // write ca.crt/ca.key/server.crt before proceeding.
        tracing::info!(
            ca_cert_exists,
            ca_key_exists,
            "Generating server CSR (CA key not available locally)"
        );
        let (server_key_pem, server_csr_pem) = generate_server_csr(
            service_cidr,
            pod_subnet,
            host_ip.as_deref(),
            node_name,
            api_fqdn,
        )?;
        write_file_keyed(
            task_supervisor,
            &server_key_path,
            server_key_pem.clone(),
            "cert_write_server_key_csr",
        )
        .await?;
        return Ok(CertInitResult::NeedsCsrSign(PendingCsr {
            server_key_path,
            server_csr_pem,
            etc_dir: etc_dir.to_path_buf(),
        }));
    } else {
        tracing::info!("Generating new CA certificates");
        let (cert, key, cert_pem, key_pem) = generate_ca_full()?;
        write_file_keyed(
            task_supervisor,
            &ca_cert_path,
            cert_pem.clone(),
            "cert_write_ca_cert",
        )
        .await?;
        write_file_keyed(
            task_supervisor,
            &ca_key_path,
            key_pem.clone(),
            "cert_write_ca_key",
        )
        .await?;
        (cert_pem, key_pem, cert, key)
    };

    ensure_service_account_signing_key(
        task_supervisor,
        &service_account_signing_key_path,
        allow_local_ca_generation,
    )
    .await?;

    // host_ip supplied by caller — discovered once at bootstrap and
    // shared with NetworkPlane.

    // Load or generate server cert
    let server_cert_exists =
        path_exists_keyed(task_supervisor, &server_cert_path, "cert_check_server_cert").await?;
    let server_key_exists =
        path_exists_keyed(task_supervisor, &server_key_path, "cert_check_server_key").await?;
    let (server_cert_pem, server_key_pem) = if server_cert_exists && server_key_exists {
        let cert =
            read_utf8_file_keyed(task_supervisor, &server_cert_path, "cert_read_server_cert")
                .await?;
        let key =
            read_utf8_file_keyed(task_supervisor, &server_key_path, "cert_read_server_key").await?;
        if server_cert_matches_config(
            &cert,
            service_cidr,
            pod_subnet,
            host_ip.as_deref(),
            node_name,
            api_fqdn,
        ) {
            tracing::info!("Loading existing server certificates");
            (cert, key)
        } else {
            tracing::info!(
                "Regenerating server certificates because existing SANs do not match current API endpoints"
            );
            let (cert, key) = generate_server_cert_with_config(
                &ca_cert,
                &ca_key,
                service_cidr,
                pod_subnet,
                host_ip.clone(),
                node_name,
                api_fqdn,
            )?;
            write_file_keyed(
                task_supervisor,
                &server_cert_path,
                cert.clone(),
                "cert_write_server_cert",
            )
            .await?;
            write_file_keyed(
                task_supervisor,
                &server_key_path,
                key.clone(),
                "cert_write_server_key",
            )
            .await?;
            (cert, key)
        }
    } else {
        tracing::info!(
            "Generating new server certificates with service_cidr={}, pod_subnet={}, host_ip={:?}",
            service_cidr,
            pod_subnet,
            host_ip
        );
        let (cert, key) = generate_server_cert_with_config(
            &ca_cert,
            &ca_key,
            service_cidr,
            pod_subnet,
            host_ip.clone(),
            node_name,
            api_fqdn,
        )?;
        write_file_keyed(
            task_supervisor,
            &server_cert_path,
            cert.clone(),
            "cert_write_server_cert",
        )
        .await?;
        write_file_keyed(
            task_supervisor,
            &server_key_path,
            key.clone(),
            "cert_write_server_key",
        )
        .await?;
        (cert, key)
    };

    // Load or generate admin cert
    let admin_cert_exists =
        path_exists_keyed(task_supervisor, &admin_cert_path, "cert_check_admin_cert").await?;
    let admin_key_exists =
        path_exists_keyed(task_supervisor, &admin_key_path, "cert_check_admin_key").await?;
    let (admin_cert_pem, admin_key_pem) = if admin_cert_exists && admin_key_exists {
        tracing::info!("Loading existing admin certificates");
        let cert =
            read_utf8_file_keyed(task_supervisor, &admin_cert_path, "cert_read_admin_cert").await?;
        let key =
            read_utf8_file_keyed(task_supervisor, &admin_key_path, "cert_read_admin_key").await?;
        (cert, key)
    } else {
        tracing::info!("Generating new admin certificates");
        let (cert, key) = generate_admin_cert(&ca_cert, &ca_key)?;
        write_file_keyed(
            task_supervisor,
            &admin_cert_path,
            cert.clone(),
            "cert_write_admin_cert",
        )
        .await?;
        write_file_keyed(
            task_supervisor,
            &admin_key_path,
            key.clone(),
            "cert_write_admin_key",
        )
        .await?;
        (cert, key)
    };

    let _api_proxy_identity = ensure_api_proxy_certificate_from_pem(
        task_supervisor,
        etc_dir,
        &ca_cert_pem,
        &ca_key_pem,
        node_name,
    )
    .await?;
    let _apiservice_proxy_identity = ensure_apiservice_proxy_certificate_from_pem(
        task_supervisor,
        etc_dir,
        &ca_cert_pem,
        &ca_key_pem,
    )
    .await?;

    // Generate kubeconfig (always regenerate to pick up cert/port changes)
    let kubeconfig = super::kubeconfig::generate_kubeconfig(super::kubeconfig::KubeconfigParams {
        ca_cert: &ca_cert_pem,
        admin_cert: &admin_cert_pem,
        admin_key: &admin_key_pem,
        tls_port,
        context_name,
        host_ip: host_ip.as_deref(),
        pod_subnet,
    })?;

    // Write kubeconfig under namespace-local etc dir.
    let kubeconfig_path = etc_dir.join("kubeconfig.yaml");
    write_file_keyed(
        task_supervisor,
        &kubeconfig_path,
        kubeconfig,
        "cert_write_kubeconfig",
    )
    .await?;

    tracing::info!("Wrote kubeconfig to {}", kubeconfig_path.display());
    tracing::info!("Use: export KUBECONFIG={}", kubeconfig_path.display());

    Ok(CertInitResult::Complete(CertPaths {
        ca_cert: ca_cert_pem,
        server_cert: server_cert_pem,
        server_key: server_key_pem,
    }))
}

async fn path_exists_keyed(
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
    path: &Path,
    label: &'static str,
) -> Result<bool> {
    let path_buf = path.to_path_buf();
    let key = path.to_string_lossy().into_owned();
    task_supervisor
        .run_blocking_file_keyed(label, key, move || path_buf.exists())
        .await
}

async fn read_utf8_file_keyed(
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
    path: &Path,
    label: &'static str,
) -> Result<String> {
    let path_buf = path.to_path_buf();
    let key = path.to_string_lossy().into_owned();
    Ok(task_supervisor
        .run_blocking_file_keyed(label, key, move || std::fs::read_to_string(path_buf))
        .await??)
}

async fn write_file_keyed(
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
    path: &Path,
    contents: String,
    label: &'static str,
) -> Result<()> {
    let path_buf = path.to_path_buf();
    let key = path.to_string_lossy().into_owned();
    task_supervisor
        .run_blocking_file_keyed(label, key, move || {
            std::fs::write(&path_buf, contents)?;
            // Private key files must not be world/group readable.
            if path_buf.extension().is_some_and(|ext| ext == "key") {
                std::fs::set_permissions(&path_buf, PermissionsExt::from_mode(0o600))?;
            }
            std::io::Result::Ok(())
        })
        .await??;
    Ok(())
}

async fn ensure_service_account_signing_key(
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
    path: &Path,
    allow_local_generation: bool,
) -> Result<()> {
    let exists = path_exists_keyed(task_supervisor, path, "cert_check_sa_signing_key").await?;
    if exists {
        let pem = read_utf8_file_keyed(task_supervisor, path, "cert_read_sa_signing_key").await?;
        validate_service_account_signing_key(path, &pem)?;
        set_key_permissions_keyed(task_supervisor, path, "cert_chmod_sa_signing_key").await?;
        return Ok(());
    }

    if !allow_local_generation {
        anyhow::bail!(
            "ServiceAccount signing key {} is missing; joining controlplanes and replicas must receive it from the leader during CSR bootstrap",
            path.display()
        );
    }

    tracing::info!(
        path = %path.display(),
        "Generating dedicated ServiceAccount signing key"
    );
    let pem = generate_service_account_signing_key_pem()?;
    write_file_keyed(task_supervisor, path, pem, "cert_write_sa_signing_key").await
}

fn validate_service_account_signing_key(path: &Path, pem: &str) -> Result<()> {
    use rsa::pkcs1::DecodeRsaPrivateKey;
    use rsa::pkcs8::DecodePrivateKey;

    if pem.trim().is_empty() {
        anyhow::bail!(
            "ServiceAccount signing key {} is invalid: file is empty. delete this file to allow klights leader bootstrap to regenerate it",
            path.display()
        );
    }

    RsaPrivateKey::from_pkcs8_pem(pem)
        .or_else(|_| RsaPrivateKey::from_pkcs1_pem(pem))
        .map(|_| ())
        .map_err(|err| {
            anyhow::anyhow!(
                "ServiceAccount signing key {} is invalid: {err}. delete this file to allow klights leader bootstrap to regenerate it",
                path.display()
            )
        })
}

fn generate_service_account_signing_key_pem() -> Result<String> {
    let private_key = RsaPrivateKey::new(&mut OsRng, 2048)
        .map_err(|e| anyhow::anyhow!("ServiceAccount RSA key generation failed: {}", e))?;
    Ok(private_key
        .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
        .map_err(|e| anyhow::anyhow!("ServiceAccount PKCS#8 serialization failed: {}", e))?
        .to_string())
}

async fn set_key_permissions_keyed(
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
    path: &Path,
    label: &'static str,
) -> Result<()> {
    let path_buf = path.to_path_buf();
    let key = path.to_string_lossy().into_owned();
    task_supervisor
        .run_blocking_file_keyed(label, key, move || {
            std::fs::set_permissions(&path_buf, PermissionsExt::from_mode(0o600))
        })
        .await??;
    Ok(())
}

/// Generate CA certificate parameters.
pub fn generate_ca_params() -> CertificateParams {
    let mut params = CertificateParams::default();

    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "klights-ca");
    params.distinguished_name = dn;

    params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params.not_before = OffsetDateTime::now_utc();
    params.not_after = OffsetDateTime::now_utc() + Duration::days(CERTIFICATE_VALIDITY_YEARS * 365);
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
pub fn generate_ca_full() -> Result<(rcgen::Certificate, KeyPair, String, String)> {
    let params = generate_ca_params();
    let key_pair = generate_rsa_key_pair()?;
    let cert = params.self_signed(&key_pair)?;
    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();

    Ok((cert, key_pair, cert_pem, key_pem))
}

/// Generate a server certificate with dynamic service CIDR and optional host IP.
///
/// This version allows configuration for testing and multi-namespace deployments.
pub fn generate_server_cert_with_config(
    ca_cert: &rcgen::Certificate,
    ca_key: &KeyPair,
    service_cidr: &str,
    pod_subnet: &str,
    host_ip: Option<String>,
    node_name: &str,
    api_fqdn: Option<&str>,
) -> Result<(String, String)> {
    let mut params = CertificateParams::default();

    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "klights-server");
    params.distinguished_name = dn;

    params.subject_alt_names = server_cert_san_types(
        service_cidr,
        pod_subnet,
        host_ip.as_deref(),
        node_name,
        api_fqdn,
    );

    params.not_before = OffsetDateTime::now_utc();
    params.not_after = OffsetDateTime::now_utc() + Duration::days(CERTIFICATE_VALIDITY_YEARS * 365);

    let key_pair = generate_rsa_key_pair()?;
    let cert = params.signed_by(&key_pair, ca_cert, ca_key)?;

    Ok((cert.pem(), key_pair.serialize_pem()))
}

/// Generate a server certificate with hardcoded defaults (for backward compatibility).
///
/// DEPRECATED: Use `generate_server_cert_with_config` instead.
/// Only used in tests.
#[cfg(test)]
pub fn generate_server_cert(
    ca_cert: &rcgen::Certificate,
    ca_key: &KeyPair,
) -> Result<(String, String)> {
    generate_server_cert_with_config(
        ca_cert,
        ca_key,
        "10.43.128.0/17",
        "10.43.0.0/17",
        None,
        "test-node",
        None,
    )
}

/// Generate an admin certificate signed by the CA.
pub fn generate_admin_cert(
    ca_cert: &rcgen::Certificate,
    ca_key: &KeyPair,
) -> Result<(String, String)> {
    let mut params = CertificateParams::default();

    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "klights-admin");
    dn.push(DnType::OrganizationName, "system:masters");
    params.distinguished_name = dn;

    params.not_before = OffsetDateTime::now_utc();
    params.not_after = OffsetDateTime::now_utc() + Duration::days(CERTIFICATE_VALIDITY_YEARS * 365);

    let key_pair = generate_rsa_key_pair()?;
    let cert = params.signed_by(&key_pair, ca_cert, ca_key)?;

    Ok((cert.pem(), key_pair.serialize_pem()))
}

/// Generate the dedicated follower API-proxy client certificate.
///
/// This credential proves "trusted follower proxy" to a leader. It is not an
/// admin credential and is not valid as an API server serving certificate.
pub fn generate_api_proxy_cert(
    ca_cert: &rcgen::Certificate,
    ca_key: &KeyPair,
    node_name: &str,
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

    params.not_before = OffsetDateTime::now_utc();
    params.not_after = OffsetDateTime::now_utc() + Duration::days(CERTIFICATE_VALIDITY_YEARS * 365);

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
) -> Result<(String, String)> {
    let mut params = CertificateParams::default();

    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, APISERVICE_PROXY_COMMON_NAME);
    dn.push(DnType::OrganizationName, APISERVICE_PROXY_GROUP);
    params.distinguished_name = dn;
    params.key_usages = vec![
        rcgen::KeyUsagePurpose::DigitalSignature,
        rcgen::KeyUsagePurpose::KeyEncipherment,
    ];
    params
        .extended_key_usages
        .push(rcgen::ExtendedKeyUsagePurpose::ClientAuth);

    params.not_before = OffsetDateTime::now_utc();
    params.not_after = OffsetDateTime::now_utc() + Duration::days(CERTIFICATE_VALIDITY_YEARS * 365);

    let key_pair = generate_rsa_key_pair()?;
    let cert = params.signed_by(&key_pair, ca_cert, ca_key)?;

    Ok((cert.pem(), key_pair.serialize_pem()))
}

pub async fn ensure_api_proxy_certificate_from_pem(
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
    etc_dir: &Path,
    ca_cert_pem: &str,
    ca_key_pem: &str,
    node_name: &str,
) -> Result<(String, String)> {
    let ca_key = KeyPair::from_pem(ca_key_pem)?;
    let ca_params = CertificateParams::from_ca_cert_pem(ca_cert_pem)?;
    let ca_cert = ca_params.self_signed(&ca_key)?;

    let cert_path = etc_dir.join("api-proxy.crt");
    let key_path = etc_dir.join("api-proxy.key");
    let cert_exists =
        path_exists_keyed(task_supervisor, &cert_path, "cert_check_api_proxy_cert").await?;
    let key_exists =
        path_exists_keyed(task_supervisor, &key_path, "cert_check_api_proxy_key").await?;

    if cert_exists && key_exists {
        let cert =
            read_utf8_file_keyed(task_supervisor, &cert_path, "cert_read_api_proxy_cert").await?;
        let key =
            read_utf8_file_keyed(task_supervisor, &key_path, "cert_read_api_proxy_key").await?;
        if api_proxy_cert_and_key_match_config(&cert, &key, node_name) {
            return Ok((cert, key));
        }
        tracing::info!("Regenerating api-proxy certificate because existing identity is invalid");
    }

    let (cert, key) = generate_api_proxy_cert(&ca_cert, &ca_key, node_name)?;
    write_file_keyed(
        task_supervisor,
        &cert_path,
        cert.clone(),
        "cert_write_api_proxy_cert",
    )
    .await?;
    write_file_keyed(
        task_supervisor,
        &key_path,
        key.clone(),
        "cert_write_api_proxy_key",
    )
    .await?;
    Ok((cert, key))
}

pub async fn ensure_apiservice_proxy_certificate_from_pem(
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
    etc_dir: &Path,
    ca_cert_pem: &str,
    ca_key_pem: &str,
) -> Result<(String, String)> {
    let ca_key = KeyPair::from_pem(ca_key_pem)?;
    let ca_params = CertificateParams::from_ca_cert_pem(ca_cert_pem)?;
    let ca_cert = ca_params.self_signed(&ca_key)?;

    let cert_path = etc_dir.join("apiservice-proxy.crt");
    let key_path = etc_dir.join("apiservice-proxy.key");
    let cert_exists = path_exists_keyed(
        task_supervisor,
        &cert_path,
        "cert_check_apiservice_proxy_cert",
    )
    .await?;
    let key_exists = path_exists_keyed(
        task_supervisor,
        &key_path,
        "cert_check_apiservice_proxy_key",
    )
    .await?;

    if cert_exists && key_exists {
        let cert = read_utf8_file_keyed(
            task_supervisor,
            &cert_path,
            "cert_read_apiservice_proxy_cert",
        )
        .await?;
        let key =
            read_utf8_file_keyed(task_supervisor, &key_path, "cert_read_apiservice_proxy_key")
                .await?;
        if apiservice_proxy_cert_and_key_match_config(&cert, &key) {
            return Ok((cert, key));
        }
        tracing::info!(
            "Regenerating apiservice-proxy certificate because existing identity is invalid"
        );
    }

    let (cert, key) = generate_apiservice_proxy_cert(&ca_cert, &ca_key)?;
    write_file_keyed(
        task_supervisor,
        &cert_path,
        cert.clone(),
        "cert_write_apiservice_proxy_cert",
    )
    .await?;
    write_file_keyed(
        task_supervisor,
        &key_path,
        key.clone(),
        "cert_write_apiservice_proxy_key",
    )
    .await?;
    Ok((cert, key))
}

fn api_proxy_cert_and_key_match_config(cert_pem: &str, key_pem: &str, node_name: &str) -> bool {
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

fn apiservice_proxy_cert_and_key_match_config(cert_pem: &str, key_pem: &str) -> bool {
    let der = match first_pem_cert_der(cert_pem) {
        Some(der) => der,
        None => return false,
    };
    let Ok(user) = super::user::user_from_cert(&der) else {
        return false;
    };
    if user.username != APISERVICE_PROXY_COMMON_NAME
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

fn parse_certificate_extended_key_usage(cert_pem: &str) -> Option<(bool, bool)> {
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
    crate::utils::derive_first_ip(pod_subnet)
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

    let kubernetes_service_ip =
        crate::controllers::kube_service::derive_kubernetes_service_ip(service_cidr);
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

fn server_cert_matches_config(
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
) -> Result<(String, Vec<u8>)> {
    let mut params = CertificateParams::default();
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "klights-server");
    params.distinguished_name = dn;
    params.subject_alt_names =
        server_cert_san_types(service_cidr, pod_subnet, host_ip, node_name, api_fqdn);
    params.not_before = OffsetDateTime::now_utc();
    params.not_after = OffsetDateTime::now_utc() + Duration::days(CERTIFICATE_VALIDITY_YEARS * 365);

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
        let params = generate_ca_params();
        assert!(matches!(params.is_ca, IsCa::Ca(_)));
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

    fn extract_ip_sans(cert_pem: &str) -> Vec<String> {
        use x509_parser::prelude::*;
        let (_, pem) = x509_parser::pem::parse_x509_pem(cert_pem.as_bytes()).unwrap();
        let (_, cert) = x509_parser::parse_x509_certificate(&pem.contents).unwrap();
        let mut ip_addrs = Vec::new();
        for ext in cert.extensions() {
            if ext.oid == x509_parser::oid_registry::OID_X509_EXT_SUBJECT_ALT_NAME
                && let ParsedExtension::SubjectAlternativeName(san) = ext.parsed_extension()
            {
                for gn in &san.general_names {
                    if let GeneralName::IPAddress(bytes) = gn {
                        let addr = match bytes.len() {
                            4 => Some(std::net::IpAddr::V4(std::net::Ipv4Addr::new(
                                bytes[0], bytes[1], bytes[2], bytes[3],
                            ))),
                            16 => {
                                let mut octets = [0u8; 16];
                                octets.copy_from_slice(bytes);
                                Some(std::net::IpAddr::V6(std::net::Ipv6Addr::from(octets)))
                            }
                            _ => None,
                        };
                        if let Some(addr) = addr {
                            ip_addrs.push(addr.to_string());
                        }
                    }
                }
            }
        }
        ip_addrs
    }

    #[tokio::test]
    async fn init_certificates_writes_dedicated_api_proxy_client_certificate() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let etc_dir = dir.path().join("etc");
        let supervisor = crate::task_supervisor::TaskSupervisor::new(Default::default());
        init_certificates(
            InitCertificateRequest {
                tls_port: 7679,
                context_name: "klights-proxy-cert-test",
                service_cidr: "10.51.0.0/24",
                pod_subnet: "10.50.0.0/24",
                etc_dir_path: etc_dir.to_str().unwrap(),
                node_name: "mn-controlplane2",
                host_ip: Some("10.99.0.14".to_string()),
                api_fqdn: None,
                allow_local_ca_generation: true,
            },
            &supervisor,
        )
        .await
        .unwrap();

        let proxy_cert_pem = std::fs::read_to_string(etc_dir.join("api-proxy.crt"))
            .expect("dedicated api-proxy.crt must be generated");
        let proxy_key_mode = std::fs::metadata(etc_dir.join("api-proxy.key"))
            .expect("dedicated api-proxy.key must be generated")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(proxy_key_mode, 0o600, "api-proxy.key must be owner-only");

        let der = pem_to_der(&proxy_cert_pem);
        let user = super::super::user::user_from_cert(&der).unwrap();
        assert_eq!(user.username, "system:klights:api-proxy:mn-controlplane2");
        assert!(
            !user.groups.contains(&"system:masters".to_string()),
            "api proxy credential must not carry admin group"
        );
        let (server_auth, client_auth) = parse_certificate_extended_key_usage(&proxy_cert_pem)
            .expect("api proxy cert must include EKU");
        assert!(
            !server_auth,
            "api proxy cert must not be valid for API serving"
        );
        assert!(
            client_auth,
            "api proxy cert must be valid for mTLS client auth"
        );
    }

    #[tokio::test]
    async fn init_certificates_writes_dedicated_apiservice_proxy_client_certificate() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let etc_dir = dir.path().join("etc");
        let supervisor = crate::task_supervisor::TaskSupervisor::new(Default::default());
        init_certificates(
            InitCertificateRequest {
                tls_port: 7679,
                context_name: "klights-apiservice-proxy-cert-test",
                service_cidr: "10.51.0.0/24",
                pod_subnet: "10.50.0.0/24",
                etc_dir_path: etc_dir.to_str().unwrap(),
                node_name: "mn-controlplane2",
                host_ip: Some("10.99.0.14".to_string()),
                api_fqdn: None,
                allow_local_ca_generation: true,
            },
            &supervisor,
        )
        .await
        .unwrap();

        let proxy_cert_pem = std::fs::read_to_string(etc_dir.join("apiservice-proxy.crt"))
            .expect("dedicated apiservice-proxy.crt must be generated");
        let proxy_key_mode = std::fs::metadata(etc_dir.join("apiservice-proxy.key"))
            .expect("dedicated apiservice-proxy.key must be generated")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(
            proxy_key_mode, 0o600,
            "apiservice-proxy.key must be owner-only"
        );

        let der = pem_to_der(&proxy_cert_pem);
        let user = super::super::user::user_from_cert(&der).unwrap();
        assert_eq!(user.username, "system:klights:apiservice-proxy");
        assert_eq!(
            user.groups,
            vec!["system:klights:apiservice-proxies".to_string()],
            "APIService proxy credential must use a dedicated non-admin group"
        );
        assert!(
            !user.groups.contains(&"system:masters".to_string()),
            "APIService proxy credential must not carry admin group"
        );
        let (server_auth, client_auth) = parse_certificate_extended_key_usage(&proxy_cert_pem)
            .expect("APIService proxy cert must include EKU");
        assert!(
            !server_auth,
            "APIService proxy cert must not be valid for API serving"
        );
        assert!(
            client_auth,
            "APIService proxy cert must be valid for mTLS client auth"
        );
    }

    #[tokio::test]
    async fn init_certificates_generates_dedicated_service_account_signing_key_for_seed_leader() {
        use rsa::pkcs8::DecodePrivateKey;
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let etc_dir = dir.path().join("etc");
        let supervisor = crate::task_supervisor::TaskSupervisor::new(Default::default());
        init_certificates(
            InitCertificateRequest {
                tls_port: 7679,
                context_name: "klights-sa-signer-test",
                service_cidr: "10.51.0.0/24",
                pod_subnet: "10.50.0.0/24",
                etc_dir_path: etc_dir.to_str().unwrap(),
                node_name: "mn-controlplane1",
                host_ip: Some("10.99.0.10".to_string()),
                api_fqdn: None,
                allow_local_ca_generation: true,
            },
            &supervisor,
        )
        .await
        .unwrap();

        let signer_path = etc_dir.join("service-account-signing.key");
        let signer_pem = std::fs::read_to_string(&signer_path)
            .expect("seed leader bootstrap must generate dedicated SA signing key");
        RsaPrivateKey::from_pkcs8_pem(&signer_pem)
            .expect("SA signing key must be an RSA PKCS#8 private key");
        let mode = std::fs::metadata(&signer_path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "SA signing key must be owner-only");
    }

    #[tokio::test]
    async fn init_certificates_repairs_missing_service_account_signing_key_with_existing_ca() {
        use rsa::pkcs8::DecodePrivateKey;

        let dir = tempfile::tempdir().unwrap();
        let etc_dir = dir.path().join("etc");
        std::fs::create_dir_all(&etc_dir).unwrap();
        let (_, _, ca_cert_pem, ca_key_pem) = generate_ca_full().unwrap();
        std::fs::write(etc_dir.join("ca.crt"), ca_cert_pem).unwrap();
        std::fs::write(etc_dir.join("ca.key"), ca_key_pem).unwrap();

        let supervisor = crate::task_supervisor::TaskSupervisor::new(Default::default());
        init_certificates(
            InitCertificateRequest {
                tls_port: 7679,
                context_name: "klights-sa-signer-repair-test",
                service_cidr: "10.51.0.0/24",
                pod_subnet: "10.50.0.0/24",
                etc_dir_path: etc_dir.to_str().unwrap(),
                node_name: "mn-controlplane1",
                host_ip: Some("10.99.0.10".to_string()),
                api_fqdn: None,
                allow_local_ca_generation: true,
            },
            &supervisor,
        )
        .await
        .unwrap();

        let signer_pem = std::fs::read_to_string(etc_dir.join("service-account-signing.key"))
            .expect("leader startup must repair a missing dedicated SA signing key");
        RsaPrivateKey::from_pkcs8_pem(&signer_pem)
            .expect("repaired SA signing key must be an RSA PKCS#8 private key");
    }

    #[tokio::test]
    async fn init_certificates_hard_fails_invalid_existing_service_account_signing_key() {
        let dir = tempfile::tempdir().unwrap();
        let etc_dir = dir.path().join("etc");
        std::fs::create_dir_all(&etc_dir).unwrap();
        let (_, _, ca_cert_pem, ca_key_pem) = generate_ca_full().unwrap();
        std::fs::write(etc_dir.join("ca.crt"), ca_cert_pem).unwrap();
        std::fs::write(etc_dir.join("ca.key"), ca_key_pem).unwrap();
        let signer_path = etc_dir.join("service-account-signing.key");
        std::fs::write(&signer_path, "not a private key").unwrap();

        let supervisor = crate::task_supervisor::TaskSupervisor::new(Default::default());
        let err = init_certificates(
            InitCertificateRequest {
                tls_port: 7679,
                context_name: "klights-sa-signer-invalid-test",
                service_cidr: "10.51.0.0/24",
                pod_subnet: "10.50.0.0/24",
                etc_dir_path: etc_dir.to_str().unwrap(),
                node_name: "mn-controlplane1",
                host_ip: Some("10.99.0.10".to_string()),
                api_fqdn: None,
                allow_local_ca_generation: true,
            },
            &supervisor,
        )
        .await
        .expect_err("invalid existing SA signing key must hard fail");

        let msg = format!("{err:#}");
        assert!(
            msg.contains(&signer_path.display().to_string()),
            "error must include the invalid signer path: {msg}"
        );
        assert!(
            msg.contains("delete") && msg.contains("regenerate"),
            "error must tell the user deleting the file allows regeneration: {msg}"
        );
    }

    #[tokio::test]
    async fn init_certificates_requires_downloaded_service_account_signing_key_when_generation_disabled()
     {
        let dir = tempfile::tempdir().unwrap();
        let etc_dir = dir.path().join("etc");
        std::fs::create_dir_all(&etc_dir).unwrap();
        let (_, _, ca_cert_pem, ca_key_pem) = generate_ca_full().unwrap();
        std::fs::write(etc_dir.join("ca.crt"), ca_cert_pem).unwrap();
        std::fs::write(etc_dir.join("ca.key"), ca_key_pem).unwrap();
        let signer_path = etc_dir.join("service-account-signing.key");

        let supervisor = crate::task_supervisor::TaskSupervisor::new(Default::default());
        let err = init_certificates(
            InitCertificateRequest {
                tls_port: 7679,
                context_name: "klights-sa-signer-joiner-test",
                service_cidr: "10.51.0.0/24",
                pod_subnet: "10.50.0.0/24",
                etc_dir_path: etc_dir.to_str().unwrap(),
                node_name: "mn-controlplane2",
                host_ip: Some("10.99.0.14".to_string()),
                api_fqdn: None,
                allow_local_ca_generation: false,
            },
            &supervisor,
        )
        .await
        .expect_err("joining controlplanes must receive the SA signer from the leader");

        let msg = format!("{err:#}");
        assert!(
            msg.contains(&signer_path.display().to_string()),
            "error must include the missing signer path: {msg}"
        );
        assert!(
            msg.contains("leader"),
            "error must explain that the signer is expected from the leader: {msg}"
        );
    }

    #[tokio::test]
    async fn init_certificates_regenerates_mismatched_api_proxy_key_pair() {
        let dir = tempfile::tempdir().unwrap();
        let etc_dir = dir.path().join("etc");
        let supervisor = crate::task_supervisor::TaskSupervisor::new(Default::default());
        let request = || InitCertificateRequest {
            tls_port: 7679,
            context_name: "klights-proxy-key-test",
            service_cidr: "10.51.0.0/24",
            pod_subnet: "10.50.0.0/24",
            etc_dir_path: etc_dir.to_str().unwrap(),
            node_name: "mn-controlplane2",
            host_ip: Some("10.99.0.14".to_string()),
            api_fqdn: None,
            allow_local_ca_generation: true,
        };
        init_certificates(request(), &supervisor).await.unwrap();

        let proxy_cert_path = etc_dir.join("api-proxy.crt");
        let proxy_key_path = etc_dir.join("api-proxy.key");
        let (_, wrong_ca_key, wrong_ca_cert_pem, _) = generate_ca_full().unwrap();
        let wrong_ca_cert = CertificateParams::from_ca_cert_pem(&wrong_ca_cert_pem)
            .unwrap()
            .self_signed(&wrong_ca_key)
            .unwrap();
        let (_, wrong_key_pem) =
            generate_api_proxy_cert(&wrong_ca_cert, &wrong_ca_key, "mn-controlplane2").unwrap();
        std::fs::write(&proxy_key_path, wrong_key_pem).unwrap();

        init_certificates(request(), &supervisor).await.unwrap();

        let repaired_cert = std::fs::read_to_string(&proxy_cert_path).unwrap();
        let repaired_key = std::fs::read_to_string(&proxy_key_path).unwrap();
        assert!(api_proxy_cert_and_key_match_config(
            &repaired_cert,
            &repaired_key,
            "mn-controlplane2"
        ));
    }

    #[tokio::test]
    async fn init_certificates_regenerates_server_cert_when_service_ip_san_changes() {
        let dir = tempfile::tempdir().unwrap();
        let etc_dir = dir.path();
        let (ca_cert, ca_key, ca_cert_pem, ca_key_pem) = generate_ca_full().unwrap();
        let (old_server_cert_pem, old_server_key_pem) = generate_server_cert_with_config(
            &ca_cert,
            &ca_key,
            "10.50.0.0/24",
            "10.50.0.0/24",
            Some("10.99.0.10".to_string()),
            "mn-controlplane1",
            None,
        )
        .unwrap();
        std::fs::write(etc_dir.join("ca.crt"), ca_cert_pem).unwrap();
        std::fs::write(etc_dir.join("ca.key"), ca_key_pem).unwrap();
        std::fs::write(etc_dir.join("server.crt"), old_server_cert_pem).unwrap();
        std::fs::write(etc_dir.join("server.key"), old_server_key_pem).unwrap();

        let supervisor = crate::task_supervisor::TaskSupervisor::new(Default::default());
        let result = init_certificates(
            InitCertificateRequest {
                tls_port: 7679,
                context_name: "klights-mn-controlplane1",
                service_cidr: "10.51.0.0/24",
                pod_subnet: "10.50.0.0/24",
                etc_dir_path: etc_dir.to_str().unwrap(),
                node_name: "mn-controlplane1",
                host_ip: Some("10.99.0.10".to_string()),
                api_fqdn: None,
                allow_local_ca_generation: true,
            },
            &supervisor,
        )
        .await
        .unwrap();

        let CertInitResult::Complete(paths) = result else {
            panic!("seed node with CA key should complete local certificate initialization");
        };
        let ip_sans = extract_ip_sans(&paths.server_cert);
        assert!(
            ip_sans.contains(&"10.51.0.1".to_string()),
            "server certificate must be regenerated with the current kubernetes Service IP SAN, got {ip_sans:?}"
        );
    }

    #[tokio::test]
    async fn init_certificates_writes_keys_0600_and_etc_dir_0700() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let etc_dir = dir.path().join("etc");
        let supervisor = crate::task_supervisor::TaskSupervisor::new(Default::default());
        init_certificates(
            InitCertificateRequest {
                tls_port: 7679,
                context_name: "klights-perm-test",
                service_cidr: "10.51.0.0/24",
                pod_subnet: "10.50.0.0/24",
                etc_dir_path: etc_dir.to_str().unwrap(),
                node_name: "perm-node",
                host_ip: Some("10.99.0.10".to_string()),
                api_fqdn: None,
                allow_local_ca_generation: true,
            },
            &supervisor,
        )
        .await
        .unwrap();

        let mode =
            |p: std::path::PathBuf| std::fs::metadata(&p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(etc_dir.clone()), 0o700, "etc dir must be 0700");
        for key in ["ca.key", "server.key", "admin.key"] {
            let p = etc_dir.join(key);
            assert!(p.exists(), "{key} must be generated");
            assert_eq!(mode(p), 0o600, "{key} must be 0600 (owner-only)");
        }
        // Public certs are not key files; they should not be forced to 0600.
        assert!(etc_dir.join("ca.crt").exists());
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
