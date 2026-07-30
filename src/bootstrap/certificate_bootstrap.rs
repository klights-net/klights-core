//! Root-owned certificate bootstrap and host-local persistence adapter.
//!
//! Certificate policy and cryptography live in `auth`; this module owns the
//! concrete filesystem layout, supervised file I/O, and bootstrap sequencing.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use anyhow::Result;
use time::OffsetDateTime;

use klights_auth::cert::{
    CertificateAuthority, api_proxy_cert_and_key_match_config,
    apiservice_proxy_cert_and_key_match_config, generate_server_csr, server_cert_matches_config,
};

/// Result of root-owned certificate initialization.
#[derive(Clone, Debug)]
pub enum CertInitResult {
    Complete,
    NeedsCsrSign(PendingCsr),
}

/// Filesystem locations and CSR bytes needed by the root join workflow.
#[derive(Clone, Debug)]
pub struct PendingCsr {
    pub server_csr_pem: Vec<u8>,
    pub etc_dir: PathBuf,
}

#[derive(Clone, Debug)]
pub struct InitCertificateRequest<'a> {
    pub tls_port: u16,
    pub context_name: &'a str,
    pub service_cidr: &'a str,
    pub pod_subnet: &'a str,
    pub etc_dir: &'a Path,
    pub node_name: &'a str,
    pub host_ip: Option<String>,
    pub api_fqdn: Option<&'a str>,
    pub valid_at: OffsetDateTime,
    pub allow_local_ca_generation: bool,
}

/// Bootstrap the node's certificate files using auth-owned crypto over
/// root-provided values.
pub(crate) async fn init_certificates(
    request: InitCertificateRequest<'_>,
    task_supervisor: &klights_supervisor::TaskSupervisor,
) -> Result<CertInitResult> {
    let InitCertificateRequest {
        tls_port,
        context_name,
        service_cidr,
        pod_subnet,
        etc_dir,
        node_name,
        host_ip,
        api_fqdn,
        valid_at,
        allow_local_ca_generation,
    } = request;

    tracing::info!("Initializing certificates...");
    let etc_dir_for_create = etc_dir.to_path_buf();
    task_supervisor
        .run_blocking_file_keyed(
            "cert_create_etc_dir",
            etc_dir.to_string_lossy().into_owned(),
            move || {
                fs::create_dir_all(&etc_dir_for_create)?;
                fs::set_permissions(&etc_dir_for_create, PermissionsExt::from_mode(0o700))
            },
        )
        .await??;
    tracing::info!("Etc directory created/verified: {}", etc_dir.display());

    let ca_cert_path = etc_dir.join("ca.crt");
    let ca_key_path = etc_dir.join("ca.key");
    let server_cert_path = etc_dir.join("server.crt");
    let server_key_path = etc_dir.join("server.key");
    let admin_cert_path = etc_dir.join("admin.crt");
    let admin_key_path = etc_dir.join("admin.key");

    let ca_cert_exists =
        path_exists_keyed(task_supervisor, &ca_cert_path, "cert_check_ca_cert").await?;
    let ca_key_exists =
        path_exists_keyed(task_supervisor, &ca_key_path, "cert_check_ca_key").await?;
    let authority = if ca_cert_exists && ca_key_exists {
        tracing::info!("Loading existing CA certificates");
        let cert_pem =
            read_utf8_file_keyed(task_supervisor, &ca_cert_path, "cert_read_ca_cert").await?;
        let key_pem =
            read_utf8_file_keyed(task_supervisor, &ca_key_path, "cert_read_ca_key").await?;
        task_supervisor
            .run_blocking(
                klights_supervisor::TaskCategory::Others,
                "parse-existing-cluster-ca",
                move || CertificateAuthority::from_pem(cert_pem, key_pem, valid_at),
            )
            .await??
    } else if ca_cert_exists || !allow_local_ca_generation {
        tracing::info!(
            ca_cert_exists,
            ca_key_exists,
            "Generating server CSR (CA key not available locally)"
        );
        let service_cidr = service_cidr.to_string();
        let pod_subnet = pod_subnet.to_string();
        let node_name = node_name.to_string();
        let api_fqdn = api_fqdn.map(str::to_string);
        let host_ip_for_csr = host_ip.clone();
        let (server_key_pem, server_csr_pem) = task_supervisor
            .run_blocking(
                klights_supervisor::TaskCategory::Others,
                "generate-server-csr",
                move || {
                    generate_server_csr(
                        &service_cidr,
                        &pod_subnet,
                        host_ip_for_csr.as_deref(),
                        &node_name,
                        api_fqdn.as_deref(),
                        valid_at,
                    )
                },
            )
            .await??;
        write_file_keyed(
            task_supervisor,
            &server_key_path,
            server_key_pem,
            "cert_write_server_key_csr",
        )
        .await?;
        return Ok(CertInitResult::NeedsCsrSign(PendingCsr {
            server_csr_pem,
            etc_dir: etc_dir.to_path_buf(),
        }));
    } else {
        tracing::info!("Generating new CA certificates");
        let authority = task_supervisor
            .run_blocking(
                klights_supervisor::TaskCategory::Others,
                "generate-cluster-ca",
                move || CertificateAuthority::generate(valid_at),
            )
            .await??;
        write_file_keyed(
            task_supervisor,
            &ca_cert_path,
            authority.certificate_pem().to_string(),
            "cert_write_ca_cert",
        )
        .await?;
        write_file_keyed(
            task_supervisor,
            &ca_key_path,
            authority.private_key_pem().to_string(),
            "cert_write_ca_key",
        )
        .await?;
        authority
    };

    let server_cert_exists =
        path_exists_keyed(task_supervisor, &server_cert_path, "cert_check_server_cert").await?;
    let server_key_exists =
        path_exists_keyed(task_supervisor, &server_key_path, "cert_check_server_key").await?;
    let _server_identity = if server_cert_exists && server_key_exists {
        let cert =
            read_utf8_file_keyed(task_supervisor, &server_cert_path, "cert_read_server_cert")
                .await?;
        let key =
            read_utf8_file_keyed(task_supervisor, &server_key_path, "cert_read_server_key").await?;
        if server_cert_matches_config_supervised(
            task_supervisor,
            cert.clone(),
            service_cidr.to_string(),
            pod_subnet.to_string(),
            host_ip.clone(),
            node_name.to_string(),
            api_fqdn.map(str::to_string),
        )
        .await?
        {
            tracing::info!("Loading existing server certificates");
            (cert, key)
        } else {
            tracing::info!(
                "Regenerating server certificates because existing SANs do not match current API endpoints"
            );
            let (cert, key) = generate_server_cert_supervised(
                task_supervisor,
                authority.clone(),
                service_cidr.to_string(),
                pod_subnet.to_string(),
                host_ip.clone(),
                node_name.to_string(),
                api_fqdn.map(str::to_string),
                valid_at,
            )
            .await?;
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
        let (cert, key) = generate_server_cert_supervised(
            task_supervisor,
            authority.clone(),
            service_cidr.to_string(),
            pod_subnet.to_string(),
            host_ip.clone(),
            node_name.to_string(),
            api_fqdn.map(str::to_string),
            valid_at,
        )
        .await?;
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
        let authority = authority.clone();
        let (cert, key) = task_supervisor
            .run_blocking(
                klights_supervisor::TaskCategory::Others,
                "generate-admin-certificate",
                move || authority.issue_admin_certificate(valid_at),
            )
            .await??;
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

    let ca_cert_pem = authority.certificate_pem().to_string();
    ensure_api_proxy_certificate(
        task_supervisor,
        etc_dir,
        authority.clone(),
        node_name,
        valid_at,
    )
    .await?;
    ensure_apiservice_proxy_certificate(task_supervisor, etc_dir, authority, valid_at).await?;

    let kubeconfig = klights_auth::kubeconfig::generate_kubeconfig(
        klights_auth::kubeconfig::KubeconfigParams {
            ca_cert: &ca_cert_pem,
            admin_cert: &admin_cert_pem,
            admin_key: &admin_key_pem,
            tls_port,
            context_name,
            host_ip: host_ip.as_deref(),
            pod_subnet,
        },
    )?;

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

    Ok(CertInitResult::Complete)
}

#[allow(clippy::too_many_arguments)]
async fn generate_server_cert_supervised(
    task_supervisor: &klights_supervisor::TaskSupervisor,
    authority: CertificateAuthority,
    service_cidr: String,
    pod_subnet: String,
    host_ip: Option<String>,
    node_name: String,
    api_fqdn: Option<String>,
    valid_at: OffsetDateTime,
) -> Result<(String, String)> {
    task_supervisor
        .run_blocking(
            klights_supervisor::TaskCategory::Others,
            "generate-server-certificate",
            move || {
                authority.issue_server_certificate(
                    &service_cidr,
                    &pod_subnet,
                    host_ip.as_deref(),
                    &node_name,
                    api_fqdn.as_deref(),
                    valid_at,
                )
            },
        )
        .await?
}

#[allow(clippy::too_many_arguments)]
async fn server_cert_matches_config_supervised(
    task_supervisor: &klights_supervisor::TaskSupervisor,
    cert: String,
    service_cidr: String,
    pod_subnet: String,
    host_ip: Option<String>,
    node_name: String,
    api_fqdn: Option<String>,
) -> Result<bool> {
    task_supervisor
        .run_blocking(
            klights_supervisor::TaskCategory::Others,
            "validate-server-certificate",
            move || {
                server_cert_matches_config(
                    &cert,
                    &service_cidr,
                    &pod_subnet,
                    host_ip.as_deref(),
                    &node_name,
                    api_fqdn.as_deref(),
                )
            },
        )
        .await
}

async fn ensure_api_proxy_certificate(
    task_supervisor: &klights_supervisor::TaskSupervisor,
    etc_dir: &Path,
    authority: CertificateAuthority,
    node_name: &str,
    valid_at: OffsetDateTime,
) -> Result<(String, String)> {
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
        let cert_for_validation = cert.clone();
        let key_for_validation = key.clone();
        let node_for_validation = node_name.to_string();
        let matches = task_supervisor
            .run_blocking(
                klights_supervisor::TaskCategory::Others,
                "validate-api-proxy-certificate",
                move || {
                    api_proxy_cert_and_key_match_config(
                        &cert_for_validation,
                        &key_for_validation,
                        &node_for_validation,
                    )
                },
            )
            .await?;
        if matches {
            return Ok((cert, key));
        }
        tracing::info!("Regenerating api-proxy certificate because existing identity is invalid");
    }

    let node_name = node_name.to_string();
    let (cert, key) = task_supervisor
        .run_blocking(
            klights_supervisor::TaskCategory::Others,
            "generate-api-proxy-certificate",
            move || authority.issue_api_proxy_certificate(&node_name, valid_at),
        )
        .await??;
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

async fn ensure_apiservice_proxy_certificate(
    task_supervisor: &klights_supervisor::TaskSupervisor,
    etc_dir: &Path,
    authority: CertificateAuthority,
    valid_at: OffsetDateTime,
) -> Result<(String, String)> {
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
        let cert_for_validation = cert.clone();
        let key_for_validation = key.clone();
        let matches = task_supervisor
            .run_blocking(
                klights_supervisor::TaskCategory::Others,
                "validate-apiservice-proxy-certificate",
                move || {
                    apiservice_proxy_cert_and_key_match_config(
                        &cert_for_validation,
                        &key_for_validation,
                    )
                },
            )
            .await?;
        if matches {
            return Ok((cert, key));
        }
        tracing::info!(
            "Regenerating apiservice-proxy certificate because existing identity is invalid"
        );
    }

    let (cert, key) = task_supervisor
        .run_blocking(
            klights_supervisor::TaskCategory::Others,
            "generate-apiservice-proxy-certificate",
            move || authority.issue_apiservice_proxy_certificate(valid_at),
        )
        .await??;
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

async fn path_exists_keyed(
    task_supervisor: &klights_supervisor::TaskSupervisor,
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
    task_supervisor: &klights_supervisor::TaskSupervisor,
    path: &Path,
    label: &'static str,
) -> Result<String> {
    let path_buf = path.to_path_buf();
    let key = path.to_string_lossy().into_owned();
    Ok(task_supervisor
        .run_blocking_file_keyed(label, key, move || fs::read_to_string(path_buf))
        .await??)
}

async fn write_file_keyed(
    task_supervisor: &klights_supervisor::TaskSupervisor,
    path: &Path,
    contents: String,
    label: &'static str,
) -> Result<()> {
    let path_buf = path.to_path_buf();
    let key = path.to_string_lossy().into_owned();
    task_supervisor
        .run_blocking_file_keyed(label, key, move || {
            fs::write(&path_buf, contents)?;
            if path_buf.extension().is_some_and(|ext| ext == "key") {
                fs::set_permissions(&path_buf, PermissionsExt::from_mode(0o600))?;
            }
            std::io::Result::Ok(())
        })
        .await??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use klights_auth::cert::{
        api_proxy_cert_and_key_match_config, generate_api_proxy_cert, generate_ca_full_at,
        generate_server_cert_with_config_at, parse_certificate_extended_key_usage,
    };
    use rcgen::CertificateParams;
    use rsa::RsaPrivateKey;

    fn pem_to_der(pem_str: &str) -> Vec<u8> {
        use x509_parser::pem::Pem;
        let (pem, _) = Pem::read(std::io::Cursor::new(pem_str.as_bytes())).unwrap();
        pem.contents
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
                for name in &san.general_names {
                    if let GeneralName::IPAddress(bytes) = name {
                        let address = match bytes.len() {
                            4 => Some(std::net::IpAddr::V4(std::net::Ipv4Addr::new(
                                bytes[0], bytes[1], bytes[2], bytes[3],
                            ))),
                            16 => {
                                let mut octets = [0_u8; 16];
                                octets.copy_from_slice(bytes);
                                Some(std::net::IpAddr::V6(std::net::Ipv6Addr::from(octets)))
                            }
                            _ => None,
                        };
                        if let Some(address) = address {
                            ip_addrs.push(address.to_string());
                        }
                    }
                }
            }
        }
        ip_addrs
    }

    #[tokio::test]
    async fn joining_node_bootstrap_persists_only_csr_key_without_local_ca() {
        let dir = tempfile::tempdir().unwrap();
        let etc_dir = dir.path().join("etc");
        let supervisor = klights_supervisor::TaskSupervisor::new(Default::default());

        let result = init_certificates(
            InitCertificateRequest {
                tls_port: 7679,
                context_name: "klights-joiner-csr-test",
                service_cidr: "10.51.0.0/24",
                pod_subnet: "10.50.0.0/24",
                etc_dir: &etc_dir,
                node_name: "mn-controlplane2",
                host_ip: Some("10.99.0.14".to_string()),
                api_fqdn: None,
                valid_at: OffsetDateTime::now_utc(),
                allow_local_ca_generation: false,
            },
            &supervisor,
        )
        .await
        .unwrap();

        let CertInitResult::NeedsCsrSign(pending) = result else {
            panic!("joining node without injected CA material must request leader signing");
        };
        assert_eq!(pending.etc_dir, etc_dir);
        assert!(etc_dir.join("server.key").exists());
        assert!(
            pending
                .server_csr_pem
                .starts_with(b"-----BEGIN CERTIFICATE REQUEST-----")
        );
        assert!(!etc_dir.join("ca.key").exists());
    }

    #[tokio::test]
    async fn init_certificates_writes_dedicated_api_proxy_client_certificate() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let etc_dir = dir.path().join("etc");
        let supervisor = klights_supervisor::TaskSupervisor::new(Default::default());
        init_certificates(
            InitCertificateRequest {
                tls_port: 7679,
                context_name: "klights-proxy-cert-test",
                service_cidr: "10.51.0.0/24",
                pod_subnet: "10.50.0.0/24",
                etc_dir: &etc_dir,
                node_name: "mn-controlplane2",
                host_ip: Some("10.99.0.14".to_string()),
                api_fqdn: None,
                valid_at: OffsetDateTime::now_utc(),
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
        let user = klights_auth::user::user_from_cert(&der).unwrap();
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
        let supervisor = klights_supervisor::TaskSupervisor::new(Default::default());
        init_certificates(
            InitCertificateRequest {
                tls_port: 7679,
                context_name: "klights-apiservice-proxy-cert-test",
                service_cidr: "10.51.0.0/24",
                pod_subnet: "10.50.0.0/24",
                etc_dir: &etc_dir,
                node_name: "mn-controlplane2",
                host_ip: Some("10.99.0.14".to_string()),
                api_fqdn: None,
                valid_at: OffsetDateTime::now_utc(),
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
        let user = klights_auth::user::user_from_cert(&der).unwrap();
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
    async fn root_signing_state_generates_dedicated_key_for_seed_leader() {
        use rsa::pkcs8::DecodePrivateKey;
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let etc_dir = dir.path().join("etc");
        let supervisor = klights_supervisor::TaskSupervisor::new(Default::default());
        init_certificates(
            InitCertificateRequest {
                tls_port: 7679,
                context_name: "klights-sa-signer-test",
                service_cidr: "10.51.0.0/24",
                pod_subnet: "10.50.0.0/24",
                etc_dir: &etc_dir,
                node_name: "mn-controlplane1",
                host_ip: Some("10.99.0.10".to_string()),
                api_fqdn: None,
                valid_at: OffsetDateTime::now_utc(),
                allow_local_ca_generation: true,
            },
            &supervisor,
        )
        .await
        .unwrap();

        let signer_path = etc_dir.join("service-account-signing.key");
        crate::signing_key_state_adapter::ensure(&signer_path, true, &supervisor)
            .await
            .unwrap();
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
    async fn root_signing_state_repairs_missing_key_with_existing_ca() {
        use rsa::pkcs8::DecodePrivateKey;

        let dir = tempfile::tempdir().unwrap();
        let etc_dir = dir.path().join("etc");
        std::fs::create_dir_all(&etc_dir).unwrap();
        let (_, _, ca_cert_pem, ca_key_pem) =
            generate_ca_full_at(time::OffsetDateTime::now_utc()).unwrap();
        std::fs::write(etc_dir.join("ca.crt"), ca_cert_pem).unwrap();
        std::fs::write(etc_dir.join("ca.key"), ca_key_pem).unwrap();

        let supervisor = klights_supervisor::TaskSupervisor::new(Default::default());
        init_certificates(
            InitCertificateRequest {
                tls_port: 7679,
                context_name: "klights-sa-signer-repair-test",
                service_cidr: "10.51.0.0/24",
                pod_subnet: "10.50.0.0/24",
                etc_dir: &etc_dir,
                node_name: "mn-controlplane1",
                host_ip: Some("10.99.0.10".to_string()),
                api_fqdn: None,
                valid_at: OffsetDateTime::now_utc(),
                allow_local_ca_generation: true,
            },
            &supervisor,
        )
        .await
        .unwrap();

        crate::signing_key_state_adapter::ensure(
            &etc_dir.join("service-account-signing.key"),
            true,
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
    async fn root_signing_state_hard_fails_invalid_existing_key() {
        let dir = tempfile::tempdir().unwrap();
        let etc_dir = dir.path().join("etc");
        std::fs::create_dir_all(&etc_dir).unwrap();
        let (_, _, ca_cert_pem, ca_key_pem) =
            generate_ca_full_at(time::OffsetDateTime::now_utc()).unwrap();
        std::fs::write(etc_dir.join("ca.crt"), ca_cert_pem).unwrap();
        std::fs::write(etc_dir.join("ca.key"), ca_key_pem).unwrap();
        let signer_path = etc_dir.join("service-account-signing.key");
        std::fs::write(&signer_path, "not a private key").unwrap();

        let supervisor = klights_supervisor::TaskSupervisor::new(Default::default());
        init_certificates(
            InitCertificateRequest {
                tls_port: 7679,
                context_name: "klights-sa-signer-invalid-test",
                service_cidr: "10.51.0.0/24",
                pod_subnet: "10.50.0.0/24",
                etc_dir: &etc_dir,
                node_name: "mn-controlplane1",
                host_ip: Some("10.99.0.10".to_string()),
                api_fqdn: None,
                valid_at: OffsetDateTime::now_utc(),
                allow_local_ca_generation: true,
            },
            &supervisor,
        )
        .await
        .unwrap();
        let err = crate::signing_key_state_adapter::ensure(&signer_path, true, &supervisor)
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
    async fn root_signing_state_requires_downloaded_key_when_generation_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let etc_dir = dir.path().join("etc");
        std::fs::create_dir_all(&etc_dir).unwrap();
        let (_, _, ca_cert_pem, ca_key_pem) =
            generate_ca_full_at(time::OffsetDateTime::now_utc()).unwrap();
        std::fs::write(etc_dir.join("ca.crt"), ca_cert_pem).unwrap();
        std::fs::write(etc_dir.join("ca.key"), ca_key_pem).unwrap();
        let signer_path = etc_dir.join("service-account-signing.key");

        let supervisor = klights_supervisor::TaskSupervisor::new(Default::default());
        init_certificates(
            InitCertificateRequest {
                tls_port: 7679,
                context_name: "klights-sa-signer-joiner-test",
                service_cidr: "10.51.0.0/24",
                pod_subnet: "10.50.0.0/24",
                etc_dir: &etc_dir,
                node_name: "mn-controlplane2",
                host_ip: Some("10.99.0.14".to_string()),
                api_fqdn: None,
                valid_at: OffsetDateTime::now_utc(),
                allow_local_ca_generation: false,
            },
            &supervisor,
        )
        .await
        .unwrap();
        let err = crate::signing_key_state_adapter::ensure(&signer_path, false, &supervisor)
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
        let supervisor = klights_supervisor::TaskSupervisor::new(Default::default());
        let request = || InitCertificateRequest {
            tls_port: 7679,
            context_name: "klights-proxy-key-test",
            service_cidr: "10.51.0.0/24",
            pod_subnet: "10.50.0.0/24",
            etc_dir: &etc_dir,
            node_name: "mn-controlplane2",
            host_ip: Some("10.99.0.14".to_string()),
            api_fqdn: None,
            valid_at: OffsetDateTime::now_utc(),
            allow_local_ca_generation: true,
        };
        init_certificates(request(), &supervisor).await.unwrap();

        let proxy_cert_path = etc_dir.join("api-proxy.crt");
        let proxy_key_path = etc_dir.join("api-proxy.key");
        let (_, wrong_ca_key, wrong_ca_cert_pem, _) =
            generate_ca_full_at(time::OffsetDateTime::now_utc()).unwrap();
        let wrong_ca_cert = CertificateParams::from_ca_cert_pem(&wrong_ca_cert_pem)
            .unwrap()
            .self_signed(&wrong_ca_key)
            .unwrap();
        let (_, wrong_key_pem) = generate_api_proxy_cert(
            &wrong_ca_cert,
            &wrong_ca_key,
            "mn-controlplane2",
            OffsetDateTime::now_utc(),
        )
        .unwrap();
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
        let (ca_cert, ca_key, ca_cert_pem, ca_key_pem) =
            generate_ca_full_at(time::OffsetDateTime::now_utc()).unwrap();
        let (old_server_cert_pem, old_server_key_pem) = generate_server_cert_with_config_at(
            &ca_cert,
            &ca_key,
            "10.50.0.0/24",
            "10.50.0.0/24",
            Some("10.99.0.10".to_string()),
            "mn-controlplane1",
            None,
            time::OffsetDateTime::now_utc(),
        )
        .unwrap();
        std::fs::write(etc_dir.join("ca.crt"), ca_cert_pem).unwrap();
        std::fs::write(etc_dir.join("ca.key"), ca_key_pem).unwrap();
        std::fs::write(etc_dir.join("server.crt"), old_server_cert_pem).unwrap();
        std::fs::write(etc_dir.join("server.key"), old_server_key_pem).unwrap();

        let supervisor = klights_supervisor::TaskSupervisor::new(Default::default());
        let result = init_certificates(
            InitCertificateRequest {
                tls_port: 7679,
                context_name: "klights-mn-controlplane1",
                service_cidr: "10.51.0.0/24",
                pod_subnet: "10.50.0.0/24",
                etc_dir,
                node_name: "mn-controlplane1",
                host_ip: Some("10.99.0.10".to_string()),
                api_fqdn: None,
                valid_at: OffsetDateTime::now_utc(),
                allow_local_ca_generation: true,
            },
            &supervisor,
        )
        .await
        .unwrap();

        let CertInitResult::Complete = result else {
            panic!("seed node with CA key should complete local certificate initialization");
        };
        let server_cert_pem = std::fs::read_to_string(etc_dir.join("server.crt")).unwrap();
        let ip_sans = extract_ip_sans(&server_cert_pem);
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
        let supervisor = klights_supervisor::TaskSupervisor::new(Default::default());
        init_certificates(
            InitCertificateRequest {
                tls_port: 7679,
                context_name: "klights-perm-test",
                service_cidr: "10.51.0.0/24",
                pod_subnet: "10.50.0.0/24",
                etc_dir: &etc_dir,
                node_name: "perm-node",
                host_ip: Some("10.99.0.10".to_string()),
                api_fqdn: None,
                valid_at: OffsetDateTime::now_utc(),
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
}
