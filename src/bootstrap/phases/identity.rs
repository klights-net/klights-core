//! Phase 4: Identity — certificates and dataplane metadata.

use anyhow::{Context, Result, anyhow};
use time::OffsetDateTime;

use super::config::ConfigPhase;
use crate::bootstrap::credential_store::BootstrapCredentialStore;

pub struct IdentityPhase {
    pub node_ip: String,
    pub follower_dataplane: Option<klights_leader_rpc::client::JoinDataplaneMetadata>,
    pub grpc_ca_cert_path: Option<std::path::PathBuf>,
}

/// Leader/full-stack identity: certs + local dataplane metadata (no bootstrap token).
pub async fn setup_leader(
    cfg: &ConfigPhase,
    node_ip: &str,
    role: &crate::bootstrap::NodeRole,
) -> Result<IdentityPhase> {
    use crate::bootstrap::certificate_bootstrap::{
        CertInitResult, InitCertificateRequest, init_certificates,
    };
    use crate::bootstrap::init::dataplane::local_join_dataplane_metadata;

    let local_dataplane = local_join_dataplane_metadata(
        &cfg.config,
        &cfg.node_mode,
        node_ip,
        cfg.supervisor.as_ref(),
    )
    .await
    .context("failed to prepare leader dataplane metadata")?;

    let cert_result = init_certificates(
        InitCertificateRequest {
            tls_port: cfg.config.tls_port,
            context_name: &cfg.config.containerd_namespace,
            service_cidr: &cfg.config.service_cidr,
            pod_subnet: &cfg.config.pod_subnet,
            etc_dir: std::path::Path::new(&cfg.etc_dir),
            node_name: &cfg.config.node_name,
            host_ip: Some(api_host_for_certificates(&cfg.config, node_ip)),
            api_fqdn: cfg.config.api_fqdn.as_deref(),
            valid_at: OffsetDateTime::now_utc(),
            allow_local_ca_generation: role_allows_local_ca_generation(role),
        },
        cfg.supervisor.as_ref(),
    )
    .await
    .context("Failed to initialize certificates")?;

    let grpc_ca_cert_path = Some(std::path::Path::new(&cfg.etc_dir).join("ca.crt"));

    match cert_result {
        CertInitResult::Complete => {}
        CertInitResult::NeedsCsrSign(pending) => {
            resolve_csr_via_rpc(cfg, role, &pending, &local_dataplane)
                .await
                .context("Failed to resolve server cert CSR via leader RPC")?;
        }
    }
    klights_cluster_datastore::signing_key_state::ensure(
        &std::path::Path::new(&cfg.etc_dir).join("service-account-signing.key"),
        role_allows_local_ca_generation(role),
        cfg.supervisor.as_ref(),
    )
    .await
    .context("Failed to initialize root-owned ServiceAccount signing state")?;

    ensure_local_node_client_certificate(cfg)
        .await
        .context("Failed to ensure local node client certificate")?;

    Ok(IdentityPhase {
        node_ip: node_ip.to_string(),
        follower_dataplane: Some(local_dataplane),
        grpc_ca_cert_path,
    })
}

/// Send the pending CSR to the leader for signing, write the response
/// certs to disk.
async fn resolve_csr_via_rpc(
    cfg: &ConfigPhase,
    role: &crate::bootstrap::NodeRole,
    pending: &crate::bootstrap::certificate_bootstrap::PendingCsr,
    local_dataplane: &klights_leader_rpc::client::JoinDataplaneMetadata,
) -> Result<()> {
    use crate::bootstrap::NodeRole;

    let (leader_endpoints, token, skip_ca) = match role {
        NodeRole::Controlplane {
            leader_endpoints,
            token,
            skip_ca,
            ..
        } => (leader_endpoints.clone(), token.clone(), *skip_ca),
        _ => {
            return Err(anyhow!(
                "CSR signing only supported for joining controlplane nodes"
            ));
        }
    };
    let persist_ca_key = should_persist_controlplane_ca_key(role);

    let leader_endpoint = leader_endpoints
        .first()
        .ok_or_else(|| anyhow!("no leader endpoint configured for CSR signing"))?
        .clone();

    let token_value = token.clone().unwrap_or_default();
    let client_identity = controlplane_rpc_client_identity_for_token(
        &token_value,
        std::path::Path::new(&cfg.etc_dir),
        &cfg.config.node_name,
        cfg.supervisor.clone(),
    )
    .await?;

    let rpc_ca_cert_path = csr_signing_ca_cert_path(&cfg.config, role);

    let client = klights_leader_rpc::client::ReplicationGrpcClient::new(
        klights_leader_rpc::client::GrpcClientConfig {
            leader_endpoint: leader_endpoint.clone(),
            token: token_value.clone(),
            node_name: cfg.config.node_name.clone(),
            role: klights_leader_api::JoinRole::Worker,
            dataplane: local_dataplane.clone(),
            ca_cert_path: rpc_ca_cert_path.clone(),
            skip_ca,
            client_cert_pem: client_identity.0,
            client_key_pem: client_identity.1,
        },
        cfg.supervisor.clone(),
        cfg.grpc_transport_policy.clone(),
    );

    tracing::info!("Sending server CSR to leader for signing");
    let response = client
        .sign_controlplane_csr_rpc(&cfg.config.node_name, &pending.server_csr_pem)
        .await
        .context("SignControlplaneCsr RPC failed")?;

    let credential_store =
        crate::bootstrap::credential_store::SupervisedBootstrapCredentialStore::new(
            cfg.supervisor.clone(),
            &cfg.etc_dir,
        );
    let server_cert_path = pending.etc_dir.join("server.crt");

    if !response.ca_cert_pem.is_empty() {
        credential_store
            .install_ca_certificate(response.ca_cert_pem.into_bytes())
            .await
            .context("failed to write ca.crt from CSR response")?;
    }

    if persist_ca_key && !response.encrypted_ca_key.is_empty() {
        let nonce_slice = response
            .ca_key_nonce
            .get(..12)
            .ok_or_else(|| anyhow!("ca_key_nonce must be 12 bytes"))?;
        let nonce: [u8; 12] = nonce_slice.try_into().unwrap();
        let ca_key_bytes = klights_auth::ca_transport::decrypt_ca_key(
            &token_value,
            &response.encrypted_ca_key,
            &nonce,
        )
        .context("failed to decrypt ca.key from CSR response")?;
        credential_store
            .install_ca_key(ca_key_bytes)
            .await
            .context("failed to write ca.key from CSR response")?;
    }

    if persist_ca_key && !response.encrypted_service_account_signing_key.is_empty() {
        let nonce_slice = response
            .service_account_signing_key_nonce
            .get(..12)
            .ok_or_else(|| anyhow!("service_account_signing_key_nonce must be 12 bytes"))?;
        let nonce: [u8; 12] = nonce_slice.try_into().unwrap();
        let service_account_signing_key_bytes = klights_auth::ca_transport::decrypt_ca_key(
            &token_value,
            &response.encrypted_service_account_signing_key,
            &nonce,
        )
        .context("failed to decrypt ServiceAccount signing key from CSR response")?;
        let service_account_signing_key_pem = String::from_utf8(service_account_signing_key_bytes)
            .context("ServiceAccount signing key from CSR response is not UTF-8 PEM")?;
        let service_account_signing_key_path = pending.etc_dir.join("service-account-signing.key");
        klights_cluster_datastore::signing_key_state::persist(
            &service_account_signing_key_path,
            &service_account_signing_key_pem,
            cfg.supervisor.as_ref(),
        )
        .await
        .context("failed to persist ServiceAccount signing key from CSR response")?;
    }

    if !response.signed_server_cert.is_empty() {
        credential_store
            .install_server_certificate(server_cert_path, response.signed_server_cert.into_bytes())
            .await
            .context("failed to write server.crt from CSR response")?;
    }

    tracing::info!("CSR resolved: wrote ca.crt, ca.key, server.crt from leader response");

    if !persist_ca_key {
        tracing::info!(
            "CSR resolved for learner: wrote ca.crt and server.crt; CA key not persisted"
        );
        return Ok(());
    }

    // Re-run cert init now that ca.crt + ca.key + server.crt exist.
    // It will load the existing CA and server cert, then generate the local
    // admin cert and kubeconfig. The follower API proxy must not use that cert.
    let second_pass = crate::bootstrap::certificate_bootstrap::init_certificates(
        crate::bootstrap::certificate_bootstrap::InitCertificateRequest {
            tls_port: cfg.config.tls_port,
            context_name: &cfg.config.containerd_namespace,
            service_cidr: &cfg.config.service_cidr,
            pod_subnet: &cfg.config.pod_subnet,
            etc_dir: std::path::Path::new(&cfg.etc_dir),
            node_name: &cfg.config.node_name,
            host_ip: Some(api_host_for_certificates(
                &cfg.config,
                &local_dataplane.endpoint,
            )),
            api_fqdn: cfg.config.api_fqdn.as_deref(),
            valid_at: OffsetDateTime::now_utc(),
            allow_local_ca_generation: false,
        },
        cfg.supervisor.as_ref(),
    )
    .await
    .context("Failed to finalize certificates after CSR resolution")?;

    match second_pass {
        crate::bootstrap::certificate_bootstrap::CertInitResult::Complete => Ok(()),
        crate::bootstrap::certificate_bootstrap::CertInitResult::NeedsCsrSign(_) => Err(anyhow!(
            "cert init returned NeedsCsrSign after CSR was resolved — this is a bug"
        )),
    }
}

fn csr_signing_ca_cert_path(
    config: &crate::KlightsConfig,
    role: &crate::bootstrap::NodeRole,
) -> Option<std::path::PathBuf> {
    crate::bootstrap::init::predicates::grpc_ca_cert_path_for_role(config, role)
}

fn should_persist_controlplane_ca_key(role: &crate::bootstrap::NodeRole) -> bool {
    matches!(role, crate::bootstrap::NodeRole::Controlplane { .. })
}

fn api_host_for_certificates(config: &crate::KlightsConfig, fallback_host: &str) -> String {
    config
        .external_endpoint
        .clone()
        .unwrap_or_else(|| fallback_host.to_string())
}

fn role_allows_local_ca_generation(role: &crate::bootstrap::NodeRole) -> bool {
    !matches!(
        role,
        crate::bootstrap::NodeRole::Controlplane {
            leader_endpoints,
            ..
        } if !leader_endpoints.is_empty()
    )
}

async fn controlplane_rpc_client_identity_for_token(
    token: &str,
    etc_dir: &std::path::Path,
    node_name: &str,
    supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
) -> Result<(Option<String>, Option<String>)> {
    if !token.is_empty() {
        return Ok((None, None));
    }

    use klights_auth::worker_credential::{WorkerCredentialSource, resolve_worker_credential};
    use klights_auth::worker_credential_store::SupervisedFilesystemWorkerCredentialStore;

    let store = SupervisedFilesystemWorkerCredentialStore::new(
        etc_dir.to_path_buf(),
        node_name,
        supervisor.clone(),
    );
    let crypto = klights_supervisor::CryptoExecutor::new(supervisor.clone());
    let credential_now = klights_auth::clock::Clock::now(&klights_auth::clock::SystemClock);
    match resolve_worker_credential(&store, &crypto, credential_now).await? {
        WorkerCredentialSource::Existing(cred) => {
            let (certificate_pem, private_key_pem) = cred.into_tls_parts();
            Ok((Some(certificate_pem), Some(private_key_pem)))
        }
        WorkerCredentialSource::BootstrapRequired => Err(anyhow!(
            "no persisted node client certificate and no token source provided; join with --token-file first"
        )),
    }
}

async fn ensure_local_node_client_certificate(cfg: &ConfigPhase) -> Result<()> {
    use klights_auth::csr_signer::CsrSigner;
    use klights_auth::worker_credential::{
        WorkerCredential, WorkerCredentialSource, WorkerCredentialStore, resolve_worker_credential,
        worker_credential_has_group,
    };
    use klights_auth::worker_credential_store::SupervisedFilesystemWorkerCredentialStore;

    let etc_dir = std::path::Path::new(&cfg.etc_dir);
    let store = SupervisedFilesystemWorkerCredentialStore::new(
        etc_dir.to_path_buf(),
        &cfg.config.node_name,
        cfg.supervisor.clone(),
    );
    let crypto = klights_supervisor::CryptoExecutor::new(cfg.supervisor.clone());
    let credential_now = klights_auth::clock::Clock::now(&klights_auth::clock::SystemClock);
    if let Ok(WorkerCredentialSource::Existing(existing)) =
        resolve_worker_credential(&store, &crypto, credential_now).await
    {
        // Reuse the persisted cert only if it already carries the
        // `system:controlplanes` group. A cert minted before that group existed
        // (in-place upgrade, or a seed-leader cert preserved across harness
        // runs) must be re-minted — otherwise this control plane cannot
        // authorize its outbound raft consensus RPCs and the cluster deadlocks.
        let credential_for_group_check = existing.clone();
        let has_controlplane_group = crypto
            .run_blocking("check-controlplane-client-certificate-group", move || {
                worker_credential_has_group(
                    &credential_for_group_check,
                    klights_auth::cert::CONTROLPLANE_NODES_GROUP,
                )
            })
            .await
            .context("controlplane certificate group check worker failed")?;
        if has_controlplane_group {
            return Ok(());
        }
        tracing::info!(
            "re-minting control-plane node client certificate to add the system:controlplanes group"
        );
    }

    let ca_cert_path = etc_dir.join("ca.crt");
    let ca_key_path = etc_dir.join("ca.key");
    let ca_cert_path_for_task = ca_cert_path.clone();
    let ca_key_path_for_task = ca_key_path.clone();
    let (ca_cert_pem, ca_key_pem) = cfg
        .supervisor
        .run_blocking_file_keyed(
            "controlplane_node_client_ca_load",
            ca_cert_path.display().to_string(),
            move || -> Result<(String, String)> {
                Ok((
                    klights_supervisor::runtime_fs::read_utf8(&ca_cert_path_for_task)?,
                    klights_supervisor::runtime_fs::read_utf8(&ca_key_path_for_task)?,
                ))
            },
        )
        .await
        .context("controlplane node client CA load task failed")?
        .with_context(|| {
            format!(
                "failed to read controlplane CA material from {} / {}",
                ca_cert_path.display(),
                ca_key_path.display()
            )
        })?;

    let node_name = cfg.config.node_name.clone();
    let (csr, signed) = crypto
        .run_blocking(
            "issue-local-controlplane-client-certificate",
            move || -> Result<_> {
                let csr =
                    klights_auth::kubelet_client_cert::generate_kubelet_client_csr(&node_name)
                        .context("failed to generate local node client CSR")?;
                let signer = klights_auth::csr_signer::CaCsrSigner::new(
                    ca_cert_pem,
                    ca_key_pem,
                    std::sync::Arc::new(klights_auth::clock::SystemClock),
                );
                let signed = signer
                    .sign(klights_auth::csr_signer::SignRequest {
                        csr_pem: csr.csr_pem.clone(),
                        common_name: format!("system:node:{node_name}"),
                        // Control-plane nodes carry the authorization group
                        // required for raft consensus RPCs.
                        organizations: vec![
                            klights_auth::cert::NODES_GROUP.to_string(),
                            klights_auth::cert::CONTROLPLANE_NODES_GROUP.to_string(),
                        ],
                        usages: vec!["client auth".to_string()],
                        ttl_seconds: 31_536_000,
                    })
                    .context("failed to sign local node client certificate")?;
                Ok((csr, signed))
            },
        )
        .await
        .context("local controlplane client certificate worker failed")??;

    store
        .save(&WorkerCredential::try_new(
            signed.certificate_pem,
            csr.private_key_pem,
            cfg.config.node_name.clone(),
            String::new(),
        )?)
        .await
        .context("failed to persist local node client certificate")?;
    tracing::info!("persisted local controlplane node client certificate");
    Ok(())
}

/// Worker identity: certs + join dataplane metadata for leader connection.
pub async fn setup_worker(cfg: &ConfigPhase, node_ip: &str) -> Result<IdentityPhase> {
    use crate::bootstrap::init::dataplane::local_join_dataplane_metadata;

    let follower_dataplane = local_join_dataplane_metadata(
        &cfg.config,
        &cfg.node_mode,
        node_ip,
        cfg.supervisor.as_ref(),
    )
    .await
    .context("failed to prepare worker dataplane join metadata")?;

    Ok(IdentityPhase {
        node_ip: node_ip.to_string(),
        follower_dataplane: Some(follower_dataplane),
        grpc_ca_cert_path: crate::bootstrap::init::predicates::grpc_ca_cert_path_for_role(
            &cfg.config,
            &crate::bootstrap::NodeRole::Worker {
                leader_endpoints: vec![],
                token: None,
                skip_ca: false,
            },
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::NodeRole;

    static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    struct EnvVarGuard {
        name: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl EnvVarGuard {
        fn set(name: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(name);
            unsafe { std::env::set_var(name, value) };
            Self { name, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(value) = &self.previous {
                unsafe { std::env::set_var(self.name, value) };
            } else {
                unsafe { std::env::remove_var(self.name) };
            }
        }
    }

    fn test_service_account_signing_key() -> String {
        use rand_core::OsRng;
        use rsa::RsaPrivateKey;
        use rsa::pkcs8::EncodePrivateKey;

        RsaPrivateKey::new(&mut OsRng, 2048)
            .unwrap()
            .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
            .unwrap()
            .to_string()
    }

    fn test_config_phase(
        mut config: crate::KlightsConfig,
        data_root: &std::path::Path,
        supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
    ) -> crate::bootstrap::phases::config::ConfigPhase {
        config.data_root = data_root.to_path_buf();
        let config = std::sync::Arc::new(config);
        let node_mode = crate::bootstrap::NodeMode::Root;
        let file_process = klights_supervisor::FileProcessExecutor::new(supervisor.clone());
        crate::bootstrap::phases::config::ConfigPhase {
            config: config.clone(),
            node_mode: node_mode.clone(),
            supervisor,
            file_process: file_process.clone(),
            grpc_transport_policy:
                klights_leader_rpc::transport_policy::GrpcTransportPolicy::shared_default(),
            network_cleanup: crate::bootstrap::network_adapters::cleanup_config(
                &node_mode, &config,
            )
            .unwrap()
            .build_cleanup(file_process),
            shutdown_token: tokio_util::sync::CancellationToken::new(),
            etc_dir: data_root.join("etc").to_string_lossy().into_owned(),
            containerd_state_dir: data_root
                .join("containerd/state")
                .to_string_lossy()
                .into_owned(),
            runtime_paths: klights_kubelet::runtime_paths::KubeletRuntimePaths::new(
                data_root.to_path_buf(),
            )
            .unwrap(),
        }
    }

    #[tokio::test]
    async fn csr_signing_ca_cert_path_prefers_leader_ca_for_controlplane_join() {
        let _lock = ENV_LOCK.lock().await;
        let _leader_ca = EnvVarGuard::set("KLIGHTS_LEADER_CA_CERT", "/tmp/seed-ca.crt");
        let mut config = crate::KlightsConfig::test_default();
        config.containerd_namespace = "joiner-local-ca".to_string();
        let role = NodeRole::Controlplane {
            leader_endpoints: vec!["https://10.99.0.10:7679".to_string()],
            token: Some("abcdef.0123456789abcdef".to_string()),
            skip_ca: false,
            as_learner: true,
        };

        assert_eq!(
            csr_signing_ca_cert_path(&config, &role),
            Some(std::path::PathBuf::from("/tmp/seed-ca.crt")),
            "controlplane CSR signing must trust the leader CA, not a joiner-local CA"
        );
    }

    #[test]
    fn learner_controlplane_persists_ca_key_for_future_promotion() {
        let learner = NodeRole::Controlplane {
            leader_endpoints: vec!["https://10.99.0.10:7679".to_string()],
            token: Some("abcdef.0123456789abcdef".to_string()),
            skip_ca: false,
            as_learner: true,
        };
        let voter = NodeRole::Controlplane {
            leader_endpoints: vec!["https://10.99.0.10:7679".to_string()],
            token: Some("abcdef.0123456789abcdef".to_string()),
            skip_ca: false,
            as_learner: false,
        };

        assert!(
            should_persist_controlplane_ca_key(&learner),
            "replica learners must persist cluster CA private key for future promotion"
        );
        assert!(
            should_persist_controlplane_ca_key(&voter),
            "controlplane voters must persist cluster CA private key"
        );
    }

    #[tokio::test]
    async fn setup_worker_does_not_create_local_ca_or_server_certs() {
        let namespace = format!("worker-no-local-ca-{}", uuid::Uuid::new_v4());
        let data_root = crate::paths::test_data_root_fixture(&namespace);
        let mut config = crate::KlightsConfig::test_default();
        config.containerd_namespace = namespace.clone();
        config.node_name = "mn-worker".to_string();
        config.dataplane_encryption = klights_networking::wireguard::DataplaneEncryption::Disabled;
        let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let cfg = test_config_phase(config, data_root.path(), supervisor.clone());

        let identity = setup_worker(&cfg, "10.99.0.20")
            .await
            .expect("worker identity setup must not require local CA");

        assert!(identity.follower_dataplane.is_some());
        assert!(
            !data_root.path().join("etc/ca.crt").exists(),
            "worker identity setup must not create a local ca.crt"
        );
        assert!(
            !data_root.path().join("etc/ca.key").exists(),
            "worker identity setup must not create a local ca.key"
        );
        assert!(
            !data_root.path().join("etc/server.crt").exists(),
            "worker identity setup must not create local server certs"
        );
        let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn setup_leader_persists_local_node_client_certificate_for_tokenless_rejoin() {
        let namespace = format!("cp-seed-node-cert-{}", uuid::Uuid::new_v4());
        let data_root = crate::paths::test_data_root_fixture(&namespace);
        let mut config = crate::KlightsConfig::test_default();
        config.containerd_namespace = namespace.clone();
        config.node_name = "mn-controlplane1".to_string();
        let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let cfg = test_config_phase(config, data_root.path(), supervisor.clone());

        setup_leader(
            &cfg,
            "10.99.0.10",
            &crate::bootstrap::NodeRole::Controlplane {
                leader_endpoints: vec![],
                token: None,
                skip_ca: false,
                as_learner: false,
            },
        )
        .await
        .expect("seed controlplane identity setup");

        let store =
            klights_auth::worker_credential_store::SupervisedFilesystemWorkerCredentialStore::new(
                data_root.path().join("etc"),
                "mn-controlplane1",
                supervisor.clone(),
            );
        let crypto = klights_supervisor::CryptoExecutor::new(cfg.supervisor.clone());
        let now = klights_auth::clock::Clock::now(&klights_auth::clock::SystemClock);
        let source =
            klights_auth::worker_credential::resolve_worker_credential(&store, &crypto, now)
                .await
                .expect("load persisted node credential");
        assert!(
            matches!(
                source,
                klights_auth::worker_credential::WorkerCredentialSource::Existing(_)
            ),
            "seed controlplane must persist a valid system:node client cert for tokenless rejoin"
        );

        let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn controlplane_token_join_persists_node_client_cert_from_leader_ca() {
        let _lock = ENV_LOCK.lock().await;
        let leader_namespace = format!("cp-leader-ca-{}", uuid::Uuid::new_v4());
        let joiner_namespace = format!("cp-join-node-cert-{}", uuid::Uuid::new_v4());
        let leader_fixture = crate::paths::test_data_root_fixture(&leader_namespace);
        let joiner_fixture = crate::paths::test_data_root_fixture(&joiner_namespace);
        let leader_data_root = leader_fixture.path().to_path_buf();
        let joiner_data_root = joiner_fixture.path().to_path_buf();
        let sqlite =
            crate::bootstrap::composition::cluster_store::selector::canonical_sqlite_fixture()
                .await
                .unwrap();
        let db = std::sync::Arc::new(sqlite.clone());
        crate::bootstrap::composition_tests::cluster_meta::ensure_cluster_metadata_sqlite(&sqlite)
            .await
            .unwrap();
        let bootstrap_store = crate::bootstrap::composition_adapters::leader_bootstrap_store_adapter::LeaderBootstrapStore::new(
            sqlite.focused_read_store(),
            sqlite.focused_read_store(),
            crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::resource_commands_for_test(
                std::sync::Arc::new(sqlite.clone()),
                std::sync::Arc::new(sqlite.clone()),
                sqlite.focused_read_store(),
            ),
        );
        let token = crate::bootstrap::bootstrap_token::ensure_controlplane_bootstrap_token(
            &bootstrap_store,
        )
        .await
        .unwrap();

        let (ca_cert, ca_key, ca_cert_pem, ca_key_pem) =
            klights_auth::test_support::generate_ca_full_at(time::OffsetDateTime::now_utc())
                .unwrap();
        let (server_cert_pem, server_key_pem) =
            klights_auth::test_support::generate_server_cert_at(
                &ca_cert,
                &ca_key,
                time::OffsetDateTime::now_utc(),
            )
            .unwrap();
        drop((ca_cert, ca_key));
        let leader_etc_dir = leader_data_root.join("etc");
        let leader_ca_cert_path = leader_etc_dir.join("ca.crt");
        std::fs::create_dir_all(&leader_etc_dir).unwrap();
        std::fs::write(&leader_ca_cert_path, ca_cert_pem).unwrap();
        std::fs::write(leader_etc_dir.join("ca.key"), ca_key_pem).unwrap();
        std::fs::write(leader_etc_dir.join("server.crt"), server_cert_pem).unwrap();
        std::fs::write(leader_etc_dir.join("server.key"), server_key_pem).unwrap();
        let _leader_ca = EnvVarGuard::set(
            "KLIGHTS_LEADER_CA_CERT",
            leader_ca_cert_path
                .to_str()
                .expect("leader CA fixture path must be valid UTF-8"),
        );
        let leader_service_account_signing_key = test_service_account_signing_key();
        std::fs::write(
            leader_etc_dir.join("service-account-signing.key"),
            &leader_service_account_signing_key,
        )
        .unwrap();

        let leader_supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let composition =
            crate::bootstrap::composition_tests::leader_rpc::support::IntegrationLeaderRpcComposition::new(
                db.clone(),
                std::sync::Arc::new(sqlite.clone()),
                sqlite
                    .clone()
                    .focused_committed_apply(),
                sqlite.clone().focused_read_store(),
            );
        let service =
            std::sync::Arc::new(composition.replication_service(leader_supervisor.clone()));
        let app = composition.mount_service_full(
            axum::Router::new(),
            service,
            None,
            None,
            None,
            None,
            None,
            leader_data_root
                .to_str()
                .expect("leader fixture root must be valid UTF-8"),
            None,
            None,
            None,
            None,
            None,
            klights_leader_rpc::transport_policy::GrpcTransportPolicy::shared_default(),
        );
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let endpoint = format!("https://localhost:{}", addr.port());
        let shutdown = tokio_util::sync::CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server_supervisor = leader_supervisor.clone();
        let server_data_root = leader_data_root.clone();
        let handle = tokio::spawn(async move {
            klights_apiserver::serve_https(
                app,
                &addr.to_string(),
                &server_data_root,
                server_supervisor,
                klights_leader_rpc::transport_policy::GrpcTransportPolicy::default()
                    .tls_handshake_timeout,
                server_shutdown.cancelled_owned(),
            )
            .await
        });
        let listener_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < listener_deadline,
                "control-plane CSR TLS fixture did not start on {addr}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let mut config = crate::KlightsConfig::test_default();
        config.containerd_namespace = joiner_namespace.clone();
        config.data_root = joiner_data_root.clone();
        config.node_name = "mn-controlplane2".to_string();
        config.dataplane_encryption = klights_networking::wireguard::DataplaneEncryption::Disabled;
        config.external_endpoint = Some("10.99.0.14".to_string());
        let joiner_supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let cfg = test_config_phase(config, &joiner_data_root, joiner_supervisor.clone());
        let joiner_etc_dir = joiner_data_root.join("etc");

        setup_leader(
            &cfg,
            "10.99.0.14",
            &crate::bootstrap::NodeRole::Controlplane {
                leader_endpoints: vec![endpoint],
                token: Some(token),
                skip_ca: false,
                as_learner: false,
            },
        )
        .await
        .expect("controlplane token join should persist node client cert from leader CA");

        let store =
            klights_auth::worker_credential_store::SupervisedFilesystemWorkerCredentialStore::new(
                joiner_etc_dir.clone(),
                "mn-controlplane2",
                joiner_supervisor.clone(),
            );
        let crypto = klights_supervisor::CryptoExecutor::new(cfg.supervisor.clone());
        let now = klights_auth::clock::Clock::now(&klights_auth::clock::SystemClock);
        let source =
            klights_auth::worker_credential::resolve_worker_credential(&store, &crypto, now)
                .await
                .expect("load persisted controlplane node credential");
        assert!(
            matches!(
                source,
                klights_auth::worker_credential::WorkerCredentialSource::Existing(_)
            ),
            "joining controlplane must persist a node client cert without generic CSR bootstrap"
        );
        let joined_sa_signer =
            std::fs::read_to_string(joiner_etc_dir.join("service-account-signing.key"))
                .expect("joining controlplane must persist the leader ServiceAccount signing key");
        assert_eq!(joined_sa_signer, leader_service_account_signing_key);

        shutdown.cancel();
        handle
            .await
            .expect("control-plane CSR TLS fixture task must join")
            .expect("control-plane CSR TLS fixture must stop cleanly");
        let _ = joiner_supervisor
            .shutdown(std::time::Duration::from_secs(1))
            .await;
        let _ = leader_supervisor
            .shutdown(std::time::Duration::from_secs(1))
            .await;
    }

    #[tokio::test]
    async fn setup_leader_prepares_wireguard_dataplane_metadata_for_controlplane_join() {
        let namespace = format!("cp-dataplane-{}", uuid::Uuid::new_v4());
        let data_root = crate::paths::test_data_root_fixture(&namespace);
        let mut config = crate::KlightsConfig::test_default();
        config.containerd_namespace = namespace.clone();
        config.node_name = "mn-controlplane2".to_string();
        config.dataplane_encryption = klights_networking::wireguard::DataplaneEncryption::Enabled;
        config.external_endpoint = Some("10.99.0.14".to_string());
        let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let cfg = test_config_phase(config, data_root.path(), supervisor.clone());

        let identity = setup_leader(
            &cfg,
            "10.99.0.14",
            &crate::bootstrap::NodeRole::Controlplane {
                leader_endpoints: vec![],
                token: None,
                skip_ca: false,
                as_learner: false,
            },
        )
        .await
        .expect("controlplane identity setup");
        let dataplane = identity
            .follower_dataplane
            .expect("leader-class identity must prepare local dataplane metadata");

        assert_eq!(dataplane.endpoint, "10.99.0.14");
        assert_eq!(
            dataplane.encryption,
            klights_leader_api::DataplaneEncryption::WireGuard
        );
        assert!(
            dataplane.public_key.is_some(),
            "encrypted raft/controlplane joins must send the local WireGuard public key"
        );
        assert!(
            data_root
                .path()
                .join("etc")
                .join("wireguard-private.key")
                .exists(),
            "dataplane identity must persist the WireGuard private key"
        );

        let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn setup_leader_server_cert_uses_external_endpoint_san_when_internal_ip_differs() {
        let namespace = format!("cp-api-external-{}", uuid::Uuid::new_v4());
        let data_root = crate::paths::test_data_root_fixture(&namespace);
        let mut config = crate::KlightsConfig::test_default();
        config.containerd_namespace = namespace.clone();
        config.node_name = "mn-controlplane1".to_string();
        config.dataplane_encryption = klights_networking::wireguard::DataplaneEncryption::Disabled;
        config.external_endpoint = Some("10.99.0.10".to_string());
        let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let cfg = test_config_phase(config, data_root.path(), supervisor.clone());

        setup_leader(
            &cfg,
            "172.31.10.2",
            &crate::bootstrap::NodeRole::Controlplane {
                leader_endpoints: vec![],
                token: None,
                skip_ca: false,
                as_learner: false,
            },
        )
        .await
        .expect("seed controlplane identity should initialize");

        let server_cert = std::fs::read_to_string(data_root.path().join("etc/server.crt"))
            .expect("server cert must exist");
        let (_, pem) = x509_parser::pem::parse_x509_pem(server_cert.as_bytes())
            .expect("server cert PEM must parse");
        let (_, cert) =
            x509_parser::parse_x509_certificate(&pem.contents).expect("server cert DER must parse");
        let mut ip_sans = std::collections::HashSet::new();
        for ext in cert.extensions() {
            if ext.oid == x509_parser::oid_registry::OID_X509_EXT_SUBJECT_ALT_NAME
                && let x509_parser::extensions::ParsedExtension::SubjectAlternativeName(san) =
                    ext.parsed_extension()
            {
                for name in &san.general_names {
                    if let x509_parser::extensions::GeneralName::IPAddress(bytes) = name
                        && bytes.len() == 4
                    {
                        ip_sans.insert(std::net::Ipv4Addr::new(
                            bytes[0], bytes[1], bytes[2], bytes[3],
                        ));
                    }
                }
            }
        }
        assert!(
            ip_sans.contains(&std::net::Ipv4Addr::new(10, 99, 0, 10)),
            "API server cert must include KLIGHTS_EXTERNAL_ENDPOINT"
        );

        let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn setup_leader_uses_config_phase_paths_instead_of_ambient_namespace_paths() {
        let namespace = format!("cp-captured-paths-{}", uuid::Uuid::new_v4());
        let data_root = crate::paths::test_data_root_fixture(&namespace);
        let mut config = crate::KlightsConfig::test_default();
        config.containerd_namespace = namespace;
        config.node_name = "mn-controlplane1".to_string();
        config.dataplane_encryption = klights_networking::wireguard::DataplaneEncryption::Disabled;
        let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let etc_dir = data_root.path().join("etc");
        let cfg = test_config_phase(config, data_root.path(), supervisor.clone());

        setup_leader(
            &cfg,
            "10.99.0.10",
            &crate::bootstrap::NodeRole::Controlplane {
                leader_endpoints: vec![],
                token: None,
                skip_ca: false,
                as_learner: false,
            },
        )
        .await
        .expect("seed identity must use the paths captured by ConfigPhase");

        assert!(etc_dir.join("ca.crt").exists());
        assert!(etc_dir.join("ca.key").exists());
        assert!(etc_dir.join("server.crt").exists());
        assert!(etc_dir.join("node.crt").exists());
        assert!(etc_dir.join("node.key").exists());

        let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
    }
}
