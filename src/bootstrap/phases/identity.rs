//! Phase 4: Identity — certificates and dataplane metadata.

use anyhow::{Context, Result, anyhow};

use super::config::ConfigPhase;

pub struct IdentityPhase {
    pub node_ip: String,
    pub follower_dataplane: Option<crate::replication::grpc::client::JoinDataplaneMetadata>,
    pub grpc_ca_cert_path: Option<std::path::PathBuf>,
}

/// Leader/full-stack identity: certs + local dataplane metadata (no bootstrap token).
pub async fn setup_leader(
    cfg: &ConfigPhase,
    node_ip: &str,
    role: &crate::bootstrap::NodeRole,
) -> Result<IdentityPhase> {
    use crate::auth;
    use crate::bootstrap::init::dataplane::local_join_dataplane_metadata;

    let local_dataplane = local_join_dataplane_metadata(
        &cfg.config,
        &cfg.node_mode,
        node_ip,
        cfg.supervisor.as_ref(),
    )
    .await
    .context("failed to prepare leader dataplane metadata")?;

    let cert_result = auth::init_certificates(
        auth::InitCertificateRequest {
            tls_port: cfg.config.tls_port,
            context_name: &cfg.config.containerd_namespace,
            service_cidr: &cfg.config.service_cidr,
            pod_subnet: &cfg.config.pod_subnet,
            etc_dir_path: &cfg.etc_dir,
            node_name: &cfg.config.node_name,
            host_ip: Some(api_host_for_certificates(&cfg.config, node_ip)),
            api_fqdn: cfg.config.api_fqdn.as_deref(),
            allow_local_ca_generation: role_allows_local_ca_generation(role),
        },
        cfg.supervisor.as_ref(),
    )
    .await
    .context("Failed to initialize certificates")?;

    let grpc_ca_cert_path = Some(crate::paths::ca_cert_path(&cfg.config.containerd_namespace));

    match cert_result {
        auth::CertInitResult::Complete(_paths) => {}
        auth::CertInitResult::NeedsCsrSign(pending) => {
            resolve_csr_via_rpc(cfg, role, &pending, &local_dataplane)
                .await
                .context("Failed to resolve server cert CSR via leader RPC")?;
        }
    }

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
    pending: &crate::auth::PendingCsr,
    local_dataplane: &crate::replication::grpc::client::JoinDataplaneMetadata,
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
        &cfg.config.containerd_namespace,
        &cfg.config.node_name,
        cfg.supervisor.clone(),
    )
    .await?;

    let rpc_ca_cert_path = csr_signing_ca_cert_path(&cfg.config, role);

    let client = crate::replication::grpc::client::ReplicationGrpcClient::new(
        crate::replication::grpc::client::GrpcClientConfig {
            leader_endpoint: leader_endpoint.clone(),
            token: token_value.clone(),
            node_name: cfg.config.node_name.clone(),
            role: crate::replication::protocol::JoinRole::Worker,
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

    let local_ca_cert_path = crate::paths::ca_cert_path(&cfg.config.containerd_namespace);
    let ca_key_path = crate::paths::ca_key_path(&cfg.config.containerd_namespace);
    let server_cert_path = pending.etc_dir.join("server.crt");

    if !response.ca_cert_pem.is_empty() {
        std::fs::write(&local_ca_cert_path, &response.ca_cert_pem)
            .context("failed to write ca.crt from CSR response")?;
    }

    if persist_ca_key && !response.encrypted_ca_key.is_empty() {
        let nonce_slice = response
            .ca_key_nonce
            .get(..12)
            .ok_or_else(|| anyhow!("ca_key_nonce must be 12 bytes"))?;
        let nonce: [u8; 12] = nonce_slice.try_into().unwrap();
        let ca_key_bytes = crate::auth::ca_transport::decrypt_ca_key(
            &token_value,
            &response.encrypted_ca_key,
            &nonce,
        )
        .context("failed to decrypt ca.key from CSR response")?;
        std::fs::write(&ca_key_path, &ca_key_bytes)
            .context("failed to write ca.key from CSR response")?;
    }

    if persist_ca_key && !response.encrypted_service_account_signing_key.is_empty() {
        let nonce_slice = response
            .service_account_signing_key_nonce
            .get(..12)
            .ok_or_else(|| anyhow!("service_account_signing_key_nonce must be 12 bytes"))?;
        let nonce: [u8; 12] = nonce_slice.try_into().unwrap();
        let service_account_signing_key_bytes = crate::auth::ca_transport::decrypt_ca_key(
            &token_value,
            &response.encrypted_service_account_signing_key,
            &nonce,
        )
        .context("failed to decrypt ServiceAccount signing key from CSR response")?;
        let service_account_signing_key_pem = String::from_utf8(service_account_signing_key_bytes)
            .context("ServiceAccount signing key from CSR response is not UTF-8 PEM")?;
        crate::auth::persist_service_account_signing_key(
            &cfg.config.containerd_namespace,
            &service_account_signing_key_pem,
            cfg.supervisor.as_ref(),
        )
        .await
        .context("failed to persist ServiceAccount signing key from CSR response")?;
    }

    if !response.signed_server_cert.is_empty() {
        std::fs::write(&server_cert_path, &response.signed_server_cert)
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
    let second_pass = crate::auth::init_certificates(
        crate::auth::InitCertificateRequest {
            tls_port: cfg.config.tls_port,
            context_name: &cfg.config.containerd_namespace,
            service_cidr: &cfg.config.service_cidr,
            pod_subnet: &cfg.config.pod_subnet,
            etc_dir_path: &cfg.etc_dir,
            node_name: &cfg.config.node_name,
            host_ip: Some(api_host_for_certificates(
                &cfg.config,
                &local_dataplane.endpoint,
            )),
            api_fqdn: cfg.config.api_fqdn.as_deref(),
            allow_local_ca_generation: false,
        },
        cfg.supervisor.as_ref(),
    )
    .await
    .context("Failed to finalize certificates after CSR resolution")?;

    match second_pass {
        crate::auth::CertInitResult::Complete(_) => Ok(()),
        crate::auth::CertInitResult::NeedsCsrSign(_) => Err(anyhow!(
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
    namespace: &str,
    node_name: &str,
    supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
) -> Result<(Option<String>, Option<String>)> {
    if !token.is_empty() {
        return Ok((None, None));
    }

    use crate::bootstrap::worker_identity::{
        CredentialSource, SupervisedFilesystemWorkerCredentialStore, resolve_credential_async,
    };

    let store =
        SupervisedFilesystemWorkerCredentialStore::for_namespace(namespace, node_name, supervisor);
    match resolve_credential_async(&store).await? {
        CredentialSource::ExistingCert(cred) => {
            Ok((Some(cred.certificate_pem), Some(cred.private_key_pem)))
        }
        CredentialSource::BootstrapRequired => Err(anyhow!(
            "no persisted node client certificate and no token source provided; join with --token-file first"
        )),
    }
}

async fn ensure_local_node_client_certificate(cfg: &ConfigPhase) -> Result<()> {
    use crate::auth::csr_signer::CsrSigner;
    use crate::bootstrap::worker_identity::{
        AsyncWorkerCredentialStore, CredentialSource, SupervisedFilesystemWorkerCredentialStore,
        WorkerCredential, credential_has_group, resolve_credential_async,
    };

    let store = SupervisedFilesystemWorkerCredentialStore::for_namespace(
        &cfg.config.containerd_namespace,
        &cfg.config.node_name,
        cfg.supervisor.clone(),
    );
    if let Ok(CredentialSource::ExistingCert(existing)) = resolve_credential_async(&store).await {
        // Reuse the persisted cert only if it already carries the
        // `system:controlplanes` group. A cert minted before that group existed
        // (in-place upgrade, or a seed-leader cert preserved across harness
        // runs) must be re-minted — otherwise this control plane cannot
        // authorize its outbound raft consensus RPCs and the cluster deadlocks.
        if credential_has_group(&existing, crate::auth::CONTROLPLANE_NODES_GROUP) {
            return Ok(());
        }
        tracing::info!(
            "re-minting control-plane node client certificate to add the system:controlplanes group"
        );
    }

    let ca_cert_path = crate::paths::ca_cert_path(&cfg.config.containerd_namespace);
    let ca_key_path = crate::paths::ca_key_path(&cfg.config.containerd_namespace);
    let ca_cert_path_for_task = ca_cert_path.clone();
    let ca_key_path_for_task = ca_key_path.clone();
    let (ca_cert_pem, ca_key_pem) = cfg
        .supervisor
        .run_blocking_file_keyed(
            "controlplane_node_client_ca_load",
            ca_cert_path.display().to_string(),
            move || -> std::io::Result<(String, String)> {
                Ok((
                    std::fs::read_to_string(&ca_cert_path_for_task)?,
                    std::fs::read_to_string(&ca_key_path_for_task)?,
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

    let csr = crate::auth::kubelet_client_cert::generate_kubelet_client_csr(&cfg.config.node_name)
        .context("failed to generate local node client CSR")?;
    let signer = crate::auth::csr_signer::CaCsrSigner::new(ca_cert_pem, ca_key_pem);
    let signed = signer
        .sign(crate::auth::csr_signer::SignRequest {
            csr_pem: csr.csr_pem,
            common_name: format!("system:node:{}", cfg.config.node_name),
            // Control-plane nodes (seed leader, joining controlplanes, and
            // learner replicas — all reach this path only after a
            // controlplane-token-gated bootstrap) carry the extra
            // `system:controlplanes` group. It is the authorization signal for
            // raft consensus RPCs; worker node certs, signed via the Kubernetes
            // CSR API, carry only `system:nodes` and cannot drive consensus.
            organizations: vec![
                crate::auth::NODES_GROUP.to_string(),
                crate::auth::CONTROLPLANE_NODES_GROUP.to_string(),
            ],
            usages: vec!["client auth".to_string()],
            ttl_seconds: 31_536_000,
        })
        .map_err(anyhow::Error::msg)
        .context("failed to sign local node client certificate")?;

    store
        .save(&WorkerCredential {
            certificate_pem: signed.certificate_pem,
            private_key_pem: csr.private_key_pem,
            node_name: cfg.config.node_name.clone(),
            kubeconfig_yaml: String::new(),
        })
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
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

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

    #[test]
    fn csr_signing_ca_cert_path_prefers_leader_ca_for_controlplane_join() {
        let _lock = ENV_LOCK.lock().unwrap();
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
        let mut config = crate::KlightsConfig::test_default();
        config.containerd_namespace = namespace.clone();
        config.node_name = "mn-worker".to_string();
        config.dataplane_encryption = crate::networking::wireguard::DataplaneEncryption::Disabled;
        let config = std::sync::Arc::new(config);
        let node_mode = crate::bootstrap::NodeMode::Root;
        let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let cfg = crate::bootstrap::phases::config::ConfigPhase {
            config: config.clone(),
            node_mode: node_mode.clone(),
            supervisor: supervisor.clone(),
            grpc_transport_policy:
                crate::replication::grpc::transport_policy::GrpcTransportPolicy::shared_default(),
            network_cleanup: crate::networking::NetworkCleanup::from_config(&node_mode, &config),
            shutdown_token: tokio_util::sync::CancellationToken::new(),
            etc_dir: crate::paths::etc_dir_path(&namespace)
                .to_string_lossy()
                .into_owned(),
            containerd_data_dir: crate::paths::containerd_data_dir_path(&namespace)
                .to_string_lossy()
                .into_owned(),
            containerd_state_dir: crate::paths::containerd_state_dir_path(&namespace)
                .to_string_lossy()
                .into_owned(),
        };

        let identity = setup_worker(&cfg, "10.99.0.20")
            .await
            .expect("worker identity setup must not require local CA");

        assert!(identity.follower_dataplane.is_some());
        assert!(
            !crate::paths::ca_cert_path(&namespace).exists(),
            "worker identity setup must not create a local ca.crt"
        );
        assert!(
            !crate::paths::ca_key_path(&namespace).exists(),
            "worker identity setup must not create a local ca.key"
        );
        assert!(
            !crate::paths::server_cert_path(&namespace).exists(),
            "worker identity setup must not create local server certs"
        );
        let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
        let _ = std::fs::remove_dir_all(crate::paths::data_root_path(&namespace));
    }

    #[tokio::test]
    async fn setup_leader_persists_local_node_client_certificate_for_tokenless_rejoin() {
        let namespace = format!("cp-seed-node-cert-{}", uuid::Uuid::new_v4());
        let mut config = crate::KlightsConfig::test_default();
        config.containerd_namespace = namespace.clone();
        config.node_name = "mn-controlplane1".to_string();
        let config = std::sync::Arc::new(config);
        let node_mode = crate::bootstrap::NodeMode::Root;
        let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let cfg = crate::bootstrap::phases::config::ConfigPhase {
            config: config.clone(),
            node_mode: node_mode.clone(),
            supervisor: supervisor.clone(),
            grpc_transport_policy:
                crate::replication::grpc::transport_policy::GrpcTransportPolicy::shared_default(),
            network_cleanup: crate::networking::NetworkCleanup::from_config(&node_mode, &config),
            shutdown_token: tokio_util::sync::CancellationToken::new(),
            etc_dir: crate::paths::etc_dir_path(&namespace)
                .to_string_lossy()
                .into_owned(),
            containerd_data_dir: crate::paths::containerd_data_dir_path(&namespace)
                .to_string_lossy()
                .into_owned(),
            containerd_state_dir: crate::paths::containerd_state_dir_path(&namespace)
                .to_string_lossy()
                .into_owned(),
        };

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

        let store = crate::bootstrap::worker_identity::SupervisedFilesystemWorkerCredentialStore::for_namespace(
            &namespace,
            "mn-controlplane1",
            supervisor.clone(),
        );
        let source = crate::bootstrap::worker_identity::resolve_credential_async(&store)
            .await
            .expect("load persisted node credential");
        assert!(
            matches!(
                source,
                crate::bootstrap::worker_identity::CredentialSource::ExistingCert(_)
            ),
            "seed controlplane must persist a valid system:node client cert for tokenless rejoin"
        );

        let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
        let _ = std::fs::remove_dir_all(crate::paths::data_root_path(&namespace));
    }

    #[tokio::test]
    async fn controlplane_token_join_persists_node_client_cert_from_leader_ca() {
        let leader_namespace = format!("cp-leader-ca-{}", uuid::Uuid::new_v4());
        let joiner_namespace = format!("cp-join-node-cert-{}", uuid::Uuid::new_v4());
        let db: crate::datastore::DatastoreHandle =
            std::sync::Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let token =
            crate::bootstrap::bootstrap_token::ensure_controlplane_bootstrap_token(db.as_ref())
                .await
                .unwrap();

        let (ca_cert, ca_key, ca_cert_pem, ca_key_pem) = crate::auth::generate_ca_full().unwrap();
        drop((ca_cert, ca_key));
        let leader_ca_cert_path = crate::paths::ca_cert_path(&leader_namespace);
        std::fs::create_dir_all(leader_ca_cert_path.parent().unwrap()).unwrap();
        std::fs::write(&leader_ca_cert_path, ca_cert_pem).unwrap();
        std::fs::write(crate::paths::ca_key_path(&leader_namespace), ca_key_pem).unwrap();
        let leader_service_account_signing_key = test_service_account_signing_key();
        std::fs::write(
            crate::paths::service_account_signing_key_path(&leader_namespace),
            &leader_service_account_signing_key,
        )
        .unwrap();

        let leader_supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let service = std::sync::Arc::new(crate::replication::service::ReplicationService::new(
            db.clone(),
            leader_supervisor.clone(),
        ));
        let app = crate::replication::grpc::server::mount_service_full(
            axum::Router::new(),
            service,
            db,
            None,
            None,
            None,
            None,
            &leader_namespace,
            None,
            None,
            crate::replication::grpc::transport_policy::GrpcTransportPolicy::shared_default(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let mut config = crate::KlightsConfig::test_default();
        config.containerd_namespace = joiner_namespace.clone();
        config.node_name = "mn-controlplane2".to_string();
        config.dataplane_encryption = crate::networking::wireguard::DataplaneEncryption::Disabled;
        config.external_endpoint = Some("10.99.0.14".to_string());
        let config = std::sync::Arc::new(config);
        let node_mode = crate::bootstrap::NodeMode::Root;
        let joiner_supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let cfg = crate::bootstrap::phases::config::ConfigPhase {
            config: config.clone(),
            node_mode: node_mode.clone(),
            supervisor: joiner_supervisor.clone(),
            grpc_transport_policy:
                crate::replication::grpc::transport_policy::GrpcTransportPolicy::shared_default(),
            network_cleanup: crate::networking::NetworkCleanup::from_config(&node_mode, &config),
            shutdown_token: tokio_util::sync::CancellationToken::new(),
            etc_dir: crate::paths::etc_dir_path(&joiner_namespace)
                .to_string_lossy()
                .into_owned(),
            containerd_data_dir: crate::paths::containerd_data_dir_path(&joiner_namespace)
                .to_string_lossy()
                .into_owned(),
            containerd_state_dir: crate::paths::containerd_state_dir_path(&joiner_namespace)
                .to_string_lossy()
                .into_owned(),
        };

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
            crate::bootstrap::worker_identity::SupervisedFilesystemWorkerCredentialStore::for_namespace(
                &joiner_namespace,
                "mn-controlplane2",
                joiner_supervisor.clone(),
            );
        let source = crate::bootstrap::worker_identity::resolve_credential_async(&store)
            .await
            .expect("load persisted controlplane node credential");
        assert!(
            matches!(
                source,
                crate::bootstrap::worker_identity::CredentialSource::ExistingCert(_)
            ),
            "joining controlplane must persist a node client cert without generic CSR bootstrap"
        );
        let joined_sa_signer = std::fs::read_to_string(
            crate::paths::service_account_signing_key_path(&joiner_namespace),
        )
        .expect("joining controlplane must persist the leader ServiceAccount signing key");
        assert_eq!(joined_sa_signer, leader_service_account_signing_key);

        handle.abort();
        let _ = joiner_supervisor
            .shutdown(std::time::Duration::from_secs(1))
            .await;
        let _ = leader_supervisor
            .shutdown(std::time::Duration::from_secs(1))
            .await;
        let _ = std::fs::remove_dir_all(crate::paths::data_root_path(&joiner_namespace));
        let _ = std::fs::remove_dir_all(crate::paths::data_root_path(&leader_namespace));
    }

    #[tokio::test]
    async fn setup_leader_prepares_wireguard_dataplane_metadata_for_controlplane_join() {
        let namespace = format!("cp-dataplane-{}", uuid::Uuid::new_v4());
        let mut config = crate::KlightsConfig::test_default();
        config.containerd_namespace = namespace.clone();
        config.node_name = "mn-controlplane2".to_string();
        config.dataplane_encryption = crate::networking::wireguard::DataplaneEncryption::Enabled;
        config.external_endpoint = Some("10.99.0.14".to_string());
        let config = std::sync::Arc::new(config);
        let node_mode = crate::bootstrap::NodeMode::Root;
        let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let cfg = crate::bootstrap::phases::config::ConfigPhase {
            config: config.clone(),
            node_mode: node_mode.clone(),
            supervisor: supervisor.clone(),
            grpc_transport_policy:
                crate::replication::grpc::transport_policy::GrpcTransportPolicy::shared_default(),
            network_cleanup: crate::networking::NetworkCleanup::from_config(&node_mode, &config),
            shutdown_token: tokio_util::sync::CancellationToken::new(),
            etc_dir: crate::paths::etc_dir_path(&namespace)
                .to_string_lossy()
                .into_owned(),
            containerd_data_dir: crate::paths::containerd_data_dir_path(&namespace)
                .to_string_lossy()
                .into_owned(),
            containerd_state_dir: crate::paths::containerd_state_dir_path(&namespace)
                .to_string_lossy()
                .into_owned(),
        };

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
            crate::networking::wireguard::DataplaneEncryption::Enabled
        );
        assert!(
            dataplane.public_key.is_some(),
            "encrypted raft/controlplane joins must send the local WireGuard public key"
        );
        assert!(
            crate::paths::etc_dir_path(&namespace)
                .join("wireguard-private.key")
                .exists(),
            "dataplane identity must persist the WireGuard private key"
        );

        let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
        let _ = std::fs::remove_dir_all(crate::paths::data_root_path(&namespace));
    }

    #[tokio::test]
    async fn setup_leader_server_cert_uses_external_endpoint_san_when_internal_ip_differs() {
        let namespace = format!("cp-api-external-{}", uuid::Uuid::new_v4());
        let mut config = crate::KlightsConfig::test_default();
        config.containerd_namespace = namespace.clone();
        config.node_name = "mn-controlplane1".to_string();
        config.dataplane_encryption = crate::networking::wireguard::DataplaneEncryption::Disabled;
        config.external_endpoint = Some("10.99.0.10".to_string());
        let config = std::sync::Arc::new(config);
        let node_mode = crate::bootstrap::NodeMode::Root;
        let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let cfg = crate::bootstrap::phases::config::ConfigPhase {
            config: config.clone(),
            node_mode: node_mode.clone(),
            supervisor: supervisor.clone(),
            grpc_transport_policy:
                crate::replication::grpc::transport_policy::GrpcTransportPolicy::shared_default(),
            network_cleanup: crate::networking::NetworkCleanup::from_config(&node_mode, &config),
            shutdown_token: tokio_util::sync::CancellationToken::new(),
            etc_dir: crate::paths::etc_dir_path(&namespace)
                .to_string_lossy()
                .into_owned(),
            containerd_data_dir: crate::paths::containerd_data_dir_path(&namespace)
                .to_string_lossy()
                .into_owned(),
            containerd_state_dir: crate::paths::containerd_state_dir_path(&namespace)
                .to_string_lossy()
                .into_owned(),
        };

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

        let server_cert = std::fs::read_to_string(crate::paths::server_cert_path(&namespace))
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
        let _ = std::fs::remove_dir_all(crate::paths::data_root_path(&namespace));
    }
}
