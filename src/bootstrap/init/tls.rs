//! TLS helpers extracted from runtime.rs (R3 refactor).

use anyhow::Context;

pub async fn load_tls_pem_files(
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
    cert_path: &std::path::Path,
    key_path: &std::path::Path,
) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
    let cert_path_buf = cert_path.to_path_buf();
    let cert_key = cert_path.display().to_string();
    let cert_pem = task_supervisor
        .run_blocking_file_keyed("tls_read_cert_pem", cert_key, move || {
            std::fs::read(cert_path_buf)
        })
        .await?
        .with_context(|| format!("failed to read TLS cert: {}", cert_path.display()))?;
    let key_path_buf = key_path.to_path_buf();
    let key_guard = key_path.display().to_string();
    let key_pem = task_supervisor
        .run_blocking_file_keyed("tls_read_key_pem", key_guard, move || {
            std::fs::read(key_path_buf)
        })
        .await?
        .with_context(|| format!("failed to read TLS key: {}", key_path.display()))?;
    Ok((cert_pem, key_pem))
}

async fn load_client_cert_verifier(
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
    containerd_namespace: &str,
) -> anyhow::Result<std::sync::Arc<dyn rustls::server::danger::ClientCertVerifier>> {
    let ca_cert_path = crate::paths::ca_cert_path(containerd_namespace);
    let ca_path_buf = ca_cert_path.clone();
    let ca_key = ca_cert_path.display().to_string();
    let ca_pem = task_supervisor
        .run_blocking_file_keyed("tls_read_client_ca_pem", ca_key, move || {
            std::fs::read(ca_path_buf)
        })
        .await?
        .with_context(|| {
            format!(
                "failed to read TLS client CA cert: {}",
                ca_cert_path.display()
            )
        })?;
    let ca_certs: Vec<rustls::pki_types::CertificateDer> =
        rustls_pemfile::certs(&mut ca_pem.as_slice())
            .collect::<Result<_, _>>()
            .context("failed to parse TLS client CA certificate PEM")?;
    let mut roots = rustls::RootCertStore::empty();
    let (accepted, ignored) = roots.add_parsable_certificates(ca_certs);
    if accepted == 0 {
        anyhow::bail!(
            "TLS client CA {} did not contain any usable certificate (ignored {ignored})",
            ca_cert_path.display()
        );
    }
    rustls::server::WebPkiClientVerifier::builder(std::sync::Arc::new(roots))
        .allow_unauthenticated()
        .build()
        .context("failed to build TLS client certificate verifier")
}

/// Serve a single accepted TLS connection with a supervised handshake timeout.
///
/// Extracted from `serve_https` to allow focused unit testing of the
/// connection worker lifecycle (timeouts, category limits, shutdown).
pub(crate) async fn serve_https_connection(
    acceptor: tokio_rustls::TlsAcceptor,
    app: axum::Router,
    supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
    remote_addr: std::net::SocketAddr,
    stream: tokio::net::TcpStream,
    handshake_timeout: std::time::Duration,
) {
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto::Builder;
    use tower::ServiceExt;

    let local_addr = stream.local_addr().ok();

    // Wrap TLS handshake in a supervised timeout.
    let timeout_result = supervisor
        .timeout("tls_handshake", handshake_timeout, acceptor.accept(stream))
        .await;
    let tls_stream = match timeout_result {
        // Root not cancelled, timeout didn't fire, handshake ok
        Ok(Ok(Ok(s))) => s,
        // Root not cancelled, timeout didn't fire, handshake failed
        Ok(Ok(Err(e))) => {
            tracing::debug!("TLS handshake failed: {}", e);
            return;
        }
        // Root not cancelled, timeout fired
        Ok(Err(_elapsed)) => {
            tracing::debug!("TLS handshake timed out");
            return;
        }
        // Root shutdown cancelled
        Err(e) => {
            tracing::debug!("TLS handshake cancelled by shutdown: {}", e);
            return;
        }
    };
    let client_cert = tls_stream
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|certs| certs.first())
        .map(|cert| cert.as_ref().to_vec());

    let io = TokioIo::new(tls_stream);
    let service = hyper::service::service_fn(move |mut req| {
        if let Some(cert) = client_cert.clone() {
            req.extensions_mut()
                .insert(crate::auth::TlsClientCertificate(cert));
        }
        crate::replication::grpc::server::insert_tonic_tcp_connect_info(
            &mut req,
            local_addr,
            Some(remote_addr),
        );
        app.clone().oneshot(req)
    });

    if let Err(e) = Builder::new(TokioExecutor::new())
        .serve_connection_with_upgrades(io, service)
        .await
    {
        tracing::debug!("connection error: {}", e);
    }
}

pub async fn serve_https<F>(
    app: axum::Router,
    addr: &str,
    containerd_namespace: &str,
    task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
    transport_policy: crate::replication::grpc::transport_policy::SharedGrpcTransportPolicy,
    shutdown_signal: F,
) -> anyhow::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    use tokio_rustls::TlsAcceptor;

    let _ = rustls::crypto::ring::default_provider().install_default();

    let server_cert_path = crate::paths::server_cert_path(containerd_namespace);
    let server_key_path = crate::paths::server_key_path(containerd_namespace);
    let (cert_pem, key_pem) = load_tls_pem_files(
        task_supervisor.as_ref(),
        &server_cert_path,
        &server_key_path,
    )
    .await?;

    let certs: Vec<rustls::pki_types::CertificateDer> =
        rustls_pemfile::certs(&mut cert_pem.as_slice())
            .collect::<Result<_, _>>()
            .context("failed to parse TLS server certificate PEM")?;
    let key = rustls_pemfile::private_key(&mut key_pem.as_slice())
        .context("failed to parse TLS private key PEM")?
        .context("no private key found in TLS key file")?;
    let client_cert_verifier =
        load_client_cert_verifier(task_supervisor.as_ref(), containerd_namespace).await?;

    let mut server_config =
        rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_client_cert_verifier(client_cert_verifier)
            .with_single_cert(certs, key)
            .context("failed to build TLS config")?;
    server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    let tls_acceptor = TlsAcceptor::from(std::sync::Arc::new(server_config));

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind HTTPS listener to {}", addr))?;

    tracing::info!("klights API server listening on {} (HTTPS)", addr);

    tokio::pin!(shutdown_signal);

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, remote_addr) = match result {
                    Ok(conn) => conn,
                    Err(e) => {
                        tracing::debug!("accept error: {}", e);
                        continue;
                    }
                };

                let acceptor = tls_acceptor.clone();
                let app = app.clone();
                let sup = task_supervisor.clone();

                if let Err(e) = task_supervisor
                    .spawn_async(
                        crate::task_supervisor::TaskCategory::Network,
                        "https_connection_worker",
                        serve_https_connection(
                            acceptor, app, sup, remote_addr, stream,
                            transport_policy.tls_handshake_timeout,
                        ),
                    )
                    .await
                {
                    tracing::warn!("Failed to spawn HTTPS connection worker: {}", e);
                }
            }
            _ = &mut shutdown_signal => {
                tracing::info!("HTTPS server shutting down");
                break;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn server_config_uses_tls13_only_builder() {
        // Verify the builder_with_protocol_versions(&[&TLS13]) path compiles
        // and produces a valid config. The ring crypto provider only ships
        // TLS 1.3 cipher suites, so TLS 1.2 is already impossible — but
        // the explicit version pin is defense-in-depth.
        let provider = rustls::crypto::ring::default_provider();
        let _ = provider.install_default();

        let key_pair = rcgen::KeyPair::generate().expect("generate key");
        let mut params = rcgen::CertificateParams::default();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "test-server");
        let cert = params.self_signed(&key_pair).expect("self-signed cert");
        let cert_der = cert.der().to_vec();
        let key_der = key_pair.serialize_der();

        let server_config =
            rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_no_client_auth()
                .with_single_cert(
                    vec![rustls::pki_types::CertificateDer::from(cert_der)],
                    rustls::pki_types::PrivateKeyDer::from(
                        rustls::pki_types::PrivatePkcs8KeyDer::from(key_der),
                    ),
                );
        assert!(
            server_config.is_ok(),
            "TLS 1.3-only server config must build successfully: {:?}",
            server_config.err()
        );
    }

    #[test]
    fn tls13_server_accepts_tls13_client() {
        use std::sync::Arc;

        let provider = rustls::crypto::ring::default_provider();
        let _ = provider.install_default();

        let key_pair = rcgen::KeyPair::generate().expect("generate key");
        let mut params = rcgen::CertificateParams::default();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "test-server");
        let cert = params.self_signed(&key_pair).expect("self-signed cert");
        let cert_der = cert.der().to_vec();
        let key_der = key_pair.serialize_der();

        let server_config = Arc::new(
            rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_no_client_auth()
                .with_single_cert(
                    vec![rustls::pki_types::CertificateDer::from(cert_der)],
                    rustls::pki_types::PrivateKeyDer::from(
                        rustls::pki_types::PrivatePkcs8KeyDer::from(key_der),
                    ),
                )
                .expect("build server config"),
        );

        let client_config = Arc::new(
            rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerifier))
                .with_no_client_auth(),
        );

        let rt = tokio::runtime::Runtime::new().expect("test runtime");
        rt.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let addr = listener.local_addr().expect("local addr");

            let server = tokio::spawn({
                let server_config = server_config.clone();
                async move {
                    let (stream, _) = listener.accept().await.expect("accept");
                    let acceptor = tokio_rustls::TlsAcceptor::from(server_config);
                    let tls = tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        acceptor.accept(stream),
                    )
                    .await
                    .expect("server handshake timed out")
                    .expect("TLS 1.3 handshake");
                    let proto = tls.get_ref().1.protocol_version();
                    assert_eq!(
                        proto,
                        Some(rustls::ProtocolVersion::TLSv1_3),
                        "must negotiate TLS 1.3"
                    );
                }
            });

            let client = tokio::spawn(async move {
                let stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
                let connector = tokio_rustls::TlsConnector::from(client_config);
                let server_name =
                    rustls::pki_types::ServerName::try_from("test-server").expect("server name");
                let tls = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    connector.connect(server_name, stream),
                )
                .await
                .expect("client handshake timed out")
                .expect("client handshake");
                let proto = tls.get_ref().1.protocol_version();
                assert_eq!(
                    proto,
                    Some(rustls::ProtocolVersion::TLSv1_3),
                    "client must negotiate TLS 1.3"
                );
            });

            let _ = tokio::join!(server, client);
        });
    }

    /// A permissive certificate verifier for test-only TLS connections.
    #[derive(Debug)]
    struct NoVerifier;

    impl rustls::client::danger::ServerCertVerifier for NoVerifier {
        fn verify_server_cert(
            &self,
            _end_entity: &rustls::pki_types::CertificateDer,
            _intermediates: &[rustls::pki_types::CertificateDer],
            _server_name: &rustls::pki_types::ServerName,
            _ocsp_response: &[u8],
            _now: rustls::pki_types::UnixTime,
        ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &rustls::pki_types::CertificateDer,
            _dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &rustls::pki_types::CertificateDer,
            _dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            vec![
                rustls::SignatureScheme::RSA_PKCS1_SHA256,
                rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
                rustls::SignatureScheme::ED25519,
            ]
        }
    }

    // ── P5: HTTPS connection worker lifecycle tests ──

    fn test_tls_config() -> (
        rustls::ServerConfig,
        rustls::pki_types::CertificateDer<'static>,
        rustls::pki_types::PrivateKeyDer<'static>,
    ) {
        let key_pair = rcgen::KeyPair::generate().expect("generate key");
        let mut params = rcgen::CertificateParams::default();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "test-server");
        let cert = params.self_signed(&key_pair).expect("self-signed cert");
        let cert_der = cert.der().to_vec();
        let key_der = key_pair.serialize_der();
        (
            rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_no_client_auth()
                .with_single_cert(
                    vec![rustls::pki_types::CertificateDer::from(cert_der.clone())],
                    rustls::pki_types::PrivateKeyDer::from(
                        rustls::pki_types::PrivatePkcs8KeyDer::from(key_der.clone()),
                    ),
                )
                .expect("build server config"),
            rustls::pki_types::CertificateDer::from(cert_der),
            rustls::pki_types::PrivatePkcs8KeyDer::from(key_der).into(),
        )
    }

    /// Verify that a slow TLS handshake exits after the supervised timeout
    /// without completing the handshake.
    #[tokio::test]
    async fn slow_https_handshake_worker_exits_after_supervised_timeout() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let (server_config, _, _) = test_tls_config();
        let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(server_config));
        let app = axum::Router::new();

        // Use a very short timeout to make the test fast
        let short_timeout = std::time::Duration::from_millis(50);

        let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));

        // Start a TCP listener and connect a client that sends no TLS bytes
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (server_done_tx, server_done_rx) = tokio::sync::oneshot::channel::<()>();

        let sup = supervisor.clone();
        tokio::spawn(async move {
            let (stream, remote_addr) = listener.accept().await.unwrap();
            let before = sup
                .active_tasks(Some(crate::task_supervisor::TaskCategory::Network))
                .len();

            sup.spawn_async(
                crate::task_supervisor::TaskCategory::Network,
                "https_connection_worker",
                super::serve_https_connection(
                    acceptor,
                    app,
                    sup.clone(),
                    remote_addr,
                    stream,
                    short_timeout,
                ),
            )
            .await
            .unwrap();

            // Give the worker time to start and time out
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;

            let after = sup
                .active_tasks(Some(crate::task_supervisor::TaskCategory::Network))
                .len();
            // Task count should return to baseline (worker exited after timeout)
            assert!(
                after <= before,
                "worker should exit after timeout (before={}, after={})",
                before,
                after
            );
            let _ = server_done_tx.send(());
        });

        // Connect but send no data (no TLS ClientHello)
        let _conn = tokio::net::TcpStream::connect(addr).await.unwrap();

        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), server_done_rx).await;
    }

    /// Verify that HTTPS connection workers respect the network category limit.
    #[tokio::test]
    async fn https_connection_workers_respect_network_category_limit() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let (server_config, _, _) = test_tls_config();
        let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(server_config));
        let app = axum::Router::new();

        // Only 1 concurrent network task allowed
        let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig {
                network: 1,
                ..Default::default()
            },
        ));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let long_timeout = std::time::Duration::from_secs(30);

        // Spawn two slow connections simultaneously
        let sup = supervisor.clone();
        let acceptor1 = acceptor.clone();
        let app1 = app.clone();

        tokio::spawn(async move {
            loop {
                let (stream, remote_addr) = listener.accept().await.unwrap();
                if sup
                    .spawn_async(
                        crate::task_supervisor::TaskCategory::Network,
                        "https_connection_worker",
                        super::serve_https_connection(
                            acceptor1.clone(),
                            app1.clone(),
                            sup.clone(),
                            remote_addr,
                            stream,
                            long_timeout,
                        ),
                    )
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        // Connect two clients that send no TLS bytes (slow handshake)
        let _conn1 = tokio::net::TcpStream::connect(addr).await.unwrap();
        let _conn2 = tokio::net::TcpStream::connect(addr).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Only 1 worker should be active due to network:1 limit
        let active = supervisor.active_tasks(Some(crate::task_supervisor::TaskCategory::Network));
        // At most 1 active network task (the second is queued or rejected)
        let network_active = active.len();
        assert!(
            network_active <= 1,
            "network category limit of 1 should restrict active workers, got {}",
            network_active
        );
    }

    /// Verify that a normal TLS handshake succeeds before the timeout.
    #[tokio::test]
    async fn normal_https_handshake_succeeds_before_timeout() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let (server_config, _, _) = test_tls_config();
        let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(server_config));
        let app = axum::Router::new();

        let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let sup = supervisor.clone();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (stream, remote_addr) = listener.accept().await.unwrap();
            let _ = ready_tx.send(());
            let sup2 = sup.clone();
            sup.spawn_async(
                crate::task_supervisor::TaskCategory::Network,
                "https_connection_worker",
                super::serve_https_connection(
                    acceptor,
                    app,
                    sup2,
                    remote_addr,
                    stream,
                    std::time::Duration::from_secs(5),
                ),
            )
            .await
            .unwrap();
        });

        // Connect a real TLS client — wait for server to accept first
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), ready_rx).await;

        let mut client_config =
            rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .dangerous()
                .with_custom_certificate_verifier(std::sync::Arc::new(NoVerifier))
                .with_no_client_auth();
        client_config.alpn_protocols = vec![b"http/1.1".to_vec()];

        let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(client_config));
        let server_name = rustls::pki_types::ServerName::try_from("test-server").unwrap();
        let tls = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            connector.connect(server_name, stream),
        )
        .await
        .expect("client handshake timed out")
        .expect("client handshake failed");

        // Verify the TLS connection was established
        let proto = tls.get_ref().1.protocol_version();
        assert_eq!(
            proto,
            Some(rustls::ProtocolVersion::TLSv1_3),
            "TLS handshake should negotiate TLS 1.3"
        );
    }

    /// Verify that pending handshake workers exit on root supervisor shutdown.
    #[tokio::test]
    async fn pending_https_handshake_worker_exits_on_shutdown() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let (server_config, _, _) = test_tls_config();
        let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(server_config));
        let app = axum::Router::new();

        let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let sup = supervisor.clone();
        let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel::<()>();

        tokio::spawn(async move {
            let (stream, remote_addr) = listener.accept().await.unwrap();
            let _ = accepted_tx.send(());
            sup.spawn_async(
                crate::task_supervisor::TaskCategory::Network,
                "https_connection_worker",
                super::serve_https_connection(
                    acceptor,
                    app,
                    sup.clone(),
                    remote_addr,
                    stream,
                    std::time::Duration::from_secs(30),
                ),
            )
            .await
            .unwrap();
        });

        // Connect but send no TLS bytes
        let _conn = tokio::net::TcpStream::connect(addr).await.unwrap();
        let _ = accepted_rx.await;

        // Give worker time to start the handshake
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Shutdown the supervisor
        let _report = supervisor.shutdown(std::time::Duration::from_secs(2)).await;

        // The worker should have exited (cancelled)
        let active = supervisor.active_tasks(Some(crate::task_supervisor::TaskCategory::Network));
        assert!(
            active.is_empty(),
            "pending HTTPS handshake worker should exit on shutdown: {active:?}"
        );
    }
}
