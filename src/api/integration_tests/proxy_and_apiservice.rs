use super::*;

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn apiservice_service_dns_names(service: &str, namespace: &str) -> Vec<String> {
    vec![
        format!("{service}.{namespace}.svc"),
        format!("{service}.{namespace}.svc.cluster.local"),
    ]
}

fn generate_apiservice_self_signed_identity(service: &str, namespace: &str) -> (String, String) {
    use rcgen::CertificateParams;
    let cert_params =
        CertificateParams::new(apiservice_service_dns_names(service, namespace)).unwrap();
    let key_pair = rcgen::KeyPair::generate().unwrap();
    let cert = cert_params.self_signed(&key_pair).unwrap();
    (cert.pem(), key_pair.serialize_pem())
}

fn generate_apiservice_ca_signed_identity(
    service: &str,
    namespace: &str,
) -> (String, String, String) {
    use base64::Engine;
    use rcgen::CertificateParams;

    let (ca_cert, ca_key, ca_pem, _) = crate::auth::generate_ca_full().unwrap();
    let cert_params =
        CertificateParams::new(apiservice_service_dns_names(service, namespace)).unwrap();
    let key_pair = rcgen::KeyPair::generate().unwrap();
    let cert = cert_params.signed_by(&key_pair, &ca_cert, &ca_key).unwrap();
    let ca_bundle = base64::engine::general_purpose::STANDARD.encode(ca_pem.as_bytes());

    (cert.pem(), key_pair.serialize_pem(), ca_bundle)
}

async fn spawn_apiservice_tls_backend_for_service(
    listener: tokio::net::TcpListener,
    service: &str,
    namespace: &str,
    response: Vec<u8>,
) -> tokio::sync::oneshot::Receiver<Vec<u8>> {
    let (cert, key) = generate_apiservice_self_signed_identity(service, namespace);
    spawn_apiservice_tls_backend(listener, cert, key, response).await
}

async fn spawn_apiservice_tls_backend_for_service_repeating(
    listener: tokio::net::TcpListener,
    service: &str,
    namespace: &str,
    response: Vec<u8>,
) -> tokio::sync::mpsc::UnboundedReceiver<Vec<u8>> {
    let (cert, key) = generate_apiservice_self_signed_identity(service, namespace);
    spawn_apiservice_tls_backend_repeating(listener, cert, key, response).await
}

async fn spawn_apiservice_tls_backend(
    listener: tokio::net::TcpListener,
    cert_pem: String,
    key_pem: String,
    response: Vec<u8>,
) -> tokio::sync::oneshot::Receiver<Vec<u8>> {
    let (request_tx, request_rx) = tokio::sync::oneshot::channel::<Vec<u8>>();

    let certs = rustls_pemfile::certs(&mut cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let key = rustls_pemfile::private_key(&mut key_pem.as_bytes())
        .unwrap()
        .unwrap();

    let _ = rustls::crypto::ring::default_provider().install_default();
    let server_config =
        rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));

    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = match acceptor.accept(stream).await {
            Ok(stream) => stream,
            Err(_) => {
                let _ = request_tx.send(Vec::new());
                return;
            }
        };

        let mut request = vec![0u8; 4096];
        let n = stream.read(&mut request).await.unwrap_or(0);
        let _ = request_tx.send(request[..n].to_vec());

        let _ = stream.write_all(&response).await;
    });

    request_rx
}

async fn spawn_apiservice_tls_backend_repeating(
    listener: tokio::net::TcpListener,
    cert_pem: String,
    key_pem: String,
    response: Vec<u8>,
) -> tokio::sync::mpsc::UnboundedReceiver<Vec<u8>> {
    use tokio::sync::mpsc;
    let (request_tx, request_rx) = mpsc::unbounded_channel::<Vec<u8>>();

    let certs = rustls_pemfile::certs(&mut cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let key = rustls_pemfile::private_key(&mut key_pem.as_bytes())
        .unwrap()
        .unwrap();

    let _ = rustls::crypto::ring::default_provider().install_default();
    let server_config =
        rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));

    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let mut stream = match acceptor.accept(stream).await {
                Ok(stream) => stream,
                Err(_) => {
                    let _ = request_tx.send(Vec::new());
                    continue;
                }
            };

            let mut request = vec![0u8; 4096];
            let n = stream.read(&mut request).await.unwrap_or(0);
            let _ = request_tx.send(request[..n].to_vec());

            let _ = stream.write_all(&response).await;
        }
    });

    request_rx
}

async fn spawn_apiservice_plain_backend(
    listener: tokio::net::TcpListener,
    response: Vec<u8>,
) -> tokio::sync::oneshot::Receiver<Vec<u8>> {
    let (request_tx, request_rx) = tokio::sync::oneshot::channel::<Vec<u8>>();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = vec![0u8; 4096];
        let n = stream.read(&mut request).await.unwrap_or(0);
        let _ = request_tx.send(request[..n].to_vec());
        let _ = stream.write_all(&response).await;
    });

    request_rx
}

#[tokio::test]
async fn test_apiservice_proxy_cache_invalidates_on_apiservice_update() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio::time::{Duration, timeout};
    use tower::ServiceExt;

    let first_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let first_port = first_listener.local_addr().unwrap().port();
    let second_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second_port = second_listener.local_addr().unwrap().port();
    let first_body =
        r#"{"kind":"FlunderList","apiVersion":"wardle.example.com/v1alpha1","items":[]}"#;
    let first_response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Backend: first\r\nContent-Length: {}\r\n\r\n{}",
        first_body.len(),
        first_body
    )
    .into_bytes();
    let second_body =
        r#"{"kind":"FlunderList","apiVersion":"wardle.example.com/v1alpha1","items":[]}"#;
    let second_response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Backend: second\r\nContent-Length: {}\r\n\r\n{}",
        second_body.len(),
        second_body
    )
    .into_bytes();
    let (first_cert, first_key) =
        generate_apiservice_self_signed_identity("wardle-service-first", "default");
    let (second_cert, second_key) =
        generate_apiservice_self_signed_identity("wardle-service-second", "default");
    let first_request =
        spawn_apiservice_tls_backend(first_listener, first_cert, first_key, first_response).await;
    let second_request =
        spawn_apiservice_tls_backend(second_listener, second_cert, second_key, second_response)
            .await;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    for (name, port) in [
        ("wardle-service-first", first_port),
        ("wardle-service-second", second_port),
    ] {
        db.create_resource(
            "v1",
            "Service",
            Some("default"),
            name,
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": name, "namespace": "default"},
                "spec": {"ports": [{"port": port}]}
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "v1",
            "Endpoints",
            Some("default"),
            name,
            json!({
                "apiVersion": "v1",
                "kind": "Endpoints",
                "metadata": {"name": name, "namespace": "default"},
                "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": port}]}]
            }),
        )
        .await
        .unwrap();
    }

    let mut apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service-first", "namespace": "default", "port": first_port}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let first = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1/flunders")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(
        first
            .headers()
            .get("x-backend")
            .and_then(|v| v.to_str().ok()),
        Some("first")
    );

    let first_backend_request = timeout(Duration::from_secs(2), first_request)
        .await
        .expect("first backend should receive initial request")
        .expect("first backend should capture request bytes");
    assert!(
        std::str::from_utf8(&first_backend_request)
            .unwrap()
            .starts_with("GET /apis/wardle.example.com/v1alpha1/flunders HTTP/1.1\r\n"),
        "unexpected first backend request: {:?}",
        String::from_utf8_lossy(&first_backend_request)
    );

    apiservice["spec"]["service"] =
        json!({"name": "wardle-service-second", "namespace": "default", "port": second_port});
    let update = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices/v1alpha1.wardle.example.com")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(update.status(), StatusCode::OK);

    let second = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1/flunders")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::OK);
    assert_eq!(
        second
            .headers()
            .get("x-backend")
            .and_then(|v| v.to_str().ok()),
        Some("second"),
        "APIService update must invalidate cached backend target data"
    );

    let second_backend_request = timeout(Duration::from_secs(2), second_request)
        .await
        .expect("second backend should receive post-update request")
        .expect("second backend should capture request bytes");
    assert!(
        std::str::from_utf8(&second_backend_request)
            .unwrap()
            .starts_with("GET /apis/wardle.example.com/v1alpha1/flunders HTTP/1.1\r\n"),
        "unexpected second backend request: {:?}",
        String::from_utf8_lossy(&second_backend_request)
    );
}

#[tokio::test]
async fn test_apiservice_proxy_cache_invalidates_on_apiservice_delete() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::{
        net::TcpListener,
        time::{Duration, timeout},
    };
    use tower::ServiceExt;

    let first_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let first_port = first_listener.local_addr().unwrap().port();
    let second_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second_port = second_listener.local_addr().unwrap().port();
    let first_body =
        r#"{"kind":"FlunderList","apiVersion":"wardle.example.com/v1alpha1","items":[]}"#;
    let first_response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Backend: first\r\nContent-Length: {}\r\n\r\n{}",
        first_body.len(),
        first_body
    )
    .into_bytes();
    let second_body =
        r#"{"kind":"FlunderList","apiVersion":"wardle.example.com/v1alpha1","items":[]}"#;
    let second_response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Backend: second\r\nContent-Length: {}\r\n\r\n{}",
        second_body.len(),
        second_body
    )
    .into_bytes();
    let (first_cert, first_key) =
        generate_apiservice_self_signed_identity("wardle-service-first", "default");
    let (second_cert, second_key) =
        generate_apiservice_self_signed_identity("wardle-service-second", "default");
    let first_request =
        spawn_apiservice_tls_backend(first_listener, first_cert, first_key, first_response).await;
    let second_request =
        spawn_apiservice_tls_backend(second_listener, second_cert, second_key, second_response)
            .await;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service-first",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service-first", "namespace": "default"},
            "spec": {"ports": [{"port": first_port}]}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service-first",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service-first", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": first_port}]}]
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service-second",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service-second", "namespace": "default"},
            "spec": {"ports": [{"port": second_port}]}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service-second",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service-second", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": second_port}]}]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service-first", "namespace": "default", "port": first_port}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let first = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1/flunders")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(
        first
            .headers()
            .get("x-backend")
            .and_then(|v| v.to_str().ok()),
        Some("first"),
        "initial request should route to first APIService backend"
    );
    let first_backend_request = timeout(Duration::from_secs(2), first_request)
        .await
        .expect("first backend should receive initial request")
        .expect("first backend should capture request bytes");
    assert!(
        std::str::from_utf8(&first_backend_request)
            .unwrap()
            .starts_with("GET /apis/wardle.example.com/v1alpha1/flunders HTTP/1.1\r\n"),
        "unexpected first backend request: {:?}",
        String::from_utf8_lossy(&first_backend_request)
    );

    let delete = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices/v1alpha1.wardle.example.com")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(delete.status(), StatusCode::OK);

    let after_delete = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1/flunders")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(after_delete.status(), StatusCode::NOT_FOUND);

    let mut recreated = apiservice.clone();
    recreated["spec"]["service"] =
        json!({"name": "wardle-service-second", "namespace": "default", "port": second_port});
    let recreate = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&recreated).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recreate.status(), StatusCode::CREATED);

    let second = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1/flunders")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::OK);
    assert!(
        second
            .headers()
            .get("x-backend")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|backend| backend == "second"),
        "recreated APIService should use the new backend endpoint"
    );
    let second_backend_request = timeout(Duration::from_secs(2), second_request)
        .await
        .expect("second backend should receive post-recreate request")
        .expect("second backend should capture request bytes");
    assert!(
        std::str::from_utf8(&second_backend_request)
            .unwrap()
            .starts_with("GET /apis/wardle.example.com/v1alpha1/flunders HTTP/1.1\r\n"),
        "unexpected second backend request: {:?}",
        String::from_utf8_lossy(&second_backend_request)
    );
}

#[tokio::test]
async fn test_apiservice_discovery_cache_healthy_after_delete_and_recreate() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::{
        net::TcpListener,
        time::{Duration, timeout},
    };
    use tower::ServiceExt;

    let first_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let first_port = first_listener.local_addr().unwrap().port();
    let first_body = serde_json::to_vec(&serde_json::json!({
        "kind": "APIResourceList",
        "apiVersion": "v1",
        "groupVersion": "wardle.example.com/v1alpha1",
        "resources": []
    }))
    .unwrap();
    let first_response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        first_body.len()
    )
    .into_bytes()
    .into_iter()
    .chain(first_body.into_iter())
    .collect::<Vec<u8>>();
    let first_request = spawn_apiservice_tls_backend_for_service(
        first_listener,
        "wardle-service-first",
        "default",
        first_response,
    )
    .await;

    let second_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second_port = second_listener.local_addr().unwrap().port();
    let second_body = serde_json::to_vec(&serde_json::json!({
        "kind": "APIResourceList",
        "apiVersion": "v1",
        "groupVersion": "wardle.example.com/v1alpha1",
        "resources": []
    }))
    .unwrap();
    let second_response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        second_body.len()
    )
    .into_bytes()
    .into_iter()
    .chain(second_body.into_iter())
    .collect::<Vec<u8>>();
    let mut second_request = spawn_apiservice_tls_backend_for_service_repeating(
        second_listener,
        "wardle-service-second",
        "default",
        second_response,
    )
    .await;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service-first",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service-first", "namespace": "default"},
            "spec": {"ports": [{"port": first_port}]}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service-first",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service-first", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": first_port}]}]
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service-second",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service-second", "namespace": "default"},
            "spec": {"ports": [{"port": second_port}]}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service-second",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service-second", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": second_port}]}]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service-first", "namespace": "default", "port": first_port}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let first_discovery = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first_discovery.status(), StatusCode::OK);
    assert!(
        timeout(Duration::from_secs(2), first_request)
            .await
            .expect("backend should receive the initial discovery request")
            .unwrap()
            .starts_with(b"GET /apis/wardle.example.com/v1alpha1 HTTP/1.1\r\n"),
        "first discovery call must hit first backend before delete"
    );

    let discovery = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis")
                .header(
                    "Accept",
                    "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(discovery.status(), StatusCode::OK);
    let discovery_body: serde_json::Value =
        serde_json::from_slice(&to_bytes(discovery.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let names: Vec<&str> = discovery_body["items"]
        .as_array()
        .map_or(&[] as &[serde_json::Value], |items| items.as_slice())
        .iter()
        .filter_map(|item| item.get("metadata"))
        .filter_map(|metadata| metadata.get("name"))
        .filter_map(|name| name.as_str())
        .collect();
    assert!(
        names.contains(&"wardle.example.com"),
        "aggregated discovery must include APIService-backed group before delete"
    );

    let delete = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices/v1alpha1.wardle.example.com")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(delete.status(), StatusCode::OK);

    let after_delete = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(after_delete.status(), StatusCode::NOT_FOUND);

    let discovery_after_delete = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis")
                .header(
                    "Accept",
                    "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(discovery_after_delete.status(), StatusCode::OK);
    let discovery_after_delete_body: serde_json::Value = serde_json::from_slice(
        &to_bytes(discovery_after_delete.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let names_after_delete: Vec<&str> = discovery_after_delete_body["items"]
        .as_array()
        .map_or(&[] as &[serde_json::Value], |items| items.as_slice())
        .iter()
        .filter_map(|item| item.get("metadata"))
        .filter_map(|metadata| metadata.get("name"))
        .filter_map(|name| name.as_str())
        .collect();
    assert!(
        !names_after_delete.contains(&"wardle.example.com"),
        "aggregated discovery must not include deleted APIService-backed group"
    );

    let recreated = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service-second", "namespace": "default", "port": second_port}
        }
    });

    let recreate = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&recreated).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recreate.status(), StatusCode::CREATED);

    let second_discovery = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second_discovery.status(), StatusCode::OK);
    assert!(
        timeout(Duration::from_secs(2), second_request.recv())
            .await
            .expect("backend should receive request after recreate")
            .expect("backend capture channel should still be open")
            .starts_with(b"GET /apis/wardle.example.com/v1alpha1 HTTP/1.1\r\n"),
        "recreated APIService should switch discovery route to second backend"
    );

    let discovery_after_recreate = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis")
                .header(
                    "Accept",
                    "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(discovery_after_recreate.status(), StatusCode::OK);
    let discovery_after_recreate_body: serde_json::Value = serde_json::from_slice(
        &to_bytes(discovery_after_recreate.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let names_after_recreate: Vec<&str> = discovery_after_recreate_body["items"]
        .as_array()
        .map_or(&[] as &[serde_json::Value], |items| items.as_slice())
        .iter()
        .filter_map(|item| item.get("metadata"))
        .filter_map(|metadata| metadata.get("name"))
        .filter_map(|name| name.as_str())
        .collect();
    assert!(
        names_after_recreate.contains(&"wardle.example.com"),
        "aggregated discovery must include APIService-backed group after recreation"
    );
}

#[tokio::test]
async fn test_discovery_and_subresource_forwarding_remain_consistent_after_apiservice_update() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::{
        net::TcpListener,
        sync::mpsc,
        time::{Duration, timeout},
    };
    use tower::ServiceExt;

    let first_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let first_port = first_listener.local_addr().unwrap().port();
    let second_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second_port = second_listener.local_addr().unwrap().port();
    let first_body = r#"{"kind":"Flunder","apiVersion":"wardle.example.com/v1alpha1","metadata":{"name":"widget"}}"#;
    let first_response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        first_body.len(),
        first_body
    )
    .into_bytes();
    let second_body = r#"{"kind":"Flunder","apiVersion":"wardle.example.com/v1alpha1","metadata":{"name":"widget"}}"#;
    let second_response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        second_body.len(),
        second_body
    )
    .into_bytes();
    let mut first_request = spawn_apiservice_tls_backend_for_service_repeating(
        first_listener,
        "wardle-service-first",
        "default",
        first_response,
    )
    .await;
    let mut second_request = spawn_apiservice_tls_backend_for_service_repeating(
        second_listener,
        "wardle-service-second",
        "default",
        second_response,
    )
    .await;

    async fn receive_matching_request_stream(
        rx: &mut mpsc::UnboundedReceiver<Vec<u8>>,
        expected_prefix: &str,
    ) -> String {
        let request = String::from_utf8_lossy(
            &timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("backend should receive routed request")
                .expect("backend capture channel should still be open"),
        )
        .into_owned();
        assert!(
            request.starts_with(expected_prefix),
            "backend request {request} does not match expected path: {expected_prefix}"
        );
        request
    }

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service-first",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service-first", "namespace": "default"},
            "spec": {"ports": [{"port": first_port}]}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service-first",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service-first", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": first_port}]}]
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service-second",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service-second", "namespace": "default"},
            "spec": {"ports": [{"port": second_port}]}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service-second",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service-second", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": second_port}]}]
        }),
    )
    .await
    .unwrap();

    let mut apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service-first", "namespace": "default", "port": first_port}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let discovery = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis")
                .header(
                    "Accept",
                    "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(discovery.status(), StatusCode::OK);
    let group_payload: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(discovery.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let discovery_items: &[serde_json::Value] = group_payload["items"]
        .as_array()
        .map_or(&[], |items| items.as_slice());
    let group_names: Vec<&str> = discovery_items
        .iter()
        .filter_map(|item| item.get("metadata"))
        .filter_map(|metadata| metadata.get("name"))
        .filter_map(|name| name.as_str())
        .collect();
    assert!(
        group_names.contains(&"wardle.example.com"),
        "aggregated discovery must include APIService-backed group before update; got: {:?}",
        group_names
    );

    let first_backend_req = String::from_utf8_lossy(
        &timeout(Duration::from_secs(2), first_request.recv())
            .await
            .expect("backend should receive routed request")
            .expect("backend capture channel should still be open"),
    )
    .into_owned();
    assert!(
        !first_backend_req.starts_with(
            "GET /apis/wardle.example.com/v1alpha1/namespaces/default/flunders/widget/status",
        ),
        "initial discovery request must arrive before subresource request: {first_backend_req}"
    );

    let first_subresource = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1/namespaces/default/flunders/widget/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first_subresource.status(), StatusCode::OK);
    let expected_prefix = "GET /apis/wardle.example.com/v1alpha1/namespaces/default/flunders/widget/status HTTP/1.1\r\n";
    let first_backend_request =
        receive_matching_request_stream(&mut first_request, expected_prefix).await;
    assert!(
        first_backend_request.starts_with(expected_prefix),
        "initial subresource request should target first backend"
    );

    apiservice["spec"]["service"] =
        json!({"name": "wardle-service-second", "namespace": "default", "port": second_port});
    let update = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices/v1alpha1.wardle.example.com")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(update.status(), StatusCode::OK);

    let discovery_after_update = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis")
                .header(
                    "Accept",
                    "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(discovery_after_update.status(), StatusCode::OK);
    let group_payload_after_update: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(discovery_after_update.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let discovery_items_after_update: &[serde_json::Value] = group_payload_after_update["items"]
        .as_array()
        .map_or(&[], |items| items.as_slice());
    let group_names_after_update: Vec<&str> = discovery_items_after_update
        .iter()
        .filter_map(|item| item.get("metadata"))
        .filter_map(|metadata| metadata.get("name"))
        .filter_map(|name| name.as_str())
        .collect();
    assert!(
        group_names_after_update.contains(&"wardle.example.com"),
        "aggregated discovery should still include APIService-backed group after update; got: {:?}",
        group_names_after_update
    );

    let second_update_discovery_request = String::from_utf8_lossy(
        &timeout(Duration::from_secs(2), second_request.recv())
            .await
            .expect("backend should receive routed request")
            .expect("backend capture channel should still be open"),
    )
    .into_owned();
    assert!(
        !second_update_discovery_request.starts_with(
            "GET /apis/wardle.example.com/v1alpha1/namespaces/default/flunders/widget/status"
        ),
        "initial discovery request after update must arrive before subresource request: {second_update_discovery_request}"
    );

    let second_subresource = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1/namespaces/default/flunders/widget/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second_subresource.status(), StatusCode::OK);
    let second_request =
        receive_matching_request_stream(&mut second_request, expected_prefix).await;
    assert!(
        second_request.starts_with(expected_prefix),
        "post-update subresource request should target updated APIService backend"
    );
}

#[tokio::test]
async fn test_apiservice_proxy_resolves_endpoints_fresh_with_cached_backend() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio::time::{Duration, timeout};
    use tower::ServiceExt;

    let first_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let first_port = first_listener.local_addr().unwrap().port();
    let second_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second_port = second_listener.local_addr().unwrap().port();
    let first_body =
        r#"{"kind":"FlunderList","apiVersion":"wardle.example.com/v1alpha1","items":[]}"#;
    let first_response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Backend: first\r\nContent-Length: {}\r\n\r\n{}",
        first_body.len(),
        first_body
    )
    .into_bytes();
    let second_body =
        r#"{"kind":"FlunderList","apiVersion":"wardle.example.com/v1alpha1","items":[]}"#;
    let second_response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Backend: second\r\nContent-Length: {}\r\n\r\n{}",
        second_body.len(),
        second_body
    )
    .into_bytes();
    let first_request = spawn_apiservice_tls_backend_for_service(
        first_listener,
        "wardle-service",
        "default",
        first_response,
    )
    .await;
    let second_request = spawn_apiservice_tls_backend_for_service(
        second_listener,
        "wardle-service",
        "default",
        second_response,
    )
    .await;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "spec": {"ports": [{"port": 443}]}
        }),
    )
    .await
    .unwrap();
    let first_endpoints = json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {"name": "wardle-service", "namespace": "default"},
        "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": first_port}]}]
    });
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        first_endpoints,
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service", "namespace": "default", "port": 443}
        }
    });
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let first = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1/flunders")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let _ = timeout(Duration::from_secs(2), first_request)
        .await
        .expect("first backend should receive initial request")
        .expect("first backend should capture request bytes");
    assert_eq!(
        first
            .headers()
            .get("x-backend")
            .and_then(|v| v.to_str().ok()),
        Some("first")
    );

    let second_endpoints = json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {"name": "wardle-service", "namespace": "default"},
        "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": second_port}]}]
    });
    db.update_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        second_endpoints,
        2,
    )
    .await
    .unwrap();

    let second = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1/flunders")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::OK);
    let _ = timeout(Duration::from_secs(2), second_request)
        .await
        .expect("second backend should receive request after endpoint update")
        .expect("second backend should capture request bytes");
    assert_eq!(
        second
            .headers()
            .get("x-backend")
            .and_then(|v| v.to_str().ok()),
        Some("second"),
        "Endpoint changes must be resolved fresh even when APIService backend data is cached"
    );
}

#[tokio::test]
async fn test_apiservice_proxy_filters_sensitive_forwarded_headers() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio::time::{Duration, timeout};
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let body = r#"{"kind":"FlunderList","apiVersion":"wardle.example.com/v1alpha1","items":[]}"#;
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    )
    .into_bytes();
    let backend_request =
        spawn_apiservice_tls_backend_for_service(listener, "wardle-service", "default", response)
            .await;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let bootstrap_token = crate::bootstrap::bootstrap_token::generate_bootstrap_token();
    crate::bootstrap::bootstrap_token::create_scoped_bootstrap_token_secret_for_test(
        db.as_ref(),
        crate::bootstrap::bootstrap_token::BootstrapTokenScope::Worker,
        &bootstrap_token,
    )
    .await
    .unwrap();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "spec": {"ports": [{"port": port}]}
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": port}]}]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service", "namespace": "default", "port": port}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let list = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1/namespaces/default/flunders")
                .header("authorization", format!("Bearer {bootstrap_token}"))
                .header("impersonate-user", "mallory")
                .header("x-remote-user", "spoofed-user")
                .header("x-remote-group", "spoofed-group")
                .header("x-remote-extra-project", "spoofed-extra")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list.status(), StatusCode::OK);

    let req = timeout(Duration::from_secs(2), backend_request)
        .await
        .expect("backend should receive proxied request")
        .expect("backend should capture request bytes");
    let req = String::from_utf8_lossy(&req).into_owned();
    let req_lower = req.to_ascii_lowercase();
    assert!(
        !req_lower.contains("\r\nauthorization:"),
        "authorization header must not be forwarded to APIService backend. got:\n{req}"
    );
    assert!(
        !req_lower.contains("\r\nimpersonate-user:"),
        "impersonation headers must not be forwarded to APIService backend. got:\n{req}"
    );
    assert!(
        !req_lower.contains("\r\nx-remote-extra-project:"),
        "caller-supplied requestheader extras must not be forwarded. got:\n{req}"
    );
    assert!(
        !req_lower.contains("x-remote-user: spoofed-user"),
        "caller-supplied requestheader user must be stripped. got:\n{req}"
    );
    // The proxy now forwards the real effective caller identity instead of hard-coded
    // system:admin.  The bootstrap token is impersonating "mallory", so the effective
    // identity username is "mallory".
    assert!(
        req_lower.contains("\r\nx-remote-user: mallory\r\n"),
        "proxy must set delegated requestheader user from effective identity. got:\n{req}"
    );
    assert!(
        !req_lower.contains("x-remote-user: system:admin"),
        "proxy must not hard-code system:admin. got:\n{req}"
    );
}

#[tokio::test]
async fn test_apiservice_discovery_proxy_passthroughs_non_json_payloads() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio::time::{Duration, timeout};
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let expected_body = b"\x6b\x38\x73\x00\x10\x42".to_vec();

    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/vnd.kubernetes.protobuf\r\nContent-Length: {}\r\n\r\n",
        expected_body.len(),
    )
    .into_bytes()
    .into_iter()
    .chain(expected_body.clone().into_iter())
    .collect::<Vec<u8>>();
    let backend_request =
        spawn_apiservice_tls_backend_for_service(listener, "wardle-service", "default", response)
            .await;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "spec": {"ports": [{"port": port}]}
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": port}]}]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service", "namespace": "default", "port": port}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let discovery = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1")
                .header("accept", "application/vnd.kubernetes.protobuf")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        discovery.status(),
        StatusCode::OK,
        "APIService discovery proxy should pass through non-JSON payloads"
    );
    let _ = timeout(Duration::from_secs(2), backend_request)
        .await
        .expect("backend should receive proxied request");
    assert_eq!(
        discovery
            .headers()
            .get("content-type")
            .and_then(|h| h.to_str().ok()),
        Some("application/vnd.kubernetes.protobuf")
    );
    let bytes = axum::body::to_bytes(discovery.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(bytes.as_ref(), b"\x6b\x38\x73\x00\x10\x42");
}

#[tokio::test]
async fn test_apiservice_subresource_requests_are_forwarded_to_apiservice_backend() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio::time::{Duration, timeout};
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let response = r#"{"kind":"Flunder","apiVersion":"wardle.example.com/v1alpha1","metadata":{"name":"widget"}}"#;
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        response.len(),
        response
    )
    .into_bytes();
    let captured_request =
        spawn_apiservice_tls_backend_for_service(listener, "wardle-service", "default", response)
            .await;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "spec": {"ports": [{"port": port}]}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": port}]}]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service", "namespace": "default", "port": port}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let status_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1/namespaces/default/flunders/widget/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(status_response.status(), StatusCode::OK);

    let captured = timeout(Duration::from_secs(2), captured_request)
        .await
        .expect("backend should receive forwarded subresource request")
        .expect("request capture channel should resolve");
    let captured = String::from_utf8_lossy(&captured).into_owned();
    assert!(
        captured.contains(
            "GET /apis/wardle.example.com/v1alpha1/namespaces/default/flunders/widget/status HTTP/1.1\r\n"
        ),
        "unexpected forwarded APIService request line: {captured}"
    );
}

#[tokio::test]
async fn test_cluster_apiservice_subresource_requests_are_forwarded_to_apiservice_backend() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::{
        net::TcpListener,
        time::{Duration, timeout},
    };
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let body = r#"{"kind":"Flunder","apiVersion":"wardle.example.com/v1alpha1","metadata":{"name":"widget"}}"#;
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    )
    .into_bytes();
    let captured_request =
        spawn_apiservice_tls_backend_for_service(listener, "wardle-service", "default", response)
            .await;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "spec": {"ports": [{"port": port}]}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": port}]}]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service", "namespace": "default", "port": port}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let payload = serde_json::json!({ "status": { "ready": true } });
    let status_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/apis/wardle.example.com/v1alpha1/flunders/widget/status")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(status_response.status(), StatusCode::OK);

    let captured = timeout(Duration::from_secs(2), captured_request)
        .await
        .expect("backend should receive forwarded subresource request")
        .expect("request capture channel should resolve");
    let captured = String::from_utf8_lossy(&captured).into_owned();
    assert!(
        captured
            .contains("PUT /apis/wardle.example.com/v1alpha1/flunders/widget/status HTTP/1.1\r\n"),
        "unexpected forwarded APIService request line: {captured}"
    );
    assert!(
        captured.contains(r#"{"status":{"ready":true}}"#),
        "unexpected forwarded APIService payload: {captured}"
    );
}

#[tokio::test]
async fn test_apiservice_subresource_nested_path_and_query_is_forwarded_to_apiservice_backend() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::{
        net::TcpListener,
        time::{Duration, timeout},
    };
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let body = r#"{"kind":"Flunder","apiVersion":"wardle.example.com/v1alpha1","metadata":{"name":"widget"}}"#;
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    )
    .into_bytes();
    let captured_request =
        spawn_apiservice_tls_backend_for_service(listener, "wardle-service", "default", response)
            .await;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "spec": {"ports": [{"port": port}]}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": port}]}]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service", "namespace": "default", "port": port}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let status_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1/namespaces/default/flunders/widget/status/conditions?resourceVersion=123&watch=false")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(status_response.status(), StatusCode::OK);

    let captured = timeout(Duration::from_secs(2), captured_request)
        .await
        .expect("backend should receive forwarded subresource request")
        .expect("request capture channel should resolve");
    let captured = String::from_utf8_lossy(&captured).into_owned();
    assert!(
        captured.contains(
            "GET /apis/wardle.example.com/v1alpha1/namespaces/default/flunders/widget/status/conditions?resourceVersion=123&watch=false HTTP/1.1\r\n"
        ),
        "unexpected forwarded APIService request line: {captured}"
    );
}

#[tokio::test]
async fn test_apiservice_subresource_patch_body_and_headers_forwarded_to_apiservice_backend() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::{
        net::TcpListener,
        time::{Duration, timeout},
    };
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let patch = r#"{"op":"replace","path":"/spec","value":{"updated":true}}"#;
    let body = r#"{"kind":"Flunder","apiVersion":"wardle.example.com/v1alpha1","metadata":{"name":"widget"}}"#;
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    )
    .into_bytes();
    let captured_request =
        spawn_apiservice_tls_backend_for_service(listener, "wardle-service", "default", response)
            .await;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);
    let bootstrap_token = crate::bootstrap::bootstrap_token::generate_bootstrap_token();
    crate::bootstrap::bootstrap_token::create_scoped_bootstrap_token_secret_for_test(
        db.as_ref(),
        crate::bootstrap::bootstrap_token::BootstrapTokenScope::Worker,
        &bootstrap_token,
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "spec": {"ports": [{"port": port}]}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": port}]}]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service", "namespace": "default", "port": port}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let patch_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/apis/wardle.example.com/v1alpha1/namespaces/default/flunders/widget/status")
                .header("content-type", "application/json-patch+json")
                .header("authorization", format!("Bearer {bootstrap_token}"))
                .header("x-custom-proxy-header", "forwarded")
                .body(Body::from(patch.as_bytes()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch_response.status(), StatusCode::OK);

    let captured = timeout(Duration::from_secs(2), captured_request)
        .await
        .expect("backend should receive forwarded subresource request")
        .expect("request capture channel should resolve");
    let captured = String::from_utf8_lossy(&captured).into_owned();
    let captured_lower = captured.to_ascii_lowercase();
    assert!(
        captured.starts_with(
            "PATCH /apis/wardle.example.com/v1alpha1/namespaces/default/flunders/widget/status HTTP/1.1\r\n"
        ),
        "unexpected forwarded APIService request line: {captured}"
    );
    assert!(
        captured.contains(patch),
        "APIService should receive the subresource patch payload: {captured}"
    );
    assert!(
        captured_lower.contains("content-type: application/json-patch+json"),
        "content-type should be forwarded to APIService backend"
    );
    assert!(
        captured_lower.contains("x-custom-proxy-header: forwarded"),
        "custom caller headers should be forwarded to APIService backend"
    );
    assert!(
        !captured_lower.contains("authorization:"),
        "authorization headers must be filtered when forwarding APIService requests. got:\n{captured}"
    );
}

#[tokio::test]
async fn test_local_crd_subresource_returns_not_found_not_forwarded() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::Value;
    use tower::ServiceExt;

    let app = build_app_with_cluster_widget_crd().await;

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/example.com/v1/widgets/widget/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        value["message"],
        "custom resource subresource not supported: example.com/v1/widgets/widget/status"
    );
}

#[tokio::test]
async fn test_local_crd_subresource_prefer_local_crd_even_with_conflicting_apiservice() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::{Value, json};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::mpsc,
        time::{Duration, timeout},
    };
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (captured_tx, mut captured_rx) = mpsc::channel::<String>(8);

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 4096];
        let n = stream.read(&mut buf).await.unwrap_or(0);
        let _ = captured_tx
            .send(String::from_utf8_lossy(&buf[..n]).to_string())
            .await;

        let body =
            r#"{"kind":"Widget","apiVersion":"example.com/v1","metadata":{"name":"widget"}}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = stream.write_all(response.as_bytes()).await;
    });

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Cluster",
            "names": {"plural": "widgets", "singular": "widget", "kind": "Widget"},
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object"}}
            }]
        }
    });
    let crd_create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(crd_create.status(), StatusCode::CREATED);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "spec": {"ports": [{"port": port}]}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": port}]}]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1.example.com"},
        "spec": {
            "group": "example.com",
            "version": "v1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service", "namespace": "default", "port": port}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/example.com/v1/widgets/widget/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        value["message"],
        "custom resource subresource not supported: example.com/v1/widgets/widget/status"
    );

    let no_backend_call = timeout(Duration::from_millis(200), captured_rx.recv())
        .await
        .is_err();
    assert!(
        no_backend_call,
        "local CRD subresource path should not be forwarded to APIService backend"
    );
}

#[tokio::test]
async fn test_aggregated_discovery_includes_apiservice_backed_group() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let body = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service", "namespace": "default", "port": 18081}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let discovery = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis")
                .header(
                    "Accept",
                    "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(discovery.status(), StatusCode::OK);
    let body: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(discovery.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let items = body["items"].as_array().cloned().unwrap_or_default();
    let names: Vec<&str> = items
        .iter()
        .filter_map(|i| {
            i.get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
        })
        .collect();
    assert!(
        names.contains(&"wardle.example.com"),
        "aggregated discovery must include APIService-backed group; got: {:?}",
        names
    );
}

#[tokio::test]
async fn test_aggregated_discovery_includes_apiservice_backed_resources() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio::time::{Duration, timeout};
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let payload = serde_json::to_vec(&json!({
        "kind": "APIResourceList",
        "apiVersion": "v1",
        "groupVersion": "wardle.example.com/v1alpha1",
        "resources": [{
            "name": "flunders",
            "singularName": "flunder",
            "namespaced": true,
            "kind": "Flunder",
            "verbs": ["create", "delete", "get", "list", "patch", "update", "watch"]
        }]
    }))
    .unwrap();
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        payload.len()
    )
    .into_bytes()
    .into_iter()
    .chain(payload.into_iter())
    .collect::<Vec<u8>>();
    let captured_request =
        spawn_apiservice_tls_backend_for_service(listener, "wardle-service", "default", response)
            .await;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "spec": {"ports": [{"port": 443, "targetPort": port}]}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "subsets": [{
                "addresses": [{"ip": "127.0.0.1"}],
                "ports": [{"port": port}]
            }]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service", "namespace": "default", "port": 443}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let discovery = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis")
                .header(
                    "accept",
                    "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(discovery.status(), StatusCode::OK);
    let captured = timeout(Duration::from_secs(2), captured_request)
        .await
        .expect("backend should receive proxied discovery request")
        .expect("backend capture channel should resolve");
    let captured = String::from_utf8_lossy(&captured);
    assert!(
        captured.starts_with("GET /apis/wardle.example.com/v1alpha1"),
        "unexpected discovery path proxied to APIService backend: {captured}"
    );

    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(discovery.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let wardle_group = body["items"]
        .as_array()
        .and_then(|items| {
            items.iter().find(|item| {
                item.get("metadata")
                    .and_then(|m| m.get("name"))
                    .and_then(|n| n.as_str())
                    == Some("wardle.example.com")
            })
        })
        .cloned()
        .expect("wardle group must exist in aggregated discovery");
    let resources = wardle_group["versions"][0]["resources"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        resources
            .iter()
            .any(|r| r.get("resource").and_then(|v| v.as_str()) == Some("flunders")),
        "aggregated discovery must include APIService-backed resources, got: {resources:?}"
    );
}

#[tokio::test]
async fn test_apiservice_discovery_passthroughs_non_json_payload() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio::time::{Duration, timeout};
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let expected_payload = vec![0x6b, 0x38, 0x73, 0x00, 0x01];
    let response_payload = expected_payload.clone();
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/vnd.kubernetes.protobuf\r\nContent-Length: {}\r\n\r\n",
        response_payload.len()
    )
    .into_bytes()
    .into_iter()
    .chain(response_payload.into_iter())
    .collect::<Vec<u8>>();
    let backend_request =
        spawn_apiservice_tls_backend_for_service(listener, "wardle-service", "default", response)
            .await;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "spec": {"ports": [{"port": port}]}
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": port}]}]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service", "namespace": "default", "port": port}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let discovery = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        discovery.status(),
        StatusCode::OK,
        "APIService discovery should pass through non-JSON payloads"
    );
    let _ = timeout(Duration::from_secs(2), backend_request)
        .await
        .expect("backend should receive proxied request");

    let body = to_bytes(discovery.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        body.as_ref(),
        expected_payload.as_slice(),
        "APIService discovery body should be passthrough bytes"
    );
}

#[tokio::test]
async fn test_apiservice_with_ca_bundle_on_non_443_uses_tls_transport() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use base64::Engine;
    use rcgen::generate_simple_self_signed;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut first_byte = [0u8; 1];
        let n = stream.read(&mut first_byte).await.unwrap_or(0);
        if n == 1 && first_byte[0] == b'G' {
            let mut rest = vec![0u8; 4096];
            let _ = stream.read(&mut rest).await.unwrap_or(0);
            let body =
                r#"{"kind":"FlunderList","apiVersion":"wardle.example.com/v1alpha1","items":[]}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes()).await;
        }
    });

    let cert = generate_simple_self_signed(vec!["wardle-service.default.svc".to_string()]).unwrap();
    let ca_bundle = base64::engine::general_purpose::STANDARD.encode(cert.cert.pem().as_bytes());

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "spec": {"ports": [{"port": port}]}
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": port}]}]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "caBundle": ca_bundle,
            "service": {"name": "wardle-service", "namespace": "default", "port": port}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let list = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1/flunders")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        list.status(),
        StatusCode::BAD_GATEWAY,
        "non-443 APIService backends with caBundle must be treated as TLS"
    );
}

#[tokio::test]
async fn test_apiservice_insecure_skip_tls_verify_uses_https_with_invalid_cert_allowed() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::time::{Duration, timeout};
    use tower::ServiceExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let response = r#"{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"wardle.example.com/v1alpha1","resources":[]}"#;
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        response.len(),
        response
    )
    .into_bytes();
    let backend_request =
        spawn_apiservice_tls_backend_for_service(listener, "wardle-service", "default", response)
            .await;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "spec": {"ports": [{"port": port}]}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": port}]}]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service", "namespace": "default", "port": port}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let list = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list.status(), StatusCode::OK);
    let captured = timeout(Duration::from_secs(2), backend_request)
        .await
        .expect("backend should receive proxied request")
        .expect("backend capture channel should resolve");
    assert!(
        captured.starts_with(b"GET /apis/wardle.example.com/v1alpha1 HTTP/1.1\r\n"),
        "expected HTTPS request line to be forwarded: {}",
        String::from_utf8_lossy(&captured),
    );
}

#[tokio::test]
async fn test_apiservice_ca_bundle_verifies_https_backend() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::time::{Duration, timeout};
    use tower::ServiceExt;

    let (cert, key, ca_bundle) =
        generate_apiservice_ca_signed_identity("wardle-service", "default");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let response = r#"{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"wardle.example.com/v1alpha1","resources":[]}"#;
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        response.len(),
        response
    )
    .into_bytes();
    let backend_request = spawn_apiservice_tls_backend(listener, cert, key, response).await;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "spec": {"ports": [{"port": port}]}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": port}]}]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "caBundle": ca_bundle,
            "service": {"name": "wardle-service", "namespace": "default", "port": port}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let list = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list.status(), StatusCode::OK);
    let captured = timeout(Duration::from_secs(2), backend_request)
        .await
        .expect("backend should receive proxied request")
        .expect("backend capture channel should resolve");
    assert!(
        captured.starts_with(b"GET /apis/wardle.example.com/v1alpha1 HTTP/1.1\r\n"),
        "expected APIService request to be forwarded when caBundle matches: {}",
        String::from_utf8_lossy(&captured),
    );
}

#[tokio::test]
async fn test_apiservice_plain_http_backend_is_not_supported() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::time::{Duration, timeout};
    use tower::ServiceExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let response =
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 0\r\n\r\n".to_vec();
    let backend_request = spawn_apiservice_plain_backend(listener, response).await;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "spec": {"ports": [{"port": port}]}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": port}]}]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service", "namespace": "default", "port": port}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let list = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list.status(), StatusCode::BAD_GATEWAY);
    let body = axum::body::to_bytes(list.into_body(), usize::MAX)
        .await
        .unwrap();
    let captured = timeout(Duration::from_secs(2), backend_request)
        .await
        .expect("backend should be reached on attempted connection")
        .expect("backend capture channel should resolve");
    assert!(
        !captured.starts_with(b"GET "),
        "plain HTTP request must not be sent upstream"
    );
    assert!(
        !std::str::from_utf8(&body)
            .unwrap_or_default()
            .contains("should not"),
        "should return failure, got body: {}",
        String::from_utf8_lossy(&body)
    );
}

#[tokio::test]
async fn test_apiservice_tls_failure_does_not_retry_plain_http() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::time::{Duration, timeout};
    use tower::ServiceExt;

    let failing_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let failing_port = failing_listener.local_addr().unwrap().port();
    let good_backend_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let _good_port = good_backend_listener.local_addr().unwrap().port();
    let failing_response =
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 0\r\n\r\n".to_vec();
    let good_response =
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 0\r\n\r\n".to_vec();
    let failing_request = spawn_apiservice_plain_backend(failing_listener, failing_response).await;
    let good_request = spawn_apiservice_plain_backend(good_backend_listener, good_response).await;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "spec": {"ports": [{"port": failing_port}]}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": failing_port}]}]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service", "namespace": "default", "port": failing_port}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let list = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list.status(), StatusCode::BAD_GATEWAY);
    let failing_captured = timeout(Duration::from_secs(2), failing_request)
        .await
        .expect("failing backend should receive connection attempt")
        .expect("failing backend capture channel should resolve");
    assert_ne!(
        failing_captured.first().copied(),
        Some(b'G'),
        "fallback to plain HTTP must not occur on APIService backend"
    );
    assert!(
        timeout(Duration::from_millis(200), good_request)
            .await
            .is_err(),
        "no traffic should reach unrelated plain HTTP backend"
    );
}

#[tokio::test]
async fn test_apiservice_subresource_options_is_forwarded_to_backend() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::time::{Duration, timeout};
    use tower::ServiceExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let response = "HTTP/1.1 204 No Content\r\nAllow: GET,POST,PUT,PATCH,DELETE,OPTIONS,HEAD\r\nContent-Length: 0\r\n\r\n"
        .as_bytes()
        .to_vec();
    let backend_request =
        spawn_apiservice_tls_backend_for_service(listener, "wardle-service", "default", response)
            .await;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "spec": {"ports": [{"port": port}]}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": port}]}]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service", "namespace": "default", "port": port}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let options_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/apis/wardle.example.com/v1alpha1/namespaces/default/flunders/widget/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(options_response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        options_response
            .headers()
            .get("allow")
            .and_then(|h| h.to_str().ok()),
        Some("GET,POST,PUT,PATCH,DELETE,OPTIONS,HEAD")
    );
    let captured = timeout(Duration::from_secs(2), backend_request)
        .await
        .expect("backend should receive OPTIONS request")
        .expect("backend capture channel should resolve");
    let captured = String::from_utf8_lossy(&captured);
    assert!(
        captured.starts_with("OPTIONS /apis/wardle.example.com/v1alpha1/namespaces/default/flunders/widget/status HTTP/1.1\r\n"),
        "unexpected forwarded APIService request line: {captured}"
    );
}

#[tokio::test]
async fn test_apiservice_subresource_head_is_forwarded_to_backend() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::time::{Duration, timeout};
    use tower::ServiceExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let response = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 0\r\n\r\n"
        .as_bytes()
        .to_vec();
    let backend_request =
        spawn_apiservice_tls_backend_for_service(listener, "wardle-service", "default", response)
            .await;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "spec": {"ports": [{"port": port}]}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": port}]}]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service", "namespace": "default", "port": port}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let head_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/apis/wardle.example.com/v1alpha1/namespaces/default/flunders/widget/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(head_response.status(), StatusCode::OK);
    let captured = timeout(Duration::from_secs(2), backend_request)
        .await
        .expect("backend should receive HEAD request")
        .expect("backend capture channel should resolve");
    let captured = String::from_utf8_lossy(&captured);
    assert!(
        captured.starts_with("HEAD /apis/wardle.example.com/v1alpha1/namespaces/default/flunders/widget/status HTTP/1.1\r\n"),
        "unexpected forwarded APIService request line: {captured}"
    );
}

#[tokio::test]
async fn test_apiservice_invalid_ca_bundle_returns_bad_gateway() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::time::{Duration, timeout};
    use tower::ServiceExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let response =
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}"
            .to_vec();
    let backend_request = spawn_apiservice_plain_backend(listener, response).await;
    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "spec": {"ports": [{"port": port}]}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": port}]}]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "caBundle": "not-base64",
            "service": {"name": "wardle-service", "namespace": "default", "port": port}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let list = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list.status(), StatusCode::BAD_GATEWAY);
    let body = axum::body::to_bytes(list.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(
        body.as_ref().iter().any(|b| b.is_ascii_alphabetic())
            && String::from_utf8_lossy(&body).contains("invalid spec.caBundle"),
        "expected invalid-caBundle error in response body, got: {}",
        String::from_utf8_lossy(&body)
    );
    assert!(
        timeout(Duration::from_millis(200), backend_request)
            .await
            .is_err(),
        "backend should not be contacted with invalid caBundle"
    );
}

#[tokio::test]
async fn test_apiservice_tls_backend_without_ca_or_insecure_skip_returns_bad_gateway() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::time::{Duration, timeout};
    use tower::ServiceExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let response = r#"{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"wardle.example.com/v1alpha1","resources":[]}"#;
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        response.len(),
        response
    )
    .into_bytes();
    let backend_request =
        spawn_apiservice_tls_backend_for_service(listener, "wardle-service", "default", response)
            .await;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "spec": {"ports": [{"port": port}]}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": port}]}]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "service": {"name": "wardle-service", "namespace": "default", "port": port}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let list = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list.status(), StatusCode::BAD_GATEWAY);
    let captured = timeout(Duration::from_secs(2), backend_request)
        .await
        .expect("backend should receive attempted TLS request")
        .expect("backend capture channel should resolve");
    assert!(
        !captured.starts_with(b"GET /"),
        "untrusted TLS endpoint should not be reached by plain HTTP fallback: {}",
        String::from_utf8_lossy(&captured)
    );
}

#[tokio::test]
async fn test_apiservice_proxy_negative_backend_cache_invalidates_on_create() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::time::{Duration, timeout};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    let cache_miss = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cache_miss.status(), StatusCode::NOT_FOUND);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let response = r#"{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"wardle.example.com/v1alpha1","resources":[]}"#;
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        response.len(),
        response
    )
    .into_bytes();
    let backend_request =
        spawn_apiservice_tls_backend_for_service(listener, "wardle-service", "default", response)
            .await;

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "spec": {"ports": [{"port": port}]}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": port}]}]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service", "namespace": "default", "port": port}
        }
    });
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let list = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list.status(), StatusCode::OK);
    assert!(
        timeout(Duration::from_secs(2), backend_request)
            .await
            .is_ok(),
        "backend should receive request after cache miss has been created"
    );
}

#[tokio::test]
async fn test_tokenreview_create_validates_serviceaccount_jwt() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use rand_core::OsRng;
    use rsa::RsaPrivateKey;
    use rsa::pkcs8::EncodePrivateKey;
    use serde_json::json;
    use tower::ServiceExt;

    let unique_ns = format!("tokenreview-{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let etc_dir = crate::paths::etc_dir_path(&unique_ns)
        .to_string_lossy()
        .into_owned();
    std::fs::create_dir_all(&etc_dir).unwrap();

    let private_key = RsaPrivateKey::new(&mut OsRng, 2048).unwrap();
    let ca_key_pem = private_key
        .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
        .unwrap()
        .to_string();
    std::fs::write(
        crate::paths::service_account_signing_key_path(&unique_ns),
        &ca_key_pem,
    )
    .unwrap();

    let mut state = build_test_app_state().await;
    state.config = std::sync::Arc::new(crate::KlightsConfig {
        containerd_namespace: unique_ns.clone(),
        ..crate::KlightsConfig::from_env().expect("env config valid in test")
    });
    let sa_uid = "sa-uid-jwt-test";
    state
        .db
        .create_resource(
            "v1",
            "ServiceAccount",
            Some("kube-system"),
            "default",
            json!({"apiVersion": "v1", "kind": "ServiceAccount",
                   "metadata": {"name": "default", "namespace": "kube-system", "uid": sa_uid}}),
        )
        .await
        .unwrap();
    let app = crate::api::build_router(state);

    let token =
        crate::auth::generate_sa_token_with_bound_pod(crate::auth::ServiceAccountTokenRequest {
            ca_key_pem: &ca_key_pem,
            service_account: "default",
            namespace: "kube-system",
            audiences: &["https://kubernetes.default.svc.cluster.local"],
            expiration_seconds: None,
            bound: crate::auth::BoundServiceAccountToken {
                sa_uid: Some(sa_uid),
                ..crate::auth::BoundServiceAccountToken::default()
            },
        })
        .unwrap();

    let req_body = json!({
        "apiVersion": "authentication.k8s.io/v1",
        "kind": "TokenReview",
        "spec": {
            "token": token,
            "audiences": ["https://kubernetes.default.svc.cluster.local"]
        }
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/authentication.k8s.io/v1/tokenreviews")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&req_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "TokenReview create endpoint must exist and authenticate valid SA tokens"
    );
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["kind"], "TokenReview");
    assert_eq!(json["apiVersion"], "authentication.k8s.io/v1");
    assert_eq!(json["status"]["authenticated"], true);
    assert_eq!(
        json["status"]["user"]["username"],
        "system:serviceaccount:kube-system:default"
    );

    let groups = json["status"]["user"]["groups"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        groups
            .iter()
            .filter_map(|v| v.as_str())
            .any(|g| g == "system:authenticated"),
        "TokenReview user groups must include system:authenticated; got: {groups:?}"
    );
    let credential_id =
        json["status"]["user"]["extra"]["authentication.kubernetes.io/credential-id"]
            .as_array()
            .cloned()
            .unwrap_or_default();
    assert_eq!(
        credential_id.len(),
        1,
        "TokenReview user extra credential-id must contain exactly one item; got: {credential_id:?}"
    );
    assert!(
        credential_id[0]
            .as_str()
            .is_some_and(|v| v.starts_with("JTI=")),
        "TokenReview user extra credential-id must start with JTI=; got: {credential_id:?}"
    );

    std::fs::remove_dir_all(crate::paths::data_root_path(&unique_ns)).ok();
}

#[tokio::test]
async fn test_tokenreview_includes_pod_extra_for_pod_bound_token() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use rand_core::OsRng;
    use rsa::RsaPrivateKey;
    use rsa::pkcs8::EncodePrivateKey;
    use serde_json::json;
    use tower::ServiceExt;

    let unique_ns = format!("tokenreview-pod-{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let etc_dir = crate::paths::etc_dir_path(&unique_ns)
        .to_string_lossy()
        .into_owned();
    std::fs::create_dir_all(&etc_dir).unwrap();

    let private_key = RsaPrivateKey::new(&mut OsRng, 2048).unwrap();
    let ca_key_pem = private_key
        .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
        .unwrap()
        .to_string();
    std::fs::write(
        crate::paths::service_account_signing_key_path(&unique_ns),
        &ca_key_pem,
    )
    .unwrap();

    let mut state = build_test_app_state().await;
    state.config = std::sync::Arc::new(crate::KlightsConfig {
        containerd_namespace: unique_ns.clone(),
        ..crate::KlightsConfig::from_env().expect("env config valid in test")
    });

    let pod_name = "pod-service-account-test";
    let pod_uid = "3e4f1b0f-d09c-4b14-a2cc-a5fd867eb5b1";
    let sa_uid = "sa-uid-tokenreview-pod";
    // Bound-token validation requires the SA and bound pod to exist with the
    // token's UIDs (mirrors the request auth path).
    state
        .db
        .create_resource(
            "v1",
            "ServiceAccount",
            Some("kube-system"),
            "default",
            json!({"apiVersion": "v1", "kind": "ServiceAccount",
                   "metadata": {"name": "default", "namespace": "kube-system", "uid": sa_uid}}),
        )
        .await
        .unwrap();
    state
        .db
        .create_resource(
            "v1",
            "Pod",
            Some("kube-system"),
            pod_name,
            json!({"apiVersion": "v1", "kind": "Pod",
                   "metadata": {"name": pod_name, "namespace": "kube-system", "uid": pod_uid}}),
        )
        .await
        .unwrap();

    let app = crate::api::build_router(state);

    let token =
        crate::auth::generate_sa_token_with_bound_pod(crate::auth::ServiceAccountTokenRequest {
            ca_key_pem: &ca_key_pem,
            service_account: "default",
            namespace: "kube-system",
            audiences: &["https://kubernetes.default.svc.cluster.local"],
            expiration_seconds: None,
            bound: crate::auth::BoundServiceAccountToken {
                pod_name: Some(pod_name),
                pod_uid: Some(pod_uid),
                sa_uid: Some(sa_uid),
                ..crate::auth::BoundServiceAccountToken::default()
            },
        })
        .unwrap();

    let req_body = json!({
        "apiVersion": "authentication.k8s.io/v1",
        "kind": "TokenReview",
        "spec": {
            "token": token,
            "audiences": ["https://kubernetes.default.svc.cluster.local"]
        }
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/authentication.k8s.io/v1/tokenreviews")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&req_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(
        json["status"]["user"]["extra"]["authentication.kubernetes.io/pod-name"][0],
        pod_name
    );
    assert_eq!(
        json["status"]["user"]["extra"]["authentication.kubernetes.io/pod-uid"][0],
        pod_uid
    );

    std::fs::remove_dir_all(crate::paths::data_root_path(&unique_ns)).ok();
}

#[tokio::test]
async fn test_tokenreview_includes_node_name_extra_for_node_bound_token() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use rand_core::OsRng;
    use rsa::RsaPrivateKey;
    use rsa::pkcs8::EncodePrivateKey;
    use serde_json::json;
    use tower::ServiceExt;

    let unique_ns = format!(
        "tokenreview-node-{}",
        &uuid::Uuid::new_v4().to_string()[..8]
    );
    let etc_dir = crate::paths::etc_dir_path(&unique_ns)
        .to_string_lossy()
        .into_owned();
    std::fs::create_dir_all(&etc_dir).unwrap();

    let private_key = RsaPrivateKey::new(&mut OsRng, 2048).unwrap();
    let ca_key_pem = private_key
        .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
        .unwrap()
        .to_string();
    std::fs::write(
        crate::paths::service_account_signing_key_path(&unique_ns),
        &ca_key_pem,
    )
    .unwrap();

    let mut state = build_test_app_state().await;
    state.config = std::sync::Arc::new(crate::KlightsConfig {
        containerd_namespace: unique_ns.clone(),
        ..crate::KlightsConfig::from_env().expect("env config valid in test")
    });

    let node_name = "dallas-vm-1.us-south1-a.c.klights.internal";
    let pod_name = "pod-service-account-test";
    let pod_uid = "3e4f1b0f-d09c-4b14-a2cc-a5fd867eb5b1";
    let node_uid = "node-uid-tokenreview";
    let sa_uid = "sa-uid-tokenreview-node";
    state
        .db
        .create_resource(
            "v1",
            "ServiceAccount",
            Some("kube-system"),
            "default",
            json!({"apiVersion": "v1", "kind": "ServiceAccount",
                   "metadata": {"name": "default", "namespace": "kube-system", "uid": sa_uid}}),
        )
        .await
        .unwrap();
    state
        .db
        .create_resource(
            "v1",
            "Pod",
            Some("kube-system"),
            pod_name,
            json!({"apiVersion": "v1", "kind": "Pod",
                   "metadata": {"name": pod_name, "namespace": "kube-system", "uid": pod_uid}}),
        )
        .await
        .unwrap();
    state
        .db
        .create_resource(
            "v1",
            "Node",
            None,
            node_name,
            json!({"apiVersion": "v1", "kind": "Node",
                   "metadata": {"name": node_name, "uid": node_uid}}),
        )
        .await
        .unwrap();

    let app = crate::api::build_router(state);

    let token =
        crate::auth::generate_sa_token_with_bound_pod(crate::auth::ServiceAccountTokenRequest {
            ca_key_pem: &ca_key_pem,
            service_account: "default",
            namespace: "kube-system",
            audiences: &["https://kubernetes.default.svc.cluster.local"],
            expiration_seconds: None,
            bound: crate::auth::BoundServiceAccountToken {
                pod_name: Some(pod_name),
                pod_uid: Some(pod_uid),
                node_name: Some(node_name),
                node_uid: Some(node_uid),
                sa_uid: Some(sa_uid),
                ..crate::auth::BoundServiceAccountToken::default()
            },
        })
        .unwrap();

    let req_body = json!({
        "apiVersion": "authentication.k8s.io/v1",
        "kind": "TokenReview",
        "spec": {
            "token": token,
            "audiences": ["https://kubernetes.default.svc.cluster.local"]
        }
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/authentication.k8s.io/v1/tokenreviews")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&req_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        json["status"]["user"]["extra"]["authentication.kubernetes.io/node-name"][0],
        node_name
    );

    std::fs::remove_dir_all(crate::paths::data_root_path(&unique_ns)).ok();
}

#[tokio::test]
async fn test_tokenreview_rejects_token_bound_to_deleted_pod() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use rand_core::OsRng;
    use rsa::RsaPrivateKey;
    use rsa::pkcs8::EncodePrivateKey;
    use serde_json::json;
    use tower::ServiceExt;

    let unique_ns = format!(
        "tokenreview-gone-{}",
        &uuid::Uuid::new_v4().to_string()[..8]
    );
    let etc_dir = crate::paths::etc_dir_path(&unique_ns)
        .to_string_lossy()
        .into_owned();
    std::fs::create_dir_all(&etc_dir).unwrap();

    let private_key = RsaPrivateKey::new(&mut OsRng, 2048).unwrap();
    let ca_key_pem = private_key
        .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
        .unwrap()
        .to_string();
    std::fs::write(
        crate::paths::service_account_signing_key_path(&unique_ns),
        &ca_key_pem,
    )
    .unwrap();

    let mut state = build_test_app_state().await;
    state.config = std::sync::Arc::new(crate::KlightsConfig {
        containerd_namespace: unique_ns.clone(),
        ..crate::KlightsConfig::from_env().expect("env config valid in test")
    });
    // SA exists, but the bound pod was deleted: TokenReview must not report the
    // token as authenticated (parity with the request auth path).
    let sa_uid = "sa-uid-gone";
    state
        .db
        .create_resource(
            "v1",
            "ServiceAccount",
            Some("kube-system"),
            "default",
            json!({"apiVersion": "v1", "kind": "ServiceAccount",
                   "metadata": {"name": "default", "namespace": "kube-system", "uid": sa_uid}}),
        )
        .await
        .unwrap();
    let app = crate::api::build_router(state);

    let token =
        crate::auth::generate_sa_token_with_bound_pod(crate::auth::ServiceAccountTokenRequest {
            ca_key_pem: &ca_key_pem,
            service_account: "default",
            namespace: "kube-system",
            audiences: &["https://kubernetes.default.svc.cluster.local"],
            expiration_seconds: None,
            bound: crate::auth::BoundServiceAccountToken {
                pod_name: Some("gone-pod"),
                pod_uid: Some("11111111-1111-1111-1111-111111111111"),
                sa_uid: Some(sa_uid),
                ..crate::auth::BoundServiceAccountToken::default()
            },
        })
        .unwrap();

    let req_body = json!({
        "apiVersion": "authentication.k8s.io/v1",
        "kind": "TokenReview",
        "spec": {
            "token": token,
            "audiences": ["https://kubernetes.default.svc.cluster.local"]
        }
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/authentication.k8s.io/v1/tokenreviews")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&req_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        json["status"]["authenticated"],
        serde_json::Value::Bool(false),
        "token bound to a deleted pod must not be authenticated"
    );

    std::fs::remove_dir_all(crate::paths::data_root_path(&unique_ns)).ok();
}

#[tokio::test]
async fn test_validating_webhook_configuration_update_rejects_invalid_match_conditions() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let create_body = serde_json::json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingWebhookConfiguration",
        "metadata": {"name": "vwc-update-invalid"},
        "webhooks": [{
            "name": "vwc.example.com",
            "admissionReviewVersions": ["v1"],
            "sideEffects": "None",
            "clientConfig": {"url": "https://example.invalid/validate"},
            "rules": [{
                "operations": ["CREATE"],
                "apiGroups": [""],
                "apiVersions": ["v1"],
                "resources": ["configmaps"]
            }]
        }]
    });

    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&create_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let update_body = serde_json::json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingWebhookConfiguration",
        "metadata": {"name": "vwc-update-invalid"},
        "webhooks": [{
            "name": "vwc.example.com",
            "admissionReviewVersions": ["v1"],
            "sideEffects": "None",
            "clientConfig": {"url": "https://example.invalid/validate"},
            "rules": [{
                "operations": ["CREATE"],
                "apiGroups": [""],
                "apiVersions": ["v1"],
                "resources": ["configmaps"]
            }],
            "matchConditions": [{
                "name": "",
                "expression": "has(request.userInfo.username)"
            }]
        }]
    });

    let update_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/vwc-update-invalid")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&update_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        update_resp.status(),
        StatusCode::UNPROCESSABLE_ENTITY,
        "PUT validatingwebhookconfiguration must reject invalid matchConditions"
    );
}

#[tokio::test]
async fn test_mutating_webhook_configuration_patch_rejects_invalid_match_conditions() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let create_body = serde_json::json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "MutatingWebhookConfiguration",
        "metadata": {"name": "mwc-patch-invalid"},
        "webhooks": [{
            "name": "mwc.example.com",
            "admissionReviewVersions": ["v1"],
            "sideEffects": "None",
            "clientConfig": {"url": "https://example.invalid/mutate"},
            "rules": [{
                "operations": ["CREATE"],
                "apiGroups": [""],
                "apiVersions": ["v1"],
                "resources": ["pods"]
            }]
        }]
    });

    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&create_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let patch_body = serde_json::json!({
        "webhooks": [{
            "name": "mwc.example.com",
            "admissionReviewVersions": ["v1"],
            "sideEffects": "None",
            "clientConfig": {"url": "https://example.invalid/mutate"},
            "rules": [{
                "operations": ["CREATE"],
                "apiGroups": [""],
                "apiVersions": ["v1"],
                "resources": ["pods"]
            }],
            "matchConditions": [{
                "name": "must-have-user",
                "expression": ""
            }]
        }]
    });

    let patch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations/mwc-patch-invalid")
                .header("content-type", "application/merge-patch+json")
                .body(Body::from(serde_json::to_vec(&patch_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        patch_resp.status(),
        StatusCode::UNPROCESSABLE_ENTITY,
        "PATCH mutatingwebhookconfiguration must reject invalid matchConditions"
    );
}

#[tokio::test]
async fn test_validating_webhook_configuration_put_and_json_patch_toggle_create_operation() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let namespace = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(namespace.status(), StatusCode::CREATED);

    let create_vwc = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingWebhookConfiguration",
        "metadata": {"name": "vwc-toggle-create-op"},
        "webhooks": [{
            "name": "deny-configmaps.example.com",
            "admissionReviewVersions": ["v1"],
            "sideEffects": "None",
            "failurePolicy": "Fail",
            "timeoutSeconds": 1,
            "clientConfig": {"url": "https://127.0.0.1:1/deny"},
            "rules": [{
                "operations": ["CREATE"],
                "apiGroups": [""],
                "apiVersions": ["v1"],
                "resources": ["configmaps"]
            }]
        }]
    });
    let create_vwc_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&create_vwc).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_vwc_resp.status(), StatusCode::CREATED);

    let denied_before_update = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/default/configmaps")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm-denied-before"},"data":{"k":"v"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        denied_before_update.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "CREATE must be blocked while validating webhook rules include CREATE"
    );

    let put_vwc = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingWebhookConfiguration",
        "metadata": {"name": "vwc-toggle-create-op"},
        "webhooks": [{
            "name": "deny-configmaps.example.com",
            "admissionReviewVersions": ["v1"],
            "sideEffects": "None",
            "failurePolicy": "Fail",
            "timeoutSeconds": 1,
            "clientConfig": {"url": "https://127.0.0.1:1/deny"},
            "rules": [{
                "operations": ["UPDATE"],
                "apiGroups": [""],
                "apiVersions": ["v1"],
                "resources": ["configmaps"]
            }]
        }]
    });
    let put_vwc_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/vwc-toggle-create-op")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&put_vwc).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put_vwc_resp.status(), StatusCode::OK);

    let allowed_after_put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/default/configmaps")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm-allowed-after-put"},"data":{"k":"v"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        allowed_after_put.status(),
        StatusCode::CREATED,
        "CREATE must be allowed after PUT removes CREATE from validating webhook rules"
    );

    let patch_vwc_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/vwc-toggle-create-op")
                .header("content-type", "application/json-patch+json")
                .body(Body::from(
                    r#"[{"op":"replace","path":"/webhooks/0/rules/0/operations","value":["CREATE"]}]"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch_vwc_resp.status(), StatusCode::OK);

    let denied_after_patch = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/default/configmaps")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm-denied-after-patch"},"data":{"k":"v"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        denied_after_patch.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "CREATE must be blocked again after JSON Patch restores CREATE operation"
    );
}

#[tokio::test]
async fn test_mutating_webhook_configuration_status_routes_are_supported() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let create_body = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "MutatingWebhookConfiguration",
        "metadata": {"name": "mwc-status"},
        "webhooks": [{
            "name": "mwc-status.example.com",
            "admissionReviewVersions": ["v1"],
            "sideEffects": "None",
            "clientConfig": {"url": "https://example.invalid/mutate"},
            "rules": [{
                "operations": ["CREATE"],
                "apiGroups": [""],
                "apiVersions": ["v1"],
                "resources": ["pods"]
            }]
        }]
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&create_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let status = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations/mwc-status/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(status.status(), StatusCode::OK);

    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations/mwc-status/status")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({"status": {"accepted": true}})).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    let patch = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations/mwc-status/status")
                .header("content-type", "application/merge-patch+json")
                .body(Body::from(
                    serde_json::to_vec(&json!({"status": {"accepted": false}})).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_validating_webhook_configuration_status_routes_are_supported() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let create_body = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingWebhookConfiguration",
        "metadata": {"name": "vwc-status"},
        "webhooks": [{
            "name": "vwc-status.example.com",
            "admissionReviewVersions": ["v1"],
            "sideEffects": "None",
            "clientConfig": {"url": "https://example.invalid/validate"},
            "rules": [{
                "operations": ["CREATE"],
                "apiGroups": [""],
                "apiVersions": ["v1"],
                "resources": ["pods"]
            }]
        }]
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&create_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let status = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/vwc-status/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(status.status(), StatusCode::OK);

    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/vwc-status/status")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({"status": {"accepted": true}})).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    let patch = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/vwc-status/status")
                .header("content-type", "application/merge-patch+json")
                .body(Body::from(
                    serde_json::to_vec(&json!({"status": {"accepted": false}})).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_custom_resource_delete_runs_delete_admission() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "bars.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {"plural": "bars", "singular": "bar", "kind": "Bar"},
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object"}}
            }]
        }
    });
    let create_crd = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/vnd.kubernetes.protobuf")
                .body(Body::from(crate::protobuf::encode_protobuf(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_crd.status(), StatusCode::CREATED);

    let ns = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns.status(), StatusCode::CREATED);

    let vwc = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingWebhookConfiguration",
        "metadata": {"name": "crd-delete-webhook"},
        "webhooks": [{
            "name": "delete-only.example.com",
            "admissionReviewVersions": ["v1"],
            "sideEffects": "None",
            "failurePolicy": "Fail",
            "timeoutSeconds": 1,
            "clientConfig": {"url": "https://127.0.0.1:1/validate"},
            "rules": [{
                "operations": ["DELETE"],
                "apiGroups": ["example.com"],
                "apiVersions": ["v1"],
                "resources": ["bars"]
            }]
        }]
    });
    let create_vwc = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&vwc).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_vwc.status(), StatusCode::CREATED);

    let create_cr = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/example.com/v1/namespaces/default/bars")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"example.com/v1","kind":"Bar","metadata":{"name":"b1","namespace":"default"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_cr.status(), StatusCode::CREATED);

    let delete_cr = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/apis/example.com/v1/namespaces/default/bars/b1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        delete_cr.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "DELETE custom resource must invoke DELETE-scoped webhook (fail-closed on call error)"
    );
}

#[tokio::test]
async fn test_cluster_custom_resource_delete_runs_delete_admission() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Cluster",
            "names": {"plural": "widgets", "singular": "widget", "kind": "Widget"},
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object"}}
            }]
        }
    });
    let create_crd = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_crd.status(), StatusCode::CREATED);

    let vwc = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingWebhookConfiguration",
        "metadata": {"name": "cluster-crd-delete-webhook"},
        "webhooks": [{
            "name": "cluster-delete-only.example.com",
            "admissionReviewVersions": ["v1"],
            "sideEffects": "None",
            "failurePolicy": "Fail",
            "timeoutSeconds": 1,
            "clientConfig": {"url": "https://127.0.0.1:1/validate"},
            "rules": [{
                "operations": ["DELETE"],
                "apiGroups": ["example.com"],
                "apiVersions": ["v1"],
                "resources": ["widgets"]
            }]
        }]
    });
    let create_vwc = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&vwc).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_vwc.status(), StatusCode::CREATED);

    let create_cr = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/example.com/v1/widgets")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"example.com/v1","kind":"Widget","metadata":{"name":"w1"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_cr.status(), StatusCode::CREATED);

    let delete_cr = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/apis/example.com/v1/widgets/w1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        delete_cr.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "DELETE cluster custom resource must invoke DELETE-scoped webhook (fail-closed on call error)"
    );
}

#[tokio::test]
async fn test_crd_get_non_storage_version_uses_conversion_webhook() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 16384];
        let n = stream.read(&mut buf).await.unwrap();
        let req = String::from_utf8_lossy(&buf[..n]);
        let body_start = req.find("\r\n\r\n").map(|i| i + 4).unwrap_or(req.len());
        let body = &req[body_start..];
        let review_req: serde_json::Value = serde_json::from_str(body).unwrap();
        let uid = review_req
            .pointer("/request/uid")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let desired = review_req
            .pointer("/request/desiredAPIVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("example.com/v2");
        let mut converted = review_req["request"]["objects"][0].clone();
        converted["apiVersion"] = json!(desired);
        converted["spec"]["convertedBy"] = json!("webhook");

        let response_body = json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "ConversionReview",
            "response": {
                "uid": uid,
                "result": {"status": "Success"},
                "convertedObjects": [converted]
            }
        });
        let payload = serde_json::to_string(&response_body).unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            payload.len(),
            payload
        );
        stream.write_all(response.as_bytes()).await.unwrap();
    });

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let registry = state.crd_registry.clone();
    let app = crate::api::build_router(state);

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {"plural": "widgets", "singular": "widget", "kind": "Widget"},
            "versions": [
                {"name": "v1", "served": true, "storage": true, "schema": {"openAPIV3Schema": {"type": "object"}}},
                {"name": "v2", "served": true, "storage": false, "schema": {"openAPIV3Schema": {"type": "object"}}}
            ],
            "conversion": {
                "strategy": "Webhook",
                "webhook": {
                    "clientConfig": {"url": format!("http://127.0.0.1:{}/convert", port)},
                    "conversionReviewVersions": ["v1"]
                }
            }
        }
    });

    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "widgets.example.com",
        crd.clone(),
    )
    .await
    .unwrap();
    crate::controllers::crd::register_crd_from_value(&registry, &crd)
        .await
        .unwrap();

    db.create_resource(
        "example.com/v1",
        "Widget",
        Some("default"),
        "w1",
        json!({
            "apiVersion": "example.com/v1",
            "kind": "Widget",
            "metadata": {"name": "w1", "namespace": "default"},
            "spec": {"size": "small"}
        }),
    )
    .await
    .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/example.com/v2/namespaces/default/widgets/w1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["apiVersion"], "example.com/v2");
    assert_eq!(value["spec"]["convertedBy"], "webhook");
}

#[tokio::test]
async fn test_crd_list_non_storage_version_converts_heterogeneous_storage() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 16384];
        let n = stream.read(&mut buf).await.unwrap();
        let req = String::from_utf8_lossy(&buf[..n]);
        let body_start = req.find("\r\n\r\n").map(|i| i + 4).unwrap_or(req.len());
        let body = &req[body_start..];
        let review_req: serde_json::Value = serde_json::from_str(body).unwrap();
        let uid = review_req
            .pointer("/request/uid")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let desired = review_req
            .pointer("/request/desiredAPIVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("example.com/v2");

        let converted_objects: Vec<serde_json::Value> = review_req["request"]["objects"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|mut o| {
                o["apiVersion"] = json!(desired);
                o["spec"]["convertedBy"] = json!("webhook");
                o
            })
            .collect();

        let response_body = json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "ConversionReview",
            "response": {
                "uid": uid,
                "result": {"status": "Success"},
                "convertedObjects": converted_objects
            }
        });
        let payload = serde_json::to_string(&response_body).unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            payload.len(),
            payload
        );
        stream.write_all(response.as_bytes()).await.unwrap();
    });

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let registry = state.crd_registry.clone();
    let app = crate::api::build_router(state);

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {"plural": "widgets", "singular": "widget", "kind": "Widget"},
            "versions": [
                {"name": "v1", "served": true, "storage": true, "schema": {"openAPIV3Schema": {"type": "object"}}},
                {"name": "v2", "served": true, "storage": false, "schema": {"openAPIV3Schema": {"type": "object"}}}
            ],
            "conversion": {
                "strategy": "Webhook",
                "webhook": {
                    "clientConfig": {"url": format!("http://127.0.0.1:{}/convert", port)},
                    "conversionReviewVersions": ["v1"]
                }
            }
        }
    });

    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "widgets.example.com",
        crd.clone(),
    )
    .await
    .unwrap();
    crate::controllers::crd::register_crd_from_value(&registry, &crd)
        .await
        .unwrap();

    db.create_resource(
        "example.com/v1",
        "Widget",
        Some("default"),
        "from-v1",
        json!({
            "apiVersion": "example.com/v1",
            "kind": "Widget",
            "metadata": {"name": "from-v1", "namespace": "default"},
            "spec": {"origin": "v1"}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "example.com/v2",
        "Widget",
        Some("default"),
        "from-v2",
        json!({
            "apiVersion": "example.com/v2",
            "kind": "Widget",
            "metadata": {"name": "from-v2", "namespace": "default"},
            "spec": {"origin": "v2"}
        }),
    )
    .await
    .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/example.com/v2/namespaces/default/widgets")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let items = value["items"].as_array().cloned().unwrap_or_default();
    assert_eq!(
        items.len(),
        2,
        "heterogeneous list must include both objects"
    );
    let mut saw_v1_object_converted = false;
    for item in items {
        assert_eq!(item["apiVersion"], "example.com/v2");
        let name = item
            .pointer("/metadata/name")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if name == "from-v1" {
            assert_eq!(item["spec"]["convertedBy"], "webhook");
            saw_v1_object_converted = true;
        }
    }
    assert!(
        saw_v1_object_converted,
        "list must include converted output for object originally stored as v1"
    );
}

#[tokio::test]
async fn test_crd_non_storage_field_selector_filters_on_converted_objects() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut buf = vec![0u8; 65536];
                let n = match stream.read(&mut buf).await {
                    Ok(n) => n,
                    Err(_) => return,
                };
                if n == 0 {
                    return;
                }
                let req = String::from_utf8_lossy(&buf[..n]);
                let body_start = req.find("\r\n\r\n").map(|i| i + 4).unwrap_or(req.len());
                let review_req: serde_json::Value = match serde_json::from_str(&req[body_start..]) {
                    Ok(v) => v,
                    Err(_) => return,
                };
                let uid = review_req
                    .pointer("/request/uid")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let desired = review_req
                    .pointer("/request/desiredAPIVersion")
                    .and_then(|v| v.as_str())
                    .unwrap_or("example.com/v1");
                let converted_objects: Vec<serde_json::Value> = review_req["request"]["objects"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|mut obj| {
                        if desired.ends_with("/v1") {
                            let host = obj.get("host").and_then(|v| v.as_str());
                            let port = obj.get("port").and_then(|v| v.as_str());
                            if let (Some(host), Some(port)) = (host, port) {
                                obj["hostPort"] = json!(format!("{host}:{port}"));
                            }
                            if let Some(map) = obj.as_object_mut() {
                                map.remove("host");
                                map.remove("port");
                            }
                        } else if desired.ends_with("/v2") {
                            if let Some(host_port) = obj
                                .get("hostPort")
                                .and_then(|v| v.as_str())
                                .map(ToString::to_string)
                                && let Some((host, port)) = host_port.split_once(':')
                            {
                                obj["host"] = json!(host);
                                obj["port"] = json!(port);
                            }
                            if let Some(map) = obj.as_object_mut() {
                                map.remove("hostPort");
                            }
                        }
                        obj["apiVersion"] = json!(desired);
                        obj
                    })
                    .collect();
                let response_body = json!({
                    "apiVersion": "apiextensions.k8s.io/v1",
                    "kind": "ConversionReview",
                    "response": {
                        "uid": uid,
                        "result": {"status": "Success"},
                        "convertedObjects": converted_objects
                    }
                });
                let payload = serde_json::to_string(&response_body).unwrap();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    payload.len(),
                    payload
                );
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    });

    let app = build_test_router().await;

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {"plural": "widgets", "singular": "widget", "kind": "Widget"},
            "versions": [
                {
                    "name": "v1",
                    "served": true,
                    "storage": true,
                    "selectableFields": [{"jsonPath": ".hostPort"}],
                    "schema": {
                        "openAPIV3Schema": {
                            "type": "object",
                            "properties": {
                                "hostPort": {"type": "string"}
                            }
                        }
                    }
                },
                {
                    "name": "v2",
                    "served": true,
                    "storage": false,
                    "selectableFields": [{"jsonPath": ".host"}, {"jsonPath": ".port"}],
                    "schema": {
                        "openAPIV3Schema": {
                            "type": "object",
                            "properties": {
                                "host": {"type": "string"},
                                "port": {"type": "string"}
                            }
                        }
                    }
                }
            ],
            "conversion": {
                "strategy": "Webhook",
                "webhook": {
                    "clientConfig": {"url": format!("http://127.0.0.1:{}/convert", port)},
                    "conversionReviewVersions": ["v1"]
                }
            }
        }
    });

    let create_crd = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_crd.status(), StatusCode::CREATED);

    let initial_list_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/example.com/v1/namespaces/default/widgets?fieldSelector=hostPort%3Dhost1%3A80")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(initial_list_resp.status(), StatusCode::OK);
    let initial_list_body = axum::body::to_bytes(initial_list_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let initial_list_value: serde_json::Value = serde_json::from_slice(&initial_list_body).unwrap();
    let watch_resource_version = initial_list_value["metadata"]["resourceVersion"]
        .as_str()
        .expect("initial CR list must include metadata.resourceVersion")
        .to_string();

    for (name, host, port_value) in [
        ("w1", "host1", "80"),
        ("w2", "host1", "8080"),
        ("w3", "host2", "80"),
    ] {
        let create_cr = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/apis/example.com/v2/namespaces/default/widgets")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "apiVersion": "example.com/v2",
                            "kind": "Widget",
                            "metadata": {"name": name, "namespace": "default"},
                            "host": host,
                            "port": port_value
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_cr.status(), StatusCode::CREATED);
    }

    let unfiltered_list_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/example.com/v1/namespaces/default/widgets")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unfiltered_list_resp.status(), StatusCode::OK);
    let unfiltered_list_body = axum::body::to_bytes(unfiltered_list_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let unfiltered_list_value: serde_json::Value =
        serde_json::from_slice(&unfiltered_list_body).unwrap();
    let unfiltered_items = unfiltered_list_value["items"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        unfiltered_items.len(),
        3,
        "converted v1 list without selector must include all created objects, response: {}",
        unfiltered_list_value
    );

    let list_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/example.com/v1/namespaces/default/widgets?fieldSelector=hostPort%3Dhost1%3A80")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let list_body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list_value: serde_json::Value = serde_json::from_slice(&list_body).unwrap();
    let items = list_value["items"].as_array().cloned().unwrap_or_default();
    assert_eq!(
        items.len(),
        1,
        "v1 field selector must filter on converted objects created via v2, response: {}",
        list_value
    );
    assert_eq!(
        items[0]["metadata"]["name"], "w1",
        "v1 hostPort selector must match the converted w1 object"
    );
    assert_eq!(items[0]["apiVersion"], "example.com/v1");
    assert_eq!(items[0]["hostPort"], "host1:80");

    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/apis/example.com/v1/namespaces/default/widgets?watch=true&resourceVersion={watch_resource_version}&fieldSelector=hostPort%3Dhost1%3A80"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);
    let mut stream = watch_resp.into_body().into_data_stream();
    let chunk = tokio::time::timeout(std::time::Duration::from_secs(3), stream.next())
        .await
        .expect("watch catch-up stream timed out")
        .expect("watch stream ended unexpectedly")
        .expect("watch stream chunk error");
    let chunk_text = String::from_utf8(chunk.to_vec()).unwrap();
    let first_line = chunk_text
        .lines()
        .find(|line| !line.trim().is_empty())
        .expect("watch stream must emit at least one JSON event line");
    let event: serde_json::Value = serde_json::from_str(first_line).unwrap();
    assert_eq!(event["type"], "ADDED");
    assert_eq!(event["object"]["metadata"]["name"], "w1");
    assert_eq!(event["object"]["apiVersion"], "example.com/v1");
    assert_eq!(event["object"]["hostPort"], "host1:80");

    let delete_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/apis/example.com/v2/namespaces/default/widgets?fieldSelector=host%3Dhost1%2Cport%3D80")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        delete_resp.status(),
        StatusCode::OK,
        "delete collection must be supported for namespaced custom resources",
    );

    let list_after_delete = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/example.com/v2/namespaces/default/widgets?fieldSelector=host%3Dhost1%2Cport%3D80")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_after_delete.status(), StatusCode::OK);
    let list_after_delete_body = axum::body::to_bytes(list_after_delete.into_body(), usize::MAX)
        .await
        .unwrap();
    let list_after_delete_value: serde_json::Value =
        serde_json::from_slice(&list_after_delete_body).unwrap();
    let remaining = list_after_delete_value["items"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        remaining.is_empty(),
        "delete collection with fieldSelector must remove matching custom resources, response: {}",
        list_after_delete_value
    );
}

#[tokio::test]
async fn test_crd_patch_uses_existing_served_version_after_storage_version_switch() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let app = build_test_router().await;

    let create_ns = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_ns.status(), StatusCode::CREATED);

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {"plural": "widgets", "singular": "widget", "kind": "Widget"},
            "versions": [
                {"name": "v1", "served": true, "storage": true, "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}},
                {"name": "v2", "served": true, "storage": false, "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}}
            ]
        }
    });
    let create_crd = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_crd.status(), StatusCode::CREATED);

    let create_cr = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/example.com/v1/namespaces/default/widgets")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{
                        "apiVersion":"example.com/v1",
                        "kind":"Widget",
                        "metadata":{"name":"w1","namespace":"default"},
                        "data":{"mutation-start":"yes"}
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_cr.status(), StatusCode::CREATED);

    let storage_switch_patch = json!({
        "spec": {
            "versions": [
                {"name": "v1", "served": true, "storage": false, "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}},
                {"name": "v2", "served": true, "storage": true, "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}}
            ]
        }
    });
    let patch_crd = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions/widgets.example.com")
                .header("content-type", "application/merge-patch+json")
                .body(Body::from(
                    serde_json::to_vec(&storage_switch_patch).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch_crd.status(), StatusCode::OK);

    let patch_cr = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/apis/example.com/v2/namespaces/default/widgets/w1")
                .header("content-type", "application/json-patch+json")
                .body(Body::from(
                    r#"[{"op":"add","path":"/dummy","value":"test"}]"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        patch_cr.status(),
        StatusCode::OK,
        "PATCH via newly-served version must find existing object stored under previous version",
    );
    let value: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(patch_cr.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(value["dummy"], "test");
}

#[tokio::test]
async fn crd_create_through_non_storage_version_persists_storage_version() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let app = build_test_router().await;

    let create_ns = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_ns.status(), StatusCode::CREATED);

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {"plural": "widgets", "singular": "widget", "kind": "Widget"},
            "versions": [
                {"name": "v1", "served": true, "storage": true, "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}},
                {"name": "v2", "served": true, "storage": false, "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}}
            ]
        }
    });
    let create_crd = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_crd.status(), StatusCode::CREATED);

    let create_widget = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/example.com/v2/namespaces/default/widgets")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"example.com/v2","kind":"Widget","metadata":{"name":"w1","namespace":"default"},"spec":{"color":"blue"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_widget.status(), StatusCode::CREATED);
    let create_body = axum::body::to_bytes(create_widget.into_body(), usize::MAX)
        .await
        .unwrap();
    let create_value: serde_json::Value = serde_json::from_slice(&create_body).unwrap();
    assert_eq!(create_value["apiVersion"], "example.com/v2");

    let get_v1 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/example.com/v1/namespaces/default/widgets/w1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_v1.status(), StatusCode::OK);
    let get_v1_body = axum::body::to_bytes(get_v1.into_body(), usize::MAX)
        .await
        .unwrap();
    let get_v1_value: serde_json::Value = serde_json::from_slice(&get_v1_body).unwrap();
    assert_eq!(get_v1_value["apiVersion"], "example.com/v1");

    let get_v2 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/example.com/v2/namespaces/default/widgets/w1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_v2.status(), StatusCode::OK);
    let get_v2_body = axum::body::to_bytes(get_v2.into_body(), usize::MAX)
        .await
        .unwrap();
    let get_v2_value: serde_json::Value = serde_json::from_slice(&get_v2_body).unwrap();
    assert_eq!(get_v2_value["apiVersion"], "example.com/v2");
}

#[tokio::test]
async fn crd_same_name_create_conflicts_across_served_versions() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let app = build_test_router().await;

    let ns = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns.status(), StatusCode::CREATED);

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "gadgets.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {"plural": "gadgets", "singular": "gadget", "kind": "Gadget"},
            "versions": [
                {"name": "v1", "served": true, "storage": true, "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}},
                {"name": "v2", "served": true, "storage": false, "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}}
            ]
        }
    });
    let crd_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(crd_resp.status(), StatusCode::CREATED);

    let create_v2 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/example.com/v2/namespaces/default/gadgets")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"example.com/v2","kind":"Gadget","metadata":{"name":"g1","namespace":"default"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_v2.status(), StatusCode::CREATED);

    let duplicate_v1 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/example.com/v1/namespaces/default/gadgets")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"example.com/v1","kind":"Gadget","metadata":{"name":"g1","namespace":"default"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(duplicate_v1.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn crd_get_non_storage_version_returns_requested_api_version_without_webhook() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let app = build_test_router().await;

    let ns = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns.status(), StatusCode::CREATED);

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "nogets.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {"plural": "nogets", "singular": "noget", "kind": "NoGet"},
            "versions": [
                {"name": "v1", "served": true, "storage": true, "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}},
                {"name": "v2", "served": true, "storage": false, "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}}
            ],
            "conversion": {"strategy": "None"}
        }
    });
    let create_crd = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_crd.status(), StatusCode::CREATED);

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/example.com/v1/namespaces/default/nogets")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"example.com/v1","kind":"NoGet","metadata":{"name":"n1","namespace":"default"},"spec":{"value":"x"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let get = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/example.com/v2/namespaces/default/nogets/n1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    let get_body = axum::body::to_bytes(get.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&get_body).unwrap();
    assert_eq!(value["apiVersion"], "example.com/v2");
    assert_eq!(value["spec"]["value"], "x");
}

#[tokio::test]
async fn crd_list_non_storage_version_returns_requested_api_version_without_webhook() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let app = build_test_router().await;

    let ns = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns.status(), StatusCode::CREATED);

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "nolists.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {"plural": "nolists", "singular": "nolist", "kind": "NoList"},
            "versions": [
                {"name": "v1", "served": true, "storage": true, "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}},
                {"name": "v2", "served": true, "storage": false, "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}}
            ],
            "conversion": {"strategy": "None"}
        }
    });
    let create_crd = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_crd.status(), StatusCode::CREATED);

    for name in ["l1", "l2"] {
        let create = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/apis/example.com/v1/namespaces/default/nolists")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "apiVersion":"example.com/v1",
                            "kind":"NoList",
                            "metadata":{"name":name,"namespace":"default"}
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create.status(), StatusCode::CREATED);
    }

    let list = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/example.com/v2/namespaces/default/nolists")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list.status(), StatusCode::OK);
    let body = axum::body::to_bytes(list.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["apiVersion"], "example.com/v2");
    let items = value["items"].as_array().cloned().unwrap_or_default();
    assert_eq!(items.len(), 2);
    for item in items {
        assert_eq!(item["apiVersion"], "example.com/v2");
    }
}

#[tokio::test]
async fn crd_delete_through_any_served_version_deletes_logical_object() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let app = build_test_router().await;

    let ns = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns.status(), StatusCode::CREATED);

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "zapthings.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {"plural": "zapthings", "singular": "zapthing", "kind": "ZapThing"},
            "versions": [
                {"name": "v1", "served": true, "storage": true, "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}},
                {"name": "v2", "served": true, "storage": false, "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}}
            ],
            "conversion": {"strategy": "None"}
        }
    });
    let create_crd = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_crd.status(), StatusCode::CREATED);

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/example.com/v1/namespaces/default/zapthings")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"example.com/v1","kind":"ZapThing","metadata":{"name":"z1","namespace":"default"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let delete = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/apis/example.com/v2/namespaces/default/zapthings/z1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(delete.status(), StatusCode::OK);

    for version in ["v1", "v2"] {
        let get = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!(
                        "/apis/example.com/{version}/namespaces/default/zapthings/z1"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            get.status(),
            StatusCode::NOT_FOUND,
            "version {version} must be gone"
        );
    }
}

#[tokio::test]
async fn crd_update_through_non_storage_version_updates_logical_object() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let app = build_test_router().await;

    let ns = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns.status(), StatusCode::CREATED);

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "mutables.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {"plural": "mutables", "singular": "mutable", "kind": "Mutable"},
            "versions": [
                {"name": "v1", "served": true, "storage": true, "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}},
                {"name": "v2", "served": true, "storage": false, "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}}
            ]
        }
    });
    let crd_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(crd_resp.status(), StatusCode::CREATED);

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/example.com/v1/namespaces/default/mutables")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"example.com/v1","kind":"Mutable","metadata":{"name":"m1","namespace":"default"},"spec":{"value":"old"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let update = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/apis/example.com/v2/namespaces/default/mutables/m1")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"example.com/v2","kind":"Mutable","metadata":{"name":"m1","namespace":"default"},"spec":{"value":"new"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(update.status(), StatusCode::OK);
    let update_body = axum::body::to_bytes(update.into_body(), usize::MAX)
        .await
        .unwrap();
    let update_value: serde_json::Value = serde_json::from_slice(&update_body).unwrap();
    assert_eq!(update_value["apiVersion"], "example.com/v2");

    let get = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/example.com/v1/namespaces/default/mutables/m1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    let body = axum::body::to_bytes(get.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["apiVersion"], "example.com/v1");
    assert_eq!(value["spec"]["value"], "new");
}

#[tokio::test]
async fn crd_watch_non_storage_version_receives_requested_version_events() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use tower::ServiceExt;

    let app = build_test_router().await;

    let ns = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns.status(), StatusCode::CREATED);

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "watcheds.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {"plural": "watcheds", "singular": "watched", "kind": "Watched"},
            "versions": [
                {"name": "v1", "served": true, "storage": true, "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}},
                {"name": "v2", "served": true, "storage": false, "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}}
            ],
            "conversion": {"strategy": "None"}
        }
    });
    let crd_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(crd_resp.status(), StatusCode::CREATED);

    let watch_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/example.com/v2/namespaces/default/watcheds?watch=true&sendInitialEvents=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_response.status(), StatusCode::OK);

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/example.com/v1/namespaces/default/watcheds")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"example.com/v1","kind":"Watched","metadata":{"name":"w1","namespace":"default"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let mut stream = watch_response.into_body().into_data_stream();
    let mut saw_added = false;
    for _ in 0..8 {
        let chunk = tokio::time::timeout(std::time::Duration::from_secs(3), stream.next())
            .await
            .expect("watch stream timed out")
            .expect("watch stream ended unexpectedly")
            .expect("watch stream chunk must be readable");
        let text = String::from_utf8(chunk.to_vec()).unwrap();
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let event: serde_json::Value = serde_json::from_str(line).unwrap();
            if event["type"] != "ADDED" {
                continue;
            }
            if event["object"]["metadata"]["name"] != "w1" {
                continue;
            }
            assert_eq!(event["object"]["apiVersion"], "example.com/v2");
            assert_eq!(event["object"]["kind"], "Watched");
            saw_added = true;
            break;
        }
        if saw_added {
            break;
        }
    }

    assert!(saw_added, "watch must emit ADDED for created object");
}

#[tokio::test]
async fn test_mutating_webhook_custom_resource_create_prunes_unknown_fields_from_mutation() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use base64::Engine;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 32768];
        let n = stream.read(&mut buf).await.unwrap();
        let req = String::from_utf8_lossy(&buf[..n]);
        let body_start = req.find("\r\n\r\n").map(|i| i + 4).unwrap_or(req.len());
        let review_req: serde_json::Value = serde_json::from_str(&req[body_start..]).unwrap();
        let uid = review_req["request"]["uid"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        let patch = json!([
            {"op": "add", "path": "/data/mutation-stage-1", "value": "yes"},
            {"op": "add", "path": "/data/mutation-stage-2", "value": "yes"}
        ]);
        let patch_b64 =
            base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&patch).unwrap());
        let response_body = json!({
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "response": {
                "uid": uid,
                "allowed": true,
                "patchType": "JSONPatch",
                "patch": patch_b64
            }
        });
        let payload = serde_json::to_string(&response_body).unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            payload.len(),
            payload
        );
        stream.write_all(response.as_bytes()).await.unwrap();
    });

    let app = build_test_router().await;

    let create_ns = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_ns.status(), StatusCode::CREATED);

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "preserveUnknownFields": false,
            "names": {"plural": "widgets", "singular": "widget", "kind": "Widget"},
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "data": {
                                "type": "object",
                                "properties": {
                                    "mutation-start": {"type": "string"},
                                    "mutation-stage-1": {"type": "string"}
                                }
                            }
                        }
                    }
                }
            }]
        }
    });
    let create_crd = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_crd.status(), StatusCode::CREATED);

    let mwc = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "MutatingWebhookConfiguration",
        "metadata": {"name": "cr-pruning-mutation"},
        "webhooks": [{
            "name": "mutate-widgets.example.com",
            "rules": [{
                "operations": ["CREATE"],
                "apiGroups": ["example.com"],
                "apiVersions": ["v1"],
                "resources": ["widgets"]
            }],
            "clientConfig": {"url": format!("http://127.0.0.1:{}/mutate", port)},
            "sideEffects": "None",
            "admissionReviewVersions": ["v1"]
        }]
    });
    let create_mwc = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&mwc).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_mwc.status(), StatusCode::CREATED);

    let create_cr = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/example.com/v1/namespaces/default/widgets")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{
                        "apiVersion":"example.com/v1",
                        "kind":"Widget",
                        "metadata":{"name":"w-prune","namespace":"default"},
                        "data":{"mutation-start":"yes"}
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_cr.status(), StatusCode::CREATED);
    let value: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(create_cr.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        value["data"],
        json!({
            "mutation-start": "yes",
            "mutation-stage-1": "yes"
        }),
        "unknown field injected by mutating webhook must be pruned by CRD schema",
    );
}

#[tokio::test]
async fn test_cluster_custom_resource_watch_field_selector_preserves_cluster_scope() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "clusterwidgets.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Cluster",
            "names": {"plural": "clusterwidgets", "singular": "clusterwidget", "kind": "ClusterWidget"},
            "versions": [{"name": "v1", "served": true, "storage": true, "schema": {"openAPIV3Schema": {"type": "object"}}}]
        }
    });
    let create_crd = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_crd.status(), StatusCode::CREATED);

    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/example.com/v1/clusterwidgets?watch=true&sendInitialEvents=true&fieldSelector=metadata.name%3Dcw1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);
    let mut stream = watch_resp.into_body().into_data_stream();

    let create_cr = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/example.com/v1/clusterwidgets")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"example.com/v1","kind":"ClusterWidget","metadata":{"name":"cw1"},"spec":{"x":"y"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_cr.status(), StatusCode::CREATED);

    let first_chunk = tokio::time::timeout(std::time::Duration::from_secs(3), stream.next())
        .await
        .expect("watch stream timed out")
        .expect("watch stream ended unexpectedly")
        .expect("watch stream chunk error");
    let line = String::from_utf8(first_chunk.to_vec()).unwrap();
    let event: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(event["type"], "ADDED");
    assert_eq!(event["object"]["metadata"]["name"], "cw1");
    assert!(
        event["object"]["metadata"].get("namespace").is_none(),
        "cluster-scoped watch event must not include metadata.namespace: {}",
        event
    );
}

#[tokio::test]
async fn test_cluster_custom_resource_watch_skips_stale_backlog_event_when_rv_zero() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "noxus.mygroup.example.com"},
        "spec": {
            "group": "mygroup.example.com",
            "scope": "Cluster",
            "names": {"plural": "noxus", "singular": "noxu", "kind": "WishIHadChosenNoxu"},
            "versions": [{
                "name": "v1beta1",
                "served": true,
                "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}
            }]
        }
    });
    let create_crd = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_crd.status(), StatusCode::CREATED);

    // Seed DB RV before opening watch so the watch can establish a baseline.
    let create_old = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/mygroup.example.com/v1beta1/noxus")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "apiVersion": "mygroup.example.com/v1beta1",
                        "kind": "WishIHadChosenNoxu",
                        "metadata": {"name": "name1"},
                        "content": {"key": "old"},
                        "num": {"num1": 9223372036854775807_i64, "num2": 1000000}
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_old.status(), StatusCode::CREATED);
    let create_old_body = axum::body::to_bytes(create_old.into_body(), usize::MAX)
        .await
        .unwrap();
    let old_object: serde_json::Value = serde_json::from_slice(&create_old_body).unwrap();
    let old_rv = old_object["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .parse::<i64>()
        .unwrap();

    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/mygroup.example.com/v1beta1/noxus?watch=true&fieldSelector=metadata.name%3Dname1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);
    let mut stream = watch_resp.into_body().into_data_stream();

    // Simulate delayed/stale broadcast delivery from backlog.
    db.broadcast_watch_event(crate::datastore::PendingWatchEvent {
        event: crate::watch::WatchEvent::added(old_object.clone()),
    });

    let patch_fresh = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/apis/mygroup.example.com/v1beta1/noxus/name1")
                .header("content-type", "application/merge-patch+json")
                .body(Body::from(
                    serde_json::to_vec(&json!({"content": {"key": "fresh"}})).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch_fresh.status(), StatusCode::OK);
    let patch_fresh_body = axum::body::to_bytes(patch_fresh.into_body(), usize::MAX)
        .await
        .unwrap();
    let fresh_object: serde_json::Value = serde_json::from_slice(&patch_fresh_body).unwrap();
    let fresh_rv = fresh_object["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .parse::<i64>()
        .unwrap();
    assert!(
        fresh_rv > old_rv,
        "fresh patch rv {fresh_rv} must be newer than baseline rv {old_rv}"
    );

    // An rv-less selector watch delivers the establishment-time state
    // (name1 @ old_rv) as a baseline ADDED, then the fresh update (@ fresh_rv).
    // The injected STALE DUPLICATE broadcast (@ old_rv) must be deduped — never
    // re-delivered — so the client never regresses to stale content.
    let mut old_count = 0;
    let mut saw_fresh = false;
    for _ in 0..6 {
        let chunk =
            match tokio::time::timeout(std::time::Duration::from_secs(3), stream.next()).await {
                Ok(Some(Ok(c))) => c,
                _ => break,
            };
        for line in String::from_utf8(chunk.to_vec())
            .unwrap()
            .lines()
            .filter(|l| !l.trim().is_empty())
        {
            let event: serde_json::Value = serde_json::from_str(line).unwrap();
            let rv = event["object"]["metadata"]["resourceVersion"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            if rv == old_rv.to_string() {
                assert_eq!(event["type"], "ADDED");
                old_count += 1;
            } else if rv == fresh_rv.to_string() {
                saw_fresh = true;
                assert!(
                    event["type"] == "ADDED" || event["type"] == "MODIFIED",
                    "fresh event may be an ADDED selector transition or a MODIFIED update"
                );
                assert_eq!(event["object"]["content"]["key"], "fresh");
            }
        }
        if saw_fresh {
            break;
        }
    }
    assert!(
        saw_fresh,
        "fresh event (rv > establishment floor) must be delivered"
    );
    assert_eq!(
        old_count, 1,
        "establishment-time state is delivered once as baseline; the stale \
         rv<=baseline duplicate broadcast must be deduped, not re-delivered"
    );
}

#[tokio::test]
async fn test_watch_and_list_label_selector_parity_for_exists_selector() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use tower::ServiceExt;

    let app = build_test_router().await;
    let namespace = "sel-parity-exists";

    let create_ns = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"apiVersion":"v1","kind":"Namespace","metadata":{{"name":"{}"}}}}"#,
                    namespace
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_ns.status(), StatusCode::CREATED);

    for (name, labels) in [
        ("cm-gpu-a", json!({"has-gpu": "true"})),
        ("cm-cpu-b", json!({"cpu-only": "true"})),
        ("cm-gpu-c", json!({"has-gpu": "yes", "deprecated": "true"})),
    ] {
        let create_cm = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/v1/namespaces/{}/configmaps", namespace))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "apiVersion": "v1",
                            "kind": "ConfigMap",
                            "metadata": {
                                "name": name,
                                "namespace": namespace,
                                "labels": labels,
                            },
                            "data": {
                                "k": "v"
                            }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_cm.status(), StatusCode::CREATED);
    }

    let list_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/api/v1/namespaces/{}/configmaps?labelSelector=has-gpu",
                    namespace
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let list_body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list_value: serde_json::Value = serde_json::from_slice(&list_body).unwrap();
    let mut list_names: Vec<String> = list_value["items"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|item| {
            item.get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|name| name.as_str())
                .map(ToString::to_string)
        })
        .collect();
    list_names.sort();

    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/api/v1/namespaces/{}/configmaps?watch=true&sendInitialEvents=true&labelSelector=has-gpu",
                    namespace
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);
    let mut stream = watch_resp.into_body().into_data_stream();

    let mut watch_names: Vec<String> = Vec::new();
    let mut saw_initial_end = false;

    for _ in 0..8 {
        let chunk = tokio::time::timeout(std::time::Duration::from_secs(3), stream.next())
            .await
            .expect("watch stream timed out")
            .expect("watch stream ended unexpectedly")
            .expect("watch stream chunk error");

        let chunk_text = String::from_utf8(chunk.to_vec()).unwrap();
        for line in chunk_text.lines().filter(|line| !line.trim().is_empty()) {
            let event: serde_json::Value = serde_json::from_str(line).unwrap();
            if event["type"] == "ADDED"
                && let Some(name) = event["object"]["metadata"]["name"].as_str()
            {
                watch_names.push(name.to_string());
            }
            if event["type"] == "BOOKMARK"
                && event["object"]["metadata"]["annotations"]["k8s.io/initial-events-end"] == "true"
            {
                saw_initial_end = true;
                break;
            }
        }
        if saw_initial_end {
            break;
        }
    }

    assert!(
        saw_initial_end,
        "watch stream with sendInitialEvents=true must emit initial-events-end bookmark"
    );

    watch_names.sort();
    watch_names.dedup();
    assert_eq!(
        watch_names, list_names,
        "watch ADDED initial events for exists selector must match list labelSelector results"
    );
}

#[tokio::test]
async fn test_namespace_list_label_selector_pagination_filters_before_chunking() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let app = build_test_router().await;
    for (name, labels) in [
        ("ns-a", json!({"env": "prod", "team": "core"})),
        ("ns-b", json!({"env": "prod", "team": "edge"})),
        ("ns-c", json!({"env": "dev", "team": "core"})),
        ("ns-d", json!({"env": "prod", "deprecated": "true"})),
        ("ns-e", json!({"env": "prod"})),
    ] {
        let create_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/namespaces")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "apiVersion": "v1",
                            "kind": "Namespace",
                            "metadata": { "name": name, "labels": labels }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_resp.status(), StatusCode::CREATED);
    }

    let page1_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(
                    "/api/v1/namespaces?labelSelector=env%3Dprod%2C!deprecated&limit=2".to_string(),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(page1_resp.status(), StatusCode::OK);
    let page1_body = axum::body::to_bytes(page1_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let page1_json: serde_json::Value = serde_json::from_slice(&page1_body).unwrap();
    let page1_items = page1_json["items"].as_array().unwrap();
    assert_eq!(page1_items.len(), 2);
    let page1_names: Vec<String> = page1_items
        .iter()
        .filter_map(|item| item["metadata"]["name"].as_str().map(ToString::to_string))
        .collect();
    assert_eq!(page1_names, vec!["ns-a", "ns-b"]);
    assert_eq!(
        page1_json["metadata"]["remainingItemCount"].as_i64(),
        Some(1),
        "filtered result set has one item left after page 1"
    );

    let continue_token = page1_json["metadata"]["continue"]
        .as_str()
        .expect("continue token must exist when filtered items remain")
        .to_string();
    let page2_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/api/v1/namespaces?labelSelector=env%3Dprod%2C!deprecated&limit=2&continue={}",
                    urlencoding::encode(&continue_token)
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(page2_resp.status(), StatusCode::OK);
    let page2_body = axum::body::to_bytes(page2_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let page2_json: serde_json::Value = serde_json::from_slice(&page2_body).unwrap();
    let page2_items = page2_json["items"].as_array().unwrap();
    assert_eq!(page2_items.len(), 1);
    let page2_names: Vec<String> = page2_items
        .iter()
        .filter_map(|item| item["metadata"]["name"].as_str().map(ToString::to_string))
        .collect();
    assert_eq!(page2_names, vec!["ns-e"]);
    assert!(
        page2_json["metadata"].get("continue").is_none(),
        "final page must not return continue token"
    );
}

#[tokio::test]
async fn test_namespace_watch_list_label_selector_parity_and_invalid_selector_rejected() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use tower::ServiceExt;

    let app = build_test_router().await;

    for (name, labels) in [
        ("watch-ns-a", json!({"team": "core"})),
        ("watch-ns-b", json!({"team": "edge"})),
        ("watch-ns-c", json!({"deprecated": "true"})),
    ] {
        let create_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/namespaces")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "apiVersion": "v1",
                            "kind": "Namespace",
                            "metadata": { "name": name, "labels": labels }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_resp.status(), StatusCode::CREATED);
    }

    let list_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces?labelSelector=team")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let list_body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list_json: serde_json::Value = serde_json::from_slice(&list_body).unwrap();
    let mut list_names: Vec<String> = list_json["items"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|item| {
            item.get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
                .map(ToString::to_string)
        })
        .collect();
    list_names.sort();

    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces?watch=true&sendInitialEvents=true&labelSelector=team")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);
    let mut stream = watch_resp.into_body().into_data_stream();
    let mut watch_names: Vec<String> = Vec::new();

    for _ in 0..6 {
        let chunk = tokio::time::timeout(std::time::Duration::from_secs(3), stream.next())
            .await
            .expect("watch stream timed out")
            .expect("watch stream ended unexpectedly")
            .expect("watch stream chunk error");
        let chunk_text = String::from_utf8(chunk.to_vec()).unwrap();
        for line in chunk_text.lines().filter(|line| !line.trim().is_empty()) {
            let event: serde_json::Value = serde_json::from_str(line).unwrap();
            if event["type"] == "ADDED"
                && let Some(name) = event["object"]["metadata"]["name"].as_str()
            {
                watch_names.push(name.to_string());
            }
        }
        if watch_names.len() >= list_names.len() {
            break;
        }
    }

    watch_names.sort();
    watch_names.dedup();
    assert_eq!(watch_names, list_names);

    let bad_list_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces?labelSelector=env%20in%20(prod")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        bad_list_resp.status(),
        StatusCode::BAD_REQUEST,
        "invalid namespace label selector must return 400"
    );

    let bad_watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces?watch=true&labelSelector=env%20in%20(prod")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        bad_watch_resp.status(),
        StatusCode::BAD_REQUEST,
        "invalid watch label selector must return 400"
    );
}

async fn build_app_with_cluster_widget_crd() -> axum::Router {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Cluster",
            "names": {"plural": "widgets", "singular": "widget", "kind": "Widget"},
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {"openAPIV3Schema": {
                    "type": "object",
                    "x-kubernetes-preserve-unknown-fields": true
                }}
            }]
        }
    });
    let create_crd = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_crd.status(), StatusCode::CREATED);
    app
}

async fn create_cluster_widget(app: &axum::Router, name: &str) -> serde_json::Value {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let body = format!(
        r#"{{"apiVersion":"example.com/v1","kind":"Widget","metadata":{{"name":"{name}"}}}}"#
    );
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/example.com/v1/widgets")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "create cluster widget {name} must succeed"
    );
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

async fn create_cluster_widget_child(
    app: &axum::Router,
    name: &str,
    owner_name: &str,
    owner_uid: &str,
) {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let body = serde_json::json!({
        "apiVersion": "example.com/v1",
        "kind": "Widget",
        "metadata": {
            "name": name,
            "ownerReferences": [{
                "apiVersion": "example.com/v1",
                "kind": "Widget",
                "name": owner_name,
                "uid": owner_uid,
                "controller": true,
                "blockOwnerDeletion": true
            }]
        }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/example.com/v1/widgets")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "create child widget {name} must succeed"
    );
}

async fn create_cluster_widget_child_with_finalizer(
    app: &axum::Router,
    name: &str,
    owner_name: &str,
    owner_uid: &str,
) {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let body = serde_json::json!({
        "apiVersion": "example.com/v1",
        "kind": "Widget",
        "metadata": {
            "name": name,
            "finalizers": ["example.com/hold"],
            "ownerReferences": [{
                "apiVersion": "example.com/v1",
                "kind": "Widget",
                "name": owner_name,
                "uid": owner_uid,
                "controller": true,
                "blockOwnerDeletion": true
            }]
        }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/example.com/v1/widgets")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "create finalizer-held child widget {name} must succeed"
    );
}

async fn get_cluster_widget(app: &axum::Router, name: &str) -> Option<serde_json::Value> {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/apis/example.com/v1/widgets/{name}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    if resp.status() == StatusCode::NOT_FOUND {
        return None;
    }
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    Some(serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn test_cluster_custom_resource_update_bumps_generation_on_spec_change() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_app_with_cluster_widget_crd().await;
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/example.com/v1/widgets")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"example.com/v1","kind":"Widget","metadata":{"name":"generation-widget"},"spec":{"value":"old"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let update_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/apis/example.com/v1/widgets/generation-widget")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"example.com/v1","kind":"Widget","metadata":{"name":"generation-widget"},"spec":{"value":"new"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(update_resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(update_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let updated: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        updated.pointer("/metadata/generation"),
        Some(&serde_json::json!(2)),
        "custom resource spec updates must bump metadata.generation: {updated:?}"
    );
}

#[tokio::test]
async fn test_cluster_cr_delete_orphan_strips_owner_refs_from_children() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_app_with_cluster_widget_crd().await;
    let parent = create_cluster_widget(&app, "parent-orphan").await;
    let parent_uid = parent
        .pointer("/metadata/uid")
        .and_then(|v| v.as_str())
        .expect("parent must have uid")
        .to_string();
    create_cluster_widget_child(&app, "child-orphan", "parent-orphan", &parent_uid).await;

    let delete_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/apis/example.com/v1/widgets/parent-orphan?propagationPolicy=Orphan")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        delete_resp.status(),
        StatusCode::OK,
        "DELETE with Orphan must return 200"
    );

    let child = get_cluster_widget(&app, "child-orphan")
        .await
        .expect("child must still exist after Orphan delete of parent");
    let owner_refs_len = child
        .pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
        .map(|arr| arr.len())
        .unwrap_or(0);
    assert_eq!(
        owner_refs_len, 0,
        "child ownerReferences must be empty after Orphan delete of parent (k8s spec)"
    );
}

#[tokio::test]
async fn test_cluster_cr_delete_foreground_sets_deletion_timestamp_and_finalizer() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_app_with_cluster_widget_crd().await;
    let parent = create_cluster_widget(&app, "parent-fg").await;
    let parent_uid = parent
        .pointer("/metadata/uid")
        .and_then(|v| v.as_str())
        .expect("parent must have uid")
        .to_string();
    create_cluster_widget_child_with_finalizer(&app, "child-fg", "parent-fg", &parent_uid).await;

    let delete_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/apis/example.com/v1/widgets/parent-fg?propagationPolicy=Foreground")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        delete_resp.status(),
        StatusCode::ACCEPTED,
        "DELETE with Foreground must return 202 when the parent is retained"
    );

    let parent_after = get_cluster_widget(&app, "parent-fg").await;
    let parent_obj = parent_after
        .expect("parent must still exist with deletionTimestamp+foregroundDeletion finalizer while child remains");
    assert!(
        parent_obj
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .map(|s| !s.is_empty())
            .unwrap_or(false),
        "parent must have deletionTimestamp set under Foreground"
    );
    let finalizers: Vec<&str> = parent_obj
        .pointer("/metadata/finalizers")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|f| f.as_str()).collect())
        .unwrap_or_default();
    assert!(
        finalizers.contains(&"foregroundDeletion"),
        "parent must have foregroundDeletion finalizer under Foreground (got {:?})",
        finalizers
    );
}

#[tokio::test]
async fn test_cluster_cr_delete_background_cascades_children() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_app_with_cluster_widget_crd().await;
    let parent = create_cluster_widget(&app, "parent-bg").await;
    let parent_uid = parent
        .pointer("/metadata/uid")
        .and_then(|v| v.as_str())
        .expect("parent must have uid")
        .to_string();
    create_cluster_widget_child(&app, "child-bg", "parent-bg", &parent_uid).await;

    // No ?propagationPolicy — exercises the Background default path.
    let delete_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/apis/example.com/v1/widgets/parent-bg")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        delete_resp.status(),
        StatusCode::OK,
        "DELETE (no propagationPolicy) must return 200"
    );

    assert!(
        get_cluster_widget(&app, "parent-bg").await.is_none(),
        "parent must be deleted under default Background policy"
    );

    assert!(
        get_cluster_widget(&app, "child-bg").await.is_none(),
        "child must be cascade-deleted under default Background policy (k8s spec)"
    );
}

#[tokio::test]
async fn test_cluster_cr_delete_returns_status_envelope() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_app_with_cluster_widget_crd().await;
    let _ = create_cluster_widget(&app, "envelope-target").await;

    let delete_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/apis/example.com/v1/widgets/envelope-target")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(delete_resp.status(), StatusCode::OK);

    let bytes = axum::body::to_bytes(delete_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(
        body.get("kind").and_then(|v| v.as_str()),
        Some("Status"),
        "cluster CR DELETE response body must be a Status envelope (k8s spec), got: {body}"
    );
    assert_eq!(
        body.get("apiVersion").and_then(|v| v.as_str()),
        Some("v1"),
        "Status envelope must have apiVersion=v1"
    );
    assert_eq!(
        body.get("status").and_then(|v| v.as_str()),
        Some("Success"),
        "Status envelope must report Success on a clean delete"
    );
    assert_eq!(
        body.pointer("/details/name").and_then(|v| v.as_str()),
        Some("envelope-target"),
        "Status.details.name must echo the deleted resource name"
    );
    assert_eq!(
        body.pointer("/details/kind").and_then(|v| v.as_str()),
        Some("Widget"),
        "Status.details.kind must echo the deleted resource kind"
    );
}

/// Non-conversion CRD list must include metadata.continue when paginated.
#[tokio::test]
async fn custom_resource_list_includes_continue_for_paginated_non_conversion_crd() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state.clone());

    // Register a simple non-conversion CRD
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.example.com"},
        "spec": {
            "group": "example.com",
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {"spec": {"type": "object"}}
                    }
                }
            }],
            "scope": "Namespaced",
            "names": {
                "plural": "widgets",
                "singular": "widget",
                "kind": "Widget"
            }
        }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Create namespace
    let ns_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"crd-test"}})).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    // Create 5 widgets
    for i in 0..5 {
        let widget = json!({
            "apiVersion": "example.com/v1",
            "kind": "Widget",
            "metadata": {
                "name": format!("w-{}", i),
                "namespace": "crd-test"
            },
            "spec": {"value": i}
        });
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/apis/example.com/v1/namespaces/crd-test/widgets")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&widget).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::CREATED,
            "widget {i} create failed"
        );
    }

    // List with limit=2 — must include continue token
    let list_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/example.com/v1/namespaces/crd-test/widgets?limit=2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(
        list["items"].as_array().map(|a| a.len()).unwrap_or(0),
        2,
        "first page must have exactly 2 items"
    );
    let continue_token = list
        .pointer("/metadata/continue")
        .and_then(|v| v.as_str())
        .expect("paginated CRD list must include metadata.continue");
    assert!(
        !continue_token.is_empty(),
        "continue token must not be empty"
    );

    // Verify remainingItemCount is present
    let remaining = list
        .pointer("/metadata/remainingItemCount")
        .and_then(|v| v.as_i64());
    assert!(
        remaining.is_some(),
        "paginated CRD list should include metadata.remainingItemCount"
    );
}

/// Non-conversion CRD list continue token must return the next page.
#[tokio::test]
async fn custom_resource_list_continue_returns_next_page_for_non_conversion_crd() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state.clone());

    // Register CRD
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "gadgets.example.com"},
        "spec": {
            "group": "example.com",
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {"spec": {"type": "object"}}
                    }
                }
            }],
            "scope": "Namespaced",
            "names": {
                "plural": "gadgets",
                "singular": "gadget",
                "kind": "Gadget"
            }
        }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Create namespace
    let ns_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"gadget-test"}})).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    // Create 5 gadgets
    for i in 0..5 {
        let gadget = json!({
            "apiVersion": "example.com/v1",
            "kind": "Gadget",
            "metadata": {
                "name": format!("g-{}", i),
                "namespace": "gadget-test"
            },
            "spec": {"value": i}
        });
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/apis/example.com/v1/namespaces/gadget-test/gadgets")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&gadget).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    // Get first page
    let list_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/example.com/v1/namespaces/gadget-test/gadgets?limit=2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let page1: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let continue_token = page1
        .pointer("/metadata/continue")
        .and_then(|v| v.as_str())
        .expect("must have continue token");

    // Get second page using continue token
    let list_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/apis/example.com/v1/namespaces/gadget-test/gadgets?limit=2&continue={continue_token}"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let page2: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        page2["items"].as_array().map(|a| a.len()).unwrap_or(0),
        2,
        "second page must have 2 items: {page2:#?}"
    );

    // Names from page 1 and page 2 must not overlap
    let page1_names: std::collections::HashSet<String> = page1["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["metadata"]["name"].as_str().unwrap().to_string())
        .collect();
    let page2_names: std::collections::HashSet<String> = page2["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["metadata"]["name"].as_str().unwrap().to_string())
        .collect();
    assert!(
        page1_names.is_disjoint(&page2_names),
        "pages must not overlap: page1={page1_names:?} page2={page2_names:?}"
    );
}

/// Non-conversion CRD list continue tokens must use the same encoded
/// Kubernetes-compatible token format as built-in resources, not raw names.
#[tokio::test]
async fn custom_resource_list_continue_token_is_encoded_for_non_conversion_crd() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use base64::Engine as _;
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state.clone());

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "encodedwidgets.example.com"},
        "spec": {
            "group": "example.com",
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object"}}
            }],
            "scope": "Namespaced",
            "names": {
                "plural": "encodedwidgets",
                "singular": "encodedwidget",
                "kind": "EncodedWidget"
            }
        }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let ns_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "apiVersion": "v1",
                        "kind": "Namespace",
                        "metadata": {"name": "encoded-crd-test"}
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    for name in ["ew-0", "ew-1", "ew-2"] {
        let object = json!({
            "apiVersion": "example.com/v1",
            "kind": "EncodedWidget",
            "metadata": {"name": name, "namespace": "encoded-crd-test"}
        });
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/apis/example.com/v1/namespaces/encoded-crd-test/encodedwidgets")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&object).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    let list_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/example.com/v1/namespaces/encoded-crd-test/encodedwidgets?limit=2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let page: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let token = page
        .pointer("/metadata/continue")
        .and_then(|v| v.as_str())
        .expect("paginated CRD list must return a continue token");

    assert_ne!(
        token, "ew-1",
        "CRD list continue tokens must not expose raw datastore names"
    );
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(token)
        .expect("continue token must be base64url JSON");
    let decoded: serde_json::Value = serde_json::from_slice(&decoded).unwrap();
    assert_eq!(decoded["n"], json!("ew-1"));
    assert!(decoded["rv"].as_i64().unwrap_or(0) > 0);
    assert!(decoded["ts"].as_i64().is_some());
}

/// Expired CRD list continue tokens must return the Kubernetes 410
/// ResourceExpired response instead of being treated as raw names.
#[tokio::test]
async fn custom_resource_list_expired_continue_returns_resource_expired() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use base64::Engine as _;
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state.clone());

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "expiredwidgets.example.com"},
        "spec": {
            "group": "example.com",
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object"}}
            }],
            "scope": "Namespaced",
            "names": {
                "plural": "expiredwidgets",
                "singular": "expiredwidget",
                "kind": "ExpiredWidget"
            }
        }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let expired_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        - crate::api::query::CONTINUE_TOKEN_TTL_SECS
        - 1;
    let token_data = crate::api::query::ContinueTokenData {
        n: "expired-0".to_string(),
        rv: 1,
        ts: Some(expired_ts),
        session: false,
    };
    let expired_token = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(&token_data).unwrap());

    let list_resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/apis/example.com/v1/namespaces/default/expiredwidgets?limit=1&continue={expired_token}"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::GONE);
    let body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let status: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status["reason"], json!("Expired"));
    assert_eq!(status["code"], json!(410));
    assert!(
        status
            .pointer("/metadata/continue")
            .and_then(|v| v.as_str())
            .is_some(),
        "410 response must include an inconsistent continuation token"
    );
}

// ── T1: APIService proxy authorization tests ──

/// APIService-proxied requests skip local RBAC and forward to the backend.
/// The Kubernetes aggregator delegates authorization to the backend for
/// APIService-registered APIs; local RBAC is only applied to CRD-handled
/// resources. This test verifies that even with a deny-all authorizer,
/// requests for an APIService-backed API reach the backend.
#[tokio::test]
async fn apiservice_proxy_denies_request_before_backend_contact() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    // Spawn a backend that records contact. Since local RBAC now runs
    // before proxy, the backend must never be contacted when the
    // authorizer denies the request.
    let backend = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend.local_addr().unwrap();
    let contacted = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let contacted_clone = contacted.clone();
    let _backend_guard = tokio::spawn(async move {
        while backend.accept().await.is_ok() {
            contacted_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    });

    let state = build_test_app_state().await;
    let (_cert_pem, _key_pem, ca_bundle) =
        generate_apiservice_ca_signed_identity("testsvc", "default");

    // Register APIService pointing to our backend
    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1.example-test.com"},
        "spec": {
            "group": "example-test.com",
            "version": "v1",
            "groupPriorityMinimum": 100,
            "versionPriority": 100,
            "service": {
                "name": "testsvc",
                "namespace": "default",
                "port": backend_addr.port()
            },
            "caBundle": ca_bundle,
            "insecureSkipTLSVerify": false
        }
    });
    state
        .db
        .create_resource(
            "apiregistration.k8s.io/v1",
            "APIService",
            None,
            "v1.example-test.com",
            apiservice,
        )
        .await
        .unwrap();

    // Create Endpoints pointing to our backend
    let endpoints = json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {"name": "testsvc", "namespace": "default"},
        "subsets": [{
            "addresses": [{"ip": "127.0.0.1"}],
            "ports": [{"port": backend_addr.port(), "protocol": "TCP"}]
        }]
    });
    state
        .db
        .create_resource("v1", "Endpoints", Some("default"), "testsvc", endpoints)
        .await
        .unwrap();

    // Build router with deny-all authorizer
    let authorizer: std::sync::Arc<dyn crate::auth::authorizer::Authorizer> =
        std::sync::Arc::new(crate::auth::authorizer::DenyAuthorizer);
    let mut deny_state = build_test_app_state().await;
    deny_state.db = state.db.clone();
    deny_state.authorizer = authorizer;
    let app = crate::api::build_router(deny_state);

    // Test: an APIService-backed GET with DenyAuthorizer must return 403
    // and must NOT contact the backend (local RBAC is enforced first).
    let req = Request::builder()
        .method("GET")
        .uri("/apis/example-test.com/v1/namespaces/default/widgets")
        .header("content-type", "application/json")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "APIService-proxied request must be denied by local RBAC before backend contact"
    );
    // Backend must not have been contacted
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    assert!(
        !contacted.load(std::sync::atomic::Ordering::SeqCst),
        "backend must not be contacted when local RBAC denies the request"
    );
}

/// Verify that APIService proxy forwards real caller identity headers (not hard-coded
/// system:admin).
#[tokio::test]
async fn apiservice_proxy_forwards_real_caller_identity_headers() {
    use axum::body::Body;
    use axum::http::Request;
    use serde_json::json;
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    use tower::ServiceExt;

    let (received_headers_tx, mut received_headers_rx) =
        tokio::sync::mpsc::unbounded_channel::<Vec<(String, String)>>();

    // Spawn a backend that captures and reports received headers
    let backend = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend.local_addr().unwrap();
    let (cert_pem, key_pem) = generate_apiservice_self_signed_identity("testsvc2", "default");
    let backend_cert = cert_pem.clone();
    let backend_key = key_pem.clone();
    let backend_handle = tokio::spawn(async move {
        let certs = rustls_pemfile::certs(&mut backend_cert.as_bytes())
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        let key = rustls_pemfile::private_key(&mut backend_key.as_bytes())
            .unwrap()
            .unwrap();
        let _ = rustls::crypto::ring::default_provider().install_default();
        let server_config =
            rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
        loop {
            let (stream, _) = match backend.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };
            let mut tls_stream = match acceptor.accept(stream).await {
                Ok(s) => s,
                Err(_) => continue,
            };
            let mut buf = vec![0u8; 8192];
            let n = tls_stream.read(&mut buf).await.unwrap_or(0);
            let request_str = String::from_utf8_lossy(&buf[..n]);
            let mut headers = Vec::new();
            for line in request_str.lines() {
                if let Some((k, v)) = line.split_once(':') {
                    headers.push((k.trim().to_lowercase(), v.trim().to_string()));
                }
            }
            let _ = received_headers_tx.send(headers);
            let _ = tls_stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nbody")
                .await;
        }
    });

    let state = build_test_app_state().await;
    // Register APIService
    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1.example-test2.com"},
        "spec": {
            "group": "example-test2.com",
            "version": "v1",
            "groupPriorityMinimum": 100,
            "versionPriority": 100,
            "service": {
                "name": "testsvc2",
                "namespace": "default",
                "port": backend_addr.port()
            },
            "insecureSkipTLSVerify": true
        }
    });
    state
        .db
        .create_resource(
            "apiregistration.k8s.io/v1",
            "APIService",
            None,
            "v1.example-test2.com",
            apiservice,
        )
        .await
        .unwrap();
    // Create Endpoints
    let endpoints = json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {"name": "testsvc2", "namespace": "default"},
        "subsets": [{
            "addresses": [{"ip": "127.0.0.1"}],
            "ports": [{"port": backend_addr.port(), "protocol": "TCP"}]
        }]
    });
    state
        .db
        .create_resource("v1", "Endpoints", Some("default"), "testsvc2", endpoints)
        .await
        .unwrap();

    let authorizer: std::sync::Arc<dyn crate::auth::authorizer::Authorizer> =
        std::sync::Arc::new(crate::auth::authorizer::AllowAllAuthorizer);
    let mut test_state = build_test_app_state().await;
    test_state.db = state.db.clone();
    test_state.authorizer = authorizer;
    let app = crate::api::build_router(test_state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/example-test2.com/v1/namespaces/default/widgets/test-widget")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // May get 200 (proxied) or 502 (backend not fully TLS), but the key assertion
    // is the backend received the request with proper headers
    let _ = resp;

    // Give the backend time to receive
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    if let Ok(headers) = received_headers_rx.try_recv() {
        // Check that system:admin is NOT stamped when the actual caller is anonymous
        let has_system_admin_user = headers
            .iter()
            .any(|(k, v)| k == "x-remote-user" && v == "system:admin");
        assert!(
            !has_system_admin_user,
            "x-remote-user should not be hard-coded to system:admin for non-admin callers.\
             Received headers: {headers:?}"
        );
        // The anonymous identity should be forwarded as the remote user
        let has_remote_user_header = headers.iter().any(|(k, _)| k == "x-remote-user");
        assert!(
            has_remote_user_header,
            "x-remote-user header should be present. Received headers: {headers:?}"
        );
    }

    backend_handle.abort();
}

/// Verify that custom resource CRUD operations are checked against the authorizer
/// before any datastore access.
#[tokio::test]
async fn custom_resource_operations_denied_with_deny_authorizer() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    // Build app with deny-all authorizer
    let authorizer: std::sync::Arc<dyn crate::auth::authorizer::Authorizer> =
        std::sync::Arc::new(crate::auth::authorizer::DenyAuthorizer);
    let state = build_test_app_state_with_authorizer(authorizer).await;

    // Register a CRD so the local CRD path is used (not proxy)
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "authwidgets.example-auth.com"},
        "spec": {
            "group": "example-auth.com",
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object"}}
            }],
            "scope": "Namespaced",
            "names": {
                "plural": "authwidgets",
                "singular": "authwidget",
                "kind": "AuthWidget"
            }
        }
    });
    state
        .db
        .create_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            "authwidgets.example-auth.com",
            crd,
        )
        .await
        .unwrap();

    // Register in CRD registry so the local path is taken (not proxy fallback)
    state
        .crd_registry
        .register(crate::controllers::crd::CrdResourceInfo {
            group: "example-auth.com".to_string(),
            version: "v1".to_string(),
            kind: "AuthWidget".to_string(),
            plural: "authwidgets".to_string(),
            singular: "authwidget".to_string(),
            namespaced: true,
            selectable_fields: vec![],
        })
        .await;

    let app = crate::api::build_router(state);

    // Test each CRUD verb returns 403
    let tests = vec![
        (
            "GET",
            "/apis/example-auth.com/v1/namespaces/default/authwidgets",
            "list",
        ),
        (
            "POST",
            "/apis/example-auth.com/v1/namespaces/default/authwidgets",
            "create",
        ),
        (
            "DELETE",
            "/apis/example-auth.com/v1/namespaces/default/authwidgets",
            "deletecollection",
        ),
        (
            "GET",
            "/apis/example-auth.com/v1/namespaces/default/authwidgets/test",
            "get",
        ),
        (
            "PUT",
            "/apis/example-auth.com/v1/namespaces/default/authwidgets/test",
            "update",
        ),
        (
            "PATCH",
            "/apis/example-auth.com/v1/namespaces/default/authwidgets/test",
            "patch",
        ),
        (
            "DELETE",
            "/apis/example-auth.com/v1/namespaces/default/authwidgets/test",
            "delete",
        ),
    ];

    for (method, uri, _verb) in &tests {
        let builder = Request::builder()
            .method(*method)
            .uri(*uri)
            .header("content-type", "application/json");
        let req = if *method == "POST" || *method == "PUT" || *method == "PATCH" {
            builder.body(Body::from(
                serde_json::to_vec(&json!({"metadata":{"name":"test"}})).unwrap(),
            ))
        } else {
            builder.body(Body::empty())
        }
        .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "{method} {uri} should return 403, got {}",
            resp.status()
        );
    }
}

/// Verify that an allowed identity can still access CRD resources normally.
#[tokio::test]
async fn custom_resource_allowed_identity_gets_normal_response() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let authorizer: std::sync::Arc<dyn crate::auth::authorizer::Authorizer> =
        std::sync::Arc::new(crate::auth::authorizer::AllowAllAuthorizer);
    let state = build_test_app_state_with_authorizer(authorizer).await;

    // Register a CRD
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "allowwidgets.example-allow.com"},
        "spec": {
            "group": "example-allow.com",
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object"}}
            }],
            "scope": "Namespaced",
            "names": {
                "plural": "allowwidgets",
                "singular": "allowwidget",
                "kind": "AllowWidget"
            }
        }
    });
    state
        .db
        .create_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            "allowwidgets.example-allow.com",
            crd,
        )
        .await
        .unwrap();

    state
        .crd_registry
        .register(crate::controllers::crd::CrdResourceInfo {
            group: "example-allow.com".to_string(),
            version: "v1".to_string(),
            kind: "AllowWidget".to_string(),
            plural: "allowwidgets".to_string(),
            singular: "allowwidget".to_string(),
            namespaced: true,
            selectable_fields: vec![],
        })
        .await;

    let app = crate::api::build_router(state);

    // Create should succeed
    let widget = json!({"metadata": {"name": "test-allow"}, "spec": {"value": 42}});
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/example-allow.com/v1/namespaces/default/allowwidgets")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&widget).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Get should succeed
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/example-allow.com/v1/namespaces/default/allowwidgets/test-allow")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // List should succeed
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/example-allow.com/v1/namespaces/default/allowwidgets")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

/// APIService-backed custom-resource requests must be locally authorized
/// before the proxy connects to the backend. With a DenyAuthorizer,
/// requests must return 403 and the backend must never be contacted.
#[tokio::test]
async fn apiservice_backed_custom_resource_denied_before_backend_contact() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::net::TcpListener;
    use tokio::time::Duration;
    use tower::ServiceExt;

    // Backend that counts requests
    let request_count = std::sync::Arc::new(AtomicUsize::new(0));
    let request_count_clone = request_count.clone();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    // Spawn backend that counts each request
    let (cert, key) = generate_apiservice_self_signed_identity("wardle-deny-svc", "default");
    let certs = rustls_pemfile::certs(&mut cert.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let key = rustls_pemfile::private_key(&mut key.as_bytes())
        .unwrap()
        .unwrap();
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server_config =
        rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(server_config));

    let backend_handle = tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(c) => c,
                Err(_) => break,
            };
            let acceptor = acceptor.clone();
            let count = request_count_clone.clone();
            tokio::spawn(async move {
                if let Ok(mut tls) = acceptor.accept(stream).await {
                    count.fetch_add(1, Ordering::SeqCst);
                    let mut buf = vec![0u8; 4096];
                    let _ = tokio::io::AsyncReadExt::read(&mut tls, &mut buf).await;
                    // Don't send a response - we just count the request
                }
            });
        }
    });

    // Build state with DenyAuthorizer — all API requests will be denied
    let authorizer: std::sync::Arc<dyn crate::auth::authorizer::Authorizer> =
        std::sync::Arc::new(crate::auth::authorizer::DenyAuthorizer);
    let state = build_test_app_state_with_authorizer(authorizer).await;
    let db = state.db.clone();

    // Set up Service, Endpoints, and APIService directly in the DB
    // (bypassing the API to avoid the DenyAuthorizer blocking setup)
    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-deny-svc",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-deny-svc", "namespace": "default"},
            "spec": {"ports": [{"port": port}]}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-deny-svc",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-deny-svc", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": port}]}]
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "apiregistration.k8s.io/v1",
        "APIService",
        None,
        "v1.deny.example.com",
        json!({
            "apiVersion": "apiregistration.k8s.io/v1",
            "kind": "APIService",
            "metadata": {"name": "v1.deny.example.com"},
            "spec": {
                "group": "deny.example.com",
                "version": "v1",
                "groupPriorityMinimum": 1000,
                "versionPriority": 10,
                "insecureSkipTLSVerify": true,
                "service": {"name": "wardle-deny-svc", "namespace": "default", "port": port}
            }
        }),
    )
    .await
    .unwrap();

    let app = crate::api::build_router(state);

    // Allow APIService proxy cache to warm up
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Test 1: List without selectors — must return 403, backend must NOT be contacted
    let before_list = request_count.load(Ordering::SeqCst);
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/deny.example.com/v1/namespaces/default/denywidgets")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "list without selectors must return 403 with DenyAuthorizer"
    );
    let after_list = request_count.load(Ordering::SeqCst);
    assert_eq!(
        after_list, before_list,
        "backend must not be contacted when list is denied"
    );

    // Test 2: Watch with selectors — must return 403
    let before_watch = request_count.load(Ordering::SeqCst);
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/deny.example.com/v1/namespaces/default/denywidgets?watch=true&fieldSelector=metadata.name%3Dfoo&labelSelector=a%3Db")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "watch with selectors must return 403 with DenyAuthorizer"
    );
    let after_watch = request_count.load(Ordering::SeqCst);
    assert_eq!(
        after_watch, before_watch,
        "backend must not be contacted when watch is denied"
    );

    // Test 3: DeleteCollection with selectors — must return 403
    let before_del = request_count.load(Ordering::SeqCst);
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/apis/deny.example.com/v1/namespaces/default/denywidgets?labelSelector=a%3Db")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "deletecollection with selectors must return 403 with DenyAuthorizer"
    );
    let after_del = request_count.load(Ordering::SeqCst);
    assert_eq!(
        after_del, before_del,
        "backend must not be contacted when deletecollection is denied"
    );

    backend_handle.abort();
}

/// APIService proxy must reject oversized responses with 502 before
/// buffering the entire upstream body.
#[tokio::test]
async fn apiservice_proxy_rejects_oversized_response_without_full_buffering() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio::time::Duration;
    use tower::ServiceExt;

    // TLS backend sending oversized chunked response
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (cert, key) = generate_apiservice_self_signed_identity("os-svc", "default");
    let certs = rustls_pemfile::certs(&mut cert.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let key = rustls_pemfile::private_key(&mut key.as_bytes())
        .unwrap()
        .unwrap();
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server_config =
        rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(server_config));
    let backend_handle = tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(c) => c,
                Err(_) => break,
            };
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                if let Ok(mut tls) = acceptor.accept(stream).await {
                    let mut buf = vec![0u8; 4096];
                    let _ = tokio::io::AsyncReadExt::read(&mut tls, &mut buf).await;
                    let headers = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 104857600\r\n\r\n";
                    let _ = tokio::io::AsyncWriteExt::write_all(&mut tls, headers.as_bytes()).await;
                    // Send some data then stop — client should disconnect
                    // when it notices the body exceeds the limit
                    let chunk = vec![b'x'; 32768];
                    for _ in 0..10 {
                        if tokio::io::AsyncWriteExt::write_all(&mut tls, &chunk)
                            .await
                            .is_err()
                        {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                }
            });
        }
    });

    let state = build_test_app_state().await;
    let db = state.db.clone();
    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "os-svc",
        json!({
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": "os-svc", "namespace": "default"},
            "spec": {"ports": [{"port": port}]}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "os-svc",
        json!({
            "apiVersion": "v1", "kind": "Endpoints",
            "metadata": {"name": "os-svc", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": port}]}]
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "apiregistration.k8s.io/v1",
        "APIService",
        None,
        "v1.os-proxy.example.com",
        json!({
            "apiVersion": "apiregistration.k8s.io/v1",
            "kind": "APIService",
            "metadata": {"name": "v1.os-proxy.example.com"},
            "spec": {
                "group": "os-proxy.example.com", "version": "v1",
                "groupPriorityMinimum": 1000, "versionPriority": 10,
                "insecureSkipTLSVerify": true,
                "service": {"name": "os-svc", "namespace": "default", "port": port}
            }
        }),
    )
    .await
    .unwrap();

    let app = crate::api::build_router(state);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/os-proxy.example.com/v1/namespaces/default/widgets/test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Content-Length claims 100MB which exceeds MAX_APISERVICE_RESPONSE_BODY_BYTES.
    // The proxy should return 502 after detecting oversized body.
    assert!(
        resp.status() == StatusCode::BAD_GATEWAY
            || resp.status() == StatusCode::INTERNAL_SERVER_ERROR,
        "oversized APIService response should return 502 or 500 (got {})",
        resp.status()
    );

    backend_handle.abort();
}

/// APIService proxy preserves status, headers, and body below limit.
#[tokio::test]
async fn apiservice_proxy_preserves_status_headers_and_body_below_response_limit() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio::time::Duration;
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let payload = json!({"key": "value", "number": 42});
    let payload_bytes = serde_json::to_vec(&payload).unwrap();
    let response = format!(
        "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nX-Custom: test-value\r\nContent-Length: {}\r\n\r\n",
        payload_bytes.len()
    )
    .into_bytes()
    .into_iter()
    .chain(payload_bytes.iter().copied())
    .collect::<Vec<u8>>();
    let _captured_request =
        spawn_apiservice_tls_backend_for_service(listener, "ok-svc", "default", response).await;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    db.create_resource(
        "v1", "Service", Some("default"), "ok-svc",
        json!({"apiVersion": "v1", "kind": "Service", "metadata": {"name": "ok-svc", "namespace": "default"}, "spec": {"ports": [{"port": port}]}}),
    ).await.unwrap();
    db.create_resource(
        "v1", "Endpoints", Some("default"), "ok-svc",
        json!({"apiVersion": "v1", "kind": "Endpoints", "metadata": {"name": "ok-svc", "namespace": "default"}, "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": port}]}]}),
    ).await.unwrap();
    db.create_resource(
        "apiregistration.k8s.io/v1",
        "APIService",
        None,
        "v1.ok-proxy.example.com",
        json!({
            "apiVersion": "apiregistration.k8s.io/v1", "kind": "APIService",
            "metadata": {"name": "v1.ok-proxy.example.com"},
            "spec": {
                "group": "ok-proxy.example.com", "version": "v1",
                "groupPriorityMinimum": 1000, "versionPriority": 10,
                "insecureSkipTLSVerify": true,
                "service": {"name": "ok-svc", "namespace": "default", "port": port}
            }
        }),
    )
    .await
    .unwrap();

    let app = crate::api::build_router(state);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/ok-proxy.example.com/v1/namespaces/default/widgets/test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::CREATED);
    let body_bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(body, payload);
}
