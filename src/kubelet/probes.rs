use anyhow::{Context, Result};
use serde_json::Value;
use std::{future::Future, pin::Pin, time::Duration};

/// Parse a port value that may be an integer or a numeric string (IntOrString)
fn parse_port(port: Option<&Value>) -> Option<u16> {
    parse_port_with_container(port, None)
}

fn parse_port_with_container(port: Option<&Value>, container: Option<&Value>) -> Option<u16> {
    let p = port?;
    // Try as integer first
    if let Some(n) = p.as_u64() {
        return u16::try_from(n).ok();
    }
    // Try as numeric string (e.g., "8080")
    if let Some(s) = p.as_str() {
        if let Ok(n) = s.parse::<u16>() {
            return Some(n);
        }
        if let Some(container) = container {
            return resolve_named_container_port(container, s);
        }
    }
    None
}

fn resolve_named_container_port(container: &Value, name: &str) -> Option<u16> {
    container
        .get("ports")
        .and_then(|ports| ports.as_array())
        .and_then(|ports| {
            ports.iter().find_map(|port| {
                if port.get("name").and_then(|v| v.as_str()) != Some(name) {
                    return None;
                }
                port.get("containerPort")
                    .and_then(|v| v.as_u64())
                    .and_then(|port| u16::try_from(port).ok())
            })
        })
}

/// HTTP probe configuration
#[derive(Debug, Clone)]
pub struct HttpProbe {
    pub path: String,
    pub port: u16,
    pub scheme: String, // "HTTP" or "HTTPS"
}

/// TCP probe configuration
#[derive(Debug, Clone)]
pub struct TcpProbe {
    pub port: u16,
}

/// Exec probe configuration
#[derive(Debug, Clone)]
pub struct ExecProbe {
    pub command: Vec<String>,
}

/// gRPC probe configuration
#[derive(Debug, Clone)]
pub struct GrpcProbe {
    pub port: u16,
    pub service: String,
}

/// Probe types
#[derive(Debug, Clone)]
pub enum Probe {
    Http(HttpProbe),
    Tcp(TcpProbe),
    Exec(ExecProbe),
    Grpc(GrpcProbe),
}

/// Parse probe from K8s JSON spec
#[cfg(test)]
pub fn parse_probe(probe_spec: &Value) -> Result<Probe> {
    parse_probe_with_container(probe_spec, None)
}

/// Parse probe from K8s JSON spec, resolving HTTP/TCP named ports against the
/// owning container's `ports` list as required by IntOrString probe ports.
pub fn parse_probe_for_container(probe_spec: &Value, container: &Value) -> Result<Probe> {
    parse_probe_with_container(probe_spec, Some(container))
}

fn parse_probe_with_container(probe_spec: &Value, container: Option<&Value>) -> Result<Probe> {
    if let Some(http_get) = probe_spec.get("httpGet") {
        let path = http_get
            .get("path")
            .and_then(|p| p.as_str())
            .unwrap_or("/")
            .to_string();
        let port = parse_port_with_container(http_get.get("port"), container)
            .context("Missing or invalid port in httpGet probe")?;
        let scheme = http_get
            .get("scheme")
            .and_then(|s| s.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or("HTTP")
            .to_string();
        return Ok(Probe::Http(HttpProbe { path, port, scheme }));
    }

    if let Some(tcp_socket) = probe_spec.get("tcpSocket") {
        let port = parse_port_with_container(tcp_socket.get("port"), container)
            .context("Missing or invalid port in tcpSocket probe")?;
        return Ok(Probe::Tcp(TcpProbe { port }));
    }

    if let Some(exec) = probe_spec.get("exec") {
        let command = exec
            .get("command")
            .and_then(|c| c.as_array())
            .context("Missing command in exec probe")?
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        return Ok(Probe::Exec(ExecProbe { command }));
    }

    // gRPC probes: send grpc.health.v1.Health/Check RPC
    if let Some(grpc) = probe_spec.get("grpc") {
        let port = parse_port(grpc.get("port")).context("Missing or invalid port in grpc probe")?;
        let service = grpc
            .get("service")
            .and_then(|s| s.as_str())
            .unwrap_or("")
            .to_string();
        return Ok(Probe::Grpc(GrpcProbe { port, service }));
    }

    anyhow::bail!("Probe must have httpGet, tcpSocket, exec, or grpc field")
}

/// Build a shared reqwest::Client for HTTP probes.
/// Reusing a single client avoids rebuilding the OpenSSL context
/// (SSL_CTX_load_verify_locations) on every probe call, which dominates
/// CPU when probes run at high frequency (e.g. periodSeconds=1).
pub fn build_probe_http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .no_proxy()
        .danger_accept_invalid_certs(true)
        .build()
        .context("Failed to build probe HTTP client")
}

/// Check HTTP probe using a shared client. Timeout is applied per-request.
pub async fn check_http_probe(
    client: &reqwest::Client,
    pod_ip: &str,
    probe: &HttpProbe,
    timeout: Duration,
) -> Result<bool> {
    let url = format!(
        "{}://{}:{}{}",
        probe.scheme.to_lowercase(),
        pod_ip,
        probe.port,
        probe.path
    );

    let response = client.get(&url).timeout(timeout).send().await?;
    Ok(response.status().is_success())
}

/// Check TCP probe
pub async fn check_tcp_probe(
    pod_ip: &str,
    probe: &TcpProbe,
    timeout: Duration,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) -> Result<bool> {
    let addr = format!("{}:{}", pod_ip, probe.port);
    match task_supervisor
        .timeout(
            "tcp_probe_connect_timeout",
            timeout,
            tokio::net::TcpStream::connect(&addr),
        )
        .await
    {
        Ok(Ok(Ok(_))) => Ok(true),
        Ok(Ok(Err(_))) => Ok(false),
        Ok(Err(_)) => Ok(false), // Timeout
        Err(e) => Err(anyhow::anyhow!("tcp probe cancelled: {e}")),
    }
}

/// Check gRPC health probe by sending a grpc.health.v1.Health/Check RPC.
/// Returns true only if the response status is SERVING (1).
pub async fn check_grpc_probe(
    pod_ip: &str,
    probe: &GrpcProbe,
    timeout: Duration,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) -> Result<bool> {
    const MAX_GRPC_PROBE_ATTEMPTS: usize = 3;

    for attempt in 0..MAX_GRPC_PROBE_ATTEMPTS {
        if check_grpc_probe_once(pod_ip, probe, timeout, task_supervisor).await? {
            return Ok(true);
        }

        if attempt + 1 < MAX_GRPC_PROBE_ATTEMPTS {
            tracing::debug!(
                "gRPC probe attempt {} failed for {}:{}, retrying",
                attempt + 1,
                pod_ip,
                probe.port
            );
        }
    }

    Ok(false)
}

async fn drive_h2_connection_until<C, F, T>(connection: &mut Pin<Box<C>>, future: F) -> Option<T>
where
    C: Future<Output = Result<(), h2::Error>>,
    F: Future<Output = T>,
{
    let mut future = std::pin::pin!(future);
    tokio::select! {
        result = &mut future => Some(result),
        connection_result = connection.as_mut() => {
            if let Err(err) = connection_result {
                tracing::debug!("gRPC probe h2 connection ended before probe operation completed: {err}");
            }
            None
        }
    }
}

async fn check_grpc_probe_once(
    pod_ip: &str,
    probe: &GrpcProbe,
    timeout: Duration,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) -> Result<bool> {
    let addr = format!("{}:{}", pod_ip, probe.port);
    let tcp = match task_supervisor
        .timeout(
            "grpc_probe_tcp_connect_timeout",
            timeout,
            tokio::net::TcpStream::connect(&addr),
        )
        .await
    {
        Ok(Ok(Ok(stream))) => stream,
        Ok(Ok(Err(_))) | Ok(Err(_)) => return Ok(false),
        Err(e) => return Err(anyhow::anyhow!("grpc probe tcp connect cancelled: {e}")),
    };

    // Build the protobuf-encoded HealthCheckRequest
    // Field 1 (service): string, wire type 2 (length-delimited)
    let request_body = if probe.service.is_empty() {
        vec![] // Empty message — check overall server health
    } else {
        // field 1, wire type 2 = tag byte 0x0a, then varint length, then bytes
        let mut buf = vec![0x0a, probe.service.len() as u8];
        buf.extend_from_slice(probe.service.as_bytes());
        buf
    };

    // gRPC frame: 1 byte compressed (0) + 4 bytes big-endian length + message
    let mut grpc_frame = vec![0u8]; // not compressed
    grpc_frame.extend_from_slice(&(request_body.len() as u32).to_be_bytes());
    grpc_frame.extend_from_slice(&request_body);

    // Perform HTTP/2 handshake
    let (h2_client, h2_conn) = match task_supervisor
        .timeout(
            "grpc_probe_h2_handshake_timeout",
            timeout,
            h2::client::handshake(tcp),
        )
        .await
    {
        Ok(Ok(Ok(pair))) => pair,
        Ok(Ok(Err(_))) | Ok(Err(_)) => return Ok(false),
        Err(e) => return Err(anyhow::anyhow!("grpc probe h2 handshake cancelled: {e}")),
    };

    let mut h2_conn = Box::pin(h2_conn);

    let mut client = match task_supervisor
        .timeout(
            "grpc_probe_h2_ready_timeout",
            timeout,
            drive_h2_connection_until(&mut h2_conn, h2_client.ready()),
        )
        .await
    {
        Ok(Ok(Some(Ok(client)))) => client,
        Ok(Ok(Some(Err(e)))) => return Err(anyhow::anyhow!("h2 ready: {}", e)),
        Ok(Ok(None)) | Ok(Err(_)) => return Ok(false),
        Err(e) => return Err(anyhow::anyhow!("grpc probe h2 ready cancelled: {e}")),
    };

    // Build the request
    let request = hyper::Request::builder()
        .method("POST")
        .uri(format!(
            "http://{}:{}/grpc.health.v1.Health/Check",
            pod_ip, probe.port
        ))
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .body(())
        .map_err(|e| anyhow::anyhow!("build request: {}", e))?;

    let (response, mut send_stream) = client
        .send_request(request, false)
        .map_err(|e| anyhow::anyhow!("send request: {}", e))?;

    // Send the gRPC frame as the request body
    send_stream
        .send_data(grpc_frame.into(), true)
        .map_err(|e| anyhow::anyhow!("send data: {}", e))?;

    // Read the response while continuously driving the h2 connection on this
    // probe task. If the connection driver waits behind unrelated supervised
    // work, otherwise healthy gRPC probes can time out under parallel e2e load.
    let resp = match task_supervisor
        .timeout(
            "grpc_probe_response_headers_timeout",
            timeout,
            drive_h2_connection_until(&mut h2_conn, response),
        )
        .await
    {
        Ok(Ok(Some(Ok(resp)))) => resp,
        Ok(Ok(Some(Err(_)))) | Ok(Ok(None)) | Ok(Err(_)) => return Ok(false),
        Err(e) => {
            return Err(anyhow::anyhow!(
                "grpc probe response headers cancelled: {e}"
            ));
        }
    };

    if !resp.status().is_success() {
        return Ok(false);
    }

    // Read response body
    let mut body = resp.into_body();
    let mut response_data = Vec::new();
    loop {
        let chunk = match task_supervisor
            .timeout(
                "grpc_probe_response_body_timeout",
                timeout,
                drive_h2_connection_until(&mut h2_conn, body.data()),
            )
            .await
        {
            Ok(Ok(Some(chunk))) => chunk,
            Ok(Ok(None)) | Ok(Err(_)) => return Ok(false),
            Err(e) => return Err(anyhow::anyhow!("grpc probe response body cancelled: {e}")),
        };

        match chunk {
            Some(Ok(data)) => {
                let _ = body.flow_control().release_capacity(data.len());
                response_data.extend_from_slice(&data);
            }
            Some(Err(_)) => return Ok(false),
            None => break,
        }
    }

    // Parse gRPC frame: skip 5-byte header (1 compressed + 4 length)
    if response_data.len() < 5 {
        return Ok(false);
    }
    let message = &response_data[5..];

    // Parse HealthCheckResponse protobuf: field 1 (status) is a varint enum
    // SERVING = 1. If message is empty or status != 1, probe fails.
    if message.is_empty() {
        // Empty response = UNKNOWN (0) = not serving
        return Ok(false);
    }

    // Simple protobuf parsing: field 1, wire type 0 (varint)
    // Tag byte: (field_number << 3) | wire_type = (1 << 3) | 0 = 0x08
    if message.len() >= 2 && message[0] == 0x08 {
        let status = message[1]; // varint for small values is just one byte
        return Ok(status == 1); // SERVING = 1
    }

    Ok(false)
}

/// Check exec probe via CRI ExecSync
pub async fn check_exec_probe(
    cri: &dyn crate::kubelet::pod_runtime::cri::CriRuntime,
    container_id: &str,
    probe: &ExecProbe,
    timeout: u64,
) -> Result<bool> {
    let response = cri
        .exec_sync(container_id, &probe.command, timeout as i64)
        .await?;
    Ok(response.exit_code == 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use hyper::http::HeaderMap;
    use serde_json::json;
    use std::sync::{LazyLock, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    static PROXY_ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    struct EnvVarRestore {
        key: &'static str,
        value: Option<String>,
    }

    impl EnvVarRestore {
        fn set(key: &'static str, value: Option<&str>) -> Self {
            let previous = std::env::var(key).ok();
            match value {
                // TODO: Audit that the environment access only happens in single-threaded code.
                Some(v) => unsafe { std::env::set_var(key, v) },
                // TODO: Audit that the environment access only happens in single-threaded code.
                None => unsafe { std::env::remove_var(key) },
            }
            Self {
                key,
                value: previous,
            }
        }
    }

    impl Drop for EnvVarRestore {
        fn drop(&mut self) {
            match self.value.as_deref() {
                // TODO: Audit that the environment access only happens in single-threaded code.
                Some(v) => unsafe { std::env::set_var(self.key, v) },
                // TODO: Audit that the environment access only happens in single-threaded code.
                None => unsafe { std::env::remove_var(self.key) },
            }
        }
    }

    #[test]
    fn test_parse_probe_http_get_extracts_path_port_scheme() {
        let spec = json!({
            "httpGet": {
                "path": "/healthz",
                "port": 8080,
                "scheme": "HTTPS"
            }
        });
        let probe = parse_probe(&spec).unwrap();
        match probe {
            Probe::Http(http) => {
                assert_eq!(http.path, "/healthz");
                assert_eq!(http.port, 8080);
                assert_eq!(http.scheme, "HTTPS");
            }
            _ => panic!("Expected Http probe"),
        }
    }

    #[test]
    fn test_parse_probe_http_get_defaults_path_to_slash() {
        let spec = json!({
            "httpGet": {
                "port": 80
            }
        });
        let probe = parse_probe(&spec).unwrap();
        match probe {
            Probe::Http(http) => assert_eq!(http.path, "/"),
            _ => panic!("Expected Http probe"),
        }
    }

    #[test]
    fn test_parse_probe_http_get_defaults_scheme_to_http() {
        let spec = json!({
            "httpGet": {
                "port": 80
            }
        });
        let probe = parse_probe(&spec).unwrap();
        match probe {
            Probe::Http(http) => assert_eq!(http.scheme, "HTTP"),
            _ => panic!("Expected Http probe"),
        }
    }

    #[test]
    fn test_parse_probe_http_get_empty_scheme_defaults_to_http() {
        let spec = json!({
            "httpGet": {
                "port": 80,
                "scheme": ""
            }
        });
        let probe = parse_probe(&spec).unwrap();
        match probe {
            Probe::Http(http) => assert_eq!(http.scheme, "HTTP"),
            _ => panic!("Expected Http probe"),
        }
    }

    #[test]
    fn test_parse_probe_for_container_resolves_named_http_port() {
        let container = json!({
            "name": "server",
            "ports": [{"name": "health", "containerPort": 8081}]
        });
        let spec = json!({
            "httpGet": {
                "path": "/healthz",
                "port": "health"
            }
        });

        let probe = parse_probe_for_container(&spec, &container).unwrap();
        match probe {
            Probe::Http(http) => assert_eq!(http.port, 8081),
            _ => panic!("Expected Http probe"),
        }
    }

    #[test]
    fn test_parse_probe_for_container_resolves_named_tcp_port() {
        let container = json!({
            "name": "server",
            "ports": [{"name": "tcp-health", "containerPort": 3307}]
        });
        let spec = json!({"tcpSocket": {"port": "tcp-health"}});

        let probe = parse_probe_for_container(&spec, &container).unwrap();
        match probe {
            Probe::Tcp(tcp) => assert_eq!(tcp.port, 3307),
            _ => panic!("Expected Tcp probe"),
        }
    }

    #[test]
    fn test_parse_probe_tcp_socket_extracts_port() {
        let spec = json!({
            "tcpSocket": {
                "port": 3306
            }
        });
        let probe = parse_probe(&spec).unwrap();
        match probe {
            Probe::Tcp(tcp) => assert_eq!(tcp.port, 3306),
            _ => panic!("Expected Tcp probe"),
        }
    }

    #[test]
    fn test_parse_probe_exec_extracts_command() {
        let spec = json!({
            "exec": {
                "command": ["/bin/sh", "-c", "cat /tmp/healthy"]
            }
        });
        let probe = parse_probe(&spec).unwrap();
        match probe {
            Probe::Exec(exec) => {
                assert_eq!(exec.command, vec!["/bin/sh", "-c", "cat /tmp/healthy"]);
            }
            _ => panic!("Expected Exec probe"),
        }
    }

    #[test]
    fn test_parse_probe_missing_all_fields_returns_error() {
        let spec = json!({"periodSeconds": 10});
        let result = parse_probe(&spec);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("httpGet, tcpSocket, exec, or grpc")
        );
    }

    #[test]
    fn test_parse_probe_http_get_missing_port_returns_error() {
        let spec = json!({
            "httpGet": {
                "path": "/healthz"
            }
        });
        let result = parse_probe(&spec);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("port"));
    }

    #[test]
    fn test_parse_probe_tcp_socket_missing_port_returns_error() {
        let spec = json!({
            "tcpSocket": {}
        });
        let result = parse_probe(&spec);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("port"));
    }

    #[test]
    fn test_parse_probe_exec_missing_command_returns_error() {
        let spec = json!({
            "exec": {}
        });
        let result = parse_probe(&spec);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("command"));
    }

    #[test]
    fn test_parse_probe_grpc_extracts_port_and_service() {
        let spec = json!({
            "grpc": {
                "port": 50051,
                "service": "my-service"
            }
        });
        let probe = parse_probe(&spec).unwrap();
        match probe {
            Probe::Grpc(grpc) => {
                assert_eq!(grpc.port, 50051);
                assert_eq!(grpc.service, "my-service");
            }
            _ => panic!("Expected Grpc probe"),
        }
    }

    #[test]
    fn test_parse_probe_grpc_defaults_service_to_empty() {
        let spec = json!({
            "grpc": {
                "port": 50051
            }
        });
        let probe = parse_probe(&spec).unwrap();
        match probe {
            Probe::Grpc(grpc) => {
                assert_eq!(grpc.port, 50051);
                assert_eq!(grpc.service, "");
            }
            _ => panic!("Expected Grpc probe"),
        }
    }

    #[test]
    fn test_parse_probe_grpc_missing_port_returns_error() {
        let spec = json!({
            "grpc": {}
        });
        let result = parse_probe(&spec);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("port"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_check_grpc_probe_does_not_depend_on_supervisor_spawn_capacity() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let port = listener.local_addr().unwrap().port();

        let server = tokio::spawn(async move {
            let (socket, _) = listener
                .accept()
                .await
                .expect("probe connection should arrive");
            let mut h2 = h2::server::handshake(socket)
                .await
                .expect("h2 server handshake should complete");
            let request = h2
                .accept()
                .await
                .expect("h2 request should arrive")
                .expect("h2 request should be valid");
            let (_request, mut respond) = request;
            let response = hyper::Response::builder()
                .status(200)
                .header("content-type", "application/grpc")
                .body(())
                .expect("response should build");
            let mut stream = respond
                .send_response(response, false)
                .expect("response should send");
            stream
                .send_data(Bytes::from_static(&[0, 0, 0, 0, 2, 0x08, 0x01]), false)
                .expect("grpc serving frame should send");
            let mut trailers = HeaderMap::new();
            trailers.insert("grpc-status", "0".parse().unwrap());
            stream
                .send_trailers(trailers)
                .expect("grpc trailers should send");
            let _ = tokio::time::timeout(Duration::from_millis(200), h2.accept()).await;
        });

        let config = crate::task_supervisor::TaskCategoryConfig {
            others: 1,
            ..Default::default()
        };
        let supervisor = crate::task_supervisor::TaskSupervisor::new(config);
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let _blocker = supervisor
            .spawn_async(
                crate::task_supervisor::TaskCategory::Others,
                "saturate_others_for_grpc_probe_test",
                async move {
                    let _ = release_rx.await;
                },
            )
            .await
            .expect("blocker task should start");

        let probe = GrpcProbe {
            port,
            service: String::new(),
        };

        let result = tokio::time::timeout(
            Duration::from_millis(500),
            check_grpc_probe("127.0.0.1", &probe, Duration::from_millis(200), &supervisor),
        )
        .await;
        let _ = release_tx.send(());

        let result = result
            .expect("gRPC probe must not wait for unrelated supervisor capacity")
            .expect("probe should not error");
        server.await.expect("test gRPC server task should complete");
        assert!(
            result,
            "serving response should pass despite saturated Others category"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_check_grpc_probe_retries_transient_first_connection_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let port = listener.local_addr().unwrap().port();

        let server = tokio::spawn(async move {
            let (socket, _) = listener
                .accept()
                .await
                .expect("first probe connection should arrive");
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                drop(socket);
            });

            let (socket, _) = listener
                .accept()
                .await
                .expect("retry probe connection should arrive");
            let mut h2 = h2::server::handshake(socket)
                .await
                .expect("h2 server handshake should complete");
            let request = h2
                .accept()
                .await
                .expect("h2 request should arrive")
                .expect("h2 request should be valid");
            let (_request, mut respond) = request;
            let response = hyper::Response::builder()
                .status(200)
                .header("content-type", "application/grpc")
                .body(())
                .expect("response should build");
            let mut stream = respond
                .send_response(response, false)
                .expect("response should send");
            stream
                .send_data(Bytes::from_static(&[0, 0, 0, 0, 2, 0x08, 0x01]), false)
                .expect("grpc serving frame should send");
            let mut trailers = HeaderMap::new();
            trailers.insert("grpc-status", "0".parse().unwrap());
            stream
                .send_trailers(trailers)
                .expect("grpc trailers should send");
            let _ = tokio::time::timeout(Duration::from_millis(200), h2.accept()).await;
        });

        let supervisor = crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        );
        let probe = GrpcProbe {
            port,
            service: String::new(),
        };

        let result = check_grpc_probe("127.0.0.1", &probe, Duration::from_millis(50), &supervisor)
            .await
            .expect("probe should not error");

        server.await.expect("test gRPC server task should complete");
        assert!(result, "retry should observe the serving gRPC response");
    }

    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::await_holding_lock)] // PROXY_ENV_LOCK serializes env-var-mutating tests; intentional
    async fn test_build_probe_http_client_bypasses_proxy_env() {
        let _env_lock = PROXY_ENV_LOCK.lock().expect("proxy env lock poisoned");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("proxy listener should bind");
        let proxy_addr = listener
            .local_addr()
            .expect("proxy listener should have local addr");
        let proxy_url = format!("http://{}", proxy_addr);

        let (proxy_hit_tx, proxy_hit_rx) = oneshot::channel();
        tokio::spawn(async move {
            let proxy_hit = match tokio::time::timeout(
                std::time::Duration::from_millis(800),
                listener.accept(),
            )
            .await
            {
                Ok(Ok((mut socket, _))) => {
                    let mut buf = [0u8; 2048];
                    // safe-to-ignore: draining the probe request bytes before responding
                    let _ = socket.read(&mut buf).await;
                    let _ = socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                        )
                        .await;
                    true
                }
                _ => false,
            };
            let _ = proxy_hit_tx.send(proxy_hit);
        });

        // Force proxy env usage; probe client must ignore these values for
        // in-cluster pod IP probe traffic.
        let _http_proxy_upper = EnvVarRestore::set("HTTP_PROXY", Some(&proxy_url));
        let _http_proxy_lower = EnvVarRestore::set("http_proxy", Some(&proxy_url));
        let _https_proxy_upper = EnvVarRestore::set("HTTPS_PROXY", Some(&proxy_url));
        let _https_proxy_lower = EnvVarRestore::set("https_proxy", Some(&proxy_url));
        let _all_proxy_upper = EnvVarRestore::set("ALL_PROXY", Some(&proxy_url));
        let _all_proxy_lower = EnvVarRestore::set("all_proxy", Some(&proxy_url));
        let _no_proxy_upper = EnvVarRestore::set("NO_PROXY", None);
        let _no_proxy_lower = EnvVarRestore::set("no_proxy", None);

        let client = build_probe_http_client().expect("probe client should build");
        let result = client
            .get("http://198.51.100.1:81/readyz")
            .timeout(std::time::Duration::from_millis(250))
            .send()
            .await;

        let proxy_hit = proxy_hit_rx.await.unwrap_or(false);

        assert!(
            result.is_err(),
            "probe client should bypass proxy and fail direct request"
        );
        assert!(
            !proxy_hit,
            "probe client should bypass proxy, but request reached proxy listener"
        );
    }
}
