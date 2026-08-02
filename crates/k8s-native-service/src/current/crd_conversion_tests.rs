use super::*;
use klights_leader_api::{
    LeaderResourceQuery, ResourceGetRequest, ResourceListRequest, ResourceListResult,
    ResourceQueryError, ResourceQueryFuture,
};
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
            Some(value) => unsafe { std::env::set_var(key, value) },
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
            Some(value) => unsafe { std::env::set_var(self.key, value) },
            None => unsafe { std::env::remove_var(self.key) },
        }
    }
}

struct EmptyConversionQuery;

impl LeaderResourceQuery for EmptyConversionQuery {
    fn get_resource(
        &self,
        _request: ResourceGetRequest,
    ) -> ResourceQueryFuture<'_, Option<Resource>> {
        Box::pin(async { Ok(None) })
    }

    fn list_resources(
        &self,
        _request: ResourceListRequest,
    ) -> ResourceQueryFuture<'_, ResourceListResult> {
        Box::pin(async { ResourceListResult::try_new(Vec::new(), 1, None, None, None) })
    }
}

struct FixedIdentity;

impl crate::ApiIdentityGenerator for FixedIdentity {
    fn generate_name(&self, prefix: &str) -> String {
        format!("{prefix}fixed")
    }

    fn new_uid(&self) -> String {
        "00000000-0000-4000-8000-000000000001".to_string()
    }
}

struct FailingConversionQuery;

impl LeaderResourceQuery for FailingConversionQuery {
    fn get_resource(
        &self,
        _request: ResourceGetRequest,
    ) -> ResourceQueryFuture<'_, Option<Resource>> {
        Box::pin(async { Err(ResourceQueryError::retryable("leader query unavailable")) })
    }

    fn list_resources(
        &self,
        _request: ResourceListRequest,
    ) -> ResourceQueryFuture<'_, ResourceListResult> {
        Box::pin(async { Err(ResourceQueryError::retryable("leader query unavailable")) })
    }
}

#[tokio::test]
async fn conversion_config_maps_retryable_query_error_to_service_unavailable() {
    let error = load_crd_conversion_config(&FailingConversionQuery, "example.com", "widgets")
        .await
        .expect_err("retryable leader query failure must reach the API caller");

    assert!(matches!(error, AppError::ServiceUnavailable(_)));
}

struct VersionedSnapshotQuery;

impl LeaderResourceQuery for VersionedSnapshotQuery {
    fn get_resource(
        &self,
        _request: ResourceGetRequest,
    ) -> ResourceQueryFuture<'_, Option<Resource>> {
        Box::pin(async { Ok(None) })
    }

    fn list_resources(
        &self,
        request: ResourceListRequest,
    ) -> ResourceQueryFuture<'_, ResourceListResult> {
        let rv = match request.api_version() {
            "example.com/v1" => 10,
            "example.com/v2" => 20,
            other => panic!("unexpected served version: {other}"),
        };
        Box::pin(async move { ResourceListResult::try_new(Vec::new(), rv, None, None, None) })
    }
}

#[tokio::test]
async fn conversion_merged_list_rv_does_not_skip_storage_create_between_version_reads() {
    let conversion = CrdConversionConfig {
        storage_version: "v1".to_string(),
        served_versions: vec!["v1".to_string(), "v2".to_string()],
        strategy: None,
        webhook_client_config: None,
        webhook_review_versions: Vec::new(),
    };

    let (items, list_rv) = gather_custom_resources_across_served_versions(
        &VersionedSnapshotQuery,
        &conversion,
        "example.com",
        "Widget",
        Some("default".to_string()),
        None,
    )
    .await
    .unwrap();
    let omitted_storage_create_rv = 11;
    assert!(
        items.is_empty(),
        "storage object created after the storage-version read should be absent from this merged list"
    );
    assert!(
        list_rv < omitted_storage_create_rv,
        "merged conversion list rv {list_rv} must let a follow-up watch replay omitted storage object rv {omitted_storage_create_rv}"
    );
}

#[test]
fn test_build_crd_conversion_webhook_client_accepts_base64_pem_ca_bundle() {
    use base64::Engine;
    use rcgen::generate_simple_self_signed;
    use serde_json::json;

    let cert = generate_simple_self_signed(vec!["conversion-webhook.test".to_string()])
        .expect("failed to generate test cert");
    let pem = cert.cert.pem();
    let ca_bundle = base64::engine::general_purpose::STANDARD.encode(pem.as_bytes());
    let client_config = json!({
        "caBundle": ca_bundle
    });

    let result = build_crd_conversion_webhook_client(&client_config, None);
    assert!(
        result.is_ok(),
        "base64-encoded PEM caBundle must be accepted, got: {:?}",
        result.err()
    );
}

#[tokio::test(flavor = "current_thread")]
#[allow(clippy::await_holding_lock)] // PROXY_ENV_LOCK serializes env-var-mutating tests; intentional
async fn test_build_crd_conversion_webhook_client_bypasses_proxy_env() {
    let _env_lock = PROXY_ENV_LOCK.lock().expect("proxy env lock poisoned");
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("proxy listener should bind");
    let proxy_addr = listener
        .local_addr()
        .expect("proxy listener should have local addr");
    let proxy_url = format!("http://{proxy_addr}");

    let (proxy_hit_tx, proxy_hit_rx) = oneshot::channel();
    tokio::spawn(async move {
        let proxy_hit =
            match tokio::time::timeout(std::time::Duration::from_millis(800), listener.accept())
                .await
            {
                Ok(Ok((mut socket, _))) => {
                    let mut buf = [0u8; 2048];
                    // safe-to-ignore: draining the test client's request before responding
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

    let _http_proxy_upper = EnvVarRestore::set("HTTP_PROXY", Some(&proxy_url));
    let _http_proxy_lower = EnvVarRestore::set("http_proxy", Some(&proxy_url));
    let _https_proxy_upper = EnvVarRestore::set("HTTPS_PROXY", Some(&proxy_url));
    let _https_proxy_lower = EnvVarRestore::set("https_proxy", Some(&proxy_url));
    let _all_proxy_upper = EnvVarRestore::set("ALL_PROXY", Some(&proxy_url));
    let _all_proxy_lower = EnvVarRestore::set("all_proxy", Some(&proxy_url));
    let _no_proxy_upper = EnvVarRestore::set("NO_PROXY", None);
    let _no_proxy_lower = EnvVarRestore::set("no_proxy", None);

    let client = build_crd_conversion_webhook_client(&json!({}), None)
        .expect("conversion webhook client should build");
    let result = client
        .get("https://198.51.100.1:4443/crdconvert")
        .timeout(std::time::Duration::from_millis(250))
        .send()
        .await;

    let proxy_hit = proxy_hit_rx.await.unwrap_or(false);
    assert!(
        result.is_err(),
        "conversion webhook request should fail in test harness"
    );
    assert!(
        !proxy_hit,
        "conversion webhook HTTP client must bypass proxy env vars for in-cluster service calls"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_crd_conversion_skips_objects_already_on_desired_version() {
    use serde_json::json;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("webhook listener should bind");
    let port = listener
        .local_addr()
        .expect("webhook listener should have local addr")
        .port();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("webhook accept");
        let mut buf = vec![0u8; 65536];
        let n = stream.read(&mut buf).await.expect("webhook read request");
        let req = String::from_utf8_lossy(&buf[..n]);
        let body_start = req.find("\r\n\r\n").map(|i| i + 4).unwrap_or(req.len());
        let review_req: Value =
            serde_json::from_slice(&buf[body_start..n]).expect("valid conversion review");
        let desired = review_req
            .pointer("/request/desiredAPIVersion")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let request_objects = review_req["request"]["objects"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let has_same_version = request_objects.iter().any(|o| {
            o.get("apiVersion")
                .and_then(|v| v.as_str())
                .is_some_and(|av| av == desired)
        });
        let uid = review_req
            .pointer("/request/uid")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let response_body = if has_same_version {
            json!({
                "apiVersion": "apiextensions.k8s.io/v1",
                "kind": "ConversionReview",
                "response": {
                    "uid": uid,
                    "result": {
                        "status": "Failure",
                        "message": format!("conversion from a version to itself should not call the webhook: {desired}")
                    }
                }
            })
        } else {
            let converted_objects: Vec<Value> = request_objects
                .into_iter()
                .map(|mut o| {
                    o["apiVersion"] = Value::String(desired.clone());
                    o
                })
                .collect();
            json!({
                "apiVersion": "apiextensions.k8s.io/v1",
                "kind": "ConversionReview",
                "response": {
                    "uid": uid,
                    "result": {"status": "Success"},
                    "convertedObjects": converted_objects
                }
            })
        };

        let payload = serde_json::to_string(&response_body).expect("serialize response");
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            payload.len(),
            payload
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write response");
    });
    let conversion = CrdConversionConfig {
        storage_version: "v1".to_string(),
        served_versions: vec!["v1".to_string(), "v2".to_string()],
        strategy: Some("Webhook".to_string()),
        webhook_client_config: Some(json!({
            "url": format!("http://127.0.0.1:{port}/crdconvert")
        })),
        webhook_review_versions: vec!["v1".to_string()],
    };

    let result = convert_crd_objects_to_requested_version(
        &FixedIdentity,
        &EmptyConversionQuery,
        &conversion,
        "stable.example.com",
        "widgets",
        "stable.example.com/v1",
        vec![
            json!({
                "apiVersion": "stable.example.com/v1",
                "kind": "Widget",
                "metadata": {"name": "already-v1"}
            }),
            json!({
                "apiVersion": "stable.example.com/v2",
                "kind": "Widget",
                "metadata": {"name": "needs-convert"}
            }),
        ],
    )
    .await
    .expect("mixed-version conversion should succeed");

    assert_eq!(result.len(), 2);
    assert_eq!(
        result[0]["apiVersion"], "stable.example.com/v1",
        "object already on desired version must bypass webhook and remain unchanged"
    );
    assert_eq!(
        result[1]["apiVersion"], "stable.example.com/v1",
        "object on another served version must be converted to desired version"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_crd_conversion_strategy_check_is_case_insensitive() {
    use serde_json::json;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("webhook listener should bind");
    let port = listener
        .local_addr()
        .expect("webhook listener should have local addr")
        .port();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("webhook accept");
        let mut buf = vec![0u8; 65536];
        let n = stream.read(&mut buf).await.expect("webhook read request");
        let req = String::from_utf8_lossy(&buf[..n]);
        let body_start = req.find("\r\n\r\n").map(|i| i + 4).unwrap_or(req.len());
        let review_req: Value =
            serde_json::from_slice(&buf[body_start..n]).expect("valid conversion review");
        let desired = review_req
            .pointer("/request/desiredAPIVersion")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let uid = review_req
            .pointer("/request/uid")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let converted_objects: Vec<Value> = review_req["request"]["objects"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|mut o| {
                o["apiVersion"] = Value::String(desired.clone());
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

        let payload = serde_json::to_string(&response_body).expect("serialize response");
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            payload.len(),
            payload
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write response");
    });
    let conversion = CrdConversionConfig {
        storage_version: "v1".to_string(),
        served_versions: vec!["v1".to_string(), "v2".to_string()],
        strategy: Some("webhook".to_string()),
        webhook_client_config: Some(json!({
            "url": format!("http://127.0.0.1:{port}/crdconvert")
        })),
        webhook_review_versions: vec!["v1".to_string()],
    };

    let result = convert_crd_objects_to_requested_version(
        &FixedIdentity,
        &EmptyConversionQuery,
        &conversion,
        "stable.example.com",
        "widgets",
        "stable.example.com/v1",
        vec![json!({
            "apiVersion": "stable.example.com/v2",
            "kind": "Widget",
            "metadata": {"name": "needs-convert"}
        })],
    )
    .await
    .expect("lowercase webhook strategy must still trigger conversion");

    assert_eq!(result.len(), 1);
    assert_eq!(result[0]["apiVersion"], "stable.example.com/v1");
}

#[tokio::test(flavor = "current_thread")]
async fn test_crd_conversion_strategy_none_with_client_config_stamps_requested_version() {
    use serde_json::json;
    let conversion = CrdConversionConfig {
        storage_version: "v1".to_string(),
        served_versions: vec!["v1".to_string(), "v2".to_string()],
        strategy: Some("None".to_string()),
        webhook_client_config: Some(json!({
            "url": "https://127.0.0.1:1/should-not-be-called"
        })),
        webhook_review_versions: vec!["v1".to_string()],
    };

    let result = convert_crd_objects_to_requested_version(
        &FixedIdentity,
        &EmptyConversionQuery,
        &conversion,
        "stable.example.com",
        "widgets",
        "stable.example.com/v2",
        vec![json!({
            "apiVersion": "stable.example.com/v1",
            "kind": "Widget",
            "metadata": {"name": "storage-version"}
        })],
    )
    .await
    .expect("strategy None must not call webhook but must normalize response shape");

    assert_eq!(result.len(), 1);
    assert_eq!(result[0]["apiVersion"], "stable.example.com/v2");
    assert_eq!(result[0]["kind"], "Widget");
    assert_eq!(result[0]["metadata"]["name"], "storage-version");
}

#[tokio::test(flavor = "current_thread")]
async fn test_crd_conversion_accepts_yaml_conversion_review_response() {
    use serde_json::json;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("webhook listener should bind");
    let port = listener
        .local_addr()
        .expect("webhook listener should have local addr")
        .port();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("webhook accept");
        let mut buf = vec![0u8; 65536];
        let n = stream.read(&mut buf).await.expect("webhook read request");
        let req = String::from_utf8_lossy(&buf[..n]);
        let body_start = req.find("\r\n\r\n").map(|i| i + 4).unwrap_or(req.len());
        let review_req: Value =
            serde_json::from_slice(&buf[body_start..n]).expect("valid conversion review");
        let desired = review_req
            .pointer("/request/desiredAPIVersion")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let uid = review_req
            .pointer("/request/uid")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let converted_objects: Vec<Value> = review_req["request"]["objects"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|mut o| {
                o["apiVersion"] = Value::String(desired.clone());
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
        let yaml_payload = serde_yaml::to_string(&response_body).expect("serialize yaml");
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/yaml\r\nContent-Length: {}\r\n\r\n{}",
            yaml_payload.len(),
            yaml_payload
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write response");
    });
    let conversion = CrdConversionConfig {
        storage_version: "v1".to_string(),
        served_versions: vec!["v1".to_string(), "v2".to_string()],
        strategy: Some("Webhook".to_string()),
        webhook_client_config: Some(json!({
            "url": format!("http://127.0.0.1:{port}/crdconvert")
        })),
        webhook_review_versions: vec!["v1".to_string()],
    };

    let result = convert_crd_objects_to_requested_version(
        &FixedIdentity,
        &EmptyConversionQuery,
        &conversion,
        "stable.example.com",
        "widgets",
        "stable.example.com/v1",
        vec![json!({
            "apiVersion": "stable.example.com/v2",
            "kind": "Widget",
            "metadata": {"name": "needs-convert"}
        })],
    )
    .await
    .expect("yaml conversion webhook response should be accepted");

    assert_eq!(result.len(), 1);
    assert_eq!(result[0]["apiVersion"], "stable.example.com/v1");
}
