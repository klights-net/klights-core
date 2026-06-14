/// Lifecycle hook execution port.
/// Wraps httpGet and exec hook execution behind a mockable trait so
/// postStart/preStop hooks can be tested without real HTTP/exec calls.
use serde_json::Value;
use std::sync::Arc;

use crate::kubelet::pod_runtime::cri::CriRuntime;

/// Outcome of a lifecycle hook execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HookOutcome {
    Succeeded,
    Failed(String),
}

#[async_trait::async_trait]
pub trait PodHookRuntime: Send + Sync {
    /// Execute a postStart hook for a container.
    /// `hook` is the raw `lifecycle.postStart` object from the container spec.
    async fn execute_post_start(
        &self,
        container_id: &str,
        pod_ip: &str,
        hook: &Value,
        container_spec: &Value,
    ) -> anyhow::Result<HookOutcome>;

    /// Execute a preStop hook for a container.
    async fn execute_pre_stop(
        &self,
        container_id: &str,
        pod_ip: &str,
        hook: &Value,
        container_spec: &Value,
    ) -> anyhow::Result<HookOutcome>;
}

/// Production hook adapter preserving the legacy lifecycle hook helper while
/// exposing it through a mockable runtime port.
pub struct RealPodHookRuntime {
    cri: Arc<dyn CriRuntime>,
    supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
}

impl RealPodHookRuntime {
    pub fn new(
        cri: Arc<dyn CriRuntime>,
        supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    ) -> Self {
        Self { cri, supervisor }
    }

    async fn execute(
        &self,
        container_id: &str,
        pod_ip: &str,
        hook: &Value,
        hook_type: &str,
        container_spec: &Value,
    ) -> anyhow::Result<HookOutcome> {
        match execute_lifecycle_hook(
            self.cri.as_ref(),
            container_id,
            pod_ip,
            hook,
            hook_type,
            container_spec,
            self.supervisor.as_ref(),
        )
        .await
        {
            Ok(()) => Ok(HookOutcome::Succeeded),
            Err(err) => Ok(HookOutcome::Failed(format!("{err:#}"))),
        }
    }
}

pub fn resolve_hook_port(http_get: &Value, container_spec: &Value) -> i64 {
    match http_get.get("port") {
        Some(Value::Number(n)) => n.as_i64().unwrap_or(80),
        Some(Value::String(name)) => container_spec
            .get("ports")
            .and_then(|p| p.as_array())
            .and_then(|ports| {
                ports
                    .iter()
                    .find(|p| p.get("name").and_then(|n| n.as_str()) == Some(name.as_str()))
            })
            .and_then(|p| p.get("containerPort").and_then(|cp| cp.as_i64()))
            .unwrap_or(80),
        _ => 80,
    }
}

pub async fn execute_lifecycle_hook(
    cri: &dyn CriRuntime,
    container_id: &str,
    pod_ip: &str,
    hook: &Value,
    hook_type: &str,
    container_spec: &Value,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) -> anyhow::Result<()> {
    let hook_timeout_secs = hook
        .get("timeoutSeconds")
        .and_then(|t| t.as_u64())
        .map(|s| if s == 0 { 30 } else { s as u32 })
        .unwrap_or(30);

    if let Some(exec) = hook.get("exec")
        && let Some(command) = exec.get("command").and_then(|c| c.as_array())
    {
        let cmd: Vec<String> = command
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        tracing::info!("Executing {} exec hook: {:?}", hook_type, cmd);
        let response = cri
            .exec_sync(container_id, &cmd, hook_timeout_secs as i64)
            .await?;
        if response.exit_code != 0 {
            anyhow::bail!(
                "{} exec hook failed with exit code {}",
                hook_type,
                response.exit_code
            );
        }
        return Ok(());
    }

    if let Some(http_get) = hook.get("httpGet") {
        let port = resolve_hook_port(http_get, container_spec);
        let path = http_get.get("path").and_then(|p| p.as_str()).unwrap_or("/");
        let scheme = http_get
            .get("scheme")
            .and_then(|s| s.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or("HTTP");
        let host = http_get
            .get("host")
            .and_then(|h| h.as_str())
            .filter(|h| !h.is_empty())
            .unwrap_or(pod_ip);
        let url = format!("{}://{}:{}{}", scheme.to_lowercase(), host, port, path);
        tracing::info!("Executing {} httpGet hook: {}", hook_type, url);

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .danger_accept_invalid_certs(true)
            .build()?;

        let mut delay_ms = 100u64;
        let max_delay_ms = 5000u64;
        let total_timeout = std::time::Duration::from_secs(30);
        let start_time = std::time::Instant::now();

        loop {
            match client.get(&url).send().await {
                Ok(response) if response.status().as_u16() < 400 => {
                    return Ok(());
                }
                Ok(response) => {
                    let status = response.status();
                    if start_time.elapsed() >= total_timeout {
                        anyhow::bail!(
                            "{} httpGet hook failed with status {} after retries",
                            hook_type,
                            status
                        );
                    }
                    tracing::debug!(
                        "{} httpGet hook status {}, retrying in {}ms",
                        hook_type,
                        status,
                        delay_ms
                    );
                }
                Err(e) => {
                    if start_time.elapsed() >= total_timeout {
                        anyhow::bail!("{} httpGet hook failed after retries: {}", hook_type, e);
                    }
                    tracing::debug!(
                        "{} httpGet hook error: {}, retrying in {}ms",
                        hook_type,
                        e,
                        delay_ms
                    );
                }
            }

            let _ = task_supervisor
                .sleep(
                    "lifecycle_http_get_retry_backoff",
                    std::time::Duration::from_millis(delay_ms),
                )
                .await;
            delay_ms = std::cmp::min(delay_ms * 2, max_delay_ms);
        }
    }

    Ok(())
}

#[async_trait::async_trait]
impl PodHookRuntime for RealPodHookRuntime {
    async fn execute_post_start(
        &self,
        container_id: &str,
        pod_ip: &str,
        hook: &Value,
        container_spec: &Value,
    ) -> anyhow::Result<HookOutcome> {
        self.execute(container_id, pod_ip, hook, "postStart", container_spec)
            .await
    }

    async fn execute_pre_stop(
        &self,
        container_id: &str,
        pod_ip: &str,
        hook: &Value,
        container_spec: &Value,
    ) -> anyhow::Result<HookOutcome> {
        self.execute(container_id, pod_ip, hook, "preStop", container_spec)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_hook_port_named_port_found() {
        let http_get = serde_json::json!({"port": "http", "path": "/"});
        let container_spec = serde_json::json!({
            "ports": [
                {"name": "http", "containerPort": 8080},
                {"name": "metrics", "containerPort": 9090}
            ]
        });
        assert_eq!(resolve_hook_port(&http_get, &container_spec), 8080);
    }

    #[test]
    fn resolve_hook_port_named_port_not_found_defaults_80() {
        let http_get = serde_json::json!({"port": "unknown-port"});
        let container_spec =
            serde_json::json!({"ports": [{"name": "http", "containerPort": 8080}]});
        assert_eq!(resolve_hook_port(&http_get, &container_spec), 80);
    }

    #[test]
    fn resolve_hook_port_integer_port() {
        let http_get = serde_json::json!({"port": 9090});
        assert_eq!(resolve_hook_port(&http_get, &serde_json::json!({})), 9090);
    }

    #[test]
    fn resolve_hook_port_no_port_defaults_80() {
        let http_get = serde_json::json!({"path": "/"});
        assert_eq!(resolve_hook_port(&http_get, &serde_json::json!({})), 80);
    }
}
