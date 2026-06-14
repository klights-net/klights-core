//! P3-7b: round-robin connect across multiple leader endpoints.
//!
//! Workers and replicas keep an ordered list of leader endpoints (CLI
//! `--leader` plus the last-known membership refreshed on each connect).
//! When the current leader becomes unreachable they walk the list with a
//! bounded per-attempt timeout. After `rounds` full passes through the
//! list without a connect, the routine gives up so the operator sees a
//! hard failure instead of the process spinning silently.
//!
//! Full integration into `worker_identity::HttpCsrBootstrapClient` and
//! `replication::grpc::client::ReplicationGrpcClient` (so a mid-flight
//! leader failure also fast-fails over to the next endpoint) lands as
//! part of P3-11 alongside the production gRPC RaftNetwork. This step
//! ships the algorithm plus a startup reachability probe that selects
//! the initial leader endpoint via `pick_reachable_leader_endpoint`.

use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};

use crate::task_supervisor::TaskSupervisor;

/// Per-attempt connect timeout (5 s) chosen so that the worst case of
/// `rounds * endpoints * timeout` stays inside the K8s `node-monitor-
/// grace-period` (`50 s`).
pub const DEFAULT_PER_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

/// Try each endpoint in `endpoints` in order, retrying up to `rounds`
/// full passes. Returns the index of the endpoint that succeeded along
/// with the closure's `Ok` value. Returns `Err` after `rounds` complete
/// passes without a successful connect.
///
/// `supervisor.timeout` provides the per-attempt deadline so the work is
/// tracked by the supervisor for shutdown integration.
pub async fn connect_round_robin<F, Fut, T>(
    supervisor: &Arc<TaskSupervisor>,
    name: &str,
    endpoints: &[String],
    rounds: usize,
    per_attempt_timeout: Duration,
    mut connect: F,
) -> Result<(usize, T)>
where
    F: FnMut(String) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    if endpoints.is_empty() {
        return Err(anyhow!("no leader endpoints provided"));
    }
    let total_rounds = rounds.max(1);
    for round in 0..total_rounds {
        for (idx, endpoint) in endpoints.iter().enumerate() {
            let fut = connect(endpoint.clone());
            let timeout_name = format!("{name}_round{round}_idx{idx}");
            match supervisor
                .timeout(timeout_name, per_attempt_timeout, fut)
                .await?
            {
                Ok(Ok(value)) => return Ok((idx, value)),
                Ok(Err(err)) => {
                    tracing::debug!(round, idx, endpoint = %endpoint, error = %err, "leader connect failed");
                }
                Err(_elapsed) => {
                    tracing::debug!(round, idx, endpoint = %endpoint, "leader connect timed out");
                }
            }
        }
    }
    Err(anyhow!(
        "all {} leader endpoint(s) unreachable after {total_rounds} round(s)",
        endpoints.len()
    ))
}

/// Worker startup helper: pick the first reachable endpoint by probing
/// each one with a TCP connect. Falls back to `endpoints[0]` when every
/// probe fails so the existing connect path can produce its usual error.
///
/// The supervised round-robin walk caps the worst-case probe latency at
/// `endpoints.len() * DEFAULT_PER_ATTEMPT_TIMEOUT`. The probe is purely
/// informational: it does not authenticate, only confirms TCP
/// reachability, so an operator hitting an offline N=1 leader still
/// gets the legacy error path on the first endpoint.
pub async fn pick_reachable_leader_endpoint(
    supervisor: &Arc<TaskSupervisor>,
    endpoints: &[String],
) -> String {
    if endpoints.is_empty() {
        return String::new();
    }
    let probe_supervisor = supervisor.clone();
    match connect_round_robin(
        supervisor,
        "worker_pick_reachable_leader",
        endpoints,
        1,
        DEFAULT_PER_ATTEMPT_TIMEOUT,
        |endpoint| {
            let sup = probe_supervisor.clone();
            async move {
                let host_port = host_port_for_endpoint(&endpoint)
                    .ok_or_else(|| anyhow!("could not parse host:port from {endpoint}"))?;
                let target = host_port.clone();
                let addrs = sup
                    .run_blocking_file_keyed(
                        "leader_endpoint_dns_resolve",
                        target.clone(),
                        move || {
                            target
                                .to_socket_addrs()
                                .map(|iter| iter.collect::<Vec<_>>())
                        },
                    )
                    .await
                    .map_err(|e| anyhow!("dns resolve task panicked for {host_port}: {e}"))?
                    .map_err(|e| anyhow!("dns resolve {host_port}: {e}"))?;
                for addr in addrs {
                    if tokio::net::TcpStream::connect(addr).await.is_ok() {
                        return Ok::<(), anyhow::Error>(());
                    }
                }
                Err(anyhow!("no addr for {host_port} accepted TCP connect"))
            }
        },
    )
    .await
    {
        Ok((idx, _)) => {
            let pick = endpoints[idx].clone();
            if idx != 0 {
                tracing::warn!(
                    chosen = %pick,
                    skipped = idx,
                    "primary leader endpoint(s) unreachable on startup; selected first reachable endpoint"
                );
            }
            pick
        }
        Err(err) => {
            tracing::warn!(
                error = %err,
                fallback = %endpoints[0],
                "no leader endpoint passed TCP probe on startup; falling back to first endpoint and letting later connect surface the error"
            );
            endpoints[0].clone()
        }
    }
}

fn host_port_for_endpoint(endpoint: &str) -> Option<String> {
    let no_scheme = endpoint
        .strip_prefix("https://")
        .or_else(|| endpoint.strip_prefix("http://"))
        .unwrap_or(endpoint);
    let host_port = no_scheme.split('/').next()?;
    if host_port.is_empty() {
        return None;
    }
    if host_port.contains(':') {
        Some(host_port.to_string())
    } else if endpoint.starts_with("http://") {
        Some(format!("{host_port}:80"))
    } else {
        Some(format!("{host_port}:443"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_supervisor::TaskCategoryConfig;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn supervisor() -> Arc<TaskSupervisor> {
        Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()))
    }

    #[tokio::test]
    async fn first_two_fail_third_succeeds() {
        let sup = supervisor();
        let endpoints = vec![
            "https://a:7679".to_string(),
            "https://b:7679".to_string(),
            "https://c:7679".to_string(),
        ];
        let attempts = Arc::new(Mutex::new(Vec::<String>::new()));
        let attempts_for_closure = attempts.clone();
        let (idx, value) = connect_round_robin(
            &sup,
            "test_first_two_fail",
            &endpoints,
            3,
            Duration::from_millis(200),
            move |endpoint| {
                let attempts = attempts_for_closure.clone();
                async move {
                    attempts.lock().unwrap().push(endpoint.clone());
                    if endpoint == "https://c:7679" {
                        Ok::<_, anyhow::Error>(42)
                    } else {
                        Err(anyhow!("connection refused"))
                    }
                }
            },
        )
        .await
        .expect("third endpoint should succeed");
        assert_eq!(idx, 2);
        assert_eq!(value, 42);
        let calls = attempts.lock().unwrap();
        assert_eq!(calls.len(), 3, "all three endpoints tried once");
        assert_eq!(calls[0], "https://a:7679");
        assert_eq!(calls[1], "https://b:7679");
        assert_eq!(calls[2], "https://c:7679");
    }

    #[tokio::test]
    async fn all_endpoints_fail_for_all_rounds_returns_error() {
        let sup = supervisor();
        let endpoints = vec![
            "https://a:7679".to_string(),
            "https://b:7679".to_string(),
            "https://c:7679".to_string(),
        ];
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_closure = calls.clone();
        let err = connect_round_robin(
            &sup,
            "test_all_fail",
            &endpoints,
            3,
            Duration::from_millis(50),
            move |_endpoint| {
                let counter = calls_for_closure.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Err::<(), anyhow::Error>(anyhow!("connection refused"))
                }
            },
        )
        .await
        .expect_err("must error after all rounds exhausted");
        assert!(
            err.to_string().contains("after 3 round"),
            "error must report round count: {err}"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            endpoints.len() * 3,
            "every endpoint tried once per round"
        );
    }

    #[tokio::test]
    async fn per_attempt_timeout_skips_to_next_endpoint() {
        let sup = supervisor();
        let endpoints = vec![
            "https://slow:7679".to_string(),
            "https://fast:7679".to_string(),
        ];
        let started = std::time::Instant::now();
        let (idx, _) = connect_round_robin(
            &sup,
            "test_timeout_skips",
            &endpoints,
            1,
            Duration::from_millis(100),
            |endpoint| async move {
                if endpoint == "https://slow:7679" {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    Ok::<_, anyhow::Error>(1)
                } else {
                    Ok::<_, anyhow::Error>(2)
                }
            },
        )
        .await
        .expect("fast endpoint should succeed after slow endpoint times out");
        assert_eq!(idx, 1, "fast endpoint won");
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(2),
            "fast path must not wait for slow endpoint full sleep (took {elapsed:?})"
        );
    }

    #[tokio::test]
    async fn pick_reachable_leader_endpoint_prefers_first_reachable() {
        // Bind a TCP listener on an ephemeral port; that endpoint is the
        // only reachable one in the list.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral listener");
        let reachable_port = listener.local_addr().unwrap().port();
        let unreachable = "https://127.0.0.1:1".to_string();
        let reachable = format!("https://127.0.0.1:{reachable_port}");
        let sup = supervisor();
        let picked = pick_reachable_leader_endpoint(&sup, &[unreachable, reachable.clone()]).await;
        assert_eq!(picked, reachable);
    }

    #[tokio::test]
    async fn pick_reachable_leader_endpoint_falls_back_when_all_unreachable() {
        let sup = supervisor();
        let endpoints = vec![
            "https://127.0.0.1:1".to_string(),
            "https://127.0.0.1:2".to_string(),
        ];
        let picked = pick_reachable_leader_endpoint(&sup, &endpoints).await;
        // Falls back to endpoints[0] so the legacy connect path surfaces
        // its own error rather than producing a new failure mode.
        assert_eq!(picked, "https://127.0.0.1:1");
    }

    #[test]
    fn host_port_strips_scheme_and_appends_default_port() {
        assert_eq!(
            host_port_for_endpoint("https://leader.test:7679").as_deref(),
            Some("leader.test:7679")
        );
        assert_eq!(
            host_port_for_endpoint("https://leader.test").as_deref(),
            Some("leader.test:443")
        );
        assert_eq!(
            host_port_for_endpoint("http://leader.test/").as_deref(),
            Some("leader.test:80")
        );
        assert_eq!(host_port_for_endpoint(""), None);
    }

    #[tokio::test]
    async fn empty_endpoint_list_errors_immediately() {
        let sup = supervisor();
        let endpoints: Vec<String> = vec![];
        let err = connect_round_robin(
            &sup,
            "test_empty",
            &endpoints,
            3,
            DEFAULT_PER_ATTEMPT_TIMEOUT,
            |_| async { Ok::<_, anyhow::Error>(()) },
        )
        .await
        .expect_err("empty endpoint list must error");
        assert!(err.to_string().contains("no leader endpoints"));
    }
}
