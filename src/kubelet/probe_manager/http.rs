use crate::kubelet::probes::{HttpProbe, check_http_probe as run_http_probe};
use reqwest::Client;
use std::time::Duration;

pub async fn check_http_probe(
    client: Option<&Client>,
    pod_ip: &str,
    probe: &HttpProbe,
    timeout: Duration,
) -> bool {
    match client {
        Some(client) => run_http_probe(client, pod_ip, probe, timeout)
            .await
            .unwrap_or(false),
        None => false,
    }
}
