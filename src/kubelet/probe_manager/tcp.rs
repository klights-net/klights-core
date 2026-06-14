use crate::kubelet::probes::{TcpProbe, check_tcp_probe as run_tcp_probe};
use std::time::Duration;

pub async fn check_tcp_probe(
    pod_ip: &str,
    probe: &TcpProbe,
    timeout: Duration,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) -> bool {
    run_tcp_probe(pod_ip, probe, timeout, task_supervisor)
        .await
        .unwrap_or(false)
}
