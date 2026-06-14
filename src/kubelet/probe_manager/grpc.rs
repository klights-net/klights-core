use crate::kubelet::probes::{GrpcProbe, check_grpc_probe as run_grpc_probe};
use std::time::Duration;

pub async fn check_grpc_probe(
    pod_ip: &str,
    probe: &GrpcProbe,
    timeout: Duration,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) -> bool {
    run_grpc_probe(pod_ip, probe, timeout, task_supervisor)
        .await
        .unwrap_or(false)
}
