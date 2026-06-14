use crate::kubelet::probes::{ExecProbe, check_exec_probe as run_exec_probe};

pub async fn check_exec_probe(
    cri: &dyn crate::kubelet::pod_runtime::cri::CriRuntime,
    container_id: &str,
    probe: &ExecProbe,
    timeout_secs: u64,
) -> bool {
    run_exec_probe(cri, container_id, probe, timeout_secs)
        .await
        .unwrap_or(false)
}
