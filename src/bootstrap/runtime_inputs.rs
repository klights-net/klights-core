const IMAGE_PULL_RESPONSE_TIMEOUT_ENV: &str = "KLIGHTS_IMAGE_PULL_RESPONSE_TIMEOUT_SECS";
const CRI_REQUEST_TIMEOUT_ENV: &str = "KLIGHTS_CRI_REQUEST_TIMEOUT_SECS";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct NetworkRuntimeInputs {
    pub(crate) image_pull_response_timeout: std::time::Duration,
    pub(crate) cri_request_timeout: std::time::Duration,
}

impl NetworkRuntimeInputs {
    pub(crate) fn capture() -> Self {
        let image_pull = std::env::var(IMAGE_PULL_RESPONSE_TIMEOUT_ENV).ok();
        let cri_request = std::env::var(CRI_REQUEST_TIMEOUT_ENV).ok();
        Self::from_image_pull_timeout_env(image_pull.as_deref(), cri_request.as_deref())
    }

    fn from_image_pull_timeout_env(image_pull: Option<&str>, cri_request: Option<&str>) -> Self {
        let image_pull_seconds = image_pull
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(klights_kubelet::cri::DEFAULT_IMAGE_PULL_RESPONSE_TIMEOUT.as_secs());
        let cri_request_seconds = cri_request
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|seconds| *seconds > 0)
            .unwrap_or(klights_kubelet::cri::DEFAULT_CRI_REQUEST_TIMEOUT.as_secs());
        Self {
            image_pull_response_timeout: std::time::Duration::from_secs(image_pull_seconds),
            cri_request_timeout: std::time::Duration::from_secs(cri_request_seconds),
        }
    }
}

/// Capture immutable host inputs once at the process composition boundary.
pub(crate) async fn capture_sandbox_inputs(
    file_process: &klights_supervisor::FileProcessExecutor,
    node_mode: &crate::bootstrap::NodeMode,
) -> klights_kubelet::pod_sandbox_config::SandboxRuntimeInputs {
    let (primary, resolved_upstream) = tokio::join!(
        klights_supervisor::runtime_fs::read_utf8_async(file_process, "/etc/resolv.conf"),
        klights_supervisor::runtime_fs::read_utf8_async(
            file_process,
            "/run/systemd/resolve/resolv.conf"
        ),
    );
    klights_kubelet::pod_sandbox_config::SandboxRuntimeInputs {
        host_dns: klights_kubelet::pod_dns::HostDnsConfig::from_resolv_conf_contents(
            primary.as_deref().ok(),
            resolved_upstream.as_deref().ok(),
        ),
        rootless: matches!(node_mode, crate::bootstrap::NodeMode::Rootless { .. }),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn image_pull_timeout_input_preserves_default_and_valid_override() {
        assert_eq!(
            super::NetworkRuntimeInputs::from_image_pull_timeout_env(None, None)
                .image_pull_response_timeout,
            std::time::Duration::from_secs(30)
        );
        assert_eq!(
            super::NetworkRuntimeInputs::from_image_pull_timeout_env(Some("600"), None)
                .image_pull_response_timeout,
            std::time::Duration::from_secs(600)
        );
        assert_eq!(
            super::NetworkRuntimeInputs::from_image_pull_timeout_env(Some("invalid"), None)
                .image_pull_response_timeout,
            std::time::Duration::from_secs(30)
        );
    }

    #[test]
    fn cri_request_timeout_is_captured_once_with_two_minute_default() {
        let defaults = super::NetworkRuntimeInputs::from_image_pull_timeout_env(None, None);
        assert_eq!(
            defaults.cri_request_timeout,
            std::time::Duration::from_secs(120)
        );
        let overridden = super::NetworkRuntimeInputs::from_image_pull_timeout_env(None, Some("7"));
        assert_eq!(
            overridden.cri_request_timeout,
            std::time::Duration::from_secs(7)
        );
        let invalid = super::NetworkRuntimeInputs::from_image_pull_timeout_env(None, Some("0"));
        assert_eq!(
            invalid.cri_request_timeout,
            std::time::Duration::from_secs(120)
        );
    }
}
