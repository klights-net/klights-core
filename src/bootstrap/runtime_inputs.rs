const IMAGE_PULL_RESPONSE_TIMEOUT_ENV: &str = "KLIGHTS_IMAGE_PULL_RESPONSE_TIMEOUT_SECS";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct NetworkRuntimeInputs {
    pub(crate) image_pull_response_timeout: std::time::Duration,
}

impl NetworkRuntimeInputs {
    pub(crate) fn capture() -> Self {
        let raw = std::env::var(IMAGE_PULL_RESPONSE_TIMEOUT_ENV).ok();
        Self::from_image_pull_timeout_env(raw.as_deref())
    }

    fn from_image_pull_timeout_env(raw: Option<&str>) -> Self {
        let seconds = raw
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(crate::kubelet::cri::DEFAULT_IMAGE_PULL_RESPONSE_TIMEOUT.as_secs());
        Self {
            image_pull_response_timeout: std::time::Duration::from_secs(seconds),
        }
    }
}

/// Capture immutable host inputs once at the process composition boundary.
pub(crate) async fn capture_sandbox_inputs(
    file_process: &klights_supervisor::FileProcessExecutor,
    node_mode: &crate::bootstrap::NodeMode,
) -> crate::kubelet::pod_sandbox_config::SandboxRuntimeInputs {
    let (primary, resolved_upstream) = tokio::join!(
        klights_supervisor::runtime_fs::read_utf8_async(file_process, "/etc/resolv.conf"),
        klights_supervisor::runtime_fs::read_utf8_async(
            file_process,
            "/run/systemd/resolve/resolv.conf"
        ),
    );
    crate::kubelet::pod_sandbox_config::SandboxRuntimeInputs {
        host_dns: crate::kubelet::pod_dns::HostDnsConfig::from_resolv_conf_contents(
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
            super::NetworkRuntimeInputs::from_image_pull_timeout_env(None)
                .image_pull_response_timeout,
            std::time::Duration::from_secs(30)
        );
        assert_eq!(
            super::NetworkRuntimeInputs::from_image_pull_timeout_env(Some("600"))
                .image_pull_response_timeout,
            std::time::Duration::from_secs(600)
        );
        assert_eq!(
            super::NetworkRuntimeInputs::from_image_pull_timeout_env(Some("invalid"))
                .image_pull_response_timeout,
            std::time::Duration::from_secs(30)
        );
    }
}
