use axum::http::HeaderName;

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct BackendProxyHeaderPolicy;

impl BackendProxyHeaderPolicy {
    pub(crate) const fn workload_backend() -> Self {
        Self
    }

    pub(crate) fn should_forward(&self, name: &HeaderName) -> bool {
        self.should_forward_str(name.as_str())
    }

    pub(crate) fn should_forward_str(&self, name: &str) -> bool {
        !self.should_skip_str(name)
    }

    pub(crate) fn should_skip_str(&self, name: &str) -> bool {
        is_hop_by_hop_or_framing_header(name) || is_kubernetes_caller_credential_header(name)
    }
}

fn is_hop_by_hop_or_framing_header(name: &str) -> bool {
    name.eq_ignore_ascii_case("host")
        || name.eq_ignore_ascii_case("content-length")
        || name.eq_ignore_ascii_case("transfer-encoding")
        || name.eq_ignore_ascii_case("connection")
        || name.eq_ignore_ascii_case("keep-alive")
        || name.eq_ignore_ascii_case("proxy-authenticate")
        || name.eq_ignore_ascii_case("proxy-authorization")
        || name.eq_ignore_ascii_case("te")
        || name.eq_ignore_ascii_case("trailer")
        || name.eq_ignore_ascii_case("upgrade")
}

fn is_kubernetes_caller_credential_header(name: &str) -> bool {
    name.eq_ignore_ascii_case("authorization")
        || name
            .get(..12)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("impersonate-"))
        || name
            .get(..9)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("x-remote-"))
}

#[cfg(test)]
mod tests {
    use super::BackendProxyHeaderPolicy;

    #[test]
    fn workload_backend_policy_strips_credentials_and_preserves_regular_headers() {
        let policy = BackendProxyHeaderPolicy::workload_backend();

        for header in [
            "authorization",
            "Authorization",
            "proxy-authorization",
            "impersonate-user",
            "Impersonate-Group",
            "x-remote-user",
            "X-Remote-Group",
            "x-remote-extra-project",
            "host",
            "content-length",
            "transfer-encoding",
            "connection",
            "upgrade",
        ] {
            assert!(
                !policy.should_forward_str(header),
                "{header} must not be forwarded to backend workloads"
            );
        }

        for header in ["accept", "content-type", "user-agent", "x-trace-id"] {
            assert!(
                policy.should_forward_str(header),
                "{header} should remain forwardable"
            );
        }
    }
}
