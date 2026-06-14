//! Role predicates extracted from `runtime.rs` (R3 refactor).
//!
//! Pure functions that answer questions about a `NodeRole`:
//! which runtime to use, validation rules, TLS CA path, etc.

use crate::bootstrap::{CliFlags, NodeMode, NodeRole};
use crate::{KlightsConfig, paths};

// ── Role predicates ──────────────────────────────────────────────────

pub fn log_role(cli: &CliFlags) {
    match &cli.role {
        NodeRole::Leader { .. } => {
            tracing::info!(role = "Leader", "klights starting as leader");
        }
        NodeRole::Controlplane {
            leader_endpoints, ..
        } => {
            if leader_endpoints.is_empty() {
                tracing::info!(
                    role = "Controlplane",
                    mode = "seed",
                    "klights starting as raft control-plane seed (N=1 cluster)"
                );
            } else {
                let leader = leader_endpoints
                    .first()
                    .map(String::as_str)
                    .unwrap_or("<none>");
                tracing::info!(
                    role = "Controlplane",
                    mode = "join",
                    leader,
                    "klights joining existing raft cluster as control-plane voter"
                );
            }
        }
        NodeRole::Worker {
            leader_endpoints, ..
        } => {
            let leader = leader_endpoints
                .first()
                .map(String::as_str)
                .unwrap_or("<none>");
            tracing::info!(role = "Worker", leader, "klights joining as worker");
        }
    }
}

pub fn uses_leader_runtime(role: &NodeRole) -> bool {
    matches!(
        role,
        NodeRole::Leader { .. } | NodeRole::Controlplane { .. }
    )
}

pub fn uses_follower_runtime(role: &NodeRole) -> bool {
    matches!(role, NodeRole::Worker { .. })
}

pub fn should_publish_local_dataplane_metadata(role: &NodeRole) -> bool {
    match role {
        NodeRole::Leader { .. } => true,
        NodeRole::Controlplane {
            leader_endpoints, ..
        } => leader_endpoints.is_empty(),
        NodeRole::Worker { .. } => false,
    }
}

pub fn validate_rootless_multinode_support(
    _role: &NodeRole,
    _node_mode: &NodeMode,
) -> anyhow::Result<()> {
    // WireGuard-over-pasta rootless dataplane is now implemented.
    // Rootless nodes create klights.wg inside the user netns and pasta
    // exposes the WireGuard UDP port at the host edge.
    // Dataplane health tracking ensures the node reports NotReady if
    // WireGuard/pasta setup fails.
    Ok(())
}

pub fn validate_worker_dataplane_ingress(
    role: &NodeRole,
    config: &KlightsConfig,
) -> anyhow::Result<()> {
    if !matches!(role, NodeRole::Worker { .. }) {
        return Ok(());
    }
    if config.dataplane_encryption != crate::networking::wireguard::DataplaneEncryption::Enabled {
        return Ok(());
    }
    if config.worker_dataplane_no_ingress {
        return Ok(());
    }
    Ok(())
}

pub fn grpc_ca_cert_path_for_role(
    config: &KlightsConfig,
    role: &NodeRole,
) -> Option<std::path::PathBuf> {
    if uses_follower_runtime(role) {
        let leader_endpoint = match role {
            NodeRole::Worker {
                leader_endpoints, ..
            } => match leader_endpoints.first() {
                Some(endpoint) => endpoint.as_str(),
                None => return None,
            },
            _ => return None,
        };
        if !leader_endpoint.starts_with("https://") {
            return None;
        }
        if let Ok(path) = std::env::var("KLIGHTS_LEADER_CA_CERT") {
            let trimmed = path.trim();
            if !trimmed.is_empty() {
                return Some(std::path::PathBuf::from(trimmed));
            }
        }
        return None;
    }

    // Joining controlplane (incl. learners via `klights replica` →
    // Controlplane { as_learner: true }). Pre-seeded KLIGHTS_LEADER_CA_CERT
    // overrides the local namespace CA so the JoinAsControlplane RPC trusts
    // the seed cluster's CA before any cluster.db has been replicated.
    if let NodeRole::Controlplane {
        leader_endpoints,
        skip_ca,
        ..
    } = role
    {
        // skip_ca: caller explicitly asked to skip TLS verification (e.g. cp3
        // joining with --skip-ca before CA cert arrives via gRPC).
        if *skip_ca {
            return None;
        }
        if let Some(endpoint) = leader_endpoints.first()
            && endpoint.starts_with("https://")
            && let Ok(path) = std::env::var("KLIGHTS_LEADER_CA_CERT")
        {
            let trimmed = path.trim();
            if !trimmed.is_empty() {
                return Some(std::path::PathBuf::from(trimmed));
            }
        }
    }

    Some(paths::ca_cert_path(&config.containerd_namespace))
}

pub fn runs_api_server(role: &NodeRole) -> bool {
    !matches!(role, NodeRole::Worker { .. })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::NodeRole;
    use crate::bootstrap::node_role::LeaderBootstrap;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvVarGuard {
        name: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl EnvVarGuard {
        fn set(name: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(name);
            // TODO: Audit that the environment access only happens in single-threaded code.
            unsafe { std::env::set_var(name, value) };
            Self { name, previous }
        }

        fn unset(name: &'static str) -> Self {
            let previous = std::env::var_os(name);
            // TODO: Audit that the environment access only happens in single-threaded code.
            unsafe { std::env::remove_var(name) };
            Self { name, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(value) = &self.previous {
                // TODO: Audit that the environment access only happens in single-threaded code.
                unsafe { std::env::set_var(self.name, value) };
            } else {
                // TODO: Audit that the environment access only happens in single-threaded code.
                unsafe { std::env::remove_var(self.name) };
            }
        }
    }

    #[test]
    fn follower_grpc_ca_path_prefers_explicit_leader_ca_env() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _leader_ca = EnvVarGuard::set("KLIGHTS_LEADER_CA_CERT", "/tmp/leader-ca.crt");
        let mut config = KlightsConfig::test_default();
        config.containerd_namespace = "worker-ca-test".to_string();
        let worker = NodeRole::Worker {
            leader_endpoints: vec!["https://dallas:443".to_string()],
            token: Some("token".to_string()),
            skip_ca: false,
        };

        assert_eq!(
            grpc_ca_cert_path_for_role(&config, &worker),
            Some(std::path::PathBuf::from("/tmp/leader-ca.crt")),
            "followers must be able to trust a remote leader CA instead of their local namespace CA"
        );
        assert_eq!(
            grpc_ca_cert_path_for_role(
                &config,
                &NodeRole::Leader {
                    bootstrap: LeaderBootstrap::Seed,
                },
            ),
            Some(crate::paths::ca_cert_path("worker-ca-test")),
            "leader-compatible roles should keep using the local namespace CA"
        );
    }

    #[test]
    fn joining_controlplane_does_not_publish_local_dataplane_metadata() {
        let joining_voter = NodeRole::Controlplane {
            leader_endpoints: vec!["https://10.99.0.10:7679".to_string()],
            token: Some("abcdef.0123456789abcdef".to_string()),
            skip_ca: false,
            as_learner: false,
        };
        let joining_learner = NodeRole::Controlplane {
            leader_endpoints: vec!["https://10.99.0.10:7679".to_string()],
            token: Some("abcdef.0123456789abcdef".to_string()),
            skip_ca: false,
            as_learner: true,
        };

        assert!(
            !should_publish_local_dataplane_metadata(&joining_voter),
            "joining controlplane voters must not mutate local cluster.db before raft apply"
        );
        assert!(
            !should_publish_local_dataplane_metadata(&joining_learner),
            "replica learners must not mutate local cluster.db before raft apply"
        );
        assert!(
            should_publish_local_dataplane_metadata(&NodeRole::Leader {
                bootstrap: LeaderBootstrap::Seed,
            }),
            "seed leaders may publish local dataplane metadata through the leader raft path"
        );
    }

    #[test]
    fn worker_without_explicit_leader_ca_does_not_use_local_namespace_ca() {
        let _lock = ENV_LOCK.lock().unwrap();
        let mut config = KlightsConfig::test_default();
        config.containerd_namespace = "fresh-worker-local-ca".to_string();
        let worker = NodeRole::Worker {
            leader_endpoints: vec!["https://10.99.0.10:7679".to_string()],
            token: Some("abcdef.0123456789abcdef".to_string()),
            skip_ca: false,
        };

        assert_eq!(
            grpc_ca_cert_path_for_role(&config, &worker),
            None,
            "fresh workers must not trust their own generated namespace CA as the remote leader CA"
        );
    }

    #[test]
    fn follower_https_grpc_ca_path_uses_explicit_leader_ca() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _leader_ca = EnvVarGuard::set("KLIGHTS_LEADER_CA_CERT", "/tmp/leader-ca.crt");
        let config = KlightsConfig::test_default();
        let worker = NodeRole::Worker {
            leader_endpoints: vec!["https://dallas:7679".to_string()],
            token: Some("token".to_string()),
            skip_ca: false,
        };

        assert_eq!(
            grpc_ca_cert_path_for_role(&config, &worker),
            Some(std::path::PathBuf::from("/tmp/leader-ca.crt")),
            "remote leader CA trust must depend on the leader endpoint scheme"
        );
    }

    #[test]
    fn worker_grpc_ca_cert_path_without_explicit_leader_ca_does_not_use_local_ca() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _leader_ca = EnvVarGuard::unset("KLIGHTS_LEADER_CA_CERT");
        let mut config = KlightsConfig::test_default();
        config.containerd_namespace = "worker-ca-regression".to_string();
        let worker = NodeRole::Worker {
            leader_endpoints: vec!["https://leader:7679".to_string()],
            token: Some("token".to_string()),
            skip_ca: false,
        };

        let result = grpc_ca_cert_path_for_role(&config, &worker);
        assert_eq!(
            result, None,
            "workers must not trust a worker-local namespace CA as the remote leader CA"
        );
    }

    #[test]
    fn joining_controlplane_grpc_ca_path_prefers_explicit_leader_ca_env() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _leader_ca = EnvVarGuard::set("KLIGHTS_LEADER_CA_CERT", "/tmp/leader-ca.crt");
        let mut config = KlightsConfig::test_default();
        config.containerd_namespace = "cp-join-ca-test".to_string();
        let cp_join = NodeRole::Controlplane {
            leader_endpoints: vec!["https://seed:7679".to_string()],
            token: Some("token".to_string()),
            skip_ca: false,
            as_learner: true,
        };

        assert_eq!(
            grpc_ca_cert_path_for_role(&config, &cp_join),
            Some(std::path::PathBuf::from("/tmp/leader-ca.crt")),
            "joining controlplane (incl. learners via `klights replica`) must \
             trust the pre-seeded leader CA when KLIGHTS_LEADER_CA_CERT is set; \
             returning the local namespace CA leaves the JoinAsControlplane \
             RPC failing with UnknownIssuer when each node has a distinct CA"
        );
    }

    #[test]
    fn seed_controlplane_grpc_ca_path_ignores_leader_ca_env() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _leader_ca = EnvVarGuard::set("KLIGHTS_LEADER_CA_CERT", "/tmp/leader-ca.crt");
        let mut config = KlightsConfig::test_default();
        config.containerd_namespace = "cp-seed-ca-test".to_string();
        let cp_seed = NodeRole::Controlplane {
            leader_endpoints: vec![],
            token: None,
            skip_ca: false,
            as_learner: false,
        };

        assert_eq!(
            grpc_ca_cert_path_for_role(&config, &cp_seed),
            Some(crate::paths::ca_cert_path("cp-seed-ca-test")),
            "seed controlplane has no remote leader to trust; falls back to local namespace CA"
        );
    }
}
