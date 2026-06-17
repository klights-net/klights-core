//! CLI parsing via clap derive.
//!
//! Subcommands:
//! - `start`         — start the klights service (single-node; alias for `leader`)
//! - `leader`        — start as a single-node leader (alias for `start`)
//! - `controlplane`  — start (no `--leader`) or join (with `--leader`) a Raft
//!   control-plane cluster. Solo controlplane is equivalent to
//!   `start` / `leader`; with `--leader` it joins an existing cluster as a
//!   voter.
//! - `replica`       — join as replica (requires --leader and a token source)
//! - `worker`        — join as worker-only node (requires --leader and a token source)
//! - `stop`          — gracefully stop a running service (SIGTERM)
//! - `cleanup`       — full teardown of containers, networking, and data

use crate::bootstrap::NodeRole;
use crate::bootstrap::node_role::{LeaderBootstrap, controlplane_limit};
use clap::{Parser, Subcommand};

/// klights — lightweight Kubernetes in Rust
///
/// A single-node K8s implementation with zero idle CPU.
#[derive(Parser, Debug)]
#[command(
    name = "klights",
    version = crate::version::GIT_VERSION_WITH_COMMIT,
    about,
    long_about = None
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,

    /// Run in rootless mode (Phase 2 — stub only in Phase 1)
    #[arg(long, global = true)]
    pub rootless: bool,

    /// Override containerd namespace [env: KLIGHTS_CONTAINERD_NAMESPACE]
    #[arg(
        long = "namespace",
        global = true,
        env = "KLIGHTS_CONTAINERD_NAMESPACE",
        default_value = "klights"
    )]
    pub namespace: String,

    /// API server bind address. Defaults to 0.0.0.0, except multinode
    /// control-plane boots default to the configured dataplane endpoint.
    #[arg(long, global = true)]
    pub bind_address: Option<String>,

    /// Enable anonymous requests as system:anonymous [default: true].
    #[arg(
        long = "anonymous-auth",
        global = true,
        action = clap::ArgAction::Set,
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = clap::value_parser!(bool)
    )]
    pub anonymous_auth: Option<bool>,
}

#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Start the klights service as the single-node seed leader.
    Start,

    /// Start as the single-node seed leader.
    Leader,

    /// Join as a raft learner (off-quorum, full leader-class stack).
    /// Internally maps to `Controlplane { as_learner: true }`. The node
    /// receives `cluster.db` via raft snapshot + AppendEntries and runs
    /// the full leader-class stack (API server, kubelet, networking) but
    /// does not vote in raft elections and does not run controllers.
    Replica {
        /// Leader API endpoints. Repeat `--leader` and/or use comma-separated
        /// values (`--leader a,b,c`) to pre-seed HA membership hints (1 entry
        /// for N=1, up to 3 for N=3). Capped at `controlplane_limit()`.
        #[arg(long, num_args = 1.., required = true, value_delimiter = ',')]
        leader: Vec<String>,

        /// Kubernetes bootstrap token used to authenticate the node join.
        #[arg(long, env = "KLIGHTS_JOIN_TOKEN")]
        token: Option<String>,

        /// File containing the Kubernetes bootstrap token. Prefer this over
        /// --token to keep tokens out of process arguments.
        #[arg(long)]
        token_file: Option<std::path::PathBuf>,

        /// Skip TLS CA verification for the leader bootstrap connection.
        /// Insecure; use only when the bearer token is delivered out of band.
        #[arg(long)]
        skip_ca: bool,
    },

    /// Join as worker-only node (kubelet/networking/heartbeat only)
    Worker {
        /// Leader API endpoints. Repeat `--leader` and/or use comma-separated
        /// values (`--leader a,b,c`) to pre-seed HA membership hints (1 entry
        /// for N=1, up to 3 for N=3). Capped at `controlplane_limit()`.
        #[arg(long, num_args = 1.., required = true, value_delimiter = ',')]
        leader: Vec<String>,

        /// Kubernetes bootstrap token used to authenticate the initial node
        /// join (CSR bootstrap). Optional if the node already has a persisted
        /// client certificate from a prior join.
        #[arg(long, env = "KLIGHTS_JOIN_TOKEN")]
        token: Option<String>,

        /// File containing the Kubernetes bootstrap token. Prefer this over
        /// --token to keep tokens out of process arguments.
        #[arg(long)]
        token_file: Option<std::path::PathBuf>,

        /// Skip TLS CA verification for the leader bootstrap connection.
        /// Insecure; use only when the bearer token is delivered out of band.
        #[arg(long)]
        skip_ca: bool,
    },

    /// Start (or join) as a Raft control-plane voter (Phase 3).
    ///
    /// - With no `--leader`: seed mode. Boots a single-node Raft cluster
    ///   (N=1) on this host; this node is trivially elected leader.
    ///   Functionally equivalent to `klights start` / `klights leader`
    ///   at this point — the only difference is intent: an operator who
    ///   types `klights controlplane` is signalling they expect more
    ///   voters to join later.
    /// - With `--leader X[,Y,Z]`: join mode. Sends `JoinAsControlplane`
    ///   to the listed peers; the existing leader runs `add_voter` and
    ///   the cluster grows N → N+1. `--token` is optional — omit it
    ///   when rejoining a cluster where this node already has raft state
    ///   (the admin cert provides mTLS identity).
    Controlplane {
        /// Optional list of existing Raft voter endpoints to join.
        /// Empty = seed mode. Comma-separated or repeated. Capped at
        /// `controlplane_limit()` entries.
        #[arg(long, num_args = 1.., value_delimiter = ',')]
        leader: Vec<String>,

        /// Kubernetes bootstrap token. Required in join mode (when
        /// `--leader` is provided); optional in seed mode (the seed
        /// creates worker/controlplane bootstrap token Secrets).
        #[arg(long, env = "KLIGHTS_JOIN_TOKEN")]
        token: Option<String>,

        /// File containing the Kubernetes bootstrap token. Prefer this over
        /// --token to keep tokens out of process arguments.
        #[arg(long)]
        token_file: Option<std::path::PathBuf>,

        /// Skip TLS CA verification for the leader bootstrap connection.
        /// Only valid when this control-plane node is joining an existing
        /// leader with --leader; seed-mode leader startup must never accept it.
        #[arg(long, requires = "leader")]
        skip_ca: bool,

        /// Join the existing raft cluster as a learner (openraft
        /// `add_learner`) instead of a voter. Learners receive
        /// `AppendEntries` and apply commits through the same state
        /// machine as voters but do not count toward quorum and do not
        /// vote. Requires `--leader` (seed mode has no cluster to learn
        /// from).
        #[arg(long)]
        as_learner: bool,
    },

    /// Gracefully stop a running klights service; pods continue running
    Stop,

    /// Full teardown: stop containers, clean up networking and data (requires stop first)
    Cleanup,

    /// Print resolved data root path and exit (for script consumption)
    GetDataRoot,
}

/// Validate the user-supplied leader endpoint list against Path A's
/// membership cap. Empty lists are rejected because clap requires at least
/// one entry, but the check is duplicated here so non-CLI callers also fail
/// fast.
fn validate_leader_endpoints(endpoints: &[String]) -> Result<(), String> {
    if endpoints.is_empty() {
        return Err("at least one --leader endpoint is required".into());
    }
    let limit = controlplane_limit();
    if endpoints.len() > limit {
        return Err(format!(
            "at most {limit} --leader endpoints allowed (got {})",
            endpoints.len()
        ));
    }
    Ok(())
}

impl Cli {
    /// Parse from `std::env::args()`.
    pub fn from_args() -> Self {
        Self::parse()
    }

    pub fn token_file(&self) -> Option<std::path::PathBuf> {
        match &self.command {
            Some(Command::Replica { token_file, .. })
            | Some(Command::Worker { token_file, .. })
            | Some(Command::Controlplane { token_file, .. }) => token_file.clone(),
            _ => None,
        }
    }

    /// Resolve the internal `NodeRole` from the parsed command.
    /// Returns `None` for non-runtime commands (stop, cleanup, get-data-root).
    /// Returns an error for Phase 3 commands that are not yet supported.
    pub fn node_role(&self) -> Result<Option<NodeRole>, String> {
        match &self.command {
            Some(Command::Start) => Ok(Some(NodeRole::Leader {
                bootstrap: LeaderBootstrap::Seed,
            })),
            Some(Command::Leader) => Ok(Some(NodeRole::Leader {
                bootstrap: LeaderBootstrap::Seed,
            })),
            Some(Command::Replica {
                leader,
                token,
                token_file,
                skip_ca,
            }) => {
                // Replicas-as-learners: `klights replica` is sugar for
                // `klights controlplane --leader X --token-file T --as-learner`.
                // The off-quorum BackupApplier path is gone; the node
                // joins raft as a learner and receives cluster.db via
                // snapshot + AppendEntries.
                validate_leader_endpoints(leader)?;
                if token.is_none() && token_file.is_none() {
                    return Err("replica requires --token or --token-file".to_string());
                }
                Ok(Some(NodeRole::Controlplane {
                    leader_endpoints: leader.clone(),
                    token: token.clone(),
                    skip_ca: *skip_ca,
                    as_learner: true,
                }))
            }
            Some(Command::Worker {
                leader,
                token,
                token_file: _,
                skip_ca,
            }) => {
                validate_leader_endpoints(leader)?;
                Ok(Some(NodeRole::Worker {
                    leader_endpoints: leader.clone(),
                    token: token.clone(),
                    skip_ca: *skip_ca,
                }))
            }
            Some(Command::Controlplane {
                leader,
                token,
                token_file: _,
                skip_ca,
                as_learner,
            }) => {
                // Seed mode: no --leader. Join mode: --leader X[,Y,Z]
                // (validate cap). A token source is required for first join,
                // but rejoin may omit it and authenticate with the persisted
                // node client certificate.
                if !leader.is_empty() {
                    validate_leader_endpoints(leader)?;
                } else if *as_learner {
                    return Err(
                        "--as-learner requires --leader (seed mode has no cluster to learn from)"
                            .to_string(),
                    );
                } else if *skip_ca {
                    return Err("--skip-ca requires --leader; seed-mode leader startup must not disable leader TLS verification".to_string());
                }
                Ok(Some(NodeRole::Controlplane {
                    leader_endpoints: leader.clone(),
                    token: token.clone(),
                    skip_ca: *skip_ca,
                    as_learner: *as_learner,
                }))
            }
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

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

        fn remove(name: &'static str) -> Self {
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
    fn no_args_returns_none_command() {
        let cli = Cli::try_parse_from(["klights"]);
        assert!(cli.is_ok(), "klights with no args should parse OK");
        let cli = cli.unwrap();
        assert!(cli.command.is_none(), "no subcommand should yield None");
        assert_eq!(cli.namespace, "klights");
        assert_eq!(
            cli.anonymous_auth, None,
            "absent CLI flag must leave the config/env default in force"
        );
    }

    #[test]
    fn anonymous_auth_global_flag_accepts_false() {
        let cli = Cli::try_parse_from(["klights", "--anonymous-auth=false", "start"]).unwrap();

        assert!(
            matches!(cli.anonymous_auth, Some(false)),
            "--anonymous-auth=false must disable anonymous requests"
        );
    }

    #[test]
    fn start_subcommand() {
        let cli = Cli::try_parse_from(["klights", "start"]).unwrap();
        assert_eq!(cli.command, Some(Command::Start));
    }

    #[test]
    fn leader_subcommand() {
        let cli = Cli::try_parse_from(["klights", "leader"]).unwrap();
        assert_eq!(cli.command, Some(Command::Leader));
    }

    #[test]
    fn replica_subcommand_with_args() {
        let cli = Cli::try_parse_from([
            "klights",
            "replica",
            "--leader",
            "https://192.168.1.10:7679",
            "--token",
            "abc123",
        ])
        .unwrap();
        match cli.command {
            Some(Command::Replica {
                leader,
                token,
                token_file,
                skip_ca,
            }) => {
                assert_eq!(leader.as_slice(), ["https://192.168.1.10:7679"]);
                assert_eq!(token.as_deref(), Some("abc123"));
                assert!(token_file.is_none());
                assert!(!skip_ca);
            }
            other => panic!("expected Replica, got {:?}", other),
        }
    }

    #[test]
    fn replica_missing_leader_fails() {
        let result = Cli::try_parse_from(["klights", "replica", "--token", "abc123"]);
        assert!(result.is_err(), "replica without --leader should fail");
    }

    #[test]
    fn replica_missing_token_fails() {
        let _lock = crate::TEST_ENV_LOCK.lock().unwrap();
        let _env = EnvVarGuard::remove("KLIGHTS_JOIN_TOKEN");
        let result = Cli::try_parse_from([
            "klights",
            "replica",
            "--leader",
            "https://192.168.1.10:7679",
        ]);
        let cli = result.expect("replica without token parses before role validation");
        assert!(
            cli.node_role().unwrap_err().contains("requires --token"),
            "replica without --token or --token-file should fail role validation"
        );
    }

    #[test]
    fn worker_subcommand_with_args() {
        let cli = Cli::try_parse_from([
            "klights",
            "worker",
            "--leader",
            "https://192.168.1.10:7679",
            "--token",
            "def456",
        ])
        .unwrap();
        match cli.command {
            Some(Command::Worker {
                leader,
                token,
                token_file,
                skip_ca,
            }) => {
                assert_eq!(leader.as_slice(), ["https://192.168.1.10:7679"]);
                assert_eq!(token.as_deref(), Some("def456"));
                assert!(token_file.is_none());
                assert!(!skip_ca);
            }
            other => panic!("expected Worker, got {:?}", other),
        }
    }

    #[test]
    fn worker_skip_ca_sets_node_role_flag() {
        let cli = Cli::try_parse_from([
            "klights",
            "worker",
            "--leader",
            "https://192.168.1.10:7679",
            "--token",
            "def456",
            "--skip-ca",
        ])
        .unwrap();
        match &cli.command {
            Some(Command::Worker {
                leader,
                token,
                token_file,
                skip_ca,
            }) => {
                assert_eq!(leader.as_slice(), ["https://192.168.1.10:7679"]);
                assert_eq!(token.as_deref(), Some("def456"));
                assert!(token_file.is_none());
                assert!(*skip_ca);
            }
            other => panic!("expected Worker, got {:?}", other),
        }

        assert_eq!(
            cli.node_role().unwrap().unwrap(),
            NodeRole::Worker {
                leader_endpoints: vec!["https://192.168.1.10:7679".into()],
                token: Some("def456".into()),
                skip_ca: true,
            }
        );
    }

    #[test]
    fn replica_skip_ca_sets_node_role_flag() {
        let cli = Cli::try_parse_from([
            "klights",
            "replica",
            "--leader",
            "https://192.168.1.10:7679",
            "--token",
            "abc123",
            "--skip-ca",
        ])
        .unwrap();
        match &cli.command {
            Some(Command::Replica {
                leader,
                token,
                token_file,
                skip_ca,
            }) => {
                assert_eq!(leader.as_slice(), ["https://192.168.1.10:7679"]);
                assert_eq!(token.as_deref(), Some("abc123"));
                assert!(token_file.is_none());
                assert!(*skip_ca);
            }
            other => panic!("expected Replica, got {:?}", other),
        }

        assert_eq!(
            cli.node_role().unwrap().unwrap(),
            NodeRole::Controlplane {
                leader_endpoints: vec!["https://192.168.1.10:7679".into()],
                token: Some("abc123".into()),
                skip_ca: true,
                as_learner: true,
            }
        );
    }

    #[test]
    fn worker_token_can_come_from_env() {
        let _lock = crate::TEST_ENV_LOCK.lock().unwrap();
        let _env = EnvVarGuard::set("KLIGHTS_JOIN_TOKEN", "env-token");
        let cli =
            Cli::try_parse_from(["klights", "worker", "--leader", "https://192.168.1.10:7679"])
                .unwrap();
        match cli.command {
            Some(Command::Worker {
                leader,
                token,
                token_file,
                skip_ca,
            }) => {
                assert_eq!(leader, vec!["https://192.168.1.10:7679".to_string()]);
                assert_eq!(token.as_deref(), Some("env-token"));
                assert!(token_file.is_none());
                assert!(!skip_ca);
            }
            other => panic!("expected Worker, got {:?}", other),
        }
    }

    #[test]
    fn worker_token_file_path_is_carried_separately_from_token_argument() {
        let token_file = tempfile::NamedTempFile::new().unwrap();
        let token_path = token_file.path().to_string_lossy().to_string();

        let cli = Cli::try_parse_from([
            "klights",
            "worker",
            "--leader",
            "https://192.168.1.10:7679",
            "--token",
            "arg-token",
            "--token-file",
            &token_path,
        ])
        .unwrap();

        assert_eq!(cli.token_file().as_deref(), Some(token_file.path()));
        assert_eq!(cli.node_role().unwrap().unwrap().token(), Some("arg-token"));
    }

    #[test]
    fn controlplane_token_file_path_allows_tokenless_first_join_mapping() {
        let token_file = tempfile::NamedTempFile::new().unwrap();
        let token_path = token_file.path().to_string_lossy().to_string();

        let cli = Cli::try_parse_from([
            "klights",
            "controlplane",
            "--leader",
            "https://192.168.1.10:7679",
            "--token-file",
            &token_path,
        ])
        .unwrap();

        assert_eq!(cli.token_file().as_deref(), Some(token_file.path()));
        assert_eq!(cli.node_role().unwrap().unwrap().token(), None);
    }

    #[test]
    fn worker_missing_leader_fails() {
        let result = Cli::try_parse_from(["klights", "worker", "--token", "abc123"]);
        assert!(result.is_err(), "worker without --leader should fail");
    }

    #[test]
    fn worker_without_token_parses_ok() {
        let _lock = crate::TEST_ENV_LOCK.lock().unwrap();
        let _env = EnvVarGuard::remove("KLIGHTS_JOIN_TOKEN");
        let cli =
            Cli::try_parse_from(["klights", "worker", "--leader", "https://192.168.1.10:7679"])
                .expect("worker without --token should parse OK");
        match &cli.command {
            Some(Command::Worker {
                leader,
                token,
                token_file,
                skip_ca,
            }) => {
                assert_eq!(leader.as_slice(), ["https://192.168.1.10:7679"]);
                assert!(token.is_none(), "token should be None when not provided");
                assert!(token_file.is_none());
                assert!(!skip_ca);
            }
            other => panic!("expected Worker, got {:?}", other),
        }
        let role = cli.node_role().unwrap().expect("worker resolves");
        assert_eq!(
            role,
            NodeRole::Worker {
                leader_endpoints: vec!["https://192.168.1.10:7679".into()],
                token: None,
                skip_ca: false,
            }
        );
    }

    #[test]
    fn controlplane_subcommand_parses_seed_form_no_args() {
        let _guard = crate::TEST_ENV_LOCK.lock().unwrap();
        let _g = EnvVarGuard::remove("KLIGHTS_JOIN_TOKEN");
        let cli = Cli::try_parse_from(["klights", "controlplane"]).unwrap();
        match cli.command {
            Some(Command::Controlplane {
                leader,
                token,
                token_file,
                skip_ca,
                as_learner,
            }) => {
                assert!(leader.is_empty(), "seed mode = no --leader");
                assert!(token.is_none(), "seed mode = no --token required");
                assert!(token_file.is_none());
                assert!(!skip_ca);
                assert!(!as_learner);
            }
            other => panic!("expected Controlplane, got {:?}", other),
        }
        drop(_g);
    }

    #[test]
    fn controlplane_seed_resolves_to_seed_node_role() {
        let _guard = crate::TEST_ENV_LOCK.lock().unwrap();
        let _g = EnvVarGuard::remove("KLIGHTS_JOIN_TOKEN");
        let cli = Cli::try_parse_from(["klights", "controlplane"]).unwrap();
        let role = cli
            .node_role()
            .unwrap()
            .expect("seed controlplane resolves");
        assert!(matches!(
            role,
            NodeRole::Controlplane {
                ref leader_endpoints,
                token: None,
                skip_ca: false,
                as_learner: false,
            } if leader_endpoints.is_empty()
        ));
        assert!(role.is_controlplane_seed());
        assert!(role.runs_full_stack());
        drop(_g);
    }

    #[test]
    fn controlplane_seed_rejects_skip_ca() {
        let _guard = crate::TEST_ENV_LOCK.lock().unwrap();
        let _g = EnvVarGuard::remove("KLIGHTS_JOIN_TOKEN");
        let result = Cli::try_parse_from(["klights", "controlplane", "--skip-ca"]);

        assert!(
            result.is_err(),
            "seed-mode controlplane is leader startup and must not accept --skip-ca"
        );
        drop(_g);
    }

    #[test]
    fn controlplane_subcommand_parses_join_form_comma_separated() {
        let _guard = crate::TEST_ENV_LOCK.lock().unwrap();
        let _g = EnvVarGuard::remove("KLIGHTS_JOIN_TOKEN");
        let cli = Cli::try_parse_from([
            "klights",
            "controlplane",
            "--leader",
            "https://a:7679,https://b:7679",
            "--token",
            "tok",
        ])
        .unwrap();
        match cli.command {
            Some(Command::Controlplane {
                leader,
                token,
                token_file,
                skip_ca,
                as_learner,
            }) => {
                assert_eq!(
                    leader,
                    vec!["https://a:7679".to_string(), "https://b:7679".to_string()]
                );
                assert_eq!(token.as_deref(), Some("tok"));
                assert!(token_file.is_none());
                assert!(!skip_ca);
                assert!(!as_learner);
            }
            other => panic!("expected Controlplane, got {:?}", other),
        }
        drop(_g);
    }

    #[test]
    fn controlplane_join_skip_ca_sets_node_role_flag() {
        let _guard = crate::TEST_ENV_LOCK.lock().unwrap();
        let _g = EnvVarGuard::remove("KLIGHTS_JOIN_TOKEN");
        let cli = Cli::try_parse_from([
            "klights",
            "controlplane",
            "--leader",
            "https://seed:7679",
            "--token",
            "tok",
            "--skip-ca",
        ])
        .unwrap();
        let role = cli
            .node_role()
            .unwrap()
            .expect("joining controlplane resolves");

        assert!(matches!(
            role,
            NodeRole::Controlplane {
                ref leader_endpoints,
                token: Some(ref token),
                skip_ca: true,
                as_learner: false,
            } if leader_endpoints.as_slice() == ["https://seed:7679"] && token == "tok"
        ));
        drop(_g);
    }

    #[test]
    fn controlplane_join_resolves_to_join_node_role_with_token() {
        let _guard = crate::TEST_ENV_LOCK.lock().unwrap();
        let _g = EnvVarGuard::remove("KLIGHTS_JOIN_TOKEN");
        let cli = Cli::try_parse_from([
            "klights",
            "controlplane",
            "--leader",
            "https://seed:7679",
            "--token",
            "tok",
        ])
        .unwrap();
        let role = cli
            .node_role()
            .unwrap()
            .expect("join controlplane resolves");
        assert!(role.is_controlplane_join());
        assert_eq!(role.leader_endpoint(), Some("https://seed:7679"));
        assert_eq!(role.token(), Some("tok"));
        drop(_g);
    }

    #[test]
    fn controlplane_join_without_token_resolves_for_persisted_cert_rejoin() {
        let _guard = crate::TEST_ENV_LOCK.lock().unwrap();
        let _g = EnvVarGuard::remove("KLIGHTS_JOIN_TOKEN");
        let cli = Cli::try_parse_from(["klights", "controlplane", "--leader", "https://seed:7679"])
            .unwrap();
        let role = cli
            .node_role()
            .unwrap()
            .expect("tokenless controlplane rejoin resolves");
        assert!(role.is_controlplane_join());
        assert_eq!(role.leader_endpoint(), Some("https://seed:7679"));
        assert_eq!(role.token(), None);
        drop(_g);
    }

    #[test]
    fn controlplane_join_with_as_learner_flag_marks_learner_join() {
        // Replicas-as-learners: `klights controlplane --leader X --token-file T
        // --as-learner` joins the existing raft cluster as a learner
        // (openraft add_learner) instead of a voter (add_voter). Voter
        // count and quorum are unchanged; the node receives a snapshot
        // and AppendEntries the same way voters do.
        let _guard = crate::TEST_ENV_LOCK.lock().unwrap();
        let _g = EnvVarGuard::remove("KLIGHTS_JOIN_TOKEN");
        let cli = Cli::try_parse_from([
            "klights",
            "controlplane",
            "--leader",
            "https://seed:7679",
            "--token",
            "tok",
            "--as-learner",
        ])
        .unwrap();
        let role = cli
            .node_role()
            .unwrap()
            .expect("controlplane --as-learner resolves to a NodeRole");
        assert!(role.is_controlplane_join());
        assert!(
            role.is_learner_join(),
            "--as-learner must mark the role as a learner join"
        );
        drop(_g);
    }

    #[test]
    fn controlplane_join_without_as_learner_defaults_to_voter() {
        let _guard = crate::TEST_ENV_LOCK.lock().unwrap();
        let _g = EnvVarGuard::remove("KLIGHTS_JOIN_TOKEN");
        let cli = Cli::try_parse_from([
            "klights",
            "controlplane",
            "--leader",
            "https://seed:7679",
            "--token",
            "tok",
        ])
        .unwrap();
        let role = cli.node_role().unwrap().unwrap();
        assert!(role.is_controlplane_join());
        assert!(
            !role.is_learner_join(),
            "omitting --as-learner must default to voter join"
        );
        drop(_g);
    }

    #[test]
    fn controlplane_seed_cannot_be_learner() {
        // Seed boots an N=1 cluster — it's trivially the voter and
        // leader of itself. --as-learner alongside seed (no --leader)
        // is incoherent: there is no existing cluster to learn from.
        let _guard = crate::TEST_ENV_LOCK.lock().unwrap();
        let _g = EnvVarGuard::remove("KLIGHTS_JOIN_TOKEN");
        let cli = Cli::try_parse_from(["klights", "controlplane", "--as-learner"]).unwrap();
        let err = cli.node_role().unwrap_err();
        assert!(
            err.to_lowercase().contains("learner"),
            "seed + --as-learner must error, got: {err}"
        );
        drop(_g);
    }

    #[test]
    fn controlplane_rejects_more_than_three_leader_endpoints() {
        let _guard = crate::TEST_ENV_LOCK.lock().unwrap();
        let _g = EnvVarGuard::remove("KLIGHTS_JOIN_TOKEN");
        let cli = Cli::try_parse_from([
            "klights",
            "controlplane",
            "--leader",
            "https://a:1,https://b:2,https://c:3,https://d:4",
            "--token",
            "tok",
        ])
        .unwrap();
        let err = cli.node_role().unwrap_err();
        assert!(err.contains("at most"), "cap-at-3 error expected: {err}");
        drop(_g);
    }

    #[test]
    fn stop_subcommand() {
        let cli = Cli::try_parse_from(["klights", "stop"]).unwrap();
        assert_eq!(cli.command, Some(Command::Stop));
    }

    #[test]
    fn cleanup_subcommand() {
        let cli = Cli::try_parse_from(["klights", "cleanup"]).unwrap();
        assert_eq!(cli.command, Some(Command::Cleanup));
    }

    #[test]
    fn rootless_flag() {
        let cli = Cli::try_parse_from(["klights", "--rootless", "start"]).unwrap();
        assert!(cli.rootless);
        assert_eq!(cli.command, Some(Command::Start));
    }

    #[test]
    fn namespace_flag() {
        let cli = Cli::try_parse_from(["klights", "--namespace", "klights-dev", "start"]).unwrap();
        assert_eq!(cli.namespace, "klights-dev");
    }

    #[test]
    fn namespace_default() {
        let cli = Cli::try_parse_from(["klights", "start"]).unwrap();
        assert_eq!(cli.namespace, "klights");
    }

    #[test]
    fn unknown_subcommand_fails() {
        let cli = Cli::try_parse_from(["klights", "unknown"]);
        assert!(cli.is_err());
    }

    #[test]
    fn start_with_rootless_and_namespace() {
        let cli =
            Cli::try_parse_from(["klights", "--rootless", "--namespace", "custom-ns", "start"])
                .unwrap();
        assert!(cli.rootless);
        assert_eq!(cli.namespace, "custom-ns");
        assert_eq!(cli.command, Some(Command::Start));
    }

    #[test]
    fn start_resolves_to_seed_leader_role() {
        let cli = Cli::try_parse_from(["klights", "start"]).unwrap();
        let role = cli
            .node_role()
            .unwrap()
            .expect("start must resolve to a NodeRole");
        assert_eq!(
            role,
            NodeRole::Leader {
                bootstrap: LeaderBootstrap::Seed,
            }
        );
    }

    #[test]
    fn stop_does_not_resolve_to_role() {
        let cli = Cli::try_parse_from(["klights", "stop"]).unwrap();
        assert!(cli.node_role().unwrap().is_none());
    }

    #[test]
    fn cleanup_does_not_resolve_to_role() {
        let cli = Cli::try_parse_from(["klights", "cleanup"]).unwrap();
        assert!(cli.node_role().unwrap().is_none());
    }

    #[test]
    fn get_data_root_does_not_resolve_to_role() {
        let cli = Cli::try_parse_from(["klights", "get-data-root"]).unwrap();
        assert!(cli.node_role().unwrap().is_none());
    }

    #[test]
    fn start_role_runs_full_stack_as_seed_leader() {
        let cli = Cli::try_parse_from(["klights", "start"]).unwrap();
        let role = cli.node_role().unwrap().unwrap();
        assert!(role.runs_full_stack());
        assert!(!role.requires_leader());
        assert!(!role.requires_token());
    }

    #[test]
    fn leader_resolves_to_leader_role() {
        let cli = Cli::try_parse_from(["klights", "leader"]).unwrap();
        let role = cli
            .node_role()
            .unwrap()
            .expect("leader must resolve to a NodeRole");
        assert_eq!(
            role,
            NodeRole::Leader {
                bootstrap: LeaderBootstrap::Seed,
            }
        );
    }

    #[test]
    fn replica_resolves_to_controlplane_learner_role() {
        // Replicas-as-learners: `klights replica` is sugar for
        // `klights controlplane --leader X --token-file T --as-learner`. The
        // node boots the full leader-class runtime, joins the raft
        // cluster as a learner, and its cluster.db is populated by
        // raft snapshot + AppendEntries — no BackupApplier.
        let cli = Cli::try_parse_from([
            "klights",
            "replica",
            "--leader",
            "https://192.0.2.4:7679",
            "--token",
            "tok",
        ])
        .unwrap();
        let role = cli.node_role().unwrap().expect("replica must resolve");
        assert_eq!(
            role,
            NodeRole::Controlplane {
                leader_endpoints: vec!["https://192.0.2.4:7679".into()],
                token: Some("tok".into()),
                skip_ca: false,
                as_learner: true,
            }
        );
        assert!(role.is_controlplane_join());
        assert!(role.is_learner_join());
    }

    #[test]
    fn worker_resolves_to_worker_role() {
        let cli = Cli::try_parse_from([
            "klights",
            "worker",
            "--leader",
            "https://192.0.2.4:7679",
            "--token",
            "tok",
        ])
        .unwrap();
        let role = cli.node_role().unwrap().expect("worker must resolve");
        assert_eq!(
            role,
            NodeRole::Worker {
                leader_endpoints: vec!["https://192.0.2.4:7679".into()],
                token: Some("tok".into()),
                skip_ca: false,
            }
        );
    }

    #[test]
    fn worker_accepts_three_leader_endpoints() {
        let cli = Cli::try_parse_from([
            "klights",
            "worker",
            "--leader",
            "https://leader-a:7679",
            "--leader",
            "https://leader-b:7679",
            "--leader",
            "https://leader-c:7679",
            "--token",
            "tok",
        ])
        .unwrap();
        let role = cli.node_role().unwrap().expect("worker must resolve");
        if let NodeRole::Worker {
            leader_endpoints, ..
        } = role
        {
            assert_eq!(leader_endpoints.len(), 3);
        } else {
            panic!("expected Worker");
        }
    }

    #[test]
    fn worker_accepts_comma_separated_leader_endpoints() {
        let cli = Cli::try_parse_from([
            "klights",
            "worker",
            "--leader",
            "https://a:7679,https://b:7679,https://c:7679",
            "--token",
            "tok",
        ])
        .unwrap();
        let role = cli.node_role().unwrap().expect("worker must resolve");
        if let NodeRole::Worker {
            leader_endpoints, ..
        } = role
        {
            assert_eq!(
                leader_endpoints,
                vec![
                    "https://a:7679".to_string(),
                    "https://b:7679".to_string(),
                    "https://c:7679".to_string(),
                ]
            );
        } else {
            panic!("expected Worker");
        }
    }

    #[test]
    fn worker_accepts_mixed_comma_and_repeated_leader_flags() {
        let cli = Cli::try_parse_from([
            "klights",
            "worker",
            "--leader",
            "https://a:7679,https://b:7679",
            "--leader",
            "https://c:7679",
            "--token",
            "tok",
        ])
        .unwrap();
        let role = cli.node_role().unwrap().expect("worker must resolve");
        if let NodeRole::Worker {
            leader_endpoints, ..
        } = role
        {
            assert_eq!(leader_endpoints.len(), 3);
            assert_eq!(leader_endpoints[0], "https://a:7679");
            assert_eq!(leader_endpoints[1], "https://b:7679");
            assert_eq!(leader_endpoints[2], "https://c:7679");
        } else {
            panic!("expected Worker");
        }
    }

    #[test]
    fn worker_rejects_more_than_three_leader_endpoints() {
        let cli = Cli::try_parse_from([
            "klights", "worker", "--leader", "a", "--leader", "b", "--leader", "c", "--leader",
            "d", "--token", "tok",
        ])
        .unwrap();
        let err = cli.node_role().unwrap_err();
        assert!(
            err.contains("at most"),
            "expected cap-at-3 error, got: {err}"
        );
    }

    #[test]
    fn start_command_parses_without_replication_mode() {
        let _guard = crate::TEST_ENV_LOCK.lock().unwrap();
        let cli = Cli::try_parse_from(["klights", "start"]).unwrap();
        assert!(matches!(cli.command, Some(Command::Start)));
    }

    #[test]
    fn get_data_root_subcommand() {
        let cli = Cli::try_parse_from(["klights", "get-data-root"]).unwrap();
        assert_eq!(cli.command, Some(Command::GetDataRoot));
    }

    #[test]
    fn get_data_root_with_namespace() {
        let cli =
            Cli::try_parse_from(["klights", "--namespace", "test-ns", "get-data-root"]).unwrap();
        assert_eq!(cli.namespace, "test-ns");
        assert_eq!(cli.command, Some(Command::GetDataRoot));
    }

    #[test]
    fn cli_version_uses_git_tag_build_version() {
        let command = Cli::command();
        let version = command.get_version().expect("--version must be set");
        assert!(
            version.starts_with(crate::version::GIT_VERSION),
            "--version must start with the K8s-compatible git version, got {version:?}"
        );
        assert!(
            version.ends_with(crate::version::GIT_COMMIT_SHORT),
            "--version must end with the short commit hash, got {version:?}"
        );
        assert_eq!(
            version,
            format!(
                "{} {}",
                crate::version::GIT_VERSION,
                crate::version::GIT_COMMIT_SHORT
            )
        );
    }
}
