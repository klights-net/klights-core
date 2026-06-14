//! Internal node role for multi-node Phase 2 (T5).
//!
//! T5 collapses the bootstrap surface to **two** runtime entry points:
//!
//! - `Leader { bootstrap }` -> leader / full-stack runtime in `runtime.rs`.
//! - `Worker { leader_endpoints, token, skip_ca }` → worker runtime in `worker_runtime.rs`.
//!
//! `replica` is CLI sugar for `Controlplane { as_learner: true }`. It is never
//! a Worker flag.
//!
//! `LeaderBootstrap` describes how a Leader's Raft state machine initializes:
//! `Seed` (N=1), `Bootstrap { peers }` (N=3 from scratch), `Join { endpoints }`
//! (AddVoter into existing cluster). Path A: every Leader runs Raft from day
//! one, so the variant always exists; the actual Raft apply path lands in T15.

use anyhow::Result;

/// Maximum number of controlplane voters in the cluster.
/// Configurable via `KLIGHTS_CONTROLPLANE_LIMIT` env var (default: 3).
pub fn controlplane_limit() -> usize {
    std::env::var("KLIGHTS_CONTROLPLANE_LIMIT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(3)
}

/// Internal node role decided at CLI parse time.
///
/// Marked `#[non_exhaustive]` so adding future variants
/// does not force every consumer to update its match arms immediately.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeRole {
    /// Full-stack control-plane node (always bundles a worker).
    /// Every Leader runs Raft; `bootstrap` only affects how the cluster
    /// initializes (Seed = N=1, Bootstrap = N=3, Join = AddVoter).
    Leader { bootstrap: LeaderBootstrap },

    /// Worker-only node. `skip_ca: true` disables TLS CA verification only
    /// when no leader CA path is known; a configured CA path always wins.
    /// Bearer-token authentication still applies.
    ///
    /// `token` is optional: required for the first join (CSR bootstrap),
    /// but not needed on subsequent starts when a persisted node client
    /// certificate exists. The runtime validates at startup that either a
    /// persisted cert or a token is available.
    ///
    /// `leader_endpoints` is plural to support HA membership without a
    /// future enum/CLI break: N=1 cluster populates one entry, N=3
    /// populates three. Capped at [`controlplane_limit()`].
    Worker {
        leader_endpoints: Vec<String>,
        token: Option<String>,
        skip_ca: bool,
    },

    /// Raft control-plane voter (Phase 3). Runs the full leader stack
    /// (API server, controllers, scheduler, kubelet, networking) and
    /// participates in the Raft quorum.
    ///
    /// - `leader_endpoints.is_empty()` → seed boot. `bootstrap_single_voter`
    ///   forms an N=1 cluster; this node is trivially elected leader.
    /// - `leader_endpoints` non-empty → join boot. Sends `JoinAsControlplane`
    ///   to the configured peers; the existing Raft leader runs `add_voter`
    ///   (P3-10) and the cluster grows N → N+1.
    /// - `token` required in join mode (the gRPC bootstrap authenticates
    ///   against the existing leader). Optional in seed mode (the seed
    ///   creates worker/controlplane bootstrap token Secrets).
    Controlplane {
        leader_endpoints: Vec<String>,
        token: Option<String>,
        skip_ca: bool,
        /// True when joining as a raft learner (`add_learner`) instead
        /// of a voter (`add_voter`). Only meaningful in join mode
        /// (non-empty `leader_endpoints`); seed mode rejects the
        /// combination at CLI parse time.
        as_learner: bool,
    },
}

/// Raft initialization mode for a Leader node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaderBootstrap {
    /// Single-voter cluster (N=1). The default; matches today's solo behavior.
    Seed,
    /// Bootstrap a fresh N≥2 cluster from scratch with the listed peers.
    /// Capped at [`controlplane_limit()`] voters total (this node plus peers).
    Bootstrap { peers: Vec<LeaderPeer> },
    /// Join an existing cluster as a new voter via AddVoter at the listed endpoints.
    Join { endpoints: Vec<String> },
}

/// A peer voter in a Raft cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaderPeer {
    pub node_name: String,
    pub endpoint: String,
}

impl NodeRole {
    /// Returns the role selected by the given CLI command string.
    ///
    /// Primarily used in tests; production code resolves via `Cli::node_role()`.
    pub fn from_command(command: &str) -> Result<Self> {
        match command {
            "start" => Ok(NodeRole::Leader {
                bootstrap: LeaderBootstrap::Seed,
            }),
            _ => Err(anyhow::anyhow!("unknown command: {}", command)),
        }
    }

    /// Returns true if this role runs the full single-node stack
    /// (API server, controllers, scheduler, kubelet, networking).
    /// Controlplane is leader-class — it runs the full stack and
    /// participates in Raft consensus.
    pub fn runs_full_stack(&self) -> bool {
        matches!(
            self,
            NodeRole::Leader { .. } | NodeRole::Controlplane { .. }
        )
    }

    /// Returns true if this role requires a leader endpoint to join.
    /// Controlplane in join mode (non-empty `leader_endpoints`) requires
    /// one; seed-mode controlplane does not.
    pub fn requires_leader(&self) -> bool {
        match self {
            NodeRole::Worker { .. } => true,
            NodeRole::Controlplane {
                leader_endpoints, ..
            } => !leader_endpoints.is_empty(),
            _ => false,
        }
    }

    /// Returns true if this role requires a bootstrap token.
    /// Worker token is optional (persisted cert can substitute).
    /// Controlplane in join mode requires one; seed-mode controlplane
    /// creates worker/controlplane bootstrap token Secrets.
    pub fn requires_token(&self) -> bool {
        match self {
            NodeRole::Worker { .. } => false,
            NodeRole::Controlplane {
                leader_endpoints, ..
            } => !leader_endpoints.is_empty(),
            _ => false,
        }
    }

    /// Returns the first leader endpoint configured for a Worker /
    /// joining Controlplane, if any. Seed-mode Controlplane returns
    /// None.
    pub fn leader_endpoint(&self) -> Option<&str> {
        match self {
            NodeRole::Worker {
                leader_endpoints, ..
            }
            | NodeRole::Controlplane {
                leader_endpoints, ..
            } => leader_endpoints.first().map(String::as_str),
            _ => None,
        }
    }

    /// Returns the bootstrap token if this role carries one.
    pub fn token(&self) -> Option<&str> {
        match self {
            NodeRole::Worker { token, .. } => token.as_deref(),
            NodeRole::Controlplane { token, .. } => token.as_deref(),
            _ => None,
        }
    }

    /// Returns true if this is a Controlplane seed boot (no peers).
    pub fn is_controlplane_seed(&self) -> bool {
        matches!(
            self,
            NodeRole::Controlplane {
                leader_endpoints,
                ..
            } if leader_endpoints.is_empty()
        )
    }

    /// Returns true if this is a Controlplane join boot (has peers).
    pub fn is_controlplane_join(&self) -> bool {
        matches!(
            self,
            NodeRole::Controlplane {
                leader_endpoints,
                ..
            } if !leader_endpoints.is_empty()
        )
    }

    /// Returns true if this role is joining as a raft learner
    /// (`add_learner`) rather than a voter (`add_voter`). Only true for
    /// `NodeRole::Controlplane` with `as_learner: true` AND a non-empty
    /// `leader_endpoints` (the CLI rejects the combination otherwise,
    /// so this check is defensive).
    pub fn is_learner_join(&self) -> bool {
        matches!(
            self,
            NodeRole::Controlplane {
                leader_endpoints,
                as_learner: true,
                ..
            } if !leader_endpoints.is_empty()
        )
    }

    /// Returns true if this Worker / Controlplane should skip TLS CA
    /// verification while connecting to the leader bootstrap endpoint.
    pub fn skips_leader_ca_verification(&self) -> bool {
        matches!(
            self,
            NodeRole::Worker { skip_ca: true, .. } | NodeRole::Controlplane { skip_ca: true, .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn worker_with(endpoints: Vec<&str>) -> NodeRole {
        NodeRole::Worker {
            leader_endpoints: endpoints.into_iter().map(String::from).collect(),
            token: Some("tok".into()),
            skip_ca: false,
        }
    }

    #[test]
    fn start_command_yields_seed_leader() {
        let role = NodeRole::from_command("start").unwrap();
        assert_eq!(
            role,
            NodeRole::Leader {
                bootstrap: LeaderBootstrap::Seed,
            }
        );
    }

    #[test]
    fn unknown_command_yields_error() {
        let err = NodeRole::from_command("nonexistent").unwrap_err();
        assert!(err.to_string().contains("unknown command"));
    }

    #[test]
    fn leader_runs_full_stack() {
        assert!(
            NodeRole::Leader {
                bootstrap: LeaderBootstrap::Seed,
            }
            .runs_full_stack()
        );
    }

    #[test]
    fn worker_default_does_not_run_full_stack() {
        let role = worker_with(vec!["https://leader:7679"]);
        assert!(!role.runs_full_stack());
        assert!(role.requires_leader());
        assert!(!role.requires_token(), "worker token is optional");
        assert_eq!(role.leader_endpoint(), Some("https://leader:7679"));
        assert_eq!(role.token(), Some("tok"));
    }

    // ----- Role enum invariants (T5 closing gate) -----
    //
    // Enforced compile-time by exhaustive matches and
    // construction-with-named-fields, plus serde-shaped round-trip tests
    // that fail loudly if a variant is renamed or its field shape
    // changes. Adding a new variant fails the exhaustive match below
    // (because the enum is `#[non_exhaustive]` only across crates;
    // internally a new variant requires updating each match arm).

    /// Compile-time exhaustive match: adding any new `NodeRole` variant
    /// without updating this match fails to compile. The function's
    /// only purpose is to gate the enum shape.
    fn _exhaustive_node_role_match(role: &NodeRole) -> &'static str {
        match role {
            NodeRole::Leader { bootstrap: _ } => "Leader",
            NodeRole::Worker {
                leader_endpoints: _,
                token: _,
                skip_ca: _,
            } => "Worker",
            NodeRole::Controlplane {
                leader_endpoints: _,
                token: _,
                skip_ca: _,
                as_learner: _,
            } => "Controlplane",
        }
    }

    /// Compile-time exhaustive match for `LeaderBootstrap`. Same
    /// rationale as above: adding `LeaderBootstrap::FuturePath` without
    /// updating this match fails to compile.
    fn _exhaustive_leader_bootstrap_match(b: &LeaderBootstrap) -> &'static str {
        match b {
            LeaderBootstrap::Seed => "Seed",
            LeaderBootstrap::Bootstrap { peers: _ } => "Bootstrap",
            LeaderBootstrap::Join { endpoints: _ } => "Join",
        }
    }

    #[test]
    fn node_role_construct_each_variant_with_documented_fields() {
        // Construction with named fields fails to compile if a field is
        // renamed or its type changes (e.g., `leader_endpoints:
        // Vec<String>` → `String`). Replaces the prior text-scan guards
        // for Worker endpoint shape and leader bootstrap shape.
        let _ = NodeRole::Leader {
            bootstrap: LeaderBootstrap::Seed,
        };
        let _ = NodeRole::Worker {
            leader_endpoints: Vec::<String>::new(),
            token: None,
            skip_ca: false,
        };
        let _ = NodeRole::Controlplane {
            leader_endpoints: Vec::<String>::new(),
            token: None,
            skip_ca: false,
            as_learner: false,
        };
    }

    #[test]
    fn controlplane_seed_runs_full_stack_and_has_no_peers() {
        let seed = NodeRole::Controlplane {
            leader_endpoints: Vec::new(),
            token: None,
            skip_ca: false,
            as_learner: false,
        };
        assert!(seed.runs_full_stack());
        assert!(seed.is_controlplane_seed());
        assert!(!seed.is_controlplane_join());
        assert!(!seed.requires_leader());
        assert!(!seed.requires_token());
        assert_eq!(seed.leader_endpoint(), None);
        assert_eq!(seed.token(), None);
        assert!(!seed.skips_leader_ca_verification());
    }

    #[test]
    fn controlplane_join_requires_leader_and_token() {
        let join = NodeRole::Controlplane {
            leader_endpoints: vec!["https://seed:7679".to_string()],
            token: Some("tok".to_string()),
            skip_ca: false,
            as_learner: false,
        };
        assert!(join.runs_full_stack());
        assert!(!join.is_controlplane_seed());
        assert!(join.is_controlplane_join());
        assert!(join.requires_leader());
        assert!(join.requires_token());
        assert_eq!(join.leader_endpoint(), Some("https://seed:7679"));
        assert_eq!(join.token(), Some("tok"));
    }

    #[test]
    fn controlplane_skip_ca_flag_round_trips() {
        let join = NodeRole::Controlplane {
            leader_endpoints: vec!["https://seed:7679".to_string()],
            token: Some("tok".to_string()),
            skip_ca: true,
            as_learner: false,
        };
        assert!(join.skips_leader_ca_verification());
    }

    #[test]
    fn controlplane_join_as_learner_round_trips_through_is_learner_join() {
        let join = NodeRole::Controlplane {
            leader_endpoints: vec!["https://seed:7679".to_string()],
            token: Some("tok".to_string()),
            skip_ca: false,
            as_learner: true,
        };
        assert!(join.is_controlplane_join());
        assert!(join.is_learner_join());
    }

    #[test]
    fn controlplane_join_default_is_voter_not_learner() {
        let join = NodeRole::Controlplane {
            leader_endpoints: vec!["https://seed:7679".to_string()],
            token: Some("tok".to_string()),
            skip_ca: false,
            as_learner: false,
        };
        assert!(!join.is_learner_join());
    }

    #[test]
    fn controlplane_seed_is_never_learner_join() {
        let seed = NodeRole::Controlplane {
            leader_endpoints: Vec::new(),
            token: None,
            skip_ca: false,
            as_learner: false,
        };
        assert!(!seed.is_learner_join());
    }

    #[test]
    fn node_role_has_no_separate_replica_variant() {
        // Replicas-as-learners (T1.6 cleanup): the off-quorum backup
        // path was deleted along with the Worker.replica flag. A
        // replica is now `NodeRole::Controlplane { as_learner: true }`,
        // not a Worker variant. If a `NodeRole::Replica { .. }` variant
        // is reintroduced, the exhaustive match in
        // `_exhaustive_node_role_match` above fails to compile, AND
        // any production `match role` that doesn't handle it also
        // fails — so we get the bell at compile time first.
        for role in [
            NodeRole::Leader {
                bootstrap: LeaderBootstrap::Seed,
            },
            NodeRole::Worker {
                leader_endpoints: vec!["e".into()],
                token: Some("t".into()),
                skip_ca: false,
            },
            NodeRole::Controlplane {
                leader_endpoints: vec!["e".into()],
                token: Some("t".into()),
                skip_ca: false,
                as_learner: true,
            },
        ] {
            // Every variant must classify cleanly through the existing
            // helpers.
            let _ = role.runs_full_stack();
            let _ = role.is_learner_join();
        }
    }

    #[test]
    fn controlplane_limit_defaults_to_three() {
        unsafe { std::env::remove_var("KLIGHTS_CONTROLPLANE_LIMIT") };
        assert_eq!(controlplane_limit(), 3);
    }

    #[test]
    fn controlplane_limit_reads_env_var() {
        unsafe { std::env::set_var("KLIGHTS_CONTROLPLANE_LIMIT", "5") };
        assert_eq!(controlplane_limit(), 5);
        unsafe { std::env::remove_var("KLIGHTS_CONTROLPLANE_LIMIT") };
    }

    #[test]
    fn controlplane_limit_ignores_invalid_env() {
        unsafe { std::env::set_var("KLIGHTS_CONTROLPLANE_LIMIT", "not-a-number") };
        assert_eq!(controlplane_limit(), 3);
        unsafe { std::env::remove_var("KLIGHTS_CONTROLPLANE_LIMIT") };
    }

    #[test]
    fn leader_bootstrap_variants_round_trip() {
        // T5 ships the variants; T15 wires them into Raft. The shape must
        // already exist so T15 doesn't require an enum break.
        let _ = LeaderBootstrap::Seed;
        let _ = LeaderBootstrap::Bootstrap {
            peers: vec![LeaderPeer {
                node_name: "n2".into(),
                endpoint: "https://n2:7679".into(),
            }],
        };
        let _ = LeaderBootstrap::Join {
            endpoints: vec!["https://n1:7679".into()],
        };
    }
}
