pub mod admission;
mod allocator;
pub mod api;
pub mod api_discovery;
pub mod api_pod_subresources;
pub mod api_status;
pub mod auth;
pub mod cli;
pub mod cni_plugin;
pub mod control_plane;
pub mod controller;
pub mod controller_dispatcher;
pub mod controllers;
pub mod datastore;
pub mod gc;
pub mod json_patch;
pub mod kubelet;
pub mod label_selector;
pub mod leader_election;
pub(crate) mod leader_tls_policy;
pub mod log_apply;
pub mod networking;
pub mod node_admin;
pub mod node_lease_tracker;
pub mod paths;
pub mod pidfile;
pub mod pod_identity;
pub mod portforward;
pub mod protobuf;
pub mod replication;
pub mod resource_semantics;
pub mod scheduler;
pub mod shutdown;
pub mod side_effects;
pub mod spdy;
pub mod task_supervisor;
pub mod utils;
pub mod version;
pub mod watch;

#[cfg(test)]
mod api_handler_tests;
#[cfg(test)]
mod api_serialization_tests;
#[cfg(test)]
#[cfg(test)]
mod crd_status_tests;
#[cfg(test)]
mod crd_tests;
#[cfg(test)]
mod cronjob_event_driven_scheduler_tests;
// Deployment script invariants are covered by the base-repo source guard run
// as part of `./build.sh`.

mod bootstrap;

#[cfg(test)]
mod deployment_replicaset_error_test;
#[cfg(test)]
mod node_conditions_tests;
#[cfg(test)]
mod resource_quota_event_driven_tests;
#[cfg(test)]
mod resourcequota_tests;
#[cfg(test)]
mod shutdown_test;

pub use bootstrap::config::DbEncryption;
pub use bootstrap::config::KlightsConfig;

#[cfg(test)]
pub(crate) static TEST_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn cli_flags_for_runtime(cli: cli::Cli) -> Result<Option<bootstrap::CliFlags>, String> {
    let role = match cli.node_role()? {
        Some(role) => role,
        None => return Ok(None),
    };
    let token_file = cli.token_file();
    Ok(Some(bootstrap::CliFlags {
        rootless: cli.rootless,
        namespace: Some(cli.namespace),
        bind_address: cli.bind_address,
        token_file,
        role,
    }))
}

pub fn main_entry() {
    // CNI plugin mode — invoked by containerd, not the user.
    if std::env::var_os("CNI_COMMAND").is_some() {
        std::process::exit(cni_plugin::run_from_env());
    }

    if std::env::args_os().nth(1).as_deref()
        == Some(std::ffi::OsStr::new(
            kubelet::rootless_runc_wrapper::WRAPPER_MODE_ARG,
        ))
    {
        let args = std::env::args_os().skip(2).collect();
        std::process::exit(kubelet::rootless_runc_wrapper::run_from_args(args));
    }

    let cli = cli::Cli::from_args();
    let is_runtime_command = matches!(
        &cli.command,
        Some(cli::Command::Start)
            | Some(cli::Command::Leader)
            | Some(cli::Command::Replica { .. })
            | Some(cli::Command::Worker { .. })
            | Some(cli::Command::Controlplane { .. })
    );
    if is_runtime_command {
        // Long-lived role: turn on jemalloc's background purge thread so
        // RSS returns to the OS after load spikes. Short-lived CNI/exec/
        // wrapper invocations above never reach here, so they pay no
        // purge-thread cost.
        allocator::enable_background_purge();
        let flags = match cli_flags_for_runtime(cli) {
            Ok(Some(flags)) => flags,
            Ok(None) => unreachable!("runtime command resolved to no runtime flags"),
            Err(err) => {
                eprintln!("klights: {err}");
                std::process::exit(2);
            }
        };
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("failed to build tokio runtime");
        let result = runtime.block_on(bootstrap::runtime::run_with_flags(flags));
        runtime.shutdown_background();
        let exit_code = match result {
            Ok(()) => 0,
            Err(err) => {
                tracing::error!("klights exiting: {:#}", err);
                1
            }
        };
        std::process::exit(exit_code);
    }

    match cli.command {
        None => {
            // clap auto-prints help when no subcommand given, but we
            // also print our own help and exit non-zero.
            let _ = clap::Command::new("klights")
                .about("lightweight Kubernetes in Rust")
                .subcommand_required(true)
                .arg(
                    clap::Arg::new("rootless")
                        .long("rootless")
                        .help("Run in rootless mode")
                        .global(true),
                )
                .arg(
                    clap::Arg::new("namespace")
                        .long("namespace")
                        .help("Containerd namespace")
                        .default_value("klights")
                        .global(true),
                )
                .subcommand(clap::Command::new("start").about("Start the klights service"))
                .subcommand(clap::Command::new("stop").about("Gracefully stop the klights service"))
                .subcommand(
                    clap::Command::new("cleanup")
                        .about("Terminate all containers and clean up networking/data"),
                )
                .subcommand(
                    clap::Command::new("get-data-root")
                        .about("Print resolved data root path and exit"),
                )
                .print_help();
            std::process::exit(1);
        }
        Some(cli::Command::Start) => {
            let cli_rootless = cli.rootless;
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("failed to build tokio runtime");
            let result =
                runtime.block_on(bootstrap::runtime::run_with_flags(bootstrap::CliFlags {
                    rootless: cli_rootless,
                    namespace: Some(cli.namespace),
                    bind_address: cli.bind_address,
                    token_file: None,
                    role: bootstrap::NodeRole::Leader {
                        bootstrap: bootstrap::node_role::LeaderBootstrap::Seed,
                    },
                }));
            runtime.shutdown_background();
            let exit_code = match result {
                Ok(()) => 0,
                Err(err) => {
                    tracing::error!("klights exiting: {:#}", err);
                    1
                }
            };
            std::process::exit(exit_code);
        }
        Some(cli::Command::Leader) => {
            let cli_rootless = cli.rootless;
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("failed to build tokio runtime");
            let result =
                runtime.block_on(bootstrap::runtime::run_with_flags(bootstrap::CliFlags {
                    rootless: cli_rootless,
                    namespace: Some(cli.namespace),
                    bind_address: cli.bind_address,
                    token_file: None,
                    role: bootstrap::NodeRole::Leader {
                        bootstrap: bootstrap::node_role::LeaderBootstrap::Seed,
                    },
                }));
            runtime.shutdown_background();
            let exit_code = match result {
                Ok(()) => 0,
                Err(err) => {
                    tracing::error!("klights leader exiting: {:#}", err);
                    1
                }
            };
            std::process::exit(exit_code);
        }
        Some(cli::Command::Replica {
            leader,
            token,
            token_file,
            skip_ca,
        }) => {
            let cli_rootless = cli.rootless;
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("failed to build tokio runtime");
            let result =
                runtime.block_on(bootstrap::runtime::run_with_flags(bootstrap::CliFlags {
                    rootless: cli_rootless,
                    namespace: Some(cli.namespace),
                    bind_address: cli.bind_address,
                    token_file,
                    // Replicas-as-learners: `klights replica` is sugar
                    // for `klights controlplane --leader X --token-file T
                    // --as-learner`. The node boots the full leader-
                    // class runtime and joins raft as a learner.
                    role: bootstrap::NodeRole::Controlplane {
                        leader_endpoints: leader,
                        token,
                        skip_ca,
                        as_learner: true,
                    },
                }));
            runtime.shutdown_background();
            let exit_code = match result {
                Ok(()) => 0,
                Err(err) => {
                    tracing::error!("klights replica exiting: {:#}", err);
                    1
                }
            };
            std::process::exit(exit_code);
        }
        Some(cli::Command::Worker {
            leader,
            token,
            token_file,
            skip_ca,
        }) => {
            let cli_rootless = cli.rootless;
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("failed to build tokio runtime");
            let result =
                runtime.block_on(bootstrap::runtime::run_with_flags(bootstrap::CliFlags {
                    rootless: cli_rootless,
                    namespace: Some(cli.namespace),
                    bind_address: cli.bind_address,
                    token_file,
                    role: bootstrap::NodeRole::Worker {
                        leader_endpoints: leader,
                        token,
                        skip_ca,
                    },
                }));
            runtime.shutdown_background();
            let exit_code = match result {
                Ok(()) => 0,
                Err(err) => {
                    tracing::error!("klights worker exiting: {:#}", err);
                    1
                }
            };
            std::process::exit(exit_code);
        }
        Some(cli::Command::Controlplane {
            leader,
            token,
            token_file,
            skip_ca,
            as_learner,
        }) => {
            let cli_rootless = cli.rootless;
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("failed to build tokio runtime");
            let result =
                runtime.block_on(bootstrap::runtime::run_with_flags(bootstrap::CliFlags {
                    rootless: cli_rootless,
                    namespace: Some(cli.namespace),
                    bind_address: cli.bind_address,
                    token_file,
                    role: bootstrap::NodeRole::Controlplane {
                        leader_endpoints: leader,
                        token,
                        skip_ca,
                        as_learner,
                    },
                }));
            runtime.shutdown_background();
            let exit_code = match result {
                Ok(()) => 0,
                Err(err) => {
                    tracing::error!("klights controlplane exiting: {:#}", err);
                    1
                }
            };
            std::process::exit(exit_code);
        }
        Some(cli::Command::Stop) => {
            let pid_path = pidfile::default_pid_path(&cli.namespace);
            if !pidfile::is_running(&pid_path) {
                eprintln!(
                    "klights: no running daemon found for namespace '{}'",
                    cli.namespace
                );
                std::process::exit(1);
            }
            let pid = pidfile::read(&pid_path).unwrap();
            // Send SIGTERM to the daemon — this triggers the soft shutdown.
            // SAFETY: kill(2) with a positive PID and SIGTERM is always safe to call.
            unsafe {
                libc::kill(pid as i32, libc::SIGTERM);
            }
            println!("Sent SIGTERM to klights daemon (pid {})", pid);
        }
        Some(cli::Command::Cleanup) => {
            let pid_path = pidfile::default_pid_path(&cli.namespace);
            if pidfile::is_running(&pid_path) {
                eprintln!(
                    "klights: daemon is still running for namespace '{}' — run 'klights stop' first",
                    cli.namespace
                );
                std::process::exit(1);
            }

            let ns = cli.namespace.clone();
            let cli_rootless = cli.rootless;
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("failed to build tokio runtime");
            let result = runtime.block_on(bootstrap::runtime::run_cleanup_with_flags(
                bootstrap::CliFlags {
                    rootless: cli_rootless,
                    namespace: Some(ns),
                    bind_address: cli.bind_address,
                    token_file: None,
                    role: bootstrap::NodeRole::Leader {
                        bootstrap: bootstrap::node_role::LeaderBootstrap::Seed,
                    },
                },
            ));
            runtime.shutdown_background();
            let exit_code = match result {
                Ok(()) => 0,
                Err(err) => {
                    tracing::error!("klights cleanup failed: {:#}", err);
                    1
                }
            };
            std::process::exit(exit_code);
        }
        Some(cli::Command::GetDataRoot) => {
            let root = paths::data_root_path(&cli.namespace);
            println!("{}", root.display());
        }
    }
}

#[cfg(test)]
mod cli_runtime_mapping_tests {
    use super::*;
    use clap::Parser;

    struct EnvVarGuard {
        name: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl EnvVarGuard {
        fn remove(name: &'static str) -> Self {
            let previous = std::env::var_os(name);
            unsafe { std::env::remove_var(name) };
            Self { name, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(value) = &self.previous {
                unsafe { std::env::set_var(self.name, value) };
            } else {
                unsafe { std::env::remove_var(self.name) };
            }
        }
    }

    #[test]
    fn runtime_mapping_allows_controlplane_rejoin_without_token() {
        let _lock = crate::TEST_ENV_LOCK.lock().unwrap();
        let _env = EnvVarGuard::remove("KLIGHTS_JOIN_TOKEN");
        let cli =
            cli::Cli::try_parse_from(["klights", "controlplane", "--leader", "https://seed:7679"])
                .unwrap();

        let flags = cli_flags_for_runtime(cli)
            .unwrap()
            .expect("tokenless controlplane rejoin is a runtime command");
        assert!(flags.role.is_controlplane_join());
        assert_eq!(flags.role.token(), None);
    }
}
