//! Phase 5: Datastore and replication wiring.
//!
//! Opens the cluster database, initializes cluster metadata, sets up
//! replication service, node-local store, outbox dispatcher, and node admin.

use std::sync::Arc;

use anyhow::{Context, Result};
use tokio_util::sync::CancellationToken;

use crate::KlightsConfig;
use crate::bootstrap::NodeRole;
use crate::datastore::DatastoreHandle;
use crate::datastore::replicated::RaftProposer;
use crate::task_supervisor::TaskSupervisor;

pub struct OpenLeaderArgs<'a> {
    pub config: &'a Arc<KlightsConfig>,
    pub role: &'a NodeRole,
    pub supervisor: Arc<TaskSupervisor>,
    pub grpc_transport_policy:
        crate::replication::grpc::transport_policy::SharedGrpcTransportPolicy,
    pub shutdown_token: CancellationToken,
    pub is_leader_rx: tokio::sync::watch::Receiver<bool>,
    pub local_dataplane: crate::replication::grpc::client::JoinDataplaneMetadata,
    pub node_ip: &'a str,
}

pub struct DatastorePhase {
    pub db_handle: DatastoreHandle,
    pub cluster_api: Arc<dyn crate::control_plane::client::LeaderApiClient>,
    pub remote_api_client: Option<Arc<crate::control_plane::client::remote::RemoteApiClient>>,
    /// The concrete leader-side LocalApiClient that the outbox dispatcher
    /// uses as its apply client. Must be reused (not re-created) by later
    /// bootstrap phases so that `set_controller_dispatcher` lands on the
    /// same instance the outbox calls into — otherwise pod-status side
    /// effects (RS/Service reconcile) silently no-op.
    pub local_api_client: Arc<crate::control_plane::client::local::LocalApiClient>,
    pub replication_service: Option<Arc<crate::replication::ReplicationService>>,
    pub node_local: crate::datastore::node_local::handle::NodeLocalHandle,
    pub outbox: Arc<crate::kubelet::outbox::Outbox>,
    pub node_lease_tracker: Arc<crate::node_lease_tracker::NodeLeaseTracker>,
    /// P3-11c: when this node is a leader-class boot under raft mode,
    /// `raft_node` holds the live `RaftNode` so later phases (kubelet
    /// label task — Step D — and the gRPC server's RaftRpcRouter /
    /// ControlplaneJoinHandler — Step E) can subscribe to its state.
    /// `None` for non-raft leader topology boots or workers.
    pub raft_node: Option<Arc<crate::datastore::raft::node::RaftNode>>,
    /// True when this leader-class boot is a joining Raft controlplane.
    /// Later phases use this to skip seed-only bootstrap writes (default
    /// namespaces, RBAC, node registration) because raft delivers that
    /// state via install_snapshot / AppendEntries instead.
    pub skip_seed_bootstrap: bool,
    /// Lease renew fallback for control-plane joiners while follower.
    /// Used by heartbeat to send `renew_node_lease` RPCs to the leader.
    pub control_plane_lease_client:
        Option<Arc<crate::replication::grpc::client::ReplicationGrpcClient>>,
}

struct RemoteForwarderParts {
    forwarder: Arc<dyn crate::control_plane::client::LeaderApiClient>,
    remote_api_client: Option<Arc<crate::control_plane::client::remote::RemoteApiClient>>,
    lease_client: Option<Arc<crate::replication::grpc::client::ReplicationGrpcClient>>,
}

/// Leader datastore wiring: local DB + replication + outbox + node admin.
pub async fn open_leader(args: OpenLeaderArgs<'_>) -> Result<DatastorePhase> {
    let OpenLeaderArgs {
        config,
        role,
        supervisor,
        grpc_transport_policy,
        shutdown_token,
        is_leader_rx,
        local_dataplane,
        node_ip,
    } = args;
    use crate::bootstrap::init::predicates::uses_leader_runtime;

    let replication_observer = crate::datastore::replicated::ReplicationObserver::new();
    let db_handle = crate::datastore::selector::open_raft_cluster(
        config,
        supervisor.clone(),
        Some(replication_observer.clone()),
    )
    .await
    .context("Failed to open datastore")?;

    // T1.6: joining controlplanes get their initial cluster.db
    // contents from raft (install_snapshot or AppendEntries from index 0
    // applied through `state_machine::apply` → `apply_log_apply_commit`).
    // There is no separate Phase A catch-up: BackupApplier ran a second
    // applier pass over the same RVs the raft log already covered, and
    // the duplicate writes collided on UNIQUE(watch_events.resource_version)
    // when raft AppendEntries reached the joiner. With Phase A removed,
    // the joiner starts with an empty cluster.db, JoinAsControlplane
    // triggers the seed's add_voter, and raft (or legacy leader-follower
    // replication for historical deployments) populates the full state
    // through its own snapshot+AppendEntries / follower replay path.
    let is_joining_controlplane = role.is_controlplane_join();

    let node_lease_tracker = Arc::new(crate::node_lease_tracker::NodeLeaseTracker::new());
    // T6 step 4: LocalApiClient now consumes the real `is_leader_rx`
    // from runtime.rs. The bootstrap shape watcher in `bootstrap.rs`
    // refreshes the value from `Raft::metrics()` on every membership /
    // leadership change. Step 1's inner gate uses this to refuse non-
    // leader writes (defense in depth alongside the switching proxy
    // below).
    let local_api_client = Arc::new(
        crate::control_plane::client::local::LocalApiClient::new_with_node_lease_tracker_and_containerd_namespace(
            db_handle.clone(),
            config.node_name.clone(),
            config.containerd_namespace.clone(),
            node_lease_tracker.clone(),
            is_leader_rx.clone(),
        ),
    );
    // T6 step 4: every leader-class boot's `cluster_api` is the
    // switching proxy from step 3 — never a bare LocalApiClient. The
    // proxy dispatches application reads/watches to the elected leader
    // target, and writes to local only when this node is the elected
    // leader.
    //
    // T6 step 4b: control-plane members build a real gRPC forwarder
    // using `RemoteApiClient` so follower writes reach the elected
    // leader's API server. Joining nodes seed it from `--leader`;
    // seed controlplanes initialize it to their own advertised endpoint
    // and the raft-shape watcher updates it to the current leader
    // address on every metrics change.
    let controlplane_remote_identity =
        controlplane_remote_client_identity_for_role(config, role, supervisor.clone()).await?;
    let remote_parts = build_remote_forwarder(
        config,
        role,
        supervisor.clone(),
        controlplane_remote_identity.clone(),
        local_dataplane.clone(),
        grpc_transport_policy.clone(),
    );
    let cluster_api: Arc<dyn crate::control_plane::client::LeaderApiClient> = Arc::new(
        crate::control_plane::client::leader_proxy::LeaderProxyApiClient::new(
            local_api_client.clone(),
            remote_parts.forwarder,
            is_leader_rx.clone(),
        ),
    );

    // For joining controlplanes, cluster metadata is delivered by the
    // replication layer (raft install_snapshot/apply path in raft mode, or
    // follower replay path in legacy deployments); seed local cluster.db
    // writes would create split-brain metadata and must be skipped.
    //
    // T7.1: seed bootstrap metadata (cluster_id, leader_epoch) is now
    // proposed through raft after the RaftNode is initialized and the
    // single-voter bootstrap completes. The direct ensure_cluster_metadata
    // and write_cluster_membership calls have been moved below to the raft
    // seed-init block so every cluster.db mutation is raft-backed.

    let replication_service = if uses_leader_runtime(role) {
        let service = Arc::new(crate::replication::ReplicationService::new(
            db_handle.clone(),
            supervisor.clone(),
        ));
        let svc = service.clone();
        replication_observer
            .set(Arc::new(move |command, meta| {
                svc.notify_entry(crate::replication::protocol::ReplicationEntry { command, meta });
            }))
            .await;
        Some(service)
    } else {
        None
    };

    let node_local_db_path: Option<&std::path::Path> = if config.in_memory {
        None
    } else {
        Some(config.node_db_path.as_path())
    };
    let (node_local, node_local_sqlite) =
        crate::datastore::node_local::selector::open_node_local_with_sqlite(
            config.node_local_backend,
            node_local_db_path,
            supervisor.clone(),
            config.db_key_file.as_deref(),
            "sqlite:node-local",
        )
        .await
        .context("Failed to open node-local datastore")?;

    // P3-11c: when running in raft mode on a leader-class boot,
    // construct the RaftNode that backs this voter. Solo seed boots
    // (klights start / leader, or controlplane with no --leader) call
    // bootstrap_single_voter and immediately become the elected leader
    // of an N=1 cluster. Joining controlplanes (controlplane with
    // --leader) skip that call and stay as Learners until the seed's
    // add_voter (driven by JoinAsControlplane in Step E) folds them in.
    // T2 step 1: raft is always on. Every leader-class boot constructs
    // a RaftNode. Single-node deployments run a single-voter raft cluster
    // (quorum = 1). Workers use SingleNode and route writes through the
    // outbox/leader proxy.
    let raft_node = match (role, node_local_sqlite.as_ref()) {
        (r, Some(sqlite)) if uses_leader_runtime(r) => {
            let node_id =
                crate::datastore::raft::types::raft_node_id_for_node_name(&config.node_name);
            let advertise_addr = format!(
                "https://{}:{}",
                config
                    .external_endpoint
                    .clone()
                    .unwrap_or_else(|| "127.0.0.1".to_string()),
                config.tls_port
            );
            // P3-11c3: wire the production GrpcRaftNetwork so the
            // seed's add_voter (driven by JoinAsControlplane) can
            // replicate AppendEntries to joining voters. Per-peer
            // clients are minted on demand via
            // `ReplicationGrpcRaftClientFactory`.
            //
            // skip_ca scope: only used before a joining control-plane has
            // the cluster CA on disk. Authentication is still mTLS-only;
            // bootstrap tokens are limited to CSR signing.
            let ca_path = crate::paths::ca_cert_path(&config.containerd_namespace);
            let (ca_cert_path, skip_ca) = if ca_path.exists() {
                (Some(ca_path), false)
            } else {
                (None, true)
            };
            let raft_identity = controlplane_remote_identity.clone();
            if raft_identity.client_cert_pem.is_none() || raft_identity.client_key_pem.is_none() {
                anyhow::bail!(
                    "control-plane raft transport requires a client certificate; bootstrap token is only valid for CSR signing"
                );
            }
            let raft_template =
                crate::bootstrap::raft_transport::ReplicationGrpcRaftClientTemplate {
                    node_name: config.node_name.clone(),
                    token: String::new(),
                    ca_cert_path,
                    skip_ca,
                    client_cert_pem: raft_identity.client_cert_pem,
                    client_key_pem: raft_identity.client_key_pem,
                    dataplane: local_dataplane.clone(),
                    transport_policy: grpc_transport_policy.clone(),
                };
            let raft_factory = Arc::new(
                crate::bootstrap::raft_transport::ReplicationGrpcRaftClientFactory::new(
                    supervisor.clone(),
                    raft_template,
                ),
            );
            let is_join = matches!(
                role,
                crate::bootstrap::NodeRole::Controlplane { leader_endpoints, .. }
                    if !leader_endpoints.is_empty()
            );
            let raft_network =
                crate::datastore::raft::grpc_network::GrpcRaftNetwork::new(raft_factory);
            let raft = Arc::new(
                crate::datastore::raft::node::RaftNode::start_with_network(
                    node_id,
                    config.node_name.clone(),
                    db_handle.clone(),
                    sqlite.clone(),
                    raft_network,
                )
                .await
                .context("Failed to start RaftNode for leader-class boot")?,
            );
            // Seed branch: no --leader on Controlplane (or any
            // seed Leader boot) bootstraps as the sole voter.
            // Joining controlplane (--leader non-empty) skips this and
            // waits for the seed's add_voter to fold it in.
            //
            // T6 step 4: bind the RaftNode into the cluster datastore
            // wrapper on EVERY leader-class boot (seed + joiners), not
            // just the seed. Three layers of protection make a follower
            // proposer call structurally safe:
            //   1. cluster_api dispatches non-leader writes to the
            //      switching proxy's remote arm (step 3), never to local.
            //   2. LocalApiClient's inner gate (step 1) refuses local
            //      writes when `is_leader_rx=false`.
            //   3. If a write somehow reaches the proposer on a
            //      non-leader (defense-in-depth bug), openraft's
            //      `client_write` returns ForwardToLeader and refuses.
            //
            // With the proposer always bound, promotion is a pure state
            // flip — the moment the shape watcher sets `is_leader=true`,
            // the same instance starts accepting writes through the
            // raft path with no re-wiring. This is what the design doc
            // calls "always wired, gated by leadership state".
            let raft_proposer: Arc<dyn crate::datastore::replicated::RaftProposer> = raft.clone();
            db_handle.attach_raft_proposer(raft_proposer);
            if !is_join {
                raft.bootstrap_single_voter(advertise_addr.clone())
                    .await
                    .context("Failed to bootstrap_single_voter for solo raft seed")?;
                wait_for_seed_raft_local_commit_ready(raft.as_ref(), supervisor.as_ref())
                    .await
                    .context("Failed waiting for solo raft seed commit readiness")?;
                tracing::info!(
                    node_id,
                    node = %config.node_name,
                    addr = %advertise_addr,
                    "P3 raft: bootstrapped N=1 cluster (solo seed)"
                );

                // T7.1: route seed bootstrap metadata through raft after
                // the N=1 voter is initialized. Every cluster.db mutation
                // from this point forward is raft-backed.
                if !is_joining_controlplane && uses_leader_runtime(role) {
                    let cluster_id = uuid::Uuid::new_v4().to_string();
                    raft.propose_command(
                        crate::datastore::command::StorageCommand::EnsureClusterMetadata {
                            cluster_id: cluster_id.clone(),
                        },
                    )
                    .await
                    .context("Failed to propose EnsureClusterMetadata through raft")?;
                    tracing::info!(
                        cluster_id = %cluster_id,
                        "T7.1: cluster identity committed via raft"
                    );

                    let db_for_membership: &dyn crate::datastore::DatastoreBackend = &*db_handle;
                    crate::bootstrap::cluster_meta::write_cluster_membership(
                        db_for_membership,
                        &crate::control_plane::client::membership::ClusterMembership {
                            cluster_id,
                            voters: initial_voters_for_role(role, &config.node_name),
                            term: 0,
                            leader_hint: Some(config.node_name.clone()),
                        },
                    )
                    .await
                    .context("Failed to initialize cluster membership metadata")?;

                    // Bootstrap token Secret: create through the replicated
                    // datastore (now raft-backed since the proposer is attached).
                    crate::bootstrap::bootstrap_token::ensure_bootstrap_tokens(db_for_membership)
                        .await
                        .context("Failed to create bootstrap tokens")?;
                    tracing::info!("T7.1: bootstrap tokens created via raft-backed datastore");
                }
            } else if let crate::bootstrap::NodeRole::Controlplane {
                leader_endpoints,
                token,
                skip_ca,
                as_learner,
            } = r
            {
                tracing::info!(
                    node_id,
                    node = %config.node_name,
                    peers = ?leader_endpoints,
                    "P3 raft: controlplane join mode, spawning JoinAsControlplane task"
                );
                // Spawn a one-shot supervised task that, after a brief
                // delay so the local API server is up, sends
                // JoinAsControlplane to the configured peers. On
                // redirect_to_leader, retry against the redirected
                // URL; on denied (no leader), retry with backoff.
                let endpoints: Vec<String> = leader_endpoints.clone();
                let token: String = token.clone().unwrap_or_default();
                let external_endpoint = local_dataplane.endpoint.clone();
                let tls_port = config.tls_port;
                let node_name = config.node_name.clone();
                let node_internal_ip = node_ip.to_string();
                let join_supervisor = supervisor.clone();
                let join_dataplane = local_dataplane.clone();
                // The JoinAsControlplane RPC must trust the seed cluster's CA
                // before any cluster.db has been replicated. Honor a pre-seeded
                // KLIGHTS_LEADER_CA_CERT (same env var the worker join path
                // reads); the harness and operator-managed deploys both set
                // this. If we found a CA path, force skip_ca=false so the
                // gRPC client uses CaFile verification rather than SkipCa.
                let join_ca_cert_path =
                    crate::bootstrap::init::predicates::grpc_ca_cert_path_for_role(config, r);
                let skip_ca = if join_ca_cert_path.is_some() {
                    false
                } else {
                    *skip_ca
                };
                let as_learner = *as_learner;
                let join_namespace = config.containerd_namespace.clone();
                let join_supervisor_for_loop = join_supervisor.clone();
                let join_grpc_transport_policy = grpc_transport_policy.clone();
                // Capture handles the join task needs to attach the
                // RaftProposer to the datastore wrapper on success.
                let join_raft = raft.clone();
                let join_db_handle = db_handle.clone();
                supervisor
                    .spawn_delay(
                        "controlplane_join_task",
                        std::time::Duration::from_secs(3),
                        async move {
                            if external_endpoint.trim().is_empty() {
                                tracing::error!(
                                    "controlplane_join_task: KLIGHTS_EXTERNAL_ENDPOINT is required for raft/API transport advertisement"
                                );
                                return;
                            }
                            let my_addr = format!(
                                "https://{}:{}",
                                external_endpoint, tls_port
                            );
                            let client_identity =
                                match controlplane_join_client_identity_for_token(
                                    &token,
                                    &join_namespace,
                                    &node_name,
                                    join_supervisor_for_loop.clone(),
                                )
                                .await
                                {
                                    Ok(identity) => identity,
                                    Err(err) => {
                                        tracing::warn!(
                                            error = %err,
                                            "controlplane_join_task: cannot authenticate without bootstrap token"
                                        );
                                        return;
                                    }
                                };
                            let mut targets: Vec<String> = endpoints;
                            let mut attempt: u32 = 0;
                            const MAX_ATTEMPTS: u32 = 20;
                            while attempt < MAX_ATTEMPTS {
                                attempt += 1;
                                let Some(target) = targets.first().cloned() else {
                                    tracing::warn!(
                                        "controlplane_join_task: no peer endpoints left to try"
                                    );
                                    return;
                                };
                                let client = crate::replication::grpc::client::ReplicationGrpcClient::new(
                                    crate::replication::grpc::client::GrpcClientConfig {
                                        leader_endpoint: target.clone(),
                                        token: token.clone(),
                                        node_name: node_name.clone(),
                                        role: crate::replication::protocol::JoinRole::Worker,
                                        dataplane: join_dataplane.clone(),
                                        ca_cert_path: join_ca_cert_path.clone(),
                                        skip_ca,
                                        client_cert_pem: client_identity.client_cert_pem.clone(),
                                        client_key_pem: client_identity.client_key_pem.clone(),
                                    },
                                    join_supervisor_for_loop.clone(),
                                    join_grpc_transport_policy.clone(),
                                );
                                tracing::info!(
                                    attempt,
                                    target = %target,
                                    node_id,
                                    "controlplane_join_task: sending JoinAsControlplane"
                                );
                                match client
                                    .join_as_controlplane_rpc(
                                        node_id,
                                        &my_addr,
                                        &node_name,
                                        as_learner,
                                        &node_internal_ip,
                                    )
                                    .await
                                {
                                    Ok(crate::replication::grpc::raft_rpc::ControlplaneJoinOutcome::Accepted {
                                        voter_count_after,
                                        admitted_as_learner,
                                        ca_cert_pem,
                                        ..
                                    }) => {
                                        tracing::info!(
                                            voter_count_after,
                                            admitted_as_learner,
                                            "controlplane_join_task: accepted"
                                        );
                                        // Write CA material received from leader.
                                        // Always overwrite: the leader's CA is authoritative.
                                        if !ca_cert_pem.is_empty() {
                                            let ca_cert_path = crate::paths::ca_cert_path(&join_namespace);
                                            if let Err(e) = std::fs::write(&ca_cert_path, &ca_cert_pem) {
                                                tracing::warn!(error = %e, "failed to write ca.crt from join response");
                                            }
                                        }
                                        let _ = (join_raft, join_db_handle);
                                        return;
                                    }
                                    Ok(crate::replication::grpc::raft_rpc::ControlplaneJoinOutcome::RedirectToLeader {
                                        leader_id,
                                        leader_addr,
                                    }) => {
                                        tracing::info!(
                                            leader_id,
                                            leader_addr = %leader_addr,
                                            "controlplane_join_task: redirected"
                                        );
                                        targets = vec![leader_addr];
                                        continue;
                                    }
                                    Ok(crate::replication::grpc::raft_rpc::ControlplaneJoinOutcome::Denied {
                                        reason,
                                    }) => {
                                        tracing::warn!(
                                            reason = %reason,
                                            attempt,
                                            "controlplane_join_task: denied, retrying"
                                        );
                                    }
                                    Err(err) => {
                                        tracing::warn!(
                                            error = %err,
                                            error_debug = ?err,
                                            attempt,
                                            "controlplane_join_task: RPC failed, retrying"
                                        );
                                    }
                                }
                                // Supervised timer backoff: timeout against a
                                // never-resolving future parks this task until
                                // the Tokio timer fires; it does not poll.
                                let retry_delay = controlplane_join_retry_delay(attempt);
                                let _ = join_supervisor_for_loop
                                    .timeout(
                                        "controlplane_join_retry_wait",
                                        retry_delay,
                                        std::future::pending::<()>(),
                                    )
                                    .await;
                            }
                            tracing::error!(
                                attempt,
                                "controlplane_join_task: gave up after max attempts"
                            );
                        },
                    )
                    .await
                    .context("Failed to spawn controlplane_join_task")?;
            }
            Some(raft)
        }
        _ => None,
    };

    let outbox = {
        let notify = Arc::new(tokio::sync::Notify::new());
        let ob = Arc::new(crate::kubelet::outbox::Outbox::with_notify(
            node_local.clone(),
            notify.clone(),
        ));
        let apply_client: Arc<dyn crate::kubelet::outbox::OutboxApplyClient> = Arc::new(
            crate::kubelet::outbox::LeaderApiOutboxClient::new(cluster_api.clone()),
        );
        let node_local_for_retry = node_local.clone();
        let supervisor_for_retry = supervisor.clone();
        let notify_for_retry = notify.clone();
        let apply_client_for_retry = apply_client.clone();
        let shutdown_token_for_retry = shutdown_token.clone();
        match supervisor
            .spawn_async(
                crate::task_supervisor::TaskCategory::Background,
                "outbox_dispatcher_bootstrap_retry",
                async move {
                    let mut delay = std::time::Duration::from_millis(500);
                    let max_delay = std::time::Duration::from_secs(30);
                    loop {
                        let dispatcher = crate::kubelet::outbox::OutboxDispatcher::production(
                            node_local_for_retry.clone(),
                            apply_client_for_retry.clone(),
                            notify_for_retry.clone(),
                        );
                        match dispatcher
                            .start(
                                supervisor_for_retry.clone(),
                                shutdown_token_for_retry.clone(),
                            )
                            .await
                        {
                            Ok(_) => {
                                tracing::info!("Outbox dispatcher task started");
                                return;
                            }
                            Err(err) => {
                                tracing::warn!(
                                    error = %err,
                                    delay_ms = %delay.as_millis(),
                                    "outbox dispatcher start failed; retrying"
                                );
                                if supervisor_for_retry
                                    .timeout(
                                        "outbox_dispatcher_retry_wait",
                                        delay,
                                        std::future::pending::<()>(),
                                    )
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                                let next_delay_ms =
                                    (delay.as_millis() * 2).min(max_delay.as_millis());
                                delay = std::time::Duration::from_millis(
                                    next_delay_ms.try_into().unwrap_or(30_000),
                                );
                            }
                        }
                    }
                },
            )
            .await
        {
            Ok(_) => {
                tracing::debug!(
                    "Outbox dispatcher startup is being retried in background if needed"
                )
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "failed to spawn outbox dispatcher retry task; continuing with queued writes"
                )
            }
        }
        ob
    };

    match crate::node_admin::start_node_admin(
        node_local.clone(),
        supervisor.clone(),
        shutdown_token.clone(),
    )
    .await
    {
        Ok(_) => tracing::info!("Node admin server started"),
        Err(err) => tracing::warn!(error = %err, "failed to start node admin server"),
    }

    let skip_seed_bootstrap = is_joining_controlplane;

    Ok(DatastorePhase {
        db_handle,
        cluster_api,
        remote_api_client: remote_parts.remote_api_client,
        local_api_client,
        replication_service,
        node_local,
        outbox,
        node_lease_tracker,
        raft_node,
        skip_seed_bootstrap,
        control_plane_lease_client: remote_parts.lease_client,
    })
}

/// Derive a stable `u64` NodeId for the openraft engine from the
/// human-readable node name. FNV-1a 64-bit hash: deterministic across
/// processes for the same node_name and dependency-free, so voters
/// agree on each other's id without a separate id-allocation step.
/// T6 step 4b: build the switching proxy's `remote` arm for a
/// leader-class boot. Returns a real `RemoteApiClient` backed by
/// `ReplicationGrpcClient` for control-plane members. Joiners seed it
/// from their configured peer endpoints; seed controlplanes seed it
/// from their own advertised API endpoint and rely on the raft metrics
/// watcher to point it at the current leader after elections.
fn build_remote_forwarder(
    config: &Arc<KlightsConfig>,
    role: &NodeRole,
    supervisor: Arc<TaskSupervisor>,
    client_identity: ControlplaneJoinClientIdentity,
    local_dataplane: crate::replication::grpc::client::JoinDataplaneMetadata,
    grpc_transport_policy: crate::replication::grpc::transport_policy::SharedGrpcTransportPolicy,
) -> RemoteForwarderParts {
    use crate::control_plane::client::leader_proxy::StubRemoteForwarder;
    use crate::control_plane::client::remote::RemoteApiClient;
    use crate::replication::grpc::client::{GrpcClientConfig, ReplicationGrpcClient};

    let (endpoints, token, skip_ca) = match role {
        NodeRole::Controlplane {
            leader_endpoints,
            token,
            skip_ca,
            ..
        } if !leader_endpoints.is_empty() => (
            leader_endpoints.clone(),
            token.clone().unwrap_or_default(),
            *skip_ca,
        ),
        NodeRole::Controlplane {
            leader_endpoints, ..
        } if leader_endpoints.is_empty() => (
            vec![local_controlplane_grpc_endpoint(config)],
            String::new(),
            false,
        ),
        _ => {
            // Seed leader / worker: no HA control-plane
            // remote forwarding target is available in this role.
            return RemoteForwarderParts {
                forwarder: Arc::new(StubRemoteForwarder::new(config.node_name.clone())),
                remote_api_client: None,
                lease_client: None,
            };
        }
    };

    // Pick the first known endpoint as the initial gRPC target.
    // `set_all_leader_endpoints` registers the full list so the
    // reconnect loop cycles through them on stream failure.
    let initial = endpoints[0].clone();

    // CA cert path: honor pre-seeded leader-ca / namespace ca via the
    // same predicate the join task uses. `skip_ca` only matters when
    // no CA path is found.
    let ca_cert_path = crate::bootstrap::init::predicates::grpc_ca_cert_path_for_role(config, role);
    let effective_skip_ca = if ca_cert_path.is_some() {
        false
    } else {
        skip_ca
    };

    let grpc_config = GrpcClientConfig {
        leader_endpoint: initial,
        token,
        node_name: config.node_name.clone(),
        role: crate::replication::protocol::JoinRole::Worker,
        dataplane: local_dataplane,
        ca_cert_path,
        skip_ca: effective_skip_ca,
        client_cert_pem: client_identity.client_cert_pem,
        client_key_pem: client_identity.client_key_pem,
    };
    let grpc = Arc::new(ReplicationGrpcClient::new(
        grpc_config,
        supervisor.clone(),
        grpc_transport_policy,
    ));
    grpc.set_all_leader_endpoints(endpoints);
    let remote = Arc::new(RemoteApiClient::from_grpc(
        grpc.clone(),
        supervisor,
        config.node_name.clone(),
    ));
    RemoteForwarderParts {
        forwarder: remote.clone(),
        remote_api_client: Some(remote),
        lease_client: Some(grpc),
    }
}

fn local_controlplane_grpc_endpoint(config: &KlightsConfig) -> String {
    let host = config.external_endpoint.as_deref().unwrap_or("127.0.0.1");
    format!("https://{host}:{}", config.tls_port)
}

async fn controlplane_remote_client_identity_for_role(
    config: &Arc<KlightsConfig>,
    role: &NodeRole,
    supervisor: Arc<TaskSupervisor>,
) -> Result<ControlplaneJoinClientIdentity> {
    match role {
        NodeRole::Leader { .. } => {
            controlplane_join_client_identity_for_token(
                "",
                &config.containerd_namespace,
                &config.node_name,
                supervisor,
            )
            .await
        }
        NodeRole::Controlplane { token, .. } => {
            controlplane_join_client_identity_for_token(
                token.as_deref().unwrap_or_default(),
                &config.containerd_namespace,
                &config.node_name,
                supervisor,
            )
            .await
        }
        _ => Ok(ControlplaneJoinClientIdentity::default()),
    }
}

async fn wait_for_seed_raft_local_commit_ready(
    raft: &crate::datastore::raft::node::RaftNode,
    supervisor: &TaskSupervisor,
) -> Result<()> {
    if raft.local_commit_materialization_ready() {
        return Ok(());
    }

    let mut metrics = raft.metrics_watch();
    let timeout = supervisor.sleep(
        "raft_seed_bootstrap_commit_ready_timeout",
        std::time::Duration::from_secs(30),
    );
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            result = &mut timeout => {
                result.context("raft seed commit-readiness timeout task failed")?;
                anyhow::bail!("timed out waiting for raft seed to publish local commit materialization state");
            }
            changed = metrics.changed() => {
                changed.context("raft metrics watch closed while waiting for seed commit readiness")?;
                if raft.local_commit_materialization_ready() {
                    return Ok(());
                }
            }
        }
    }
}

fn initial_voters_for_role(role: &NodeRole, local_node_name: &str) -> Vec<String> {
    let mut voters = vec![local_node_name.to_string()];
    if let NodeRole::Leader {
        bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Bootstrap { peers },
    } = role
    {
        voters.extend(peers.iter().map(|peer| peer.node_name.clone()));
    }
    voters.sort();
    voters.dedup();
    voters
}

fn controlplane_join_retry_delay(attempt: u32) -> std::time::Duration {
    let secs = attempt.saturating_mul(5).min(60);
    std::time::Duration::from_secs(u64::from(secs))
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ControlplaneJoinClientIdentity {
    client_cert_pem: Option<String>,
    client_key_pem: Option<String>,
}

async fn controlplane_join_client_identity_for_token(
    token: &str,
    namespace: &str,
    node_name: &str,
    supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
) -> anyhow::Result<ControlplaneJoinClientIdentity> {
    use crate::bootstrap::worker_identity::{
        CredentialSource, SupervisedFilesystemWorkerCredentialStore, resolve_credential_async,
    };

    let store =
        SupervisedFilesystemWorkerCredentialStore::for_namespace(namespace, node_name, supervisor);
    match resolve_credential_async(&store).await {
        Ok(CredentialSource::ExistingCert(cred)) => Ok(ControlplaneJoinClientIdentity {
            client_cert_pem: Some(cred.certificate_pem),
            client_key_pem: Some(cred.private_key_pem),
        }),
        Ok(CredentialSource::BootstrapRequired) if !token.is_empty() => Err(anyhow::anyhow!(
            "bootstrap token may only request a certificate with CSR; no persisted node client certificate is available"
        )),
        Ok(CredentialSource::BootstrapRequired) => Err(anyhow::anyhow!(
            "no persisted node client certificate and no token source provided; join with --token-file first"
        )),
        Err(err) if !token.is_empty() => Err(err).context(
            "persisted controlplane node client certificate is invalid; bootstrap token may only request a replacement certificate with CSR",
        ),
        Err(err) => Err(err).context(
            "no valid persisted node client certificate and no token source provided; join with --token-file first",
        ),
    }
}

#[cfg(test)]
mod tests {
    use crate::bootstrap::node_role::{LeaderBootstrap, LeaderPeer, NodeRole};

    #[test]
    fn seed_initializes_single_local_voter() {
        let voters = super::initial_voters_for_role(
            &NodeRole::Leader {
                bootstrap: LeaderBootstrap::Seed,
            },
            "mn-leader",
        );

        assert_eq!(voters, vec!["mn-leader"]);
    }

    #[test]
    fn bootstrap_initializes_local_and_peer_voters() {
        let voters = super::initial_voters_for_role(
            &NodeRole::Leader {
                bootstrap: LeaderBootstrap::Bootstrap {
                    peers: vec![
                        LeaderPeer {
                            node_name: "mn-leader-3".to_string(),
                            endpoint: "https://10.99.0.13:7679".to_string(),
                        },
                        LeaderPeer {
                            node_name: "mn-leader-2".to_string(),
                            endpoint: "https://10.99.0.12:7679".to_string(),
                        },
                    ],
                },
            },
            "mn-leader",
        );

        assert_eq!(voters, vec!["mn-leader", "mn-leader-2", "mn-leader-3"]);
    }

    #[test]
    fn skip_seed_bootstrap_false_for_controlplane_seed() {
        // A seed controlplane (no --leader) should NOT skip bootstrap.
        let role = NodeRole::Controlplane {
            leader_endpoints: vec![],
            token: None,
            skip_ca: false,
            as_learner: false,
        };
        let raft_node_present = true;
        let skip = matches!(&role, NodeRole::Controlplane { leader_endpoints, .. } if !leader_endpoints.is_empty())
            && raft_node_present;
        assert!(!skip, "seed controlplane should not skip bootstrap");
    }

    #[test]
    fn skip_seed_bootstrap_true_for_controlplane_join() {
        // A joining controlplane (with --leader) SHOULD skip bootstrap.
        let role = NodeRole::Controlplane {
            leader_endpoints: vec!["https://seed:7679".to_string()],
            token: Some("tok".to_string()),
            skip_ca: false,
            as_learner: false,
        };
        let raft_node_present = true;
        let skip = matches!(&role, NodeRole::Controlplane { leader_endpoints, .. } if !leader_endpoints.is_empty())
            && raft_node_present;
        assert!(skip, "joining controlplane should skip bootstrap");
    }

    #[test]
    fn skip_seed_bootstrap_false_for_worker() {
        let role = NodeRole::Worker {
            leader_endpoints: vec!["https://seed:7679".to_string()],
            token: Some("tok".to_string()),
            skip_ca: false,
        };
        let raft_node_present = false; // workers don't have raft_node
        let skip = matches!(&role, NodeRole::Controlplane { leader_endpoints, .. } if !leader_endpoints.is_empty())
            && raft_node_present;
        assert!(!skip, "worker should never skip bootstrap");
    }

    #[test]
    fn controlplane_join_retry_delay_increases_linearly_and_caps_at_sixty_seconds() {
        let cases = [
            (1, 5),
            (2, 10),
            (3, 15),
            (4, 20),
            (5, 25),
            (6, 30),
            (7, 35),
            (8, 40),
            (9, 45),
            (10, 50),
            (11, 55),
            (12, 60),
            (13, 60),
        ];

        for (attempt, expected_secs) in cases {
            assert_eq!(
                super::controlplane_join_retry_delay(attempt),
                std::time::Duration::from_secs(expected_secs),
                "attempt {attempt} should back off for {expected_secs}s"
            );
        }
    }

    #[tokio::test]
    async fn tokenless_controlplane_join_uses_persisted_node_client_cert() {
        use crate::bootstrap::worker_identity::{
            AsyncWorkerCredentialStore, SupervisedFilesystemWorkerCredentialStore, WorkerCredential,
        };

        let namespace = format!("cp-rejoin-node-cert-{}", uuid::Uuid::new_v4());
        let node_name = "mn-controlplane1";
        let supervisor = test_supervisor();
        let store = SupervisedFilesystemWorkerCredentialStore::for_namespace(
            &namespace,
            node_name,
            supervisor.clone(),
        );
        let (certificate_pem, private_key_pem) = generate_test_node_cert(node_name);
        store
            .save(&WorkerCredential {
                certificate_pem: certificate_pem.clone(),
                private_key_pem: private_key_pem.clone(),
                node_name: node_name.to_string(),
                kubeconfig_yaml: String::new(),
            })
            .await
            .expect("persist test credential");

        let identity = super::controlplane_join_client_identity_for_token(
            "", &namespace, node_name, supervisor,
        )
        .await
        .expect("load persisted identity");

        assert_eq!(
            identity.client_cert_pem.as_deref(),
            Some(certificate_pem.as_str())
        );
        assert_eq!(
            identity.client_key_pem.as_deref(),
            Some(private_key_pem.as_str())
        );

        let _ = std::fs::remove_dir_all(crate::paths::data_root_path(&namespace));
    }

    #[tokio::test]
    async fn token_controlplane_join_prefers_persisted_node_client_cert_for_steady_state_rpcs() {
        use crate::bootstrap::worker_identity::{
            AsyncWorkerCredentialStore, SupervisedFilesystemWorkerCredentialStore, WorkerCredential,
        };

        let namespace = format!("cp-token-join-node-cert-{}", uuid::Uuid::new_v4());
        let node_name = "mn-controlplane2";
        let supervisor = test_supervisor();
        let store = SupervisedFilesystemWorkerCredentialStore::for_namespace(
            &namespace,
            node_name,
            supervisor.clone(),
        );
        let (certificate_pem, private_key_pem) = generate_test_node_cert(node_name);
        store
            .save(&WorkerCredential {
                certificate_pem: certificate_pem.clone(),
                private_key_pem: private_key_pem.clone(),
                node_name: node_name.to_string(),
                kubeconfig_yaml: String::new(),
            })
            .await
            .expect("persist test credential");

        let identity = super::controlplane_join_client_identity_for_token(
            "bootstrap-token",
            &namespace,
            node_name,
            supervisor,
        )
        .await
        .expect("load persisted identity even when token is present");

        assert_eq!(
            identity.client_cert_pem.as_deref(),
            Some(certificate_pem.as_str()),
            "NodeRestriction requires the controlplane forwarder to present a node client cert"
        );
        assert_eq!(
            identity.client_key_pem.as_deref(),
            Some(private_key_pem.as_str())
        );

        let _ = std::fs::remove_dir_all(crate::paths::data_root_path(&namespace));
    }

    #[tokio::test]
    async fn token_controlplane_join_without_persisted_cert_rejects_steady_state_auth() {
        let namespace = format!("cp-token-no-cert-{}", uuid::Uuid::new_v4());
        let node_name = "mn-controlplane2";
        let supervisor = test_supervisor();

        let err = super::controlplane_join_client_identity_for_token(
            "bootstrap-token",
            &namespace,
            node_name,
            supervisor,
        )
        .await
        .expect_err("token must not authenticate steady-state controlplane RPCs");

        assert!(
            err.to_string().contains("CSR"),
            "error should point operators at CSR bootstrap, got: {err}"
        );

        let _ = std::fs::remove_dir_all(crate::paths::data_root_path(&namespace));
    }

    #[tokio::test]
    async fn seed_leader_remote_identity_uses_persisted_node_client_cert() {
        use crate::bootstrap::worker_identity::{
            AsyncWorkerCredentialStore, SupervisedFilesystemWorkerCredentialStore, WorkerCredential,
        };

        let namespace = format!("leader-seed-node-cert-{}", uuid::Uuid::new_v4());
        let node_name = "mn-controlplane1";
        let supervisor = test_supervisor();
        let store = SupervisedFilesystemWorkerCredentialStore::for_namespace(
            &namespace,
            node_name,
            supervisor.clone(),
        );
        let (certificate_pem, private_key_pem) = generate_test_node_cert(node_name);
        store
            .save(&WorkerCredential {
                certificate_pem: certificate_pem.clone(),
                private_key_pem: private_key_pem.clone(),
                node_name: node_name.to_string(),
                kubeconfig_yaml: String::new(),
            })
            .await
            .expect("persist test credential");
        let mut config = crate::KlightsConfig::test_default();
        config.containerd_namespace = namespace.clone();
        config.node_name = node_name.to_string();
        let config = std::sync::Arc::new(config);
        let role = NodeRole::Leader {
            bootstrap: LeaderBootstrap::Seed,
        };

        let identity =
            super::controlplane_remote_client_identity_for_role(&config, &role, supervisor)
                .await
                .expect("load seed leader persisted identity");

        assert_eq!(
            identity.client_cert_pem.as_deref(),
            Some(certificate_pem.as_str())
        );
        assert_eq!(
            identity.client_key_pem.as_deref(),
            Some(private_key_pem.as_str())
        );

        let _ = std::fs::remove_dir_all(crate::paths::data_root_path(&namespace));
    }

    #[tokio::test]
    async fn seed_controlplane_remote_identity_uses_persisted_node_client_cert() {
        use crate::bootstrap::worker_identity::{
            AsyncWorkerCredentialStore, SupervisedFilesystemWorkerCredentialStore, WorkerCredential,
        };

        let namespace = format!("cp-seed-node-cert-{}", uuid::Uuid::new_v4());
        let node_name = "mn-controlplane1";
        let supervisor = test_supervisor();
        let store = SupervisedFilesystemWorkerCredentialStore::for_namespace(
            &namespace,
            node_name,
            supervisor.clone(),
        );
        let (certificate_pem, private_key_pem) = generate_test_node_cert(node_name);
        store
            .save(&WorkerCredential {
                certificate_pem: certificate_pem.clone(),
                private_key_pem: private_key_pem.clone(),
                node_name: node_name.to_string(),
                kubeconfig_yaml: String::new(),
            })
            .await
            .expect("persist test credential");
        let mut config = crate::KlightsConfig::test_default();
        config.containerd_namespace = namespace.clone();
        config.node_name = node_name.to_string();
        let config = std::sync::Arc::new(config);
        let role = NodeRole::Controlplane {
            leader_endpoints: vec![],
            token: None,
            skip_ca: false,
            as_learner: false,
        };

        let identity =
            super::controlplane_remote_client_identity_for_role(&config, &role, supervisor)
                .await
                .expect("load seed persisted identity");

        assert_eq!(
            identity.client_cert_pem.as_deref(),
            Some(certificate_pem.as_str())
        );
        assert_eq!(
            identity.client_key_pem.as_deref(),
            Some(private_key_pem.as_str())
        );

        let _ = std::fs::remove_dir_all(crate::paths::data_root_path(&namespace));
    }

    fn generate_test_node_cert(node_name: &str) -> (String, String) {
        use rcgen::{CertificateParams, DnType, KeyPair, KeyUsagePurpose};
        use time::{Duration, OffsetDateTime};

        let mut params = CertificateParams::default();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, format!("system:node:{node_name}"));
        params
            .distinguished_name
            .push(DnType::OrganizationName, "system:nodes");
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        params.not_before = OffsetDateTime::now_utc() - Duration::seconds(60);
        params.not_after = OffsetDateTime::now_utc() + Duration::seconds(31_536_000);

        let key = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        (cert.pem(), key.serialize_pem())
    }

    // ──────────────────────────────────────────────────────────────────
    // T6 step 4b: real remote forwarder selection.
    // ──────────────────────────────────────────────────────────────────

    fn test_config() -> std::sync::Arc<crate::KlightsConfig> {
        std::sync::Arc::new(crate::KlightsConfig::test_default())
    }

    fn test_supervisor() -> std::sync::Arc<crate::task_supervisor::TaskSupervisor> {
        std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ))
    }

    fn test_transport_policy()
    -> crate::replication::grpc::transport_policy::SharedGrpcTransportPolicy {
        crate::replication::grpc::transport_policy::GrpcTransportPolicy::shared_default()
    }

    fn test_dataplane() -> crate::replication::grpc::client::JoinDataplaneMetadata {
        crate::replication::grpc::client::JoinDataplaneMetadata {
            public_key: Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
            endpoint: "10.99.0.14".to_string(),
            port: Some(7679),
            mode: crate::networking::wireguard::DataplaneMode::Root,
            encryption: crate::networking::wireguard::DataplaneEncryption::Enabled,
        }
    }

    /// Type-level check that the helper exists and returns the trait
    /// object the bootstrap expects. Acts as a regression guard if the
    /// signature changes in a way that breaks the open_leader wiring.
    #[test]
    fn build_remote_forwarder_returns_leader_api_client_for_seed() {
        let role = NodeRole::Controlplane {
            leader_endpoints: vec![],
            token: None,
            skip_ca: false,
            as_learner: false,
        };
        let _: std::sync::Arc<dyn crate::control_plane::client::LeaderApiClient> =
            super::build_remote_forwarder(
                &test_config(),
                &role,
                test_supervisor(),
                super::ControlplaneJoinClientIdentity::default(),
                test_dataplane(),
                test_transport_policy(),
            )
            .forwarder;
    }

    #[test]
    fn seed_controlplane_remote_forwarder_can_track_promoted_peer() {
        let role = NodeRole::Controlplane {
            leader_endpoints: vec![],
            token: None,
            skip_ca: false,
            as_learner: false,
        };
        let mut config = crate::KlightsConfig::test_default();
        config.external_endpoint = Some("10.99.0.10".to_string());
        config.tls_port = 7679;
        let config = std::sync::Arc::new(config);

        let remote_parts = super::build_remote_forwarder(
            &config,
            &role,
            test_supervisor(),
            super::ControlplaneJoinClientIdentity {
                client_cert_pem: Some("cert".to_string()),
                client_key_pem: Some("key".to_string()),
            },
            test_dataplane(),
            test_transport_policy(),
        );

        let client = remote_parts
            .lease_client
            .expect("seed controlplane needs a real remote forwarder after demotion");
        assert_eq!(client.current_leader_endpoint(), "https://10.99.0.10:7679");
        client.set_current_leader_endpoint(Some("https://10.99.0.14:7679".to_string()));
        assert_eq!(client.current_leader_endpoint(), "https://10.99.0.14:7679");
    }

    /// Seed control-plane (empty leader_endpoints) still gets a real
    /// `RemoteApiClient`. It initializes to its own advertised endpoint
    /// and the raft metrics watcher points it at the elected leader
    /// after a later promotion/demotion.
    #[test]
    fn build_remote_forwarder_returns_real_client_for_seed_controlplane() {
        let role = NodeRole::Controlplane {
            leader_endpoints: vec![],
            token: None,
            skip_ca: false,
            as_learner: false,
        };
        let mut config = crate::KlightsConfig::test_default();
        config.external_endpoint = Some("10.99.0.10".to_string());
        config.tls_port = 7679;
        let config = std::sync::Arc::new(config);
        let remote_parts = super::build_remote_forwarder(
            &config,
            &role,
            test_supervisor(),
            super::ControlplaneJoinClientIdentity {
                client_cert_pem: Some("cert".to_string()),
                client_key_pem: Some("key".to_string()),
            },
            test_dataplane(),
            test_transport_policy(),
        );
        let client = remote_parts
            .lease_client
            .expect("seed remote must be backed by a real gRPC client");
        assert_eq!(client.current_leader_endpoint(), "https://10.99.0.10:7679");
    }

    /// Joining control-plane (non-empty leader_endpoints) gets a real
    /// `RemoteApiClient` over `ReplicationGrpcClient`. We can't make
    /// an actual gRPC call in this unit test (no server), but we
    /// verify the helper does NOT return the stub by asserting the
    /// error from apply_outbox is a connection failure, not the
    /// stub's "not yet wired" message.
    #[tokio::test]
    async fn build_remote_forwarder_returns_real_client_for_joining_controlplane() {
        let role = NodeRole::Controlplane {
            leader_endpoints: vec!["https://10.99.0.10:7679".to_string()],
            token: Some("tok".to_string()),
            skip_ca: true, // no CA available in unit test
            as_learner: false,
        };
        let remote_parts = super::build_remote_forwarder(
            &test_config(),
            &role,
            test_supervisor(),
            super::ControlplaneJoinClientIdentity::default(),
            test_dataplane(),
            test_transport_policy(),
        );
        // The call will fail — there's no real server at 10.99.0.10 in
        // this test environment. The assertion is that the failure is
        // a real gRPC/network error, NOT the stub's signature message.
        let err = remote_parts
            .forwarder
            .apply_outbox(
                "test",
                crate::kubelet::outbox::payload::OutboxOperation::PodStatus,
                bytes::Bytes::from_static(b"x"),
            )
            .await
            .expect_err("connection to nonexistent leader fails");
        let msg = match &err {
            crate::kubelet::outbox::OutboxApplyError::Retryable(m) => m.clone(),
            other => format!("{other:?}"),
        };
        assert!(
            !msg.contains("not yet wired"),
            "joiner must get a REAL remote, not the stub. got: {msg}"
        );
    }

    #[test]
    fn build_remote_forwarder_passes_node_client_cert_to_joiner_grpc() {
        let role = NodeRole::Controlplane {
            leader_endpoints: vec!["https://10.99.0.10:7679".to_string()],
            token: None,
            skip_ca: true,
            as_learner: false,
        };
        let remote_parts = super::build_remote_forwarder(
            &test_config(),
            &role,
            test_supervisor(),
            super::ControlplaneJoinClientIdentity {
                client_cert_pem: Some("cert".to_string()),
                client_key_pem: Some("key".to_string()),
            },
            test_dataplane(),
            test_transport_policy(),
        );
        assert!(
            remote_parts
                .lease_client
                .expect("joiner must have a remote gRPC client")
                .uses_client_cert_auth_for_test(),
            "tokenless controlplane rejoin remote RPCs must use persisted node mTLS cert"
        );
    }

    #[test]
    fn build_remote_forwarder_passes_local_dataplane_to_joiner_grpc() {
        let role = NodeRole::Controlplane {
            leader_endpoints: vec!["https://10.99.0.10:7679".to_string()],
            token: Some("tok".to_string()),
            skip_ca: true,
            as_learner: false,
        };
        let dataplane = test_dataplane();
        let remote_parts = super::build_remote_forwarder(
            &test_config(),
            &role,
            test_supervisor(),
            super::ControlplaneJoinClientIdentity::default(),
            dataplane.clone(),
            test_transport_policy(),
        );

        assert_eq!(
            remote_parts
                .lease_client
                .expect("joiner must have a remote gRPC client")
                .dataplane_for_test(),
            dataplane,
            "joining controlplane remote RPCs must carry local WireGuard metadata"
        );
    }
}
