use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::bootstrap::local_leader_adapters::{
    LocalNodeLeaseRenewalAdapter, LocalNodeLifecycleStatusAdapter, LocalProjectedTokenAdapter,
};
use klights_leader_api::{
    LeaderNetworkTopologyQuery, LeaderNodeSubnetAllocation, LeaderOutboxDelivery,
    LeaderPodCleanupIntents, LeaderResourceQuery, NodeDataplaneQuery, NodeSubnetAllocationRequest,
    NodeSubnetQuery, OutboxDeliveryRequest, PeerSubnetsQuery, PodCleanupIntentAckRequest,
    ResourceQueryConsistency, pod_get_request,
};

fn test_outbox_delivery(
    db: &klights_cluster_datastore::sqlite::embedded::Datastore,
    local_node: &str,
) -> (
    Arc<crate::bootstrap::composition_adapters::committed_outbox_delivery_adapter::RootCommittedOutboxDelivery>,
    Arc<crate::bootstrap::composition_adapters::committed_outbox_delivery_adapter::RootOutboxSideEffectState>,
){
    let authority = crate::bootstrap::composition::authority::AuthorityHandle::from(
        crate::bootstrap::composition_adapters::authority_adapter::always_leader_authority(),
    );
    let ports =
        crate::bootstrap::composition::cluster_store::selector::sqlite_opened_passive_store(db);
    let canonical = db.clone();
    let side_effects = crate::bootstrap::local_leader_adapters::new_local_outbox_side_effect_state(
        ports.read_ports.resource_reads(),
        ports.resource_mutations.clone(),
        ports.ownership_reads.clone(),
    );
    let delivery = crate::bootstrap::composition_adapters::
        committed_outbox_delivery_adapter::test_outbox_delivery(
            &authority,
            side_effects.clone(),
            local_node.to_string(),
            Arc::new(canonical.clone()),
            Arc::new(canonical.clone()),
            canonical.focused_read_store(),
        );
    (delivery, side_effects)
}

async fn deliver_test_outbox(
    delivery: &dyn LeaderOutboxDelivery,
    idempotency_key: &str,
    operation: OutboxOperation,
    payload: Bytes,
    client_id: &str,
    stream_id: i64,
    stream_seq: i64,
) -> Result<OutboxApplyResult, OutboxApplyError> {
    use klights_kubelet::node_outbox::payload::OutboxOperationExt as _;

    let request = OutboxDeliveryRequest::try_new(
        idempotency_key,
        operation.try_delivery_operation()?,
        Arc::<[u8]>::from(payload.to_vec()),
        client_id,
        stream_id,
        stream_seq,
    )?;
    delivery.deliver_outbox(request).await
}

fn test_network_port(
    db: &klights_cluster_datastore::sqlite::embedded::Datastore,
) -> Arc<crate::bootstrap::composition_adapters::leader_topology_cleanup_adapter::ClusterStoreLeaderNetwork>
{
    let canonical = db.clone();
    Arc::new(
        crate::bootstrap::composition_adapters::leader_topology_cleanup_adapter::ClusterStoreLeaderNetwork::new(
            db.focused_read_store(),
            Arc::new(crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(Arc::new(canonical.clone()), Arc::new(canonical.clone()), canonical.focused_read_store())),
            crate::bootstrap::composition_adapters::authority_adapter::always_leader_watch(),
        ),
    )
}

fn test_cleanup_port(
    db: &klights_cluster_datastore::sqlite::embedded::Datastore,
) -> Arc<crate::bootstrap::composition_adapters::leader_topology_cleanup_adapter::ClusterStoreLeaderPodCleanup>
{
    let canonical = db.clone();
    Arc::new(
        crate::bootstrap::composition_adapters::leader_topology_cleanup_adapter::ClusterStoreLeaderPodCleanup::new(
            crate::bootstrap::composition::cluster_store::selector::sqlite_opened_passive_store(db).pod_cleanup,
            Arc::new(crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(Arc::new(canonical.clone()), Arc::new(canonical.clone()), canonical.focused_read_store())),
            crate::bootstrap::composition_adapters::authority_adapter::always_leader_watch(),
        ),
    )
}

#[test]
fn concrete_leader_clients_implement_focused_pod_effect_ports() {
    fn assert_ports<T>()
    where
        T: klights_leader_api::LeaderProjectedServiceAccountToken,
    {
    }

    assert_ports::<LocalProjectedTokenAdapter>();
    assert_ports::<klights_leader_rpc::client::RemoteApiClient>();
    assert_ports::<crate::bootstrap::composition::authority_routed_leader::AuthorityRoutedLeader>();
    assert_ports::<crate::bootstrap::composition::authority_routed_leader::StubRemoteForwarder>();

    fn assert_network<T>()
    where
        T: klights_leader_api::LeaderNodeSubnetAllocation
            + klights_leader_api::LeaderNetworkTopologyQuery
            + klights_leader_api::LeaderNetworkTopologyCommand,
    {
    }
    assert_network::<
        crate::bootstrap::composition_adapters::leader_topology_cleanup_adapter::ClusterStoreLeaderNetwork,
    >();
    fn assert_cleanup<T: klights_leader_api::LeaderPodCleanupIntents>() {}
    assert_cleanup::<
        crate::bootstrap::composition_adapters::leader_topology_cleanup_adapter::ClusterStoreLeaderPodCleanup,
    >();
}

#[test]
fn seed_bootstrap_adapter_is_limited_to_focused_controller_stores() {
    fn assert_seed_ports<T>()
    where
        T: klights_controllers::namespace::NamespaceBootstrapStore
            + klights_controllers::rbac_reconcile::RbacPolicyStore
            + klights_controllers::kube_service::KubernetesBootstrapStore,
    {
    }

    assert_seed_ports::<
        crate::bootstrap::composition_adapters::leader_bootstrap_store_adapter::LeaderBootstrapStore,
    >();
}

struct RecordingBootstrapCommands {
    commands: std::sync::Mutex<Vec<StorageCommand>>,
}

impl klights_leader_api::LeaderResourceCommand for RecordingBootstrapCommands {
    fn submit_resource_command(
        &self,
        request: klights_leader_api::ResourceCommandRequest,
    ) -> klights_leader_api::ResourceCommandFuture<'_, klights_leader_api::ResourceCommandResult>
    {
        Box::pin(async move {
            let command = request.into_command();
            self.commands.lock().unwrap().push(command.clone());
            let StorageCommand::CreateResource { mut data, .. } = command else {
                panic!("test expects a create command")
            };
            data["metadata"]["uid"] = serde_json::json!("seed-uid");
            data["metadata"]["resourceVersion"] = serde_json::json!("1");
            Ok(klights_leader_api::ResourceCommandResult::Resource(
                klights_cluster_core::Resource::try_from_data(Arc::new(data)).unwrap(),
            ))
        })
    }
}

struct RecordingCronJobCommands {
    commands: Mutex<Vec<StorageCommand>>,
    reject_as_follower: bool,
}

impl klights_leader_api::LeaderResourceCommand for RecordingCronJobCommands {
    fn submit_resource_command(
        &self,
        request: klights_leader_api::ResourceCommandRequest,
    ) -> klights_leader_api::ResourceCommandFuture<'_, klights_leader_api::ResourceCommandResult>
    {
        Box::pin(async move {
            let command = request.into_command();
            self.commands.lock().unwrap().push(command.clone());
            if self.reject_as_follower {
                return Err(klights_leader_api::ResourceCommandError::NotLeader);
            }
            match command {
                StorageCommand::CreateResource { mut data, .. } => {
                    data["metadata"]["uid"] = serde_json::json!("cronjob-job-uid");
                    data["metadata"]["resourceVersion"] = serde_json::json!("72");
                    Ok(klights_leader_api::ResourceCommandResult::Resource(
                        klights_cluster_core::Resource::try_from_data(Arc::new(data)).unwrap(),
                    ))
                }
                StorageCommand::UpdateStatus {
                    api_version,
                    kind,
                    namespace,
                    name,
                    status,
                    ..
                } => Ok(klights_leader_api::ResourceCommandResult::Resource(
                    klights_cluster_core::Resource::try_from_data(Arc::new(serde_json::json!({
                        "apiVersion": api_version,
                        "kind": kind,
                        "metadata": {
                            "name": name,
                            "namespace": namespace,
                            "uid": "cronjob-uid",
                            "resourceVersion": "72"
                        },
                        "status": status
                    })))
                    .unwrap(),
                )),
                StorageCommand::DeleteResource { .. } => {
                    Ok(klights_leader_api::ResourceCommandResult::Ack {
                        resource_version: 72,
                    })
                }
                other => panic!("unexpected CronJob command: {other:?}"),
            }
        })
    }
}

fn cronjob_job() -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "forbid-786492620",
            "namespace": "cronjob-8930",
            "ownerReferences": [{
                "apiVersion": "batch/v1",
                "kind": "CronJob",
                "name": "forbid",
                "uid": "cronjob-uid",
                "controller": true,
                "blockOwnerDeletion": true
            }]
        },
        "spec": {"template": {"spec": {"restartPolicy": "Never", "containers": []}}}
    })
}

#[tokio::test]
async fn cronjob_job_create_routes_exact_raft_command_without_passive_mutation() {
    use klights_controllers::cronjob::CronJobStore as _;

    let passive =
        crate::bootstrap::composition::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
    let before_rv = passive.get_current_resource_version().await.unwrap();
    let commands = Arc::new(RecordingCronJobCommands {
        commands: Mutex::new(Vec::new()),
        reject_as_follower: false,
    });
    let port = crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::new_with_commands(
        passive.focused_read_store(),
        passive.focused_read_store(),
        commands.clone(),
    );
    let job = cronjob_job();

    let created = port
        .create_job("cronjob-8930", "forbid-786492620", job.clone())
        .await
        .expect("authoritative CronJob Job create is proposed");

    assert_eq!(created.resource_version, 72);
    assert_eq!(
        commands.commands.lock().unwrap().as_slice(),
        &[StorageCommand::CreateResource {
            api_version: "batch/v1".into(),
            kind: "Job".into(),
            namespace: Some("cronjob-8930".into()),
            name: "forbid-786492620".into(),
            data: job,
        }]
    );
    assert!(
        passive
            .get_resource("batch/v1", "Job", Some("cronjob-8930"), "forbid-786492620",)
            .await
            .unwrap()
            .is_none(),
        "CronJob controller must not materialize the Job in its passive local store"
    );
    assert_eq!(
        passive.get_current_resource_version().await.unwrap(),
        before_rv
    );
}

#[tokio::test]
async fn cronjob_job_create_follower_rejection_preserves_passive_store() {
    use klights_controllers::cronjob::CronJobStore as _;

    let passive =
        crate::bootstrap::composition::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
    let before_rv = passive.get_current_resource_version().await.unwrap();
    let commands = Arc::new(RecordingCronJobCommands {
        commands: Mutex::new(Vec::new()),
        reject_as_follower: true,
    });
    let port = crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::new_with_commands(
        passive.focused_read_store(),
        passive.focused_read_store(),
        commands,
    );

    let error = port
        .create_job("cronjob-8930", "forbid-786492620", cronjob_job())
        .await
        .expect_err("follower CronJob effect must be rejected");

    assert!(
        matches!(
            error,
            klights_reconcile_api::ControllerStoreError::Unavailable(_)
        ),
        "follower rejection must remain retryable"
    );
    assert!(
        passive
            .get_resource("batch/v1", "Job", Some("cronjob-8930"), "forbid-786492620",)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        passive.get_current_resource_version().await.unwrap(),
        before_rv
    );
}

#[tokio::test]
async fn cronjob_job_delete_and_status_preserve_strict_uid_rv_preconditions() {
    use klights_controllers::common::ControllerStatusStore as _;
    use klights_controllers::cronjob::CronJobStore as _;

    let passive =
        crate::bootstrap::composition::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
    let commands = Arc::new(RecordingCronJobCommands {
        commands: Mutex::new(Vec::new()),
        reject_as_follower: false,
    });
    let port = crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::new_with_commands(
        passive.focused_read_store(),
        passive.focused_read_store(),
        commands.clone(),
    );

    port.delete_job(
        "cronjob-8930",
        "forbid-786492620",
        "cronjob-job-uid".into(),
        71,
    )
    .await
    .expect("CronJob history delete is proposed");
    port.update_status(
        "batch/v1",
        "CronJob",
        Some("cronjob-8930"),
        "forbid",
        serde_json::json!({"lastScheduleTime": "2026-08-11T23:57:00Z"}),
        klights_cluster_core::ResourcePreconditions::uid_and_resource_version("cronjob-uid", 941),
    )
    .await
    .expect("CronJob status CAS is proposed");

    assert!(matches!(
        commands.commands.lock().unwrap().as_slice(),
        [
            StorageCommand::DeleteResource {
                api_version,
                kind,
                namespace: Some(namespace),
                name,
                preconditions,
            },
            StorageCommand::UpdateStatus {
                api_version: status_api_version,
                kind: status_kind,
                namespace: Some(status_namespace),
                name: status_name,
                expected_rv: Some(941),
                preconditions: status_preconditions,
                ..
            }
        ] if api_version == "batch/v1"
            && kind == "Job"
            && namespace == "cronjob-8930"
            && name == "forbid-786492620"
            && preconditions.uid.as_deref() == Some("cronjob-job-uid")
            && preconditions.resource_version == Some(71)
            && status_api_version == "batch/v1"
            && status_kind == "CronJob"
            && status_namespace == "cronjob-8930"
            && status_name == "forbid"
            && status_preconditions.uid.as_deref() == Some("cronjob-uid")
            && status_preconditions.resource_version == Some(941)
    ));
}

#[tokio::test]
async fn seed_bootstrap_mutation_uses_command_port_without_passive_write() {
    use klights_controllers::kube_service::KubernetesBootstrapStore as _;

    let passive =
        crate::bootstrap::composition::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
    let commands = Arc::new(RecordingBootstrapCommands {
        commands: std::sync::Mutex::new(Vec::new()),
    });
    let store = crate::bootstrap::composition_adapters::leader_bootstrap_store_adapter::LeaderBootstrapStore::new(
        passive.focused_read_store(),
        passive.focused_read_store(),
        commands.clone(),
    );

    store
        .create_bootstrap_resource(
            "rbac.authorization.k8s.io/v1",
            "ClusterRole",
            None,
            "system:test",
            serde_json::json!({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "ClusterRole",
                "metadata": {"name": "system:test"},
                "rules": []
            }),
        )
        .await
        .expect("submit seed mutation");

    assert!(
        passive
            .get_resource(
                "rbac.authorization.k8s.io/v1",
                "ClusterRole",
                None,
                "system:test",
            )
            .await
            .unwrap()
            .is_none(),
        "bootstrap adapter must never mutate the passive apply store directly"
    );
    assert!(matches!(
        commands.commands.lock().unwrap().as_slice(),
        [StorageCommand::CreateResource { kind, name, .. }]
            if kind == "ClusterRole" && name == "system:test"
    ));
}

struct RecordingNodeCleanupCommands {
    commands: Mutex<Vec<StorageCommand>>,
}

impl klights_leader_api::LeaderResourceCommand for RecordingNodeCleanupCommands {
    fn submit_resource_command(
        &self,
        request: klights_leader_api::ResourceCommandRequest,
    ) -> klights_leader_api::ResourceCommandFuture<'_, klights_leader_api::ResourceCommandResult>
    {
        Box::pin(async move {
            self.commands.lock().unwrap().push(request.into_command());
            Ok(klights_leader_api::ResourceCommandResult::Ack {
                resource_version: 196,
            })
        })
    }
}

#[tokio::test]
async fn node_hard_delete_cleanup_routes_one_raft_command_without_passive_rv_mutation() {
    let passive =
        crate::bootstrap::composition::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
    let before_rv = passive.get_current_resource_version().await.unwrap();
    let commands = Arc::new(RecordingNodeCleanupCommands {
        commands: Mutex::new(Vec::new()),
    });

    crate::bootstrap::composition_adapters::generated_handler_adapter::submit_node_cleanup_intents(
        commands.as_ref(),
        "e2e-fake-node",
    )
    .await
    .expect("submit node cleanup through raft owner");

    assert_eq!(
        passive.get_current_resource_version().await.unwrap(),
        before_rv,
        "passive cluster store must not consume a leader-only resourceVersion"
    );
    assert!(matches!(
        commands.commands.lock().unwrap().as_slice(),
        [StorageCommand::DeletePodCleanupIntentsForNode { node_name }]
            if node_name == "e2e-fake-node"
    ));
}

#[test]
fn node_effect_ports_have_the_frozen_authority_split() {
    fn assert_lease<T: klights_leader_api::LeaderNodeLeaseRenewal>() {}
    fn assert_local_lifecycle<T: klights_leader_api::LeaderNodeLifecycleStatus>() {}

    assert_lease::<LocalNodeLeaseRenewalAdapter>();
    assert_lease::<klights_leader_rpc::client::RemoteApiClient>();
    assert_lease::<crate::bootstrap::composition::authority_routed_leader::AuthorityRoutedLeader>();
    assert_lease::<crate::bootstrap::composition::authority_routed_leader::StubRemoteForwarder>();
    assert_local_lifecycle::<LocalNodeLifecycleStatusAdapter>();
}

#[tokio::test]
async fn node_effect_ports_gate_follower_lease_before_tracker_mutation() {
    let tracker = Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(
        chrono::Utc::now(),
    ));
    let (_leader_tx, follower_rx) = tokio::sync::watch::channel(false);
    let local = LocalNodeLeaseRenewalAdapter::new(tracker.clone(), follower_rx);
    let request = klights_leader_api::NodeLeaseRenewalRequest::try_new(
        "cp-1",
        klights_cluster_core::k8s_time::format_time(chrono::Utc::now()),
        30,
    )
    .expect("valid renewal");

    let error = klights_leader_api::LeaderNodeLeaseRenewal::renew_node_lease(&local, request)
        .await
        .expect_err("follower must reject local lease renewal");
    assert_eq!(error, klights_leader_api::NodeLeaseRenewalError::NotLeader);
    assert!(
        tracker.observed("cp-1").await.is_none(),
        "leadership must be checked before the in-memory tracker is mutated"
    );
}

#[tokio::test]
async fn node_effect_ports_remote_rejects_cross_node_before_transport() {
    let remote = klights_leader_rpc::client::RemoteApiClient::without_transport(
        "worker-1",
        Arc::new(
            crate::bootstrap::composition_adapters::remote_informer_cache_adapter::WatchCacheAdapter::new(),
        ),
    );
    let request = klights_leader_api::NodeLeaseRenewalRequest::try_new(
        "worker-2",
        klights_cluster_core::k8s_time::format_time(chrono::Utc::now()),
        30,
    )
    .expect("valid renewal shape");
    assert!(matches!(
        klights_leader_api::LeaderNodeLeaseRenewal::renew_node_lease(&remote, request).await,
        Err(klights_leader_api::NodeLeaseRenewalError::Unauthorized { .. })
    ));
}

#[tokio::test]
async fn node_effect_lease_renewal_has_no_cluster_rv_watch_or_lease_row() {
    let db = crate::bootstrap::composition::cluster_store::selector::canonical_sqlite_fixture()
        .await
        .unwrap();
    let tracker = Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(
        chrono::Utc::now(),
    ));
    let client = LocalNodeLeaseRenewalAdapter::new(
        tracker.clone(),
        crate::bootstrap::composition_adapters::authority_adapter::always_leader_watch(),
    );
    let before_rv = db.get_current_resource_version().await.expect("read RV");
    let request = klights_leader_api::NodeLeaseRenewalRequest::try_new(
        "cp-1",
        klights_cluster_core::k8s_time::format_time(chrono::Utc::now()),
        30,
    )
    .expect("valid renewal");
    klights_leader_api::LeaderNodeLeaseRenewal::renew_node_lease(&client, request)
        .await
        .expect("renew in memory");

    assert!(tracker.observed("cp-1").await.is_some());
    assert_eq!(
        db.get_current_resource_version().await.expect("read RV"),
        before_rv
    );
    assert!(
        db.get_resource(
            "coordination.k8s.io/v1",
            "Lease",
            Some("kube-node-lease"),
            "cp-1",
        )
        .await
        .expect("read Lease")
        .is_none()
    );
}

#[tokio::test]
async fn node_effect_lifecycle_status_preserves_spec_metadata_and_conflicts_stale_rv() {
    let db = crate::bootstrap::composition::cluster_store::selector::canonical_sqlite_fixture()
        .await
        .unwrap();
    let created = db
        .create_resource(
            "v1",
            "Node",
            None,
            "worker-a",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": "worker-a",
                    "uid": "node-uid-a",
                    "labels": {"owned-by": "control-plane"}
                },
                "spec": {"unschedulable": true},
                "status": {"conditions": []}
            }),
        )
        .await
        .expect("create Node");
    let canonical = db.clone();
    let authority =
        crate::bootstrap::composition_adapters::authority_adapter::always_leader_watch();
    let resource_query = klights_watch::DatastoreResourceQueryAdapter::new_focused_for_test(
        canonical.focused_read_store(),
        crate::bootstrap::composition::authority::AuthorityHandle::from(authority.clone())
            .authority_arc(),
    );
    let resource_commands = crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::resource_commands_for_test(
        Arc::new(canonical.clone()),
        Arc::new(canonical.clone()),
        canonical.focused_read_store(),
    );
    let client = LocalNodeLifecycleStatusAdapter::new(resource_query, resource_commands, authority);
    let command = StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Node".to_string(),
        namespace: None,
        name: "worker-a".to_string(),
        status: serde_json::json!({
            "conditions": [{"type": "Ready", "status": "Unknown"}]
        }),
        expected_rv: Some(created.resource_version),
        preconditions: ResourcePreconditions::from_resource(&created),
        observed_status_stamp: None,
    };
    let request = klights_leader_api::NodeLifecycleStatusRequest::try_new(command.clone())
        .expect("valid lifecycle CAS");
    let result = klights_leader_api::LeaderNodeLifecycleStatus::submit_node_lifecycle_status(
        &client, request,
    )
    .await
    .expect("apply lifecycle status");
    assert!(matches!(
        result,
        klights_leader_api::NodeLifecycleStatusResult::Updated { resource_version }
            if resource_version > created.resource_version
    ));

    let stored = db
        .get_resource("v1", "Node", None, "worker-a")
        .await
        .expect("read Node")
        .expect("Node exists");
    assert_eq!(
        stored.data.pointer("/spec/unschedulable"),
        Some(&serde_json::json!(true))
    );
    assert_eq!(
        stored.data.pointer("/metadata/labels/owned-by"),
        Some(&serde_json::json!("control-plane"))
    );
    assert_eq!(
        stored.data.pointer("/status/conditions/0/status"),
        Some(&serde_json::json!("Unknown"))
    );

    let stale = klights_leader_api::NodeLifecycleStatusRequest::try_new(command)
        .expect("same old CAS remains structurally valid");
    assert!(matches!(
            klights_leader_api::LeaderNodeLifecycleStatus::submit_node_lifecycle_status(
                &client, stale,
            )
            .await,
            Err(klights_leader_api::NodeLifecycleStatusError::Conflict { .. })
        ));
}

struct RecordingNodeLifecycleCommands {
    commands: Mutex<Vec<StorageCommand>>,
    resource_version: i64,
}

impl klights_leader_api::LeaderResourceCommand for RecordingNodeLifecycleCommands {
    fn submit_resource_command(
        &self,
        request: klights_leader_api::ResourceCommandRequest,
    ) -> klights_leader_api::ResourceCommandFuture<'_, klights_leader_api::ResourceCommandResult>
    {
        Box::pin(async move {
            self.commands
                .lock()
                .expect("record Node lifecycle command")
                .push(request.into_command());
            Ok(klights_leader_api::ResourceCommandResult::Ack {
                resource_version: self.resource_version,
            })
        })
    }
}

#[tokio::test]
async fn node_lifecycle_status_cannot_mutate_the_passive_cluster_store() {
    let db = crate::bootstrap::composition::cluster_store::selector::canonical_sqlite_fixture()
        .await
        .unwrap();
    let created = db
        .create_resource(
            "v1",
            "Node",
            None,
            "fake-node",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "fake-node", "uid": "fake-node-uid"},
                "status": {"conditions": [{"type": "Ready", "status": "True"}]}
            }),
        )
        .await
        .expect("create Node");
    let authority =
        crate::bootstrap::composition_adapters::authority_adapter::always_leader_watch();
    let resource_query = klights_watch::DatastoreResourceQueryAdapter::new_focused_for_test(
        db.focused_read_store(),
        crate::bootstrap::composition::authority::AuthorityHandle::from(authority.clone())
            .authority_arc(),
    );
    let resource_commands = Arc::new(RecordingNodeLifecycleCommands {
        commands: Mutex::new(Vec::new()),
        resource_version: created.resource_version + 1,
    });
    let client =
        LocalNodeLifecycleStatusAdapter::new(resource_query, resource_commands.clone(), authority);
    let command = StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Node".to_string(),
        namespace: None,
        name: "fake-node".to_string(),
        status: serde_json::json!({
            "conditions": [{
                "type": "Ready",
                "status": "Unknown",
                "reason": "NodeStatusUnknown"
            }]
        }),
        expected_rv: Some(created.resource_version),
        preconditions: ResourcePreconditions::from_resource(&created),
        observed_status_stamp: None,
    };
    let request = klights_leader_api::NodeLifecycleStatusRequest::try_new(command.clone())
        .expect("valid lifecycle status command");

    klights_leader_api::LeaderNodeLifecycleStatus::submit_node_lifecycle_status(&client, request)
        .await
        .expect("leader accepts lifecycle status");

    let unchanged = db
        .get_resource("v1", "Node", None, "fake-node")
        .await
        .expect("read Node")
        .expect("Node exists");
    assert_eq!(unchanged.resource_version, created.resource_version);
    assert_eq!(unchanged.data, created.data);
    assert_eq!(
        resource_commands
            .commands
            .lock()
            .expect("read Node lifecycle commands")
            .as_slice(),
        &[command]
    );
}
use crate::bootstrap::composition_tests::support::OutboxPayload;
use klights_cluster_core::ResourcePreconditions;
use klights_cluster_core::command::StorageCommand;
use klights_cluster_store::{DataplaneEncryption, DataplaneMode, DataplanePeerMetadata};
use klights_kubelet::node_outbox::payload::OutboxOperation;
use klights_leader_api::{
    OutboxDeliveryError as OutboxApplyError, OutboxDeliveryResult as OutboxApplyResult,
};

#[test]
fn positioned_watch_adapters_implement_focused_ports() {
    fn assert_focused<
        T: klights_leader_api::LeaderWatch + klights_leader_api::LeaderCacheReadiness,
    >() {
    }

    assert_focused::<klights_leader_rpc::client::RemoteApiClient>();
    assert_focused::<crate::bootstrap::composition::authority_routed_leader::AuthorityRoutedLeader>(
    );
    assert_focused::<crate::bootstrap::composition::authority_routed_leader::StubRemoteForwarder>();
}

#[tokio::test]
async fn stub_watch_and_readiness_are_typed_unavailable() {
    let stub = crate::bootstrap::composition::authority_routed_leader::StubRemoteForwarder::new(
        "cp-stub".to_string(),
    );
    let watch =
        klights_leader_api::WatchRequest::try_new("v1", "Pod", None, None, None, Some(41), None)
            .expect("valid watch");
    assert!(matches!(
        klights_leader_api::LeaderWatch::watch_resources(&stub, watch).await,
        Err(klights_leader_api::LeaderWatchError::Unavailable { .. })
    ));

    let readiness = klights_leader_api::CacheReadinessRequest::try_new(
        "v1",
        "Pod",
        None,
        None,
        Some("spec.nodeName=worker-a".to_string()),
    )
    .expect("valid readiness scope");
    assert!(matches!(
        klights_leader_api::LeaderCacheReadiness::wait_cache_ready(&stub, readiness).await,
        Err(klights_leader_api::CacheReadinessError::Unavailable { .. })
    ));
}

fn pod_status_payload(uid: &str) -> Bytes {
    let command = StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "web".to_string(),
        status: serde_json::json!({"phase": "Running"}),
        expected_rv: None,
        preconditions: ResourcePreconditions {
            uid: Some(uid.to_string()),
            resource_version: None,
        },
        observed_status_stamp: None,
    };
    Bytes::from(
        OutboxPayload::from_command(command)
            .encode_protobuf()
            .expect("encode payload"),
    )
}

fn pod_delete_payload(name: &str, uid: &str, observed_resource_version: i64) -> Bytes {
    pod_delete_payload_for("default", name, uid, observed_resource_version)
}

fn pod_delete_payload_for(
    namespace: &str,
    name: &str,
    uid: &str,
    observed_resource_version: i64,
) -> Bytes {
    let command = StorageCommand::FinalizeBoundPod {
        namespace: namespace.to_string(),
        name: name.to_string(),
        pod_uid: uid.to_string(),
        node_name: "worker-a".to_string(),
        observed_resource_version,
    };
    Bytes::from(
        OutboxPayload::from_command(command)
            .encode_protobuf()
            .expect("encode payload"),
    )
}

#[tokio::test]
async fn local_client_reads_pods_through_focused_resource_query() {
    let db = crate::bootstrap::composition::cluster_store::selector::canonical_sqlite_fixture()
        .await
        .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "web",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "web",
                "uid": "uid-1"
            },
            "spec": {"containers": [{"name": "app", "image": "nginx"}]}
        }),
    )
    .await
    .expect("create pod");
    let resource_query = klights_watch::DatastoreResourceQueryAdapter::new_focused_for_test(
        db.focused_read_store(),
        crate::bootstrap::composition_adapters::authority_adapter::always_leader_authority(),
    );

    assert!(
        resource_query
            .get_resource(
                pod_get_request("default", "web", ResourceQueryConsistency::Cached)
                    .expect("valid Pod request"),
            )
            .await
            .expect("get pod")
            .is_some()
    );
}

#[tokio::test]
async fn cleanup_intent_ack_is_idempotent_and_never_touches_same_name_pod_row() {
    let db = crate::bootstrap::composition::cluster_store::selector::canonical_sqlite_fixture()
        .await
        .unwrap();
    let replacement = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "web",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "web",
                    "uid": "replacement-uid"
                },
                "spec": {
                    "nodeName": "node-a",
                    "containers": [{"name": "app", "image": "nginx"}]
                }
            }),
        )
        .await
        .expect("create same-name replacement Pod");
    let ack = PodCleanupIntentAckRequest::try_new(
        "node-a",
        "default",
        "web",
        "old-uid",
        klights_cluster_store::POD_CLEANUP_REASON_NODE_LOST,
    )
    .unwrap();

    for _ in 0..2 {
        test_cleanup_port(&db)
            .acknowledge_pod_cleanup_intent(ack.clone())
            .await
            .expect("missing cleanup intent acknowledgement is idempotent");
    }

    let stored = db
        .get_resource("v1", "Pod", Some("default"), "web")
        .await
        .unwrap()
        .expect("same-name replacement Pod must remain");
    assert_eq!(stored.uid, "replacement-uid");
    assert_eq!(stored.resource_version, replacement.resource_version);
}

#[tokio::test]
async fn local_client_apply_outbox_is_idempotent_and_uid_bound() {
    let db = crate::bootstrap::composition::cluster_store::selector::canonical_sqlite_fixture()
        .await
        .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "web",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "web",
                "uid": "uid-1"
            },
            "spec": {"containers": [{"name": "app", "image": "nginx"}]},
            "status": {"phase": "Pending"}
        }),
    )
    .await
    .expect("create pod");
    let (delivery, side_effects) = test_outbox_delivery(&db, "node-a");
    side_effects.set_controller_dispatcher(
        crate::bootstrap::controller_adapters::controller_runtime_adapter::dispatcher_for_test(
            &db,
            Arc::new(klights_controllers::service::ServiceIpam::new(
                "10.43.128.0/17",
            )),
        ),
    );

    let first = deliver_test_outbox(
        delivery.as_ref(),
        "stable-key",
        OutboxOperation::PodStatus,
        pod_status_payload("uid-1"),
        "client",
        1,
        1,
    )
    .await
    .expect("first apply");
    let duplicate = deliver_test_outbox(
        delivery.as_ref(),
        "stable-key",
        OutboxOperation::PodStatus,
        pod_status_payload("uid-1"),
        "client",
        1,
        1,
    )
    .await
    .expect("duplicate apply");
    assert!(matches!(first, OutboxApplyResult::Applied { .. }));
    assert!(matches!(
        duplicate,
        OutboxApplyResult::AlreadyApplied { .. }
    ));
    let stored = db
        .get_resource("v1", "Pod", Some("default"), "web")
        .await
        .expect("get pod")
        .expect("pod exists");
    assert_eq!(
        stored
            .data
            .pointer("/status/phase")
            .and_then(|v| v.as_str()),
        Some("Running")
    );
    let rv_before_mismatch = stored.resource_version;
    let watch_before_mismatch = db
        .current_watch_replay_position()
        .await
        .expect("watch position before assigned UID mismatch");

    let err = deliver_test_outbox(
        delivery.as_ref(),
        "uid-mismatch-key",
        OutboxOperation::PodStatus,
        pod_status_payload("uid-2"),
        "client",
        1,
        2,
    )
    .await
    .expect_err("assigned uid mismatch");
    assert!(matches!(err, OutboxApplyError::UidMismatch { .. }));
    assert_eq!(
        db.get_current_resource_version().await.expect("read RV"),
        rv_before_mismatch,
        "terminal UID mismatch must not allocate a public resourceVersion"
    );
    assert_eq!(
        db.current_watch_replay_position()
            .await
            .expect("watch position after assigned UID mismatch"),
        watch_before_mismatch,
        "terminal UID mismatch must not append watch history"
    );
    let ledger = db
        .get_applied_outbox("uid-mismatch-key")
        .await
        .expect("read terminal ledger")
        .expect("terminal ledger row");
    assert!(matches!(
        klights_leader_rpc::storage_wire_codec::decode_response_protobuf(&ledger.result_proto),
        Ok(klights_cluster_core::command::StorageResponse::Error { message })
            if message.contains("delivery UID mismatch")
    ));
    assert_eq!(
        db.list_outbox_stream_watermarks()
            .await
            .expect("read terminal watermark")[0]
            .stream_seq,
        2,
        "terminal UID mismatch must consume its exact assigned sequence"
    );
}

#[tokio::test]
async fn local_client_apply_outbox_returns_committed_resource_version() {
    let db = crate::bootstrap::composition::cluster_store::selector::canonical_sqlite_fixture()
        .await
        .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "web",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "web",
                "uid": "uid-1"
            },
            "spec": {"containers": [{"name": "app", "image": "nginx"}]},
            "status": {"phase": "Pending"}
        }),
    )
    .await
    .expect("create pod");
    let (delivery, side_effects) = test_outbox_delivery(&db, "node-a");
    side_effects.set_controller_dispatcher(
        crate::bootstrap::controller_adapters::controller_runtime_adapter::dispatcher_for_test(
            &db,
            Arc::new(klights_controllers::service::ServiceIpam::new(
                "10.43.128.0/17",
            )),
        ),
    );

    let applied = deliver_test_outbox(
        delivery.as_ref(),
        "raft-client-key",
        OutboxOperation::PodStatus,
        pod_status_payload("uid-1"),
        "client",
        1,
        1,
    )
    .await
    .expect("apply outbox through local client");

    let OutboxApplyResult::Applied { applied_rv } = applied else {
        panic!("first local apply must commit a new write");
    };
    assert!(applied_rv > 0);
}

#[tokio::test]
async fn local_client_pod_delete_outbox_reconciles_terminating_namespace() {
    let db = crate::bootstrap::composition::cluster_store::selector::canonical_sqlite_fixture()
        .await
        .unwrap();
    db.create_namespace(
        "worker-finalize-ns",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": "worker-finalize-ns",
                "uid": "worker-finalize-ns-uid"
            },
            "spec": {"finalizers": ["kubernetes"]},
            "status": {"phase": "Active"}
        }),
    )
    .await
    .expect("create namespace");
    let namespace = db
        .get_namespace("worker-finalize-ns")
        .await
        .expect("read namespace")
        .expect("namespace exists");
    let mut terminating = std::sync::Arc::unwrap_or_clone(namespace.data);
    k8s_native_service::set_namespace_terminating_status_at(
        &mut terminating,
        false,
        klights_supervisor::SystemWallClock::now_utc(),
    );
    db.update_namespace(
        "worker-finalize-ns",
        terminating,
        namespace.resource_version,
    )
    .await
    .expect("mark namespace terminating");
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("worker-finalize-ns"),
        "leftover-cm",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "namespace": "worker-finalize-ns",
                "name": "leftover-cm"
            },
            "data": {"k": "v"}
        }),
    )
    .await
    .expect("create non-pod content");
    let observed_pod = db
        .create_resource(
            "v1",
            "Pod",
            Some("worker-finalize-ns"),
            "worker-pod",
            serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "worker-finalize-ns",
                "name": "worker-pod",
                "uid": "worker-pod-uid",
                "deletionTimestamp": "2026-05-20T00:00:00Z",
                "deletionGracePeriodSeconds": 0
            },
            "spec": {
                "nodeName": "worker-a",
                "containers": [{"name": "app", "image": "nginx"}]
            },
            "status": {"phase": "Running"}
            }),
        )
        .await
        .expect("create terminating pod");

    let (delivery, side_effects) = test_outbox_delivery(&db, "worker-a");
    side_effects.set_namespace_termination(
            crate::bootstrap::composition_adapters::api_state_adapter::RootNamespaceTerminationReconciler::new(
            crate::bootstrap::composition_adapters::api_state_adapter::RootNamespaceTerminationStore::new_with_commands(
                    db.focused_read_store(),
                    db.focused_read_store(),
                    crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::resource_commands_for_test(
                        Arc::new(db.clone()), Arc::new(db.clone()), db.focused_read_store(),
                    ),
                ),
                klights_controllers::side_effects::SideEffectMetrics::new(),
            ),
        );
    side_effects.set_non_pod_finalization(Arc::new(
        crate::bootstrap::controller_adapters::gc_delete_adapter::GcNonPodFinalizationAdapter::new_for_test(
            Arc::new(db.clone()),
            Arc::new(db.clone()),
            db.focused_read_store(),
            db.focused_read_store(),
        ),
    ));
    let dispatcher =
        crate::bootstrap::controller_adapters::controller_runtime_adapter::dispatcher_for_test(
            &db,
            Arc::new(klights_controllers::service::ServiceIpam::new(
                "10.43.128.0/17",
            )),
        );
    side_effects.set_controller_dispatcher(dispatcher);
    let applied = deliver_test_outbox(
        delivery.as_ref(),
        "worker-pod-actor-finalize-delete",
        OutboxOperation::PodMetadata,
        pod_delete_payload_for(
            "worker-finalize-ns",
            "worker-pod",
            "worker-pod-uid",
            observed_pod.resource_version,
        ),
        "client",
        1,
        1,
    )
    .await
    .expect("apply worker pod delete outbox");
    assert!(matches!(applied, OutboxApplyResult::Applied { .. }));

    assert!(
        db.get_resource("v1", "Pod", Some("worker-finalize-ns"), "worker-pod")
            .await
            .expect("get pod")
            .is_none(),
        "leader apply must remove the actor-finalized Pod row"
    );
    assert!(
        db.get_namespace("worker-finalize-ns")
            .await
            .expect("get namespace")
            .is_none(),
        "leader must reconcile namespace termination immediately after applying worker Pod delete"
    );
}

#[tokio::test]
async fn local_client_pod_delete_outbox_finalizes_ready_foreground_owner() {
    let db = crate::bootstrap::composition::cluster_store::selector::canonical_sqlite_fixture()
        .await
        .unwrap();
    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "foreground-owner",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "metadata": {
                "name": "foreground-owner",
                "namespace": "default",
                "uid": "foreground-owner-uid",
                "deletionTimestamp": "2026-05-17T00:00:00Z",
                "finalizers": ["foregroundDeletion"]
            },
            "spec": {"replicas": 1, "selector": {"app": "foreground-owner"}}
        }),
    )
    .await
    .expect("create foreground owner");
    let observed_child = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "foreground-child",
            serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "foreground-child",
                "namespace": "default",
                "uid": "foreground-child-uid",
                "deletionTimestamp": "2026-05-17T00:00:00Z",
                "deletionGracePeriodSeconds": 0,
                "ownerReferences": [{
                    "apiVersion": "v1",
                    "kind": "ReplicationController",
                    "name": "foreground-owner",
                    "uid": "foreground-owner-uid",
                    "controller": true,
                    "blockOwnerDeletion": true
                }]
            },
            "spec": {"nodeName": "worker-a", "containers": [{"name": "app", "image": "nginx"}]},
            "status": {"phase": "Running"}
            }),
        )
        .await
        .expect("create foreground child");

    let (delivery, side_effects) = test_outbox_delivery(&db, "worker-a");
    side_effects.set_non_pod_finalization(Arc::new(
        crate::bootstrap::controller_adapters::gc_delete_adapter::GcNonPodFinalizationAdapter::new_for_test(
            Arc::new(db.clone()),
            Arc::new(db.clone()),
            db.focused_read_store(),
            db.focused_read_store(),
        ),
    ));
    let dispatcher =
        crate::bootstrap::controller_adapters::controller_runtime_adapter::dispatcher_for_test(
            &db,
            Arc::new(klights_controllers::service::ServiceIpam::new(
                "10.43.128.0/17",
            )),
        );
    side_effects.set_controller_dispatcher(dispatcher);

    let applied = deliver_test_outbox(
        delivery.as_ref(),
        "foreground-child-actor-finalize-delete",
        OutboxOperation::PodMetadata,
        pod_delete_payload(
            "foreground-child",
            "foreground-child-uid",
            observed_child.resource_version,
        ),
        "client",
        1,
        1,
    )
    .await
    .expect("apply pod delete outbox");
    assert!(matches!(applied, OutboxApplyResult::Applied { .. }));

    assert!(
        db.get_resource("v1", "Pod", Some("default"), "foreground-child")
            .await
            .expect("get child")
            .is_none(),
        "leader apply must remove the finalized Pod row"
    );
    assert!(
        db.get_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "foreground-owner"
        )
        .await
        .expect("get foreground owner")
        .is_none(),
        "leader apply of the final dependent Pod delete must remove a ready foreground owner"
    );
}

#[tokio::test]
async fn local_client_serves_network_metadata_without_calling_forwarder() {
    let db = crate::bootstrap::composition::cluster_store::selector::canonical_sqlite_fixture()
        .await
        .unwrap();
    let network = test_network_port(&db);

    let subnet = network
        .allocate_node_subnet(
            NodeSubnetAllocationRequest::try_new("node-a", "10.42.0.0/16", "192.0.2.10")
                .expect("valid allocation request"),
        )
        .await
        .expect("allocate local subnet through leader API")
        .into_subnet();
    assert_eq!(subnet.node_name(), "node-a");
    assert_eq!(subnet.subnet(), "10.42.0.0/24");

    let stored = network
        .get_node_subnet(NodeSubnetQuery::try_new("node-a").expect("valid query"))
        .await
        .expect("get local subnet through leader API")
        .into_option()
        .expect("allocated subnet should exist");
    assert_eq!(stored, subnet);

    let peer = network
        .allocate_node_subnet(
            NodeSubnetAllocationRequest::try_new("node-b", "10.42.0.0/16", "192.0.2.11")
                .expect("valid allocation request"),
        )
        .await
        .expect("allocate peer subnet")
        .into_subnet();
    let peers = network
        .list_peer_subnets(PeerSubnetsQuery::try_new("node-a").expect("valid query"))
        .await
        .expect("list peer subnets through leader API")
        .into_vec();
    assert_eq!(peers, vec![peer]);

    let metadata = DataplanePeerMetadata::try_new(
        "node-b".to_string(),
        DataplaneMode::Root,
        DataplaneEncryption::Disabled,
        None,
        Some("192.0.2.11".to_string()),
        None,
    )
    .expect("valid dataplane metadata");
    db.update_node_dataplane(metadata.clone())
        .await
        .expect("store dataplane metadata");
    assert_eq!(
        network
            .get_node_dataplane(NodeDataplaneQuery::try_new("node-b").expect("valid query"),)
            .await
            .expect("get dataplane metadata through leader API")
            .into_option(),
        Some(
            crate::bootstrap::leader_conversions::topology::focused_dataplane(metadata)
                .expect("valid focused metadata"),
        )
    );
}
