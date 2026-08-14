//! Focused root composition for Raft integration tests.

use std::sync::Arc;

use crate::datastore::DatastoreHandle as IntegrationDatastoreHandle;

/// Opaque feature-gated capability for the root's exact Raft composition.
///
/// The concrete datastore and adapter implementations remain private. Tests
/// receive only focused Raft ports and explicit fixture operations.
pub struct IntegrationRaftComposition {
    db: Arc<crate::datastore::sqlite::Datastore>,
}

impl IntegrationRaftComposition {
    pub const SNAPSHOT_CAPTURE_PAGE_SIZE: usize = klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE;

    pub fn new(db: Arc<crate::datastore::sqlite::Datastore>) -> Self {
        Self { db }
    }

    pub fn store_ports(&self) -> klights_replication::node::RaftStorePorts {
        crate::bootstrap::composition_adapters::cluster_store_replication_adapter::raft_store_ports_for_test(
            self.db.clone(),
        )
    }

    fn resource_commands(
        &self,
        node: Arc<klights_replication::node::RaftNode>,
    ) -> Arc<dyn klights_leader_api::LeaderResourceCommand> {
        let db: IntegrationDatastoreHandle = self.db.clone();
        let authority =
            crate::bootstrap::composition_adapters::authority_adapter::always_leader_authority();
        let query = crate::bootstrap::composition_adapters::resource_query_adapter::DatastoreResourceQueryAdapter::new(
            db, authority.clone(),
        );
        Arc::new(
            klights_replication::leader_api::EmbeddedLeaderResourceCommand::new(
                node.proposal(),
                query,
                authority,
            ),
        )
    }

    pub fn commit_materializer(
        &self,
    ) -> Arc<dyn klights_replication::materializer::RaftCommitMaterializer> {
        let db: IntegrationDatastoreHandle = self.db.clone();
        Arc::new(
            crate::bootstrap::composition_adapters::cluster_store_replication_adapter::DatastoreRaftCommitMaterializer::new(db),
        )
    }

    pub async fn state_machine(
        &self,
        applied_state: Arc<dyn klights_node_store::RaftAppliedStateDurability>,
        snapshot_applied_state: Arc<dyn klights_node_store::RaftAppliedStateDurability>,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> (
        klights_replication::state_machine::SqliteRaftStateMachine<
            klights_replication::snapshot::SqliteRaftSnapshotBuilder,
        >,
        Arc<klights_replication::activation::CommandCodecV3Activation>,
    ) {
        let materializer = self.commit_materializer();
        let activation = Arc::new(
            klights_replication::activation::CommandCodecV3Activation::load(materializer.as_ref())
                .await
                .expect("load command codec activation"),
        );
        let stores = crate::bootstrap::composition_adapters::cluster_store_replication_adapter::raft_state_machine_store_ports_for_test(self.db.clone());
        let snapshot_builder = klights_replication::snapshot::SqliteRaftSnapshotBuilder::new(
            self.db.focused_recovery_store(),
            self.db.focused_read_store(),
            Arc::new(crate::datastore::DatastoreBackendLifecyclePort::new(
                self.db.clone(),
            )),
            snapshot_applied_state,
            supervisor,
        );
        (
            klights_replication::state_machine::SqliteRaftStateMachine::new_with_command_codec_activation(
                stores,
                applied_state,
                snapshot_builder,
                activation.clone(),
            ),
            activation,
        )
    }

    pub fn controlplane_join_handler(
        &self,
        node: Arc<klights_replication::node::RaftNode>,
    ) -> Arc<dyn klights_leader_api::ControlplaneJoinHandler> {
        let db: IntegrationDatastoreHandle = self.db.clone();
        let authority =
            crate::bootstrap::composition_adapters::authority_adapter::always_leader_authority();
        let query =
            crate::bootstrap::composition_adapters::resource_query_adapter::DatastoreResourceQueryAdapter::new(
                db.clone(), authority.clone(),
            );
        let commands = Arc::new(
            klights_replication::leader_api::EmbeddedLeaderResourceCommand::new(
                Arc::new(
                    crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(db.clone()),
                ),
                query,
                authority,
            ),
        );
        let store = Arc::new(
            crate::bootstrap::composition_adapters::leader_bootstrap_store_adapter::LeaderBootstrapStore::new(
                db.clone(),
                commands,
            ),
        );
        crate::bootstrap::controlplane_join_adapters::build_controlplane_join_handler(
            node, db, store,
        )
    }

    pub fn controlplane_join_handler_with_raft_store(
        &self,
        node: Arc<klights_replication::node::RaftNode>,
    ) -> Arc<dyn klights_leader_api::ControlplaneJoinHandler> {
        let db: IntegrationDatastoreHandle = self.db.clone();
        let authority =
            crate::bootstrap::composition_adapters::authority_adapter::always_leader_authority();
        let query =
            crate::bootstrap::composition_adapters::resource_query_adapter::DatastoreResourceQueryAdapter::new(
                db.clone(), authority.clone(),
            );
        let commands = Arc::new(
            klights_replication::leader_api::EmbeddedLeaderResourceCommand::new(
                node.proposal(),
                query,
                authority,
            ),
        );
        let store = Arc::new(
            crate::bootstrap::composition_adapters::leader_bootstrap_store_adapter::LeaderBootstrapStore::new(
                db.clone(),
                commands,
            ),
        );
        crate::bootstrap::controlplane_join_adapters::build_controlplane_join_handler(
            node, db, store,
        )
    }

    pub async fn create_pod_through_root_persistence(
        &self,
        node: Arc<klights_replication::node::RaftNode>,
        namespace: &str,
        name: &str,
        body: serde_json::Value,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        let db: IntegrationDatastoreHandle = self.db.clone();
        let authority =
            crate::bootstrap::composition_adapters::authority_adapter::always_leader_authority();
        let query = crate::bootstrap::composition_adapters::resource_query_adapter::DatastoreResourceQueryAdapter::new(
            db.clone(), authority.clone(),
        );
        let commands = Arc::new(
            klights_replication::leader_api::EmbeddedLeaderResourceCommand::new(
                node.proposal(),
                query,
                authority,
            ),
        );
        let parts = crate::bootstrap::composition_adapters::pod_repository_persistence_adapter::new_raft_root_parts(
            self.db.clone(),
            commands,
            Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
        );
        parts.store.create(namespace, name, body).await
    }

    pub async fn approve_csr_through_controller(
        &self,
        node: Arc<klights_replication::node::RaftNode>,
        name: &str,
        uid: &str,
        resource_version: i64,
    ) -> klights_reconcile_api::ControllerStoreResult<()> {
        let db: IntegrationDatastoreHandle = self.db.clone();
        let authority =
            crate::bootstrap::composition_adapters::authority_adapter::always_leader_authority();
        let query = crate::bootstrap::composition_adapters::resource_query_adapter::DatastoreResourceQueryAdapter::new(
            db.clone(), authority.clone(),
        );
        let commands = Arc::new(
            klights_replication::leader_api::EmbeddedLeaderResourceCommand::new(
                node.proposal(),
                query,
                authority,
            ),
        );
        let port = crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::new_with_commands(
            db, commands,
        );
        klights_controllers::csr_signer::CsrStatusStore::update_csr_status(
            &port,
            name,
            uid,
            resource_version,
            serde_json::json!({"conditions": [{"type": "Approved", "status": "True"}]}),
        )
        .await
    }

    pub async fn create_namespace_defaults_through_root_adapters(
        &self,
        node: Arc<klights_replication::node::RaftNode>,
        namespace: &str,
    ) -> anyhow::Result<()> {
        let identity =
            crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity();
        let store = crate::bootstrap::composition_adapters::leader_bootstrap_store_adapter::LeaderBootstrapStore::new(
            self.db.clone(),
            self.resource_commands(node),
        );
        klights_controllers::namespace::create_default_service_account_at(
            &store,
            namespace,
            chrono::Utc::now(),
            identity.as_ref(),
        )
        .await?;
        klights_controllers::namespace::create_kube_root_ca_configmap_at(
            &store,
            namespace,
            "test-cluster-ca",
            chrono::Utc::now(),
            identity.as_ref(),
        )
        .await
    }

    pub async fn reconcile_namespace_termination_through_root_adapters(
        &self,
        node: Arc<klights_replication::node::RaftNode>,
        namespace: &str,
        namespace_uid: &str,
    ) -> anyhow::Result<bool> {
        let db: IntegrationDatastoreHandle = self.db.clone();
        let store = crate::bootstrap::composition_adapters::api_state_adapter::RootNamespaceTerminationStore::new_with_commands(
            db,
            self.resource_commands(node),
        );
        let outcome = k8s_native_service::reconcile_namespace_termination_for_uid_with_outcome_at(
            store.as_ref(),
            namespace,
            namespace_uid,
            klights_controllers::side_effects::SideEffectMetrics::new().as_ref(),
            chrono::Utc::now(),
        )
        .await
        .map_err(|error| anyhow::anyhow!("{error:?}"))?;
        Ok(matches!(
            outcome,
            k8s_native_service::NamespaceTerminationOutcome::Finalized
        ))
    }

    pub async fn read_cluster_membership(
        &self,
    ) -> anyhow::Result<klights_cluster_core::ClusterMembership> {
        crate::bootstrap::cluster_meta::read_cluster_membership(self.db.as_ref()).await
    }

    pub fn inject_resource_version(
        data: impl Into<Arc<serde_json::Value>>,
        resource_version: i64,
    ) -> serde_json::Value {
        crate::bootstrap::controller_adapters::controller_runtime_adapter::inject_resource_version(
            data,
            resource_version,
        )
    }
}
