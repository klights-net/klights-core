//! Focused root composition for Raft integration tests.

use std::sync::Arc;

use super::IntegrationDatastoreHandle;

/// Opaque feature-gated capability for the root's exact Raft composition.
///
/// The concrete datastore and adapter implementations remain private. Tests
/// receive only focused Raft ports and explicit fixture operations.
pub struct IntegrationRaftComposition {
    db: Arc<crate::datastore::sqlite::Datastore>,
}

impl IntegrationRaftComposition {
    pub const SNAPSHOT_EMIT_PAGE_SIZE: usize =
        crate::datastore::snapshot_export::SNAPSHOT_EMIT_PAGE_SIZE;

    pub fn new(db: Arc<crate::datastore::sqlite::Datastore>) -> Self {
        Self { db }
    }

    pub fn store_ports(&self) -> klights_replication::node::RaftStorePorts {
        crate::bootstrap::composition_adapters::cluster_store_replication_adapter::raft_store_ports_for_test(
            self.db.clone(),
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
        crate::bootstrap::controlplane_join_adapters::build_controlplane_join_handler(node, db)
    }

    pub async fn write_cluster_membership(
        &self,
        membership: &klights_cluster_core::ClusterMembership,
    ) -> anyhow::Result<()> {
        crate::bootstrap::cluster_meta::write_cluster_membership(self.db.as_ref(), membership).await
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
