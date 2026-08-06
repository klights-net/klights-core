pub(crate) mod node_delivery_support {
    //! Feature-gated real-adapter helpers for node-delivery integration tests.

    use std::sync::Arc;

    use anyhow::Result;
    use klights_kubelet::node_outbox::{Outbox, OutboxDispatcher, OutboxStores};
    use klights_leader_api::LeaderOutboxDelivery;
    use tokio::sync::Notify;

    use crate::datastore::DatastoreBackend as _;
    use crate::datastore::node_local::{LegacyDeliveryTestStore as _, NodeLocalStores};

    #[derive(Clone)]
    pub struct IntegrationNodeDeliveryCluster {
        db: Arc<crate::datastore::sqlite::Datastore>,
    }

    impl IntegrationNodeDeliveryCluster {
        pub async fn open() -> Result<Self> {
            Ok(Self {
                db: Arc::new(crate::datastore::sqlite::Datastore::new_in_memory().await?),
            })
        }

        pub async fn seed_node(
            &self,
            name: &str,
            value: serde_json::Value,
        ) -> Result<klights_cluster_core::Resource> {
            self.db
                .create_resource("v1", "Node", None, name, value)
                .await
        }

        pub async fn observe_node(
            &self,
            name: &str,
        ) -> Result<Option<klights_cluster_core::Resource>> {
            self.db.get_resource("v1", "Node", None, name).await
        }

        pub async fn replace_node_if_current(
            &self,
            name: &str,
            value: serde_json::Value,
            current: &klights_cluster_core::Resource,
        ) -> Result<klights_cluster_core::Resource> {
            self.db
                .update_resource_with_preconditions(
                    "v1",
                    "Node",
                    None,
                    name,
                    value,
                    klights_cluster_core::ResourcePreconditions::from_resource(current),
                )
                .await
        }

        pub async fn allocate_node_subnet(
            &self,
            node_name: &str,
            cluster_cidr: &str,
            node_ip: &str,
        ) -> Result<()> {
            self.db
                .allocate_node_subnet(node_name, cluster_cidr, node_ip)
                .await
                .map(|_| ())
        }

        pub async fn seed_pod(
            &self,
            namespace: &str,
            name: &str,
            value: serde_json::Value,
        ) -> Result<klights_cluster_core::Resource> {
            self.db
                .create_resource("v1", "Pod", Some(namespace), name, value)
                .await
        }

        pub async fn mark_pod_terminating(
            &self,
            namespace: &str,
            name: &str,
            value: serde_json::Value,
            expected_rv: i64,
        ) -> Result<klights_cluster_core::Resource> {
            self.db
                .update_resource("v1", "Pod", Some(namespace), name, value, expected_rv)
                .await
        }

        pub async fn observe_pod(
            &self,
            namespace: &str,
            name: &str,
        ) -> Result<Option<klights_cluster_core::Resource>> {
            self.db
                .get_resource("v1", "Pod", Some(namespace), name)
                .await
        }

        pub async fn public_resource_version(&self) -> Result<i64> {
            self.db.get_current_resource_version().await
        }

        pub async fn watch_replay_position(
            &self,
        ) -> Result<klights_cluster_core::WatchReplayPosition> {
            self.db.current_watch_replay_position().await
        }

        pub async fn outbox_stream_watermarks(
            &self,
        ) -> Result<Vec<klights_cluster_core::OutboxStreamWatermark>> {
            self.db.list_outbox_stream_watermarks().await
        }

        pub async fn seed_namespace(&self, name: &str, value: serde_json::Value) -> Result<()> {
            self.db.create_namespace(name, value).await.map(|_| ())
        }

        pub async fn observe_events(
            &self,
            namespace: &str,
        ) -> Result<Vec<klights_cluster_core::Resource>> {
            Ok(self
                .db
                .list_resources(
                    "v1",
                    "Event",
                    Some(namespace),
                    crate::datastore::ResourceListQuery::all(),
                )
                .await?
                .items)
        }

        pub async fn observe_events_all_namespaces(
            &self,
        ) -> Result<Vec<klights_cluster_core::Resource>> {
            Ok(self
                .db
                .list_resources(
                    "v1",
                    "Event",
                    None,
                    crate::datastore::ResourceListQuery::all(),
                )
                .await?
                .items)
        }

        pub async fn apply_outbox_event_create(&self, payload: &[u8]) -> Result<()> {
            let command =
                klights_leader_rpc::storage_wire_codec::decode_outbox_payload_protobuf(payload)?
                    .into_command();
            match command {
                klights_cluster_core::StorageCommand::CreateResource {
                    api_version,
                    kind,
                    namespace,
                    name,
                    data,
                    ..
                } => self
                    .db
                    .create_resource(&api_version, &kind, namespace.as_deref(), &name, data)
                    .await
                    .map(|_| ()),
                other => {
                    anyhow::bail!("unsupported outbox command in pod-event fixture: {other:?}")
                }
            }
        }

        pub fn node_ports(
            &self,
            authenticated_node: &str,
        ) -> crate::integration_test_harness::leader_rpc::IntegrationLeaderRpcNodePorts {
            crate::integration_test_harness::leader_rpc::IntegrationLeaderRpcComposition::local_node_ports(
                self.db.clone(),
                authenticated_node.to_string(),
            )
        }

        pub fn heartbeat_event_source(
            &self,
        ) -> Arc<dyn klights_kubelet::node_heartbeat::NodeHeartbeatEventSource> {
            let passive = crate::integration_test_harness::leader_rpc::IntegrationLeaderRpcComposition::passive_reads_for(
                self.db.as_ref(),
            );
            crate::integration_test_harness::leader_rpc::IntegrationLeaderRpcComposition::node_heartbeat_event_source(
                &passive,
                self.db.clone(),
            )
        }

        pub async fn observe_lease_resource_version(&self, node_name: &str) -> Result<Option<i64>> {
            Ok(self
                .db
                .get_resource(
                    "coordination.k8s.io/v1",
                    "Lease",
                    Some("kube-node-lease"),
                    node_name,
                )
                .await?
                .map(|resource| resource.resource_version))
        }

        pub async fn register_node_snapshot(
            &self,
            outbox: Option<&IntegrationNodeOutbox>,
            dataplane_health: Option<&klights_network_api::DataplaneHealthSnapshot>,
            snapshot: &klights_kubelet::node::NodeRegistrationSnapshot,
        ) -> Result<()> {
            crate::bootstrap::node_registration_adapter::register_node_snapshot(
                self.db.as_ref(),
                outbox.map(IntegrationNodeOutbox::inner),
                dataplane_health,
                snapshot,
            )
            .await
        }

        fn pod_event_query(&self) -> IntegrationPodEventAdapter<'_> {
            IntegrationPodEventAdapter::new(self.db.as_ref())
        }

        pub async fn emit_pod_event_to_outbox(
            &self,
            outbox: &IntegrationNodeOutbox,
            record: klights_kubelet::pod_events::PodEventRecord<'_>,
        ) -> Result<serde_json::Value> {
            let adapter = self.pod_event_query();
            outbox.emit_pod_event(&adapter, record).await
        }

        pub async fn emit_control_plane_pod_event(
            &self,
            record: klights_kubelet::pod_events::PodEventRecord<'_>,
        ) -> Result<serde_json::Value> {
            let adapter = self.pod_event_query();
            klights_kubelet::pod_events::emit_control_plane_pod_event(&adapter, &adapter, record)
                .await
        }

        pub async fn reject_pod_event_without_outbox(
            &self,
            record: klights_kubelet::pod_events::PodEventRecord<'_>,
        ) -> Result<serde_json::Value> {
            let adapter = self.pod_event_query();
            klights_kubelet::pod_events::emit_pod_event_with_outbox(&adapter, None, record).await
        }
    }

    struct IntegrationPodEventAdapter<'a> {
        inner:
            crate::bootstrap::composition_adapters::pod_event_adapter::DatastorePodEventAdapter<'a>,
    }

    impl<'a> IntegrationPodEventAdapter<'a> {
        fn new(db: &'a dyn crate::datastore::DatastoreBackend) -> Self {
            Self {
            inner: crate::bootstrap::composition_adapters::pod_event_adapter::DatastorePodEventAdapter::new(db),
        }
        }
    }

    #[async_trait::async_trait]
    impl klights_kubelet::pod_events::PodEventQuery for IntegrationPodEventAdapter<'_> {
        async fn namespace_eligibility(
            &self,
            namespace: &str,
        ) -> Result<klights_kubelet::pod_events::PodEventNamespaceEligibility> {
            klights_kubelet::pod_events::PodEventQuery::namespace_eligibility(
                &self.inner,
                namespace,
            )
            .await
        }
        async fn list_events(
            &self,
            namespace: &str,
        ) -> Result<Vec<klights_cluster_core::Resource>> {
            klights_kubelet::pod_events::PodEventQuery::list_events(&self.inner, namespace).await
        }
    }

    #[async_trait::async_trait]
    impl klights_kubelet::pod_events::PodEventEffect for IntegrationPodEventAdapter<'_> {
        async fn create_event(
            &self,
            namespace: &str,
            name: &str,
            event: serde_json::Value,
        ) -> Result<()> {
            klights_kubelet::pod_events::PodEventEffect::create_event(
                &self.inner,
                namespace,
                name,
                event,
            )
            .await
        }
    }

    pub fn author_bound_pod_finalization(
        namespace: String,
        name: String,
        pod_uid: String,
        node_name: String,
        observed_resource_version: i64,
    ) -> klights_cluster_core::StorageCommand {
        crate::bootstrap::composition_adapters::bound_pod_finalization_adapter::author(
            namespace,
            name,
            pod_uid,
            node_name,
            observed_resource_version,
        )
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct IntegrationNodeDeliveryOutboxInsert {
        pub idempotency_key: String,
        pub enqueued_ms: i64,
        pub subject_key: String,
        pub subject_api_version: String,
        pub subject_kind: String,
        pub subject_namespace: Option<String>,
        pub subject_name: String,
        pub subject_uid: Option<String>,
        pub pod_uid: String,
        pub operation: String,
        pub classification: klights_node_store::OutboxClassification,
        pub payload_proto: Vec<u8>,
        pub next_due_ms: i64,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct IntegrationNodeDeliveryOutboxRow {
        pub id: i64,
        pub client_id: String,
        pub idempotency_key: String,
        pub enqueued_ms: i64,
        pub subject_key: String,
        pub subject_api_version: String,
        pub subject_kind: String,
        pub subject_namespace: Option<String>,
        pub subject_name: String,
        pub subject_uid: Option<String>,
        pub pod_uid: String,
        pub operation: String,
        pub priority_class: i64,
        pub supersedable_pod_status: bool,
        pub is_terminal_pod_delete: bool,
        pub stream_id: i64,
        pub stream_seq: i64,
        pub payload_proto: Vec<u8>,
        pub attempt: i64,
        pub next_due_ms: i64,
        pub leased_until_ms: i64,
        pub lease_token: Option<String>,
        pub last_error: Option<String>,
    }

    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
    pub struct IntegrationNodeDeliveryDeadLetterRow {
        pub id: i64,
        pub original_id: i64,
        pub client_id: String,
        pub idempotency_key: String,
        pub enqueued_ms: i64,
        pub subject_key: String,
        pub subject_api_version: String,
        pub subject_kind: String,
        pub subject_namespace: Option<String>,
        pub subject_name: String,
        pub subject_uid: Option<String>,
        pub pod_uid: String,
        pub operation: String,
        pub stream_id: i64,
        pub stream_seq: i64,
        pub payload_proto: Vec<u8>,
        pub attempts: i64,
        pub last_error: String,
        pub moved_at_ms: i64,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct IntegrationNodeDeliveryDeadLetterInsert<'a> {
        pub idempotency_key: &'a str,
        pub operation: &'a str,
        pub subject_key: &'a str,
        pub subject_api_version: &'a str,
        pub subject_kind: &'a str,
        pub subject_namespace: Option<&'a str>,
        pub subject_name: &'a str,
        pub subject_uid: Option<&'a str>,
        pub pod_uid: &'a str,
        pub payload_proto: &'a [u8],
        pub attempts: i64,
        pub last_error: &'a str,
        pub moved_at_ms: i64,
    }

    #[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
    pub struct IntegrationNodeDeliveryOutboxStats {
        pub pending: i64,
        pub oldest_age_seconds: f64,
        pub dead_letter_count: i64,
        pub dispatch_total: i64,
        pub dispatch_errors_total: i64,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct IntegrationNodeDeliveryPodStatusCheckpoint {
        pub pod_uid: String,
        pub namespace: String,
        pub pod_name: String,
        pub base_rv: i64,
        pub applied_rv: Option<i64>,
        pub status: serde_json::Value,
        pub updated_ms: i64,
    }

    impl From<crate::datastore::node_local::OutboxRow> for IntegrationNodeDeliveryOutboxRow {
        fn from(row: crate::datastore::node_local::OutboxRow) -> Self {
            Self {
                id: row.id,
                client_id: row.client_id,
                idempotency_key: row.idempotency_key,
                enqueued_ms: row.enqueued_ms,
                subject_key: row.subject_key,
                subject_api_version: row.subject_api_version,
                subject_kind: row.subject_kind,
                subject_namespace: row.subject_namespace,
                subject_name: row.subject_name,
                subject_uid: row.subject_uid,
                pod_uid: row.pod_uid,
                operation: row.operation,
                priority_class: row.priority_class,
                supersedable_pod_status: row.supersedable_pod_status,
                is_terminal_pod_delete: row.is_terminal_pod_delete,
                stream_id: row.stream_id,
                stream_seq: row.stream_seq,
                payload_proto: row.payload_proto,
                attempt: row.attempt,
                next_due_ms: row.next_due_ms,
                leased_until_ms: row.leased_until_ms,
                lease_token: row.lease_token,
                last_error: row.last_error,
            }
        }
    }

    impl From<crate::datastore::node_local::DeadLetterRow> for IntegrationNodeDeliveryDeadLetterRow {
        fn from(row: crate::datastore::node_local::DeadLetterRow) -> Self {
            Self {
                id: row.id,
                original_id: row.original_id,
                client_id: row.client_id,
                idempotency_key: row.idempotency_key,
                enqueued_ms: row.enqueued_ms,
                subject_key: row.subject_key,
                subject_api_version: row.subject_api_version,
                subject_kind: row.subject_kind,
                subject_namespace: row.subject_namespace,
                subject_name: row.subject_name,
                subject_uid: row.subject_uid,
                pod_uid: row.pod_uid,
                operation: row.operation,
                stream_id: row.stream_id,
                stream_seq: row.stream_seq,
                payload_proto: row.payload_proto,
                attempts: row.attempts,
                last_error: row.last_error,
                moved_at_ms: row.moved_at_ms,
            }
        }
    }

    #[derive(Clone)]
    pub struct IntegrationNodeOutbox {
        inner: Outbox,
    }

    impl IntegrationNodeOutbox {
        pub async fn record_pod_status_checkpoint(
            &self,
            pod: &klights_cluster_core::Resource,
            status: serde_json::Value,
            updated_ms: i64,
        ) -> Result<()> {
            self.inner
                .record_pod_status_checkpoint(pod, status, updated_ms)
                .await
        }

        pub async fn enqueue_command(
            &self,
            command: klights_kubelet::node_outbox::OutboxCommand,
        ) -> Result<()> {
            self.inner.enqueue_command(command).await
        }

        pub async fn merge_pod_status_checkpoint(
            &self,
            pod: klights_cluster_core::Resource,
        ) -> Result<klights_cluster_core::Resource> {
            self.inner.merge_pod_status_checkpoint(pod).await
        }

        pub async fn record_runtime_observation_checkpoint(
            &self,
            pod_uid: &str,
            container_ids: Vec<String>,
            generation: u64,
            updated_ms: i64,
        ) -> Result<()> {
            self.inner
                .record_runtime_observation_checkpoint(
                    pod_uid,
                    container_ids,
                    generation,
                    updated_ms,
                )
                .await
        }

        pub async fn get_runtime_observation_checkpoint(
            &self,
            pod_uid: &str,
        ) -> Result<Option<klights_kubelet::node_outbox::RuntimeObservationCheckpointState>>
        {
            self.inner.get_runtime_observation_checkpoint(pod_uid).await
        }

        pub async fn delete_runtime_observation_checkpoint(&self, pod_uid: &str) -> Result<()> {
            self.inner
                .delete_runtime_observation_checkpoint(pod_uid)
                .await
        }

        pub async fn next_status_stamp_at(&self, now_us: i64) -> Result<i64> {
            klights_kubelet::node_outbox::next_status_stamp_with_clock_for_integration_test(
                &self.inner,
                now_us,
            )
            .await
        }

        async fn emit_pod_event(
            &self,
            query: &IntegrationPodEventAdapter<'_>,
            record: klights_kubelet::pod_events::PodEventRecord<'_>,
        ) -> Result<serde_json::Value> {
            klights_kubelet::pod_events::emit_pod_event_with_outbox(
                query,
                Some(&self.inner),
                record,
            )
            .await
        }

        pub(crate) fn inner(&self) -> &Outbox {
            &self.inner
        }
    }

    impl klights_leader_api::NodeOutbox for IntegrationNodeOutbox {
        fn enqueue(
            &self,
            command: klights_leader_api::NodeOutboxCommand,
        ) -> klights_leader_api::NodeOutboxFuture<'_, klights_leader_api::NodeOutboxRoute> {
            klights_leader_api::NodeOutbox::enqueue(&self.inner, command)
        }
        fn next_status_stamp(&self) -> klights_leader_api::NodeOutboxFuture<'_, i64> {
            klights_leader_api::NodeOutbox::next_status_stamp(&self.inner)
        }
        fn record_pod_status_checkpoint<'a>(
            &'a self,
            checkpoint: &'a klights_cluster_core::Resource,
            updated_ms: i64,
        ) -> klights_leader_api::NodeOutboxFuture<'a, ()> {
            klights_leader_api::NodeOutbox::record_pod_status_checkpoint(
                &self.inner,
                checkpoint,
                updated_ms,
            )
        }
        fn merge_pod_status_checkpoint(
            &self,
            pod: klights_cluster_core::Resource,
        ) -> klights_leader_api::NodeOutboxFuture<'_, klights_cluster_core::Resource> {
            klights_leader_api::NodeOutbox::merge_pod_status_checkpoint(&self.inner, pod)
        }
        fn delete_pod_status_checkpoint<'a>(
            &'a self,
            pod_uid: &'a str,
        ) -> klights_leader_api::NodeOutboxFuture<'a, ()> {
            klights_leader_api::NodeOutbox::delete_pod_status_checkpoint(&self.inner, pod_uid)
        }
        fn record_runtime_observation_checkpoint<'a>(
            &'a self,
            pod_uid: &'a str,
            container_ids: Vec<String>,
            generation: u64,
            updated_ms: i64,
        ) -> klights_leader_api::NodeOutboxFuture<'a, ()> {
            klights_leader_api::NodeOutbox::record_runtime_observation_checkpoint(
                &self.inner,
                pod_uid,
                container_ids,
                generation,
                updated_ms,
            )
        }
        fn get_runtime_observation_checkpoint<'a>(
            &'a self,
            pod_uid: &'a str,
        ) -> klights_leader_api::NodeOutboxFuture<
            'a,
            Option<klights_leader_api::NodeRuntimeObservationCheckpoint>,
        > {
            klights_leader_api::NodeOutbox::get_runtime_observation_checkpoint(&self.inner, pod_uid)
        }
        fn delete_runtime_observation_checkpoint<'a>(
            &'a self,
            pod_uid: &'a str,
        ) -> klights_leader_api::NodeOutboxFuture<'a, ()> {
            klights_leader_api::NodeOutbox::delete_runtime_observation_checkpoint(
                &self.inner,
                pod_uid,
            )
        }
    }

    pub struct IntegrationNodeDispatcher {
        inner: OutboxDispatcher,
    }

    impl IntegrationNodeDispatcher {
        pub async fn dispatch_due_once(
            &self,
            now_ms: i64,
        ) -> Result<klights_kubelet::node_outbox::DispatchOutcome> {
            self.inner.dispatch_due_once(now_ms).await
        }

        pub fn rtt_estimate_ms(&self) -> i64 {
            self.inner.rtt_estimate_ms()
        }
    }

    #[derive(Clone)]
    pub struct IntegrationNodeDeliveryStore {
        stores: NodeLocalStores,
    }

    impl IntegrationNodeDeliveryStore {
        pub async fn open(connection_key: &'static str) -> Result<Self> {
            Ok(Self {
                stores: crate::datastore::node_local::selector::open_node_local(
                    crate::datastore::backend_kind::BackendKind::Sqlite,
                    None,
                    Arc::new(klights_supervisor::TaskSupervisor::new(
                        klights_supervisor::TaskCategoryConfig::default(),
                    )),
                    None,
                    connection_key,
                )
                .await?,
            })
        }

        pub async fn open_with_sqlite(
            connection_key: &'static str,
        ) -> Result<(Self, Option<Self>)> {
            let (stores, sqlite) =
                crate::datastore::node_local::selector::open_node_local_with_sqlite(
                    crate::datastore::backend_kind::BackendKind::Sqlite,
                    None,
                    Arc::new(klights_supervisor::TaskSupervisor::new(
                        klights_supervisor::TaskCategoryConfig::default(),
                    )),
                    None,
                    connection_key,
                )
                .await?;
            Ok((
                Self { stores },
                sqlite.map(|stores| Self {
                    stores: (*stores).clone(),
                }),
            ))
        }

        pub fn outbox(&self) -> IntegrationNodeOutbox {
            IntegrationNodeOutbox {
                inner: outbox_from_node_db(self.stores.clone()),
            }
        }

        pub fn outbox_with_notify(&self, notify: Arc<Notify>) -> IntegrationNodeOutbox {
            IntegrationNodeOutbox {
                inner: outbox_with_notify(self.stores.clone(), notify),
            }
        }

        pub fn dispatcher(
            &self,
            client: Arc<dyn LeaderOutboxDelivery>,
        ) -> IntegrationNodeDispatcher {
            IntegrationNodeDispatcher {
                inner: dispatcher_for_tests(self.stores.clone(), client),
            }
        }

        pub fn dispatcher_with_notify(
            &self,
            client: Arc<dyn LeaderOutboxDelivery>,
            notify: Arc<Notify>,
        ) -> IntegrationNodeDispatcher {
            IntegrationNodeDispatcher {
                inner: dispatcher_with_notify(self.stores.clone(), client, notify),
            }
        }

        pub fn dispatcher_with_rtt(
            &self,
            client: Arc<dyn LeaderOutboxDelivery>,
            rtt: Arc<klights_types::RttEstimator>,
        ) -> IntegrationNodeDispatcher {
            IntegrationNodeDispatcher {
                inner: dispatcher_with_rtt_estimator(self.stores.clone(), client, rtt),
            }
        }

        pub fn dispatcher_with_lease_renewal(
            &self,
            client: Arc<dyn LeaderOutboxDelivery>,
            supervisor: Arc<klights_supervisor::TaskSupervisor>,
            lease_ms: i64,
        ) -> IntegrationNodeDispatcher {
            IntegrationNodeDispatcher {
                inner: dispatcher_for_tests(self.stores.clone(), client)
                    .with_lease_renewal_for_test(supervisor, lease_ms),
            }
        }

        pub fn batch_dispatcher(
            &self,
            client: Arc<dyn LeaderOutboxDelivery>,
            batch_size: usize,
        ) -> IntegrationNodeDispatcher {
            IntegrationNodeDispatcher {
                inner: dispatcher_for_tests(self.stores.clone(), client)
                    .with_batch_mode(batch_size),
            }
        }

        pub fn production_dispatcher(
            &self,
            client: Arc<dyn LeaderOutboxDelivery>,
            notify: Arc<Notify>,
        ) -> IntegrationNodeDispatcher {
            IntegrationNodeDispatcher {
                inner: dispatcher_with_notify(self.stores.clone(), client, notify)
                    .with_batch_mode(klights_kubelet::node_outbox::PRODUCTION_DISPATCH_BATCH_SIZE),
            }
        }

        pub async fn enqueue_fixture_row(
            &self,
            row: IntegrationNodeDeliveryOutboxInsert,
        ) -> Result<()> {
            self.stores
                .legacy_enqueue_outbox(crate::datastore::node_local::OutboxInsert {
                    idempotency_key: row.idempotency_key,
                    enqueued_ms: row.enqueued_ms,
                    subject_key: row.subject_key,
                    subject_api_version: row.subject_api_version,
                    subject_kind: row.subject_kind,
                    subject_namespace: row.subject_namespace,
                    subject_name: row.subject_name,
                    subject_uid: row.subject_uid,
                    pod_uid: row.pod_uid,
                    operation: row.operation,
                    classification: row.classification,
                    payload_proto: row.payload_proto,
                    next_due_ms: row.next_due_ms,
                })
                .await
        }
        pub async fn claim_next_due(
            &self,
            now_ms: i64,
            lease_ms: i64,
            lease_token: &str,
        ) -> Result<Option<IntegrationNodeDeliveryOutboxRow>> {
            self.stores
                .legacy_claim_next_due_outbox(now_ms, lease_ms, lease_token)
                .await
                .map(|row| row.map(Into::into))
        }
        pub async fn fail_claim_attempt(
            &self,
            id: i64,
            lease_token: &str,
            backoff_until_ms: i64,
            error: &str,
        ) -> Result<bool> {
            self.stores
                .legacy_mark_outbox_attempt_failed(id, lease_token, backoff_until_ms, error)
                .await
        }
        pub async fn complete_claim(&self, id: i64, lease_token: &str) -> Result<bool> {
            self.stores.legacy_complete_outbox(id, lease_token).await
        }
        pub async fn claim_due_batch(
            &self,
            now_ms: i64,
            limit: usize,
            lease_ms: i64,
            lease_token: &str,
        ) -> Result<Vec<IntegrationNodeDeliveryOutboxRow>> {
            self.stores
                .legacy_claim_due_outbox_batch(now_ms, limit, lease_ms, lease_token)
                .await
                .map(|rows| rows.into_iter().map(Into::into).collect())
        }
        pub async fn requeue_expired_leases(&self, now_ms: i64) -> Result<usize> {
            self.stores
                .legacy_requeue_expired_outbox_leases(now_ms)
                .await
        }
        pub async fn next_wake_ms(&self, now_ms: i64) -> Result<Option<i64>> {
            self.stores.legacy_next_outbox_wake_ms(now_ms).await
        }
        pub async fn dead_letter_at_attempt_limit(&self, key: &str, max: i64) -> Result<bool> {
            self.stores
                .legacy_move_outbox_to_dead_letter_if_max_attempts(key, max)
                .await
        }
        pub async fn list_dead_letters(&self) -> Result<Vec<IntegrationNodeDeliveryDeadLetterRow>> {
            self.stores
                .legacy_list_dead_letter()
                .await
                .map(|rows| rows.into_iter().map(Into::into).collect())
        }
        pub async fn delete_dead_letter(&self, id: i64) -> Result<bool> {
            self.stores.legacy_delete_dead_letter(id).await
        }
        pub async fn replay_dead_letter(
            &self,
            id: i64,
            classification: klights_node_store::OutboxClassification,
        ) -> Result<bool> {
            self.stores
                .legacy_replay_dead_letter(id, classification)
                .await
        }
        pub async fn delivery_stats(&self) -> Result<IntegrationNodeDeliveryOutboxStats> {
            self.stores.legacy_outbox_stats().await.map(|stats| {
                IntegrationNodeDeliveryOutboxStats {
                    pending: stats.pending,
                    oldest_age_seconds: stats.oldest_age_seconds,
                    dead_letter_count: stats.dead_letter_count,
                    dispatch_total: stats.dispatch_total,
                    dispatch_errors_total: stats.dispatch_errors_total,
                }
            })
        }
        pub async fn upsert_pod_status_checkpoint(
            &self,
            uid: &str,
            namespace: &str,
            name: &str,
            rv: i64,
            status: serde_json::Value,
            updated_ms: i64,
        ) -> Result<()> {
            self.stores
                .legacy_upsert_pod_status_checkpoint(uid, namespace, name, rv, status, updated_ms)
                .await
        }
        pub async fn get_pod_status_checkpoint(
            &self,
            uid: &str,
        ) -> Result<Option<IntegrationNodeDeliveryPodStatusCheckpoint>> {
            self.stores
                .legacy_get_pod_status_checkpoint(uid)
                .await
                .map(|row| {
                    row.map(|checkpoint| IntegrationNodeDeliveryPodStatusCheckpoint {
                        pod_uid: checkpoint.pod_uid,
                        namespace: checkpoint.namespace,
                        pod_name: checkpoint.pod_name,
                        base_rv: checkpoint.base_rv,
                        applied_rv: checkpoint.applied_rv,
                        status: checkpoint.status,
                        updated_ms: checkpoint.updated_ms,
                    })
                })
        }
        pub async fn mark_pod_status_checkpoint_applied(
            &self,
            uid: &str,
            rv: i64,
            applied_ms: i64,
        ) -> Result<()> {
            self.stores
                .legacy_mark_pod_status_checkpoint_applied(uid, rv, applied_ms)
                .await
        }
        pub async fn insert_dead_letter(
            &self,
            row: IntegrationNodeDeliveryDeadLetterInsert<'_>,
        ) -> Result<()> {
            self.stores
                .insert_dead_letter_test_only(crate::datastore::node_local::DeadLetterTestInsert {
                    idempotency_key: row.idempotency_key,
                    operation: row.operation,
                    subject_key: row.subject_key,
                    subject_api_version: row.subject_api_version,
                    subject_kind: row.subject_kind,
                    subject_namespace: row.subject_namespace,
                    subject_name: row.subject_name,
                    subject_uid: row.subject_uid,
                    pod_uid: row.pod_uid,
                    payload_proto: row.payload_proto,
                    attempts: row.attempts,
                    last_error: row.last_error,
                    moved_at_ms: row.moved_at_ms,
                })
                .await
        }
        pub async fn outbox_stream_position(&self, key: &str) -> Result<Option<(i64, i64)>> {
            self.stores.outbox_stream_position_for_test(key).await
        }
        pub async fn set_outbox_operation(&self, key: &str, operation: &str) -> Result<()> {
            self.stores
                .set_outbox_operation_for_test(key, operation)
                .await
        }
        pub async fn outbox_operation(&self, key: &str) -> Result<Option<String>> {
            self.stores.outbox_operation_for_test(key).await
        }

        pub async fn client_id(&self) -> Result<Option<String>> {
            self.stores
                .identity()
                .get_node_meta("outbox_client_id")
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()))
        }
    }

    #[cfg(test)]
    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct OutboxPayload {
        pub command: klights_cluster_core::StorageCommand,
    }

    #[cfg(test)]
    impl OutboxPayload {
        pub(crate) fn from_command(command: klights_cluster_core::StorageCommand) -> Self {
            Self { command }
        }

        pub(crate) fn encode_protobuf(&self) -> Result<Vec<u8>> {
            Ok(
                klights_leader_rpc::storage_wire_codec::encode_outbox_payload_protobuf(
                    &klights_cluster_core::OutboxPayload::new(self.command.clone()),
                )?,
            )
        }

        pub(crate) fn decode_protobuf(bytes: &[u8]) -> Result<Self> {
            Ok(Self {
                command: klights_leader_rpc::storage_wire_codec::decode_outbox_payload_protobuf(
                    bytes,
                )?
                .into_command(),
            })
        }
    }

    pub(crate) trait NodeLocalStoresRef {
        fn node_local_stores(&self) -> &NodeLocalStores;
    }

    impl NodeLocalStoresRef for NodeLocalStores {
        fn node_local_stores(&self) -> &NodeLocalStores {
            self
        }
    }

    impl NodeLocalStoresRef for Arc<NodeLocalStores> {
        fn node_local_stores(&self) -> &NodeLocalStores {
            self.as_ref()
        }
    }

    pub(crate) fn outbox_stores(node_db: &NodeLocalStores) -> OutboxStores {
        OutboxStores::new(
            node_db.outbox_producer(),
            node_db.outbox_dispatcher(),
            node_db.pod_status_checkpoints(),
            node_db.runtime_observation_checkpoints(),
            node_db.outbox_status_stamps(),
        )
    }

    pub(crate) fn outbox_from_node_db(node_db: impl NodeLocalStoresRef) -> Outbox {
        outbox_with_notify(node_db, Arc::new(Notify::new()))
    }

    pub(crate) fn outbox_with_notify(
        node_db: impl NodeLocalStoresRef,
        notify: Arc<Notify>,
    ) -> Outbox {
        let node_db = node_db.node_local_stores();
        Outbox::compose(
            outbox_stores(node_db),
            crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec(),
            notify,
            Arc::new(klights_supervisor::SystemWallClock),
        )
    }

    pub(crate) fn dispatcher_for_tests(
        node_db: impl NodeLocalStoresRef,
        client: Arc<dyn LeaderOutboxDelivery>,
    ) -> OutboxDispatcher {
        dispatcher_with_notify(node_db, client, Arc::new(Notify::new()))
    }

    pub(crate) fn dispatcher_with_notify(
        node_db: impl NodeLocalStoresRef,
        client: Arc<dyn LeaderOutboxDelivery>,
        notify: Arc<Notify>,
    ) -> OutboxDispatcher {
        let node_db = node_db.node_local_stores();
        OutboxDispatcher::new(
            outbox_stores(node_db),
            crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec(),
            client,
            notify,
            Arc::new(klights_supervisor::SystemWallClock),
        )
    }

    pub(crate) fn dispatcher_with_rtt_estimator(
        node_db: impl NodeLocalStoresRef,
        client: Arc<dyn LeaderOutboxDelivery>,
        rtt: Arc<klights_types::RttEstimator>,
    ) -> OutboxDispatcher {
        let node_db = node_db.node_local_stores();
        OutboxDispatcher::compose_with_rtt_estimator_for_test(
            outbox_stores(node_db),
            crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec(),
            client,
            Arc::new(Notify::new()),
            rtt,
            Arc::new(klights_supervisor::SystemWallClock),
        )
    }
}

pub use node_delivery_support::{
    IntegrationNodeDeliveryCluster, IntegrationNodeDeliveryDeadLetterInsert,
    IntegrationNodeDeliveryOutboxInsert, IntegrationNodeDeliveryStore, IntegrationNodeDispatcher,
    IntegrationNodeOutbox, author_bound_pod_finalization,
};
