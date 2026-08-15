//! Base-repository-only assembly for full-stack API integration tests.
//!
//! This module exists only as a root-private base-owned test module; normal builds
//! neither compile nor export it.

#[cfg(test)]
pub(crate) mod support {
    use std::sync::Arc;

    use k8s_native_service::test_support::admission::DeterministicApiIdentity;
    use klights_auth::test_support::{
        AllowAllAuthorizer, IntegrationCsrSignerObservation, recording_csr_signer,
    };

    pub(crate) struct IntegrationHeldSupervisorTask {
        handle: klights_supervisor::SupervisedJoinHandle<()>,
    }

    impl IntegrationHeldSupervisorTask {
        pub(crate) fn abort(&self) {
            self.handle.abort();
        }
    }

    /// Narrow integration handle around one real registered replication follower.
    pub(crate) struct IntegrationFollowerSession {
        replication: Arc<klights_replication::ReplicationService>,
        control_rx: tokio::sync::mpsc::Receiver<klights_node_api::FollowerControlMessage>,
        node_name: String,
        session_id: u64,
    }

    impl IntegrationFollowerSession {
        pub(crate) async fn recv(&mut self) -> Option<klights_node_api::FollowerControlMessage> {
            self.control_rx.recv().await
        }

        pub(crate) async fn complete_node_log_event(
            &self,
            event: klights_node_api::RoutedNodeLogEvent,
        ) -> anyhow::Result<()> {
            self.replication
                .complete_node_log_event(
                    klights_node_api::FollowerCompletionContext::new(
                        &self.node_name,
                        self.session_id,
                        klights_node_api::NodeOperationKind::Log,
                    ),
                    event,
                )
                .await
        }

        pub(crate) async fn complete_node_exec_sync(
            &self,
            response: klights_node_api::RoutedNodeExecSyncResponse,
        ) -> anyhow::Result<()> {
            self.replication
                .complete_node_exec_sync(
                    klights_node_api::FollowerCompletionContext::new(
                        &self.node_name,
                        self.session_id,
                        klights_node_api::NodeOperationKind::ExecSync,
                    ),
                    response,
                )
                .await
        }
    }

    #[derive(Default)]
    struct IntegrationHarnessOptions {
        csr_signer: Option<Arc<dyn klights_auth::csr_signer::CsrSigner>>,
        task_categories: klights_supervisor::TaskCategoryConfig,
        auth_clock: Option<Arc<dyn klights_auth::clock::Clock>>,
        list_cursor_clock: Option<Arc<dyn klights_supervisor::WallClock>>,
        authority: Option<Arc<dyn klights_leader_api::LeaderAuthority>>,
        bootstrap_token_authenticator:
            Option<Arc<dyn klights_leader_api::LeaderBootstrapTokenAuthentication>>,
    }

    type MutationSideEffectsFactory = Box<
        dyn FnOnce(
                klights_cluster_datastore::test_support::ResourceTestStore,
            ) -> Arc<klights_controllers::side_effects::SideEffectRegistry>
            + Send,
    >;

    struct IntegrationHarnessAssembly {
        authorizer: Arc<dyn klights_auth::authorizer::Authorizer>,
        pod_lifecycle_diagnostics: Option<Arc<dyn klights_pod_api::PodLifecycleDiagnosticsQuery>>,
        signing_keys: Arc<dyn klights_leader_api::LeaderServiceAccountSigningKeyState>,
        oidc: Option<Arc<dyn klights_auth::oidc::OidcValidator>>,
        webhook: Option<Arc<dyn klights_auth::webhook_auth::WebhookAuthenticator>>,
        watch_history_failure:
            Option<klights_cluster_datastore::test_support::WatchHistoryFailureControl>,
        mutation_side_effects_factory: Option<MutationSideEffectsFactory>,
        service_routing_network: Option<Arc<klights_networking::Network>>,
        audit_sink: Option<Arc<dyn k8s_native_service::audit::AuditSink>>,
        priority_fairness: Option<Arc<k8s_native_service::priority_fairness::ApiPriorityFairness>>,
        mount_operational_endpoints: bool,
    }

    impl IntegrationHarnessAssembly {
        fn standard() -> Self {
            Self {
                authorizer: Arc::new(AllowAllAuthorizer),
                pod_lifecycle_diagnostics: None,
                signing_keys: crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
                oidc: None,
                webhook: None,
                watch_history_failure: None,
                mutation_side_effects_factory: None,
                service_routing_network: None,
                audit_sink: None,
                priority_fairness: None,
                mount_operational_endpoints: false,
            }
        }
    }

    /// Opaque full-stack API fixture owned by the base integration-test package.
    #[derive(Clone)]
    pub(crate) struct NativeApiTestHarness {
        router: axum::Router,
        datastore: Arc<klights_cluster_datastore::sqlite::embedded::Datastore>,
        resource_store: klights_cluster_datastore::test_support::ResourceTestStore,
        endpoint_resource_fixture: klights_cluster_datastore::test_support::EndpointResourceFixture,
        commit_watch_fixture: Arc<klights_watch::test_support::CommitWatchFixture>,
        nodeport_exhaustion_fixture: klights_controllers::test_support::NodePortExhaustionFixture,
        bound_pod_finalization_fixture: klights_pod_api::test_support::BoundPodFinalizationFixture,
        _node_local: Arc<crate::bootstrap::composition::node_store::NodeLocalStores>,
        controller_runtime_fixture: klights_controllers::test_support::ControllerRuntimeFixture,
        endpoint_reconcile_fixture: klights_controllers::test_support::EndpointReconcileFixture,
        crd_registry: klights_controllers::crd::CrdRegistry,
        node_metrics_fixture: Arc<k8s_native_service::test_support::resource::NodeMetricsFixture>,
        operational_replication: Option<Arc<klights_replication::ReplicationService>>,
        task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
        node_lease_tracker: Arc<klights_controllers::node_lease::NodeLeaseTracker>,
        authority: Option<Arc<dyn klights_leader_api::LeaderAuthority>>,
        node_name: String,
    }

    impl NativeApiTestHarness {
        pub(crate) async fn new() -> anyhow::Result<Self> {
            Self::with_authorizer(Arc::new(AllowAllAuthorizer)).await
        }

        pub(crate) async fn with_authorizer(
            authorizer: Arc<dyn klights_auth::authorizer::Authorizer>,
        ) -> anyhow::Result<Self> {
            Self::assemble(IntegrationHarnessAssembly {
                authorizer,
                ..IntegrationHarnessAssembly::standard()
            })
            .await
        }

        pub(crate) async fn with_authorizer_and_operational_endpoints(
            authorizer: Arc<dyn klights_auth::authorizer::Authorizer>,
        ) -> anyhow::Result<Self> {
            Self::assemble(IntegrationHarnessAssembly {
                authorizer,
                mount_operational_endpoints: true,
                ..IntegrationHarnessAssembly::standard()
            })
            .await
        }

        pub(crate) async fn with_authorizer_and_audit_sink(
            authorizer: Arc<dyn klights_auth::authorizer::Authorizer>,
            audit_sink: Arc<dyn k8s_native_service::audit::AuditSink>,
        ) -> anyhow::Result<Self> {
            Self::assemble(IntegrationHarnessAssembly {
                authorizer,
                audit_sink: Some(audit_sink),
                ..IntegrationHarnessAssembly::standard()
            })
            .await
        }

        pub(crate) async fn with_pod_lifecycle_diagnostics(
            diagnostics: Arc<dyn klights_pod_api::PodLifecycleDiagnosticsQuery>,
        ) -> anyhow::Result<Self> {
            Self::assemble(IntegrationHarnessAssembly {
                pod_lifecycle_diagnostics: Some(diagnostics),
                ..IntegrationHarnessAssembly::standard()
            })
            .await
        }

        pub(crate) async fn with_authentication_dependencies(
            signing_keys: Arc<dyn klights_leader_api::LeaderServiceAccountSigningKeyState>,
            oidc: Option<Arc<dyn klights_auth::oidc::OidcValidator>>,
            webhook: Option<Arc<dyn klights_auth::webhook_auth::WebhookAuthenticator>>,
        ) -> anyhow::Result<Self> {
            Self::assemble(IntegrationHarnessAssembly {
                signing_keys,
                oidc,
                webhook,
                ..IntegrationHarnessAssembly::standard()
            })
            .await
        }

        pub(crate) async fn with_authenticators(
            oidc: Option<Arc<dyn klights_auth::oidc::OidcValidator>>,
            webhook: Option<Arc<dyn klights_auth::webhook_auth::WebhookAuthenticator>>,
        ) -> anyhow::Result<Self> {
            Self::with_authentication_dependencies(
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            oidc,
            webhook,
        )
        .await
        }

        pub(crate) async fn with_bootstrap_token_authenticator(
            bootstrap_token_authenticator: Arc<
                dyn klights_leader_api::LeaderBootstrapTokenAuthentication,
            >,
        ) -> anyhow::Result<Self> {
            Self::assemble_with_options(
                IntegrationHarnessAssembly::standard(),
                IntegrationHarnessOptions {
                    bootstrap_token_authenticator: Some(bootstrap_token_authenticator),
                    ..Default::default()
                },
            )
            .await
        }

        pub(crate) async fn with_signing_key_pem(signing_key_pem: String) -> anyhow::Result<Self> {
            Self::with_authentication_dependencies(
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::from_pem(
                signing_key_pem,
            ),
            None,
            None,
        )
        .await
        }

        pub(crate) async fn with_auth_clock(
            clock: Arc<dyn klights_auth::clock::Clock>,
        ) -> anyhow::Result<Self> {
            Self::assemble_with_options(
                IntegrationHarnessAssembly::standard(),
                IntegrationHarnessOptions {
                    auth_clock: Some(clock),
                    ..Default::default()
                },
            )
            .await
        }

        pub(crate) async fn with_list_cursor_clock(
            clock: Arc<dyn klights_supervisor::WallClock>,
        ) -> anyhow::Result<Self> {
            Self::assemble_with_options(
                IntegrationHarnessAssembly::standard(),
                IntegrationHarnessOptions {
                    list_cursor_clock: Some(clock),
                    ..Default::default()
                },
            )
            .await
        }

        pub(crate) async fn with_leader_authority() -> anyhow::Result<Self> {
            let (authority, _publisher) =
                klights_replication::authority::WatchLeaderAuthority::channel(true, None);
            Self::assemble_with_options(
                IntegrationHarnessAssembly::standard(),
                IntegrationHarnessOptions {
                    authority: Some(authority),
                    ..Default::default()
                },
            )
            .await
        }

        pub(crate) async fn with_toggle_failing_watch_history() -> anyhow::Result<(
            Self,
            klights_cluster_datastore::test_support::WatchHistoryFailureControl,
        )> {
            let control =
                klights_cluster_datastore::test_support::WatchHistoryFailureControl::new();
            let harness = Self::assemble(IntegrationHarnessAssembly {
                watch_history_failure: Some(control.clone()),
                ..IntegrationHarnessAssembly::standard()
            })
            .await?;
            Ok((harness, control))
        }

        pub(crate) async fn with_mutation_side_effect_factory<F>(factory: F) -> anyhow::Result<Self>
        where
            F: FnOnce(
                    klights_cluster_datastore::test_support::ResourceTestStore,
                )
                    -> Arc<klights_controllers::side_effects::SideEffectRegistry>
                + Send
                + 'static,
        {
            Self::assemble(IntegrationHarnessAssembly {
                mutation_side_effects_factory: Some(Box::new(factory)),
                ..IntegrationHarnessAssembly::standard()
            })
            .await
        }

        pub(crate) async fn with_service_routing_observation() -> anyhow::Result<(
            Self,
            klights_networking::test_support::ServiceRoutingObservation,
        )> {
            let (network, observation) =
                klights_networking::test_support::mock_network_with_service_routing_observation();
            let harness = Self::assemble(IntegrationHarnessAssembly {
                service_routing_network: Some(network),
                ..IntegrationHarnessAssembly::standard()
            })
            .await?;
            Ok((harness, observation))
        }

        pub(crate) async fn with_priority_fairness() -> anyhow::Result<(
            Self,
            Arc<k8s_native_service::priority_fairness::ApiPriorityFairness>,
        )> {
            let priority_fairness =
                Arc::new(k8s_native_service::priority_fairness::ApiPriorityFairness::new());
            let harness = Self::assemble(IntegrationHarnessAssembly {
                priority_fairness: Some(priority_fairness.clone()),
                ..IntegrationHarnessAssembly::standard()
            })
            .await?;
            Ok((harness, priority_fairness))
        }

        pub(crate) async fn with_csr_signer_observation()
        -> anyhow::Result<(Self, IntegrationCsrSignerObservation)> {
            let (csr_signer, observation) = recording_csr_signer();
            let harness = Self::assemble_with_options(
                IntegrationHarnessAssembly::standard(),
                IntegrationHarnessOptions {
                    csr_signer: Some(csr_signer),
                    ..Default::default()
                },
            )
            .await?;
            Ok((harness, observation))
        }

        pub(crate) async fn with_held_pod_delete_workqueue()
        -> anyhow::Result<(Self, IntegrationHeldSupervisorTask)> {
            let harness = Self::assemble_with_options(
                IntegrationHarnessAssembly::standard(),
                IntegrationHarnessOptions {
                    task_categories: klights_supervisor::TaskCategoryConfig {
                        pod_delete_workqueue: 1,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await?;
            let handle = harness
                .task_supervisor
                .spawn_async(
                    klights_supervisor::TaskCategory::PodDeleteWorkqueue,
                    "hold_foreground_delete_workqueue_for_integration",
                    std::future::pending(),
                )
                .await?;
            Ok((harness, IntegrationHeldSupervisorTask { handle }))
        }

        async fn assemble(assembly: IntegrationHarnessAssembly) -> anyhow::Result<Self> {
            Self::assemble_with_options(assembly, IntegrationHarnessOptions::default()).await
        }

        async fn assemble_with_options(
            assembly: IntegrationHarnessAssembly,
            options: IntegrationHarnessOptions,
        ) -> anyhow::Result<Self> {
            let IntegrationHarnessAssembly {
                authorizer,
                pod_lifecycle_diagnostics,
                signing_keys,
                oidc,
                webhook,
                watch_history_failure,
                mutation_side_effects_factory,
                service_routing_network,
                audit_sink,
                priority_fairness,
                mount_operational_endpoints,
            } = assembly;
            let IntegrationHarnessOptions {
                csr_signer,
                task_categories,
                auth_clock,
                list_cursor_clock,
                authority,
                bootstrap_token_authenticator,
            } = options;
            let auth_clock =
                auth_clock.unwrap_or_else(|| Arc::new(klights_auth::clock::SystemClock));
            let list_cursor_clock =
                list_cursor_clock.unwrap_or_else(|| Arc::new(klights_supervisor::SystemWallClock));
            let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(task_categories));
            let commit_watch_fixture =
                Arc::new(klights_watch::test_support::CommitWatchFixture::new(64));
            let executor = klights_cluster_datastore::sqlite::open_in_memory(
                supervisor.clone(),
                "sqlite:native-api-integration",
            )
            .await?;
            let db =
            klights_cluster_datastore::sqlite::embedded::Datastore::new_in_memory_with_watch_and_executor_with_sink(
                executor,
                commit_watch_fixture.clone(),
                crate::bootstrap::composition_adapters::outbox_response_codec_adapter::new_codec(),
                Arc::new(klights_supervisor::SystemWallClock),
            )
            .await?;
            let canonical = db.clone();
            let canonical_ports =
                crate::bootstrap::composition::cluster_store::selector::sqlite_opened_passive_store(
                    &db,
                );
            let passive_reads = if let Some(control) = watch_history_failure {
                let focused_reads = db.focused_read_store();
                crate::bootstrap::composition::cluster_store::selector::PassiveReadPorts::new(
                    focused_reads.clone(),
                    klights_cluster_datastore::test_support::toggle_failing_watch_history_for_test_support(
                        focused_reads.clone(),
                        control,
                    ),
                    focused_reads.clone(),
                    focused_reads,
                )
            } else {
                crate::bootstrap::composition::cluster_store::selector::sqlite_passive_read_ports(
                    &db,
                )
            };
            let resource_store =
            klights_cluster_datastore::test_support::ResourceTestStore::from_embedded_for_test_support(
                db.clone(),
            );
            let datastore = Arc::new(db.clone());
            let config = crate::KlightsConfig::test_default();
            let identity: Arc<dyn k8s_native_service::ApiIdentityGenerator> =
                Arc::new(DeterministicApiIdentity::default());
            let controller_identity: Arc<dyn klights_controllers::ControllerIdentityGenerator> =
                Arc::new(
                    klights_controllers::test_support::DeterministicControllerIdentity::default(),
                );
            let service_ipam = Arc::new(klights_controllers::service::ServiceIpam::new(
                &config.service_cidr,
            ));
            let nodeport_alloc = Arc::new(klights_controllers::service::NodePortAllocator::new());
            nodeport_alloc.set_ready();
            let nodeport_exhaustion_fixture =
                klights_controllers::test_support::NodePortExhaustionFixture::new(
                    nodeport_alloc.clone(),
                );
            let metrics = klights_controllers::side_effects::SideEffectMetrics::new();
            let resource_query_authority = authority.clone().unwrap_or_else(|| {
                crate::bootstrap::composition_adapters::authority_adapter::always_leader_authority()
            });
            let resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery> =
                klights_watch::DatastoreResourceQueryAdapter::new_with_resource_reads_and_clock(
                    passive_reads.resource_reads(),
                    resource_query_authority,
                    list_cursor_clock,
                );
            let proposal: Arc<dyn klights_replication::proposal::RaftProposal> = Arc::new(
                crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(
                    Arc::new(canonical.clone()),
                    Arc::new(canonical.clone()),
                    canonical.focused_read_store(),
                ),
            );
            let resource_command: Arc<dyn klights_leader_api::LeaderResourceCommand> = Arc::new(
            klights_replication::leader_api::EmbeddedLeaderResourceCommand::new(
                proposal.clone(),
                resource_query.clone(),
                crate::bootstrap::composition_adapters::authority_adapter::always_leader_authority(
                ),
            ),
        );
            let node_local = Arc::new(
                crate::bootstrap::composition::node_store::open_node_local(
                    crate::bootstrap::composition::cluster_store::backend_kind::BackendKind::Sqlite,
                    None,
                    supervisor.clone(),
                    "sqlite:native-api-integration-node-local",
                )
                .await?,
            );
            let outbox_notify = Arc::new(tokio::sync::Notify::new());
            let outbox_stores = klights_kubelet::node_outbox::OutboxStores::new(
                node_local.outbox_producer(),
                node_local.outbox_dispatcher(),
                node_local.pod_status_checkpoints(),
                node_local.runtime_observation_checkpoints(),
                node_local.outbox_status_stamps(),
            );
            let outbox_codec =
                crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec();
            let outbox = Arc::new(klights_kubelet::node_outbox::Outbox::compose(
                outbox_stores.clone(),
                outbox_codec.clone(),
                outbox_notify.clone(),
                Arc::new(klights_supervisor::SystemWallClock),
            ));
            let side_effects = Arc::new(crate::bootstrap::side_effects::default_registry(
                metrics.clone(),
                None,
                Some(supervisor.clone()),
                canonical_ports.applied_outbox.clone(),
                Arc::new(canonical.clone()),
                canonical_ports.read_ports.resource_reads(),
                canonical_ports.ownership_reads.clone(),
                canonical_ports.namespace_content_reads.clone(),
                canonical_ports.topology_reads.clone(),
                controller_identity.clone(),
            ));
            let gc_coordination = Arc::new(klights_controllers::ControllerCoordination::new());
            let pod_repository_config = crate::bootstrap::composition::pod_repository::PodRepositoryBuildConfig {
            resource_query: resource_query.clone(),
            ownership_reads: canonical_ports.ownership_reads.clone(),
            resource_reads: canonical_ports.read_ports.resource_reads(),
            namespace_content_reads: canonical_ports.namespace_content_reads.clone(),
            topology_reads: canonical_ports.topology_reads.clone(),
            pod_workqueue_store: Some(node_local.pod_workqueue()),
            supervisor: supervisor.clone(),
            side_effects: side_effects.clone(),
            metrics: metrics.clone(),
            pod_network_cache: node_local.pod_network_cache(),
            assignment_waiter: Arc::new(klights_networking::PodNetworkAssignmentBus::new()),
            scheduling_mode: crate::bootstrap::composition::pod_repository::PodSchedulingMode::InlineSingleNode,
            outbox: Some(outbox),
            cluster_api: Some(resource_query.clone()),
            resource_commands: Some(resource_command.clone()),
            remote_delivery_required: false,
            controller_identity: controller_identity.clone(),
            api_identity: identity.clone(),
            gc_coordination: gc_coordination.clone(),
            scheduler_bind_gate: None,
            post_write_maintenance_notify: None,
        };
            let (
            pod_query,
            pod_snapshot,
            pod_update,
            _pod_status_writer,
            _pod_workqueue,
            _pod_network_assignment,
            _pod_host_ip,
            _background,
            _deletion_finalizer,
            _dirty_counter,
            mutation_reconcile,
            gc_delete,
            eviction_admission,
            namespace_bootstrap,
            namespace_termination_queue,
            pod_api,
            pod_subresource,
            _pod_scheduling,
            _watch_source,
            bound_pod_finalization,
            _deferred_runtime,
            _test_api,
            _test_subresource,
        ) = crate::bootstrap::composition::pod_repository::build_native_api_test_pod_repository_parts(
            pod_repository_config,
            None,
            identity.clone(),
            gc_coordination.clone(),
        );
            let pod_api = pod_api.expect("native API root Pod API service");
            let pod_subresource = pod_subresource.expect("native API root Pod subresource service");
            let pod_query = pod_query;
            let api_pod_repository =
            crate::bootstrap::composition_adapters::api_state_adapter::RootApiPodRepository::new(
                pod_query.clone(),
                pod_snapshot.clone(),
                mutation_reconcile.clone(),
                namespace_termination_queue.clone(),
                eviction_admission.clone(),
                namespace_bootstrap.clone(),
                pod_api.clone(),
                pod_subresource.clone(),
            );
            let controller_pod_port = Arc::new(
            crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerPodPort::new(
                pod_query.clone(),
                pod_update.clone(),
                pod_api.clone(),
                pod_subresource.clone(),
            ),
        );
            let controller_pod_mutations =
                Arc::new(klights_controllers::ControllerPodMutationAdapter::new(
                    controller_pod_port.clone(),
                    controller_pod_port.clone(),
                ));
            side_effects.set_pod_ports(pod_query.clone(), gc_delete.clone());
            let finalizer_lifecycle = crate::bootstrap::finalizer_lifecycle_adapter::
            DatastoreFinalizerLifecycleAdapter::new_with_coordination(
                canonical_ports.read_ports.resource_reads(),
                canonical_ports.ownership_reads.clone(),
                resource_command.clone(),
                gc_delete.clone(),
                side_effects.clone(),
                metrics.clone(),
                gc_coordination.clone(),
            );
            let mutation_side_effects = mutation_side_effects_factory
                .map(|factory| factory(resource_store.clone()))
                .unwrap_or_else(|| side_effects.clone());
            let mutation_effects = klights_controllers::side_effects::ResourceMutationEffects::new(
                mutation_side_effects,
                metrics.clone(),
            );
            let positioned_watch = crate::bootstrap::composition_adapters::positioned_watch_adapter::datastore_positioned_watch_service(
                &passive_reads,
                commit_watch_fixture.signal_source(),
            );
            let watch_signals = commit_watch_fixture.signal_source();
            let generated = crate::bootstrap::composition_adapters::generated_handler_adapter::GeneratedHandlerAdapter::new(
            crate::bootstrap::composition_adapters::generated_handler_adapter::GeneratedHandlerStorage::new(
                canonical_ports.read_ports.resource_reads(),
                canonical_ports.topology_reads.clone(),
                canonical_ports.read_ports.allocator_reads(),
                resource_query.clone(),
                resource_command.clone(),
            ),
            watch_signals.clone(),
            positioned_watch.clone(),
            klights_supervisor::FileProcessExecutor::new(supervisor.clone()),
            supervisor.clone(),
            config.data_root.join("etc/ca.crt"),
            controller_identity.clone(),
        );
            let network = service_routing_network
                .unwrap_or_else(klights_networking::test_support::mock_network);
            let controller_leader_ports = Arc::new(
            crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::new_with_commands(
                canonical_ports.read_ports.resource_reads(), canonical_ports.ownership_reads.clone(), resource_command.clone()),
        );
            let non_pod_finalization = Arc::new(
            crate::bootstrap::controller_adapters::gc_delete_adapter::GcNonPodFinalizationAdapter::new_with_commands(
                canonical_ports.read_ports.resource_reads(), canonical_ports.ownership_reads.clone(), resource_command.clone()),
        );
            let endpoint_reconcile_fixture =
                klights_controllers::test_support::EndpointReconcileFixture::new(
                    controller_leader_ports.clone(),
                    api_pod_repository.clone(),
                    controller_leader_ports.clone(),
                    non_pod_finalization.clone(),
                    gc_coordination.clone(),
                    controller_identity.clone(),
                );
            let controller_dependencies = klights_controllers::ControllerRuntimeDependencies {
            wall_time: chrono::Utc::now,
            resource_query: controller_leader_ports.clone(),
            deployment_store: controller_leader_ports.clone(),
            replicaset_store: controller_leader_ports.clone(),
            statefulset_store: controller_leader_ports.clone(),
            daemonset_store: controller_leader_ports.clone(),
            job_store: controller_leader_ports.clone(),
            service_store: controller_leader_ports.clone(),
            pvc_store: controller_leader_ports.clone(),
            pdb_store: controller_leader_ports.clone(),
            replicationcontroller_store: controller_leader_ports.clone(),
            apiservice_store: controller_leader_ports.clone(),
            csr_status_store: controller_leader_ports.clone(),
            pod_query: api_pod_repository.clone(),
            deployment_pod_mutation: controller_pod_mutations.clone(),
            replicaset_pod_mutation: controller_pod_mutations.clone(),
            statefulset_pod_mutation: controller_pod_mutations.clone(),
            daemonset_pod_mutation: controller_pod_mutations.clone(),
            job_pod_mutation: controller_pod_mutations.clone(),
            replicationcontroller_pod_mutation: controller_pod_mutations.clone(),
            pod_delete_sink: gc_delete.clone(),
            reconcile: Arc::new(
                crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerReconcilePort::new(
                    non_pod_finalization.clone(),
                ),
            ),
            network: Arc::new(
                crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerNetworkPort::new(
                    network.services().clone(),
                ),
            ),
            effects: Arc::new(
                crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerEffectPort::new(
                    klights_supervisor::FileProcessExecutor::new(supervisor.clone()),
                    config.data_root.join("local-path-provisioner"),
                ),
            ),
            coordination: gc_coordination.clone(),
            node_name: Arc::from(config.node_name.as_str()),
        };
            let node_metrics_fixture =
                Arc::new(k8s_native_service::test_support::resource::NodeMetricsFixture::new());
            let csr_issuer = csr_signer.map(|signer| {
                Arc::new(crate::bootstrap::auth_adapters::AuthCsrIssuer::new(
                    signer,
                    auth_clock.clone(),
                    supervisor.clone(),
                )) as Arc<dyn klights_controllers::csr_signer::CsrIssuer>
            });
            let hpa_controller =
                crate::bootstrap::controller_adapters::hpa_controller_adapter::controller(
                    controller_leader_ports.clone(),
                    api_pod_repository.clone(),
                    gc_delete.clone(),
                    controller_pod_mutations,
                    non_pod_finalization,
                    gc_coordination.clone(),
                    Arc::from(config.node_name.as_str()),
                    node_metrics_fixture.clone(),
                    controller_identity.clone(),
                );
            let controller_dispatcher =
                Arc::new(klights_controllers::ControllerDispatcher::new_complete(
                    service_ipam.clone(),
                    nodeport_alloc.clone(),
                    supervisor.clone(),
                    csr_issuer,
                    hpa_controller,
                    controller_dependencies,
                    controller_identity.clone(),
                ));
            side_effects.set_controller_dispatcher(controller_dispatcher.clone());
            let service_routing: Arc<dyn klights_reconcile_api::ServiceRoutingSync> = Arc::new(
                crate::bootstrap::network_adapters::ApiServiceRoutingSyncAdapter::new(
                    network.services().clone(),
                ),
            );
            let pod_logs_root = config.data_root.join("logs/pods");
            let wall_clock: Arc<dyn klights_supervisor::WallClock> =
                Arc::new(klights_supervisor::SystemWallClock);
            let pod_logs = crate::bootstrap::composition_adapters::node_log_runtime_adapter::pod_log_capabilities(
            Arc::new(
                klights_kubelet::node_api::logs::LocalNodeLogRuntime::new_with_pod_event_store(
                    pod_logs_root.clone(),
                    supervisor.clone(),
                    wall_clock.clone(),
                    klights_kubelet::node_api::logs::PodLogFollowWatchSource::new(Arc::new(
                        klights_kubelet::node_api::logs::LeaderPodLogFollowWatchPort::new(Arc::new(
                            positioned_watch.clone(),
                        )),
                    )),
                ),
            ),
            Arc::new(
                klights_kubelet::node_api::logs::LocalNodeLogRuntime::new_without_pod_event_store(
                    pod_logs_root,
                    supervisor.clone(),
                    wall_clock,
                ),
            ),
            supervisor.clone(),
            config.node_name.clone(),
        );
            let rbac_policy_store: Arc<dyn klights_auth::rbac_policy_store::RbacPolicyStore> =
                Arc::new(
                    klights_auth::rbac_policy_store::ReaderBackedRbacPolicyStore::new(Arc::new(
                        crate::bootstrap::auth_adapters::DatastoreRbacResourceReader::new(
                            canonical_ports.read_ports.resource_reads(),
                        ),
                    )),
                );
            let runtime_paths =
                k8s_native_service::ApiRuntimePaths::from_data_root(config.data_root.clone())?;
            let mut runtime_inputs = k8s_native_service::ApiRuntimeInputs::new(
                runtime_paths,
                config.api_slow_log_threshold,
            )?;
            if let Some(audit_sink) = audit_sink {
                runtime_inputs = runtime_inputs.with_audit_sink(audit_sink);
            }
            if let Some(priority_fairness) = priority_fairness {
                runtime_inputs = runtime_inputs.with_priority_fairness(priority_fairness);
            }
            let node_name = config.node_name.clone();
            let bootstrap_token_authenticator =
                bootstrap_token_authenticator.unwrap_or_else(|| {
                    Arc::new(
                        crate::bootstrap::auth_adapters::DatastoreBootstrapTokenAuthenticator::new(
                            canonical_ports.read_ports.resource_reads(),
                        ),
                    )
                });
            let crd_registry = klights_controllers::crd::CrdRegistry::new();
            let operational_replication = mount_operational_endpoints.then(|| {
                Arc::new(klights_replication::ReplicationService::new_with_ports(
                    db.focused_recovery_store(),
                    Arc::new(
                        crate::bootstrap::bootstrap_token::DatastoreBootstrapTokenValidation::new(
                            canonical_ports.read_ports.resource_reads(),
                        ),
                    ),
                    supervisor.clone(),
                ))
            });
            let remote_node_services = operational_replication.as_ref().map(|replication| {
                let adapter =
                    crate::bootstrap::grpc_runtime_adapter::GrpcReplicationRuntimeAdapter::new(
                        replication.clone(),
                    );
                (
                    adapter.clone() as Arc<dyn klights_node_api::NodeExec>,
                    adapter as Arc<dyn klights_node_api::NodeLog>,
                )
            });
            let node_lease_tracker = Arc::new(
                klights_controllers::node_lease::NodeLeaseTracker::new_at(chrono::Utc::now()),
            );
            let (router, outer_layers) = k8s_native_service::build_current_router(
            identity.clone(),
            authorizer,
            rbac_policy_store,
            bootstrap_token_authenticator.clone(),
            oidc,
            webhook,
            None,
            Arc::new(
                crate::bootstrap::composition_adapters::watch_stream_adapter::DatastoreWatchStreamAdapter::new(
                    resource_query.clone(),
                    canonical_ports.read_ports.allocator_reads(),
                    watch_signals,
                    positioned_watch.clone(),
                ),
            ),
            crate::bootstrap::composition_adapters::api_state_adapter::RootNamespaceTerminationStore::new_with_commands(
                canonical_ports.read_ports.resource_reads(),
                canonical_ports.namespace_content_reads.clone(),
                resource_command.clone(),
            ),
            resource_query.clone(),
            resource_command.clone(),
            finalizer_lifecycle,
            mutation_effects,
            crate::bootstrap::controller_adapters::resource_quota_admission_adapter::ResourceQuotaAdmissionAdapter::new(
                canonical_ports.read_ports.resource_reads(),
            ),
            crate::bootstrap::composition_adapters::resource_admission_adapter::ResourceAdmissionAdapter::new_with_resource_reads(
                identity,
                canonical_ports.read_ports.resource_reads(),
            ),
            crate::bootstrap::composition_adapters::custom_resource_read_adapter::CustomResourceReadAdapter::new(
                canonical_ports.read_ports.resource_scopes(),
                resource_query.clone(),
                canonical_ports.read_ports.allocator_reads(),
                commit_watch_fixture.signal_source(),
                positioned_watch,
                supervisor.clone(),
            ),
            generated.clone(),
            generated.clone(),
            generated.clone(),
            generated,
            Arc::new(
                crate::bootstrap::controller_adapters::gc_delete_adapter::GcOwnerLifecycleAdapter::new_with_coordination(
                    canonical_ports.read_ports.resource_reads(),
                    canonical_ports.ownership_reads.clone(),
                    resource_command.clone(),
                    gc_delete.clone(),
                    gc_coordination,
                ),
            ),
            api_pod_repository.clone(),
            crd_registry.clone(),
            crate::bootstrap::service_adapters::ApiServiceWriteAllocator::new(
                Arc::new(
                    crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::new_with_commands(
                        canonical_ports.read_ports.resource_reads(),
                        canonical_ports.ownership_reads.clone(),
                        resource_command.clone(),
                    ),
                ),
                service_ipam,
                nodeport_alloc.clone(),
                controller_identity,
            ),
            controller_dispatcher.clone(),
            crate::bootstrap::composition_adapters::api_state_adapter::RootApiFailureMetrics::new(metrics),
            crate::bootstrap::composition_adapters::api_state_adapter::RootApiNodeLeaseObservations::new(node_lease_tracker.clone()),
            service_routing.clone(),
            pod_logs,
            None,
            node_metrics_fixture.clone(),
            klights_kubelet::node_api::port_forward::local_node_port_forward(supervisor.clone()),
            pod_lifecycle_diagnostics,
            None,
            remote_node_services,
            config.node_name,
            config.anonymous_auth,
            runtime_inputs,
            auth_clock,
            supervisor.clone(),
            signing_keys,
            authority.clone(),
        );
            let router = if mount_operational_endpoints {
                let operational_endpoints = klights_apiserver::OperationalEndpointHandlers::new(
                    klights_apiserver::OperationalEndpointInputs::new(
                        klights_apiserver::OperationalNodeRole::Leader,
                        Arc::new(String::new),
                        crate::version::api_version_info(),
                        crate::bootstrap::operational_adapters::ApiClusterStatusMetadata::new(
                            canonical_ports.metadata_reads.clone(),
                        ),
                        operational_replication.as_ref().map(|replication| {
                            replication.clone()
                                as Arc<dyn klights_leader_api::LeaderFollowerDiagnostics>
                        }),
                        supervisor.clone(),
                    ),
                );
                klights_apiserver::mount_operational_endpoints(
                    router.into_router(),
                    operational_endpoints,
                )
            } else {
                router.into_router()
            };
            let controller_runtime_fixture =
                klights_controllers::test_support::ControllerRuntimeFixture::new(
                    controller_dispatcher.clone(),
                    supervisor.clone(),
                );
            let bound_pod_finalization_fixture =
                klights_pod_api::test_support::BoundPodFinalizationFixture::new(
                    bound_pod_finalization.clone(),
                );
            let endpoint_resource_fixture =
                klights_cluster_datastore::test_support::EndpointResourceFixture::new(
                    resource_store.clone(),
                );
            Ok(Self {
                router: outer_layers.finish(router),
                datastore,
                resource_store,
                endpoint_resource_fixture,
                commit_watch_fixture,
                nodeport_exhaustion_fixture,
                bound_pod_finalization_fixture,
                _node_local: node_local,
                controller_runtime_fixture,
                endpoint_reconcile_fixture,
                crd_registry,
                node_metrics_fixture,
                operational_replication,
                task_supervisor: supervisor,
                node_lease_tracker,
                authority,
                node_name,
            })
        }

        pub(crate) fn router(&self) -> axum::Router {
            self.router.clone()
        }

        pub(crate) fn router_with_authority(&self, is_leader: bool) -> axum::Router {
            let authority = if is_leader {
                self.authority
                    .clone()
                    .expect("leader authority harness must be selected")
            } else {
                let (authority, _publisher) =
                    klights_replication::authority::WatchLeaderAuthority::channel(false, None);
                authority
            };
            klights_apiserver::wrap_authority_router(
                self.router.clone(),
                Some(Arc::new(
                    klights_apiserver::HttpAuthorityRouter::from_authority(authority, None),
                )),
            )
        }

        pub(crate) async fn record_node_lease(
            &self,
            node_name: &str,
            lease: &serde_json::Value,
        ) -> anyhow::Result<()> {
            self.node_lease_tracker
                .record_from_lease_object(node_name, lease)
                .await
                .map(|_| ())
        }

        /// Root assembly transport retained for P12.2f deletion. The returned
        /// fixture owns only named actor-finalization behavior.
        pub(crate) fn bound_pod_finalization_fixture(
            &self,
        ) -> klights_pod_api::test_support::BoundPodFinalizationFixture {
            self.bound_pod_finalization_fixture.clone()
        }

        /// Root assembly transport retained for P12.2f deletion. The returned
        /// fixture owns only named controller runtime behavior.
        pub(crate) fn controller_runtime_fixture(
            &self,
        ) -> klights_controllers::test_support::ControllerRuntimeFixture {
            self.controller_runtime_fixture.clone()
        }

        /// Root assembly transport retained for P12.2f deletion. Endpoint
        /// reconciliation behavior belongs to the canonical controller fixture.
        pub(crate) fn endpoint_reconcile_fixture(
            &self,
        ) -> klights_controllers::test_support::EndpointReconcileFixture {
            self.endpoint_reconcile_fixture.clone()
        }

        /// Root assembly transport retained for P12.2f deletion. Resource
        /// operations remain owned by the canonical datastore fixture.
        pub(crate) fn endpoint_resource_fixture(
            &self,
        ) -> klights_cluster_datastore::test_support::EndpointResourceFixture {
            self.endpoint_resource_fixture.clone()
        }

        pub(crate) fn nodeport_exhaustion_fixture(
            &self,
        ) -> klights_controllers::test_support::NodePortExhaustionFixture {
            self.nodeport_exhaustion_fixture.clone()
        }

        /// Returns the neutral native-service registry already wired into this
        /// router. CRD fixture behavior remains owned by native-service support.
        pub(crate) fn crd_registry(&self) -> klights_leader_api::CrdRegistry {
            self.crd_registry.clone()
        }

        /// Returns the canonical native-service fixture composed into this router.
        /// Root assembly transport is retained for P12.2f; metric behavior belongs
        /// to native-service test support.
        pub(crate) fn node_metrics_fixture(
            &self,
        ) -> Arc<k8s_native_service::test_support::resource::NodeMetricsFixture> {
            self.node_metrics_fixture.clone()
        }

        pub(crate) async fn ensure_operational_cluster_metadata(&self) -> anyhow::Result<()> {
            crate::bootstrap::cluster_meta::ensure_cluster_metadata_sqlite(self.datastore.as_ref())
                .await?;
            Ok(())
        }

        pub(crate) async fn seed_default_rbac(&self) -> anyhow::Result<()> {
            let db = self.datastore.as_ref();
            let store = crate::bootstrap::composition_adapters::leader_bootstrap_store_adapter::LeaderBootstrapStore::new(
                db.focused_read_store(),
                db.focused_read_store(),
                crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::resource_commands_for_test(
                    Arc::new(db.clone()),
                    Arc::new(db.clone()),
                    db.focused_read_store(),
                ),
            );
            klights_controllers::rbac_reconcile::reconcile_default_rbac_objects(&store).await
        }

        pub(crate) async fn register_operational_follower(
            &self,
            dataplane: klights_leader_api::NetworkDataplane,
        ) -> anyhow::Result<()> {
            let replication = self
                .operational_replication
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("operational replication is not installed"))?;
            let (_control_rx, _session) = replication.register_follower(dataplane).await;
            Ok(())
        }

        pub(crate) async fn register_integration_follower(
            &self,
            dataplane: klights_leader_api::NetworkDataplane,
        ) -> anyhow::Result<IntegrationFollowerSession> {
            let replication = self
                .operational_replication
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("operational replication is not installed"))?
                .clone();
            let node_name = dataplane.node_name().to_string();
            let (control_rx, session_id) = replication.register_follower(dataplane).await;
            Ok(IntegrationFollowerSession {
                replication,
                control_rx,
                node_name,
                session_id,
            })
        }

        pub(crate) fn integration_remote_exec_sync(
            &self,
        ) -> anyhow::Result<
            k8s_native_service::test_support::streaming::RemoteExecSyncWebSocketFixture,
        > {
            let replication = self
                .operational_replication
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("operational replication is not installed"))?
                .clone();
            Ok(
                k8s_native_service::test_support::streaming::RemoteExecSyncWebSocketFixture::new(
                    crate::bootstrap::grpc_runtime_adapter::GrpcReplicationRuntimeAdapter::new(
                        replication,
                    ),
                    self.task_supervisor.clone(),
                ),
            )
        }

        /// Canonical narrow persistence fixture bound to this router's exact store.
        pub(crate) fn resource_store(
            &self,
        ) -> klights_cluster_datastore::test_support::ResourceTestStore {
            self.resource_store.clone()
        }

        pub(crate) fn commit_watch_fixture(
            &self,
        ) -> Arc<klights_watch::test_support::CommitWatchFixture> {
            self.commit_watch_fixture.clone()
        }

        pub(crate) fn node_name(&self) -> &str {
            &self.node_name
        }
    }
}
