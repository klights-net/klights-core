use klights_cluster_core::{Resource, ResourcePreconditions};
use klights_reconcile_api::{
    FinalizerLifecyclePort, FinalizerResourceTarget, FinalizerTombstoneDeleteRequest,
    FinalizerUpdateRequest,
};

use klights_cluster_store::{ClusterOwnershipRead, ClusterResourceRead};

pub(crate) struct GcOwnerLifecycleAdapter {
    gc: std::sync::Arc<super::controller_runtime_adapter::RootControllerLeaderPort>,
    pod_delete_sink: std::sync::Arc<dyn klights_reconcile_api::GcPodDeleteSink>,
    non_pod_finalization: GcNonPodFinalizationAdapter,
    coordination: std::sync::Arc<dyn klights_reconcile_api::GcForegroundDeleteCoordination>,
}

impl GcOwnerLifecycleAdapter {
    pub(crate) fn new_with_coordination(
        resource_reads: std::sync::Arc<dyn ClusterResourceRead>,
        ownership_reads: std::sync::Arc<dyn ClusterOwnershipRead>,
        resource_commands: std::sync::Arc<dyn klights_leader_api::LeaderResourceCommand>,
        pod_delete_sink: std::sync::Arc<dyn klights_reconcile_api::GcPodDeleteSink>,
        coordination: std::sync::Arc<dyn klights_reconcile_api::GcForegroundDeleteCoordination>,
    ) -> Self {
        let gc = std::sync::Arc::new(
            super::controller_runtime_adapter::RootControllerLeaderPort::new_with_commands(
                resource_reads.clone(),
                ownership_reads.clone(),
                resource_commands.clone(),
            ),
        );
        Self {
            non_pod_finalization: GcNonPodFinalizationAdapter::new_with_commands(
                resource_reads,
                ownership_reads,
                resource_commands,
            ),
            gc,
            pod_delete_sink,
            coordination,
        }
    }

    fn sink_error(error: anyhow::Error) -> klights_reconcile_api::ReconcileSinkError {
        klights_reconcile_api::ReconcileSinkError::unavailable(error.to_string())
    }
}

impl klights_reconcile_api::GcOwnerLifecyclePort for GcOwnerLifecycleAdapter {
    fn reconcile_owner_references(
        &self,
        resource: Resource,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        Box::pin(async move {
            klights_controllers::gc::reconcile_owner_references(
                self.gc.as_ref(),
                resource,
                self.pod_delete_sink.as_ref(),
                &self.non_pod_finalization,
                self.coordination.as_ref(),
            )
            .await
            .map(|_| ())
            .map_err(Self::sink_error)
        })
    }

    fn cascade_delete(
        &self,
        owner: klights_reconcile_api::GcOwnerIdentity,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        Box::pin(async move {
            klights_controllers::gc::cascade_delete_with_uid(
                self.gc.as_ref(),
                &owner.uid,
                &owner.api_version,
                &owner.name,
                &owner.kind,
                owner.namespace,
                self.pod_delete_sink.as_ref(),
                &self.non_pod_finalization,
                self.coordination.as_ref(),
            )
            .await
            .map_err(Self::sink_error)
        })
    }

    fn sweep_dependents(
        &self,
        owner: klights_reconcile_api::GcOwnerIdentity,
    ) -> klights_reconcile_api::GcOwnerBoolFuture<'_> {
        Box::pin(async move {
            klights_controllers::gc::owner_cascade_sweep_once(
                self.gc.as_ref(),
                &owner.uid,
                &owner.api_version,
                &owner.name,
                &owner.kind,
                owner.namespace,
                self.pod_delete_sink.as_ref(),
                &self.non_pod_finalization,
                self.coordination.as_ref(),
            )
            .await
            .map_err(Self::sink_error)
        })
    }

    fn finalize_foreground_owner(
        &self,
        owner: Resource,
    ) -> klights_reconcile_api::GcOwnerBoolFuture<'_> {
        Box::pin(async move {
            klights_controllers::gc::finalize_foreground_owner_if_ready(
                self.gc.as_ref(),
                &owner,
                self.pod_delete_sink.as_ref(),
                &self.non_pod_finalization,
                self.coordination.as_ref(),
            )
            .await
            .map_err(Self::sink_error)
        })
    }
}

pub(crate) struct GcNonPodFinalizationAdapter {
    lifecycle: crate::bootstrap::finalizer_lifecycle_adapter::CommandFinalizerLifecycleStore,
}

impl GcNonPodFinalizationAdapter {
    #[cfg(test)]
    pub(crate) fn new_for_test(
        applied_outbox: std::sync::Arc<dyn klights_cluster_store::AppliedOutboxLedger>,
        canonical: std::sync::Arc<klights_cluster_datastore::sqlite::embedded::Datastore>,
        resource_reads: std::sync::Arc<dyn klights_cluster_store::ClusterResourceRead>,
        ownership_reads: std::sync::Arc<dyn ClusterOwnershipRead>,
    ) -> Self {
        let commands =
            super::controller_runtime_adapter::RootControllerLeaderPort::resource_commands_for_test(
                applied_outbox,
                canonical,
                resource_reads.clone(),
            );
        Self::new_with_commands(resource_reads, ownership_reads, commands)
    }
    pub(crate) fn new_with_commands(
        resource_reads: std::sync::Arc<dyn ClusterResourceRead>,
        ownership_reads: std::sync::Arc<dyn ClusterOwnershipRead>,
        commands: std::sync::Arc<dyn klights_leader_api::LeaderResourceCommand>,
    ) -> Self {
        Self {
            lifecycle:
                crate::bootstrap::finalizer_lifecycle_adapter::CommandFinalizerLifecycleStore::new(
                    resource_reads,
                    ownership_reads,
                    commands,
                ),
        }
    }
}

impl klights_reconcile_api::GcNonPodFinalizationPort for GcNonPodFinalizationAdapter {
    fn finalize_non_pod(
        &self,
        request: klights_reconcile_api::GcNonPodFinalizationRequest,
    ) -> klights_reconcile_api::GcNonPodFinalizationFuture<'_> {
        Box::pin(async move {
            if request.resource.api_version == "v1" && request.resource.kind == "Pod" {
                return Err(klights_reconcile_api::ReconcileSinkError::unavailable(
                    "GC non-Pod finalization port rejects v1/Pod",
                ));
            }
            if request.remove_foreground_finalizer {
                let resource = request.resource;
                let mut data = (*resource.data).clone();
                let retained: Vec<serde_json::Value> = data
                    .pointer("/metadata/finalizers")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter(|value| value.as_str() != Some("foregroundDeletion"))
                    .cloned()
                    .collect();
                let metadata = data
                    .get_mut("metadata")
                    .and_then(serde_json::Value::as_object_mut)
                    .ok_or_else(|| {
                        klights_reconcile_api::ReconcileSinkError::unavailable(
                            "GC non-Pod finalization resource has no metadata object",
                        )
                    })?;
                if retained.is_empty() {
                    metadata.remove("finalizers");
                } else {
                    metadata.insert(
                        "finalizers".to_string(),
                        serde_json::Value::Array(retained.clone()),
                    );
                }
                let target = FinalizerResourceTarget::try_new(
                    &resource.api_version,
                    &resource.kind,
                    resource.namespace.as_deref(),
                    &resource.name,
                )
                .map_err(|error| {
                    klights_reconcile_api::ReconcileSinkError::unavailable(error.to_string())
                })?;
                let updated = self
                    .lifecycle
                    .update_resource(FinalizerUpdateRequest {
                        target: target.clone(),
                        data,
                        preconditions: ResourcePreconditions::from_resource(&resource),
                    })
                    .await
                    .map_err(|error| {
                        klights_reconcile_api::ReconcileSinkError::unavailable(error.to_string())
                    })?;
                if !retained.is_empty() {
                    return Ok(
                        klights_reconcile_api::GcNonPodFinalizationOutcome::MarkedTerminating,
                    );
                }
                self.lifecycle
                    .delete_with_tombstone(FinalizerTombstoneDeleteRequest {
                        target,
                        preconditions: ResourcePreconditions::from_resource(&updated),
                        grace_seconds: 0,
                    })
                    .await
                    .map_err(|error| {
                        klights_reconcile_api::ReconcileSinkError::unavailable(error.to_string())
                    })?;
                return Ok(klights_reconcile_api::GcNonPodFinalizationOutcome::HardDeleted);
            }
            let delete = k8s_native_service::generic_command::NonForegroundDeleteRequest {
                target: k8s_native_service::generic_command::ResourceDeleteTarget {
                    api_version: &request.resource.api_version,
                    kind: &request.resource.kind,
                    namespace: request.resource.namespace.as_deref(),
                    name: &request.resource.name,
                },
                initial_resource: request.resource.clone(),
                delete_preconditions: ResourcePreconditions::uid(request.resource.uid.clone()),
                orphan_children_before_completion: request.orphan_children,
                uid_mismatch_is_conflict: false,
                grace_seconds: 0,
                operation_now: klights_supervisor::SystemWallClock::now_utc(),
            };
            match k8s_native_service::generic_command::complete_non_foreground_delete_with_live_recheck(
                &self.lifecycle,
                delete,
            )
            .await
            {
                Ok(k8s_native_service::generic_command::DeleteCompletion::HardDeleted(_)) => Ok(
                    klights_reconcile_api::GcNonPodFinalizationOutcome::HardDeleted,
                ),
                Ok(k8s_native_service::generic_command::DeleteCompletion::MarkedTerminating(_)) => {
                    Ok(klights_reconcile_api::GcNonPodFinalizationOutcome::MarkedTerminating)
                }
                Ok(k8s_native_service::generic_command::DeleteCompletion::GoneOrUidChanged)
                | Err(k8s_native_service::AppError::NotFound(_)) => {
                    Ok(klights_reconcile_api::GcNonPodFinalizationOutcome::Gone)
                }
                Err(error) => Err(klights_reconcile_api::ReconcileSinkError::unavailable(
                    format!("{error:?}"),
                )),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use klights_reconcile_api::{GcNonPodFinalizationPort, GcNonPodFinalizationRequest};

    #[tokio::test]
    async fn non_pod_port_rejects_pod_without_touching_datastore() {
        let db = crate::bootstrap::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
        let pod = db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "guarded",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "guarded",
                        "uid": "guarded-uid"
                    },
                    "spec": {"nodeName": "node-a", "containers": []}
                }),
            )
            .await
            .expect("create Pod");
        let ports = crate::bootstrap::cluster_store::selector::sqlite_opened_passive_store(&db);
        let adapter = GcNonPodFinalizationAdapter::new_for_test(
            ports.applied_outbox,
            std::sync::Arc::new(db.clone()),
            ports.read_ports.resource_reads(),
            ports.ownership_reads,
        );

        let error = adapter
            .finalize_non_pod(GcNonPodFinalizationRequest {
                resource: pod,
                orphan_children: false,
                remove_foreground_finalizer: false,
            })
            .await
            .expect_err("non-Pod port must reject Pod");

        assert!(error.to_string().contains("rejects v1/Pod"));
        assert!(
            db.get_resource("v1", "Pod", Some("default"), "guarded")
                .await
                .expect("read Pod")
                .is_some(),
            "rejected request must not remove the actor-owned Pod row"
        );
    }

    #[derive(Default)]
    struct RecordingCommands {
        commands: std::sync::Mutex<Vec<klights_cluster_core::StorageCommand>>,
        reject_as_follower: bool,
    }

    impl klights_leader_api::LeaderResourceCommand for RecordingCommands {
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
                    klights_cluster_core::StorageCommand::UpdateResource { mut data, .. } => {
                        data["metadata"]["resourceVersion"] = serde_json::json!("42");
                        Ok(klights_leader_api::ResourceCommandResult::Resource(
                            klights_cluster_core::Resource::try_from_data(std::sync::Arc::new(
                                data,
                            ))
                            .unwrap(),
                        ))
                    }
                    klights_cluster_core::StorageCommand::DeleteResourceWithTombstone {
                        ..
                    } => Ok(klights_leader_api::ResourceCommandResult::Resource(
                        klights_cluster_core::Resource::try_from_data(std::sync::Arc::new(
                            serde_json::json!({
                                "apiVersion": "v1",
                                "kind": "ReplicationController",
                                "metadata": {
                                    "namespace": "default",
                                    "name": "foreground-owner",
                                    "uid": "foreground-owner-uid",
                                    "resourceVersion": "43"
                                }
                            }),
                        ))
                        .unwrap(),
                    )),
                    other => panic!("unexpected command: {other:?}"),
                }
            })
        }
    }

    #[tokio::test]
    async fn ready_foreground_non_pod_finalization_is_command_routed_end_to_end() {
        let db = crate::bootstrap::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
        let owner = db
            .create_resource(
                "v1",
                "ReplicationController",
                Some("default"),
                "foreground-owner",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ReplicationController",
                    "metadata": {
                        "namespace": "default",
                        "name": "foreground-owner",
                        "uid": "foreground-owner-uid",
                        "deletionTimestamp": "2026-08-12T00:00:00Z",
                        "finalizers": ["foregroundDeletion"]
                    },
                    "spec": {"replicas": 0}
                }),
            )
            .await
            .expect("create owner");
        let observed_rv = owner.resource_version;
        let commands = std::sync::Arc::new(RecordingCommands::default());
        let ports = crate::bootstrap::cluster_store::selector::sqlite_opened_passive_store(&db);
        let adapter = GcNonPodFinalizationAdapter::new_with_commands(
            ports.read_ports.resource_reads(),
            ports.ownership_reads,
            commands.clone(),
        );

        let outcome = adapter
            .finalize_non_pod(GcNonPodFinalizationRequest {
                resource: owner,
                orphan_children: false,
                remove_foreground_finalizer: true,
            })
            .await
            .expect("finalize through leader commands");

        assert_eq!(
            outcome,
            klights_reconcile_api::GcNonPodFinalizationOutcome::HardDeleted
        );
        {
            let submitted = commands.commands.lock().unwrap();
            assert_eq!(
                submitted.len(),
                2,
                "remove and delete must both be commands"
            );
            let klights_cluster_core::StorageCommand::UpdateResource {
                expected_rv,
                preconditions,
                data,
                ..
            } = &submitted[0]
            else {
                panic!("first command must remove the foreground finalizer")
            };
            assert_eq!(*expected_rv, observed_rv);
            assert_eq!(preconditions.uid.as_deref(), Some("foreground-owner-uid"));
            assert_eq!(preconditions.resource_version, Some(observed_rv));
            assert!(data.pointer("/metadata/finalizers").is_none());
            let klights_cluster_core::StorageCommand::DeleteResourceWithTombstone {
                preconditions,
                ..
            } = &submitted[1]
            else {
                panic!("second command must delete the finalizer-free owner")
            };
            assert_eq!(preconditions.uid.as_deref(), Some("foreground-owner-uid"));
            assert_eq!(preconditions.resource_version, Some(42));
        }
        let passive = db
            .get_resource(
                "v1",
                "ReplicationController",
                Some("default"),
                "foreground-owner",
            )
            .await
            .expect("read passive store")
            .expect("commands are not applied by the recorder");
        assert_eq!(passive.resource_version, observed_rv);
        assert_eq!(
            passive
                .data
                .pointer("/metadata/finalizers/0")
                .and_then(|v| v.as_str()),
            Some("foregroundDeletion"),
            "adapter must not mutate the passive store before committed apply"
        );
    }

    #[tokio::test]
    async fn ready_foreground_non_pod_finalization_rejects_follower_without_local_mutation() {
        let db = crate::bootstrap::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
        let owner = db
            .create_resource(
                "v1",
                "ReplicationController",
                Some("default"),
                "follower-owner",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ReplicationController",
                    "metadata": {
                        "namespace": "default",
                        "name": "follower-owner",
                        "uid": "follower-owner-uid",
                        "deletionTimestamp": "2026-08-12T00:00:00Z",
                        "finalizers": ["foregroundDeletion"]
                    },
                    "spec": {"replicas": 0}
                }),
            )
            .await
            .expect("create owner");
        let observed_rv = owner.resource_version;
        let commands = std::sync::Arc::new(RecordingCommands {
            commands: std::sync::Mutex::new(Vec::new()),
            reject_as_follower: true,
        });
        let ports = crate::bootstrap::cluster_store::selector::sqlite_opened_passive_store(&db);
        let adapter = GcNonPodFinalizationAdapter::new_with_commands(
            ports.read_ports.resource_reads(),
            ports.ownership_reads,
            commands.clone(),
        );

        let error = adapter
            .finalize_non_pod(GcNonPodFinalizationRequest {
                resource: owner,
                orphan_children: false,
                remove_foreground_finalizer: true,
            })
            .await
            .expect_err("follower authority must reject GC finalization");

        assert!(!error.to_string().is_empty());
        assert_eq!(commands.commands.lock().unwrap().len(), 1);
        let passive = db
            .get_resource(
                "v1",
                "ReplicationController",
                Some("default"),
                "follower-owner",
            )
            .await
            .expect("read passive store")
            .expect("follower rejection preserves row");
        assert_eq!(passive.resource_version, observed_rv);
        assert_eq!(
            passive
                .data
                .pointer("/metadata/finalizers/0")
                .and_then(|v| v.as_str()),
            Some("foregroundDeletion")
        );
    }
}
