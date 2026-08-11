//! Composition-owned bridge from the canonical replication outbox capability
//! to leader-side controller effects.
//!
//! `EmbeddedOutboxDelivery` remains the sole command/auth/CAS/ledger owner.
//! This adapter only translates its committed effect into the controller and
//! namespace side-effect ports that are wired later during bootstrap.

use std::sync::Arc;

use klights_leader_api::{
    AuthenticatedOutboxDeliveryRequest, LeaderAuthenticatedOutboxDelivery, LeaderOutboxDelivery,
    OutboxDeliveryFuture, OutboxDeliveryRequest, OutboxPayloadCodec,
};
use tokio::sync::OnceCell;

use crate::datastore::{DatastoreHandle, Resource};
use klights_cluster_core::command::StorageCommand;
use klights_controllers::ControllerDispatcher;

pub(crate) struct RootOutboxSideEffectState {
    db: DatastoreHandle,
    controller_dispatcher: OnceCell<Arc<ControllerDispatcher>>,
    non_pod_finalization: OnceCell<Arc<dyn klights_reconcile_api::GcNonPodFinalizationPort>>,
    namespace_termination: OnceCell<Arc<dyn klights_reconcile_api::NamespaceTerminationSink>>,
}

impl RootOutboxSideEffectState {
    pub(crate) fn new(db: DatastoreHandle) -> Self {
        Self {
            db,
            controller_dispatcher: OnceCell::new(),
            non_pod_finalization: OnceCell::new(),
            namespace_termination: OnceCell::new(),
        }
    }

    pub(crate) fn set_controller_dispatcher(&self, dispatcher: Arc<ControllerDispatcher>) {
        let _ = self.controller_dispatcher.set(dispatcher);
    }

    pub(crate) fn set_non_pod_finalization(
        &self,
        port: Arc<dyn klights_reconcile_api::GcNonPodFinalizationPort>,
    ) {
        let _ = self.non_pod_finalization.set(port);
    }

    pub(crate) fn set_namespace_termination(
        &self,
        port: Arc<dyn klights_reconcile_api::NamespaceTerminationSink>,
    ) {
        let _ = self.namespace_termination.set(port);
    }
}

pub(crate) struct RootCommittedOutboxDelivery {
    embedded: Arc<klights_replication::leader_api::EmbeddedOutboxDelivery>,
    side_effects: Arc<RootOutboxSideEffectState>,
    codec: Arc<dyn OutboxPayloadCodec>,
    local_node: String,
}

impl RootCommittedOutboxDelivery {
    pub(crate) fn new(
        embedded: Arc<klights_replication::leader_api::EmbeddedOutboxDelivery>,
        side_effects: Arc<RootOutboxSideEffectState>,
        codec: Arc<dyn OutboxPayloadCodec>,
        local_node: String,
    ) -> Self {
        Self {
            embedded,
            side_effects,
            codec,
            local_node,
        }
    }

    async fn deliver_authenticated(
        &self,
        request: AuthenticatedOutboxDeliveryRequest,
    ) -> Result<klights_leader_api::OutboxDeliveryResult, klights_leader_api::OutboxDeliveryError>
    {
        let (authenticated_node, request) = request.into_parts();
        let (
            codec_version,
            idempotency_key,
            operation,
            payload,
            client_id,
            stream_id,
            stream_sequence,
        ) = request.into_parts();
        if !klights_cluster_core::supports_command_codec_version(codec_version) {
            return Err(klights_leader_api::OutboxDeliveryError::codec_incompatible(
                codec_version,
                klights_cluster_core::COMMAND_CODEC_VERSION,
            ));
        }
        let decoded_command = self.codec.decode(payload.as_ref()).map_err(|error| {
            klights_leader_api::OutboxDeliveryError::invalid("delivery.payload", error.to_string())
        });
        let side_effect_command = decoded_command
            .as_ref()
            .ok()
            .filter(|command| {
                klights_controllers::side_effects::applied_pod::needs_committed_pod_side_effects(
                    command,
                )
            })
            .cloned();
        let watermark = Some(klights_cluster_core::OutboxStreamWatermark {
            client_id,
            stream_id,
            stream_seq: stream_sequence,
        });
        let effect = self
            .embedded
            .deliver_authenticated_outbox_command_effect(
                authenticated_node,
                idempotency_key,
                operation,
                decoded_command,
                watermark,
            )
            .await?;
        let (result, resource_effect, pod_endpoint_effect, resource) = effect.into_parts();
        if let Some(command) = side_effect_command.as_ref() {
            self.dispatch_committed_side_effects(
                command,
                resource.as_ref(),
                resource_effect,
                pod_endpoint_effect,
            )
            .await?;
        }
        Ok(result.into())
    }

    async fn dispatch_committed_side_effects(
        &self,
        command: &StorageCommand,
        resource: Option<&Resource>,
        resource_effect: klights_cluster_core::ResourceMutationEffect,
        pod_endpoint_effect: klights_cluster_core::PodEndpointEffect,
    ) -> Result<(), klights_leader_api::OutboxDeliveryError> {
        if resource_effect == klights_cluster_core::ResourceMutationEffect::Unchanged
            && resource.is_none()
        {
            return Ok(());
        }
        let controller_dispatcher =
            self.side_effects
                .controller_dispatcher
                .get()
                .ok_or_else(|| {
                    klights_leader_api::OutboxDeliveryError::unavailable(
                        "controller dispatcher is not ready for committed Pod side effects",
                    )
                })?;
        let gc_pod_delete_sink = if matches!(
            command,
            StorageCommand::DeleteResource { api_version, kind, .. }
                if api_version == "v1" && kind == "Pod"
        ) || matches!(command, StorageCommand::FinalizeBoundPod { .. })
        {
            Some(controller_dispatcher.pod_delete_sink())
        } else {
            None
        };
        crate::bootstrap::controller_adapters::applied_pod_side_effect_adapter::handle_applied_pod_side_effects(
            klights_controllers::side_effects::applied_pod::AppliedPodSideEffectSinks::new(
                Some(controller_dispatcher.as_ref()
                    as &dyn klights_reconcile_api::ControllerReconcileSink),
                Some(controller_dispatcher.as_ref()
                    as &dyn klights_reconcile_api::ServiceReconcileSink),
                gc_pod_delete_sink,
                self.side_effects
                    .non_pod_finalization
                    .get()
                    .map(Arc::as_ref),
                self.side_effects
                    .namespace_termination
                    .get()
                    .map(Arc::as_ref),
                controller_dispatcher.gc_coordination(),
            ),
            command,
            resource,
            pod_endpoint_effect,
            self.side_effects.db.as_ref(),
        )
        .await
        .map_err(|error| klights_leader_api::OutboxDeliveryError::unavailable(error.to_string()))
    }
}

impl LeaderAuthenticatedOutboxDelivery for RootCommittedOutboxDelivery {
    fn deliver_authenticated_outbox(
        &self,
        request: AuthenticatedOutboxDeliveryRequest,
    ) -> OutboxDeliveryFuture<'_> {
        Box::pin(self.deliver_authenticated(request))
    }
}

impl LeaderOutboxDelivery for RootCommittedOutboxDelivery {
    fn deliver_outbox(&self, request: OutboxDeliveryRequest) -> OutboxDeliveryFuture<'_> {
        Box::pin(async move {
            let request =
                AuthenticatedOutboxDeliveryRequest::try_new(self.local_node.clone(), request)?;
            self.deliver_authenticated(request).await
        })
    }
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
#[allow(dead_code)]
pub(crate) fn test_resource_command(
    db: DatastoreHandle,
    authority: &crate::bootstrap::authority::AuthorityHandle,
) -> Arc<dyn klights_leader_api::LeaderResourceCommand> {
    let proposal: Arc<dyn klights_replication::proposal::RaftProposal> =
        Arc::new(crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(db.clone()));
    let resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery> = Arc::new(
        crate::bootstrap::outbox_apply_adapter::BackendResourceQueryFixture::new(
            db,
            authority
                .legacy_watch_for_test()
                .expect("test authority retains its source watch"),
        ),
    );
    Arc::new(
        klights_replication::leader_api::EmbeddedLeaderResourceCommand::new(
            proposal,
            resource_query,
            authority
                .legacy_watch_for_test()
                .expect("test authority retains its source watch"),
        ),
    )
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
pub(crate) fn test_outbox_delivery(
    db: DatastoreHandle,
    authority: &crate::bootstrap::authority::AuthorityHandle,
    side_effects: Arc<RootOutboxSideEffectState>,
    local_node: String,
) -> Arc<RootCommittedOutboxDelivery> {
    let proposal: Arc<dyn klights_replication::proposal::RaftProposal> =
        Arc::new(crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(db.clone()));
    let resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery> = Arc::new(
        crate::bootstrap::outbox_apply_adapter::BackendResourceQueryFixture::new(
            db,
            authority
                .legacy_watch_for_test()
                .expect("test authority retains its source watch"),
        ),
    );
    let embedded = Arc::new(
        klights_replication::leader_api::EmbeddedOutboxDelivery::new(
            proposal,
            resource_query,
            authority
                .legacy_watch_for_test()
                .expect("test authority retains its source watch"),
        ),
    );
    Arc::new(RootCommittedOutboxDelivery::new(
        embedded,
        side_effects,
        crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec(),
        local_node,
    ))
}
