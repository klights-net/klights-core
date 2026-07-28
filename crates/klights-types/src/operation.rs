//! Dormant semantic operation identities for transport migration proof.
//!
//! These values describe the current HTTP and internal-RPC surface. Production
//! routing does not consult them during the crate refactor.

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum OperationId {
    HealthRead,
    RuntimeMetricsRead,
    VersionRead,
    OpenIdConfigurationRead,
    OpenIdKeysRead,
    OpenApiRead,
    ApiDiscoveryRead,
    ResourceGet,
    ResourceList,
    ResourceWatch,
    ResourceCommand,
    ResourceStatusGet,
    ResourceStatusCommand,
    ResourceScaleGet,
    ResourceScaleCommand,
    NamespaceFinalize,
    PodLogRead,
    PodExec,
    PodAttach,
    PodPortForward,
    PodBinding,
    PodEviction,
    PodProxy,
    NodeProxy,
    ServiceProxy,
    CustomResourceSubresource,
    AuthorizationReview,
    TokenReview,
    ServiceAccountToken,
    CertificateApproval,
    ResourceMetricsRead,
    AggregatedApiProxy,
    DebugRead,
    InternalFollowerConnect,
    InternalMetadataRead,
    InternalProjectedToken,
    InternalOutboxApply,
    InternalNodeLeaseRenew,
    InternalNodeSubnetAllocate,
    InternalNodeSubnetGet,
    InternalPeerSubnetList,
    InternalNodeDataplaneGet,
    InternalPeerEndpointObserve,
    InternalPodCleanupList,
    InternalPodCleanupDelete,
    InternalRaftAppend,
    InternalRaftVote,
    InternalRaftSnapshot,
    InternalControlPlaneJoin,
    InternalControlPlaneSignCsr,
}

impl OperationId {
    pub const ALL: [Self; 50] = [
        Self::HealthRead,
        Self::RuntimeMetricsRead,
        Self::VersionRead,
        Self::OpenIdConfigurationRead,
        Self::OpenIdKeysRead,
        Self::OpenApiRead,
        Self::ApiDiscoveryRead,
        Self::ResourceGet,
        Self::ResourceList,
        Self::ResourceWatch,
        Self::ResourceCommand,
        Self::ResourceStatusGet,
        Self::ResourceStatusCommand,
        Self::ResourceScaleGet,
        Self::ResourceScaleCommand,
        Self::NamespaceFinalize,
        Self::PodLogRead,
        Self::PodExec,
        Self::PodAttach,
        Self::PodPortForward,
        Self::PodBinding,
        Self::PodEviction,
        Self::PodProxy,
        Self::NodeProxy,
        Self::ServiceProxy,
        Self::CustomResourceSubresource,
        Self::AuthorizationReview,
        Self::TokenReview,
        Self::ServiceAccountToken,
        Self::CertificateApproval,
        Self::ResourceMetricsRead,
        Self::AggregatedApiProxy,
        Self::DebugRead,
        Self::InternalFollowerConnect,
        Self::InternalMetadataRead,
        Self::InternalProjectedToken,
        Self::InternalOutboxApply,
        Self::InternalNodeLeaseRenew,
        Self::InternalNodeSubnetAllocate,
        Self::InternalNodeSubnetGet,
        Self::InternalPeerSubnetList,
        Self::InternalNodeDataplaneGet,
        Self::InternalPeerEndpointObserve,
        Self::InternalPodCleanupList,
        Self::InternalPodCleanupDelete,
        Self::InternalRaftAppend,
        Self::InternalRaftVote,
        Self::InternalRaftSnapshot,
        Self::InternalControlPlaneJoin,
        Self::InternalControlPlaneSignCsr,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HealthRead => "health.read",
            Self::RuntimeMetricsRead => "runtime-metrics.read",
            Self::VersionRead => "version.read",
            Self::OpenIdConfigurationRead => "openid-configuration.read",
            Self::OpenIdKeysRead => "openid-keys.read",
            Self::OpenApiRead => "openapi.read",
            Self::ApiDiscoveryRead => "api-discovery.read",
            Self::ResourceGet => "resource.get",
            Self::ResourceList => "resource.list",
            Self::ResourceWatch => "resource.watch",
            Self::ResourceCommand => "resource.command",
            Self::ResourceStatusGet => "resource-status.get",
            Self::ResourceStatusCommand => "resource-status.command",
            Self::ResourceScaleGet => "resource-scale.get",
            Self::ResourceScaleCommand => "resource-scale.command",
            Self::NamespaceFinalize => "namespace.finalize",
            Self::PodLogRead => "pod-log.read",
            Self::PodExec => "pod.exec",
            Self::PodAttach => "pod.attach",
            Self::PodPortForward => "pod.port-forward",
            Self::PodBinding => "pod.binding",
            Self::PodEviction => "pod.eviction",
            Self::PodProxy => "pod.proxy",
            Self::NodeProxy => "node.proxy",
            Self::ServiceProxy => "service.proxy",
            Self::CustomResourceSubresource => "custom-resource.subresource",
            Self::AuthorizationReview => "authorization.review",
            Self::TokenReview => "authentication.token-review",
            Self::ServiceAccountToken => "service-account.token",
            Self::CertificateApproval => "certificate.approval",
            Self::ResourceMetricsRead => "resource-metrics.read",
            Self::AggregatedApiProxy => "aggregated-api.proxy",
            Self::DebugRead => "debug.read",
            Self::InternalFollowerConnect => "internal.follower-connect",
            Self::InternalMetadataRead => "internal.metadata.read",
            Self::InternalProjectedToken => "internal.projected-token",
            Self::InternalOutboxApply => "internal.outbox.apply",
            Self::InternalNodeLeaseRenew => "internal.node-lease.renew",
            Self::InternalNodeSubnetAllocate => "internal.node-subnet.allocate",
            Self::InternalNodeSubnetGet => "internal.node-subnet.get",
            Self::InternalPeerSubnetList => "internal.peer-subnet.list",
            Self::InternalNodeDataplaneGet => "internal.node-dataplane.get",
            Self::InternalPeerEndpointObserve => "internal.peer-endpoint.observe",
            Self::InternalPodCleanupList => "internal.pod-cleanup.list",
            Self::InternalPodCleanupDelete => "internal.pod-cleanup.delete",
            Self::InternalRaftAppend => "internal.raft.append",
            Self::InternalRaftVote => "internal.raft.vote",
            Self::InternalRaftSnapshot => "internal.raft.snapshot",
            Self::InternalControlPlaneJoin => "internal.control-plane.join",
            Self::InternalControlPlaneSignCsr => "internal.control-plane.sign-csr",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum OperationKind {
    Query,
    Command,
    Effect,
    Stream,
    Representation,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PolicyProfileId {
    PublicOperational,
    AuthorizedRead,
    AuthorizedCommand,
    AuthorizedStream,
    ResourceRead,
    ResourceCommand,
    InternalAuthenticated,
    InternalConsensus,
    InternalBootstrap,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FallbackFamilyId {
    Operational,
    Discovery,
    OpenApi,
    Authentication,
    Authorization,
    StatusSubresource,
    ScaleSubresource,
    PodSubresource,
    NodeSubresource,
    ServiceProxy,
    CustomResource,
    Aggregation,
    Metrics,
    Debug,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum OperationSelection {
    Inactive,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MigrationState {
    LegacyDirect,
    CanonicalDirect,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AttemptIdentity {
    None,
    Preserve,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationMetadata {
    pub id: OperationId,
    pub family: &'static str,
    pub kind: OperationKind,
    pub policy_profile: PolicyProfileId,
    pub state: MigrationState,
    pub selection: OperationSelection,
    pub attempt_identity: AttemptIdentity,
    pub native_destination: Option<&'static str>,
    pub fallback_family: Option<FallbackFamilyId>,
}

macro_rules! native {
    ($id:ident, $family:literal, $kind:ident, $policy:ident, $attempt:ident, $destination:literal) => {
        OperationMetadata {
            id: OperationId::$id,
            family: $family,
            kind: OperationKind::$kind,
            policy_profile: PolicyProfileId::$policy,
            state: MigrationState::CanonicalDirect,
            selection: OperationSelection::Inactive,
            attempt_identity: AttemptIdentity::$attempt,
            native_destination: Some($destination),
            fallback_family: None,
        }
    };
}

macro_rules! fallback {
    ($id:ident, $family:literal, $kind:ident, $policy:ident, $attempt:ident, $fallback:ident) => {
        OperationMetadata {
            id: OperationId::$id,
            family: $family,
            kind: OperationKind::$kind,
            policy_profile: PolicyProfileId::$policy,
            state: MigrationState::LegacyDirect,
            selection: OperationSelection::Inactive,
            attempt_identity: AttemptIdentity::$attempt,
            native_destination: None,
            fallback_family: Some(FallbackFamilyId::$fallback),
        }
    };
}

pub const ALL_OPERATION_METADATA: [OperationMetadata; OperationId::ALL.len()] = [
    fallback!(
        HealthRead,
        "operational",
        Query,
        PublicOperational,
        None,
        Operational
    ),
    fallback!(
        RuntimeMetricsRead,
        "operational",
        Query,
        PublicOperational,
        None,
        Operational
    ),
    fallback!(
        VersionRead,
        "operational",
        Query,
        PublicOperational,
        None,
        Operational
    ),
    fallback!(
        OpenIdConfigurationRead,
        "authentication",
        Representation,
        PublicOperational,
        None,
        Authentication
    ),
    fallback!(
        OpenIdKeysRead,
        "authentication",
        Representation,
        PublicOperational,
        None,
        Authentication
    ),
    fallback!(
        OpenApiRead,
        "openapi",
        Representation,
        AuthorizedRead,
        None,
        OpenApi
    ),
    fallback!(
        ApiDiscoveryRead,
        "discovery",
        Representation,
        AuthorizedRead,
        None,
        Discovery
    ),
    native!(
        ResourceGet,
        "resource-read",
        Query,
        ResourceRead,
        None,
        "klights-leader-api::LeaderResourceQuery::get_resource"
    ),
    native!(
        ResourceList,
        "resource-read",
        Query,
        ResourceRead,
        None,
        "klights-leader-api::LeaderResourceQuery::list_resources"
    ),
    native!(
        ResourceWatch,
        "resource-watch",
        Stream,
        ResourceRead,
        None,
        "klights-leader-api::LeaderWatch::watch_resources"
    ),
    native!(
        ResourceCommand,
        "resource-command",
        Command,
        ResourceCommand,
        Preserve,
        "klights-leader-api::LeaderResourceCommand::submit_resource_command"
    ),
    fallback!(
        ResourceStatusGet,
        "status-subresource",
        Query,
        AuthorizedRead,
        None,
        StatusSubresource
    ),
    fallback!(
        ResourceStatusCommand,
        "status-subresource",
        Command,
        AuthorizedCommand,
        Preserve,
        StatusSubresource
    ),
    fallback!(
        ResourceScaleGet,
        "scale-subresource",
        Query,
        AuthorizedRead,
        None,
        ScaleSubresource
    ),
    fallback!(
        ResourceScaleCommand,
        "scale-subresource",
        Command,
        AuthorizedCommand,
        Preserve,
        ScaleSubresource
    ),
    fallback!(
        NamespaceFinalize,
        "namespace-lifecycle",
        Command,
        AuthorizedCommand,
        Preserve,
        StatusSubresource
    ),
    fallback!(
        PodLogRead,
        "pod-streaming",
        Stream,
        AuthorizedStream,
        None,
        PodSubresource
    ),
    fallback!(
        PodExec,
        "pod-streaming",
        Stream,
        AuthorizedStream,
        None,
        PodSubresource
    ),
    fallback!(
        PodAttach,
        "pod-streaming",
        Stream,
        AuthorizedStream,
        None,
        PodSubresource
    ),
    fallback!(
        PodPortForward,
        "pod-streaming",
        Stream,
        AuthorizedStream,
        None,
        PodSubresource
    ),
    fallback!(
        PodBinding,
        "pod-lifecycle",
        Effect,
        AuthorizedCommand,
        Preserve,
        PodSubresource
    ),
    fallback!(
        PodEviction,
        "pod-lifecycle",
        Effect,
        AuthorizedCommand,
        Preserve,
        PodSubresource
    ),
    fallback!(
        PodProxy,
        "pod-proxy",
        Stream,
        AuthorizedStream,
        None,
        PodSubresource
    ),
    fallback!(
        NodeProxy,
        "node-proxy",
        Stream,
        AuthorizedStream,
        None,
        NodeSubresource
    ),
    fallback!(
        ServiceProxy,
        "service-proxy",
        Stream,
        AuthorizedStream,
        None,
        ServiceProxy
    ),
    fallback!(
        CustomResourceSubresource,
        "custom-resource",
        Stream,
        AuthorizedStream,
        Preserve,
        CustomResource
    ),
    fallback!(
        AuthorizationReview,
        "authorization",
        Query,
        AuthorizedRead,
        None,
        Authorization
    ),
    fallback!(
        TokenReview,
        "authentication",
        Query,
        AuthorizedRead,
        None,
        Authentication
    ),
    fallback!(
        ServiceAccountToken,
        "authentication",
        Command,
        AuthorizedCommand,
        Preserve,
        Authentication
    ),
    fallback!(
        CertificateApproval,
        "certificate-lifecycle",
        Command,
        AuthorizedCommand,
        Preserve,
        StatusSubresource
    ),
    fallback!(
        ResourceMetricsRead,
        "metrics",
        Representation,
        AuthorizedRead,
        None,
        Metrics
    ),
    fallback!(
        AggregatedApiProxy,
        "aggregation",
        Stream,
        AuthorizedStream,
        Preserve,
        Aggregation
    ),
    fallback!(DebugRead, "debug", Query, AuthorizedRead, None, Debug),
    native!(
        InternalFollowerConnect,
        "internal-transport",
        Stream,
        InternalAuthenticated,
        None,
        "klights-leader-rpc::FollowerStreamHandler::connect"
    ),
    native!(
        InternalMetadataRead,
        "internal-metadata",
        Query,
        InternalAuthenticated,
        None,
        "klights-leader-api::LeaderClusterStatusMetadata::cluster_status_metadata"
    ),
    native!(
        InternalProjectedToken,
        "projected-token",
        Command,
        InternalAuthenticated,
        Preserve,
        "klights-leader-api::LeaderAuthenticatedProjectedServiceAccountToken::issue_authenticated_projected_service_account_token"
    ),
    native!(
        InternalOutboxApply,
        "outbox",
        Command,
        InternalAuthenticated,
        Preserve,
        "klights-leader-api::LeaderAuthenticatedOutboxDelivery::deliver_authenticated_outbox"
    ),
    native!(
        InternalNodeLeaseRenew,
        "node-lifecycle",
        Command,
        InternalAuthenticated,
        Preserve,
        "klights-leader-api::LeaderNodeLeaseRenewal::renew_node_lease"
    ),
    native!(
        InternalNodeSubnetAllocate,
        "node-network",
        Command,
        InternalAuthenticated,
        Preserve,
        "klights-leader-api::LeaderNodeSubnetAllocation::allocate_node_subnet"
    ),
    native!(
        InternalNodeSubnetGet,
        "node-network",
        Query,
        InternalAuthenticated,
        None,
        "klights-leader-api::LeaderNetworkTopologyQuery::get_node_subnet"
    ),
    native!(
        InternalPeerSubnetList,
        "node-network",
        Query,
        InternalAuthenticated,
        None,
        "klights-leader-api::LeaderNetworkTopologyQuery::list_peer_subnets"
    ),
    native!(
        InternalNodeDataplaneGet,
        "node-network",
        Query,
        InternalAuthenticated,
        None,
        "klights-leader-api::LeaderNetworkTopologyQuery::get_node_dataplane"
    ),
    native!(
        InternalPeerEndpointObserve,
        "node-network",
        Effect,
        InternalAuthenticated,
        Preserve,
        "klights-leader-rpc::PeerEndpointObservation::observe_peer_endpoint"
    ),
    native!(
        InternalPodCleanupList,
        "pod-cleanup",
        Query,
        InternalAuthenticated,
        None,
        "klights-leader-api::LeaderPodCleanupIntents::list_pod_cleanup_intents"
    ),
    native!(
        InternalPodCleanupDelete,
        "pod-cleanup",
        Command,
        InternalAuthenticated,
        Preserve,
        "klights-leader-api::LeaderPodCleanupIntents::acknowledge_pod_cleanup_intent"
    ),
    native!(
        InternalRaftAppend,
        "raft-transport",
        Command,
        InternalConsensus,
        Preserve,
        "klights-leader-rpc::RaftRpcHandler::append_entries"
    ),
    native!(
        InternalRaftVote,
        "raft-transport",
        Command,
        InternalConsensus,
        Preserve,
        "klights-leader-rpc::RaftRpcHandler::vote"
    ),
    native!(
        InternalRaftSnapshot,
        "raft-transport",
        Command,
        InternalConsensus,
        Preserve,
        "klights-leader-rpc::RaftRpcHandler::install_snapshot"
    ),
    native!(
        InternalControlPlaneJoin,
        "control-plane-membership",
        Command,
        InternalBootstrap,
        Preserve,
        "klights-leader-api::ControlplaneJoinHandler::join"
    ),
    native!(
        InternalControlPlaneSignCsr,
        "control-plane-identity",
        Command,
        InternalBootstrap,
        Preserve,
        "klights-leader-rpc::ControlplaneCredentialIssuer::sign_server_csr"
    ),
];

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{ALL_OPERATION_METADATA, OperationId};

    #[test]
    fn operation_metadata_is_closed_bijective_and_singly_classified() {
        assert_eq!(ALL_OPERATION_METADATA.len(), OperationId::ALL.len());
        let mut ids = HashSet::new();
        for metadata in ALL_OPERATION_METADATA {
            assert!(ids.insert(metadata.id));
            assert_ne!(
                metadata.native_destination.is_some(),
                metadata.fallback_family.is_some()
            );
        }
        assert_eq!(ids, OperationId::ALL.into_iter().collect());
    }
}
