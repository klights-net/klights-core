pub mod leader_proxy;
pub mod local;

#[cfg(test)]
#[path = "tests/remote.rs"]
mod remote_tests;

#[cfg(test)]
#[path = "tests/worker.rs"]
mod worker_tests;

#[cfg(test)]
use klights_leader_api::ResourceListRequest;
use klights_leader_api::{
    DataplaneEncryption, HostPortRange as LeaderHostPortRange, LeaderCacheReadiness,
    LeaderNetworkTopologyQuery, LeaderNodeSubnetAllocation, LeaderPodCleanupIntents,
    LeaderProjectedServiceAccountToken, LeaderResourceQuery, LeaderWatch, NetworkDataplane,
    NetworkNodeMode, NetworkTopologyError, ResourceListResult, ResourceQueryError,
};
use std::sync::{Arc, Mutex};

use crate::datastore::ResourceList;
use tokio::sync::watch;

/// Small composition-time handle for the existing backend-neutral authority
/// contract.  Client implementations route through permits, never through
/// a raw leadership boolean.
#[derive(Clone)]
pub(crate) struct AuthorityHandle {
    authority: Arc<dyn klights_leader_api::LeaderAuthority>,
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    legacy_watch: Option<watch::Receiver<bool>>,
}

impl AuthorityHandle {
    pub(crate) fn route(&self) -> klights_leader_api::AuthorityRoute {
        self.authority.route()
    }

    pub(crate) fn local_permit(
        &self,
    ) -> Result<klights_leader_api::AuthorityPermit, klights_leader_api::AuthorityError> {
        match self.authority.route() {
            klights_leader_api::AuthorityRoute::Local(permit) => {
                self.authority.validate(&permit)?;
                Ok(permit)
            }
            klights_leader_api::AuthorityRoute::Forward { .. }
            | klights_leader_api::AuthorityRoute::Unavailable => {
                Err(klights_leader_api::AuthorityError::NotAuthoritative)
            }
        }
    }

    pub(crate) fn validate(
        &self,
        permit: &klights_leader_api::AuthorityPermit,
    ) -> Result<(), klights_leader_api::AuthorityError> {
        self.authority.validate(permit)
    }

    pub(crate) fn acquire(&self) -> klights_leader_api::AuthorityAcquireFuture<'_> {
        self.authority.acquire()
    }

    pub(crate) fn wait_for_revocation<'a>(
        &'a self,
        permit: &'a klights_leader_api::AuthorityPermit,
    ) -> klights_leader_api::AuthorityRevocationFuture<'a> {
        self.authority.wait_for_revocation(permit)
    }
}

impl<T> From<Arc<T>> for AuthorityHandle
where
    T: klights_leader_api::LeaderAuthority + 'static,
{
    fn from(authority: Arc<T>) -> Self {
        Self {
            authority: authority as Arc<dyn klights_leader_api::LeaderAuthority>,
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            legacy_watch: None,
        }
    }
}

impl From<Arc<dyn klights_leader_api::LeaderAuthority>> for AuthorityHandle {
    fn from(authority: Arc<dyn klights_leader_api::LeaderAuthority>) -> Self {
        Self {
            authority,
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            legacy_watch: None,
        }
    }
}

impl From<watch::Receiver<bool>> for AuthorityHandle {
    fn from(receiver: watch::Receiver<bool>) -> Self {
        let authority = Arc::new(WatchReceiverAuthority::new(receiver.clone()));
        Self {
            authority,
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            legacy_watch: Some(receiver),
        }
    }
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
impl AuthorityHandle {
    pub(crate) fn legacy_watch_for_test(&self) -> Option<watch::Receiver<bool>> {
        self.legacy_watch.clone()
    }
}

/// Compatibility input adapter for existing bootstrap/test construction. It
/// translates the legacy signal at the composition boundary into the same
/// opaque permit contract consumed by local and proxy clients.
struct WatchReceiverAuthority {
    receiver: Mutex<watch::Receiver<bool>>,
    generation: std::sync::atomic::AtomicU64,
    issuer: klights_leader_api::AuthorityPermitIssuer,
}

impl WatchReceiverAuthority {
    fn new(receiver: watch::Receiver<bool>) -> Self {
        Self {
            receiver: Mutex::new(receiver),
            generation: std::sync::atomic::AtomicU64::new(1),
            issuer: klights_leader_api::AuthorityPermitIssuer::new(),
        }
    }

    fn state(&self) -> (bool, u64) {
        use std::sync::atomic::Ordering;
        let mut receiver = self
            .receiver
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if receiver.has_changed().unwrap_or(true) {
            let _ = receiver.borrow_and_update();
            let generation = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
            (*receiver.borrow(), generation)
        } else {
            (*receiver.borrow(), self.generation.load(Ordering::Acquire))
        }
    }
}

impl klights_leader_api::LeaderAuthority for WatchReceiverAuthority {
    fn route(&self) -> klights_leader_api::AuthorityRoute {
        let (local, generation) = self.state();
        if local {
            klights_leader_api::AuthorityRoute::Local(self.issuer.issue(generation))
        } else {
            klights_leader_api::AuthorityRoute::Unavailable
        }
    }

    fn validate(
        &self,
        permit: &klights_leader_api::AuthorityPermit,
    ) -> Result<(), klights_leader_api::AuthorityError> {
        let (local, generation) = self.state();
        if !local {
            return Err(klights_leader_api::AuthorityError::NotAuthoritative);
        }
        self.issuer.validate(permit, generation)
    }

    fn acquire(&self) -> klights_leader_api::AuthorityAcquireFuture<'_> {
        let mut receiver = self
            .receiver
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        Box::pin(async move {
            loop {
                if let klights_leader_api::AuthorityRoute::Local(permit) = self.route() {
                    return Ok(permit);
                }
                receiver
                    .changed()
                    .await
                    .map_err(|_| klights_leader_api::AuthorityError::Closed)?;
            }
        })
    }

    fn wait_for_revocation<'a>(
        &'a self,
        permit: &'a klights_leader_api::AuthorityPermit,
    ) -> klights_leader_api::AuthorityRevocationFuture<'a> {
        let mut receiver = self
            .receiver
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let permit = permit.clone();
        Box::pin(async move {
            loop {
                if self.validate(&permit).is_err() || receiver.changed().await.is_err() {
                    return;
                }
            }
        })
    }
}

pub(crate) fn focused_node_subnet(
    subnet: klights_cluster_store::StoredNodeSubnet,
) -> std::result::Result<klights_leader_api::NodeSubnet, NetworkTopologyError> {
    let mode = match subnet.mode {
        klights_controllers::annotations::NodePeerMode::Root => NetworkNodeMode::Root,
        klights_controllers::annotations::NodePeerMode::Rootless => NetworkNodeMode::Rootless,
    };
    let hostport_range = subnet
        .hostport_range
        .map(|range| LeaderHostPortRange::try_new(range.start, range.end))
        .transpose()?;
    klights_leader_api::NodeSubnet::try_new(
        subnet.node_name.into_string(),
        subnet.subnet.to_string(),
        subnet.subnet_base_int,
        subnet.gateway_ip,
        subnet.node_ip,
        mode,
        hostport_range,
    )
}

pub(crate) fn legacy_node_subnet(
    subnet: klights_leader_api::NodeSubnet,
) -> std::result::Result<klights_cluster_store::StoredNodeSubnet, NetworkTopologyError> {
    let node_name = klights_types::NodeName::parse(subnet.node_name())
        .map_err(NetworkTopologyError::corrupt_response)?;
    let pod_subnet = klights_types::PodSubnet::parse(subnet.subnet())
        .map_err(NetworkTopologyError::corrupt_response)?;
    let mode = match subnet.mode() {
        NetworkNodeMode::Root => klights_controllers::annotations::NodePeerMode::Root,
        NetworkNodeMode::Rootless => klights_controllers::annotations::NodePeerMode::Rootless,
    };
    let hostport_range = subnet
        .hostport_range()
        .map(|range| klights_types::HostPortRange {
            start: range.start(),
            end: range.end(),
        });
    Ok(klights_cluster_store::StoredNodeSubnet {
        node_name,
        subnet: pod_subnet,
        subnet_base_int: subnet.subnet_base_int(),
        gateway_ip: subnet.gateway_ip(),
        node_ip: subnet.node_ip(),
        mode,
        hostport_range,
    })
}

pub(crate) fn focused_dataplane(
    metadata: klights_cluster_store::DataplanePeerMetadata,
) -> std::result::Result<NetworkDataplane, NetworkTopologyError> {
    NetworkDataplane::try_new(
        metadata.node_name,
        match metadata.mode {
            klights_cluster_store::DataplaneMode::Root => NetworkNodeMode::Root,
            klights_cluster_store::DataplaneMode::Rootless => NetworkNodeMode::Rootless,
        },
        match metadata.encryption {
            klights_cluster_store::DataplaneEncryption::Enabled => DataplaneEncryption::WireGuard,
            klights_cluster_store::DataplaneEncryption::Disabled => DataplaneEncryption::Direct,
        },
        metadata.public_key.as_ref().map(|key| key.as_str()),
        metadata.endpoint,
        metadata.port,
    )
}

pub(crate) fn legacy_dataplane(
    metadata: NetworkDataplane,
) -> std::result::Result<klights_cluster_store::DataplanePeerMetadata, NetworkTopologyError> {
    let mode = match metadata.mode() {
        NetworkNodeMode::Root => klights_cluster_store::DataplaneMode::Root,
        NetworkNodeMode::Rootless => klights_cluster_store::DataplaneMode::Rootless,
    };
    let encryption = match metadata.encryption() {
        DataplaneEncryption::WireGuard => klights_cluster_store::DataplaneEncryption::Enabled,
        DataplaneEncryption::Direct => klights_cluster_store::DataplaneEncryption::Disabled,
    };
    klights_cluster_store::DataplanePeerMetadata::try_new(
        metadata.node_name().to_string(),
        mode,
        encryption,
        metadata.public_key().map(str::to_owned),
        Some(metadata.endpoint().to_string()),
        metadata.port(),
    )
    .map_err(|error| NetworkTopologyError::corrupt_response(error.to_string()))
}

pub(crate) fn node_subnet_allocation_is_exhausted(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("exhaust") || lower.contains("query returned no rows")
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg(test)]
pub struct ListRequest {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub label_selector: Option<String>,
    pub field_selector: Option<String>,
    pub limit: Option<i64>,
    pub continue_token: Option<String>,
}

pub(crate) fn query_error(error: impl std::fmt::Display) -> ResourceQueryError {
    ResourceQueryError::query_failed(error.to_string())
}

#[cfg(test)]
pub(crate) fn legacy_list_request(request: &ResourceListRequest) -> ListRequest {
    ListRequest {
        api_version: request.api_version().to_string(),
        kind: request.kind().to_string(),
        namespace: request.namespace().map(str::to_owned),
        label_selector: request.label_selector().map(str::to_owned),
        field_selector: request.field_selector().map(str::to_owned),
        limit: request.limit(),
        continue_token: request.continue_token().map(str::to_owned),
    }
}

pub(crate) fn query_list_result(
    list: ResourceList,
) -> std::result::Result<ResourceListResult, ResourceQueryError> {
    ResourceListResult::try_new(
        list.items,
        list.resource_version,
        list.watch_replay_position,
        list.continue_token,
        list.remaining_item_count,
    )
}

/// Focused leader capabilities assembled only at a composition root.
///
/// Consumers take the individual port they need; this bundle exists to move
/// the complete immutable set between bootstrap phases without recreating the
/// former seven-capability trait object.
#[derive(Clone)]
pub struct LeaderClientPorts {
    pub resource_query: Arc<dyn LeaderResourceQuery>,
    pub watch: Arc<dyn LeaderWatch>,
    pub cache_readiness: Arc<dyn LeaderCacheReadiness>,
    pub projected_tokens: Arc<dyn LeaderProjectedServiceAccountToken>,
    pub pod_cleanup_intents: Arc<dyn LeaderPodCleanupIntents>,
    pub node_subnet_allocation: Arc<dyn LeaderNodeSubnetAllocation>,
    pub network_topology: Arc<dyn LeaderNetworkTopologyQuery>,
}

impl LeaderClientPorts {
    pub fn from_client<T>(client: Arc<T>) -> Self
    where
        T: LeaderResourceQuery
            + LeaderWatch
            + LeaderCacheReadiness
            + LeaderProjectedServiceAccountToken
            + LeaderPodCleanupIntents
            + LeaderNodeSubnetAllocation
            + LeaderNetworkTopologyQuery
            + Send
            + Sync
            + 'static,
    {
        Self {
            resource_query: client.clone(),
            watch: client.clone(),
            cache_readiness: client.clone(),
            projected_tokens: client.clone(),
            pod_cleanup_intents: client.clone(),
            node_subnet_allocation: client.clone(),
            network_topology: client,
        }
    }
}

#[cfg(test)]
macro_rules! impl_unavailable_leader_pod_effects {
    ($client:ty) => {
        impl klights_leader_api::LeaderProjectedServiceAccountToken for $client {
            fn issue_projected_service_account_token(
                &self,
                _request: klights_leader_api::ProjectedServiceAccountTokenRequest,
            ) -> klights_leader_api::ProjectedServiceAccountTokenFuture<'_> {
                Box::pin(async {
                    Err(
                        klights_leader_api::ProjectedServiceAccountTokenError::unavailable(
                            "projected token issuance is not used by this test client",
                        ),
                    )
                })
            }
        }

        impl klights_leader_api::LeaderPodCleanupIntents for $client {
            fn list_pod_cleanup_intents(
                &self,
                _request: klights_leader_api::PodCleanupIntentListRequest,
            ) -> klights_leader_api::PodCleanupIntentFuture<
                '_,
                Vec<klights_leader_api::PodCleanupIntent>,
            > {
                Box::pin(async {
                    Err(klights_leader_api::PodCleanupIntentError::unavailable(
                        "cleanup intents are not used by this test client",
                    ))
                })
            }

            fn acknowledge_pod_cleanup_intent(
                &self,
                _request: klights_leader_api::PodCleanupIntentAckRequest,
            ) -> klights_leader_api::PodCleanupIntentFuture<'_, ()> {
                Box::pin(async {
                    Err(klights_leader_api::PodCleanupIntentError::unavailable(
                        "cleanup-intent acknowledgement is not used by this test client",
                    ))
                })
            }
        }

        $crate::control_plane::client::impl_unavailable_leader_network!($client);
    };
}

#[cfg(test)]
pub(crate) use impl_unavailable_leader_pod_effects;

#[cfg(test)]
macro_rules! impl_unavailable_leader_network {
    ($client:ty) => {
        impl klights_leader_api::LeaderNodeSubnetAllocation for $client {
            fn allocate_node_subnet(
                &self,
                _request: klights_leader_api::NodeSubnetAllocationRequest,
            ) -> klights_leader_api::NodeSubnetAllocationFuture<
                '_,
                klights_leader_api::NodeSubnetAllocationResult,
            > {
                Box::pin(async {
                    Err(klights_leader_api::NodeSubnetAllocationError::retryable(
                        "network allocation is not used by this test client",
                    ))
                })
            }
        }

        impl klights_leader_api::LeaderNetworkTopologyQuery for $client {
            fn get_node_subnet(
                &self,
                _request: klights_leader_api::NodeSubnetQuery,
            ) -> klights_leader_api::NetworkTopologyFuture<
                '_,
                klights_leader_api::NodeSubnetResult,
            > {
                Box::pin(async {
                    Err(klights_leader_api::NetworkTopologyError::retryable(
                        "network topology is not used by this test client",
                    ))
                })
            }

            fn list_peer_subnets(
                &self,
                _request: klights_leader_api::PeerSubnetsQuery,
            ) -> klights_leader_api::NetworkTopologyFuture<
                '_,
                klights_leader_api::PeerSubnetsResult,
            > {
                Box::pin(async {
                    Err(klights_leader_api::NetworkTopologyError::retryable(
                        "network topology is not used by this test client",
                    ))
                })
            }

            fn get_node_dataplane(
                &self,
                _request: klights_leader_api::NodeDataplaneQuery,
            ) -> klights_leader_api::NetworkTopologyFuture<
                '_,
                klights_leader_api::NodeDataplaneResult,
            > {
                Box::pin(async {
                    Err(klights_leader_api::NetworkTopologyError::retryable(
                        "network topology is not used by this test client",
                    ))
                })
            }
        }
    };
}

#[cfg(test)]
pub(crate) use impl_unavailable_leader_network;

#[cfg(test)]
#[path = "tests/conversion.rs"]
mod tests;
