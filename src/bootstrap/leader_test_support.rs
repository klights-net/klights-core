//! Narrow test-only focused-port helpers for bootstrap-owned tests.

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

        $crate::bootstrap::leader_test_support::impl_unavailable_leader_network!($client);
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
