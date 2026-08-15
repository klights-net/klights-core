//! Portable test-only implementations for deliberately unavailable leader ports.

/// Deterministic unavailable watch port for compositions that do not exercise
/// positioned watch delivery.
pub struct UnavailableLeaderWatch {
    message: String,
}

impl UnavailableLeaderWatch {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl crate::LeaderWatch for UnavailableLeaderWatch {
    fn watch_resources(&self, _request: crate::WatchRequest) -> crate::LeaderWatchFuture<'_> {
        let message = self.message.clone();
        Box::pin(async move { Err(crate::LeaderWatchError::Unavailable { message }) })
    }
}

#[doc(hidden)]
#[macro_export]
macro_rules! __impl_unavailable_leader_pod_effects {
    ($client:ty) => {
        impl $crate::LeaderProjectedServiceAccountToken for $client {
            fn issue_projected_service_account_token(
                &self,
                _request: $crate::ProjectedServiceAccountTokenRequest,
            ) -> $crate::ProjectedServiceAccountTokenFuture<'_> {
                Box::pin(async {
                    Err($crate::ProjectedServiceAccountTokenError::unavailable(
                        "projected token issuance is not used by this test client",
                    ))
                })
            }
        }

        impl $crate::LeaderPodCleanupIntents for $client {
            fn list_pod_cleanup_intents(
                &self,
                _request: $crate::PodCleanupIntentListRequest,
            ) -> $crate::PodCleanupIntentFuture<'_, Vec<$crate::PodCleanupIntent>> {
                Box::pin(async {
                    Err($crate::PodCleanupIntentError::unavailable(
                        "cleanup intents are not used by this test client",
                    ))
                })
            }

            fn acknowledge_pod_cleanup_intent(
                &self,
                _request: $crate::PodCleanupIntentAckRequest,
            ) -> $crate::PodCleanupIntentFuture<'_, ()> {
                Box::pin(async {
                    Err($crate::PodCleanupIntentError::unavailable(
                        "cleanup-intent acknowledgement is not used by this test client",
                    ))
                })
            }
        }

        $crate::test_support::impl_unavailable_leader_network!($client);
    };
}

pub use crate::__impl_unavailable_leader_pod_effects as impl_unavailable_leader_pod_effects;

#[doc(hidden)]
#[macro_export]
macro_rules! __impl_unavailable_leader_network {
    ($client:ty) => {
        impl $crate::LeaderNodeSubnetAllocation for $client {
            fn allocate_node_subnet(
                &self,
                _request: $crate::NodeSubnetAllocationRequest,
            ) -> $crate::NodeSubnetAllocationFuture<'_, $crate::NodeSubnetAllocationResult> {
                Box::pin(async {
                    Err($crate::NodeSubnetAllocationError::retryable(
                        "network allocation is not used by this test client",
                    ))
                })
            }
        }

        impl $crate::LeaderNetworkTopologyQuery for $client {
            fn get_node_subnet(
                &self,
                _request: $crate::NodeSubnetQuery,
            ) -> $crate::NetworkTopologyFuture<'_, $crate::NodeSubnetResult> {
                Box::pin(async {
                    Err($crate::NetworkTopologyError::retryable(
                        "network topology is not used by this test client",
                    ))
                })
            }

            fn list_peer_subnets(
                &self,
                _request: $crate::PeerSubnetsQuery,
            ) -> $crate::NetworkTopologyFuture<'_, $crate::PeerSubnetsResult> {
                Box::pin(async {
                    Err($crate::NetworkTopologyError::retryable(
                        "network topology is not used by this test client",
                    ))
                })
            }

            fn get_node_dataplane(
                &self,
                _request: $crate::NodeDataplaneQuery,
            ) -> $crate::NetworkTopologyFuture<'_, $crate::NodeDataplaneResult> {
                Box::pin(async {
                    Err($crate::NetworkTopologyError::retryable(
                        "network topology is not used by this test client",
                    ))
                })
            }
        }
    };
}

pub use crate::__impl_unavailable_leader_network as impl_unavailable_leader_network;

#[cfg(test)]
mod tests {
    use super::*;

    struct UnavailableClient;

    impl_unavailable_leader_pod_effects!(UnavailableClient);

    #[tokio::test]
    async fn unavailable_client_preserves_typed_port_failures() {
        let client = UnavailableClient;

        let token_error =
            crate::LeaderProjectedServiceAccountToken::issue_projected_service_account_token(
                &client,
                crate::ProjectedServiceAccountTokenRequest::try_new(
                    "default",
                    "default",
                    vec!["api".to_string()],
                    600,
                    "pod-a",
                    "pod-uid",
                    "node-a",
                    None,
                )
                .expect("valid projected-token request"),
            )
            .await
            .expect_err("portable fake must reject token issuance");
        assert!(matches!(
            token_error,
            crate::ProjectedServiceAccountTokenError::Unavailable { .. }
        ));

        let allocation_error = crate::LeaderNodeSubnetAllocation::allocate_node_subnet(
            &client,
            crate::NodeSubnetAllocationRequest::try_new("node-a", "10.42.0.0/16", "10.0.0.2")
                .expect("valid subnet allocation request"),
        )
        .await
        .expect_err("portable fake must reject network allocation");
        assert!(matches!(
            allocation_error,
            crate::NodeSubnetAllocationError::Retryable { .. }
        ));

        let watch = UnavailableLeaderWatch::new("watch intentionally omitted");
        let watch_result = crate::LeaderWatch::watch_resources(
            &watch,
            crate::WatchRequest::try_new("v1", "Pod", None, None, None, None, None)
                .expect("valid watch request"),
        )
        .await;
        let watch_error = match watch_result {
            Err(error) => error,
            Ok(_) => panic!("portable fake must reject watch setup"),
        };
        assert_eq!(
            watch_error,
            crate::LeaderWatchError::Unavailable {
                message: "watch intentionally omitted".to_string(),
            }
        );
    }
}
