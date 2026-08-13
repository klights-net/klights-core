#[cfg(test)]
mod tests {
    use super::super::assembly_support::support::*;

    #[tokio::test]
    async fn read_pod_network_assignment_returns_assigned_ip() {
        let repo =
            super::super::assembly_support::support::IntegrationPodNetworkFixture::node_local_with_waiter(
                Arc::new(klights_networking::PodNetworkAssignmentBus::new()),
            )
            .await;

        repo.reserve_assignment(
            "sandbox-net-1",
            "p-net",
            "uid-1",
            "vethXYZ",
            "/var/run/netns/cni-1",
        )
        .await
        .unwrap();
        let assignment = repo
            .read_assignment("sandbox-net-1", "default", "p-net", "uid-1", false)
            .await
            .unwrap();
        assert_eq!(assignment.pod_ip, "10.42.0.2");
    }
    #[tokio::test]
    async fn read_pod_network_assignment_falls_back_to_pod_identity() {
        struct UidFallbackCache;

        impl klights_node_store::PodNetworkCache for UidFallbackCache {
            fn get_network_for_uid(
                &self,
                pod_uid: klights_node_store::PodUidKey,
            ) -> klights_node_store::CacheNetworkFuture<
                '_,
                Option<klights_node_store::PodNetworkEndpoint>,
            > {
                Box::pin(async move {
                    assert_eq!(pod_uid.as_str(), "uid-1");
                    Ok(Some(
                        klights_node_store::PodNetworkEndpoint::try_new(
                            "10.42.0.43",
                            "vethXYZ",
                            "/var/run/netns/cni-1",
                        )
                        .unwrap(),
                    ))
                })
            }

            fn get_network_for_pod(
                &self,
                pod: klights_types::PodIdentity,
            ) -> klights_node_store::CacheNetworkFuture<
                '_,
                Option<klights_node_store::PodNetworkEndpoint>,
            > {
                Box::pin(async move {
                    assert_eq!(
                        pod,
                        klights_types::PodIdentity::new("default", "p-net", "uid-1")
                    );
                    Ok(Some(
                        klights_node_store::PodNetworkEndpoint::try_new(
                            "10.42.0.43",
                            "vethXYZ",
                            "/var/run/netns/cni-1",
                        )
                        .unwrap(),
                    ))
                })
            }

            fn get_network_for_sandbox(
                &self,
                sandbox_id: klights_node_store::SandboxKey,
            ) -> klights_node_store::CacheNetworkFuture<
                '_,
                Option<klights_node_store::PodNetworkEndpoint>,
            > {
                Box::pin(async move {
                    assert_eq!(sandbox_id.as_str(), "runtime-sandbox-id");
                    Ok(None)
                })
            }

            fn get_network_for_assignment(
                &self,
                sandbox_id: klights_node_store::SandboxKey,
                pod: klights_types::PodIdentity,
            ) -> klights_node_store::CacheNetworkFuture<
                '_,
                Option<klights_node_store::PodNetworkEndpoint>,
            > {
                Box::pin(async move {
                    assert_eq!(sandbox_id.as_str(), "runtime-sandbox-id");
                    assert_eq!(
                        pod,
                        klights_types::PodIdentity::new("default", "p-net", "uid-1")
                    );
                    Ok(None)
                })
            }

            fn delete_network_for_sandbox(
                &self,
                _sandbox_id: klights_node_store::SandboxKey,
            ) -> klights_node_store::CacheNetworkFuture<'_, ()> {
                Box::pin(async { unreachable!("read-only consumer") })
            }

            fn delete_network_if_matches(
                &self,
                _request: klights_node_store::PodNetworkAllocationRequest,
            ) -> klights_node_store::CacheNetworkFuture<'_, bool> {
                Box::pin(async { unreachable!("read-only consumer") })
            }

            fn list_network_assignments(
                &self,
            ) -> klights_node_store::CacheNetworkFuture<
                '_,
                Vec<klights_node_store::PodNetworkAssignmentSnapshot>,
            > {
                Box::pin(async { unreachable!("read-only consumer") })
            }
        }

        let service =
            super::super::assembly_support::support::IntegrationPodNetworkFixture::with_cache_and_waiter(
                std::sync::Arc::new(UidFallbackCache),
                Arc::new(klights_networking::PodNetworkAssignmentBus::new()),
            );
        let assignment = service
            .read_assignment("runtime-sandbox-id", "default", "p-net", "uid-1", false)
            .await
            .unwrap();
        assert_eq!(assignment.pod_ip, "10.42.0.43");
    }
    #[tokio::test]
    async fn read_pod_network_assignment_host_network_returns_host_ip_twice_without_db() {
        let repo = build_network_repo().await;
        // No row inserted; host_network=true must not consult the DB.
        let assignment = repo
            .read_pod_network_assignment(
                "does-not-exist-sandbox",
                "default",
                "hostnet",
                "uid-host",
                true,
            )
            .await
            .unwrap();
        assert_eq!(assignment.pod_ip, assignment.host_ip);
        assert!(!assignment.pod_ip.is_empty());
    }
    #[tokio::test]
    async fn read_pod_network_assignment_retries_then_succeeds() {
        use klights_network_api::{PodNetworkAssignmentKey, PodNetworkAssignmentPublisher};

        let (events, mut registered) = RegistrationSignalingAssignmentBus::new();
        let repo = std::sync::Arc::new(
            super::super::assembly_support::support::IntegrationPodNetworkFixture::node_local_with_waiter(
                events.clone(),
            )
            .await,
        );

        let key = PodNetworkAssignmentKey::try_new(
            "sandbox-net-late",
            "default",
            "p-net-late",
            "uid-late",
        )
        .unwrap();
        let repo_clone = repo.clone();
        let read_handle = tokio::spawn(async move {
            repo_clone
                .read_assignment(
                    "sandbox-net-late",
                    "default",
                    "p-net-late",
                    "uid-late",
                    false,
                )
                .await
        });

        registered.changed().await.unwrap();
        repo.reserve_assignment(
            "sandbox-net-late",
            "p-net-late",
            "uid-late",
            "vethL",
            "/var/run/netns/cni-late",
        )
        .await
        .unwrap();
        events.publish_assignment(&key);

        let assignment = read_handle.await.unwrap().unwrap();
        assert_eq!(assignment.pod_ip, "10.42.0.2");
    }
    #[tokio::test]
    async fn read_pod_network_assignment_retains_publish_inside_first_lookup_gap() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use klights_network_api::{PodNetworkAssignmentKey, PodNetworkAssignmentPublisher};

        struct PublishInsideLookupCache {
            bus: std::sync::Arc<klights_networking::PodNetworkAssignmentBus>,
            key: PodNetworkAssignmentKey,
            sandbox_reads: AtomicUsize,
        }

        impl klights_node_store::PodNetworkCache for PublishInsideLookupCache {
            fn get_network_for_uid(
                &self,
                _pod_uid: klights_node_store::PodUidKey,
            ) -> klights_node_store::CacheNetworkFuture<
                '_,
                Option<klights_node_store::PodNetworkEndpoint>,
            > {
                Box::pin(async { Ok(None) })
            }

            fn get_network_for_pod(
                &self,
                _pod: klights_types::PodIdentity,
            ) -> klights_node_store::CacheNetworkFuture<
                '_,
                Option<klights_node_store::PodNetworkEndpoint>,
            > {
                Box::pin(async { Ok(None) })
            }

            fn get_network_for_sandbox(
                &self,
                _sandbox_id: klights_node_store::SandboxKey,
            ) -> klights_node_store::CacheNetworkFuture<
                '_,
                Option<klights_node_store::PodNetworkEndpoint>,
            > {
                Box::pin(async move {
                    if self.sandbox_reads.fetch_add(1, Ordering::SeqCst) == 0 {
                        self.bus.publish_assignment(&self.key);
                        Ok(None)
                    } else {
                        Ok(Some(
                            klights_node_store::PodNetworkEndpoint::try_new(
                                "10.42.0.101",
                                "veth-gap",
                                "/run/netns/gap",
                            )
                            .unwrap(),
                        ))
                    }
                })
            }

            fn get_network_for_assignment(
                &self,
                _sandbox_id: klights_node_store::SandboxKey,
                _pod: klights_types::PodIdentity,
            ) -> klights_node_store::CacheNetworkFuture<
                '_,
                Option<klights_node_store::PodNetworkEndpoint>,
            > {
                Box::pin(async move {
                    if self.sandbox_reads.fetch_add(1, Ordering::SeqCst) == 0 {
                        self.bus.publish_assignment(&self.key);
                        Ok(None)
                    } else {
                        Ok(Some(
                            klights_node_store::PodNetworkEndpoint::try_new(
                                "10.42.0.101",
                                "veth-gap",
                                "/run/netns/gap",
                            )
                            .unwrap(),
                        ))
                    }
                })
            }

            fn delete_network_for_sandbox(
                &self,
                _sandbox_id: klights_node_store::SandboxKey,
            ) -> klights_node_store::CacheNetworkFuture<'_, ()> {
                Box::pin(async { unreachable!("read-only consumer") })
            }

            fn delete_network_if_matches(
                &self,
                _request: klights_node_store::PodNetworkAllocationRequest,
            ) -> klights_node_store::CacheNetworkFuture<'_, bool> {
                Box::pin(async { unreachable!("read-only consumer") })
            }

            fn list_network_assignments(
                &self,
            ) -> klights_node_store::CacheNetworkFuture<
                '_,
                Vec<klights_node_store::PodNetworkAssignmentSnapshot>,
            > {
                Box::pin(async { unreachable!("read-only consumer") })
            }
        }

        let key = PodNetworkAssignmentKey::try_new("sandbox-gap", "default", "pod-gap", "uid-gap")
            .unwrap();
        let bus = Arc::new(klights_networking::PodNetworkAssignmentBus::new());
        let cache = std::sync::Arc::new(PublishInsideLookupCache {
            bus: bus.clone(),
            key,
            sandbox_reads: AtomicUsize::new(0),
        });
        let service =
            super::super::assembly_support::support::IntegrationPodNetworkFixture::with_cache_and_waiter(
                cache.clone(),
                bus,
            );

        let assignment = service
            .read_assignment("sandbox-gap", "default", "pod-gap", "uid-gap", false)
            .await
            .unwrap();

        assert_eq!(assignment.pod_ip, "10.42.0.101");
        assert_eq!(cache.sandbox_reads.load(Ordering::SeqCst), 2);
    }
    #[tokio::test]
    async fn read_pod_network_assignment_tolerates_cni_db_backlog() {
        use klights_network_api::{PodNetworkAssignmentKey, PodNetworkAssignmentPublisher};

        let (events, mut registered) = RegistrationSignalingAssignmentBus::new();
        let repo = std::sync::Arc::new(
            super::super::assembly_support::support::IntegrationPodNetworkFixture::node_local_with_waiter(
                events.clone(),
            )
            .await,
        );

        let key = PodNetworkAssignmentKey::try_new(
            "sandbox-net-backlogged",
            "default",
            "p-net-backlogged",
            "uid-backlogged",
        )
        .unwrap();
        let repo_clone = repo.clone();
        let read_handle = tokio::spawn(async move {
            repo_clone
                .read_assignment(
                    "sandbox-net-backlogged",
                    "default",
                    "p-net-backlogged",
                    "uid-backlogged",
                    false,
                )
                .await
        });

        registered.changed().await.unwrap();
        // Full conformance can queue DB work around RunPodSandbox; the reader must
        // stay parked on the event rather than burning retry sleeps.
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        repo.reserve_assignment(
            "sandbox-net-backlogged",
            "p-net-backlogged",
            "uid-backlogged",
            "vethB",
            "/var/run/netns/cni-backlogged",
        )
        .await
        .unwrap();
        events.publish_assignment(&key);

        let assignment = read_handle.await.unwrap().unwrap();
        assert_eq!(assignment.pod_ip, "10.42.0.2");
    }
    #[test]
    fn read_pod_network_assignment_waits_for_assignment_notification() {
        // R4: invariant now enforced by check_kubelet_invariants.sh
    }
    #[tokio::test]
    async fn read_pod_network_assignment_exhausts_retries_returns_error() {
        let repo = build_network_repo().await;
        let err = repo
            .read_pod_network_assignment(
                "nonexistent-sandbox",
                "default",
                "p-net-missing",
                "uid-missing",
                false,
            )
            .await
            .expect_err("missing row must error after bounded assignment wait");
        let msg = err.to_string();
        assert!(
            msg.contains("nonexistent-sandbox") && msg.contains("timed out"),
            "expected assignment wait timeout message, got {msg:?}"
        );
    }
}
