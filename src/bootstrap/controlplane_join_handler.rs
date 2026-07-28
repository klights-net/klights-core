use std::sync::Arc;

use klights_leader_api::{
    ControlplaneJoinAdmission, ControlplaneJoinAuthority, ControlplaneJoinError,
    ControlplaneJoinFuture, ControlplaneJoinHandler, ControlplaneJoinMetadata,
    ControlplaneJoinOutcome, ControlplaneJoinRegistration, ControlplaneJoinRequest,
    ControlplaneJoinRoute, ControlplaneMemberQuery, ControlplaneMemberQueryFuture,
};

/// Neutral coordinator for authenticated control-plane joins.
///
/// Root supplies focused authority, admission, membership-query, registration,
/// and metadata capabilities. The coordinator owns only sequencing and error
/// context, so transport and engine adapters remain independently movable.
pub struct ControlplaneJoinCoordinator {
    authority: Arc<dyn ControlplaneJoinAuthority>,
    admission: Arc<dyn ControlplaneJoinAdmission>,
    member_query: Arc<dyn ControlplaneMemberQuery>,
    registration: Arc<dyn ControlplaneJoinRegistration>,
    metadata: Arc<dyn ControlplaneJoinMetadata>,
}

impl ControlplaneJoinCoordinator {
    pub fn new(
        authority: Arc<dyn ControlplaneJoinAuthority>,
        admission: Arc<dyn ControlplaneJoinAdmission>,
        member_query: Arc<dyn ControlplaneMemberQuery>,
        registration: Arc<dyn ControlplaneJoinRegistration>,
        metadata: Arc<dyn ControlplaneJoinMetadata>,
    ) -> Self {
        Self {
            authority,
            admission,
            member_query,
            registration,
            metadata,
        }
    }
}

pub(crate) fn validate_command_codec_v3_join(command_codec_version: u32) -> Result<(), String> {
    if command_codec_version != klights_cluster_core::COMMAND_CODEC_VERSION {
        return Err(
            "joining voters and learners must advertise exact command codec version 3".to_string(),
        );
    }
    Ok(())
}

impl ControlplaneJoinHandler for ControlplaneJoinCoordinator {
    fn join(&self, request: ControlplaneJoinRequest) -> ControlplaneJoinFuture<'_> {
        Box::pin(async move {
            match self.authority.route() {
                ControlplaneJoinRoute::Local => {}
                ControlplaneJoinRoute::Redirect {
                    leader_id,
                    leader_addr,
                } => {
                    return Ok(ControlplaneJoinOutcome::RedirectToLeader {
                        leader_id,
                        leader_addr,
                    });
                }
                ControlplaneJoinRoute::Unavailable => {
                    return Ok(ControlplaneJoinOutcome::Denied {
                        reason: "no leader currently elected; retry later".to_string(),
                    });
                }
            }
            if let Err(reason) = validate_command_codec_v3_join(request.command_codec_version) {
                return Ok(ControlplaneJoinOutcome::Denied { reason });
            }
            tracing::info!(
                joining_node_id = request.node_id,
                joining_node_name = %request.node_name,
                joining_addr = %request.addr,
                storage_incarnation = %request.storage_incarnation,
                as_learner = request.as_learner,
                "JoinAsControlplane: leader admitting durable consensus storage incarnation"
            );
            let admission = self.admission.admit(&request).await.map_err(|error| {
                ControlplaneJoinError::new(format!(
                    "admit control-plane member {}: {error}",
                    request.node_id
                ))
            })?;
            if admission.changed {
                self.registration
                    .register(&request, admission.voter_count_after)
                    .await
                    .map_err(|error| {
                        ControlplaneJoinError::new(format!(
                            "register joining Node row for {}: {error}",
                            request.node_name
                        ))
                    })?;
                self.metadata
                    .refresh(&request.node_name, request.as_learner)
                    .await
                    .map_err(|error| {
                        ControlplaneJoinError::new(format!(
                            "refresh cluster membership metadata: {error}"
                        ))
                    })?;
            }
            Ok(ControlplaneJoinOutcome::Accepted {
                voter_count_after: admission.voter_count_after,
                admitted_as_learner: request.as_learner,
                ca_cert_pem: String::new(),
                encrypted_ca_key: Vec::new(),
                ca_key_nonce: [0u8; 12],
            })
        })
    }

    fn is_controlplane_member<'a>(
        &'a self,
        node_name: &'a str,
    ) -> ControlplaneMemberQueryFuture<'a> {
        self.member_query.is_controlplane_member(node_name)
    }
}

#[cfg(test)]
mod coordinator_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::ControlplaneJoinCoordinator;
    use klights_leader_api::{
        ControlplaneJoinAdmission, ControlplaneJoinAdmissionFuture,
        ControlplaneJoinAdmissionOutcome, ControlplaneJoinAuthority, ControlplaneJoinHandler,
        ControlplaneJoinMetadata, ControlplaneJoinMetadataFuture, ControlplaneJoinOutcome,
        ControlplaneJoinRegistration, ControlplaneJoinRegistrationFuture, ControlplaneJoinRequest,
        ControlplaneJoinRoute, ControlplaneMemberQuery, ControlplaneMemberQueryFuture,
        RaftStorageAttestation,
    };

    struct FakeAuthority {
        route: ControlplaneJoinRoute,
    }

    impl ControlplaneJoinAuthority for FakeAuthority {
        fn route(&self) -> ControlplaneJoinRoute {
            self.route.clone()
        }
    }

    struct FakeAdmission {
        calls: AtomicUsize,
        outcome: ControlplaneJoinAdmissionOutcome,
    }

    impl ControlplaneJoinAdmission for FakeAdmission {
        fn admit<'a>(
            &'a self,
            _request: &'a ControlplaneJoinRequest,
        ) -> ControlplaneJoinAdmissionFuture<'a> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(self.outcome)
            })
        }
    }

    #[derive(Default)]
    struct FakeMemberQuery;

    impl ControlplaneMemberQuery for FakeMemberQuery {
        fn is_controlplane_member<'a>(
            &'a self,
            _node_name: &'a str,
        ) -> ControlplaneMemberQueryFuture<'a> {
            Box::pin(async { false })
        }
    }

    #[derive(Default)]
    struct FakeRegistration {
        calls: AtomicUsize,
    }

    impl ControlplaneJoinRegistration for FakeRegistration {
        fn register<'a>(
            &'a self,
            _request: &'a ControlplaneJoinRequest,
            _voter_count_after: u32,
        ) -> ControlplaneJoinRegistrationFuture<'a> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    #[derive(Default)]
    struct FakeMetadata {
        calls: AtomicUsize,
    }

    impl ControlplaneJoinMetadata for FakeMetadata {
        fn refresh<'a>(
            &'a self,
            _node_name: &'a str,
            _as_learner: bool,
        ) -> ControlplaneJoinMetadataFuture<'a> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    fn request() -> ControlplaneJoinRequest {
        ControlplaneJoinRequest {
            node_id: 2,
            addr: "https://10.0.0.2:7679".to_string(),
            node_name: "cp2".to_string(),
            as_learner: false,
            storage_incarnation: uuid::Uuid::new_v4().to_string(),
            storage_log_attestation: RaftStorageAttestation {
                high_watermark: None,
                current_boundary: None,
            },
            command_codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
            node_internal_ip: Some("10.0.0.2".to_string()),
            node_registration: None,
            legacy_node_git_commit: None,
        }
    }

    #[tokio::test]
    async fn changed_admission_runs_registration_and_metadata_once() {
        let admission = Arc::new(FakeAdmission {
            calls: AtomicUsize::new(0),
            outcome: ControlplaneJoinAdmissionOutcome {
                changed: true,
                voter_count_after: 3,
            },
        });
        let registration = Arc::new(FakeRegistration::default());
        let metadata = Arc::new(FakeMetadata::default());
        let coordinator = ControlplaneJoinCoordinator::new(
            Arc::new(FakeAuthority {
                route: ControlplaneJoinRoute::Local,
            }),
            admission.clone(),
            Arc::new(FakeMemberQuery),
            registration.clone(),
            metadata.clone(),
        );

        let outcome = coordinator.join(request()).await.unwrap();
        assert!(matches!(
            outcome,
            ControlplaneJoinOutcome::Accepted {
                voter_count_after: 3,
                admitted_as_learner: false,
                ..
            }
        ));
        assert_eq!(admission.calls.load(Ordering::SeqCst), 1);
        assert_eq!(registration.calls.load(Ordering::SeqCst), 1);
        assert_eq!(metadata.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn unchanged_admission_skips_registration_and_metadata() {
        let registration = Arc::new(FakeRegistration::default());
        let metadata = Arc::new(FakeMetadata::default());
        let coordinator = ControlplaneJoinCoordinator::new(
            Arc::new(FakeAuthority {
                route: ControlplaneJoinRoute::Local,
            }),
            Arc::new(FakeAdmission {
                calls: AtomicUsize::new(0),
                outcome: ControlplaneJoinAdmissionOutcome {
                    changed: false,
                    voter_count_after: 2,
                },
            }),
            Arc::new(FakeMemberQuery),
            registration.clone(),
            metadata.clone(),
        );

        let outcome = coordinator.join(request()).await.unwrap();
        assert!(matches!(
            outcome,
            ControlplaneJoinOutcome::Accepted {
                voter_count_after: 2,
                ..
            }
        ));
        assert_eq!(registration.calls.load(Ordering::SeqCst), 0);
        assert_eq!(metadata.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn redirect_short_circuits_admission_and_effects() {
        let admission = Arc::new(FakeAdmission {
            calls: AtomicUsize::new(0),
            outcome: ControlplaneJoinAdmissionOutcome {
                changed: true,
                voter_count_after: 3,
            },
        });
        let registration = Arc::new(FakeRegistration::default());
        let metadata = Arc::new(FakeMetadata::default());
        let coordinator = ControlplaneJoinCoordinator::new(
            Arc::new(FakeAuthority {
                route: ControlplaneJoinRoute::Redirect {
                    leader_id: 1,
                    leader_addr: "https://10.0.0.1:7679".to_string(),
                },
            }),
            admission.clone(),
            Arc::new(FakeMemberQuery),
            registration.clone(),
            metadata.clone(),
        );

        let outcome = coordinator.join(request()).await.unwrap();
        assert!(matches!(
            outcome,
            ControlplaneJoinOutcome::RedirectToLeader { leader_id: 1, .. }
        ));
        assert_eq!(admission.calls.load(Ordering::SeqCst), 0);
        assert_eq!(registration.calls.load(Ordering::SeqCst), 0);
        assert_eq!(metadata.calls.load(Ordering::SeqCst), 0);
    }
}
