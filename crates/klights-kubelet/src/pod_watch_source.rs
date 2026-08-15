//! Kubelet-owned positioned Pod-manager watch contracts.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use klights_cluster_core::WatchReplayPosition;
use klights_leader_api::{
    LeaderWatch, LeaderWatchError, ResourceListScope, WatchEventType, WatchRequest,
    WatchResumeCursor,
};
use serde_json::Value;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PodWatchScope {
    Pod,
    PersistentVolumeClaim,
    PersistentVolume,
    Secret,
    ConfigMap,
    Namespace,
}

impl PodWatchScope {
    pub const ALL: [Self; 6] = [
        Self::Pod,
        Self::PersistentVolumeClaim,
        Self::PersistentVolume,
        Self::Secret,
        Self::ConfigMap,
        Self::Namespace,
    ];

    const fn index(self) -> usize {
        self as usize
    }
}

#[derive(Clone, Debug, Default)]
pub struct PodWatchCheckpoint {
    cursors: [Option<WatchResumeCursor>; 6],
}

impl PodWatchCheckpoint {
    pub fn cursor_for(&self, scope: PodWatchScope) -> Option<WatchResumeCursor> {
        self.cursors[scope.index()]
    }

    pub fn accept_open_cursor(&mut self, scope: PodWatchScope, cursor: WatchResumeCursor) {
        if self.cursors[scope.index()].is_none() {
            self.cursors[scope.index()] = Some(cursor);
        }
    }

    pub fn advance_after_apply(
        &mut self,
        scope: PodWatchScope,
        resource_version: i64,
        replay_position: Option<WatchReplayPosition>,
    ) -> Result<(), LeaderWatchError> {
        if let (Some(current), Some(delivered)) = (
            self.cursor_for(scope)
                .and_then(|cursor| cursor.replay_position()),
            replay_position,
        ) && !current.permits_successor(delivered)
        {
            return Err(LeaderWatchError::OutOfOrderEvent {
                current_event_id: current.event_id,
                delivered_event_id: delivered.event_id,
            });
        }
        let previous_rv = self
            .cursor_for(scope)
            .and_then(|cursor| cursor.resource_version())
            .unwrap_or_default();
        self.cursors[scope.index()] = Some(WatchResumeCursor::try_new(
            Some(previous_rv.max(resource_version)),
            replay_position,
        )?);
        Ok(())
    }

    pub fn recovery_plan(&self, disconnect: PodWatchDisconnect) -> PodWatchRecoveryPlan {
        let mut checkpoint = self.clone();
        let mut relist = [false; 6];
        if let PodWatchDisconnect::ReplayExpired(scope) = disconnect {
            checkpoint.cursors[scope.index()] = None;
            relist[scope.index()] = true;
        }
        PodWatchRecoveryPlan { checkpoint, relist }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PodWatchDisconnect {
    EndOfStream,
    Failed(PodWatchScope),
    ReplayExpired(PodWatchScope),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PodWatchRecoveryMode {
    Resume,
    Relist,
}

#[derive(Clone, Debug, Default)]
pub struct PodWatchRecoveryPlan {
    checkpoint: PodWatchCheckpoint,
    relist: [bool; 6],
}

impl PodWatchRecoveryPlan {
    pub fn initial() -> Self {
        Self::default()
    }

    pub fn mode(&self) -> PodWatchRecoveryMode {
        if self.relist.iter().any(|relist| *relist) {
            PodWatchRecoveryMode::Relist
        } else {
            PodWatchRecoveryMode::Resume
        }
    }

    pub fn cursor_for(&self, scope: PodWatchScope) -> Option<WatchResumeCursor> {
        self.checkpoint.cursor_for(scope)
    }

    pub fn must_relist(&self, scope: PodWatchScope) -> bool {
        self.relist[scope.index()]
    }
}

#[derive(Clone, Debug)]
pub struct PodWatchEvent {
    pub scope: PodWatchScope,
    pub event_type: WatchEventType,
    pub object: Arc<Value>,
    pub resume_position: Option<WatchReplayPosition>,
}

impl PodWatchEvent {
    pub fn from_resource_event(
        scope: PodWatchScope,
        event: klights_leader_api::ResourceEvent,
    ) -> Self {
        let (event_type, resource, resume_position) = event.into_parts();
        Self {
            scope,
            event_type,
            object: resource.data,
            resume_position,
        }
    }

    pub fn resource_version(&self) -> Option<i64> {
        self.object
            .pointer("/metadata/resourceVersion")
            .and_then(Value::as_str)
            .and_then(|value| value.parse().ok())
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn added(object: Value) -> Self {
        Self {
            scope: PodWatchScope::Pod,
            event_type: WatchEventType::Added,
            object: Arc::new(object),
            resume_position: None,
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn modified(object: Value) -> Self {
        Self {
            scope: PodWatchScope::Pod,
            event_type: WatchEventType::Modified,
            object: Arc::new(object),
            resume_position: None,
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn deleted(object: Value) -> Self {
        Self {
            scope: PodWatchScope::Pod,
            event_type: WatchEventType::Deleted,
            object: Arc::new(object),
            resume_position: None,
        }
    }
}

#[derive(Debug)]
pub struct PodWatchStreamError {
    pub scope: PodWatchScope,
    pub source: LeaderWatchError,
}

pub type PodWatchStream =
    Pin<Box<dyn Stream<Item = Result<PodWatchEvent, PodWatchStreamError>> + Send + 'static>>;

pub type PodWatchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<PodWatchSession, LeaderWatchError>> + Send + 'a>>;

pub struct PodWatchSession {
    pub stream: PodWatchStream,
    pub checkpoint: PodWatchCheckpoint,
}

pub fn scope_watch_stream(
    scope: PodWatchScope,
    stream: klights_leader_api::WatchStream,
) -> PodWatchStream {
    use futures::StreamExt as _;

    let events = stream.map(move |event| {
        event
            .map(|event| PodWatchEvent::from_resource_event(scope, event))
            .map_err(|source| PodWatchStreamError { scope, source })
    });
    let closed = futures::stream::once(async move {
        Err(PodWatchStreamError {
            scope,
            source: LeaderWatchError::transport(format!(
                "{scope:?} positioned watch stream closed"
            )),
        })
    });
    Box::pin(events.chain(closed))
}

pub trait PodWatchSource: Send + Sync {
    fn open_pod_manager_watch(
        &self,
        node_name: String,
        recovery: PodWatchRecoveryPlan,
    ) -> PodWatchFuture<'_>;
}

/// Leader-backed positioned watch source for the kubelet Pod manager.
///
/// The kubelet owns the exact resource scopes and per-scope recovery policy;
/// root bootstrap supplies only the concrete leader-watch implementation.
pub struct LeaderPodWatchSource {
    leader_watch: Arc<dyn LeaderWatch>,
}

impl LeaderPodWatchSource {
    pub fn new(leader_watch: Arc<dyn LeaderWatch>) -> Self {
        Self { leader_watch }
    }
}

impl PodWatchSource for LeaderPodWatchSource {
    fn open_pod_manager_watch(
        &self,
        node_name: String,
        recovery: PodWatchRecoveryPlan,
    ) -> PodWatchFuture<'_> {
        Box::pin(async move {
            let requests = [
                (
                    PodWatchScope::Pod,
                    WatchRequest::try_new_with_scope(
                        "v1",
                        "Pod",
                        None,
                        ResourceListScope::AllNamespaces,
                        None,
                        Some(format!("spec.nodeName={node_name}")),
                        None,
                        None,
                    )?,
                ),
                (
                    PodWatchScope::PersistentVolumeClaim,
                    WatchRequest::try_new_with_scope(
                        "v1",
                        "PersistentVolumeClaim",
                        None,
                        ResourceListScope::AllNamespaces,
                        None,
                        None,
                        None,
                        None,
                    )?,
                ),
                (
                    PodWatchScope::PersistentVolume,
                    WatchRequest::try_new_with_scope(
                        "v1",
                        "PersistentVolume",
                        None,
                        ResourceListScope::Cluster,
                        None,
                        None,
                        None,
                        None,
                    )?,
                ),
                (
                    PodWatchScope::Secret,
                    WatchRequest::try_new_with_scope(
                        "v1",
                        "Secret",
                        None,
                        ResourceListScope::AllNamespaces,
                        None,
                        None,
                        None,
                        None,
                    )?,
                ),
                (
                    PodWatchScope::ConfigMap,
                    WatchRequest::try_new_with_scope(
                        "v1",
                        "ConfigMap",
                        None,
                        ResourceListScope::AllNamespaces,
                        None,
                        None,
                        None,
                        None,
                    )?,
                ),
                (
                    PodWatchScope::Namespace,
                    WatchRequest::try_new_with_scope(
                        "v1",
                        "Namespace",
                        None,
                        ResourceListScope::Cluster,
                        None,
                        None,
                        None,
                        None,
                    )?,
                ),
            ];
            let mut streams = Vec::with_capacity(requests.len());
            let mut checkpoint = PodWatchCheckpoint::default();
            for (scope, request) in requests {
                // A typed replay expiry deliberately omits only that scope's
                // cursor. The focused LeaderWatch implementation then invokes
                // its authoritative fresh establishment/relist kernel.
                let request = if recovery.must_relist(scope) {
                    request
                } else if let Some(cursor) = recovery.cursor_for(scope) {
                    request.with_resume_cursor(cursor)?
                } else {
                    request
                };
                let stream = self.leader_watch.watch_resources(request).await?;
                if let Some(cursor) = stream.accepted_cursor() {
                    checkpoint.accept_open_cursor(scope, cursor);
                } else if let Some(cursor) = recovery.cursor_for(scope) {
                    checkpoint.accept_open_cursor(scope, cursor);
                }
                streams.push(scope_watch_stream(scope, stream));
            }
            Ok(PodWatchSession {
                stream: Box::pin(futures::stream::select_all(streams)) as PodWatchStream,
                checkpoint,
            })
        })
    }
}

#[cfg(test)]
mod reconnect_contract_tests {
    use super::*;
    use klights_cluster_core::WatchReplayPosition;

    struct RecordingLeaderWatch(std::sync::Mutex<Vec<WatchRequest>>);

    impl LeaderWatch for RecordingLeaderWatch {
        fn watch_resources(
            &self,
            request: WatchRequest,
        ) -> klights_leader_api::LeaderWatchFuture<'_> {
            self.0.lock().expect("watch request mutex").push(request);
            Box::pin(async {
                Ok(klights_leader_api::WatchStream::unpositioned_test_stream(
                    futures::stream::empty(),
                ))
            })
        }
    }

    #[test]
    fn leader_pod_watch_source_is_owned_by_kubelet() {
        fn assert_source(_: &LeaderPodWatchSource) {}
        let _ = assert_source;
    }

    #[tokio::test]
    async fn leader_pod_watch_source_opens_the_exact_kubelet_scope_set() {
        let leader = Arc::new(RecordingLeaderWatch(std::sync::Mutex::new(Vec::new())));
        let source = LeaderPodWatchSource::new(leader.clone());
        source
            .open_pod_manager_watch("node-a".to_string(), PodWatchRecoveryPlan::initial())
            .await
            .expect("open kubelet watch scopes");

        let requests = leader.0.lock().expect("watch request mutex");
        let actual = requests
            .iter()
            .map(|request| {
                (
                    request.kind(),
                    request.scope().clone(),
                    request.field_selector(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            vec![
                (
                    "Pod",
                    ResourceListScope::AllNamespaces,
                    Some("spec.nodeName=node-a")
                ),
                (
                    "PersistentVolumeClaim",
                    ResourceListScope::AllNamespaces,
                    None
                ),
                ("PersistentVolume", ResourceListScope::Cluster, None),
                ("Secret", ResourceListScope::AllNamespaces, None),
                ("ConfigMap", ResourceListScope::AllNamespaces, None),
                ("Namespace", ResourceListScope::Cluster, None),
            ]
        );
    }

    #[test]
    fn bootstrap_eof_reconnects_from_each_scopes_last_durable_position() {
        let pod_position = WatchReplayPosition {
            resource_version: 41,
            event_id: 101,
            resource_version_filter_through_event_id: 0,
        };
        let config_map_position = WatchReplayPosition {
            resource_version: 39,
            event_id: 99,
            resource_version_filter_through_event_id: 0,
        };
        let mut checkpoint = PodWatchCheckpoint::default();
        checkpoint
            .advance_after_apply(PodWatchScope::Pod, 41, Some(pod_position))
            .expect("pod checkpoint");
        checkpoint
            .advance_after_apply(PodWatchScope::ConfigMap, 39, Some(config_map_position))
            .expect("ConfigMap checkpoint");

        let plan = checkpoint.recovery_plan(PodWatchDisconnect::EndOfStream);

        assert_eq!(plan.mode(), PodWatchRecoveryMode::Resume);
        assert_eq!(
            plan.cursor_for(PodWatchScope::Pod)
                .and_then(|cursor| cursor.replay_position()),
            Some(pod_position)
        );
        assert_eq!(
            plan.cursor_for(PodWatchScope::ConfigMap)
                .and_then(|cursor| cursor.replay_position()),
            Some(config_map_position)
        );
    }

    #[test]
    fn typed_replay_expiry_relists_only_the_expired_scope() {
        let mut checkpoint = PodWatchCheckpoint::default();
        checkpoint
            .advance_after_apply(
                PodWatchScope::Pod,
                41,
                Some(WatchReplayPosition {
                    resource_version: 41,
                    event_id: 101,
                    resource_version_filter_through_event_id: 0,
                }),
            )
            .expect("pod checkpoint");
        checkpoint
            .advance_after_apply(
                PodWatchScope::Secret,
                40,
                Some(WatchReplayPosition {
                    resource_version: 40,
                    event_id: 100,
                    resource_version_filter_through_event_id: 0,
                }),
            )
            .expect("Secret checkpoint");

        let plan = checkpoint.recovery_plan(PodWatchDisconnect::ReplayExpired(PodWatchScope::Pod));

        assert_eq!(plan.mode(), PodWatchRecoveryMode::Relist);
        assert!(plan.must_relist(PodWatchScope::Pod));
        assert!(!plan.must_relist(PodWatchScope::Secret));
        assert!(
            plan.cursor_for(PodWatchScope::Pod).is_none(),
            "an expired exact cursor must never be silently reused or reset as a lenient resume"
        );
        assert!(
            plan.cursor_for(PodWatchScope::Secret).is_some(),
            "unaffected scopes must preserve their exact durable checkpoint"
        );
    }

    #[tokio::test]
    async fn one_scope_eof_is_observable_before_other_scopes_can_mask_it() {
        use futures::StreamExt as _;

        let empty =
            klights_leader_api::WatchStream::unpositioned_test_stream(futures::stream::empty());
        let mut stream = scope_watch_stream(PodWatchScope::ConfigMap, empty);
        let error = stream
            .next()
            .await
            .expect("scope EOF must become a reconnect signal")
            .expect_err("scope EOF is not a resource event");

        assert_eq!(error.scope, PodWatchScope::ConfigMap);
        assert!(matches!(error.source, LeaderWatchError::Transport { .. }));
    }
}
