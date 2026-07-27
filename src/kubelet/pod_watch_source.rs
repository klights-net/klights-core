use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use klights_cluster_core::WatchReplayPosition;
use klights_leader_api::{LeaderWatchError, WatchEventType, WatchResumeCursor};
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

    #[cfg(test)]
    pub fn added(object: Value) -> Self {
        Self {
            scope: PodWatchScope::Pod,
            event_type: WatchEventType::Added,
            object: Arc::new(object),
            resume_position: None,
        }
    }

    #[cfg(test)]
    pub fn modified(object: Value) -> Self {
        Self {
            scope: PodWatchScope::Pod,
            event_type: WatchEventType::Modified,
            object: Arc::new(object),
            resume_position: None,
        }
    }

    #[cfg(test)]
    pub fn deleted(object: Value) -> Self {
        Self {
            scope: PodWatchScope::Pod,
            event_type: WatchEventType::Deleted,
            object: Arc::new(object),
            resume_position: None,
        }
    }
}

#[cfg(test)]
impl From<crate::watch::WatchEvent> for PodWatchEvent {
    fn from(event: crate::watch::WatchEvent) -> Self {
        let event_type = match event.event_type {
            crate::watch::EventType::Added => WatchEventType::Added,
            crate::watch::EventType::Modified => WatchEventType::Modified,
            crate::watch::EventType::Deleted => WatchEventType::Deleted,
            crate::watch::EventType::Bookmark => WatchEventType::Bookmark,
            crate::watch::EventType::Error => WatchEventType::Error,
        };
        Self {
            scope: PodWatchScope::Pod,
            event_type,
            object: event.object,
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

#[cfg(test)]
mod reconnect_contract_tests {
    use super::*;
    use klights_cluster_core::WatchReplayPosition;

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
