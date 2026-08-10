use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Result, anyhow};
use futures::StreamExt as _;
use klights_leader_api::{
    CacheReadinessError, CacheReadinessRequest, LeaderWatchError, ResourceEvent,
    ResourceListRequest, WatchRequest, WatchResumeCursor, WatchStream,
};
use klights_supervisor::{SupervisedJoinHandle, TaskCategory, TaskSupervisor};
use klights_watch::RemoteInformerCache;
use tokio_util::sync::CancellationToken;

use super::{ReplicationGrpcClient, resource};

/// A quiet healthy stream receives a heartbeat within this window.
pub const WATCH_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

enum IdleNext {
    Item(std::result::Result<ResourceEvent, LeaderWatchError>),
    Closed,
    Idle,
}

async fn next_event_within_idle(
    supervisor: Option<&Arc<TaskSupervisor>>,
    idle: std::time::Duration,
    stream: &mut WatchStream,
) -> IdleNext {
    let Some(supervisor) = supervisor else {
        return match stream.next().await {
            Some(item) => IdleNext::Item(item),
            None => IdleNext::Closed,
        };
    };
    match supervisor
        .timeout("remote_watch_idle", idle, stream.next())
        .await
    {
        Ok(Ok(Some(item))) => IdleNext::Item(item),
        Ok(Ok(None)) => IdleNext::Closed,
        Ok(Err(_elapsed)) => IdleNext::Idle,
        Err(_shutdown) => IdleNext::Closed,
    }
}

pub fn watch_error_requires_relist(error: &LeaderWatchError) -> bool {
    matches!(error, LeaderWatchError::ReplayExpired { .. })
}

pub async fn watch_resources(
    grpc: Option<&Arc<ReplicationGrpcClient>>,
    request: WatchRequest,
) -> Result<WatchStream, LeaderWatchError> {
    grpc.ok_or_else(|| LeaderWatchError::unavailable("RemoteApiClient missing gRPC transport"))?
        .watch_resources_rpc(request)
        .await
}

pub async fn wait_cache_ready(
    has_transport: bool,
    cache: &dyn RemoteInformerCache,
    scope: CacheReadinessRequest,
) -> Result<(), CacheReadinessError> {
    if !has_transport && !cache.is_ready(&scope).await {
        return Err(CacheReadinessError::unavailable(format!(
            "cache scope {scope:?} not yet primed"
        )));
    }
    cache.wait_ready(scope).await;
    Ok(())
}

async fn sleep_before_reconnect(supervisor: Option<&Arc<TaskSupervisor>>, attempt: u32) {
    if let Some(supervisor) = supervisor {
        let _ = supervisor
            .sleep(
                "remote_api_informer_reconnect",
                klights_supervisor::reconnect_backoff::delay(attempt),
            )
            .await;
    }
}

pub async fn run_watch_driver(
    grpc: Option<Arc<ReplicationGrpcClient>>,
    supervisor: Option<Arc<TaskSupervisor>>,
    cache: Arc<dyn RemoteInformerCache>,
    request: ResourceListRequest,
    cancel: CancellationToken,
    watch_idle_timeout: std::time::Duration,
) {
    let mut next_resource_version = None;
    let mut next_watch_replay_position = None;
    let mut reconnect_attempt = 0_u32;
    loop {
        if cancel.is_cancelled() {
            return;
        }
        if next_resource_version.is_none() {
            match resource::prime_list_scope(grpc.as_ref(), cache.as_ref(), &request).await {
                Ok(list) => {
                    next_resource_version = Some(list.resource_version());
                    next_watch_replay_position = list.watch_replay_position();
                }
                Err(error) => {
                    tracing::warn!(
                        api_version = %request.api_version(),
                        kind = %request.kind(),
                        error = %error,
                        "failed to prime remote informer scope"
                    );
                    sleep_before_reconnect(supervisor.as_ref(), reconnect_attempt).await;
                    reconnect_attempt = reconnect_attempt.saturating_add(1);
                    continue;
                }
            }
        }
        let watch_request = WatchRequest::try_new(
            request.api_version().to_string(),
            request.kind().to_string(),
            request.namespace().map(str::to_owned),
            request.label_selector().map(str::to_owned),
            request.field_selector().map(str::to_owned),
            next_resource_version,
            next_watch_replay_position,
        )
        .expect("worker informer LIST identity and cursor are validated");
        match watch_resources(grpc.as_ref(), watch_request).await {
            Ok(mut stream) => loop {
                let next = tokio::select! {
                    _ = cancel.cancelled() => return,
                    next = next_event_within_idle(
                        supervisor.as_ref(),
                        watch_idle_timeout,
                        &mut stream,
                    ) => next,
                };
                match next {
                    IdleNext::Item(Ok(event)) => {
                        let mut applied_cursor = WatchResumeCursor::try_new(
                            next_resource_version,
                            next_watch_replay_position,
                        )
                        .expect("informer cursor remains valid");
                        if let Err(error) = applied_cursor.advance_after_apply(&event) {
                            tracing::warn!(error = %error, "remote informer cursor rejected event before apply");
                            break;
                        }
                        cache.apply_event(&event).await;
                        next_resource_version = applied_cursor.resource_version();
                        next_watch_replay_position = applied_cursor.replay_position();
                        reconnect_attempt = 0;
                    }
                    IdleNext::Item(Err(error)) => {
                        if watch_error_requires_relist(&error) {
                            next_resource_version = None;
                            next_watch_replay_position = None;
                        }
                        tracing::warn!(
                            api_version = %request.api_version(),
                            kind = %request.kind(),
                            error = %error,
                            "remote informer watch stream failed"
                        );
                        break;
                    }
                    IdleNext::Idle => {
                        tracing::warn!(
                            api_version = %request.api_version(),
                            kind = %request.kind(),
                            "remote informer watch idle past heartbeat window; reconnecting from last resourceVersion"
                        );
                        break;
                    }
                    IdleNext::Closed => break,
                }
            },
            Err(error) => {
                if watch_error_requires_relist(&error) {
                    next_resource_version = None;
                    next_watch_replay_position = None;
                }
                tracing::warn!(
                    api_version = %request.api_version(),
                    kind = %request.kind(),
                    error = %error,
                    "failed to open remote informer watch stream"
                );
            }
        }
        sleep_before_reconnect(supervisor.as_ref(), reconnect_attempt).await;
        reconnect_attempt = reconnect_attempt.saturating_add(1);
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn start_required_worker_informers(
    grpc: Option<Arc<ReplicationGrpcClient>>,
    supervisor: Option<Arc<TaskSupervisor>>,
    cache: Arc<dyn RemoteInformerCache>,
    worker_informers_started: Arc<AtomicBool>,
    requests: Vec<ResourceListRequest>,
    cancel: CancellationToken,
    watch_idle_timeout: std::time::Duration,
) -> Result<Vec<SupervisedJoinHandle<()>>> {
    let supervisor = supervisor
        .as_ref()
        .ok_or_else(|| anyhow!("RemoteApiClient missing TaskSupervisor"))?
        .clone();
    if worker_informers_started
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Ok(Vec::new());
    }
    let mut handles = Vec::new();
    for request in requests {
        let grpc = grpc.clone();
        let driver_supervisor = Some(supervisor.clone());
        let cache = cache.clone();
        let cancel = cancel.clone();
        match supervisor
            .spawn_async(
                TaskCategory::Network,
                "remote_api_informer_watch",
                async move {
                    run_watch_driver(
                        grpc,
                        driver_supervisor,
                        cache,
                        request,
                        cancel,
                        watch_idle_timeout,
                    )
                    .await;
                },
            )
            .await
        {
            Ok(handle) => handles.push(handle),
            Err(error) => {
                worker_informers_started.store(false, Ordering::Release);
                return Err(error.into());
            }
        }
    }
    Ok(handles)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use klights_cluster_core::Resource;
    use klights_leader_api::{LeaderWatchError, ResourceEvent, WatchEventType, WatchStream};
    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};

    use super::{IdleNext, next_event_within_idle, watch_error_requires_relist};

    #[test]
    fn watch_error_requires_relist_requires_typed_replay_expiry() {
        let expired = LeaderWatchError::ReplayExpired {
            accepted_resource_version: 51,
        };
        assert!(
            watch_error_requires_relist(&expired),
            "typed replay expiry must trigger relist"
        );

        for (error, name) in [
            (
                LeaderWatchError::transport("expired but unmarked"),
                "transport",
            ),
            (LeaderWatchError::Timeout, "timeout"),
            (LeaderWatchError::Cancelled, "cancelled"),
        ] {
            assert!(
                !watch_error_requires_relist(&error),
                "{name} must not trigger relist"
            );
        }
    }

    #[tokio::test]
    async fn watch_idle_timeout_fires_when_stream_is_wedged() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let mut wedged = WatchStream::unpositioned_test_stream(futures::stream::pending());
        let started = std::time::Instant::now();
        let outcome = next_event_within_idle(
            Some(&supervisor),
            std::time::Duration::from_millis(150),
            &mut wedged,
        )
        .await;
        assert!(matches!(outcome, IdleNext::Idle));
        assert!(started.elapsed() < std::time::Duration::from_secs(5));

        let event = ResourceEvent::try_new(
            WatchEventType::Added,
            Resource::from_data_lossy(Arc::new(serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "web",
                    "uid": "uid-1",
                    "resourceVersion": "1"
                }
            }))),
            None,
        )
        .expect("valid live event");
        let mut live =
            WatchStream::unpositioned_test_stream(futures::stream::once(async move { Ok(event) }));
        let outcome = next_event_within_idle(
            Some(&supervisor),
            std::time::Duration::from_secs(5),
            &mut live,
        )
        .await;
        assert!(matches!(outcome, IdleNext::Item(Ok(_))));
    }
}
