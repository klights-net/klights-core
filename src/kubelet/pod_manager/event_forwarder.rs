use super::*;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

pub(super) async fn spawn_cri_event_forwarder(
    cri: std::sync::Arc<dyn crate::kubelet::pod_runtime::cri::CriRuntime>,
    cancel_token: tokio_util::sync::CancellationToken,
    task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
    lifecycle_tx: Option<
        tokio::sync::mpsc::Sender<crate::kubelet::reconciler::cri_reconnect::CriStreamLifecycle>,
    >,
) -> CriEventReceiver {
    use crate::kubelet::cri_events::{KubeletEvent, KubeletEventKind};
    use crate::kubelet::pod_runtime::cri::CriRuntimeContainerEventKind;

    let (tx, rx) = mpsc::channel(1024);
    let task_supervisor_for_worker = task_supervisor.clone();
    if let Err(err) = task_supervisor.spawn_async(
        crate::task_supervisor::TaskCategory::Background,
        "cri_event_forwarder",
        async move {
        let mut reconnect_attempt = 0u32;
        let mut ever_connected = false;
        let mut disconnected_at_ms: Option<i64> = None;
        let mut generation = 0u64;

        loop {
            let subscribe_result = tokio::select! {
                _ = cancel_token.cancelled() => {
                    tracing::debug!("CRI event forwarder cancelled before subscribe");
                    return;
                }
                result = cri.subscribe_container_events() => result,
            };

            let mut stream = match subscribe_result {
                Ok(stream) => {
                    if ever_connected {
                        generation = generation.saturating_add(1);
                        let reconnected_at_ms = now_ms();
                        if let Some(tx) = lifecycle_tx.as_ref() {
                            let _ = tx
                                .send(crate::kubelet::reconciler::cri_reconnect::CriStreamLifecycle::Reconnected {
                                    generation,
                                    disconnected_at_ms: disconnected_at_ms.unwrap_or(reconnected_at_ms),
                                    reconnected_at_ms,
                                })
                                .await;
                        }
                    }
                    if reconnect_attempt > 0 {
                        tracing::info!(
                            "CRI event stream re-established after {} attempt(s)",
                            reconnect_attempt
                        );
                    }
                    reconnect_attempt = 0;
                    ever_connected = true;
                    disconnected_at_ms = None;
                    stream
                }
                Err(e) => {
                    let delay = crate::utils::watch_reconnect_delay(reconnect_attempt);
                    tracing::warn!(
                        "CRI event-stream subscribe attempt {} failed: {:#} - retry in {:?}",
                        reconnect_attempt + 1,
                        e,
                        delay
                    );
                    reconnect_attempt = reconnect_attempt.saturating_add(1);
                    tokio::select! {
                        _ = cancel_token.cancelled() => return,
                        sleep_result = task_supervisor_for_worker.sleep("cri_event_forwarder_retry_backoff", delay) => {
                            if let Err(err) = sleep_result {
                                tracing::debug!("CRI event-stream retry timer interrupted: {err}");
                            }
                        }
                    }
                    continue;
                }
            };

            loop {
                let message = tokio::select! {
                    _ = cancel_token.cancelled() => {
                        tracing::debug!("CRI event forwarder cancelled");
                        return;
                    }
                    message = stream.next_event() => message,
                };

                match message {
                    Ok(Some(raw_event)) => {
                        let kind = match raw_event.kind {
                            CriRuntimeContainerEventKind::Started => KubeletEventKind::Started,
                            CriRuntimeContainerEventKind::Stopped => KubeletEventKind::Stopped,
                            CriRuntimeContainerEventKind::Created
                            | CriRuntimeContainerEventKind::Deleted => continue,
                        };
                        let ev = KubeletEvent {
                            kind,
                            container_id: raw_event.container_id,
                            pod_namespace: None,
                            pod_name: None,
                            pod_uid: None,
                            timestamp_ns: 0,
                        };
                        if tx.send(ev).await.is_err() {
                            tracing::debug!("CRI event receiver dropped; stopping forwarder");
                            return;
                        }
                    }
                    Ok(None) => {
                    tracing::warn!("CRI event stream ended; reconnect loop will resubscribe");
                        disconnected_at_ms.get_or_insert_with(now_ms);
                        break;
                    }
                    Err(e) => {
                        tracing::warn!("CRI event stream error: {:#} - reconnecting", e);
                        disconnected_at_ms.get_or_insert_with(now_ms);
                        break;
                    }
                }
            }
        }
        },
    )
    .await
    {
        tracing::warn!("failed to spawn CRI event forwarder: {}", err);
    }

    rx
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use anyhow::Result;
    use tokio_util::sync::CancellationToken;

    use crate::kubelet::pod_runtime::cri::{
        CriRuntime, CriRuntimeContainerEvent, CriRuntimeContainerEventStream,
    };

    struct EndingStream;

    #[async_trait::async_trait]
    impl CriRuntimeContainerEventStream for EndingStream {
        async fn next_event(&mut self) -> Result<Option<CriRuntimeContainerEvent>> {
            Ok(None)
        }
    }

    struct PendingStream {
        cancel: CancellationToken,
    }

    #[async_trait::async_trait]
    impl CriRuntimeContainerEventStream for PendingStream {
        async fn next_event(&mut self) -> Result<Option<CriRuntimeContainerEvent>> {
            self.cancel.cancelled().await;
            Ok(None)
        }
    }

    struct SequenceCriRuntime {
        streams: Mutex<VecDeque<Box<dyn CriRuntimeContainerEventStream>>>,
        cancel: CancellationToken,
    }

    impl SequenceCriRuntime {
        fn new(
            streams: Vec<Box<dyn CriRuntimeContainerEventStream>>,
            cancel: CancellationToken,
        ) -> Self {
            Self {
                streams: Mutex::new(VecDeque::from(streams)),
                cancel,
            }
        }
    }

    #[async_trait::async_trait]
    impl CriRuntime for SequenceCriRuntime {
        async fn image_status(&self, _image: &str) -> Result<bool> {
            Ok(true)
        }
        async fn pull_image(&self, image: &str) -> Result<String> {
            Ok(image.to_string())
        }
        async fn run_pod_sandbox(
            &self,
            _sandbox_config: k8s_cri::v1::PodSandboxConfig,
        ) -> Result<String> {
            Ok("sandbox".to_string())
        }
        async fn stop_pod_sandbox(&self, _sandbox_id: &str) -> Result<()> {
            Ok(())
        }
        async fn remove_pod_sandbox(&self, _sandbox_id: &str) -> Result<()> {
            Ok(())
        }
        async fn list_pod_sandboxes(
            &self,
            _pod_uid_filter: Option<&str>,
        ) -> Result<Vec<(String, String)>> {
            Ok(Vec::new())
        }
        async fn create_container(
            &self,
            _container_config: k8s_cri::v1::ContainerConfig,
            _sandbox_id: &str,
            _sandbox_config: k8s_cri::v1::PodSandboxConfig,
        ) -> Result<String> {
            Ok("container".to_string())
        }
        async fn start_container(&self, _container_id: &str) -> Result<()> {
            Ok(())
        }
        async fn stop_container(&self, _container_id: &str, _timeout_seconds: i64) -> Result<()> {
            Ok(())
        }
        async fn remove_container(&self, _container_id: &str) -> Result<()> {
            Ok(())
        }
        async fn container_status(
            &self,
            _container_id: &str,
        ) -> Result<k8s_cri::v1::ContainerStatusResponse> {
            Ok(Default::default())
        }
        async fn exec_sync(
            &self,
            _container_id: &str,
            _command: &[String],
            _timeout_seconds: i64,
        ) -> Result<k8s_cri::v1::ExecSyncResponse> {
            Ok(Default::default())
        }
        async fn subscribe_container_events(
            &self,
        ) -> Result<Box<dyn CriRuntimeContainerEventStream>> {
            let next = { self.streams.lock().unwrap().pop_front() };
            match next {
                Some(stream) => Ok(stream),
                None => {
                    self.cancel.cancelled().await;
                    Ok(Box::new(EndingStream))
                }
            }
        }
    }

    fn supervisor() -> Arc<crate::task_supervisor::TaskSupervisor> {
        Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ))
    }

    #[tokio::test]
    async fn initial_subscribe_does_not_emit_reconnect() {
        let cancel = CancellationToken::new();
        let (_tx, mut lifecycle_rx) = tokio::sync::mpsc::channel(4);
        let cri = Arc::new(SequenceCriRuntime::new(
            vec![Box::new(PendingStream {
                cancel: cancel.clone(),
            })],
            cancel.clone(),
        ));

        let _events =
            super::spawn_cri_event_forwarder(cri, cancel.clone(), supervisor(), Some(_tx)).await;

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), lifecycle_rx.recv())
                .await
                .is_err(),
            "initial CRI subscription must not emit a reconnect lifecycle event"
        );
        cancel.cancel();
    }

    #[tokio::test]
    async fn stream_end_then_successful_resubscribe_emits_one_reconnect() {
        let cancel = CancellationToken::new();
        let (tx, mut lifecycle_rx) = tokio::sync::mpsc::channel(4);
        let cri = Arc::new(SequenceCriRuntime::new(
            vec![
                Box::new(EndingStream),
                Box::new(PendingStream {
                    cancel: cancel.clone(),
                }),
            ],
            cancel.clone(),
        ));

        let _events =
            super::spawn_cri_event_forwarder(cri, cancel.clone(), supervisor(), Some(tx)).await;

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), lifecycle_rx.recv())
            .await
            .expect("reconnect lifecycle event")
            .expect("lifecycle channel open");
        assert!(matches!(
            event,
            crate::kubelet::reconciler::cri_reconnect::CriStreamLifecycle::Reconnected { .. }
        ));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), lifecycle_rx.recv())
                .await
                .is_err(),
            "one disconnect window must emit exactly one reconnect lifecycle event"
        );
        cancel.cancel();
    }
}
