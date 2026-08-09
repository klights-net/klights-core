use super::*;

pub(super) async fn spawn_cri_event_forwarder(
    cri: std::sync::Arc<dyn klights_kubelet::runtime::cri::CriRuntime>,
    cancel_token: tokio_util::sync::CancellationToken,
    task_supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
    lifecycle_tx: Option<
        tokio::sync::mpsc::Sender<crate::kubelet::reconciler::cri_reconnect::CriStreamLifecycle>,
    >,
    wall_clock: std::sync::Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
) -> CriEventReceiver {
    use klights_kubelet::cri_events::KubeletEventKind;

    let (tx, rx) = mpsc::channel(1024);
    let task_supervisor_for_worker = task_supervisor.clone();
    if let Err(err) = task_supervisor.spawn_async(
        klights_supervisor::TaskCategory::Background,
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
                        let reconnected_at_ms = wall_clock.now_ms();
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
                    let delay = klights_supervisor::reconnect_backoff::delay(reconnect_attempt);
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
                        match raw_event.kind {
                            KubeletEventKind::Started | KubeletEventKind::Stopped => {}
                            KubeletEventKind::Created | KubeletEventKind::Deleted => continue,
                        }
                        if raw_event.is_pod_sandbox_transition() {
                            tracing::debug!(
                                sandbox_id = %raw_event.container_id,
                                event_kind = raw_event.kind.as_str(),
                                "ignoring pod sandbox lifecycle transition"
                            );
                            continue;
                        }
                        if (raw_event.pod_namespace.is_none()
                            || raw_event.pod_name.is_none()
                            || raw_event.pod_uid.is_none())
                            && let Some(lifecycle_tx) = lifecycle_tx.as_ref()
                        {
                            let _ = lifecycle_tx
                                .send(crate::kubelet::reconciler::cri_reconnect::CriStreamLifecycle::IdentityUnresolved {
                                    container_id: raw_event.container_id.clone(),
                                    timestamp_ns: raw_event.timestamp_ns,
                                })
                                .await;
                        }
                        if tx.send(raw_event).await.is_err() {
                            tracing::debug!("CRI event receiver dropped; stopping forwarder");
                            return;
                        }
                    }
                    Ok(None) => {
                    tracing::warn!("CRI event stream ended; reconnect loop will resubscribe");
                        disconnected_at_ms.get_or_insert_with(|| wall_clock.now_ms());
                        break;
                    }
                    Err(e) => {
                        tracing::warn!("CRI event stream error: {:#} - reconnecting", e);
                        disconnected_at_ms.get_or_insert_with(|| wall_clock.now_ms());
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

    use klights_kubelet::cri_events::KubeletEvent;
    use klights_kubelet::runtime::cri::{CriRuntime, CriRuntimeContainerEventStream};

    struct EndingStream;

    #[async_trait::async_trait]
    impl CriRuntimeContainerEventStream for EndingStream {
        async fn next_event(&mut self) -> Result<Option<KubeletEvent>> {
            Ok(None)
        }
    }

    struct OneEventStream(Option<KubeletEvent>);

    #[async_trait::async_trait]
    impl CriRuntimeContainerEventStream for OneEventStream {
        async fn next_event(&mut self) -> Result<Option<KubeletEvent>> {
            Ok(self.0.take())
        }
    }

    struct EventsStream(VecDeque<KubeletEvent>);

    #[async_trait::async_trait]
    impl CriRuntimeContainerEventStream for EventsStream {
        async fn next_event(&mut self) -> Result<Option<KubeletEvent>> {
            Ok(self.0.pop_front())
        }
    }

    struct PendingStream {
        cancel: CancellationToken,
    }

    #[async_trait::async_trait]
    impl CriRuntimeContainerEventStream for PendingStream {
        async fn next_event(&mut self) -> Result<Option<KubeletEvent>> {
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

    #[tokio::test]
    async fn forwards_cri_event_identity_and_timestamp_without_loss() {
        use klights_kubelet::cri_events::KubeletEventKind;

        let cancel = CancellationToken::new();
        let runtime = Arc::new(SequenceCriRuntime::new(
            vec![Box::new(OneEventStream(Some(KubeletEvent {
                container_id: "container-identity".into(),
                kind: KubeletEventKind::Stopped,
                pod_namespace: Some("workloads".into()),
                pod_name: Some("identity-pod".into()),
                pod_uid: Some("identity-uid".into()),
                pod_sandbox_id: Some("sandbox-identity".into()),
                timestamp_ns: 1_777_000_123,
            })))],
            cancel.clone(),
        ));
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let wall_clock = Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock);

        let mut receiver =
            super::spawn_cri_event_forwarder(runtime, cancel.clone(), supervisor, None, wall_clock)
                .await;
        let event = receiver.recv().await.expect("forwarded CRI event");
        cancel.cancel();

        assert_eq!(event.container_id, "container-identity");
        assert_eq!(event.pod_namespace.as_deref(), Some("workloads"));
        assert_eq!(event.pod_name.as_deref(), Some("identity-pod"));
        assert_eq!(event.pod_uid.as_deref(), Some("identity-uid"));
        assert_eq!(event.timestamp_ns, 1_777_000_123);
    }

    #[tokio::test]
    async fn filters_pod_sandbox_transition_but_forwards_workload_container_stopped() {
        use klights_kubelet::cri_events::KubeletEventKind;

        let cancel = CancellationToken::new();
        let identity = || {
            (
                Some("workloads".into()),
                Some("pod-a".into()),
                Some("uid-a".into()),
            )
        };
        let (namespace, name, uid) = identity();
        let sandbox = KubeletEvent {
            container_id: "sandbox-a".into(),
            kind: KubeletEventKind::Started,
            pod_namespace: namespace,
            pod_name: name,
            pod_uid: uid,
            pod_sandbox_id: Some("sandbox-a".into()),
            timestamp_ns: 10,
        };
        let (namespace, name, uid) = identity();
        let app = KubeletEvent {
            container_id: "container-app".into(),
            kind: KubeletEventKind::Stopped,
            pod_namespace: namespace,
            pod_name: name,
            pod_uid: uid,
            pod_sandbox_id: Some("sandbox-a".into()),
            timestamp_ns: 11,
        };
        let runtime = Arc::new(SequenceCriRuntime::new(
            vec![Box::new(EventsStream(VecDeque::from([sandbox, app])))],
            cancel.clone(),
        ));
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let mut receiver = super::spawn_cri_event_forwarder(
            runtime,
            cancel.clone(),
            supervisor,
            None,
            Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
        )
        .await;

        let forwarded = receiver.recv().await.expect("workload event forwarded");
        cancel.cancel();
        assert_eq!(forwarded.container_id, "container-app");
        assert_eq!(forwarded.kind, KubeletEventKind::Stopped);
    }

    #[tokio::test]
    async fn unresolved_cri_event_requests_event_driven_inventory_resync() {
        use klights_kubelet::cri_events::KubeletEventKind;

        let cancel = CancellationToken::new();
        let runtime = Arc::new(SequenceCriRuntime::new(
            vec![Box::new(OneEventStream(Some(KubeletEvent {
                container_id: "container-without-metadata".into(),
                kind: KubeletEventKind::Stopped,
                pod_namespace: None,
                pod_name: None,
                pod_uid: None,
                pod_sandbox_id: None,
                timestamp_ns: 1_777_000_456,
            })))],
            cancel.clone(),
        ));
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let (lifecycle_tx, mut lifecycle_rx) = tokio::sync::mpsc::channel(1);

        let mut receiver = super::spawn_cri_event_forwarder(
            runtime,
            cancel.clone(),
            supervisor,
            Some(lifecycle_tx),
            Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
        )
        .await;
        receiver.recv().await.expect("forwarded unresolved event");
        let lifecycle = lifecycle_rx.recv().await.expect("inventory resync request");
        cancel.cancel();

        assert!(matches!(
            lifecycle,
            crate::kubelet::reconciler::cri_reconnect::CriStreamLifecycle::IdentityUnresolved {
                container_id,
                timestamp_ns: 1_777_000_456,
            } if container_id == "container-without-metadata"
        ));
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
        async fn list_pod_sandbox_summaries(
            &self,
        ) -> Result<Vec<klights_kubelet::runtime::cri::CriPodSandboxSummary>> {
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

    fn supervisor() -> Arc<klights_supervisor::TaskSupervisor> {
        Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
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

        let _events = super::spawn_cri_event_forwarder(
            cri,
            cancel.clone(),
            supervisor(),
            Some(_tx),
            Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
        )
        .await;

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

        let _events = super::spawn_cri_event_forwarder(
            cri,
            cancel.clone(),
            supervisor(),
            Some(tx),
            Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
        )
        .await;

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
