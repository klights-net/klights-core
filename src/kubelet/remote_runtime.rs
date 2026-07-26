use std::sync::Arc;

use anyhow::{Result, anyhow};
use klights_node_api::{
    ExecStreamChannel, ExecTerminalError, NodeExecFrame, NodeExecRequest, NodeExecRuntime,
    NodeExecRuntimeFuture, NodeExecSession, NodeExecSyncRequest, NodeExecSyncResult,
    NodeMetricsRequest, NodeMetricsResult, NodeMetricsRuntime,
};
use klights_supervisor::TaskSupervisor;

pub(crate) struct CriNodeExecRuntime {
    pub(crate) cri: Arc<tokio::sync::Mutex<crate::kubelet::cri::CriClient>>,
    pub(crate) task_supervisor: Arc<TaskSupervisor>,
}

impl CriNodeExecRuntime {
    pub(crate) fn new(
        cri: Arc<tokio::sync::Mutex<crate::kubelet::cri::CriClient>>,
        task_supervisor: Arc<TaskSupervisor>,
    ) -> Self {
        Self {
            cri,
            task_supervisor,
        }
    }
}

pub(crate) struct CriNodeMetricsRuntime {
    sampler: Arc<dyn klights_node_api::NodeMetricsSampler>,
}

impl CriNodeMetricsRuntime {
    pub(crate) fn new(sampler: Arc<dyn klights_node_api::NodeMetricsSampler>) -> Self {
        Self { sampler }
    }
}

impl NodeExecRuntime for CriNodeExecRuntime {
    fn exec_sync(
        &self,
        request: NodeExecSyncRequest,
    ) -> NodeExecRuntimeFuture<'_, NodeExecSyncResult> {
        Box::pin(async move {
            let (target, command, timeout_seconds) = request.into_parts();
            let result = {
                let mut cri = self.cri.lock().await;
                crate::kubelet::cri_exec::exec_sync_with_created_state_retry(
                    &mut cri,
                    self.task_supervisor.as_ref(),
                    target.container_id(),
                    &command,
                    timeout_seconds,
                )
                .await
            };
            match result {
                Ok(response) => NodeExecSyncResult::success(
                    response.stdout,
                    response.stderr,
                    response.exit_code,
                ),
                Err(err) => NodeExecSyncResult::failed(
                    Vec::new(),
                    Vec::new(),
                    126,
                    ExecTerminalError::new(err.to_string()),
                ),
            }
        })
    }

    fn exec_stream(
        &self,
        request: NodeExecRequest,
        mut session: Box<dyn NodeExecSession>,
    ) -> NodeExecRuntimeFuture<'_, ()> {
        Box::pin(async move {
            if let Err(err) = run_cri_node_exec_stream(
                self.cri.clone(),
                self.task_supervisor.clone(),
                request,
                session.as_mut(),
            )
            .await
            {
                let _ = session
                    .send_frame(node_exec_error_frame(format!(
                        "remote node exec failed: {err:#}"
                    )))
                    .await;
            }
        })
    }
}

impl NodeMetricsRuntime for CriNodeMetricsRuntime {
    fn collect_metrics(
        &self,
        request: NodeMetricsRequest,
    ) -> klights_node_api::NodeMetricsFuture<'_, NodeMetricsResult> {
        self.sampler.sample_metrics(request)
    }
}

fn node_exec_error_frame(message: String) -> NodeExecFrame {
    NodeExecFrame::new(
        ExecStreamChannel::Error,
        serde_json::json!({
            "metadata": {},
            "status": "Failure",
            "message": message,
        })
        .to_string()
        .into_bytes(),
        true,
    )
}

async fn run_cri_node_exec_stream(
    cri: Arc<tokio::sync::Mutex<crate::kubelet::cri::CriClient>>,
    task_supervisor: Arc<TaskSupervisor>,
    request: NodeExecRequest,
    session: &mut dyn NodeExecSession,
) -> Result<()> {
    use crate::spdy::{SpdyExec, SpdyFrame, StreamType};

    let (target, command, options, attach) = request.into_parts();
    let streaming_url = {
        let mut cri_client = cri.lock().await;
        if attach {
            crate::kubelet::cri_exec::attach_with_created_state_retry(
                &mut cri_client,
                task_supervisor.as_ref(),
                crate::kubelet::cri_exec::AttachRequest {
                    container_id: target.container_id(),
                    stream_options: crate::kubelet::cri_exec::ExecStreamOptions {
                        tty: options.tty(),
                        stdin: options.stdin(),
                        stdout: options.stdout(),
                        stderr: options.stderr() && !options.tty(),
                    },
                },
            )
            .await?
            .url
        } else {
            crate::kubelet::cri_exec::exec_with_created_state_retry(
                &mut cri_client,
                task_supervisor.as_ref(),
                crate::kubelet::cri_exec::ExecRequest {
                    container_id: target.container_id(),
                    command: &command,
                    stream_options: crate::kubelet::cri_exec::ExecStreamOptions {
                        tty: options.tty(),
                        stdin: options.stdin(),
                        stdout: options.stdout(),
                        stderr: options.stderr() && !options.tty(),
                    },
                },
            )
            .await?
            .url
        }
    };

    let mut containerd_stream = SpdyExec::connect_to_streaming_url(&streaming_url).await?;
    let mut containerd_spdy = SpdyExec::new();
    if options.stdin() {
        containerd_spdy
            .write_syn_stream(&mut containerd_stream, 1, StreamType::Stdin)
            .await?;
    }
    if options.stdout() {
        containerd_spdy
            .write_syn_stream(&mut containerd_stream, 3, StreamType::Stdout)
            .await?;
    }
    if options.stderr() && !options.tty() {
        containerd_spdy
            .write_syn_stream(&mut containerd_stream, 5, StreamType::Stderr)
            .await?;
    }
    containerd_spdy
        .write_syn_stream(&mut containerd_stream, 7, StreamType::Error)
        .await?;
    if options.tty() {
        containerd_spdy
            .write_syn_stream(&mut containerd_stream, 9, StreamType::Resize)
            .await?;
        containerd_spdy
            .write_data_frame(
                &mut containerd_stream,
                9,
                serde_json::json!({"Width": 80, "Height": 24})
                    .to_string()
                    .as_bytes(),
                false,
            )
            .await?;
    }

    let mut stdin_closed = !options.stdin();
    let mut input_closed = false;
    loop {
        tokio::select! {
            frame = session.recv_frame(), if !input_closed && (!stdin_closed || options.tty()) => {
                match frame {
                    Ok(Some(frame)) => match frame.channel() {
                        ExecStreamChannel::Stdin if options.stdin() => {
                            if !frame.data().is_empty() {
                                containerd_spdy.write_data_frame(&mut containerd_stream, 1, frame.data(), false).await?;
                            }
                            if frame.fin() {
                                containerd_spdy.write_data_frame(&mut containerd_stream, 1, &[], true).await?;
                                stdin_closed = true;
                            }
                        }
                        ExecStreamChannel::Resize
                            if options.tty() && !frame.data().is_empty() =>
                        {
                            containerd_spdy
                                .write_data_frame(&mut containerd_stream, 9, frame.data(), false)
                                .await?;
                        }
                        _ => {}
                    },
                    Ok(None) => {
                        if options.stdin() && !stdin_closed {
                            let _ = containerd_spdy.write_data_frame(&mut containerd_stream, 1, &[], true).await;
                        }
                        stdin_closed = true;
                        input_closed = true;
                    }
                    Err(error) => return Err(anyhow!(error)),
                }
            }
            frame = containerd_spdy.read_frame(&mut containerd_stream) => {
                match frame? {
                    SpdyFrame::Data { stream_id, data, fin } => {
                        let channel = match stream_id {
                            3 => Some(ExecStreamChannel::Stdout),
                            5 => Some(ExecStreamChannel::Stderr),
                            7 => Some(ExecStreamChannel::Error),
                            _ => None,
                        };
                        if let Some(channel) = channel {
                            let frame = NodeExecFrame::new(channel, data, fin);
                            let terminal = frame.is_terminal();
                            session.send_frame(frame).await.map_err(|error| anyhow!(error))?;
                            if terminal {
                                return Ok(());
                            }
                        }
                    }
                    SpdyFrame::SynReply { .. } => {}
                    SpdyFrame::Ping { id } => containerd_spdy.write_ping(&mut containerd_stream, id).await?,
                    SpdyFrame::RstStream { .. } | SpdyFrame::GoAway => break,
                    SpdyFrame::Settings | SpdyFrame::WindowUpdate { .. } | SpdyFrame::Unknown | SpdyFrame::SynStream { .. } => {}
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct RecordingMetricsSampler {
        calls: AtomicUsize,
    }

    impl klights_node_api::NodeMetricsSampler for RecordingMetricsSampler {
        fn sample_metrics(
            &self,
            request: NodeMetricsRequest,
        ) -> klights_node_api::NodeMetricsFuture<'_, NodeMetricsResult> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Box::pin(async move {
                Ok(NodeMetricsResult::new(
                    request.target().clone(),
                    None,
                    Vec::new(),
                ))
            })
        }
    }

    #[tokio::test]
    async fn node_metrics_runtime_delegates_to_injected_sampler() {
        let sampler = Arc::new(RecordingMetricsSampler {
            calls: AtomicUsize::new(0),
        });
        let runtime = CriNodeMetricsRuntime::new(sampler.clone());
        let target =
            klights_node_api::NodeMetricsTarget::try_new("worker-a").expect("valid node target");
        let result = runtime
            .collect_metrics(NodeMetricsRequest::new(
                target.clone(),
                vec!["pod-a".to_string()],
            ))
            .await
            .expect("injected sampler result");

        assert_eq!(result.target(), &target);
        assert_eq!(sampler.calls.load(Ordering::Relaxed), 1);
    }
}
