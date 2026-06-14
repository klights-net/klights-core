use std::collections::HashMap;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use super::*;
use crate::replication::protocol::{
    ExecStreamChannel, NodeExecRequest, exec_error_status_payload_is_terminal,
    node_exec_error_frame_is_terminal,
};
use crate::spdy::{SpdyExec, SpdyFrame, StreamType};

const SPDY_UPGRADE_VALUE: &str = "SPDY/3.1";
const SPDY_PROTOCOL_HEADER: &str = "X-Stream-Protocol-Version";
const OPTIONAL_STREAM_NEGOTIATION_GRACE: std::time::Duration =
    std::time::Duration::from_millis(100);

#[derive(Debug, Clone, Copy)]
pub struct SpdyExecStreamRequest {
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
    pub tty: bool,
}

pub struct LocalExecSpdyRequest {
    pub cri: Arc<tokio::sync::Mutex<crate::kubelet::cri::CriClient>>,
    pub task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    pub target: ExecTarget,
    pub stream_request: SpdyExecStreamRequest,
}

pub struct RemoteExecSpdyRequest {
    pub replication: Arc<crate::replication::ReplicationService>,
    pub task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    pub node_name: String,
    pub target: ExecTarget,
    pub stream_request: SpdyExecStreamRequest,
}

#[derive(Debug, Default, Clone)]
pub struct SpdyClientStreams {
    stdin: Option<u32>,
    stdout: Option<u32>,
    stderr: Option<u32>,
    error: Option<u32>,
    resize: Option<u32>,
}

impl SpdyClientStreams {
    fn insert(&mut self, stream_id: u32, stream_type: StreamType) {
        match stream_type {
            StreamType::Stdin => self.stdin = Some(stream_id),
            StreamType::Stdout => self.stdout = Some(stream_id),
            StreamType::Stderr => self.stderr = Some(stream_id),
            StreamType::Error => self.error = Some(stream_id),
            StreamType::Resize => self.resize = Some(stream_id),
            StreamType::Data => {}
        }
    }

    fn stream_id_for(&self, stream_type: StreamType) -> Option<u32> {
        match stream_type {
            StreamType::Stdin => self.stdin,
            StreamType::Stdout => self.stdout,
            StreamType::Stderr => self.stderr,
            StreamType::Error => self.error,
            StreamType::Resize => self.resize,
            StreamType::Data => None,
        }
    }

    fn has_expected(&self, req: SpdyExecStreamRequest) -> bool {
        (!req.stdin || self.stdin.is_some())
            && (!req.stdout || self.stdout.is_some())
            && (!req.stderr || req.tty || self.stderr.is_some())
            && (!req.tty || self.resize.is_some())
    }
}

pub fn is_spdy_upgrade(headers: &header::HeaderMap) -> bool {
    headers
        .get(header::UPGRADE)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.eq_ignore_ascii_case(SPDY_UPGRADE_VALUE))
        .unwrap_or(false)
}

pub fn negotiate_spdy_subprotocol(headers: &header::HeaderMap) -> String {
    const PREFERRED: &[&str] = &[
        "v4.channel.k8s.io",
        "v3.channel.k8s.io",
        "v2.channel.k8s.io",
        "channel.k8s.io",
    ];

    let mut offered = Vec::new();
    for value in headers.get_all(SPDY_PROTOCOL_HEADER) {
        if let Ok(raw) = value.to_str() {
            offered.extend(raw.split(',').map(str::trim).filter(|s| !s.is_empty()));
        }
    }

    for preferred in PREFERRED {
        if offered.iter().any(|offered| offered == preferred) {
            return (*preferred).to_string();
        }
    }

    "v4.channel.k8s.io".to_string()
}

fn spdy_stream_type_from_headers(headers: &HashMap<String, String>) -> Option<StreamType> {
    let raw = headers.iter().find_map(|(key, value)| {
        if key.eq_ignore_ascii_case("streamtype") {
            Some(value.as_str())
        } else {
            None
        }
    })?;
    raw.split('\0')
        .find_map(|value| match value.to_ascii_lowercase().as_str() {
            "stdin" => Some(StreamType::Stdin),
            "stdout" => Some(StreamType::Stdout),
            "stderr" => Some(StreamType::Stderr),
            "error" => Some(StreamType::Error),
            "resize" => Some(StreamType::Resize),
            "data" => Some(StreamType::Data),
            _ => None,
        })
}

pub async fn collect_spdy_client_streams<S>(
    client_spdy: &mut SpdyExec,
    client_stream: &mut S,
    request: SpdyExecStreamRequest,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) -> anyhow::Result<SpdyClientStreams>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut streams = SpdyClientStreams::default();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);

    while !streams.has_expected(request) {
        tokio::select! {
            timer = task_supervisor.sleep_until("spdy_exec_stream_negotiation", deadline) => {
                timer?;
                anyhow::bail!(
                    "timed out waiting for SPDY exec streams: request={:?}, streams={:?}",
                    request,
                    streams
                );
            }
            frame = client_spdy.read_frame(client_stream) => {
                match frame? {
                    SpdyFrame::SynStream { stream_id, headers } => {
                        if let Some(stream_type) = spdy_stream_type_from_headers(&headers) {
                            streams.insert(stream_id, stream_type);
                            client_spdy.write_syn_reply(client_stream, stream_id).await?;
                        } else {
                            tracing::debug!(stream_id, ?headers, "SPDY exec client stream missing streamType");
                        }
                    }
                    SpdyFrame::Ping { id } => {
                        client_spdy.write_ping(client_stream, id).await?;
                    }
                    SpdyFrame::Settings | SpdyFrame::WindowUpdate { .. } => {}
                    SpdyFrame::GoAway | SpdyFrame::RstStream { .. } => {
                        anyhow::bail!("SPDY exec client closed before stream negotiation completed");
                    }
                    SpdyFrame::Data { .. } | SpdyFrame::SynReply { .. } | SpdyFrame::Unknown => {}
                }
            }
        }
    }

    let optional_deadline = tokio::time::Instant::now() + OPTIONAL_STREAM_NEGOTIATION_GRACE;
    while streams.error.is_none() {
        tokio::select! {
            timer = task_supervisor.sleep_until("spdy_exec_optional_stream_negotiation", optional_deadline) => {
                timer?;
                break;
            }
            frame = client_spdy.read_frame(client_stream) => {
                match frame {
                    Ok(SpdyFrame::SynStream { stream_id, headers }) => {
                        if let Some(stream_type) = spdy_stream_type_from_headers(&headers) {
                            let is_error_stream = stream_type == StreamType::Error;
                            streams.insert(stream_id, stream_type);
                            client_spdy.write_syn_reply(client_stream, stream_id).await?;
                            if is_error_stream {
                                break;
                            }
                        } else {
                            tracing::debug!(stream_id, ?headers, "SPDY exec optional client stream missing streamType");
                        }
                    }
                    Ok(SpdyFrame::Ping { id }) => {
                        client_spdy.write_ping(client_stream, id).await?;
                    }
                    Ok(SpdyFrame::Settings | SpdyFrame::WindowUpdate { .. }) => {}
                    Ok(SpdyFrame::GoAway | SpdyFrame::RstStream { .. }) => break,
                    Ok(SpdyFrame::Data { .. } | SpdyFrame::SynReply { .. } | SpdyFrame::Unknown) => {}
                    Err(err) => {
                        tracing::debug!("SPDY exec optional stream negotiation ended: {}", err);
                        break;
                    }
                }
            }
        }
    }

    Ok(streams)
}

pub async fn write_spdy_exec_channel_frame<S>(
    client_spdy: &SpdyExec,
    client_stream: &mut S,
    streams: &SpdyClientStreams,
    channel: StreamType,
    data: &[u8],
    fin: bool,
) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    if let Some(stream_id) = streams.stream_id_for(channel) {
        client_spdy
            .write_data_frame(client_stream, stream_id, data, fin)
            .await?;
    }
    Ok(())
}

async fn write_spdy_exec_error<S>(
    client_spdy: &SpdyExec,
    client_stream: &mut S,
    streams: &SpdyClientStreams,
    message: String,
) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let payload = serde_json::json!({
        "metadata": {},
        "status": "Failure",
        "message": message,
        "details": {"causes": []}
    })
    .to_string();
    write_spdy_exec_channel_frame(
        client_spdy,
        client_stream,
        streams,
        StreamType::Error,
        payload.as_bytes(),
        true,
    )
    .await
}

struct LocalSpdyExecTarget<'a> {
    cri: Arc<tokio::sync::Mutex<crate::kubelet::cri::CriClient>>,
    task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    container_id: &'a str,
    command: &'a [String],
    request: SpdyExecStreamRequest,
}

#[derive(Debug, Clone)]
pub struct ContainerdSpdyBridgeState {
    wait_for_container_close: bool,
    terminal_error_seen: bool,
}

impl ContainerdSpdyBridgeState {
    pub fn new(request: SpdyExecStreamRequest) -> Self {
        Self {
            wait_for_container_close: request.stdout || (request.stderr && !request.tty),
            terminal_error_seen: false,
        }
    }

    pub fn terminal_error_seen(&self) -> bool {
        self.terminal_error_seen
    }

    pub fn observe_data_frame(&mut self, stream_id: u32, data: &[u8], fin: bool) -> bool {
        match stream_id {
            7 if fin || exec_error_status_payload_is_terminal(data) => {
                self.terminal_error_seen = true;
            }
            _ => {}
        }

        self.terminal_error_seen && !self.wait_for_container_close
    }
}

fn spdy_stream_error_is_unexpected_eof(err: &anyhow::Error) -> bool {
    err.downcast_ref::<std::io::Error>()
        .map(|io_err| io_err.kind() == std::io::ErrorKind::UnexpectedEof)
        .unwrap_or(false)
}

async fn bridge_containerd_spdy_to_client<S>(
    client_spdy: &SpdyExec,
    client_stream: &mut S,
    streams: &SpdyClientStreams,
    target: LocalSpdyExecTarget<'_>,
) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let streaming_url = {
        let mut cri_client = target.cri.lock().await;
        exec_with_created_state_retry(
            &mut cri_client,
            target.task_supervisor.as_ref(),
            ExecRequest {
                container_id: target.container_id,
                command: target.command,
                stream_options: ExecStreamOptions {
                    tty: target.request.tty,
                    stdin: target.request.stdin,
                    stdout: target.request.stdout,
                    stderr: target.request.stderr && !target.request.tty,
                },
            },
        )
        .await?
        .url
    };

    let mut containerd_stream = SpdyExec::connect_to_streaming_url(&streaming_url).await?;
    let mut containerd_spdy = SpdyExec::new();
    if target.request.stdout {
        containerd_spdy
            .write_syn_stream(&mut containerd_stream, 3, StreamType::Stdout)
            .await?;
    }
    if target.request.stderr && !target.request.tty {
        containerd_spdy
            .write_syn_stream(&mut containerd_stream, 5, StreamType::Stderr)
            .await?;
    }
    containerd_spdy
        .write_syn_stream(&mut containerd_stream, 7, StreamType::Error)
        .await?;

    let mut completion = ContainerdSpdyBridgeState::new(target.request);
    loop {
        let frame = match containerd_spdy.read_frame(&mut containerd_stream).await {
            Ok(frame) => frame,
            Err(err)
                if completion.terminal_error_seen()
                    && spdy_stream_error_is_unexpected_eof(&err) =>
            {
                return Ok(());
            }
            Err(err) => return Err(err),
        };

        match frame {
            SpdyFrame::Data {
                stream_id,
                data,
                fin,
            } => {
                let channel = match stream_id {
                    3 => Some(StreamType::Stdout),
                    5 => Some(StreamType::Stderr),
                    7 => Some(StreamType::Error),
                    _ => None,
                };
                if let Some(channel) = channel {
                    let complete = completion.observe_data_frame(stream_id, &data, fin);
                    write_spdy_exec_channel_frame(
                        client_spdy,
                        client_stream,
                        streams,
                        channel,
                        &data,
                        fin,
                    )
                    .await?;
                    if complete {
                        return Ok(());
                    }
                }
            }
            SpdyFrame::Ping { id } => {
                containerd_spdy
                    .write_ping(&mut containerd_stream, id)
                    .await?;
            }
            SpdyFrame::RstStream { .. } | SpdyFrame::GoAway => return Ok(()),
            SpdyFrame::SynReply { .. }
            | SpdyFrame::Settings
            | SpdyFrame::WindowUpdate { .. }
            | SpdyFrame::Unknown
            | SpdyFrame::SynStream { .. } => {}
        }
    }
}

async fn bridge_remote_exec_stream_to_client<S>(
    client_spdy: &SpdyExec,
    client_stream: &mut S,
    streams: &SpdyClientStreams,
    mut session: crate::replication::service::NodeExecStreamSession,
) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    while let Some(frame) = session.recv_frame().await? {
        let channel = match frame.channel {
            ExecStreamChannel::Stdout => Some(StreamType::Stdout),
            ExecStreamChannel::Stderr => Some(StreamType::Stderr),
            ExecStreamChannel::Error => Some(StreamType::Error),
            ExecStreamChannel::Stdin | ExecStreamChannel::Resize => None,
        };
        if let Some(channel) = channel {
            let terminal = node_exec_error_frame_is_terminal(&frame);
            write_spdy_exec_channel_frame(
                client_spdy,
                client_stream,
                streams,
                channel,
                &frame.data,
                frame.fin,
            )
            .await?;
            if terminal {
                session.close().await;
                return Ok(());
            }
        }
    }
    session.close().await;
    Ok(())
}

pub async fn handle_local_exec_spdy<S>(mut client_stream: S, request: LocalExecSpdyRequest)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let LocalExecSpdyRequest {
        cri,
        task_supervisor,
        target,
        stream_request: request,
    } = request;
    let ExecTarget {
        namespace,
        pod_name,
        container_id,
        command,
    } = target;
    let mut client_spdy = SpdyExec::new();
    let streams = match collect_spdy_client_streams(
        &mut client_spdy,
        &mut client_stream,
        request,
        task_supervisor.as_ref(),
    )
    .await
    {
        Ok(streams) => streams,
        Err(err) => {
            tracing::error!("SPDY exec stream negotiation failed: {}", err);
            let _ = client_stream.shutdown().await;
            return;
        }
    };

    if let Err(err) = bridge_containerd_spdy_to_client(
        &client_spdy,
        &mut client_stream,
        &streams,
        LocalSpdyExecTarget {
            cri,
            task_supervisor,
            container_id: &container_id,
            command: &command,
            request,
        },
    )
    .await
    {
        tracing::error!(
            "SPDY exec failed: pod={}/{}, container={}, error={}",
            namespace,
            pod_name,
            container_id,
            err
        );
        let _ = write_spdy_exec_error(&client_spdy, &mut client_stream, &streams, err.to_string())
            .await;
    }

    let _ = client_stream.shutdown().await;
    tracing::info!("SPDY exec completed: pod={}/{}", namespace, pod_name);
}

pub async fn handle_remote_exec_spdy<S>(mut client_stream: S, request: RemoteExecSpdyRequest)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let RemoteExecSpdyRequest {
        replication,
        task_supervisor,
        node_name,
        target,
        stream_request: request,
    } = request;
    let ExecTarget {
        namespace,
        pod_name,
        container_id,
        command,
    } = target;
    let mut client_spdy = SpdyExec::new();
    let streams = match collect_spdy_client_streams(
        &mut client_spdy,
        &mut client_stream,
        request,
        task_supervisor.as_ref(),
    )
    .await
    {
        Ok(streams) => streams,
        Err(err) => {
            tracing::error!("Remote SPDY exec stream negotiation failed: {}", err);
            let _ = client_stream.shutdown().await;
            return;
        }
    };

    let session = replication
        .open_node_exec_stream(NodeExecRequest {
            request_id: String::new(),
            node_name,
            namespace: namespace.clone(),
            pod_name: pod_name.clone(),
            container_id: container_id.clone(),
            command: command.clone(),
            tty: request.tty,
            stdin: request.stdin,
            stdout: request.stdout,
            stderr: request.stderr,
        })
        .await;

    match session {
        Ok(session) => {
            if let Err(err) = bridge_remote_exec_stream_to_client(
                &client_spdy,
                &mut client_stream,
                &streams,
                session,
            )
            .await
            {
                tracing::error!(
                    "Remote SPDY exec failed: pod={}/{}, container={}, error={}",
                    namespace,
                    pod_name,
                    container_id,
                    err
                );
                let _ = write_spdy_exec_error(
                    &client_spdy,
                    &mut client_stream,
                    &streams,
                    err.to_string(),
                )
                .await;
            }
        }
        Err(err) => {
            tracing::error!("Remote SPDY exec stream open failed: {}", err);
            let _ =
                write_spdy_exec_error(&client_spdy, &mut client_stream, &streams, err.to_string())
                    .await;
        }
    }

    let _ = client_stream.shutdown().await;
    tracing::info!("Remote SPDY exec completed: pod={}/{}", namespace, pod_name);
}

pub fn spdy_switching_protocols_response(subprotocol: String) -> Result<Response, AppError> {
    Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header(header::UPGRADE, SPDY_UPGRADE_VALUE)
        .header(header::CONNECTION, "Upgrade")
        .header(SPDY_PROTOCOL_HEADER, subprotocol)
        .body(axum::body::Body::empty())
        .map_err(|err| AppError::Internal(format!("Failed to build SPDY response: {err}")))
}
