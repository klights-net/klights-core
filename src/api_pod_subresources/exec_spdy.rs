use std::collections::HashMap;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use super::*;
use crate::spdy::{SpdyExec, SpdyFrame, StreamType};
use klights_node_api::{
    ExecStreamChannel, ExecStreamOptions as NodeExecStreamOptions, NodeExec, NodeExecRequest,
    NodeExecSession, NodeExecTarget, exec_error_status_payload_is_terminal,
};

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
    pub attach: bool,
}

pub struct LocalExecSpdyRequest {
    pub cri: Arc<tokio::sync::Mutex<crate::kubelet::cri::CriClient>>,
    pub task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    pub target: ExecTarget,
    pub stream_request: SpdyExecStreamRequest,
}

pub struct RemoteExecSpdyRequest {
    pub node_exec: Arc<dyn NodeExec>,
    pub task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
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
    task_supervisor: &klights_supervisor::TaskSupervisor,
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
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
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
        if target.request.attach {
            attach_with_created_state_retry(
                &mut cri_client,
                target.task_supervisor.as_ref(),
                AttachRequest {
                    container_id: target.container_id,
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
        } else {
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
        }
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

async fn bridge_remote_exec_full_duplex<S>(
    mut client_spdy: SpdyExec,
    client_stream: S,
    streams: SpdyClientStreams,
    mut session: Box<dyn NodeExecSession>,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut client_read, mut client_write) = tokio::io::split(client_stream);
    let client_writer = SpdyExec::new();
    let mut pending_input: Option<klights_node_api::NodeExecFrame> = None;
    let mut pending_output: Option<klights_node_api::NodeExecFrame> = None;

    loop {
        tokio::select! {
            result = async {
                let frame = pending_input.as_ref().expect("guarded pending input");
                session.send_frame(frame.clone()).await
            }, if pending_input.is_some() => {
                if let Err(error) = result {
                    let _ = session.cancel().await;
                    return Err(error.into());
                }
                pending_input = None;
            }
            result = async {
                let frame: &klights_node_api::NodeExecFrame =
                    pending_output.as_ref().expect("guarded pending output");
                let channel = match frame.channel() {
                    ExecStreamChannel::Stdout => Some(StreamType::Stdout),
                    ExecStreamChannel::Stderr => Some(StreamType::Stderr),
                    ExecStreamChannel::Error => Some(StreamType::Error),
                    ExecStreamChannel::Stdin | ExecStreamChannel::Resize => None,
                };
                if let Some(channel) = channel {
                    write_spdy_exec_channel_frame(
                        &client_writer,
                        &mut client_write,
                        &streams,
                        channel,
                        frame.data(),
                        frame.fin(),
                    ).await?;
                }
                Ok::<bool, anyhow::Error>(frame.is_terminal())
            }, if pending_output.is_some() => {
                let terminal = match result {
                    Ok(terminal) => terminal,
                    Err(error) => {
                        let _ = session.cancel().await;
                        return Err(error);
                    }
                };
                pending_output = None;
                if terminal {
                    let _ = session.cancel().await;
                    return Ok(());
                }
            }
            inbound = client_spdy.read_frame(&mut client_read), if pending_input.is_none() => {
                let inbound = match inbound {
                    Ok(frame) => frame,
                    Err(error) => {
                        let _ = session.cancel().await;
                        return Err(error);
                    }
                };
                match inbound {
                    SpdyFrame::Data { stream_id, data, fin } => {
                        let channel = if Some(stream_id) == streams.stdin {
                            Some(ExecStreamChannel::Stdin)
                        } else if Some(stream_id) == streams.resize {
                            Some(ExecStreamChannel::Resize)
                        } else {
                            None
                        };
                        if let Some(channel) = channel {
                            pending_input = Some(klights_node_api::NodeExecFrame::new(
                                channel, data, fin,
                            ));
                        }
                    }
                    SpdyFrame::Ping { id } => {
                        if let Err(error) = client_writer.write_ping(&mut client_write, id).await {
                            let _ = session.cancel().await;
                            return Err(error);
                        }
                    }
                    SpdyFrame::RstStream { .. } | SpdyFrame::GoAway => {
                        let _ = session.cancel().await;
                        return Ok(());
                    }
                    SpdyFrame::SynReply { .. }
                    | SpdyFrame::Settings
                    | SpdyFrame::WindowUpdate { .. }
                    | SpdyFrame::Unknown
                    | SpdyFrame::SynStream { .. } => {}
                }
            }
            outbound = session.recv_frame(), if pending_output.is_none() => {
                let outbound = match outbound {
                    Ok(frame) => frame,
                    Err(error) => {
                        let _ = session.cancel().await;
                        return Err(error.into());
                    }
                };
                match outbound {
                    Some(frame) => pending_output = Some(frame),
                    None => {
                        let _ = session.cancel().await;
                        return Ok(());
                    }
                }
            }
        }
    }
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
        node_exec,
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

    let target = match NodeExecTarget::try_new(
        node_name,
        namespace.clone(),
        pod_name.clone(),
        container_id.clone(),
    ) {
        Ok(target) => target,
        Err(error) => {
            tracing::error!(%error, "Remote SPDY exec target validation failed");
            let _ = client_stream.shutdown().await;
            return;
        }
    };
    let options =
        NodeExecStreamOptions::new(request.stdin, request.stdout, request.stderr, request.tty);
    let node_request = if request.attach {
        NodeExecRequest::attach(target, options)
    } else {
        NodeExecRequest::exec(target, command.clone(), options)
    };
    let session = node_exec.open_exec(node_request).await;

    match session {
        Ok(session) => {
            if let Err(err) =
                bridge_remote_exec_full_duplex(client_spdy, client_stream, streams, session).await
            {
                tracing::error!(
                    "Remote SPDY exec failed: pod={}/{}, container={}, error={}",
                    namespace,
                    pod_name,
                    container_id,
                    err
                );
            }
            tracing::info!("Remote SPDY exec completed: pod={}/{}", namespace, pod_name);
            return;
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

#[cfg(test)]
mod remote_full_duplex_tests {
    use super::*;
    use klights_node_api::{
        BoundedByteStream, ByteStreamBounds, ByteStreamError, ByteStreamFuture, NodeExecFrame,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::mpsc;

    struct FakeSession {
        input_tx: mpsc::Sender<NodeExecFrame>,
        output_rx: tokio::sync::Mutex<mpsc::Receiver<NodeExecFrame>>,
        cancelled: std::sync::Arc<AtomicBool>,
    }

    impl BoundedByteStream for FakeSession {
        type Frame = NodeExecFrame;

        fn bounds(&self) -> ByteStreamBounds {
            ByteStreamBounds::try_new(1, 1).unwrap()
        }

        fn is_cancelled(&self) -> bool {
            self.cancelled.load(Ordering::Acquire)
        }

        fn send_frame(&self, frame: NodeExecFrame) -> ByteStreamFuture<'_, ()> {
            Box::pin(async move {
                self.input_tx
                    .send(frame)
                    .await
                    .map_err(|_| ByteStreamError::closed("input closed"))
            })
        }

        fn recv_frame(&self) -> ByteStreamFuture<'_, Option<NodeExecFrame>> {
            Box::pin(async move { Ok(self.output_rx.lock().await.recv().await) })
        }

        fn cancel(&mut self) -> ByteStreamFuture<'_, ()> {
            Box::pin(async move {
                self.cancelled.store(true, Ordering::Release);
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn remote_spdy_relays_input_and_output_while_input_is_backpressured() {
        let (server_io, mut client_io) = tokio::io::duplex(4096);
        let (input_tx, mut input_rx) = mpsc::channel(1);
        input_tx
            .send(NodeExecFrame::new(
                ExecStreamChannel::Stdin,
                b"occupied".to_vec(),
                false,
            ))
            .await
            .unwrap();
        let (output_tx, output_rx) = mpsc::channel(1);
        let cancelled = std::sync::Arc::new(AtomicBool::new(false));
        let session = Box::new(FakeSession {
            input_tx,
            output_rx: tokio::sync::Mutex::new(output_rx),
            cancelled: cancelled.clone(),
        });
        let streams = SpdyClientStreams {
            stdin: Some(1),
            stdout: Some(3),
            stderr: None,
            error: Some(5),
            resize: None,
        };
        let relay = tokio::spawn(bridge_remote_exec_full_duplex(
            SpdyExec::new(),
            server_io,
            streams,
            session,
        ));
        let client_spdy = SpdyExec::new();
        client_spdy
            .write_data_frame(&mut client_io, 1, b"blocked-input", false)
            .await
            .unwrap();
        output_tx
            .send(NodeExecFrame::new(
                ExecStreamChannel::Stdout,
                b"output-progress".to_vec(),
                false,
            ))
            .await
            .unwrap();

        let mut decoder = SpdyExec::new();
        let frame = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            decoder.read_frame(&mut client_io),
        )
        .await
        .expect("output must progress while input delivery is backpressured")
        .unwrap();
        assert!(
            matches!(frame, SpdyFrame::Data { stream_id: 3, ref data, fin: false } if data == b"output-progress")
        );

        assert_eq!(input_rx.recv().await.unwrap().data(), b"occupied");
        assert_eq!(input_rx.recv().await.unwrap().data(), b"blocked-input");
        drop(client_io);
        assert!(relay.await.unwrap().is_err());
        assert!(cancelled.load(Ordering::Acquire));
    }
}
