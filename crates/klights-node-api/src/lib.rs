//! Transport-neutral node-facing API contracts for klights.

use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;

use bytes::Bytes;

/// Transport limits needed by the kubelet's local containerd CRI channel.
///
/// Inter-node keepalive, TLS, lane, and RPC deadline policy deliberately
/// remain outside this node-facing value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CriTransportPolicy {
    connect_timeout: std::time::Duration,
    max_message_bytes: usize,
}

impl CriTransportPolicy {
    pub const fn new(connect_timeout: std::time::Duration, max_message_bytes: usize) -> Self {
        Self {
            connect_timeout,
            max_message_bytes,
        }
    }

    pub const fn connect_timeout(self) -> std::time::Duration {
        self.connect_timeout
    }

    pub const fn max_message_bytes(self) -> usize {
        self.max_message_bytes
    }
}

fn require_nonempty(value: &str, field: &'static str) -> Result<(), ExecSetupError> {
    if value.trim().is_empty() {
        Err(ExecSetupError::invalid(field, "must not be empty"))
    } else {
        Ok(())
    }
}

/// Exact node-local container identity for exec or attach setup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeExecTarget {
    node_name: String,
    namespace: String,
    pod_name: String,
    container_id: String,
}

impl NodeExecTarget {
    pub fn try_new(
        node_name: impl Into<String>,
        namespace: impl Into<String>,
        pod_name: impl Into<String>,
        container_id: impl Into<String>,
    ) -> Result<Self, ExecSetupError> {
        let target = Self {
            node_name: node_name.into(),
            namespace: namespace.into(),
            pod_name: pod_name.into(),
            container_id: container_id.into(),
        };
        require_nonempty(&target.node_name, "exec.node_name")?;
        require_nonempty(&target.namespace, "exec.namespace")?;
        require_nonempty(&target.pod_name, "exec.pod_name")?;
        require_nonempty(&target.container_id, "exec.container_id")?;
        Ok(target)
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn pod_name(&self) -> &str {
        &self.pod_name
    }

    pub fn container_id(&self) -> &str {
        &self.container_id
    }

    pub fn into_parts(self) -> (String, String, String, String) {
        (
            self.node_name,
            self.namespace,
            self.pod_name,
            self.container_id,
        )
    }
}

/// Validated non-interactive exec request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeExecSyncRequest {
    target: NodeExecTarget,
    command: Vec<String>,
    timeout_seconds: i64,
}

impl NodeExecSyncRequest {
    pub fn try_new(
        target: NodeExecTarget,
        command: Vec<String>,
        timeout_seconds: i64,
    ) -> Result<Self, ExecSetupError> {
        if timeout_seconds < 0 {
            return Err(ExecSetupError::invalid(
                "exec.timeout_seconds",
                "must be non-negative",
            ));
        }
        Ok(Self {
            target,
            command,
            timeout_seconds,
        })
    }

    pub const fn target(&self) -> &NodeExecTarget {
        &self.target
    }

    pub fn command(&self) -> &[String] {
        &self.command
    }

    pub const fn timeout_seconds(&self) -> i64 {
        self.timeout_seconds
    }

    pub fn into_parts(self) -> (NodeExecTarget, Vec<String>, i64) {
        (self.target, self.command, self.timeout_seconds)
    }
}

/// Runtime-reported terminal failure for an otherwise completed unary exec.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecTerminalError {
    message: String,
}

impl ExecTerminalError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn into_message(self) -> String {
        self.message
    }
}

impl fmt::Display for ExecTerminalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ExecTerminalError {}

/// Complete unary exec output. Runtime failure remains a terminal result so
/// stdout/stderr and the CRI-compatible exit code are never discarded.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeExecSyncResult {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    exit_code: i32,
    terminal_error: Option<ExecTerminalError>,
}

impl NodeExecSyncResult {
    pub fn success(stdout: Vec<u8>, stderr: Vec<u8>, exit_code: i32) -> Self {
        Self {
            stdout,
            stderr,
            exit_code,
            terminal_error: None,
        }
    }

    pub fn failed(
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        exit_code: i32,
        terminal_error: ExecTerminalError,
    ) -> Self {
        Self {
            stdout,
            stderr,
            exit_code,
            terminal_error: Some(terminal_error),
        }
    }

    pub fn stdout(&self) -> &[u8] {
        &self.stdout
    }

    pub fn stderr(&self) -> &[u8] {
        &self.stderr
    }

    pub const fn exit_code(&self) -> i32 {
        self.exit_code
    }

    pub const fn terminal_error(&self) -> Option<&ExecTerminalError> {
        self.terminal_error.as_ref()
    }

    pub fn into_parts(self) -> (Vec<u8>, Vec<u8>, i32, Option<ExecTerminalError>) {
        (
            self.stdout,
            self.stderr,
            self.exit_code,
            self.terminal_error,
        )
    }
}

/// Requested channels for a streaming exec or attach session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecStreamOptions {
    stdin: bool,
    stdout: bool,
    stderr: bool,
    tty: bool,
}

impl ExecStreamOptions {
    pub const fn new(stdin: bool, stdout: bool, stderr: bool, tty: bool) -> Self {
        Self {
            stdin,
            stdout,
            stderr,
            tty,
        }
    }

    pub const fn stdin(self) -> bool {
        self.stdin
    }

    pub const fn stdout(self) -> bool {
        self.stdout
    }

    pub const fn stderr(self) -> bool {
        self.stderr
    }

    pub const fn tty(self) -> bool {
        self.tty
    }
}

/// Validated streaming exec or attach setup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeExecRequest {
    target: NodeExecTarget,
    command: Vec<String>,
    options: ExecStreamOptions,
    attach: bool,
}

impl NodeExecRequest {
    pub fn exec(target: NodeExecTarget, command: Vec<String>, options: ExecStreamOptions) -> Self {
        Self {
            target,
            command,
            options,
            attach: false,
        }
    }

    pub fn attach(target: NodeExecTarget, options: ExecStreamOptions) -> Self {
        Self {
            target,
            command: Vec::new(),
            options,
            attach: true,
        }
    }

    pub const fn target(&self) -> &NodeExecTarget {
        &self.target
    }

    pub fn command(&self) -> &[String] {
        &self.command
    }

    pub const fn options(&self) -> ExecStreamOptions {
        self.options
    }

    pub const fn is_attach(&self) -> bool {
        self.attach
    }

    pub fn into_parts(self) -> (NodeExecTarget, Vec<String>, ExecStreamOptions, bool) {
        (self.target, self.command, self.options, self.attach)
    }
}

/// Semantic channel carried by one exec/attach byte frame.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ExecStreamChannel {
    Stdin,
    Stdout,
    Stderr,
    Error,
    Resize,
}

impl ExecStreamChannel {
    pub const fn as_wire_name(self) -> &'static str {
        match self {
            Self::Stdin => "stdin",
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
            Self::Error => "error",
            Self::Resize => "resize",
        }
    }

    pub fn try_from_wire_name(value: &str) -> Option<Self> {
        match value {
            "stdin" => Some(Self::Stdin),
            "stdout" => Some(Self::Stdout),
            "stderr" => Some(Self::Stderr),
            "error" => Some(Self::Error),
            "resize" => Some(Self::Resize),
            _ => None,
        }
    }
}

/// One owned byte frame. `Vec<u8>` preserves move-only wire and runtime paths
/// without imposing a transport buffer type or adding a copy at conversion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeExecFrame {
    channel: ExecStreamChannel,
    data: Vec<u8>,
    fin: bool,
}

impl NodeExecFrame {
    pub fn new(channel: ExecStreamChannel, data: Vec<u8>, fin: bool) -> Self {
        Self { channel, data, fin }
    }

    pub const fn channel(&self) -> ExecStreamChannel {
        self.channel
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub const fn fin(&self) -> bool {
        self.fin
    }

    pub fn is_terminal(&self) -> bool {
        self.channel == ExecStreamChannel::Error
            && (self.fin || exec_error_status_payload_is_terminal(&self.data))
    }

    pub fn into_parts(self) -> (ExecStreamChannel, Vec<u8>, bool) {
        (self.channel, self.data, self.fin)
    }
}

/// Kubernetes status objects on the error channel complete exec/attach even
/// when the underlying transport omits an explicit FIN bit.
pub fn exec_error_status_payload_is_terminal(data: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(data)
        .ok()
        .and_then(|value| {
            value
                .get("status")
                .and_then(|status| status.as_str())
                .map(|status| status == "Success" || status == "Failure")
        })
        .unwrap_or(false)
}

/// Finite queue limits visible at the session boundary. A full send queue
/// keeps `send_frame` pending until capacity is available; frames are neither
/// dropped nor buffered beyond this bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ByteStreamBounds {
    send_frames: NonZeroUsize,
    send_bytes: NonZeroUsize,
    receive_frames: NonZeroUsize,
    receive_bytes: NonZeroUsize,
}

impl ByteStreamBounds {
    pub fn try_new(
        send_frames: usize,
        receive_frames: usize,
    ) -> Result<Self, ByteStreamBoundsError> {
        let send_frames = NonZeroUsize::new(send_frames).ok_or_else(|| {
            ByteStreamBoundsError::invalid("byte_stream.send_frames", "must be non-zero")
        })?;
        let receive_frames = NonZeroUsize::new(receive_frames).ok_or_else(|| {
            ByteStreamBoundsError::invalid("byte_stream.receive_frames", "must be non-zero")
        })?;
        Ok(Self {
            send_frames,
            send_bytes: NonZeroUsize::MAX,
            receive_frames,
            receive_bytes: NonZeroUsize::MAX,
        })
    }

    pub fn try_new_with_bytes(
        send_frames: usize,
        send_bytes: usize,
        receive_frames: usize,
        receive_bytes: usize,
    ) -> Result<Self, ByteStreamBoundsError> {
        let send_frames = NonZeroUsize::new(send_frames).ok_or_else(|| {
            ByteStreamBoundsError::invalid("byte_stream.send_frames", "must be non-zero")
        })?;
        let send_bytes = NonZeroUsize::new(send_bytes).ok_or_else(|| {
            ByteStreamBoundsError::invalid("byte_stream.send_bytes", "must be non-zero")
        })?;
        let receive_frames = NonZeroUsize::new(receive_frames).ok_or_else(|| {
            ByteStreamBoundsError::invalid("byte_stream.receive_frames", "must be non-zero")
        })?;
        let receive_bytes = NonZeroUsize::new(receive_bytes).ok_or_else(|| {
            ByteStreamBoundsError::invalid("byte_stream.receive_bytes", "must be non-zero")
        })?;
        Ok(Self {
            send_frames,
            send_bytes,
            receive_frames,
            receive_bytes,
        })
    }

    pub const fn send_frames(self) -> NonZeroUsize {
        self.send_frames
    }

    pub const fn receive_frames(self) -> NonZeroUsize {
        self.receive_frames
    }

    pub const fn send_bytes(self) -> NonZeroUsize {
        self.send_bytes
    }

    pub const fn receive_bytes(self) -> NonZeroUsize {
        self.receive_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ByteStreamBoundsError {
    field: &'static str,
    message: String,
}

impl ByteStreamBoundsError {
    fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self {
            field,
            message: message.into(),
        }
    }

    pub const fn field(&self) -> &'static str {
        self.field
    }
}

impl fmt::Display for ByteStreamBoundsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid {}: {}", self.field, self.message)
    }
}

impl std::error::Error for ByteStreamBoundsError {}

#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecSetupError {
    InvalidRequest {
        field: &'static str,
        message: String,
    },
    Unavailable {
        message: String,
    },
    DuplicateSession {
        message: String,
    },
    Timeout {
        message: String,
    },
}

impl ExecSetupError {
    pub fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            field,
            message: message.into(),
        }
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable {
            message: message.into(),
        }
    }

    pub fn duplicate_session(message: impl Into<String>) -> Self {
        Self::DuplicateSession {
            message: message.into(),
        }
    }

    pub fn timeout(message: impl Into<String>) -> Self {
        Self::Timeout {
            message: message.into(),
        }
    }
}

impl fmt::Display for ExecSetupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { field, message } => {
                write!(formatter, "invalid {field}: {message}")
            }
            Self::Unavailable { message }
            | Self::DuplicateSession { message }
            | Self::Timeout { message } => formatter.write_str(message),
        }
    }
}

impl std::error::Error for ExecSetupError {}

#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ByteStreamError {
    Closed { message: String },
    Cancelled,
    Failed { message: String },
}

impl ByteStreamError {
    pub fn closed(message: impl Into<String>) -> Self {
        Self::Closed {
            message: message.into(),
        }
    }

    pub const fn cancelled() -> Self {
        Self::Cancelled
    }

    pub fn failed(message: impl Into<String>) -> Self {
        Self::Failed {
            message: message.into(),
        }
    }
}

impl fmt::Display for ByteStreamError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed { message } | Self::Failed { message } => formatter.write_str(message),
            Self::Cancelled => formatter.write_str("byte stream was cancelled"),
        }
    }
}

impl std::error::Error for ByteStreamError {}

pub type NodeExecFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ExecSetupError>> + Send + 'a>>;
pub type ByteStreamFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ByteStreamError>> + Send + 'a>>;
pub type NodeExecRuntimeFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Control-plane capability for unary exec and streaming exec/attach setup.
pub trait NodeExec: Send + Sync {
    fn exec_sync(&self, request: NodeExecSyncRequest) -> NodeExecFuture<'_, NodeExecSyncResult>;

    fn open_exec(&self, request: NodeExecRequest) -> NodeExecFuture<'_, Box<dyn NodeExecSession>>;
}

/// A transport-neutral frame with an owned byte payload.
pub trait ByteFrame: Send + Sync + 'static {
    fn payload(&self) -> &[u8];
}

impl ByteFrame for NodeExecFrame {
    fn payload(&self) -> &[u8] {
        self.data()
    }
}

/// Bounded bidirectional byte session shared by node-runtime capabilities.
/// Implementations must apply async backpressure at `bounds`, and `cancel`
/// must be idempotent and make later receives return
/// `ByteStreamError::Cancelled` or end the stream.
pub trait BoundedByteStream: Send + Sync {
    type Frame: ByteFrame;

    fn bounds(&self) -> ByteStreamBounds;
    fn is_cancelled(&self) -> bool;
    fn send_frame(&self, frame: Self::Frame) -> ByteStreamFuture<'_, ()>;
    /// Receive one frame. The shared receiver permits one task to drive a
    /// full-duplex relay without spawning unsupervised directional children.
    fn recv_frame(&self) -> ByteStreamFuture<'_, Option<Self::Frame>>;
    fn cancel(&mut self) -> ByteStreamFuture<'_, ()>;
}

/// Exec/attach specialization of the reusable bounded byte-stream contract.
pub trait NodeExecSession: BoundedByteStream<Frame = NodeExecFrame> {}

impl<T> NodeExecSession for T where T: BoundedByteStream<Frame = NodeExecFrame> + ?Sized {}

/// Node-local runtime capability consumed by a private transport adapter.
pub trait NodeExecRuntime: Send + Sync {
    fn exec_sync(
        &self,
        request: NodeExecSyncRequest,
    ) -> NodeExecRuntimeFuture<'_, NodeExecSyncResult>;

    fn exec_stream(
        &self,
        request: NodeExecRequest,
        session: Box<dyn NodeExecSession>,
    ) -> NodeExecRuntimeFuture<'_, ()>;
}

fn require_log_nonempty(value: &str, field: &'static str) -> Result<(), NodeLogSetupError> {
    if value.trim().is_empty() {
        Err(NodeLogSetupError::invalid(field, "must not be empty"))
    } else {
        Ok(())
    }
}

/// Exact node-local Pod container identity for a log request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeLogTarget {
    node_name: String,
    namespace: String,
    pod_name: String,
    pod_uid: String,
    container_name: String,
}

impl NodeLogTarget {
    pub fn try_new(
        node_name: impl Into<String>,
        namespace: impl Into<String>,
        pod_name: impl Into<String>,
        pod_uid: impl Into<String>,
        container_name: impl Into<String>,
    ) -> Result<Self, NodeLogSetupError> {
        let target = Self {
            node_name: node_name.into(),
            namespace: namespace.into(),
            pod_name: pod_name.into(),
            pod_uid: pod_uid.into(),
            container_name: container_name.into(),
        };
        require_log_nonempty(&target.node_name, "log.node_name")?;
        require_log_nonempty(&target.namespace, "log.namespace")?;
        require_log_nonempty(&target.pod_name, "log.pod_name")?;
        require_log_nonempty(&target.pod_uid, "log.pod_uid")?;
        require_log_nonempty(&target.container_name, "log.container_name")?;
        Ok(target)
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn pod_name(&self) -> &str {
        &self.pod_name
    }

    pub fn pod_uid(&self) -> &str {
        &self.pod_uid
    }

    pub fn container_name(&self) -> &str {
        &self.container_name
    }

    pub fn into_parts(self) -> (String, String, String, String, String) {
        (
            self.node_name,
            self.namespace,
            self.pod_name,
            self.pod_uid,
            self.container_name,
        )
    }
}

/// The existing Kubernetes Pod-log query fields carried to a node runtime.
/// String-valued booleans stay raw so adapters preserve today's exact parsing
/// and comparison behavior.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeLogOptions {
    follow: Option<String>,
    tail_lines: Option<usize>,
    timestamps: Option<String>,
    since_time: Option<String>,
    since_seconds: Option<i64>,
    limit_bytes: Option<usize>,
    previous: Option<String>,
}

impl NodeLogOptions {
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        follow: Option<String>,
        tail_lines: Option<usize>,
        timestamps: Option<String>,
        since_time: Option<String>,
        since_seconds: Option<i64>,
        limit_bytes: Option<usize>,
        previous: Option<String>,
    ) -> Self {
        Self {
            follow,
            tail_lines,
            timestamps,
            since_time,
            since_seconds,
            limit_bytes,
            previous,
        }
    }

    pub fn follow(&self) -> Option<&str> {
        self.follow.as_deref()
    }

    pub const fn tail_lines(&self) -> Option<usize> {
        self.tail_lines
    }

    pub fn timestamps(&self) -> Option<&str> {
        self.timestamps.as_deref()
    }

    pub fn since_time(&self) -> Option<&str> {
        self.since_time.as_deref()
    }

    pub const fn since_seconds(&self) -> Option<i64> {
        self.since_seconds
    }

    pub const fn limit_bytes(&self) -> Option<usize> {
        self.limit_bytes
    }

    pub fn previous(&self) -> Option<&str> {
        self.previous.as_deref()
    }

    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        Option<String>,
        Option<usize>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<usize>,
        Option<String>,
    ) {
        (
            self.follow,
            self.tail_lines,
            self.timestamps,
            self.since_time,
            self.since_seconds,
            self.limit_bytes,
            self.previous,
        )
    }
}

/// Transport-neutral Pod log setup request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeLogRequest {
    target: NodeLogTarget,
    options: NodeLogOptions,
}

impl NodeLogRequest {
    pub const fn new(target: NodeLogTarget, options: NodeLogOptions) -> Self {
        Self { target, options }
    }

    pub const fn target(&self) -> &NodeLogTarget {
        &self.target
    }

    pub const fn options(&self) -> &NodeLogOptions {
        &self.options
    }

    pub fn into_parts(self) -> (NodeLogTarget, NodeLogOptions) {
        (self.target, self.options)
    }
}

/// Runtime-reported terminal Pod log failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeLogTerminalError {
    message: String,
}

impl NodeLogTerminalError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn into_message(self) -> String {
        self.message
    }
}

impl fmt::Display for NodeLogTerminalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for NodeLogTerminalError {}

/// Complete finite Pod log output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeLogResult {
    content: Vec<u8>,
    terminal_error: Option<NodeLogTerminalError>,
}

impl NodeLogResult {
    pub fn success(content: Vec<u8>) -> Self {
        Self {
            content,
            terminal_error: None,
        }
    }

    pub fn failed(content: Vec<u8>, terminal_error: NodeLogTerminalError) -> Self {
        Self {
            content,
            terminal_error: Some(terminal_error),
        }
    }

    pub fn content(&self) -> &[u8] {
        &self.content
    }

    pub const fn terminal_error(&self) -> Option<&NodeLogTerminalError> {
        self.terminal_error.as_ref()
    }

    pub fn into_parts(self) -> (Vec<u8>, Option<NodeLogTerminalError>) {
        (self.content, self.terminal_error)
    }
}

/// One follow-stream data or terminal event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeLogEvent {
    content: Vec<u8>,
    terminal_error: Option<NodeLogTerminalError>,
    terminal: bool,
}

impl NodeLogEvent {
    pub fn data(content: Vec<u8>) -> Self {
        Self {
            content,
            terminal_error: None,
            terminal: false,
        }
    }

    pub fn terminal() -> Self {
        Self::complete(Vec::new())
    }

    pub fn complete(content: Vec<u8>) -> Self {
        Self {
            content,
            terminal_error: None,
            terminal: true,
        }
    }

    pub fn failed(content: Vec<u8>, terminal_error: NodeLogTerminalError) -> Self {
        Self {
            content,
            terminal_error: Some(terminal_error),
            terminal: true,
        }
    }

    pub fn content(&self) -> &[u8] {
        &self.content
    }

    pub const fn terminal_error(&self) -> Option<&NodeLogTerminalError> {
        self.terminal_error.as_ref()
    }

    pub const fn is_terminal(&self) -> bool {
        self.terminal
    }

    pub fn into_parts(self) -> (Vec<u8>, Option<NodeLogTerminalError>, bool) {
        (self.content, self.terminal_error, self.terminal)
    }
}

impl ByteFrame for NodeLogEvent {
    fn payload(&self) -> &[u8] {
        self.content()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeLogSetupError {
    InvalidRequest {
        field: &'static str,
        message: String,
    },
    Unavailable {
        message: String,
    },
    DuplicateStream {
        message: String,
    },
    Timeout {
        message: String,
    },
}

impl NodeLogSetupError {
    pub fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            field,
            message: message.into(),
        }
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable {
            message: message.into(),
        }
    }

    pub fn duplicate_stream(message: impl Into<String>) -> Self {
        Self::DuplicateStream {
            message: message.into(),
        }
    }

    pub fn timeout(message: impl Into<String>) -> Self {
        Self::Timeout {
            message: message.into(),
        }
    }
}

impl fmt::Display for NodeLogSetupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { field, message } => {
                write!(formatter, "invalid {field}: {message}")
            }
            Self::Unavailable { message }
            | Self::DuplicateStream { message }
            | Self::Timeout { message } => formatter.write_str(message),
        }
    }
}

impl std::error::Error for NodeLogSetupError {}

pub type NodeLogFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, NodeLogSetupError>> + Send + 'a>>;

/// Control-plane capability for finite and follow Pod log setup.
pub trait NodeLog: Send + Sync {
    fn read_logs(&self, request: NodeLogRequest) -> NodeLogFuture<'_, NodeLogResult>;

    fn open_logs(
        &self,
        request: NodeLogRequest,
    ) -> NodeLogFuture<'_, Box<dyn BoundedByteStream<Frame = NodeLogEvent>>>;
}

/// Node-local runtime capability consumed by a private transport adapter.
pub trait NodeLogRuntime: Send + Sync {
    fn read_logs(&self, request: NodeLogRequest) -> NodeLogFuture<'_, NodeLogResult>;

    fn open_logs(
        &self,
        request: NodeLogRequest,
    ) -> NodeLogFuture<'_, Box<dyn BoundedByteStream<Frame = NodeLogEvent>>>;
}

fn require_port_forward_nonempty(
    value: &str,
    field: &'static str,
) -> Result<(), NodePortForwardSetupError> {
    if value.trim().is_empty() {
        Err(NodePortForwardSetupError::invalid(
            field,
            "must not be empty",
        ))
    } else {
        Ok(())
    }
}

/// Pod network endpoint used by the node-local port-forward runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodePortForwardTarget {
    namespace: String,
    pod_name: String,
    pod_ip: String,
}

impl NodePortForwardTarget {
    pub fn try_new(
        namespace: impl Into<String>,
        pod_name: impl Into<String>,
        pod_ip: impl Into<String>,
    ) -> Result<Self, NodePortForwardSetupError> {
        let target = Self {
            namespace: namespace.into(),
            pod_name: pod_name.into(),
            pod_ip: pod_ip.into(),
        };
        require_port_forward_nonempty(&target.namespace, "port_forward.namespace")?;
        require_port_forward_nonempty(&target.pod_name, "port_forward.pod_name")?;
        require_port_forward_nonempty(&target.pod_ip, "port_forward.pod_ip")?;
        Ok(target)
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn pod_name(&self) -> &str {
        &self.pod_name
    }

    pub fn pod_ip(&self) -> &str {
        &self.pod_ip
    }

    pub fn into_parts(self) -> (String, String, String) {
        (self.namespace, self.pod_name, self.pod_ip)
    }
}

/// Ordered port-forward setup. Port positions are significant because the
/// Kubernetes streaming protocol assigns two channels to each request index.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodePortForwardRequest {
    target: NodePortForwardTarget,
    ports: Vec<u16>,
}

impl NodePortForwardRequest {
    pub fn try_new(
        target: NodePortForwardTarget,
        ports: Vec<u16>,
    ) -> Result<Self, NodePortForwardSetupError> {
        if ports.is_empty() {
            return Err(NodePortForwardSetupError::invalid(
                "port_forward.ports",
                "must not be empty",
            ));
        }
        if ports.len() > 128 {
            return Err(NodePortForwardSetupError::invalid(
                "port_forward.ports",
                "must contain at most 128 ports",
            ));
        }
        Ok(Self { target, ports })
    }

    pub const fn target(&self) -> &NodePortForwardTarget {
        &self.target
    }

    pub fn ports(&self) -> &[u16] {
        &self.ports
    }

    pub fn into_parts(self) -> (NodePortForwardTarget, Vec<u16>) {
        (self.target, self.ports)
    }
}

/// Semantic half of one Kubernetes port-forward channel pair.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum NodePortForwardChannel {
    Data,
    Error,
}

/// One transport-neutral port-forward byte frame. `port_index` refers to the
/// request's ordered port list; concrete transport channel IDs remain private
/// to the HTTP adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodePortForwardFrame {
    port_index: usize,
    channel: NodePortForwardChannel,
    data: Bytes,
}

impl NodePortForwardFrame {
    pub fn data(port_index: usize, data: impl Into<Bytes>) -> Self {
        Self {
            port_index,
            channel: NodePortForwardChannel::Data,
            data: data.into(),
        }
    }

    pub fn error(port_index: usize, data: impl Into<Bytes>) -> Self {
        Self {
            port_index,
            channel: NodePortForwardChannel::Error,
            data: data.into(),
        }
    }

    pub const fn port_index(&self) -> usize {
        self.port_index
    }

    pub const fn channel(&self) -> NodePortForwardChannel {
        self.channel
    }

    pub fn data_bytes(&self) -> &[u8] {
        &self.data
    }

    pub const fn is_error(&self) -> bool {
        matches!(self.channel, NodePortForwardChannel::Error)
    }

    pub fn into_parts(self) -> (usize, NodePortForwardChannel, Bytes) {
        (self.port_index, self.channel, self.data)
    }
}

impl ByteFrame for NodePortForwardFrame {
    fn payload(&self) -> &[u8] {
        self.data_bytes()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodePortForwardSetupError {
    InvalidRequest {
        field: &'static str,
        message: String,
    },
    Unavailable {
        message: String,
    },
    Timeout {
        message: String,
    },
}

impl NodePortForwardSetupError {
    pub fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            field,
            message: message.into(),
        }
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable {
            message: message.into(),
        }
    }

    pub fn timeout(message: impl Into<String>) -> Self {
        Self::Timeout {
            message: message.into(),
        }
    }
}

impl fmt::Display for NodePortForwardSetupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { field, message } => {
                write!(formatter, "invalid {field}: {message}")
            }
            Self::Unavailable { message } | Self::Timeout { message } => {
                formatter.write_str(message)
            }
        }
    }
}

impl std::error::Error for NodePortForwardSetupError {}

pub type NodePortForwardFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, NodePortForwardSetupError>> + Send + 'a>>;

/// Port-forward specialization of the reusable bounded byte-stream contract.
pub trait NodePortForwardSession: BoundedByteStream<Frame = NodePortForwardFrame> {}

impl<T> NodePortForwardSession for T where
    T: BoundedByteStream<Frame = NodePortForwardFrame> + ?Sized
{
}

/// Control-plane capability for opening one node-local port-forward session.
pub trait NodePortForward: Send + Sync {
    fn open_port_forward(
        &self,
        request: NodePortForwardRequest,
    ) -> NodePortForwardFuture<'_, Box<dyn NodePortForwardSession>>;
}

/// Node-local runtime capability consumed by the private control-plane adapter.
pub trait NodePortForwardRuntime: Send + Sync {
    fn open_port_forward(
        &self,
        request: NodePortForwardRequest,
    ) -> NodePortForwardFuture<'_, Box<dyn NodePortForwardSession>>;
}

fn require_metrics_nonempty(value: &str, field: &'static str) -> Result<(), NodeMetricsError> {
    if value.trim().is_empty() {
        Err(NodeMetricsError::invalid(field, "must not be empty"))
    } else {
        Ok(())
    }
}

/// Node selected for one runtime metrics sample.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeMetricsTarget {
    node_name: String,
}

impl NodeMetricsTarget {
    pub fn try_new(node_name: impl Into<String>) -> Result<Self, NodeMetricsError> {
        let target = Self {
            node_name: node_name.into(),
        };
        require_metrics_nonempty(&target.node_name, "metrics.node_name")?;
        Ok(target)
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn into_node_name(self) -> String {
        self.node_name
    }
}

/// Transport-neutral node metrics request. Correlation identity belongs to
/// the private adapter that routes this request and is intentionally absent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeMetricsRequest {
    target: NodeMetricsTarget,
    pod_uids: Vec<String>,
}

impl NodeMetricsRequest {
    pub fn new(target: NodeMetricsTarget, pod_uids: Vec<String>) -> Self {
        Self { target, pod_uids }
    }

    pub const fn target(&self) -> &NodeMetricsTarget {
        &self.target
    }

    pub fn pod_uids(&self) -> &[String] {
        &self.pod_uids
    }

    pub fn into_parts(self) -> (NodeMetricsTarget, Vec<String>) {
        (self.target, self.pod_uids)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeMetricsNodeSample {
    cpu_nanos: u64,
    memory_bytes: u64,
}

impl NodeMetricsNodeSample {
    pub const fn new(cpu_nanos: u64, memory_bytes: u64) -> Self {
        Self {
            cpu_nanos,
            memory_bytes,
        }
    }

    pub const fn cpu_nanos(self) -> u64 {
        self.cpu_nanos
    }

    pub const fn memory_bytes(self) -> u64 {
        self.memory_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeMetricsContainerSample {
    name: String,
    cpu_nanos: u64,
    memory_bytes: u64,
}

impl NodeMetricsContainerSample {
    pub fn new(name: impl Into<String>, cpu_nanos: u64, memory_bytes: u64) -> Self {
        Self {
            name: name.into(),
            cpu_nanos,
            memory_bytes,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn cpu_nanos(&self) -> u64 {
        self.cpu_nanos
    }

    pub const fn memory_bytes(&self) -> u64 {
        self.memory_bytes
    }

    pub fn into_parts(self) -> (String, u64, u64) {
        (self.name, self.cpu_nanos, self.memory_bytes)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeMetricsPodSample {
    namespace: String,
    name: String,
    uid: String,
    containers: Vec<NodeMetricsContainerSample>,
}

impl NodeMetricsPodSample {
    pub fn new(
        namespace: impl Into<String>,
        name: impl Into<String>,
        uid: impl Into<String>,
        containers: Vec<NodeMetricsContainerSample>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
            uid: uid.into(),
            containers,
        }
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn uid(&self) -> &str {
        &self.uid
    }

    pub fn containers(&self) -> &[NodeMetricsContainerSample] {
        &self.containers
    }

    pub fn into_parts(self) -> (String, String, String, Vec<NodeMetricsContainerSample>) {
        (self.namespace, self.name, self.uid, self.containers)
    }
}

/// Complete successful sample for one node. Node-only results intentionally
/// retain an empty Pod sample list when CRI statistics are unavailable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeMetricsResult {
    target: NodeMetricsTarget,
    node: Option<NodeMetricsNodeSample>,
    pods: Vec<NodeMetricsPodSample>,
}

impl NodeMetricsResult {
    pub fn new(
        target: NodeMetricsTarget,
        node: Option<NodeMetricsNodeSample>,
        pods: Vec<NodeMetricsPodSample>,
    ) -> Self {
        Self { target, node, pods }
    }

    pub const fn target(&self) -> &NodeMetricsTarget {
        &self.target
    }

    pub const fn node(&self) -> Option<&NodeMetricsNodeSample> {
        self.node.as_ref()
    }

    pub fn pods(&self) -> &[NodeMetricsPodSample] {
        &self.pods
    }

    pub fn into_parts(
        self,
    ) -> (
        NodeMetricsTarget,
        Option<NodeMetricsNodeSample>,
        Vec<NodeMetricsPodSample>,
    ) {
        (self.target, self.node, self.pods)
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeMetricsError {
    InvalidRequest {
        field: &'static str,
        message: String,
    },
    Unavailable {
        message: String,
    },
    DuplicateRequest {
        message: String,
    },
    Timeout {
        message: String,
    },
    Closed {
        message: String,
    },
}

impl NodeMetricsError {
    pub fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            field,
            message: message.into(),
        }
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable {
            message: message.into(),
        }
    }

    pub fn duplicate_request(message: impl Into<String>) -> Self {
        Self::DuplicateRequest {
            message: message.into(),
        }
    }

    pub fn timeout(message: impl Into<String>) -> Self {
        Self::Timeout {
            message: message.into(),
        }
    }

    pub fn closed(message: impl Into<String>) -> Self {
        Self::Closed {
            message: message.into(),
        }
    }
}

impl fmt::Display for NodeMetricsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { field, message } => {
                write!(formatter, "invalid {field}: {message}")
            }
            Self::Unavailable { message }
            | Self::DuplicateRequest { message }
            | Self::Timeout { message }
            | Self::Closed { message } => formatter.write_str(message),
        }
    }
}

impl std::error::Error for NodeMetricsError {}

pub type NodeMetricsFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, NodeMetricsError>> + Send + 'a>>;

/// Control-plane capability for sampling one node runtime.
pub trait NodeMetrics: Send + Sync {
    fn collect_metrics(
        &self,
        request: NodeMetricsRequest,
    ) -> NodeMetricsFuture<'_, NodeMetricsResult>;
}

/// Node-local runtime capability consumed by a private transport adapter.
pub trait NodeMetricsRuntime: Send + Sync {
    fn collect_metrics(
        &self,
        request: NodeMetricsRequest,
    ) -> NodeMetricsFuture<'_, NodeMetricsResult>;
}

/// Focused sampler injected into the node runtime transport adapter.
pub trait NodeMetricsSampler: Send + Sync {
    fn sample_metrics(
        &self,
        request: NodeMetricsRequest,
    ) -> NodeMetricsFuture<'_, NodeMetricsResult>;
}
