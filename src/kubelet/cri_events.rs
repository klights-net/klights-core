//! Typed CRI container-lifecycle event + decoder (P0-LEAK-03).
//!
//! containerd's `ContainerEventResponse` is a low-level wire-format
//! struct with optional fields and a numeric `container_event_type`
//! enum. The pod_watcher's event-loop arm previously matched on the
//! raw int and re-derived the event semantics inline; that made it
//! easy for new event topics to leak through unhandled and made
//! testing the decode path require a real containerd.
//!
//! [`KubeletEvent`] is the typed shape the pod_watcher reasons about,
//! and [`KubeletEvent::from_cri`] is the single place where wire-
//! format quirks live (e.g. the known `PodSandboxMetadata.namespace`
//! decode failure on certain containerd versions — handled by
//! falling back to just `container_id` when the metadata is absent).

use bytes::Buf;
use k8s_cri::v1::{ContainerEventType, GetEventsRequest};
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::{Code, Status};

/// CRI container-lifecycle event wire type.
///
/// `k8s-cri 0.8` generated `ContainerEventResponse` from the older CRI shape
/// where field 4 is `PodSandboxMetadata`. Kubernetes CRI v1.29+ changed field 4
/// to `PodSandboxStatus` and added field 5 `containers_statuses`. Decoding the
/// current wire shape into the old generated type fails the whole tonic stream.
///
/// Keep field 4/5 as raw length-delimited bytes so the stream remains decodable
/// across both shapes, then parse just the pod metadata we need below.
#[derive(Clone, Debug, PartialEq)]
pub struct CriContainerEventResponse {
    pub container_id: String,
    pub container_event_type: i32,
    pub created_at: i64,
    pod_sandbox: Vec<u8>,
    containers_statuses: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, Default, PartialEq)]
struct CompatPodSandboxStatus {
    id: String,
    metadata: Option<CompatPodSandboxMetadata>,
}

#[derive(Clone, Debug, Default, PartialEq)]
struct CompatPodSandboxMetadata {
    name: String,
    uid: String,
    namespace: String,
    attempt: u32,
}

/// Tonic codec for CRI `GetContainerEvents`.
///
/// The request is an empty protobuf message. The response is decoded manually so
/// klights is insulated from the CRI field-4 shape change described above while
/// still using the normal gRPC transport/framing.
#[derive(Clone, Debug, Default)]
pub struct CriContainerEventCodec;

#[derive(Clone, Debug, Default)]
pub struct CriContainerEventEncoder;

#[derive(Clone, Debug, Default)]
pub struct CriContainerEventDecoder;

impl Codec for CriContainerEventCodec {
    type Encode = GetEventsRequest;
    type Decode = CriContainerEventResponse;
    type Encoder = CriContainerEventEncoder;
    type Decoder = CriContainerEventDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        CriContainerEventEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        CriContainerEventDecoder
    }
}

impl Encoder for CriContainerEventEncoder {
    type Item = GetEventsRequest;
    type Error = Status;

    fn encode(&mut self, _item: Self::Item, _dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl Decoder for CriContainerEventDecoder {
    type Item = CriContainerEventResponse;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        let bytes = src.copy_to_bytes(src.remaining());
        CriContainerEventResponse::decode_wire(&bytes)
            .map(Some)
            .map_err(|e| Status::new(Code::Internal, e))
    }
}

impl CriContainerEventResponse {
    #[cfg(test)]
    fn with_legacy_metadata(
        kind: ContainerEventType,
        container_id: &str,
        metadata: Option<CompatPodSandboxMetadata>,
    ) -> Self {
        Self {
            container_id: container_id.to_string(),
            container_event_type: kind as i32,
            created_at: 1_000_000_000,
            pod_sandbox: metadata.map(encode_metadata).unwrap_or_default(),
            containers_statuses: Vec::new(),
        }
    }

    fn decode_wire(bytes: &[u8]) -> Result<Self, String> {
        let mut cursor = bytes;
        let mut event = Self {
            container_id: String::new(),
            container_event_type: 0,
            created_at: 0,
            pod_sandbox: Vec::new(),
            containers_statuses: Vec::new(),
        };

        while !cursor.is_empty() {
            let (field, wire_type) = read_key(&mut cursor)?;
            match (field, wire_type) {
                (1, 2) => event.container_id = read_string(&mut cursor)?,
                (2, 0) => event.container_event_type = read_varint(&mut cursor)? as i32,
                (3, 0) => event.created_at = read_varint(&mut cursor)? as i64,
                (4, 2) => event.pod_sandbox = read_len_delimited(&mut cursor)?.to_vec(),
                (5, 2) => event
                    .containers_statuses
                    .push(read_len_delimited(&mut cursor)?.to_vec()),
                (_, other) => skip_field(&mut cursor, other)?,
            }
        }

        Ok(event)
    }

    fn pod_metadata(&self) -> Option<CompatPodSandboxMetadata> {
        if self.pod_sandbox.is_empty() {
            return None;
        }

        if let Some(status) = decode_pod_sandbox_status(&self.pod_sandbox)
            && let Some(metadata) = status.metadata
            && (!metadata.name.is_empty()
                || !metadata.namespace.is_empty()
                || !metadata.uid.is_empty())
        {
            return Some(metadata);
        }

        decode_metadata(&self.pod_sandbox)
    }
}

fn decode_pod_sandbox_status(bytes: &[u8]) -> Option<CompatPodSandboxStatus> {
    let mut cursor = bytes;
    let mut status = CompatPodSandboxStatus::default();
    while !cursor.is_empty() {
        let (field, wire_type) = read_key(&mut cursor).ok()?;
        match (field, wire_type) {
            (1, 2) => status.id = read_string(&mut cursor).ok()?,
            (2, 2) => {
                let metadata_bytes = read_len_delimited(&mut cursor).ok()?;
                status.metadata = decode_metadata(metadata_bytes);
            }
            (_, other) => skip_field(&mut cursor, other).ok()?,
        }
    }
    Some(status)
}

fn decode_metadata(bytes: &[u8]) -> Option<CompatPodSandboxMetadata> {
    let mut cursor = bytes;
    let mut metadata = CompatPodSandboxMetadata::default();
    while !cursor.is_empty() {
        let (field, wire_type) = read_key(&mut cursor).ok()?;
        match (field, wire_type) {
            (1, 2) => metadata.name = read_string(&mut cursor).ok()?,
            (2, 2) => metadata.uid = read_string(&mut cursor).ok()?,
            (3, 2) => metadata.namespace = read_string(&mut cursor).ok()?,
            (4, 0) => metadata.attempt = read_varint(&mut cursor).ok()? as u32,
            (_, other) => skip_field(&mut cursor, other).ok()?,
        }
    }
    Some(metadata)
}

fn read_key(cursor: &mut &[u8]) -> Result<(u64, u8), String> {
    let key = read_varint(cursor)?;
    let field = key >> 3;
    if field == 0 {
        return Err("protobuf event contained field number 0".to_string());
    }
    Ok((field, (key & 0x07) as u8))
}

fn read_varint(cursor: &mut &[u8]) -> Result<u64, String> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let Some((&byte, rest)) = cursor.split_first() else {
            return Err("truncated protobuf varint".to_string());
        };
        *cursor = rest;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err("protobuf varint overflow".to_string())
}

fn read_len_delimited<'a>(cursor: &mut &'a [u8]) -> Result<&'a [u8], String> {
    let len = read_varint(cursor)? as usize;
    if cursor.len() < len {
        return Err("truncated protobuf length-delimited field".to_string());
    }
    let (value, rest) = cursor.split_at(len);
    *cursor = rest;
    Ok(value)
}

fn read_string(cursor: &mut &[u8]) -> Result<String, String> {
    let bytes = read_len_delimited(cursor)?;
    std::str::from_utf8(bytes)
        .map(str::to_string)
        .map_err(|e| format!("invalid protobuf utf-8 string: {e}"))
}

fn skip_field(cursor: &mut &[u8], wire_type: u8) -> Result<(), String> {
    match wire_type {
        0 => {
            let _ = read_varint(cursor)?;
        }
        1 => {
            if cursor.len() < 8 {
                return Err("truncated protobuf fixed64 field".to_string());
            }
            *cursor = &cursor[8..];
        }
        2 => {
            let _ = read_len_delimited(cursor)?;
        }
        5 => {
            if cursor.len() < 4 {
                return Err("truncated protobuf fixed32 field".to_string());
            }
            *cursor = &cursor[4..];
        }
        _ => return Err(format!("unsupported protobuf wire type {wire_type}")),
    }
    Ok(())
}

#[cfg(test)]
fn encode_metadata(metadata: CompatPodSandboxMetadata) -> Vec<u8> {
    let mut out = Vec::new();
    encode_string_field(&mut out, 1, &metadata.name);
    encode_string_field(&mut out, 2, &metadata.uid);
    encode_string_field(&mut out, 3, &metadata.namespace);
    encode_varint_field(&mut out, 4, u64::from(metadata.attempt));
    out
}

#[cfg(test)]
fn encode_key(out: &mut Vec<u8>, field: u64, wire_type: u8) {
    encode_varint(out, (field << 3) | u64::from(wire_type));
}

#[cfg(test)]
fn encode_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

#[cfg(test)]
fn encode_varint_field(out: &mut Vec<u8>, field: u64, value: u64) {
    encode_key(out, field, 0);
    encode_varint(out, value);
}

#[cfg(test)]
fn encode_bytes_field(out: &mut Vec<u8>, field: u64, value: &[u8]) {
    encode_key(out, field, 2);
    encode_varint(out, value.len() as u64);
    out.extend_from_slice(value);
}

#[cfg(test)]
fn encode_string_field(out: &mut Vec<u8>, field: u64, value: &str) {
    encode_bytes_field(out, field, value.as_bytes());
}

/// Kind of container-lifecycle transition we react to.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum KubeletEventKind {
    Created,
    Started,
    Stopped,
    Deleted,
}

impl KubeletEventKind {
    fn from_raw(raw: i32) -> Option<Self> {
        match ContainerEventType::try_from(raw).ok()? {
            ContainerEventType::ContainerCreatedEvent => Some(Self::Created),
            ContainerEventType::ContainerStartedEvent => Some(Self::Started),
            ContainerEventType::ContainerStoppedEvent => Some(Self::Stopped),
            ContainerEventType::ContainerDeletedEvent => Some(Self::Deleted),
        }
    }

    /// Short label for tracing.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Started => "started",
            Self::Stopped => "stopped",
            Self::Deleted => "deleted",
        }
    }
}

/// Decoded container-lifecycle event from containerd.
///
/// `pod_namespace` and `pod_name` are populated when containerd's
/// `pod_sandbox_metadata` field decodes correctly. Some containerd
/// versions emit a malformed `PodSandboxMetadata.namespace` (varint
/// where length-delimited is expected) which makes the metadata
/// field absent at the kubelet — see the protobuf decode warning
/// the pod_watcher logs and reconnects on. In that case the event
/// still arrives with a usable `container_id`; the kubelet looks up
/// the owning pod via CRI (`list_containers` filtered by id).
#[derive(Debug, Clone)]
pub struct KubeletEvent {
    pub kind: KubeletEventKind,
    pub container_id: String,
    pub pod_namespace: Option<String>,
    pub pod_name: Option<String>,
    /// Carried for diagnostics/future stale-uid handling. Not consumed by
    /// the current event handler — the (namespace, name) pair is enough to
    /// reach the live Pod object in the datastore.
    pub pod_uid: Option<String>,
    /// Event creation timestamp from containerd (nanoseconds). Carried for
    /// future ordering / latency-tracing use; the handler doesn't need it
    /// today since per-pod reconciliation always reads fresh CRI state.
    pub timestamp_ns: i64,
}

impl KubeletEvent {
    /// Decode a `ContainerEventResponse` into the typed shape.
    /// Returns `None` for event types we don't react to (so the
    /// caller can drop them silently without per-event branching).
    pub fn from_cri(raw: CriContainerEventResponse) -> Option<Self> {
        let kind = KubeletEventKind::from_raw(raw.container_event_type)?;
        let (pod_namespace, pod_name, pod_uid) = match raw.pod_metadata() {
            Some(meta) => (
                non_empty(meta.namespace),
                non_empty(meta.name),
                non_empty(meta.uid),
            ),
            None => (None, None, None),
        };
        Some(Self {
            kind,
            container_id: raw.container_id,
            pod_namespace,
            pod_name,
            pod_uid,
            timestamp_ns: raw.created_at,
        })
    }
}

fn non_empty(s: String) -> Option<String> {
    if s.is_empty() { None } else { Some(s) }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_raw(
        kind: ContainerEventType,
        container_id: &str,
        meta: Option<CompatPodSandboxMetadata>,
    ) -> CriContainerEventResponse {
        CriContainerEventResponse::with_legacy_metadata(kind, container_id, meta)
    }

    fn meta(ns: &str, name: &str, uid: &str) -> CompatPodSandboxMetadata {
        CompatPodSandboxMetadata {
            namespace: ns.to_string(),
            name: name.to_string(),
            uid: uid.to_string(),
            attempt: 0,
        }
    }

    #[test]
    fn compat_cri_event_decoder_accepts_current_pod_sandbox_status_wire() {
        let metadata = encode_metadata(meta("default", "log-test", "uid-1"));
        let mut pod_sandbox_status = Vec::new();
        encode_string_field(&mut pod_sandbox_status, 1, "sandbox123");
        encode_bytes_field(&mut pod_sandbox_status, 2, &metadata);

        let mut encoded = Vec::new();
        encode_string_field(&mut encoded, 1, "abc123");
        encode_varint_field(
            &mut encoded,
            2,
            ContainerEventType::ContainerStoppedEvent as u64,
        );
        encode_varint_field(&mut encoded, 3, 1_000_000_000);
        encode_bytes_field(&mut encoded, 4, &pod_sandbox_status);

        let decoded = CriContainerEventResponse::decode_wire(encoded.as_slice())
            .expect("compat event decoder must accept CRI v1.29+ PodSandboxStatus payloads");
        let ev = KubeletEvent::from_cri(decoded).unwrap();
        assert_eq!(ev.kind, KubeletEventKind::Stopped);
        assert_eq!(ev.container_id, "abc123");
        assert_eq!(ev.pod_namespace.as_deref(), Some("default"));
        assert_eq!(ev.pod_name.as_deref(), Some("log-test"));
        assert_eq!(ev.pod_uid.as_deref(), Some("uid-1"));
    }

    #[test]
    fn decode_started_with_metadata() {
        let raw = make_raw(
            ContainerEventType::ContainerStartedEvent,
            "abc123",
            Some(meta("default", "nginx", "uid-1")),
        );
        let ev = KubeletEvent::from_cri(raw).unwrap();
        assert_eq!(ev.kind, KubeletEventKind::Started);
        assert_eq!(ev.container_id, "abc123");
        assert_eq!(ev.pod_namespace.as_deref(), Some("default"));
        assert_eq!(ev.pod_name.as_deref(), Some("nginx"));
        assert_eq!(ev.pod_uid.as_deref(), Some("uid-1"));
    }

    #[test]
    fn decode_stopped_without_metadata() {
        // Containerd's protobuf-decode-failure path: metadata absent.
        let raw = make_raw(ContainerEventType::ContainerStoppedEvent, "xyz789", None);
        let ev = KubeletEvent::from_cri(raw).unwrap();
        assert_eq!(ev.kind, KubeletEventKind::Stopped);
        assert_eq!(ev.container_id, "xyz789");
        assert!(ev.pod_namespace.is_none());
        assert!(ev.pod_name.is_none());
        assert!(ev.pod_uid.is_none());
    }

    #[test]
    fn decode_kind_for_each_topic() {
        for (raw_kind, expected) in [
            (
                ContainerEventType::ContainerCreatedEvent,
                KubeletEventKind::Created,
            ),
            (
                ContainerEventType::ContainerStartedEvent,
                KubeletEventKind::Started,
            ),
            (
                ContainerEventType::ContainerStoppedEvent,
                KubeletEventKind::Stopped,
            ),
            (
                ContainerEventType::ContainerDeletedEvent,
                KubeletEventKind::Deleted,
            ),
        ] {
            let raw = make_raw(raw_kind, "id", None);
            let ev = KubeletEvent::from_cri(raw).expect("known kind decodes");
            assert_eq!(ev.kind, expected);
        }
    }

    #[test]
    fn empty_metadata_strings_become_none() {
        let raw = make_raw(
            ContainerEventType::ContainerCreatedEvent,
            "id",
            Some(meta("", "", "")),
        );
        let ev = KubeletEvent::from_cri(raw).unwrap();
        assert!(ev.pod_namespace.is_none());
        assert!(ev.pod_name.is_none());
        assert!(ev.pod_uid.is_none());
    }

    #[test]
    fn kubelet_event_kind_as_str() {
        assert_eq!(KubeletEventKind::Created.as_str(), "created");
        assert_eq!(KubeletEventKind::Started.as_str(), "started");
        assert_eq!(KubeletEventKind::Stopped.as_str(), "stopped");
        assert_eq!(KubeletEventKind::Deleted.as_str(), "deleted");
    }

    #[test]
    fn unknown_kind_decodes_to_none() {
        // i32 outside ContainerEventType discriminants.
        let raw = CriContainerEventResponse {
            container_id: "id".to_string(),
            container_event_type: 9999,
            created_at: 0,
            pod_sandbox: Vec::new(),
            containers_statuses: Vec::new(),
        };
        assert!(KubeletEvent::from_cri(raw).is_none());
    }
}
