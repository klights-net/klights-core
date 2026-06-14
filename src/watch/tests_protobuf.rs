//! Protobuf watch frame format tests (F5-01).
//!
//! Tests that ADDED / MODIFIED / DELETED / BOOKMARK events can be encoded
//! to protobuf frames and decoded back, preserving event type and resource.

use crate::protobuf::encode_protobuf_resource;
use crate::watch::WatchEvent;
use bytes::{Buf, BytesMut};
use k8s_pb::apimachinery::pkg::apis::meta::v1::WatchEvent as PbWatchEvent;
use k8s_pb::apimachinery::pkg::runtime::RawExtension;
use prost::Message;
use serde_json::json;

/// Encode a WatchEvent to a protobuf frame.
/// Format: 4-byte big-endian length prefix + protobuf WatchEvent message.
fn encode_watch_event_frame(event: &WatchEvent) -> anyhow::Result<Vec<u8>> {
    // Get the object kind
    let kind = event
        .object
        .get("kind")
        .and_then(|k| k.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing kind"))?;

    // Encode the object to protobuf
    let object_pb = encode_protobuf_resource(kind, &event.object)?;

    // Build the WatchEvent protobuf message
    let pb_event = PbWatchEvent {
        r#type: Some(event.event_type.to_string()),
        object: Some(RawExtension {
            raw: Some(object_pb),
        }),
    };

    // Encode the WatchEvent
    let event_bytes = pb_event.encode_to_vec();

    // Prepend 4-byte big-endian length
    let mut frame = Vec::with_capacity(4 + event_bytes.len());
    frame.extend_from_slice(&(event_bytes.len() as u32).to_be_bytes());
    frame.extend(event_bytes);

    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that a Pod ADDED event can be encoded to a protobuf frame.
    #[test]
    fn protobuf_watch_frame_pod_added() {
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "test-pod",
                "namespace": "default",
                "uid": "test-uid-123",
                "resourceVersion": "42"
            },
            "spec": {
                "containers": [{
                    "name": "nginx",
                    "image": "nginx:latest"
                }]
            },
            "status": {
                "phase": "Pending"
            }
        });

        let event = WatchEvent::added(pod);

        // Encode to protobuf frame
        let frame = encode_watch_event_frame(&event).expect("encode watch event frame");

        // Verify frame structure
        assert!(frame.len() > 4, "frame should have length prefix + data");

        // Decode length prefix
        let mut buf = &frame[..];
        let len = buf.get_u32() as usize;
        assert_eq!(frame.len(), 4 + len, "frame length should match prefix");

        // Verify we can decode the protobuf WatchEvent
        let pb_event = PbWatchEvent::decode(&frame[4..]).expect("decode protobuf WatchEvent");
        assert_eq!(pb_event.r#type, Some("ADDED".to_string()));
        assert!(pb_event.object.is_some(), "object should be present");
    }

    /// Test that a ConfigMap MODIFIED event can be encoded.
    #[test]
    fn protobuf_watch_frame_configmap_modified() {
        let cm = json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "test-config",
                "namespace": "default",
                "uid": "cm-uid-456",
                "resourceVersion": "100"
            },
            "data": {
                "key": "value"
            }
        });

        let event = WatchEvent::modified(cm);
        let frame = encode_watch_event_frame(&event).expect("encode watch event frame");

        let pb_event = PbWatchEvent::decode(&frame[4..]).expect("decode protobuf WatchEvent");
        assert_eq!(pb_event.r#type, Some("MODIFIED".to_string()));
    }

    /// Test that a Node DELETED event can be encoded.
    #[test]
    fn protobuf_watch_frame_node_deleted() {
        let node = json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {
                "name": "test-node",
                "uid": "node-uid-789",
                "resourceVersion": "200"
            }
        });

        let event = WatchEvent::deleted(node);
        let frame = encode_watch_event_frame(&event).expect("encode watch event frame");

        let pb_event = PbWatchEvent::decode(&frame[4..]).expect("decode protobuf WatchEvent");
        assert_eq!(pb_event.r#type, Some("DELETED".to_string()));
    }

    /// Test that a BOOKMARK event can be encoded.
    #[test]
    fn protobuf_watch_frame_bookmark() {
        let bookmark = WatchEvent::bookmark_typed(999, "v1", "Pod");

        let frame = encode_watch_event_frame(&bookmark).expect("encode bookmark frame");

        let pb_event = PbWatchEvent::decode(&frame[4..]).expect("decode protobuf WatchEvent");
        assert_eq!(pb_event.r#type, Some("BOOKMARK".to_string()));
    }

    /// Test that multiple events can be encoded and decoded in sequence.
    #[test]
    fn protobuf_watch_frame_multiple_events() {
        let events: Vec<WatchEvent> = vec![
            WatchEvent::added(json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "pod-1", "namespace": "default", "uid": "uid-1", "resourceVersion": "1"}
            })),
            WatchEvent::modified(json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "pod-1", "namespace": "default", "uid": "uid-1", "resourceVersion": "2"}
            })),
            WatchEvent::deleted(json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "pod-1", "namespace": "default", "uid": "uid-1", "resourceVersion": "3"}
            })),
        ];

        // Encode all events into a single buffer
        let mut buffer = BytesMut::new();
        for event in &events {
            let frame = encode_watch_event_frame(event).expect("encode event");
            buffer.extend_from_slice(&frame);
        }

        // Decode events back
        let mut offset = 0;
        let mut decoded_count = 0;
        let data = buffer.freeze();

        while offset < data.len() {
            let pb_event = {
                let len = u32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]) as usize;
                PbWatchEvent::decode(&data[offset + 4..offset + 4 + len]).expect("decode event")
            };
            offset += 4 + pb_event.encoded_len();

            match decoded_count {
                0 => assert_eq!(pb_event.r#type, Some("ADDED".to_string())),
                1 => assert_eq!(pb_event.r#type, Some("MODIFIED".to_string())),
                2 => assert_eq!(pb_event.r#type, Some("DELETED".to_string())),
                _ => {}
            }
            decoded_count += 1;
        }

        assert_eq!(decoded_count, 3, "should decode all 3 events");
    }

    /// Test frame length encoding is correct.
    #[test]
    fn protobuf_watch_frame_length_prefix() {
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "test", "namespace": "default", "uid": "uid", "resourceVersion": "1"}
        });

        let event = WatchEvent::added(pod);
        let frame = encode_watch_event_frame(&event).expect("encode");

        // Extract and verify length prefix
        let len = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;

        // The length should be the size of the protobuf WatchEvent
        let pb_event = PbWatchEvent::decode(&frame[4..]).unwrap();
        assert_eq!(
            len,
            pb_event.encoded_len(),
            "length prefix should match encoded size"
        );
        assert_eq!(
            frame.len(),
            4 + len,
            "frame should be exactly length prefix + data"
        );
    }
}
