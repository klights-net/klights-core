//! Kubernetes runtime.Unknown and watch-stream framing.

use prost::Message;

use crate::apimachinery::pkg::apis::meta::v1::WatchEvent;
use crate::apimachinery::pkg::runtime::RawExtension;

/// Encode one length-prefixed protobuf watch event.
///
/// The outer stream payload is a bare `WatchEvent`, as required by the
/// Kubernetes raw stream serializer. `object_envelope` is the normal `k8s\0`
/// runtime.Unknown envelope for the embedded resource.
pub fn encode_watch_event_frame(event_type: &str, object_envelope: Vec<u8>) -> Vec<u8> {
    let event = WatchEvent {
        r#type: Some(event_type.to_string()),
        object: Some(RawExtension {
            raw: Some(object_envelope),
        }),
    };
    let event_bytes = event.encode_to_vec();
    let mut frame = Vec::with_capacity(4 + event_bytes.len());
    frame.extend_from_slice(&(event_bytes.len() as u32).to_be_bytes());
    frame.extend(event_bytes);
    frame
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TypeMeta, Unknown};

    #[test]
    fn watch_frame_has_raw_outer_event_and_enveloped_object() {
        let object_envelope =
            crate::wrap_protobuf_resource_envelope("v1", "Pod", vec![0x0a, 0x00]).unwrap();
        let frame = encode_watch_event_frame("ADDED", object_envelope.clone());
        let payload_len = u32::from_be_bytes(frame[..4].try_into().unwrap()) as usize;
        assert_eq!(payload_len, frame.len() - 4);

        let event = WatchEvent::decode(&frame[4..]).unwrap();
        assert_eq!(event.r#type.as_deref(), Some("ADDED"));
        let raw = event.object.unwrap().raw.unwrap();
        assert_eq!(raw, object_envelope);
        assert_eq!(&raw[..4], b"k8s\0");

        let unknown = Unknown::decode(&raw[4..]).unwrap();
        assert_eq!(
            unknown.type_meta,
            Some(TypeMeta {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
            })
        );
        assert_eq!(unknown.raw, [0x0a, 0x00]);
    }

    #[test]
    fn watch_frames_preserve_each_kubernetes_event_type() {
        for event_type in ["ADDED", "MODIFIED", "DELETED", "BOOKMARK"] {
            let object_envelope =
                crate::wrap_protobuf_resource_envelope("v1", "Pod", vec![0x0a, 0x00]).unwrap();
            let frame = encode_watch_event_frame(event_type, object_envelope);
            let event = WatchEvent::decode(&frame[4..]).unwrap();
            assert_eq!(event.r#type.as_deref(), Some(event_type));
            assert_eq!(
                event
                    .object
                    .and_then(|object| object.raw)
                    .as_deref()
                    .map(|raw| &raw[..4]),
                Some(&b"k8s\0"[..]),
            );
        }
    }

    #[test]
    fn concatenated_watch_frames_retain_exact_boundaries() {
        let mut stream = Vec::new();
        for (event_type, kind) in [
            ("ADDED", "Pod"),
            ("MODIFIED", "ConfigMap"),
            ("DELETED", "Node"),
        ] {
            let object_envelope =
                crate::wrap_protobuf_resource_envelope("v1", kind, vec![0x0a, 0x00]).unwrap();
            stream.extend(encode_watch_event_frame(event_type, object_envelope));
        }

        let mut offset = 0;
        let mut decoded = Vec::new();
        while offset < stream.len() {
            let payload_len =
                u32::from_be_bytes(stream[offset..offset + 4].try_into().unwrap()) as usize;
            let frame_end = offset + 4 + payload_len;
            let event = WatchEvent::decode(&stream[offset + 4..frame_end]).unwrap();
            decoded.push(event.r#type.unwrap());
            offset = frame_end;
        }

        assert_eq!(offset, stream.len());
        assert_eq!(decoded, ["ADDED", "MODIFIED", "DELETED"]);
    }
}
