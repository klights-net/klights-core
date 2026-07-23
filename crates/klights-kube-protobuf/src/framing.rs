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
}
