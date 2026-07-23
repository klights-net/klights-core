//! Generated public Kubernetes protobuf messages for klights.

pub use k8s_pb::*;

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::{
        api::core::v1::Pod,
        apimachinery::pkg::apis::meta::v1::{ObjectMeta, Status},
    };

    #[test]
    fn generated_pod_wire_tags_round_trip() {
        let pod = Pod {
            metadata: Some(ObjectMeta {
                name: Some("wire-pod".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let encoded = pod.encode_to_vec();
        assert_eq!(
            encoded,
            [
                0x0a, 0x0a, 0x0a, 0x08, b'w', b'i', b'r', b'e', b'-', b'p', b'o', b'd',
            ]
        );
        assert_eq!(Pod::decode(encoded.as_slice()).unwrap(), pod);
    }

    #[test]
    fn generated_status_wire_tags_round_trip() {
        let status = Status {
            status: Some("Failure".to_string()),
            message: Some("wire status".to_string()),
            ..Default::default()
        };

        let encoded = status.encode_to_vec();
        assert_eq!(
            encoded,
            [
                0x12, 0x07, b'F', b'a', b'i', b'l', b'u', b'r', b'e', 0x1a, 0x0b, b'w', b'i', b'r',
                b'e', b' ', b's', b't', b'a', b't', b'u', b's',
            ]
        );
        assert_eq!(Status::decode(encoded.as_slice()).unwrap(), status);
    }

    #[test]
    fn generated_unknown_fields_remain_forward_compatible() {
        let pod = Pod {
            metadata: Some(ObjectMeta {
                name: Some("known".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let canonical = pod.encode_to_vec();
        let mut with_unknown = canonical.clone();
        // Unknown field 127, varint value 1. Prost intentionally accepts and
        // discards it while preserving every known field.
        with_unknown.extend_from_slice(&[0xf8, 0x07, 0x01]);

        let decoded = Pod::decode(with_unknown.as_slice()).unwrap();
        assert_eq!(decoded, pod);
        assert_eq!(decoded.encode_to_vec(), canonical);
    }
}
