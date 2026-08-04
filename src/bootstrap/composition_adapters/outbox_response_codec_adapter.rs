use klights_cluster_store::OutboxResponseCodec;

pub(crate) struct ClusterDatastoreOutboxResponseCodec;

impl OutboxResponseCodec for ClusterDatastoreOutboxResponseCodec {
    fn encode(&self, response: &klights_cluster_core::StorageResponse) -> Result<Vec<u8>, String> {
        klights_leader_rpc::storage_wire_codec::encode_response_protobuf(response)
            .map_err(|error| error.to_string())
    }

    fn decode(&self, bytes: &[u8]) -> Result<klights_cluster_core::StorageResponse, String> {
        klights_leader_rpc::storage_wire_codec::decode_response_protobuf(bytes)
            .map_err(|error| error.to_string())
    }
}

pub(crate) fn new_codec() -> std::sync::Arc<dyn OutboxResponseCodec> {
    std::sync::Arc::new(ClusterDatastoreOutboxResponseCodec)
}

#[cfg(test)]
mod tests {
    use klights_cluster_core::StorageResponse;
    use serde_json::json;

    use super::new_codec;

    #[test]
    fn canonical_response_codec_preserves_supported_variants() {
        let codec = new_codec();
        for expected in [
            StorageResponse::Resource {
                resource_version: 41,
                data: json!({"kind": "Pod"}),
            },
            StorageResponse::Ack {
                resource_version: 42,
            },
            StorageResponse::NodeSubnet {
                node_name: "node-a".to_string(),
                subnet: "10.244.1.0/24".to_string(),
                subnet_base_int: 0x0af4_0100,
                gateway_ip: "10.244.1.1".to_string(),
                node_ip: "192.0.2.1".to_string(),
                mode: "root".to_string(),
                hostport_range: Some("30000-32767".to_string()),
            },
            StorageResponse::Error {
                message: "boom".to_string(),
            },
        ] {
            let bytes = codec.encode(&expected).expect("encode response");
            let actual = codec.decode(&bytes).expect("decode response");
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn canonical_response_codec_matches_every_historical_persisted_variant() {
        let codec = new_codec();
        let historical = [
            (
                vec![
                    10, 18, 8, 41, 18, 14, 123, 34, 107, 105, 110, 100, 34, 58, 34, 80, 111, 100,
                    34, 125,
                ],
                StorageResponse::Resource {
                    resource_version: 41,
                    data: json!({"kind": "Pod"}),
                },
            ),
            (
                vec![18, 2, 8, 42],
                StorageResponse::Ack {
                    resource_version: 42,
                },
            ),
            (
                vec![
                    26, 70, 10, 6, 110, 111, 100, 101, 45, 97, 18, 13, 49, 48, 46, 50, 52, 52, 46,
                    49, 46, 48, 47, 50, 52, 24, 128, 130, 208, 87, 34, 10, 49, 48, 46, 50, 52, 52,
                    46, 49, 46, 49, 42, 9, 49, 57, 50, 46, 48, 46, 50, 46, 49, 50, 4, 114, 111,
                    111, 116, 58, 11, 51, 48, 48, 48, 48, 45, 51, 50, 55, 54, 55,
                ],
                StorageResponse::NodeSubnet {
                    node_name: "node-a".to_string(),
                    subnet: "10.244.1.0/24".to_string(),
                    subnet_base_int: 0x0af4_0100,
                    gateway_ip: "10.244.1.1".to_string(),
                    node_ip: "192.0.2.1".to_string(),
                    mode: "root".to_string(),
                    hostport_range: Some("30000-32767".to_string()),
                },
            ),
            (
                vec![34, 6, 10, 4, 98, 111, 111, 109],
                StorageResponse::Error {
                    message: "boom".to_string(),
                },
            ),
        ];
        for (historical_bytes, expected) in historical {
            assert_eq!(
                codec.encode(&expected).expect("encode canonical response"),
                historical_bytes,
                "canonical encoding changed for {expected:?}"
            );
            assert_eq!(
                codec
                    .decode(&historical_bytes)
                    .expect("decode historical response"),
                expected
            );
        }
    }
}
