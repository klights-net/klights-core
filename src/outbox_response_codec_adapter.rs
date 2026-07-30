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
}
