//! Replication-owned internal protobuf codec for outbox responses.

use klights_cluster_core::StorageResponse;
use klights_internal_protobuf::storage::{
    ProtoAckResp, ProtoErrorResp, ProtoNodeSubnetResp, ProtoResourceResp, ProtoStorageResponse,
    proto_storage_response,
};
use prost::Message;

#[derive(Debug, thiserror::Error)]
pub enum OutboxResponseWireError {
    #[error("protobuf encode failure: {0}")]
    Encode(#[from] prost::EncodeError),
    #[error("protobuf decode failure: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("outbox response is missing its response value")]
    MissingResponse,
    #[error("outbox resource response contains invalid JSON: {0}")]
    InvalidResourceJson(#[from] serde_json::Error),
    #[error("outbox response variant is not supported by this wire codec")]
    UnsupportedResponse,
}

pub fn encode_outbox_response(
    response: &StorageResponse,
) -> Result<Vec<u8>, OutboxResponseWireError> {
    let response = match response {
        StorageResponse::Resource {
            resource_version,
            data,
        } => proto_storage_response::Response::Resource(ProtoResourceResp {
            resource_version: *resource_version,
            data: serde_json::to_vec(data)?,
        }),
        StorageResponse::Ack { resource_version } => {
            proto_storage_response::Response::Ack(ProtoAckResp {
                resource_version: *resource_version,
            })
        }
        StorageResponse::NodeSubnet {
            node_name,
            subnet,
            subnet_base_int,
            gateway_ip,
            node_ip,
            mode,
            hostport_range,
        } => proto_storage_response::Response::NodeSubnet(ProtoNodeSubnetResp {
            node_name: node_name.clone(),
            subnet: subnet.clone(),
            subnet_base_int: *subnet_base_int,
            gateway_ip: gateway_ip.clone(),
            node_ip: node_ip.clone(),
            mode: mode.clone(),
            hostport_range: hostport_range.clone(),
        }),
        StorageResponse::Error { message } => {
            proto_storage_response::Response::Error(ProtoErrorResp {
                message: message.clone(),
            })
        }
        _ => return Err(OutboxResponseWireError::UnsupportedResponse),
    };
    let wire = ProtoStorageResponse {
        response: Some(response),
    };
    let mut bytes = Vec::with_capacity(wire.encoded_len());
    wire.encode(&mut bytes)?;
    Ok(bytes)
}

pub fn decode_outbox_response(bytes: &[u8]) -> Result<StorageResponse, OutboxResponseWireError> {
    let wire = ProtoStorageResponse::decode(bytes)?;
    match wire
        .response
        .ok_or(OutboxResponseWireError::MissingResponse)?
    {
        proto_storage_response::Response::Resource(resource) => Ok(StorageResponse::Resource {
            resource_version: resource.resource_version,
            data: serde_json::from_slice(&resource.data)?,
        }),
        proto_storage_response::Response::Ack(ack) => Ok(StorageResponse::Ack {
            resource_version: ack.resource_version,
        }),
        proto_storage_response::Response::NodeSubnet(subnet) => Ok(StorageResponse::NodeSubnet {
            node_name: subnet.node_name,
            subnet: subnet.subnet,
            subnet_base_int: subnet.subnet_base_int,
            gateway_ip: subnet.gateway_ip,
            node_ip: subnet.node_ip,
            mode: subnet.mode,
            hostport_range: subnet.hostport_range,
        }),
        proto_storage_response::Response::Error(error) => Ok(StorageResponse::Error {
            message: error.message,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_outbox_response, encode_outbox_response};
    use klights_cluster_core::StorageResponse;

    #[test]
    fn cached_outbox_response_round_trips_all_variants() {
        let cases = [
            StorageResponse::Ack {
                resource_version: 41,
            },
            StorageResponse::Resource {
                resource_version: 42,
                data: serde_json::json!({"metadata": {"name": "web"}}),
            },
            StorageResponse::NodeSubnet {
                node_name: "worker-a".to_string(),
                subnet: "10.42.1.0/24".to_string(),
                subnet_base_int: 170_525_952,
                gateway_ip: "10.42.1.1".to_string(),
                node_ip: "192.0.2.10".to_string(),
                mode: "root".to_string(),
                hostport_range: Some("30000-32767".to_string()),
            },
            StorageResponse::Error {
                message: "conflict".to_string(),
            },
        ];

        for expected in cases {
            let bytes = encode_outbox_response(&expected).expect("encode response");
            let actual = decode_outbox_response(&bytes).expect("decode response");
            assert_eq!(actual, expected);
        }
    }
}
