//! Conversion between the public Kubernetes wire representation and the
//! canonical transport-neutral cluster resource.

use std::sync::Arc;

use klights_cluster_core::{Resource, ResourceIdentityError};

#[derive(Debug)]
pub enum ResourceCodecError {
    Encode { message: String },
    Decode { message: String },
    InvalidResource(ResourceIdentityError),
}

impl std::fmt::Display for ResourceCodecError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Encode { message } => {
                write!(formatter, "protobuf resource encode failed: {message}")
            }
            Self::Decode { message } => {
                write!(formatter, "protobuf resource decode failed: {message}")
            }
            Self::InvalidResource(error) => {
                write!(formatter, "decoded resource is invalid: {error}")
            }
        }
    }
}

impl std::error::Error for ResourceCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidResource(error) => Some(error),
            Self::Encode { .. } | Self::Decode { .. } => None,
        }
    }
}

/// Encode the canonical neutral resource as a Kubernetes protobuf envelope.
pub fn encode_resource(resource: &Resource) -> Result<Vec<u8>, ResourceCodecError> {
    crate::encode_protobuf(resource.data.as_ref()).map_err(|error| ResourceCodecError::Encode {
        message: error.to_string(),
    })
}

/// Decode a Kubernetes protobuf envelope into the canonical neutral resource.
pub fn decode_resource(data: &[u8]) -> Result<Resource, ResourceCodecError> {
    let value = crate::decode_protobuf(data).map_err(|error| ResourceCodecError::Decode {
        message: error.to_string(),
    })?;
    Resource::try_from_data(Arc::new(value)).map_err(ResourceCodecError::InvalidResource)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn neutral_resource_round_trip_preserves_kubernetes_identity_and_body() {
        let resource = Resource::try_from_data(Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "codec-boundary",
                "namespace": "default",
                "uid": "uid-1",
                "resourceVersion": "42"
            },
            "spec": {
                "containers": [{"name": "main", "image": "example.invalid/image:v1"}]
            }
        })))
        .unwrap();

        let encoded = encode_resource(&resource).unwrap();
        assert_eq!(&encoded[..4], b"k8s\0");
        let decoded = decode_resource(&encoded).unwrap();

        assert_eq!(decoded.api_version, resource.api_version);
        assert_eq!(decoded.kind, resource.kind);
        assert_eq!(decoded.namespace, resource.namespace);
        assert_eq!(decoded.name, resource.name);
        assert_eq!(decoded.uid, resource.uid);
        assert_eq!(decoded.resource_version, resource.resource_version);
        assert_eq!(decoded.data, resource.data);
    }

    #[test]
    fn neutral_decode_rejects_non_resource_values_with_typed_error() {
        let status = json!({
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Failure",
            "message": "missing metadata identity"
        });
        let encoded = crate::encode_protobuf(&status).unwrap();
        assert!(matches!(
            decode_resource(&encoded),
            Err(ResourceCodecError::InvalidResource(
                ResourceIdentityError::ResourceMissingMetadataName
            ))
        ));
    }
}
