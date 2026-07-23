//! Public Kubernetes protobuf messages and codec boundaries for klights.

pub use k8s_pb::*;

mod error;
mod framing;
mod negotiation;
mod protobuf;
mod resource;
mod utils;

pub use error::CodecError;
pub use framing::encode_watch_event_frame;
pub use resource::{ResourceCodecError, decode_resource, encode_resource};

pub use negotiation::{
    AcceptValue, JSON_MEDIA_TYPE, NegotiationError, PROTOBUF_MEDIA_TYPE, PROTOBUF_WATCH_MEDIA_TYPE,
    ResponseFormat, negotiate_unary_response, negotiate_watch_response,
};

pub use protobuf::{
    TypeMeta, Unknown, decode_protobuf, encode_protobuf, encode_protobuf_resource,
    encode_protobuf_resource_from_json_bytes, encode_status_protobuf, supports_protobuf_resource,
    supports_raw_json_protobuf_resource, wrap_protobuf_resource_envelope,
};

#[cfg(test)]
mod generated;
