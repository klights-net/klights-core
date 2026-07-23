#[path = "decode_discovery_v1_lists.rs"]
mod decode_discovery_v1_lists;
#[path = "decode_discovery_v1_slice.rs"]
mod decode_discovery_v1_slice;
#[path = "encode_discovery_v1_lists.rs"]
mod encode_discovery_v1_lists;
#[path = "encode_discovery_v1_resources.rs"]
mod encode_discovery_v1_resources;

pub(in crate::protobuf) use self::decode_discovery_v1_lists::*;
pub(in crate::protobuf) use self::decode_discovery_v1_slice::*;
pub(in crate::protobuf) use self::encode_discovery_v1_lists::*;
pub(in crate::protobuf) use self::encode_discovery_v1_resources::*;
