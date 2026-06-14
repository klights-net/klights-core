#[path = "decode_coordination_v1_leases.rs"]
mod decode_coordination_v1_leases;
#[path = "encode_coordination_v1_lists.rs"]
mod encode_coordination_v1_lists;
#[path = "encode_coordination_v1_resources.rs"]
mod encode_coordination_v1_resources;

pub(in crate::protobuf) use self::decode_coordination_v1_leases::*;
pub(in crate::protobuf) use self::encode_coordination_v1_lists::*;
pub(in crate::protobuf) use self::encode_coordination_v1_resources::*;
