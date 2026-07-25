#[path = "decode_authentication_v1.rs"]
mod decode_authentication_v1;
#[path = "encode_authentication_v1.rs"]
mod encode_authentication_v1;

pub(in crate::protobuf) use self::decode_authentication_v1::*;
pub(in crate::protobuf) use self::encode_authentication_v1::*;
