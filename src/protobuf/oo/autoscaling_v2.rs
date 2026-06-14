#[path = "decode_autoscaling_v2.rs"]
mod decode_autoscaling_v2;
#[path = "encode_autoscaling_v2.rs"]
mod encode_autoscaling_v2;

pub(in crate::protobuf) use self::decode_autoscaling_v2::*;
pub(in crate::protobuf) use self::encode_autoscaling_v2::*;
