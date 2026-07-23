// Private OO protobuf implementation boundary.
//
// Resource dispatch is owned by ResourceProtoCodec objects. The many
// per-resource conversion helpers remain regular functions, but they live
// under this private module and are only reachable from the codec objects and
// protobuf tests.
#[macro_use]
mod decode_common;
mod decode_intorstring;
mod decode_listmeta;
mod encode_core_helpers;
mod encode_listmeta;

mod admissionregistration_v1;
mod apiextensions_v1;
mod apiregistration_v1;
mod apps_v1;
mod authentication_v1;
mod authorization_v1;
mod autoscaling_v2;
mod batch_v1;
mod builtin_codec;
mod certificates_v1;
mod coordination_v1;
mod core_v1;
mod discovery_v1;
mod flowcontrol_v1;
mod flowcontrol_v1_codec;
mod networking_v1;
mod node_k8s_io_v1;
mod policy_v1;
mod rbac_v1;
mod resource_codec;
mod scheduling_v1;
mod storage_v1;

pub(in crate::protobuf) use admissionregistration_v1::*;
pub(in crate::protobuf) use apiextensions_v1::*;
pub(in crate::protobuf) use apiregistration_v1::*;
pub(in crate::protobuf) use apps_v1::*;
pub(in crate::protobuf) use authentication_v1::*;
pub(in crate::protobuf) use authorization_v1::*;
pub(in crate::protobuf) use autoscaling_v2::*;
pub(in crate::protobuf) use batch_v1::*;
pub(in crate::protobuf) use builtin_codec::*;
pub(in crate::protobuf) use certificates_v1::*;
pub(in crate::protobuf) use coordination_v1::*;
pub(in crate::protobuf) use core_v1::*;
pub(in crate::protobuf) use decode_common::*;
pub(in crate::protobuf) use decode_intorstring::*;
pub(in crate::protobuf) use decode_listmeta::*;
pub(in crate::protobuf) use discovery_v1::*;
pub(in crate::protobuf) use encode_core_helpers::*;
pub(in crate::protobuf) use encode_listmeta::*;
pub(in crate::protobuf) use flowcontrol_v1::*;
pub(in crate::protobuf) use flowcontrol_v1_codec::*;
pub(in crate::protobuf) use networking_v1::*;
pub(in crate::protobuf) use node_k8s_io_v1::*;
pub(in crate::protobuf) use policy_v1::*;
pub(in crate::protobuf) use rbac_v1::*;
pub(in crate::protobuf) use resource_codec::*;
pub(in crate::protobuf) use scheduling_v1::*;
pub(in crate::protobuf) use storage_v1::*;
