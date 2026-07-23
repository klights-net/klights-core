#[path = "decode_admissionregistration_v1.rs"]
mod decode_admissionregistration_v1;
#[path = "decode_admissionregistration_v1_webhook.rs"]
mod decode_admissionregistration_v1_webhook;
#[path = "encode_admissionregistration_v1_webhooklists.rs"]
mod encode_admissionregistration_v1_webhooklists;
#[path = "encode_admissionregistration_v1_webhooks.rs"]
mod encode_admissionregistration_v1_webhooks;

pub(in crate::protobuf) use self::decode_admissionregistration_v1::*;
pub(in crate::protobuf) use self::decode_admissionregistration_v1_webhook::*;
pub(in crate::protobuf) use self::encode_admissionregistration_v1_webhooklists::*;
pub(in crate::protobuf) use self::encode_admissionregistration_v1_webhooks::*;
