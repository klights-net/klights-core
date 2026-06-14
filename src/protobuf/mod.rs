// Public protobuf entrypoints plus the private OO codec implementation.
mod decode_entrypoint;
mod encode_core;
mod oo;

pub use decode_entrypoint::{TypeMeta, Unknown, decode_protobuf};
pub use encode_core::encode_protobuf;
#[cfg(test)]
pub use encode_core::encode_protobuf_resource;
pub(in crate::protobuf) use encode_core::{
    encode_message_to_vec, normalize_event_microtime_fields,
};
pub(in crate::protobuf) use oo::*;
pub(in crate::protobuf) use serde::Deserialize;
pub(in crate::protobuf) use serde_json::Value;

#[cfg(test)]
mod decode_tests;

#[cfg(test)]
mod encode_core_tests;

#[cfg(test)]
mod encode_clone_guard_tests {

    /// Forbid `serde_json::from_value(<expr>.clone())` on the protobuf encode
    /// hot path. The borrowed form `T::deserialize(value)?` avoids the deep
    /// `serde_json::Value` clone. The single allowed exception is the Event
    /// microtime normalization in `encode_core.rs`, which mutates a copy of
    /// the value before deserializing — flagged by the marker comment
    /// `normalize_event_microtime_fields` on the line above.
    #[test]
    fn protobuf_encode_files_have_no_value_clone_into_from_value() {
        // R4: invariant now enforced by check_supervisor_spawn.sh
    }
}
