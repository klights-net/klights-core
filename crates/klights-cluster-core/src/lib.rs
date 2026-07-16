//! Pure cluster-state domain logic for klights.

pub mod command;
pub mod resource;

pub use command::{
    COMMAND_CODEC_VERSION, CommandError, CommandId, CommandMeta, StorageCommand, StorageResponse,
};
pub use resource::{
    PatchKind, Resource, ResourceBatchOperation, ResourceBatchPutMode, ResourceEventObject,
    ResourcePatchRequest, ResourcePreconditions,
};
