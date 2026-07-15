pub mod delete;
pub mod event;
pub mod identity;
pub mod options;
pub mod response;
pub mod write;

pub use event::{MutationEvent, MutationOperation, dispatch_mutation_event};
#[allow(deprecated)]
#[deprecated(note = "use klights_types::ResourceKey directly; removed in Phase 3.4")]
pub use identity::ResourceIdentity;
pub use options::{DeleteIntent, DryRunMode, PropagationPolicy};
