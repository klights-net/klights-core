pub mod delete;
pub mod event;
pub mod options;
pub mod response;
pub mod write;

#[deprecated(
    note = "use klights_reconcile_api::MutationOperation; remove in Phase 18.2 compatibility cleanup"
)]
pub type MutationOperation = klights_reconcile_api::MutationOperation;
pub use event::{MutationEvent, dispatch_mutation_event};
pub use options::{DeleteIntent, DryRunMode, PropagationPolicy};
