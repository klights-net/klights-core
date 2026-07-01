pub mod delete;
pub mod identity;
pub mod options;
pub mod response;

pub use identity::ResourceIdentity;
pub use options::{DeleteIntent, DryRunMode, PropagationPolicy};
