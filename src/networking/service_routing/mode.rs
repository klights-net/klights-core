//! Mode marker for service routing.

use crate::bootstrap::NodeMode;

#[derive(Clone, Debug)]
pub struct ServiceRoutingMode;

impl ServiceRoutingMode {
    pub fn new(_node_mode: NodeMode) -> Self {
        Self
    }

    /// Convenience for tests and any cleanup path that doesn't depend on mode
    /// behavior.
    #[cfg(test)]
    pub fn default_root_for_test() -> Self {
        Self::new(NodeMode::Root)
    }
}
