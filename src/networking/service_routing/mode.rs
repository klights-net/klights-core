//! Mode marker for service routing.

#[derive(Clone, Debug, Default)]
pub struct ServiceRoutingMode;

impl ServiceRoutingMode {
    pub fn new() -> Self {
        Self
    }

    /// Convenience for tests and any cleanup path that doesn't depend on mode
    /// behavior.
    #[cfg(test)]
    pub fn default_root_for_test() -> Self {
        Self::new()
    }
}
