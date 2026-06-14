//! Error policy for side-effect hooks.

/// Error policy determining how side-effect failures are handled.
///
/// `Ignore` and `Fail` are part of the public surface for future hook
/// registrations; only `Warn` is wired today (post-mutation hooks should
/// not block the HTTP response on a non-critical reconcile failure).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorPolicy {
    /// Log error and continue (most side effects are non-critical)
    Ignore,
    /// Log warning and continue (default for most hooks)
    Warn,
    /// Fail request — this side effect is critical (not used today)
    Fail,
}
