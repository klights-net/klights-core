//! Generated API handlers — directory module (refactored from single file).
//!
//! Split from a 2638 LOC single file into:
//! - `helpers` — shared helper functions
//! - `inners` — inner CRUD handler functions
//! - `macros` — wrapper macros for handler generation
//! - `register` — resource handler registrations (macro invocations)
//!
//! Backward-compatible: all pub items are re-exported.

pub mod helpers;
pub mod inners;
pub mod macros;

pub use helpers::*;
pub use macros::*;
