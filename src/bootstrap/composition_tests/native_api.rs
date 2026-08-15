//! Private root composition tests for the complete native API fixture.
//!
//! P12.2f keeps assembly that necessarily binds root-private datastore,
//! bootstrap, replication, and adapter implementations inside this crate.
//! The base integration target therefore has no private-root import seam.

#[cfg(test)]
mod assembly_support;
#[cfg(test)]
mod cases;
#[cfg(test)]
pub(crate) mod support;
