//! Phase 10C.1 Redb ordinary resource and Namespace mutation ownership.
//!
//! This module family contains only direct API-facing mutation primitives.
//! Status, applied-outbox, watermark, and committed-apply behavior stays in
//! `live_committed_apply`; canonical reads already live in
//! `klights-cluster-datastore`.

mod namespaces;
mod resources;

pub(super) use namespaces::RedbOrdinaryNamespaceStore;
pub(super) use resources::RedbOrdinaryResourceStore;
