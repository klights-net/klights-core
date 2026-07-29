//! Opaque node-local durability ports for OpenRaft-owned state.
//!
//! The replication owner performs all encoding and decoding. This leaf
//! contract carries only coordinates and uninterpreted bytes.

use std::fmt;
use std::future::Future;
use std::pin::Pin;

#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaftDurabilityError {
    InvalidInput {
        field: &'static str,
        message: String,
    },
    PersistenceFailed {
        operation: &'static str,
        message: String,
    },
    CorruptData {
        field: &'static str,
        message: String,
    },
    Retryable {
        operation: &'static str,
        message: String,
    },
    Timeout,
    Cancelled,
}

impl RaftDurabilityError {
    pub fn persistence_failed(operation: &'static str, message: impl Into<String>) -> Self {
        Self::PersistenceFailed {
            operation,
            message: message.into(),
        }
    }

    pub fn corrupt_data(field: &'static str, message: impl Into<String>) -> Self {
        Self::CorruptData {
            field,
            message: message.into(),
        }
    }

    fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidInput {
            field,
            message: message.into(),
        }
    }
}

impl fmt::Display for RaftDurabilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput { field, message } => {
                write!(formatter, "invalid {field}: {message}")
            }
            Self::PersistenceFailed { operation, message }
            | Self::Retryable { operation, message } => {
                write!(formatter, "{operation}: {message}")
            }
            Self::CorruptData { field, message } => {
                write!(formatter, "corrupt {field}: {message}")
            }
            Self::Timeout => formatter.write_str("Raft durability operation timed out"),
            Self::Cancelled => formatter.write_str("Raft durability operation was cancelled"),
        }
    }
}

impl std::error::Error for RaftDurabilityError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpaqueRaftBytes(Vec<u8>);

impl OpaqueRaftBytes {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RaftLogCoordinate {
    index: u64,
    term: u64,
    leader_node_id: u64,
}

impl RaftLogCoordinate {
    pub fn new(index: u64, term: u64, leader_node_id: u64) -> Self {
        Self {
            index,
            term,
            leader_node_id,
        }
    }

    pub fn index(self) -> u64 {
        self.index
    }

    pub fn term(self) -> u64 {
        self.term
    }

    pub fn leader_node_id(self) -> u64 {
        self.leader_node_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedRaftLogEntry {
    coordinate: RaftLogCoordinate,
    payload: OpaqueRaftBytes,
}

/// Exact encoded last-applied value plus its decoded neutral coordinate when
/// the encoded value contains one. An encoded JSON `null` remains a present,
/// byte-exact value with no coordinate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedRaftAppliedValue {
    coordinate: Option<RaftLogCoordinate>,
    payload: OpaqueRaftBytes,
}

impl EncodedRaftAppliedValue {
    pub fn new(coordinate: Option<RaftLogCoordinate>, payload: OpaqueRaftBytes) -> Self {
        Self {
            coordinate,
            payload,
        }
    }

    pub fn into_parts(self) -> (Option<RaftLogCoordinate>, OpaqueRaftBytes) {
        (self.coordinate, self.payload)
    }
}

impl EncodedRaftLogEntry {
    pub fn new(coordinate: RaftLogCoordinate, payload: OpaqueRaftBytes) -> Self {
        Self {
            coordinate,
            payload,
        }
    }

    pub fn coordinate(&self) -> RaftLogCoordinate {
        self.coordinate
    }

    pub fn payload(&self) -> &OpaqueRaftBytes {
        &self.payload
    }

    pub fn into_parts(self) -> (RaftLogCoordinate, OpaqueRaftBytes) {
        (self.coordinate, self.payload)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RaftLogRange {
    start_inclusive: u64,
    end_exclusive: Option<u64>,
}

impl RaftLogRange {
    pub fn new(
        start_inclusive: u64,
        end_exclusive: Option<u64>,
    ) -> Result<Self, RaftDurabilityError> {
        if end_exclusive.is_some_and(|end| end < start_inclusive) {
            return Err(RaftDurabilityError::invalid(
                "end_exclusive",
                "must not be below start_inclusive",
            ));
        }
        Ok(Self {
            start_inclusive,
            end_exclusive,
        })
    }

    pub fn start_inclusive(self) -> u64 {
        self.start_inclusive
    }

    pub fn end_exclusive(self) -> Option<u64> {
        self.end_exclusive
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftLogBatch(Vec<EncodedRaftLogEntry>);

impl RaftLogBatch {
    pub fn new(entries: Vec<EncodedRaftLogEntry>) -> Result<Self, RaftDurabilityError> {
        if entries
            .windows(2)
            .any(|pair| pair[0].coordinate.index >= pair[1].coordinate.index)
        {
            return Err(RaftDurabilityError::invalid(
                "entries",
                "indices must be strictly increasing",
            ));
        }
        Ok(Self(entries))
    }

    pub fn as_slice(&self) -> &[EncodedRaftLogEntry] {
        &self.0
    }

    pub fn into_vec(self) -> Vec<EncodedRaftLogEntry> {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedRaftLogState {
    last_entry: Option<RaftLogCoordinate>,
    encoded_last_purged: Option<OpaqueRaftBytes>,
}

/// Atomically observed inputs needed by the OpenRaft adapter to reconstruct
/// the current durable boundary.
///
/// The retained log coordinate is neutral. Purged and applied anchors retain
/// their exact historical byte encoding and are decoded only by replication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedRaftStorageBoundary {
    last_entry: Option<RaftLogCoordinate>,
    encoded_last_purged: Option<OpaqueRaftBytes>,
    encoded_last_applied: Option<OpaqueRaftBytes>,
}

impl EncodedRaftStorageBoundary {
    pub fn new(
        last_entry: Option<RaftLogCoordinate>,
        encoded_last_purged: Option<OpaqueRaftBytes>,
        encoded_last_applied: Option<OpaqueRaftBytes>,
    ) -> Self {
        Self {
            last_entry,
            encoded_last_purged,
            encoded_last_applied,
        }
    }

    pub fn into_parts(
        self,
    ) -> (
        Option<RaftLogCoordinate>,
        Option<OpaqueRaftBytes>,
        Option<OpaqueRaftBytes>,
    ) {
        (
            self.last_entry,
            self.encoded_last_purged,
            self.encoded_last_applied,
        )
    }
}

impl EncodedRaftLogState {
    pub fn new(
        last_entry: Option<RaftLogCoordinate>,
        encoded_last_purged: Option<OpaqueRaftBytes>,
    ) -> Self {
        Self {
            last_entry,
            encoded_last_purged,
        }
    }

    pub fn into_parts(self) -> (Option<RaftLogCoordinate>, Option<OpaqueRaftBytes>) {
        (self.last_entry, self.encoded_last_purged)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftPurgeRequest {
    through: RaftLogCoordinate,
    encoded_last_purged: OpaqueRaftBytes,
}

impl RaftPurgeRequest {
    pub fn new(through: RaftLogCoordinate, encoded_last_purged: OpaqueRaftBytes) -> Self {
        Self {
            through,
            encoded_last_purged,
        }
    }

    pub fn into_parts(self) -> (RaftLogCoordinate, OpaqueRaftBytes) {
        (self.through, self.encoded_last_purged)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedRaftAppliedState {
    encoded_last_applied: Option<OpaqueRaftBytes>,
    encoded_last_membership: Option<OpaqueRaftBytes>,
}

impl EncodedRaftAppliedState {
    pub fn new(
        encoded_last_applied: Option<OpaqueRaftBytes>,
        encoded_last_membership: Option<OpaqueRaftBytes>,
    ) -> Self {
        Self {
            encoded_last_applied,
            encoded_last_membership,
        }
    }

    pub fn into_parts(self) -> (Option<OpaqueRaftBytes>, Option<OpaqueRaftBytes>) {
        (self.encoded_last_applied, self.encoded_last_membership)
    }
}

/// Atomic applied-state update. `None` preserves the existing row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftAppliedStateWrite {
    encoded_last_applied: Option<OpaqueRaftBytes>,
    encoded_last_membership: Option<OpaqueRaftBytes>,
}

impl RaftAppliedStateWrite {
    pub fn new(
        encoded_last_applied: Option<OpaqueRaftBytes>,
        encoded_last_membership: Option<OpaqueRaftBytes>,
    ) -> Self {
        Self {
            encoded_last_applied,
            encoded_last_membership,
        }
    }

    pub fn into_parts(self) -> (Option<OpaqueRaftBytes>, Option<OpaqueRaftBytes>) {
        (self.encoded_last_applied, self.encoded_last_membership)
    }
}

/// Persistence-side applied-state update. The replication adapter pairs the
/// opaque last-applied bytes with their neutral coordinate before crossing
/// this lower boundary. `None` preserves the existing row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftAppliedStatePersistenceWrite {
    encoded_last_applied: Option<EncodedRaftAppliedValue>,
    encoded_last_membership: Option<OpaqueRaftBytes>,
}

impl RaftAppliedStatePersistenceWrite {
    pub fn new(
        encoded_last_applied: Option<EncodedRaftAppliedValue>,
        encoded_last_membership: Option<OpaqueRaftBytes>,
    ) -> Self {
        Self {
            encoded_last_applied,
            encoded_last_membership,
        }
    }

    pub fn into_parts(self) -> (Option<EncodedRaftAppliedValue>, Option<OpaqueRaftBytes>) {
        (self.encoded_last_applied, self.encoded_last_membership)
    }
}

pub type RaftDurabilityFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, RaftDurabilityError>> + Send + 'a>>;

pub trait RaftLogPersistence: Send + Sync {
    fn read_log_range(
        &self,
        range: RaftLogRange,
    ) -> RaftDurabilityFuture<'_, Vec<EncodedRaftLogEntry>>;
    fn load_log_state(&self) -> RaftDurabilityFuture<'_, EncodedRaftLogState>;
    fn append_log_entries(&self, entries: RaftLogBatch) -> RaftDurabilityFuture<'_, ()>;
    fn truncate_log_from(&self, from_inclusive: u64) -> RaftDurabilityFuture<'_, ()>;
    fn purge_log_through(&self, request: RaftPurgeRequest) -> RaftDurabilityFuture<'_, ()>;
    fn load_vote(&self) -> RaftDurabilityFuture<'_, Option<OpaqueRaftBytes>>;
    fn store_vote(&self, encoded_vote: OpaqueRaftBytes) -> RaftDurabilityFuture<'_, ()>;
    fn load_committed(&self) -> RaftDurabilityFuture<'_, Option<OpaqueRaftBytes>>;
    fn store_committed(&self, encoded_committed: OpaqueRaftBytes) -> RaftDurabilityFuture<'_, ()>;
    /// Return the durable identity of this node-local Raft store, creating it
    /// atomically on first open. Reopening the same node.db returns the same
    /// value; recreating node.db produces a new value.
    fn load_or_create_storage_incarnation(&self) -> RaftDurabilityFuture<'_, String>;
    /// Monotonic highest Raft LogId ever durably accepted by this incarnation.
    /// Full term/leader identity detects an anchored node.db rollback even
    /// when a restored backup contains the same incarnation UUID.
    fn load_storage_log_attestation(&self) -> RaftDurabilityFuture<'_, Option<RaftLogCoordinate>>;
    /// Atomically load the opaque inputs used by the replication adapter to
    /// reconstruct the current durable Raft boundary.
    fn load_storage_boundary_state(&self) -> RaftDurabilityFuture<'_, EncodedRaftStorageBoundary>;
    /// Atomically discard a learner-only log suffix that has neither a
    /// snapshot/purge boundary nor applied state and starts above index zero.
    ///
    /// Such a suffix cannot be replayed: its predecessor identity is unknown.
    /// Callers must restrict this operation to nodes configured as non-voting
    /// learners, which can safely reacquire authoritative state from a leader.
    /// The durable vote is deliberately preserved.
    fn reset_orphaned_learner_log(&self) -> RaftDurabilityFuture<'_, bool>;
}

/// OpenRaft-facing durability adapter. All ordinary persistence methods come
/// from the neutral lower port; only current-boundary reconstruction requires
/// adapter-owned decoding.
pub trait RaftLogDurability: RaftLogPersistence {
    fn load_storage_current_boundary(&self) -> RaftDurabilityFuture<'_, Option<RaftLogCoordinate>>;
}

pub trait RaftAppliedStatePersistence: Send + Sync {
    fn load_applied_state(&self) -> RaftDurabilityFuture<'_, EncodedRaftAppliedState>;
    fn store_applied_state_persistence(
        &self,
        state: RaftAppliedStatePersistenceWrite,
    ) -> RaftDurabilityFuture<'_, ()>;
}

pub trait RaftAppliedStateDurability: Send + Sync {
    fn load_applied_state(&self) -> RaftDurabilityFuture<'_, EncodedRaftAppliedState>;
    fn store_applied_state(&self, state: RaftAppliedStateWrite) -> RaftDurabilityFuture<'_, ()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(index: u64) -> EncodedRaftLogEntry {
        EncodedRaftLogEntry::new(
            RaftLogCoordinate::new(index, 1, u64::MAX),
            OpaqueRaftBytes::new(vec![0, index as u8]),
        )
    }

    #[test]
    fn opaque_bytes_and_coordinates_are_lossless() {
        assert_eq!(
            OpaqueRaftBytes::new(vec![0, 1, 0]).into_vec(),
            vec![0, 1, 0]
        );
        assert_eq!(entry(1).coordinate().leader_node_id(), u64::MAX);
    }

    #[test]
    fn range_validates_only_reversed_bounds() {
        assert!(RaftLogRange::new(4, Some(4)).is_ok());
        assert!(RaftLogRange::new(4, None).is_ok());
        assert!(RaftLogRange::new(4, Some(3)).is_err());
    }

    #[test]
    fn batch_requires_strict_order_but_not_contiguity() {
        assert!(RaftLogBatch::new(vec![]).is_ok());
        assert!(RaftLogBatch::new(vec![entry(1), entry(3)]).is_ok());
        assert!(RaftLogBatch::new(vec![entry(1), entry(1)]).is_err());
        assert!(RaftLogBatch::new(vec![entry(2), entry(1)]).is_err());
    }

    #[test]
    fn traits_are_independently_object_safe() {
        fn log(_: Option<&dyn RaftLogDurability>) {}
        fn log_persistence(_: Option<&dyn RaftLogPersistence>) {}
        fn applied(_: Option<&dyn RaftAppliedStateDurability>) {}
        fn applied_persistence(_: Option<&dyn RaftAppliedStatePersistence>) {}
        log(None);
        log_persistence(None);
        applied(None);
        applied_persistence(None);
    }

    #[test]
    fn boundary_state_preserves_all_opaque_inputs() {
        let boundary = EncodedRaftStorageBoundary::new(
            Some(RaftLogCoordinate::new(5, 4, 3)),
            Some(OpaqueRaftBytes::new(vec![1, 0, 1])),
            Some(OpaqueRaftBytes::new(vec![2, 0, 2])),
        );
        let (last, purged, applied) = boundary.into_parts();
        assert_eq!(last.unwrap().index(), 5);
        assert_eq!(purged.unwrap().as_slice(), [1, 0, 1]);
        assert_eq!(applied.unwrap().as_slice(), [2, 0, 2]);
    }
}
