pub use super::sqlite::{
    DeadLetterRow, OutboxInsert, OutboxRow, OutboxStats, PodRuntimeRow, PodStatusCheckpoint,
    ProbeStateRow, ReplicationCheckpoint, RuntimeObservationCheckpoint,
};

/// Durable result of recording one leased outbox delivery failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboxFailureDisposition {
    /// The leased row was released with its incremented attempt and backoff.
    RetryScheduled,
    /// The incremented attempt reached the threshold and the row moved atomically.
    DeadLettered,
    /// The row was absent or no longer owned by the supplied lease token.
    LeaseLost,
}

#[cfg(test)]
pub use super::sqlite::DeadLetterTestInsert;
