pub use super::sqlite::{
    DeadLetterRow, OutboxInsert, OutboxRow, OutboxStats, PodRuntimeRow, PodStatusCheckpoint,
    ProbeStateRow, ReplicationCheckpoint, RuntimeObservationCheckpoint,
};

/// Exact durable identity of one node-local pod-network allocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodNetworkAssignmentRow {
    pub sandbox_id: String,
    pub namespace: String,
    pub pod_name: String,
    pub pod_uid: String,
    pub subnet_base_int: u32,
    pub subnet_size: u32,
    pub ip_addr: String,
    pub ip_int: u32,
    pub veth_host: String,
    pub netns_path: String,
}

/// Typed outcome failures from the atomic node-local IPAM reservation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodNetworkReservationError {
    AddressExhausted {
        subnet_base_int: u32,
        subnet_size: u32,
    },
    IdentityConflict {
        sandbox_id: String,
    },
    Persistence {
        message: String,
    },
}

impl std::fmt::Display for PodNetworkReservationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AddressExhausted {
                subnet_base_int,
                subnet_size,
            } => write!(
                formatter,
                "pod address range {subnet_base_int}/{subnet_size} is exhausted"
            ),
            Self::IdentityConflict { sandbox_id } => {
                write!(
                    formatter,
                    "sandbox {sandbox_id} already has a different identity"
                )
            }
            Self::Persistence { message } => formatter.write_str(message),
        }
    }
}

impl std::error::Error for PodNetworkReservationError {}

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
