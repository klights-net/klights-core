//! Node-local durable delivery and checkpoint capabilities.
//!
//! Payloads are intentionally opaque. Encoding, Kubernetes API semantics, and
//! concrete persistence belong outside this leaf contract.

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use klights_types::{PodIdentity, ResourceKey};

/// Hard cap for one atomic outbox claim.
pub const MAX_OUTBOX_BATCH: usize = 256;

/// Diagnostic rows age into workload priority after this duration.
pub const OUTBOX_DIAGNOSTIC_AGING_MS: i64 = 30_000;

/// Failure returned by node-local delivery persistence.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeliveryError {
    InvalidInput {
        field: &'static str,
        message: String,
    },
    PersistenceFailed {
        message: String,
    },
    CorruptData {
        message: String,
    },
    UnsupportedMode {
        message: String,
    },
    Retryable {
        message: String,
    },
    Timeout,
    Cancelled,
}

impl DeliveryError {
    pub fn persistence_failed(message: impl Into<String>) -> Self {
        Self::PersistenceFailed {
            message: message.into(),
        }
    }

    pub fn corrupt_data(message: impl Into<String>) -> Self {
        Self::CorruptData {
            message: message.into(),
        }
    }

    pub fn retryable(message: impl Into<String>) -> Self {
        Self::Retryable {
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

impl fmt::Display for DeliveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput { field, message } => {
                write!(formatter, "invalid {field}: {message}")
            }
            Self::PersistenceFailed { message }
            | Self::CorruptData { message }
            | Self::UnsupportedMode { message }
            | Self::Retryable { message } => formatter.write_str(message),
            Self::Timeout => formatter.write_str("node delivery persistence timed out"),
            Self::Cancelled => formatter.write_str("node delivery persistence was cancelled"),
        }
    }
}

impl std::error::Error for DeliveryError {}

fn require_nonempty(value: &str, field: &'static str) -> Result<(), DeliveryError> {
    if value.is_empty() {
        Err(DeliveryError::invalid(field, "must not be empty"))
    } else {
        Ok(())
    }
}

fn require_nonnegative(value: i64, field: &'static str) -> Result<(), DeliveryError> {
    if value < 0 {
        Err(DeliveryError::invalid(field, "must be non-negative"))
    } else {
        Ok(())
    }
}

fn require_positive(value: i64, field: &'static str) -> Result<(), DeliveryError> {
    if value <= 0 {
        Err(DeliveryError::invalid(field, "must be positive"))
    } else {
        Ok(())
    }
}

fn validate_pod_identity(pod: &PodIdentity) -> Result<(), DeliveryError> {
    require_nonempty(&pod.namespace, "pod.namespace")?;
    require_nonempty(&pod.name, "pod.name")?;
    require_nonempty(&pod.uid, "pod.uid")
}

/// Persisted scheduling priority. Lower persisted values are more urgent.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(i64)]
pub enum OutboxPriority {
    Lease = 0,
    NodeHealth = 1,
    Workload = 2,
    Diagnostic = 3,
}

impl OutboxPriority {
    pub const fn persisted_value(self) -> i64 {
        self as i64
    }
}

impl TryFrom<i64> for OutboxPriority {
    type Error = DeliveryError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Lease),
            1 => Ok(Self::NodeHealth),
            2 => Ok(Self::Workload),
            3 => Ok(Self::Diagnostic),
            _ => Err(DeliveryError::invalid(
                "outbox.priority",
                "must be one of the persisted values 0 through 3",
            )),
        }
    }
}

/// Whether an older delivery can be removed after a newer terminal Pod delete.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum OutboxSupersedability {
    Never,
    PodStatus,
}

impl OutboxSupersedability {
    pub const fn persisted_value(self) -> i64 {
        match self {
            Self::Never => 0,
            Self::PodStatus => 1,
        }
    }
}

impl TryFrom<i64> for OutboxSupersedability {
    type Error = DeliveryError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Never),
            1 => Ok(Self::PodStatus),
            _ => Err(DeliveryError::invalid(
                "outbox.supersedability",
                "must be persisted as 0 or 1",
            )),
        }
    }
}

/// Explicit actor-owned terminal Pod-delete classification.
///
/// This is persisted metadata. Persistence must never infer it by decoding the
/// opaque payload, and only the Pod lifecycle actor may originate the actor-
/// owned variant.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TerminalDeleteClassification {
    NotTerminalDelete,
    ActorOwnedPodDelete,
}

impl TerminalDeleteClassification {
    pub const fn persisted_value(self) -> i64 {
        match self {
            Self::NotTerminalDelete => 0,
            Self::ActorOwnedPodDelete => 1,
        }
    }
}

impl TryFrom<i64> for TerminalDeleteClassification {
    type Error = DeliveryError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::NotTerminalDelete),
            1 => Ok(Self::ActorOwnedPodDelete),
            _ => Err(DeliveryError::invalid(
                "outbox.terminal_delete",
                "must be persisted as 0 or 1",
            )),
        }
    }
}

/// Whether persistence serializes this delivery with its subject stream.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum OutboxSequencePolicy {
    Unsequenced,
    PerSubject,
}

impl OutboxSequencePolicy {
    pub const fn persisted_value(self) -> i64 {
        match self {
            Self::Unsequenced => 0,
            Self::PerSubject => 1,
        }
    }
}

impl TryFrom<i64> for OutboxSequencePolicy {
    type Error = DeliveryError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Unsequenced),
            1 => Ok(Self::PerSubject),
            _ => Err(DeliveryError::invalid(
                "outbox.sequence_policy",
                "must be persisted as 0 or 1",
            )),
        }
    }
}

/// Persistence-owned classification needed to enqueue, order, dead-letter,
/// and replay opaque outbox payloads without decoding them.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct OutboxClassification {
    priority: OutboxPriority,
    supersedability: OutboxSupersedability,
    terminal_delete: TerminalDeleteClassification,
    sequence_policy: OutboxSequencePolicy,
}

impl OutboxClassification {
    pub fn try_new(
        priority: OutboxPriority,
        supersedability: OutboxSupersedability,
        terminal_delete: TerminalDeleteClassification,
        sequence_policy: OutboxSequencePolicy,
    ) -> Result<Self, DeliveryError> {
        if terminal_delete == TerminalDeleteClassification::ActorOwnedPodDelete
            && supersedability != OutboxSupersedability::Never
        {
            return Err(DeliveryError::invalid(
                "outbox.classification",
                "an actor-owned terminal Pod delete must not be supersedable",
            ));
        }
        if (terminal_delete == TerminalDeleteClassification::ActorOwnedPodDelete
            || supersedability == OutboxSupersedability::PodStatus)
            && sequence_policy != OutboxSequencePolicy::PerSubject
        {
            return Err(DeliveryError::invalid(
                "outbox.classification",
                "Pod status and terminal Pod-delete delivery must be per-subject sequenced",
            ));
        }
        Ok(Self {
            priority,
            supersedability,
            terminal_delete,
            sequence_policy,
        })
    }

    pub fn try_from_persisted(
        priority: i64,
        supersedability: i64,
        terminal_delete: i64,
        sequence_policy: i64,
    ) -> Result<Self, DeliveryError> {
        Self::try_new(
            OutboxPriority::try_from(priority)?,
            OutboxSupersedability::try_from(supersedability)?,
            TerminalDeleteClassification::try_from(terminal_delete)?,
            OutboxSequencePolicy::try_from(sequence_policy)?,
        )
    }

    pub const fn persisted_values(self) -> (i64, i64, i64, i64) {
        (
            self.priority.persisted_value(),
            self.supersedability.persisted_value(),
            self.terminal_delete.persisted_value(),
            self.sequence_policy.persisted_value(),
        )
    }

    pub const fn priority(self) -> OutboxPriority {
        self.priority
    }

    pub const fn supersedability(self) -> OutboxSupersedability {
        self.supersedability
    }

    pub const fn terminal_delete(self) -> TerminalDeleteClassification {
        self.terminal_delete
    }

    pub const fn sequence_policy(self) -> OutboxSequencePolicy {
        self.sequence_policy
    }
}

/// Persisted stream facts. `(0, 0)` is unsequenced, `(stream_id, 0)` is a
/// per-subject stream whose sequence has not yet been assigned, and positive
/// values for both fields are an assigned sequence.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct OutboxSequence {
    stream_id: i64,
    stream_seq: i64,
}

impl OutboxSequence {
    pub const fn unassigned() -> Self {
        Self {
            stream_id: 0,
            stream_seq: 0,
        }
    }

    pub fn try_new(stream_id: i64, stream_seq: i64) -> Result<Self, DeliveryError> {
        require_nonnegative(stream_id, "outbox.stream_id")?;
        require_nonnegative(stream_seq, "outbox.stream_seq")?;
        if stream_id == 0 && stream_seq != 0 {
            return Err(DeliveryError::invalid(
                "outbox.sequence",
                "a positive stream sequence requires a positive stream ID",
            ));
        }
        Ok(Self {
            stream_id,
            stream_seq,
        })
    }

    pub const fn stream_id(self) -> i64 {
        self.stream_id
    }

    pub const fn stream_seq(self) -> i64 {
        self.stream_seq
    }

    pub const fn is_assigned(self) -> bool {
        self.stream_id > 0 && self.stream_seq > 0
    }
}

fn validate_sequence(
    classification: OutboxClassification,
    sequence: OutboxSequence,
    assigned_required: bool,
) -> Result<(), DeliveryError> {
    match classification.sequence_policy() {
        OutboxSequencePolicy::Unsequenced if sequence != OutboxSequence::unassigned() => {
            Err(DeliveryError::invalid(
                "outbox.sequence",
                "an unsequenced delivery cannot carry a stream assignment",
            ))
        }
        OutboxSequencePolicy::PerSubject if assigned_required && !sequence.is_assigned() => {
            Err(DeliveryError::invalid(
                "outbox.sequence",
                "a claimed per-subject delivery must carry an assigned stream sequence",
            ))
        }
        _ => Ok(()),
    }
}

/// Exact persisted subject identity for an outbox item.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxSubject {
    subject_key: String,
    resource: ResourceKey,
    subject_uid: Option<String>,
    pod_uid: String,
}

impl OutboxSubject {
    /// Preserves legacy identity strings without parsing or normalization.
    pub fn new(
        subject_key: impl Into<String>,
        resource: ResourceKey,
        subject_uid: Option<String>,
        pod_uid: impl Into<String>,
    ) -> Self {
        Self {
            subject_key: subject_key.into(),
            resource,
            subject_uid,
            pod_uid: pod_uid.into(),
        }
    }

    pub fn subject_key(&self) -> &str {
        &self.subject_key
    }

    pub const fn resource(&self) -> &ResourceKey {
        &self.resource
    }

    pub fn subject_uid(&self) -> Option<&str> {
        self.subject_uid.as_deref()
    }

    pub fn pod_uid(&self) -> &str {
        &self.pod_uid
    }

    pub fn into_parts(self) -> (String, ResourceKey, Option<String>, String) {
        (
            self.subject_key,
            self.resource,
            self.subject_uid,
            self.pod_uid,
        )
    }
}

/// One idempotent entry to enqueue for durable delivery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxEnqueue {
    idempotency_key: String,
    enqueued_ms: i64,
    subject: OutboxSubject,
    operation: String,
    classification: OutboxClassification,
    payload: Vec<u8>,
    next_due_ms: i64,
}

impl OutboxEnqueue {
    pub fn try_new(
        idempotency_key: impl Into<String>,
        enqueued_ms: i64,
        subject: OutboxSubject,
        operation: impl Into<String>,
        classification: OutboxClassification,
        payload: Vec<u8>,
        next_due_ms: i64,
    ) -> Result<Self, DeliveryError> {
        let idempotency_key = idempotency_key.into();
        let operation = operation.into();
        require_nonempty(&idempotency_key, "idempotency_key")?;
        require_nonempty(&operation, "operation")?;
        require_nonnegative(enqueued_ms, "enqueued_ms")?;
        require_nonnegative(next_due_ms, "next_due_ms")?;
        Ok(Self {
            idempotency_key,
            enqueued_ms,
            subject,
            operation,
            classification,
            payload,
            next_due_ms,
        })
    }

    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
    pub const fn enqueued_ms(&self) -> i64 {
        self.enqueued_ms
    }
    pub const fn subject(&self) -> &OutboxSubject {
        &self.subject
    }
    pub fn operation(&self) -> &str {
        &self.operation
    }
    pub const fn classification(&self) -> OutboxClassification {
        self.classification
    }
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
    pub const fn next_due_ms(&self) -> i64 {
        self.next_due_ms
    }

    pub fn into_parts(
        self,
    ) -> (
        String,
        i64,
        OutboxSubject,
        String,
        OutboxClassification,
        Vec<u8>,
        i64,
    ) {
        (
            self.idempotency_key,
            self.enqueued_ms,
            self.subject,
            self.operation,
            self.classification,
            self.payload,
            self.next_due_ms,
        )
    }
}

/// One claimed or inspectable durable outbox entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxRecord {
    id: i64,
    client_id: String,
    idempotency_key: String,
    enqueued_ms: i64,
    subject: OutboxSubject,
    operation: String,
    classification: OutboxClassification,
    sequence: OutboxSequence,
    payload: Vec<u8>,
    attempt: i64,
    next_due_ms: i64,
    leased_until_ms: i64,
    lease_token: Option<String>,
    last_error: Option<String>,
}

#[allow(clippy::too_many_arguments)]
impl OutboxRecord {
    pub fn try_new(
        id: i64,
        client_id: impl Into<String>,
        idempotency_key: impl Into<String>,
        enqueued_ms: i64,
        subject: OutboxSubject,
        operation: impl Into<String>,
        classification: OutboxClassification,
        sequence: OutboxSequence,
        payload: Vec<u8>,
        attempt: i64,
        next_due_ms: i64,
        leased_until_ms: i64,
        lease_token: Option<String>,
        last_error: Option<String>,
    ) -> Result<Self, DeliveryError> {
        let client_id = client_id.into();
        let idempotency_key = idempotency_key.into();
        let operation = operation.into();
        require_positive(id, "outbox.id")?;
        require_nonempty(&client_id, "outbox.client_id")?;
        require_nonempty(&idempotency_key, "outbox.idempotency_key")?;
        require_nonempty(&operation, "outbox.operation")?;
        require_nonnegative(enqueued_ms, "outbox.enqueued_ms")?;
        validate_sequence(classification, sequence, true)?;
        require_nonnegative(attempt, "outbox.attempt")?;
        require_nonnegative(next_due_ms, "outbox.next_due_ms")?;
        require_nonnegative(leased_until_ms, "outbox.leased_until_ms")?;
        if let Some(token) = lease_token.as_deref() {
            require_nonempty(token, "outbox.lease_token")?;
            require_positive(leased_until_ms, "outbox.leased_until_ms")?;
        } else if leased_until_ms != 0 {
            return Err(DeliveryError::invalid(
                "outbox.lease_token",
                "must be present when leased_until_ms is positive",
            ));
        }
        Ok(Self {
            id,
            client_id,
            idempotency_key,
            enqueued_ms,
            subject,
            operation,
            classification,
            sequence,
            payload,
            attempt,
            next_due_ms,
            leased_until_ms,
            lease_token,
            last_error,
        })
    }

    pub const fn id(&self) -> i64 {
        self.id
    }
    pub fn client_id(&self) -> &str {
        &self.client_id
    }
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
    pub const fn enqueued_ms(&self) -> i64 {
        self.enqueued_ms
    }
    pub const fn subject(&self) -> &OutboxSubject {
        &self.subject
    }
    pub fn operation(&self) -> &str {
        &self.operation
    }
    pub const fn classification(&self) -> OutboxClassification {
        self.classification
    }
    pub const fn sequence(&self) -> OutboxSequence {
        self.sequence
    }
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
    pub const fn attempt(&self) -> i64 {
        self.attempt
    }
    pub const fn next_due_ms(&self) -> i64 {
        self.next_due_ms
    }
    pub const fn leased_until_ms(&self) -> i64 {
        self.leased_until_ms
    }
    pub fn lease_token(&self) -> Option<&str> {
        self.lease_token.as_deref()
    }
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }
}

/// Validated wall-clock input for lease expiry and event-driven wake queries.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct OutboxNow(i64);

impl OutboxNow {
    pub fn try_new(now_ms: i64) -> Result<Self, DeliveryError> {
        require_nonnegative(now_ms, "now_ms")?;
        Ok(Self(now_ms))
    }

    pub const fn get(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxClaimRequest {
    now_ms: i64,
    lease_ms: i64,
    lease_token: String,
}

impl OutboxClaimRequest {
    pub fn try_new(
        now_ms: i64,
        lease_ms: i64,
        lease_token: impl Into<String>,
    ) -> Result<Self, DeliveryError> {
        let lease_token = lease_token.into();
        require_nonnegative(now_ms, "now_ms")?;
        require_positive(lease_ms, "lease_ms")?;
        require_nonempty(&lease_token, "lease_token")?;
        Ok(Self {
            now_ms,
            lease_ms,
            lease_token,
        })
    }
    pub const fn now_ms(&self) -> i64 {
        self.now_ms
    }
    pub const fn lease_ms(&self) -> i64 {
        self.lease_ms
    }
    pub fn lease_token(&self) -> &str {
        &self.lease_token
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxBatchClaimRequest {
    now_ms: i64,
    limit: usize,
    lease_ms: i64,
    lease_token: String,
}

impl OutboxBatchClaimRequest {
    pub fn try_new(
        now_ms: i64,
        limit: usize,
        lease_ms: i64,
        lease_token: impl Into<String>,
    ) -> Result<Self, DeliveryError> {
        let lease_token = lease_token.into();
        require_nonnegative(now_ms, "now_ms")?;
        require_positive(lease_ms, "lease_ms")?;
        require_nonempty(&lease_token, "lease_token")?;
        Ok(Self {
            now_ms,
            limit,
            lease_ms,
            lease_token,
        })
    }
    pub const fn now_ms(&self) -> i64 {
        self.now_ms
    }
    pub const fn limit(&self) -> usize {
        self.limit
    }
    /// Current persistence semantics cap one claimed batch at 256 entries.
    pub const fn effective_limit(&self) -> usize {
        if self.limit > MAX_OUTBOX_BATCH {
            MAX_OUTBOX_BATCH
        } else {
            self.limit
        }
    }
    pub const fn lease_ms(&self) -> i64 {
        self.lease_ms
    }
    pub fn lease_token(&self) -> &str {
        &self.lease_token
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxLease {
    id: i64,
    lease_token: String,
    leased_until_ms: i64,
}

impl OutboxLease {
    pub fn try_new(
        id: i64,
        lease_token: impl Into<String>,
        leased_until_ms: i64,
    ) -> Result<Self, DeliveryError> {
        let lease_token = lease_token.into();
        require_positive(id, "outbox.id")?;
        require_nonempty(&lease_token, "lease_token")?;
        require_positive(leased_until_ms, "leased_until_ms")?;
        Ok(Self {
            id,
            lease_token,
            leased_until_ms,
        })
    }
    pub const fn id(&self) -> i64 {
        self.id
    }
    pub fn lease_token(&self) -> &str {
        &self.lease_token
    }
    pub const fn leased_until_ms(&self) -> i64 {
        self.leased_until_ms
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxAttemptFailure {
    id: i64,
    lease_token: String,
    backoff_until_ms: i64,
    error: String,
}

impl OutboxAttemptFailure {
    pub fn try_new(
        id: i64,
        lease_token: impl Into<String>,
        backoff_until_ms: i64,
        error: impl Into<String>,
    ) -> Result<Self, DeliveryError> {
        let lease_token = lease_token.into();
        require_positive(id, "outbox.id")?;
        require_nonempty(&lease_token, "lease_token")?;
        require_nonnegative(backoff_until_ms, "backoff_until_ms")?;
        Ok(Self {
            id,
            lease_token,
            backoff_until_ms,
            error: error.into(),
        })
    }
    pub const fn id(&self) -> i64 {
        self.id
    }
    pub fn lease_token(&self) -> &str {
        &self.lease_token
    }
    pub const fn backoff_until_ms(&self) -> i64 {
        self.backoff_until_ms
    }
    pub fn error(&self) -> &str {
        &self.error
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxCompletion {
    id: i64,
    lease_token: String,
}

impl OutboxCompletion {
    pub fn try_new(id: i64, lease_token: impl Into<String>) -> Result<Self, DeliveryError> {
        let lease_token = lease_token.into();
        require_positive(id, "outbox.id")?;
        require_nonempty(&lease_token, "lease_token")?;
        Ok(Self { id, lease_token })
    }
    pub const fn id(&self) -> i64 {
        self.id
    }
    pub fn lease_token(&self) -> &str {
        &self.lease_token
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxSupersedeRequest {
    subject_key: String,
    terminal_delete_id: i64,
}

impl OutboxSupersedeRequest {
    pub fn try_new(
        subject_key: impl Into<String>,
        terminal_delete_id: i64,
    ) -> Result<Self, DeliveryError> {
        let subject_key = subject_key.into();
        require_nonempty(&subject_key, "subject_key")?;
        require_positive(terminal_delete_id, "terminal_delete_id")?;
        Ok(Self {
            subject_key,
            terminal_delete_id,
        })
    }
    pub fn subject_key(&self) -> &str {
        &self.subject_key
    }
    pub const fn terminal_delete_id(&self) -> i64 {
        self.terminal_delete_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeadLetterMoveRequest {
    idempotency_key: String,
    max_attempts: i64,
}

impl DeadLetterMoveRequest {
    /// Creates a threshold request. Zero is valid and makes any existing row
    /// eligible regardless of its current attempt count.
    pub fn try_new(
        idempotency_key: impl Into<String>,
        max_attempts: i64,
    ) -> Result<Self, DeliveryError> {
        let idempotency_key = idempotency_key.into();
        require_nonempty(&idempotency_key, "idempotency_key")?;
        require_nonnegative(max_attempts, "max_attempts")?;
        Ok(Self {
            idempotency_key,
            max_attempts,
        })
    }
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
    pub const fn max_attempts(&self) -> i64 {
        self.max_attempts
    }
}

/// Positive dead-letter row identity used by administrative operations.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct DeadLetterKey(i64);

impl DeadLetterKey {
    pub fn try_new(id: i64) -> Result<Self, DeliveryError> {
        require_positive(id, "dead_letter.id")?;
        Ok(Self(id))
    }

    pub const fn get(self) -> i64 {
        self.0
    }
}

/// Explicit replay request. Implementations replay the persisted
/// classification and sequencing policy from the dead-letter row; callers do
/// not reclassify or decode its opaque payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeadLetterReplayRequest {
    key: DeadLetterKey,
}

impl DeadLetterReplayRequest {
    pub const fn new(key: DeadLetterKey) -> Self {
        Self { key }
    }

    pub const fn key(self) -> DeadLetterKey {
        self.key
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeadLetterEntry {
    id: i64,
    original_id: i64,
    idempotency_key: String,
    enqueued_ms: i64,
    subject: OutboxSubject,
    operation: String,
    classification: OutboxClassification,
    sequence: OutboxSequence,
    payload: Vec<u8>,
    attempts: i64,
    last_error: String,
    moved_at_ms: i64,
}

#[allow(clippy::too_many_arguments)]
impl DeadLetterEntry {
    pub fn try_new(
        id: i64,
        original_id: i64,
        idempotency_key: impl Into<String>,
        enqueued_ms: i64,
        subject: OutboxSubject,
        operation: impl Into<String>,
        classification: OutboxClassification,
        sequence: OutboxSequence,
        payload: Vec<u8>,
        attempts: i64,
        last_error: impl Into<String>,
        moved_at_ms: i64,
    ) -> Result<Self, DeliveryError> {
        let idempotency_key = idempotency_key.into();
        let operation = operation.into();
        require_positive(id, "dead_letter.id")?;
        require_positive(original_id, "dead_letter.original_id")?;
        require_nonempty(&idempotency_key, "dead_letter.idempotency_key")?;
        require_nonempty(&operation, "dead_letter.operation")?;
        validate_sequence(classification, sequence, false)?;
        require_nonnegative(enqueued_ms, "dead_letter.enqueued_ms")?;
        require_nonnegative(attempts, "dead_letter.attempts")?;
        require_nonnegative(moved_at_ms, "dead_letter.moved_at_ms")?;
        Ok(Self {
            id,
            original_id,
            idempotency_key,
            enqueued_ms,
            subject,
            operation,
            classification,
            sequence,
            payload,
            attempts,
            last_error: last_error.into(),
            moved_at_ms,
        })
    }
    pub const fn id(&self) -> i64 {
        self.id
    }
    pub const fn original_id(&self) -> i64 {
        self.original_id
    }
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
    pub const fn enqueued_ms(&self) -> i64 {
        self.enqueued_ms
    }
    pub const fn subject(&self) -> &OutboxSubject {
        &self.subject
    }
    pub fn operation(&self) -> &str {
        &self.operation
    }
    pub const fn classification(&self) -> OutboxClassification {
        self.classification
    }
    pub const fn sequence(&self) -> OutboxSequence {
        self.sequence
    }
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
    pub const fn attempts(&self) -> i64 {
        self.attempts
    }
    pub fn last_error(&self) -> &str {
        &self.last_error
    }
    pub const fn moved_at_ms(&self) -> i64 {
        self.moved_at_ms
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct OutboxStats {
    pending: i64,
    oldest_age_seconds: f64,
    dead_letter_count: i64,
    dispatch_total: i64,
    dispatch_errors_total: i64,
}

impl OutboxStats {
    pub fn try_new(
        pending: i64,
        oldest_age_seconds: f64,
        dead_letter_count: i64,
        dispatch_total: i64,
        dispatch_errors_total: i64,
    ) -> Result<Self, DeliveryError> {
        require_nonnegative(pending, "stats.pending")?;
        require_nonnegative(dead_letter_count, "stats.dead_letter_count")?;
        require_nonnegative(dispatch_total, "stats.dispatch_total")?;
        require_nonnegative(dispatch_errors_total, "stats.dispatch_errors_total")?;
        if !oldest_age_seconds.is_finite() {
            return Err(DeliveryError::invalid(
                "stats.oldest_age_seconds",
                "must be finite",
            ));
        }
        if oldest_age_seconds < 0.0 {
            return Err(DeliveryError::invalid(
                "stats.oldest_age_seconds",
                "must be non-negative",
            ));
        }
        Ok(Self {
            pending,
            oldest_age_seconds,
            dead_letter_count,
            dispatch_total,
            dispatch_errors_total,
        })
    }
    pub const fn pending(&self) -> i64 {
        self.pending
    }
    pub const fn oldest_age_seconds(&self) -> f64 {
        self.oldest_age_seconds
    }
    pub const fn dead_letter_count(&self) -> i64 {
        self.dead_letter_count
    }
    pub const fn dispatch_total(&self) -> i64 {
        self.dispatch_total
    }
    pub const fn dispatch_errors_total(&self) -> i64 {
        self.dispatch_errors_total
    }
}

/// UID-only key for node-local checkpoint lookup and deletion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodCheckpointKey {
    pod_uid: String,
}

impl PodCheckpointKey {
    pub fn try_new(pod_uid: impl Into<String>) -> Result<Self, DeliveryError> {
        let pod_uid = pod_uid.into();
        require_nonempty(&pod_uid, "pod_uid")?;
        Ok(Self { pod_uid })
    }
    pub fn pod_uid(&self) -> &str {
        &self.pod_uid
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodStatusCheckpointUpsert {
    pod: PodIdentity,
    base_position: i64,
    status_payload: Vec<u8>,
    updated_ms: i64,
}

impl PodStatusCheckpointUpsert {
    pub fn try_new(
        pod: PodIdentity,
        base_position: i64,
        status_payload: Vec<u8>,
        updated_ms: i64,
    ) -> Result<Self, DeliveryError> {
        validate_pod_identity(&pod)?;
        require_nonnegative(base_position, "base_position")?;
        require_nonnegative(updated_ms, "updated_ms")?;
        Ok(Self {
            pod,
            base_position,
            status_payload,
            updated_ms,
        })
    }
    pub const fn pod(&self) -> &PodIdentity {
        &self.pod
    }
    pub const fn base_position(&self) -> i64 {
        self.base_position
    }
    pub fn status_payload(&self) -> &[u8] {
        &self.status_payload
    }
    pub const fn updated_ms(&self) -> i64 {
        self.updated_ms
    }
    pub fn into_parts(self) -> (PodIdentity, i64, Vec<u8>, i64) {
        (
            self.pod,
            self.base_position,
            self.status_payload,
            self.updated_ms,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodStatusCheckpoint {
    pod: PodIdentity,
    base_position: i64,
    applied_position: Option<i64>,
    status_payload: Vec<u8>,
    updated_ms: i64,
}

impl PodStatusCheckpoint {
    pub fn try_new(
        pod: PodIdentity,
        base_position: i64,
        applied_position: Option<i64>,
        status_payload: Vec<u8>,
        updated_ms: i64,
    ) -> Result<Self, DeliveryError> {
        validate_pod_identity(&pod)?;
        require_nonnegative(base_position, "base_position")?;
        if let Some(position) = applied_position {
            require_nonnegative(position, "applied_position")?;
        }
        require_nonnegative(updated_ms, "updated_ms")?;
        Ok(Self {
            pod,
            base_position,
            applied_position,
            status_payload,
            updated_ms,
        })
    }
    pub const fn pod(&self) -> &PodIdentity {
        &self.pod
    }
    pub const fn base_position(&self) -> i64 {
        self.base_position
    }
    pub const fn applied_position(&self) -> Option<i64> {
        self.applied_position
    }
    pub fn status_payload(&self) -> &[u8] {
        &self.status_payload
    }
    pub const fn updated_ms(&self) -> i64 {
        self.updated_ms
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodStatusCheckpointApplied {
    pod_uid: String,
    applied_position: i64,
    updated_ms: i64,
}

impl PodStatusCheckpointApplied {
    pub fn try_new(
        pod_uid: impl Into<String>,
        applied_position: i64,
        updated_ms: i64,
    ) -> Result<Self, DeliveryError> {
        let pod_uid = pod_uid.into();
        require_nonempty(&pod_uid, "pod_uid")?;
        require_nonnegative(applied_position, "applied_position")?;
        require_nonnegative(updated_ms, "updated_ms")?;
        Ok(Self {
            pod_uid,
            applied_position,
            updated_ms,
        })
    }
    pub fn pod_uid(&self) -> &str {
        &self.pod_uid
    }
    pub const fn applied_position(&self) -> i64 {
        self.applied_position
    }
    pub const fn updated_ms(&self) -> i64 {
        self.updated_ms
    }
}

/// Runtime observation generation representable by the signed persistence
/// column (`0..=i64::MAX`).
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RuntimeObservationGeneration(i64);

impl RuntimeObservationGeneration {
    pub fn try_new(value: i128) -> Result<Self, DeliveryError> {
        if !(0..=i128::from(i64::MAX)).contains(&value) {
            return Err(DeliveryError::invalid(
                "runtime_observation.generation",
                "must be between 0 and i64::MAX",
            ));
        }
        Ok(Self(value as i64))
    }

    pub const fn get(self) -> i64 {
        self.0
    }
}

impl TryFrom<i64> for RuntimeObservationGeneration {
    type Error = DeliveryError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        Self::try_new(i128::from(value))
    }
}

impl TryFrom<u64> for RuntimeObservationGeneration {
    type Error = DeliveryError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::try_new(i128::from(value))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeObservationCheckpoint {
    pod_uid: String,
    container_ids: Vec<String>,
    generation: RuntimeObservationGeneration,
    updated_ms: i64,
}

impl RuntimeObservationCheckpoint {
    pub fn try_new(
        pod_uid: impl Into<String>,
        container_ids: Vec<String>,
        generation: RuntimeObservationGeneration,
        updated_ms: i64,
    ) -> Result<Self, DeliveryError> {
        let pod_uid = pod_uid.into();
        require_nonempty(&pod_uid, "pod_uid")?;
        require_nonnegative(updated_ms, "updated_ms")?;
        Ok(Self {
            pod_uid,
            container_ids,
            generation,
            updated_ms,
        })
    }
    pub fn pod_uid(&self) -> &str {
        &self.pod_uid
    }
    pub fn container_ids(&self) -> &[String] {
        &self.container_ids
    }
    pub const fn generation(&self) -> RuntimeObservationGeneration {
        self.generation
    }
    pub const fn updated_ms(&self) -> i64 {
        self.updated_ms
    }
    pub fn into_parts(self) -> (String, Vec<String>, RuntimeObservationGeneration, i64) {
        (
            self.pod_uid,
            self.container_ids,
            self.generation,
            self.updated_ms,
        )
    }
}

/// Heap-erased future used at the coarse node-persistence boundary.
pub type DeliveryFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, DeliveryError>> + Send + 'a>>;

/// Durable producer authority.
///
/// Enqueue is idempotent by `idempotency_key`. Persistence stores the supplied
/// classification as columns and must not derive it from `operation` or decode
/// `payload`. Per-subject stream identity is assigned by persistence; stream
/// sequence is assigned exactly once when an item first becomes claimable.
pub trait OutboxProducerStore: Send + Sync {
    fn enqueue_outbox(&self, entry: OutboxEnqueue) -> DeliveryFuture<'_, ()>;
}

/// Durable dispatcher authority.
///
/// Claims are atomic across independent claimers. A due row is eligible when
/// its lease is absent or expires at or before `now_ms`. At most one row for a
/// subject and one row for an assigned stream may be in flight: an older live
/// row blocks a younger same-subject or same-stream row. A due actor-owned
/// terminal Pod delete may bypass older, available, supersedable Pod-status
/// rows for that subject; those rows remain until explicit UID-safe terminal
/// completion. Across eligible rows, priority is Lease, NodeHealth, Workload,
/// then Diagnostic, followed by `enqueued_ms` and row ID. While an eligible
/// supersedable Pod-status row exists, unaged Diagnostic work remains last;
/// otherwise it joins Workload immediately. Diagnostic work always ages into
/// Workload after [`OUTBOX_DIAGNOSTIC_AGING_MS`].
///
/// A claim limit of zero returns no rows. Any limit above
/// [`MAX_OUTBOX_BATCH`] is capped. Claim creates one positive lease expiry per
/// row. Renew, failure, and completion are compare-and-swap operations on row
/// ID plus lease token; a missing row or wrong token returns `false` without
/// mutation. Expiry at exactly `now_ms` is expired. Different subjects and
/// streams may be claimed concurrently without waiting for each other.
pub trait OutboxDispatcherStore: Send + Sync {
    fn claim_next_due_outbox(
        &self,
        request: OutboxClaimRequest,
    ) -> DeliveryFuture<'_, Option<OutboxRecord>>;
    fn renew_outbox_lease(&self, lease: OutboxLease) -> DeliveryFuture<'_, bool>;
    fn mark_outbox_attempt_failed(&self, failure: OutboxAttemptFailure)
    -> DeliveryFuture<'_, bool>;
    fn complete_outbox(&self, completion: OutboxCompletion) -> DeliveryFuture<'_, bool>;
    fn requeue_expired_outbox_leases(&self, now: OutboxNow) -> DeliveryFuture<'_, usize>;
    fn next_outbox_wake_ms(&self, now: OutboxNow) -> DeliveryFuture<'_, Option<i64>>;
    fn claim_due_outbox_batch(
        &self,
        request: OutboxBatchClaimRequest,
    ) -> DeliveryFuture<'_, Vec<OutboxRecord>>;
    fn complete_superseded_status_outbox_for_terminal_pod_delete(
        &self,
        request: OutboxSupersedeRequest,
    ) -> DeliveryFuture<'_, usize>;
}

/// Dead-letter administration and outbox diagnostics without queue mutation
/// rights beyond the explicit move/replay operations.
///
/// Moving preserves opaque bytes, classification, and any assigned sequencing
/// facts. Replay consumes that persisted metadata, resets retry/lease state,
/// and does not decode the payload or classify it from the operation string.
pub trait DeadLetterStore: Send + Sync {
    fn move_outbox_to_dead_letter_if_max_attempts(
        &self,
        request: DeadLetterMoveRequest,
    ) -> DeliveryFuture<'_, bool>;
    fn list_dead_letter(&self) -> DeliveryFuture<'_, Vec<DeadLetterEntry>>;
    fn get_dead_letter(&self, key: DeadLetterKey) -> DeliveryFuture<'_, Option<DeadLetterEntry>>;
    fn delete_dead_letter(&self, key: DeadLetterKey) -> DeliveryFuture<'_, bool>;
    fn replay_dead_letter(&self, request: DeadLetterReplayRequest) -> DeliveryFuture<'_, bool>;
    fn outbox_stats(&self) -> DeliveryFuture<'_, OutboxStats>;
}

/// UID-bound Pod status checkpoint persistence.
pub trait PodStatusCheckpointStore: Send + Sync {
    fn upsert_pod_status_checkpoint(
        &self,
        checkpoint: PodStatusCheckpointUpsert,
    ) -> DeliveryFuture<'_, ()>;
    fn get_pod_status_checkpoint(
        &self,
        key: PodCheckpointKey,
    ) -> DeliveryFuture<'_, Option<PodStatusCheckpoint>>;
    fn mark_pod_status_checkpoint_applied(
        &self,
        applied: PodStatusCheckpointApplied,
    ) -> DeliveryFuture<'_, ()>;
    fn delete_pod_status_checkpoint(&self, key: PodCheckpointKey) -> DeliveryFuture<'_, ()>;
}

/// UID-bound runtime-observation checkpoint persistence.
pub trait RuntimeObservationCheckpointStore: Send + Sync {
    fn upsert_runtime_observation_checkpoint(
        &self,
        checkpoint: RuntimeObservationCheckpoint,
    ) -> DeliveryFuture<'_, ()>;
    fn get_runtime_observation_checkpoint(
        &self,
        key: PodCheckpointKey,
    ) -> DeliveryFuture<'_, Option<RuntimeObservationCheckpoint>>;
    fn delete_runtime_observation_checkpoint(
        &self,
        key: PodCheckpointKey,
    ) -> DeliveryFuture<'_, ()>;
}
