//! Pure resource-apply decisions shared by persistence adapters.

use serde_json::Value;

/// Which preconditions an apply adapter must enforce.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyPreconditionPolicy {
    /// Validate structural presence plus exact UID/resourceVersion CAS.
    Strict,
    /// Validate only create/update presence so legacy follower replay converges.
    PresenceOnly,
}

/// Borrowed apply requirements from a resource mutation envelope.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ApplyPreconditions<'a> {
    pub require_absent: bool,
    pub require_existing: bool,
    pub uid: Option<&'a str>,
    pub resource_version: Option<i64>,
}

/// The live identity/version facts needed by apply policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CurrentResourceState<'a> {
    pub uid: Option<&'a str>,
    pub resource_version: i64,
}

/// Stable, adapter-neutral precondition failure classification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApplyPreconditionViolation {
    AlreadyExists,
    NotFound,
    Uid {
        expected: String,
        actual: Option<String>,
    },
    ResourceVersion {
        expected: i64,
        actual: i64,
    },
}

impl std::fmt::Display for ApplyPreconditionViolation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyExists => formatter.write_str("resource already exists"),
            Self::NotFound => formatter.write_str("resource not found"),
            Self::Uid { expected, actual } => write!(
                formatter,
                "UID precondition failed: expected {expected} got {}",
                actual.as_deref().unwrap_or("<missing>")
            ),
            Self::ResourceVersion { expected, actual } => write!(
                formatter,
                "resourceVersion precondition failed: expected {expected} got {actual}"
            ),
        }
    }
}

impl std::error::Error for ApplyPreconditionViolation {}

/// Validate structural and, for strict apply, exact UID/resourceVersion CAS.
pub fn validate_apply_preconditions(
    policy: ApplyPreconditionPolicy,
    preconditions: ApplyPreconditions<'_>,
    current: Option<CurrentResourceState<'_>>,
) -> Result<(), ApplyPreconditionViolation> {
    if preconditions.require_absent && current.is_some() {
        return Err(ApplyPreconditionViolation::AlreadyExists);
    }
    if preconditions.require_existing && current.is_none() {
        return Err(ApplyPreconditionViolation::NotFound);
    }
    if policy == ApplyPreconditionPolicy::PresenceOnly {
        return Ok(());
    }
    let Some(current) = current else {
        return Ok(());
    };
    if let Some(expected) = preconditions.uid
        && current.uid != Some(expected)
    {
        return Err(ApplyPreconditionViolation::Uid {
            expected: expected.to_string(),
            actual: current.uid.map(str::to_string),
        });
    }
    if let Some(expected) = preconditions.resource_version
        && current.resource_version != expected
    {
        return Err(ApplyPreconditionViolation::ResourceVersion {
            expected,
            actual: current.resource_version,
        });
    }
    Ok(())
}

/// Kubernetes watch transition produced by a visible resource mutation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResourceEventType {
    Added,
    Modified,
    Deleted,
}

impl ResourceEventType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Added => "ADDED",
            Self::Modified => "MODIFIED",
            Self::Deleted => "DELETED",
        }
    }
}

/// Whether an already-normalized PUT changes public resource state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResourceWriteDecision {
    NoOp,
    Write(ResourceEventType),
}

/// Decide the PUT no-op and corresponding Kubernetes event transition.
pub const fn decide_resource_put(
    resource_exists: bool,
    same_resource_version_and_body: bool,
) -> ResourceWriteDecision {
    if resource_exists && same_resource_version_and_body {
        ResourceWriteDecision::NoOp
    } else if resource_exists {
        ResourceWriteDecision::Write(ResourceEventType::Modified)
    } else {
        ResourceWriteDecision::Write(ResourceEventType::Added)
    }
}

/// Whether an UID-bound delete changes public resource state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResourceDeleteDecision {
    NoOp,
    Delete(ResourceEventType),
}

/// Decide delete no-op/CAS behavior without performing persistence effects.
pub fn decide_resource_delete(
    policy: ApplyPreconditionPolicy,
    requested_uid: Option<&str>,
    expected_resource_version: Option<i64>,
    current: Option<CurrentResourceState<'_>>,
) -> Result<ResourceDeleteDecision, ApplyPreconditionViolation> {
    let Some(current) = current else {
        return Ok(ResourceDeleteDecision::NoOp);
    };
    if requested_uid.is_some_and(|uid| !uid.is_empty() && current.uid != Some(uid)) {
        return Ok(ResourceDeleteDecision::NoOp);
    }
    validate_apply_preconditions(
        policy,
        ApplyPreconditions {
            resource_version: expected_resource_version,
            ..ApplyPreconditions::default()
        },
        Some(current),
    )?;
    Ok(ResourceDeleteDecision::Delete(ResourceEventType::Deleted))
}

/// Status-stamp freshness decides whether public state/RV/watch may change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatusStampDecision {
    ApplyResourceMutation,
    /// Persist the idempotency ledger only; preserve public RV and watch state.
    RecordLedgerOnly,
}

/// Compare a positive incoming status stamp with the latest applied stamp.
pub const fn decide_status_stamp(
    last_applied_stamp: Option<i64>,
    incoming_stamp: Option<i64>,
) -> StatusStampDecision {
    match (last_applied_stamp, incoming_stamp) {
        (Some(last), Some(incoming)) if incoming > 0 && incoming <= last => {
            StatusStampDecision::RecordLedgerOnly
        }
        _ => StatusStampDecision::ApplyResourceMutation,
    }
}

/// Structural equality ignoring one top-level metadata field without cloning.
pub fn resource_bodies_equal_ignoring_metadata_field(
    left: &Value,
    right: &Value,
    metadata_field: &str,
) -> bool {
    let (Value::Object(left), Value::Object(right)) = (left, right) else {
        return left == right;
    };
    if left.len() != right.len() {
        return false;
    }
    left.iter().all(|(key, left_value)| {
        right.get(key).is_some_and(|right_value| {
            if key == "metadata" {
                objects_equal_ignoring_key(left_value, right_value, metadata_field)
            } else {
                left_value == right_value
            }
        })
    })
}

fn objects_equal_ignoring_key(left: &Value, right: &Value, key: &str) -> bool {
    let (Value::Object(left), Value::Object(right)) = (left, right) else {
        return left == right;
    };
    let left_count = left.keys().filter(|candidate| *candidate != key).count();
    let right_count = right.keys().filter(|candidate| *candidate != key).count();
    left_count == right_count
        && left.iter().all(|(candidate, value)| {
            candidate == key || right.get(candidate).is_some_and(|other| other == value)
        })
}

/// Compare only client-owned resource state, excluding status/server metadata.
pub fn resource_client_owned_state_equal(left: &Value, right: &Value) -> bool {
    let mut left = left.clone();
    let mut right = right.clone();
    strip_status_and_server_metadata(&mut left);
    strip_status_and_server_metadata(&mut right);
    left == right
}

fn strip_status_and_server_metadata(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    object.remove("status");
    let Some(metadata) = object.get_mut("metadata").and_then(Value::as_object_mut) else {
        return;
    };
    for key in [
        "resourceVersion",
        "uid",
        "creationTimestamp",
        "generation",
        "deletionTimestamp",
        "deletionGracePeriodSeconds",
        "managedFields",
    ] {
        metadata.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn strict_and_presence_preconditions_are_table_driven() {
        struct Case<'a> {
            name: &'static str,
            policy: ApplyPreconditionPolicy,
            preconditions: ApplyPreconditions<'a>,
            current: Option<CurrentResourceState<'a>>,
            expected: Option<ApplyPreconditionViolation>,
        }
        let current = CurrentResourceState {
            uid: Some("live"),
            resource_version: 7,
        };
        let cases = [
            Case {
                name: "strict match",
                policy: ApplyPreconditionPolicy::Strict,
                preconditions: ApplyPreconditions {
                    uid: Some("live"),
                    resource_version: Some(7),
                    ..ApplyPreconditions::default()
                },
                current: Some(current),
                expected: None,
            },
            Case {
                name: "strict uid mismatch",
                policy: ApplyPreconditionPolicy::Strict,
                preconditions: ApplyPreconditions {
                    uid: Some("old"),
                    ..ApplyPreconditions::default()
                },
                current: Some(current),
                expected: Some(ApplyPreconditionViolation::Uid {
                    expected: "old".to_string(),
                    actual: Some("live".to_string()),
                }),
            },
            Case {
                name: "strict rv mismatch",
                policy: ApplyPreconditionPolicy::Strict,
                preconditions: ApplyPreconditions {
                    resource_version: Some(6),
                    ..ApplyPreconditions::default()
                },
                current: Some(current),
                expected: Some(ApplyPreconditionViolation::ResourceVersion {
                    expected: 6,
                    actual: 7,
                }),
            },
            Case {
                name: "legacy ignores uid and rv",
                policy: ApplyPreconditionPolicy::PresenceOnly,
                preconditions: ApplyPreconditions {
                    uid: Some("old"),
                    resource_version: Some(6),
                    ..ApplyPreconditions::default()
                },
                current: Some(current),
                expected: None,
            },
            Case {
                name: "required existing is absent",
                policy: ApplyPreconditionPolicy::PresenceOnly,
                preconditions: ApplyPreconditions {
                    require_existing: true,
                    ..ApplyPreconditions::default()
                },
                current: None,
                expected: Some(ApplyPreconditionViolation::NotFound),
            },
            Case {
                name: "required absent already exists",
                policy: ApplyPreconditionPolicy::Strict,
                preconditions: ApplyPreconditions {
                    require_absent: true,
                    ..ApplyPreconditions::default()
                },
                current: Some(current),
                expected: Some(ApplyPreconditionViolation::AlreadyExists),
            },
        ];
        for case in cases {
            assert_eq!(
                validate_apply_preconditions(case.policy, case.preconditions, case.current).err(),
                case.expected,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn resource_and_status_transition_decisions_are_table_driven() {
        let puts = [
            (
                false,
                false,
                ResourceWriteDecision::Write(ResourceEventType::Added),
            ),
            (
                true,
                false,
                ResourceWriteDecision::Write(ResourceEventType::Modified),
            ),
            (true, true, ResourceWriteDecision::NoOp),
        ];
        for (exists, equal, expected) in puts {
            assert_eq!(decide_resource_put(exists, equal), expected);
        }
        let stamps = [
            (None, None, StatusStampDecision::ApplyResourceMutation),
            (
                Some(9),
                Some(10),
                StatusStampDecision::ApplyResourceMutation,
            ),
            (Some(10), Some(10), StatusStampDecision::RecordLedgerOnly),
            (Some(11), Some(10), StatusStampDecision::RecordLedgerOnly),
            (
                Some(11),
                Some(0),
                StatusStampDecision::ApplyResourceMutation,
            ),
        ];
        for (last, incoming, expected) in stamps {
            assert_eq!(decide_status_stamp(last, incoming), expected);
        }
    }

    #[test]
    fn delete_outcomes_preserve_uid_safety_and_strict_rv() {
        let current = CurrentResourceState {
            uid: Some("new"),
            resource_version: 9,
        };
        assert_eq!(
            decide_resource_delete(
                ApplyPreconditionPolicy::Strict,
                Some("old"),
                Some(8),
                Some(current)
            ),
            Ok(ResourceDeleteDecision::NoOp)
        );
        assert_eq!(
            decide_resource_delete(
                ApplyPreconditionPolicy::Strict,
                Some("new"),
                Some(8),
                Some(current)
            ),
            Err(ApplyPreconditionViolation::ResourceVersion {
                expected: 8,
                actual: 9
            })
        );
        assert_eq!(
            decide_resource_delete(ApplyPreconditionPolicy::PresenceOnly, None, Some(8), None),
            Ok(ResourceDeleteDecision::NoOp)
        );
    }

    #[test]
    fn resource_equality_helpers_preserve_no_op_contracts() {
        let left = json!({
            "metadata": {"name": "n", "resourceVersion": "7", "uid": "u"},
            "spec": {"value": 1},
            "status": {"ready": false}
        });
        let right = json!({
            "metadata": {"name": "n", "resourceVersion": "8", "uid": "u"},
            "spec": {"value": 1},
            "status": {"ready": false}
        });
        assert!(resource_bodies_equal_ignoring_metadata_field(
            &left,
            &right,
            "resourceVersion"
        ));
        let server_only = json!({
            "metadata": {"name": "n", "resourceVersion": "9", "uid": "other"},
            "spec": {"value": 1},
            "status": {"ready": true}
        });
        assert!(resource_client_owned_state_equal(&left, &server_only));
        assert!(!resource_client_owned_state_equal(
            &left,
            &json!({"metadata": {"name": "n"}, "spec": {"value": 2}})
        ));
    }
}
