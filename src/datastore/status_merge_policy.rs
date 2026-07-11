use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusApplyFreshness {
    Fresh,
    Stale,
}

/// Originator of a status apply. Selects the typed merge owner for kinds whose
/// status is co-owned by multiple writers (currently Pod; Node routes through
/// its own typed delegate regardless of origin).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusApplyOrigin {
    /// Raft replication / leader-direct apply without an outbox stamp.
    ReplicatedApply,
    /// Kubelet outbox status snapshot (carries a monotonic stamp; the kubelet
    /// terminal-state preservation guarantee applies).
    KubeletOutbox,
    /// A client write through an API `/status` subresource.
    ApiSubresource,
}

/// How stale condition arrays are merged against the live status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConditionMergeMode {
    /// Drop live conditions entirely; keep only the incoming ones.
    ReplaceAll,
    /// Keep incoming conditions, back-filling live condition types the writer
    /// omitted, keyed by `type` (never overwrite an incoming condition).
    PreserveUnmentionedByType,
    /// Keep live conditions on same-type collisions, while preserving incoming
    /// condition types the live status does not have.
    PreserveLiveByType,
    /// Merge by `type`, preferring whichever of the live/incoming condition has
    /// the newer `lastTransitionTime` (live wins ties / when incoming lacks it).
    MergeByNewestTransitionTime,
}

/// How stale non-condition status fields are merged against the live status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldMergeMode {
    /// Drop live fields; keep only incoming.
    ReplaceAll,
    /// Keep incoming fields, back-filling live fields the writer omitted.
    PreserveUnmentioned,
    /// Keep live fields on key collisions, while preserving incoming fields the
    /// live status does not have.
    PreserveLive,
}

/// Parameterized stale-status behavior for a generic (non-typed) kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenericStaleStatusMode {
    /// Authoritatively replace incoming stale status with the live status.
    UseLiveStatus,
    /// Merge incoming with live per the contained condition/field policies.
    Merge {
        condition_merge: ConditionMergeMode,
        field_merge: FieldMergeMode,
    },
}

/// Parameterized merge policy for a generic kind. Table data, not enum variants:
/// new generic kinds are expressed by adding a registry entry, not a new
/// `StatusMergeProfileKind` variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenericStatusMergePolicy {
    /// Condition `type`s whose presence (status `True`) marks the resource
    /// terminal; a terminal live status is preserved authoritatively.
    pub terminal_condition_types: &'static [&'static str],
    pub stale_mode: GenericStaleStatusMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusMergeProfileKind {
    PodTyped,
    NodeTyped,
    Generic(GenericStatusMergePolicy),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatusMergeProfile {
    pub kind: StatusMergeProfileKind,
}

impl StatusMergeProfile {
    pub const fn new(kind: StatusMergeProfileKind) -> Self {
        Self { kind }
    }
}

#[derive(Default)]
pub struct StatusMergeRegistry {
    _private: (),
}

impl StatusMergeRegistry {
    pub fn profile(&self, api_version: &str, kind: &str) -> StatusMergeProfile {
        match (api_version, kind) {
            ("v1", "Pod") => StatusMergeProfile::new(StatusMergeProfileKind::PodTyped),
            ("v1", "Node") => StatusMergeProfile::new(StatusMergeProfileKind::NodeTyped),
            ("batch/v1", "Job") => {
                StatusMergeProfile::new(StatusMergeProfileKind::Generic(GenericStatusMergePolicy {
                    terminal_condition_types: &["Complete", "Failed"],
                    stale_mode: GenericStaleStatusMode::Merge {
                        condition_merge: ConditionMergeMode::MergeByNewestTransitionTime,
                        field_merge: FieldMergeMode::PreserveUnmentioned,
                    },
                }))
            }
            ("batch/v1", "CronJob") => {
                StatusMergeProfile::new(StatusMergeProfileKind::Generic(GenericStatusMergePolicy {
                    terminal_condition_types: &[],
                    stale_mode: GenericStaleStatusMode::Merge {
                        condition_merge: ConditionMergeMode::PreserveUnmentionedByType,
                        field_merge: FieldMergeMode::PreserveUnmentioned,
                    },
                }))
            }
            ("policy/v1", "PodDisruptionBudget") => {
                StatusMergeProfile::new(StatusMergeProfileKind::Generic(GenericStatusMergePolicy {
                    terminal_condition_types: &[],
                    stale_mode: GenericStaleStatusMode::Merge {
                        condition_merge: ConditionMergeMode::PreserveUnmentionedByType,
                        field_merge: FieldMergeMode::PreserveUnmentioned,
                    },
                }))
            }
            ("v1", "PersistentVolume") | ("v1", "PersistentVolumeClaim") => {
                StatusMergeProfile::new(StatusMergeProfileKind::Generic(GenericStatusMergePolicy {
                    terminal_condition_types: &[],
                    stale_mode: GenericStaleStatusMode::Merge {
                        condition_merge: ConditionMergeMode::PreserveLiveByType,
                        field_merge: FieldMergeMode::PreserveLive,
                    },
                }))
            }
            ("apps/v1", "ReplicaSet")
            | ("apps/v1", "Deployment")
            | ("apps/v1", "StatefulSet")
            | ("apps/v1", "DaemonSet") => {
                StatusMergeProfile::new(StatusMergeProfileKind::Generic(GenericStatusMergePolicy {
                    terminal_condition_types: &[],
                    stale_mode: GenericStaleStatusMode::Merge {
                        condition_merge: ConditionMergeMode::PreserveUnmentionedByType,
                        field_merge: FieldMergeMode::PreserveUnmentioned,
                    },
                }))
            }
            ("v1", "Service") => {
                StatusMergeProfile::new(StatusMergeProfileKind::Generic(GenericStatusMergePolicy {
                    terminal_condition_types: &[],
                    stale_mode: GenericStaleStatusMode::Merge {
                        condition_merge: ConditionMergeMode::PreserveUnmentionedByType,
                        field_merge: FieldMergeMode::PreserveUnmentioned,
                    },
                }))
            }
            _ => {
                StatusMergeProfile::new(StatusMergeProfileKind::Generic(GenericStatusMergePolicy {
                    terminal_condition_types: &[],
                    stale_mode: GenericStaleStatusMode::UseLiveStatus,
                }))
            }
        }
    }
}

pub fn merge_status_for_apply(
    api_version: &str,
    kind: &str,
    live_resource: &Value,
    incoming_status: &mut Value,
    freshness: StatusApplyFreshness,
    origin: StatusApplyOrigin,
) {
    let profile = StatusMergeRegistry::default().profile(api_version, kind);

    // A fresh apply of most generic kinds is authoritative — the writer had the
    // latest resourceVersion, so its status replaces the live one without
    // merging (the registry has nothing to preserve). Pod and Node are never
    // short-circuited: Pod status is always reconciled against the live
    // kubelet-owned object, and Node status is a heartbeat where each reporter
    // sends partial conditions that must always be merged by transition time
    // (a stale worker Ready=True must not overwrite a fresher leader Unknown).
    // Service is explicit exception: even fresh status writes must preserve
    // external/live fields (loadBalancer, annotations, etc.) managed by other
    // actors.
    if freshness == StatusApplyFreshness::Fresh
        && matches!(profile.kind, StatusMergeProfileKind::Generic(_))
        && !(api_version == "v1" && kind == "Service")
    {
        return;
    }

    match profile.kind {
        StatusMergeProfileKind::PodTyped => {
            merge_pod_status(live_resource, incoming_status, origin)
        }
        StatusMergeProfileKind::NodeTyped => {
            crate::kubelet::node::merge_node_status_for_update(incoming_status, live_resource);
        }
        StatusMergeProfileKind::Generic(policy) => {
            merge_generic_status(policy, live_resource, incoming_status)
        }
    }
}

/// Pure (no I/O) status-merge decision shared by every apply/forward site so the
/// per-kind `StatusMergeRegistry` is the single owner of preservation behavior
/// and dispatch sites never re-filter by kind (raft-fix.md: collapse the
/// load→freshness→origin→merge boilerplate that was copy-pasted across the
/// sqlite/replicated/forwarded apply paths with diverging `Pod || Node` gates).
///
/// Mutates `incoming_status` in place:
/// - a **stale** apply (`expected_rv < current_rv`) routes through the
///   registry's per-kind policy — Pod/Node typed merge, and for generic kinds
///   their registered `Merge` policy preserves live actor-owned
///   fields/conditions instead of clobbering them;
/// - a **fresh** non-`Service` non-Pod apply is a no-op (the registry
///   early-returns), so it is always safe to call this whenever a live row
///   exists. Pod always merges (typed) regardless of freshness, and Service
///   gets merge-based preservation for API-sourced status writes.
///
/// `kubelet_origin` is true when the command carries a kubelet status-outbox
/// stamp (`observed_status_stamp.is_some()`); the origin is stamp-derived, not
/// kind-derived. Returns the resolved freshness so each caller can apply its
/// own resourceVersion-precondition clear rule (the authoritative raft-apply
/// path clears for every kind; worker-forward paths keep their Pod
/// idempotency-stamp rule).
pub fn apply_status_merge(
    api_version: &str,
    kind: &str,
    live_resource: &Value,
    incoming_status: &mut Value,
    expected_rv: Option<i64>,
    current_rv: i64,
    kubelet_origin: bool,
) -> StatusApplyFreshness {
    let freshness = if expected_rv.is_some_and(|expected| expected < current_rv) {
        StatusApplyFreshness::Stale
    } else {
        StatusApplyFreshness::Fresh
    };
    let origin = if kubelet_origin {
        StatusApplyOrigin::KubeletOutbox
    } else {
        StatusApplyOrigin::ReplicatedApply
    };
    merge_status_for_apply(
        api_version,
        kind,
        live_resource,
        incoming_status,
        freshness,
        origin,
    );
    freshness
}

fn merge_generic_status(
    policy: GenericStatusMergePolicy,
    live_resource: &Value,
    incoming_status: &mut Value,
) {
    if live_has_terminal_condition(live_resource, policy.terminal_condition_types) {
        preserve_live_status_authoritatively(live_resource, incoming_status);
        return;
    }
    match policy.stale_mode {
        GenericStaleStatusMode::UseLiveStatus => {
            preserve_live_status_authoritatively(live_resource, incoming_status);
        }
        GenericStaleStatusMode::Merge {
            condition_merge,
            field_merge,
        } => {
            match condition_merge {
                ConditionMergeMode::ReplaceAll => {}
                ConditionMergeMode::PreserveUnmentionedByType => {
                    preserve_unmentioned_live_status_conditions_by_type(
                        live_resource,
                        incoming_status,
                    );
                }
                ConditionMergeMode::PreserveLiveByType => {
                    preserve_live_status_conditions_by_type(live_resource, incoming_status);
                }
                ConditionMergeMode::MergeByNewestTransitionTime => {
                    merge_conditions_by_newest_transition_time(live_resource, incoming_status);
                }
            }
            match field_merge {
                FieldMergeMode::ReplaceAll => {}
                FieldMergeMode::PreserveUnmentioned => {
                    preserve_unmentioned_live_status_fields(live_resource, incoming_status);
                }
                FieldMergeMode::PreserveLive => {
                    preserve_live_status_fields(live_resource, incoming_status);
                }
            }
        }
    }
}

fn live_has_terminal_condition(live_resource: &Value, terminal_types: &[&str]) -> bool {
    live_resource
        .pointer("/status/conditions")
        .and_then(Value::as_array)
        .is_some_and(|conditions| {
            conditions.iter().any(|condition| {
                condition
                    .get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|condition_type| terminal_types.contains(&condition_type))
                    && condition.get("status").and_then(Value::as_str) == Some("True")
            })
        })
}

fn merge_conditions_by_newest_transition_time(live_resource: &Value, incoming_status: &mut Value) {
    let Some(live_conditions) = live_resource
        .pointer("/status/conditions")
        .and_then(Value::as_array)
    else {
        return;
    };
    if live_conditions.is_empty() {
        return;
    }
    let Some(status_obj) = incoming_status.as_object_mut() else {
        return;
    };
    let incoming_conditions = status_obj
        .entry("conditions".to_string())
        .or_insert_with(|| Value::Array(Vec::new()));
    let Some(incoming_conditions) = incoming_conditions.as_array_mut() else {
        return;
    };

    let mut seen_types = std::collections::HashSet::new();
    for incoming in incoming_conditions.iter_mut() {
        let Some(condition_type) = incoming
            .get("type")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
        else {
            continue;
        };
        if let Some(live_condition) = live_conditions.iter().find(|condition| {
            condition.get("type").and_then(Value::as_str) == Some(condition_type.as_str())
        }) && live_condition_is_newer(live_condition, incoming)
        {
            *incoming = live_condition.clone();
        }
        seen_types.insert(condition_type);
    }

    for live_condition in live_conditions {
        let Some(condition_type) = live_condition
            .get("type")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        if seen_types.insert(condition_type.to_string()) {
            incoming_conditions.push(live_condition.clone());
        }
    }
}

fn live_condition_is_newer(live_condition: &Value, incoming_condition: &Value) -> bool {
    match (
        condition_last_transition_time(live_condition),
        condition_last_transition_time(incoming_condition),
    ) {
        (Some(live), Some(incoming)) => live > incoming,
        (Some(_), None) => true,
        _ => false,
    }
}

fn condition_last_transition_time(
    condition: &Value,
) -> Option<chrono::DateTime<chrono::FixedOffset>> {
    condition
        .get("lastTransitionTime")
        .and_then(Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
}

fn preserve_live_status_authoritatively(live_resource: &Value, incoming_status: &mut Value) {
    let Some(live_status) = live_resource.get("status") else {
        return;
    };
    *incoming_status = live_status.clone();
}

fn preserve_unmentioned_live_status_fields(live_resource: &Value, incoming_status: &mut Value) {
    let Some(live_status) = live_resource.get("status").and_then(Value::as_object) else {
        return;
    };
    let Some(incoming_status) = incoming_status.as_object_mut() else {
        return;
    };
    for (key, value) in live_status {
        incoming_status
            .entry(key.clone())
            .or_insert_with(|| value.clone());
    }
}

fn preserve_live_status_fields(live_resource: &Value, incoming_status: &mut Value) {
    let Some(live_status) = live_resource.get("status").and_then(Value::as_object) else {
        return;
    };
    let Some(incoming_status) = incoming_status.as_object_mut() else {
        return;
    };
    for (key, value) in live_status {
        if key == "conditions" {
            continue;
        }
        incoming_status.insert(key.clone(), value.clone());
    }
}

fn preserve_unmentioned_live_status_conditions_by_type(
    live_resource: &Value,
    incoming_status: &mut Value,
) {
    let Some(live_conditions) = live_resource
        .pointer("/status/conditions")
        .and_then(Value::as_array)
    else {
        return;
    };
    if live_conditions.is_empty() {
        return;
    }
    let Some(status_obj) = incoming_status.as_object_mut() else {
        return;
    };
    let incoming_conditions = status_obj
        .entry("conditions".to_string())
        .or_insert_with(|| Value::Array(Vec::new()));
    let Some(incoming_conditions) = incoming_conditions.as_array_mut() else {
        return;
    };

    let mut seen_types = std::collections::HashSet::new();
    for incoming in incoming_conditions.iter() {
        let Some(condition_type) = incoming
            .get("type")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
        else {
            continue;
        };
        seen_types.insert(condition_type);
    }

    for live_condition in live_conditions {
        let Some(condition_type) = live_condition
            .get("type")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        if seen_types.insert(condition_type.to_string()) {
            incoming_conditions.push(live_condition.clone());
        }
    }
}

fn preserve_live_status_conditions_by_type(live_resource: &Value, incoming_status: &mut Value) {
    let Some(live_conditions) = live_resource
        .pointer("/status/conditions")
        .and_then(Value::as_array)
    else {
        return;
    };
    if live_conditions.is_empty() {
        return;
    }
    let Some(status_obj) = incoming_status.as_object_mut() else {
        return;
    };
    let incoming_conditions = status_obj
        .entry("conditions".to_string())
        .or_insert_with(|| Value::Array(Vec::new()));
    let Some(incoming_conditions) = incoming_conditions.as_array_mut() else {
        return;
    };

    let mut seen_types = std::collections::HashSet::new();
    for incoming in incoming_conditions.iter_mut() {
        let Some(condition_type) = incoming
            .get("type")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
        else {
            continue;
        };
        if let Some(live_condition) = live_conditions.iter().find(|condition| {
            condition.get("type").and_then(Value::as_str) == Some(condition_type.as_str())
        }) {
            *incoming = live_condition.clone();
        }
        seen_types.insert(condition_type);
    }

    for live_condition in live_conditions {
        let Some(condition_type) = live_condition
            .get("type")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        if seen_types.insert(condition_type.to_string()) {
            incoming_conditions.push(live_condition.clone());
        }
    }
}

fn merge_pod_status(live_resource: &Value, incoming_status: &mut Value, origin: StatusApplyOrigin) {
    let owner = match origin {
        StatusApplyOrigin::KubeletOutbox => crate::pod_status_merge::PodStatusOwner::KubeletRuntime,
        StatusApplyOrigin::ApiSubresource => {
            crate::pod_status_merge::PodStatusOwner::ApiStatusSubresource
        }
        StatusApplyOrigin::ReplicatedApply => {
            crate::pod_status_merge::PodStatusOwner::ReplicatedApply
        }
    };
    crate::pod_status_merge::merge_pod_status_for_update(
        "v1",
        "Pod",
        live_resource,
        incoming_status,
        owner,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct GenericStatusPolicyCase {
        api_version: &'static str,
        kind: &'static str,
        terminal_type: Option<&'static str>,
        live_preserved_field: (&'static str, serde_json::Value),
    }

    struct ServiceStatusMergePathCase {
        label: &'static str,
        freshness: StatusApplyFreshness,
        origin: StatusApplyOrigin,
        incoming_status: serde_json::Value,
        expected_lb_ip: &'static str,
        expected_conditions: &'static [(&'static str, &'static str)],
    }

    fn generic_status_policy_cases() -> [GenericStatusPolicyCase; 10] {
        [
            GenericStatusPolicyCase {
                api_version: "batch/v1",
                kind: "Job",
                terminal_type: Some("Complete"),
                live_preserved_field: ("startTime", json!("2026-07-01T00:00:00Z")),
            },
            GenericStatusPolicyCase {
                api_version: "batch/v1",
                kind: "CronJob",
                terminal_type: None,
                live_preserved_field: ("active", json!([])),
            },
            GenericStatusPolicyCase {
                api_version: "policy/v1",
                kind: "PodDisruptionBudget",
                terminal_type: None,
                live_preserved_field: ("observedGeneration", json!(7)),
            },
            GenericStatusPolicyCase {
                api_version: "v1",
                kind: "PersistentVolume",
                terminal_type: None,
                live_preserved_field: ("phase", json!("Bound")),
            },
            GenericStatusPolicyCase {
                api_version: "v1",
                kind: "PersistentVolumeClaim",
                terminal_type: None,
                live_preserved_field: ("phase", json!("Bound")),
            },
            GenericStatusPolicyCase {
                api_version: "apps/v1",
                kind: "ReplicaSet",
                terminal_type: None,
                live_preserved_field: ("replicas", json!(1)),
            },
            GenericStatusPolicyCase {
                api_version: "apps/v1",
                kind: "Deployment",
                terminal_type: None,
                live_preserved_field: ("replicas", json!(1)),
            },
            GenericStatusPolicyCase {
                api_version: "apps/v1",
                kind: "StatefulSet",
                terminal_type: None,
                live_preserved_field: ("replicas", json!(1)),
            },
            GenericStatusPolicyCase {
                api_version: "apps/v1",
                kind: "DaemonSet",
                terminal_type: None,
                live_preserved_field: ("numberReady", json!(1)),
            },
            GenericStatusPolicyCase {
                api_version: "v1",
                kind: "Service",
                terminal_type: None,
                live_preserved_field: (
                    "loadBalancer",
                    json!({"ingress": [{"ip": "198.51.100.1"}]}),
                ),
            },
        ]
    }

    #[test]
    fn status_merge_registry_has_profiles_for_current_special_cases() {
        assert_eq!(
            StatusMergeRegistry::default().profile("v1", "Pod").kind,
            StatusMergeProfileKind::PodTyped
        );
        assert_eq!(
            StatusMergeRegistry::default().profile("v1", "Node").kind,
            StatusMergeProfileKind::NodeTyped
        );

        let job = StatusMergeRegistry::default().profile("batch/v1", "Job");
        let StatusMergeProfileKind::Generic(policy) = job.kind else {
            panic!("Job must use a Generic policy");
        };
        assert_eq!(policy.terminal_condition_types.len(), 2);
        assert!(policy.terminal_condition_types.contains(&"Complete"));
        assert!(policy.terminal_condition_types.contains(&"Failed"));
        assert_eq!(
            policy.stale_mode,
            GenericStaleStatusMode::Merge {
                condition_merge: ConditionMergeMode::MergeByNewestTransitionTime,
                field_merge: FieldMergeMode::PreserveUnmentioned,
            }
        );

        let cronjob = StatusMergeRegistry::default().profile("batch/v1", "CronJob");
        let StatusMergeProfileKind::Generic(policy) = cronjob.kind else {
            panic!("CronJob must use a Generic policy");
        };
        assert!(policy.terminal_condition_types.is_empty());
        assert_eq!(
            policy.stale_mode,
            GenericStaleStatusMode::Merge {
                condition_merge: ConditionMergeMode::PreserveUnmentionedByType,
                field_merge: FieldMergeMode::PreserveUnmentioned,
            }
        );

        let service = StatusMergeRegistry::default().profile("v1", "Service");
        let StatusMergeProfileKind::Generic(policy) = service.kind else {
            panic!("Service must use a Generic policy");
        };
        assert!(policy.terminal_condition_types.is_empty());
        assert_eq!(
            policy.stale_mode,
            GenericStaleStatusMode::Merge {
                condition_merge: ConditionMergeMode::PreserveUnmentionedByType,
                field_merge: FieldMergeMode::PreserveUnmentioned,
            }
        );

        for (api_version, kind) in [("policy/v1", "PodDisruptionBudget")] {
            let pv = StatusMergeRegistry::default().profile(api_version, kind);
            let StatusMergeProfileKind::Generic(policy) = pv.kind else {
                panic!("{kind} must use a Generic policy");
            };
            assert!(policy.terminal_condition_types.is_empty());
            assert_eq!(
                policy.stale_mode,
                GenericStaleStatusMode::Merge {
                    condition_merge: ConditionMergeMode::PreserveUnmentionedByType,
                    field_merge: FieldMergeMode::PreserveUnmentioned,
                }
            );
        }

        for (api_version, kind) in [("v1", "PersistentVolume"), ("v1", "PersistentVolumeClaim")] {
            let pv = StatusMergeRegistry::default().profile(api_version, kind);
            let StatusMergeProfileKind::Generic(policy) = pv.kind else {
                panic!("{kind} must use a Generic policy");
            };
            assert!(policy.terminal_condition_types.is_empty());
            assert_eq!(
                policy.stale_mode,
                GenericStaleStatusMode::Merge {
                    condition_merge: ConditionMergeMode::PreserveLiveByType,
                    field_merge: FieldMergeMode::PreserveLive,
                }
            );
        }

        let default_kind = StatusMergeRegistry::default().profile("apps/v1", "Deployment");
        let StatusMergeProfileKind::Generic(policy) = default_kind.kind else {
            panic!("unknown kind must use a Generic policy");
        };
        assert_eq!(
            policy.stale_mode,
            GenericStaleStatusMode::Merge {
                condition_merge: ConditionMergeMode::PreserveUnmentionedByType,
                field_merge: FieldMergeMode::PreserveUnmentioned,
            }
        );
    }

    #[test]
    fn stale_unknown_status_preserves_live_status_authoritatively() {
        let live = json!({"status": {"observedGeneration": 9}});
        let mut incoming = json!({"observedGeneration": 1});
        merge_status_for_apply(
            "custom.example/v1",
            "CustomUnknown",
            &live,
            &mut incoming,
            StatusApplyFreshness::Stale,
            StatusApplyOrigin::ReplicatedApply,
        );
        assert_eq!(incoming, json!({"observedGeneration": 9}));
    }

    #[test]
    fn api_subresource_origin_allows_pod_status_client_conditions() {
        let live = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "status": {
                "conditions": [
                    {"type": "Ready", "status": "False"}
                ]
            }
        });
        let mut incoming = json!({
            "conditions": [
                {"type": "Ready", "status": "True"}
            ]
        });

        merge_status_for_apply(
            "v1",
            "Pod",
            &live,
            &mut incoming,
            StatusApplyFreshness::Fresh,
            StatusApplyOrigin::ApiSubresource,
        );

        assert_eq!(
            incoming.pointer("/conditions/0/status"),
            Some(&json!("True")),
            "API /status clients must remain authoritative for non-scheduler Pod conditions"
        );
    }

    fn service_condition_status<'a>(
        incoming_status: &'a Value,
        condition_type: &str,
    ) -> Option<&'a str> {
        incoming_status
            .get("conditions")
            .and_then(Value::as_array)
            .and_then(|conditions| {
                conditions
                    .iter()
                    .find_map(|condition| {
                        (condition.get("type").and_then(Value::as_str) == Some(condition_type))
                            .then(|| condition.get("status").and_then(Value::as_str))
                    })
                    .flatten()
            })
    }

    #[test]
    fn service_status_merge_matrix_covers_api_and_replicated_paths() {
        let live = json!({
            "status": {
                "loadBalancer": {"ingress": [{"ip": "198.51.100.1"}]},
                "conditions": [
                    {"type": "Ready", "status": "False"},
                    {"type": "ExternalTrafficPolicy", "status": "True"}
                ],
                "metadataField": "from-live",
            }
        });

        let cases = [
            ServiceStatusMergePathCase {
                label: "api subresource condition update keeps controller-owned fields",
                freshness: StatusApplyFreshness::Fresh,
                origin: StatusApplyOrigin::ApiSubresource,
                incoming_status: json!({
                    "conditions": [
                        {"type": "Ready", "status": "True"}
                    ]
                }),
                expected_lb_ip: "198.51.100.1",
                expected_conditions: &[("Ready", "True"), ("ExternalTrafficPolicy", "True")],
            },
            ServiceStatusMergePathCase {
                label: "replicated status write updates LB while preserving external conditions",
                freshness: StatusApplyFreshness::Fresh,
                origin: StatusApplyOrigin::ReplicatedApply,
                incoming_status: json!({
                    "loadBalancer": {"ingress": [{"ip": "198.51.100.9"}]},
                    "conditions": []
                }),
                expected_lb_ip: "198.51.100.9",
                expected_conditions: &[("Ready", "False"), ("ExternalTrafficPolicy", "True")],
            },
            ServiceStatusMergePathCase {
                label: "stale replicated status write preserves external conditions",
                freshness: StatusApplyFreshness::Stale,
                origin: StatusApplyOrigin::ReplicatedApply,
                incoming_status: json!({
                    "conditions": [
                        {"type": "ExternalTrafficPolicy", "status": "False"}
                    ]
                }),
                expected_lb_ip: "198.51.100.1",
                expected_conditions: &[("Ready", "False"), ("ExternalTrafficPolicy", "False")],
            },
        ];

        for case in cases {
            let mut incoming = case.incoming_status;
            merge_status_for_apply(
                "v1",
                "Service",
                &live,
                &mut incoming,
                case.freshness,
                case.origin,
            );

            assert_eq!(
                incoming["loadBalancer"]["ingress"][0]["ip"],
                json!(case.expected_lb_ip),
                "{}: loadBalancer should be preserved unless explicitly replaced",
                case.label
            );
            assert_eq!(
                incoming["metadataField"],
                json!("from-live"),
                "{}: metadataField should be preserved",
                case.label
            );

            for (condition_type, expected_status) in case.expected_conditions {
                assert_eq!(
                    service_condition_status(&incoming, condition_type),
                    Some(*expected_status),
                    "{}: missing preserved condition {}",
                    case.label,
                    condition_type
                );
            }
        }
    }

    #[test]
    fn status_merge_matrix_protects_every_generic_registry_kind() {
        for case in generic_status_policy_cases() {
            if let Some(term) = case.terminal_type {
                let mut live_status = serde_json::Map::new();
                live_status.insert(
                    "conditions".to_string(),
                    json!([
                        {"type": term, "status": "True", "lastTransitionTime": "2026-07-01T00:00:00Z"}
                    ]),
                );
                live_status.insert(
                    case.live_preserved_field.0.to_string(),
                    case.live_preserved_field.1.clone(),
                );
                let live = json!({"status": live_status});
                let mut incoming = json!({
                    "conditions": [
                        {"type": term, "status": "False", "lastTransitionTime": "2026-06-30T00:00:00Z"}
                    ]
                });
                merge_status_for_apply(
                    case.api_version,
                    case.kind,
                    &live,
                    &mut incoming,
                    StatusApplyFreshness::Stale,
                    StatusApplyOrigin::ReplicatedApply,
                );
                assert_eq!(
                    incoming.pointer("/conditions/0/status"),
                    Some(&json!("True")),
                    "{} {} stale apply dropped live terminal condition",
                    case.api_version,
                    case.kind
                );

                let live = json!({
                    "status": {
                        "conditions": [
                            {"type": term, "status": "False", "lastTransitionTime": "2026-07-01T00:00:00Z"}
                        ]
                    }
                });
                let mut incoming = json!({
                    "conditions": [
                        {"type": term, "status": "True", "lastTransitionTime": "2026-07-02T00:00:00Z"}
                    ]
                });
                merge_status_for_apply(
                    case.api_version,
                    case.kind,
                    &live,
                    &mut incoming,
                    StatusApplyFreshness::Stale,
                    StatusApplyOrigin::ReplicatedApply,
                );
                assert_eq!(
                    incoming.pointer("/conditions/0/status"),
                    Some(&json!("True")),
                    "{} {} stale apply must keep newer incoming condition",
                    case.api_version,
                    case.kind
                );
            }

            let mut live_status = serde_json::Map::new();
            live_status.insert(
                case.live_preserved_field.0.to_string(),
                case.live_preserved_field.1.clone(),
            );
            live_status.insert("conditions".to_string(), json!([]));
            let live = json!({"status": live_status});
            let mut incoming = json!({"conditions": []});
            merge_status_for_apply(
                case.api_version,
                case.kind,
                &live,
                &mut incoming,
                StatusApplyFreshness::Stale,
                StatusApplyOrigin::ReplicatedApply,
            );
            assert_eq!(
                incoming.get(case.live_preserved_field.0),
                Some(&case.live_preserved_field.1),
                "{} {} stale apply dropped live field",
                case.api_version,
                case.kind
            );

            if case.terminal_type.is_none() {
                let live = json!({
                    "status": {
                        "conditions": [
                            {"type": "Bound", "status": "True"}
                        ]
                    }
                });
                let mut incoming = json!({
                    "conditions": [
                        {"type": "Resizing", "status": "False"}
                    ]
                });
                merge_status_for_apply(
                    case.api_version,
                    case.kind,
                    &live,
                    &mut incoming,
                    StatusApplyFreshness::Stale,
                    StatusApplyOrigin::ReplicatedApply,
                );
                let condition_types: std::collections::HashSet<_> = incoming
                    .get("conditions")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|condition| {
                        condition.get("type").and_then(serde_json::Value::as_str)
                    })
                    .collect();
                assert!(
                    condition_types.contains("Bound") && condition_types.contains("Resizing"),
                    "{} {} stale apply must preserve unmentioned live conditions by type: {incoming:?}",
                    case.api_version,
                    case.kind
                );
            }

            let mut fresh = json!({"replaced": true});
            merge_status_for_apply(
                case.api_version,
                case.kind,
                &live,
                &mut fresh,
                StatusApplyFreshness::Fresh,
                StatusApplyOrigin::ApiSubresource,
            );
            if case.api_version == "v1" && case.kind == "Service" {
                assert_eq!(
                    fresh.get("replaced"),
                    Some(&json!(true)),
                    "{} {} fresh API-origin status should preserve unrelated live fields",
                    case.api_version,
                    case.kind
                );
                assert_eq!(
                    fresh.get(case.live_preserved_field.0),
                    Some(&case.live_preserved_field.1),
                    "{} {} fresh API-origin Service status must preserve live fields",
                    case.api_version,
                    case.kind
                );
            } else {
                assert_eq!(fresh, json!({"replaced": true}));
            }
        }

        let live = json!({"status": {"observedGeneration": 9}});
        let mut incoming = json!({"observedGeneration": 1});
        // Use an unknown type that still gets UseLiveStatus (the default).
        merge_status_for_apply(
            "custom.example/v1",
            "CustomUnknown",
            &live,
            &mut incoming,
            StatusApplyFreshness::Stale,
            StatusApplyOrigin::ReplicatedApply,
        );
        assert_eq!(incoming, json!({"observedGeneration": 9}));
    }

    /// ReplicaSet stale status merge must preserve incoming conditions
    /// while back-filling controller-owned conditions from the live status.
    #[test]
    fn stale_replicaset_status_merge_preserves_incoming_conditions() {
        // Live status has controller-owned conditions and fields.
        let live = json!({
            "status": {
                "replicas": 1,
                "readyReplicas": 1,
                "conditions": [
                    {"type": "Available", "status": "True", "lastTransitionTime": "2026-07-05T00:00:00Z"}
                ]
            }
        });

        // Incoming status from API /status PUT has a custom condition.
        let mut incoming = json!({
            "conditions": [
                {"type": "NotExistingCondition", "status": "True", "lastTransitionTime": "2026-07-06T07:09:22Z", "reason": "TestReason"}
            ]
        });

        merge_status_for_apply(
            "apps/v1",
            "ReplicaSet",
            &live,
            &mut incoming,
            StatusApplyFreshness::Stale,
            StatusApplyOrigin::ReplicatedApply,
        );

        // The stale merge must:
        // 1. Preserve the incoming custom condition
        assert_eq!(incoming["conditions"][0]["type"], "NotExistingCondition");
        // 2. Back-fill the live controller condition (by type)
        let types: Vec<&str> = incoming["conditions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["type"].as_str().unwrap())
            .collect();
        assert!(
            types.contains(&"Available"),
            "stale merge must back-fill live conditions by type"
        );
        // 3. Preserve live fields not mentioned in incoming
        assert_eq!(incoming["replicas"], 1);
        assert_eq!(incoming["readyReplicas"], 1);
    }
}
