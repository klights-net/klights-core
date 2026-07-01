use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusApplyFreshness {
    Fresh,
    Stale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusMergeProfileKind {
    PodTyped,
    JobConditionsByTransitionTime,
    PreserveUnmentionedFieldsAndConditions,
    PreserveLiveStatusAuthoritatively,
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
            ("batch/v1", "Job") => {
                StatusMergeProfile::new(StatusMergeProfileKind::JobConditionsByTransitionTime)
            }
            ("v1", "PersistentVolume" | "PersistentVolumeClaim") => StatusMergeProfile::new(
                StatusMergeProfileKind::PreserveUnmentionedFieldsAndConditions,
            ),
            _ => StatusMergeProfile::new(StatusMergeProfileKind::PreserveLiveStatusAuthoritatively),
        }
    }
}

pub fn merge_status_for_apply(
    api_version: &str,
    kind: &str,
    live_resource: &Value,
    incoming_status: &mut Value,
    freshness: StatusApplyFreshness,
) {
    let profile = StatusMergeRegistry::default().profile(api_version, kind);

    if freshness == StatusApplyFreshness::Fresh && profile.kind != StatusMergeProfileKind::PodTyped
    {
        return;
    }

    match profile.kind {
        StatusMergeProfileKind::PodTyped => merge_pod_status(live_resource, incoming_status),
        StatusMergeProfileKind::JobConditionsByTransitionTime => {
            merge_stale_job_status(live_resource, incoming_status)
        }
        StatusMergeProfileKind::PreserveUnmentionedFieldsAndConditions => {
            preserve_unmentioned_live_status_conditions_by_type(live_resource, incoming_status);
            preserve_unmentioned_live_status_fields(live_resource, incoming_status);
        }
        StatusMergeProfileKind::PreserveLiveStatusAuthoritatively => {
            preserve_live_status_authoritatively(live_resource, incoming_status)
        }
    }
}

fn merge_stale_job_status(live_resource: &Value, incoming_status: &mut Value) {
    if live_job_status_is_terminal(live_resource) {
        preserve_live_status_authoritatively(live_resource, incoming_status);
        return;
    }
    preserve_newer_live_job_status_conditions_by_type(live_resource, incoming_status);
    preserve_unmentioned_live_status_fields(live_resource, incoming_status);
}

fn live_job_status_is_terminal(live_resource: &Value) -> bool {
    live_resource
        .pointer("/status/conditions")
        .and_then(Value::as_array)
        .is_some_and(|conditions| {
            conditions.iter().any(|condition| {
                matches!(
                    condition.get("type").and_then(Value::as_str),
                    Some("Complete" | "Failed")
                ) && condition.get("status").and_then(Value::as_str) == Some("True")
            })
        })
}

fn preserve_newer_live_job_status_conditions_by_type(
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
        }) && live_job_condition_is_newer(live_condition, incoming)
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

fn live_job_condition_is_newer(live_condition: &Value, incoming_condition: &Value) -> bool {
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

fn merge_pod_status(live_resource: &Value, incoming_status: &mut Value) {
    crate::pod_status_merge::merge_pod_status_for_update(
        "v1",
        "Pod",
        live_resource,
        incoming_status,
        crate::pod_status_merge::PodStatusOwner::KubeletRuntime,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn status_merge_registry_has_profiles_for_current_special_cases() {
        assert_eq!(
            StatusMergeRegistry::default().profile("v1", "Pod").kind,
            StatusMergeProfileKind::PodTyped
        );
        assert_eq!(
            StatusMergeRegistry::default()
                .profile("batch/v1", "Job")
                .kind,
            StatusMergeProfileKind::JobConditionsByTransitionTime
        );
        assert_eq!(
            StatusMergeRegistry::default()
                .profile("v1", "PersistentVolume")
                .kind,
            StatusMergeProfileKind::PreserveUnmentionedFieldsAndConditions
        );
        assert_eq!(
            StatusMergeRegistry::default()
                .profile("v1", "PersistentVolumeClaim")
                .kind,
            StatusMergeProfileKind::PreserveUnmentionedFieldsAndConditions
        );
    }

    #[test]
    fn stale_unknown_status_preserves_live_status_authoritatively() {
        let live = json!({"status": {"observedGeneration": 9}});
        let mut incoming = json!({"observedGeneration": 1});
        merge_status_for_apply(
            "apps/v1",
            "Deployment",
            &live,
            &mut incoming,
            StatusApplyFreshness::Stale,
        );
        assert_eq!(incoming, json!({"observedGeneration": 9}));
    }
}
