use crate::datastore::{DatastoreBackend, Resource, ResourcePreconditions};
use anyhow::Result;
use serde_json::{Value, json};

use super::helpers::templates_match;

pub fn build_conditions_and_revision(
    available_pods: i64,
    updated_pods: i64,
    desired_replicas: i64,
    created_rs_name: &Option<String>,
    matching_rs: &Option<&Resource>,
    next_revision: i64,
    existing_status: Option<&Value>,
) -> (Vec<Value>, Option<String>) {
    let now = crate::utils::k8s_timestamp();
    let mut conditions = Vec::new();

    let (available_status, available_reason, available_message) = if available_pods > 0 {
        (
            "True",
            "MinimumReplicasAvailable",
            "Deployment has minimum availability.",
        )
    } else {
        (
            "False",
            "MinimumReplicasUnavailable",
            "Deployment does not have minimum availability.",
        )
    };

    let mut available_condition = json!({
        "type": "Available",
        "status": available_status,
        "reason": available_reason,
        "message": available_message
    });
    preserve_condition_timestamps(
        &mut available_condition,
        previous_condition(existing_status, "Available"),
        &now,
    );
    conditions.push(available_condition);

    let (current_revision, rs_was_existing, rs_name_for_msg_owned) = if created_rs_name.is_some() {
        let name = created_rs_name.as_deref().unwrap_or("unknown").to_string();
        (Some(next_revision.to_string()), false, name)
    } else if let Some(rs) = matching_rs {
        let rev = rs
            .data
            .pointer("/metadata/annotations/deployment.kubernetes.io~1revision")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let name = rs
            .data
            .pointer("/metadata/name")
            .and_then(|n| n.as_str())
            .unwrap_or("unknown")
            .to_string();
        (rev, true, name)
    } else {
        (None, false, "unknown".to_string())
    };

    let progressing_reason = if rs_was_existing && updated_pods == desired_replicas {
        "NewReplicaSetAvailable"
    } else {
        "NewReplicaSetCreated"
    };

    let progressing_message = if progressing_reason == "NewReplicaSetAvailable" {
        format!(
            "ReplicaSet \"{}\" has successfully progressed.",
            rs_name_for_msg_owned
        )
    } else {
        format!("Created new replica set \"{}\".", rs_name_for_msg_owned)
    };

    let mut progressing_condition = json!({
        "type": "Progressing",
        "status": "True",
        "reason": progressing_reason,
        "message": progressing_message
    });
    preserve_condition_timestamps(
        &mut progressing_condition,
        previous_condition(existing_status, "Progressing"),
        &now,
    );
    conditions.push(progressing_condition);

    (conditions, current_revision)
}

fn previous_condition<'a>(status: Option<&'a Value>, condition_type: &str) -> Option<&'a Value> {
    status
        .and_then(|status| status.get("conditions"))
        .and_then(|conditions| conditions.as_array())
        .and_then(|conditions| {
            conditions.iter().find(|condition| {
                condition.get("type").and_then(|value| value.as_str()) == Some(condition_type)
            })
        })
}

fn condition_string_field<'a>(condition: &'a Value, field: &str) -> Option<&'a str> {
    condition.get(field).and_then(|value| value.as_str())
}

fn same_condition_state(next: &Value, previous: &Value) -> bool {
    ["status", "reason", "message"]
        .iter()
        .all(|field| condition_string_field(next, field) == condition_string_field(previous, field))
}

fn preserve_condition_timestamps(condition: &mut Value, previous: Option<&Value>, now: &str) {
    let previous_transition = previous
        .and_then(|old| old.get("lastTransitionTime"))
        .cloned();
    let previous_update = previous.and_then(|old| old.get("lastUpdateTime")).cloned();
    let previous_status = previous.and_then(|old| condition_string_field(old, "status"));
    let next_status = condition_string_field(condition, "status");
    let same_status = previous_status.is_some() && previous_status == next_status;
    let same_state = previous.is_some_and(|old| same_condition_state(condition, old));

    let Some(condition_obj) = condition.as_object_mut() else {
        return;
    };
    let transition_time = if same_status {
        previous_transition.unwrap_or_else(|| json!(now))
    } else {
        json!(now)
    };
    let update_time = if same_state {
        previous_update.unwrap_or_else(|| json!(now))
    } else {
        json!(now)
    };
    condition_obj.insert("lastTransitionTime".to_string(), transition_time);
    condition_obj.insert("lastUpdateTime".to_string(), update_time);
}

pub(super) async fn apply_revision_and_gc(
    db: &dyn DatastoreBackend,
    namespace: &str,
    deployment_name: &str,
    spec: &Value,
    owned_rs_list: &[Resource],
    template: &Value,
    current_revision: Option<String>,
) -> Result<()> {
    if let Some(rev) = current_revision {
        let Some(deployment) = db
            .get_resource("apps/v1", "Deployment", Some(namespace), deployment_name)
            .await?
        else {
            return Ok(());
        };
        let annotation_patch = json!({
            "metadata": {
                "annotations": {
                    "deployment.kubernetes.io/revision": rev
                }
            }
        });
        db.patch_resource_latest_with_preconditions(
            "apps/v1",
            "Deployment",
            Some(namespace),
            deployment_name,
            crate::datastore::ResourcePatchRequest::new(
                crate::datastore::PatchKind::Merge,
                annotation_patch,
                ResourcePreconditions::uid(deployment.uid),
            ),
        )
        .await?;
    }

    let revision_history_limit = spec
        .get("revisionHistoryLimit")
        .and_then(|r| r.as_i64())
        .unwrap_or(10);

    let mut old_zero_replicas_rs: Vec<_> = owned_rs_list
        .iter()
        .filter(|rs| {
            let rs_replicas = rs
                .data
                .get("spec")
                .and_then(|s| s.get("replicas"))
                .and_then(|r| r.as_i64())
                .unwrap_or(0);

            let rs_template = rs.data.get("spec").and_then(|s| s.get("template"));
            let is_current = rs_template
                .map(|t| templates_match(t, template))
                .unwrap_or(false);
            rs_replicas == 0 && !is_current
        })
        .collect();

    old_zero_replicas_rs.sort_by(|a, b| {
        let a_time = a
            .data
            .get("metadata")
            .and_then(|m| m.get("creationTimestamp"))
            .and_then(|t| t.as_str())
            .unwrap_or("");
        let b_time = b
            .data
            .get("metadata")
            .and_then(|m| m.get("creationTimestamp"))
            .and_then(|t| t.as_str())
            .unwrap_or("");
        a_time.cmp(b_time)
    });

    if old_zero_replicas_rs.len() as i64 > revision_history_limit {
        let to_delete_count = old_zero_replicas_rs.len() as i64 - revision_history_limit;
        for rs in old_zero_replicas_rs.iter().take(to_delete_count as usize) {
            let rs_name = rs
                .data
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
                .unwrap_or("");
            tracing::debug!(
                "Garbage collecting old ReplicaSet {}/{} (exceeds revisionHistoryLimit={})",
                namespace,
                rs_name,
                revision_history_limit
            );
            db.delete_resource_with_preconditions(
                "apps/v1",
                "ReplicaSet",
                Some(namespace),
                rs_name,
                ResourcePreconditions::uid(rs.uid.clone()),
            )
            .await?;
        }
    }

    Ok(())
}
