//! Shared helper functions for generated native API handlers.

use crate::current::ApiState;
use klights_auth::AuthenticatedIdentity;
use serde_json::Value;

// Per-handler authorization helpers have been removed: authorization is now
// enforced once, for every route, by the global authorize_request middleware
// chokepoint in `crate::auth_http`, using `crate::request_info`.

/// Stamp CSR spec identity fields from the authenticated identity.
///
/// Per Kubernetes semantics, clients must not be able to forge these fields.
/// The server overwrites spec.username, spec.groups, spec.uid, and spec.extra
/// from the authenticated identity regardless of what the client sent.
pub fn stamp_csr_identity(body: &mut Value, identity: &AuthenticatedIdentity) {
    let spec = match body.pointer_mut("/spec") {
        Some(s) => s,
        None => {
            body.as_object_mut()
                .map(|obj| obj.insert("spec".to_string(), serde_json::json!({})));
            body.pointer_mut("/spec").unwrap()
        }
    };

    if let Some(spec_obj) = spec.as_object_mut() {
        spec_obj.insert("username".to_string(), serde_json::json!(identity.username));

        if let Some(ref uid) = identity.uid {
            spec_obj.insert("uid".to_string(), serde_json::json!(uid));
        } else {
            spec_obj.insert("uid".to_string(), serde_json::json!(identity.username));
        }

        spec_obj.insert("groups".to_string(), serde_json::json!(identity.groups));

        if !identity.extra.is_empty() {
            let extra: std::collections::BTreeMap<&str, Vec<&str>> =
                identity
                    .extra
                    .iter()
                    .fold(std::collections::BTreeMap::new(), |mut acc, (k, v)| {
                        acc.entry(k).or_default().push(v);
                        acc
                    });
            spec_obj.insert(
                "extra".to_string(),
                serde_json::json!(
                    extra
                        .into_iter()
                        .collect::<std::collections::BTreeMap<_, _>>()
                ),
            );
        } else {
            spec_obj.remove("extra");
        }
    }
}

#[cfg(test)]
#[path = "helpers_tests.rs"]
mod tests;

pub fn initialize_statefulset_revision_status_on_create(name: &str, body: &mut Value) {
    let Some(template) = body.pointer("/spec/template") else {
        return;
    };
    let revision = klights_reconcile_api::compute_statefulset_update_revision(name, template);

    let Some(obj) = body.as_object_mut() else {
        return;
    };
    let status = obj.entry("status").or_insert_with(|| serde_json::json!({}));
    let Some(status_obj) = status.as_object_mut() else {
        return;
    };

    let current_missing = status_obj
        .get("currentRevision")
        .is_none_or(|v| v.is_null() || v.as_str().is_some_and(str::is_empty));
    if current_missing {
        status_obj.insert(
            "currentRevision".to_string(),
            serde_json::Value::String(revision.clone()),
        );
    }

    let update_missing = status_obj
        .get("updateRevision")
        .is_none_or(|v| v.is_null() || v.as_str().is_some_and(str::is_empty));
    if update_missing {
        status_obj.insert(
            "updateRevision".to_string(),
            serde_json::Value::String(revision),
        );
    }

    for key in [
        "replicas",
        "readyReplicas",
        "currentReplicas",
        "updatedReplicas",
        "availableReplicas",
    ] {
        if status_obj.get(key).is_none_or(|v| v.is_null()) {
            status_obj.insert(key.to_string(), serde_json::json!(0));
        }
    }
}

pub(in crate::current) async fn reconcile_owner_refs_after_mutation(
    state: &std::sync::Arc<ApiState>,
    resource: &klights_cluster_core::Resource,
    context: &'static str,
) {
    if resource
        .data
        .pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
        .is_none_or(|refs| refs.is_empty())
    {
        return;
    }

    if let Err(e) = crate::current::gc_ports::reconcile_owner_references(
        state.resource_mutation().gc_owner_lifecycle.as_ref(),
        resource.clone(),
    )
    .await
    {
        state
            .controller_reconcile()
            .metrics
            .record_cascade_delete_failure();
        tracing::error!(
            context,
            api_version = %resource.api_version,
            kind = %resource.kind,
            namespace = ?resource.namespace,
            name = %resource.name,
            error = %e,
            "ownerReference GC reconciliation failed"
        );
    }
}

// `cluster_delete_collection_handler!` is defined once in `crate::current::macros`
// and re-imported via `#[macro_use] mod macros;` in api/mod.rs. Do not
// re-define it here — focused native coverage enforces a single
// definition.

// ============================================================================
// Shared inner handlers used by both `namespaced_resource_handlers!` and
// `cluster_resource_handlers!` macros below. Each takes `ns: Option<&str>`:
// `Some(_)` for namespaced URLs, `None` for cluster-scoped URLs. The two
// wrapper macros are now thin axum-extractor adapters that delegate here.
// Kind-specific branches (Pod, ConfigMap, Secret, Deployment, etc.) are
// runtime `if kind == "..."` checks; for non-matching kinds they're no-ops.
// ============================================================================
