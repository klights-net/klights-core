//! Shared helper functions for generated API handlers.
//! Extracted from generated_handlers.rs (refactor).

use crate::api::*;
use crate::auth::identity::AuthenticatedIdentity;

/// Extract the API group from a compound apiVersion string.
///
/// `"v1"` → `""`, `"apps/v1"` → `"apps"`,
/// `"certificates.k8s.io/v1"` → `"certificates.k8s.io"`.
pub fn api_group_from_version(api_version: &str) -> &str {
    match api_version.rsplit_once('/') {
        Some((group, _)) => group,
        None => "",
    }
}

// Per-handler authorization helpers have been removed: authorization is now
// enforced once, for every route, by the global authorize_request middleware
// chokepoint (see src/auth/middleware.rs and src/auth/request_info.rs).

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
        }
    }
}

/// Reject namespaced creates whose target namespace does not exist or is being
/// terminated. Mirrors the upstream `NamespaceLifecycle` admission plugin:
/// objects may not be created in a missing namespace (403 Forbidden) nor in a
/// namespace carrying a `deletionTimestamp`.
///
/// Shared by every namespaced create handler (generic + Pod + event paths).
pub async fn reject_if_namespace_missing_or_terminating(
    db: &dyn DatastoreBackend,
    namespace: &str,
) -> Result<(), AppError> {
    match db.get_namespace(namespace).await? {
        None => {
            // The immortal system namespaces (default, kube-system, kube-public,
            // kube-node-lease) always exist in a running cluster: they are
            // created at bootstrap and cannot be deleted (T3.9). Treat them as
            // present even if a row lookup races, so we never spuriously reject
            // creates into a guaranteed-present namespace. For any other
            // namespace, a missing row means the namespace does not exist.
            if crate::api::is_protected_namespace(namespace) {
                Ok(())
            } else {
                Err(AppError::Forbidden(format!(
                    "namespace {} not found",
                    namespace
                )))
            }
        }
        Some(ns) => {
            let terminating = ns
                .data
                .pointer("/metadata/deletionTimestamp")
                .and_then(|v| v.as_str())
                .is_some();
            if terminating {
                return Err(AppError::Forbidden(format!(
                    "namespace {} is being terminated",
                    namespace
                )));
            }
            Ok(())
        }
    }
}

/// Pod update/patch callers used to hard-delete a Pod row when finalizers were
/// drained. Pod lifecycle is actor-owned now: once a Pod has a
/// deletionTimestamp, the UID-bound workqueue/actor path owns the final row
/// removal after local runtime and cache cleanup are confirmed.
///
/// Only meaningful for Pods; callers gate on $kind == "Pod" so the resource
/// kind is also checked here defensively.
pub async fn maybe_hard_delete_pod_after_finalizers_drained(
    db: &dyn DatastoreBackend,
    api_version: &str,
    kind: &str,
    namespace: &str,
    name: &str,
    data: &Value,
) {
    if kind != "Pod" {
        return;
    }
    let has_deletion_timestamp = data
        .pointer("/metadata/deletionTimestamp")
        .and_then(|v| v.as_str())
        .is_some();
    if !has_deletion_timestamp {
        return;
    }
    if resource_has_finalizers(data, "/metadata/finalizers") {
        return;
    }
    let _ = (db, api_version, namespace, name);
    tracing::debug!(
        namespace = %namespace,
        name = %name,
        "pod finalizers drained; actor-owned UID cleanup will remove the row"
    );
}

pub async fn maybe_reconcile_service_after_controller_endpointslice_delete(
    state: &std::sync::Arc<AppState>,
    namespace: &str,
    deleted: &Value,
) -> Result<(), AppError> {
    let managed_by = deleted
        .pointer("/metadata/labels/endpointslice.kubernetes.io~1managed-by")
        .and_then(|v| v.as_str());
    if managed_by != Some("endpointslice-controller.k8s.io") {
        return Ok(());
    }

    let Some(service_name) = deleted
        .pointer("/metadata/labels/kubernetes.io~1service-name")
        .and_then(|v| v.as_str())
        .filter(|name| !name.is_empty())
    else {
        return Ok(());
    };

    let Some(service) = state
        .db
        .get_resource("v1", "Service", Some(namespace), service_name)
        .await?
    else {
        return Ok(());
    };

    let spec = service.data.get("spec");
    let service_type = spec
        .and_then(|s| s.get("type"))
        .and_then(|t| t.as_str())
        .unwrap_or("ClusterIP");
    if service_type == "ExternalName" {
        return Ok(());
    }

    let service_uid = service
        .data
        .pointer("/metadata/uid")
        .and_then(|uid| uid.as_str())
        .unwrap_or("");

    crate::controllers::endpoints::reconcile_endpointslice(
        state.db.as_ref(),
        state.pod_repository.as_ref(),
        service_name,
        service_uid,
        namespace,
        spec.and_then(|s| s.get("selector")),
        spec.and_then(|s| s.get("ports")),
    )
    .await?;

    Ok(())
}

pub fn initialize_statefulset_revision_status_on_create(name: &str, body: &mut Value) {
    let Some(template) = body.pointer("/spec/template") else {
        return;
    };
    let revision =
        crate::controllers::statefulset::compute_statefulset_update_revision(name, template);

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

pub async fn reconcile_owner_refs_after_mutation(
    state: &std::sync::Arc<AppState>,
    resource: &crate::datastore::Resource,
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

    if let Err(e) = controllers::gc::reconcile_owner_references(
        state.db.as_ref(),
        resource.clone(),
        state.pod_repository.as_ref() as &dyn crate::controllers::gc::GcPodDeleteSink,
    )
    .await
    {
        state
            .metrics
            .cascade_delete_failures_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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

// `cluster_delete_collection_handler!` is defined once in `src/api/macros.rs`
// and re-imported via `#[macro_use] mod macros;` in api/mod.rs. Do not
// re-define it here — the mod_tests coverage enforces a single
// definition.

// ============================================================================
// Shared inner handlers used by both `namespaced_resource_handlers!` and
// `cluster_resource_handlers!` macros below. Each takes `ns: Option<&str>`:
// `Some(_)` for namespaced URLs, `None` for cluster-scoped URLs. The two
// wrapper macros are now thin axum-extractor adapters that delegate here.
// Kind-specific branches (Pod, ConfigMap, Secret, Deployment, etc.) are
// runtime `if kind == "..."` checks; for non-matching kinds they're no-ops.
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn bootstrap_identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::bootstrap("abcdef", &[])
    }

    fn sa_identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::service_account(
            "system:serviceaccount:default:my-sa".to_string(),
            vec![
                "system:serviceaccounts".to_string(),
                "system:serviceaccounts:default".to_string(),
            ],
            Some("uid-123".to_string()),
        )
    }

    #[test]
    fn stamp_csr_identity_overwrites_client_supplied_fields() {
        let mut body = serde_json::json!({
            "apiVersion": "certificates.k8s.io/v1",
            "kind": "CertificateSigningRequest",
            "metadata": { "name": "my-csr" },
            "spec": {
                "request": "dGhpcyBpcyBmYWtl",
                "signerName": "kubernetes.io/kube-apiserver-client-kubelet",
                "username": "forged-user",
                "groups": ["forged-group"],
                "uid": "forged-uid"
            }
        });

        let identity = bootstrap_identity();
        stamp_csr_identity(&mut body, &identity);

        let spec = body.get("spec").unwrap();
        assert_eq!(spec["username"], "system:bootstrap:abcdef");
        assert!(
            spec["groups"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("system:bootstrappers"))
        );
        assert!(
            !spec["groups"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("forged-group"))
        );
        assert_ne!(spec["uid"], "forged-uid");
    }

    #[test]
    fn stamp_csr_identity_creates_spec_when_absent() {
        let mut body = serde_json::json!({
            "apiVersion": "certificates.k8s.io/v1",
            "kind": "CertificateSigningRequest",
            "metadata": { "name": "my-csr" }
        });

        let identity = bootstrap_identity();
        stamp_csr_identity(&mut body, &identity);

        let spec = body.get("spec").unwrap();
        assert_eq!(spec["username"], "system:bootstrap:abcdef");
    }

    #[test]
    fn stamp_csr_identity_uses_uid_from_identity_when_present() {
        let mut body = serde_json::json!({
            "metadata": { "name": "csr" },
            "spec": { "signerName": "kubernetes.io/kube-apiserver-client-kubelet" }
        });

        let identity = sa_identity();
        stamp_csr_identity(&mut body, &identity);

        let spec = body.get("spec").unwrap();
        assert_eq!(spec["uid"], "uid-123");
    }

    #[test]
    fn stamp_csr_identity_falls_back_to_username_for_uid() {
        let mut body = serde_json::json!({
            "metadata": { "name": "csr" },
            "spec": {}
        });

        let identity = bootstrap_identity();
        stamp_csr_identity(&mut body, &identity);

        let spec = body.get("spec").unwrap();
        assert_eq!(spec["uid"], "system:bootstrap:abcdef");
    }

    #[tokio::test]
    async fn create_in_missing_namespace_is_forbidden() {
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        let err = reject_if_namespace_missing_or_terminating(&db as &dyn DatastoreBackend, "ghost")
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::Forbidden(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn create_in_existing_active_namespace_is_allowed() {
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        db.create_namespace(
            "team-a",
            serde_json::json!({"metadata": {"name": "team-a"}}),
        )
        .await
        .unwrap();
        reject_if_namespace_missing_or_terminating(&db as &dyn DatastoreBackend, "team-a")
            .await
            .expect("active namespace must accept creates");
    }

    #[tokio::test]
    async fn create_in_immortal_system_namespace_is_allowed_even_without_row() {
        // default/kube-system/kube-public/kube-node-lease always exist in a
        // running cluster, so creates target them are never rejected.
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        for ns in ["default", "kube-system", "kube-public", "kube-node-lease"] {
            reject_if_namespace_missing_or_terminating(&db as &dyn DatastoreBackend, ns)
                .await
                .unwrap_or_else(|e| panic!("system namespace {ns} must be allowed: {e:?}"));
        }
    }

    #[tokio::test]
    async fn create_in_terminating_namespace_is_forbidden() {
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        db.create_namespace(
            "team-b",
            serde_json::json!({
                "metadata": {"name": "team-b", "deletionTimestamp": "2026-06-13T00:00:00Z"}
            }),
        )
        .await
        .unwrap();
        let err =
            reject_if_namespace_missing_or_terminating(&db as &dyn DatastoreBackend, "team-b")
                .await
                .unwrap_err();
        assert!(matches!(err, AppError::Forbidden(_)), "got {err:?}");
    }
}
