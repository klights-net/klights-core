//! Composition-owned passive resource query.
//!
//! The selected cluster store is adapted to the transport-neutral query port
//! at the bootstrap boundary.  Local leader effects may consume that port,
//! but the local client does not own the store adapter or its read surface.

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use base64::Engine as _;
use klights_cluster_core::WatchReplayPosition;
use klights_leader_api::{
    LeaderResourceQuery, ResourceGetRequest, ResourceListContinuationMode, ResourceListRequest,
    ResourceListResult, ResourceListScope, ResourceQueryConsistency, ResourceQueryError,
    ResourceQueryFuture,
};
use sha2::{Digest as _, Sha256};

use klights_cluster_store::{
    ClusterResourceRead, ResourceCollectionKey, ResourceCollectionScope, ResourceContinuation,
    ResourceListQuery, ResourceListRead, ResourceListRecoveryContinuation,
    ResourceListRequest as StoreResourceListRequest, ResourceListSnapshot, ResourceReadError,
    ResourceVersionMatch,
};

use crate::bootstrap::authority::AuthorityHandle;
use crate::datastore::{DatastoreHandle, Resource};

const PRIVATE_CONTINUATION_CODEC_VERSION: u8 = 2;
/// Kubernetes clients must be able to restart a chunked LIST after bounded
/// server-side retention. Keep the private pinned snapshot lifetime aligned
/// with the upstream compaction cadence rather than an unbounded row count.
pub(crate) const PRIVATE_PINNED_CONTINUATION_TTL: Duration = Duration::from_secs(5 * 60);

/// Root-owned, versioned LIST cursor payload. It is intentionally private:
/// native HTTP and leader RPC only transport its ASCII representation, while
/// the store only receives the decoded typed continuation.
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct PrivateListContinuation {
    v: u8,
    mode: PrivateContinuationMode,
    api_version: String,
    kind: String,
    scope: PrivateCollectionScope,
    label_selector: Option<String>,
    field_selector: Option<String>,
    after_namespace: Option<String>,
    after_name: String,
    position: Option<PrivateReplayPosition>,
    /// Root-owned issuance instant for the pinned snapshot. Older private
    /// cursors without this field are safely recovered instead of remaining
    /// valid forever after an upgrade.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    issued_at_unix_ms: Option<i64>,
    #[serde(default)]
    crd_plan: Option<PrivateCrdPlanRef>,
}

/// Immutable CRD collection plan captured with page one. It is private root
/// state: HTTP and RPC transport the enclosing cursor without interpreting it.
#[derive(Clone, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct PrivateCrdPlanRef {
    crd_name: String,
    crd_uid: String,
    crd_resource_version: i64,
    definition_digest: String,
}

/// Fully resolved root-only CRD plan. The cursor retains only a compact
/// reference; the canonical definition itself is reloaded at the pinned
/// replay position before a page-two collection read.
struct ResolvedCrdReadPlan {
    definition: Resource,
    snapshot: ResourceListSnapshot,
    api_versions: Vec<String>,
}

#[derive(Clone, Copy, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum PrivateContinuationMode {
    Pinned,
    Recovery,
}

#[derive(PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", content = "namespace", rename_all = "snake_case")]
enum PrivateCollectionScope {
    Cluster,
    AllNamespaces,
    Namespace(String),
}

#[derive(serde::Deserialize, serde::Serialize)]
struct PrivateReplayPosition {
    resource_version: i64,
    event_id: i64,
    resource_version_filter_through_event_id: i64,
}

enum DecodedListContinuation {
    Pinned(ResourceContinuation),
    Recovery(ResourceListRecoveryContinuation),
}

struct PrivateContinuationOptions<'a> {
    issued_at_unix_ms: Option<i64>,
    crd_plan: Option<&'a PrivateCrdPlanRef>,
}

fn private_pinned_cursor_expired_at(
    issued_at_unix_ms: Option<i64>,
    now_unix_ms: i64,
) -> Result<bool, ResourceQueryError> {
    let Some(issued_at_unix_ms) = issued_at_unix_ms else {
        return Ok(true);
    };
    if now_unix_ms < issued_at_unix_ms {
        return Err(ResourceQueryError::InvalidRequest {
            field: "list.continue_token",
            message: "pinned continuation issuance timestamp is in the future".to_string(),
        });
    }
    Ok(now_unix_ms - issued_at_unix_ms >= PRIVATE_PINNED_CONTINUATION_TTL.as_millis() as i64)
}

fn encode_private_continuation(
    api_version: &str,
    kind: &str,
    scope: &ResourceCollectionScope,
    label_selector: Option<&str>,
    field_selector: Option<&str>,
    continuation: DecodedListContinuation,
    options: PrivateContinuationOptions<'_>,
) -> Result<String, ResourceQueryError> {
    let (mode, after, position) = match continuation {
        DecodedListContinuation::Pinned(cursor) => (
            PrivateContinuationMode::Pinned,
            cursor.after().clone(),
            Some(cursor.snapshot().position()),
        ),
        DecodedListContinuation::Recovery(cursor) => (
            PrivateContinuationMode::Recovery,
            cursor.after().clone(),
            None,
        ),
    };
    let scope = match scope {
        ResourceCollectionScope::Cluster => PrivateCollectionScope::Cluster,
        ResourceCollectionScope::AllNamespaces => PrivateCollectionScope::AllNamespaces,
        ResourceCollectionScope::Namespace(namespace) => {
            PrivateCollectionScope::Namespace(namespace.clone())
        }
    };
    let after_scope_valid = match &scope {
        PrivateCollectionScope::Cluster => after.namespace().is_none(),
        PrivateCollectionScope::AllNamespaces => after
            .namespace()
            .is_some_and(|namespace| !namespace.is_empty()),
        PrivateCollectionScope::Namespace(namespace) => after.namespace() == Some(namespace),
    };
    if after.name().is_empty() || !after_scope_valid {
        return Err(ResourceQueryError::InvalidRequest {
            field: "list.continue_token",
            message: "private continuation key does not match LIST scope".to_string(),
        });
    }
    let encoded = serde_json::to_vec(&PrivateListContinuation {
        v: PRIVATE_CONTINUATION_CODEC_VERSION,
        mode,
        api_version: api_version.to_string(),
        kind: kind.to_string(),
        scope,
        label_selector: label_selector.map(str::to_owned),
        field_selector: field_selector.map(str::to_owned),
        after_namespace: after.namespace().map(str::to_owned),
        after_name: after.name().to_string(),
        position: position.map(|position| PrivateReplayPosition {
            resource_version: position.resource_version,
            event_id: position.event_id,
            resource_version_filter_through_event_id: position
                .resource_version_filter_through_event_id,
        }),
        issued_at_unix_ms: options.issued_at_unix_ms,
        crd_plan: options.crd_plan.cloned(),
    })
    .map_err(|error| ResourceQueryError::corrupt_response(error.to_string()))?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(encoded))
}

fn decode_private_continuation(
    raw: &str,
    api_version: &str,
    kind: &str,
    scope: &ResourceCollectionScope,
    label_selector: Option<&str>,
    field_selector: Option<&str>,
    requested_mode: ResourceListContinuationMode,
) -> Result<DecodedListContinuation, ResourceQueryError> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(raw)
        .map_err(|_| ResourceQueryError::InvalidRequest {
            field: "list.continue_token",
            message: "malformed private continuation".to_string(),
        })?;
    let cursor: PrivateListContinuation =
        serde_json::from_slice(&bytes).map_err(|_| ResourceQueryError::InvalidRequest {
            field: "list.continue_token",
            message: "malformed private continuation".to_string(),
        })?;
    if cursor.v != PRIVATE_CONTINUATION_CODEC_VERSION {
        return Err(ResourceQueryError::InvalidRequest {
            field: "list.continue_token",
            message: "unsupported private continuation version".to_string(),
        });
    }
    let expected_scope = match scope {
        ResourceCollectionScope::Cluster => PrivateCollectionScope::Cluster,
        ResourceCollectionScope::AllNamespaces => PrivateCollectionScope::AllNamespaces,
        ResourceCollectionScope::Namespace(namespace) => {
            PrivateCollectionScope::Namespace(namespace.clone())
        }
    };
    let scope_matches = cursor.scope == expected_scope;
    let mode_matches = matches!(
        (cursor.mode, requested_mode),
        (
            PrivateContinuationMode::Pinned,
            ResourceListContinuationMode::Pinned
        ) | (
            PrivateContinuationMode::Recovery,
            ResourceListContinuationMode::Recovery
        )
    );
    let after_scope_matches = match scope {
        ResourceCollectionScope::Cluster => cursor.after_namespace.is_none(),
        ResourceCollectionScope::AllNamespaces => cursor
            .after_namespace
            .as_deref()
            .is_some_and(|namespace| !namespace.is_empty()),
        ResourceCollectionScope::Namespace(namespace) => {
            cursor.after_namespace.as_deref() == Some(namespace)
        }
    };
    if cursor.api_version != api_version
        || cursor.kind != kind
        || !scope_matches
        || cursor.label_selector.as_deref() != label_selector
        || cursor.field_selector.as_deref() != field_selector
        || !mode_matches
        || cursor.after_name.is_empty()
        || !after_scope_matches
    {
        return Err(ResourceQueryError::InvalidRequest {
            field: "list.continue_token",
            message: "private continuation context does not match LIST".to_string(),
        });
    }
    let after = ResourceCollectionKey::new(cursor.after_namespace, cursor.after_name);
    match (cursor.mode, cursor.position) {
        (PrivateContinuationMode::Pinned, Some(position)) => {
            let snapshot = ResourceListSnapshot::try_new(WatchReplayPosition {
                resource_version: position.resource_version,
                event_id: position.event_id,
                resource_version_filter_through_event_id: position
                    .resource_version_filter_through_event_id,
            })
            .map_err(|error| ResourceQueryError::InvalidRequest {
                field: "list.continue_token",
                message: error.to_string(),
            })?;
            Ok(DecodedListContinuation::Pinned(ResourceContinuation::new(
                after, snapshot,
            )))
        }
        (PrivateContinuationMode::Recovery, None) => Ok(DecodedListContinuation::Recovery(
            ResourceListRecoveryContinuation::new(after),
        )),
        _ => Err(ResourceQueryError::InvalidRequest {
            field: "list.continue_token",
            message: "private continuation position does not match its mode".to_string(),
        }),
    }
}

fn private_crd_plan(raw: &str) -> Result<Option<PrivateCrdPlanRef>, ResourceQueryError> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(raw)
        .map_err(|_| ResourceQueryError::InvalidRequest {
            field: "list.continue_token",
            message: "malformed private continuation".to_string(),
        })?;
    let cursor: PrivateListContinuation =
        serde_json::from_slice(&bytes).map_err(|_| ResourceQueryError::InvalidRequest {
            field: "list.continue_token",
            message: "malformed private continuation".to_string(),
        })?;
    Ok(cursor.crd_plan)
}

fn private_crd_plan_ref(definition: &Resource) -> Result<PrivateCrdPlanRef, ResourceQueryError> {
    let bytes = serde_json::to_vec(definition.data.as_ref())
        .map_err(|error| ResourceQueryError::corrupt_response(error.to_string()))?;
    Ok(PrivateCrdPlanRef {
        crd_name: definition.name.clone(),
        crd_uid: definition.uid.clone(),
        crd_resource_version: definition.resource_version,
        definition_digest: base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(Sha256::digest(bytes)),
    })
}

pub(crate) struct DatastoreResourceQueryAdapter {
    db: Option<DatastoreHandle>,
    resource_reads: Option<Arc<dyn ClusterResourceRead>>,
    authority: AuthorityHandle,
    wall_clock: Arc<dyn klights_supervisor::WallClock>,
}

impl DatastoreResourceQueryAdapter {
    #[cfg(test)]
    pub(crate) fn new<A: Into<AuthorityHandle>>(db: DatastoreHandle, authority: A) -> Arc<Self> {
        Arc::new(Self {
            db: Some(db),
            resource_reads: None,
            authority: authority.into(),
            wall_clock: Arc::new(klights_supervisor::SystemWallClock),
        })
    }

    /// The root-selected public LIST path. The focused read port receives
    /// decoded typed cursors and the root injects the clock that defines a
    /// pinned pagination session's bounded lifetime.
    pub(crate) fn new_with_resource_reads_and_clock<A: Into<AuthorityHandle>>(
        resource_reads: Arc<dyn ClusterResourceRead>,
        authority: A,
        wall_clock: Arc<dyn klights_supervisor::WallClock>,
    ) -> Arc<Self> {
        Arc::new(Self {
            db: None,
            resource_reads: Some(resource_reads),
            authority: authority.into(),
            wall_clock,
        })
    }

    #[cfg(test)]
    pub(crate) fn new_focused_for_test<A: Into<AuthorityHandle>>(
        resource_reads: Arc<dyn ClusterResourceRead>,
        authority: A,
    ) -> Arc<Self> {
        Arc::new(Self {
            db: None,
            resource_reads: Some(resource_reads),
            authority: authority.into(),
            wall_clock: Arc::new(klights_supervisor::SystemWallClock),
        })
    }

    #[cfg(test)]
    pub(crate) fn new_focused_for_test_with_clock<A: Into<AuthorityHandle>>(
        resource_reads: Arc<dyn ClusterResourceRead>,
        authority: A,
        wall_clock: Arc<dyn klights_supervisor::WallClock>,
    ) -> Arc<Self> {
        Arc::new(Self {
            db: None,
            resource_reads: Some(resource_reads),
            authority: authority.into(),
            wall_clock,
        })
    }

    fn private_cursor_now_unix_ms(&self) -> Result<i64, ResourceQueryError> {
        i64::try_from(
            self.wall_clock
                .now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| ResourceQueryError::InvalidRequest {
                    field: "list.continue_token",
                    message: "pinned continuation clock precedes the Unix epoch".to_string(),
                })?
                .as_millis(),
        )
        .map_err(|_| ResourceQueryError::InvalidRequest {
            field: "list.continue_token",
            message: "pinned continuation clock is out of range".to_string(),
        })
    }

    fn private_pinned_continuation_issued_at(
        &self,
        raw: &str,
    ) -> Result<Option<i64>, ResourceQueryError> {
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(raw)
            .map_err(|_| ResourceQueryError::InvalidRequest {
                field: "list.continue_token",
                message: "malformed private continuation".to_string(),
            })?;
        let cursor: PrivateListContinuation =
            serde_json::from_slice(&bytes).map_err(|_| ResourceQueryError::InvalidRequest {
                field: "list.continue_token",
                message: "malformed private continuation".to_string(),
            })?;
        if !matches!(cursor.mode, PrivateContinuationMode::Pinned) {
            return Ok(None);
        }
        Ok(cursor.issued_at_unix_ms)
    }

    fn private_pinned_continuation_expired(&self, raw: &str) -> Result<bool, ResourceQueryError> {
        private_pinned_cursor_expired_at(
            self.private_pinned_continuation_issued_at(raw)?,
            self.private_cursor_now_unix_ms()?,
        )
    }

    fn expired_pinned_continuation(
        &self,
        request: &ResourceListRequest,
        scope: &ResourceCollectionScope,
        cursor: &ResourceContinuation,
        crd_plan: Option<&PrivateCrdPlanRef>,
    ) -> Result<ResourceQueryError, ResourceQueryError> {
        let replacement_continue_token = encode_private_continuation(
            request.api_version(),
            request.kind(),
            scope,
            request.label_selector(),
            request.field_selector(),
            DecodedListContinuation::Recovery(ResourceListRecoveryContinuation::new(
                cursor.after().clone(),
            )),
            PrivateContinuationOptions {
                issued_at_unix_ms: None,
                crd_plan,
            },
        )?;
        Ok(ResourceQueryError::Expired {
            requested: cursor.snapshot().resource_version(),
            // This is a continuation-session retention boundary, not a global
            // datastore compaction boundary. The timed-out pinned snapshot is
            // unavailable; reporting its successor makes that strict boundary
            // truthful without doing an unrelated live-store read.
            oldest_available: cursor.snapshot().resource_version().saturating_add(1),
            replacement_continue_token: Some(replacement_continue_token),
        })
    }

    fn sample_leader_fresh(
        &self,
        consistency: ResourceQueryConsistency,
    ) -> Result<Option<klights_leader_api::AuthorityPermit>, ResourceQueryError> {
        if consistency != ResourceQueryConsistency::LeaderFresh {
            return Ok(None);
        }
        self.authority.local_permit().map(Some).map_err(|_| {
            ResourceQueryError::retryable(
                "leader-fresh resource query reached a non-authoritative local store",
            )
        })
    }

    fn query_error(error: impl std::fmt::Display) -> ResourceQueryError {
        ResourceQueryError::query_failed(error.to_string())
    }

    fn focused_read_error(error: ResourceReadError) -> ResourceQueryError {
        match error {
            ResourceReadError::InvalidRequest { message }
            | ResourceReadError::InvalidSelector { message }
            | ResourceReadError::InvalidContinuation { message } => {
                ResourceQueryError::InvalidRequest {
                    field: "list",
                    message,
                }
            }
            ResourceReadError::InvalidLimit { limit } => ResourceQueryError::InvalidRequest {
                field: "list.limit",
                message: format!("invalid limit {limit}"),
            },
            ResourceReadError::Expired {
                requested,
                oldest_available,
            } => ResourceQueryError::Expired {
                requested,
                oldest_available,
                replacement_continue_token: None,
            },
            ResourceReadError::Conflict { message } => ResourceQueryError::Conflict { message },
            ResourceReadError::UnsupportedMode { message } => {
                ResourceQueryError::Unsupported { message }
            }
            ResourceReadError::CorruptData { message } => {
                ResourceQueryError::corrupt_response(message)
            }
            ResourceReadError::Retryable { message } => ResourceQueryError::retryable(message),
            ResourceReadError::Timeout => ResourceQueryError::Timeout,
            ResourceReadError::Cancelled => ResourceQueryError::Cancelled,
            _ => ResourceQueryError::query_failed("unknown focused resource read failure"),
        }
    }

    fn store_scope(scope: &ResourceListScope) -> ResourceCollectionScope {
        match scope {
            ResourceListScope::Cluster => ResourceCollectionScope::Cluster,
            ResourceListScope::AllNamespaces => ResourceCollectionScope::AllNamespaces,
            ResourceListScope::Namespace(namespace) => {
                ResourceCollectionScope::Namespace(namespace.clone())
            }
        }
    }

    fn focused_list_request(
        &self,
        request: &ResourceListRequest,
    ) -> Result<(StoreResourceListRequest, Option<i64>), ResourceQueryError> {
        let scope = Self::store_scope(request.scope());
        let (continuation, recovery_continuation, pinned_issued_at_unix_ms) = match request
            .continuation_mode()
        {
            ResourceListContinuationMode::Initial => (None, None, None),
            ResourceListContinuationMode::Pinned | ResourceListContinuationMode::Recovery => {
                let raw =
                    request
                        .continue_token()
                        .ok_or_else(|| ResourceQueryError::InvalidRequest {
                            field: "list.continue_token",
                            message: "continuation is required".to_string(),
                        })?;
                let decoded = decode_private_continuation(
                    raw,
                    request.api_version(),
                    request.kind(),
                    &scope,
                    request.label_selector(),
                    request.field_selector(),
                    request.continuation_mode(),
                )?;
                if let DecodedListContinuation::Pinned(cursor) = &decoded
                    && self.private_pinned_continuation_expired(raw)?
                {
                    return Err(self.expired_pinned_continuation(request, &scope, cursor, None)?);
                }
                match decoded {
                    DecodedListContinuation::Pinned(cursor) => (
                        Some(cursor),
                        None,
                        self.private_pinned_continuation_issued_at(raw)?,
                    ),
                    DecodedListContinuation::Recovery(cursor) => (None, Some(cursor), None),
                }
            }
        };
        let query = ResourceListQuery::try_new_with_recovery(
            request.label_selector().map(str::to_owned),
            request.field_selector().map(str::to_owned),
            request.limit(),
            continuation,
            recovery_continuation,
            match request.resource_version_match() {
                klights_leader_api::ResourceListResourceVersionMatch::Any => {
                    ResourceVersionMatch::Any
                }
                klights_leader_api::ResourceListResourceVersionMatch::NotOlderThan(rv) => {
                    ResourceVersionMatch::NotOlderThan(rv)
                }
                klights_leader_api::ResourceListResourceVersionMatch::Exact(rv) => {
                    ResourceVersionMatch::Exact(rv)
                }
            },
        )
        .map_err(Self::focused_read_error)?;
        Ok((
            StoreResourceListRequest::new(request.api_version(), request.kind(), scope, query),
            pinned_issued_at_unix_ms,
        ))
    }

    fn focused_result(
        &self,
        request: &ResourceListRequest,
        read: ResourceListRead,
        pinned_issued_at_unix_ms: Option<i64>,
    ) -> Result<ResourceListResult, ResourceQueryError> {
        let scope = Self::store_scope(request.scope());
        match read {
            ResourceListRead::Current(page) | ResourceListRead::Historical(page) => {
                let snapshot = page.snapshot();
                let remaining_item_count = page.remaining_item_count();
                let issued_at_unix_ms = match pinned_issued_at_unix_ms {
                    Some(issued_at_unix_ms) => issued_at_unix_ms,
                    None => self.private_cursor_now_unix_ms()?,
                };
                let continuation = page
                    .continuation()
                    .cloned()
                    .map(DecodedListContinuation::Pinned)
                    .map(|cursor| {
                        encode_private_continuation(
                            request.api_version(),
                            request.kind(),
                            &scope,
                            request.label_selector(),
                            request.field_selector(),
                            cursor,
                            PrivateContinuationOptions {
                                issued_at_unix_ms: Some(issued_at_unix_ms),
                                crd_plan: None,
                            },
                        )
                    })
                    .transpose()?;
                ResourceListResult::try_new(
                    page.into_items(),
                    snapshot.resource_version(),
                    Some(snapshot.position()),
                    continuation,
                    remaining_item_count,
                )
            }
            ResourceListRead::Expired {
                requested,
                oldest_available,
                replacement,
            } => {
                let replacement_continue_token = replacement
                    .map(DecodedListContinuation::Recovery)
                    .map(|cursor| {
                        encode_private_continuation(
                            request.api_version(),
                            request.kind(),
                            &scope,
                            request.label_selector(),
                            request.field_selector(),
                            cursor,
                            PrivateContinuationOptions {
                                issued_at_unix_ms: None,
                                crd_plan: None,
                            },
                        )
                    })
                    .transpose()?;
                Err(ResourceQueryError::Expired {
                    requested,
                    oldest_available,
                    replacement_continue_token,
                })
            }
        }
    }

    /// Find the storage and still-served collections for a CRD request. This
    /// belongs at root composition: native routing does not inspect cursor
    /// bytes or build a parallel cross-version pagination session.
    async fn focused_crd_plan(
        resource_reads: &Arc<dyn ClusterResourceRead>,
        request: &ResourceListRequest,
        position: Option<WatchReplayPosition>,
        recovery_after: Option<ResourceCollectionKey>,
    ) -> Result<Option<ResolvedCrdReadPlan>, ResourceQueryError> {
        let Some(identity) = request.custom_resource_identity() else {
            return Ok(None);
        };
        let group = identity.group();
        let requested_version = identity.requested_version();
        let expected_name = format!("{}.{}", identity.plural(), group);
        let query = ResourceListQuery::try_new(
            None,
            Some(format!("metadata.name={expected_name}")),
            Some(1),
            None,
            match position {
                Some(position) => ResourceVersionMatch::AtPosition(position),
                None => match request.resource_version_match() {
                    klights_leader_api::ResourceListResourceVersionMatch::Any => {
                        ResourceVersionMatch::Any
                    }
                    klights_leader_api::ResourceListResourceVersionMatch::NotOlderThan(rv) => {
                        ResourceVersionMatch::NotOlderThan(rv)
                    }
                    klights_leader_api::ResourceListResourceVersionMatch::Exact(rv) => {
                        ResourceVersionMatch::Exact(rv)
                    }
                },
            },
        )
        .map_err(Self::focused_read_error)?;
        let crds = resource_reads
            .list_resources(StoreResourceListRequest::new(
                "apiextensions.k8s.io/v1",
                "CustomResourceDefinition",
                ResourceCollectionScope::Cluster,
                query,
            ))
            .await
            .map_err(Self::focused_read_error)?;
        let (crds, snapshot) = match crds {
            ResourceListRead::Current(page) | ResourceListRead::Historical(page) => {
                let snapshot = page.snapshot();
                (page.into_items(), snapshot)
            }
            ResourceListRead::Expired {
                requested,
                oldest_available,
                ..
            } => {
                let scope = Self::store_scope(request.scope());
                let replacement_continue_token = recovery_after
                    .map(ResourceListRecoveryContinuation::new)
                    .map(DecodedListContinuation::Recovery)
                    .map(|cursor| {
                        encode_private_continuation(
                            request.api_version(),
                            request.kind(),
                            &scope,
                            request.label_selector(),
                            request.field_selector(),
                            cursor,
                            PrivateContinuationOptions {
                                issued_at_unix_ms: None,
                                crd_plan: None,
                            },
                        )
                    })
                    .transpose()?;
                return Err(ResourceQueryError::Expired {
                    requested,
                    oldest_available,
                    replacement_continue_token,
                });
            }
        };
        for crd in crds {
            if crd.name != expected_name {
                continue;
            }
            let spec = crd.data.get("spec");
            let matches_request = spec
                .and_then(|spec| spec.get("group"))
                .and_then(serde_json::Value::as_str)
                == Some(group)
                && spec
                    .and_then(|spec| spec.pointer("/names/kind"))
                    .and_then(serde_json::Value::as_str)
                    == Some(request.kind());
            if !matches_request {
                continue;
            }
            let Some(versions) = spec
                .and_then(|spec| spec.get("versions"))
                .and_then(serde_json::Value::as_array)
            else {
                continue;
            };
            let served = versions
                .iter()
                .filter(|version| {
                    version.get("served").and_then(serde_json::Value::as_bool) == Some(true)
                })
                .filter_map(|version| {
                    version
                        .get("name")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                })
                .collect::<Vec<_>>();
            if !served.iter().any(|version| version == requested_version) {
                continue;
            }
            let storage_versions = versions
                .iter()
                .filter_map(|version| {
                    (version.get("storage").and_then(serde_json::Value::as_bool) == Some(true))
                        .then(|| version.get("name").and_then(serde_json::Value::as_str))
                        .flatten()
                })
                .collect::<Vec<_>>();
            if storage_versions.len() != 1 {
                return Err(ResourceQueryError::InvalidRequest {
                    field: "list.custom_resource",
                    message: "CRD must declare exactly one storage version".to_string(),
                });
            }
            let storage = storage_versions[0];
            let expected_scope = match request.scope() {
                ResourceListScope::Cluster => "Cluster",
                ResourceListScope::AllNamespaces | ResourceListScope::Namespace(_) => "Namespaced",
            };
            if spec
                .and_then(|spec| spec.get("scope"))
                .and_then(serde_json::Value::as_str)
                != Some(expected_scope)
            {
                return Err(ResourceQueryError::InvalidRequest {
                    field: "list.scope",
                    message: "CRD scope does not match requested collection scope".to_string(),
                });
            }
            let mut api_versions = Vec::with_capacity(served.len());
            api_versions.push(format!("{group}/{storage}"));
            api_versions.extend(
                served
                    .into_iter()
                    .filter(|version| version != storage)
                    .map(|version| format!("{group}/{version}")),
            );
            return Ok(Some(ResolvedCrdReadPlan {
                definition: crd,
                snapshot,
                api_versions,
            }));
        }
        Ok(None)
    }

    /// Compose retained pre-migration CRD collections at one typed datastore
    /// snapshot. The cursor remains a single root-owned composite key and
    /// replay position, so an old served-version row cannot force native code
    /// back to raw-name continuation or a synthetic session.
    async fn focused_composed_crd_list(
        &self,
        resource_reads: &Arc<dyn ClusterResourceRead>,
        request: &ResourceListRequest,
        plan: &ResolvedCrdReadPlan,
        pinned_issued_at_unix_ms: Option<i64>,
    ) -> Result<ResourceListResult, ResourceQueryError> {
        let scope = Self::store_scope(request.scope());
        let issued_at_unix_ms = match pinned_issued_at_unix_ms {
            Some(issued_at_unix_ms) => issued_at_unix_ms,
            None => self.private_cursor_now_unix_ms()?,
        };
        let decoded = match request.continuation_mode() {
            ResourceListContinuationMode::Initial => None,
            ResourceListContinuationMode::Pinned | ResourceListContinuationMode::Recovery => {
                let raw =
                    request
                        .continue_token()
                        .ok_or_else(|| ResourceQueryError::InvalidRequest {
                            field: "list.continue_token",
                            message: "continuation is required".to_string(),
                        })?;
                Some(decode_private_continuation(
                    raw,
                    request.api_version(),
                    request.kind(),
                    &scope,
                    request.label_selector(),
                    request.field_selector(),
                    request.continuation_mode(),
                )?)
            }
        };
        let after = decoded.as_ref().map(|cursor| match cursor {
            DecodedListContinuation::Pinned(cursor) => cursor.after().clone(),
            DecodedListContinuation::Recovery(cursor) => cursor.after().clone(),
        });
        let pinned_position = decoded.as_ref().and_then(|cursor| match cursor {
            DecodedListContinuation::Pinned(cursor) => Some(cursor.snapshot().position()),
            DecodedListContinuation::Recovery(_) => None,
        });
        let mut snapshot = None;
        let mut merged = std::collections::BTreeMap::<ResourceCollectionKey, Resource>::new();
        // Requested-version custom fields are not meaningful against raw
        // storage objects. Native applies them after conversion below.
        let store_field_selector = request
            .custom_resource_identity()
            .is_none()
            .then(|| request.field_selector().map(str::to_owned))
            .flatten();
        let requested_limit = request
            .limit()
            .and_then(|limit| usize::try_from(limit).ok())
            .filter(|limit| *limit > 0);
        let candidate_mode =
            request.custom_resource_identity().is_some() && request.field_selector().is_some();
        let page_limit = if candidate_mode {
            Some(64)
        } else {
            requested_limit
        };
        // Each served storage stream contributes at most the requested page
        // plus one probe.  The root-owned merge retains storage-version
        // precedence for duplicate identities without materializing an
        // unbounded historical collection.
        let per_version_limit =
            page_limit.map(|limit| limit.saturating_add(1).min(i64::MAX as usize) as i64);
        let source_snapshot = pinned_position
            .map(ResourceListSnapshot::try_new)
            .transpose()
            .map_err(Self::focused_read_error)?
            .unwrap_or(plan.snapshot);
        let source_after = after
            .clone()
            .map(|after| ResourceContinuation::new(after, source_snapshot));
        let mut source_has_more = false;
        for api_version in &plan.api_versions {
            let query = ResourceListQuery::try_new(
                request.label_selector().map(str::to_owned),
                store_field_selector.clone(),
                per_version_limit,
                source_after.clone(),
                pinned_position
                    .or(Some(source_snapshot.position()))
                    .or(snapshot.map(ResourceListSnapshot::position))
                    .map(ResourceVersionMatch::AtPosition)
                    .unwrap_or(ResourceVersionMatch::Any),
            )
            .map_err(Self::focused_read_error)?;
            let read = resource_reads
                .list_resources(StoreResourceListRequest::new(
                    api_version,
                    request.kind(),
                    scope.clone(),
                    query,
                ))
                .await
                .map_err(Self::focused_read_error)?;
            let page = match read {
                ResourceListRead::Current(page) | ResourceListRead::Historical(page) => page,
                ResourceListRead::Expired {
                    requested,
                    oldest_available,
                    ..
                } => {
                    // A converted field-selector LIST may be consuming
                    // several private candidate pages for one public page.
                    // Nothing from those pages has been committed to the
                    // client yet, so recovering from the internal boundary
                    // would skip earlier matching candidates.  Until the
                    // cursor protocol carries a separate public boundary,
                    // deliberately offer no replacement: restarting the
                    // public collection is safe; resuming past unseen data is
                    // not.
                    let replacement_continue_token = request
                        .field_selector()
                        .is_none()
                        .then(|| after.clone())
                        .flatten()
                        .map(ResourceListRecoveryContinuation::new)
                        .map(DecodedListContinuation::Recovery)
                        .map(|cursor| {
                            encode_private_continuation(
                                request.api_version(),
                                request.kind(),
                                &scope,
                                request.label_selector(),
                                request.field_selector(),
                                cursor,
                                PrivateContinuationOptions {
                                    issued_at_unix_ms: None,
                                    crd_plan: Some(&private_crd_plan_ref(&plan.definition)?),
                                },
                            )
                        })
                        .transpose()?;
                    return Err(ResourceQueryError::Expired {
                        requested,
                        oldest_available,
                        replacement_continue_token,
                    });
                }
            };
            source_has_more |= page.continuation().is_some();
            let page_snapshot = page.snapshot();
            if let Some(expected) = pinned_position.or(snapshot.map(ResourceListSnapshot::position))
                && page_snapshot.position() != expected
            {
                return Err(ResourceQueryError::Conflict {
                    message: "CRD collections did not share a LIST replay position".to_string(),
                });
            }
            snapshot = Some(page_snapshot);
            for resource in page.into_items() {
                let key =
                    ResourceCollectionKey::new(resource.namespace.clone(), resource.name.clone());
                // The storage version is queried first and remains authoritative
                // when a pre-migration duplicate exists under another version.
                merged.entry(key).or_insert(resource);
            }
        }
        let snapshot = snapshot.ok_or_else(|| {
            ResourceQueryError::corrupt_response("CRD list has no served collections")
        })?;
        let mut items = merged.into_values().collect::<Vec<_>>();
        // Cross-version duplicate collapse means a bounded source probe cannot
        // know the total remaining merged cardinality without draining every
        // served stream. Kubernetes permits this field to be omitted.
        let remaining_item_count = None;
        let continuation = page_limit.and_then(|limit| {
            (items.len() > limit || source_has_more).then(|| {
                let last = &items[limit - 1];
                ResourceContinuation::new(
                    ResourceCollectionKey::new(last.namespace.clone(), last.name.clone()),
                    snapshot,
                )
            })
        });
        if let Some(limit) = page_limit {
            items.truncate(limit);
        }
        let candidate_continue_tokens = if candidate_mode {
            items
                .iter()
                .map(|item| {
                    encode_private_continuation(
                        request.api_version(),
                        request.kind(),
                        &scope,
                        request.label_selector(),
                        request.field_selector(),
                        DecodedListContinuation::Pinned(ResourceContinuation::new(
                            ResourceCollectionKey::new(item.namespace.clone(), item.name.clone()),
                            snapshot,
                        )),
                        PrivateContinuationOptions {
                            issued_at_unix_ms: Some(issued_at_unix_ms),
                            crd_plan: Some(&private_crd_plan_ref(&plan.definition)?),
                        },
                    )
                    .map(Some)
                })
                .collect::<Result<Vec<_>, ResourceQueryError>>()?
        } else {
            Vec::new()
        };
        let continuation = continuation
            .map(DecodedListContinuation::Pinned)
            .map(|cursor| {
                encode_private_continuation(
                    request.api_version(),
                    request.kind(),
                    &scope,
                    request.label_selector(),
                    request.field_selector(),
                    cursor,
                    PrivateContinuationOptions {
                        issued_at_unix_ms: Some(issued_at_unix_ms),
                        crd_plan: Some(&private_crd_plan_ref(&plan.definition)?),
                    },
                )
            })
            .transpose()?;
        ResourceListResult::try_new(
            items,
            snapshot.resource_version(),
            Some(snapshot.position()),
            continuation,
            remaining_item_count,
        )
        .map(|result| {
            result
                .with_frozen_custom_resource_definition(plan.definition.clone())
                .with_candidate_continue_tokens(candidate_continue_tokens)
        })
    }
}

impl LeaderResourceQuery for DatastoreResourceQueryAdapter {
    fn get_resource(
        &self,
        request: ResourceGetRequest,
    ) -> ResourceQueryFuture<'_, Option<Resource>> {
        Box::pin(async move {
            let leadership = self.sample_leader_fresh(request.consistency())?;
            let resource = if let Some(resource_reads) = &self.resource_reads {
                resource_reads
                    .get_resource(klights_cluster_store::ResourceGetRequest::from_key(
                        request.key().clone(),
                    ))
                    .await
                    .map_err(Self::focused_read_error)?
            } else {
                let key = request.key();
                self.db
                    .as_ref()
                    .expect("legacy adapter construction supplies datastore")
                    .get_resource(
                        &key.api_version,
                        &key.kind,
                        key.namespace.as_deref(),
                        &key.name,
                    )
                    .await
                    .map_err(Self::query_error)?
            };
            if leadership
                .as_ref()
                .is_some_and(|permit| self.authority.validate(permit).is_err())
            {
                return Err(ResourceQueryError::retryable(
                    "leader authority changed during local leader-fresh resource query",
                ));
            }
            Ok(resource)
        })
    }

    fn list_resources(
        &self,
        request: ResourceListRequest,
    ) -> ResourceQueryFuture<'_, ResourceListResult> {
        Box::pin(async move {
            let leadership = self.sample_leader_fresh(request.consistency())?;
            let result =
                if let Some(resource_reads) = &self.resource_reads {
                    let (plan, pinned_issued_at_unix_ms) =
                        if request.custom_resource_identity().is_none() {
                            // Ordinary typed LIST continuations carry no CRD plan.
                            // They must stay on the focused store path instead of
                            // being rejected as an incomplete CRD cursor.
                            (None, None)
                        } else {
                            match request.continuation_mode() {
                                ResourceListContinuationMode::Pinned => {
                                    let raw = request.continue_token().ok_or_else(|| {
                                        ResourceQueryError::InvalidRequest {
                                            field: "list.continue_token",
                                            message: "pinned continuation is missing".to_string(),
                                        }
                                    })?;
                                    let cursor_ref = private_crd_plan(raw)?.ok_or_else(|| {
                                        ResourceQueryError::InvalidRequest {
                                    field: "list.continue_token",
                                    message: "CRD continuation is missing its definition reference"
                                        .to_string(),
                                }
                                    })?;
                                    let scope = Self::store_scope(request.scope());
                                    let decoded = decode_private_continuation(
                                        raw,
                                        request.api_version(),
                                        request.kind(),
                                        &scope,
                                        request.label_selector(),
                                        request.field_selector(),
                                        ResourceListContinuationMode::Pinned,
                                    )?;
                                    let DecodedListContinuation::Pinned(cursor) = decoded else {
                                        return Err(ResourceQueryError::InvalidRequest {
                                            field: "list.continue_token",
                                            message: "CRD cursor mode is not pinned".to_string(),
                                        });
                                    };
                                    if self.private_pinned_continuation_expired(raw)? {
                                        return Err(self.expired_pinned_continuation(
                                            &request,
                                            &scope,
                                            &cursor,
                                            Some(&cursor_ref),
                                        )?);
                                    }
                                    let plan = Self::focused_crd_plan(
                                        resource_reads,
                                        &request,
                                        Some(cursor.snapshot().position()),
                                        Some(cursor.after().clone()),
                                    )
                                    .await?
                                    .ok_or_else(|| ResourceQueryError::InvalidRequest {
                                        field: "list.continue_token",
                                        message: "pinned CRD definition is unavailable".to_string(),
                                    })?;
                                    if private_crd_plan_ref(&plan.definition)? != cursor_ref {
                                        return Err(ResourceQueryError::InvalidRequest {
                                    field: "list.continue_token",
                                    message:
                                        "CRD definition reference does not match pinned history"
                                            .to_string(),
                                });
                                    }
                                    (Some(plan), self.private_pinned_continuation_issued_at(raw)?)
                                }
                                ResourceListContinuationMode::Initial => (
                                    Self::focused_crd_plan(resource_reads, &request, None, None)
                                        .await?,
                                    None,
                                ),
                                ResourceListContinuationMode::Recovery => {
                                    let raw = request.continue_token().ok_or_else(|| {
                                        ResourceQueryError::InvalidRequest {
                                            field: "list.continue_token",
                                            message: "recovery continuation is missing".to_string(),
                                        }
                                    })?;
                                    let scope = Self::store_scope(request.scope());
                                    let decoded = decode_private_continuation(
                                        raw,
                                        request.api_version(),
                                        request.kind(),
                                        &scope,
                                        request.label_selector(),
                                        request.field_selector(),
                                        ResourceListContinuationMode::Recovery,
                                    )?;
                                    let DecodedListContinuation::Recovery(cursor) = decoded else {
                                        return Err(ResourceQueryError::InvalidRequest {
                                            field: "list.continue_token",
                                            message: "CRD cursor mode is not recovery".to_string(),
                                        });
                                    };
                                    (
                                        Self::focused_crd_plan(
                                            resource_reads,
                                            &request,
                                            None,
                                            Some(cursor.after().clone()),
                                        )
                                        .await?,
                                        None,
                                    )
                                }
                            }
                        };
                    if let Some(plan) = plan {
                        self.focused_composed_crd_list(
                            resource_reads,
                            &request,
                            &plan,
                            pinned_issued_at_unix_ms,
                        )
                        .await?
                    } else if request.custom_resource_identity().is_some() {
                        // A custom LIST is meaningful only with its frozen CRD
                        // definition/served-version plan. Falling through to an
                        // ordinary collection read would return no definition and
                        // let native or a remote worker fail later with a bogus
                        // 500/corrupt response.
                        return Err(ResourceQueryError::InvalidRequest {
                        field: "list.custom_resource",
                        message:
                            "custom resource definition is unavailable at the requested snapshot"
                                .to_string(),
                    });
                    } else {
                        let (store_request, pinned_issued_at_unix_ms) =
                            self.focused_list_request(&request)?;
                        let read = resource_reads
                            .list_resources(store_request)
                            .await
                            .map_err(Self::focused_read_error)?;
                        self.focused_result(&request, read, pinned_issued_at_unix_ms)?
                    }
                } else {
                    let list = self
                        .db
                        .as_ref()
                        .expect("legacy adapter construction supplies datastore")
                        .list_resources(
                            request.api_version(),
                            request.kind(),
                            request.namespace(),
                            klights_cluster_store::ResourceListOptions::new(
                                request.label_selector(),
                                request.field_selector(),
                                request.limit(),
                                request.continue_token(),
                            ),
                        )
                        .await
                        .map_err(Self::query_error)?;
                    ResourceListResult::try_new(
                        list.items,
                        list.resource_version,
                        list.watch_replay_position,
                        list.continue_token,
                        list.remaining_item_count,
                    )?
                };
            if leadership
                .as_ref()
                .is_some_and(|permit| self.authority.validate(permit).is_err())
            {
                return Err(ResourceQueryError::retryable(
                    "leader authority changed during local leader-fresh resource query",
                ));
            }
            Ok(result)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct CrdMergeSpyCall {
        api_version: String,
        limit: Option<i64>,
        after: Option<ResourceCollectionKey>,
        position: ResourceVersionMatch,
    }

    #[derive(Clone)]
    struct CrdMergeSpy {
        calls: Arc<std::sync::Mutex<Vec<CrdMergeSpyCall>>>,
        position: WatchReplayPosition,
    }

    impl CrdMergeSpy {
        fn new(position: WatchReplayPosition) -> Self {
            Self {
                calls: Arc::new(std::sync::Mutex::new(Vec::new())),
                position,
            }
        }

        fn resource(api_version: &str, name: &str, winner: &str) -> Resource {
            Resource {
                id: 0,
                api_version: api_version.to_string(),
                kind: "Widget".to_string(),
                namespace: Some("tenant-a".to_string()),
                name: name.to_string(),
                uid: format!("{name}-{winner}"),
                resource_version: 9,
                data: Arc::new(serde_json::json!({
                    "apiVersion": api_version, "kind": "Widget",
                    "metadata": {"namespace": "tenant-a", "name": name, "uid": format!("{name}-{winner}")},
                    "winner": winner,
                })),
            }
        }
    }

    impl ClusterResourceRead for CrdMergeSpy {
        fn get_resource(
            &self,
            _request: klights_cluster_store::ResourceGetRequest,
        ) -> klights_cluster_store::ResourceReadFuture<'_, Option<Resource>> {
            Box::pin(async { Ok(None) })
        }

        fn list_resources(
            &self,
            request: StoreResourceListRequest,
        ) -> klights_cluster_store::ResourceReadFuture<'_, ResourceListRead> {
            let api_version = request.api_version().to_string();
            let after = request.query().start_after().cloned();
            let snapshot = ResourceListSnapshot::try_new(self.position).unwrap();
            self.calls.lock().unwrap().push(CrdMergeSpyCall {
                api_version: api_version.clone(),
                limit: request.query().limit(),
                after: after.clone(),
                position: request.query().resource_version_match(),
            });
            Box::pin(async move {
                let items = match (
                    api_version.as_str(),
                    after.as_ref().map(ResourceCollectionKey::name),
                ) {
                    ("example.io/v1", None) => vec![
                        Self::resource("example.io/v1", "dup", "storage"),
                        Self::resource("example.io/v1", "zulu", "storage"),
                    ],
                    ("example.io/v2", None) => vec![
                        Self::resource("example.io/v2", "dup", "served"),
                        Self::resource("example.io/v2", "zulu-next", "served"),
                    ],
                    ("example.io/v2", Some("zulu")) => {
                        vec![Self::resource("example.io/v2", "zulu-next", "served")]
                    }
                    _ => Vec::new(),
                };
                Ok(ResourceListRead::Historical(
                    klights_cluster_store::ResourceListPage::try_new(items, snapshot, None, None)
                        .unwrap(),
                ))
            })
        }
    }

    fn crd_merge_plan(position: WatchReplayPosition) -> ResolvedCrdReadPlan {
        ResolvedCrdReadPlan {
            definition: CrdMergeSpy::resource(
                "apiextensions.k8s.io/v1",
                "widgets.example.io",
                "definition",
            ),
            snapshot: ResourceListSnapshot::try_new(position).unwrap(),
            api_versions: vec!["example.io/v1".to_string(), "example.io/v2".to_string()],
        }
    }

    fn crd_merge_request(token: Option<String>) -> ResourceListRequest {
        ResourceListRequest::try_new_with_continuation_mode(
            "example.io/v2",
            "Widget",
            ResourceListScope::Namespace("tenant-a".to_string()),
            None,
            None,
            Some(2),
            token.clone(),
            if token.is_some() {
                ResourceListContinuationMode::Pinned
            } else {
                ResourceListContinuationMode::Initial
            },
            ResourceQueryConsistency::Cached,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn composed_crd_merge_bounds_each_served_read_and_forwards_one_typed_cursor() {
        let position = WatchReplayPosition {
            resource_version: 41,
            event_id: 68,
            resource_version_filter_through_event_id: 69,
        };
        let spy = CrdMergeSpy::new(position);
        let reads: Arc<dyn ClusterResourceRead> = Arc::new(spy.clone());
        let adapter = DatastoreResourceQueryAdapter::new_focused_for_test(
            reads.clone(),
            crate::bootstrap::composition_adapters::authority_adapter::always_leader_authority(),
        );
        let plan = crd_merge_plan(position);
        let first = adapter
            .focused_composed_crd_list(&reads, &crd_merge_request(None), &plan, None)
            .await
            .unwrap();
        assert_eq!(
            first
                .items()
                .iter()
                .map(|item| item.name.as_str())
                .collect::<Vec<_>>(),
            ["dup", "zulu"],
        );
        assert_eq!(first.items()[0].data["winner"], "storage");
        let page_two_token = first.continue_token().unwrap().to_string();
        let second = adapter
            .focused_composed_crd_list(
                &reads,
                &crd_merge_request(Some(page_two_token)),
                &plan,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            second
                .items()
                .iter()
                .map(|item| item.name.as_str())
                .collect::<Vec<_>>(),
            ["zulu-next"],
        );
        let calls = spy.calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 4);
        assert_eq!(
            calls
                .iter()
                .map(|call| call.api_version.as_str())
                .collect::<Vec<_>>(),
            [
                "example.io/v1",
                "example.io/v2",
                "example.io/v1",
                "example.io/v2"
            ],
            "storage version must be queried first for deterministic duplicate precedence",
        );
        for call in &calls {
            assert!(call.limit.is_some_and(|limit| limit <= 65));
            assert_eq!(call.position, ResourceVersionMatch::AtPosition(position));
        }
        assert!(calls[..2].iter().all(|call| call.after.is_none()));
        assert!(calls[2..].iter().all(|call| {
            call.after.as_ref().is_some_and(|after| {
                after.namespace() == Some("tenant-a") && after.name() == "zulu"
            })
        }));
    }

    fn all_namespaces() -> ResourceCollectionScope {
        ResourceCollectionScope::AllNamespaces
    }

    #[test]
    fn private_pinned_cursor_round_trips_all_three_replay_fields_and_namespace() {
        let scope = all_namespaces();
        let cursor = ResourceContinuation::new(
            ResourceCollectionKey::new(Some("team-b"), "same/name \u{1f680}"),
            ResourceListSnapshot::try_new(WatchReplayPosition {
                resource_version: 41,
                event_id: 68,
                resource_version_filter_through_event_id: 69,
            })
            .unwrap(),
        );
        let encoded = encode_private_continuation(
            "example.io/v1",
            "Widget",
            &scope,
            Some("emoji=\u{1f680},tier in (prod,canary)"),
            Some("metadata.name!=old/name"),
            DecodedListContinuation::Pinned(cursor),
            PrivateContinuationOptions {
                issued_at_unix_ms: Some(1_000_000),
                crd_plan: None,
            },
        )
        .unwrap();
        assert!(encoded.is_ascii());
        let DecodedListContinuation::Pinned(decoded) = decode_private_continuation(
            &encoded,
            "example.io/v1",
            "Widget",
            &scope,
            Some("emoji=\u{1f680},tier in (prod,canary)"),
            Some("metadata.name!=old/name"),
            ResourceListContinuationMode::Pinned,
        )
        .unwrap() else {
            panic!("expected pinned continuation")
        };
        assert_eq!(decoded.after().namespace(), Some("team-b"));
        assert_eq!(decoded.after().name(), "same/name \u{1f680}");
        assert_eq!(
            decoded.snapshot().position(),
            WatchReplayPosition {
                resource_version: 41,
                event_id: 68,
                resource_version_filter_through_event_id: 69,
            }
        );
    }

    #[test]
    fn private_pinned_cursor_expiry_is_time_bounded_and_inclusive_at_the_ttl() {
        let issued_at_unix_ms = 1_000_000;
        assert!(
            !private_pinned_cursor_expired_at(
                Some(issued_at_unix_ms),
                issued_at_unix_ms + PRIVATE_PINNED_CONTINUATION_TTL.as_millis() as i64 - 1,
            )
            .unwrap(),
            "the pinned cursor must remain valid strictly before its TTL"
        );
        assert!(
            private_pinned_cursor_expired_at(
                Some(issued_at_unix_ms),
                issued_at_unix_ms + PRIVATE_PINNED_CONTINUATION_TTL.as_millis() as i64,
            )
            .unwrap(),
            "the TTL boundary must expire the pinned cursor"
        );
        assert!(
            private_pinned_cursor_expired_at(None, issued_at_unix_ms).unwrap(),
            "pre-TTL private pinned cursors must safely recover rather than remain indefinitely valid"
        );
        assert!(matches!(
            private_pinned_cursor_expired_at(Some(issued_at_unix_ms), issued_at_unix_ms - 1),
            Err(ResourceQueryError::InvalidRequest { .. })
        ));
    }

    #[test]
    fn legacy_pinned_cursor_recovers_at_the_same_key_while_fresh_cursor_keeps_its_issuance() {
        use std::time::{Duration, UNIX_EPOCH};

        struct FixedClock(std::time::SystemTime);

        impl klights_supervisor::WallClock for FixedClock {
            fn now(&self) -> std::time::SystemTime {
                self.0
            }
        }

        const NOW_UNIX_MS: i64 = 1_700_000_000_000;
        let scope = all_namespaces();
        let position = WatchReplayPosition {
            resource_version: 41,
            event_id: 68,
            resource_version_filter_through_event_id: 69,
        };
        let cursor = ResourceContinuation::new(
            ResourceCollectionKey::new(Some("team-a"), "after-this"),
            ResourceListSnapshot::try_new(position).unwrap(),
        );
        let request = |token, mode| {
            ResourceListRequest::try_new_with_continuation_mode(
                "v1",
                "ConfigMap",
                ResourceListScope::AllNamespaces,
                Some("team=blue".to_string()),
                None,
                Some(10),
                Some(token),
                mode,
                ResourceQueryConsistency::Cached,
            )
            .unwrap()
        };
        let adapter = DatastoreResourceQueryAdapter::new_focused_for_test_with_clock(
            Arc::new(CrdMergeSpy::new(position)),
            crate::bootstrap::composition_adapters::authority_adapter::always_leader_authority(),
            Arc::new(FixedClock(
                UNIX_EPOCH + Duration::from_millis(NOW_UNIX_MS as u64),
            )),
        );

        let legacy = encode_private_continuation(
            "v1",
            "ConfigMap",
            &scope,
            Some("team=blue"),
            None,
            DecodedListContinuation::Pinned(cursor.clone()),
            PrivateContinuationOptions {
                issued_at_unix_ms: None,
                crd_plan: None,
            },
        )
        .unwrap();
        let error = adapter
            .focused_list_request(&request(legacy, ResourceListContinuationMode::Pinned))
            .unwrap_err();
        let ResourceQueryError::Expired {
            requested,
            oldest_available,
            replacement_continue_token: Some(replacement),
            ..
        } = error
        else {
            panic!("legacy pinned cursor must return typed recovery")
        };
        assert_eq!(requested, position.resource_version);
        assert_eq!(
            oldest_available,
            position.resource_version.saturating_add(1),
            "TTL expiry must mark the pinned snapshot unavailable rather than claim its RV remains available"
        );
        let DecodedListContinuation::Recovery(recovery) = decode_private_continuation(
            &replacement,
            "v1",
            "ConfigMap",
            &scope,
            Some("team=blue"),
            None,
            ResourceListContinuationMode::Recovery,
        )
        .unwrap() else {
            panic!("legacy pinned cursor replacement must be recovery")
        };
        assert_eq!(recovery.after(), cursor.after());

        let issued_at_unix_ms = NOW_UNIX_MS - 1;
        let fresh = encode_private_continuation(
            "v1",
            "ConfigMap",
            &scope,
            Some("team=blue"),
            None,
            DecodedListContinuation::Pinned(cursor),
            PrivateContinuationOptions {
                issued_at_unix_ms: Some(issued_at_unix_ms),
                crd_plan: None,
            },
        )
        .unwrap();
        let (_, observed_issued_at_unix_ms) = adapter
            .focused_list_request(&request(fresh, ResourceListContinuationMode::Pinned))
            .expect("fresh pinned cursor must retain its original snapshot session");
        assert_eq!(observed_issued_at_unix_ms, Some(issued_at_unix_ms));
    }

    #[test]
    fn private_cursor_rejects_cross_context_and_recovery_omits_position() {
        let scope = all_namespaces();
        let encoded = encode_private_continuation(
            "v1",
            "ConfigMap",
            &scope,
            None,
            None,
            DecodedListContinuation::Recovery(ResourceListRecoveryContinuation::new(
                ResourceCollectionKey::new(Some("team-a"), "same"),
            )),
            PrivateContinuationOptions {
                issued_at_unix_ms: None,
                crd_plan: None,
            },
        )
        .unwrap();
        let DecodedListContinuation::Recovery(decoded) = decode_private_continuation(
            &encoded,
            "v1",
            "ConfigMap",
            &scope,
            None,
            None,
            ResourceListContinuationMode::Recovery,
        )
        .unwrap() else {
            panic!("expected recovery continuation")
        };
        assert_eq!(decoded.after().namespace(), Some("team-a"));
        assert!(matches!(
            decode_private_continuation(
                &encoded,
                "v1",
                "ConfigMap",
                &ResourceCollectionScope::Namespace("team-a".into()),
                None,
                None,
                ResourceListContinuationMode::Recovery,
            ),
            Err(ResourceQueryError::InvalidRequest { .. })
        ));
    }

    #[test]
    fn private_cursor_enforces_exact_composite_key_scope() {
        let pinned = |namespace, name| {
            DecodedListContinuation::Recovery(ResourceListRecoveryContinuation::new(
                ResourceCollectionKey::new(namespace, name),
            ))
        };
        for (scope, cursor) in [
            (
                ResourceCollectionScope::Cluster,
                pinned(Some("wrong"), "name"),
            ),
            (
                ResourceCollectionScope::Namespace("team-a".into()),
                pinned(Some("team-b"), "name"),
            ),
            (
                ResourceCollectionScope::AllNamespaces,
                pinned(Some(""), "name"),
            ),
        ] {
            assert!(matches!(
                encode_private_continuation(
                    "v1",
                    "ConfigMap",
                    &scope,
                    None,
                    None,
                    cursor,
                    PrivateContinuationOptions {
                        issued_at_unix_ms: None,
                        crd_plan: None,
                    },
                ),
                Err(ResourceQueryError::InvalidRequest { .. })
            ));
        }
    }

    #[test]
    fn private_cursor_rejects_malformed_version_selector_mode_and_position() {
        let scope = ResourceCollectionScope::Namespace("team-a".into());
        let cursor = PrivateListContinuation {
            v: PRIVATE_CONTINUATION_CODEC_VERSION,
            mode: PrivateContinuationMode::Pinned,
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            scope: PrivateCollectionScope::Namespace("team-a".into()),
            label_selector: Some("team=blue".into()),
            field_selector: None,
            after_namespace: Some("team-a".into()),
            after_name: "same".into(),
            position: Some(PrivateReplayPosition {
                resource_version: 8,
                event_id: -1,
                resource_version_filter_through_event_id: 0,
            }),
            issued_at_unix_ms: None,
            crd_plan: None,
        };
        let malformed_position = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&cursor).unwrap());
        let valid = encode_private_continuation(
            "v1",
            "ConfigMap",
            &scope,
            Some("team=blue"),
            None,
            DecodedListContinuation::Pinned(ResourceContinuation::new(
                ResourceCollectionKey::new(Some("team-a"), "same"),
                ResourceListSnapshot::try_new(WatchReplayPosition {
                    resource_version: 8,
                    event_id: 12,
                    resource_version_filter_through_event_id: 0,
                })
                .unwrap(),
            )),
            PrivateContinuationOptions {
                issued_at_unix_ms: Some(1_000_000),
                crd_plan: None,
            },
        )
        .unwrap();
        for (case, raw, selector, mode) in [
            (
                "malformed",
                "not-base64",
                Some("team=blue"),
                ResourceListContinuationMode::Pinned,
            ),
            (
                "position",
                &malformed_position,
                Some("team=blue"),
                ResourceListContinuationMode::Pinned,
            ),
            (
                "selector",
                &valid,
                Some("other=true"),
                ResourceListContinuationMode::Pinned,
            ),
            (
                "mode",
                &valid,
                Some("team=blue"),
                ResourceListContinuationMode::Recovery,
            ),
        ] {
            assert!(
                matches!(
                    decode_private_continuation(
                        raw,
                        "v1",
                        "ConfigMap",
                        &scope,
                        selector,
                        None,
                        mode,
                    ),
                    Err(ResourceQueryError::InvalidRequest { .. })
                ),
                "{case}"
            );
        }
        assert!(matches!(
            decode_private_continuation(
                &valid,
                "apps/v1",
                "ConfigMap",
                &scope,
                Some("team=blue"),
                None,
                ResourceListContinuationMode::Pinned,
            ),
            Err(ResourceQueryError::InvalidRequest { .. })
        ));
    }
}
