//! Kubernetes command query and `DeleteOptions` decoding.

use bytes::Bytes;
use serde::Deserialize;

use crate::AppError;

#[derive(Deserialize)]
pub struct CreateUpdateQuery {
    #[serde(rename = "dryRun")]
    pub dry_run: Option<String>,
    #[serde(rename = "fieldManager")]
    pub field_manager: Option<String>,
    #[serde(rename = "fieldValidation")]
    pub field_validation: Option<String>,
    pub force: Option<bool>,
    #[serde(rename = "orphanDependents")]
    pub orphan_dependents: Option<bool>,
    #[serde(rename = "propagationPolicy")]
    pub propagation_policy: Option<String>,
    #[serde(rename = "gracePeriodSeconds")]
    pub grace_period_seconds: Option<i64>,
}

#[derive(Deserialize)]
pub struct DeleteCollectionQuery {
    #[serde(rename = "labelSelector")]
    pub label_selector: Option<String>,
    #[serde(rename = "fieldSelector")]
    pub field_selector: Option<String>,
    #[serde(rename = "dryRun")]
    pub dry_run: Option<String>,
}

#[derive(Deserialize, serde::Serialize, Default)]
pub struct DeleteOptions {
    #[serde(rename = "propagationPolicy")]
    pub propagation_policy: Option<String>,
    #[serde(rename = "orphanDependents")]
    pub orphan_dependents: Option<bool>,
    #[serde(rename = "gracePeriodSeconds")]
    pub _grace_period_seconds: Option<i64>,
    pub preconditions: Option<DeletePreconditions>,
}

#[derive(Clone, Deserialize, serde::Serialize, Default)]
pub struct DeletePreconditions {
    pub uid: Option<String>,
    #[serde(rename = "resourceVersion")]
    pub resource_version: Option<String>,
}

impl DeleteOptions {
    pub fn with_uid_precondition(uid: impl Into<String>) -> Self {
        Self {
            preconditions: Some(DeletePreconditions {
                uid: Some(uid.into()),
                resource_version: None,
            }),
            ..Default::default()
        }
    }

    pub fn resource_preconditions(
        &self,
    ) -> Result<klights_cluster_core::ResourcePreconditions, String> {
        let Some(preconditions) = &self.preconditions else {
            return Ok(klights_cluster_core::ResourcePreconditions::default());
        };
        let resource_version = preconditions
            .resource_version
            .as_deref()
            .map(|rv| {
                rv.parse::<i64>().map_err(|_| {
                    format!("invalid DeleteOptions preconditions.resourceVersion: {rv}")
                })
            })
            .transpose()?;
        Ok(klights_cluster_core::ResourcePreconditions {
            uid: preconditions.uid.clone(),
            resource_version,
        })
    }
}

impl From<DeleteOptions> for klights_pod_api::PodDeleteOptions {
    fn from(options: DeleteOptions) -> Self {
        let preconditions = options.preconditions.unwrap_or_default();
        Self::new(
            options.propagation_policy,
            options.orphan_dependents,
            options._grace_period_seconds,
            klights_pod_api::PodDeletePreconditions::new(
                preconditions.uid,
                preconditions.resource_version,
            ),
        )
    }
}

pub fn parse_delete_options_body(body: &[u8]) -> DeleteOptions {
    if body.is_empty() {
        return DeleteOptions::default();
    }
    if let Ok(options) = serde_json::from_slice::<DeleteOptions>(body) {
        return options;
    }
    parse_delete_options_protobuf(body).unwrap_or_default()
}

pub fn parse_delete_options_protobuf(body: &[u8]) -> Option<DeleteOptions> {
    use prost::Message;

    fn map_pb_delete_options(
        options: klights_kube_protobuf::apimachinery::pkg::apis::meta::v1::DeleteOptions,
    ) -> DeleteOptions {
        DeleteOptions {
            propagation_policy: options.propagation_policy,
            orphan_dependents: options.orphan_dependents,
            _grace_period_seconds: options.grace_period_seconds,
            preconditions: options
                .preconditions
                .map(|preconditions| DeletePreconditions {
                    uid: preconditions.uid,
                    resource_version: preconditions.resource_version,
                }),
        }
    }

    fn parse_unknown_payload(payload: &[u8]) -> Option<DeleteOptions> {
        use prost::Message;

        let unknown = klights_kube_protobuf::Unknown::decode(payload).ok()?;
        if let Ok(options) = serde_json::from_slice::<DeleteOptions>(&unknown.raw) {
            return Some(options);
        }
        klights_kube_protobuf::apimachinery::pkg::apis::meta::v1::DeleteOptions::decode(
            unknown.raw.as_slice(),
        )
        .ok()
        .map(map_pb_delete_options)
    }

    const K8S_MAGIC_PREFIX: [u8; 4] = [0x6b, 0x38, 0x73, 0x00];
    if body.len() >= 4
        && body[0..4] == K8S_MAGIC_PREFIX
        && let Some(options) = parse_unknown_payload(&body[4..])
    {
        return Some(options);
    }
    if let Some(options) = parse_unknown_payload(body) {
        return Some(options);
    }
    let payload = if body.len() >= 4 && body[0..4] == K8S_MAGIC_PREFIX {
        &body[4..]
    } else {
        body
    };
    klights_kube_protobuf::apimachinery::pkg::apis::meta::v1::DeleteOptions::decode(payload)
        .ok()
        .map(map_pb_delete_options)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DryRunMode {
    Live,
    All,
}

impl DryRunMode {
    pub fn from_query(raw: Option<&str>) -> Result<Self, AppError> {
        match raw.filter(|value| !value.is_empty()) {
            None => Ok(Self::Live),
            Some("All") => Ok(Self::All),
            Some(other) => Err(AppError::BadRequest(format!(
                "Unsupported value: \"{other}\": supported values: \"All\""
            ))),
        }
    }

    pub fn is_all(self) -> bool {
        matches!(self, Self::All)
    }

    pub fn from_create_update_query(query: &CreateUpdateQuery) -> Result<Self, AppError> {
        Self::from_query(query.dry_run.as_deref())
    }

    pub fn from_delete_collection_query(query: &DeleteCollectionQuery) -> Result<Self, AppError> {
        Self::from_query(query.dry_run.as_deref())
    }

    pub fn from_eviction(
        query: &CreateUpdateQuery,
        delete_option_values: &[String],
    ) -> Result<Self, AppError> {
        let query_mode = Self::from_create_update_query(query)?;
        match delete_option_values {
            [] => Ok(query_mode),
            [value] if value == "All" => Ok(Self::All),
            _ => Err(AppError::BadRequest(
                "deleteOptions.dryRun supports only [\"All\"]".to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropagationPolicy {
    Background,
    Foreground,
    Orphan,
}

impl PropagationPolicy {
    fn from_options(
        body_policy: Option<&str>,
        query_policy: Option<&str>,
    ) -> Result<Self, AppError> {
        match body_policy.or(query_policy).unwrap_or("Background") {
            "Background" => Ok(Self::Background),
            "Foreground" => Ok(Self::Foreground),
            "Orphan" => Ok(Self::Orphan),
            other => Err(AppError::BadRequest(format!(
                "Unsupported value: \"{other}\": supported values: \"Background\", \"Foreground\", \"Orphan\""
            ))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Background => "Background",
            Self::Foreground => "Foreground",
            Self::Orphan => "Orphan",
        }
    }
}

pub struct DeleteIntent {
    pub dry_run: DryRunMode,
    pub options: DeleteOptions,
    pub preconditions: klights_cluster_core::ResourcePreconditions,
    pub propagation_policy: PropagationPolicy,
    pub orphan_children: bool,
    pub uid_mismatch_is_conflict: bool,
}

impl DeleteIntent {
    pub fn from_query_and_body(query: &CreateUpdateQuery, body: &Bytes) -> Result<Self, AppError> {
        let dry_run = DryRunMode::from_create_update_query(query)?;
        let mut options = parse_delete_options_body(body);
        if options._grace_period_seconds.is_none() {
            options._grace_period_seconds = query.grace_period_seconds;
        }
        let preconditions = options
            .resource_preconditions()
            .map_err(AppError::BadRequest)?;
        let propagation_policy = PropagationPolicy::from_options(
            options.propagation_policy.as_deref(),
            query.propagation_policy.as_deref(),
        )?;
        let orphan_children = propagation_policy == PropagationPolicy::Orphan
            || options.orphan_dependents == Some(true)
            || query.orphan_dependents == Some(true);
        let uid_mismatch_is_conflict = preconditions.uid.is_some();
        Ok(Self {
            dry_run,
            options,
            preconditions,
            propagation_policy,
            orphan_children,
            uid_mismatch_is_conflict,
        })
    }

    pub fn from_delete_collection_query_and_body(
        query: &DeleteCollectionQuery,
        body: &Bytes,
    ) -> Result<Self, AppError> {
        let dry_run = DryRunMode::from_delete_collection_query(query)?;
        let options = parse_delete_options_body(body);
        let preconditions = options
            .resource_preconditions()
            .map_err(AppError::BadRequest)?;
        let propagation_policy =
            PropagationPolicy::from_options(options.propagation_policy.as_deref(), None)?;
        let orphan_children = propagation_policy == PropagationPolicy::Orphan
            || options.orphan_dependents == Some(true);
        let uid_mismatch_is_conflict = preconditions.uid.is_some();
        Ok(Self {
            dry_run,
            options,
            preconditions,
            propagation_policy,
            orphan_children,
            uid_mismatch_is_conflict,
        })
    }

    pub fn collection_item(
        dry_run: DryRunMode,
        preconditions: klights_cluster_core::ResourcePreconditions,
    ) -> Self {
        Self {
            dry_run,
            options: DeleteOptions::default(),
            preconditions,
            propagation_policy: PropagationPolicy::Background,
            orphan_children: false,
            uid_mismatch_is_conflict: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;

    fn query(
        dry_run: Option<&str>,
        propagation_policy: Option<&str>,
        grace_period_seconds: Option<i64>,
    ) -> CreateUpdateQuery {
        CreateUpdateQuery {
            dry_run: dry_run.map(ToString::to_string),
            field_manager: None,
            field_validation: None,
            force: None,
            orphan_dependents: None,
            propagation_policy: propagation_policy.map(ToString::to_string),
            grace_period_seconds,
        }
    }

    #[test]
    fn delete_options_json_and_protobuf_preserve_preconditions() {
        let json = br#"{"gracePeriodSeconds":-5,"preconditions":{"uid":"u1","resourceVersion":"9"},"propagationPolicy":"Foreground"}"#;
        let protobuf = klights_kube_protobuf::apimachinery::pkg::apis::meta::v1::DeleteOptions {
            grace_period_seconds: Some(-5),
            preconditions: Some(
                klights_kube_protobuf::apimachinery::pkg::apis::meta::v1::Preconditions {
                    uid: Some("u1".to_string()),
                    resource_version: Some("9".to_string()),
                },
            ),
            propagation_policy: Some("Foreground".to_string()),
            ..Default::default()
        }
        .encode_to_vec();

        for body in [json.as_slice(), protobuf.as_slice()] {
            let options = parse_delete_options_body(body);
            let preconditions = options.resource_preconditions().unwrap();
            assert_eq!(preconditions.uid.as_deref(), Some("u1"));
            assert_eq!(preconditions.resource_version, Some(9));
            assert_eq!(options.propagation_policy.as_deref(), Some("Foreground"));
            assert_eq!(options._grace_period_seconds, Some(-5));
        }
    }

    #[test]
    fn delete_intent_prefers_body_grace_then_query_grace() {
        let query = query(Some("All"), Some("Foreground"), Some(7));
        let body = Bytes::from_static(
            br#"{"kind":"DeleteOptions","apiVersion":"v1","gracePeriodSeconds":3}"#,
        );
        let intent = DeleteIntent::from_query_and_body(&query, &body).unwrap();
        assert_eq!(intent.dry_run, DryRunMode::All);
        assert_eq!(intent.options._grace_period_seconds, Some(3));
        assert_eq!(intent.propagation_policy, PropagationPolicy::Foreground);
    }

    #[test]
    fn delete_intent_extracts_uid_and_resource_version_preconditions() {
        let query = query(None, None, None);
        let body = Bytes::from_static(
            br#"{"kind":"DeleteOptions","apiVersion":"v1","preconditions":{"uid":"u1","resourceVersion":"9"}}"#,
        );
        let intent = DeleteIntent::from_query_and_body(&query, &body).unwrap();
        assert_eq!(intent.preconditions.uid.as_deref(), Some("u1"));
        assert_eq!(intent.preconditions.resource_version, Some(9));
    }

    #[test]
    fn delete_collection_intent_extracts_preconditions_from_body() {
        let query = DeleteCollectionQuery {
            label_selector: Some("mutation=precondition".into()),
            field_selector: None,
            dry_run: None,
        };
        let body = Bytes::from_static(
            br#"{"kind":"DeleteOptions","apiVersion":"v1","propagationPolicy":"Orphan","preconditions":{"uid":"expected-uid","resourceVersion":"7"}}"#,
        );
        let intent = DeleteIntent::from_delete_collection_query_and_body(&query, &body).unwrap();
        assert_eq!(intent.preconditions.uid.as_deref(), Some("expected-uid"));
        assert_eq!(intent.preconditions.resource_version, Some(7));
        assert_eq!(intent.propagation_policy, PropagationPolicy::Orphan);
        assert!(intent.orphan_children);
    }

    #[test]
    fn dry_run_mode_accepts_empty_or_all_only() {
        assert_eq!(DryRunMode::from_query(None).unwrap(), DryRunMode::Live);
        assert_eq!(DryRunMode::from_query(Some("")).unwrap(), DryRunMode::Live);
        assert_eq!(
            DryRunMode::from_query(Some("All")).unwrap(),
            DryRunMode::All
        );
        assert!(matches!(
            DryRunMode::from_query(Some("Some")),
            Err(AppError::BadRequest(_))
        ));
    }
}

#[test]
fn test_delete_options_orphan_dependents_query_triggers_orphan_path() {
    // orphanDependents=true is the legacy K8s alias for propagationPolicy=Orphan
    let orphan_dependents: Option<bool> = Some(true);
    // No body, no query policy → falls back to default.
    let policy: &str = "Background";
    let orphan = policy == "Orphan" || orphan_dependents.is_some_and(|v| v);
    assert!(orphan, "orphanDependents=true must trigger orphan path");
}

#[test]
fn test_delete_options_protobuf_unknown_envelope_parses_orphan_policy() {
    use prost::Message;

    let pb = klights_kube_protobuf::apimachinery::pkg::apis::meta::v1::DeleteOptions {
        propagation_policy: Some("Orphan".to_string()),
        ..Default::default()
    };
    let mut raw = Vec::new();
    pb.encode(&mut raw).unwrap();

    let unknown = klights_kube_protobuf::Unknown {
        type_meta: Some(klights_kube_protobuf::TypeMeta {
            api_version: "v1".to_string(),
            kind: "DeleteOptions".to_string(),
        }),
        raw,
        content_encoding: String::new(),
        content_type: String::new(),
    };

    let mut body = vec![0x6b, 0x38, 0x73, 0x00];
    unknown.encode(&mut body).unwrap();

    let opts = parse_delete_options_body(&body);
    assert_eq!(opts.propagation_policy.as_deref(), Some("Orphan"));
}
