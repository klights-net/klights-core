use bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DryRunMode {
    Live,
    All,
}

impl DryRunMode {
    pub fn from_query(raw: Option<&str>) -> Result<Self, crate::api::AppError> {
        match raw.filter(|value| !value.is_empty()) {
            None => Ok(Self::Live),
            Some("All") => Ok(Self::All),
            Some(other) => Err(crate::api::AppError::BadRequest(format!(
                "Unsupported value: \"{other}\": supported values: \"All\""
            ))),
        }
    }

    pub fn is_all(self) -> bool {
        matches!(self, Self::All)
    }

    pub fn from_create_update_query(
        query: &crate::api::CreateUpdateQuery,
    ) -> Result<Self, crate::api::AppError> {
        Self::from_query(query.dry_run.as_deref())
    }

    pub fn from_delete_collection_query(
        query: &crate::api::DeleteCollectionQuery,
    ) -> Result<Self, crate::api::AppError> {
        Self::from_query(query.dry_run.as_deref())
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
    ) -> Result<Self, crate::api::AppError> {
        match body_policy.or(query_policy).unwrap_or("Background") {
            "Background" => Ok(Self::Background),
            "Foreground" => Ok(Self::Foreground),
            "Orphan" => Ok(Self::Orphan),
            other => Err(crate::api::AppError::BadRequest(format!(
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
    pub options: crate::api::DeleteOptions,
    pub preconditions: crate::datastore::ResourcePreconditions,
    pub propagation_policy: PropagationPolicy,
    pub orphan_children: bool,
}

impl DeleteIntent {
    pub fn from_query_and_body(
        query: &crate::api::CreateUpdateQuery,
        body: &Bytes,
    ) -> Result<Self, crate::api::AppError> {
        let dry_run = DryRunMode::from_create_update_query(query)?;
        let mut options = crate::api::parse_delete_options_body(body);
        if options._grace_period_seconds.is_none() {
            options._grace_period_seconds = query.grace_period_seconds;
        }
        let preconditions = options
            .resource_preconditions()
            .map_err(crate::api::AppError::BadRequest)?;
        let propagation_policy = PropagationPolicy::from_options(
            options.propagation_policy.as_deref(),
            query.propagation_policy.as_deref(),
        )?;
        let orphan_children = propagation_policy == PropagationPolicy::Orphan
            || options.orphan_dependents == Some(true)
            || query.orphan_dependents == Some(true);

        Ok(Self {
            dry_run,
            options,
            preconditions,
            propagation_policy,
            orphan_children,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(
        dry_run: Option<&str>,
        propagation_policy: Option<&str>,
        grace_period_seconds: Option<i64>,
    ) -> crate::api::CreateUpdateQuery {
        crate::api::CreateUpdateQuery {
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
    fn dry_run_mode_accepts_empty_or_all_only() {
        assert_eq!(DryRunMode::from_query(None).unwrap(), DryRunMode::Live);
        assert_eq!(DryRunMode::from_query(Some("")).unwrap(), DryRunMode::Live);
        assert_eq!(
            DryRunMode::from_query(Some("All")).unwrap(),
            DryRunMode::All
        );
        assert!(matches!(
            DryRunMode::from_query(Some("Some")),
            Err(crate::api::AppError::BadRequest(_))
        ));
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
}
