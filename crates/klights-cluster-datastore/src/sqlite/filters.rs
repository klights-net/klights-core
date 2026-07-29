use anyhow::Result;
use klights_cluster_core::Resource;
use serde_json::Value;
type LabelRequirement = klights_types::LabelRequirement;

pub fn matches_label_requirements(data: &Value, requirements: &[LabelRequirement]) -> bool {
    let labels = data
        .get("metadata")
        .and_then(|m| m.get("labels"))
        .and_then(|l| l.as_object());
    requirements.iter().all(|req| req.matches(labels))
}

pub fn resolve_field_path<'a>(data: &'a Value, path: &str) -> Option<std::borrow::Cow<'a, str>> {
    klights_types::resolve_field_value(data, path)
}

#[cfg(any(test, feature = "test-support"))]
pub fn filter_by_field_selector(items: Vec<Resource>, selector: &str) -> Vec<Resource> {
    let selector = klights_types::FieldSelector::parse(selector)
        .expect("API validation must reject malformed field selectors before datastore filtering");
    items
        .into_iter()
        .filter(|item| {
            selector.matches_resource_with_identity(&item.api_version, &item.kind, &item.data)
        })
        .collect()
}

/// SQL-level pushdown plan for a field selector. The fields directly indexed
/// in the namespaced/cluster tables (`name`, `namespace`) can become extra
/// SQL `AND` clauses; everything else stays as a residual selector that
/// `matches_field_selector_conditions` evaluates in Rust.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SqlPushdownConditions {
    pub sql_name_eq: Option<String>,
    pub sql_namespace_eq: Option<String>,
    pub residual_fields: Vec<klights_types::FieldRequirement>,
}

/// Split a field selector string into SQL-pushdown-eligible equality conditions
/// on `metadata.name` / `metadata.namespace` and the residual selector that
/// must still be evaluated in Rust over the JSON body.
pub fn split_sql_pushdown_conditions(selector: &str) -> Result<SqlPushdownConditions> {
    let mut sql_name_eq: Option<String> = None;
    let mut sql_namespace_eq: Option<String> = None;
    let mut residual_fields = Vec::new();
    let parsed = klights_types::FieldSelector::parse(selector)?;
    for requirement in parsed.requirements() {
        let pushable = requirement.operator() == klights_types::FieldSelectorOperator::Equals
            && !requirement.value().is_empty();
        match requirement.field() {
            "metadata.name" if pushable && sql_name_eq.is_none() => {
                sql_name_eq = Some(requirement.value().to_string());
            }
            "metadata.namespace" if pushable && sql_namespace_eq.is_none() => {
                sql_namespace_eq = Some(requirement.value().to_string());
            }
            _ => residual_fields.push(requirement.clone()),
        }
    }
    Ok(SqlPushdownConditions {
        sql_name_eq,
        sql_namespace_eq,
        residual_fields,
    })
}

pub fn matches_field_selector_conditions(
    resource: &Resource,
    conditions: &[klights_types::FieldRequirement],
) -> bool {
    conditions.iter().all(|condition| {
        condition.matches_resource_with_identity(
            &resource.api_version,
            &resource.kind,
            &resource.data,
        )
    })
}

#[cfg(any(test, feature = "test-support"))]
pub fn split_selector(selector: &str) -> Vec<&str> {
    klights_types::split_selector(selector)
}

pub fn parse_label_selector(selector: &str) -> Result<Vec<LabelRequirement>> {
    Ok(klights_types::parse_label_selector(selector)?)
}

#[cfg(test)]
mod sql_pushdown_tests {
    use super::*;

    #[test]
    fn split_sql_pushdown_extracts_metadata_name_eq() {
        let parsed =
            split_sql_pushdown_conditions("metadata.name=pod-9,status.phase=Running").unwrap();
        assert_eq!(parsed.sql_name_eq.as_deref(), Some("pod-9"));
        assert!(parsed.sql_namespace_eq.is_none());
        assert_eq!(parsed.residual_fields.len(), 1);
        assert_eq!(parsed.residual_fields[0].field(), "status.phase");
    }

    #[test]
    fn split_sql_pushdown_extracts_metadata_namespace_eq() {
        let parsed = split_sql_pushdown_conditions("metadata.namespace=kube-system").unwrap();
        assert_eq!(parsed.sql_namespace_eq.as_deref(), Some("kube-system"));
        assert!(parsed.sql_name_eq.is_none());
        assert!(parsed.residual_fields.is_empty());
    }

    #[test]
    fn split_sql_pushdown_keeps_inequality_residual() {
        let parsed = split_sql_pushdown_conditions("metadata.name!=other").unwrap();
        assert!(parsed.sql_name_eq.is_none());
        assert_eq!(parsed.residual_fields.len(), 1);
    }

    #[test]
    fn split_sql_pushdown_keeps_unknown_keys_residual() {
        let parsed = split_sql_pushdown_conditions("status.phase=Running").unwrap();
        assert!(parsed.sql_name_eq.is_none());
        assert!(parsed.sql_namespace_eq.is_none());
        assert_eq!(parsed.residual_fields.len(), 1);
    }

    #[test]
    fn split_sql_pushdown_handles_empty_selector() {
        let parsed = split_sql_pushdown_conditions("").unwrap();
        assert!(parsed.sql_name_eq.is_none());
        assert!(parsed.sql_namespace_eq.is_none());
        assert!(parsed.residual_fields.is_empty());
    }
}
