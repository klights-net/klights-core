use super::*;

type LabelRequirement = crate::label_selector::LabelRequirement;

pub(super) fn matches_label_requirements(data: &Value, requirements: &[LabelRequirement]) -> bool {
    let labels = data
        .get("metadata")
        .and_then(|m| m.get("labels"))
        .and_then(|l| l.as_object());
    requirements.iter().all(|req| req.matches(labels))
}

pub(super) fn resolve_field_path<'a>(
    data: &'a Value,
    path: &str,
) -> Option<std::borrow::Cow<'a, str>> {
    fn non_empty_str(value: Option<&Value>) -> Option<&str> {
        value.and_then(|v| v.as_str()).filter(|s| !s.is_empty())
    }

    if path == "source" {
        if let Some(component) = non_empty_str(data.get("source").and_then(|s| s.get("component")))
        {
            return Some(std::borrow::Cow::Borrowed(component));
        }
        if let Some(component) = non_empty_str(
            data.get("deprecatedSource")
                .and_then(|s| s.get("component")),
        ) {
            return Some(std::borrow::Cow::Borrowed(component));
        }
        if let Some(component) = non_empty_str(data.get("reportingController")) {
            return Some(std::borrow::Cow::Borrowed(component));
        }
        if let Some(component) = non_empty_str(data.get("reportingComponent")) {
            return Some(std::borrow::Cow::Borrowed(component));
        }
    }

    if let Some(suffix) = path.strip_prefix("involvedObject.") {
        let mut current = data
            .get("involvedObject")
            .or_else(|| data.get("regarding"))?;
        for segment in suffix.split('.') {
            current = current.get(segment)?;
        }
        return match current {
            Value::String(s) => Some(std::borrow::Cow::Borrowed(s.as_str())),
            Value::Bool(b) => Some(std::borrow::Cow::Owned(b.to_string())),
            Value::Number(n) => Some(std::borrow::Cow::Owned(n.to_string())),
            _ => None,
        };
    }

    let mut current = data;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    match current {
        Value::String(s) => Some(std::borrow::Cow::Borrowed(s.as_str())),
        Value::Bool(b) => Some(std::borrow::Cow::Owned(b.to_string())),
        Value::Number(n) => Some(std::borrow::Cow::Owned(n.to_string())),
        _ => None,
    }
}

#[cfg(test)]
pub fn filter_by_field_selector(items: Vec<Resource>, selector: &str) -> Vec<Resource> {
    if selector.is_empty() {
        return items;
    }

    let conditions: Vec<(&str, &str, bool)> = selector
        .split(',')
        .filter_map(|part| {
            let part = part.trim();
            if part.is_empty() {
                return None;
            }
            if let Some(idx) = part.find("!=") {
                let key = part[..idx].trim();
                let value = part[idx + 2..].trim();
                Some((key, value, false))
            } else if let Some(idx) = part.find('=') {
                let key = part[..idx].trim();
                let value = part[idx + 1..].trim();
                Some((key, value, true))
            } else {
                None
            }
        })
        .collect();

    if conditions.is_empty() {
        return items;
    }

    items
        .into_iter()
        .filter(|item| {
            conditions.iter().all(|(path, expected, is_eq)| {
                let actual = resolve_field_path(&item.data, path);
                let effective = actual.as_deref().or(if *expected == "false" {
                    Some("false")
                } else {
                    None
                });
                let matches = effective == Some(*expected);
                if *is_eq { matches } else { !matches }
            })
        })
        .collect()
}

pub(super) type FieldSelectorCondition = (String, String, bool);

/// SQL-level pushdown plan for a field selector. The fields directly indexed
/// in the namespaced/cluster tables (`name`, `namespace`) can become extra
/// SQL `AND` clauses; everything else stays as a residual selector that
/// `matches_field_selector_conditions` evaluates in Rust.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct SqlPushdownConditions {
    pub sql_name_eq: Option<String>,
    pub sql_namespace_eq: Option<String>,
    pub residual_selector: String,
}

/// Split a field selector string into SQL-pushdown-eligible equality conditions
/// on `metadata.name` / `metadata.namespace` and the residual selector that
/// must still be evaluated in Rust over the JSON body.
pub(super) fn split_sql_pushdown_conditions(selector: &str) -> SqlPushdownConditions {
    let mut sql_name_eq: Option<String> = None;
    let mut sql_namespace_eq: Option<String> = None;
    let mut residual: Vec<String> = Vec::new();

    for part in selector.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        // Inequality (!=) is never pushed down — keep it residual.
        if part.contains("!=") {
            residual.push(part.to_string());
            continue;
        }
        if let Some((key, value)) = part.split_once('=') {
            let key = key.trim();
            let value = value.trim();
            match key {
                "metadata.name" if sql_name_eq.is_none() => {
                    sql_name_eq = Some(value.to_string());
                    continue;
                }
                "metadata.namespace" if sql_namespace_eq.is_none() => {
                    sql_namespace_eq = Some(value.to_string());
                    continue;
                }
                _ => {}
            }
        }
        residual.push(part.to_string());
    }

    SqlPushdownConditions {
        sql_name_eq,
        sql_namespace_eq,
        residual_selector: residual.join(","),
    }
}

pub(super) fn parse_field_selector_conditions(selector: &str) -> Vec<FieldSelectorCondition> {
    selector
        .split(',')
        .filter_map(|part| {
            let part = part.trim();
            if part.is_empty() {
                return None;
            }
            if let Some(idx) = part.find("!=") {
                let key = part[..idx].trim();
                let value = part[idx + 2..].trim();
                Some((key.to_string(), value.to_string(), false))
            } else if let Some(idx) = part.find('=') {
                let key = part[..idx].trim();
                let value = part[idx + 1..].trim();
                Some((key.to_string(), value.to_string(), true))
            } else {
                None
            }
        })
        .collect()
}

pub(super) fn matches_field_selector_conditions(
    data: &Value,
    conditions: &[FieldSelectorCondition],
) -> bool {
    conditions.iter().all(|(path, expected, is_eq)| {
        let actual = resolve_field_path(data, path);
        let effective = actual.as_deref().or(if expected == "false" {
            Some("false")
        } else {
            None
        });
        let matches = effective == Some(expected.as_str());
        if *is_eq { matches } else { !matches }
    })
}

#[cfg(test)]
pub(super) fn split_selector(selector: &str) -> Vec<&str> {
    crate::label_selector::split_selector(selector)
}

pub(super) fn parse_label_selector(selector: &str) -> Result<Vec<LabelRequirement>> {
    crate::label_selector::parse_label_selector(selector)
}

#[cfg(test)]
mod sql_pushdown_tests {
    use super::*;

    #[test]
    fn split_sql_pushdown_extracts_metadata_name_eq() {
        let parsed = split_sql_pushdown_conditions("metadata.name=pod-9,status.phase=Running");
        assert_eq!(parsed.sql_name_eq.as_deref(), Some("pod-9"));
        assert!(parsed.sql_namespace_eq.is_none());
        assert_eq!(parsed.residual_selector, "status.phase=Running");
    }

    #[test]
    fn split_sql_pushdown_extracts_metadata_namespace_eq() {
        let parsed = split_sql_pushdown_conditions("metadata.namespace=kube-system");
        assert_eq!(parsed.sql_namespace_eq.as_deref(), Some("kube-system"));
        assert!(parsed.sql_name_eq.is_none());
        assert!(parsed.residual_selector.is_empty());
    }

    #[test]
    fn split_sql_pushdown_keeps_inequality_residual() {
        let parsed = split_sql_pushdown_conditions("metadata.name!=other");
        assert!(parsed.sql_name_eq.is_none());
        assert_eq!(parsed.residual_selector, "metadata.name!=other");
    }

    #[test]
    fn split_sql_pushdown_keeps_unknown_keys_residual() {
        let parsed = split_sql_pushdown_conditions("status.phase=Running");
        assert!(parsed.sql_name_eq.is_none());
        assert!(parsed.sql_namespace_eq.is_none());
        assert_eq!(parsed.residual_selector, "status.phase=Running");
    }

    #[test]
    fn split_sql_pushdown_handles_empty_selector() {
        let parsed = split_sql_pushdown_conditions("");
        assert!(parsed.sql_name_eq.is_none());
        assert!(parsed.sql_namespace_eq.is_none());
        assert!(parsed.residual_selector.is_empty());
    }
}
