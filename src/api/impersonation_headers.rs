//! HTTP parsing adapter for Kubernetes impersonation headers.

use crate::auth::{AuthError, impersonation::ImpersonationRequest};
use axum::http::{HeaderMap, HeaderName};

const IMPERSONATE_USER: &str = "impersonate-user";
const IMPERSONATE_GROUP: &str = "impersonate-group";
const IMPERSONATE_UID: &str = "impersonate-uid";
const IMPERSONATE_EXTRA_PREFIX: &str = "impersonate-extra-";

pub fn parse(headers: &HeaderMap) -> Result<Option<ImpersonationRequest>, AuthError> {
    let users = header_values(headers, IMPERSONATE_USER)?;
    let groups = header_values(headers, IMPERSONATE_GROUP)?;
    let uids = header_values(headers, IMPERSONATE_UID)?;
    let extra = impersonation_extra_values(headers)?;

    if users.is_empty() {
        if !groups.is_empty() || !uids.is_empty() || !extra.is_empty() {
            return Err(AuthError::invalid_request(
                "Impersonate-User is required when using impersonation headers",
            ));
        }
        return Ok(None);
    }
    if users.len() > 1 {
        return Err(AuthError::invalid_request(
            "Impersonate-User may only be specified once",
        ));
    }
    let username = users.into_iter().next().expect("one user checked above");
    if username.is_empty() {
        return Err(AuthError::invalid_request(
            "Impersonate-User must not be empty",
        ));
    }
    if groups.iter().any(String::is_empty) {
        return Err(AuthError::invalid_request(
            "Impersonate-Group must not be empty",
        ));
    }
    if uids.len() > 1 {
        return Err(AuthError::invalid_request(
            "Impersonate-Uid may only be specified once",
        ));
    }
    if uids.iter().any(String::is_empty) {
        return Err(AuthError::invalid_request(
            "Impersonate-Uid must not be empty",
        ));
    }

    Ok(Some(ImpersonationRequest {
        username,
        groups,
        uid: uids.into_iter().next(),
        extra,
    }))
}

fn header_values(headers: &HeaderMap, name: &'static str) -> Result<Vec<String>, AuthError> {
    headers
        .get_all(name)
        .iter()
        .map(|value| {
            value.to_str().map(ToString::to_string).map_err(|_| {
                AuthError::invalid_request(format!("{name} contains invalid header value"))
            })
        })
        .collect()
}

fn impersonation_extra_values(headers: &HeaderMap) -> Result<Vec<(String, String)>, AuthError> {
    let mut extra_headers = headers
        .keys()
        .filter_map(|name| {
            header_suffix_ignore_ascii_case(name.as_str(), IMPERSONATE_EXTRA_PREFIX)
                .map(|suffix| (name.clone(), suffix.to_string()))
        })
        .collect::<Vec<(HeaderName, String)>>();
    extra_headers.sort_by(|left, right| left.1.cmp(&right.1));

    let mut values = Vec::new();
    for (name, suffix) in extra_headers {
        if suffix.is_empty() {
            return Err(AuthError::invalid_request(
                "Impersonate-Extra header name must not be empty",
            ));
        }
        let decoded = urlencoding::decode(&suffix)
            .map_err(|_| {
                AuthError::invalid_request(format!(
                    "invalid Impersonate-Extra header name: {suffix}"
                ))
            })?
            .into_owned();
        for value in headers.get_all(&name).iter() {
            let value = value.to_str().map_err(|_| {
                AuthError::invalid_request(format!(
                    "{} contains invalid header value",
                    name.as_str()
                ))
            })?;
            if value.is_empty() {
                return Err(AuthError::invalid_request(
                    "Impersonate-Extra value must not be empty",
                ));
            }
            values.push((decoded.clone(), value.to_string()));
        }
    }
    Ok(values)
}

fn header_suffix_ignore_ascii_case<'a>(name: &'a str, prefix: &str) -> Option<&'a str> {
    name.get(..prefix.len())
        .is_some_and(|actual| actual.eq_ignore_ascii_case(prefix))
        .then(|| &name[prefix.len()..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn other_impersonation_headers_require_user() {
        let mut headers = HeaderMap::new();
        headers.append(IMPERSONATE_GROUP, "developers".parse().unwrap());

        let error = parse(&headers).expect_err("missing user must fail");

        assert!(matches!(error, AuthError::InvalidRequest(_)));
    }

    #[test]
    fn parses_all_supported_impersonation_values() {
        let mut headers = HeaderMap::new();
        headers.insert(IMPERSONATE_USER, "alice".parse().unwrap());
        headers.append(IMPERSONATE_GROUP, "developers".parse().unwrap());
        headers.insert(IMPERSONATE_UID, "uid-a".parse().unwrap());
        headers.append("impersonate-extra-scope", "view".parse().unwrap());

        let request = parse(&headers)
            .expect("valid headers")
            .expect("impersonation requested");

        assert_eq!(request.username, "alice");
        assert_eq!(request.groups, ["developers"]);
        assert_eq!(request.uid.as_deref(), Some("uid-a"));
        assert_eq!(request.extra, [("scope".to_string(), "view".to_string())]);
    }
}
