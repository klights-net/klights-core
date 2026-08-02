use axum::http::{HeaderMap, HeaderValue};
use base64::Engine as _;
use klights_auth::ImpersonationError;

use super::*;

#[test]
fn impersonation_header_extraction_is_api_owned_and_strict() {
    let mut headers = HeaderMap::new();
    headers.append(IMPERSONATE_GROUP, HeaderValue::from_static("developers"));
    assert!(matches!(
        parse_impersonation_headers(&headers),
        Err(ImpersonationError::InvalidRequest { .. })
    ));

    headers.insert(IMPERSONATE_USER, HeaderValue::from_static("alice"));
    headers.insert("impersonate-extra-scopes", HeaderValue::from_static("view"));
    let request = parse_impersonation_headers(&headers)
        .expect("valid headers")
        .expect("impersonation present");
    assert_eq!(request.username, "alice");
    assert_eq!(request.groups, vec!["developers"]);
    assert_eq!(
        request.extra,
        vec![("scopes".to_string(), "view".to_string())]
    );
}

#[test]
fn requestheader_identity_cannot_assert_system_masters() {
    let mut headers = HeaderMap::new();
    headers.insert("x-remote-user", HeaderValue::from_static("alice"));
    headers.append("x-remote-group", HeaderValue::from_static("developers"));
    headers.append("x-remote-group", HeaderValue::from_static("system:masters"));
    let identity = requestheader_identity_from_headers(&headers).unwrap();
    assert_eq!(identity.groups, vec!["developers"]);
}

#[test]
fn forwarded_client_cert_header_roundtrips_base64_der() {
    let der = vec![0x30, 0x82, 0x01, 0x02, 0x03];
    let encoded = base64::engine::general_purpose::STANDARD.encode(&der);
    let mut headers = HeaderMap::new();
    headers.insert(
        FORWARDED_CLIENT_CERT_HEADER,
        HeaderValue::from_str(&encoded).unwrap(),
    );
    assert_eq!(forwarded_client_cert_from_headers(&headers), Some(der));

    assert_eq!(forwarded_client_cert_from_headers(&HeaderMap::new()), None);
    headers.insert(
        FORWARDED_CLIENT_CERT_HEADER,
        HeaderValue::from_static("not base64!!!"),
    );
    assert_eq!(forwarded_client_cert_from_headers(&headers), None);
}
