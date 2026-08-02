use super::decode_json_or_proto;

#[test]
fn test_decode_json_or_proto_parses_json() {
    let body = br#"{"kind":"SubjectAccessReview"}"#;
    let val = decode_json_or_proto(body).unwrap();
    assert_eq!(val["kind"], "SubjectAccessReview");
}

#[test]
fn test_decode_json_or_proto_rejects_invalid() {
    let body = b"not json";
    assert!(decode_json_or_proto(body).is_err());
}
