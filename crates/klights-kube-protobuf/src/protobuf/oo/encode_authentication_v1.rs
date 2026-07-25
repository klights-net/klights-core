use crate::protobuf::*;
use serde::Deserialize;

pub fn json_tokenreview_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::authentication::v1::TokenReview> {
    use k8s_pb::api::authentication::v1 as pb;

    let review = k8s_openapi::api::authentication::v1::TokenReview::deserialize(value)?;
    Ok(pb::TokenReview {
        metadata: Some(json_meta_to_pb(&review.metadata)),
        spec: Some(pb::TokenReviewSpec {
            token: review.spec.token,
            audiences: review.spec.audiences.unwrap_or_default(),
        }),
        status: review.status.map(|status| pb::TokenReviewStatus {
            authenticated: status.authenticated,
            user: status.user.map(|user| pb::UserInfo {
                username: user.username,
                uid: user.uid,
                groups: user.groups.unwrap_or_default(),
                extra: user
                    .extra
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(key, items)| (key, pb::ExtraValue { items }))
                    .collect(),
            }),
            audiences: status.audiences.unwrap_or_default(),
            error: status.error,
        }),
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    #[test]
    fn tokenreview_request_and_response_round_trip() {
        for value in [
            json!({
                "apiVersion": "authentication.k8s.io/v1",
                "kind": "TokenReview",
                "metadata": {},
                "spec": {
                    "token": "opaque-token",
                    "audiences": ["https://kubernetes.default.svc"]
                }
            }),
            json!({
                "apiVersion": "authentication.k8s.io/v1",
                "kind": "TokenReview",
                "metadata": {},
                "spec": {"token": "opaque-token"},
                "status": {
                    "authenticated": true,
                    "audiences": ["https://kubernetes.default.svc"],
                    "user": {
                        "username": "alice",
                        "uid": "uid-1",
                        "groups": ["devs"],
                        "extra": {"example.io/key": ["one", "two"]}
                    }
                }
            }),
            json!({
                "apiVersion": "authentication.k8s.io/v1",
                "kind": "TokenReview",
                "metadata": {},
                "spec": {"token": "rejected-token"},
                "status": {
                    "authenticated": false,
                    "error": "token rejected"
                }
            }),
        ] {
            let encoded = crate::protobuf::encode_protobuf(&value).unwrap();
            let decoded = crate::protobuf::decode_protobuf(&encoded).unwrap();
            assert_eq!(decoded, value);
        }
    }
}
