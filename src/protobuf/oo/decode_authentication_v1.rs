use crate::protobuf::*;
pb_decode!(
    pb_tokenreview_to_json,
    k8s_pb::api::authentication::v1::TokenReview,
    tr,
    "authentication.k8s.io/v1",
    "TokenReview",
    obj,
    {
        if let Some(spec) = &tr.spec {
            let mut spec_obj = json!({});
            if let Some(token) = &spec.token {
                spec_obj["token"] = json!(token);
            }
            if !spec.audiences.is_empty() {
                spec_obj["audiences"] = json!(spec.audiences);
            }
            if spec_obj.as_object().is_some_and(|o| !o.is_empty()) {
                obj["spec"] = spec_obj;
            }
        }

        if let Some(status) = &tr.status {
            let mut status_obj = json!({});
            if let Some(authenticated) = status.authenticated {
                status_obj["authenticated"] = json!(authenticated);
            }
            if let Some(user) = &status.user {
                let mut user_obj = json!({});
                if let Some(username) = &user.username {
                    user_obj["username"] = json!(username);
                }
                if let Some(uid) = &user.uid {
                    user_obj["uid"] = json!(uid);
                }
                if !user.groups.is_empty() {
                    user_obj["groups"] = json!(user.groups);
                }
                if !user.extra.is_empty() {
                    let extra = user
                        .extra
                        .iter()
                        .map(|(k, v)| (k.clone(), json!(v.items)))
                        .collect::<serde_json::Map<String, Value>>();
                    user_obj["extra"] = Value::Object(extra);
                }
                if user_obj.as_object().is_some_and(|o| !o.is_empty()) {
                    status_obj["user"] = user_obj;
                }
            }
            if !status.audiences.is_empty() {
                status_obj["audiences"] = json!(status.audiences);
            }
            if let Some(err) = &status.error {
                status_obj["error"] = json!(err);
            }
            if status_obj.as_object().is_some_and(|o| !o.is_empty()) {
                obj["status"] = status_obj;
            }
        }
    }
);
