// API discovery and OpenAPI endpoints
use super::*;

pub use axum::{
    Json,
    body::Body,
    extract::{OriginalUri, Path, State},
    http::{HeaderMap, Method},
    response::{IntoResponse, Response},
};
pub use serde::ser::{SerializeSeq, SerializeStruct};
pub use serde::{Serialize, Serializer};
pub use serde_json::Value;
pub use std::sync::Arc;

pub use crate::api::{AppError, AppState};
pub use crate::datastore::DatastoreBackend;

/// Compute the `storageVersionHash` advertised in discovery for a built-in
/// kind. Upstream emits a base64-encoded hash that clients use only to detect
/// when a resource's storage version changes (discovery-cache invalidation);
/// they never validate it against a canonical value. A stable per-kind hash
/// therefore satisfies the contract. Returns base64 of the first 8 bytes of
/// SHA-256(kind), matching upstream's 8-byte hash width.
pub fn storage_version_hash_for(kind: &str) -> String {
    use base64::Engine;
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(kind.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(&digest[..8])
}

pub async fn apiservice_group_versions(
    state: &AppState,
) -> Result<std::collections::HashMap<String, std::collections::BTreeSet<String>>, AppError> {
    let mut groups: std::collections::HashMap<String, std::collections::BTreeSet<String>> =
        std::collections::HashMap::new();
    let list = state
        .db
        .list_resources(
            "apiregistration.k8s.io/v1",
            "APIService",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;
    for item in list.items {
        let Some(spec) = item.data.get("spec").and_then(|v| v.as_object()) else {
            continue;
        };
        let Some(group) = spec.get("group").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(version) = spec.get("version").and_then(|v| v.as_str()) else {
            continue;
        };
        if group.is_empty() || version.is_empty() {
            continue;
        }
        groups
            .entry(group.to_string())
            .or_default()
            .insert(version.to_string());
    }
    Ok(groups)
}

pub async fn apiservice_discovery_resources(
    state: &Arc<AppState>,
    group: &str,
    version: &str,
) -> Vec<APIResourceDiscovery> {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::ACCEPT,
        "application/json".parse().unwrap(),
    );

    let path = format!("/apis/{group}/{version}");
    // Use a system:authenticated identity for APIService discovery
    // proxying. Backends typically allow authenticated users to read
    // the discovery document. The aggregator's own admin client cert
    // is used for the mTLS connection.
    let discovery_identity = crate::auth::identity::AuthenticatedIdentity::client_cert(
        "system:apiserver".to_string(),
        vec!["system:authenticated".to_string()],
    );
    let response = match crate::api::proxy_apiservice_request(
        state,
        group,
        version,
        Method::GET,
        &path,
        axum::body::Bytes::new(),
        Some(&headers),
        &discovery_identity,
    )
    .await
    {
        Ok(resp) => resp,
        Err(err) => {
            tracing::warn!(
                "Failed to proxy APIService discovery for {}/{}: {:?}",
                group,
                version,
                err
            );
            return Vec::new();
        }
    };

    let Some(response) = response else {
        return Vec::new();
    };
    if !response.status().is_success() {
        tracing::warn!(
            "APIService discovery for {}/{} returned {}",
            group,
            version,
            response.status()
        );
        return Vec::new();
    }

    let payload = match axum::body::to_bytes(
        response.into_body(),
        crate::api_pod_subresources::MAX_PROXY_RESPONSE_BODY_BYTES,
    )
    .await
    {
        Ok(bytes) => bytes,
        Err(err) => {
            tracing::warn!(
                "Failed reading APIService discovery response body for {}/{}: {}",
                group,
                version,
                err
            );
            return Vec::new();
        }
    };

    let value: Value = match serde_json::from_slice(&payload) {
        Ok(v) => v,
        Err(err) => {
            tracing::warn!(
                "Failed parsing APIService discovery response as JSON for {}/{}: {}",
                group,
                version,
                err
            );
            return Vec::new();
        }
    };

    let mut resources = Vec::new();
    for resource in value
        .get("resources")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
    {
        let Some(name) = resource.get("name").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(kind) = resource.get("kind").and_then(|v| v.as_str()) else {
            continue;
        };
        let namespaced = resource
            .get("namespaced")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let singular_resource = resource
            .get("singularName")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let verbs: Vec<String> = resource
            .get("verbs")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|verb| verb.as_str().map(ToString::to_string))
                    .collect()
            })
            .unwrap_or_default();

        resources.push(APIResourceDiscovery {
            resource: name.to_string(),
            response_kind: APIResourceResponseKind {
                kind: kind.to_string(),
            },
            scope: if namespaced {
                "Namespaced".to_string()
            } else {
                "Cluster".to_string()
            },
            singular_resource,
            verbs,
            ..Default::default()
        });
    }

    resources
}

#[derive(Serialize)]
pub struct APIVersions {
    pub kind: String,
    pub versions: Vec<String>,
    #[serde(rename = "serverAddressByClientCIDRs")]
    pub server_address_by_client_cidrs: Vec<ServerAddressByClientCIDR>,
}

#[derive(Serialize)]
pub struct ServerAddressByClientCIDR {
    #[serde(rename = "clientCIDR")]
    pub client_cidr: String,
    #[serde(rename = "serverAddress")]
    pub server_address: String,
}
