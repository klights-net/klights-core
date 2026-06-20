use crate::api::AppError;
use crate::api::apiservice_proxy::resolve_service_proxy_target;
use crate::datastore::{CatchUpResource, DatastoreBackend, Resource};
use crate::watch::{EventType, WatchEvent};
use serde_json::Value;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

pub fn build_crd_conversion_webhook_url(client_config: &Value) -> Result<String, AppError> {
    if let Some(url) = client_config.get("url").and_then(|u| u.as_str()) {
        return Ok(url.to_string());
    }
    Err(AppError::BadRequest(
        "CRD conversion webhook clientConfig.url is required".to_string(),
    ))
}

pub fn build_crd_conversion_webhook_client(
    client_config: &Value,
    dns_override: Option<(&str, SocketAddr)>,
) -> Result<reqwest::Client, AppError> {
    let mut builder = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy();
    if let Some(ca_bundle) = client_config.get("caBundle").and_then(|v| v.as_str()) {
        use base64::Engine;
        let der = base64::engine::general_purpose::STANDARD
            .decode(ca_bundle)
            .map_err(|e| {
                AppError::BadRequest(format!(
                    "Invalid CRD conversion webhook clientConfig.caBundle base64: {e}"
                ))
            })?;
        let cert = reqwest::Certificate::from_der(&der)
            .or_else(|_| reqwest::Certificate::from_pem(&der))
            .map_err(|e| {
                AppError::BadRequest(format!(
                    "Invalid CRD conversion webhook clientConfig.caBundle cert: {e}"
                ))
            })?;
        builder = builder.add_root_certificate(cert);
    }
    if let Some((host, addr)) = dns_override {
        builder = builder.resolve(host, addr);
    }
    builder.build().map_err(|e| {
        AppError::Internal(format!(
            "Failed to build CRD conversion webhook HTTP client: {e}"
        ))
    })
}

#[derive(Clone, Debug)]
pub struct CrdConversionConfig {
    pub storage_version: String,
    pub served_versions: Vec<String>,
    pub strategy: Option<String>,
    pub webhook_client_config: Option<Value>,
    pub webhook_review_versions: Vec<String>,
}

pub async fn load_crd_conversion_config(
    db: &dyn DatastoreBackend,
    group: &str,
    plural: &str,
) -> Result<Option<CrdConversionConfig>, AppError> {
    let crd_name = format!("{plural}.{group}");
    let Some(crd) = db
        .get_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            &crd_name,
        )
        .await?
    else {
        return Ok(None);
    };

    let versions = crd
        .data
        .pointer("/spec/versions")
        .and_then(|v| v.as_array())
        .ok_or_else(|| AppError::Internal("CRD spec.versions must be an array".to_string()))?;

    let mut served_versions = Vec::new();
    let mut storage_version = None::<String>;
    for version in versions {
        let Some(name) = version.get("name").and_then(|v| v.as_str()) else {
            continue;
        };
        let served = version
            .get("served")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let storage = version
            .get("storage")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if served {
            served_versions.push(name.to_string());
        }
        if storage {
            storage_version = Some(name.to_string());
        }
    }

    if served_versions.is_empty() {
        return Ok(None);
    }

    let storage_version = storage_version.unwrap_or_else(|| served_versions[0].clone());
    let strategy = crd
        .data
        .pointer("/spec/conversion/strategy")
        .and_then(|v| v.as_str())
        .map(ToString::to_string);
    let webhook_client_config = crd
        .data
        .pointer("/spec/conversion/webhook/clientConfig")
        .cloned();
    let webhook_review_versions = crd
        .data
        .pointer("/spec/conversion/webhook/conversionReviewVersions")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(ToString::to_string))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    Ok(Some(CrdConversionConfig {
        storage_version,
        served_versions,
        strategy,
        webhook_client_config,
        webhook_review_versions,
    }))
}

pub async fn convert_crd_objects_to_requested_version(
    db: &dyn DatastoreBackend,
    conversion: &CrdConversionConfig,
    group: &str,
    plural: &str,
    desired_api_version: &str,
    objects: Vec<Value>,
) -> Result<Vec<Value>, AppError> {
    if objects.is_empty() {
        return Ok(objects);
    }

    let strategy_is_webhook = conversion
        .strategy
        .as_deref()
        .is_some_and(|strategy| strategy.eq_ignore_ascii_case("Webhook"));
    let Some(client_config) = conversion.webhook_client_config.as_ref() else {
        if strategy_is_webhook {
            return Err(AppError::BadRequest(format!(
                "CRD {plural}.{group} conversion strategy is Webhook but webhook.clientConfig is missing"
            )));
        }
        return Ok(stamp_crd_objects_api_version(objects, desired_api_version));
    };
    if !strategy_is_webhook {
        return Ok(stamp_crd_objects_api_version(objects, desired_api_version));
    }

    let mut ordered_results: Vec<Option<Value>> = vec![None; objects.len()];
    let mut convert_indices: Vec<usize> = Vec::new();
    let mut convert_objects: Vec<Value> = Vec::new();
    for (idx, object) in objects.into_iter().enumerate() {
        let is_already_desired = object
            .get("apiVersion")
            .and_then(|v| v.as_str())
            .is_some_and(|api_version| api_version == desired_api_version);
        if is_already_desired {
            ordered_results[idx] = Some(object);
        } else {
            convert_indices.push(idx);
            convert_objects.push(object);
        }
    }
    if convert_objects.is_empty() {
        let mut passthrough = Vec::with_capacity(ordered_results.len());
        for object in ordered_results.into_iter().flatten() {
            passthrough.push(object);
        }
        return Ok(passthrough);
    }

    let review_version = if conversion.webhook_review_versions.iter().any(|v| v == "v1") {
        "v1".to_string()
    } else {
        conversion
            .webhook_review_versions
            .first()
            .cloned()
            .unwrap_or_else(|| "v1".to_string())
    };
    let conversion_api_version = format!("apiextensions.k8s.io/{review_version}");
    let (webhook_url, webhook_client) = match build_crd_conversion_webhook_url(client_config) {
        Ok(url) => (
            url,
            build_crd_conversion_webhook_client(client_config, None)?,
        ),
        Err(_) => {
            let service = client_config
                .get("service")
                .and_then(|s| s.as_object())
                .ok_or_else(|| {
                    AppError::BadRequest(format!(
                        "CRD {plural}.{group} conversion webhook clientConfig must set url or service"
                    ))
                })?;
            let service_name = service
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    AppError::BadRequest(
                        "CRD conversion webhook service.name is required".to_string(),
                    )
                })?;
            let service_namespace = service
                .get("namespace")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    AppError::BadRequest(
                        "CRD conversion webhook service.namespace is required".to_string(),
                    )
                })?;
            let desired_port = service
                .get("port")
                .and_then(|v| v.as_u64())
                .and_then(|v| u16::try_from(v).ok())
                .unwrap_or(443);
            let service_target =
                resolve_service_proxy_target(db, service_namespace, service_name, desired_port)
                    .await?;
            let path = service
                .get("path")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            (
                format!(
                    "https://{}:{}{}",
                    service_target.host,
                    service_target.endpoint_addr.port(),
                    path
                ),
                build_crd_conversion_webhook_client(
                    client_config,
                    Some((&service_target.host, service_target.endpoint_addr)),
                )?,
            )
        }
    };

    let req_body = serde_json::json!({
        "apiVersion": conversion_api_version,
        "kind": "ConversionReview",
        "request": {
            "uid": uuid::Uuid::new_v4().to_string(),
            "desiredAPIVersion": desired_api_version,
            "objects": convert_objects,
        }
    });

    let resp = webhook_client
        .post(&webhook_url)
        .timeout(Duration::from_secs(10))
        .json(&req_body)
        .send()
        .await
        .map_err(|e| {
            AppError::InternalError(format!(
                "CRD conversion webhook call failed for {plural}.{group}: {e}"
            ))
        })?;

    if !resp.status().is_success() {
        return Err(AppError::InternalError(format!(
            "CRD conversion webhook returned {} for {plural}.{group}",
            resp.status()
        )));
    }

    let status = resp.status();
    // Bound the conversion-webhook response to avoid unbounded memory growth
    // from a malicious or buggy webhook (DoS).
    let response_bytes = crate::api_pod_subresources::read_reqwest_body_limited(
        resp,
        crate::api_pod_subresources::MAX_APISERVICE_RESPONSE_BODY_BYTES,
        "CRD conversion webhook",
    )
    .await?;
    let review: Value = match serde_json::from_slice(&response_bytes) {
        Ok(review) => review,
        Err(json_err) => match serde_yaml::from_slice(&response_bytes) {
            Ok(review) => review,
            Err(_yaml_err) => {
                let preview_len = response_bytes.len().min(200);
                let preview = String::from_utf8_lossy(&response_bytes[..preview_len]);
                return Err(AppError::InternalError(format!(
                    "CRD conversion webhook returned invalid JSON for {plural}.{group}: {json_err}; status={status}; body_prefix={preview:?}"
                )));
            }
        },
    };
    let status = review
        .pointer("/response/result/status")
        .and_then(|v| v.as_str())
        .unwrap_or("Success");
    if status != "Success" {
        let message = review
            .pointer("/response/result/message")
            .and_then(|v| v.as_str())
            .unwrap_or("conversion webhook rejected request");
        return Err(AppError::InternalError(format!(
            "CRD conversion webhook failed for {plural}.{group}: {message}"
        )));
    }
    let converted_objects = review
        .pointer("/response/convertedObjects")
        .and_then(|v| v.as_array())
        .cloned()
        .ok_or_else(|| {
            AppError::InternalError(format!(
                "CRD conversion webhook response missing convertedObjects for {plural}.{group}"
            ))
        })?;
    if converted_objects.len() != convert_indices.len() {
        return Err(AppError::InternalError(format!(
            "CRD conversion webhook returned {} converted objects for {plural}.{group}, expected {}",
            converted_objects.len(),
            convert_indices.len()
        )));
    }

    for (idx, mut converted) in convert_indices.into_iter().zip(converted_objects) {
        if converted.get("apiVersion").is_none() {
            converted["apiVersion"] = Value::String(desired_api_version.to_string());
        }
        ordered_results[idx] = Some(converted);
    }
    let mut result = Vec::with_capacity(ordered_results.len());
    for maybe_object in ordered_results {
        let object = maybe_object.ok_or_else(|| {
            AppError::InternalError(format!(
                "CRD conversion pipeline lost an object for {plural}.{group}"
            ))
        })?;
        result.push(object);
    }
    Ok(result)
}

fn stamp_crd_objects_api_version(objects: Vec<Value>, desired_api_version: &str) -> Vec<Value> {
    objects
        .into_iter()
        .map(|mut object| {
            if object.is_object() {
                object["apiVersion"] = Value::String(desired_api_version.to_string());
            }
            object
        })
        .collect()
}

pub async fn gather_custom_resources_across_served_versions(
    db: &dyn DatastoreBackend,
    conversion: &CrdConversionConfig,
    group: &str,
    kind: &str,
    namespace: Option<String>,
    label_selector: Option<String>,
) -> Result<(Vec<Resource>, i64), AppError> {
    let mut safe_snapshot_rv = None::<i64>;
    let mut merged: std::collections::HashMap<(Option<String>, String), Resource> =
        std::collections::HashMap::new();

    let mut version_order = Vec::with_capacity(conversion.served_versions.len());
    if conversion
        .served_versions
        .iter()
        .any(|v| v == &conversion.storage_version)
    {
        version_order.push(conversion.storage_version.clone());
    }
    for served in &conversion.served_versions {
        if served != &conversion.storage_version {
            version_order.push(served.clone());
        }
    }

    for served_version in &version_order {
        let api_version = format!("{group}/{served_version}");
        let list = db
            .list_resources(
                &api_version,
                kind,
                namespace.clone().as_deref(),
                crate::datastore::ResourceListQuery::new(
                    label_selector.clone().as_deref(),
                    None,
                    None,
                    None,
                ),
            )
            .await?;
        safe_snapshot_rv = Some(
            safe_snapshot_rv
                .map(|rv| rv.min(list.resource_version))
                .unwrap_or(list.resource_version),
        );
        for item in list.items {
            let key = (item.namespace.clone(), item.name.clone());
            match merged.get(&key) {
                Some(existing) if existing.resource_version >= item.resource_version => {}
                _ => {
                    merged.insert(key, item);
                }
            }
        }
    }

    // The merged conversion view is assembled from multiple live LIST snapshots.
    // It is only watch-resume safe through the earliest component snapshot: an
    // object committed after the storage-version read but before a later served
    // version read must be omitted here and replayed by the follow-up watch.
    let safe_snapshot_rv = safe_snapshot_rv.unwrap_or(0);
    let mut items: Vec<Resource> = merged
        .into_values()
        .filter(|item| item.resource_version <= safe_snapshot_rv)
        .collect();
    items.sort_by(|a, b| a.name.cmp(&b.name));
    Ok((items, safe_snapshot_rv))
}

pub async fn gather_custom_resource_events_across_served_versions(
    db: &dyn DatastoreBackend,
    conversion: &CrdConversionConfig,
    group: &str,
    kind: &str,
    namespace: Option<String>,
    since_rv: i64,
) -> Result<Vec<CatchUpResource>, AppError> {
    let mut version_order = Vec::with_capacity(conversion.served_versions.len());
    if conversion
        .served_versions
        .iter()
        .any(|v| v == &conversion.storage_version)
    {
        version_order.push(conversion.storage_version.clone());
    }
    for served in &conversion.served_versions {
        if served != &conversion.storage_version {
            version_order.push(served.clone());
        }
    }

    let mut events = Vec::new();
    for served_version in &version_order {
        let api_version = format!("{group}/{served_version}");
        let mut version_events = db
            .list_resources_modified_since(
                &api_version,
                kind,
                namespace.clone().as_deref(),
                since_rv,
            )
            .await?;
        events.append(&mut version_events);
    }
    events.sort_by_key(|event| event.resource.resource_version);
    Ok(events)
}

pub async fn convert_custom_resource_watch_event_to_requested_version(
    db: &dyn DatastoreBackend,
    conversion: Option<&CrdConversionConfig>,
    group: &str,
    plural: &str,
    requested_api_version: &str,
    mut event: WatchEvent,
) -> Result<WatchEvent, AppError> {
    if event.event_type == EventType::Bookmark {
        return Ok(event);
    }
    let Some(conversion) = conversion else {
        return Ok(event);
    };

    let already_desired =
        event.object.get("apiVersion").and_then(|v| v.as_str()) == Some(requested_api_version);
    if already_desired {
        return Ok(event);
    }

    let source_object = Arc::try_unwrap(std::mem::replace(
        &mut event.object,
        Arc::new(serde_json::Value::Null),
    ))
    .unwrap_or_else(|arc| (*arc).clone());
    let mut converted = convert_crd_objects_to_requested_version(
        db,
        conversion,
        group,
        plural,
        requested_api_version,
        vec![source_object],
    )
    .await?;
    if let Some(object) = converted.pop() {
        event.object = Arc::new(object);
    }
    Ok(event)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::sqlite::Datastore;
    use serde_json::json;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn conversion_merged_list_rv_does_not_skip_storage_create_between_version_reads() {
        let db = Datastore::new_in_memory().await.unwrap();
        let conversion = CrdConversionConfig {
            storage_version: "v1".to_string(),
            served_versions: vec!["v1".to_string(), "v2".to_string()],
            strategy: None,
            webhook_client_config: None,
            webhook_review_versions: Vec::new(),
        };

        let pause = Datastore::install_list_resources_snapshot_pause_for_test(
            "example.com/v2",
            "Widget",
            Some("default"),
            None,
            None,
            None,
            None,
        );
        let list_db = db.clone();
        let list_conversion = conversion.clone();
        let list_task = tokio::spawn(async move {
            gather_custom_resources_across_served_versions(
                &list_db,
                &list_conversion,
                "example.com",
                "Widget",
                Some("default".to_string()),
                None,
            )
            .await
            .unwrap()
        });

        pause.wait_for_hit().await;
        let storage_object = db
            .create_resource(
                "example.com/v1",
                "Widget",
                Some("default"),
                "late-storage",
                json!({
                    "apiVersion": "example.com/v1",
                    "kind": "Widget",
                    "metadata": {
                        "name": "late-storage",
                        "namespace": "default"
                    },
                    "hostPort": "host1:80"
                }),
            )
            .await
            .unwrap();
        let advanced_rv = db
            .advance_resource_version_after(storage_object.resource_version + 10)
            .await
            .unwrap();
        assert!(advanced_rv > storage_object.resource_version);
        pause.resume();

        let (items, list_rv) = list_task.await.expect("conversion list task panicked");
        assert!(
            items.is_empty(),
            "storage object created after the storage-version read should be absent from this merged list"
        );
        assert!(
            list_rv < storage_object.resource_version,
            "merged conversion list rv {list_rv} must let a follow-up watch replay omitted storage object rv {}",
            storage_object.resource_version
        );
    }
}
