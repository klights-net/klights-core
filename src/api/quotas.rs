use crate::api::AppError;
use serde_json::Value;

fn kind_to_quota_info(kind: &str) -> Option<(&'static str, &'static str, &'static str)> {
    match kind {
        "Pod" => Some(("pods", "", "pods")),
        "Secret" => Some(("secrets", "", "secrets")),
        "ConfigMap" => Some(("configmaps", "", "configmaps")),
        "PersistentVolumeClaim" => Some(("persistentvolumeclaims", "", "persistentvolumeclaims")),
        "Service" => Some(("services", "", "services")),
        "ReplicationController" => Some(("replicationcontrollers", "", "replicationcontrollers")),
        "ResourceQuota" => Some(("resourcequotas", "", "resourcequotas")),
        "Endpoints" => Some(("endpoints", "", "endpoints")),
        "ServiceAccount" => Some(("serviceaccounts", "", "serviceaccounts")),
        "Deployment" => Some(("", "apps", "deployments")),
        "ReplicaSet" => Some(("", "apps", "replicasets")),
        "StatefulSet" => Some(("", "apps", "statefulsets")),
        "DaemonSet" => Some(("", "apps", "daemonsets")),
        "Job" => Some(("", "batch", "jobs")),
        "CronJob" => Some(("", "batch", "cronjobs")),
        "Ingress" => Some(("", "networking.k8s.io", "ingresses")),
        "NetworkPolicy" => Some(("", "networking.k8s.io", "networkpolicies")),
        _ => None,
    }
}

fn pod_quota_bucket_and_resource(quota_key: &str) -> Option<(&'static str, &str)> {
    if let Some(suffix) = quota_key.strip_prefix("requests.") {
        Some(("requests", suffix))
    } else if let Some(suffix) = quota_key.strip_prefix("limits.") {
        Some(("limits", suffix))
    } else if quota_key == "cpu" {
        Some(("requests", "cpu"))
    } else if quota_key == "memory" {
        Some(("requests", "memory"))
    } else if quota_key == "ephemeral-storage" {
        Some(("requests", "ephemeral-storage"))
    } else {
        None
    }
}

pub async fn check_resource_quota_for_pod_update(
    db: &dyn crate::datastore::DatastoreBackend,
    namespace: &str,
    old_pod: &Value,
    new_pod: &Value,
) -> Result<(), AppError> {
    let rq_list = match db
        .list_resources(
            "v1",
            "ResourceQuota",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
    {
        Ok(list) => list,
        Err(_) => return Ok(()),
    };

    for rq_resource in rq_list.items {
        let hard = match rq_resource
            .data
            .pointer("/spec/hard")
            .and_then(|h| h.as_object())
        {
            Some(h) => h.clone(),
            None => continue,
        };
        let scopes: Vec<&str> = rq_resource
            .data
            .pointer("/spec/scopes")
            .and_then(|s| s.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();
        let old_matches_scope = scopes.is_empty()
            || crate::controllers::resource_quota::pod_matches_scopes(old_pod, &scopes);
        let new_matches_scope = scopes.is_empty()
            || crate::controllers::resource_quota::pod_matches_scopes(new_pod, &scopes);
        let used_map = rq_resource
            .data
            .pointer("/status/used")
            .and_then(|u| u.as_object());

        for (quota_key, limit_value) in &hard {
            let Some((bucket, resource_key)) = pod_quota_bucket_and_resource(quota_key) else {
                continue;
            };
            let Some(limit_raw) = limit_value.as_str() else {
                continue;
            };
            let Some(limit) = crate::controllers::resource_quota::parse_resource_quantity(
                resource_key,
                limit_raw,
            ) else {
                continue;
            };

            let old_usage = if old_matches_scope {
                crate::controllers::resource_quota::calculate_pod_effective_resource_for_key(
                    old_pod,
                    bucket,
                    resource_key,
                )
            } else {
                0
            };
            let new_usage = if new_matches_scope {
                crate::controllers::resource_quota::calculate_pod_effective_resource_for_key(
                    new_pod,
                    bucket,
                    resource_key,
                )
            } else {
                0
            };
            if old_usage == 0 && new_usage == 0 {
                continue;
            }

            let used_raw = used_map
                .and_then(|map| map.get(quota_key))
                .and_then(|v| v.as_str())
                .unwrap_or("0");
            let used =
                crate::controllers::resource_quota::parse_resource_quantity(resource_key, used_raw)
                    .unwrap_or(0);
            let adjusted_used = used.saturating_sub(old_usage).saturating_add(new_usage);
            if adjusted_used > limit {
                let requested_delta = new_usage.saturating_sub(old_usage);
                let requested_fmt = crate::controllers::resource_quota::format_resource_quantity(
                    resource_key,
                    requested_delta.max(0),
                );
                let used_fmt = crate::controllers::resource_quota::format_resource_quantity(
                    resource_key,
                    used.saturating_sub(old_usage),
                );
                let limit_fmt = crate::controllers::resource_quota::format_resource_quantity(
                    resource_key,
                    limit,
                );
                return Err(AppError::Forbidden(format!(
                    "exceeded quota: {}, requested: {}, used: {}, limited: {}",
                    quota_key, requested_fmt, used_fmt, limit_fmt
                )));
            }
        }
    }
    Ok(())
}

async fn count_nodeport_allocating_services(
    db: &dyn crate::datastore::DatastoreBackend,
    namespace: &str,
) -> i64 {
    db.list_resources(
        "v1",
        "Service",
        Some(namespace),
        crate::datastore::ResourceListQuery::all(),
    )
    .await
    .map(|list| {
        list.items
            .iter()
            .filter(|s| {
                matches!(
                    s.data.pointer("/spec/type").and_then(|t| t.as_str()),
                    Some("NodePort") | Some("LoadBalancer")
                )
            })
            .count() as i64
    })
    .unwrap_or(0)
}

async fn count_services_of_type(
    db: &dyn crate::datastore::DatastoreBackend,
    namespace: &str,
    svc_type: &str,
) -> i64 {
    db.list_resources(
        "v1",
        "Service",
        Some(namespace),
        crate::datastore::ResourceListQuery::all(),
    )
    .await
    .map(|list| {
        list.items
            .iter()
            .filter(|s| s.data.pointer("/spec/type").and_then(|t| t.as_str()) == Some(svc_type))
            .count() as i64
    })
    .unwrap_or(0)
}

pub async fn check_resource_quota_for_creation(
    db: &dyn crate::datastore::DatastoreBackend,
    namespace: &str,
    kind: &str,
    body: &Value,
) -> Result<(), AppError> {
    let rq_list = match db
        .list_resources(
            "v1",
            "ResourceQuota",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
    {
        Ok(list) => list,
        Err(_) => return Ok(()),
    };

    for rq_resource in rq_list.items {
        let hard = match rq_resource
            .data
            .pointer("/spec/hard")
            .and_then(|h| h.as_object())
        {
            Some(h) => h.clone(),
            None => continue,
        };

        let scopes: Vec<&str> = rq_resource
            .data
            .pointer("/spec/scopes")
            .and_then(|s| s.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();

        if kind == "Pod" && !scopes.is_empty() {
            if !crate::controllers::resource_quota::pod_matches_scopes(body, &scopes) {
                continue;
            }
        } else if kind != "Pod" && !scopes.is_empty() {
            let has_pod_scopes = scopes.iter().any(|s| {
                matches!(
                    *s,
                    "BestEffort" | "NotBestEffort" | "Terminating" | "NotTerminating"
                )
            });
            if has_pod_scopes {
                continue;
            }
        }

        if let Some((direct_name, group, plural)) = kind_to_quota_info(kind) {
            if !direct_name.is_empty()
                && let Some(limit_str) = hard.get(direct_name)
            {
                let limit: i64 = limit_str
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(i64::MAX);
                let current_count = db
                    .list_resources(
                        "v1",
                        kind,
                        Some(namespace),
                        crate::datastore::ResourceListQuery::all(),
                    )
                    .await
                    .map(|list| list.items.len() as i64)
                    .unwrap_or(0);
                if current_count >= limit {
                    return Err(AppError::Forbidden(format!(
                        "exceeded quota: {}, requested: 1, used: {}, limited: {}",
                        direct_name, current_count, limit
                    )));
                }
            }

            let count_key = if group.is_empty() {
                format!("count/{}", plural)
            } else {
                format!("count/{}.{}", plural, group)
            };
            if let Some(limit_str) = hard.get(&count_key) {
                let limit: i64 = limit_str
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(i64::MAX);
                let api_version = if group.is_empty() {
                    "v1".to_string()
                } else {
                    format!("{}/v1", group)
                };
                let current_count = db
                    .list_resources(
                        &api_version,
                        kind,
                        Some(namespace),
                        crate::datastore::ResourceListQuery::all(),
                    )
                    .await
                    .map(|list| list.items.len() as i64)
                    .unwrap_or(0);
                if current_count >= limit {
                    return Err(AppError::Forbidden(format!(
                        "exceeded quota: {}, requested: 1, used: {}, limited: {}",
                        count_key, current_count, limit
                    )));
                }
            }
        }

        if kind == "Pod" {
            let used_map = rq_resource
                .data
                .pointer("/status/used")
                .and_then(|u| u.as_object());
            for (quota_key, limit_value) in &hard {
                let Some((bucket, resource_key)) = pod_quota_bucket_and_resource(quota_key) else {
                    continue;
                };
                let Some(limit_raw) = limit_value.as_str() else {
                    continue;
                };
                let Some(limit) = crate::controllers::resource_quota::parse_resource_quantity(
                    resource_key,
                    limit_raw,
                ) else {
                    continue;
                };
                let requested =
                    crate::controllers::resource_quota::calculate_pod_effective_resource_for_key(
                        body,
                        bucket,
                        resource_key,
                    );
                if requested <= 0 {
                    continue;
                }

                let used_raw = used_map
                    .and_then(|map| map.get(quota_key))
                    .and_then(|v| v.as_str())
                    .unwrap_or("0");
                let used = crate::controllers::resource_quota::parse_resource_quantity(
                    resource_key,
                    used_raw,
                )
                .unwrap_or(0);
                if used + requested > limit {
                    let requested_fmt =
                        crate::controllers::resource_quota::format_resource_quantity(
                            resource_key,
                            requested,
                        );
                    let used_fmt = crate::controllers::resource_quota::format_resource_quantity(
                        resource_key,
                        used,
                    );
                    let limit_fmt = crate::controllers::resource_quota::format_resource_quantity(
                        resource_key,
                        limit,
                    );
                    return Err(AppError::Forbidden(format!(
                        "exceeded quota: {}, requested: {}, used: {}, limited: {}",
                        quota_key, requested_fmt, used_fmt, limit_fmt
                    )));
                }
            }
        }

        if kind == "Service" {
            let svc_type = body
                .pointer("/spec/type")
                .and_then(|t| t.as_str())
                .unwrap_or("ClusterIP");
            if matches!(svc_type, "NodePort" | "LoadBalancer")
                && let Some(limit_str) = hard.get("services.nodeports")
            {
                let limit: i64 = limit_str
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(i64::MAX);
                let current_count = count_nodeport_allocating_services(db, namespace).await;
                if current_count >= limit {
                    return Err(AppError::Forbidden(format!(
                        "exceeded quota: services.nodeports, requested: 1, used: {}, limited: {}",
                        current_count, limit
                    )));
                }
            }
            if svc_type == "LoadBalancer"
                && let Some(limit_str) = hard.get("services.loadbalancers")
            {
                let limit: i64 = limit_str
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(i64::MAX);
                let current_count = count_services_of_type(db, namespace, "LoadBalancer").await;
                if current_count >= limit {
                    return Err(AppError::Forbidden(format!(
                        "exceeded quota: services.loadbalancers, requested: 1, used: {}, limited: {}",
                        current_count, limit
                    )));
                }
            }
        }
    }

    Ok(())
}

pub async fn check_service_type_quota(
    db: &dyn crate::datastore::DatastoreBackend,
    namespace: &str,
    service_body: &Value,
) -> Result<(), AppError> {
    let service_type = service_body
        .pointer("/spec/type")
        .and_then(|t| t.as_str())
        .unwrap_or("ClusterIP");
    let is_nodeport = service_type == "NodePort";
    let is_loadbalancer = service_type == "LoadBalancer";
    if !is_nodeport && !is_loadbalancer {
        return Ok(());
    }

    let rq_list = db
        .list_resources(
            "v1",
            "ResourceQuota",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap_or_else(|_| crate::datastore::ResourceList {
            items: vec![],
            resource_version: 0,
            continue_token: None,
            remaining_item_count: None,
        });

    for rq_resource in rq_list.items {
        let hard = match rq_resource
            .data
            .pointer("/spec/hard")
            .and_then(|h| h.as_object())
        {
            Some(h) => h.clone(),
            None => continue,
        };

        if (is_nodeport || is_loadbalancer) && hard.contains_key("services.nodeports") {
            let limit: i64 = hard
                .get("services.nodeports")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(i64::MAX);
            let all_svcs = db
                .list_resources(
                    "v1",
                    "Service",
                    Some(namespace),
                    crate::datastore::ResourceListQuery::all(),
                )
                .await
                .unwrap_or_else(|_| crate::datastore::ResourceList {
                    items: vec![],
                    resource_version: 0,
                    continue_token: None,
                    remaining_item_count: None,
                });
            let used = all_svcs
                .items
                .iter()
                .filter(|s| {
                    matches!(
                        s.data.pointer("/spec/type").and_then(|t| t.as_str()),
                        Some("NodePort") | Some("LoadBalancer")
                    )
                })
                .count() as i64;
            if used >= limit {
                return Err(AppError::Forbidden(format!(
                    "exceeded quota: services.nodeports, used: {used}, limited: {limit}"
                )));
            }
        }

        if is_loadbalancer && hard.contains_key("services.loadbalancers") {
            let limit: i64 = hard
                .get("services.loadbalancers")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(i64::MAX);
            let all_svcs = db
                .list_resources(
                    "v1",
                    "Service",
                    Some(namespace),
                    crate::datastore::ResourceListQuery::all(),
                )
                .await
                .unwrap_or_else(|_| crate::datastore::ResourceList {
                    items: vec![],
                    resource_version: 0,
                    continue_token: None,
                    remaining_item_count: None,
                });
            let used = all_svcs
                .items
                .iter()
                .filter(|s| {
                    s.data.pointer("/spec/type").and_then(|t| t.as_str()) == Some("LoadBalancer")
                })
                .count() as i64;
            if used >= limit {
                return Err(AppError::Forbidden(format!(
                    "exceeded quota: services.loadbalancers, used: {used}, limited: {limit}"
                )));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kind_to_quota_info_returns_expected_tuples_for_known_kinds() {
        assert_eq!(kind_to_quota_info("Pod"), Some(("pods", "", "pods")));
        assert_eq!(
            kind_to_quota_info("Deployment"),
            Some(("", "apps", "deployments"))
        );
        assert_eq!(kind_to_quota_info("Job"), Some(("", "batch", "jobs")));
        assert_eq!(
            kind_to_quota_info("Ingress"),
            Some(("", "networking.k8s.io", "ingresses"))
        );
    }

    #[test]
    fn test_kind_to_quota_info_returns_none_for_unknown_kinds() {
        assert_eq!(kind_to_quota_info("Unknown"), None);
        assert_eq!(kind_to_quota_info(""), None);
        assert_eq!(kind_to_quota_info("pod"), None, "lowercase must not match");
    }

    #[test]
    fn test_pod_quota_bucket_and_resource_strips_requests_and_limits_prefixes() {
        assert_eq!(
            pod_quota_bucket_and_resource("requests.cpu"),
            Some(("requests", "cpu"))
        );
        assert_eq!(
            pod_quota_bucket_and_resource("limits.memory"),
            Some(("limits", "memory"))
        );
        assert_eq!(
            pod_quota_bucket_and_resource("requests.ephemeral-storage"),
            Some(("requests", "ephemeral-storage"))
        );
    }

    #[test]
    fn test_pod_quota_bucket_and_resource_implicit_keys_default_to_requests() {
        assert_eq!(
            pod_quota_bucket_and_resource("cpu"),
            Some(("requests", "cpu"))
        );
        assert_eq!(
            pod_quota_bucket_and_resource("memory"),
            Some(("requests", "memory"))
        );
        assert_eq!(
            pod_quota_bucket_and_resource("ephemeral-storage"),
            Some(("requests", "ephemeral-storage"))
        );
    }

    #[test]
    fn test_pod_quota_bucket_and_resource_returns_none_for_unknown_keys() {
        assert_eq!(pod_quota_bucket_and_resource("pods"), None);
        assert_eq!(pod_quota_bucket_and_resource(""), None);
        assert_eq!(
            pod_quota_bucket_and_resource("requests."),
            Some(("requests", ""))
        );
    }
}
