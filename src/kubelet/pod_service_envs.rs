/// Generate K8s service discovery env vars for all Services in a namespace.
/// K8s injects these into every pod: {SERVICE_NAME}_SERVICE_HOST, {SERVICE_NAME}_SERVICE_PORT,
/// plus per-port vars like {SERVICE_NAME}_PORT, {SERVICE_NAME}_PORT_{PORT}_{PROTO}_*.
#[cfg(test)]
pub async fn resolve_service_envs(
    namespace: &str,
    db: &dyn crate::datastore::DatastoreBackend,
) -> Vec<(String, String)> {
    let services = match db
        .list_resources(
            "v1",
            "Service",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
    {
        Ok(list) => list.items,
        Err(_) => return Vec::new(),
    };

    service_envs_from_resources(&services)
}

pub async fn resolve_service_envs_from_source(
    namespace: &str,
    source: &dyn crate::kubelet::pod_env::EnvSourceReader,
) -> Vec<(String, String)> {
    match source.services(namespace).await {
        Ok(services) => service_envs_from_resources(&services),
        Err(_) => Vec::new(),
    }
}

fn service_envs_from_resources(services: &[crate::datastore::Resource]) -> Vec<(String, String)> {
    let mut envs = Vec::new();

    for svc in services {
        let svc_name = match svc.data.pointer("/metadata/name").and_then(|n| n.as_str()) {
            Some(n) => n,
            None => continue,
        };

        let cluster_ip = match svc.data.pointer("/spec/clusterIP").and_then(|v| v.as_str()) {
            Some(ip) if ip != "None" && !ip.is_empty() => ip,
            _ => continue, // Skip headless services
        };

        let ports = match svc.data.pointer("/spec/ports").and_then(|p| p.as_array()) {
            Some(p) => p,
            None => continue,
        };

        // Convert service name to env var prefix: uppercase, dashes to underscores
        let prefix = svc_name.to_uppercase().replace('-', "_");

        // {PREFIX}_SERVICE_HOST and {PREFIX}_SERVICE_PORT
        envs.push((format!("{}_SERVICE_HOST", prefix), cluster_ip.to_string()));

        if let Some(first_port) = ports.first() {
            let port_num = first_port.get("port").and_then(|p| p.as_u64()).unwrap_or(0);
            // Default to "TCP" — K8s spec says protocol defaults to TCP; treat empty string
            // the same as absent so env var names never get double-underscores.
            let proto = first_port
                .get("protocol")
                .and_then(|p| p.as_str())
                .filter(|s| !s.is_empty())
                .unwrap_or("TCP")
                .to_lowercase();

            envs.push((format!("{}_SERVICE_PORT", prefix), port_num.to_string()));
            envs.push((
                format!("{}_PORT", prefix),
                format!("{}://{}:{}", proto, cluster_ip, port_num),
            ));
        }

        // Per-port env vars
        for port_spec in ports {
            let port_num = port_spec.get("port").and_then(|p| p.as_u64()).unwrap_or(0);
            // Default protocol to "TCP" if absent or empty string.
            let proto = port_spec
                .get("protocol")
                .and_then(|p| p.as_str())
                .filter(|s| !s.is_empty())
                .unwrap_or("TCP");
            let proto_upper = proto.to_uppercase();
            let proto_lower = proto.to_lowercase();

            // {PREFIX}_PORT_{PORT}_{PROTO} = {proto}://{ip}:{port}
            envs.push((
                format!("{}_PORT_{}_{}", prefix, port_num, proto_upper),
                format!("{}://{}:{}", proto_lower, cluster_ip, port_num),
            ));
            // {PREFIX}_PORT_{PORT}_{PROTO}_PROTO = {PROTO}
            envs.push((
                format!("{}_PORT_{}_{}_PROTO", prefix, port_num, proto_upper),
                proto_lower.clone(),
            ));
            // {PREFIX}_PORT_{PORT}_{PROTO}_PORT = {port}
            envs.push((
                format!("{}_PORT_{}_{}_PORT", prefix, port_num, proto_upper),
                port_num.to_string(),
            ));
            // {PREFIX}_PORT_{PORT}_{PROTO}_ADDR = {ip}
            envs.push((
                format!("{}_PORT_{}_{}_ADDR", prefix, port_num, proto_upper),
                cluster_ip.to_string(),
            ));

            // Named port: {PREFIX}_SERVICE_PORT_{PORT_NAME} = {port}
            if let Some(port_name) = port_spec.get("name").and_then(|n| n.as_str())
                && !port_name.is_empty()
            {
                let port_name_upper = port_name.to_uppercase().replace('-', "_");
                envs.push((
                    format!("{}_SERVICE_PORT_{}", prefix, port_name_upper),
                    port_num.to_string(),
                ));
            }
        }
    }

    envs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_resolve_service_envs_generates_service_discovery_vars() {
        let db = crate::datastore::test_support::in_memory().await;
        // Create a Service with ClusterIP and ports
        let svc = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "my-svc", "namespace": "default"},
            "spec": {
                "clusterIP": "10.43.128.50",
                "ports": [
                    {"port": 8080, "protocol": "TCP", "name": "http"},
                    {"port": 443, "protocol": "TCP", "name": "https"}
                ]
            }
        });
        db.create_resource("v1", "Service", Some("default"), "my-svc", svc)
            .await
            .unwrap();

        let envs = resolve_service_envs("default", &db).await;
        let env_map: std::collections::HashMap<String, String> = envs.into_iter().collect();

        // {PREFIX}_SERVICE_HOST
        assert_eq!(
            env_map.get("MY_SVC_SERVICE_HOST").map(|s| s.as_str()),
            Some("10.43.128.50"),
        );
        // {PREFIX}_SERVICE_PORT (first port)
        assert_eq!(
            env_map.get("MY_SVC_SERVICE_PORT").map(|s| s.as_str()),
            Some("8080"),
        );
        // {PREFIX}_PORT
        assert_eq!(
            env_map.get("MY_SVC_PORT").map(|s| s.as_str()),
            Some("tcp://10.43.128.50:8080"),
        );
        // Per-port vars
        assert_eq!(
            env_map.get("MY_SVC_PORT_8080_TCP").map(|s| s.as_str()),
            Some("tcp://10.43.128.50:8080"),
        );
        assert_eq!(
            env_map
                .get("MY_SVC_PORT_8080_TCP_PROTO")
                .map(|s| s.as_str()),
            Some("tcp"),
        );
        assert_eq!(
            env_map.get("MY_SVC_PORT_8080_TCP_PORT").map(|s| s.as_str()),
            Some("8080"),
        );
        assert_eq!(
            env_map.get("MY_SVC_PORT_8080_TCP_ADDR").map(|s| s.as_str()),
            Some("10.43.128.50"),
        );
        // Named port: {PREFIX}_SERVICE_PORT_{NAME}
        assert_eq!(
            env_map.get("MY_SVC_SERVICE_PORT_HTTP").map(|s| s.as_str()),
            Some("8080"),
        );
        assert_eq!(
            env_map.get("MY_SVC_SERVICE_PORT_HTTPS").map(|s| s.as_str()),
            Some("443"),
        );
    }

    /// Regression for P0-E2E-20260424-10: service port with empty/absent protocol must default to TCP.
    /// Without the fix, env var names get double-underscores (FOOSERVICE_PORT_8765__ADDR instead of
    /// FOOSERVICE_PORT_8765_TCP_ADDR) because empty-string protocol produced empty proto_upper.
    #[tokio::test]
    async fn test_resolve_service_envs_empty_protocol_defaults_to_tcp() {
        let db = crate::datastore::test_support::in_memory().await;
        // Service with empty-string protocol (as K8s API may return when field is omitted)
        let svc = serde_json::json!({
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": "fooservice", "namespace": "default"},
            "spec": {
                "clusterIP": "10.43.128.205",
                "ports": [{"port": 8765, "protocol": ""}]  // empty string protocol
            }
        });
        db.create_resource("v1", "Service", Some("default"), "fooservice", svc)
            .await
            .unwrap();

        let envs = resolve_service_envs("default", &db).await;
        let env_map: std::collections::HashMap<String, String> = envs.into_iter().collect();

        // Must use TCP as default protocol — no double underscores in names
        assert_eq!(
            env_map.get("FOOSERVICE_PORT_8765_TCP").map(|s| s.as_str()),
            Some("tcp://10.43.128.205:8765"),
            "empty protocol must default to tcp://"
        );
        assert_eq!(
            env_map
                .get("FOOSERVICE_PORT_8765_TCP_PORT")
                .map(|s| s.as_str()),
            Some("8765"),
            "must have _TCP_PORT env var"
        );
        assert_eq!(
            env_map
                .get("FOOSERVICE_PORT_8765_TCP_ADDR")
                .map(|s| s.as_str()),
            Some("10.43.128.205"),
            "must have _TCP_ADDR env var"
        );
        assert_eq!(
            env_map
                .get("FOOSERVICE_PORT_8765_TCP_PROTO")
                .map(|s| s.as_str()),
            Some("tcp"),
            "must have _TCP_PROTO env var"
        );

        // Must NOT have double-underscore variants
        assert!(
            !env_map.contains_key("FOOSERVICE_PORT_8765__ADDR"),
            "must not have double-underscore"
        );
        assert!(
            !env_map.contains_key("FOOSERVICE_PORT_8765__PORT"),
            "must not have double-underscore"
        );
    }

    #[tokio::test]
    async fn test_resolve_service_envs_skips_headless_services() {
        let db = crate::datastore::test_support::in_memory().await;
        let svc = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "headless", "namespace": "default"},
            "spec": {
                "clusterIP": "None",
                "ports": [{"port": 80, "protocol": "TCP"}]
            }
        });
        db.create_resource("v1", "Service", Some("default"), "headless", svc)
            .await
            .unwrap();

        let envs = resolve_service_envs("default", &db).await;
        assert!(
            envs.is_empty(),
            "Headless services (clusterIP=None) should not generate env vars"
        );
    }

    #[tokio::test]
    async fn test_resolve_service_envs_empty_namespace() {
        let db = crate::datastore::test_support::in_memory().await;
        let envs = resolve_service_envs("empty-ns", &db).await;
        assert!(envs.is_empty(), "No services means no env vars");
    }
}
