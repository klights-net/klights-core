use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

pub const DEFAULT_REGISTRY_PROXY_ENDPOINT: &str = "http://127.0.0.1:16797";

/// Immutable registry-proxy settings captured once by the bootstrap root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistryProxyConfig {
    endpoint: String,
    enabled: bool,
}

impl RegistryProxyConfig {
    pub fn from_inputs(
        enabled: bool,
        endpoint: Option<&str>,
        external_containerd: bool,
    ) -> Result<Self> {
        if enabled && external_containerd {
            bail!(
                "registry proxy mode cannot configure an external containerd; disable proxy mode or use the klights-managed runtime"
            );
        }

        let raw_endpoint = endpoint.unwrap_or(DEFAULT_REGISTRY_PROXY_ENDPOINT);
        let Some(after_scheme) = raw_endpoint.strip_prefix("http://") else {
            bail!("registry proxy endpoint must use absolute http:// URL");
        };
        if after_scheme.is_empty() || after_scheme.starts_with('/') {
            bail!("registry proxy endpoint must include an explicit authority");
        }
        let parsed =
            reqwest::Url::parse(raw_endpoint).context("invalid registry proxy endpoint URL")?;
        if parsed.scheme() != "http" {
            bail!("registry proxy endpoint must use absolute http:// URL");
        }
        if parsed.host_str().is_none() {
            bail!("registry proxy endpoint must include a host");
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            bail!("registry proxy endpoint must not contain credentials");
        }
        if parsed.path() != "/" {
            bail!("registry proxy endpoint path must be /");
        }
        if parsed.query().is_some() || parsed.fragment().is_some() {
            bail!("registry proxy endpoint must not contain a query or fragment");
        }

        Ok(Self {
            endpoint: parsed.to_string(),
            enabled,
        })
    }

    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn health_endpoint(&self) -> String {
        format!("{}healthz", self.endpoint)
    }

    pub async fn verify_ready(
        &self,
        supervisor: &klights_supervisor::TaskSupervisor,
    ) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let endpoint = self.health_endpoint();
        let request = reqwest::Client::new().get(&endpoint).send();
        let response = supervisor
            .timeout(
                "registry_proxy_readiness",
                std::time::Duration::from_secs(5),
                request,
            )
            .await
            .context("registry proxy readiness task failed")?
            .map_err(|_| anyhow::anyhow!("registry proxy readiness timed out at {endpoint}"))?
            .with_context(|| format!("registry proxy is unreachable at {endpoint}"))?;
        if !response.status().is_success() {
            bail!(
                "registry proxy readiness failed at {endpoint}: HTTP {}",
                response.status()
            );
        }
        Ok(())
    }
}

/// Canonical containerd registry namespace and upstream authority for one
/// already-normalized image reference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistryOrigin {
    namespace: String,
    upstream_authority: String,
}

impl RegistryOrigin {
    pub fn from_normalized_image(image: &str) -> Result<Self> {
        let (raw_namespace, repository) = image
            .split_once('/')
            .context("normalized image reference must include a registry namespace")?;
        if raw_namespace.is_empty()
            || raw_namespace.ends_with(':')
            || repository.is_empty()
            || repository.starts_with('/')
            || (!raw_namespace.contains('.')
                && !raw_namespace.contains(':')
                && raw_namespace != "localhost")
        {
            bail!("normalized image reference has no explicit registry namespace");
        }

        let parsed = reqwest::Url::parse(&format!("http://{raw_namespace}/"))
            .context("normalized image registry namespace is not a valid authority")?;
        if parsed.username() != ""
            || parsed.password().is_some()
            || parsed.path() != "/"
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            bail!("normalized image registry namespace must be an authority only");
        }
        let host = parsed
            .host_str()
            .context("normalized image registry namespace has no host")?;
        if host != "localhost"
            && host.parse::<std::net::IpAddr>().is_err()
            && host.split('.').any(|label| {
                label.is_empty()
                    || label.starts_with('-')
                    || label.ends_with('-')
                    || !label
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            })
        {
            bail!("normalized image registry namespace has an invalid host");
        }
        let host = if host.contains(':') {
            format!("[{host}]")
        } else {
            host.to_ascii_lowercase()
        };
        let namespace = match parsed.port() {
            Some(port) => format!("{host}:{port}"),
            None => host,
        };
        let upstream_authority = if namespace == "docker.io" {
            "registry-1.docker.io".to_string()
        } else {
            namespace.clone()
        };
        Ok(Self {
            namespace,
            upstream_authority,
        })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn upstream_authority(&self) -> &str {
        &self.upstream_authority
    }
}

/// Owns containerd's origin-specific registry host files. All filesystem work
/// crosses the app-owned supervised file executor.
#[derive(Clone)]
pub struct ContainerdRegistryProxyConfigurator {
    config: RegistryProxyConfig,
    certs_dir: PathBuf,
    file_process: klights_supervisor::FileProcessExecutor,
}

fn reconcile_registry_proxy_root(certs_dir: &Path, enabled: bool) -> Result<()> {
    match std::fs::remove_dir_all(certs_dir) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "remove stale registry proxy directory {}",
                    certs_dir.display()
                )
            });
        }
    }
    if enabled {
        std::fs::create_dir_all(certs_dir).with_context(|| {
            format!(
                "create containerd registry proxy directory {}",
                certs_dir.display()
            )
        })?;
    }
    Ok(())
}

fn write_registry_hosts_atomically(
    origin_dir: &Path,
    hosts_path: &Path,
    temp_path: &Path,
    rendered: &str,
) -> Result<()> {
    std::fs::create_dir_all(origin_dir)
        .with_context(|| format!("create registry host directory {}", origin_dir.display()))?;
    if std::fs::read_to_string(hosts_path).is_ok_and(|existing| existing == rendered) {
        return Ok(());
    }
    let result = (|| -> Result<()> {
        let mut file = std::fs::File::create(temp_path).with_context(|| {
            format!(
                "create registry host temporary file {}",
                temp_path.display()
            )
        })?;
        file.write_all(rendered.as_bytes()).with_context(|| {
            format!("write registry host temporary file {}", temp_path.display())
        })?;
        file.sync_all().with_context(|| {
            format!("sync registry host temporary file {}", temp_path.display())
        })?;
        drop(file);
        std::fs::rename(temp_path, hosts_path).with_context(|| {
            format!(
                "atomically replace containerd registry host file {}",
                hosts_path.display()
            )
        })?;
        if let Ok(parent) = std::fs::File::open(origin_dir) {
            let _ = parent.sync_all();
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temp_path);
    }
    result
}

impl ContainerdRegistryProxyConfigurator {
    pub fn new(
        config: RegistryProxyConfig,
        certs_dir: PathBuf,
        file_process: klights_supervisor::FileProcessExecutor,
    ) -> Self {
        Self {
            config,
            certs_dir,
            file_process,
        }
    }

    pub fn certs_dir(&self) -> &Path {
        &self.certs_dir
    }

    pub async fn reconcile_root(&self) -> Result<()> {
        let certs_dir = self.certs_dir.clone();
        let enabled = self.config.enabled();
        self.file_process
            .run_blocking_file_keyed(
                "containerd_registry_proxy_reconcile_root",
                certs_dir.display().to_string(),
                move || reconcile_registry_proxy_root(&certs_dir, enabled),
            )
            .await
    }

    pub async fn ensure_for_normalized_image(&self, image: &str) -> Result<()> {
        if !self.config.enabled() {
            return Ok(());
        }
        let origin = RegistryOrigin::from_normalized_image(image)?;
        self.ensure_origin(&origin).await
    }

    async fn ensure_origin(&self, origin: &RegistryOrigin) -> Result<()> {
        let origin_dir = self.certs_dir.join(origin.namespace());
        let hosts_path = origin_dir.join("hosts.toml");
        let temp_path = origin_dir.join(format!(".hosts.toml.klights-{}.tmp", std::process::id()));
        let rendered = Self::render_hosts_toml(&self.config, origin);
        let key = hosts_path.display().to_string();
        self.file_process
            .run_blocking_file_keyed("containerd_registry_proxy_write_origin", key, move || {
                write_registry_hosts_atomically(&origin_dir, &hosts_path, &temp_path, &rendered)
            })
            .await
    }

    fn render_hosts_toml(config: &RegistryProxyConfig, origin: &RegistryOrigin) -> String {
        format!(
            "server = \"{}\"\ncapabilities = [\"pull\", \"resolve\"]\n\n[header]\n  Klights-Registry-Origin = \"{}\"\n",
            config.endpoint(),
            origin.upstream_authority()
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{ContainerdRegistryProxyConfigurator, RegistryOrigin, RegistryProxyConfig};

    #[test]
    fn disabled_mode_keeps_direct_registry_behavior() {
        let config = RegistryProxyConfig::from_inputs(false, None, false).unwrap();
        assert!(!config.enabled());
        assert_eq!(config.endpoint(), "http://127.0.0.1:16797/");
    }

    #[test]
    fn enabled_mode_uses_the_default_proxy_endpoint() {
        let config = RegistryProxyConfig::from_inputs(true, None, false).unwrap();
        assert!(config.enabled());
        assert_eq!(config.endpoint(), "http://127.0.0.1:16797/");
        assert_eq!(config.health_endpoint(), "http://127.0.0.1:16797/healthz");
    }

    #[test]
    fn enabled_mode_accepts_a_reachable_host_bridge_endpoint() {
        let config =
            RegistryProxyConfig::from_inputs(true, Some("http://10.99.0.1:16797"), false).unwrap();
        assert_eq!(config.endpoint(), "http://10.99.0.1:16797/");
    }

    #[test]
    fn endpoint_rejects_unsupported_or_ambiguous_urls() {
        for invalid in [
            "https://127.0.0.1:16797",
            "http://user:pass@127.0.0.1:16797",
            "http://127.0.0.1:16797/v2",
            "http://127.0.0.1:16797/?query=1",
            "http://127.0.0.1:16797/#fragment",
            "127.0.0.1:16797",
            "http:///missing-host",
        ] {
            let error = RegistryProxyConfig::from_inputs(true, Some(invalid), false)
                .expect_err("invalid registry proxy endpoint must fail closed");
            assert!(
                error.to_string().contains("registry proxy"),
                "unexpected diagnostic for {invalid}: {error}"
            );
        }
    }

    #[test]
    fn invalid_endpoint_diagnostics_do_not_expose_credentials() {
        let endpoint = "http://secret-user:secret%zz@127.0.0.1:16797";
        let error = RegistryProxyConfig::from_inputs(true, Some(endpoint), false)
            .expect_err("malformed credential-bearing endpoint must fail closed");
        let diagnostic = format!("{error:#}");
        assert!(!diagnostic.contains("secret-user"));
        assert!(!diagnostic.contains("secret%zz"));
    }

    #[test]
    fn enabled_proxy_rejects_externally_managed_containerd() {
        let error = RegistryProxyConfig::from_inputs(true, None, true)
            .expect_err("external containerd cannot be configured by klights");
        assert!(error.to_string().contains("external containerd"));
    }

    #[test]
    fn registry_origin_canonicalizes_normalized_image_references() {
        let cases = [
            (
                "docker.io/library/nginx:latest",
                "docker.io",
                "registry-1.docker.io",
            ),
            (
                "docker.io/example/team/image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "docker.io",
                "registry-1.docker.io",
            ),
            (
                "registry.k8s.io/e2e-test-images/agnhost:2.56",
                "registry.k8s.io",
                "registry.k8s.io",
            ),
            (
                "localhost:5000/team/image:tag",
                "localhost:5000",
                "localhost:5000",
            ),
        ];

        for (image, namespace, authority) in cases {
            let origin = RegistryOrigin::from_normalized_image(image).unwrap();
            assert_eq!(origin.namespace(), namespace, "{image}");
            assert_eq!(origin.upstream_authority(), authority, "{image}");
        }
    }

    #[test]
    fn registry_origin_rejects_non_normalized_or_unsafe_references() {
        for invalid in [
            "nginx:latest",
            "library/nginx:latest",
            "/library/nginx:latest",
            "../registry/image:tag",
            "https://registry.k8s.io/image:tag",
        ] {
            assert!(
                RegistryOrigin::from_normalized_image(invalid).is_err(),
                "{invalid} must be rejected"
            );
        }
    }

    #[test]
    fn hosts_toml_has_one_proxy_host_and_no_upstream_fallback() {
        let config =
            RegistryProxyConfig::from_inputs(true, Some("http://10.99.0.1:16797"), false).unwrap();
        let origin = RegistryOrigin::from_normalized_image("registry.k8s.io/pause:3.10").unwrap();
        let rendered = ContainerdRegistryProxyConfigurator::render_hosts_toml(&config, &origin);

        assert_eq!(
            rendered,
            concat!(
                "server = \"http://10.99.0.1:16797/\"\n",
                "capabilities = [\"pull\", \"resolve\"]\n\n",
                "[header]\n",
                "  Klights-Registry-Origin = \"registry.k8s.io\"\n",
            )
        );
        assert!(!rendered.contains("https://registry.k8s.io"));
    }

    #[tokio::test]
    async fn configurator_creates_origin_specific_hosts_atomically_and_idempotently() {
        let temp = tempfile::tempdir().unwrap();
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let config =
            RegistryProxyConfig::from_inputs(true, Some("http://127.0.0.1:16797"), false).unwrap();
        let configurator = ContainerdRegistryProxyConfigurator::new(
            config,
            temp.path().join("certs.d"),
            klights_supervisor::FileProcessExecutor::new(supervisor),
        );
        let image = "registry.k8s.io/e2e-test-images/agnhost:2.56";

        let (first, second) = tokio::join!(
            configurator.ensure_for_normalized_image(image),
            configurator.ensure_for_normalized_image(image)
        );
        first.unwrap();
        second.unwrap();

        let hosts_path = temp.path().join("certs.d/registry.k8s.io/hosts.toml");
        let contents = std::fs::read_to_string(hosts_path).unwrap();
        assert!(contents.contains("Klights-Registry-Origin = \"registry.k8s.io\""));
        let entries = std::fs::read_dir(temp.path().join("certs.d/registry.k8s.io"))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(entries.len(), 1, "atomic temporary files must be removed");
    }

    #[tokio::test]
    async fn disabled_config_removes_stale_proxy_namespaces() {
        let temp = tempfile::tempdir().unwrap();
        let certs_dir = temp.path().join("certs.d");
        std::fs::create_dir_all(certs_dir.join("registry.k8s.io")).unwrap();
        std::fs::write(
            certs_dir.join("registry.k8s.io/hosts.toml"),
            "stale proxy config",
        )
        .unwrap();
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let configurator = ContainerdRegistryProxyConfigurator::new(
            RegistryProxyConfig::from_inputs(false, None, false).unwrap(),
            certs_dir.clone(),
            klights_supervisor::FileProcessExecutor::new(supervisor),
        );

        configurator.reconcile_root().await.unwrap();

        assert!(!certs_dir.exists());
    }
}
