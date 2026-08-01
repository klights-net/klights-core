pub fn is_loopback_nameserver(nameserver: &str) -> bool {
    nameserver.starts_with("127.") || nameserver == "::1"
}

/// Parse resolv.conf content to extract nameservers, search domains, and options.
pub fn parse_resolv_conf_content(content: &str) -> (Vec<String>, Vec<String>, Vec<String>) {
    let mut nameservers = Vec::new();
    let mut searches = Vec::new();
    let mut options = Vec::new();

    for line in content.lines() {
        let line = line.trim();
        if line.starts_with('#') || line.is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }

        match parts[0] {
            "nameserver" if parts.len() > 1 => {
                nameservers.push(parts[1].to_string());
            }
            "search" => {
                for domain in &parts[1..] {
                    searches.push(domain.to_string());
                }
            }
            "options" => {
                for opt in &parts[1..] {
                    options.push(opt.to_string());
                }
            }
            _ => {}
        }
    }

    (nameservers, searches, options)
}

pub fn without_loopback_nameservers(
    nameservers: Vec<String>,
    searches: Vec<String>,
    options: Vec<String>,
) -> (Vec<String>, Vec<String>, Vec<String>) {
    let nameservers: Vec<String> = nameservers
        .into_iter()
        .filter(|ns| !is_loopback_nameserver(ns))
        .collect();
    (nameservers, searches, options)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostDnsConfig {
    nameservers: Vec<String>,
    searches: Vec<String>,
    options: Vec<String>,
}

impl HostDnsConfig {
    /// Build the immutable DNS snapshot injected by root construction.
    ///
    /// `resolved_upstream` is used only when the primary file contains no
    /// non-loopback nameserver, matching systemd-resolved stub behavior.
    pub fn from_resolv_conf_contents(
        primary: Option<&str>,
        resolved_upstream: Option<&str>,
    ) -> Self {
        let parse = |content: &str| {
            let (nameservers, searches, options) = parse_resolv_conf_content(content);
            without_loopback_nameservers(nameservers, searches, options)
        };
        let mut selected = primary.map(parse).unwrap_or_default();
        if selected.0.is_empty()
            && let Some(upstream) = resolved_upstream.map(parse)
            && !upstream.0.is_empty()
        {
            selected = upstream;
        }
        if selected.0.is_empty() {
            selected.0.push("192.0.2.53".to_string());
        }
        Self {
            nameservers: selected.0,
            searches: selected.1,
            options: selected.2,
        }
    }

    pub fn as_parts(&self) -> (Vec<String>, Vec<String>, Vec<String>) {
        (
            self.nameservers.clone(),
            self.searches.clone(),
            self.options.clone(),
        )
    }
}

impl Default for HostDnsConfig {
    fn default() -> Self {
        Self::from_resolv_conf_contents(None, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_host_resolv_conf_avoids_loopback_stub() {
        let stub = r#"
nameserver 127.0.0.53
options edns0 trust-ad
search example.internal
"#;
        let (nameservers, searches, options) = parse_resolv_conf_content(stub);
        let (nameservers, searches, options) =
            without_loopback_nameservers(nameservers, searches, options);

        assert!(
            nameservers.is_empty(),
            "loopback DNS stubs are unsafe inside pod netns"
        );
        assert_eq!(searches, vec!["example.internal"]);
        assert_eq!(options, vec!["edns0", "trust-ad"]);
    }

    #[test]
    fn host_dns_config_prefers_injected_upstream_over_loopback_stub() {
        let config = HostDnsConfig::from_resolv_conf_contents(
            Some("nameserver 127.0.0.53\nsearch local"),
            Some("nameserver 192.0.2.10\nsearch example.internal"),
        );
        assert_eq!(
            config.as_parts(),
            (
                vec!["192.0.2.10".to_string()],
                vec!["example.internal".to_string()],
                Vec::new(),
            )
        );
    }
}
