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

/// Cached host resolv.conf parse result. The host DNS config rarely changes
/// during a klights process lifetime; re-reading on every pod start (the only
/// caller is `dnsPolicy: Default`) was a sync FS hit on the async kubelet path.
/// One read at first call, then served from memory thereafter.
static HOST_RESOLV_CONF: std::sync::OnceLock<(Vec<String>, Vec<String>, Vec<String>)> =
    std::sync::OnceLock::new();

fn read_host_resolv_conf_uncached() -> (Vec<String>, Vec<String>, Vec<String>) {
    let mut selected = crate::utils::read_utf8_file("/etc/resolv.conf")
        .ok()
        .map(|content| parse_resolv_conf_content(&content))
        .map(|(ns, search, opts)| without_loopback_nameservers(ns, search, opts))
        .unwrap_or_default();

    if selected.0.is_empty() {
        for path in ["/run/systemd/resolve/resolv.conf"] {
            let Some(parsed) = crate::utils::read_utf8_file(path)
                .ok()
                .map(|content| parse_resolv_conf_content(&content))
                .map(|(ns, search, opts)| without_loopback_nameservers(ns, search, opts))
            else {
                continue;
            };
            if !parsed.0.is_empty() {
                selected = parsed;
                break;
            }
        }
    }

    let (mut nameservers, searches, options) = selected;
    if nameservers.is_empty() {
        nameservers.push("192.0.2.53".to_string());
    }

    (nameservers, searches, options)
}

/// Parse host DNS config for pod dnsPolicy=Default.
/// If /etc/resolv.conf points at systemd-resolved's loopback stub, use the
/// resolved upstream file instead so pods do not point 127.0.0.53 at themselves.
///
/// Result is cached process-wide; the host resolv.conf is read at most once.
pub fn parse_host_resolv_conf() -> (Vec<String>, Vec<String>, Vec<String>) {
    HOST_RESOLV_CONF
        .get_or_init(read_host_resolv_conf_uncached)
        .clone()
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
}
