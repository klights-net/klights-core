// Integration tests — root + the `nft` binary required for verification.
// Run via `sudo -E cargo test -- --ignored networking::service_routing`.
#[cfg(test)]
mod integration_tests {
    use crate::networking::netfilter::Netfilter;
    use crate::networking::service_routing::*;
    use crate::networking::{ClusterCidr, PodSubnet};
    use std::net::Ipv4Addr;
    use std::process::Command;
    use tokio_util::sync::CancellationToken;

    /// Drop the test table on scope exit, even on panic.
    struct TableGuard {
        name: String,
    }
    impl Drop for TableGuard {
        fn drop(&mut self) {
            let _ = Command::new("nft")
                .args(["delete", "table", "inet", &self.name])
                .status();
        }
    }

    struct IpTableGuard {
        name: String,
    }
    impl Drop for IpTableGuard {
        fn drop(&mut self) {
            let _ = Command::new("nft")
                .args(["delete", "table", "ip", &self.name])
                .status();
        }
    }

    fn unique_name(prefix: &str) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        format!("{prefix}_{}_{}", std::process::id(), nanos)
    }

    fn nft_listing(table: &str) -> String {
        let out = Command::new("nft")
            .args(["list", "table", "inet", table])
            .output()
            .expect("nft list table");
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    fn test_task_supervisor() -> std::sync::Arc<crate::task_supervisor::TaskSupervisor> {
        std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ))
    }

    fn build(name: &str) -> KlightsTable {
        let nf = Netfilter::new(test_task_supervisor()).expect("Netfilter::new");
        let pod = PodSubnet::parse("10.99.0.0/24").expect("static cidr");
        let cluster = ClusterCidr::parse("10.99.0.0/16").expect("static cidr");
        KlightsTable::with_name(
            nf,
            name,
            pod,
            cluster,
            ClusterCidr::parse("10.99.128.0/17").expect("static service cidr"),
            ServiceRoutingMode::default_root_for_test(),
        )
        .expect("build table handle")
    }

    #[tokio::test]
    #[ignore]
    async fn test_init_creates_filter_forward_with_pod_subnet_rules() {
        let name = unique_name("klights_sr_fwd");
        let _guard = TableGuard { name: name.clone() };

        build(&name).init().await.expect("init");

        let listing = nft_listing(&name);
        assert!(
            listing.contains("chain filter-forward"),
            "expected filter-forward chain in:\n{listing}"
        );
        assert!(
            listing.contains("hook forward"),
            "expected forward hook in:\n{listing}"
        );
        let accept_lines = listing
            .lines()
            .filter(|l| l.contains("accept"))
            .filter(|l| !l.contains("policy"))
            .count();
        assert!(
            accept_lines >= 3,
            "expected at least 3 accept rules in filter-forward, got {accept_lines}:\n{listing}"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_reconcile_forward_compat_installs_idempotent_accepts_without_kube_mark() {
        let name = unique_name("klights_sr_fwd_compat");
        let ip_table = unique_name("klights_sr_ip_fwd");
        let _guard = IpTableGuard {
            name: ip_table.clone(),
        };

        let add_table = Command::new("nft")
            .args(["add", "table", "ip", &ip_table])
            .status()
            .expect("nft add ip table");
        assert!(add_table.success(), "create ip test table");
        let add_chain = Command::new("nft")
            .args(["add", "chain", "ip", &ip_table, "FORWARD"])
            .status()
            .expect("nft add ip chain");
        assert!(add_chain.success(), "create ip test chain");

        let table = build(&name);
        table
            .reconcile_forward_compat_chain(
                "ip",
                &ip_table,
                "FORWARD",
                "klights-forward-compat-test",
            )
            .await
            .expect("first reconcile");
        table
            .reconcile_forward_compat_chain(
                "ip",
                &ip_table,
                "FORWARD",
                "klights-forward-compat-test",
            )
            .await
            .expect("second reconcile must replace, not duplicate");

        let chain = nft_ip_chain(&ip_table, "FORWARD");
        let comment_count = chain.matches("klights-forward-compat-test").count();
        assert_eq!(
            comment_count, 2,
            "reconcile must leave exactly one egress and one ingress accept rule:\n{chain}"
        );
        assert!(
            chain.contains("iifname \"klights0\"")
                && chain.contains("ip saddr 10.99.0.0/24")
                && chain.contains("accept"),
            "pod egress from the klights bridge must be explicitly accepted:\n{chain}"
        );
        assert!(
            chain.contains("oifname \"klights0\"")
                && chain.contains("ip daddr 10.99.0.0/24")
                && chain.contains("accept"),
            "pod ingress to the klights bridge must be explicitly accepted:\n{chain}"
        );
        assert!(
            !chain.contains("meta mark"),
            "compat accepts must not use KUBE-FORWARD's 0x4000 mark, which also triggers KUBE-POSTROUTING masquerade:\n{chain}"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_init_creates_nat_postrouting_with_masquerade() {
        let name = unique_name("klights_sr_pst");
        let _guard = TableGuard { name: name.clone() };

        build(&name).init().await.expect("init");

        let listing = nft_listing(&name);
        assert!(
            listing.contains("chain nat-postrouting"),
            "expected nat-postrouting chain in:\n{listing}"
        );
        assert!(
            listing.contains("hook postrouting"),
            "expected postrouting hook in:\n{listing}"
        );
        assert!(
            listing.contains("masquerade"),
            "expected masquerade verdict in:\n{listing}"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_init_snats_service_dnat_from_node_sources_to_pod_gateway() {
        let name = unique_name("klights_sr_pst_node_svc");
        let _guard = TableGuard { name: name.clone() };

        build(&name).init().await.expect("init");

        let chain = nft_chain(&name, "nat-postrouting");
        let node_service_dnat_snat = chain.lines().find(|line| {
            line.contains("@nh,96,32 & 0xffffff00 != 0xa630000")
                && line.contains("@nh,128,32 & 0xffff0000 == 0xa630000")
                && line.contains("ct status dnat")
                && line.contains("snat ip to 10.99.0.1")
        });
        assert!(
            node_service_dnat_snat.is_some(),
            "node-originated service DNAT flows must SNAT to the local pod gateway for remote pod replies:\n{chain}"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_init_is_idempotent() {
        let name = unique_name("klights_sr_idem");
        let _guard = TableGuard { name: name.clone() };

        let table = build(&name);
        table.init().await.expect("first init");

        // Second init must succeed and leave the same set of chains
        // present. We deliberately do NOT assert listing equality:
        // replace_chain (DEL+ADD) re-creates each chain, and the kernel
        // orders chains in `nft list table` output by creation time, so
        // chain order in the listing changes between runs even though
        // semantically the table is identical.
        table
            .init()
            .await
            .expect("second init should be idempotent");

        let listing = nft_listing(&name);
        // All five chains created by init() must be present in both runs.
        for chain in [
            "filter-forward",
            "nat-postrouting",
            "services",
            "nat-prerouting",
            "nat-output",
        ] {
            assert!(
                listing.contains(&format!("chain {chain} {{")),
                "chain {chain} must exist after second init:\n{listing}"
            );
        }
        // Hooks must be registered correctly (DEL+ADD preserves the
        // hook info because the Rust Chain object retains it across
        // calls).
        for hook_signature in [
            "type filter hook forward priority filter",
            "type nat hook postrouting priority srcnat",
            "type nat hook prerouting priority dstnat",
            "type nat hook output priority dstnat",
        ] {
            assert!(
                listing.contains(hook_signature),
                "hook signature `{hook_signature}` must be present:\n{listing}"
            );
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_cleanup_drops_table_in_one_call() {
        let name = unique_name("klights_sr_clean");
        let _guard = TableGuard { name: name.clone() };

        let table = build(&name);
        table.init().await.expect("init");
        let before = nft_listing(&name);
        assert!(!before.is_empty(), "table should exist after init");

        table.cleanup().await.expect("cleanup");

        let after = Command::new("nft")
            .args(["list", "table", "inet", &name])
            .output()
            .expect("nft list");
        assert!(
            !after.status.success(),
            "table should not exist after cleanup; nft listing succeeded:\n{}",
            String::from_utf8_lossy(&after.stdout)
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_cleanup_is_safe_when_table_absent() {
        let name = unique_name("klights_sr_absent");
        // No init — table never existed. cleanup() should swallow the
        // ENOENT and return Ok so shutdown isn't blocked by it.
        build(&name)
            .cleanup()
            .await
            .expect("cleanup must tolerate missing table");
    }

    #[tokio::test]
    #[ignore]
    async fn test_init_on_different_subnet_produces_different_listing() {
        let name_a = unique_name("klights_sr_subnet_a");
        let name_b = unique_name("klights_sr_subnet_b");
        let _ga = TableGuard {
            name: name_a.clone(),
        };
        let _gb = TableGuard {
            name: name_b.clone(),
        };

        let nf = Netfilter::new(test_task_supervisor()).expect("nf");
        let table_a = KlightsTable::with_name(
            nf.clone(),
            &name_a,
            PodSubnet::parse("10.99.0.0/24").expect("static cidr"),
            ClusterCidr::parse("10.99.0.0/16").expect("static cidr"),
            ClusterCidr::parse("10.99.128.0/17").expect("static service cidr"),
            ServiceRoutingMode::default_root_for_test(),
        )
        .expect("build a");
        let table_b = KlightsTable::with_name(
            nf,
            &name_b,
            PodSubnet::parse("172.16.0.0/16").expect("static cidr"),
            ClusterCidr::parse("172.16.0.0/12").expect("static cidr"),
            ClusterCidr::parse("172.31.0.0/16").expect("static service cidr"),
            ServiceRoutingMode::default_root_for_test(),
        )
        .expect("build b");
        table_a.init().await.expect("init a");
        table_b.init().await.expect("init b");

        let listing_a = nft_listing(&name_a);
        let listing_b = nft_listing(&name_b);
        // Both must list a forward chain
        assert!(listing_a.contains("filter-forward"));
        assert!(listing_b.contains("filter-forward"));
        // nftnl emits raw payload syntax (`@nh,96,32 & 0x... == 0x...`),
        // not the friendly `ip saddr` form, so check the wire-format
        // hex constants. 10.99.0.0 → 0x0a630000; 172.16.0.0 → 0xac100000.
        // nft strips the leading zero and lowercases the hex.
        assert!(
            listing_a.contains("0xa630000"),
            "a should encode 10.99.0.0 as 0xa630000:\n{listing_a}"
        );
        assert!(
            listing_b.contains("0xac100000"),
            "b should encode 172.16.0.0 as 0xac100000:\n{listing_b}"
        );
        // And the subnet masks differ (/24 vs /16)
        assert!(
            listing_a.contains("0xffffff00"),
            "a should use /24 mask 0xffffff00:\n{listing_a}"
        );
        assert!(
            listing_b.contains("0xffff0000"),
            "b should use /16 mask 0xffff0000:\n{listing_b}"
        );
    }

    /// Regression test for 969d8c3 — the commit that switched the table
    /// name source from `bridge_name` (Linux IFNAMSIZ-1 = 15-char limit,
    /// gets truncated) to `containerd_namespace` (no length limit). If a
    /// future change re-truncates the table name, this test fails because
    /// the long fixture name won't be findable in the listing.
    #[tokio::test]
    #[ignore]
    async fn test_with_name_uses_full_untruncated_namespace_as_table_name() {
        let long_ns = "klights-untruncated-ns-regression-49chars-xxxxxxx";
        // Sanity-check the fixture: must exceed Linux IFNAMSIZ-1 to be
        // a meaningful regression test. If someone shortens this string
        // below 16 chars, the test stops protecting against the bug.
        assert!(
            long_ns.len() > 15,
            "fixture must exceed Linux IFNAMSIZ-1=15 to exercise the regression"
        );
        let _guard = TableGuard {
            name: long_ns.to_string(),
        };

        let nf = Netfilter::new(test_task_supervisor()).expect("Netfilter::new");
        let table = KlightsTable::with_name(
            nf,
            long_ns,
            PodSubnet::parse("10.99.0.0/24").expect("static cidr"),
            ClusterCidr::parse("10.99.0.0/16").expect("static cidr"),
            ClusterCidr::parse("10.99.128.0/17").expect("static service cidr"),
            ServiceRoutingMode::default_root_for_test(),
        )
        .expect("with_name");
        table.init().await.expect("init");

        // The kernel must report the FULL untruncated name. If naming
        // were truncated to e.g. the first 15 chars, listing the table
        // by long_ns would fail with ENOENT.
        let listing = nft_listing(long_ns);
        assert!(
            listing.contains(&format!("table inet {long_ns}")),
            "expected `table inet {long_ns}` (full untruncated name) in listing; got:\n{listing}"
        );
    }

    // ---- Services chain integration tests -----------------------------

    fn nft_chain(table: &str, chain: &str) -> String {
        let out = Command::new("nft")
            .args(["list", "chain", "inet", table, chain])
            .output()
            .expect("nft list chain");
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    fn nft_ip_chain(table: &str, chain: &str) -> String {
        let out = Command::new("nft")
            .args(["list", "chain", "ip", table, chain])
            .output()
            .expect("nft list ip chain");
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    /// Build a ServiceSpec with literal IPs/ports for tests.
    fn spec(cluster_ip: [u8; 4], ports: Vec<PortSpec>) -> ServiceSpec {
        ServiceSpec {
            cluster_ip: Ipv4Addr::from(cluster_ip),
            ports,
            session_affinity: SessionAffinity::None,
        }
    }

    fn port(
        service_port: u16,
        target_port: u16,
        node_port: Option<u16>,
        protocol: Protocol,
        endpoints: Vec<[u8; 4]>,
    ) -> PortSpec {
        PortSpec {
            service_port,
            target_port,
            node_port,
            protocol,
            endpoints: endpoints.into_iter().map(Ipv4Addr::from).collect(),
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_init_creates_services_chain_empty() {
        let name = unique_name("klights_sr_svc_init");
        let _guard = TableGuard { name: name.clone() };

        build(&name).init().await.expect("init");

        let listing = nft_listing(&name);
        assert!(
            listing.contains("chain services {"),
            "services chain must be created by init:\n{listing}"
        );
        assert!(
            listing.contains("chain nat-prerouting"),
            "nat-prerouting chain must be created:\n{listing}"
        );
        assert!(
            listing.contains("chain nat-output"),
            "nat-output chain must be created:\n{listing}"
        );
        // The base chains must jump to services. nft pretty-prints
        // "jump services" for chain-name jump verdicts.
        assert!(
            listing.contains("jump services"),
            "nat-prerouting/nat-output must jump to services chain:\n{listing}"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_replace_services_writes_clusterip_dnat_rule() {
        let name = unique_name("klights_sr_svc_clusterip");
        let _guard = TableGuard { name: name.clone() };
        let table = build(&name);
        table.init().await.expect("init");

        let services = vec![spec(
            [10, 43, 128, 5],
            vec![port(80, 8080, None, Protocol::Tcp, vec![[10, 43, 0, 10]])],
        )];
        table.replace_services(&services).await.expect("replace");

        let chain = nft_chain(&name, "services");
        // nft pretty-prints the DNAT target in decimal (`dnat ip to
        // 10.43.0.10:8080`) but the *match* fields are emitted as raw
        // payload syntax with hex (`@nh,128,32 0xa2b8005` — daddr at
        // network-header offset 128 bits, length 32 bits, equal to
        // 10.43.128.5 = 0x0a2b8005). nft strips the leading zero from
        // displayed hex.
        assert!(
            chain.contains("dnat ip to 10.43.0.10:8080"),
            "rule must DNAT to endpoint 10.43.0.10:8080:\n{chain}"
        );
        assert!(
            chain.contains("0xa2b8005"),
            "rule must match cluster IP 10.43.128.5 (encoded 0xa2b8005):\n{chain}"
        );
        // dport 80 — nft pretty-prints as decimal.
        assert!(
            chain.contains("dport 80"),
            "rule must match service port 80:\n{chain}"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_replace_services_writes_one_rule_per_endpoint() {
        let name = unique_name("klights_sr_svc_multi_ep");
        let _guard = TableGuard { name: name.clone() };
        let table = build(&name);
        table.init().await.expect("init");

        // 3 endpoints → 3 rules in the services chain (probability ladder
        // for first 2, unconditional dnat for the last).
        let services = vec![spec(
            [10, 43, 128, 5],
            vec![port(
                80,
                8080,
                None,
                Protocol::Tcp,
                vec![[10, 43, 0, 10], [10, 43, 0, 11], [10, 43, 0, 12]],
            )],
        )];
        table.replace_services(&services).await.expect("replace");

        let chain = nft_chain(&name, "services");
        let dnat_count = chain.matches("dnat").count();
        assert_eq!(
            dnat_count, 3,
            "expected 3 dnat rules (one per endpoint), got {dnat_count}:\n{chain}"
        );
        // The first 2 rules use the probability ladder via meta random;
        // nft pretty-prints `meta random` as "numgen random" or "meta
        // random" depending on version. Either way the literal "random"
        // appears.
        assert!(
            chain.contains("random"),
            "multi-endpoint rules must include `meta random` for LB:\n{chain}"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_replace_services_writes_nodeport_rule_separately() {
        let name = unique_name("klights_sr_svc_nodeport");
        let _guard = TableGuard { name: name.clone() };
        let table = build(&name);
        table.init().await.expect("init");

        let services = vec![spec(
            [10, 43, 128, 10],
            vec![port(
                80,
                8080,
                Some(30080),
                Protocol::Tcp,
                vec![[10, 43, 0, 20]],
            )],
        )];
        table.replace_services(&services).await.expect("replace");

        let chain = nft_chain(&name, "services");
        // 1 ClusterIP DNAT + 1 NodePort DNAT = 2 dnat rules per endpoint.
        // nft pretty-prints `dnat ip to ...`, so each rule contains the
        // literal `dnat`.
        let dnat_count = chain.matches("dnat").count();
        assert_eq!(
            dnat_count, 2,
            "expected 2 dnat rules (ClusterIP + NodePort), got {dnat_count}:\n{chain}"
        );
        // NodePort dport 30080 — pretty-printed as decimal.
        assert!(
            chain.contains("dport 30080"),
            "rule must match NodePort 30080:\n{chain}"
        );
        // ClusterIP rule still present.
        assert!(
            chain.contains("dport 80"),
            "rule must match service port 80:\n{chain}"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_replace_services_with_empty_input_clears_chain() {
        let name = unique_name("klights_sr_svc_empty");
        let _guard = TableGuard { name: name.clone() };
        let table = build(&name);
        table.init().await.expect("init");

        // Populate, then drain.
        let services = vec![spec(
            [10, 43, 128, 5],
            vec![port(80, 8080, None, Protocol::Tcp, vec![[10, 43, 0, 10]])],
        )];
        table.replace_services(&services).await.expect("populate");
        assert!(nft_chain(&name, "services").contains("dnat"));

        table.replace_services(&[]).await.expect("drain");
        let after = nft_chain(&name, "services");
        assert!(
            !after.contains("dnat"),
            "draining must remove all dnat rules:\n{after}"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_replace_services_is_atomic_no_partial_state() {
        // Round 1: 5 rules. Round 2: 1 rule. After round 2, observers
        // must see exactly 1 (not 6, not 0). replace_chain inside
        // replace_services uses the DEL+ADD-in-one-batch atomic pattern.
        let name = unique_name("klights_sr_svc_atomic");
        let _guard = TableGuard { name: name.clone() };
        let table = build(&name);
        table.init().await.expect("init");

        let many = vec![spec(
            [10, 43, 128, 5],
            vec![port(
                80,
                8080,
                None,
                Protocol::Tcp,
                vec![
                    [10, 43, 0, 10],
                    [10, 43, 0, 11],
                    [10, 43, 0, 12],
                    [10, 43, 0, 13],
                    [10, 43, 0, 14],
                ],
            )],
        )];
        table.replace_services(&many).await.expect("populate 5");
        assert_eq!(nft_chain(&name, "services").matches("dnat").count(), 5);

        let one = vec![spec(
            [10, 43, 128, 5],
            vec![port(80, 8080, None, Protocol::Tcp, vec![[10, 43, 0, 10]])],
        )];
        table.replace_services(&one).await.expect("replace with 1");
        assert_eq!(
            nft_chain(&name, "services").matches("dnat").count(),
            1,
            "atomic replace must produce exactly the new rule set"
        );
    }

    // ---- Hostports chain integration tests ----------------------------

    fn hp(host_ip: Option<[u8; 4]>, host_port: u16, container_port: u16) -> HostPortSpec {
        HostPortSpec {
            host_ip: host_ip.map(Ipv4Addr::from),
            host_port,
            container_port,
            protocol: Protocol::Tcp,
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_init_creates_hostports_chain_empty() {
        let name = unique_name("klights_sr_hp_init");
        let _guard = TableGuard { name: name.clone() };
        build(&name).init().await.expect("init");

        let listing = nft_listing(&name);
        assert!(
            listing.contains("chain hostports {"),
            "hostports chain must be created by init:\n{listing}"
        );
        // nat-prerouting must jump to BOTH hostports and services.
        let pre = nft_chain(&name, "nat-prerouting");
        assert!(
            pre.contains("jump hostports") && pre.contains("jump services"),
            "nat-prerouting must jump to both hostports and services:\n{pre}"
        );
        let out = nft_chain(&name, "nat-output");
        assert!(
            out.contains("jump hostports") && out.contains("jump services"),
            "nat-output must jump to both hostports and services:\n{out}"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_add_hostports_for_pod_writes_dnat_rule() {
        let name = unique_name("klights_sr_hp_add");
        let _guard = TableGuard { name: name.clone() };
        let table = build(&name);
        table.init().await.expect("init");

        table
            .add_hostports_for_pod(Ipv4Addr::new(10, 99, 0, 42), vec![hp(None, 8080, 80)])
            .await
            .expect("add");

        let chain = nft_chain(&name, "hostports");
        assert!(
            chain.contains("dnat ip to 10.99.0.42:80"),
            "rule must DNAT to pod_ip:container_port:\n{chain}"
        );
        assert!(
            chain.contains("dport 8080"),
            "rule must match host port 8080:\n{chain}"
        );
        // No `ip daddr` match because hostIP is None — host_port matches
        // any destination.
        assert!(
            !chain.contains("@nh,128,32 0x"),
            "rule must NOT contain a daddr match when hostIP is None:\n{chain}"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_add_hostports_for_pod_with_specific_hostip_includes_daddr_match() {
        let name = unique_name("klights_sr_hp_specific");
        let _guard = TableGuard { name: name.clone() };
        let table = build(&name);
        table.init().await.expect("init");

        table
            .add_hostports_for_pod(
                Ipv4Addr::new(10, 99, 0, 50),
                vec![hp(Some([192, 168, 1, 5]), 8080, 80)],
            )
            .await
            .expect("add");

        let chain = nft_chain(&name, "hostports");
        // 192.168.1.5 = 0xc0a80105 — nft strips no leading zero here.
        assert!(
            chain.contains("0xc0a80105"),
            "rule must contain ip daddr match for 192.168.1.5 (0xc0a80105):\n{chain}"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_remove_hostports_for_pod_clears_only_that_pods_rules() {
        let name = unique_name("klights_sr_hp_rm");
        let _guard = TableGuard { name: name.clone() };
        let table = build(&name);
        table.init().await.expect("init");

        let pod_a = Ipv4Addr::new(10, 99, 0, 10);
        let pod_b = Ipv4Addr::new(10, 99, 0, 11);
        table
            .add_hostports_for_pod(pod_a, vec![hp(None, 8080, 80)])
            .await
            .unwrap();
        table
            .add_hostports_for_pod(pod_b, vec![hp(None, 8081, 81)])
            .await
            .unwrap();
        let chain_before = nft_chain(&name, "hostports");
        assert_eq!(chain_before.matches("dnat").count(), 2);

        table.remove_hostports_for_pod(pod_a).await.expect("remove");
        let chain_after = nft_chain(&name, "hostports");
        assert_eq!(
            chain_after.matches("dnat").count(),
            1,
            "removing pod A must leave pod B's rule:\n{chain_after}"
        );
        assert!(
            chain_after.contains("dnat ip to 10.99.0.11:81"),
            "pod B's rule must remain:\n{chain_after}"
        );
        assert!(
            !chain_after.contains("dnat ip to 10.99.0.10:80"),
            "pod A's rule must be gone:\n{chain_after}"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_remove_hostports_for_unknown_pod_is_noop() {
        let name = unique_name("klights_sr_hp_unknown");
        let _guard = TableGuard { name: name.clone() };
        let table = build(&name);
        table.init().await.expect("init");

        // Should not error, even though no entry exists.
        table
            .remove_hostports_for_pod(Ipv4Addr::new(10, 99, 0, 99))
            .await
            .expect("remove of unknown pod must be a no-op, not an error");
    }

    #[tokio::test]
    #[ignore]
    async fn test_add_hostports_for_pod_replaces_previous_specs() {
        let name = unique_name("klights_sr_hp_replace");
        let _guard = TableGuard { name: name.clone() };
        let table = build(&name);
        table.init().await.expect("init");

        let pod = Ipv4Addr::new(10, 99, 0, 30);
        table
            .add_hostports_for_pod(pod, vec![hp(None, 8080, 80), hp(None, 8081, 81)])
            .await
            .unwrap();
        assert_eq!(nft_chain(&name, "hostports").matches("dnat").count(), 2);

        // Re-add with a smaller spec list — registry replaces, chain
        // shrinks accordingly. This is the safety property for pod
        // updates that change hostport mappings.
        table
            .add_hostports_for_pod(pod, vec![hp(None, 9000, 90)])
            .await
            .unwrap();
        let chain = nft_chain(&name, "hostports");
        assert_eq!(chain.matches("dnat").count(), 1);
        assert!(chain.contains("dnat ip to 10.99.0.30:90"));
        assert!(!chain.contains("dnat ip to 10.99.0.30:80"));
    }

    /// Async race regression for Stab-1: many concurrent add/remove
    /// calls on the same table must converge to a deterministic chain
    /// state matching the post-condition (each pod's last operation
    /// wins). Without the per-table async mutex we'd see lost updates
    /// — A's stale snapshot overwriting B's later one.
    ///
    /// This test launches 50 interleaved tasks (25 add + 25 remove for
    /// a fixed pod set) and asserts the final chain matches the set of
    /// pods that ended in "added" state.
    #[tokio::test]
    #[ignore]
    async fn test_concurrent_hostport_mutations_converge_deterministically() {
        let name = unique_name("klights_sr_hp_race");
        let _guard = TableGuard { name: name.clone() };
        let table = std::sync::Arc::new(build(&name));
        table.init().await.expect("init");

        // Five distinct pods. Each has a "final" state in our test plan:
        // pods 0,2,4 end ADDED; pods 1,3 end REMOVED.
        let pods: Vec<Ipv4Addr> = (0..5).map(|i| Ipv4Addr::new(10, 99, 0, 100 + i)).collect();

        // Generate a churn schedule: many add/remove ops per pod, ending
        // with the desired final state. Each pod's ops are launched as
        // its own task so the tokio scheduler interleaves them.
        let mut tasks = Vec::new();
        for (idx, &pod_ip) in pods.iter().enumerate() {
            let table = table.clone();
            let final_added = idx % 2 == 0;
            tasks.push(tokio::task::spawn(async move {
                // Churn: add, remove, add, remove, ...
                for _ in 0..5 {
                    table
                        .add_hostports_for_pod(pod_ip, vec![hp(None, 9000 + idx as u16, 80)])
                        .await
                        .expect("churn add");
                    table
                        .remove_hostports_for_pod(pod_ip)
                        .await
                        .expect("churn rm");
                }
                // Final state.
                if final_added {
                    table
                        .add_hostports_for_pod(pod_ip, vec![hp(None, 9000 + idx as u16, 80)])
                        .await
                        .expect("final add");
                }
            }));
        }
        for t in tasks {
            t.await.expect("task panicked");
        }

        // Final state must reflect exactly the pods that ended in
        // ADDED state — no lost updates. Without the mutex, a remove
        // from one task with a stale snapshot could blow away a later
        // add from another task.
        let chain = nft_chain(&name, "hostports");
        for (idx, pod_ip) in pods.iter().enumerate() {
            let expected_added = idx % 2 == 0;
            let needle = format!("dnat ip to {pod_ip}:80");
            let present = chain.contains(&needle);
            assert_eq!(
                present, expected_added,
                "pod {pod_ip} (idx {idx}) expected={expected_added} but chain={present}\n{chain}"
            );
        }
        // And exactly the right number of dnat lines.
        let expected_count = pods.iter().enumerate().filter(|(i, _)| i % 2 == 0).count();
        assert_eq!(
            chain.matches("dnat").count(),
            expected_count,
            "chain must have exactly {expected_count} dnat rules:\n{chain}"
        );
    }

    #[tokio::test]
    #[ignore = "requires root/netfilter access"]
    async fn test_worker_exits_within_100ms_on_cancel() {
        use crate::networking::service_router::ServiceRouter;
        let cancel = CancellationToken::new();
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .expect("in-mem datastore");
        let task_supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let cluster_api: std::sync::Arc<dyn crate::control_plane::client::LeaderApiClient> =
            std::sync::Arc::new(crate::control_plane::client::local::LocalApiClient::new(
                std::sync::Arc::new(db),
                "node-a".to_string(),
                crate::control_plane::client::local::always_leader_watch(),
            ));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            task_supervisor.clone(),
            None,
            "sqlite:service-router-shutdown-test",
        )
        .await
        .expect("open node-local test db");
        let rt = NftServiceRouter::boot(NftServiceRouterBoot::new(
            NftServiceRouterStores::new(cluster_api, node_local),
            NftServiceRouterTableConfig::new("node-a", "klights-test-shutdown", "klights-test"),
            NftServiceRouterNetworkConfig::new(
                PodSubnet::parse("10.42.0.0/24").unwrap(),
                ClusterCidr::parse("10.42.0.0/16").unwrap(),
                ClusterCidr::parse("10.43.128.0/17").unwrap(),
                ServiceRoutingMode::default_root_for_test(),
            ),
            NftServiceRouterRuntime::new(
                std::time::Duration::from_millis(50),
                cancel,
                task_supervisor.clone(),
            ),
        ))
        .await
        .expect("boot router");

        let started = std::time::Instant::now();
        let _ = rt.cleanup().await;
        assert!(
            started.elapsed() < std::time::Duration::from_millis(100),
            "cleanup took {:?}",
            started.elapsed()
        );
    }
}
