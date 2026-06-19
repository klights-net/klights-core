//! Thin wrapper over `nftnl` + `mnl` for klights service-routing rule management.
//!
//! Exposes only what [`crate::networking::service_routing`] needs to talk to
//! the kernel `nf_tables` subsystem over a persistent netlink socket:
//!
//! - [`Netfilter::ensure_table`]          — idempotent table create
//! - [`Netfilter::replace_chain`]         — atomic flush + repopulate of one chain
//! - [`Netfilter::replace_chain_rules`]   — atomic flush + repopulate of one chain's rules (chain unchanged)
//! - [`Netfilter::delete_rule_by_handle`] — surgical per-rule delete by kernel handle
//! - [`Netfilter::send`] / [`Batch`]      — group N ops into one netlink transaction
//!
//! Everything else (rule construction, expression building, set/map manipulation)
//! is `nftnl` directly — the wrapper deliberately does not abstract that surface.
//! This module depends on libnftnl and libmnl through the Rust bindings.

use anyhow::{Context, Result, anyhow};
use mnl::Socket;
use nftnl::{Batch as NftBatch, Chain, MsgType, NlMsg, ProtoFamily, Rule, Table};
use std::ffi::CStr;
use std::process::Output;
use std::sync::{Arc, Mutex};

/// Process-wide handle to the nf_tables netlink socket.
///
/// Holds one persistent `NETLINK_NETFILTER` socket. Every send/recv reuses it,
/// so there is no per-call `socket()` overhead. Cloneable cheaply (`Arc`
/// internally) so multiple async tasks can share the handle. The send path is
/// serialized through an internal mutex.
#[derive(Clone)]
pub struct Netfilter {
    inner: Arc<NetfilterInner>,
}

struct NetfilterInner {
    socket: Mutex<Socket>,
    portid: u32,
    task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
}

// SAFETY: mnl::Socket wraps a raw `*mut mnl_socket`. The pointer itself is not
// Send by default, but the underlying file descriptor is safe to use from any
// thread as long as no two threads call into libmnl on the same socket
// concurrently. The Mutex around the Socket enforces that invariant.
unsafe impl Send for NetfilterInner {}
unsafe impl Sync for NetfilterInner {}

impl Netfilter {
    /// Open a netlink socket to the kernel `nf_tables` subsystem.
    /// Requires `CAP_NET_ADMIN` (i.e. root for klights).
    pub fn new(task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>) -> Result<Self> {
        let socket = Socket::new(mnl::Bus::Netfilter).context("open NETLINK_NETFILTER socket")?;
        let portid = socket.portid();
        Ok(Self {
            inner: Arc::new(NetfilterInner {
                socket: Mutex::new(socket),
                portid,
                task_supervisor,
            }),
        })
    }

    /// Idempotent table create. nftables `NEWTABLE` with `NLM_F_CREATE` (no
    /// `NLM_F_EXCL`) succeeds whether the table exists or not, so this is
    /// safe to call on every startup.
    pub async fn ensure_table(&self, family: ProtoFamily, name: &CStr) -> Result<()> {
        let table = Table::new(name, family);
        let mut batch = Batch::new();
        batch.add(&table, MsgType::Add);
        self.send(batch).await
    }

    /// Atomic flush + repopulate of a single chain.
    ///
    /// Implemented as two batches:
    /// 1. **Ensure** — `ADD` chain (idempotent). Creates the chain if it
    ///    doesn't exist; no-op if it does. This guarantees the next batch's
    ///    `DEL` won't fail with `ENOENT` on the first call.
    /// 2. **Rebuild (atomic)** — one batched netlink transaction containing
    ///    `DEL` chain → `ADD` chain → `ADD` rule × N. The kernel applies
    ///    this under a single transaction lock, so observers see either the
    ///    old rule set or the new one — never a partial state.
    ///
    /// Two round-trips total (microseconds each on a local netlink socket).
    /// `nftnl 0.9` does not expose `MsgType::Flush`, so atomic rebuild
    /// goes through `DEL` + re-`ADD` — the same pattern upstream
    /// kube-proxy uses in its nftables mode.
    pub async fn replace_chain(&self, chain: &Chain<'_>, rules: &[Rule<'_>]) -> Result<()> {
        // 1. Ensure the chain exists so the rebuild's DEL is safe.
        let mut create = Batch::new();
        create.add(chain, MsgType::Add);
        self.send(create).await?;

        // 2. Atomic rebuild.
        let mut batch = Batch::new();
        batch.add(chain, MsgType::Del);
        batch.add(chain, MsgType::Add);
        for rule in rules {
            batch.add(rule, MsgType::Add);
        }
        self.send(batch).await
    }

    /// Atomically replace the rules of an existing chain WITHOUT touching
    /// the chain itself. Use this when the chain is referenced by `jump`
    /// or `goto` from other chains — `replace_chain` would fail with
    /// `EBUSY` on the chain `DEL` because the kernel forbids deleting a
    /// chain that has incoming references.
    ///
    /// Implemented as one batched netlink transaction:
    ///   1. `DEL` rule (with chain reference, no rule handle) — the
    ///      kernel interprets this as "flush all rules in chain", same
    ///      as `nft flush chain ...`.
    ///   2. `ADD` rule × N — repopulates the chain.
    ///
    /// Caller must ensure the chain already exists (e.g. via a prior
    /// `replace_chain` of the chain definition).
    pub async fn replace_chain_rules(&self, chain: &Chain<'_>, rules: &[Rule<'_>]) -> Result<()> {
        let mut batch = Batch::new();
        // A handleless Rule serializes to NFTA_RULE_TABLE+NFTA_RULE_CHAIN
        // with no NFTA_RULE_HANDLE; kernel treats that as "flush rules
        // matching the chain".
        let flush_marker = Rule::new(chain);
        batch.add(&flush_marker, MsgType::Del);
        for rule in rules {
            batch.add(rule, MsgType::Add);
        }
        self.send(batch).await
    }

    /// Surgical delete of one rule by its kernel-assigned handle.
    ///
    /// Handles are returned by the kernel when a rule is added; you can read
    /// them back with `nft -a list chain ...` or by parsing the response from
    /// a `GETRULE` query. Use this when you want to remove one rule without
    /// touching the rest of the chain.
    ///
    /// Currently exercised only by the integration tests below; kept as part
    /// of the wrapper API for future use cases that need surgical deletes.
    pub async fn delete_rule_by_handle(&self, chain: &Chain<'_>, handle: u64) -> Result<()> {
        let mut rule = Rule::new(chain);
        rule.set_handle(handle);
        let mut batch = Batch::new();
        batch.add(&rule, MsgType::Del);
        self.send(batch).await
    }

    /// Send and ACK a [`Batch`]. Lower-level escape hatch when the four
    /// helpers above don't fit (e.g. multi-chain transactions, set element
    /// updates, table teardown).
    ///
    /// The actual `send_all` + `recv` loop runs inside `spawn_blocking` so
    /// the tokio runtime thread is never blocked on netlink I/O — even
    /// though netlink syscalls are typically microseconds, this preserves
    /// klights's "event loop never blocks" invariant.
    pub async fn send(&self, batch: Batch) -> Result<()> {
        let inner = self.inner.clone();
        self.inner
            .task_supervisor
            .run_blocking(
                crate::task_supervisor::TaskCategory::Network,
                "netfilter_send_batch",
                move || inner.send_blocking(batch),
            )
            .await
            .context("netlink send task failed")?
    }

    pub async fn nft_output(&self, name: impl Into<String>, args: Vec<String>) -> Result<Output> {
        self.inner
            .task_supervisor
            .run_blocking(
                crate::task_supervisor::TaskCategory::Network,
                name,
                move || std::process::Command::new("nft").args(args).output(),
            )
            .await
            .context("nft command task failed")?
            .context("run nft command")
    }
}

impl NetfilterInner {
    fn send_blocking(&self, batch: Batch) -> Result<()> {
        let finalized = batch.into_inner().finalize();
        let socket = self
            .socket
            .lock()
            .map_err(|_| anyhow!("netlink socket mutex poisoned"))?;

        socket
            .send_all(&finalized)
            .context("send batched nf_tables netlink messages")?;

        let mut buffer = vec![0u8; nftnl::nft_nlmsg_maxsize() as usize];
        let mut expected_seqs = finalized.sequence_numbers();

        // Drain ACKs until every batched message has been confirmed.
        // The kernel may pack multiple ACKs into one recv or fragment them
        // across recvs, so we loop on recv until the expected-seq range is
        // exhausted rather than assuming one-recv-per-ACK. Pattern matches
        // the upstream nftnl `add-rules.rs` example.
        while !expected_seqs.is_empty() {
            let messages = socket.recv(&mut buffer[..]).context("recv nf_tables ACK")?;
            for message in messages {
                let message = message.context("decode netlink message")?;
                let expected_seq = expected_seqs
                    .next()
                    .ok_or_else(|| anyhow!("kernel returned more ACKs than the batch contained"))?;
                mnl::cb_run(message, expected_seq, self.portid)
                    .map_err(|e| anyhow!("nf_tables ACK reported error: {e}"))?;
            }
        }
        Ok(())
    }
}

/// Builder for one batched netlink transaction.
///
/// Wraps [`nftnl::Batch`] so callers don't need a direct dependency on
/// `nftnl`. Add objects in the order you want them applied; the kernel
/// processes the batch atomically when [`Netfilter::send`] is called.
pub struct Batch {
    inner: NftBatch,
    op_count: usize,
}

impl Batch {
    /// Create a new empty batch. Internally writes the
    /// `NFNL_MSG_BATCH_BEGIN` marker so the kernel knows a transaction
    /// has started.
    pub fn new() -> Self {
        Self {
            inner: NftBatch::new(),
            op_count: 0,
        }
    }

    /// Append one nftables object (table, chain, rule, set, set element)
    /// with the given message type (`Add`, `Del`).
    pub fn add<T: NlMsg>(&mut self, obj: &T, msg: MsgType) {
        self.inner.add(obj, msg);
        self.op_count += 1;
    }

    /// Number of caller-added operations in this batch (excludes the
    /// implicit `BEGIN`/`END` markers). Currently only used by tests.
    pub fn len(&self) -> usize {
        self.op_count
    }

    /// True when no caller operations have been added yet. Currently
    /// only used by tests.
    pub fn is_empty(&self) -> bool {
        self.op_count == 0
    }

    fn into_inner(self) -> NftBatch {
        self.inner
    }
}

impl Default for Batch {
    fn default() -> Self {
        Self::new()
    }
}

/// nftnl `hash` expression for ClientIP session affinity.
///
/// Computes `jhash(payload[offset..offset+len]) mod modulus` and stores
/// the result in `dreg`. Used in service routing to select a consistent
/// backend for each source IP: `jhash(ip saddr, 4 bytes) mod N → dreg`,
/// then compare `dreg == i` to pick endpoint `i`.
///
/// This struct implements nftnl's `Expression` trait by directly calling
/// the underlying libnftnl C API (`nftnl_expr_alloc("hash")` + attr setters)
/// since nftnl 0.9 doesn't expose a high-level hash wrapper.
pub struct JhashExpr {
    /// Source register that holds the value to hash (must already be loaded).
    pub sreg: u32,
    /// Destination register that receives the hash result (0..N-1).
    pub dreg: u32,
    /// Number of bytes to hash starting at `offset`.
    pub len: u32,
    /// Result is `hash mod modulus`, selecting endpoint index.
    pub modulus: u32,
    /// Jenkins hash seed. Use a fixed constant for determinism.
    pub seed: u32,
    /// Byte offset within the source register payload to start hashing.
    pub offset: u32,
}

impl nftnl::expr::Expression for JhashExpr {
    fn to_expr(&self, _rule: &nftnl::Rule) -> std::ptr::NonNull<nftnl::nftnl_sys::nftnl_expr> {
        use nftnl::nftnl_sys as sys;
        // SAFETY: every nftnl_sys call inside this block follows the
        // libnftnl contract: `nftnl_expr_alloc` returns either NULL
        // (turned into a panic via `NonNull::new(...).expect(...)`) or
        // a freshly-allocated owned expression; the subsequent setters
        // borrow the expr pointer for the duration of the call only,
        // and the constants written conform to nf_tables.h. Ownership
        // of the returned expression is transferred to the caller via
        // the wrapping NonNull return value.
        unsafe {
            // "hash" is the nftnl expression name for jhash/symmetric hash.
            let expr = sys::nftnl_expr_alloc(c"hash".as_ptr());
            let expr =
                std::ptr::NonNull::new(expr).expect("nftnl_expr_alloc(hash) must not return null");
            // NFT_HASH_JENKINS = 0 (symmetric = 1), see nft_hash_types in nf_tables.h.
            //
            // Use raw `nftnl_expr_set` instead of `_u32` to avoid the libnftnl
            // "value == 0 means unset" optimisation that silently drops the
            // HASH_TYPE attribute when it is 0 (Jenkins). Kernel 7.x rejects
            // hash rules missing the type attribute with EINVAL.
            let nft_hash_jenkins: u32 = 0;
            sys::nftnl_expr_set(
                expr.as_ptr(),
                sys::NFTNL_EXPR_HASH_TYPE as u16,
                &nft_hash_jenkins as *const u32 as *const std::ffi::c_void,
                std::mem::size_of::<u32>() as u32,
            );
            sys::nftnl_expr_set_u32(expr.as_ptr(), sys::NFTNL_EXPR_HASH_SREG as u16, self.sreg);
            sys::nftnl_expr_set_u32(expr.as_ptr(), sys::NFTNL_EXPR_HASH_DREG as u16, self.dreg);
            sys::nftnl_expr_set_u32(expr.as_ptr(), sys::NFTNL_EXPR_HASH_LEN as u16, self.len);
            sys::nftnl_expr_set_u32(
                expr.as_ptr(),
                sys::NFTNL_EXPR_HASH_MODULUS as u16,
                self.modulus,
            );
            sys::nftnl_expr_set_u32(expr.as_ptr(), sys::NFTNL_EXPR_HASH_SEED as u16, self.seed);
            // Raw `nftnl_expr_set` avoids the libnftnl "value == 0 means
            // unset" optimisation (same rationale as HASH_TYPE above).
            // Offset is 0 in our production call sites, and kernel 7.x
            // rejects hash rules without an explicit offset attribute.
            sys::nftnl_expr_set(
                expr.as_ptr(),
                sys::NFTNL_EXPR_HASH_OFFSET as u16,
                &self.offset as *const u32 as *const std::ffi::c_void,
                std::mem::size_of::<u32>() as u32,
            );
            expr
        }
    }
}

// SAFETY: the raw pointer is allocated via libnftnl which is thread-safe
// for per-expression creation; we never alias the pointer across threads.
unsafe impl Send for JhashExpr {}
unsafe impl Sync for JhashExpr {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    // Pure-data tests that do not touch netlink. These run in build.sh on
    // every commit. Real netlink behavior is exercised by integration
    // tests that need root and run via validate.sh.

    #[test]
    fn test_batch_new_returns_empty_batch() {
        let batch = Batch::new();
        assert!(
            batch.is_empty(),
            "fresh batch should report empty (no caller ops added yet)"
        );
        assert_eq!(batch.len(), 0);
    }

    #[test]
    fn test_batch_default_matches_new() {
        assert_eq!(Batch::default().len(), Batch::new().len());
    }

    #[test]
    fn test_batch_add_table_increments_op_count() {
        let name = CString::new("klights-test").unwrap();
        let table = Table::new(&name, ProtoFamily::Inet);
        let mut batch = Batch::new();
        batch.add(&table, MsgType::Add);
        assert_eq!(batch.len(), 1, "one Add should bring op_count to 1");
        assert!(!batch.is_empty(), "batch with one Add should not be empty");
    }

    #[test]
    fn test_batch_add_three_messages_counts_three() {
        let name = CString::new("klights-test").unwrap();
        let table = Table::new(&name, ProtoFamily::Inet);

        let mut batch = Batch::new();
        batch.add(&table, MsgType::Add);
        batch.add(&table, MsgType::Add);
        batch.add(&table, MsgType::Add);

        assert_eq!(batch.len(), 3, "three Adds should yield op_count 3");
    }

    #[test]
    fn test_batch_mixed_add_and_del_both_count() {
        let name = CString::new("klights-test").unwrap();
        let table = Table::new(&name, ProtoFamily::Inet);

        let mut batch = Batch::new();
        batch.add(&table, MsgType::Add);
        batch.add(&table, MsgType::Del);

        assert_eq!(batch.len(), 2, "Add + Del should yield op_count 2");
    }
}

// Integration tests — require root (CAP_NET_ADMIN) plus the `nft` binary
// for verification. Marked `#[ignore]` so `cargo test` skips them by default;
// run with `sudo -E cargo test -- --ignored networking::netfilter`.
//
// Each test isolates on its own `inet klights_it_<rand>` table and tears it
// down on drop so concurrent test runs don't collide. If a test panics
// before the guard runs, leftover tables can be cleaned up with
// `sudo nft list ruleset | grep klights_it_ | awk ...`.
#[cfg(test)]
mod integration_tests {
    use super::*;
    use std::ffi::CString;
    use std::process::Command;

    /// Drops the test table on scope exit, even on panic.
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

    fn unique_table_name(prefix: &str) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        format!("{prefix}_{}_{}", std::process::id(), nanos)
    }

    fn nft_table_exists(name: &str) -> bool {
        Command::new("nft")
            .args(["list", "table", "inet", name])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    fn test_task_supervisor() -> Arc<crate::task_supervisor::TaskSupervisor> {
        Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ))
    }

    #[tokio::test]
    #[ignore]
    async fn test_netfilter_new_opens_socket_when_run_as_root() {
        let nf = Netfilter::new(test_task_supervisor())
            .expect("opening NETLINK_NETFILTER socket needs CAP_NET_ADMIN");
        // Just confirm we can clone the handle (verifies Arc wiring).
        let _clone = nf.clone();
    }

    #[tokio::test]
    #[ignore]
    async fn test_ensure_table_creates_and_is_idempotent() {
        let name = unique_table_name("klights_it_ensure");
        let _guard = TableGuard { name: name.clone() };

        let nf = Netfilter::new(test_task_supervisor()).expect("Netfilter::new");
        let cname = CString::new(name.as_str()).unwrap();

        nf.ensure_table(ProtoFamily::Inet, &cname)
            .await
            .expect("first ensure_table");
        assert!(
            nft_table_exists(&name),
            "table should exist in the kernel after first ensure_table"
        );

        // Second call must not error — NEWTABLE without F_EXCL is idempotent.
        nf.ensure_table(ProtoFamily::Inet, &cname)
            .await
            .expect("second ensure_table should be idempotent");
        assert!(
            nft_table_exists(&name),
            "table should still exist after second ensure_table"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_replace_chain_rebuilds_atomically() {
        let table_name = unique_table_name("klights_it_replace");
        let _guard = TableGuard {
            name: table_name.clone(),
        };

        let nf = Netfilter::new(test_task_supervisor()).expect("Netfilter::new");
        let table_c = CString::new(table_name.as_str()).unwrap();
        nf.ensure_table(ProtoFamily::Inet, &table_c)
            .await
            .expect("ensure_table");

        // Build a regular chain (no hook) so we can populate it without
        // needing nat hook semantics.
        let chain_name = CString::new("test-chain").unwrap();
        let table = Table::new(&table_c, ProtoFamily::Inet);
        let chain = Chain::new(&chain_name, &table);

        // Round 1: populate with two rules (each just an `accept` verdict).
        let r1 = {
            let mut r = Rule::new(&chain);
            r.add_expr(&nftnl::nft_expr!(verdict accept));
            r
        };
        let r2 = {
            let mut r = Rule::new(&chain);
            r.add_expr(&nftnl::nft_expr!(verdict accept));
            r
        };
        nf.replace_chain(&chain, &[r1, r2])
            .await
            .expect("first replace_chain");

        let listing = Command::new("nft")
            .args(["list", "chain", "inet", &table_name, "test-chain"])
            .output()
            .expect("nft list chain");
        let listing = String::from_utf8_lossy(&listing.stdout);
        let rule_count_round1 = listing.matches("accept").count();
        assert!(
            rule_count_round1 >= 2,
            "round 1 should have at least 2 accept rules, got listing:\n{listing}"
        );

        // Round 2: replace with a single rule. Atomic rebuild means we
        // observe exactly one rule, never four (old + new) and never zero
        // (mid-flush).
        let r_only = {
            let mut r = Rule::new(&chain);
            r.add_expr(&nftnl::nft_expr!(verdict accept));
            r
        };
        nf.replace_chain(&chain, &[r_only])
            .await
            .expect("second replace_chain");

        let listing = Command::new("nft")
            .args(["list", "chain", "inet", &table_name, "test-chain"])
            .output()
            .expect("nft list chain");
        let listing = String::from_utf8_lossy(&listing.stdout);
        let rule_count_round2 = listing.matches("accept").count();
        assert_eq!(
            rule_count_round2, 1,
            "round 2 must observe exactly the one new rule, got listing:\n{listing}"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_delete_rule_by_handle_removes_only_that_rule() {
        let table_name = unique_table_name("klights_it_delete");
        let _guard = TableGuard {
            name: table_name.clone(),
        };

        let nf = Netfilter::new(test_task_supervisor()).expect("Netfilter::new");
        let table_c = CString::new(table_name.as_str()).unwrap();
        nf.ensure_table(ProtoFamily::Inet, &table_c)
            .await
            .expect("ensure_table");

        let chain_name = CString::new("del-chain").unwrap();
        let table = Table::new(&table_c, ProtoFamily::Inet);
        let chain = Chain::new(&chain_name, &table);

        // Seed the chain with three rules so we can pick the middle one.
        let rules: Vec<Rule<'_>> = (0..3)
            .map(|_| {
                let mut r = Rule::new(&chain);
                r.add_expr(&nftnl::nft_expr!(verdict accept));
                r
            })
            .collect();
        nf.replace_chain(&chain, &rules)
            .await
            .expect("seed replace_chain");

        // Read the handle of the second rule via `nft -a list chain ...`.
        let listing = Command::new("nft")
            .args(["-a", "list", "chain", "inet", &table_name, "del-chain"])
            .output()
            .expect("nft -a list chain");
        let listing = String::from_utf8_lossy(&listing.stdout);
        // Each rule line ends with `# handle <N>`. Pick the second one.
        // `nft -a list chain` puts a `# handle <N>` comment on every
        // object including the table/chain itself; we only want the rule
        // handles, which sit on lines containing the `accept` verdict.
        let rule_handles: Vec<u64> = listing
            .lines()
            .filter(|l| l.contains("accept"))
            .filter_map(|l| l.rsplit_once("# handle "))
            .filter_map(|(_, h)| h.trim().parse::<u64>().ok())
            .collect();
        assert_eq!(
            rule_handles.len(),
            3,
            "expected 3 rule handles, got {rule_handles:?} from listing:\n{listing}"
        );
        let target_handle = rule_handles[1];

        nf.delete_rule_by_handle(&chain, target_handle)
            .await
            .expect("delete_rule_by_handle");

        // Verify exactly two rules remain and the deleted handle is gone.
        let listing = Command::new("nft")
            .args(["-a", "list", "chain", "inet", &table_name, "del-chain"])
            .output()
            .expect("nft -a list chain post-delete");
        let listing = String::from_utf8_lossy(&listing.stdout);
        let remaining: Vec<u64> = listing
            .lines()
            .filter(|l| l.contains("accept"))
            .filter_map(|l| l.rsplit_once("# handle "))
            .filter_map(|(_, h)| h.trim().parse::<u64>().ok())
            .collect();
        assert_eq!(
            remaining.len(),
            2,
            "should have exactly 2 rules after deleting one, got {remaining:?} from:\n{listing}"
        );
        assert!(
            !remaining.contains(&target_handle),
            "deleted handle {target_handle} should not appear in {remaining:?}"
        );
    }
}
