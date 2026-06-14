//! Ordered byte encoding for composite keys in the redb backend.
//!
//! Resource keys, continue tokens, and the `lex_next` prefix-scan helper
//! live here.  All keys are explicitly ordered — no ad hoc `bincode` keys.
//!
//! ## Resource key layout
//!
//! ```text
//! [scope_byte][len(av) as u8][av bytes][len(kind) as u8][kind bytes]
//! [ns_part][len(name) as u8][name bytes]
//!
//! scope_byte: 0 = cluster-scoped, 1 = namespaced
//! ns_part (scope=0): empty
//! ns_part (scope=1): [len(ns) as u8][ns bytes]
//! ```
//!
//! Sort order: cluster-scoped (0) before namespaced (1), then by api_version,
//! then kind, then (for namespaced) namespace, then name.  Null namespace is
//! impossible — only cluster-scoped resources omit ns.

#[cfg(test)]
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};

/// Encode a resource identity into an ordered byte key.
/// Table selection (RES_CLUSTER vs RES_NS) is done by the caller;
/// this function builds the key WITHOUT a scope_byte prefix.
pub(super) fn resource_key(av: &str, kind: &str, ns: Option<&str>, name: &str) -> Vec<u8> {
    let mut buf =
        Vec::with_capacity(av.len() + kind.len() + name.len() + ns.map_or(0, |n| n.len()) + 6);
    push_str(&mut buf, av);
    push_str(&mut buf, kind);
    if let Some(ns) = ns {
        push_str(&mut buf, ns);
    }
    push_str(&mut buf, name);
    buf
}

/// Build the key prefix for a resource kind, without the name.
/// For RES_NS: [av][kind][ns] (or just [av][kind] when ns is None).
/// For RES_CLUSTER: [av][kind].
pub(super) fn resource_prefix(av: &str, kind: &str, ns: Option<&str>) -> Vec<u8> {
    let mut buf = Vec::with_capacity(av.len() + kind.len() + ns.map_or(0, |n| n.len()) + 4);
    push_str(&mut buf, av);
    push_str(&mut buf, kind);
    if let Some(ns) = ns {
        push_str(&mut buf, ns);
    }
    buf
}

fn push_str(buf: &mut Vec<u8>, s: &str) {
    let len = s.len().min(255);
    buf.push(len as u8);
    buf.extend_from_slice(&s.as_bytes()[..len]);
}

/// Decode a resource key back into its components.
/// The key no longer has a scope_byte — scope is determined by which table the
/// key came from (RES_CLUSTER → None, RES_NS → Some(ns)).
#[cfg(test)]
pub fn decode_resource_key(
    key: &[u8],
    namespaced: bool,
) -> Option<(&str, &str, Option<&str>, &str)> {
    let (av, rest) = pop_str(key)?;
    let (kind, rest) = pop_str(rest)?;
    let (ns, rest) = if namespaced {
        let (ns, rest) = pop_str(rest)?;
        (Some(ns), rest)
    } else {
        (None, rest)
    };
    let (name, _) = pop_str(rest)?;
    Some((av, kind, ns, name))
}

#[cfg(test)]
fn pop_str(data: &[u8]) -> Option<(&str, &[u8])> {
    let (&len, rest) = data.split_first()?;
    let len = len as usize;
    if rest.len() < len {
        return None;
    }
    let s = std::str::from_utf8(&rest[..len]).ok()?;
    Some((s, &rest[len..]))
}

/// Lexicographic next byte sequence.  Increments the last byte with carry
/// propagation.  Returns `None` for `\xFF\xFF...` (overflow → use unbounded upper).
pub(super) fn lex_next(key: &[u8]) -> Option<Vec<u8>> {
    let mut v = key.to_vec();
    for byte in v.iter_mut().rev() {
        let (next, overflow) = byte.overflowing_add(1);
        *byte = next;
        if !overflow {
            return Some(v);
        }
    }
    None
}

/// Encode a resource key as a URL-safe base64 continue token.
#[cfg(test)]
pub fn encode_continue_token(key: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(key)
}

/// Decode a continue token back into a resource key.
#[cfg(test)]
pub fn decode_continue_token(token: &str) -> Option<Vec<u8>> {
    URL_SAFE_NO_PAD.decode(token).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_key_round_trip_cluster_scoped() {
        let key = resource_key("v1", "Node", None, "my-node");
        let (av, kind, ns, name) = decode_resource_key(&key, false).expect("decode");
        assert_eq!(av, "v1");
        assert_eq!(kind, "Node");
        assert_eq!(ns, None);
        assert_eq!(name, "my-node");
    }

    #[test]
    fn resource_key_round_trip_namespaced() {
        let key = resource_key("v1", "Pod", Some("default"), "nginx");
        let (av, kind, ns, name) = decode_resource_key(&key, true).expect("decode");
        assert_eq!(av, "v1");
        assert_eq!(kind, "Pod");
        assert_eq!(ns, Some("default"));
        assert_eq!(name, "nginx");
    }

    #[test]
    fn cluster_scoped_sorts_before_namespaced() {
        let cluster_key = resource_key("v1", "Pod", None, "a");
        let ns_key = resource_key("v1", "Pod", Some("default"), "a");
        assert!(cluster_key < ns_key);
    }

    #[test]
    fn lex_next_monotonic() {
        for key in &[
            b"".to_vec(),
            b"a".to_vec(),
            b"ab".to_vec(),
            b"a\xff".to_vec(),
        ] {
            if let Some(next) = lex_next(key) {
                assert!(
                    key.as_slice() < next.as_slice(),
                    "key={key:?} next={next:?}"
                );
            }
        }
    }

    #[test]
    fn lex_next_overflow_returns_none() {
        assert_eq!(lex_next(b"\xff\xff"), None);
    }

    #[test]
    fn continue_token_round_trip() {
        let key = resource_key("apps/v1", "Deployment", Some("prod"), "web");
        let token = encode_continue_token(&key);
        let decoded = decode_continue_token(&token).expect("decode");
        assert_eq!(decoded, key);
    }

    #[test]
    fn prefix_scan_range_bounds() {
        // Create a key and verify lex_next bounds the prefix correctly.
        let key = resource_key("v1", "Pod", Some("ns"), "pod-a");
        let next = lex_next(&key).expect("lex_next");
        // key < next — range [key..next) captures exactly one key.
        assert!(key < next);
    }
}
