//! Kubernetes generated-name policy.

/// Append the existing five-character lowercase alphanumeric suffix.
pub fn generate(prefix: &str) -> String {
    use rand::distr::{Distribution, Uniform};

    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    let range = Uniform::new(0, CHARSET.len()).expect("valid range");
    let suffix: String = (0..5)
        .map(|_| CHARSET[range.sample(&mut rng)] as char)
        .collect();
    format!("{prefix}{suffix}")
}

#[cfg(test)]
mod tests {
    use super::generate;

    #[test]
    fn suffix_matches_kubernetes_name_contract() {
        let name = generate("my-app.v2-");
        let suffix = &name["my-app.v2-".len()..];
        assert_eq!(suffix.len(), 5);
        assert!(
            suffix
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        );
    }
}
