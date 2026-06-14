/// Parse K8s CPU resource format to nanoseconds.
/// Supports: "1" (1 core), "500m" (500 millicores = 0.5 cores), "1.5" (1.5 cores)
/// Returns CPU quota in nanoseconds (for cgroup cpu.max quota value).
pub fn parse_cpu_resource(cpu_str: &str) -> Option<i64> {
    if cpu_str.is_empty() {
        return None;
    }

    if let Some(millis) = cpu_str.strip_suffix('m') {
        // Millicores: "500m" = 500 millicores = 0.5 cores
        millis.parse::<i64>().ok().map(|m| m * 1_000_000) // 1 millicore = 1ms = 1,000,000 ns
    } else {
        // Cores: "1" or "1.5"
        cpu_str
            .parse::<f64>()
            .ok()
            .map(|cores| (cores * 1_000_000_000.0) as i64) // 1 core = 100ms period = 1,000,000,000 ns
    }
}

/// Parse K8s memory resource format to bytes.
/// Supports: "128974848" (bytes), "129e6" (scientific), "129M" (MB), "123Mi" (MiB),
///           "1G" (GB), "1Gi" (GiB), "1T" (TB), "1Ti" (TiB)
pub fn parse_memory_resource(mem_str: &str) -> Option<i64> {
    if mem_str.is_empty() {
        return None;
    }

    // Try parsing as plain bytes first
    if let Ok(bytes) = mem_str.parse::<i64>() {
        return Some(bytes);
    }

    // Scientific notation (e.g., "129e6" = 129 * 10^6)
    if let Ok(bytes) = mem_str.parse::<f64>() {
        return Some(bytes as i64);
    }

    // Parse with suffix
    let (num_str, multiplier) = if let Some(rest) = mem_str.strip_suffix("Ki") {
        (rest, 1024_i64)
    } else if let Some(rest) = mem_str.strip_suffix("Mi") {
        (rest, 1024 * 1024)
    } else if let Some(rest) = mem_str.strip_suffix("Gi") {
        (rest, 1024 * 1024 * 1024)
    } else if let Some(rest) = mem_str.strip_suffix("Ti") {
        (rest, 1024 * 1024 * 1024 * 1024)
    } else if let Some(rest) = mem_str.strip_suffix("Pi") {
        (rest, 1024_i64.pow(5))
    } else if let Some(rest) = mem_str.strip_suffix("Ei") {
        (rest, 1024_i64.pow(6))
    } else if let Some(rest) = mem_str.strip_suffix('k') {
        (rest, 1000_i64)
    } else if let Some(rest) = mem_str.strip_suffix('M') {
        (rest, 1_000_000)
    } else if let Some(rest) = mem_str.strip_suffix('G') {
        (rest, 1_000_000_000)
    } else if let Some(rest) = mem_str.strip_suffix('T') {
        (rest, 1_000_000_000_000)
    } else if let Some(rest) = mem_str.strip_suffix('P') {
        (rest, 1_000_000_000_000_000)
    } else if let Some(rest) = mem_str.strip_suffix('E') {
        (rest, 1_000_000_000_000_000_000)
    } else {
        return None;
    };

    num_str.parse::<i64>().ok().map(|n| n * multiplier)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cpu_resource_cores() {
        assert_eq!(parse_cpu_resource("1"), Some(1_000_000_000)); // 1 core = 1,000,000,000 ns
        assert_eq!(parse_cpu_resource("2"), Some(2_000_000_000)); // 2 cores
        assert_eq!(parse_cpu_resource("0.5"), Some(500_000_000)); // 0.5 cores = 500ms
    }

    #[test]
    fn test_parse_cpu_resource_millicores() {
        assert_eq!(parse_cpu_resource("500m"), Some(500_000_000)); // 500 millicores = 0.5 cores = 500ms
        assert_eq!(parse_cpu_resource("1000m"), Some(1_000_000_000)); // 1000m = 1 core
        assert_eq!(parse_cpu_resource("250m"), Some(250_000_000)); // 250m = 0.25 cores
    }

    #[test]
    fn test_parse_cpu_resource_invalid() {
        assert_eq!(parse_cpu_resource(""), None);
        assert_eq!(parse_cpu_resource("invalid"), None);
    }

    #[test]
    fn test_parse_memory_resource_bytes() {
        assert_eq!(parse_memory_resource("128974848"), Some(128974848));
        assert_eq!(parse_memory_resource("1024"), Some(1024));
    }

    #[test]
    fn test_parse_memory_resource_binary_units() {
        assert_eq!(parse_memory_resource("128Mi"), Some(128 * 1024 * 1024));
        assert_eq!(parse_memory_resource("1Gi"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_memory_resource("2Gi"), Some(2 * 1024 * 1024 * 1024));
        assert_eq!(parse_memory_resource("512Ki"), Some(512 * 1024));
    }

    #[test]
    fn test_parse_memory_resource_decimal_units() {
        assert_eq!(parse_memory_resource("100M"), Some(100_000_000));
        assert_eq!(parse_memory_resource("1G"), Some(1_000_000_000));
        assert_eq!(parse_memory_resource("500k"), Some(500_000));
    }

    #[test]
    fn test_parse_memory_resource_invalid() {
        assert_eq!(parse_memory_resource(""), None);
        assert_eq!(parse_memory_resource("invalid"), None);
    }
}
