/// Parse ports from query string (e.g., "ports=8080&ports=9090")
pub fn parse_ports_query(query: &str) -> Vec<u16> {
    let mut ports = Vec::new();

    // Parse manually: split by & and then by =
    for pair in query.split('&') {
        let parts: Vec<&str> = pair.split('=').collect();
        if parts.len() == 2
            && parts[0] == "ports"
            && let Ok(port) = parts[1].parse::<u16>()
        {
            ports.push(port);
        }
    }

    ports
}

/// Calculate channel ID for portforward protocol
/// Each port gets 2 channels: data (even) and error (odd)
/// Port index 0: data=0, error=1
/// Port index 1: data=2, error=3
/// etc.
pub fn port_channel_id(port_index: usize, is_error: bool) -> u8 {
    ((port_index * 2) + if is_error { 1 } else { 0 }) as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test parsing ports from query string
    #[test]
    fn test_parse_ports_single() {
        let query = "ports=8080";
        let ports = parse_ports_query(query);
        assert_eq!(ports, vec![8080]);
    }

    #[test]
    fn test_parse_ports_multiple() {
        let query = "ports=8080&ports=9090&ports=3000";
        let ports = parse_ports_query(query);
        assert_eq!(ports, vec![8080, 9090, 3000]);
    }

    #[test]
    fn test_parse_ports_empty() {
        let query = "";
        let ports = parse_ports_query(query);
        assert_eq!(ports, Vec::<u16>::new());
    }

    #[test]
    fn test_parse_ports_invalid() {
        let query = "ports=invalid&ports=8080";
        let ports = parse_ports_query(query);
        // Should skip invalid and return valid ones
        assert_eq!(ports, vec![8080]);
    }

    /// Test channel ID mapping for portforward protocol
    #[test]
    fn test_channel_id_data_port0() {
        // Port index 0, data stream = channel 0
        let channel_id = port_channel_id(0, false);
        assert_eq!(channel_id, 0);
    }

    #[test]
    fn test_channel_id_error_port0() {
        // Port index 0, error stream = channel 1
        let channel_id = port_channel_id(0, true);
        assert_eq!(channel_id, 1);
    }

    #[test]
    fn test_channel_id_data_port1() {
        // Port index 1, data stream = channel 2
        let channel_id = port_channel_id(1, false);
        assert_eq!(channel_id, 2);
    }

    #[test]
    fn test_channel_id_error_port1() {
        // Port index 1, error stream = channel 3
        let channel_id = port_channel_id(1, true);
        assert_eq!(channel_id, 3);
    }
}
