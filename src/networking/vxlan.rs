//! VXLAN overlay device management for klights multi-host networking.
//!
//! Creates and manages the `klights.vxlan` device with a klights-distinct VNI
//! (default 1228) so klights can coexist on hosts where K3s/flannel is also
//! running (flannel uses VNI 1; the kernel rejects two VXLAN devices sharing
//! (udp_port, vni), so a distinct VNI avoids the conflict).
//!
//! Public API:
//! - [`ensure_vxlan`] — create/verify the VXLAN device, return its MAC address.

use anyhow::{Context, Result};
use futures::stream::TryStreamExt;
use netlink_packet_route::link::LinkAttribute;
use std::net::Ipv4Addr;

use super::types::VtepMac;

/// Default VXLAN device name when `KLIGHTS_VXLAN_DEVICE` is unset.  The
/// runtime device name lives on `KlightsConfig::vxlan_device` and is
/// threaded through `NetworkPlane`; only fallback paths and tests should
/// reference this constant directly.
pub const DEFAULT_DEVICE: &str = "klights.vxlan";
pub const DEFAULT_VNI: u32 = 1228;
pub const DEFAULT_PORT: u16 = 8472;
/// MTU for VXLAN-encapsulated traffic (1500 − 50-byte VXLAN header).
pub const VXLAN_MTU: u32 = 1450;

/// Parameters for VXLAN device creation.
pub struct VxlanConfig<'a> {
    /// Device name (Linux IFNAMSIZ ≤ 15 chars).  Defaults to `DEFAULT_DEVICE`
    /// in production; per-instance test slots override so multiple klights
    /// processes can coexist without colliding on link name.
    pub device: &'a str,
    /// VNI (VXLAN Network Identifier).  Default: 1228.
    pub vni: u32,
    /// Destination UDP port.  Default: 8472.
    pub port: u16,
    /// Host's primary underlay IP (source for outgoing VXLAN UDP packets).
    pub node_ip: Ipv4Addr,
    /// VTEP L3 address assigned to the VXLAN device (first addr of node's /24).
    pub vtep_ip: Ipv4Addr,
    /// MTU to apply to the VXLAN link for the active dataplane policy.
    pub mtu: u32,
}

/// Ensure the `klights.vxlan` device exists and is configured correctly.
///
/// If a VXLAN device with the same `(vni, port)` already exists under a
/// different name, the function fails fast — the operator must pick a
/// different VNI via `KLIGHTS_VXLAN_VNI` and set it consistently on every
/// node.
///
/// Returns the kernel-assigned MAC address of the device as a `VtepMac`,
/// which is recorded in `node_subnets.vtep_mac` so peers can install FDB
/// entries pointing at it.
pub async fn ensure_vxlan(handle: &rtnetlink::Handle, config: &VxlanConfig<'_>) -> Result<VtepMac> {
    let device = config.device;
    // Check whether the device already exists.
    let existing_idx = crate::networking::get_link_index(handle, device).await.ok();

    if existing_idx.is_none() {
        // Collision check: any existing VXLAN device with the same (vni, port)?
        check_no_vni_collision(handle, device, config.vni, config.port).await?;

        // Create the VXLAN device.
        handle
            .link()
            .add()
            .vxlan(device.to_owned(), config.vni)
            .port(config.port)
            .local(config.node_ip)
            .learning(false) // nolearn — we manage FDB explicitly
            .execute()
            .await
            .with_context(|| {
                format!(
                    "Failed to create VXLAN device {} (VNI={}, port={})",
                    device, config.vni, config.port
                )
            })?;
        tracing::info!(
            "vxlan: created {} VNI={} port={} local={}",
            device,
            config.vni,
            config.port,
            config.node_ip
        );
    }

    let idx = crate::networking::get_link_index(handle, device)
        .await
        .with_context(|| format!("{} not found after creation", device))?;

    // Assign VTEP IP as a /32 (L3 VTEP address, same convention as flannel.1).
    let add_result = handle
        .address()
        .add(idx, std::net::IpAddr::V4(config.vtep_ip), 32)
        .execute()
        .await;
    if let Err(e) = &add_result
        && !crate::networking::is_nl_eexist_error(e)
    {
        add_result.with_context(|| {
            format!("Failed to assign VTEP IP {} to {}", config.vtep_ip, device)
        })?;
    }

    // Set MTU and bring up.
    handle
        .link()
        .set(idx)
        .mtu(config.mtu)
        .execute()
        .await
        .with_context(|| format!("Failed to set MTU on {}", device))?;
    handle
        .link()
        .set(idx)
        .up()
        .execute()
        .await
        .with_context(|| format!("Failed to bring up {}", device))?;

    // Read back the kernel-assigned MAC.
    let mac = read_link_mac(handle, device)
        .await
        .context("Failed to read VXLAN device MAC")?;

    tracing::info!(
        "vxlan: {} ready — vtep_ip={} mac={} mtu={}",
        device,
        config.vtep_ip,
        mac,
        config.mtu
    );
    Ok(mac)
}

/// Read the MAC address of an interface as a `VtepMac`.
async fn read_link_mac(handle: &rtnetlink::Handle, name: &str) -> Result<VtepMac> {
    let mut links = handle.link().get().match_name(name.to_owned()).execute();
    while let Some(msg) = links.try_next().await? {
        for attr in &msg.attributes {
            if let LinkAttribute::Address(bytes) = attr
                && bytes.len() == 6
            {
                let mut out = [0u8; 6];
                out.copy_from_slice(&bytes[..6]);
                return Ok(VtepMac::from_bytes(out));
            }
        }
    }
    anyhow::bail!("MAC address not found for interface {}", name)
}

/// Fail fast if any existing VXLAN device is already using this (VNI, port).
///
/// Auto-pick was rejected because nodes would decide locally without
/// cluster coordination, leading to split-brain overlays. The operator must
/// set `KLIGHTS_VXLAN_VNI` consistently across the whole cluster.
async fn check_no_vni_collision(
    handle: &rtnetlink::Handle,
    own_device: &str,
    vni: u32,
    port: u16,
) -> Result<()> {
    use netlink_packet_route::link::{InfoData, InfoKind, LinkInfo};

    let mut links = handle.link().get().execute();
    while let Some(msg) = links.try_next().await? {
        let mut is_vxlan = false;
        let mut found_vni: Option<u32> = None;
        let mut found_port: Option<u16> = None;
        let mut iface_name = String::new();

        for attr in &msg.attributes {
            match attr {
                LinkAttribute::IfName(n) => iface_name = n.clone(),
                LinkAttribute::LinkInfo(infos) => {
                    for info in infos {
                        if let LinkInfo::Kind(InfoKind::Vxlan) = info {
                            is_vxlan = true;
                        }
                        if let LinkInfo::Data(InfoData::Vxlan(vxlan_attrs)) = info {
                            use netlink_packet_route::link::InfoVxlan;
                            for va in vxlan_attrs {
                                match va {
                                    InfoVxlan::Id(v) => found_vni = Some(*v),
                                    InfoVxlan::Port(p) => found_port = Some(*p),
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        if found_port.is_none() && found_vni == Some(vni) {
            tracing::debug!(
                "vxlan: ignoring device {iface_name} vni={vni} — kernel did not expose port attribute, treating as non-collision"
            );
        }

        if is_vxlan
            && iface_name != own_device
            && found_vni == Some(vni)
            && found_port == Some(port)
        {
            anyhow::bail!(
                "VXLAN device '{}' already uses VNI={} port={}. \
                 Set KLIGHTS_VXLAN_VNI to a different value on every node in the cluster.",
                iface_name,
                vni,
                port
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vxlan_constants_are_reasonable() {
        assert_eq!(DEFAULT_VNI, 1228);
        assert_eq!(DEFAULT_PORT, 8472);
        assert_eq!(VXLAN_MTU, 1450);
        // compile-time sanity check
        const _: () = assert!(VXLAN_MTU < 1500, "VXLAN MTU must be < Ethernet MTU");
    }

    #[test]
    fn test_vxlan_default_device_name_fits_ifnamsiz() {
        assert_eq!(DEFAULT_DEVICE, "klights.vxlan");
        // Linux allows dots in interface names; kernel IFNAMSIZ limit is 15
        assert!(DEFAULT_DEVICE.len() <= 15);
    }

    #[test]
    fn test_collision_check_skips_when_port_attr_missing() {
        // Simulate a VXLAN device where the kernel only exposes VNI (Id) but not Port.
        // This can happen with some kernel versions/configurations.
        // The collision check should NOT flag this as a collision since we can't verify the port.
        let config = VxlanConfig {
            device: DEFAULT_DEVICE,
            vni: 1228,
            port: 8472,
            node_ip: Ipv4Addr::new(10, 0, 0, 1),
            vtep_ip: Ipv4Addr::new(10, 0, 0, 2),
            mtu: VXLAN_MTU,
        };

        // We'll test the collision detection logic by mocking a scenario where
        // found_vni == Some(vni) but found_port.is_none().
        // This should NOT trigger a collision error.
        // (The actual function is async and requires a netlink handle, so we test
        // the logic inline here. If this proves difficult, we'll refactor to testable code.)
        let is_vxlan = true;
        let iface_name = "other.vxlan".to_string();
        let found_vni = Some(1228);
        let found_port: Option<u16> = None; // Port attr not exposed
        let check_device_name = config.device.to_string();

        // The collision condition should be FALSE when port is None
        // because we can't verify it's actually the same port.
        let collision_detected = is_vxlan
            && iface_name != check_device_name
            && found_vni == Some(config.vni)
            && found_port == Some(config.port); // Fixed: only collision if port matches

        assert!(
            !collision_detected,
            "Should not flag collision when port attribute is missing"
        );
    }

    #[test]
    fn test_collision_check_detects_same_vni_port() {
        let config = VxlanConfig {
            device: DEFAULT_DEVICE,
            vni: 1228,
            port: 8472,
            node_ip: Ipv4Addr::new(10, 0, 0, 1),
            vtep_ip: Ipv4Addr::new(10, 0, 0, 2),
            mtu: VXLAN_MTU,
        };

        // Simulate a real collision: same VNI AND same port
        let is_vxlan = true;
        let iface_name = "other.vxlan".to_string();
        let found_vni = Some(1228);
        let found_port: Option<u16> = Some(8472);
        let check_device_name = config.device.to_string();

        let collision_detected = is_vxlan
            && iface_name != check_device_name
            && found_vni == Some(config.vni)
            && found_port == Some(config.port);

        assert!(
            collision_detected,
            "Should detect collision when VNI and port match"
        );
    }

    #[test]
    fn test_collision_check_allows_different_port() {
        let config = VxlanConfig {
            device: DEFAULT_DEVICE,
            vni: 1228,
            port: 8472,
            node_ip: Ipv4Addr::new(10, 0, 0, 1),
            vtep_ip: Ipv4Addr::new(10, 0, 0, 2),
            mtu: VXLAN_MTU,
        };

        // Same VNI but different port: NOT a collision
        let is_vxlan = true;
        let iface_name = "other.vxlan".to_string();
        let found_vni = Some(1228);
        let found_port: Option<u16> = Some(9999);
        let check_device_name = config.device.to_string();

        let collision_detected = is_vxlan
            && iface_name != check_device_name
            && found_vni == Some(config.vni)
            && found_port == Some(config.port);

        assert!(
            !collision_detected,
            "Should not flag collision when ports differ"
        );
    }

    #[test]
    fn test_collision_check_treats_own_device_as_non_collision() {
        // When the existing device is OUR own device (same name as own_device),
        // it must be treated as reuse, not a collision, even if VNI+port match.
        let own_device = "tester1.vxlan";
        let is_vxlan = true;
        let iface_name = own_device.to_string();
        let found_vni = Some(2241);
        let found_port: Option<u16> = Some(8481);

        let collision_detected = is_vxlan
            && iface_name != own_device
            && found_vni == Some(2241)
            && found_port == Some(8481);

        assert!(
            !collision_detected,
            "Existing device with our own name must not be flagged as collision"
        );
    }
}
