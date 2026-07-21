//! Rootless / hybrid network surface.
//!
//! The live bridge/veth/IPAM CNI datapath lives on `RootlessNetworkPlane`.
//! Service routing and pod-endpoint resolution remain shared across modes.
//!
//! Service routing and pod-endpoint resolution are reused unchanged —
//! `NftServiceRouter` and `SqlitePodEndpointResolver` work in both
//! modes.

pub mod pasta;
