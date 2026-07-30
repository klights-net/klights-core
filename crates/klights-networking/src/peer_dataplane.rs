//! Root-mode peer dataplane behind the backend-neutral `PeerRouter` port.

use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use anyhow::{Context, Result};
use futures::TryStreamExt;
use tokio_util::sync::CancellationToken;

use crate::RootDatapath;
use crate::dataplane_health::DataplaneHealth;
use crate::device_state::{self, LinkKind};
use crate::wireguard::{
    self, DataplaneEncryption, WireGuardBootConfig, WireGuardController, WireGuardDeviceConfig,
    WireGuardIdentity,
};

pub(crate) type WireGuardPeerStepFuture<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>>;

pub(crate) async fn apply_wireguard_peer_route_with_rollback<
    'a,
    ApplyPeer,
    ApplyRoute,
    RemoveRoute,
    RemovePeer,
>(
    apply_peer: ApplyPeer,
    apply_route: ApplyRoute,
    remove_route: RemoveRoute,
    remove_peer: RemovePeer,
) -> Result<()>
where
    ApplyPeer: FnOnce() -> WireGuardPeerStepFuture<'a>,
    ApplyRoute: FnOnce() -> WireGuardPeerStepFuture<'a>,
    RemoveRoute: FnOnce() -> WireGuardPeerStepFuture<'a>,
    RemovePeer: FnOnce() -> WireGuardPeerStepFuture<'a>,
{
    apply_peer().await?;
    let Err(apply_error) = apply_route().await else {
        return Ok(());
    };

    let route_rollback_error = remove_route().await.err();
    let peer_rollback_error = remove_peer().await.err();
    let mut message = format!("{apply_error:#}");
    if let Some(error) = route_rollback_error {
        message.push_str(&format!("; route rollback failed: {error:#}"));
    }
    if let Some(error) = peer_rollback_error {
        message.push_str(&format!("; peer rollback failed: {error:#}"));
    }
    Err(anyhow::anyhow!(message))
}

pub struct RootPeerDataplaneBoot {
    encryption: DataplaneEncryption,
    wireguard: WireGuardBootConfig,
    cancel: CancellationToken,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
}

impl RootPeerDataplaneBoot {
    pub fn new(
        encryption: DataplaneEncryption,
        wireguard: WireGuardBootConfig,
        cancel: CancellationToken,
        task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Self {
        Self {
            encryption,
            wireguard,
            cancel,
            task_supervisor,
        }
    }
}

/// Root peer routing. Direct mode installs only gateway routes; it never
/// creates a WireGuard, VXLAN, or other overlay device.
pub struct RootPeerDataplane {
    handle: rtnetlink::Handle,
    local_pod_subnet: klights_types::PodSubnet,
    wireguard_device: String,
    wireguard_idx: OnceLock<u32>,
    wireguard: OnceLock<Arc<WireGuardController>>,
    health: DataplaneHealth,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
}

impl RootPeerDataplane {
    pub async fn boot(root: &RootDatapath, request: RootPeerDataplaneBoot) -> Self {
        let RootPeerDataplaneBoot {
            encryption,
            wireguard,
            cancel,
            task_supervisor,
        } = request;
        let peer = Self::direct(
            root.handle().clone(),
            root.pod_subnet(),
            wireguard.device().to_string(),
            task_supervisor,
        );

        if encryption == DataplaneEncryption::Enabled
            && let Err(error) = peer
                .ensure_wireguard_enabled(wireguard.key_path(), wireguard.port(), cancel)
                .await
        {
            peer.health
                .set_unavailable(format!("WireGuard dataplane: {error:#}"));
            tracing::error!(
                error = %error,
                "root WireGuard dataplane setup failed; node will report NotReady"
            );
        }

        peer
    }

    fn direct(
        handle: rtnetlink::Handle,
        local_pod_subnet: klights_types::PodSubnet,
        wireguard_device: String,
        task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Self {
        Self {
            handle,
            local_pod_subnet,
            wireguard_device,
            wireguard_idx: OnceLock::new(),
            wireguard: OnceLock::new(),
            health: DataplaneHealth::new_healthy(),
            task_supervisor,
        }
    }

    #[cfg(feature = "test-support")]
    pub fn direct_for_test(
        handle: rtnetlink::Handle,
        local_pod_subnet: klights_types::PodSubnet,
        wireguard_device: impl Into<String>,
        task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Self {
        Self::direct(
            handle,
            local_pod_subnet,
            wireguard_device.into(),
            task_supervisor,
        )
    }

    pub fn health(&self) -> &DataplaneHealth {
        &self.health
    }

    async fn ensure_wireguard_once(&self) -> Result<u32> {
        match crate::root_datapath::get_link_index(&self.handle, &self.wireguard_device).await {
            Ok(index) => {
                let _ = self.wireguard_idx.set(index);
            }
            Err(_) => {
                RootDatapath::ignore_eexist(
                    self.handle
                        .link()
                        .add()
                        .wireguard(self.wireguard_device.clone())
                        .execute()
                        .await,
                )
                .with_context(|| format!("failed to create {}", self.wireguard_device))?;
            }
        }

        let message = self
            .handle
            .link()
            .get()
            .match_name(self.wireguard_device.clone())
            .execute()
            .try_next()
            .await?
            .with_context(|| format!("{} not found after creation", self.wireguard_device))?;
        let state = device_state::parse_link_state(&message);
        if !matches!(state.kind, LinkKind::Wireguard) {
            anyhow::bail!(
                "expected interface {} to be wireguard kind, got {:?}",
                self.wireguard_device,
                state.kind
            );
        }
        self.handle
            .link()
            .set(state.ifindex)
            .up()
            .mtu(wireguard::WIREGUARD_MTU)
            .execute()
            .await
            .with_context(|| format!("failed to bring up {}", self.wireguard_device))?;
        let _ = self.wireguard_idx.set(state.ifindex);
        Ok(state.ifindex)
    }

    async fn ensure_wireguard_enabled(
        &self,
        key_path: &std::path::Path,
        port: u16,
        cancel: CancellationToken,
    ) -> Result<()> {
        self.ensure_wireguard_once().await?;
        let identity =
            WireGuardIdentity::load_or_create(key_path, self.task_supervisor.as_ref()).await?;
        let config = WireGuardDeviceConfig::try_new(
            self.wireguard_device.clone(),
            identity.private_key().clone(),
            port,
        )?;
        let controller = Arc::new(
            WireGuardController::open(config, self.task_supervisor.as_ref(), cancel).await?,
        );
        let _ = self.wireguard.set(controller);
        Ok(())
    }

    async fn wireguard_index(&self) -> Result<u32> {
        if let Some(index) = self.wireguard_idx.get() {
            return Ok(*index);
        }
        let index =
            crate::root_datapath::get_link_index(&self.handle, &self.wireguard_device).await?;
        let _ = self.wireguard_idx.set(index);
        Ok(index)
    }

    async fn apply(&self, route: &klights_network_api::PeerRoute) -> Result<()> {
        match route {
            klights_network_api::PeerRoute::WireGuard(route) => {
                let controller = self
                    .wireguard
                    .get()
                    .context("WireGuard dataplane is not initialized")?;
                let index = self.wireguard_index().await?;
                apply_wireguard_peer_route_with_rollback(
                    || Box::pin(controller.apply_peer(route)),
                    || {
                        Box::pin(wireguard::apply_wireguard_pod_route(
                            &self.handle,
                            index,
                            route,
                            self.local_pod_subnet.bridge_ip(),
                        ))
                    },
                    || {
                        Box::pin(wireguard::remove_wireguard_pod_route(
                            &self.handle,
                            index,
                            route,
                            self.local_pod_subnet.bridge_ip(),
                        ))
                    },
                    || Box::pin(controller.remove_peer(route)),
                )
                .await
            }
            klights_network_api::PeerRoute::Direct(route) => {
                wireguard::apply_unencrypted_direct_route(&self.handle, route).await
            }
        }
    }

    async fn remove(&self, route: &klights_network_api::PeerRoute) -> Result<()> {
        match route {
            klights_network_api::PeerRoute::WireGuard(route) => {
                let index = self.wireguard_index().await?;
                wireguard::remove_wireguard_pod_route(
                    &self.handle,
                    index,
                    route,
                    self.local_pod_subnet.bridge_ip(),
                )
                .await?;
                if let Some(controller) = self.wireguard.get() {
                    controller.remove_peer(route).await?;
                }
                Ok(())
            }
            klights_network_api::PeerRoute::Direct(route) => {
                wireguard::remove_unencrypted_direct_route(&self.handle, route).await
            }
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        if let Some(controller) = self.wireguard.get() {
            controller.shutdown().await?;
        }
        Ok(())
    }
}

impl klights_network_api::PeerRouter for RootPeerDataplane {
    fn apply_peer_route<'a>(
        &'a self,
        route: &'a klights_network_api::PeerRoute,
    ) -> klights_network_api::PeerRouterFuture<'a> {
        Box::pin(async move {
            self.apply(route)
                .await
                .map_err(|error| klights_network_api::PeerRouterError::apply(error.to_string()))
        })
    }

    fn remove_peer_route<'a>(
        &'a self,
        route: &'a klights_network_api::PeerRoute,
    ) -> klights_network_api::PeerRouterFuture<'a> {
        Box::pin(async move {
            self.remove(route)
                .await
                .map_err(|error| klights_network_api::PeerRouterError::remove(error.to_string()))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::apply_wireguard_peer_route_with_rollback;
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn route_failure_rolls_back_route_and_peer_before_returning() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let apply_peer_calls = calls.clone();
        let apply_route_calls = calls.clone();
        let remove_route_calls = calls.clone();
        let remove_peer_calls = calls.clone();

        let error = apply_wireguard_peer_route_with_rollback(
            || {
                Box::pin(async move {
                    apply_peer_calls.lock().unwrap().push("apply-peer");
                    Ok(())
                })
            },
            || {
                Box::pin(async move {
                    apply_route_calls.lock().unwrap().push("apply-route");
                    Err(anyhow::anyhow!("deterministic route add failure"))
                })
            },
            || {
                Box::pin(async move {
                    remove_route_calls.lock().unwrap().push("remove-route");
                    Ok(())
                })
            },
            || {
                Box::pin(async move {
                    remove_peer_calls.lock().unwrap().push("remove-peer");
                    Ok(())
                })
            },
        )
        .await
        .expect_err("route failure must be returned");

        assert!(
            error
                .to_string()
                .contains("deterministic route add failure")
        );
        assert_eq!(
            calls.lock().unwrap().as_slice(),
            ["apply-peer", "apply-route", "remove-route", "remove-peer"]
        );
    }
}
