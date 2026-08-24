//! Authority-aware dispatch for API-server remote pod exec/attach.
//!
//! Every control-plane terminates Kubernetes WebSocket upgrades locally. The
//! node runtime route, however, is leader-owned: only the current leader has
//! the complete set of follower control streams. This adapter samples the
//! authority route for each new exec/attach session instead of binding the API
//! state permanently to the local replication service.

use std::sync::Arc;

use klights_leader_api::AuthorityRoute;
use klights_node_api::{
    ExecSetupError, NodeExec, NodeExecFuture, NodeExecRequest, NodeExecSession,
    NodeExecSyncRequest, NodeExecSyncResult,
};

use super::authority::AuthorityHandle;

pub(crate) struct AuthorityRoutedNodeExec {
    local: Arc<dyn NodeExec>,
    remote: Arc<dyn NodeExec>,
    authority: AuthorityHandle,
}

impl AuthorityRoutedNodeExec {
    pub(crate) fn new(
        local: Arc<dyn NodeExec>,
        remote: Arc<dyn NodeExec>,
        authority: impl Into<AuthorityHandle>,
    ) -> Self {
        Self {
            local,
            remote,
            authority: authority.into(),
        }
    }

    fn target(&self) -> Result<Arc<dyn NodeExec>, ExecSetupError> {
        match self.authority.route() {
            AuthorityRoute::Local(permit) => {
                self.authority.validate(&permit).map_err(|_| {
                    ExecSetupError::unavailable(
                        "leader authority changed before node-exec dispatch",
                    )
                })?;
                Ok(self.local.clone())
            }
            AuthorityRoute::Forward { .. } => Ok(self.remote.clone()),
            AuthorityRoute::Unavailable => Err(ExecSetupError::unavailable(
                "leader authority is unavailable; retry node exec after election",
            )),
        }
    }
}

impl NodeExec for AuthorityRoutedNodeExec {
    fn exec_sync(&self, request: NodeExecSyncRequest) -> NodeExecFuture<'_, NodeExecSyncResult> {
        let target = self.target();
        Box::pin(async move { target?.exec_sync(request).await })
    }

    fn open_exec(&self, request: NodeExecRequest) -> NodeExecFuture<'_, Box<dyn NodeExecSession>> {
        let target = self.target();
        Box::pin(async move { target?.open_exec(request).await })
    }
}

#[cfg(test)]
#[path = "../tests/authority_routed_node_exec.rs"]
mod tests;
