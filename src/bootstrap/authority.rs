use std::sync::{Arc, Mutex};

use tokio::sync::watch;

/// Small composition-time handle for the existing backend-neutral authority
/// contract. Client implementations route through permits, never through
/// a raw leadership boolean.
#[derive(Clone)]
pub(crate) struct AuthorityHandle {
    authority: Arc<dyn klights_leader_api::LeaderAuthority>,
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    legacy_watch: Option<watch::Receiver<bool>>,
}

impl AuthorityHandle {
    pub(crate) fn route(&self) -> klights_leader_api::AuthorityRoute {
        self.authority.route()
    }

    pub(crate) fn local_permit(
        &self,
    ) -> Result<klights_leader_api::AuthorityPermit, klights_leader_api::AuthorityError> {
        match self.authority.route() {
            klights_leader_api::AuthorityRoute::Local(permit) => {
                self.authority.validate(&permit)?;
                Ok(permit)
            }
            klights_leader_api::AuthorityRoute::Forward { .. }
            | klights_leader_api::AuthorityRoute::Unavailable => {
                Err(klights_leader_api::AuthorityError::NotAuthoritative)
            }
        }
    }

    pub(crate) fn validate(
        &self,
        permit: &klights_leader_api::AuthorityPermit,
    ) -> Result<(), klights_leader_api::AuthorityError> {
        self.authority.validate(permit)
    }

    pub(crate) fn acquire(&self) -> klights_leader_api::AuthorityAcquireFuture<'_> {
        self.authority.acquire()
    }

    pub(crate) fn wait_for_revocation<'a>(
        &'a self,
        permit: &'a klights_leader_api::AuthorityPermit,
    ) -> klights_leader_api::AuthorityRevocationFuture<'a> {
        self.authority.wait_for_revocation(permit)
    }
}

impl<T> From<Arc<T>> for AuthorityHandle
where
    T: klights_leader_api::LeaderAuthority + 'static,
{
    fn from(authority: Arc<T>) -> Self {
        Self {
            authority: authority as Arc<dyn klights_leader_api::LeaderAuthority>,
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            legacy_watch: None,
        }
    }
}

impl From<Arc<dyn klights_leader_api::LeaderAuthority>> for AuthorityHandle {
    fn from(authority: Arc<dyn klights_leader_api::LeaderAuthority>) -> Self {
        Self {
            authority,
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            legacy_watch: None,
        }
    }
}

impl From<watch::Receiver<bool>> for AuthorityHandle {
    fn from(receiver: watch::Receiver<bool>) -> Self {
        let authority = Arc::new(WatchReceiverAuthority::new(receiver.clone()));
        Self {
            authority,
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            legacy_watch: Some(receiver),
        }
    }
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
impl AuthorityHandle {
    pub(crate) fn legacy_watch_for_test(&self) -> Option<watch::Receiver<bool>> {
        self.legacy_watch.clone()
    }
}

/// Compatibility input adapter for existing bootstrap/test construction. It
/// translates the legacy signal at the composition boundary into the same
/// opaque permit contract consumed by local and authority-routed clients.
struct WatchReceiverAuthority {
    receiver: Mutex<watch::Receiver<bool>>,
    generation: std::sync::atomic::AtomicU64,
    issuer: klights_leader_api::AuthorityPermitIssuer,
}

impl WatchReceiverAuthority {
    fn new(receiver: watch::Receiver<bool>) -> Self {
        Self {
            receiver: Mutex::new(receiver),
            generation: std::sync::atomic::AtomicU64::new(1),
            issuer: klights_leader_api::AuthorityPermitIssuer::new(),
        }
    }

    fn state(&self) -> (bool, u64) {
        use std::sync::atomic::Ordering;
        let mut receiver = self
            .receiver
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if receiver.has_changed().unwrap_or(true) {
            let _ = receiver.borrow_and_update();
            let generation = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
            (*receiver.borrow(), generation)
        } else {
            (*receiver.borrow(), self.generation.load(Ordering::Acquire))
        }
    }
}

impl klights_leader_api::LeaderAuthority for WatchReceiverAuthority {
    fn route(&self) -> klights_leader_api::AuthorityRoute {
        let (local, generation) = self.state();
        if local {
            klights_leader_api::AuthorityRoute::Local(self.issuer.issue(generation))
        } else {
            klights_leader_api::AuthorityRoute::Unavailable
        }
    }

    fn validate(
        &self,
        permit: &klights_leader_api::AuthorityPermit,
    ) -> Result<(), klights_leader_api::AuthorityError> {
        let (local, generation) = self.state();
        if !local {
            return Err(klights_leader_api::AuthorityError::NotAuthoritative);
        }
        self.issuer.validate(permit, generation)
    }

    fn acquire(&self) -> klights_leader_api::AuthorityAcquireFuture<'_> {
        let mut receiver = self
            .receiver
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        Box::pin(async move {
            loop {
                if let klights_leader_api::AuthorityRoute::Local(permit) = self.route() {
                    return Ok(permit);
                }
                receiver
                    .changed()
                    .await
                    .map_err(|_| klights_leader_api::AuthorityError::Closed)?;
            }
        })
    }

    fn wait_for_revocation<'a>(
        &'a self,
        permit: &'a klights_leader_api::AuthorityPermit,
    ) -> klights_leader_api::AuthorityRevocationFuture<'a> {
        let mut receiver = self
            .receiver
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let permit = permit.clone();
        Box::pin(async move {
            loop {
                if self.validate(&permit).is_err() || receiver.changed().await.is_err() {
                    return;
                }
            }
        })
    }
}
