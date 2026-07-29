use klights_node_store::{NodeIdentity, NodeIdentityError, NodeIdentityFuture};

struct FakeNodeIdentity;

impl NodeIdentity for FakeNodeIdentity {
    fn close(&self) {}

    fn backend_name(&self) -> &'static str {
        "fake"
    }

    fn ensure_node_identity<'a>(
        &'a self,
        _cluster_id: &'a str,
        _node_uid: &'a str,
    ) -> NodeIdentityFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn get_node_meta<'a>(&'a self, key: &'a str) -> NodeIdentityFuture<'a, Option<String>> {
        Box::pin(async move { Ok(Some(key.to_string())) })
    }

    fn set_node_meta<'a>(&'a self, _key: &'a str, _value: &'a str) -> NodeIdentityFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

#[test]
fn node_identity_is_a_focused_object_safe_port() {
    let identity: &dyn NodeIdentity = &FakeNodeIdentity;
    assert_eq!(identity.backend_name(), "fake");
    identity.close();
}

#[test]
fn node_identity_futures_borrow_the_port_and_inputs() {
    let identity: &dyn NodeIdentity = &FakeNodeIdentity;
    let ensure = identity.ensure_node_identity("cluster-a", "node-a");
    let write = identity.set_node_meta("key", "value");
    let read = identity.get_node_meta("key");
    drop((ensure, write, read));
}

#[test]
fn node_identity_error_keeps_persistence_context() {
    let error =
        NodeIdentityError::persistence_failed("ensure_node_identity", "node.db identity mismatch");
    assert!(error.to_string().contains("node.db identity mismatch"));
}
