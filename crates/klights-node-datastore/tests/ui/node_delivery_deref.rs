use klights_node_datastore::test_support::NodeDeliveryTestStore;

fn require_delivery(_: &klights_node_datastore::delivery::SqliteDeliveryStore) {}

fn escape(store: &NodeDeliveryTestStore) {
    require_delivery(store);
}

fn main() {}
