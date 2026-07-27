use klights_node_store::{
    DeliveryFuture, OutboxAttemptFailureRecord, OutboxDispatchCounters, OutboxDispatcherStore,
    OutboxFailureDisposition, OutboxStatusStampStore,
};

struct FocusedDeliveryStore;

impl OutboxStatusStampStore for FocusedDeliveryStore {
    fn read_status_stamp_high_water(&self) -> DeliveryFuture<'_, i64> {
        Box::pin(async { Ok(0) })
    }

    fn write_status_stamp_high_water(&self, _high_water: i64) -> DeliveryFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

fn assert_status_stamp_store_object_safe(_: &dyn OutboxStatusStampStore) {}

#[test]
fn dispatcher_failure_and_counter_contracts_are_typed() {
    assert_status_stamp_store_object_safe(&FocusedDeliveryStore);

    let failure = OutboxAttemptFailureRecord::try_new(7, "lease-a", 500, "retry", 3).unwrap();
    assert_eq!(failure.id(), 7);
    assert_eq!(failure.max_attempts(), 3);

    let counters = OutboxDispatchCounters::try_new(11, 2).unwrap();
    assert_eq!(counters.dispatch_total(), 11);
    assert_eq!(counters.dispatch_errors_total(), 2);

    fn assert_disposition(_: OutboxFailureDisposition) {}
    assert_disposition(OutboxFailureDisposition::RetryScheduled);

    fn assert_dispatcher_object_safe(_: &dyn OutboxDispatcherStore) {}
    let _ = assert_dispatcher_object_safe;
}
