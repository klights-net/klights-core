/// Convert k8s-openapi Lease to k8s-pb Lease
use crate::protobuf::*;
pub fn json_lease_to_pb(
    lease: &k8s_openapi::api::coordination::v1::Lease,
) -> anyhow::Result<k8s_pb::api::coordination::v1::Lease> {
    Ok(k8s_pb::api::coordination::v1::Lease {
        metadata: Some(json_meta_to_pb(&lease.metadata)),
        spec: lease
            .spec
            .as_ref()
            .map(|spec| k8s_pb::api::coordination::v1::LeaseSpec {
                holder_identity: spec.holder_identity.clone(),
                lease_duration_seconds: spec.lease_duration_seconds,
                acquire_time: spec.acquire_time.as_ref().map(json_microtime_to_pb),
                renew_time: spec.renew_time.as_ref().map(json_microtime_to_pb),
                lease_transitions: spec.lease_transitions,
                preferred_holder: spec.preferred_holder.clone(),
                strategy: spec.strategy.clone(),
            }),
    })
}

/// Convert k8s-openapi MicroTime to k8s-pb MicroTime
pub fn json_microtime_to_pb(
    time: &k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime,
) -> k8s_pb::apimachinery::pkg::apis::meta::v1::MicroTime {
    k8s_pb::apimachinery::pkg::apis::meta::v1::MicroTime {
        seconds: Some(time.0.timestamp()),
        nanos: Some(time.0.timestamp_subsec_nanos() as i32),
    }
}
