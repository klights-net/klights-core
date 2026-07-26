//! Cluster datastore adapters for klights.

mod outbox_response_wire;

pub use outbox_response_wire::{
    OutboxResponseWireError, decode_outbox_response, encode_outbox_response,
};
