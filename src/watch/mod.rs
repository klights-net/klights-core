mod bookmark;
pub mod bus;
pub mod events;
mod filter;
mod selection;
#[cfg(test)]
mod selector_membership;

pub use bus::WatchBus;
#[cfg(test)]
pub use bus::WatchReceiver;
pub use events::{
    EventType, WatchContentType, WatchEvent, encode_watch_payload, value_matches_field_selector,
};
pub use filter::WatchEventFilter;
pub use selection::WatchEventSelection;
#[cfg(test)]
pub(crate) use selector_membership::{SelectorMembership, event_key, resource_key};

#[cfg(test)]
mod tests;
