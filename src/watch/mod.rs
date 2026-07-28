mod bookmark;
pub mod bus;
#[cfg(test)]
mod cursor;
pub mod events;
mod filter;
#[cfg(test)]
mod raw_signal_cursor;
#[cfg(test)]
mod replay;
#[cfg(test)]
mod scope;
mod selection;
#[cfg(test)]
mod selector_membership;
#[cfg(test)]
mod signal_cursor;
#[cfg(test)]
mod signal_replay_cursor_core;
#[cfg(test)]
mod window;

pub use bus::WatchBus;
#[cfg(test)]
pub use bus::WatchReceiver;
#[cfg(test)]
pub(crate) use bus::test_signal_channel;
#[cfg(test)]
pub use cursor::{WatchBootstrap, WatchCursor};
pub use events::{
    EventType, WatchContentType, WatchEvent, encode_watch_payload, value_matches_field_selector,
};
pub use filter::WatchEventFilter;
#[cfg(test)]
pub use raw_signal_cursor::RawSignalWatchCursor;
#[cfg(test)]
pub use replay::{WatchCursorError, WatchReplaySource};
#[cfg(test)]
pub use scope::WatchDeliveryScope;
pub use selection::WatchEventSelection;
#[cfg(test)]
pub(crate) use selector_membership::{SelectorMembership, event_key, resource_key};
#[cfg(test)]
pub use signal_cursor::SignalWatchCursor;
#[cfg(test)]
pub use window::WindowPolicy;

#[cfg(test)]
mod tests;
