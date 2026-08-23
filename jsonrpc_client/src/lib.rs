//! Umbrella crate over the `jsonrpc-rs` client stack: always re-exports
//! [`jsonrpc_core`]'s shared protocol types and [`Client`] trait, and
//! additionally re-exports [`jsonrpc_http_client`]'s [`HttpClient`] under the
//! `http` feature and [`jsonrpc_ws_client`]'s [`WsClient`]/[`WsClientBuilder`]
//! under the `ws` feature. Enable `full` for both. None of these features are
//! on by default.

pub use jsonrpc_core::*;

#[cfg(feature = "http")]
pub use jsonrpc_http_client::*;

#[cfg(feature = "ws")]
pub use jsonrpc_ws_client::*;
