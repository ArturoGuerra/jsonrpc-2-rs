//! A [`jsonrpc_core::Client`]/[`jsonrpc_core::BidirectionalClient`]
//! implementation over a WebSocket connection (via [`stream_tungstenite`]),
//! with automatic reconnection and typestate-enforced usage: a freshly
//! built [`WsClient`] starts [`Disconnected`] (no background tasks, no
//! `request`/`notify`/`batch_request`/`subscribe`), and [`WsClient::run`]
//! transitions it to [`Connected`], where that API becomes available.

mod builder;
mod client;
mod handles;

pub use builder::WsClientBuilder;
pub use client::{Connected, Disconnected, WsClient};
