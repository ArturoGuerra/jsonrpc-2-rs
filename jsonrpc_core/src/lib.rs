//! Client-side building blocks for the [JSON-RPC 2.0](https://www.jsonrpc.org/specification)
//! wire protocol: request/notification/response types ([`protocol`]), a
//! zero-copy decoder for incoming messages ([`decode`]), request id
//! allocation ([`request_id`]), batch request building ([`batch`]), and a
//! transport-agnostic [`Client`] trait implemented by the
//! `jsonrpc_http_client`/`jsonrpc_ws_client` crates.
//!
//! This crate is client-only: it can serialize outbound requests and
//! notifications and decode inbound responses/notifications, but has no
//! `Serialize` impl for [`protocol::JsonRpcResponse`]/
//! [`protocol::JsonRpcErrorResponse`] and rejects any inbound message shaped
//! as a request (see [`decode`]) — it cannot be used to implement a
//! JSON-RPC server.
//!
//! Every type below is also reachable through its owning submodule (e.g.
//! [`client::Client`]) for callers that prefer fully-qualified paths.

pub mod batch;
pub mod client;
pub mod decode;
pub mod error;
pub mod method_id;
pub(crate) mod params;
pub mod protocol;
pub mod request_id;
pub mod result;
pub mod version;

pub use batch::{Batch, BatchBuilder, BatchResponseItem, DowncastResponse};
#[cfg(feature = "bidirectional")]
pub use client::BidirectionalClient;
pub use client::Client;
pub use decode::{JsonRpcBatchResponse, JsonRpcIncoming, decode_single_response};
pub use error::{Error, JsonRpcErrorCode};
pub use method_id::{MethodId, MethodIdBuf};
pub use params::IntoOptionParams;
#[cfg(feature = "bidirectional")]
pub use protocol::Notification;
pub use protocol::{
    JsonRpcError, JsonRpcErrorResponse, JsonRpcNotification, JsonRpcRequest, JsonRpcResponse,
};
pub use request_id::{RequestId, RequestIdManager};
pub use result::Result;
pub use version::{JSONRPC_VERSION, JsonRpcVersion};
