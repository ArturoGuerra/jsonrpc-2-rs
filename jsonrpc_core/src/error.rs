use crate::protocol::JsonRpcErrorResponse;
use crate::request_id::RequestId;
use serde_json::value::RawValue;
use std::borrow::Cow;
use std::time::Duration;
use thiserror::Error;

/// A JSON-RPC error code. The spec reserves `-32768..=-32000` for a handful
/// of standard, pre-defined errors; anything outside that range is
/// application-defined and round-trips through `Other`/`ServerError`
/// without loss.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonRpcErrorCode {
    /// Invalid JSON was received by the server.
    ParseError,
    /// The JSON sent is not a valid Request object.
    InvalidRequest,
    /// The method does not exist or is not available.
    MethodNotFound,
    /// Invalid method parameter(s).
    InvalidParams,
    /// Internal JSON-RPC error.
    InternalError,
    /// Reserved for implementation-defined server errors (`-32000..=-32099`).
    ServerError(i64),
    /// Any code outside the reserved range — application-defined.
    Other(i64),
}

impl From<i64> for JsonRpcErrorCode {
    fn from(code: i64) -> Self {
        match code {
            -32700 => Self::ParseError,
            -32600 => Self::InvalidRequest,
            -32601 => Self::MethodNotFound,
            -32602 => Self::InvalidParams,
            -32603 => Self::InternalError,
            -32099..=-32000 => Self::ServerError(code),
            other => Self::Other(other),
        }
    }
}

impl From<JsonRpcErrorCode> for i64 {
    fn from(code: JsonRpcErrorCode) -> i64 {
        match code {
            JsonRpcErrorCode::ParseError => -32700,
            JsonRpcErrorCode::InvalidRequest => -32600,
            JsonRpcErrorCode::MethodNotFound => -32601,
            JsonRpcErrorCode::InvalidParams => -32602,
            JsonRpcErrorCode::InternalError => -32603,
            JsonRpcErrorCode::ServerError(code) | JsonRpcErrorCode::Other(code) => code,
        }
    }
}

/// Every way a client-side JSON-RPC call can fail: a wire-level error
/// response from the peer, a malformed/unexpected message, a local
/// serialization failure, a transport failure, or a client-enforced
/// condition like a timeout or a missed notification.
#[derive(Error, Debug)]
pub enum Error {
    /// The peer answered with a JSON-RPC error object. `id` is `None` only
    /// when the peer itself couldn't determine which request this error
    /// belongs to (per spec, e.g. on a parse error).
    #[error("RPC error code: {code:?}, message: {message}")]
    RpcError {
        id: Option<RequestId>,
        code: JsonRpcErrorCode,
        message: String,
        data: Option<Box<RawValue>>,
    },

    /// Failed to serialize an outgoing message or deserialize an incoming
    /// one as valid JSON.
    #[error("Serde error: {0}")]
    SerdeError(#[from] serde_json::Error),

    /// Params failed to serialize to a JSON-RPC Structured value (an Array
    /// or Object) — the only shapes the spec allows for `params`.
    #[error("Invalid params, expected Array, Object or Null")]
    InvalidParams,

    /// A response body wasn't valid UTF-8.
    #[error("Invalid str: {0}")]
    InvalidUTF8Str(#[from] std::str::Utf8Error),

    /// No response arrived before the request's configured timeout elapsed.
    #[error("Request timed out after {0:?}")]
    RequestTimeout(Duration),

    /// A message was valid JSON but didn't have a shape this client accepts
    /// — e.g. missing `result`/`error`, an id that doesn't match any pending
    /// request, or a wire shape this client doesn't handle (see the
    /// crate-level docs).
    #[error("Received an invalid JSON-RPC message: {0}")]
    InvalidMessage(&'static str),

    /// The underlying transport (HTTP, WebSocket, ...) failed independently
    /// of the JSON-RPC protocol itself — a connection drop, a non-2xx HTTP
    /// status, a DNS failure, etc.
    #[error("Transport error: {0}")]
    TransportError(#[from] anyhow::Error),

    /// A [`crate::client::BidirectionalClient`] notification stream fell
    /// behind its bounded internal channel and dropped this many
    /// notifications before catching back up.
    #[cfg(feature = "bidirectional")]
    #[error("missed {0} notifications due to a slow consumer")]
    NotificationsMissed(u64),
}

impl From<JsonRpcErrorResponse<'_>> for Error {
    fn from(response: JsonRpcErrorResponse<'_>) -> Self {
        Error::RpcError {
            id: response.id,
            code: JsonRpcErrorCode::from(response.error.code),
            message: response.error.message.to_owned(),
            data: response.error.data.map(Cow::into_owned),
        }
    }
}
