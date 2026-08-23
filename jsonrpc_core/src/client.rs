use crate::{
    batch::{BatchBuilder, BatchResponseItem},
    method_id::MethodIdBuf,
    params::IntoOptionParams,
    result::Result,
};
use serde::de::DeserializeOwned;
use std::future::Future;
use std::time::Duration;
#[cfg(feature = "bidirectional")]
use {crate::protocol::Notification, futures::Stream};

/// A transport-agnostic JSON-RPC 2.0 client — implemented by
/// `jsonrpc_http_client::HttpClient` and `jsonrpc_ws_client::WsClient`.
pub trait Client {
    /// Sends a request and awaits its response, deserialized as `Response`.
    /// `timeout: None` uses the implementation's own configured default.
    fn request<Method, Params, Response>(
        &self,
        method: Method,
        params: Params,
        timeout: Option<Duration>,
    ) -> impl Future<Output = Result<Response>> + Send + Sync
    where
        Method: Into<MethodIdBuf> + Send + Sync,
        Params: IntoOptionParams,
        Response: DeserializeOwned + Send + Sync;

    /// Sends a fire-and-forget notification — no `id`, no reply expected.
    fn notify<Method, Params>(
        &self,
        method: Method,
        params: Params,
    ) -> impl Future<Output = Result<()>> + Send + Sync
    where
        Method: Into<MethodIdBuf> + Send + Sync,
        Params: IntoOptionParams;

    /// Sends every request/notification queued on `builder` as a single
    /// JSON-RPC batch and awaits the reply. `Ok(None)` only when `builder`
    /// had nothing queued at all. `timeout: None` uses the implementation's
    /// own configured default.
    fn batch_request(
        &self,
        builder: BatchBuilder,
        timeout: Option<Duration>,
    ) -> impl Future<Output = Result<Option<Vec<BatchResponseItem>>>> + Send + Sync;

    /// [`Client::request`] with no params.
    fn request_without_params<Method, Response>(
        &self,
        method: Method,
        timeout: Option<Duration>,
    ) -> impl Future<Output = Result<Response>> + Send + Sync
    where
        Method: Into<MethodIdBuf> + Send + Sync,
        Response: DeserializeOwned + Send + Sync,
    {
        self.request(method, (), timeout)
    }

    /// [`Client::notify`] with no params.
    fn notify_without_params<Method>(
        &self,
        method: Method,
    ) -> impl Future<Output = Result<()>> + Send + Sync
    where
        Method: Into<MethodIdBuf> + Send + Sync,
    {
        self.notify(method, ())
    }
}

/// A `Client` over a duplex transport (e.g. WebSocket) that may receive
/// unsolicited JSON-RPC Notifications pushed by the peer at any time, in
/// addition to ordinary request/response and outbound-notification traffic.
///
/// This trait exposes only the raw, spec-level Notification primitive. It
/// does not implement subscription-id matching or filtering: any
/// server-specific convention for correlating pushed notifications to a
/// prior request (e.g. an `eth_subscribe`-style subscription id embedded in
/// `params`) is the consumer's responsibility to interpret.
#[cfg(feature = "bidirectional")]
pub trait BidirectionalClient: Client {
    /// Returns a stream of Notifications pushed by the peer.
    ///
    /// Each call returns an independent stream; every call observes every
    /// Notification received from the moment of the call onward (broadcast
    /// semantics) — calling this more than once is supported.
    ///
    /// An `Err(Error::NotificationsMissed(n))` item means the implementation
    /// could not guarantee delivery of `n` notifications between it and the
    /// previous item (e.g. a slow consumer falling behind a bounded internal
    /// channel); the stream continues afterward. An implementation backed by
    /// a lossless channel is free to never yield `Err`.
    ///
    /// The stream ends when the underlying connection closes. There is no
    /// special sentinel item for closure; connection lifecycle is not part
    /// of this trait.
    fn subscribe(&self) -> impl Stream<Item = Result<Notification>> + Send + Sync;
}
