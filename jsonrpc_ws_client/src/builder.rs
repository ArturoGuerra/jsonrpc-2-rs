use crate::client::{Disconnected, PendingBatchRequests, PendingRequests, WsClient, WsClientInner};
use jsonrpc_core::RequestIdManager;
use std::time::Duration;
use stream_tungstenite::client::WebSocketClient;

/// Builds a [`WsClient`], allowing `ping_interval` and `request_timeout` to
/// be configured — plumbing that otherwise wasn't reachable through
/// `WsClient::new`/`build_from_websocket_client`.
pub struct WsClientBuilder {
    client: WebSocketClient,
    ping_interval: Option<Duration>,
    request_timeout: Option<Duration>,
}

impl WsClientBuilder {
    pub fn new(url: &str) -> Self {
        Self::from_websocket_client(WebSocketClient::new(url))
    }

    pub fn from_websocket_client(client: WebSocketClient) -> Self {
        Self {
            client,
            ping_interval: None,
            request_timeout: None,
        }
    }

    /// Sends a `Ping` on this interval to keep the connection alive and
    /// detect a dead peer. Disabled (`None`) by default.
    pub fn ping_interval(mut self, interval: Duration) -> Self {
        self.ping_interval = Some(interval);
        self
    }

    /// Default timeout applied by [`jsonrpc_core::Client::request`]/
    /// [`jsonrpc_core::Client::batch_request`] when no explicit timeout is
    /// given. Defaults to `DEFAULT_REQUEST_TIMEOUT`.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    /// Builds the client. Does not connect until [`WsClient::run`] is
    /// called.
    pub fn build(self) -> WsClient<Disconnected> {
        let inner = WsClientInner::new(
            self.client,
            RequestIdManager::new(),
            PendingRequests::new(),
            PendingBatchRequests::new(),
            self.ping_interval,
            self.request_timeout,
        );
        WsClient::from_inner(inner)
    }
}
