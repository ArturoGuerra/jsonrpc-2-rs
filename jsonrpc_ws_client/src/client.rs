use crate::builder::WsClientBuilder;
use crate::handles::{disconnect_task, incoming_task, management_task};
use dashmap::DashMap;
use futures::Stream;
use jsonrpc_core::{
    Batch, BatchBuilder, BatchResponseItem, BidirectionalClient, Client, Error, IntoOptionParams,
    JsonRpcNotification, JsonRpcRequest, MethodIdBuf, Notification, RequestId, RequestIdManager,
    Result,
};
use serde::de::DeserializeOwned;
use smallvec::SmallVec;
use std::{ops::Range, ops::RangeInclusive, sync::Arc, time::Duration};
use stream_tungstenite::{
    SharedMessage,
    client::WebSocketClient,
    tokio_tungstenite::tungstenite::{Message, Utf8Bytes},
};
use tokio::{
    sync::{broadcast, oneshot},
    task::JoinHandle,
    time,
};

/// Default timeout for a request/batch when neither the call site nor the
/// client's own configured [`WsClientInner::request_timeout`] supplies one.
pub(crate) const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Bounded capacity of [`WsClientInner::notification_channel`] — how many
/// unconsumed pushed notifications a slow [`BidirectionalClient::subscribe`]
/// consumer can fall behind before older ones are dropped.
pub(crate) const NOTIFICATION_CHANNEL_CAPACITY: usize = 100;

/// In-flight unary requests awaiting a reply, keyed by the id they were
/// sent with.
pub(crate) type PendingRequests =
    DashMap<RequestId, oneshot::Sender<Result<(SharedMessage, Range<usize>)>>>;

/// In-flight batch requests awaiting a reply, keyed by the contiguous id
/// range they were sent with.
pub(crate) type PendingBatchRequests =
    DashMap<RangeInclusive<RequestId>, oneshot::Sender<Result<BatchResult>>>;

/// A resolved batch reply: the raw message it arrived in, plus each item's
/// byte range within it (or the error resolving that item), in caller
/// request-queue order.
#[derive(Debug)]
pub(crate) struct BatchResult {
    pub(crate) message: SharedMessage,
    pub(crate) items: Vec<Result<Range<usize>>>,
}

/// Shared state behind every clone of a `WsClient` and its background
/// tasks — the single underlying [`WebSocketClient`] connection plus the
/// bookkeeping (pending requests, notification fan-out) that correlates
/// wire traffic back to callers.
pub(crate) struct WsClientInner {
    pub(crate) client: WebSocketClient,
    pub(crate) request_id_manager: RequestIdManager,
    pub(crate) pending_requests: PendingRequests,
    pub(crate) pending_batch_requests: PendingBatchRequests,
    pub(crate) notification_channel: broadcast::Sender<Notification>,
    pub(crate) ping_interval: Option<Duration>,
    pub(crate) request_timeout: Option<Duration>,
}

impl WsClientInner {
    pub(crate) fn new(
        client: WebSocketClient,
        request_id_manager: RequestIdManager,
        pending_requests: PendingRequests,
        pending_batch_requests: PendingBatchRequests,
        ping_interval: Option<Duration>,
        request_timeout: Option<Duration>,
    ) -> Arc<Self> {
        Arc::new(Self {
            notification_channel: broadcast::channel(NOTIFICATION_CHANNEL_CAPACITY).0,
            client,
            request_id_manager,
            pending_requests,
            pending_batch_requests,
            ping_interval,
            request_timeout,
        })
    }
}

/// Typestate marker: not yet connected — no background tasks are running
/// and the `Client`/`BidirectionalClient` API isn't available yet.
/// [`WsClient::run`] transitions to [`Connected`].
pub struct Disconnected;

/// Typestate marker: connected — background dispatch tasks are running and
/// `request`/`notify`/`batch_request`/`subscribe` become available.
pub struct Connected {
    inner: Arc<WsClientInner>,
    incoming_dispatcher: JoinHandle<()>,
    management_dispatcher: JoinHandle<()>,
    disconnect_dispatcher: JoinHandle<()>,
    run_dispatcher: JoinHandle<Result<()>>,
}

impl Drop for Connected {
    fn drop(&mut self) {
        self.incoming_dispatcher.abort();
        self.management_dispatcher.abort();
        self.disconnect_dispatcher.abort();
        self.run_dispatcher.abort();
        self.inner.client.shutdown();
    }
}

pub struct WsClient<State = Disconnected> {
    inner: Arc<WsClientInner>,
    /// Never read directly — its only jobs are to select which `impl
    /// WsClient<...>` block applies, and (for `Connected`) to run its own
    /// `Drop` at the right time.
    #[allow(dead_code)]
    state: State,
}

impl<State> WsClient<State> {
    /// The underlying [`WebSocketClient`], for access to lower-level
    /// operations (connection state, event subscription, ...) not exposed
    /// directly on `WsClient`. Available regardless of typestate.
    pub fn get_websocket_client(&self) -> &WebSocketClient {
        &self.inner.client
    }

    /// Whether the underlying connection is currently established. Always
    /// `false` before [`WsClient::run`] has been called.
    pub fn is_connected(&self) -> bool {
        self.inner.client.is_connected()
    }
}

impl WsClient<Disconnected> {
    /// Builds a client connecting to `url` with default `ping_interval`/
    /// `request_timeout`. Use [`WsClient::builder`] to configure those.
    /// Does not connect until [`WsClient::run`] is called.
    pub fn new(url: &str) -> Self {
        Self::builder(url).build()
    }

    /// Builds a client around an already-constructed [`WebSocketClient`]
    /// (e.g. one configured with a custom retry strategy or handshaker).
    pub fn build_from_websocket_client(client: WebSocketClient) -> Self {
        WsClientBuilder::from_websocket_client(client).build()
    }

    /// Starts building a `WsClient`, allowing `ping_interval`/`request_timeout`
    /// to be configured before connecting.
    pub fn builder(url: &str) -> WsClientBuilder {
        WsClientBuilder::new(url)
    }

    pub(crate) fn from_inner(inner: Arc<WsClientInner>) -> Self {
        WsClient {
            inner,
            state: Disconnected,
        }
    }

    /// Spawns the background dispatch tasks and starts driving the
    /// connection, transitioning to [`Connected`] — only after this does
    /// the `Client`/`BidirectionalClient` API (`request`/`notify`/
    /// `batch_request`/`subscribe`) become available.
    pub fn run(self) -> WsClient<Connected> {
        let incoming_dispatcher = tokio::spawn({
            let inner = self.inner.clone();
            async move { incoming_task(inner).await }
        });
        let management_dispatcher = tokio::spawn({
            let inner = self.inner.clone();
            async move { management_task(inner).await }
        });
        let disconnect_dispatcher = tokio::spawn({
            let inner = self.inner.clone();
            async move { disconnect_task(inner).await }
        });
        let run_dispatcher = tokio::spawn({
            let inner = self.inner.clone();
            async move {
                inner
                    .client
                    .run()
                    .await
                    .map_err(|e| Error::TransportError(anyhow::anyhow!(e)))
            }
        });

        WsClient {
            inner: self.inner.clone(),
            state: Connected {
                inner: self.inner,
                incoming_dispatcher,
                management_dispatcher,
                disconnect_dispatcher,
                run_dispatcher,
            },
        }
    }
}

impl WsClient<Connected> {
    /// Signals the connection to stop — in-flight requests fail with
    /// `Error::TransportError` rather than waiting out their timeout.
    /// Terminal: the underlying [`WebSocketClient`]'s cancellation is
    /// one-shot, so this client can't be `run` again afterward — build a
    /// new `WsClient` to reconnect.
    pub fn shutdown(self) {
        self.inner.client.shutdown();
    }
}

#[hotpath::measure_all]
impl Client for WsClient<Connected> {
    #[hotpath::skip]
    async fn request<Method, Params, Response>(
        &self,
        method: Method,
        params: Params,
        timeout: Option<Duration>,
    ) -> Result<Response>
    where
        Method: Into<MethodIdBuf> + Send + Sync,
        Params: IntoOptionParams,
        Response: DeserializeOwned + Send + Sync,
    {
        let timeout = timeout
            .or(self.inner.request_timeout)
            .unwrap_or(DEFAULT_REQUEST_TIMEOUT);
        let method = method.into();

        let WsClientInner {
            client,
            request_id_manager,
            pending_requests,
            ..
        } = &*self.inner;

        let id = request_id_manager.reserve_id();
        let mut buf = SmallVec::new();
        let request =
            JsonRpcRequest::try_new(id, method.as_ref(), params.into_option_params(), &mut buf)?;
        let payload = serde_json::to_string(&request).map_err(Error::SerdeError)?;

        let (tx, rx) = oneshot::channel();
        pending_requests.insert(id, tx);
        if let Err(e) = client.send(Message::Text(Utf8Bytes::from(payload))).await {
            pending_requests.remove(&id);
            return Err(Error::TransportError(e.into()));
        }

        let (message, range): (SharedMessage, Range<usize>) = match time::timeout(timeout, rx).await
        {
            Ok(result) => result.map_err(|e| Error::TransportError(e.into()))??,
            Err(_) => {
                pending_requests.remove(&id);
                return Err(Error::RequestTimeout(timeout));
            }
        };

        let buf = match message.as_ref() {
            Message::Text(buf) => buf.as_bytes(),
            Message::Binary(buf) => buf,
            _ => return Err(Error::InvalidMessage("expected a Text or Binary message")),
        };
        serde_json::from_slice::<Response>(&buf[range]).map_err(Error::SerdeError)
    }

    async fn notify<Method, Params>(&self, method: Method, params: Params) -> Result<()>
    where
        Method: Into<MethodIdBuf> + Send + Sync,
        Params: IntoOptionParams,
    {
        let method = method.into();

        let WsClientInner { client, .. } = &*self.inner;

        let mut buf = SmallVec::new();
        let notification =
            JsonRpcNotification::try_new(method.as_ref(), params.into_option_params(), &mut buf)?;
        let payload = serde_json::to_string(&notification).map_err(Error::SerdeError)?;
        if let Err(err) = client.send(Message::Text(Utf8Bytes::from(payload))).await {
            return Err(Error::TransportError(err.into()));
        }
        Ok(())
    }

    #[hotpath::skip]
    async fn batch_request(
        &self,
        builder: BatchBuilder,
        timeout: Option<Duration>,
    ) -> Result<Option<Vec<BatchResponseItem>>> {
        let timeout = timeout
            .or(self.inner.request_timeout)
            .unwrap_or(DEFAULT_REQUEST_TIMEOUT);
        let WsClientInner {
            client,
            request_id_manager,
            pending_batch_requests,
            ..
        } = &*self.inner;
        let Some(Batch {
            items,
            parsers,
            request_id_range,
        }) = builder.build(request_id_manager)?
        else {
            return Ok(None);
        };

        let payload = serde_json::to_string(&items).map_err(Error::SerdeError)?;

        let Some(key) = request_id_range else {
            if let Err(e) = client.send(Message::Text(Utf8Bytes::from(payload))).await {
                return Err(Error::TransportError(e.into()));
            }
            return Ok(None);
        };

        let (tx, rx) = oneshot::channel();
        pending_batch_requests.insert(key.clone(), tx);

        if let Err(e) = client.send(Message::Text(Utf8Bytes::from(payload))).await {
            pending_batch_requests.remove(&key);
            return Err(Error::TransportError(e.into()));
        }

        let batch_response = match time::timeout(timeout, rx).await {
            Ok(result) => result.map_err(|e| Error::TransportError(e.into()))??,
            Err(_) => {
                pending_batch_requests.remove(&key);
                return Err(Error::RequestTimeout(timeout));
            }
        };

        let buf = match batch_response.message.as_ref() {
            Message::Text(buf) => buf.as_bytes(),
            Message::Binary(buf) => buf,
            _ => return Err(Error::InvalidMessage("expected a Text or Binary message")),
        };

        let result: Vec<BatchResponseItem> = batch_response
            .items
            .into_iter()
            .zip(parsers)
            .map(|(item, parser)| {
                let range = item?;
                let text = std::str::from_utf8(&buf[range])
                    .map_err(|_| Error::InvalidMessage("response was not valid UTF-8"))?;
                parser(text)
            })
            .collect();

        Ok(Some(result))
    }
}

impl BidirectionalClient for WsClient<Connected> {
    fn subscribe(&self) -> impl Stream<Item = Result<Notification>> + Send + Sync {
        let rx = self.inner.notification_channel.subscribe();
        futures::stream::unfold(rx, |mut rx| async move {
            match rx.recv().await {
                Ok(n) => Some((Ok(n), rx)),
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    Some((Err(Error::NotificationsMissed(n)), rx))
                }
                Err(broadcast::error::RecvError::Closed) => None,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{SinkExt, StreamExt};
    use jsonrpc_core::DowncastResponse;
    use stream_tungstenite::tokio_tungstenite::accept_async;
    use tokio::net::TcpListener;

    /// Accepts WebSocket connections (one after another, e.g. across a
    /// client `shutdown`/`run` reconnect) and replies on each to every text
    /// message `handler` returns `Some(_)` for. Returns the server's
    /// `ws://host:port/` endpoint.
    async fn start_ws_server<F>(handler: F) -> String
    where
        F: Fn(&str) -> Option<String> + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handler = Arc::new(handler);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let handler = handler.clone();
                tokio::spawn(async move {
                    let Ok(mut ws) = accept_async(stream).await else {
                        return;
                    };
                    while let Some(Ok(msg)) = ws.next().await {
                        if let Message::Text(text) = &msg
                            && let Some(reply) = handler(text.as_str())
                            && ws.send(Message::Text(reply.into())).await.is_err()
                        {
                            break;
                        }
                    }
                });
            }
        });
        format!("ws://{addr}/")
    }

    /// Polls `client.is_connected()` until it's `true` or panics after 5s.
    async fn wait_until_connected<State>(client: &WsClient<State>) {
        for _ in 0..500 {
            if client.is_connected() {
                return;
            }
            time::sleep(Duration::from_millis(10)).await;
        }
        panic!("client did not connect in time");
    }

    #[tokio::test]
    async fn request_round_trips_over_a_real_connection() {
        let url = start_ws_server(|text| {
            let value: serde_json::Value = serde_json::from_str(text).ok()?;
            let id = value.get("id")?.as_u64()?;
            Some(format!(r#"{{"jsonrpc":"2.0","id":{id},"result":42}}"#))
        })
        .await;

        let client = Arc::new(WsClient::new(&url).run());
        wait_until_connected(&client).await;

        let result: i64 = client.request_without_params("ping", None).await.unwrap();
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn request_times_out_when_server_never_replies() {
        let url = start_ws_server(|_| None).await;

        let client = Arc::new(
            WsClientBuilder::new(&url)
                .request_timeout(Duration::from_millis(100))
                .build()
                .run(),
        );
        wait_until_connected(&client).await;

        let result = client.request_without_params::<_, i64>("ping", None).await;
        assert!(matches!(result, Err(Error::RequestTimeout(_))));
    }

    #[tokio::test]
    async fn batch_request_zips_responses_to_requests_in_order() {
        let url = start_ws_server(|text| {
            let items: Vec<serde_json::Value> = serde_json::from_str(text).ok()?;
            let replies: Vec<String> = items
                .iter()
                .map(|item| {
                    let id = item.get("id").unwrap().as_u64().unwrap();
                    format!(r#"{{"jsonrpc":"2.0","id":{id},"result":{id}}}"#)
                })
                .collect();
            Some(format!("[{}]", replies.join(",")))
        })
        .await;

        let client = Arc::new(WsClient::new(&url).run());
        wait_until_connected(&client).await;

        let mut builder = BatchBuilder::new();
        builder
            .add_request::<u64, _, _>("a", Option::<()>::None)
            .unwrap();
        builder
            .add_request::<u64, _, _>("b", Option::<()>::None)
            .unwrap();
        let results = client.batch_request(builder, None).await.unwrap().unwrap();
        let values: Vec<u64> = results
            .into_iter()
            .map(|r| r.unwrap().downcast_response::<u64>().unwrap())
            .collect();
        assert_eq!(values, vec![0, 1]);
    }

    #[tokio::test]
    async fn shutdown_stops_the_connection() {
        let url = start_ws_server(|text| {
            let value: serde_json::Value = serde_json::from_str(text).ok()?;
            let id = value.get("id")?.as_u64()?;
            Some(format!(r#"{{"jsonrpc":"2.0","id":{id},"result":1}}"#))
        })
        .await;

        let client = WsClient::build_from_websocket_client(WebSocketClient::new(&url)).run();
        wait_until_connected(&client).await;

        client.shutdown();
    }
}
