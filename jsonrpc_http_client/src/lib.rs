use jsonrpc_core::{
    Batch, BatchBuilder, BatchResponseItem, Client, Error, IntoOptionParams,
    JsonRpcBatchResponse, JsonRpcIncoming, JsonRpcNotification, JsonRpcRequest, MethodIdBuf,
    RequestId, RequestIdManager, Result, decode_single_response,
};
use reqwest::Method as HttpMethod;
use reqwest::{Client as ReqwestClient, RequestBuilder};
use serde::de::DeserializeOwned;
use smallvec::SmallVec;
use std::ops::RangeInclusive;
use std::time::Duration;
use tracing::{debug, info};

/// A [`jsonrpc_core::Client`] that sends every request/notification
/// as its own HTTP POST to a single JSON-RPC endpoint. Not a
/// `jsonrpc_core::BidirectionalClient` (requires the `bidirectional` feature)
/// — HTTP has no channel for
/// the peer to push unsolicited notifications back.
pub struct HttpClient<A: AuthStrategy> {
    client: ReqwestClient,
    endpoint: String,
    auth_strategy: A,
    id_manager: RequestIdManager,
}

/// Applied to every outgoing request to attach authentication (a bearer
/// token, an API key header, request signing, ...).
pub trait AuthStrategy: Send + Sync {
    fn apply(&self, builder: RequestBuilder) -> RequestBuilder;
}

pub struct DefaultAuthStrategy;

impl AuthStrategy for DefaultAuthStrategy {
    fn apply(&self, builder: RequestBuilder) -> RequestBuilder {
        builder
    }
}

/// Rejects a non-2xx HTTP response before its body is treated as a JSON-RPC
/// message — a proxy error page, an auth failure, etc. isn't JSON-RPC at
/// all, and parsing it as such would otherwise surface as an opaque
/// `SerdeError`/`InvalidMessage` with no indication it was actually a
/// transport-level failure.
fn ensure_success(response: &reqwest::Response) -> Result<()> {
    if response.status().is_success() {
        Ok(())
    } else {
        Err(Error::TransportError(anyhow::anyhow!(
            "HTTP request failed with status {}",
            response.status()
        )))
    }
}

/// Places each decoded batch response item at the index matching the
/// caller's original request-queue order, regardless of what order the
/// items arrived on the wire in. Per spec, batch responses "MAY be returned
/// in an array in any order" — the client must correlate by `id`. Mirrors
/// `jsonrpc_ws_client::handle_message`'s out-of-order batch correlation,
/// adapted for HTTP's single-shot request/response shape: any mismatch here
/// (a wrong count, a gap, a duplicate, or an error item with no `id`)
/// surfaces as an `Err` immediately, rather than a reply the ws client would
/// otherwise silently drop and let the caller time out on.
fn correlate_batch_items<'a, F>(
    items: Vec<JsonRpcBatchResponse<'a>>,
    range: RangeInclusive<RequestId>,
    parsers: Vec<F>,
) -> Result<Vec<BatchResponseItem>>
where
    F: Fn(&str) -> BatchResponseItem,
{
    let first = *range.start();
    let last = *range.end();
    let expected_len = u64::from(last) - u64::from(first) + 1;

    if items.len() as u64 != expected_len {
        return Err(Error::InvalidMessage(
            "batch response item count does not match the sent batch",
        ));
    }

    let mut slots: Vec<Option<JsonRpcBatchResponse<'a>>> =
        (0..expected_len).map(|_| None).collect();
    for item in items {
        let id = match &item {
            JsonRpcBatchResponse::Response(r) => Some(r.id),
            JsonRpcBatchResponse::ErrorResponse(e) => e.id,
        }
        .ok_or(Error::InvalidMessage(
            "a batch error response is missing its `id`; can't correlate it to a request",
        ))?;

        if id < first || id > last {
            return Err(Error::InvalidMessage(
                "batch response id is outside the range of ids sent",
            ));
        }

        let idx = (u64::from(id) - u64::from(first)) as usize;
        if slots[idx].is_some() {
            return Err(Error::InvalidMessage(
                "batch response contains a duplicate id",
            ));
        }
        slots[idx] = Some(item);
    }

    Ok(slots
        .into_iter()
        .zip(parsers)
        .map(|(item, parser)| {
            match item.expect("batch response slot unexpectedly empty") {
                JsonRpcBatchResponse::Response(r) => parser(r.result.get()),
                JsonRpcBatchResponse::ErrorResponse(e) => Err(Error::from(e)),
            }
        })
        .collect())
}

impl<A: AuthStrategy> HttpClient<A> {
    /// Builds a client posting to `endpoint`. `timeout` is the default
    /// applied to every request/connection unless overridden per-call via
    /// [`Client::request`]/[`Client::batch_request`]'s own `timeout` argument.
    pub fn try_new(
        endpoint: impl Into<String>,
        auth_strategy: A,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        let mut builder = ReqwestClient::builder();
        if let Some(timeout) = timeout {
            builder = builder.timeout(timeout);
        }
        let client = builder
            .build()
            .map_err(|e| Error::TransportError(e.into()))?;
        Ok(Self {
            client,
            endpoint: endpoint.into(),
            auth_strategy,
            id_manager: RequestIdManager::new(),
        })
    }

    /// A fresh POST request builder for `endpoint`, with content type and
    /// auth already applied.
    #[inline]
    fn req_builder(&self) -> RequestBuilder {
        let mut builder = self.client.request(HttpMethod::POST, &self.endpoint);
        builder = builder.header("Content-Type", "application/json");
        builder = self.auth_strategy.apply(builder);
        builder
    }

    /// Builds and POSTs `builder`'s queued items as a single JSON-RPC
    /// batch, then correlates the reply back to each queued request via
    /// [`correlate_batch_items`]. Shared by [`Client::batch_request`]'s two
    /// callers (with/without an explicit timeout).
    async fn send_batch(
        &self,
        builder: BatchBuilder,
        req_builder: RequestBuilder,
    ) -> Result<Option<Vec<BatchResponseItem>>> {
        let Some(Batch {
            items,
            parsers,
            request_id_range,
        }) = builder.build(&self.id_manager)?
        else {
            return Ok(None);
        };

        let body = serde_json::to_string(&items).map_err(Error::SerdeError)?;

        let Some(range) = request_id_range else {
            req_builder
                .body(body)
                .send()
                .await
                .map_err(|e| Error::TransportError(e.into()))?;
            return Ok(None);
        };

        let response = req_builder
            .body(body)
            .send()
            .await
            .map_err(|e| Error::TransportError(e.into()))?;
        ensure_success(&response)?;
        let bytes = response
            .bytes()
            .await
            .map_err(|e| Error::TransportError(e.into()))?;

        let items = match JsonRpcIncoming::try_from(&bytes[..])? {
            JsonRpcIncoming::BatchResponse(items) => items,
            _ => return Err(Error::InvalidMessage("expected a batch response")),
        };

        let result = correlate_batch_items(items, range, parsers)?;
        Ok(Some(result))
    }
}

#[hotpath::measure_all]
impl<A: AuthStrategy> Client for HttpClient<A> {
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
        let mut builder = self.req_builder();
        if let Some(timeout) = timeout {
            builder = builder.timeout(timeout);
        }
        let method = method.into();
        let mut buf = SmallVec::new();
        let request = JsonRpcRequest::try_new(
            self.id_manager.reserve_id(),
            method.as_ref(),
            params.into_option_params(),
            &mut buf,
        )?;
        let body = serde_json::to_string(&request).map_err(Error::SerdeError)?;
        debug!(?request, ?body, "sending request");

        let response = builder
            .body(body)
            .send()
            .await
            .map_err(|e| Error::TransportError(e.into()))?;
        info!("Response status: {}", response.status());
        ensure_success(&response)?;
        let bytes = response
            .bytes()
            .await
            .map_err(|e| Error::TransportError(e.into()))?;
        let incoming = JsonRpcIncoming::try_from(&bytes[..])?;
        decode_single_response(incoming, request.id)
    }

    async fn notify<Method, Params>(&self, method: Method, params: Params) -> Result<()>
    where
        Method: Into<MethodIdBuf> + Send + Sync,
        Params: IntoOptionParams,
    {
        let builder = self.req_builder();
        let method = method.into();
        let mut buf = SmallVec::new();
        let notification =
            JsonRpcNotification::try_new(method.as_ref(), params.into_option_params(), &mut buf)?;
        let body = serde_json::to_string(&notification).map_err(Error::SerdeError)?;
        builder
            .body(body)
            .send()
            .await
            .map_err(|e| Error::TransportError(e.into()))?;

        Ok(())
    }

    async fn batch_request(
        &self,
        builder: BatchBuilder,
        timeout: Option<Duration>,
    ) -> Result<Option<Vec<BatchResponseItem>>> {
        let mut req_builder = self.req_builder();
        if let Some(timeout) = timeout {
            req_builder = req_builder.timeout(timeout);
        }
        self.send_batch(builder, req_builder).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonrpc_core::{DowncastResponse, JsonRpcErrorCode};
    use std::io::{Read, Write};
    use std::net::TcpListener;

    /// Spins up a background thread that accepts exactly one HTTP
    /// connection, discards the request, and writes back a fixed 200 OK
    /// response with `body`. Returns the server's `http://host:port/`
    /// endpoint.
    fn mock_server(body: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0u8; 8192];
            let _ = stream.read(&mut buf);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body,
            );
            let _ = stream.write_all(response.as_bytes());
        });
        format!("http://{addr}/")
    }

    /// Like [`mock_server`], but with a caller-chosen HTTP status line
    /// instead of a fixed `200 OK` — for exercising non-2xx handling.
    fn mock_server_with_status(status_line: &'static str, body: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0u8; 8192];
            let _ = stream.read(&mut buf);
            let response = format!(
                "{}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                status_line,
                body.len(),
                body,
            );
            let _ = stream.write_all(response.as_bytes());
        });
        format!("http://{addr}/")
    }

    #[tokio::test]
    async fn request_round_trip_returns_deserialized_result() {
        let endpoint = mock_server(r#"{"jsonrpc":"2.0","id":0,"result":42}"#);
        let client = HttpClient::try_new(endpoint, DefaultAuthStrategy, None).unwrap();
        let result: i64 = client.request_without_params("ping", None).await.unwrap();
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn request_maps_error_response_to_rpc_error() {
        let endpoint = mock_server(
            r#"{"jsonrpc":"2.0","id":0,"error":{"code":-32601,"message":"Method not found"}}"#,
        );
        let client = HttpClient::try_new(endpoint, DefaultAuthStrategy, None).unwrap();
        let result = client.request_without_params::<_, i64>("missing", None).await;
        assert!(matches!(
            result,
            Err(Error::RpcError {
                code: JsonRpcErrorCode::MethodNotFound,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn request_rejects_response_with_mismatched_id() {
        let endpoint = mock_server(r#"{"jsonrpc":"2.0","id":99,"result":42}"#);
        let client = HttpClient::try_new(endpoint, DefaultAuthStrategy, None).unwrap();
        let result = client.request_without_params::<_, i64>("ping", None).await;
        assert!(matches!(result, Err(Error::InvalidMessage(_))));
    }

    #[tokio::test]
    async fn request_maps_malformed_body_to_serde_error() {
        let endpoint = mock_server("not json");
        let client = HttpClient::try_new(endpoint, DefaultAuthStrategy, None).unwrap();
        let result = client.request_without_params::<_, i64>("ping", None).await;
        assert!(matches!(result, Err(Error::SerdeError(_))));
    }

    #[tokio::test]
    async fn request_rejects_non_success_status_before_parsing_body() {
        let endpoint =
            mock_server_with_status("HTTP/1.1 500 Internal Server Error", "<html>oops</html>");
        let client = HttpClient::try_new(endpoint, DefaultAuthStrategy, None).unwrap();
        let result = client.request_without_params::<_, i64>("ping", None).await;
        assert!(matches!(result, Err(Error::TransportError(_))));
    }

    #[tokio::test]
    async fn batch_request_zips_responses_to_requests_in_order() {
        let endpoint = mock_server(
            r#"[{"jsonrpc":"2.0","id":0,"result":1},{"jsonrpc":"2.0","id":1,"result":2}]"#,
        );
        let client = HttpClient::try_new(endpoint, DefaultAuthStrategy, None).unwrap();
        let mut builder = BatchBuilder::new();
        builder
            .add_request::<i64, _, _>("a", Option::<()>::None)
            .unwrap();
        builder
            .add_request::<i64, _, _>("b", Option::<()>::None)
            .unwrap();

        let results = client.batch_request(builder, None).await.unwrap().unwrap();
        let values: Vec<i64> = results
            .into_iter()
            .map(|r| r.unwrap().downcast_response::<i64>().unwrap())
            .collect();
        assert_eq!(values, vec![1, 2]);
    }

    #[tokio::test]
    async fn batch_request_correlates_out_of_order_responses_by_id() {
        let endpoint = mock_server(
            r#"[{"jsonrpc":"2.0","id":1,"result":2},{"jsonrpc":"2.0","id":0,"result":1}]"#,
        );
        let client = HttpClient::try_new(endpoint, DefaultAuthStrategy, None).unwrap();
        let mut builder = BatchBuilder::new();
        builder
            .add_request::<i64, _, _>("a", Option::<()>::None)
            .unwrap();
        builder
            .add_request::<i64, _, _>("b", Option::<()>::None)
            .unwrap();

        let results = client.batch_request(builder, None).await.unwrap().unwrap();
        let values: Vec<i64> = results
            .into_iter()
            .map(|r| r.unwrap().downcast_response::<i64>().unwrap())
            .collect();
        assert_eq!(values, vec![1, 2]);
    }

    #[tokio::test]
    async fn batch_request_rejects_response_with_id_gap() {
        let endpoint = mock_server(r#"[{"jsonrpc":"2.0","id":0,"result":1}]"#);
        let client = HttpClient::try_new(endpoint, DefaultAuthStrategy, None).unwrap();
        let mut builder = BatchBuilder::new();
        builder
            .add_request::<i64, _, _>("a", Option::<()>::None)
            .unwrap();
        builder
            .add_request::<i64, _, _>("b", Option::<()>::None)
            .unwrap();

        let result = client.batch_request(builder, None).await;
        assert!(matches!(result, Err(Error::InvalidMessage(_))));
    }

    #[tokio::test]
    async fn batch_request_rejects_response_with_duplicate_id() {
        let endpoint = mock_server(
            r#"[{"jsonrpc":"2.0","id":0,"result":1},{"jsonrpc":"2.0","id":0,"result":2}]"#,
        );
        let client = HttpClient::try_new(endpoint, DefaultAuthStrategy, None).unwrap();
        let mut builder = BatchBuilder::new();
        builder
            .add_request::<i64, _, _>("a", Option::<()>::None)
            .unwrap();
        builder
            .add_request::<i64, _, _>("b", Option::<()>::None)
            .unwrap();

        let result = client.batch_request(builder, None).await;
        assert!(matches!(result, Err(Error::InvalidMessage(_))));
    }
}
