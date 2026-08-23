use crate::client::{BatchResult, PendingBatchRequests, PendingRequests, WsClientInner};
use bytes::Bytes;
use jsonrpc_core::{Error, JsonRpcBatchResponse, JsonRpcIncoming, Notification, Result};
use std::{ops::Range, sync::Arc};
use stream_tungstenite::{
    ConnectionEvent, DisconnectReason, SharedMessage,
    tokio_tungstenite::tungstenite::Message,
};
use tokio::sync::{broadcast, broadcast::error::RecvError};
use tokio::time::{MissedTickBehavior, interval};

/// Sends periodic keepalive pings (if `ping_interval` is configured) and
/// logs connection-lifecycle frames (`Close`/`Ping`/`Pong`) it observes on
/// the shared broadcast channel. Runs for the lifetime of a
/// [`crate::client::Connected`] client.
#[hotpath::measure]
pub(crate) async fn management_task(ws_client: Arc<WsClientInner>) {
    let WsClientInner {
        client,
        ping_interval,
        ..
    } = &*ws_client;
    let mut messages = client.subscribe();

    let mut ticker = ping_interval.map(|d| {
        let mut i = interval(d);
        i.set_missed_tick_behavior(MissedTickBehavior::Delay);
        i
    });

    loop {
        let tick = async {
            match ticker.as_mut() {
                Some(t) => {
                    t.tick().await;
                }
                None => std::future::pending::<()>().await,
            }
        };

        tokio::select! {
            msg = messages.recv() => {
                match msg {
                    Ok(msg) => match msg.as_ref() {
                        Message::Close(frame) => {
                            match frame {
                                Some(f) => tracing::info!(
                                    code = %f.code,
                                    reason = %f.reason,
                                    "websocket connection closed by peer"
                                ),
                                None => tracing::info!("websocket connection closed by peer"),
                            }
                        }
                        Message::Ping(_) => {
                            tracing::trace!("received ping from peer (auto-ponged by transport)");
                        }
                        Message::Pong(_) => {
                            tracing::trace!("received pong (keepalive liveness confirmed)");
                        }
                        _ => {}
                    },
                    Err(RecvError::Lagged(n)) => {
                        tracing::warn!(n, "management task lagged behind websocket broadcast");
                    }
                    Err(RecvError::Closed) => break,
                }
            }
            _ = tick => {
                if let Err(err) = client.send(Message::Ping(Bytes::new())).await {
                    tracing::warn!("failed to send keepalive ping: {err}");
                }
            }
        }
    }
}

/// Decodes one incoming wire message and routes it: a single response/error
/// resolves the matching entry in `pending_requests`, a full batch response
/// resolves the matching entry in `pending_batch_requests` (re-sorted back
/// into caller request-queue order by id), and a notification is
/// broadcast on `notification_channel`. Malformed frames and responses that
/// don't correlate to anything pending are logged and dropped.
pub(crate) async fn handle_message(
    msg: SharedMessage,
    data: &[u8],
    pending_requests: &PendingRequests,
    pending_batch_requests: &PendingBatchRequests,
    notification_channel: &broadcast::Sender<Notification>,
) {
    match JsonRpcIncoming::try_from(data) {
        Ok(JsonRpcIncoming::BatchResponse(batch)) => {
            let mut ids = batch
                .iter()
                .filter_map(|r| match r {
                    JsonRpcBatchResponse::Response(resp) => Some(resp.id),
                    JsonRpcBatchResponse::ErrorResponse(err) => err.id,
                })
                .collect::<Vec<_>>();
            ids.sort_unstable();

            let (Some(&first), Some(&last)) = (ids.first(), ids.last()) else {
                return;
            };

            let is_contiguous = ids
                .windows(2)
                .all(|w| u64::from(w[1]) == u64::from(w[0]) + 1);
            if !is_contiguous {
                return;
            }

            let range = first..=last;

            let Some((_, channel)) = pending_batch_requests.remove(&range) else {
                return;
            };

            let mut items: Vec<Option<Result<Range<usize>>>> =
                (0..ids.len()).map(|_| None).collect();
            for r in batch {
                let (id, item) = match r {
                    JsonRpcBatchResponse::Response(r) => {
                        let start = r.result.get().as_ptr() as usize - data.as_ptr() as usize;
                        let range = start..start + r.result.get().len();
                        (r.id, Ok(range))
                    }
                    JsonRpcBatchResponse::ErrorResponse(err) => match err.id {
                        Some(id) => (id, Err(Error::from(err))),
                        None => continue,
                    },
                };
                let idx = (u64::from(id) - u64::from(first)) as usize;
                items[idx] = Some(item);
            }
            let items = items
                .into_iter()
                .map(|item| item.expect("id range validated to be fully covered above"))
                .collect::<Vec<_>>();

            let _ = channel.send(Ok(BatchResult {
                message: msg,
                items,
            }));
        }
        Ok(JsonRpcIncoming::Response(resp)) => {
            if let Some((_, tx)) = pending_requests.remove(&resp.id) {
                tracing::debug!(id = ?resp.id, "received response");
                let start = resp.result.get().as_ptr() as usize - data.as_ptr() as usize;
                let range = start..start + resp.result.get().len();
                let _ = tx.send(Ok((msg, range)));
            }
        }
        Ok(JsonRpcIncoming::ErrorResponse(err)) => {
            if let Some(id) = err.id
                && let Some((_, tx)) = pending_requests.remove(&id)
            {
                let _ = tx.send(Err(Error::from(err)));
            }
        }
        Ok(JsonRpcIncoming::Notification(data)) => {
            let notification = Notification::from(data);
            let _ = notification_channel.send(notification);
        }
        Err(err) => {
            tracing::warn!("received malformed JSON-RPC frame: {err}");
        }
    }
}

/// Reads every message off the shared broadcast channel and dispatches it
/// via [`handle_message`]. Runs for the lifetime of a
/// [`crate::client::Connected`] client.
#[hotpath::measure]
pub(crate) async fn incoming_task(ws_client: Arc<WsClientInner>) {
    let WsClientInner {
        client,
        pending_requests,
        pending_batch_requests,
        notification_channel,
        ..
    } = &*ws_client;
    let mut messages = client.subscribe();

    loop {
        match messages.recv().await {
            Ok(msg) => match msg.as_ref() {
                Message::Text(data) => {
                    handle_message(
                        msg.clone(),
                        data.as_bytes(),
                        pending_requests,
                        pending_batch_requests,
                        notification_channel,
                    )
                    .await;
                }
                Message::Binary(data) => {
                    handle_message(
                        msg.clone(),
                        data.as_ref(),
                        pending_requests,
                        pending_batch_requests,
                        notification_channel,
                    )
                    .await;
                }
                _ => {}
            },
            Err(RecvError::Lagged(n)) => {
                tracing::warn!(n, "incoming task lagged behind websocket broadcast");
            }
            Err(RecvError::Closed) => break,
        }
    }
}

/// Fails every currently in-flight unary and batch request with a
/// `TransportError` describing `reason`. Driven by `disconnect_task` so
/// callers waiting on a response fail promptly on every disconnect —
/// including reconnects — rather than waiting out their full timeout.
pub(crate) fn fail_pending_requests(
    pending_requests: &PendingRequests,
    pending_batch_requests: &PendingBatchRequests,
    reason: &DisconnectReason,
) {
    for key in pending_requests
        .iter()
        .map(|r| *r.key())
        .collect::<Vec<_>>()
    {
        if let Some((_, tx)) = pending_requests.remove(&key) {
            let _ = tx.send(Err(Error::TransportError(anyhow::anyhow!(
                "connection lost before a response was received: {reason:?}"
            ))));
        }
    }

    for key in pending_batch_requests
        .iter()
        .map(|r| r.key().clone())
        .collect::<Vec<_>>()
    {
        if let Some((_, tx)) = pending_batch_requests.remove(&key) {
            let _ = tx.send(Err(Error::TransportError(anyhow::anyhow!(
                "connection lost before a response was received: {reason:?}"
            ))));
        }
    }
}

/// Watches connection lifecycle events and fails in-flight requests
/// immediately on every disconnect, rather than relying on their timeout to
/// elapse. `client` reconnects transparently, so this must not treat a
/// single disconnect as terminal for the task itself.
#[hotpath::measure]
pub(crate) async fn disconnect_task(ws_client: Arc<WsClientInner>) {
    let WsClientInner {
        client,
        pending_requests,
        pending_batch_requests,
        ..
    } = &*ws_client;
    let mut events = client.subscribe_events();

    loop {
        match events.recv().await {
            Ok(ConnectionEvent::Disconnected { reason }) => {
                tracing::info!(
                    ?reason,
                    "websocket disconnected; failing in-flight requests"
                );
                fail_pending_requests(pending_requests, pending_batch_requests, &reason);
            }
            Ok(_) => {}
            Err(RecvError::Lagged(n)) => {
                tracing::warn!(n, "disconnect task lagged behind connection events");
            }
            Err(RecvError::Closed) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dashmap::DashMap;
    use jsonrpc_core::{Error, MethodIdBuf, RequestId};
    use std::sync::Arc;
    use stream_tungstenite::tokio_tungstenite::tungstenite::Utf8Bytes;
    use tokio::sync::oneshot;

    fn block_on<F: Future>(fut: F) -> F::Output {
        tokio::runtime::Runtime::new().unwrap().block_on(fut)
    }

    fn text_message(json: impl Into<String>) -> SharedMessage {
        Arc::new(Message::Text(Utf8Bytes::from(json.into())))
    }

    fn data_of(msg: &SharedMessage) -> &[u8] {
        match msg.as_ref() {
            Message::Text(d) => d.as_bytes(),
            Message::Binary(d) => d.as_ref(),
            _ => unreachable!(),
        }
    }

    fn value_at(msg: &SharedMessage, range: Range<usize>) -> serde_json::Value {
        serde_json::from_slice(&data_of(msg)[range]).unwrap()
    }

    #[test]
    fn handle_message_resolves_pending_request() {
        block_on(async {
            let pending_requests: PendingRequests = DashMap::new();
            let pending_batch_requests: PendingBatchRequests = DashMap::new();
            let (notif_tx, _) = broadcast::channel(4);

            let (tx, rx) = oneshot::channel();
            pending_requests.insert(RequestId::new(7), tx);

            let msg = text_message(r#"{"jsonrpc":"2.0","id":7,"result":{"foo":42}}"#);
            let data = data_of(&msg);
            handle_message(
                msg.clone(),
                data,
                &pending_requests,
                &pending_batch_requests,
                &notif_tx,
            )
            .await;

            assert!(pending_requests.is_empty());
            let (result_msg, range) = rx.await.unwrap().unwrap();
            assert_eq!(value_at(&result_msg, range), serde_json::json!({"foo": 42}));
        });
    }

    #[test]
    fn handle_message_resolves_pending_request_as_error() {
        block_on(async {
            let pending_requests: PendingRequests = DashMap::new();
            let pending_batch_requests: PendingBatchRequests = DashMap::new();
            let (notif_tx, _) = broadcast::channel(4);

            let (tx, rx) = oneshot::channel();
            pending_requests.insert(RequestId::new(1), tx);

            let msg = text_message(
                r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"Method not found"}}"#,
            );
            let data = data_of(&msg);
            handle_message(
                msg.clone(),
                data,
                &pending_requests,
                &pending_batch_requests,
                &notif_tx,
            )
            .await;

            assert!(pending_requests.is_empty());
            let err = rx.await.unwrap().unwrap_err();
            assert!(matches!(err, Error::RpcError { .. }));
        });
    }

    #[test]
    fn handle_message_broadcasts_notification() {
        block_on(async {
            let pending_requests: PendingRequests = DashMap::new();
            let pending_batch_requests: PendingBatchRequests = DashMap::new();
            let (notif_tx, mut notif_rx) = broadcast::channel(4);

            let msg = text_message(r#"{"jsonrpc":"2.0","method":"tick","params":[1]}"#);
            let data = data_of(&msg);
            handle_message(
                msg.clone(),
                data,
                &pending_requests,
                &pending_batch_requests,
                &notif_tx,
            )
            .await;

            let notification = notif_rx.try_recv().unwrap();
            assert_eq!(notification.method, MethodIdBuf::from("tick"));
        });
    }

    #[test]
    fn handle_message_resolves_full_batch() {
        block_on(async {
            let pending_requests: PendingRequests = DashMap::new();
            let pending_batch_requests: PendingBatchRequests = DashMap::new();
            let (notif_tx, _) = broadcast::channel(4);

            let (tx, rx) = oneshot::channel();
            let key = RequestId::new(1)..=RequestId::new(3);
            pending_batch_requests.insert(key.clone(), tx);

            let msg = text_message(
                r#"[{"jsonrpc":"2.0","id":1,"result":1},{"jsonrpc":"2.0","id":2,"result":2},{"jsonrpc":"2.0","id":3,"result":3}]"#,
            );
            let data = data_of(&msg);
            handle_message(
                msg.clone(),
                data,
                &pending_requests,
                &pending_batch_requests,
                &notif_tx,
            )
            .await;

            assert!(pending_batch_requests.is_empty());
            let batch = rx.await.unwrap().unwrap();
            assert_eq!(batch.items.len(), 3);
            for (i, item) in batch.items.into_iter().enumerate() {
                let range = item.unwrap();
                assert_eq!(value_at(&batch.message, range), serde_json::json!(i + 1));
            }
        });
    }

    /// JSON-RPC 2.0 explicitly permits a server to answer a batch out of
    /// order; the client must correlate by id, not by position on the wire.
    #[test]
    fn handle_message_resolves_batch_out_of_order() {
        block_on(async {
            let pending_requests: PendingRequests = DashMap::new();
            let pending_batch_requests: PendingBatchRequests = DashMap::new();
            let (notif_tx, _) = broadcast::channel(4);

            let (tx, rx) = oneshot::channel();
            let key = RequestId::new(1)..=RequestId::new(3);
            pending_batch_requests.insert(key.clone(), tx);

            let msg = text_message(
                r#"[{"jsonrpc":"2.0","id":3,"result":"c"},{"jsonrpc":"2.0","id":1,"result":"a"},{"jsonrpc":"2.0","id":2,"result":"b"}]"#,
            );
            let data = data_of(&msg);
            handle_message(
                msg.clone(),
                data,
                &pending_requests,
                &pending_batch_requests,
                &notif_tx,
            )
            .await;

            let batch = rx.await.unwrap().unwrap();
            assert_eq!(batch.items.len(), 3);
            let expected = ["a", "b", "c"];
            for (i, item) in batch.items.into_iter().enumerate() {
                let range = item.unwrap();
                assert_eq!(
                    value_at(&batch.message, range),
                    serde_json::json!(expected[i])
                );
            }
        });
    }

    #[test]
    fn handle_message_ignores_batch_with_gap() {
        block_on(async {
            let pending_requests: PendingRequests = DashMap::new();
            let pending_batch_requests: PendingBatchRequests = DashMap::new();
            let (notif_tx, _) = broadcast::channel(4);

            let (tx, mut rx) = oneshot::channel();
            let key = RequestId::new(1)..=RequestId::new(3);
            pending_batch_requests.insert(key.clone(), tx);

            let msg = text_message(
                r#"[{"jsonrpc":"2.0","id":1,"result":1},{"jsonrpc":"2.0","id":3,"result":3}]"#,
            );
            let data = data_of(&msg);
            handle_message(
                msg.clone(),
                data,
                &pending_requests,
                &pending_batch_requests,
                &notif_tx,
            )
            .await;

            assert!(pending_batch_requests.contains_key(&key));
            assert!(rx.try_recv().is_err());
        });
    }

    #[test]
    fn handle_message_ignores_batch_with_duplicate_id() {
        block_on(async {
            let pending_requests: PendingRequests = DashMap::new();
            let pending_batch_requests: PendingBatchRequests = DashMap::new();
            let (notif_tx, _) = broadcast::channel(4);

            let (tx, mut rx) = oneshot::channel();
            let key = RequestId::new(1)..=RequestId::new(2);
            pending_batch_requests.insert(key.clone(), tx);

            let msg = text_message(
                r#"[{"jsonrpc":"2.0","id":1,"result":1},{"jsonrpc":"2.0","id":1,"result":99}]"#,
            );
            let data = data_of(&msg);
            handle_message(
                msg.clone(),
                data,
                &pending_requests,
                &pending_batch_requests,
                &notif_tx,
            )
            .await;

            assert!(pending_batch_requests.contains_key(&key));
            assert!(rx.try_recv().is_err());
        });
    }

    #[test]
    fn fail_pending_requests_drains_and_errors_both_maps() {
        let pending_requests: PendingRequests = DashMap::new();
        let pending_batch_requests: PendingBatchRequests = DashMap::new();

        let (tx1, mut rx1) = oneshot::channel();
        pending_requests.insert(RequestId::new(1), tx1);

        let (tx2, mut rx2) = oneshot::channel();
        pending_batch_requests.insert(RequestId::new(2)..=RequestId::new(4), tx2);

        fail_pending_requests(
            &pending_requests,
            &pending_batch_requests,
            &DisconnectReason::Normal,
        );

        assert!(pending_requests.is_empty());
        assert!(pending_batch_requests.is_empty());
        assert!(matches!(
            rx1.try_recv().unwrap().unwrap_err(),
            Error::TransportError(_)
        ));
        assert!(matches!(
            rx2.try_recv().unwrap().unwrap_err(),
            Error::TransportError(_)
        ));
    }
}
