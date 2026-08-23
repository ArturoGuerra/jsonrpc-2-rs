use crate::error::Error;
use crate::method_id::MethodIdBuf;
use crate::params::serialize_params;
use crate::protocol::{JsonRpcNotification, JsonRpcOutgoing, JsonRpcRequest};
use crate::request_id::{RequestId, RequestIdManager};
use crate::result::Result;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::value::RawValue;
use std::any::Any;
use std::ops::RangeInclusive;

/// Recovers a queued request's registered response type from the raw JSON
/// text of its reply.
type ResponseParser =
    Box<dyn Fn(&str) -> Result<Box<dyn Any + Send + Sync>> + Send + Sync + 'static>;

/// One batch response item, still type-erased — see [`DowncastResponse`].
pub type BatchResponseItem = Result<Box<dyn Any + Send + Sync>>;

/// A request queued via [`BatchBuilder::add_request`], already serialized.
type QueuedRequest = (MethodIdBuf, Option<Box<RawValue>>, ResponseParser);

/// A notification queued via [`BatchBuilder::add_notification`], already
/// serialized.
type QueuedNotification = (MethodIdBuf, Option<Box<RawValue>>);

/// The result of [`BatchBuilder::build`]: every queued request and
/// notification, ready to send as a single JSON-RPC batch.
pub struct Batch<'a> {
    pub request_id_range: Option<RangeInclusive<RequestId>>,
    pub parsers: Vec<ResponseParser>,
    pub items: Vec<JsonRpcOutgoing<'a>>,
}

/// Recovers a batch item's concrete response type from the type-erased value
/// its parser closure produced.
pub trait DowncastResponse {
    /// `Err(Error::InvalidMessage(_))` means `T` doesn't match what was
    /// registered via `add_request::<T, _, _>` for this item.
    fn downcast_response<T: 'static>(self) -> Result<T>;
}

impl DowncastResponse for Box<dyn Any + Send + Sync> {
    fn downcast_response<T: 'static>(self) -> Result<T> {
        self.downcast::<T>()
            .map(|b| *b)
            .map_err(|_| Error::InvalidMessage("batch response type mismatch"))
    }
}

/// Queues up requests and notifications to send together as a single
/// JSON-RPC batch via [`crate::client::Client::batch_request`].
/// Every `add_*` call serializes and validates its params immediately;
/// [`build`](BatchBuilder::build) only propagates whichever failed first and
/// reserves ids.
pub struct BatchBuilder {
    requests: Vec<QueuedRequest>,
    notifications: Vec<QueuedNotification>,
}

impl Default for BatchBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl BatchBuilder {
    /// Creates an empty builder.
    pub fn new() -> Self {
        Self {
            requests: vec![],
            notifications: vec![],
        }
    }

    /// Queues a fire-and-forget notification with `method`/`params` to be
    /// sent as part of the batch. Never reserves a request id.
    pub fn add_notification<Method, Params>(
        &mut self,
        method: Method,
        params: Option<Params>,
    ) -> Result<()>
    where
        Method: Into<MethodIdBuf> + Send + Sync,
        Params: Serialize + Send + Sync,
    {
        let params = params.map(|p| serialize_params(&p)).transpose()?;
        self.notifications.push((method.into(), params));

        Ok(())
    }

    /// Queues a request with `method`/`params` to be sent as part of the
    /// batch, remembering how to parse *this request's own* response as `T`
    /// — unlike [`crate::client::Client::batch_request`], different requests in the same
    /// batch can expect different response types.
    pub fn add_request<Response, Method, Params>(
        &mut self,
        method: Method,
        params: Option<Params>,
    ) -> Result<()>
    where
        Method: Into<MethodIdBuf> + Send + Sync,
        Params: Serialize + Send + Sync,
        Response: DeserializeOwned + Send + Sync + 'static,
    {
        let closure = |v: &str| -> Result<Box<dyn Any + Send + Sync>> {
            Ok(Box::new(serde_json::from_str::<Response>(v)?))
        };
        let params = params.map(|p| serialize_params(&p)).transpose()?;
        self.requests
            .push((method.into(), params, Box::new(closure)));
        Ok(())
    }

    /// Reserves ids for the queued requests and serializes everything queued
    /// into a [`Batch`]. `Ok(None)` only when nothing was queued at all — a
    /// notifications-only batch is still worth sending, so it builds fine
    /// with no reserved id range. `Err` if any queued params fail to
    /// serialize to a JSON-RPC Structured value.
    pub fn build<'a>(self, idmgr: &'a RequestIdManager) -> Result<Option<Batch<'a>>> {
        if self.requests.is_empty() && self.notifications.is_empty() {
            return Ok(None);
        }

        let request_id_range = idmgr.reserve_range(self.requests.len() as u64);
        let mut items = Vec::with_capacity(self.requests.len() + self.notifications.len());
        let mut parsers = Vec::with_capacity(self.requests.len());

        if let Some(range) = &request_id_range {
            let ids = u64::from(*range.start())..=u64::from(*range.end());
            for ((method, params, parser), id) in self.requests.into_iter().zip(ids) {
                items.push(JsonRpcOutgoing::Request(JsonRpcRequest::try_new_owned(
                    RequestId::from(id),
                    method,
                    params,
                )?));
                parsers.push(parser);
            }
        }

        for (method, params) in self.notifications {
            items.push(JsonRpcOutgoing::Notification(
                JsonRpcNotification::try_new_owned(method, params)?,
            ));
        }

        Ok(Some(Batch {
            items,
            parsers,
            request_id_range,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_returns_none_for_empty_batch() {
        let idmgr = RequestIdManager::new();
        let builder = BatchBuilder::new();
        assert!(builder.build(&idmgr).unwrap().is_none());
        assert_eq!(idmgr.current_id(), RequestId::new(0));
    }

    #[test]
    fn build_reserves_contiguous_range_matching_request_count() {
        let idmgr = RequestIdManager::new();
        let mut builder = BatchBuilder::new();
        builder
            .add_request::<i32, _, _>("a", Option::<()>::None)
            .unwrap();
        builder
            .add_request::<i32, _, _>("b", Option::<()>::None)
            .unwrap();
        builder
            .add_request::<i32, _, _>("c", Option::<()>::None)
            .unwrap();

        let batch = builder.build(&idmgr).unwrap().unwrap();
        let range = batch.request_id_range.clone().unwrap();
        let requests: Vec<&JsonRpcRequest> = batch
            .items
            .iter()
            .map(|item| match item {
                JsonRpcOutgoing::Request(req) => req,
                JsonRpcOutgoing::Notification(_) => panic!("expected only requests"),
            })
            .collect();

        assert_eq!(requests.len(), 3);
        assert_eq!(*range.start(), requests[0].id);
        assert_eq!(*range.end(), requests[2].id);
        assert_eq!(u64::from(*range.end()) - u64::from(*range.start()) + 1, 3);
    }

    #[test]
    fn build_includes_notification_only_batch_without_reserving_ids() {
        let idmgr = RequestIdManager::new();
        let mut builder = BatchBuilder::new();
        builder
            .add_notification("ping", Option::<()>::None)
            .unwrap();
        builder
            .add_notification("pong", Option::<()>::None)
            .unwrap();

        let batch = builder.build(&idmgr).unwrap().unwrap();
        assert!(batch.request_id_range.is_none());
        assert_eq!(batch.items.len(), 2);
        assert!(
            batch
                .items
                .iter()
                .all(|item| matches!(item, JsonRpcOutgoing::Notification(_)))
        );
        assert_eq!(idmgr.current_id(), RequestId::new(0));
    }

    #[test]
    fn add_request_propagates_param_serialization_errors() {
        let mut builder = BatchBuilder::new();
        assert!(builder.add_request::<i32, _, _>("a", Some(42i32)).is_err());
    }

    #[test]
    fn add_request_closure_roundtrips_through_downcast() {
        let idmgr = RequestIdManager::new();
        let mut builder = BatchBuilder::new();
        builder
            .add_request::<i32, _, _>("a", Option::<()>::None)
            .unwrap();

        let batch = builder.build(&idmgr).unwrap().unwrap();
        let parser = &batch.parsers[0];

        let boxed = parser("42").unwrap();
        assert_eq!(boxed.downcast_response::<i32>().unwrap(), 42);

        let boxed = parser("42").unwrap();
        assert!(matches!(
            boxed.downcast_response::<String>(),
            Err(Error::InvalidMessage(_))
        ));
    }
}
