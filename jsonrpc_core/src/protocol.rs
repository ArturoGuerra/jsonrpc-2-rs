use crate::method_id::{MethodId, MethodIdBuf};
use crate::params::{deserialize_params_with, params_to_raw_value, serialize_params};
use crate::request_id::RequestId;
use crate::result::Result;
use crate::version::JsonRpcVersion;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use smallvec::SmallVec;
use std::borrow::Cow;

/// An owned request-or-notification, ready to serialize to the wire.
/// `#[serde(untagged)]` writes whichever variant is present using its own
/// `Serialize` impl directly — a request keeps its `id`, a notification has
/// none — so a batch mixing both serializes as one flat JSON array without
/// an extra discriminant.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum JsonRpcOutgoing<'a> {
    Request(JsonRpcRequest<'a>),
    Notification(JsonRpcNotification<'a>),
}

/// The borrowed counterpart of [`JsonRpcOutgoing`].
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum JsonRpcOutgoingRef<'a> {
    Request(&'a JsonRpcRequest<'a>),
    Notification(&'a JsonRpcNotification<'a>),
}

/// The `error` member of a JSON-RPC error response.
#[derive(Debug, Clone, Serialize)]
pub struct JsonRpcError<'a> {
    pub code: i64,
    pub message: &'a str,
    pub data: Option<Cow<'a, RawValue>>,
}

/// A JSON-RPC request. `params: None` must be serialized by omitting the
/// field, not as `null` — per spec the member MAY be omitted, but if present
/// must be a Structured value (an Array or Object).
#[derive(Debug, Clone, Serialize)]
pub struct JsonRpcRequest<'a> {
    pub jsonrpc: JsonRpcVersion,
    pub id: RequestId,
    pub method: Cow<'a, MethodId>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_params_with"
    )]
    pub params: Option<Cow<'a, RawValue>>,
}

impl<'a> JsonRpcRequest<'a> {
    /// Builds a request from an already-serialized params value, checking
    /// it's a Structured value (an Array or Object) per spec.
    pub fn try_new_owned<T: Serialize + Send + Sync>(
        id: RequestId,
        method: MethodIdBuf,
        params: Option<T>,
    ) -> Result<Self> {
        let params = params.map(|p| serialize_params(&p)).transpose()?;
        Ok(Self {
            jsonrpc: JsonRpcVersion,
            id,
            method: Cow::Owned(method),
            params: params.map(Cow::Owned),
        })
    }

    /// Builds a request whose `method`/`params` borrow from `method` and
    /// from the caller-owned `buf` — avoids allocating for `params` that
    /// fit within `buf`'s inline capacity. See
    /// `params_to_raw_value`.
    pub fn try_new<T: Serialize + Send + Sync>(
        id: RequestId,
        method: &'a MethodId,
        params: Option<T>,
        buf: &'a mut SmallVec<[u8; 1024]>,
    ) -> Result<Self> {
        let params = match params {
            None => None,
            Some(params) => Some(Cow::Borrowed(params_to_raw_value(&params, buf)?)),
        };

        Ok(Self {
            jsonrpc: JsonRpcVersion,
            id,
            method: Cow::Borrowed(method),
            params,
        })
    }
}

/// Serializes a request to its wire JSON string.
impl<'a> TryFrom<JsonRpcRequest<'a>> for String {
    type Error = serde_json::Error;

    fn try_from(value: JsonRpcRequest) -> std::result::Result<Self, Self::Error> {
        serde_json::to_string(&value)
    }
}

/// Serializes a request to its wire JSON bytes.
impl<'a> TryFrom<JsonRpcRequest<'a>> for Vec<u8> {
    type Error = serde_json::Error;

    fn try_from(value: JsonRpcRequest<'a>) -> std::result::Result<Self, Self::Error> {
        serde_json::to_vec(&value)
    }
}

/// A JSON-RPC notification — a request with no `id` that the peer must not
/// reply to. Same `params` omission rule as [`JsonRpcRequest`].
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(bound(deserialize = "'de: 'a"))]
pub struct JsonRpcNotification<'a> {
    pub jsonrpc: JsonRpcVersion,
    pub method: Cow<'a, MethodId>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_params_with"
    )]
    pub params: Option<Cow<'a, RawValue>>,
}

impl<'a> JsonRpcNotification<'a> {
    /// Builds a notification whose `method`/`params` borrow from `method`
    /// and from the caller-owned `buf` — avoids allocating for `params`
    /// that fit within `buf`'s inline capacity. See
    /// `params_to_raw_value`.
    pub fn try_new<T: Serialize + Send + Sync>(
        method: &'a MethodId,
        params: Option<T>,
        buf: &'a mut SmallVec<[u8; 1024]>,
    ) -> Result<Self> {
        let params = match params {
            None => None,
            Some(params) => Some(Cow::Borrowed(params_to_raw_value(&params, buf)?)),
        };

        Ok(Self {
            jsonrpc: JsonRpcVersion,
            method: Cow::Borrowed(method),
            params,
        })
    }

    /// Builds a notification from an already-serialized params value,
    /// checking it's a Structured value (an Array or Object) per spec.
    pub fn try_new_owned<T: Serialize + Send + Sync>(
        method: MethodIdBuf,
        params: Option<T>,
    ) -> Result<Self> {
        let params = params.map(|p| serialize_params(&p)).transpose()?;
        Ok(Self {
            jsonrpc: JsonRpcVersion,
            method: Cow::Owned(method),
            params: params.map(Cow::Owned),
        })
    }
}

/// Owned, public-facing form of a notification received from the peer.
/// `jsonrpc` is omitted since it's already validated by the time this is
/// built, and unlike [`JsonRpcNotification`] this carries no borrow on the
/// buffer it was decoded from — that's the whole reason it exists, since
/// this is what crosses a channel/stream boundary into
/// [`BidirectionalClient::subscribe`](crate::client::BidirectionalClient::subscribe).
#[cfg(feature = "bidirectional")]
#[derive(Debug, Clone)]
pub struct Notification {
    pub method: MethodIdBuf,
    pub params: Option<Box<RawValue>>,
}

/// Copies the borrowed wire data out into an owned `Notification`.
#[cfg(feature = "bidirectional")]
impl From<JsonRpcNotification<'_>> for Notification {
    fn from(data: JsonRpcNotification<'_>) -> Self {
        Self {
            method: data.method.into_owned(),
            params: data.params.map(|p| p.into_owned()),
        }
    }
}

/// A successful JSON-RPC response. `result` is REQUIRED on a success
/// response — it's `error` that's mutually exclusive with it, not `result`
/// itself being optional. Not `Serialize` (see the crate-level docs): this
/// crate only ever decodes responses, never emits them.
#[derive(Debug, Clone, Deserialize)]
pub struct JsonRpcResponse<'a> {
    pub jsonrpc: JsonRpcVersion,
    pub id: RequestId,
    pub result: Cow<'a, RawValue>,
}

/// A JSON-RPC error response. `id` is `None` only when the peer couldn't
/// determine which request this error belongs to (e.g. on a parse error).
#[derive(Debug, Clone, Serialize)]
pub struct JsonRpcErrorResponse<'a> {
    pub jsonrpc: JsonRpcVersion,
    pub id: Option<RequestId>,
    pub error: JsonRpcError<'a>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_from_string_omits_params_when_none() {
        let request = JsonRpcRequest::try_new_owned(
            RequestId::new(1),
            MethodIdBuf::new("ping"),
            Option::<()>::None,
        )
        .unwrap();
        let json = String::try_from(request).unwrap();
        assert_eq!(json, r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#);
    }

    #[test]
    fn try_from_string_includes_structured_params() {
        let params = RawValue::from_string(r#"[1,2,3]"#.to_string()).unwrap();
        let request =
            JsonRpcRequest::try_new_owned(RequestId::new(2), MethodIdBuf::new("sum"), Some(params))
                .unwrap();
        let json = String::try_from(request).unwrap();
        assert_eq!(
            json,
            r#"{"jsonrpc":"2.0","id":2,"method":"sum","params":[1,2,3]}"#
        );
    }

    #[test]
    fn try_from_vec_u8_matches_try_from_string_bytes() {
        let params = RawValue::from_string(r#"{"a":1}"#.to_string()).unwrap();
        let request =
            JsonRpcRequest::try_new_owned(RequestId::new(3), MethodIdBuf::new("do"), Some(params))
                .unwrap();
        let bytes = Vec::try_from(request).unwrap();

        let params = RawValue::from_string(r#"{"a":1}"#.to_string()).unwrap();
        let request =
            JsonRpcRequest::try_new_owned(RequestId::new(3), MethodIdBuf::new("do"), Some(params))
                .unwrap();
        let string = String::try_from(request).unwrap();

        assert_eq!(bytes, string.into_bytes());
    }
}
