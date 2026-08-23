use crate::error::Error;
use crate::method_id::MethodId;
use crate::protocol::{JsonRpcError, JsonRpcErrorResponse, JsonRpcNotification, JsonRpcResponse};
use crate::request_id::RequestId;
use crate::version::JsonRpcVersion;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;
use std::borrow::Cow;
use tracing::debug;

/// Which of the three wire shapes a received message turned out to be.
/// Folding an error response into `Error` is left to the caller (via
/// `From<JsonRpcErrorResponse>`, already defined in `error.rs`) rather than
/// done here, since only the caller knows whether this error correlates to a
/// pending request or needs different handling.
pub enum JsonRpcIncoming<'a> {
    Response(JsonRpcResponse<'a>),
    ErrorResponse(JsonRpcErrorResponse<'a>),
    Notification(JsonRpcNotification<'a>),
    BatchResponse(Vec<JsonRpcBatchResponse<'a>>),
}

/// One element of a decoded [`JsonRpcIncoming::BatchResponse`].
pub enum JsonRpcBatchResponse<'a> {
    Response(JsonRpcResponse<'a>),
    ErrorResponse(JsonRpcErrorResponse<'a>),
}

/// Decodes a single (non-batch) response for `expected_id` — the shape a
/// one-request-per-connection transport (e.g. HTTP) needs: match the wire
/// message against `expected_id`, map an error response through
/// `From<JsonRpcErrorResponse>`, and reject a `Notification`/`BatchResponse`
/// as unexpected for a single in-flight request. Transports that multiplex
/// many in-flight requests over one connection (e.g. WebSocket) typically
/// route replies by id through a lookup table instead, and don't fit this
/// shape.
pub fn decode_single_response<Response: DeserializeOwned>(
    incoming: JsonRpcIncoming<'_>,
    expected_id: RequestId,
) -> crate::result::Result<Response> {
    match incoming {
        JsonRpcIncoming::Response(r) => {
            if r.id != expected_id {
                return Err(Error::InvalidMessage(
                    "response id does not match request id",
                ));
            }
            serde_json::from_str(r.result.get()).map_err(Error::SerdeError)
        }
        JsonRpcIncoming::ErrorResponse(e) => {
            if let Some(id) = &e.id
                && *id != expected_id
            {
                return Err(Error::InvalidMessage(
                    "error response id does not match request id",
                ));
            }
            Err(Error::from(e))
        }
        JsonRpcIncoming::Notification(_) => Err(Error::InvalidMessage("notification received")),
        JsonRpcIncoming::BatchResponse(_) => Err(Error::InvalidMessage("batch response received")),
    }
}

/// `Cow`'s automatic `Deserialize` impl always allocates (it has no way to
/// borrow generically), and `#[serde(untagged)]` would make that worse by
/// buffering the whole input before picking a variant. So the public,
/// `Cow`-based protocol structs are deliberately not `Deserialize` themselves.
/// This probe deserializes the borrowable primitives directly, discriminates
/// which shape they belong to, and `TryFrom` wraps them in `Cow::Borrowed` by
/// hand.
#[derive(Deserialize, Debug)]
struct Probe<'a> {
    jsonrpc: JsonRpcVersion,
    #[serde(default, deserialize_with = "deserialize_probe_id")]
    id: ProbeId,
    #[serde(default, borrow)]
    method: Option<&'a str>,
    #[serde(default, borrow)]
    params: Option<&'a RawValue>,
    /// Same "absent vs. present-but-null" trap as `id` above, but the fix
    /// looks different: a `result` of `null` is a completely ordinary,
    /// spec-legal success response (e.g. a method with no meaningful return
    /// value), so it must still round-trip as `Some(<raw "null">)`, not
    /// collapse to `None` and get misread as "no result at all".
    /// Deserializing straight into `&'a RawValue` (rather than
    /// `Option<&'a RawValue>`) sidesteps the problem entirely — `RawValue`
    /// just captures whatever raw JSON text is there, `null` included,
    /// without `Option`'s null-shortcut.
    #[serde(default, deserialize_with = "deserialize_present_raw", borrow)]
    result: Option<&'a RawValue>,
    #[serde(default, borrow)]
    error: Option<ProbeError<'a>>,
}

/// `deserialize_with` for [`Probe::result`]. Only invoked when the `result`
/// member is present at all — `#[serde(default)]` handles the wholly-absent
/// case.
fn deserialize_present_raw<'de: 'a, 'a, D>(
    deserializer: D,
) -> std::result::Result<Option<&'a RawValue>, D::Error>
where
    D: Deserializer<'de>,
{
    <&'a RawValue>::deserialize(deserializer).map(Some)
}

/// A probe's `id` member, distinguishing "absent" from "present but `null`"
/// from "present with a value this client didn't issue" — collapsing all
/// three into `Option<RequestId>` up front would lose exactly the
/// distinction spec-shape detection needs.
#[derive(Debug, Clone, Copy, Default)]
enum ProbeId {
    #[default]
    Absent,
    Null,
    Value(RequestId),
    Other,
}

impl ProbeId {
    fn is_present(self) -> bool {
        !matches!(self, ProbeId::Absent)
    }

    fn value(self) -> Option<RequestId> {
        match self {
            ProbeId::Value(id) => Some(id),
            _ => None,
        }
    }
}

/// `deserialize_with` for [`Probe::id`].
fn deserialize_probe_id<'de, D>(deserializer: D) -> std::result::Result<ProbeId, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Wire {
        Id(RequestId),
        Other(serde::de::IgnoredAny),
    }

    Ok(match Option::<Wire>::deserialize(deserializer)? {
        None => ProbeId::Null,
        Some(Wire::Id(id)) => ProbeId::Value(id),
        Some(Wire::Other(_)) => ProbeId::Other,
    })
}

/// A probe's `error` member.
#[derive(Deserialize, Debug)]
struct ProbeError<'a> {
    code: i64,
    #[serde(borrow)]
    message: &'a str,
    #[serde(default, borrow)]
    data: Option<&'a RawValue>,
}

/// Builds a [`JsonRpcBatchResponse`] if `probe` has a `result` or `error`
/// member (a response shape); `None` if it has neither (so the caller can
/// try the notification shape instead).
fn probe_as_response<'a>(probe: &Probe<'a>) -> Option<Result<JsonRpcBatchResponse<'a>, Error>> {
    let numeric_id = probe.id.value();

    if let Some(error) = &probe.error {
        return Some(Ok(JsonRpcBatchResponse::ErrorResponse(
            JsonRpcErrorResponse {
                jsonrpc: probe.jsonrpc,
                id: numeric_id,
                error: JsonRpcError {
                    code: error.code,
                    message: error.message,
                    data: error.data.map(Cow::Borrowed),
                },
            },
        )));
    }

    if let Some(result) = probe.result {
        let id = match numeric_id {
            Some(id) => id,
            None => {
                let message = if !probe.id.is_present() {
                    "a response is missing its `id`"
                } else {
                    "a response `id` is not a value this client could have issued"
                };
                return Some(Err(Error::InvalidMessage(message)));
            }
        };
        return Some(Ok(JsonRpcBatchResponse::Response(JsonRpcResponse {
            jsonrpc: probe.jsonrpc,
            id,
            result: Cow::Borrowed(result),
        })));
    }

    None
}

/// Classifies a single (non-batch) probe as a response, error response, or
/// notification, per the JSON-RPC spec shapes this client accepts.
#[hotpath::measure]
fn process_probe<'a>(probe: Probe<'a>) -> Result<JsonRpcIncoming<'a>, Error> {
    debug!("Processing probe: {:?}", probe);
    if let Some(response) = probe_as_response(&probe) {
        return response.map(|r| match r {
            JsonRpcBatchResponse::Response(r) => JsonRpcIncoming::Response(r),
            JsonRpcBatchResponse::ErrorResponse(e) => JsonRpcIncoming::ErrorResponse(e),
        });
    }

    if let Some(method) = probe.method {
        if probe.id.is_present() {
            return Err(Error::InvalidMessage(
                "received a method call with an id; this client does not handle inbound requests",
            ));
        }

        return Ok(JsonRpcIncoming::Notification(JsonRpcNotification {
            jsonrpc: probe.jsonrpc,
            method: Cow::Borrowed(MethodId::new(method)),
            params: probe.params.map(Cow::Borrowed),
        }));
    }

    Err(Error::InvalidMessage(
        "message has no `result`, `error`, or `method` field",
    ))
}

/// Classifies every probe in a batch as a response or error response; fails
/// the whole batch if any item is shaped as neither.
#[hotpath::measure]
fn process_batch<'a>(probes: Vec<Probe<'a>>) -> Result<JsonRpcIncoming<'a>, Error> {
    let items = probes
        .iter()
        .map(|probe| {
            probe_as_response(probe).unwrap_or(Err(Error::InvalidMessage(
                "batch item has no `result` or `error`",
            )))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(JsonRpcIncoming::BatchResponse(items))
}

/// Decodes a UTF-8 JSON-RPC message, borrowing from `s` wherever possible.
impl<'a> TryFrom<&'a str> for JsonRpcIncoming<'a> {
    type Error = Error;

    #[hotpath::measure(label = "decode_str")]
    fn try_from(s: &'a str) -> Result<Self, Self::Error> {
        if s.trim_start().starts_with('[') {
            let probes: Vec<Probe<'a>> = serde_json::from_str(s)?;
            return process_batch(probes);
        }
        let probe: Probe<'a> = serde_json::from_str(s)?;
        process_probe(probe)
    }
}

/// Decodes a JSON-RPC message from raw bytes, borrowing from `bytes`
/// wherever possible.
impl<'a> TryFrom<&'a [u8]> for JsonRpcIncoming<'a> {
    type Error = Error;

    #[hotpath::measure(label = "decode_bytes")]
    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        debug!("Decoding bytes: {:?}", bytes);
        if let Ok(s) = String::from_utf8(bytes.to_vec()) {
            debug!("Decoding string: {}", s);
        }

        let is_batch = bytes
            .iter()
            .find(|b| !b.is_ascii_whitespace())
            .is_some_and(|&b| b == b'[');
        if is_batch {
            debug!("Decoding batch");
            let probes: Vec<Probe<'a>> = serde_json::from_slice(bytes)?;
            return process_batch(probes);
        }
        debug!("Decoding single probe");
        let probe: Probe<'a> = serde_json::from_slice(bytes)?;
        process_probe(probe)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::JsonRpcErrorCode;

    #[test]
    fn decodes_response_without_copying() {
        let bytes = br#"{"jsonrpc":"2.0","id":13,"result":{"foo":"bar"}}"#.to_vec();
        let range = bytes.as_ptr_range();

        match JsonRpcIncoming::try_from(bytes.as_slice()).unwrap() {
            JsonRpcIncoming::Response(response) => {
                let result_ptr = response.result.get().as_ptr();
                assert!(
                    range.contains(&result_ptr),
                    "result was copied out of the input buffer"
                );
            }
            _ => panic!("expected a response"),
        }
    }

    #[test]
    fn decodes_notification_without_copying() {
        let bytes = br#"{"jsonrpc":"2.0","method":"ping","params":[1,2,3]}"#.to_vec();
        let range = bytes.as_ptr_range();

        match JsonRpcIncoming::try_from(bytes.as_slice()).unwrap() {
            JsonRpcIncoming::Notification(notification) => {
                assert_eq!(notification.method.as_ref(), MethodId::new("ping"));
                let params_ptr = notification.params.unwrap().get().as_ptr();
                assert!(
                    range.contains(&params_ptr),
                    "params was copied out of the input buffer"
                );
            }
            _ => panic!("expected a notification"),
        }
    }

    #[test]
    fn decodes_error_response() {
        let bytes: &[u8] =
            br#"{"jsonrpc":"2.0","id":7,"error":{"code":-32601,"message":"Method not found"}}"#;

        match JsonRpcIncoming::try_from(bytes).unwrap() {
            JsonRpcIncoming::ErrorResponse(response) => {
                assert_eq!(response.error.code, -32601);
                assert_eq!(response.error.message, "Method not found");

                let err = Error::from(response);
                assert!(matches!(
                    err,
                    Error::RpcError {
                        code: JsonRpcErrorCode::MethodNotFound,
                        ..
                    }
                ));
            }
            _ => panic!("expected an error response"),
        }
    }

    #[test]
    fn rejects_unrecognized_shape() {
        let bytes: &[u8] = br#"{"jsonrpc":"2.0"}"#;
        assert!(matches!(
            JsonRpcIncoming::try_from(bytes),
            Err(Error::InvalidMessage(_))
        ));
    }

    #[test]
    fn rejects_request_shaped_messages() {
        let bytes: &[u8] = br#"{"jsonrpc":"2.0","id":5,"method":"ping"}"#;
        assert!(matches!(
            JsonRpcIncoming::try_from(bytes),
            Err(Error::InvalidMessage(_))
        ));
    }

    #[test]
    fn rejects_wrong_version() {
        let bytes: &[u8] = br#"{"jsonrpc":"1.0","id":1,"result":{}}"#;
        assert!(matches!(
            JsonRpcIncoming::try_from(bytes),
            Err(Error::SerdeError(_))
        ));
    }

    #[test]
    fn decodes_batch_response_without_copying() {
        let bytes = br#"[{"jsonrpc":"2.0","id":1,"result":1},{"jsonrpc":"2.0","id":2,"error":{"code":-32601,"message":"Method not found"}}]"#.to_vec();
        let range = bytes.as_ptr_range();

        match JsonRpcIncoming::try_from(bytes.as_slice()).unwrap() {
            JsonRpcIncoming::BatchResponse(items) => {
                assert_eq!(items.len(), 2);
                match &items[0] {
                    JsonRpcBatchResponse::Response(r) => {
                        assert_eq!(r.id, RequestId::new(1));
                        assert!(range.contains(&r.result.get().as_ptr()));
                    }
                    _ => panic!("expected a response at index 0"),
                }
                match &items[1] {
                    JsonRpcBatchResponse::ErrorResponse(e) => {
                        assert_eq!(e.id, Some(RequestId::new(2)));
                        assert_eq!(e.error.code, -32601);
                    }
                    _ => panic!("expected an error response at index 1"),
                }
            }
            _ => panic!("expected a batch response"),
        }
    }

    #[test]
    fn decodes_batch_response_with_leading_whitespace() {
        let bytes: &[u8] = b"  \n[{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":1}]";
        assert!(matches!(
            JsonRpcIncoming::try_from(bytes).unwrap(),
            JsonRpcIncoming::BatchResponse(items) if items.len() == 1
        ));
    }

    #[test]
    fn rejects_batch_item_missing_result_and_error() {
        let bytes: &[u8] = br#"[{"jsonrpc":"2.0","id":1}]"#;
        assert!(matches!(
            JsonRpcIncoming::try_from(bytes),
            Err(Error::InvalidMessage(_))
        ));
    }

    #[test]
    fn rejects_null_id_method_call_as_notification() {
        let bytes: &[u8] = br#"{"jsonrpc":"2.0","id":null,"method":"foo"}"#;
        assert!(matches!(
            JsonRpcIncoming::try_from(bytes),
            Err(Error::InvalidMessage(_))
        ));
    }

    #[test]
    fn treats_absent_id_as_notification() {
        let bytes: &[u8] = br#"{"jsonrpc":"2.0","method":"foo"}"#;
        assert!(matches!(
            JsonRpcIncoming::try_from(bytes),
            Ok(JsonRpcIncoming::Notification(_))
        ));
    }

    #[test]
    fn rejects_non_u64_response_id_gracefully() {
        let bytes: &[u8] = br#"{"jsonrpc":"2.0","id":"abc","result":1}"#;
        assert!(matches!(
            JsonRpcIncoming::try_from(bytes),
            Err(Error::InvalidMessage(_))
        ));
    }

    #[test]
    fn treats_non_u64_error_id_as_indeterminate() {
        let bytes: &[u8] =
            br#"{"jsonrpc":"2.0","id":"abc","error":{"code":-32700,"message":"Parse error"}}"#;
        match JsonRpcIncoming::try_from(bytes).unwrap() {
            JsonRpcIncoming::ErrorResponse(e) => assert_eq!(e.id, None),
            _ => panic!("expected an error response"),
        }
    }

    #[test]
    fn accepts_explicit_null_result_as_a_valid_response() {
        let bytes: &[u8] = br#"{"jsonrpc":"2.0","id":1,"result":null}"#;
        match JsonRpcIncoming::try_from(bytes).unwrap() {
            JsonRpcIncoming::Response(r) => {
                assert_eq!(r.id, RequestId::new(1));
                assert_eq!(r.result.get(), "null");
            }
            _ => panic!("expected a response"),
        }
    }

    #[test]
    fn accepts_explicit_null_result_in_a_batch() {
        let bytes: &[u8] = br#"[{"jsonrpc":"2.0","id":1,"result":null}]"#;
        match JsonRpcIncoming::try_from(bytes).unwrap() {
            JsonRpcIncoming::BatchResponse(items) => {
                assert_eq!(items.len(), 1);
                match &items[0] {
                    JsonRpcBatchResponse::Response(r) => assert_eq!(r.result.get(), "null"),
                    _ => panic!("expected a response at index 0"),
                }
            }
            _ => panic!("expected a batch response"),
        }
    }

    #[test]
    fn rejects_message_with_neither_result_nor_error_nor_method_still() {
        let bytes: &[u8] = br#"{"jsonrpc":"2.0","id":1}"#;
        assert!(matches!(
            JsonRpcIncoming::try_from(bytes),
            Err(Error::InvalidMessage(_))
        ));
    }
}
