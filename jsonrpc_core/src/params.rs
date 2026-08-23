use crate::error::Error;
use crate::result::Result;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value, value::RawValue};
use smallvec::SmallVec;
use std::borrow::Cow;

/// An owned, structured JSON-RPC params value — either an Object or an
/// Array, the only two shapes the spec allows for `params`.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Params {
    /// A params object, e.g. `{"a": 1}`.
    Object(Map<String, Value>),
    /// A params array, e.g. `[1, 2, 3]`.
    Array(Vec<Value>),
}

/// Adapts a caller-supplied params argument into the `Option<Value>` every
/// [`crate::client::Client`] method needs internally. `()` means "no
/// params" — the common case, letting callers write `client.request(method,
/// ())` instead of `client.request(method, Option::<()>::None)`.
/// `Some(params)`/`None` pass through unchanged for callers that decide
/// whether to send params at runtime.
pub trait IntoOptionParams: Send + Sync {
    /// The concrete, serializable params type once unwrapped.
    type Value: Serialize + Send + Sync;

    /// Performs the conversion.
    fn into_option_params(self) -> Option<Self::Value>;
}

impl IntoOptionParams for () {
    type Value = ();
    fn into_option_params(self) -> Option<()> {
        None
    }
}

impl<T: Serialize + Send + Sync> IntoOptionParams for Option<T> {
    type Value = T;
    fn into_option_params(self) -> Option<T> {
        self
    }
}

/// Whether `data`'s first non-whitespace byte is `{` or `[` — the two
/// Structured value shapes JSON-RPC `params` is allowed to take.
pub(crate) fn is_object_or_array(data: &str) -> bool {
    matches!(
        data.trim_start().as_bytes().first(),
        Some(b'{') | Some(b'[')
    )
}

/// Serializes `value` and validates it's a Structured value (an Object or
/// Array) per spec, producing an owned, heap-allocated [`RawValue`].
pub(crate) fn serialize_params<T: Serialize>(value: &T) -> Result<Box<RawValue>> {
    let json = serde_json::to_string(value).map_err(Error::SerdeError)?;
    if !is_object_or_array(&json) {
        return Err(Error::InvalidParams);
    }
    RawValue::from_string(json).map_err(Error::SerdeError)
}

/// Serializes `value` into the caller-owned `buf`, growing it as needed
/// (staying stack-only up to `buf`'s inline capacity), and validates it's a
/// Structured value — the zero-allocation-for-small-payloads counterpart of
/// [`serialize_params`]. The returned `&RawValue` borrows from `buf`, so
/// `buf` must outlive it.
pub(crate) fn params_to_raw_value<'a, T: Serialize + Send + Sync>(
    value: &T,
    buf: &'a mut SmallVec<[u8; 1024]>,
) -> Result<&'a RawValue> {
    serde_json::to_writer(&mut *buf, value).map_err(Error::SerdeError)?;
    let json = std::str::from_utf8(buf).map_err(Error::InvalidUTF8Str)?;
    if !is_object_or_array(json) {
        return Err(Error::InvalidParams);
    }
    serde_json::from_str(json).map_err(Error::SerdeError)
}

/// `deserialize_with` for a `params` field — rejects anything that isn't a
/// Structured value (an Object or Array) per spec, and borrows rather than
/// copies when possible.
pub(crate) fn deserialize_params_with<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Cow<'de, RawValue>>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw: Option<&'de RawValue> = Option::deserialize(deserializer)?;
    match raw {
        Some(r) if is_object_or_array(r.get()) => Ok(Some(Cow::Borrowed(r))),
        Some(_) => Err(D::Error::custom(
            "JSON-RPC params must be an object or array",
        )),
        None => Ok(None),
    }
}
