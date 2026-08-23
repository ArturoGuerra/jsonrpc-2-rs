use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The JSON-RPC protocol version this crate speaks — the spec fixes this at
/// `"2.0"`.
pub const JSONRPC_VERSION: &str = "2.0";

/// Zero-sized marker for the `"jsonrpc": "2.0"` member every JSON-RPC
/// message carries. Serializes as the literal string `"2.0"`; deserializing
/// anything else fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct JsonRpcVersion;

impl Serialize for JsonRpcVersion {
    fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(JSONRPC_VERSION)
    }
}

impl<'de> Deserialize<'de> for JsonRpcVersion {
    fn deserialize<D: Deserializer<'de>>(deser: D) -> Result<Self, D::Error> {
        match <&str>::deserialize(deser)? {
            JSONRPC_VERSION => Ok(JsonRpcVersion),
            _ => Err(serde::de::Error::custom("invalid JSON-RPC version")),
        }
    }
}
