use serde::{Deserialize, Serialize};
use std::borrow::{Borrow, Cow};

/// A borrowed JSON-RPC method name — `#[repr(transparent)]` over `str`, so
/// it can be constructed from a `&str` without allocating.
#[derive(Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
#[repr(transparent)]
pub struct MethodId(str);

impl MethodId {
    /// Borrows `s` as a `&MethodId` — free, no allocation.
    pub fn new(s: &str) -> &MethodId {
        // Safety: `MethodId` is `#[repr(transparent)]` over `str`, so a
        // `&str` and a `&MethodId` share the same layout.
        #[allow(unsafe_code)]
        unsafe {
            &*(s as *const str as *const MethodId)
        }
    }
}

impl ToOwned for MethodId {
    type Owned = MethodIdBuf;
    fn to_owned(&self) -> Self::Owned {
        MethodIdBuf::new(self.0.to_owned())
    }
}

/// An owned JSON-RPC method name. Backed by `Cow<'static, str>` rather than
/// `String` so that the common case of a `&'static str` method literal
/// converts in without allocating.
#[derive(Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Clone)]
#[serde(transparent)]
#[repr(transparent)]
pub struct MethodIdBuf(Cow<'static, str>);

impl MethodIdBuf {
    /// Builds a `MethodIdBuf` from a `&'static str` or an owned `String`.
    pub fn new(s: impl Into<Cow<'static, str>>) -> Self {
        MethodIdBuf(s.into())
    }
}

impl Borrow<MethodId> for MethodIdBuf {
    fn borrow(&self) -> &MethodId {
        MethodId::new(&self.0)
    }
}

impl AsRef<MethodId> for MethodIdBuf {
    fn as_ref(&self) -> &MethodId {
        self.borrow()
    }
}

impl From<String> for MethodIdBuf {
    fn from(s: String) -> Self {
        MethodIdBuf::new(s)
    }
}

impl From<&'static str> for MethodIdBuf {
    fn from(s: &'static str) -> Self {
        MethodIdBuf::new(s)
    }
}
