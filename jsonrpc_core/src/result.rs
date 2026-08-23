use crate::error::Error;

/// This crate's `Result` alias — every fallible operation resolves to this
/// crate's [`Error`] type.
pub type Result<T> = std::result::Result<T, Error>;
