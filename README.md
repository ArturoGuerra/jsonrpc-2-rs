# jsonrpc-rs

A Rust workspace implementing the client side of [JSON-RPC 2.0](https://www.jsonrpc.org/specification): protocol types, a zero-copy decoder, request batching, and transports over HTTP and WebSocket.

This crate is client-only. It can serialize outbound requests/notifications and decode inbound responses/notifications, but has no way to serialize a response or handle an inbound request — it cannot be used to implement a JSON-RPC server.

## Crate map

| Crate | Status | Description |
|---|---|---|
| [`jsonrpc_core`](jsonrpc_core) | Implemented | Protocol types, zero-copy message decoding, request id allocation, batch request building, and the transport-agnostic [`Client`](jsonrpc_core/src/client.rs) trait. Everything is re-exported from the crate root; submodules (`client`, `decode`, ... ) stay available for fully-qualified paths. `BidirectionalClient`/`Notification` require the `bidirectional` feature. |
| [`jsonrpc_http_client`](jsonrpc_http_client) | Implemented | A `Client` that sends each request/notification as its own HTTP POST, via `reqwest`. |
| [`jsonrpc_ws_client`](jsonrpc_ws_client) | Implemented | A `Client`/`BidirectionalClient` over a single long-lived WebSocket connection (via `stream-tungstenite`), with reconnection, ping/pong keepalive, per-request timeouts, and a broadcast stream of server-pushed notifications. |
| [`jsonrpc_client`](jsonrpc_client) | Implemented | Umbrella crate re-exporting `jsonrpc_core` plus, behind feature flags, `jsonrpc_http_client`/`jsonrpc_ws_client`: `http`, `ws`, or `full` for both. None on by default. |
| [`jsonrpc_cli`](jsonrpc_cli) | **Stub** | Untouched `cargo new` scaffold. Intended to become a CLI over the library, but not yet implemented. |

## Quickstart

Decoding a JSON-RPC message:

```rust
use jsonrpc_core::JsonRpcIncoming;

let message = r#"{"jsonrpc":"2.0","id":1,"result":{"foo":"bar"}}"#;
let incoming = JsonRpcIncoming::try_from(message)?;
```

Making a request over HTTP or WebSocket via the shared `Client` trait:

```rust
use jsonrpc_core::Client;
use jsonrpc_http_client::HttpClient;

let client = HttpClient::try_new("https://example.com/rpc", None::<NoAuth>, None)?;
let result: MyResponse = client.request("my_method", Some(params)).await?;
```

Consuming server-pushed notifications over WebSocket needs `jsonrpc_ws_client`'s
default-enabled `bidirectional` forwarding to `jsonrpc_core`, which unlocks
`BidirectionalClient`:

```rust
use jsonrpc_core::BidirectionalClient;

let stream = client.notifications();
```

Or depend on [`jsonrpc_client`](jsonrpc_client) alone with `features = ["full"]`
(or just `"http"`/`"ws"`) to pull in `jsonrpc_core` and whichever transport(s)
you need under one crate/import:

```rust
use jsonrpc_client::{Client, HttpClient};
```

See [`jsonrpc_core/examples/profile.rs`](jsonrpc_core/examples/profile.rs) for a decode-path micro-benchmark using the `hotpath` feature:

```sh
cargo run -p jsonrpc_core --example profile --features hotpath
```

## Development

```sh
cargo build --workspace
cargo test --workspace
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.
