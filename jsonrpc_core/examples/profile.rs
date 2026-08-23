use jsonrpc_core::JsonRpcIncoming;

const RESPONSE: &str = r#"{"jsonrpc":"2.0","id":1,"result":{"foo":"bar","baz":[1,2,3]}}"#;
const NOTIFICATION: &str = r#"{"jsonrpc":"2.0","method":"tick","params":{"count":42}}"#;
const ERROR_RESPONSE: &str =
    r#"{"jsonrpc":"2.0","id":2,"error":{"code":-32601,"message":"Method not found"}}"#;

#[hotpath::measure]
fn decode(message: &str) {
    let incoming = JsonRpcIncoming::try_from(message).expect("valid message");
    std::hint::black_box(incoming);
}

#[hotpath::main]
fn main() {
    for _ in 0..100_000 {
        decode(RESPONSE);
        decode(NOTIFICATION);
        decode(ERROR_RESPONSE);
    }
}
