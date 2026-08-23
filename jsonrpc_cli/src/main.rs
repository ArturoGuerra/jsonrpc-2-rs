use clap::{ArgAction, Parser, ValueEnum};
use jsonrpc_client::{Client, DefaultAuthStrategy, HttpClient, WsClientBuilder};
use serde_json::Value;
use tracing_subscriber::EnvFilter;

/// A minimal JSON-RPC 2.0 client for sending one request or notification
/// per invocation, over HTTP or WebSocket.
#[derive(Parser, Clone, Debug)]
struct Cli {
    /// Transport to use.
    #[arg(short, long)]
    mode: Mode,
    /// Whether to send a request (and await its reply) or a fire-and-forget
    /// notification.
    #[arg(short, long)]
    command: Command,
    /// The endpoint to connect to (an `http(s)://` or `ws(s)://` URL,
    /// matching `--mode`).
    #[arg(short, long)]
    url: String,
    /// The JSON-RPC method name to call.
    #[arg(long)]
    method: String,
    /// Params to send, as a JSON-encoded string (e.g. `'[1,2,3]'` or
    /// `'{"a":1}'`). Omit for no params.
    #[arg(short, long, value_parser = parse_json_collection)]
    params: Option<Value>,
    /// Increase log verbosity: -v for info, -vv for debug, -vvv for trace.
    /// Set `RUST_LOG` to override this with per-module filtering instead.
    #[arg(short, long, action = ArgAction::Count)]
    verbose: u8,
}

/// Installs a `tracing` subscriber writing to stderr, so it never mixes with
/// a request's result on stdout. `RUST_LOG` (standard `tracing_subscriber`
/// `EnvFilter` syntax, e.g. `jsonrpc_ws_client=trace`) takes priority when
/// set; otherwise the verbosity flag picks a blanket level.
fn init_tracing(verbose: u8) {
    let default_level = match verbose {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}

/// The transport a run targets.
#[derive(ValueEnum, Clone, Debug)]
enum Mode {
    Websocket,
    Http,
}

/// Whether a run sends a request (and awaits a reply) or a fire-and-forget
/// notification.
#[derive(ValueEnum, Clone, Debug)]
enum Command {
    Request,
    Notification,
}

/// Connects over WebSocket and sends `cli`'s configured request or
/// notification, printing the result (for a request) to stdout.
#[tracing::instrument(skip(cli), fields(url = %cli.url, method = %cli.method))]
async fn ws_mode(cli: Cli) {
    tracing::debug!("connecting");
    let client = WsClientBuilder::new(&cli.url).build().run();

    match &cli.command {
        Command::Request => {
            tracing::info!(?cli.params, "sending request");
            let params = cli.params.map(|params| serde_json::json!(params));
            let response: String = client.request(cli.method, params, None).await.unwrap();
            tracing::debug!(?response, "received response");
            println!("{:?}", response);
        }
        Command::Notification => {
            tracing::info!(?cli.params, "sending notification");
            client.notify(cli.method, cli.params).await.unwrap();
        }
    }
}

/// Connects over HTTP and sends `cli`'s configured request or notification,
/// printing the result (for a request) to stdout.
#[tracing::instrument(skip(cli), fields(url = %cli.url, method = %cli.method))]
async fn http_mode(cli: Cli) {
    let client = HttpClient::try_new(&cli.url, DefaultAuthStrategy, None).unwrap();

    match &cli.command {
        Command::Request => {
            tracing::info!(?cli.params, "sending request");
            let params = cli.params.map(|params| serde_json::json!(params));
            let response: String = client.request(cli.method, params, None).await.unwrap();
            tracing::debug!(?response, "received response");
            println!("{:?}", response);
        }
        Command::Notification => {
            tracing::info!(?cli.params, "sending notification");
            client.notify(cli.method, cli.params).await.unwrap();
        }
    }
}

/// Custom clap validator that ensures the input is a valid JSON Object or Array
fn parse_json_collection(s: &str) -> anyhow::Result<Value> {
    let parsed: Value = serde_json::from_str(s).map_err(|e| anyhow::anyhow!(e))?;

    match parsed {
        Value::Object(_) | Value::Array(_) => Ok(parsed),
        _ => anyhow::bail!("JSON must be a top-level Object ({}) or Array ([])".to_string()),
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    init_tracing(cli.verbose);
    match &cli.mode {
        Mode::Websocket => ws_mode(cli).await,
        Mode::Http => http_mode(cli).await,
    }
}
