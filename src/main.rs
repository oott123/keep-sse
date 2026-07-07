use std::process::ExitCode;

use keep_sse::{config, create_client, server};
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cfg = match config::parse_or_exit() {
        Ok(c) => c,
        Err(code) => return code,
    };
    info!(listen = %cfg.listen, upstream = %cfg.upstream, "keep-sse starting");

    let client = create_client(&cfg);
    #[cfg(feature = "pprof")]
    keep_sse::pprof::start_if_enabled();

    let listener = match TcpListener::bind(cfg.listen).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("keep-sse: bind {}: {e}", cfg.listen);
            return ExitCode::FAILURE;
        }
    };

    server::run(cfg, client, listener, server::shutdown_signal()).await;
    ExitCode::SUCCESS
}
