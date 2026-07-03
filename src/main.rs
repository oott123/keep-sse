use std::process::ExitCode;

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use keep_sse::{config, create_client, handle};
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

    let listener = match TcpListener::bind(cfg.listen).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("keep-sse: bind {}: {e}", cfg.listen);
            return ExitCode::FAILURE;
        }
    };

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "accept failed");
                continue;
            }
        };
        let io = TokioIo::new(stream);
        let cfg = cfg.clone();
        let client = client.clone();
        tokio::task::spawn(async move {
            if let Err(e) = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |req| handle(cfg.clone(), client.clone(), req)),
                )
                .await
            {
                tracing::debug!(error = %e, %peer, "connection closed");
            }
        });
    }
}
