//! Accept 循环与 graceful shutdown。

use std::future::Future;

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use hyper_util::server::graceful::GracefulShutdown;
use tokio::net::TcpListener;
use tracing::warn;

use crate::config::ResolvedConfig;
use crate::{create_client, handle, GatewayClient};

/// 运行网关：accept 连接并服务，直到 `shutdown` 完成。
///
/// 收到 shutdown 信号后停止 accept，等待存量连接在 `cfg.shutdown_timeout` 内自然结束，
/// 超时则直接返回（进程退出掐断残余连接）。
pub async fn run(
    cfg: ResolvedConfig,
    client: GatewayClient,
    listener: TcpListener,
    shutdown: impl Future<Output = ()>,
) {
    let graceful = GracefulShutdown::new();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                break;
            }
            accept = listener.accept() => {
                match accept {
                    Ok((stream, peer)) => {
                        let io = TokioIo::new(stream);
                        let cfg = cfg.clone();
                        let client = client.clone();
                        let conn = http1::Builder::new().serve_connection(
                            io,
                            service_fn(move |req| {
                                let cfg = cfg.clone();
                                let client = client.clone();
                                async move { handle(cfg, client, req).await }
                            }),
                        );
                        let handle = graceful.watch(conn);
                        tokio::task::spawn(async move {
                            if let Err(e) = handle.await {
                                tracing::debug!(error = %e, %peer, "connection closed");
                            }
                        });
                    }
                    Err(e) => {
                        warn!(error = %e, "accept failed");
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }

    // 等待存量连接完成，超时则强制退出。
    tokio::select! {
        _ = graceful.shutdown() => {}
        _ = tokio::time::sleep(cfg.shutdown_timeout) => {
            warn!(timeout = ?cfg.shutdown_timeout, "graceful shutdown timed out, forcing exit");
        }
    }
}

/// 构造生产用 shutdown future：`ctrl_c` 与 unix `SIGTERM` 先到者。
pub async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let sigterm = async {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let sigterm = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = sigterm => {}
    }
}

/// 仅供测试：用 listener 与已有 client 运行网关，shutdown 信号由调用方控制。
pub async fn run_with_shutdown(
    cfg: ResolvedConfig,
    listener: TcpListener,
    shutdown: impl Future<Output = ()>,
) {
    let client = create_client(&cfg);
    run(cfg, client, listener, shutdown).await;
}
