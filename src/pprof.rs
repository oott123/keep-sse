//! 内嵌 CPU 采样器（pprof-rs）。
//!
//! 仅在 `pprof` feature 开启时编译。运行时由 `KEEP_SSE_PPROF=1` 激活，
//! 启动 99Hz 采样并在后台监听 `SIGUSR1`：收到信号即把火焰图 SVG 与
//! protobuf 报告写入 `KEEP_SSE_PPROF_DIR`（默认 `/tmp`）。

use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use pprof::ProfilerGuard;
use tokio::signal::unix::{signal, SignalKind};
use tracing::{info, warn};

/// 采样频率（Hz）。避开 100Hz 整数倍以避免与系统活动锁相。
const SAMPLE_HZ: i32 = 99;

/// 若 `KEEP_SSE_PPROF=1` 则启动后台采样与 dump 任务；否则立即返回（零开销）。
///
/// 调用方须处于 tokio 多线程运行时上下文中。
pub fn start_if_enabled() {
    if !enabled() {
        return;
    }
    info!(
        hz = SAMPLE_HZ,
        "pprof: sampling enabled; send SIGUSR1 to dump a profile"
    );
    tokio::spawn(dump_loop());
}

fn enabled() -> bool {
    match std::env::var("KEEP_SSE_PPROF") {
        Ok(v) => v == "1",
        Err(_) => false,
    }
}

fn dump_dir() -> PathBuf {
    std::env::var("KEEP_SSE_PPROF_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp"))
}

fn timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// 常驻任务：持有 `ProfilerGuard`、监听 `SIGUSR1`，每次触发写一份报告。
///
/// guard 随本任务存活；任务被运行时回收时 guard drop，自动停止采样。
async fn dump_loop() {
    let guard = match ProfilerGuard::new(SAMPLE_HZ) {
        Ok(g) => g,
        Err(e) => {
            warn!(error = %e, "pprof: failed to start sampler; profiling disabled");
            return;
        }
    };

    let mut sig = match signal(SignalKind::user_defined1()) {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, "pprof: failed to install SIGUSR1 handler; profiling disabled");
            return;
        }
    };

    loop {
        sig.recv().await;
        // 取出 guard 的报告；report() 借用 guard，写文件时仍需持有 guard。
        let report = match guard.report().build() {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "pprof: failed to build report");
                continue;
            }
        };

        let dir = dump_dir();
        let ts = timestamp();
        let svg_path = dir.join(format!("keep-sse-{ts}.svg"));
        let pb_path = dir.join(format!("keep-sse-{ts}.pb"));

        // 确保输出目录存在（distroless 下 /tmp 默认存在，但显式创建更稳）。
        if let Err(e) = fs::create_dir_all(&dir) {
            warn!(error = %e, dir = %dir.display(), "pprof: cannot create output dir");
            continue;
        }

        match fs::File::create(&svg_path) {
            Ok(mut f) => match report.flamegraph(&mut f) {
                Ok(()) => info!(path = %svg_path.display(), "pprof: wrote flamegraph"),
                Err(e) => warn!(error = %e, path = %svg_path.display(), "pprof: failed to write flamegraph"),
            },
            Err(e) => warn!(error = %e, path = %svg_path.display(), "pprof: failed to open flamegraph file"),
        }

        match report.pprof() {
            Ok(profile) => {
                let content = pprof::protos::Message::encode_to_vec(&profile);
                match fs::write(&pb_path, &content) {
                    Ok(()) => info!(path = %pb_path.display(), "pprof: wrote protobuf"),
                    Err(e) => warn!(error = %e, path = %pb_path.display(), "pprof: failed to write protobuf"),
                }
            }
            Err(e) => warn!(error = %e, "pprof: failed to build protobuf report"),
        }
    }
}
