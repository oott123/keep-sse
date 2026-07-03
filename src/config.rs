use std::net::SocketAddr;
use std::process::ExitCode;

use clap::Parser;
use hyper::Uri;

/// keep-sse — LLM SSE 保活网关。
#[derive(Debug, Clone, Parser)]
#[command(name = "keep-sse", version)]
pub struct Config {
    /// 监听地址。
    #[arg(long, env = "KEEP_SSE_LISTEN", default_value = "0.0.0.0:8080")]
    pub listen: SocketAddr,

    /// 上游 base 地址，仅接受 `http://` scheme，如 `http://host:port`。
    #[arg(long, env = "KEEP_SSE_UPSTREAM")]
    pub upstream: String,

    /// 空闲保活间隔（秒）。
    #[arg(long, env = "KEEP_SSE_HEARTBEAT_INTERVAL", default_value_t = 60)]
    pub heartbeat_interval: u64,

    /// 上游 TCP 连接超时（秒）；连接建立后不设整体超时。
    #[arg(long, env = "KEEP_SSE_CONNECT_TIMEOUT", default_value_t = 10)]
    pub connect_timeout: u64,

    /// 流式探测的请求体缓冲上限（字节），超限走透明代理。
    #[arg(long, env = "KEEP_SSE_MAX_PROBE_BODY", default_value_t = 32 * 1024 * 1024)]
    pub max_probe_body: usize,
}

/// 校验后的运行时配置。`upstream` 已解析为 `Uri`。
#[derive(Debug, Clone)]
pub struct ResolvedConfig {
    pub listen: SocketAddr,
    pub upstream: Uri,
    pub upstream_authority: String,
    pub heartbeat_interval: std::time::Duration,
    pub connect_timeout: std::time::Duration,
    pub max_probe_body: usize,
}

impl ResolvedConfig {
    /// 解析并校验配置。失败时返回要打印到 stderr 的错误信息。
    pub fn parse(cfg: Config) -> Result<Self, String> {
        let upstream: Uri = cfg
            .upstream
            .parse()
            .map_err(|e| format!("invalid --upstream `{}`: {e}", cfg.upstream))?;

        let scheme = upstream.scheme_str();
        if scheme != Some("http") {
            return Err(format!(
                "--upstream must use `http://` scheme, got `{}`",
                scheme.unwrap_or("(none)")
            ));
        }
        let upstream_authority = upstream
            .authority()
            .ok_or_else(|| "--upstream missing authority (host[:port])".to_string())?
            .as_str()
            .to_owned();

        if upstream.path() != "/" && !upstream.path().is_empty() {
            return Err(format!(
                "--upstream must not carry a path; got `{}`",
                upstream.path()
            ));
        }

        Ok(Self {
            listen: cfg.listen,
            upstream,
            upstream_authority,
            heartbeat_interval: std::time::Duration::from_secs(cfg.heartbeat_interval),
            connect_timeout: std::time::Duration::from_secs(cfg.connect_timeout),
            max_probe_body: cfg.max_probe_body,
        })
    }
}

/// 解析配置；失败时打印错误并返回 `ExitCode::FAILURE`。
pub fn parse_or_exit() -> Result<ResolvedConfig, ExitCode> {
    let cfg = Config::parse();
    match ResolvedConfig::parse(cfg) {
        Ok(rc) => Ok(rc),
        Err(e) => {
            eprintln!("keep-sse: {e}");
            Err(ExitCode::FAILURE)
        }
    }
}
