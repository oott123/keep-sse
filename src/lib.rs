//! keep-sse — LLM SSE 保活网关库。
//!
//! 库入口，暴露各模块供集成测试使用。

pub mod config;
pub mod detect;
pub mod encoding;
pub mod error_event;
#[cfg(feature = "pprof")]
pub mod pprof;
pub mod proxy;
pub mod server;
pub mod sse;

use std::convert::Infallible;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::header::CONTENT_LENGTH;
use hyper::{Request, Response, StatusCode};
use hyper_util::client::legacy::{connect::HttpConnector, Client};

use crate::config::ResolvedConfig;
use crate::detect::{accepts_event_stream, match_endpoint, probe_stream_flag, EndpointKind};
use crate::encoding::{decode_bytes, parse_content_encoding};
use crate::proxy::{build_client, proxy, proxy_buffered, ReqBody, RespBody};

/// 共享的 hyper-util client 类型。
pub type GatewayClient = Client<HttpConnector, ReqBody>;

/// 构建网关客户端。
pub fn create_client(cfg: &ResolvedConfig) -> GatewayClient {
    build_client(cfg)
}

/// 请求分发：识别 LLM 流式请求 → SSE 包装通路；否则 → 透明代理。
pub async fn handle(
    cfg: ResolvedConfig,
    client: GatewayClient,
    req: Request<Incoming>,
) -> Result<Response<RespBody>, Infallible> {
    if let Some(kind) = match_endpoint(req.method(), req.uri().path(), req.uri().query()) {
        let content_length = req
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<usize>().ok());

        if let Some(cl) = content_length {
            if cl > cfg.max_probe_body {
                tracing::info!(
                    path = %req.uri().path(),
                    content_length = cl,
                    max_probe_body = cfg.max_probe_body,
                    "content-length exceeds max-probe-body; going transparent (no SSE keepalive)"
                );
                return Ok(proxy(&cfg, &client, req).await);
            }
        }

        let (parts, body) = req.into_parts();
        let limited = Limited::new(body, cfg.max_probe_body);
        let collected = match limited.collect().await {
            Ok(c) => c.to_bytes(),
            Err(e) => {
                if e.downcast_ref::<http_body_util::LengthLimitError>()
                    .is_some()
                {
                    return Ok(response_413());
                }
                return Ok(response_400());
            }
        };

        if kind == EndpointKind::GeminiStream {
            return Ok(sse::handle_stream(cfg, client, parts, collected, kind).await);
        }

        if accepts_event_stream(&parts.headers) {
            return Ok(sse::handle_stream(cfg, client, parts, collected, kind).await);
        }

        let content_encoding = parse_content_encoding(&parts.headers);
        let probe_body = match content_encoding {
            Some(ce) => decode_bytes(ce, &collected, cfg.max_probe_body)
                .await
                .unwrap_or_default(),
            None => collected.to_vec(),
        };

        if probe_stream_flag(&probe_body) {
            return Ok(sse::handle_stream(cfg, client, parts, collected, kind).await);
        }

        return Ok(proxy_buffered(&cfg, &client, parts, collected).await);
    }

    Ok(proxy(&cfg, &client, req).await)
}

/// 413 Payload Too Large 响应。
fn response_413() -> Response<RespBody> {
    let body = serde_json::json!({
        "error": {
            "message": "request body exceeds max-probe-body limit",
            "type": "invalid_request_error"
        }
    });
    let bytes = serde_json::to_vec(&body).expect("json serializable");
    Response::builder()
        .status(StatusCode::PAYLOAD_TOO_LARGE)
        .header("content-type", "application/json")
        .body(
            Full::new(Bytes::from(bytes))
                .map_err(|e: Infallible| match e {})
                .boxed(),
        )
        .expect("response buildable")
}

/// 400 Bad Request JSON 响应（请求体读取失败）。
fn response_400() -> Response<RespBody> {
    let body = serde_json::json!({
        "error": {
            "message": "failed to read request body",
            "type": "invalid_request_error"
        }
    });
    let bytes = serde_json::to_vec(&body).expect("json serializable");
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .header("content-type", "application/json")
        .body(
            Full::new(Bytes::from(bytes))
                .map_err(|e: Infallible| match e {})
                .boxed(),
        )
        .expect("response buildable")
}
