//! 透明代理通路与共享 body/client 类型。
//!
//! 本模块定义整个网关共享的请求/响应 body 类型（`ReqBody`/`RespBody`），
//! 两条通路都通过 `BoxBody<Bytes, BoxError>` 统一以实现零拷贝帧直通：
//! - 透明通路：`Incoming` body 经 `map_err` 包为 `BoxBody`，帧不缓冲、不复制；
//! - SSE 通路：缓冲后的 `Bytes` 经 `Full` 包为 `BoxBody`。

use std::convert::Infallible;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::{HeaderMap, HeaderName, HeaderValue, CONNECTION, HOST};
use hyper::{Request, Response, StatusCode, Uri};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::{Builder, Client};

use crate::config::ResolvedConfig;

/// 共享的"任意 body 错误"类型。
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
/// 上行请求 body 类型。
pub type ReqBody = BoxBody<Bytes, BoxError>;
/// 下行响应 body 类型。
pub type RespBody = BoxBody<Bytes, BoxError>;

/// Hop-by-hop 头（RFC 7230 + design.md）：`Connection` 在 `strip_hop_by_hop` 中单独处理。
const HOP_BY_HOP: &[&str] = &[
    "keep-alive",
    "transfer-encoding",
    "te",
    "trailer",
    "upgrade",
    "proxy-authorization",
    "proxy-authenticate",
];

/// 移除 hop-by-hop 头及 `Connection` 头中列出的头。
pub fn strip_hop_by_hop(headers: &mut HeaderMap) {
    // 收集 Connection 头中列出的额外头名，再统一删除。
    let connection_list: Vec<HeaderName> = headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(',').map(str::trim))
        .filter(|s| !s.is_empty())
        .filter_map(|s| HeaderName::try_from(s).ok())
        .collect();
    headers.remove(CONNECTION);
    for name in connection_list {
        headers.remove(&name);
    }
    for name in HOP_BY_HOP {
        headers.remove(*name);
    }
}

pub fn build_client(cfg: &ResolvedConfig) -> Client<HttpConnector, ReqBody> {
    let mut connector = HttpConnector::new();
    connector.set_connect_timeout(Some(cfg.connect_timeout));
    connector.enforce_http(true);
    Builder::new(hyper_util::rt::TokioExecutor::new())
        .pool_idle_timeout(Some(Duration::from_secs(90)))
        // 我们自己改写 Host 头，不让 client 介入。
        .set_host(false)
        .build(connector)
}
pub fn upstream_uri(cfg: &ResolvedConfig, original: &Uri) -> Uri {
    let path_and_query = original.path_and_query().map(|p| p.as_str()).unwrap_or("/");
    let mut parts = cfg.upstream.clone().into_parts();
    parts.path_and_query = Some(path_and_query.parse().expect("path_and_query valid"));
    Uri::from_parts(parts).expect("uri parts valid")
}

fn box_err_infallible(e: Infallible) -> BoxError {
    match e {}
}

/// 502 Bad Gateway JSON 错误响应（透明通路）。
pub fn bad_gateway(reason: &str) -> Response<RespBody> {
    let body = serde_json::json!({
        "error": {
            "message": format!("upstream request failed: {reason}"),
            "type": "server_error"
        }
    });
    let bytes = serde_json::to_vec(&body).expect("json serializable");
    Response::builder()
        .status(StatusCode::BAD_GATEWAY)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(
            Full::new(Bytes::from(bytes))
                .map_err(box_err_infallible)
                .boxed(),
        )
        .expect("response buildable")
}

/// 透明代理：转发请求到上游，原样回传响应。上游连接失败返回 502 JSON。
pub async fn proxy(
    cfg: &ResolvedConfig,
    client: &Client<HttpConnector, ReqBody>,
    req: Request<Incoming>,
) -> Response<RespBody> {
    let (parts, body) = req.into_parts();
    let boxed_body = body.map_err(box_err_generic).boxed();
    proxy_req(cfg, client, Request::from_parts(parts, boxed_body)).await
}

/// 透明代理（已缓冲 body 版本）：用于 SSE 探测后走透传的场景。
pub async fn proxy_buffered(
    cfg: &ResolvedConfig,
    client: &Client<HttpConnector, ReqBody>,
    parts: hyper::http::request::Parts,
    body: Bytes,
) -> Response<RespBody> {
    let boxed_body = Full::new(body).map_err(box_err_infallible).boxed();
    proxy_req(cfg, client, Request::from_parts(parts, boxed_body)).await
}

/// 核心代理逻辑：接收 `ReqBody`，转发到上游，原样回传响应。
pub async fn proxy_req(
    cfg: &ResolvedConfig,
    client: &Client<HttpConnector, ReqBody>,
    mut req: Request<ReqBody>,
) -> Response<RespBody> {
    let original_uri = req.uri().clone();
    let upstream_uri = upstream_uri(cfg, &original_uri);
    let host_val = HeaderValue::from_str(&cfg.upstream_authority).expect("authority valid");

    let headers = req.headers_mut();
    headers.remove(HOST);
    headers.insert(HOST, host_val);
    strip_hop_by_hop(headers);
    *req.uri_mut() = upstream_uri;

    match client.request(req).await {
        Ok(upstream_resp) => passthrough_response(upstream_resp),
        Err(e) => {
            tracing::warn!(error = %e, "upstream request failed (transparent)");
            bad_gateway(&e.to_string())
        }
    }
}

/// 透传上游响应：strip hop-by-hop、body 直通装箱。
pub fn passthrough_response(upstream_resp: Response<Incoming>) -> Response<RespBody> {
    let (mut parts, body) = upstream_resp.into_parts();
    strip_hop_by_hop(&mut parts.headers);
    Response::from_parts(parts, body.map_err(box_err_generic).boxed())
}

/// 把任意 body 错误统一转为 `BoxError`。
fn box_err_generic<E>(e: E) -> BoxError
where
    E: std::error::Error + Send + Sync + 'static,
{
    Box::new(e)
}
