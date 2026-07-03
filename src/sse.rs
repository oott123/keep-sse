//! SSE 包装通路：延迟 200 响应、心跳计时、上游桥接。

use std::convert::Infallible;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::header::{
    HeaderMap, HeaderValue, ACCEPT_ENCODING, CACHE_CONTROL, CONTENT_ENCODING, CONTENT_LENGTH,
    CONTENT_TYPE, HOST,
};
use hyper::http::request::Parts;
use hyper::{Request, Response, StatusCode};
use hyper_util::client::legacy::{connect::HttpConnector, Client, ResponseFuture};
use tokio::sync::mpsc;
use tokio_stream::StreamExt as _;

use crate::config::ResolvedConfig;
use crate::detect::EndpointKind;
use crate::encoding::{self, Coding, SseWriter};
use crate::error_event;
use crate::proxy::{self, BoxError, ReqBody, RespBody};

/// 心跳注释事件字节。
const HEARTBEAT: &[u8] = b":\n\n";

/// SSE 包装通路入口。
///
/// 与上游响应赛跑一个心跳间隔：窗口内上游返回则透传/桥接，超时则自行发 200 SSE。
pub async fn handle_stream(
    cfg: ResolvedConfig,
    client: Client<HttpConnector, ReqBody>,
    parts: Parts,
    body: Bytes,
    kind: EndpointKind,
) -> Response<RespBody> {
    // 协商下行编码。
    let accept = parts
        .headers
        .get(ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok());
    let down_coding = encoding::negotiate(accept);

    let interval = cfg.heartbeat_interval;

    // 构建上游请求并发起，与心跳间隔赛跑。
    let upstream_req = build_upstream_request(&cfg, parts, body);
    let mut upstream_fut = client.request(upstream_req);

    tokio::select! {
        result = &mut upstream_fut => {
            match result {
                Ok(resp) => {
                    let status = resp.status();
                    let headers = resp.headers();
                    let upstream_ce = encoding::parse_content_encoding(headers);
                    let is_sse = is_event_stream(headers);
                    if status.is_success() && is_sse {
                        if let Some(ce) = upstream_ce {
                            passthrough_sse_response(resp, kind, down_coding, ce, interval).await
                        } else {
                            proxy::passthrough_response(resp)
                        }
                    } else {
                        proxy::passthrough_response(resp)
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "upstream request failed (sse)");
                    proxy::bad_gateway(&e.to_string())
                }
            }
        }
        _ = tokio::time::sleep(interval) => {
            // 窗口超时：网关自行发 200 SSE，后台继续桥接上游。
            let (tx, rx) = mpsc::channel::<Result<Frame<Bytes>, std::io::Error>>(16);
            let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
            let resp_body = StreamBody::new(stream)
                .map_err(|e: std::io::Error| -> BoxError { Box::new(e) })
                .boxed();

            let mut resp = Response::new(resp_body);
            *resp.status_mut() = StatusCode::OK;
            let resp_headers = resp.headers_mut();
            resp_headers.insert(
                CONTENT_TYPE,
                HeaderValue::from_static("text/event-stream; charset=utf-8"),
            );
            resp_headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
            resp_headers.insert("X-Accel-Buffering", HeaderValue::from_static("no"));
            if let Some(val) = down_coding.header_value() {
                resp_headers.insert(CONTENT_ENCODING, HeaderValue::from_static(val));
            }

            tokio::task::spawn(bridge(
                kind,
                down_coding,
                interval,
                tx,
                upstream_fut,
            ));

            resp
        }
    }
}

/// 后台桥接：等待上游响应 → 解压 → SseWriter；心跳计时；错误事件。
async fn bridge(
    kind: EndpointKind,
    down_coding: Coding,
    interval: Duration,
    tx: mpsc::Sender<Result<Frame<Bytes>, std::io::Error>>,
    upstream_fut: ResponseFuture,
) {
    let mut writer = SseWriter::new(down_coding, tx);
    let mut heartbeat = Box::pin(tokio::time::sleep(interval));
    let mut upstream_fut = upstream_fut;

    // 阶段 1：等待上游响应，同时保活。
    let upstream_resp = loop {
        tokio::select! {
            _ = &mut heartbeat => {
                if writer.write_event(Bytes::from_static(HEARTBEAT)).await.is_err() {
                    return; // 客户端已断开
                }
                heartbeat = Box::pin(tokio::time::sleep(interval));
            }
            result = &mut upstream_fut => {
                break result;
            }
        }
    };

    match upstream_resp {
        Ok(resp) => {
            let status = resp.status();
            let (resp_parts, resp_body) = resp.into_parts();
            let upstream_ce = encoding::parse_content_encoding(&resp_parts.headers);

            if status.is_success() {
                handle_success(
                    &mut writer,
                    &mut heartbeat,
                    interval,
                    kind,
                    upstream_ce,
                    resp_body,
                )
                .await;
            } else {
                handle_error(
                    &mut writer,
                    &mut heartbeat,
                    interval,
                    kind,
                    status,
                    upstream_ce,
                    resp_body,
                )
                .await;
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "upstream request failed (sse)");
            let ev = error_event::build(kind, None, None, Some(&e.to_string()));
            let _ = writer.write_event(ev).await;
        }
    }

    writer.end().await;
}

/// 判断响应 `Content-Type` 是否为 `text/event-stream`（取 `;` 前媒体类型，不区分大小写）。
fn is_event_stream(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| {
            v.split(';')
                .next()
                .unwrap_or("")
                .trim()
                .eq_ignore_ascii_case("text/event-stream")
        })
        .unwrap_or(false)
}

/// 头透传 + 桥接 body：透传上游状态码与响应头（去掉 hop-by-hop、Content-Length，
/// 替换 Content-Encoding 为下行编码），body 走现有解压 → SseWriter → 重编码通路。
async fn passthrough_sse_response(
    resp: Response<Incoming>,
    kind: EndpointKind,
    down_coding: Coding,
    upstream_ce: Coding,
    interval: Duration,
) -> Response<RespBody> {
    let (mut parts, body) = resp.into_parts();

    proxy::strip_hop_by_hop(&mut parts.headers);
    parts.headers.remove(CONTENT_LENGTH);
    parts.headers.remove(CONTENT_ENCODING);
    if let Some(val) = down_coding.header_value() {
        parts
            .headers
            .insert(CONTENT_ENCODING, HeaderValue::from_static(val));
    }
    parts
        .headers
        .insert("X-Accel-Buffering", HeaderValue::from_static("no"));

    let (tx, rx) = mpsc::channel::<Result<Frame<Bytes>, std::io::Error>>(16);
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let resp_body = StreamBody::new(stream)
        .map_err(|e: std::io::Error| -> BoxError { Box::new(e) })
        .boxed();

    let resp = Response::from_parts(parts, resp_body);

    tokio::task::spawn(async move {
        let mut writer = SseWriter::new(down_coding, tx);
        let mut heartbeat = Box::pin(tokio::time::sleep(interval));
        handle_success(
            &mut writer,
            &mut heartbeat,
            interval,
            kind,
            Some(upstream_ce),
            body,
        )
        .await;
        writer.end().await;
    });

    resp
}

/// 处理 2xx 上游响应：流式解压并写入客户端。
async fn handle_success(
    writer: &mut SseWriter,
    heartbeat: &mut std::pin::Pin<Box<tokio::time::Sleep>>,
    interval: Duration,
    kind: EndpointKind,
    upstream_ce: Option<Coding>,
    body: Incoming,
) {
    let Some(ce) = upstream_ce else {
        // 不可解码的 Content-Encoding → 错误事件。
        let ev = error_event::build(
            kind,
            None,
            None,
            Some("unsupported upstream content-encoding"),
        );
        let _ = writer.write_event(ev).await;
        return;
    };

    let stream = encoding::decoder_stream(ce, body);
    tokio::pin!(stream);

    loop {
        tokio::select! {
            _ = &mut *heartbeat => {
                if writer.write_event(Bytes::from_static(HEARTBEAT)).await.is_err() {
                    return;
                }
                *heartbeat = Box::pin(tokio::time::sleep(interval));
            }
            chunk = stream.next() => {
                match chunk {
                    Some(Ok(bytes)) => {
                        if writer.write_event(bytes).await.is_err() {
                            return;
                        }
                        *heartbeat = Box::pin(tokio::time::sleep(interval));
                    }
                    Some(Err(e)) => {
                        tracing::warn!(error = %e, "upstream stream error (sse)");
                        let ev = error_event::build(kind, None, None, Some(&e.to_string()));
                        let _ = writer.write_event(ev).await;
                        return;
                    }
                    None => {
                        // 上游流正常结束。
                        return;
                    }
                }
            }
        }
    }
}

/// 处理非 2xx 上游响应：读取错误体（解压，上限 1 MiB），构造错误事件。
async fn handle_error(
    writer: &mut SseWriter,
    heartbeat: &mut std::pin::Pin<Box<tokio::time::Sleep>>,
    interval: Duration,
    kind: EndpointKind,
    status: StatusCode,
    upstream_ce: Option<Coding>,
    body: Incoming,
) {
    // 读取错误体（上限 1 MiB）。
    let collect_fut = Limited::new(body, 1024 * 1024).collect();
    tokio::pin!(collect_fut);
    let collected = loop {
        tokio::select! {
            _ = &mut *heartbeat => {
                if writer.write_event(Bytes::from_static(HEARTBEAT)).await.is_err() {
                    return;
                }
                *heartbeat = Box::pin(tokio::time::sleep(interval));
            }
            result = &mut collect_fut => {
                match result {
                    Ok(c) => break c.to_bytes(),
                    Err(_) => break Bytes::new(),
                }
            }
        }
    };

    // 解压错误体（如需）。
    let error_body: Vec<u8> = match upstream_ce {
        Some(Coding::Identity) | None => collected.to_vec(),
        Some(ce) => encoding::decode_bytes(ce, &collected)
            .await
            .unwrap_or_else(|_| collected.to_vec()),
    };

    let ev = error_event::build(kind, Some(status), Some(&error_body), None);
    let _ = writer.write_event(ev).await;
}

/// 构建上游请求：过滤 hop-by-hop、改写 Host 与 URI、body 为原始 Bytes。
fn build_upstream_request(cfg: &ResolvedConfig, mut parts: Parts, body: Bytes) -> Request<ReqBody> {
    let upstream_uri = proxy::upstream_uri(cfg, &parts.uri);
    let host_val = HeaderValue::from_str(&cfg.upstream_authority).expect("authority valid");

    parts.headers.remove(HOST);
    parts.headers.insert(HOST, host_val);
    proxy::strip_hop_by_hop(&mut parts.headers);
    parts.uri = upstream_uri;

    let boxed = Full::new(body).map_err(|e: Infallible| match e {}).boxed();
    Request::from_parts(parts, boxed)
}
