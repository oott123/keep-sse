use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::header::CONTENT_TYPE;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::sleep;

use keep_sse::config::ResolvedConfig;
use keep_sse::{create_client, handle};

/// 启动 mock 上游（Full body 响应），返回 (addr, shutdown)。
async fn start_mock<F>(handler: F) -> (SocketAddr, oneshot::Sender<()>)
where
    F: Fn(Request<Incoming>) -> Response<Full<Bytes>> + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, mut rx) = oneshot::channel::<()>();
    let handler = std::sync::Arc::new(handler);
    tokio::task::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut rx => break,
                accept = listener.accept() => {
                    let (stream, _) = match accept { Ok(v) => v, Err(_) => continue };
                    let io = TokioIo::new(stream);
                    let h = handler.clone();
                    tokio::task::spawn(async move {
                        let _ = http1::Builder::new()
                            .serve_connection(io, service_fn(move |req| {
                                let h = h.clone();
                                async move { Ok::<_, Infallible>(h(req)) }
                            }))
                            .await;
                    });
                }
            }
        }
    });
    (addr, tx)
}

/// 启动可流式发送数据的 mock 上游（原始 TCP）。
/// handler 返回 (status, headers, header_delay_ms, Vec<(data, delay_ms)>) —
/// header_delay_ms 为发送响应头前的延迟；每块发送后 sleep delay_ms。
async fn start_mock_raw<F>(handler: F) -> (SocketAddr, oneshot::Sender<()>)
where
    F: Fn(&[u8], &str) -> (u16, Vec<(&'static str, String)>, u64, Vec<(Vec<u8>, u64)>)
        + Send
        + Sync
        + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, mut rx) = oneshot::channel::<()>();
    let handler = std::sync::Arc::new(handler);
    tokio::task::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut rx => break,
                accept = listener.accept() => {
                    let (mut stream, _) = match accept { Ok(v) => v, Err(_) => continue };
                    let h = handler.clone();
                    tokio::task::spawn(async move {
                        let mut buf = vec![0u8; 8192];
                        let mut req_data = Vec::new();
                        loop {
                            let n = stream.read(&mut buf).await.unwrap();
                            if n == 0 { return; }
                            req_data.extend_from_slice(&buf[..n]);
                            if req_data.windows(4).any(|w| w == b"\r\n\r\n") { break; }
                        }
                        let header_end = req_data.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
                        let req_str = String::from_utf8_lossy(&req_data[..header_end]);
                        let path = req_str.lines().next().unwrap().split_whitespace().nth(1).unwrap_or("/");
                        let cl = req_str.lines()
                            .find(|l| l.to_lowercase().starts_with("content-length:"))
                            .and_then(|l| l.split(':').nth(1))
                            .and_then(|v| v.trim().parse::<usize>().ok())
                            .unwrap_or(0);
                        let mut body = req_data[header_end..].to_vec();
                        while body.len() < cl {
                            let n = stream.read(&mut buf).await.unwrap();
                            if n == 0 { break; }
                            body.extend_from_slice(&buf[..n]);
                        }
                        let (status, headers, header_delay_ms, chunks) = h(&body, path);
                        if header_delay_ms > 0 {
                            sleep(Duration::from_millis(header_delay_ms)).await;
                        }
                        let mut resp = format!("HTTP/1.1 {}\r\n", status);
                        for (k, v) in &headers {
                            resp.push_str(&format!("{}: {}\r\n", k, v));
                        }
                        resp.push_str("transfer-encoding: chunked\r\n\r\n");
                        stream.write_all(resp.as_bytes()).await.unwrap();
                        for (chunk, delay_ms) in &chunks {
                            if *delay_ms > 0 {
                                sleep(Duration::from_millis(*delay_ms)).await;
                            }
                            stream.write_all(format!("{:x}\r\n", chunk.len()).as_bytes()).await.unwrap();
                            stream.write_all(chunk).await.unwrap();
                            stream.write_all(b"\r\n").await.unwrap();
                            stream.flush().await.unwrap();
                        }
                        stream.write_all(b"0\r\n\r\n").await.unwrap();
                        stream.flush().await.unwrap();
                    });
                }
            }
        }
    });
    (addr, tx)
}

/// 启动网关。
async fn start_gateway(upstream: SocketAddr, heartbeat: u64) -> (SocketAddr, oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let upstream_str = format!("http://{}", upstream);
    let cfg = ResolvedConfig {
        listen: addr,
        upstream: upstream_str.parse().unwrap(),
        upstream_authority: upstream.to_string(),
        heartbeat_interval: Duration::from_secs(heartbeat),
        connect_timeout: Duration::from_secs(5),
        max_probe_body: 32 * 1024 * 1024,
    };
    let client = create_client(&cfg);
    let (tx, mut rx) = oneshot::channel::<()>();
    tokio::task::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut rx => break,
                accept = listener.accept() => {
                    let (stream, _) = match accept { Ok(v) => v, Err(_) => continue };
                    let io = TokioIo::new(stream);
                    let cfg = cfg.clone();
                    let client = client.clone();
                    tokio::task::spawn(async move {
                        let _ = http1::Builder::new()
                            .serve_connection(io, service_fn(move |req| {
                                let cfg = cfg.clone();
                                let client = client.clone();
                                async move { handle(cfg, client, req).await }
                            }))
                            .await;
                    });
                }
            }
        }
    });
    sleep(Duration::from_millis(50)).await;
    (addr, tx)
}

/// 发送 HTTP 请求并读取完整响应（原始字节）。
async fn send_raw(addr: SocketAddr, req: &str) -> Vec<u8> {
    send_raw_bytes(addr, req.as_bytes()).await
}

async fn send_raw_bytes(addr: SocketAddr, req: &[u8]) -> Vec<u8> {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    // Insert Connection: close if not present (so read_to_end terminates).
    if !req.windows(10).any(|w| w == b"Connection") {
        let pos = req
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .unwrap_or(req.len());
        let mut buf = Vec::with_capacity(req.len() + 20);
        buf.extend_from_slice(&req[..pos]);
        buf.extend_from_slice(b"\r\nConnection: close");
        buf.extend_from_slice(&req[pos..]);
        stream.write_all(&buf).await.unwrap();
    } else {
        stream.write_all(req).await.unwrap();
    }
    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await.unwrap();
    resp
}

/// 解析 HTTP 响应：返回 (status, headers, body)。
fn parse_response(raw: &[u8]) -> (u16, Vec<(String, String)>, Vec<u8>) {
    let header_end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .unwrap_or(raw.len());
    let header_str = String::from_utf8_lossy(&raw[..header_end]);
    let mut lines = header_str.lines();
    let status: u16 = lines
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let mut headers = Vec::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(": ") {
            headers.push((k.to_lowercase(), v.to_string()));
        }
    }
    let body = if header_end + 4 <= raw.len() {
        raw[header_end + 4..].to_vec()
    } else {
        Vec::new()
    };
    (status, headers, body)
}

// === Tests ===

#[tokio::test]
async fn test_transparent_get() {
    let (upstream, tx) = start_mock(|req| {
        let body = format!("GET {}", req.uri().path());
        Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "text/plain")
            .body(Full::new(Bytes::from(body)))
            .unwrap()
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 60).await;

    let resp = send_raw(gw, "GET /foo HTTP/1.1\r\nHost: localhost\r\n\r\n").await;
    let (status, _headers, body) = parse_response(&resp);
    assert_eq!(status, 200);
    assert_eq!(&body[..], b"GET /foo");

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_transparent_post_with_gzip_body() {
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;
    let payload = br#"{"hello":"world"}"#;
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    gz.write_all(payload).unwrap();
    let compressed = gz.finish().unwrap();

    let (upstream, tx) = start_mock(move |req| {
        let body = Full::new(Bytes::from(format!("echo:{}", req.uri().path())));
        Response::builder()
            .status(StatusCode::OK)
            .body(body)
            .unwrap()
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 60).await;

    let req = format!(
        "POST /api/data HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Encoding: gzip\r\n\r\n",
        compressed.len()
    );
    let mut req = req.into_bytes();
    req.extend_from_slice(&compressed);
    let resp = send_raw_bytes(gw, &req).await;
    let (status, _, _) = parse_response(&resp);
    assert_eq!(status, 200);

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_stream_false_goes_transparent() {
    let (upstream, tx) = start_mock(|_req| {
        Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from_static(b"{\"id\":\"chatcmpl-1\"}")))
            .unwrap()
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 60).await;

    let body = r#"{"model":"x","stream":false,"messages":[]}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(), body
    );
    let resp = send_raw(gw, &req).await;
    let (status, headers, resp_body) = parse_response(&resp);
    assert_eq!(status, 200);
    // Transparent path: Content-Type is application/json (not text/event-stream)
    assert_eq!(
        headers
            .iter()
            .find(|(k, _)| k == "content-type")
            .map(|(_, v)| v.as_str()),
        Some("application/json")
    );
    assert_eq!(&resp_body, b"{\"id\":\"chatcmpl-1\"}");

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_stream_true_chat_completions() {
    let (upstream, tx) = start_mock_raw(|_body, _path| {
        (
            200,
            vec![("content-type", "text/event-stream".to_string())],
            0,
            vec![
                (
                    b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n".to_vec(),
                    0,
                ),
                (b"data: [DONE]\n\n".to_vec(), 50),
            ],
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 60).await;

    let body = r#"{"model":"x","stream":true}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(), body
    );
    let resp = send_raw(gw, &req).await;
    let (status, headers, resp_body) = parse_response(&resp);
    assert_eq!(status, 200);
    // Fast SSE passthrough: content-type is upstream's original value
    assert_eq!(
        headers
            .iter()
            .find(|(k, _)| k == "content-type")
            .map(|(_, v)| v.as_str()),
        Some("text/event-stream")
    );
    let body_str = String::from_utf8_lossy(&resp_body);
    assert!(body_str.contains("data: {\"choices\""));
    assert!(body_str.contains("[DONE]"));

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_stream_true_all_four_endpoints() {
    for (path, body, _expect_event) in &[
        (
            "/v1/chat/completions",
            r#"{"stream":true}"#,
            "data: {\"choices\"",
        ),
        ("/v1/responses", r#"{"stream":true}"#, "data: {\"type\""),
        ("/v1/messages", r#"{"stream":true}"#, "data: {\"type\""),
        (
            "/v1beta/models/gemini-pro:streamGenerateContent?alt=sse",
            r#"{}"#,
            "data: {\"candidates\"",
        ),
    ] {
        let (upstream, tx) = start_mock_raw(|_b, _p| {
            (
                200,
                vec![("content-type", "text/event-stream".to_string())],
                0,
                vec![(
                    b"data: {\"type\":\"message\",\"choices\":[],\"candidates\":[]}\n\n".to_vec(),
                    0,
                )],
            )
        })
        .await;
        let (gw, gw_tx) = start_gateway(upstream, 60).await;

        let req = format!(
            "POST {} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
            path, body.len(), body
        );
        let resp = send_raw(gw, &req).await;
        let (status, headers, resp_body) = parse_response(&resp);
        assert_eq!(status, 200, "path {} should return 200", path);
        let ct = headers
            .iter()
            .find(|(k, _)| k == "content-type")
            .map(|(_, v)| v.as_str());
        assert_eq!(ct, Some("text/event-stream"), "path {} content-type", path);
        let body_str = String::from_utf8_lossy(&resp_body);
        assert!(
            body_str.contains("data: "),
            "path {} should have SSE data event",
            path
        );

        tx.send(()).unwrap();
        gw_tx.send(()).unwrap();
    }
}

#[tokio::test]
async fn test_heartbeat_during_delay() {
    let (upstream, tx) = start_mock_raw(|_b, _p| {
        (
            200,
            vec![("content-type", "text/event-stream".to_string())],
            0,
            vec![
                (b"data: hello\n\n".to_vec(), 2500), // 2.5s delay before first data
            ],
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 1).await; // 1s heartbeat

    let body = r#"{"stream":true}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(), body
    );
    let resp = send_raw(gw, &req).await;
    let (status, _, resp_body) = parse_response(&resp);
    assert_eq!(status, 200);
    let heartbeats = resp_body.windows(3).filter(|w| w == b":\n\n").count();
    // Should have at least 2 heartbeats before the data arrives
    assert!(
        heartbeats >= 2,
        "expected >= 2 heartbeats, got {} in {:?}",
        heartbeats,
        String::from_utf8_lossy(&resp_body)
    );
    assert!(
        resp_body.windows(5).any(|w| w == b"hello"),
        "should contain hello in {:?}",
        String::from_utf8_lossy(&resp_body)
    );

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_fast_429_passthrough() {
    let err = r#"{"error":{"message":"rate limited","type":"rate_limit_error","param":null,"code":null}}"#;
    let (upstream, tx) = start_mock(move |_req| {
        Response::builder()
            .status(StatusCode::TOO_MANY_REQUESTS)
            .header(CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from_static(err.as_bytes())))
            .unwrap()
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 60).await;

    let body = r#"{"stream":true}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(), body
    );
    let resp = send_raw(gw, &req).await;
    let (status, headers, resp_body) = parse_response(&resp);
    // Fast 429 → complete passthrough (status, headers, body)
    assert_eq!(status, 429);
    assert_eq!(
        headers
            .iter()
            .find(|(k, _)| k == "content-type")
            .map(|(_, v)| v.as_str()),
        Some("application/json")
    );
    assert_eq!(&resp_body[..], err.as_bytes());
    let body_str = String::from_utf8_lossy(&resp_body);
    assert!(!body_str.contains("[DONE]"));

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_upstream_connection_failure_sse() {
    // Bind and immediately close to get a port that's not listening.
    let addr = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();

    let (gw, gw_tx) = start_gateway(addr, 60).await;

    let body = r#"{"stream":true}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(), body
    );
    let resp = send_raw(gw, &req).await;
    let (status, _, resp_body) = parse_response(&resp);
    assert_eq!(status, 502); // TCP error → 502 Bad Gateway
    let body_str = String::from_utf8_lossy(&resp_body);
    assert!(body_str.contains("upstream request failed"));
    assert!(!body_str.contains("[DONE]"));

    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_upstream_connection_failure_transparent() {
    let addr = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    let (gw, gw_tx) = start_gateway(addr, 60).await;

    let resp = send_raw(gw, "GET /test HTTP/1.1\r\nHost: localhost\r\n\r\n").await;
    let (status, _, resp_body) = parse_response(&resp);
    assert_eq!(status, 502);
    let body_str = String::from_utf8_lossy(&resp_body);
    assert!(body_str.contains("upstream request failed"));

    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_gzip_stream_request_body() {
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    let payload = r#"{"model":"x","stream":true}"#;
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    gz.write_all(payload.as_bytes()).unwrap();
    let compressed = gz.finish().unwrap();

    let received_body = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let rb = received_body.clone();
    let (upstream, tx) = start_mock_raw(move |body, _path| {
        *rb.lock().unwrap() = body.to_vec();
        (
            200,
            vec![("content-type", "text/event-stream".to_string())],
            0,
            vec![(b"data: hi\n\n".to_vec(), 0)],
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 60).await;

    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Encoding: gzip\r\nContent-Type: application/json\r\n\r\n",
        compressed.len()
    );
    let mut req = req.into_bytes();
    req.extend_from_slice(&compressed);
    let resp = send_raw_bytes(gw, &req).await;
    let (status, _, _) = parse_response(&resp);
    assert_eq!(status, 200);

    // Verify upstream received the original compressed bytes
    let rb = received_body.lock().unwrap();
    assert_eq!(
        &rb[..],
        &compressed[..],
        "upstream should receive original compressed body"
    );

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_accept_encoding_passthrough() {
    let (upstream, tx) =
        start_mock_raw(|_body, _path| (200, vec![], 0, vec![(b"ok".to_vec(), 0)])).await;
    let (gw, gw_tx) = start_gateway(upstream, 60).await;

    // Transparent path with Accept-Encoding
    let resp = send_raw(
        gw,
        "GET /test HTTP/1.1\r\nHost: localhost\r\nAccept-Encoding: gzip, br\r\n\r\n",
    )
    .await;
    let (status, _, _) = parse_response(&resp);
    assert_eq!(status, 200);

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_gemini_without_alt_sse_goes_transparent() {
    let (upstream, tx) = start_mock(|_req| {
        Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from_static(b"[{\"candidates\":[]}]")))
            .unwrap()
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 60).await;

    let body = r#"{}"#;
    let req = format!(
        "POST /v1beta/models/gemini-pro:streamGenerateContent HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(), body
    );
    let resp = send_raw(gw, &req).await;
    let (status, headers, _resp_body) = parse_response(&resp);
    assert_eq!(status, 200);
    // No alt=sse → transparent, content-type from upstream
    assert_eq!(
        headers
            .iter()
            .find(|(k, _)| k == "content-type")
            .map(|(_, v)| v.as_str()),
        Some("application/json")
    );

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_slow_upstream_gateway_200() {
    // Upstream header delay > heartbeat window → gateway sends its own 200 SSE.
    let (upstream, tx) = start_mock_raw(|_b, _p| {
        (
            200,
            vec![("content-type", "text/event-stream".to_string())],
            1500, // 1.5s header delay > 1s heartbeat window
            vec![(b"data: hello\n\n".to_vec(), 0)],
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 1).await; // 1s heartbeat

    let body = r#"{"stream":true}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(), body
    );
    let resp = send_raw(gw, &req).await;
    let (status, headers, resp_body) = parse_response(&resp);
    assert_eq!(status, 200);
    // Gateway's own content-type (with charset)
    assert_eq!(
        headers
            .iter()
            .find(|(k, _)| k == "content-type")
            .map(|(_, v)| v.as_str()),
        Some("text/event-stream; charset=utf-8")
    );
    let body_str = String::from_utf8_lossy(&resp_body);
    assert!(body_str.contains("hello"));

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_slow_upstream_429_error_event() {
    // Upstream header delay > window and non-2xx → SSE error event via gateway 200.
    let (upstream, tx) = start_mock_raw(|_b, _p| {
        (
            429,
            vec![("content-type", "application/json".to_string())],
            1500, // 1.5s header delay > 1s window
            vec![(b"{\"error\":\"rate limited\"}".to_vec(), 0)],
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 1).await; // 1s heartbeat

    let body = r#"{"stream":true}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(), body
    );
    let resp = send_raw(gw, &req).await;
    let (status, _, resp_body) = parse_response(&resp);
    assert_eq!(status, 200); // Gateway 200 (window timeout)
    let body_str = String::from_utf8_lossy(&resp_body);
    assert!(body_str.contains("rate limited"));
    assert!(body_str.contains("[DONE]"));

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_fast_sse_passthrough_headers_and_heartbeat() {
    // Fast 2xx SSE with x-request-id header, data delayed 2.5s → header passthrough + heartbeats.
    let (upstream, tx) = start_mock_raw(|_b, _p| {
        (
            200,
            vec![
                ("content-type", "text/event-stream".to_string()),
                ("x-request-id", "test-123".to_string()),
            ],
            0,
            vec![(b"data: hello\n\n".to_vec(), 2500)],
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 1).await; // 1s heartbeat

    let body = r#"{"stream":true}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(), body
    );
    let resp = send_raw(gw, &req).await;
    let (status, headers, resp_body) = parse_response(&resp);
    assert_eq!(status, 200);
    // x-request-id should be passed through
    assert_eq!(
        headers
            .iter()
            .find(|(k, _)| k == "x-request-id")
            .map(|(_, v)| v.as_str()),
        Some("test-123")
    );
    let heartbeats = resp_body.windows(3).filter(|w| w == b":\n\n").count();
    assert!(
        heartbeats >= 2,
        "expected >= 2 heartbeats, got {} in {:?}",
        heartbeats,
        String::from_utf8_lossy(&resp_body)
    );
    assert!(
        resp_body.windows(5).any(|w| w == b"hello"),
        "should contain hello in {:?}",
        String::from_utf8_lossy(&resp_body)
    );

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_fast_json_response_pure_passthrough() {
    // Fast 2xx application/json for stream:true request → pure passthrough (no SSE wrapping).
    let json_body = br#"{"id":"chatcmpl-1","object":"chat.completion","choices":[]}"#;
    let (upstream, tx) = start_mock(move |_req| {
        Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from_static(json_body)))
            .unwrap()
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 1).await;

    let body = r#"{"stream":true}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(), body
    );
    let resp = send_raw(gw, &req).await;
    let (status, headers, resp_body) = parse_response(&resp);
    assert_eq!(status, 200);
    assert_eq!(
        headers
            .iter()
            .find(|(k, _)| k == "content-type")
            .map(|(_, v)| v.as_str()),
        Some("application/json")
    );
    // Body is byte-for-byte identical to upstream
    assert_eq!(&resp_body[..], json_body);
    // No heartbeat bytes
    assert!(!resp_body.windows(3).any(|w| w == b":\n\n"));

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}
