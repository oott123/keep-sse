# Plan: SSE 通路延迟 200 响应

## 步骤 1：proxy.rs 提取透传响应函数

- 从 `proxy_req` 的 `Ok(upstream_resp)` 分支提取 `pub fn passthrough_response(upstream_resp: Response<Incoming>) -> Response<RespBody>`：`into_parts` → `strip_hop_by_hop` → body `map_err(box_err_generic).boxed()` → `from_parts`。
- `proxy_req` 改为调用它。

## 步骤 2：sse.rs 重构 handle_stream

- 新增私有辅助函数 `is_event_stream(headers: &HeaderMap) -> bool`：取 `Content-Type` 值 `;` 前部分，trim 后与 `text/event-stream` 不区分大小写比较。
- `handle_stream` 新流程：
  1. 协商 `down_coding`（现有逻辑）；
  2. `build_upstream_request` + `client.request()`，`Box::pin` 该 future；
  3. `tokio::select!`：
     - `result = &mut upstream_fut`：
       - `Ok(resp)` 且 `resp.status().is_success()` 且 `is_event_stream` 且 `parse_content_encoding` 为 `Some(ce)` → 走步骤 3 的头透传桥接；
       - 其它 `Ok(resp)` → `proxy::passthrough_response(resp)`；
       - `Err(e)` → `tracing::warn!` + `proxy::bad_gateway(&e.to_string())`；
     - `_ = tokio::time::sleep(cfg.heartbeat_interval)` → 构造现有 200 SSE 响应（headers 逻辑不变），spawn `bridge`（见步骤 4）。

## 步骤 3：头透传 + 桥接 body 分支

- 新增私有函数（如 `passthrough_sse_response`）：入参 `resp_parts`、`resp_body: Incoming`、`kind`、`down_coding`、`ce`、`interval`。
- 响应头处理：`strip_hop_by_hop`、移除 `CONTENT_LENGTH`、按 `down_coding.header_value()` 替换/移除 `CONTENT_ENCODING`、插入 `X-Accel-Buffering: no`，保留其余头与状态码。
- 构造 mpsc(16) + `ReceiverStream` + `StreamBody` body（同现有 `handle_stream`）。
- spawn 后台任务：`SseWriter::new(down_coding, tx)`、新建心跳计时器，调用现有 `handle_success`，结束后 `writer.end()`。

## 步骤 4：bridge 签名调整

- `bridge` 不再接收 `parts`/`body`、不再自建上游请求，改为接收已 pin 的上游响应 future（`Pin<Box<...>>`，与 `handle_stream` 中类型一致）。
- 阶段 1 心跳等待循环、`handle_success`/`handle_error`/错误事件逻辑不变。

## 步骤 5：更新集成测试

- `start_mock_raw` 的 handler 返回值增加响应头前延迟（如返回 `(status, headers, header_delay_ms, chunks)`，或在 headers 写出前 sleep 的独立字段），更新所有现有调用点。
- 更新断言：
  - `test_stream_true_chat_completions`、`test_stream_true_all_four_endpoints`：`content-type` 改为断言 `text/event-stream`（上游原值）；
  - `test_upstream_429_error_event_chat` → 重命名为快速 429 透传测试：断言状态 429、body 为上游 JSON 原样、无 `[DONE]`；
  - `test_upstream_connection_failure_sse`：断言状态 502、body 含 `upstream request failed`。
- 新增测试（网关心跳配置 1s）：
  - `test_slow_upstream_gateway_200`：上游头延迟 1.5s → 断言 `content-type: text/event-stream; charset=utf-8`（网关自有头），body 含上游数据；
  - `test_slow_upstream_429_error_event`：上游头延迟 1.5s 且 429 → 断言状态 200、body 含错误信息与 `[DONE]`；
  - `test_fast_sse_passthrough_headers_and_heartbeat`：上游立即返回 200 SSE 带 `x-request-id: test-123`，数据延迟 2.5s → 断言响应头含 `x-request-id`、body 心跳 ≥2、数据到达；
  - `test_fast_json_response_pure_passthrough`：上游立即返回 200 `application/json`（对 `stream:true` 请求）→ 断言 `content-type: application/json`、body 逐字节原样、无心跳字节。

## 步骤 6：验证

- `cargo fmt --check`、`cargo clippy`、`cargo test`。
- 更新 README.md 中关于"立即返回 200"的行为描述（如有）。
