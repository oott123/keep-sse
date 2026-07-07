# Plan: 按 Accept 头探测 SSE

## 1. `src/detect.rs` — 新增 `accepts_event_stream`

- 引入 `use hyper::header::{ACCEPT, HeaderMap}`。
- 实现 `pub fn accepts_event_stream(headers: &HeaderMap) -> bool`：取 `ACCEPT` 头，`to_str()` 失败返回 `false`，`trim()` 后严格等于 `text/event-stream`。
- 单元测试覆盖：精确命中、前后空白命中、多值不命中、带参数不命中、无头返回 `false`、空字符串返回 `false`。

## 2. `src/lib.rs` — 插入 Accept 判定分支

- `use crate::detect::{accepts_event_stream, match_endpoint, probe_stream_flag, EndpointKind};` 补入 `accepts_event_stream`。
- 在 `handle` 中 Gemini 分支之后、`parse_content_encoding` 之前插入：

  ```rust
  if accepts_event_stream(&parts.headers) {
      return Ok(sse::handle_stream(cfg, client, parts, collected, kind).await);
  }
  ```

## 3. `README.md` — 更新识别规则

- 「LLM 流式请求识别」节表格的「流式条件」列补充：或请求带 `Accept: text/event-stream`。
- 表格下方补一行说明：带该头时跳过 body 探测直接走 SSE 通路（Gemini 仍靠 `alt=sse`）。

## 4. 验证

- `cargo test -p keep-sse detect::tests` — 新增单元测试通过。
- `cargo test --test gateway` — 既有集成测试不回归。
- 新增一条集成测试：对 `/chat/completions` 发送 `Accept: text/event-stream`、body 为 `{"stream": false}` 的请求，断言走 SSE 通路（收到 SSE 心跳/事件而非透明转发）。
- `cargo clippy`、`cargo fmt` 通过。
