# Proposal: 修复 code review 发现的问题

对 [specs/2026-07-03-llm-sse-gateway](../2026-07-03-llm-sse-gateway/proposal.md) 与 [specs/2026-07-03-sse-delayed-200](../2026-07-03-sse-delayed-200/proposal.md) 实现的 code review 发现了以下问题，需要修复。原 spec 保持不变，仅作参考。

## Review 发现的问题

### 较严重

1. **解压炸弹（DoS 风险）**：`max_probe_body` 限制的是压缩后请求体大小，但 `decode_bytes`（`src/encoding.rs`）解压输出无上限；探测路径（`src/lib.rs`）与错误体解压路径（`src/sse.rs` `handle_error`）都可被高压缩比载荷打出 OOM。
2. **心跳可能插进 SSE 事件中间**（`src/sse.rs` `handle_success`）：心跳按解压后 chunk 边界注入，chunk 边界与 SSE 事件边界无关；上游在事件中间停顿超过 interval 时，`":\n\n"` 会破坏正在传输的事件。
3. **上游错误 JSON 原样嵌入会破坏 SSE 帧**（`src/error_event.rs`）：pretty-printed 错误 JSON 中的换行会把 `data: <body>` 劈成非法 SSE 帧；同路径的 `expect("upstream body utf8")` 存在 panic 面。

### 中等

4. `src/lib.rs` 中 `Limited::collect` 的错误一律按 413 处理，未区分超限与底层 body 读取错误。
5. `src/main.rs` accept 失败后 `continue` 形成忙循环（典型如 EMFILE）。
6. 探测体超限走透明代理时静默失去保活（Gemini 多模态请求易触发），无日志可查。

### 轻微

7. `src/detect.rs` 三处 `path == "..." ||` 与 `ends_with` 冗余。
8. `Cargo.toml`：`zstd` dev-dependency 未使用；`tokio` `features = ["full"]` 过宽。
9. `src/encoding.rs` `parse_content_encoding` 文档仅提"响应"，实际也用于请求头。
10. `src/sse.rs` 窗口超时分支与 `passthrough_sse_response` 存在重复的 mpsc/StreamBody 构造。
11. `src/error_event.rs` `message()` 截断判断用 `trimmed.len()` 但截断作用于原始字节。
12. `src/main.rs` 无 graceful shutdown，SIGTERM 直接掐断进行中的流。

### 测试覆盖缺口

13. SSE 通路下行压缩（`Accept-Encoding` 协商 + `SseWriter` 编码分支）无端到端测试。
14. "窗口内 2xx SSE 但上游 `Content-Encoding` 不可解码 → 完整透传"分支无测试。

## 澄清补充（2026-07-03）

- **心跳边界（问题 2）**：修代码，按事件边界注入——桥接循环跟踪 SSE 事件边界，只在完整事件（空行）之后注入心跳，彻底消除数据损坏风险；不采用"仅记为已知取舍"。
- **graceful shutdown（问题 12）**：纳入本次修复。监听 SIGTERM/SIGINT，停止 accept 新连接，等待存量连接完成，超时强制退出。
- **测试缺口（问题 13、14）**：补齐，同时为本次修复的行为（解压上限、错误事件重序列化、心跳边界）添加回归测试。
