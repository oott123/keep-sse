# Design: 修复 code review 发现的问题

改动分布在既有模块内，新增 `src/server.rs`（accept 循环 + graceful shutdown）。不引入新依赖，仅调整既有依赖的 feature。

## 1. 解压输出上限（问题 1）

`encoding::decode_bytes` 签名增加上限参数：

```rust
pub async fn decode_bytes(coding: Coding, input: &[u8], max_out: usize) -> io::Result<Vec<u8>>
```

实现：解码器经 `AsyncReadExt::take(max_out as u64 + 1)` 读取，读满 `max_out + 1` 字节即返回 `io::Error`（`InvalidData`，message 注明超限）。

调用点：

| 调用点 | 上限 | 超限/失败行为 |
|---|---|---|
| `lib.rs` 流式探测解压 | `cfg.max_probe_body`（解压后同一上限） | 现有 `unwrap_or_default()` 路径：探测体为空 → `stream` 判定 false → 走透明代理，原始压缩字节原样转发上游 |
| `sse.rs` `handle_error` 错误体解压 | `MAX_ERROR_BODY = 1 MiB`（提为常量，与收集上限同值） | 现有 `unwrap_or_else` 路径：回退用原始字节合成 message（非 UTF-8 时落到状态行描述） |

## 2. 心跳按事件边界注入（问题 2）

`sse.rs` 新增字节级状态机 `EventBoundaryTracker`，跟踪写往客户端的**解压后** SSE 字节流是否处于事件边界：

```rust
struct EventBoundaryTracker { at_boundary: bool, line_empty: bool, prev_cr: bool }
```

- 初始 `at_boundary = true`（流开头允许注入）。
- 行终结符按 SSE 规范处理 `\n`、`\r`、`\r\n`（`\r\n` 只算一次终结）。
- 行终结时：该行为空行 → `at_boundary = true`，否则 `false`；任何非终结字节 → `at_boundary = false`。

接入点仅 `handle_success`（阶段 1 心跳循环与 `handle_error` 只写心跳本身，天然保持边界，不需要 tracker）：

- 每个数据 chunk 在 `write_event` 之前 `feed` 进 tracker；
- 心跳计时器到期时，`at_boundary == false` 则**跳过本次注入**并重置计时器（事件中间停顿说明上游正在传输，注入只会损坏数据；下个 interval 再检查）；
- 写错误事件（上游流中途异常、不可解码编码）前若 `at_boundary == false`，先写 `"\n\n"` 终结残缺事件，再写错误事件。

数据 chunk 照旧逐帧直通，不做任何缓冲——tracker 只观察字节，不持有字节，实时性与 zero-copy 路径不变。

## 3. 错误 JSON 紧凑重序列化（问题 3）

`error_event.rs` 三处"标准错误体原样嵌入"路径（`build_chat`、`build_anthropic`、`build_gemini`）改为：解析成功后用 `serde_json::to_string(&value)` 输出紧凑单行 JSON。字段与结构不改写，仅字节表示归一为单行，保证 `data:` 行内无裸换行。三处 `std::str::from_utf8(b).expect("upstream body utf8")` 随之删除。

此行为覆盖原 [api.md](../2026-07-03-llm-sse-gateway/api.md) 中"整体原样嵌入"的字节级含义，见本 spec 的 api.md。

## 4. 探测体收集错误区分（问题 4）

`lib.rs` 中 `Limited::collect` 的 `Err(e)`：

- `e.downcast_ref::<http_body_util::LengthLimitError>()` 命中 → 413（现行为）；
- 其它（底层 body 读取失败，如客户端断开）→ 400 JSON：`{"error":{"message":"failed to read request body","type":"invalid_request_error"}}`。

## 5. accept 失败退避（问题 5）

`server.rs` accept 循环中 `accept()` 出错时 `warn!` 后 `sleep(100ms)` 再继续，消除 EMFILE 忙循环。

## 6. 探测超限降级日志（问题 6）

`lib.rs` 中 `Content-Length > max_probe_body` 走透明代理的分支加一条 `tracing::info!`（记录 path 与 content_length，注明该请求不再享有 SSE 保活）。

## 7. Graceful shutdown（问题 12）

- **配置**：`config.rs` 新增 `--shutdown-timeout` / `KEEP_SSE_SHUTDOWN_TIMEOUT`，默认 `30`（秒），`ResolvedConfig` 增加对应 `Duration` 字段。
- **新模块 `src/server.rs`**：

  ```rust
  pub async fn run(
      cfg: ResolvedConfig,
      client: GatewayClient,
      listener: TcpListener,
      shutdown: impl Future<Output = ()>,
  )
  ```

  内部使用 `hyper_util::server::graceful::GracefulShutdown`：accept 循环中每个连接经 `graceful.watch(conn)` 后 spawn；`shutdown` future 完成后停止 accept，`tokio::select!` 等待 `graceful.shutdown()` 与 `sleep(cfg.shutdown_timeout)` 先到者，超时则直接返回（进程退出掐断残余连接）。SSE 桥接任务的输出通过 body 流绑定在连接上，连接在流结束前不会被视为完成，无需额外跟踪。
- **`main.rs`**：只负责解析配置、bind、构造 shutdown future（`tokio::signal::ctrl_c` 与 unix `SIGTERM` 二选一先到）并调用 `server::run`。
- **集成测试**改用 `server::run` 驱动网关（替代测试内嵌的 accept 循环），使测试覆盖真实 server 路径。

依赖调整：`hyper-util` features 增加 `server-graceful`。

## 8. 轻微项（问题 7–11）

- `detect.rs`：删除三处冗余的 `path == "..."` 相等判断（`ends_with` 已覆盖）。
- `Cargo.toml`：删除未使用的 `zstd` dev-dependency；`tokio` features 由 `full` 收窄为 `["rt-multi-thread", "macros", "net", "time", "io-util", "sync", "signal"]`。
- `encoding.rs`：`parse_content_encoding` 文档改为"解析请求或响应的 `Content-Encoding`"。
- `sse.rs`：提取 `sse_channel() -> (FrameTx, RespBody)` helper，消除窗口超时分支与 `passthrough_sse_response` 的重复构造。
- `error_event.rs` `message()`：截断作用于 `trimmed`，用 `is_char_boundary` 回退到字符边界。

## 测试策略

单元测试：

- `EventBoundaryTracker`：`\n\n`、`\r\n\r\n`、混合终结符、跨 feed 调用的边界状态、事件中间状态。
- `decode_bytes` 上限：恰好等于上限通过、超一字节报错。
- `message()` 截断落在字符边界。

集成测试（`tests/gateway.rs`，新增 chunked 解码 helper `dechunk`）：

- **下行压缩端到端**：`Accept-Encoding: gzip` 的流式请求 + 慢数据上游 → 响应 `Content-Encoding: gzip`，dechunk 后整流解压，验证心跳与数据事件完整（覆盖缺口 13）。
- **不可解码编码透传**：窗口内上游 2xx SSE + `Content-Encoding: snappy` → 完整透传，头与 body 原样（覆盖缺口 14）。
- **解压上限回归**：`max_probe_body` 调小（如 1 KiB），发送压缩后小于上限、解压后超上限的 gzip 体 → 走透明代理，上游收到原始压缩字节。
- **错误事件重序列化回归**：慢上游（超窗口）返回 429 + pretty-printed 错误 JSON → SSE 错误事件的 `data:` 行为单行紧凑 JSON，无裸换行。
- **心跳边界回归**：心跳间隔 1s，上游把一个事件拆两个 chunk、中间停 2.5s → 客户端收到的字节中该事件连续完整，心跳只出现在事件之间。
- **graceful shutdown**：进行中的慢 SSE 请求 + 触发 shutdown → 该流完整收完；shutdown 后新连接无法建立。

`start_gateway` 重构为接受完整 `ResolvedConfig`（支持自定义 `max_probe_body`、`shutdown_timeout`）并返回 shutdown 触发端。
