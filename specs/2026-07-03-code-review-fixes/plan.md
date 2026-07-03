# Plan: 修复 code review 发现的问题

按依赖顺序执行；每步完成后 `cargo test` 保持绿。

## 1. 依赖与配置调整

- `Cargo.toml`：
  - `tokio` features 由 `full` 收窄为 `["rt-multi-thread", "macros", "net", "time", "io-util", "sync", "signal"]`；
  - `hyper-util` features 增加 `"server-graceful"`；
  - 删除 dev-dependencies 中的 `zstd`。
- `src/config.rs`：`Config` 新增 `--shutdown-timeout` / `KEEP_SSE_SHUTDOWN_TIMEOUT`，默认 `30`；`ResolvedConfig` 新增 `shutdown_timeout: Duration`。
- 编译验证 feature 收窄无缺失。

## 2. 解压输出上限

- `src/encoding.rs`：`decode_bytes` 增加 `max_out: usize` 参数，`take(max_out + 1)` 读取，超限返回 `io::Error::new(InvalidData, ...)`；更新 `parse_content_encoding` 文档为"请求或响应"。
- `src/lib.rs`：探测解压调用传 `cfg.max_probe_body`。
- `src/sse.rs`：`handle_error` 中收集上限 `1024 * 1024` 提为常量 `MAX_ERROR_BODY`，解压调用传该常量。
- 单元测试：解压结果恰好等于上限通过；超一字节报错。修正 `encoding.rs` 既有 `roundtrip_all_codings` 测试的调用签名。

## 3. 探测体收集错误区分与超限日志

- `src/lib.rs`：
  - `Limited::collect` 的 `Err(e)` 按 `downcast_ref::<http_body_util::LengthLimitError>()` 分流：超限 → 413（现 `response_413`），其它 → 新增 `response_400`（body 见 api.md）；
  - `Content-Length > max_probe_body` 走透明代理的分支加 `tracing::info!(path, content_length, ...)`。

## 4. 错误事件修复

- `src/error_event.rs`：
  - `build_chat` / `build_anthropic` / `build_gemini` 的标准错误体嵌入路径改为 `serde_json::to_string(&value)` 紧凑重序列化，删除三处 `expect("upstream body utf8")`（`parse_top_level_error` / `parse_anthropic_error` 返回的 `Value` 直接序列化）；
  - `message()` 截断改为作用于 `trimmed`，用 `is_char_boundary` 回退。
- 更新受影响单元测试（原样嵌入断言改为紧凑序列化断言），新增 pretty-printed 输入 → 单行输出、截断字符边界两个用例。

## 5. 心跳按事件边界注入

- `src/sse.rs`：
  - 新增 `EventBoundaryTracker`（字段与转移规则见 design.md 第 2 节）及其 `feed(&[u8])`；
  - `handle_success`：数据 chunk 写出前 feed；心跳到期且 `!at_boundary` 时跳过注入、仅重置计时器；两处错误事件写出前 `!at_boundary` 时先写 `"\n\n"`；
  - 提取 `sse_channel() -> (FrameTx, RespBody)`，替换窗口超时分支与 `passthrough_sse_response` 中的重复构造。
- `EventBoundaryTracker` 单元测试：`\n\n`、`\r\n\r\n`、混合终结符、跨 feed 边界、事件中间状态。

## 6. server 模块与 graceful shutdown

- 新增 `src/server.rs`：`pub async fn run(cfg, client, listener, shutdown)`，`GracefulShutdown` watch 每个连接；accept 出错 `warn!` + `sleep(100ms)`；shutdown 触发后 `select!` 等待 `graceful.shutdown()` 与 `sleep(cfg.shutdown_timeout)`。
- `src/lib.rs`：导出 `server` 模块。
- `src/main.rs`：改为解析配置、bind listener、构造信号 future（`ctrl_c` 与 unix `SIGTERM` 先到者）、调用 `server::run`；原 accept 循环删除。
- `src/detect.rs`：删除三处冗余相等判断（顺手项）。

## 7. 集成测试

- `tests/gateway.rs`：
  - `start_gateway` 重构：接受 `ResolvedConfig`（提供带默认值的构造 helper），内部改用 `keep_sse::server::run` 驱动，返回 shutdown 触发端；
  - 新增 `dechunk(&[u8]) -> Vec<u8>` helper；
  - 新增测试：
    1. `test_sse_downstream_gzip`：`Accept-Encoding: gzip` + 慢数据上游 → 响应 `Content-Encoding: gzip`，dechunk 后 flate2 解压，含 ≥2 心跳与数据事件；
    2. `test_undecodable_encoding_passthrough`：窗口内 2xx SSE + `Content-Encoding: snappy` → 状态、头、body 完整透传；
    3. `test_probe_decompress_bomb_goes_transparent`：`max_probe_body = 1024`，压缩后 < 1 KiB、解压后 > 1 KiB 的 gzip 体 → 透明代理，上游收到原始压缩字节；
    4. `test_error_event_reserialized_single_line`：慢上游 429 + pretty-printed 错误 JSON → 错误事件 `data:` 行内无裸换行且字段保留；
    5. `test_heartbeat_not_injected_mid_event`：间隔 1s，事件拆两 chunk 中间停 2.5s → 事件字节连续完整，心跳只在事件之间；
    6. `test_graceful_shutdown_drains_stream`：慢 SSE 请求进行中触发 shutdown → 流完整收完；随后新连接失败。

## 8. 收尾

- `cargo test`、`cargo clippy --all-targets` 全绿；
- `cargo build --release` 验证 feature 收窄后的发布构建。
