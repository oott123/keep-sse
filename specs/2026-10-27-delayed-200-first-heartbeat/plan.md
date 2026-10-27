# Plan: SSE 包装通路的首个心跳在窗口截止时到期

## 1. `tests/gateway.rs` — 先写测试

- 辅助函数 `read_head_and_first_heartbeat`：在期限内读到响应头与第一个 `:\n\n`，提前 EOF 或超时都失败。
- `test_delayed_200_sent_at_window`：2 s 间隔，上游 5 s 后才回响应头；3 s 内应收到 `HTTP/1.1 200` 与首个心跳（改前在 4 s）。
- `test_passthrough_headers_sent_by_window_end`：2 s 间隔，上游 1.5 s 回响应头、再过 3.5 s 才发数据；2.8 s 内应收到透传的响应头（含 `x-request-id`）与首个心跳（改前在 3.5 s）。
- `test_passthrough_byte_identical_before_window_end`：2 s 间隔，上游立即回响应头、0.3 s 后发数据；响应体应与上游逐字节相同。
- 撤销对应改动时各自失败：`bridge` 计时器改回 `interval`、透传计时器改回 `interval` 或改成 `Duration::ZERO`。

## 2. `src/sse.rs`

- `handle_stream` 记下 `window_end`，窗口赛跑用 `sleep_until(window_end)`。
- `bridge` 计时器初值 `Duration::ZERO`；`passthrough_sse_response` 增加 `window_end` 参数，计时器初值 `sleep_until(window_end)`。

## 3. `README.md`

- 「已知取舍」补上首个心跳在窗口截止时到期、第一个字节不晚于一个间隔。

## 4. 验证

- `cargo test` 全部通过；`cargo clippy --all-targets` 无 warning；`cargo fmt --check` 通过。
