# 执行计划：EventBoundaryTracker 用 memchr

## 1. 依赖

- `Cargo.toml`：`[dependencies]` 加 `memchr = "2"`。

## 2. sse.rs：EventBoundaryTracker::feed 重写

- 用 `memchr::memchr2(b'\n', b'\r', ..)` 循环定位换行符。
- 非换行字节段一次性更新 `prev_cr=false; at_boundary=false; line_empty=false`。
- 换行符处状态机分支保持现状逻辑（`\n` / `\r` / `\r\n` 去重）。
- 注意空段（连续换行符之间无字节）不更新「有内容」——保留 `line_empty` 语义。

## 3. 验证

- `cargo test boundary`：现有 8 个 `EventBoundaryTracker` 单元测试全部通过（`boundary_lf_lf` / `boundary_crlf_crlf` / `boundary_mixed_terminators` / `boundary_mid_event_not_at_boundary` / `boundary_split_across_feeds` / `boundary_cr_only_terminator` / `boundary_cr_then_non_newline` 等）。
- `cargo clippy` 无新 warning。
