# 执行计划：减少每请求小分配

## 1. encoding.rs：negotiate 去 HashMap

- 删除 `HashMap<String, bool>` 与 `name.to_ascii_lowercase()`。
- 用五个 `Option<bool>` + `wildcard: Option<bool>` 栈上变量，`eq_ignore_ascii_case` 匹配。
- `accepted` 闭包改为读栈上变量。
- 优先级判断顺序不变（zstd > br > gzip > deflate > identity），`q=0` 排除、wildcard 兜底、identity 默认可接受 语义不变。

## 2. lib.rs：handle 借用 path/query

- `match_endpoint(req.method(), req.uri().path(), req.uri().query())` 直接借用，删除 `path`/`query` 的 `to_string()`。
- 后续 `tracing::info!` 直接借用 `req.uri().path()`（在 `req` 仍存活的作用域内）。
- 注意：`req.into_parts()` 消费 `req`，需在 `into_parts` 前完成 `match_endpoint` 与日志借用；命中 endpoint 后再 `into_parts`，path 从 `parts.uri.path()` 借用。
- 与 streaming-stream-probe 协同：两 spec 都改 `handle`，实现时合并为一次编辑。

## 3. 验证

- `cargo test`：`encoding.rs` 现有 negotiate 单元测试（`negotiate_*`）+ `detect.rs` 端点匹配测试通过。
- `cargo clippy` 无新 warning。
