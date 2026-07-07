# 执行计划：SSE 下行不压缩

## 1. encoding.rs：删除下行重编码机制

- 删除 `negotiate` 函数及其测试 `negotiate_priority`、`negotiate_q0_exclusion`。
- `SseWriter` 由 enum 改为 `pub struct SseWriter { tx: FrameTx }`：
  - `new(tx: FrameTx) -> Self`（不再接 `Coding`）。
  - `write_event`：`tx.send(Ok(Frame::data(bytes))).await.map_err(|_| io::Error::other("client gone"))`。
  - `end`：`drop(tx)`。
- 删除 `VecSink`、`write_coded`、`finish_coded`。
- 删除 `Coding::header_value` 与 `Coding::as_str`（`as_str` 仅 `header_value` 使用）。
- 删除 `use async_compression::tokio::write::{...}` 编码器导入；清理 `tokio::io` 中不再使用的 `AsyncWrite` / `AsyncWriteExt` 导入。
- 保留：`Coding` enum、`parse`、`parse_content_encoding`、`decode_bytes`、`decode_async`、`body_to_io_stream`、`decoder_stream`、`FrameTx` 别名、`bufread` 解码器导入。

## 2. sse.rs：移除下行编码协商

- `handle_stream`：删除读取 `ACCEPT_ENCODING` 与 `encoding::negotiate`；慢路径 200 响应删除 `CONTENT_ENCODING` 头写入（保留 Content-Type / Cache-Control / X-Accel-Buffering）。
- `bridge`：签名去掉 `down_coding: Coding`；`SseWriter::new(tx)`。
- `passthrough_sse_response`：签名去掉 `down_coding`；保留 `parts.headers.remove(CONTENT_ENCODING)`（下行 identity 不写头），删除 `down_coding.header_value()` 写入分支；`SseWriter::new(tx)`。
- 更新文档注释：`bridge` / `passthrough_sse_response` 中「重编码」「替换 Content-Encoding 为下行编码」改为「以 identity 转发」「剥离上游 Content-Encoding」。
- 清理 import：从 `hyper::header` 导入列表删除 `ACCEPT_ENCODING`（不再使用）；`CONTENT_ENCODING` 仍用于 `remove`，保留；`Coding` 仍用于 `handle_success` / `handle_error` 参数，保留。

## 3. tests/gateway.rs：重写 SSE 压缩测试

- `test_sse_downstream_gzip` → `test_sse_downstream_identity`：客户端发 `Accept-Encoding: gzip`；断言无 `content-encoding` 头；`dechunk` 后 body 为明文，含 `hello` / `world` 与心跳（不再 gz 解压）。
- `test_sse_fast_path_negotiates_zstd` → `test_sse_fast_path_identity`：断言无 `content-encoding` 头；body == `data: hello\n\n` 明文。
- `test_sse_fast_path_upstream_br_replaced_with_zstd` → `test_sse_fast_path_upstream_gzip_decoded_to_identity`：上游返回真实 gzip 压缩 SSE（`Content-Encoding: gzip`，用 `flate2` 编码 `data: hello\n\n`），客户端接受 gzip；断言无 `content-encoding` 头；`dechunk` 后 body == `data: hello\n\n`。
- `test_sse_slow_path_negotiates_zstd` → `test_sse_slow_path_identity`：断言无 `content-encoding` 头；body 明文含 `hello` + 心跳。
- 不改动：`test_undecodable_encoding_passthrough`、`test_accept_encoding_passthrough`（透明通路）。

## 4. README.md：同步压缩行为

- 「压缩」一节 SSE 包装通路描述改为：下行一律 identity（不压缩、不写 `Content-Encoding`）；上游响应体仍按其 `Content-Encoding` 解压以注入心跳；上行 `Accept-Encoding` 原样透传。
- 透明代理通路描述不变。

## 5. 验证

- `cargo fmt`
- `cargo clippy`
- `cargo test`（单测 + 集成测试，重点跑重写的 4 个 SSE 测试与 `test_undecodable_encoding_passthrough`）
