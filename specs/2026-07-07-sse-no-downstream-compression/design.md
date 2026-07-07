# 设计：SSE 下行不压缩

## 目标

SSE 包装通路（`src/sse.rs`）向下游发送的响应永远为 identity：不读客户端 `Accept-Encoding`、不协商、不写 `Content-Encoding` 头、不对下行字节流重编码。上游响应体仍按其 `Content-Encoding` 解压，以便在事件边界注入心跳；解压后的明文 SSE 字节直接以 identity 转发。

## 范围

### 改动（SSE 链路）

- `src/sse.rs`：`handle_stream`、`bridge`、`passthrough_sse_response` 去掉下行编码协商与 `Content-Encoding` 写入。
- `src/encoding.rs`：删除仅为下行重编码存在的机制——`negotiate`、`SseWriter` 的压缩变体、`VecSink`、`write_coded`、`finish_coded`、`Coding::header_value`、`Coding::as_str`、`async_compression::tokio::write` 编码器导入。`SseWriter` 退化为持有 `FrameTx` 的单一结构体。
- `tests/gateway.rs`：重写断言下行 `Content-Encoding` 的 SSE 测试，改为断言无该头且下行明文正确。
- `README.md`：同步「压缩」一节的行为描述。

### 不改动（非 SSE 链路）

- 透明代理通路 `src/proxy.rs`：原样转发，不解不压——保持不变。
- `handle_stream` 中上游 2xx SSE 但 `Content-Encoding` 不可解析（如 `snappy`）时回退到 `proxy::passthrough_response` 的分支：这是透明转发上游自身编码，非网关压缩，保持不变。
- 上行 `Accept-Encoding` 透传：`build_upstream_request` 不剥离该头（非 hop-by-hop），保持不变。
- 上游解压机制 `decoder_stream` / `decode_bytes`：注入心跳需要解码 SSE 字节流，保留。

## 行为变化

1. SSE 下行响应永远 identity，不写 `Content-Encoding`。
2. 慢路径（网关自建 200 SSE）与快路径（透传上游 2xx SSE）均不压缩下行。
3. 上游返回压缩 SSE（gzip/br/zstd/deflate）时，网关解压后以 identity 明文转发，心跳注入能力不变。
4. 上游返回不可解 `Content-Encoding`（如 snappy）的 2xx SSE 时，仍走透明透传（上游编码原样转发）——此分支属透明通路，不在本次改动内。

## 设计取舍

`SseWriter` 不再需要多编码抽象，保留为薄结构体：集中「客户端断开 → `io::Error`」的错误映射与帧发送，避免在 `bridge` / `handle_success` / `handle_error` 多处调用点重复 `tx.send(...).map_err(...)`。`negotiate` 等死代码按工程原则删除，不保留兼容路径。
