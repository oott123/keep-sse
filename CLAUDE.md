# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

keep-sse 是一个 Rust 反向代理（LLM SSE 保活网关）：识别 LLM 流式请求并在整个响应期间注入 SSE 心跳注释（`":\n\n"`），防止空闲超时断连；其余请求透明转发。仅支持 HTTP/1.1 与 `http://` 上游。

## 常用命令

```sh
cargo build                  # 构建
cargo test                   # 全部测试（单元测试 + tests/gateway.rs 集成测试）
cargo test --test gateway    # 只跑集成测试
cargo test boundary_lf_lf    # 按名字跑单个测试
cargo clippy                 # lint
cargo fmt                    # 格式化
```

工具链由 mise 管理（`mise.toml`，rust latest）。

## 架构

请求处理分两条通路，分发逻辑在 `src/lib.rs::handle`：

1. **透明代理通路**（`src/proxy.rs`）：非 LLM 流式请求原样转发，body 零拷贝直通，不解压不重压。
2. **SSE 包装通路**（`src/sse.rs`）：命中 LLM 流式请求后，网关与上游响应「赛跑」一个心跳间隔——窗口内上游返回 2xx SSE 则透传其状态码/响应头并桥接 body；超时则网关先行发出 200 SSE 响应，后台 `bridge` 任务继续等待上游并持续发心跳。这是核心取舍：一旦网关先发 200，上游后续的状态码和响应头就无法透传，非 2xx 上游响应只能转为 SSE 错误事件。

分发流程：`match_endpoint`（`src/detect.rs`，按路径后缀匹配 4 种 LLM 接口）→ 缓冲请求体（受 `max_probe_body` 限制）→ `probe_stream_flag` 探测 `"stream": true`（Gemini 靠查询串 `alt=sse`，跳过探测）→ 命中走 `sse::handle_stream`，否则走 `proxy_buffered`。

关键模块协作：

- `src/detect.rs` — 端点识别（EndpointKind：ChatCompletions / Responses / AnthropicMessages / GeminiStream）与 `stream` 字段探测。
- `src/encoding.rs` — gzip/deflate/br/zstd 编解码。SSE 通路下行编码按客户端 `Accept-Encoding` 协商（zstd > br > gzip > deflate > identity），`SseWriter` 负责重编码并在每个事件后 flush。
- `src/error_event.rs` — 上游非 2xx 或失败时，按 EndpointKind 构造对应 API 格式的 SSE 错误事件（4 种接口格式各不相同，见 README）；上游体若能解析出该 API 标准错误 JSON 则原样嵌入。
- `src/sse.rs::EventBoundaryTracker` — 跟踪下行 SSE 字节流的事件边界，保证心跳只在边界处注入，不会切断半个事件。
- `src/proxy.rs` — 定义两条通路共享的 body 类型（`ReqBody`/`RespBody` = `BoxBody<Bytes, BoxError>`）、hop-by-hop 头剥离、Host/URI 改写。
- `src/server.rs` — accept 循环与 graceful shutdown（SIGTERM/ctrl-c）。

`src/lib.rs` 暴露所有模块是为了让 `tests/gateway.rs` 集成测试可用；测试通过 `server::run_with_shutdown` 启动真实网关，配合 mock 上游（含原始 TCP 的流式 mock，可控制分块与延迟）做端到端验证。

## 约定

- 代码注释与文档注释使用简体中文。
- `specs/` 目录按 `YYYY-MM-DD-<feature>/`（proposal.md / design.md / plan.md / api.md）记录每个特性的提案、设计与实施计划，改动行为时可参考对应 spec。
- 行为约定（端点匹配规则、错误事件格式、压缩策略、已知取舍）在 README.md 中有权威描述，改动相关行为时需同步更新 README。
