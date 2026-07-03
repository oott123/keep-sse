# Design: keep-sse — LLM SSE 保活网关

## 总体架构

单二进制 Rust 反向代理，监听 HTTP，转发到单一固定上游（HTTP）。核心分两条数据通路：

1. **SSE 包装通路**（识别为 LLM 流式请求）：网关立即向客户端回复 `200 text/event-stream`，随后向上游发起请求，把上游流实时桥接进客户端 SSE 流；全程空闲 60 秒即发一个 `":\n\n"` 注释事件保活；上游失败时发送该 API 风格的 SSE 错误事件后结束流。
2. **透明代理通路**（其它一切请求）：方法、路径、查询串、请求体、响应体、`Content-Encoding` 全部原样双向透传，不做任何转码，帧级 zero-copy。

## 技术选型

| 组件 | 选择 | 理由 |
|---|---|---|
| 异步运行时 | tokio | 事实标准 |
| HTTP server/client | hyper 1.x + hyper-util | 直接操作 `Bytes` 帧，无框架开销，client 带连接池 |
| Body 工具 | http-body-util, bytes | `Bytes` 引用计数缓冲区是 zero-copy 的基础 |
| 压缩 | async-compression（tokio 特性，gzip/deflate/brotli/zstd） | 流式编解码，支持逐事件 flush |
| JSON | serde + serde_json | 只反序列化 `stream` 字段的探测结构体，其余字段跳过 |
| CLI/env | clap（derive + env 特性） | 命令行参数与环境变量统一定义 |
| 日志 | tracing + tracing-subscriber | 结构化日志 |

不引入 axum/tower：本项目本质是字节级代理，直接用 hyper 的 `service_fn` 可完全控制 body 流。

## LLM 流式请求识别

识别只看**路径后缀**（去掉查询串后），与任意前缀无关。四种接口的判定规则：

| 接口 | 路径后缀 | 方法 | 流式条件 |
|---|---|---|---|
| OpenAI Chat Completions | `/chat/completions` | POST | 请求体 JSON `"stream": true` |
| OpenAI Responses | `/responses` | POST | 请求体 JSON `"stream": true` |
| Anthropic Messages | `/messages` | POST | 请求体 JSON `"stream": true` |
| Gemini StreamGenerateContent | 最后一个路径段以 `:streamGenerateContent` 结尾 | POST | 查询串含 `alt=sse` |

- 后缀匹配为完整路径段匹配：`/api/v1/chat/completions`、`/openai/api/v1/chat/completions` 都命中；`/v1/messages/batches`、`/v1/responses/{id}` 不命中。
- 前三种接口需要读取请求体判断 `stream` 字段：命中后缀的 POST 请求体被完整缓冲（上限 `--max-probe-body`，默认 32 MiB；`Content-Length` 超限的直接走透明代理不缓冲，无 `Content-Length` 且收集中超限的以 413 拒绝），若带 `Content-Encoding` 则解压一份仅用于探测，**转发给上游的仍是原始压缩字节**（同一 `Bytes`，不复制）。JSON 解析失败或 `stream` 非 `true` 的，走透明代理通路。
- Gemini `alt=sse` 缺失时上游返回的是 JSON 数组流，无法插入 SSE 注释事件，走透明代理通路。
- 未命中任何后缀的请求（含所有非 POST）直接走透明代理通路。

## SSE 包装通路

时序：

1. 收到请求，缓冲请求体并判定为流式后，**立即**向客户端发送响应头：`200 OK`、`Content-Type: text/event-stream; charset=utf-8`、`Cache-Control: no-cache`、`X-Accel-Buffering: no`，`Content-Encoding` 按客户端 `Accept-Encoding` 协商（见压缩一节）。
2. 启动 60 秒（`--heartbeat-interval` 可调）空闲计时器：每次向客户端写出任何字节即重置；计时器到期写出 `":\n\n"` 并 flush，然后继续计时。心跳贯穿整个响应生命周期，直到流结束。
3. 同时向上游发起请求：原始方法、路径+查询串、过滤 hop-by-hop 后的原始头、原始请求体字节；`Accept-Encoding` 原样保留客户端请求中的值（客户端未携带则不发送）。
4. 上游返回 2xx：按上游 `Content-Encoding` 流式解压后，将数据帧原样写入客户端流（客户端协商了压缩则再经编码器写出，每帧后 flush）。上游返回网关无法解码的 `Content-Encoding`（非 gzip/deflate/br/zstd/identity）时，视为上游流异常，发送错误事件后结束流。上游流结束则结束客户端流。
5. 上游返回非 2xx：读取完整错误体（解压，上限 1 MiB），按接口类型包装成 SSE 错误事件（格式见 api.md）写给客户端，结束流。
6. 连接失败、超时、上游流中途断开：同样生成对应格式的错误事件后结束流。
7. 客户端断开：drop 上游请求，连接随之取消。

**已知取舍**：此通路下网关先于上游发出响应头，上游的响应头（如 `x-request-id`、限流头）无法透传给客户端。这是"先回 200 SSE 再请求上游"这一需求的固有代价。

## 透明代理通路

- 请求与响应 body 均不缓冲、不转码，`Bytes` 帧直接搬运。
- 过滤 hop-by-hop 头：`Connection` 及其列出的头、`Keep-Alive`、`Transfer-Encoding`、`TE`、`Trailer`、`Upgrade`、`Proxy-Authorization`、`Proxy-Authenticate`。
- `Host` 改写为上游 authority；除此之外不增删改任何请求头（不添加 `X-Forwarded-For` 等代理头）。
- `Content-Encoding`、`Accept-Encoding` 原样透传——压缩内容不解不压，天然支持任意编码。
- 上游连接失败时返回 `502 Bad Gateway`（JSON 错误体）。

## 压缩（Content-Encoding）

支持 `gzip`、`deflate`、`br`、`zstd` 四种编码，`identity` 兜底。

- **请求体**：透明通路原样透传。SSE 包装通路中仅为探测 `stream` 字段解压一份副本，上行仍发送原始字节。
- **响应体（透明通路）**：原样透传，零转码。
- **响应体（SSE 包装通路）**：网关是此响应的生产者（心跳与上游数据交织，无法拼接进上游的压缩流），因此：
  - 下行编码由客户端 `Accept-Encoding` 按 `zstd > br > gzip > deflate > identity` 优先级协商，选定后整流经 async-compression 编码器输出，**每个事件/心跳写出后 flush**，保证实时性与保活字节真正落到网络上；
  - 上行 `Accept-Encoding` 原样透传客户端的值，上游响应按其 `Content-Encoding` 流式解压后再进入下行编码器。

## Zero-copy 策略

- 全链路以 `bytes::Bytes`（引用计数切片）为帧单位，hyper server/client 原生支持，转发不发生内存复制。
- 透明通路：request body 与 response body 都是 `Frame<Bytes>` 直通，无缓冲、无复制。
- SSE 包装通路：请求体缓冲一次后以同一 `Bytes` 上行（探测解压是独立副本，不影响转发路径）；上游为 `identity` 且客户端为 `identity` 时，响应帧同样直通无复制；涉及压缩转码时复制不可避免，仅发生在编解码器内部。
- 心跳事件为 `Bytes::from_static(b":\n\n")`，零分配。

## 配置

clap 定义，命令行参数优先于环境变量：

| 参数 | 环境变量 | 默认值 | 说明 |
|---|---|---|---|
| `--listen` | `KEEP_SSE_LISTEN` | `0.0.0.0:8080` | 监听地址 |
| `--upstream` | `KEEP_SSE_UPSTREAM` | 必填 | 上游 base 地址，如 `http://host:port`，仅接受 `http://` scheme，其它 scheme 启动时报错退出 |
| `--heartbeat-interval` | `KEEP_SSE_HEARTBEAT_INTERVAL` | `60`（秒） | 空闲保活间隔 |
| `--connect-timeout` | `KEEP_SSE_CONNECT_TIMEOUT` | `10`（秒） | 上游 TCP 连接超时；连接建立后不设整体超时（LLM 流可以很长） |
| `--max-probe-body` | `KEEP_SSE_MAX_PROBE_BODY` | `33554432`（32 MiB） | 流式探测的请求体缓冲上限，超限走透明代理 |

## 模块划分

```
src/
  main.rs         — 入口：解析配置、启动 hyper server、按请求分发两条通路
  config.rs       — clap 配置定义与校验
  detect.rs       — EndpointKind 枚举、路径后缀识别、stream 字段探测
  proxy.rs        — 透明代理通路（头过滤、body 直通）
  sse.rs          — SSE 包装通路（先行 200、心跳计时、上游桥接）
  error_event.rs  — 四种接口的 SSE 错误事件构造
  encoding.rs     — Content-Encoding 解析、Accept-Encoding 协商、流式编解码器包装
```

## 测试策略

- `detect.rs`、`error_event.rs`、`encoding.rs` 的协商逻辑：单元测试。
- 集成测试（`tests/`）：用 hyper 起 mock 上游，覆盖——透明透传（含压缩体原样性）、四种接口的流式识别、心跳按时发出（用 1 秒间隔配置测试）、上游非 2xx 转错误事件、上游连接失败转错误事件、压缩请求体的 stream 探测且上行字节不变、客户端 `Accept-Encoding` 协商。
