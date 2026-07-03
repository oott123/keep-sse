# Plan: keep-sse 实施计划

按序执行，每步结束时 `cargo build`（或 `cargo test`）通过后再进入下一步。

## 1. 项目骨架与依赖

- `cargo init --name keep-sse`。
- `Cargo.toml` 添加依赖：
  - `tokio`（features: `full`）
  - `hyper`（1.x，features: `http1`, `server`, `client`）
  - `hyper-util`（features: `tokio`, `server-auto`, `client-legacy`, `http1`）
  - `http-body-util`、`bytes`、`futures-util`
  - `async-compression`（features: `tokio`, `gzip`, `zlib`, `brotli`, `zstd`）
  - `serde`（derive）、`serde_json`
  - `clap`（features: `derive`, `env`）
  - `tracing`、`tracing-subscriber`（features: `env-filter`）
  - `tokio-util`（features: `io`，用于 Stream/AsyncRead 互转）
- dev-dependencies：`flate2`、`zstd`（集成测试里构造/校验压缩体用，或统一用 async-compression）。

## 2. config.rs + main.rs 启动骨架

- `Config`：`listen: SocketAddr`、`upstream: Uri`、`heartbeat_interval: u64`、`connect_timeout: u64`、`max_probe_body: usize`，参数与环境变量对照见 design.md。
- 校验：`upstream` scheme 必须为 `http`、必须有 authority，否则报错退出。
- `main.rs`：初始化 tracing；构建 `hyper_util::client::legacy::Client`（HTTP/1.1，连接池，connect timeout）；TCP listener 循环 accept，每连接 spawn `hyper` 服务；`service_fn` 里暂时对所有请求返回 501，作为通路占位。

## 3. detect.rs — 端点识别与 stream 探测

- `enum EndpointKind { ChatCompletions, Responses, AnthropicMessages, GeminiStream }`。
- `fn match_endpoint(method: &Method, path: &str, query: Option<&str>) -> Option<EndpointKind>`：按 design.md 的后缀规则（完整路径段匹配）；Gemini 额外要求查询串中存在 `alt=sse`。
- `fn probe_stream_flag(json_body: &[u8]) -> bool`：`#[derive(Deserialize)] struct Probe { #[serde(default)] stream: bool }`，解析失败返回 false。
- 单元测试：前缀无关命中、`/v1/messages/batches` 不命中、`/v1/responses/{id}` 不命中、非 POST 不命中、Gemini 有无 `alt=sse` 两种情况、`stream` 缺省/false/true、非法 JSON。

## 4. encoding.rs — Content-Encoding 工具

- `enum Coding { Identity, Gzip, Deflate, Br, Zstd }`；`parse_content_encoding(&HeaderMap) -> Option<Coding>`（多重编码或未知编码返回 None，调用方视为不可解码）。
- `negotiate(accept_encoding: &HeaderMap) -> Coding`：按 `zstd > br > gzip > deflate > identity` 选择（考虑 q=0 排除项即可，不做完整 q 值排序——只识别 `;q=0` 为排除）。
- `fn decode_bytes(coding, &[u8]) -> io::Result<Vec<u8>>`：一次性解压（探测与错误体读取用）。
- `fn decoder_stream(coding, body_stream) -> impl Stream<Item = io::Result<Bytes>>`：上游响应流式解压（`tokio_util::io::StreamReader` + async-compression 解码器 + `ReaderStream`）。
- 下行编码器：包装一个 `SseSink`，内部持有选定 coding 的 async-compression 编码器，`write_event(Bytes)` 后立即 flush 并把产出的压缩字节推给 hyper body channel；`Identity` 时直接透传原 `Bytes`（zero-copy）。
- 单元测试：协商优先级、q=0 排除、四种编码 roundtrip、未知编码返回 None。

## 5. proxy.rs — 透明代理通路

- `strip_hop_by_hop(&mut HeaderMap)`：移除 design.md 列出的头及 `Connection` 中列出的头。
- 构建上游请求：改写 URI（上游 scheme+authority + 原 path+query）、`Host` 改写，其余请求头原样保留（不添加 `X-Forwarded-For` 等代理头），请求体用 `Incoming` 直接作为 client body（帧直通，不缓冲）。
- 响应：过滤 hop-by-hop 后原样回传状态码、头、body 流。
- 上游错误 → 502 JSON（api.md 格式）。
- 此步完成后 main.rs 接入：所有请求先走透明代理，网关已可用作纯反代。

## 6. sse.rs — SSE 包装通路

- 入口：`handle_stream(req_parts, buffered_body: Bytes, kind: EndpointKind, ...)`。
- main.rs 分发逻辑改为：`match_endpoint` 命中且非 Gemini 时缓冲请求体并探测 `stream`。缓冲规则：`Content-Length` 超过 `max_probe_body` 的直接走透明代理（不缓冲）；否则用 `http_body_util::Limited` + `collect` 完整收集；无 `Content-Length`（chunked）且收集中超限的以 413 拒绝。带 `Content-Encoding` 的解压一份副本仅用于探测。Gemini 命中即走包装通路（同样缓冲请求体以便上行发送）。
- 响应构造：立即返回 200 + design.md 规定的头，body 用 `mpsc` channel 包装的 `StreamBody`。
- spawn 任务：
  1. 心跳：`tokio::time::sleep` 循环，每次向客户端写出后重置；到期写 `":\n\n"`（经 SseSink，含 flush）。用 `tokio::select!` 与桥接任务合并在同一循环中，避免锁。
  2. 上游请求：原头过滤 hop-by-hop、`Accept-Encoding` 原样保留客户端值（不添加 `X-Forwarded-For` 等代理头）、body 为缓冲的原始 `Bytes`。
  3. 2xx → `decoder_stream` 解压后逐帧写入 SseSink；上游 `Content-Encoding` 不可解码（非 gzip/deflate/br/zstd/identity）→ 视为上游流异常，发错误事件后结束；非 2xx → 读错误体（1 MiB 上限、解压）→ `error_event::build(kind, status, body)` 写入后结束；连接失败 → `error_event::build(kind, 502, reason)`。
  4. 客户端断开（channel 关闭）→ 退出循环，drop 上游连接。

## 7. error_event.rs — 错误事件构造

- `fn build(kind: EndpointKind, status: Option<StatusCode>, body: Option<&[u8]>, reason: Option<&str>) -> Bytes`，按 api.md 四种格式实现，含"上游体可解析则原样嵌入"的判定。
- 单元测试：每种接口 × {标准错误体原样嵌入、非 JSON 体合成、连接失败合成}，校验输出字节精确匹配 api.md 模板（含结尾空行与 chat completions 的 `[DONE]`）。

## 8. 集成测试（tests/gateway.rs）

用 hyper 在随机端口起 mock 上游，网关以测试配置启动（心跳间隔设 1 秒）：

1. 非 LLM 路径 GET/POST 透传：状态码、头、体（含 gzip 压缩体字节原样）双向一致。
2. `stream: false` 的 chat completions 走透传。
3. `stream: true` 的四种接口：客户端立刻收到 200 + `text/event-stream`，上游数据逐帧到达。
4. mock 上游延迟 2.5 秒再响应：客户端在数据前收到 2 个 `":\n\n"`。
5. mock 上游返回 429 JSON：客户端收到对应格式错误事件（四种接口各一例）。
6. 上游端口不通：收到连接失败错误事件；透明通路收到 502。
7. gzip 压缩的 `stream: true` 请求体：正确识别为流式，且 mock 上游收到的 body 字节与客户端发出的完全一致。
8. 客户端 `Accept-Encoding: gzip` 的流式请求：响应带 `Content-Encoding: gzip`，解压后事件序列正确，且首个心跳在 ~1 秒内可解出（验证 flush 生效）。
9. Gemini 无 `alt=sse` 走透传。
10. 两条通路下 mock 上游收到的 `Accept-Encoding` 与客户端发出的一致（含客户端未携带时上游也收不到），且不存在 `X-Forwarded-For`。

## 9. 收尾

- `cargo clippy -- -D warnings`、`cargo fmt` 通过。
- 编写 `README.md`：用途、配置表、识别规则、错误事件格式摘要、已知取舍（SSE 包装通路不透传上游响应头；无 `Content-Length` 且超过 `max-probe-body` 的候选请求返回 413）。
