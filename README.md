# keep-sse

LLM SSE 保活网关 — 一个 Rust 反向代理，识别 LLM 流式请求并自动注入 SSE 心跳，其余请求透明转发。

## 用途

LLM 流式响应中，上游在首包前或流中间可能有长时间停顿（思考、推理中）。keep-sse 在整个流式响应期间每 60 秒（可调）发送一个 SSE 注释事件（`":\n\n"`），防止客户端或中间代理因空闲超时断开连接。

## 配置

命令行参数优先于环境变量：

| 参数 | 环境变量 | 默认值 | 说明 |
|---|---|---|---|
| `--listen` | `KEEP_SSE_LISTEN` | `0.0.0.0:8080` | 监听地址 |
| `--upstream` | `KEEP_SSE_UPSTREAM` | 必填 | 上游 base 地址，仅 `http://` |
| `--heartbeat-interval` | `KEEP_SSE_HEARTBEAT_INTERVAL` | `60` | 空闲保活间隔（秒） |
| `--connect-timeout` | `KEEP_SSE_CONNECT_TIMEOUT` | `10` | 上游 TCP 连接超时（秒） |
| `--max-probe-body` | `KEEP_SSE_MAX_PROBE_BODY` | `33554432` | 流式探测请求体缓冲上限（字节） |
| `--anthropic-late-error-type` | `KEEP_SSE_ANTHROPIC_LATE_ERROR_TYPE` | 不设 | 网关先回 200 之后，上游返回 5xx 或请求失败（如连上后未回响应头就断开）时，Anthropic 错误事件改写成的 `error.type`（如 `overloaded_error`），只允许 `[a-z_]+`；见「错误事件格式」 |

```sh
keep-sse --listen 0.0.0.0:8080 --upstream http://localhost:11434
```

## LLM 流式请求识别

按路径后缀匹配（完整路径段，任意前缀）：

| 接口 | 路径后缀 | 方法 | 流式条件 |
|---|---|---|---|
| OpenAI Chat Completions | `/chat/completions` | POST | 请求体 `"stream": true`，或请求带 `Accept: text/event-stream` |
| OpenAI Responses | `/responses` | POST | 请求体 `"stream": true`，或请求带 `Accept: text/event-stream` |
| Anthropic Messages | `/messages` | POST | 请求体 `"stream": true`，或请求带 `Accept: text/event-stream` |
| Gemini StreamGenerateContent | `:streamGenerateContent` | POST | 查询串含 `alt=sse` |

- `/api/v1/chat/completions`、`/openai/api/v1/chat/completions` 均命中
- `/v1/messages/batches`、`/v1/responses/{id}` 不命中
- 非流式请求（`stream: false` 或缺省）走透明代理
- 请求带 `Accept: text/event-stream` 头（精确匹配，去除前后空白后等于 `text/event-stream`；多值或带参数不命中）时跳过请求体探测，直接走 SSE 包装通路；Gemini 仍靠查询串 `alt=sse`。

## 错误事件格式

上游非 2xx 或连接失败时，按接口类型发送 SSE 错误事件后结束流。连接级失败按 HTTP 502 处理。

### OpenAI Chat Completions

```
data: {"error":{"message":"<message>","type":"<type>","param":null,"code":null}}

data: [DONE]

```

### OpenAI Responses

```
event: error
data: {"type":"error","code":"<code>","message":"<message>","param":null,"sequence_number":0}

```

### Anthropic Messages

```
event: error
data: {"type":"error","error":{"type":"<type>","message":"<message>"}}

```

设置 `--anthropic-late-error-type` 后，网关已先回 200（窗口超时）、随后上游返回 5xx 或请求失败（`status` 为空，如连上后未回响应头就断开）时，`error.type` 一律改写为该值：上游体是标准错误 JSON 时只改这一个字段，其余字段原样保留；否则按模板合成，`message` 与不设时相同。窗口内的响应（含窗口内的连接失败，它以 502 透传）、4xx（含 429）、2xx 流中途出错、不可解码的 Content-Encoding 以及其他三种接口均不受影响。

用途：Claude Code 收到 200 之后的流内 `overloaded_error` 时会重新发起流式请求（有次数上限），而收到其他流内错误时会立即改用非流式请求；非流式请求走透明代理、没有心跳，长回答在有读超时的反向代理（如 Cloudflare 的 120 s）后容易超时。设为 `overloaded_error` 可让这类迟到的上游故障继续走流式重试。

### Gemini（alt=sse）

```
data: {"error":{"code":<http_status>,"message":"<message>","status":"<status>"}}

```

上游体若能解析出该 API 的标准错误 JSON，则原样嵌入（不改写字段；`--anthropic-late-error-type` 是唯一例外）。

## 压缩

- 支持 `gzip`、`deflate`、`br`、`zstd` 四种 Content-Encoding
- 透明代理通路：请求体和响应体原样透传，不解不压
- SSE 包装通路：下行一律 identity（不压缩、不写 `Content-Encoding`）；上游响应体仍按其 `Content-Encoding` 解压以在事件边界注入心跳，解压后以明文转发；上行 `Accept-Encoding` 原样透传客户端值

## CPU Profiling

keep-sse 可选编入 [pprof-rs](https://github.com/tikv/pprof-rs) 内嵌采样器，用于在不重启、不改动代码逻辑的前提下定位 CPU 热点。采样默认关闭，需在编译与运行时分别开启。

### 构建 profiling 镜像

Dockerfile 暴露 `PPROF` 构建参数。本地构建 profiling 镜像：

```sh
docker build --build-arg PPROF=true -t keep-sse:pprof .
```

CI 在常规镜像之外，会额外构建并推送带 `-pprof` 后缀的镜像（如 `ghcr.io/<repo>:main-pprof`、`:1.2.3-pprof`、`:sha-xxxxxxx-pprof`）。profiling 镜像与常规镜像同 Dockerfile、同 runtime 基础镜像，仅 cargo profile 与 feature 不同。

也可本地用 cargo 直接构建：

```sh
cargo build --features pprof --profile pprof
# 产物在 target/pprof/keep-sse
```

### 运行并触发 dump

profiling 二进制通过环境变量激活采样、通过 `SIGUSR1` 触发 dump：

| 环境变量 | 默认 | 说明 |
|---|---|---|
| `KEEP_SSE_PPROF` | （未设置）| 设为 `1` 启动 99Hz 采样；其它值/未设置则零开销不采样 |
| `KEEP_SSE_PPROF_DIR` | `/tmp` | dump 报告输出目录 |

容器内运行并触发一次 dump：

```sh
docker run -d -e KEEP_SSE_PPROF=1 -p 8080:8080 --name keep-sse keep-sse:pprof
docker kill -s USR1 keep-sse
docker cp keep-sse:/tmp/keep-sse-<ts>.svg ./   # 取出火焰图
docker cp keep-sse:/tmp/keep-sse-<ts>.pb ./    # 取出 protobuf 报告
```

每次 `SIGUSR1` 在 `KEEP_SSE_PPROF_DIR` 下写两个文件：

- `keep-sse-<unix_ts>.svg` —— 火焰图，浏览器直接打开。
- `keep-sse-<unix_ts>.pb` —— 未压缩 protobuf，`go tool pprof keep-sse-<ts>.pb` 可读。

dump 成功打印 `info` 日志（含路径），失败打印 `warn` 但不中止进程，可多次发送信号取多次快照。`SIGUSR1` 与 graceful shutdown 用的 `SIGTERM`/`SIGINT` 互不冲突。

### 读取火焰图

`.svg` 用浏览器打开即可：横轴为采样数（按调用栈聚合），纵轴为调用深度，宽栈段即 CPU 热点。`.pb` 可用 `go tool pprof` 做交互式分析（`top`、`list`、`web` 等子命令）。

## 已知取舍

- SSE 包装通路下网关等待一个心跳间隔与上游响应赛跑：窗口内上游返回 2xx SSE 流式响应则透传上游状态码与响应头（含 `x-request-id`、限流头等）并桥接 body；窗口超时则网关先行发出 200，此后上游的响应头无法透传给客户端
- 无 `Content-Length` 且缓冲超过 `--max-probe-body` 的候选请求返回 413
- `Content-Length` 超过 `--max-probe-body` 的候选请求直接走透明代理（不探测 `stream` 字段）
