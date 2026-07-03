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

```sh
keep-sse --listen 0.0.0.0:8080 --upstream http://localhost:11434
```

## LLM 流式请求识别

按路径后缀匹配（完整路径段，任意前缀）：

| 接口 | 路径后缀 | 方法 | 流式条件 |
|---|---|---|---|
| OpenAI Chat Completions | `/chat/completions` | POST | 请求体 `"stream": true` |
| OpenAI Responses | `/responses` | POST | 请求体 `"stream": true` |
| Anthropic Messages | `/messages` | POST | 请求体 `"stream": true` |
| Gemini StreamGenerateContent | `:streamGenerateContent` | POST | 查询串含 `alt=sse` |

- `/api/v1/chat/completions`、`/openai/api/v1/chat/completions` 均命中
- `/v1/messages/batches`、`/v1/responses/{id}` 不命中
- 非流式请求（`stream: false` 或缺省）走透明代理

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

### Gemini（alt=sse）

```
data: {"error":{"code":<http_status>,"message":"<message>","status":"<status>"}}

```

上游体若能解析出该 API 的标准错误 JSON，则原样嵌入（不改写字段）。

## 压缩

- 支持 `gzip`、`deflate`、`br`、`zstd` 四种 Content-Encoding
- 透明代理通路：请求体和响应体原样透传，不解不压
- SSE 包装通路：下行编码按客户端 `Accept-Encoding` 协商（`zstd > br > gzip > deflate > identity`），每个事件后 flush；上行 `Accept-Encoding` 原样透传客户端值

## 已知取舍

- SSE 包装通路下网关等待一个心跳间隔与上游响应赛跑：窗口内上游返回 2xx SSE 流式响应则透传上游状态码与响应头（含 `x-request-id`、限流头等）并桥接 body；窗口超时则网关先行发出 200，此后上游的响应头无法透传给客户端
- 无 `Content-Length` 且缓冲超过 `--max-probe-body` 的候选请求返回 413
- `Content-Length` 超过 `--max-probe-body` 的候选请求直接走透明代理（不探测 `stream` 字段）
