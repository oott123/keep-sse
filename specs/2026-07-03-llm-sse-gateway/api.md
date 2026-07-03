# API: keep-sse 对外行为与 SSE 错误事件格式

## 请求分发

| 判定 | 行为 |
|---|---|
| POST + 后缀 `/chat/completions` + 体 `"stream": true` | SSE 包装，错误事件用 OpenAI Chat Completions 格式 |
| POST + 后缀 `/responses` + 体 `"stream": true` | SSE 包装，错误事件用 OpenAI Responses 格式 |
| POST + 后缀 `/messages` + 体 `"stream": true` | SSE 包装，错误事件用 Anthropic 格式 |
| POST + 末段后缀 `:streamGenerateContent` + 查询串 `alt=sse` | SSE 包装，错误事件用 Gemini 格式 |
| 其它一切请求 | 透明代理 |

后缀匹配基于完整路径段，任意前缀均可命中（`/api/v1/chat/completions`、`/openai/api/v1/chat/completions` 等）。

## SSE 包装通路的响应头

```
HTTP/1.1 200 OK
Content-Type: text/event-stream; charset=utf-8
Cache-Control: no-cache
X-Accel-Buffering: no
Content-Encoding: <按 Accept-Encoding 协商，identity 时省略>
Transfer-Encoding: chunked
```

心跳为 SSE 注释行，原样字节：

```
:\n\n
```

## 错误事件格式

触发条件：上游返回非 2xx、连接失败、连接超时、上游流中途异常断开。连接级失败按 HTTP 502 处理，`message` 为 `upstream request failed: <原因>`。

上游错误体若能解析出该 API 的标准错误 JSON，则原样嵌入（不改写字段）；否则按下述模板合成，`message` 取错误体原文（截断至 4 KiB，无法解码时用状态行描述）。

### OpenAI Chat Completions

```
data: {"error":{"message":"<message>","type":"<type>","param":null,"code":null}}

data: [DONE]

```

`type`：4xx → `invalid_request_error`（429 → `rate_limit_error`），5xx/连接失败 → `server_error`。上游体已含顶层 `{"error":{...}}` 时整体原样作为 data。

### OpenAI Responses

```
event: error
data: {"type":"error","code":"<code>","message":"<message>","param":null,"sequence_number":0}

```

`code`：429 → `rate_limit_exceeded`，5xx/连接失败 → `server_error`，其它 4xx → `invalid_request_error`。上游体含 `{"error":{...}}` 时取其 `code`/`message`/`param` 填入。

### Anthropic Messages

```
event: error
data: {"type":"error","error":{"type":"<type>","message":"<message>"}}

```

HTTP 状态 → `type` 映射：400 → `invalid_request_error`，401 → `authentication_error`，403 → `permission_error`，404 → `not_found_error`，413 → `request_too_large`，429 → `rate_limit_error`，529 → `overloaded_error`，其余（含连接失败）→ `api_error`。上游体已是 `{"type":"error","error":{...}}` 时整体原样作为 data。

### Gemini（alt=sse）

```
data: {"error":{"code":<http_status>,"message":"<message>","status":"<status>"}}

```

HTTP 状态 → `status` 映射：400 → `INVALID_ARGUMENT`，401 → `UNAUTHENTICATED`，403 → `PERMISSION_DENIED`，404 → `NOT_FOUND`，429 → `RESOURCE_EXHAUSTED`，500 → `INTERNAL`，503 → `UNAVAILABLE`，504 → `DEADLINE_EXCEEDED`，其余（含连接失败，code 记 502）→ `UNKNOWN`。上游体含顶层 `{"error":{...}}` 时整体原样作为 data。

## 透明代理通路的错误

上游连接失败/超时时：

```
HTTP/1.1 502 Bad Gateway
Content-Type: application/json

{"error":{"message":"upstream request failed: <原因>","type":"server_error"}}
```
