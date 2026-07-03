# API: 对外行为变化

本 spec 相对 [2026-07-03-llm-sse-gateway/api.md](../2026-07-03-llm-sse-gateway/api.md) 的对外行为变化。未列出的行为不变。

## SSE 错误事件：标准错误体的嵌入方式

上游错误体能解析出该 API 的标准错误 JSON 时，嵌入的不再是原始字节，而是**紧凑单行重序列化**结果（字段与结构不变，仅去除换行与缩进），保证 `data:` 行为合法 SSE 帧。

原文（pretty-printed）：

```json
{
  "error": { "message": "rate limited", "type": "rate_limit_error" }
}
```

发出的事件：

```
data: {"error":{"message":"rate limited","type":"rate_limit_error"}}

```

## 心跳注入条件

心跳 `":\n\n"` 仅在下行 SSE 字节流处于**事件边界**（流开头或上一事件已以空行终结）时注入。空闲计时器到期但流处于事件中间时，本次心跳跳过，计时器重置后于下一个 interval 再检查。

上游流中途异常需要发送错误事件时，若流处于事件中间，先发出 `"\n\n"` 终结残缺事件，再发错误事件。

## 流式探测的请求体读取

| 情况 | 响应 |
|---|---|
| 请求体超过 `--max-probe-body`（无 `Content-Length` 或申报值撒谎） | `413`（不变） |
| 请求体读取失败（客户端中途断开等） | `400`，JSON：`{"error":{"message":"failed to read request body","type":"invalid_request_error"}}` |
| 压缩请求体解压后超过 `--max-probe-body` | 按非流式处理，走透明代理（原始压缩字节原样转发上游） |

## 配置

新增：

| 参数 | 环境变量 | 默认值 | 说明 |
|---|---|---|---|
| `--shutdown-timeout` | `KEEP_SSE_SHUTDOWN_TIMEOUT` | `30`（秒） | 收到 SIGTERM/SIGINT 后等待存量连接完成的时长，超时强制退出 |

## 进程信号

收到 `SIGTERM` 或 `SIGINT`：停止 accept 新连接，等待存量连接（含进行中的 SSE 流）自然结束，最长 `--shutdown-timeout`，随后进程退出。
