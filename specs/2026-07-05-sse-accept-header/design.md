# Design: 按 Accept 头探测 SSE

## 目标

在 `match_endpoint` 命中的 LLM 端点分支内，新增一条优先于 body 探测的判定：请求 `Accept` 头精确等于 `text/event-stream` 时，直接走 SSE 包装通路，跳过请求体解码与 `"stream": true` 字段解析。

## 判定流程

`handle`（`src/lib.rs`）命中端点后的分支顺序调整如下：

1. `content_length > max_probe_body` → 透明代理（不变）。
2. 缓冲请求体（`Limited`，`max_probe_body` 上限，超限 413 / 读失败 400，不变）。
3. Gemini → `sse::handle_stream`（不变）。
4. **新增**：`accepts_event_stream(&parts.headers)` 为真 → `sse::handle_stream`。
5. 否则：解码 + `probe_stream_flag` → SSE 或 `proxy_buffered`（不变）。

Accept 头判定置于缓冲之后、解码之前：跳过的是「解码 + 探测」步骤，缓冲本身仍需保留以将请求体转发给上游。`content_length` 超限透明代理的早退守卫保持不变——超大请求体仍回退透明代理（无 SSE 保活），与既有取舍一致。

## 匹配规则

新增 `detect::accepts_event_stream(headers: &HeaderMap) -> bool`：

- 取 `ACCEPT` 头，`to_str()` 失败返回 `false`。
- 去除前后空白（HTTP OWS 语义上无意义，非宽松化）后与 `text/event-stream` 严格相等。
- 多值 Accept、带参数的 media type 一律不命中。无 `Accept` 头返回 `false`。

## 影响面

- `src/detect.rs`：新增 `accepts_event_stream` 及单元测试（精确匹配、多值不命中、带参数不命中、无头、空值）。
- `src/lib.rs`：在 Gemini 分支后插入 Accept 头判定分支，复用既有 `sse::handle_stream`。
- `README.md`：在「LLM 流式请求识别」节补充 Accept 头探测规则。
- 不引入新依赖，不改变错误事件格式、压缩策略、配置项与已知取舍。

## 取舍

- 超大请求体（`Content-Length > max_probe_body`）即使带 Accept 头仍走透明代理：保留既有内存上限守卫，换取一致性。这是可接受的回退——`max_probe_body` 默认 32 MiB，覆盖正常流式请求体。
- 精确匹配不接受多值 Accept：客户端若同时声明 `text/event-stream` 与其它类型不会被识别。LLM 客户端发送流式请求时 Accept 通常独占为 `text/event-stream`，此约束符合实际。
