# Proposal: 按 Accept 头探测 SSE 流式请求

增加一条 SSE 探测规则：命中 LLM 端点后，若请求带 `Accept: text/event-stream` 头，则直接判定为流式请求，跳过请求体 `"stream": true` 字段探测，立即走 SSE 包装通路。

## 澄清补充（2026-07-05）

- **作用范围**：仅在 `match_endpoint` 命中（OpenAI Chat Completions / Responses、Anthropic Messages、Gemini）后生效。非 LLM 路径即使带该头仍透明代理——错误事件格式依赖 `EndpointKind`，超出命中范围无法构造。
- **匹配方式**：精确匹配。Accept 头值去除前后空白后须等于 `text/event-stream`；多值 Accept（如 `text/event-stream, application/json`）或带参数（如 `text/event-stream; charset=utf-8`）不命中。
- **与 Gemini 的关系**：Gemini 已通过查询串 `alt=sse` 跳过 body 探测，Accept 头判定对其冗余但无害。
- **与 body 探测的关系**：Accept 头命中时跳过 `probe_stream_flag`（含 Content-Encoding 解码）；请求体仍按现有逻辑缓冲转发，`max-probe-body` 缓冲上限与 413/超限透明代理行为保持不变。
