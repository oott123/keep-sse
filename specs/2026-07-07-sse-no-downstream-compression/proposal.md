# SSE 下行不压缩

## 需求

SSE 包装通路向下游响应时，无论如何都不要压缩——一律以 identity（明文）发送，不写 `Content-Encoding` 头，不按客户端 `Accept-Encoding` 协商下行编码。

## 范围约束（用户补充）

只动 SSE 链路，别的不动：

- 透明代理通路保持原样（含 `handle_stream` 中上游返回不可解 `Content-Encoding` 时回退到 `proxy::passthrough_response` 的分支）。
- 上行 `Accept-Encoding` 透传给上游的行为保持原样。
- 上游响应体的解压（为在事件边界注入心跳）保持原样；仅移除下行重编码。
