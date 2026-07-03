# Proposal: LLM SSE 保活网关

我需要用 rust 做一个 LLM 网关，功能是这样的：判断用户发来的是否为流式请求，如果是，则响应 200 SSE，并开始每 60 秒回复一个空白事件（`":\n\n"`），再向上游发送请求，当上游请求失败时，返回对应格式的 stream 错误事件。其它情况下，都直接将请求转发给上游，并把上游的响应实时回复给客户端。

需要支持 openai chat completions, openai responses, anthropic messages stream, gemini streamcontent 四种接口，自动探测 API 路径是否是 LLM 请求（后缀识别，比如 `/api/v1/chat/completions` 和 `/openai/api/v1/chat/completions` 都得识别出来）。

尽量使用 zero copy 技术。不需要 SSL，上下游都是 http。需要支持请求体和响应体压缩（Content-Encoding）。

## 澄清补充（2026-07-03）

- **上游路由**：单一固定上游，配置一个上游 base 地址，所有请求路径原样透传；路径前缀只是上游自己的路径结构，不用于路由。
- **心跳时机**：全程空闲保活。整个流式响应期间，只要连续 60 秒没有向客户端发送任何数据，就发一个 `":\n\n"`；上游首包前和流中间的长停顿都要保活。
- **上游错误**：流式请求下，上游返回非 2xx（如 429/500 带 JSON 错误体）时，读取错误体并包装成该 API 风格的 SSE 错误事件发给客户端后结束流；连接失败、超时同样处理。
- **配置方式**：命令行参数 + 环境变量（如 `keep-sse --listen 0.0.0.0:8080 --upstream http://host:port`），无配置文件。
- **上行 Accept-Encoding**：向上游发送的 `Accept-Encoding` 与下游客户端请求中的保持一致，不做改写。
