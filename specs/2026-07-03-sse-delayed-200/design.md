# Design: SSE 通路延迟 200 响应

## 目标行为

SSE 包装通路不再"立即先行 200"。判定为 LLM 流式请求后，网关立刻向上游发起请求，并以**一个心跳间隔**（`--heartbeat-interval`，复用现有配置，不新增参数）为等待窗口，与上游响应赛跑：

| 场景 | 网关行为 |
|---|---|
| 窗口内上游返回 2xx，`Content-Type` 为 `text/event-stream`，且 `Content-Encoding` 可解码 | **头透传 + 桥接 body**：透传上游状态码与响应头；body 走现有解压 → 桥接 → 重编码通路，流中空闲仍注入 `":\n\n"` 心跳 |
| 窗口内上游返回其它 HTTP 响应（非 2xx；或 2xx 非 SSE；或 2xx 但 `Content-Encoding` 不可解码） | **完整透传**：状态码、响应头、body 全部零转码直通，同透明代理通路 |
| 窗口内上游请求出错（连接失败、连接超时等 `client.request()` 错误） | 返回 `502 Bad Gateway` JSON（复用透明通路的 `bad_gateway`） |
| 窗口超时，上游仍无响应头 | 网关自行发出 `200 text/event-stream`（现行为），后台继续等待上游并进入现有桥接逻辑，**此后行为完全不变**：心跳保活、非 2xx → SSE 错误事件、请求出错 → SSE 错误事件、2xx → 桥接 |

判定"上游已返回 HTTP 响应"的时刻是响应头到达（`client.request()` future 完成）。

原设计中"上游响应头（`x-request-id`、限流头）无法透传"的已知取舍，在上游快于一个心跳间隔时不再成立；仅上游慢于窗口时仍存在。

## 头透传 + 桥接 body 的响应头处理

以上游响应头为基础：

- 过滤 hop-by-hop 头（复用 `strip_hop_by_hop`）；
- 移除 `Content-Length`（body 改为 chunked 流式输出）；
- `Content-Encoding` 替换为按客户端 `Accept-Encoding` 协商的下行编码（`identity` 则移除该头）；
- 添加 `X-Accel-Buffering: no`；
- 其余头（含 `Content-Type`、`x-request-id`、限流头等）原样保留。

SSE 判定：取 `Content-Type` 值 `;` 之前的媒体类型，trim 后与 `text/event-stream` 不区分大小写比较。

## 时序细节

- 等待窗口内客户端收不到任何字节（连响应头都没有）；客户端此时断开则 hyper 丢弃 service future，上游请求随之取消。
- 窗口超时发出 200 后，心跳计时从零重新开始（响应头本身就是刚发往客户端的字节）：上游持续沉默时客户端在 T=interval 收到 200 头，T=2×interval 收到第一个心跳。
- 头透传 + 桥接 body 分支同样以全新心跳计时进入桥接循环。

## 实现要点

改动集中在 `src/sse.rs`，`src/proxy.rs` 提取一个复用函数，其余模块不动：

- `proxy.rs`：从 `proxy_req` 的 `Ok` 分支提取 `passthrough_response(upstream_resp) -> Response<RespBody>`（strip hop-by-hop + body 直通装箱），供 `proxy_req` 与 `sse.rs` 共用。
- `sse.rs` `handle_stream` 重构：
  1. 协商下行编码（不变）；
  2. 构建上游请求并发起 `client.request()`，pin 住 future；
  3. `tokio::select!` 该 future 与 `sleep(heartbeat_interval)`：
     - future 先完成且 `Ok(resp)`：按上表分派——2xx SSE 可解码 → 构造透传头的 mpsc/StreamBody 响应，spawn 后台任务跑现有 `handle_success` 循环 + `writer.end()`；否则 → `passthrough_response`；
     - future 先完成且 `Err(e)` → `bad_gateway`；
     - 计时器先完成 → 构造现有的 200 SSE 响应，spawn `bridge`。
- `bridge` 签名调整：接收 pending 的上游响应 future（不再自建请求），阶段 1 心跳等待循环及其后逻辑不变。

不引入新依赖、不新增配置项。

## 测试

集成测试 `tests/gateway.rs`：

- `start_mock_raw` 增加"发送响应头前延迟"能力（现有 mock 头总是立即发出，无法构造窗口超时场景）。
- 更新现有测试：
  - `test_stream_true_chat_completions`、`test_stream_true_all_four_endpoints`：上游立即响应，走头透传分支，`Content-Type` 断言改为上游原值 `text/event-stream`；
  - `test_upstream_429_error_event_chat` → 改造为快速 429 完整透传（断言状态码 429、JSON body 原样）；
  - `test_upstream_connection_failure_sse` → 断言改为 502 JSON。
- 新增测试（心跳间隔 1s）：
  - 上游头延迟 > 窗口 → 客户端收到网关自有 200 头（`text/event-stream; charset=utf-8`），随后数据到达；
  - 上游头延迟 > 窗口且非 2xx → SSE 错误事件（原行为保持）；
  - 快速 2xx SSE 带 `x-request-id` 头且数据延迟 2.5s → 客户端可见该头，且 body 中有 ≥2 个心跳；
  - 快速 2xx `application/json`（上游忽略 stream 标志）→ 纯透传，body 逐字节原样。
