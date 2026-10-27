# Design: 迟到的 5xx 可改写 Anthropic 错误类型

## 目标

在不改变窗口、心跳与透传语义的前提下，让网关先回 200 之后的 Anthropic 5xx/请求失败可以用指定的 `error.type` 写出，使 Claude Code 继续走流式重试。

## 配置（`src/config.rs`）

- `Config` 新增 `anthropic_late_error_type: Option<String>`（`--anthropic-late-error-type`，`KEEP_SSE_ANTHROPIC_LATE_ERROR_TYPE`）。
- `ResolvedConfig` 新增同名字段。`parse` 中先 `trim`：为空视为 `None`；非空值须全部由 `[a-z_]` 组成，否则返回 `invalid --anthropic-late-error-type ...`，进程以 FAILURE 退出（与 `--upstream` 校验失败一致）。

## 错误事件（`src/error_event.rs`）

`build` 的签名与行为不变。新增：

```rust
pub fn build_late(
    kind: EndpointKind,
    status: Option<StatusCode>,
    body: Option<&[u8]>,
    reason: Option<&str>,
    anthropic_type: Option<&str>,
) -> Bytes
```

- 改写条件：`anthropic_type` 为 `Some`，`kind` 为 `AnthropicMessages`，`status` 为 `None`（请求失败，如连上后未回响应头就断开）或 5xx。
- 改写方式：私有的 `build_anthropic` 增加 `etype_override` 参数（`build` 传 `None`）。
  - 上游体能被 `parse_anthropic_error` 解析且 `error` 为对象：就地把 `error.type` 设为该值，其余字段原样保留，紧凑序列化为单行 `data:`。
  - 解析不了，或 `error` 不是对象：按现有模板合成，`type` 取该值，`message` 仍由 `message(body, reason, status)` 生成。
- 其余情况直接调用 `build`，输出与现状逐字节相同。

## 调用点（`src/sse.rs`）

- `handle_stream` 把 `cfg.anthropic_late_error_type` 克隆后传给 `bridge`。
- `bridge` 的两条错误路径改用 `build_late`：
  - 连接上游失败（`Err(e)`）；
  - 上游非 2xx：原 `handle_error` 改为 `read_error_body`，只负责读取并解压错误体（期间照常发心跳，客户端断开时返回 `None`），错误事件由 `bridge` 构造。这样 `bridge` 的两处错误事件写在一起，也避免 `handle_error` 再加参数。
- 不改：`handle_success` 的两处 `build`（不可解码的编码；2xx 流中途出错）；窗口内的所有路径。

## 影响面

- `README.md`：「配置」表加一行；「错误事件格式 → Anthropic Messages」补充开关的作用范围与用途；「原样嵌入」一句注明例外。
- 不引入新依赖；默认不设时不改变任何输出。

## 取舍

- 只覆盖 Anthropic：Claude Code 是已知按流内错误类型决定重试方式的客户端；其他接口没有对应的约定。
- 2xx 流中途出错不改写：此时上游内容可能已部分转发，让客户端重发同一请求会重复输出。
- 4xx 不改写：429、401 等改成 overloaded 后，客户端会用错误的方式重试。
- 改写类型由配置给出、不写死 `overloaded_error`：取值是客户端约定，留给部署方决定。
