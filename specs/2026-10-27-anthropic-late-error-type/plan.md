# Plan: 迟到的 5xx 可改写 Anthropic 错误类型

## 1. `src/config.rs` — 新增配置项

- `Config`、`ResolvedConfig` 各加 `anthropic_late_error_type`。
- `parse` 中 `trim`，空值为 `None`，非 `[a-z_]+` 返回错误。
- 新建 `#[cfg(test)] mod tests`：非法值（含空格、含大写）被拒；空白为 `None`；合法值去除前后空白。

## 2. `src/error_event.rs` — 新增 `build_late`

- `build_anthropic` 增加 `etype_override`，`build` 传 `None`。
- `build_late` 按改写条件分派到 `build_anthropic(.., Some(t))` 或 `build`。
- 单元测试：
  - sub2api 风格的 502 错误体：`error.type` 改写，`message` 保留，单行 `data:`；
  - 请求失败（`status` 为 `None`）：`type` 改写，`message` 与 `build` 相同；
  - 非 JSON 的 503：`type` 改写，`message` 为原文；
  - `error` 不是对象：按模板合成，`type` 改写；
  - 400/401/429：与 `build` 逐字节相同；
  - 其他三种接口（502 与请求失败）：与 `build` 逐字节相同；
  - 开关为 `None`：与 `build` 逐字节相同。

## 3. `src/sse.rs` — 调用点

- `handle_stream` 传开关给 `bridge`；`bridge` 的请求失败分支（`Err`）改用 `build_late`。
- `handle_error` 改为 `read_error_body`，返回错误体；`bridge` 用 `build_late` 构造事件。
- `handle_success` 不改。

## 4. `tests/gateway.rs` — 集成测试

- `test_cfg` 补字段。
- `test_late_502_anthropic_overloaded`：上游 1.5 s 后返回 502 与 sub2api 风格错误体，1 s 心跳；断言下行 200、含 `"type":"overloaded_error"` 与原 message、不含 `upstream_error`。
- `test_late_connect_failure_anthropic_overloaded`：上游收下请求后 1.5 s 不回任何字节即关闭；断言下行 200、含 `"type":"overloaded_error"`。

## 5. `README.md`

- 「配置」表加一行；「错误事件格式 → Anthropic Messages」补一段作用范围与用途。

## 6. 验证

- 先写测试：未实现时新测试失败。
- `cargo test`（单元 + `--test gateway`）全部通过。
- `cargo clippy --all-targets` 无 warning；`cargo fmt --check` 通过。
