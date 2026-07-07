# 执行计划：复用 tokio Sleep

## 1. sse.rs：三处 reset 替换

- `bridge`（`:189`）、`handle_success`（`:333`）、`handle_error`（`:382`）中的：
  ```rust
  *heartbeat = Box::pin(tokio::time::sleep(interval));
  ```
  替换为：
  ```rust
  heartbeat.as_mut().reset(tokio::time::Instant::now() + interval);
  ```
- 初始化处（`bridge` 的 `:179`、`passthrough_sse_response` spawn 体的 `:279`）保持 `Box::pin(tokio::time::sleep(interval))` 不变。

## 2. 验证

- `cargo test`：现有心跳相关集成测试（`tests/gateway.rs` 中验证心跳注入、客户端断开的用例）全部通过。
- `cargo clippy` 无新 warning。
