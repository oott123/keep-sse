# 设计：复用 tokio Sleep

## 问题

`sse.rs` 三处（`:189` bridge 等待上游、`:333` handle_success、`:382` handle_error）每来一个 chunk 或心跳到期都执行：

```rust
*heartbeat = Box::pin(tokio::time::sleep(interval));
```

即：堆分配新 `Sleep` → 注册进 timer wheel → drop 旧 `Sleep` 注销。每路 SSE 几百个 chunk × 并发路数 = 大量短命堆分配与 timer 注册/注销抖动。

## 方案

`tokio::time::Sleep` 支持 `reset(&mut self, deadline: Instant)` 原地重置 deadline，无需重新分配、无需重注册（timer wheel 内原地更新）。这是官方推荐的复用方式。

`heartbeat` 类型保持 `Pin<Box<Sleep>>`，但**只创建一次**；到期后调用 `heartbeat.as_mut().reset(tokio::time::Instant::now() + interval)`，而非重新 `Box::pin(sleep(...))`。

三处统一改法：

```rust
// before
*heartbeat = Box::pin(tokio::time::sleep(interval));
// after
heartbeat.as_mut().reset(tokio::time::Instant::now() + interval);
```

初始化处（`bridge`、`passthrough_sse_response` spawn 体内）保持 `Box::pin(Sleep::new(interval))` 或 `Box::pin(tokio::time::sleep(interval))` 一次创建。

`select!` 中 `&mut *heartbeat` 的 poll 方式不变，`reset` 不影响 `Sleep` 的 pinned 可 poll 性质。

## 不做的事

- 不改心跳间隔、触发时机、客户端断开处理。
- 不改 `heartbeat` 的类型签名（仍是 `Pin<Box<Sleep>>`，避免波及 `handle_success`/`handle_error` 的参数）。
