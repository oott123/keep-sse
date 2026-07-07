# 复用 tokio Sleep，消除每 chunk 重新分配

## 背景

`src/sse.rs` 在 SSE 桥接通路的心跳计时中，每收到一个上游 chunk 或每发一次心跳，都执行 `*heartbeat = Box::pin(tokio::time::sleep(interval))`，即堆上分配一个新的 `Sleep` 并注册进 tokio timer wheel，再 drop 旧的。LLM token 流场景下每路响应几百个 chunk × 并发路数，产生大量短命堆分配与 timer 注册/注销。

## 需求

- 复用同一个 `tokio::time::Sleep` 实例，到期后用 `Sleep::reset(deadline)` 重置，不再每 chunk 重新 `Box::pin` 分配。
- 行为不变：心跳间隔、触发时机、客户端断开处理逻辑保持一致。
