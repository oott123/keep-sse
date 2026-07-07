# EventBoundaryTracker 用 memchr 加速扫描

## 背景

`src/sse.rs::EventBoundaryTracker::feed` 逐字节 `match` 下行 SSE 字节流中的 `\n` / `\r`。纯 ASCII token 流里绝大部分字节不是换行符，逐字节分支开销随吞吐线性增长。

## 需求

- 用 `memchr` crate 的 `memchr2(b'\n', b'\r', ..)` 批量定位换行符，跳过中间的非换行字节段（一段无换行字节只更新一次状态），仅在换行符附近推进状态机。
- 状态机逻辑（`\n` / `\r` / `\r\n` 处理、`at_boundary` / `line_empty` / `prev_cr` 转移）保持不变，现有单元测试全部通过。
