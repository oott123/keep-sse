# 设计：EventBoundaryTracker 用 memchr 加速

## 问题

`sse.rs::EventBoundaryTracker::feed` 逐字节 `match`：

```rust
for &b in bytes {
    match b { b'\n' => ..., b'\r' => ..., _ => { ... 三个字段赋值 } }
}
```

下行 SSE 字节流里绝大部分是 token 文本（非换行），逐字节分支 + 状态字段反复赋值，开销随吞吐线性增长。

## 方案

引入 `memchr` crate 的 `memchr2(b'\n', b'\r', bytes)`：用 SIMD 一次扫 64 字节定位换行符，跳过中间非换行段。

`feed` 重写为：以 `memchr2` 循环定位每个换行符，两换行符之间的非空字节段视为「有内容」一次性更新（`prev_cr=false; at_boundary=false; line_empty=false`），仅在换行符处推进状态机。

```rust
fn feed(&mut self, bytes: &[u8]) {
    let mut pos = 0;
    loop {
        match memchr::memchr2(b'\n', b'\r', &bytes[pos..]) {
            None => {
                // 尾段无换行符：若有任何字节，更新一次「有内容」
                if pos < bytes.len() {
                    self.prev_cr = false;
                    self.at_boundary = false;
                    self.line_empty = false;
                }
                return;
            }
            Some(i) => {
                let start = pos;
                let nl = pos + i;
                // 换行符前的非空段（若有字节）→ 更新「有内容」
                if nl > start {
                    self.prev_cr = false;
                    self.at_boundary = false;
                    self.line_empty = false;
                }
                // 处理换行符（状态机逻辑与现状一致）
                match bytes[nl] {
                    b'\n' => { ... }
                    b'\r' => { ... }
                    _ => unreachable!(),
                }
                pos = nl + 1;
            }
        }
    }
}
```

换行符处的状态机分支（`\n` / `\r` / `\r\n` 处理，含 `prev_cr` 去重）与现状逐字节版本完全一致——这是行为不变的关键，现有 8 个单元测试覆盖各种边界组合。

## 不做的事

- 不改 `EventBoundaryTracker` 的状态字段与判定语义。
- 不改 `at_boundary` / `line_empty` / `prev_cr` 的转移规则。
