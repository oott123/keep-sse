# 设计：减少每请求小分配

## 问题 1：negotiate 的 HashMap

`encoding.rs::negotiate` 每请求建 `HashMap<String, bool>`，对 `Accept-Encoding` 的每个 token 做 `name.to_ascii_lowercase()` 分配 String 后 insert。`Accept-Encoding` 通常就 `zstd, br, gzip, deflate, identity` 五个固定值，用栈上变量即可。

## 方案

用五个 `Option<bool>`（`zstd/br/gzip/deflate/identity`）+ 一个 `wildcard: Option<bool>`，一遍 `split(',')` 扫描，用 `eq_ignore_ascii_case` 匹配 token 名（不分配）：

```rust
let (mut zstd, mut br, mut gzip, mut deflate, mut identity) = (None, None, None, None, None);
let mut wildcard = None;
for raw in val.split(',') {
    // ... 解析 name, q
    let allowed = q > 0.0;
    if name == "*" { wildcard = Some(allowed); }
    else if name.eq_ignore_ascii_case("zstd") { zstd = Some(allowed); }
    else if name.eq_ignore_ascii_case("br") { br = Some(allowed); }
    else if name.eq_ignore_ascii_case("gzip") { gzip = Some(allowed); }
    else if name.eq_ignore_ascii_case("deflate") { deflate = Some(allowed); }
    else if name.eq_ignore_ascii_case("identity") { identity = Some(allowed); }
}
let accepted = |v: Option<bool>| v.unwrap_or_else(|| wildcard.unwrap_or(false));
// 优先级 zstd > br > gzip > deflate > identity
if accepted(zstd) { return Zstd; }
...
let identity_ok = identity.unwrap_or(true);
```

协商优先级与 `q=0` 排除语义保持不变。

## 问题 2：handle 的 path/query to_string

`lib.rs::handle` 每请求 `req.uri().path().to_string()` 与 `query().map(|q| q.to_string())`，仅为传给同步的 `match_endpoint`。但 `match_endpoint` 是同步函数，可在 `req.into_parts()` 前直接借用 `req.uri().path()` / `.query()` 调用，无需 clone。

## 方案

```rust
let kind = match_endpoint(
    req.method(),
    req.uri().path(),
    req.uri().query(),
);
```

命中后按需在日志里 clone（`tracing::info!(path = %req.uri().path(), ...)` 直接借用，无需 owned）。`match_endpoint` 签名已是 `&Method, &str, Option<&str>`，无需改动。

## 不做的事

- 不改 `match_endpoint` 签名（已接收 `&str`）。
- 不改协商优先级。
- 不动探测 body 逻辑（由 streaming-stream-probe spec 处理）。
