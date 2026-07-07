# 减少每请求小分配

## 背景

`src/lib.rs::handle` 与 `src/encoding.rs::negotiate` 在每请求路径上有若干小堆分配，高 QPS 下累加：

1. `negotiate` 每请求建一个 `HashMap<String, bool>` 解析 `Accept-Encoding`。
2. `handle` 每请求 `req.uri().path().to_string()` 与 `query().map(|q| q.to_string())` 预先 clone，仅为传给同步的 `match_endpoint`。

## 需求

- `negotiate` 去掉 `HashMap`，改用对 `zstd/br/gzip/deflate/identity` 五个固定编码的栈上状态变量，一遍扫描直接设值。
- `handle` 不预先 `to_string`；`match_endpoint` 接收 `&str` 借用 `req.uri()`，命中后再按需 clone。
- 行为不变：协商优先级、端点匹配规则保持一致。
