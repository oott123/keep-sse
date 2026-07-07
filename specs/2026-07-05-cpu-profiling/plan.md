# 执行计划

## 1. 依赖与 profile

- `Cargo.toml`：新增 `[features] default = []`、`pprof = ["dep:pprof"]`；`pprof = { version = "0.14", optional = true, features = ["flamegraph", "prost-codec"] }`。
- 新增 `[profile.pprof]`：`inherits = "release"`、`debug = true`、`lto = false`。

## 2. pprof 模块

- 新建 `src/pprof.rs`：
  - `pub fn start_if_enabled()`：检查 `KEEP_SSE_PPROF==1`，是则 `tokio::spawn` 常驻 dump 任务；否则直接返回。
  - 常驻任务：`ProfilerGuard::new(99)` 建 guard 并持有 → `signal(SignalKind::user_defined1())` 循环 `recv()` → 每次 dump：
    - `guard.report().build()` 得 `Report`。
    - 写 `KEEP_SSE_PPROF_DIR/keep-sse-<ts>.svg`（`report.flamegraph(&mut file)`）。
    - 写 `KEEP_SSE_PPROF_DIR/keep-sse-<ts>.pb`（`report.pprof()` 得 `protos::Profile`，`Message::encode_to_vec` 写文件）。
    - 成功 `info!` 打印两路径；失败 `warn!` 打印错误、继续。
  - guard 生命周期随任务存活，进程退出时 drop 自动停采样。
- `src/lib.rs`：`#[cfg(feature = "pprof")] pub mod pprof;`

## 3. 接入入口

- `src/main.rs`：tracing 初始化后、绑定 listener 前，加：
  ```rust
  #[cfg(feature = "pprof")]
  keep_sse::pprof::start_if_enabled();
  ```

## 4. Dockerfile

- builder 阶段加 `ARG PPROF=false`：
  - `PPROF=true`：`cargo build --profile pprof --locked --features pprof`，二进制从 `target/pprof/keep-sse` 取。
  - 否则维持 `cargo build --release --locked`，从 `target/release/keep-sse` 取。
  - 用 shell `if`/`else` 在 RUN 内分支（`RUN if [ "$PPROF" = "true" ]; then cargo build --profile pprof --locked --features pprof && cp target/pprof/keep-sse /keep-sse; else cargo build --release --locked && cp target/release/keep-sse /keep-sse; fi`）。
- runtime 阶段不变。

## 5. GitHub Actions

- `.github/workflows/docker.yaml` 改为构建矩阵：
  ```yaml
  strategy:
    fail-fast: false
    matrix:
      variant: [default, pprof]
  ```
- 步骤里把 `PPROF` arg 与镜像 tag suffix 按 variant 派生：
  - `variant=default` → `PPROF=false`，`flavor:` 为空（沿用现有 tag）。
  - `variant=pprof` → `PPROF=true`，`flavor: suffix=-pprof`（每条 tag 自动加 `-pprof`）。
- `docker/metadata-action` 按矩阵设置 `flavor`：
  ```yaml
  flavor: ${{ matrix.variant == 'pprof' && 'suffix=-pprof' || '' }}
  ```
- `docker/build-push-action` 传入 `build-args: PPROF=...`（矩阵映射），GHA cache key 加 `${{ matrix.variant }}` 区分避免缓存互踩。
- PR 构建（`push=false`）行为不变；profiling 变体同理只构建不推送。

## 6. 验证

- `cargo build --features pprof --profile pprof` 编译通过。
- `cargo build --release` 仍通过（默认 feature 关闭，行为不变）。
- `cargo clippy --features pprof` 无 warning。
- 本地起 profiling 构建进程，`KEEP_SSE_PPROF=1`，打一个上游 mock，`kill -USR1` 后确认 `/tmp` 下生成 `.svg`/`.pb`，SVG 在浏览器可打开并显示调用栈。
- `docker build --build-arg PPROF=true -t keep-sse:pprof .` 构建 profiling 镜像，`docker run -e KEEP_SSE_PPROF=1`，`docker kill -s USR1`，`docker cp` 取出报告确认存在。
- `docker build --build-arg PPROF=false -t keep-sse:local .` 构建默认镜像，行为与现状一致。

## 7. 文档

- README 增补「CPU Profiling」小节：何时用、如何构建 profiling 镜像（本地 `--build-arg PPROF=true` 或拉 `:main-pprof` 等 tag）、如何触发 dump、如何读火焰图。

