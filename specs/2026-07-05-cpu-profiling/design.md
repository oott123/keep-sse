# 设计：内嵌 pprof-rs CPU 采样器

## 方案选型

| 方案 | 可行性 | 结论 |
|---|---|---|
| `perf` + `cargo-flamegraph` | 需要 `perf` 二进制、`perf_event_paranoid<=1`、`CAP_SYS_ADMIN`；distroless 无 shell、生产容器不满足 | ❌ 线上不可用 |
| tokio-console | 仅异步任务调度分析，不抓 CPU 栈 | ❌ 不是 CPU profiler |
| **pprof-rs（内嵌）** | 纯 Rust，无需内核能力，普通容器即可；feature 门控、env 开启、信号 dump | ✅ 采用 |

pprof-rs 通过 SIGPROF 信号在固定频率（99Hz）采样所有线程栈，生成报告后可输出火焰图 SVG 与 protobuf，不依赖内核 perf 子系统。

## 设计

### 编译门控

- 新增 Cargo feature `pprof`（默认关闭），仅在 profiling 构建引入依赖：
  ```toml
  pprof = { version = "0.14", optional = true, features = ["flamegraph", "protobuf"] }
  ```
- 默认 release 构建完全不受影响（依赖不编入、代码 cfg 掉）。

### 启动激活

- 进程启动时读取 `KEEP_SSE_PPROF`：值为 `1` 则激活 99Hz 采样；其它值/未设置则不激活（零开销）。
- 激活后打印一条 `info` 日志告知「发 SIGUSR1 触发 dump」。
- 激活动作在 tokio runtime 内执行（main 中调用，`start_if_enabled()` 内 `tokio::spawn` 一个常驻信号任务持有 guard）。

### dump 触发

- 监听 `SIGUSR1`：收到即在常驻任务里用当前 `ProfilerGuard` 生成报告。
- 输出两类文件到 `KEEP_SSE_PPROF_DIR`（默认 `/tmp`），文件名带 unix 时间戳：
  - `keep-sse-<ts>.svg` —— 火焰图，浏览器直接打开。
  - `keep-sse-<ts>.pb` —— 未压缩 protobuf，`go tool pprof` 可读。
- dump 完成打印日志（含路径），失败打印 `warn` 但不中止进程。
- 可多次 `kill -USR1` 取多次快照。

### Docker 适配

- 专用 cargo profile（不污染默认 release）：
  ```toml
  [profile.pprof]
  inherits = "release"
  debug = true      # 保留符号，火焰图可读
  lto = false       # 关闭 LTO，避免内联模糊调用栈、加快编译
  ```
- Dockerfile 新增 build arg `PPROF`：
  - 默认（`PPROF=false`）走现有 `--release` 流程，镜像不变。
  - `PPROF=true` 改走 `--profile pprof --features pprof`，产出 profiling 镜像。
- profiling 镜像与常规镜像同 Dockerfile、同 runtime 基础镜像，仅构建参数不同。

### CI 镜像 tag

- 同一次 push 触发两个构建矩阵变体：`pprof=false`（默认）与 `pprof=true`。
- 默认变体沿用 `docker/metadata-action` 产出的常规 tag（branch / semver / sha / commit_date）。
- profiling 变体在每条常规 tag 后追加 `-pprof` 后缀（如 `main-pprof`、`1.2.3-pprof`、`sha-xxxxxxx-pprof`），通过 `docker/metadata-action` 的 `flavor: suffix=-pprof` 实现。
- 两个变体共享 GHA build cache（`type=gha`），key 按 `pprof` 变体区分避免互踩。
- PR 构建（`push=false`）只构建不推送，保持现有行为；profiling 变体同理。

### 信号兼容

- `SIGUSR1` 与现有 `SIGTERM`/`SIGINT`（graceful shutdown）互不冲突。
- profiling 任务独立于 shutdown 信号，进程退出时随运行时回收，guard drop 自动停采样。

### 不做的事

- 不开 HTTP 端点触发 dump（多一个攻击面 + 依赖 server 路由）；信号已足够。
- 不在默认 release 编入 profiler（避免生产镜像引入依赖与潜在开销）。
- 不压缩 protobuf（避免引入 flate2 主依赖；`go tool pprof` 接受未压缩 pb）。
