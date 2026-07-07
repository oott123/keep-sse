# CPU Profiling 支持

## 背景

负载高时 keep-sse CPU 占用偏高，需要能定位热点。

## 需求

- 提供在线 CPU profiling 能力，能在不重启、不改代码逻辑的前提下抓取热点栈。
- 必须能在线上 Docker 容器内运行：生产镜像基于 distroless（无 shell、非 root）、无 `perf`、无 `CAP_SYS_ADMIN`、`perf_event_paranoid` 受限。
- 可在容器内触发 dump 并把报告取出分析。
- Dockerfile 增加 build arg 区分 profiling 构建；CI 在常规镜像之外，额外构建并推送带 `-pprof` 后缀的 profiling 镜像 tag。
