# Local validation record

本文保留早期 MVP 验证及单次冒烟数据。正式的重复 benchmark 见 [2026-10-31 报告](../benchmarks/results/2026-10-31/report.md)，测量口径与复现命令见 [performance.md](performance.md)。

验证平台：macOS arm64，AppleClang 21，CMake 4.3.2，gRPC 1.85.1，Protobuf 7.36.2，Google Test 1.18.0。

验证结果：

- Debug、Release、ASan、TSan 四种构建均通过全部 17 项 Google Test 和真实进程恢复测试。
- 覆盖容量、背压、幂等提交、重复/错误/过期结果、心跳失联、重试预算、上报失败隔离、客户端超时和优雅退出。独立审查发现的失败派发重复命中同一 worker 问题已修复，并通过先失败后成功的回归测试验证。
- 仓库内 `run_suite.py --tasks 20` 的 28 个基准组合全部完成；下面记录另一轮每批 80 项的 Release 数据。
- sanitizer 同时作用于生成的 Protobuf 代码和业务代码，避免消息布局不一致；ASan 使用 Protobuf 的 MESSAGE_GLOBALS 临时 opt-out 与系统预编译库保持 ABI 一致。

这是单次 Release 冒烟基准，不是经过重复统计的性能结论。每批 80 项，worker 容量按 1、2 交替分配；mixed 为 5% 的 100ms sleep 和 95% 的 5ms sleep。每项任务均完成且无重试。测试使用同机回环网络，不代表跨机器性能。

| 策略 | Worker 数 | 总槽位 | 吞吐 tasks/s | p99 ms |
|---|---:|---:|---:|---:|
| round-robin | 1 | 1 | 58.0 | 1354.5 |
| round-robin | 2 | 3 | 166.2 | 458.9 |
| round-robin | 4 | 6 | 296.4 | 205.4 |
| round-robin | 8 | 12 | 448.5 | 141.4 |
| least-loaded | 1 | 1 | 59.1 | 1328.0 |
| least-loaded | 2 | 3 | 172.7 | 446.1 |
| least-loaded | 4 | 6 | 279.0 | 217.8 |
| least-loaded | 8 | 12 | 438.0 | 142.3 |

这组短批次结果没有证明哪种策略更好；调度策略的选择开销很小，固定扫描周期和同步派发仍是框架开销来源。应使用更大的 workload、预热、多次重复和受控硬件继续评估。

复现完整矩阵（本地/线程池基线，两种策略，1/2/4/8 workers，tiny/uniform/mixed）：

```sh
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j4
python3 benchmarks/run_suite.py --build-dir build-release --tasks 80 --repeats 1 --output-dir benchmarks/results/smoke-80
```

当前基准脚本已扩展为预热、重复测量、同构/异构、过载与故障的完整矩阵，原始输出为目录内 raw.json，包含环境和逐批样本。上表是扩展前的旧数据，不与新结果合并。

Docker Compose 配置已通过 `docker compose config --quiet`；本机 Docker daemon 未运行，镜像构建和 Linux CI 未在此环境实测。
