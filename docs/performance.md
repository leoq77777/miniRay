# Performance experiments

分析与结论见 [benchmark-results.md](benchmark-results.md)。完整实测结果见 [2026-10-31 报告](../benchmarks/results/2026-10-31/report.md)，逐批数据见 [raw.json](../benchmarks/results/2026-10-31/raw.json)，所有指标的中位数、IQR、范围、均值与样本标准差见 [summary.json](../benchmarks/results/2026-10-31/summary.json)。早期单次冒烟记录在 [validation.md](validation.md)，不应混入正式结果。

## 复现

只使用 Release、无 sanitizer 的构建，实验期间不要同时运行编译、测试或其他 benchmark。

```sh
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j4
python3 benchmarks/run_suite.py --build-dir build-release \
  --tasks 400 --repeats 5 --warmup 16 \
  --output-dir benchmarks/results/reproduction
```

macOS 可在 configure 时加 `-DCMAKE_PREFIX_PATH="$(brew --prefix)"`。不需要额外 Python 第三方包。suite 会检查 Release 配置、自动选择本机端口、启动/清理独立进程，按固定种子打乱每次重复的配置顺序。每完成一批便原子写入 raw.json；任何非预期失败都会退出，不会把不完整数据标为成功。重新汇总已有原始数据无需重新测量：

```sh
python3 benchmarks/summarize.py benchmarks/results/reproduction
```

快速验证完整实验链路：

```sh
python3 benchmarks/run_suite.py --tasks 20 --repeats 1 --warmup 2 \
  --output-dir benchmarks/results/smoke
```

输出包含 raw.json（环境、版本、源码与二进制 SHA-256、每次命令、全部指标）、summary.json、report.md，以及本机 logs/。若中途失败，保留的 raw.json 是部分数据；summarize 会拒绝缺组、缺重复或重复批次。再次运行会更新指定目录，正式数据和复现实验应使用不同目录。

## 实验矩阵

总计 42 个配置，默认各 5 次，共 210 个批次，逐批串行执行：

| 场景 | 配置 | 每配置重复数 |
|---|---|---:|
| tiny 开销 | local / thread-pool / RPC / remote，串行逐项等待 | 5 |
| 本地计算基线 | local / 4-thread pool × uniform / mixed | 5 |
| 同构扩展性 | 2 策略 × 1/2/4/8 workers × tiny/uniform/mixed，每 worker 固定 2 槽位 | 5 |
| 异构容量 | 2 策略 × tiny/uniform/mixed，worker 容量 1/2/4 | 5 |
| 过载背压 | 2 策略，1 worker / 2 slots / 接纳预算 8，400 次提交 | 5 |
| 执行中故障 | 2 策略，2 workers / 各 2 slots，32 项中终止 1 worker | 5 |

常规每批 400 项。worker、协调器和客户端每批重新启动。remote 先提交总槽位数两倍的 sleep 20ms 任务做连接预热，再执行 16 项工作负载预热；过载与故障工作负载预热为 2 项。预热任务和预热时间都不计入测量。RPC 基线通过预热查询建立连接。

工作负载：tiny 是单字节 echo；uniform 每项 sleep 5ms；mixed 固定顺序，每 20 项中的第 1 项 sleep 100ms，其余 sleep 5ms；failure 每项 sleep 250ms。sleep 模拟受控服务时长，不等同于 CPU 密集计算。突发模式为客户端连续发同步提交 RPC，随后收集结果；任务会分批抵达，不是同时到达。

## 计时口径

- `throughput_per_s`：测量阶段成功完成的任务数 / 从首项提交到取完结果的客户端墙钟时间。拒绝和失败不算成功吞吐。
- `latency_scope=client`：tiny/serial 使用客户端提交至取回结果的端到端时间。线程池 serial 在调用方 `future.get()` 返回后计时，包含投递及返回唤醒；remote serial 在 `Wait()` 返回后计时。
- RPC 基线调用 `GetTaskResult` 查询未知 ID，预期返回 `NOT_FOUND`，测客户端调用耗时，不含任务调度。该模式的 completed 表示 RPC 次数。
- `latency_scope=coordinator`：remote/burst 使用协调器接纳至终态时间，不包含提交请求的入站网络耗时，也不含客户端顺序查询造成的额外观察延迟。不能与 client 口径直接相减。
- thread-pool/burst 从本地投递至执行完成计时，不包含随后顺序收集 future 的时间；local 是直接函数执行，没有并发队列。
- `queue_p99_ms`：各 attempt 在协调器等待队列中的累计时间；本地线程池为投递至启动。`scheduling_p99_ms` 仅测策略选择，不包括状态锁等待、序列化或网络。
- `slot_utilization`：已收到的执行时间总和 /（客户端批次墙钟 × 总槽位），是执行槽位占用估算，不是 CPU 使用率。必须传入实际总容量。故障丢失的执行时间不可统计，故障批次不使用该指标。
- `attempts`：成功任务的最终 attempt_id + 1 总和。`recovered_tasks` 是最终 attempt 大于 0 的任务数，不是重试次数总和。
- p50/p95/p99 使用每批样本的 nearest-rank 分位数（向上取整）。报告展示每批分位数的中位数与 IQR，不冒充全体任务的合并分位数。完整汇总同时保留 min/max、mean、sample stdev。

CSV 还包含 tasks、warmup、connection_warmup、accepted、rejected、completed、failed 和 elapsed_s。所有批次验证 `tasks=accepted+rejected`、`accepted=completed+failed`；健康批次禁止隐含重试，故障批次必须确实发生重试。

## 过载

400 次连续提交 sleep 250ms，协调器接纳预算设置为 8，worker 容量 2。驱动捕获 `RESOURCE_EXHAUSTED`，继续统计其他请求并等待全部已接纳任务完成；输出 CSV 后以状态码 2 退出。suite 只在该场景把状态码 2 当作预期结果，并要求既有接纳又有拒绝。

总接纳数可能超过 8，因为提交过程中已完成任务会释放预算。接纳总量不是瞬时 outstanding 峰值，不能用它推断内存或队列峰值。运行时的容量不变量由单元测试验证；这里验证真实 RPC 背压和已接纳任务完成情况。

## 故障注入

suite 启动的两个 worker 各有独立的进程 incarnation ID。驱动在协调器确认指定 worker 有 RUNNING 任务后，向 suite 持有、尚未回收的那个子进程 PID 发送 SIGKILL。该选项仅供 suite 使用，不应手动传入不相关进程的 PID。父进程直到测量结束才回收该进程，因此正常 suite 路径不会发生 PID 重用。

心跳间隔 50ms、超时 500ms。驱动对选中任务约每 2ms 轮询一次，记录从 kill 返回至首次观察到下列状态的时间：

1. `detection_observed_ms`：attempt ID 递增，表示协调器已经做出重试决策。
2. `reassignment_observed_ms`：新 attempt 离开 PENDING，协调器已经为它预留/派发任务；不保证此时用户函数已开始。
3. `recovery_observed_ms`：选中任务完成。其余任务继续运行，整批完成时间另外记录。

这些指标包含轮询与 RPC 误差，是状态变化的观测上界，不是精确服务端事件时间。新任务和重试共用 FIFO 待调度队列，因此检测完成后重试还可能等待原有任务，不能把恢复延迟全部归因于心跳探测。

## 使用单个驱动

```sh
./build-release/miniray-benchmark --mode local --tasks 400 --workload tiny --pattern serial
./build-release/miniray-benchmark --mode thread-pool --slots 4 --tasks 400 --workload mixed
./build-release/miniray-benchmark --mode rpc --coordinator localhost:50051 --tasks 400 --workload tiny --pattern serial
./build-release/miniray-benchmark --mode remote --coordinator localhost:50051 --slots 6 --tasks 400 --workload mixed
```

单驱动使用已有集群；suite 才负责集群生命周期。CLI 的 warmup 必须为正数，默认 16。协调器没有 worker 时普通任务会继续等待，因此运行 remote 前应先启动 worker。

## 适用范围

local tiny 的计时接近时钟分辨率，保留了读取时钟的开销，未做基线扣除；这些亚微秒数值只宜看作量级参考，不应用来推断精确单函数周期。

当前结果来自 macOS arm64 的单机回环网络。没有模拟多机隔离、跨机延迟、Linux 调度、CPU 绑核或独占机器；后台负载和定时器合并会带来波动。实际 RPC、默认任务日志、客户端提交与查询开销都保留在实验中。

当前两种调度策略都会跳过满载 worker，因此不保证 least-loaded 总是胜过 round-robin。单调度线程、同步派发、空闲 10ms 轮询和客户端顺序提交是候选瓶颈；仅凭策略选择耗时无法确认某一个瓶颈，需要 profiling 才能下因果结论。此 benchmark 不改动调度实现，不混入性能优化前后的不同版本结果。
