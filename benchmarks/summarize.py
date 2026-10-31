"""Regenerate summary.json and report.md from a complete raw.json without rerunning experiments."""
import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path

METRICS = ['throughput_per_s', 'elapsed_s', 'p50_ms', 'p95_ms', 'p99_ms', 'queue_p99_ms',
           'scheduling_p99_ms', 'slot_utilization', 'accepted', 'rejected', 'completed', 'failed',
           'attempts', 'recovered_tasks', 'detection_observed_ms', 'reassignment_observed_ms', 'recovery_observed_ms']


def distribution(values):
    q1, _, q3 = statistics.quantiles(values, n=4, method='inclusive') if len(values) > 1 else [values[0]] * 3
    return dict(median=statistics.median(values), min=min(values), max=max(values), iqr=q3 - q1,
                mean=statistics.mean(values), stdev=statistics.stdev(values) if len(values) > 1 else 0)


def summarize(directory):
    from run_suite import cases, validate
    directory = Path(directory)
    data = json.loads((directory / 'raw.json').read_text())
    metadata = data['metadata']
    expected = {case['case_id']: case for case in cases(metadata['tasks'])}
    groups = defaultdict(list)
    for sample in data['samples']:
        validate(sample, expected[sample['case_id']])
        groups[sample['case_id']].append(sample)
    if groups.keys() != expected.keys():
        raise ValueError('missing or unexpected benchmark cases')
    output = []
    for key, samples in sorted(groups.items()):
        if len(samples) != metadata['repeats'] or {x['repeat'] for x in samples} != set(range(1, metadata['repeats'] + 1)):
            raise ValueError(f'incomplete or duplicate repetitions: {key}')
        first = samples[0]
        output.append({**{k: first[k] for k in ['case_id', 'scenario', 'mode', 'workload', 'capacities', 'slots', 'policy', 'latency_scope', 'pattern', 'tasks']},
                       'n': len(samples), 'metrics': {metric: distribution([sample[metric] for sample in samples]) for metric in METRICS}})
    (directory / 'summary.json').write_text(json.dumps({'metadata': metadata, 'cases': output}, indent=2) + '\n')
    lines = ['# MiniRay benchmark report', '',
             f'完成 {len(output)} 个配置，每组 {metadata["repeats"]} 次，共 {len(data["samples"])} 个有效批次。常规批次 {metadata["tasks"]} 项；故障批次至多 32 项。', '',
             f'环境：{metadata["cpu"]}；{metadata["platform"]}；逻辑 CPU {metadata["logical_cpus"]}。',
             f'构建：Release，无 sanitizer；{metadata["compiler"].splitlines()[0]}；{metadata["protobuf"]}；gRPC {metadata["grpc"]}。', '',
             f'开始时间（UTC）：{metadata["started_utc"]}。原始数据见 [raw.json](raw.json)，完整统计见 [summary.json](summary.json)。', '',
             '每批使用新进程；启动后先执行连接预热（remote 为总槽位的 2 倍、每项 sleep 20ms），再执行工作负载预热。预热不计入批次任务和时间。每次重复按固定种子打乱配置顺序，实验串行运行。以下数值为各批指标的中位数，IQR 为这些批次指标的四分位距；p99 是每批 p99 的中位数，不是合并所有任务后的 p99。', '',
             '## 微任务开销', '',
             'tiny/serial，每次提交后等待结果才提交下一项。RPC 基线是 GetTaskResult 对未知 ID 返回 NOT_FOUND 的往返，不经过调度；local 包括注册表查找；thread-pool 包括固定线程池投递和等待；remote 包括提交、调度、执行、结果上报和查询。单位为微秒。', '',
             '| 模式 | p50 μs | p99 μs | p99 IQR μs | 吞吐 /s |', '|---|---:|---:|---:|---:|']
    def med(case, metric):
        return case['metrics'][metric]['median']
    for case in sorted((c for c in output if c['scenario'] == 'overhead'), key=lambda c: ['local', 'thread-pool', 'rpc', 'remote'].index(c['mode'])):
        lines.append(f'| {case["mode"]} | {1000*med(case,"p50_ms"):.3f} | {1000*med(case,"p99_ms"):.3f} | {1000*case["metrics"]["p99_ms"]["iqr"]:.3f} | {med(case,"throughput_per_s"):.1f} |')
    lines += ['', '## 同构扩展性与调度策略', '',
              '每 worker 固定 2 槽位，worker 数为 1/2/4/8，总槽位为 2/4/8/16；固定总任务数。客户端连续同步提交 RPC 形成突发积压，并不表示所有任务在同一时刻到达。延迟从协调器接纳算起。', '']
    for workload in ['tiny', 'uniform', 'mixed']:
        lines += ['', f'### {workload}', '', '| 策略 | workers | slots | 吞吐 /s | 吞吐 IQR | p99 ms | p99 IQR | queue p99 ms | slot 利用率 | 1-worker 加速比 |', '|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
        group = sorted((c for c in output if c['scenario'] == 'scaling' and c['workload'] == workload), key=lambda c: (c['policy'], len(c['capacities'])))
        for case in group:
            base = next(c for c in group if c['policy'] == case['policy'] and len(c['capacities']) == 1)
            speedup = med(case, 'throughput_per_s') / med(base, 'throughput_per_s')
            lines.append(f'| {case["policy"]} | {len(case["capacities"])} | {case["slots"]} | {med(case,"throughput_per_s"):.1f} | {case["metrics"]["throughput_per_s"]["iqr"]:.1f} | {med(case,"p99_ms"):.2f} | {case["metrics"]["p99_ms"]["iqr"]:.2f} | {med(case,"queue_p99_ms"):.2f} | {100*med(case,"slot_utilization"):.1f}% | {speedup:.2f}× |')
    lines += ['', '## 异构容量', '', '容量为 1/2/4，总共 7 槽位。两种策略都跳过饱和 worker，所以不能把 round-robin 解释为向满载 worker 继续堆积任务的静态分配策略。', '',
              '| 工作负载 | 策略 | 吞吐 /s | 吞吐 IQR | p99 ms | p99 IQR | scheduling p99 μs |', '|---|---|---:|---:|---:|---:|---:|']
    for case in (c for c in output if c['scenario'] == 'heterogeneous'):
        lines.append(f'| {case["workload"]} | {case["policy"]} | {med(case,"throughput_per_s"):.1f} | {case["metrics"]["throughput_per_s"]["iqr"]:.1f} | {med(case,"p99_ms"):.2f} | {case["metrics"]["p99_ms"]["iqr"]:.2f} | {1000*med(case,"scheduling_p99_ms"):.3f} |')
    lines += ['', '## 本地计算基线', '', '| 模式 | workload | slots | 吞吐 /s | p99 ms |', '|---|---|---:|---:|---:|']
    for case in (c for c in output if c['scenario'] == 'local-work'):
        lines.append(f'| {case["mode"]} | {case["workload"]} | {1 if case["mode"]=="local" else case["slots"]} | {med(case,"throughput_per_s"):.1f} | {med(case,"p99_ms"):.2f} |')
    lines += ['', '## 过载与背压', '', '1 worker、2 槽位、接纳上限 8、每项 sleep 250ms。提交期间可能已有任务完成并释放预算，所以总 accepted 可以超过 8；该指标不等于瞬时队列长度。拒绝返回 RESOURCE_EXHAUSTED，benchmark 保留数据并以状态 2 退出；suite 将其作为预期结果。', '',
              '| 策略 | 提交/批 | 接纳中位数 | 拒绝中位数 | 拒绝范围 | 接纳任务成功率 |', '|---|---:|---:|---:|---:|---:|']
    for case in (c for c in output if c['scenario'] == 'overload'):
        rejected = case['metrics']['rejected']
        lines.append(f'| {case["policy"]} | {case["tasks"]} | {med(case,"accepted"):g} | {med(case,"rejected"):g} | {rejected["min"]}–{rejected["max"]} | 100% |')
    lines += ['', '## 执行中故障恢复', '',
              '2 workers 各 2 槽位，每项 sleep 250ms。确认指定 worker 有 RUNNING 任务后，向 suite 持有的该子进程发送 SIGKILL；心跳间隔 50ms、超时 500ms，剩余 worker 继续处理同批任务。只对选中的一个故障任务每约 2ms 查询一次。', '',
              '检测列表示客户端首次观察到 attempt 递增的时间；派发列表示首次观察到新 attempt 离开 PENDING（可能为 DISPATCHED 或 RUNNING）；恢复列表示首次观察到选中任务 COMPLETED。均从 kill 返回之后计时，包含 RPC 与轮询误差，是状态变化的观测上界，不是服务端精确事件时间。恢复列针对选中任务，整批墙钟时间单独列出。', '',
              '| 策略 | 任务/批 | 成功率 | 重试任务中位数 | 检测 ms | 新代派发 ms | 选中任务恢复 ms | 恢复 IQR ms | 整批 s |', '|---|---:|---:|---:|---:|---:|---:|---:|---:|']
    for case in (c for c in output if c['scenario'] == 'failure'):
        lines.append(f'| {case["policy"]} | {case["tasks"]} | 100% | {med(case,"recovered_tasks"):g} | {med(case,"detection_observed_ms"):.2f} | {med(case,"reassignment_observed_ms"):.2f} | {med(case,"recovery_observed_ms"):.2f} | {case["metrics"]["recovery_observed_ms"]["iqr"]:.2f} | {med(case,"elapsed_s"):.2f} |')
    lines += ['', '## 测量边界', '',
              '- uniform 为 sleep 5ms；mixed 为每 20 项中的第 1 项 sleep 100ms，其余 sleep 5ms，固定顺序，未覆盖其他随机分布；tiny 为 echo 单字节。sleep 是受控服务时间，不能代表 CPU 密集型计算。',
              '- 单机回环网络、真实独立进程；未测跨机器网络、Linux、CPU 绑核和独占主机。系统后台负载与 macOS timer 合并会影响结果。',
              '- 常规批次的工作负载预热数见原始元数据；过载和故障为 2 项，另有连接预热。运行时默认任务日志保持开启；单个客户端串行提交与取回结果也可能限制 tiny 吞吐。',
              '- burst 远程延迟是协调器接纳至终态，排除按顺序收集结果的延迟；serial 微任务是客户端提交至取回的端到端时间。两种口径不直接相减。',
              '- queue_p99 包括各 attempt 等待队列的累计时间；scheduling_p99 仅含策略选择，不能用于推断锁、序列化或网络开销。',
              '- slot 利用率由已上报执行时长 /（批次墙钟 × 总槽位）估算；不等于 CPU 利用率。故障丢失的执行时长未计入，不对故障批次使用此指标。',
              '- 所有健康和故障批次均校验接纳/完成守恒；过载拒绝不会计入完成率或成功吞吐。程序异常、RPC 错误和非有限指标使 suite 失败，并保留已完成批次。',
              f'- {metadata["repeats"]} 次重复用于描述这台机器上的波动，不提供跨硬件的性能保证；结果不支持宣称某种调度策略普遍更快。', '']
    (directory / 'report.md').write_text('\n'.join(lines))
    return output


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    summarize(parser.parse_args().directory)
