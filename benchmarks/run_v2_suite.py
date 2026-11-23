"""Reproducible Release benchmark suite for MiniRay V2 innovations (Speculative Execution & Zero-Copy DAG ObjectStore)."""
import argparse
import csv
import io
import json
import os
import platform
import select
import socket
import statistics
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def loopback_env():
    env = dict(os.environ)
    for key in ('http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'all_proxy', 'ALL_PROXY'):
        env.pop(key, None)
    env['no_proxy'] = '127.0.0.1,localhost'
    env['NO_PROXY'] = '127.0.0.1,localhost'
    return env


def available_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def start(command, processes, logs, log_path):
    log = log_path.open('w')
    logs.append(log)
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=log, text=True, env=loopback_env())
    processes.append(process)
    ready, _, _ = select.select([process.stdout], [], [], 10)
    line = process.stdout.readline() if ready else ''
    if 'ready' not in line:
        raise RuntimeError(f'process not ready: {command}; see {log_path}')
    return process


def cleanup(processes, logs):
    for process in reversed(processes):
        if process.poll() is None:
            process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        if process.stdout:
            process.stdout.close()
    for log in logs:
        log.close()


def run_straggler_case(build_dir, log_dir, speculate, repeat, tasks=150):
    processes, logs = [], []
    address = f'127.0.0.1:{available_port()}'
    tag = f'{repeat}-straggler-speculate-{"on" if speculate else "off"}'
    try:
        start([str(build_dir / 'miniray-coordinator'), '--listen', address, '--policy', 'least-loaded',
               '--speculate', 'on' if speculate else 'off'],
              processes, logs, log_dir / f'{tag}.coordinator.log')
        for idx in range(4):
            endpoint = f'127.0.0.1:{available_port()}'
            start([str(build_dir / 'miniray-worker'), '--listen', endpoint, '--advertise', endpoint,
                   '--coordinator', address, '--capacity', '2', '--heartbeat-ms', '40'],
                  processes, logs, log_dir / f'{tag}.worker-{idx}.log')
        completed = subprocess.run(
            [str(build_dir / 'miniray-benchmark'), '--coordinator', address, '--mode', 'remote',
             '--workload', 'straggler', '--pattern', 'serial', '--tasks', str(tasks),
             '--slots', '8', '--warmup', '20'],
            capture_output=True, text=True, timeout=120, check=True, env=loopback_env()
        )
        row = next(csv.DictReader(io.StringIO(completed.stdout)))
        return {k: (v if k in {'mode', 'workload', 'pattern', 'latency_scope'} else float(v))
                for k, v in row.items()}
    finally:
        cleanup(processes, logs)


def run_dag_case(build_dir, log_dir, label, blob_bytes, waves, fanout, distinct_hosts, inline_threshold, repeat):
    processes, logs = [], []
    address = f'127.0.0.1:{available_port()}'
    tag = f'{repeat}-{label}'
    try:
        start([str(build_dir / 'miniray-coordinator'), '--listen', address, '--policy', 'eft'],
              processes, logs, log_dir / f'{tag}.coordinator.log')
        for idx in range(4):
            endpoint = f'127.0.0.1:{available_port()}'
            cmd = [str(build_dir / 'miniray-worker'), '--listen', endpoint, '--advertise', endpoint,
                   '--coordinator', address, '--capacity', '2', '--heartbeat-ms', '50',
                   '--inline-threshold', str(inline_threshold)]
            if distinct_hosts:
                cmd += ['--host-id', f'host-{idx}']
            start(cmd, processes, logs, log_dir / f'{tag}.worker-{idx}.log')
        completed = subprocess.run(
            [str(build_dir / 'miniray-benchmark'), '--coordinator', address, '--mode', 'remote',
             '--workload', 'dag-object', '--tasks', str(waves), '--warmup', '3',
             '--fanout', str(fanout), '--blob-bytes', str(blob_bytes)],
            capture_output=True, text=True, timeout=180, check=True, env=loopback_env()
        )
        row = next(csv.DictReader(io.StringIO(completed.stdout)))
        return {k: float(v) for k, v in row.items()}
    finally:
        cleanup(processes, logs)


def summarize_samples(samples, keys):
    out = {}
    for key in keys:
        vals = [s[key] for s in samples]
        q1, _, q3 = statistics.quantiles(vals, n=4, method='inclusive') if len(vals) > 1 else [vals[0]] * 3
        out[key] = {
            'median': statistics.median(vals),
            'min': min(vals),
            'max': max(vals),
            'iqr': q3 - q1,
        }
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-dir', type=Path, default=ROOT / 'build-release')
    parser.add_argument('--output-dir', type=Path, default=ROOT / 'benchmarks/results/latest')
    parser.add_argument('--repeats', type=int, default=3)
    args = parser.parse_args()

    build_dir = args.build_dir.resolve()
    output_dir = args.output_dir.resolve()
    log_dir = output_dir / 'logs'
    log_dir.mkdir(parents=True, exist_ok=True)

    straggler_off, straggler_on = [], []
    dag_1m_shm, dag_1m_inline, dag_64m_shm, dag_64m_pull = [], [], [], []

    for repeat in range(1, args.repeats + 1):
        print(f'[repeat {repeat}/{args.repeats}] straggler --speculate off ...', flush=True)
        straggler_off.append(run_straggler_case(build_dir, log_dir, False, repeat))
        print(f'[repeat {repeat}/{args.repeats}] straggler --speculate on ...', flush=True)
        straggler_on.append(run_straggler_case(build_dir, log_dir, True, repeat))
        print(f'[repeat {repeat}/{args.repeats}] dag 1MiB fanout=8 same-host shm ...', flush=True)
        dag_1m_shm.append(run_dag_case(build_dir, log_dir, 'dag-1m-shm', 1024 * 1024, 100, 8, False, 65536, repeat))
        print(f'[repeat {repeat}/{args.repeats}] dag 1MiB fanout=8 inline relay ...', flush=True)
        dag_1m_inline.append(run_dag_case(build_dir, log_dir, 'dag-1m-inline', 1024 * 1024, 100, 8, False, 16 * 1024 * 1024, repeat))
        print(f'[repeat {repeat}/{args.repeats}] dag 64MiB fanout=8 same-host shm ...', flush=True)
        dag_64m_shm.append(run_dag_case(build_dir, log_dir, 'dag-64m-shm', 64 * 1024 * 1024, 20, 8, False, 65536, repeat))
        print(f'[repeat {repeat}/{args.repeats}] dag 64MiB fanout=8 cross-host pull ...', flush=True)
        dag_64m_pull.append(run_dag_case(build_dir, log_dir, 'dag-64m-pull', 64 * 1024 * 1024, 20, 8, True, 65536, repeat))

    straggler_keys = ['throughput_per_s', 'p50_ms', 'p95_ms', 'p99_ms', 'attempts']
    dag_keys = ['waves_per_s', 'logical_gib_per_s', 'wave_p50_ms', 'wave_p95_ms', 'wave_p99_ms', 'zero_copy_hits', 'total_consumers']

    summary = {
        'metadata': {
            'platform': platform.platform(),
            'architecture': platform.machine(),
            'cpu': subprocess.check_output(['sysctl', '-n', 'machdep.cpu.brand_string'], text=True).strip() if sys.platform == 'darwin' else platform.processor(),
            'repeats': args.repeats,
            'finished_utc': datetime.now(timezone.utc).isoformat(),
        },
        'straggler': {
            'speculate_off': summarize_samples(straggler_off, straggler_keys),
            'speculate_on': summarize_samples(straggler_on, straggler_keys),
        },
        'dag_object_store': {
            '1mib_same_host_shm': summarize_samples(dag_1m_shm, dag_keys),
            '1mib_inline_protobuf': summarize_samples(dag_1m_inline, dag_keys),
            '64mib_same_host_shm': summarize_samples(dag_64m_shm, dag_keys),
            '64mib_cross_host_pull': summarize_samples(dag_64m_pull, dag_keys),
        },
    }

    (output_dir / 'v2_summary.json').write_text(json.dumps(summary, indent=2) + '\n')

    s_off = summary['straggler']['speculate_off']
    s_on = summary['straggler']['speculate_on']
    d1_shm = summary['dag_object_store']['1mib_same_host_shm']
    d1_inl = summary['dag_object_store']['1mib_inline_protobuf']
    d64_shm = summary['dag_object_store']['64mib_same_host_shm']
    d64_pull = summary['dag_object_store']['64mib_cross_host_pull']

    report = [
        '# MiniRay V2 Innovations Benchmark Report',
        '',
        f'- 硬件与系统：{summary["metadata"]["cpu"]} ({summary["metadata"]["platform"]})',
        f'- 重复次数：{args.repeats} 次取中位数（Release 构建，真实多进程 Coordinator + 4 Workers × 2 Slots）',
        f'- 生成时间（UTC）：{summary["metadata"]["finished_utc"]}',
        '',
        '## 1. 自适应推测执行 + 协作式取消（Milestone V2.1：4% 节点瞬态长尾毛刺 `jitter_sleep`）',
        '',
        '| 配置 | 吞吐 /s | P50 ms | P95 ms | P99 ms | 平均每批总尝试数 (150 项) |',
        '|---|---:|---:|---:|---:|---:|',
        f'| `--speculate off` | {s_off["throughput_per_s"]["median"]:.1f} | {s_off["p50_ms"]["median"]:.2f} | {s_off["p95_ms"]["median"]:.2f} | {s_off["p99_ms"]["median"]:.2f} | {s_off["attempts"]["median"]:.0f} |',
        f'| `--speculate on` | {s_on["throughput_per_s"]["median"]:.1f} | {s_on["p50_ms"]["median"]:.2f} | {s_on["p95_ms"]["median"]:.2f} | {s_on["p99_ms"]["median"]:.2f} | {s_on["attempts"]["median"]:.0f} |',
        '',
        '## 2. POSIX 共享内存零拷贝 ObjectStore + EFT 局部性 DAG（Milestone V2.2：1→8 Fan-out）',
        '',
        '| 负载与传输路径 | 波次吞吐 waves/s | 逻辑消费带宽 GiB/s | 单波 P50 ms | 单波 P95 ms | 单波 P99 ms | 零拷贝命中 |',
        '|---|---:|---:|---:|---:|---:|---:|',
        f'| 1 MiB × 8 扇出：同机零拷贝 POSIX `shm` | {d1_shm["waves_per_s"]["median"]:.1f} | {d1_shm["logical_gib_per_s"]["median"]:.2f} | {d1_shm["wave_p50_ms"]["median"]:.2f} | {d1_shm["wave_p95_ms"]["median"]:.2f} | {d1_shm["wave_p99_ms"]["median"]:.2f} | {d1_shm["zero_copy_hits"]["median"]:.0f}/{d1_shm["total_consumers"]["median"]:.0f} |',
        f'| 1 MiB × 8 扇出：Coordinator 内联传值基线 | {d1_inl["waves_per_s"]["median"]:.1f} | {d1_inl["logical_gib_per_s"]["median"]:.2f} | {d1_inl["wave_p50_ms"]["median"]:.2f} | {d1_inl["wave_p95_ms"]["median"]:.2f} | {d1_inl["wave_p99_ms"]["median"]:.2f} | {d1_inl["zero_copy_hits"]["median"]:.0f}/{d1_inl["total_consumers"]["median"]:.0f} |',
        f'| 64 MiB × 8 扇出：同机零拷贝 POSIX `shm` | {d64_shm["waves_per_s"]["median"]:.2f} | {d64_shm["logical_gib_per_s"]["median"]:.2f} | {d64_shm["wave_p50_ms"]["median"]:.2f} | {d64_shm["wave_p95_ms"]["median"]:.2f} | {d64_shm["wave_p99_ms"]["median"]:.2f} | {d64_shm["zero_copy_hits"]["median"]:.0f}/{d64_shm["total_consumers"]["median"]:.0f} |',
        f'| 64 MiB × 8 扇出：跨主机流式 `PullObject` + `Singleflight` | {d64_pull["waves_per_s"]["median"]:.2f} | {d64_pull["logical_gib_per_s"]["median"]:.2f} | {d64_pull["wave_p50_ms"]["median"]:.2f} | {d64_pull["wave_p95_ms"]["median"]:.2f} | {d64_pull["wave_p99_ms"]["median"]:.2f} | {d64_pull["zero_copy_hits"]["median"]:.0f}/{d64_pull["total_consumers"]["median"]:.0f} |',
        '',
    ]
    (output_dir / 'v2_report.md').write_text('\n'.join(report))
    print(f'Results: {output_dir / "v2_report.md"}', flush=True)


if __name__ == '__main__':
    main()
