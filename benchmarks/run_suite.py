"""Reproducible Release experiments. Runs one batch at a time and retains raw samples."""
import argparse
import csv
import hashlib
import io
import json
import math
import platform
import random
import select
import socket
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def available_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def start(command, processes, logs, log_path):
    log = log_path.open('w')
    logs.append(log)
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=log, text=True)
    processes.append(process)
    ready, _, _ = select.select([process.stdout], [], [], 10)
    line = process.stdout.readline() if ready else ''
    if 'ready' not in line:
        raise RuntimeError(f'process not ready: {command}; see {log_path}')
    return process, line


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


def cases(tasks):
    result = []
    def add(scenario, mode, workload, capacities, policy='least-loaded', pattern='burst', count=tasks):
        key = f'{scenario}/{policy}/{mode}/{workload}/{"-".join(map(str, capacities)) or "none"}'
        result.append(dict(case_id=key, scenario=scenario, mode=mode, workload=workload,
                           capacities=capacities, policy=policy, pattern=pattern, tasks=count))
    for mode in ['local', 'thread-pool', 'rpc', 'remote']:
        add('overhead', mode, 'tiny', [2, 2] if mode == 'remote' else [], pattern='serial')
    for mode in ['local', 'thread-pool']:
        for workload in ['uniform', 'mixed']:
            add('local-work', mode, workload, [], pattern='serial' if mode == 'local' else 'burst')
    for policy in ['round-robin', 'least-loaded']:
        for workers in [1, 2, 4, 8]:
            for workload in ['tiny', 'uniform', 'mixed']:
                add('scaling', 'remote', workload, [2] * workers, policy)
        for workload in ['tiny', 'uniform', 'mixed']:
            add('heterogeneous', 'remote', workload, [1, 2, 4], policy)
        add('overload', 'remote', 'failure', [2], policy)
        add('failure', 'remote', 'failure', [2, 2], policy, count=min(32, tasks))
    return result


def validate(row, case):
    if row['tasks'] != case['tasks'] or row['accepted'] + row['rejected'] != row['tasks']:
        raise RuntimeError(f'submission accounting mismatch: {row}')
    if row['completed'] + row['failed'] != row['accepted'] or row['failed']:
        raise RuntimeError(f'completion accounting mismatch: {row}')
    if not all(math.isfinite(v) for v in row.values() if isinstance(v, (int, float))):
        raise RuntimeError('non-finite metric')
    if not 0 <= row['p50_ms'] <= row['p95_ms'] <= row['p99_ms'] or row['elapsed_s'] <= 0:
        raise RuntimeError('invalid latency/elapsed time')
    if case['scenario'] == 'overload':
        if not row['rejected'] or not row['completed']:
            raise RuntimeError('overload did not exercise both acceptance and rejection')
    elif row['rejected'] or row['completed'] != row['tasks']:
        raise RuntimeError('healthy batch lost or rejected tasks')
    if case['scenario'] == 'failure':
        if not row['recovered_tasks'] or row['attempts'] <= row['accepted']:
            raise RuntimeError('injection did not exercise a retry')
        if not 0 <= row['detection_observed_ms'] <= row['reassignment_observed_ms'] <= row['recovery_observed_ms']:
            raise RuntimeError('invalid recovery observations')
    elif case['mode'] == 'remote' and row['attempts'] != row['completed']:
        raise RuntimeError('unexpected retries in a healthy experiment')


def measure(case, repeat, args):
    processes, logs = [], []
    address = f'127.0.0.1:{available_port()}'
    root = args.build_dir
    log_prefix = args.output_dir / 'logs' / (f'{repeat}-' + case['case_id'].replace('/', '_'))
    try:
        victim = None
        if case['mode'] in ['remote', 'rpc']:
            command = [str(root / 'miniray-coordinator'), '--listen', address, '--policy', case['policy'],
                       '--max-pending', '8' if case['scenario'] == 'overload' else '1024']
            if case['scenario'] == 'failure':
                command += ['--heartbeat-timeout-ms', '500']
            start(command, processes, logs, log_prefix.with_suffix('.coordinator.log'))
            for index, capacity in enumerate(case['capacities']):
                endpoint = f'127.0.0.1:{available_port()}'
                process, line = start([str(root / 'miniray-worker'), '--listen', endpoint, '--advertise', endpoint,
                                       '--coordinator', address, '--capacity', str(capacity), '--heartbeat-ms', '50'],
                                      processes, logs, log_prefix.with_suffix(f'.worker-{index}.log'))
                if index == 0:
                    victim = (process, line.split()[2])
        slots = sum(case['capacities']) or 4
        command = [str(root / 'miniray-benchmark'), '--coordinator', address, '--tasks', str(case['tasks']),
                   '--slots', str(slots), '--mode', case['mode'], '--pattern', case['pattern'],
                   '--workload', case['workload'], '--warmup', str(2 if case['scenario'] in ['failure', 'overload'] else args.warmup)]
        if case['scenario'] == 'failure':
            command += ['--kill-worker-pid', str(victim[0].pid), '--kill-worker-id', victim[1]]
        completed = subprocess.run(command, capture_output=True, text=True, timeout=120)
        expected_code = 2 if case['scenario'] == 'overload' else 0
        if completed.returncode != expected_code:
            raise RuntimeError(f'benchmark exit {completed.returncode}: {completed.stderr}; see {log_prefix}.*.log')
        rows = list(csv.DictReader(io.StringIO(completed.stdout)))
        if len(rows) != 1:
            raise RuntimeError(f'expected one CSV row: {completed.stdout}')
        text_fields = {'mode', 'workload', 'pattern', 'latency_scope'}
        int_fields = {'tasks', 'warmup', 'connection_warmup', 'accepted', 'rejected', 'completed', 'failed', 'attempts', 'recovered_tasks'}
        row = {k: (v if k in text_fields else int(v) if k in int_fields else float(v)) for k, v in rows[0].items()}
        validate(row, case)
        for process in processes:
            if case['scenario'] == 'failure' and process is victim[0]:
                if process.wait(timeout=5) != -9:
                    raise RuntimeError('fault victim did not exit by SIGKILL')
            elif process.poll() is not None:
                raise RuntimeError('cluster process exited unexpectedly')
        return {**case, 'repeat': repeat, **row, 'slots': slots,
                'finished_utc': datetime.now(timezone.utc).isoformat(), 'command': command}
    finally:
        cleanup(processes, logs)


def command_output(command):
    try:
        return subprocess.check_output(command, text=True, stderr=subprocess.STDOUT, timeout=10).strip()
    except (OSError, subprocess.SubprocessError) as error:
        return f'unavailable: {error}'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-dir', type=Path, default=ROOT / 'build-release')
    parser.add_argument('--output-dir', type=Path, default=ROOT / 'benchmarks/results/latest')
    parser.add_argument('--tasks', type=int, default=400)
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--warmup', type=int, default=16)
    args = parser.parse_args()
    if not 20 <= args.tasks <= 1024 or not 1 <= args.repeats <= 100 or not 1 <= args.warmup <= 1000:
        parser.error('require tasks 20..1024, repeats 1..100, warmup 1..1000')
    args.build_dir = args.build_dir.resolve()
    args.output_dir = args.output_dir.resolve()
    cache = (args.build_dir / 'CMakeCache.txt').read_text()
    if 'CMAKE_BUILD_TYPE:STRING=Release' not in cache or 'MINIRAY_SANITIZER:STRING=\n' not in cache:
        parser.error('use a Release build without sanitizers')
    (args.output_dir / 'logs').mkdir(parents=True, exist_ok=True)
    inputs = [*ROOT.glob('src/*.cpp'), *ROOT.glob('include/miniray/*.hpp'), *ROOT.glob('proto/*.proto'),
              *ROOT.glob('benchmarks/*.cpp'), *ROOT.glob('benchmarks/*.py'), ROOT / 'CMakeLists.txt']
    import re
    grpc_dir = re.search(r'^gRPC_DIR:PATH=(.+)$', cache, re.MULTILINE)
    grpc_version = 'unavailable'
    if grpc_dir:
        version_file = Path(grpc_dir.group(1)) / 'gRPCConfigVersion.cmake'
        if version_file.exists():
            match = re.search(r'set\(PACKAGE_VERSION "([^"]+)"\)', version_file.read_text())
            if match:
                grpc_version = match.group(1)
    metadata = dict(platform=platform.platform(), architecture=platform.machine(),
                    started_utc=datetime.now(timezone.utc).isoformat(), tasks=args.tasks, repeats=args.repeats,
                    warmup=args.warmup, order_seed=20261031,
                    compiler=command_output(['c++', '--version']), cmake=command_output(['cmake', '--version']),
                    protobuf=command_output(['protoc', '--version']),
                    grpc=grpc_version,
                    cpu=command_output(['sysctl', '-n', 'machdep.cpu.brand_string']) if sys.platform == 'darwin' else command_output(['lscpu']),
                    logical_cpus=__import__('os').cpu_count(),
                    source_sha256={str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(inputs)},
                    benchmark_sha256=hashlib.sha256((args.build_dir / 'miniray-benchmark').read_bytes()).hexdigest())
    data = {'metadata': metadata, 'samples': []}
    total = len(cases(args.tasks)) * args.repeats
    for repeat in range(1, args.repeats + 1):
        jobs = cases(args.tasks)
        random.Random(20261031 + repeat).shuffle(jobs)
        for case in jobs:
            row = measure(case, repeat, args)
            data['samples'].append(row)
            temporary = args.output_dir / 'raw.json.tmp'
            temporary.write_text(json.dumps(data, indent=2) + '\n')
            temporary.replace(args.output_dir / 'raw.json')
            print(f'[{len(data["samples"])}/{total}] {case["case_id"]} repeat={repeat} completed={row["completed"]} rejected={row["rejected"]}', flush=True)
    from summarize import summarize
    summarize(args.output_dir)
    print(f'Results: {args.output_dir / "report.md"}', flush=True)


if __name__ == '__main__':
    main()
