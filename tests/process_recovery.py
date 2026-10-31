"""Kill an executing worker, then verify retry completion on a fresh worker process."""
import os
import queue
import signal
import socket
import subprocess
import sys
import threading
import time


def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def start(args):
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    lines = queue.Queue()
    history = []
    def collect():
        for line in proc.stdout:
            history.append(line.strip())
            lines.put(line.strip())
    threading.Thread(target=collect, daemon=True).start()
    processes.append((proc, history))
    return proc, lines


def wait_line(proc, lines, pattern, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            line = lines.get(timeout=0.1)
            if pattern in line:
                return line
        except queue.Empty:
            if proc.poll() is not None:
                raise RuntimeError(f'process exited {proc.returncode} before {pattern}')
    raise RuntimeError(f'timed out waiting for {pattern}')


processes = []
try:
    coordinator, worker, client = sys.argv[1:]
    address = f'127.0.0.1:{port()}'
    cp, cq = start([coordinator, '--listen', address, '--heartbeat-timeout-ms', '500'])
    wait_line(cp, cq, 'Coordinator ready')
    def launch_worker():
        endpoint = f'127.0.0.1:{port()}'
        return start([worker, '--coordinator', address, '--listen', endpoint,
                      '--advertise', endpoint, '--capacity', '1', '--heartbeat-ms', '50'])
    wp, wq = launch_worker()
    wait_line(wp, wq, 'Worker ready')
    task_id = f'crash-test-{os.getpid()}'
    cli, cli_queue = start([client, address, 'sleep', '1000', task_id])
    wait_line(wp, wq, f'Executing task={task_id} attempt=0')
    started = time.monotonic()
    wp.kill()
    wp.wait(timeout=5)
    replacement, replacement_queue = launch_worker()
    wait_line(replacement, replacement_queue, 'Worker ready')
    line = wait_line(cli, cli_queue, 'result=1000', timeout=10)
    cli.wait(timeout=5)
    if cli.returncode != 0 or 'attempt=1' not in line:
        raise RuntimeError(f'incorrect recovery result: {line}')
    recovery_ms = (time.monotonic() - started) * 1000
    # SIGTERM drains an accepted task rather than dropping it.
    drain_id = f'drain-test-{os.getpid()}'
    drain, drain_queue = start([client, address, 'sleep', '300', drain_id])
    wait_line(replacement, replacement_queue, f'Executing task={drain_id} attempt=0')
    replacement.send_signal(signal.SIGTERM)
    drain_result = wait_line(drain, drain_queue, 'result=300')
    drain.wait(timeout=5)
    replacement.wait(timeout=5)
    if drain.returncode or replacement.returncode or 'attempt=0' not in drain_result:
        raise RuntimeError('graceful shutdown lost accepted work')
    print(f'worker crash recovered and graceful drain passed; recovery_ms={recovery_ms:.1f}')
finally:
    for proc, history in reversed(processes):
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        if sys.exc_info()[0]:
            print('\n'.join(history), file=sys.stderr)
