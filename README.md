# MiniRay

A C++20 distributed task and DAG runtime built on gRPC, with a central coordinator, independent workers, a zero-copy POSIX shared-memory object store, asynchronous batch dispatch, and fault-tolerant execution. The baseline MVP follows [PROPOSAL.md](PROPOSAL.md) and [TUTORIAL.md](TUTORIAL.md); the V2 architecture is documented in [docs/v2-architecture.md](docs/v2-architecture.md).

The runtime implements:

- **Event-driven async batch dispatch** (`DispatchBatch` via gRPC Callback API) and **3-tier priority scheduling** (`recovery_queue_` > EWMA `fast_queue_` > `normal_queue_` with `50 ms` anti-starvation aging).
- **Scheduling policies**: `round-robin`, normalized `least-loaded`, and **Earliest Finish Time (`eft`)** data-locality-aware scheduling across local worker cache, same-host POSIX shared memory, and cross-host network pulls.
- **Adaptive speculative execution** (`--speculate on`) with per-task-type runtime P95 hedging and cooperative `CancelTask` slot reclamation.
- **Dynamic task DAGs** (`TASK_STATUS_WAITING_DEPS` dependency unlocking, transitive failure cascading, consumer reference pinning, and TTL eviction).
- **Zero-copy POSIX `ObjectStore`** (`shm_open` + `mmap`, 64-byte cache-line-aligned `ShmHeader` with IEEE CRC32 and atomic seal, `flock`-guarded orphan segment reaping) for outputs `>= 64 KiB` (up to `256 MiB`), with **1 MiB chunked `PullObject` streaming** and **`Singleflight` request coalescing** across logical hosts.
- **Automatic object lineage reconstruction** when all workers holding a required out-of-band `ObjectRef` expire, alongside bounded admission backpressure, heartbeat-based failure detection, bounded retries, attempt-level result fencing, submission deduplication, and graceful worker drain.

## Build and test

Linux (Ubuntu 24.04):

```sh
sudo apt-get update
sudo apt-get install -y g++ cmake libgrpc++-dev libprotobuf-dev protobuf-compiler protobuf-compiler-grpc libgtest-dev python3
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j4
ctest --test-dir build --output-on-failure
```

macOS:

```sh
brew install cmake grpc googletest
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DCMAKE_PREFIX_PATH="$(brew --prefix)"
cmake --build build -j4
ctest --test-dir build --output-on-failure
```

CMake uses system packages and generates Protobuf/gRPC code in the build directory; it does not download C++ source dependencies. To build without tests, use `-DBUILD_TESTING=OFF`.

## Run

Start the coordinator and two workers in separate terminals:

```sh
./build/miniray-coordinator --listen 0.0.0.0:50051 --policy eft --max-pending 1024 --heartbeat-timeout-ms 2500 --batch-size 16 --speculate off
```

```sh
./build/miniray-worker --coordinator localhost:50051 --listen 0.0.0.0:50052 --advertise localhost:50052 --capacity 2 --inline-threshold 65536
```

```sh
./build/miniray-worker --coordinator localhost:50051 --listen 0.0.0.0:50053 --advertise localhost:50053 --capacity 4 --inline-threshold 65536
```

Submit example tasks:

```sh
./build/distributed-compute localhost:50051 fibonacci 40
./build/distributed-compute localhost:50051 echo hello
./build/distributed-compute localhost:50051 sleep 1000
```

The expected Fibonacci result is `102334155`. Output includes the task ID, final attempt, and worker ID. `--advertise` must be a worker address reachable by the coordinator and peer workers; use a routable address instead of `localhost` when running across machines. Coordinator policies are `round-robin`, `least-loaded`, and `eft` (`least-loaded` also enables EFT locality when tasks carry `ObjectRef` dependencies). Pass distinct `--host-id` values to workers to force cross-host `PullObject` streaming on a single machine. The worker's `--heartbeat-ms` defaults to `500` and should be well below the coordinator timeout.

Built-in tasks are `echo` (byte string), `fibonacci` (decimal integer `0–93`), `sleep` (cancellable sleep `0–60000` ms), `jitter_sleep` (cancellable sleep with 4% transient worker straggler injection for hedging benchmarks), `blob_produce` (deterministic byte buffer up to `128 MiB`), and `blob_reduce` (zero-copy page-touching DAG consumer). Invalid inputs and unknown task types fail immediately; other execution errors are retried according to the task configuration.

## C++ SDK, DAGs, and zero-copy objects

```cpp
#include "miniray/client.hpp"

miniray::MiniRayClient runtime("localhost:50051");
auto upstream = runtime.Submit("blob_produce", "67108864"); // 64 MiB stored in POSIX shm
auto downstream = runtime.SubmitDag("blob_reduce", "0", {upstream});

// Zero-copy read-only mmap view on the same host (falls back to PullObject remotely):
miniray::ObjectView view = upstream.GetView(std::chrono::seconds(30));
std::string summary = downstream.Get(std::chrono::seconds(30));
```

`Submit(type, payload, max_retries=2, retryable=true, task_id="")` and `SubmitDag(type, payload, dependencies, ...)` return a `TaskFuture` immediately. `Wait()` returns the structured `GetTaskResultResponse`; `GetView()` resolves either inline bytes or a read-only `ObjectView` (zero-copy `mmap` when co-located on the same `host_id`); `Get()` materializes an owned `std::string` and throws on failure. A wait timeout does not cancel the task.

After a transport failure, the SDK retries submission up to twice with the same ID and includes that ID in the error message. Supplying an identical application `task_id` and spec is idempotent; reusing an ID with different parameters returns `ALREADY_EXISTS`. At admission capacity, the coordinator returns `RESOURCE_EXHAUSTED`.

Register stateless, cancellable, or DAG-aware handlers before starting a worker:

```cpp
miniray::TaskRegistry registry;
registry.RegisterTask("stateless", [](const std::string& payload) {
  return payload + "!";
});
registry.RegisterCancellableTask("cancellable", [](const std::string& payload, const miniray::CancellationToken& token) {
  if (token.IsCancelled()) throw miniray::TaskCancelledError();
  return payload;
});
registry.RegisterDagTask("dag_op", [](const std::string& payload, const miniray::TaskContext& ctx) {
  // ctx.args provides zero-copy ObjectView spans for upstream dependencies
  return std::to_string(ctx.args.size()) + ":" + payload;
});
```

Handlers run outside the registry lock and must be thread-safe. Inline request payloads are capped at `1 MiB` (up to 64 upstream dependencies per task); outputs `>= --inline-threshold` (default `64 KiB`, up to `256 MiB`) are automatically sealed into `ObjectStore` and passed by `ObjectRef`.

## Docker Compose

```sh
docker compose -f docker/docker-compose.yml up --build -d
docker compose -f docker/docker-compose.yml exec coordinator ./build/distributed-compute coordinator:50051 fibonacci 40
docker compose -f docker/docker-compose.yml down
```

The image build runs the full test suite. Workers run as separate processes with different capacities. To change the policy, use `MINIRAY_POLICY=round-robin docker compose -f docker/docker-compose.yml up -d`.

## Validation and performance

```sh
# Build memory and concurrency checks separately; do not combine them.
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DMINIRAY_SANITIZER=address
cmake --build build-asan -j4
ctest --test-dir build-asan --output-on-failure

cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DMINIRAY_SANITIZER=thread
cmake --build build-tsan -j4
ctest --test-dir build-tsan --output-on-failure
```

Run the reproducible Release benchmark suites (both scripts automatically isolate loopback gRPC traffic from `http_proxy` / `https_proxy`):

```sh
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j8

# 42-case core regression suite (micro-task overhead, 1..8 worker scaling, overload, SIGKILL recovery):
python3 benchmarks/run_suite.py --build-dir build-release --tasks 400 --repeats 5 --output-dir benchmarks/results/reproduction

# V2 innovation suite (adaptive speculative execution + 1 MiB / 64 MiB fan-out zero-copy DAG):
python3 benchmarks/run_v2_suite.py --build-dir build-release --repeats 3 --output-dir benchmarks/results/reproduction
```

Measured Release highlights on Apple M5 (macOS arm64, independent Coordinator + Worker processes over loopback TCP):

- **Micro-task serial latency (`remote / tiny / serial`)**: `296.0 μs` p50 / `368.9 μs` p99 at `3,337.3 tasks/s` (down from `14,305.7 μs` p50 in V1; raw `NOT_FOUND` gRPC roundtrip baseline is `134.7 μs` p50).
- **Homogeneous scaling (`uniform` 5 ms tasks, 8 workers / 16 slots)**: `2,464.5 tasks/s` (`91.2%` slot utilization, `8.24×` over 1 worker), `89.0 ms` p99.
- **In-flight `SIGKILL` recovery (`heartbeat=50 ms`, `timeout=500 ms`)**: `8.7 ms` queue wait from failure detection to reassignment via `recovery_queue_` (`747.9 ms` end-to-end task recovery vs. `4,216.0 ms` in V1).
- **Adaptive speculative hedging (`4%` 250 ms worker stragglers, `--speculate on`)**: reduces p99 tail latency from `252.22 ms` to `24.42 ms` (`-90.3%`) with `+4.0%` extra attempts (`156` attempts per `150` tasks).
- **`64 MiB × 8` fan-out DAG (`10.0 GiB` logical consumption per 20 waves)**: `3.69 GiB/s` logical bandwidth (`134.42 ms` wave p50 / `143.57 ms` p99, `100%` zero-copy `mmap` hits) vs. `0.60 GiB/s` (`890.53 ms` p99) over cross-host `PullObject` streaming (`6.19×` speedup).

See [docs/v2-architecture.md](docs/v2-architecture.md), [docs/performance.md](docs/performance.md), [docs/benchmark-results.md](docs/benchmark-results.md), and [benchmarks/results/2026-10-31/report.md](benchmarks/results/2026-10-31/report.md) for historical baseline methodology and measurement boundaries.

## Semantics and limitations

- **At-least-once execution with attempt fencing**: retries and speculative hedging require idempotent handlers; external side effects are not guaranteed to occur exactly once, while coordinator result state is strictly fenced by `(task_id, attempt_id, worker_id)`.
- **Bounded admission**: `--max-pending` bounds all nonterminal tasks (`WAITING_DEPS`, `PENDING`, `DISPATCHED`, and `RUNNING`), reserving capacity for in-flight tasks or upstream lineage reconstructions to re-enter queues without dropping accepted work.
- **One-shot worker incarnations**: worker IDs are generated on startup; an expired incarnation cannot rejoin and must restart. On `SIGTERM`/`SIGINT`, a worker stops admission, drains accepted handlers, reports results, unlinks local POSIX shared-memory objects, and exits; `SIGKILL`-orphaned shared-memory segments are swept via `/tmp/miniray_shm_manifest_<uid>.log` on subsequent startups.
- **Cooperative cancellation**: `CancelTask` signals a `CancellationToken`; handlers that do not poll the token run until natural completion and release their worker slot upon return.
- **Single-coordinator memory state**: the coordinator is a single point of failure with in-memory task and object metadata. Multi-dimensional resource vectors (CPU/GPU/memory scheduling), stateful actors, coordinator HA/persistence, dynamic code deployment, TLS/authentication, and multi-host physical network benchmarks are not implemented.

Design details are in [docs/v2-architecture.md](docs/v2-architecture.md), [docs/architecture.md](docs/architecture.md), and [docs/fault_tolerance.md](docs/fault_tolerance.md).
