# MiniRay

A C++20 distributed task runtime built on gRPC, with a central coordinator, independent workers, registered task handlers, asynchronous submission, and result retrieval. The implementation follows the MVP in [PROPOSAL.md](PROPOSAL.md) and the development sequence in [TUTORIAL.md](TUTORIAL.md).

The runtime implements round-robin and normalized least-loaded scheduling, capacity reservations, bounded admission and backpressure, heartbeat-based failure detection, bounded retries, attempt-level result fencing, submission deduplication, and graceful worker shutdown. The repository includes unit tests, real RPC tests, SIGKILL recovery tests, and CSV benchmark tools.

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
./build/miniray-coordinator --listen 0.0.0.0:50051 --policy least-loaded --max-pending 1024 --heartbeat-timeout-ms 2500
```

```sh
./build/miniray-worker --coordinator localhost:50051 --listen 0.0.0.0:50052 --advertise localhost:50052 --capacity 2
```

```sh
./build/miniray-worker --coordinator localhost:50051 --listen 0.0.0.0:50053 --advertise localhost:50053 --capacity 4
```

Submit example tasks:

```sh
./build/distributed-compute localhost:50051 fibonacci 40
./build/distributed-compute localhost:50051 echo hello
./build/distributed-compute localhost:50051 sleep 1000
```

The expected Fibonacci result is `102334155`. Output includes the task ID, final attempt, and worker ID. `--advertise` must be a worker address reachable by the coordinator; use a routable address instead of localhost when running across machines. Coordinator policies are `round-robin` and `least-loaded`. The worker's `--heartbeat-ms` defaults to 500 and should be well below the coordinator timeout.

Built-in tasks are `echo` (a byte string), `fibonacci` (a decimal integer from 0–93), and `sleep` (decimal milliseconds from 0–60000). Invalid inputs and unknown task types fail immediately; other execution errors are retried according to the task configuration.

## C++ SDK and custom tasks

```cpp
#include "miniray/client.hpp"
miniray::MiniRayClient runtime("localhost:50051");
auto future = runtime.Submit("fibonacci", "40", 2, true);
auto result = future.Get(std::chrono::seconds(30));
// Persist future.id() to retrieve the result later with runtime.Lookup(id).Get().
```

`Submit(type, payload, max_retries=2, retryable=true, task_id="")` returns a `TaskFuture` immediately. `Wait()` returns a structured result, including failure status; `Get()` throws on failure. A wait timeout does not cancel the task. RPC failures throw `RpcError`; inspect its `code` to distinguish capacity exhaustion, an unknown ID, and network errors.

A timed-out submission may already have succeeded. After a transport failure, the SDK retries up to twice with the same ID and includes that ID in the error message. For recovery across client processes, supply a unique application task ID. The same ID and content return the existing task; the same ID with different content returns `ALREADY_EXISTS`. At capacity, the coordinator returns `RESOURCE_EXHAUSTED`; callers control backoff and submission rate.

Register handlers before starting a worker, and deploy the change to every worker:

```cpp
miniray::TaskRegistry registry;
registry.RegisterTask("my_task", [](const std::string& payload) {
  return payload + "!";
});
```

Inputs and outputs are opaque byte strings; applications can encode them with Protobuf. Handlers run outside the registry lock and may be invoked concurrently, so they must be thread-safe. Each input and output payload is limited to 1 MiB.

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

# With running workers providing a total capacity of 6:
./build/miniray-benchmark --coordinator localhost:50051 --tasks 200 --workload mixed --slots 6
./build/miniray-benchmark --mode local --tasks 200 --workload tiny
./build/miniray-benchmark --mode thread-pool --slots 6 --tasks 200 --workload mixed
```

Run the full experiment suite, including warmup, five repeats, homogeneous and heterogeneous workers, overload, and failure recovery:

```sh
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j4
python3 benchmarks/run_suite.py --tasks 400 --repeats 5 --output-dir benchmarks/results/reproduction
```

See the [benchmark analysis](docs/benchmark-results.md) for measured findings and the [benchmark report](benchmarks/results/2026-10-31/report.md) for full tables. [docs/performance.md](docs/performance.md) defines metrics and scheduling/scaling experiments; [docs/validation.md](docs/validation.md) records local results and validation coverage.

## Semantics and limitations

- Retries provide bounded at-least-once execution attempts and require idempotent tasks. External side effects are not guaranteed to occur exactly once.
- `max_retries` counts retries after the initial attempt; `retryable=false` disables all automatic retries.
- `--max-pending` bounds all nonterminal tasks (queued and running). It reserves room for failed running tasks to re-enter the queue, so recovery cannot exceed the bound. Tasks remain queued when no workers are available.
- Worker IDs are generated on each startup. A timed-out process cannot re-register with its old ID and must restart. Heartbeats indicate liveness; they cannot overwrite the coordinator's slot accounting.
- On SIGTERM/SIGINT, a worker stops admission, waits for accepted handlers to finish, reports their results, and then stops heartbeats. Arbitrary running handlers cannot be safely interrupted; a hung handler requires terminating the worker process to recover.
- The coordinator is a single point of failure. Tasks and results are held in memory and lost on restart. Terminal results, idempotency keys, and worker deduplication records remain until process exit; long-running deployments would need TTLs or persistence.
- gRPC is currently unencrypted and unauthenticated, intended for experiments on trusted networks. DAGs, GPUs, dynamic code deployment, high availability, and object storage are not implemented.

Design details are in [docs/architecture.md](docs/architecture.md) and [docs/fault_tolerance.md](docs/fault_tolerance.md).
