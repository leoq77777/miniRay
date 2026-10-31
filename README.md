# MiniRay

C++20 / gRPC 分布式任务运行时。按 [PROPOSAL.md](PROPOSAL.md) 的 MVP 和 [TUTORIAL.md](TUTORIAL.md) 的实现路线构建：一个中心协调器、多个独立 worker、注册任务、异步提交与结果获取。

实现功能：轮询及归一化最小负载调度、容量预留、有界接纳和背压、心跳故障检测、有限重试、attempt 级结果隔离、重复提交去重、worker 优雅退出。附单元测试、真实 RPC 测试、SIGKILL 恢复测试及 CSV 基准工具。

## 构建与测试

Linux（Ubuntu 24.04）：

```sh
sudo apt-get update
sudo apt-get install -y g++ cmake libgrpc++-dev libprotobuf-dev protobuf-compiler protobuf-compiler-grpc libgtest-dev python3
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j4
ctest --test-dir build --output-on-failure
```

macOS：

```sh
brew install cmake grpc googletest
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DCMAKE_PREFIX_PATH="$(brew --prefix)"
cmake --build build -j4
ctest --test-dir build --output-on-failure
```

不需要网络下载 C++ 源码依赖；CMake 使用系统包，并在构建目录生成 Protobuf/gRPC 代码。只构建程序可用 `-DBUILD_TESTING=OFF`。

## 运行

分别在三个终端启动：

```sh
./build/miniray-coordinator --listen 0.0.0.0:50051 --policy least-loaded --max-pending 1024 --heartbeat-timeout-ms 2500
```

```sh
./build/miniray-worker --coordinator localhost:50051 --listen 0.0.0.0:50052 --advertise localhost:50052 --capacity 2
```

```sh
./build/miniray-worker --coordinator localhost:50051 --listen 0.0.0.0:50053 --advertise localhost:50053 --capacity 4
```

提交示例：

```sh
./build/distributed-compute localhost:50051 fibonacci 40
./build/distributed-compute localhost:50051 echo hello
./build/distributed-compute localhost:50051 sleep 1000
```

预期 Fibonacci 结果为 `102334155`，输出含 task ID、最终 attempt 和 worker ID。`--advertise` 必须是协调器能够连接的 worker 地址，跨机器运行不能使用 localhost。协调器策略可选 `round-robin` / `least-loaded`；worker `--heartbeat-ms` 默认为 500，应明显小于协调器超时。

内置任务：`echo`（字节串）、`fibonacci`（十进制整数 0–93）、`sleep`（十进制毫秒 0–60000）。无效输入和未知任务立即终止；其他执行异常按任务配置重试。

## C++ SDK 与任务扩展

```cpp
#include "miniray/client.hpp"
miniray::MiniRayClient runtime("localhost:50051");
auto future = runtime.Submit("fibonacci", "40", 2, true);
auto result = future.Get(std::chrono::seconds(30));
// future.id() 可持久记录；通过 runtime.Lookup(id).Get() 再取结果。
```

`Submit(type, payload, max_retries=2, retryable=true, task_id="")` 立即返回 `TaskFuture`；`Wait()` 返回包括失败状态在内的结构化结果，`Get()` 对失败抛异常。等待超时不取消任务。RPC 错误抛出 `RpcError`，可读取 `code` 区分满载、未知 ID 或网络错误。

提交超时可能已经成功。SDK 在传输失败后使用同一个 ID 重试最多两次；错误信息包含该 ID。需要跨进程恢复提交时，传入业务唯一 task ID；同 ID 同内容返回原任务，同 ID 不同内容返回 `ALREADY_EXISTS`。满载时返回 `RESOURCE_EXHAUSTED`，调用方自行控制退避与提交速率。

在 worker 启动前注册处理函数，并将修改部署到所有 worker：

```cpp
miniray::TaskRegistry registry;
registry.RegisterTask("my_task", [](const std::string& payload) {
  return payload + "!";
});
```

参数与返回值均为不透明字节串，可自行使用 Protobuf 编解码。函数在注册表锁外执行，可并发调用；处理函数自行保证线程安全。业务数据单次输入、输出均限制为 1 MiB。

## Docker Compose

```sh
docker compose -f docker/docker-compose.yml up --build -d
docker compose -f docker/docker-compose.yml exec coordinator ./build/distributed-compute coordinator:50051 fibonacci 40
docker compose -f docker/docker-compose.yml down
```

镜像构建阶段运行全部测试，worker 使用独立进程与不同容量。更改策略：`MINIRAY_POLICY=round-robin docker compose -f docker/docker-compose.yml up -d`。

## 验证和性能

```sh
# 内存与并发检测分别构建，不能组合。
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DMINIRAY_SANITIZER=address
cmake --build build-asan -j4
ctest --test-dir build-asan --output-on-failure

cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DMINIRAY_SANITIZER=thread
cmake --build build-tsan -j4
ctest --test-dir build-tsan --output-on-failure

# 已运行 worker 总容量为 6 时：
./build/miniray-benchmark --coordinator localhost:50051 --tasks 200 --workload mixed --slots 6
./build/miniray-benchmark --mode local --tasks 200 --workload tiny
./build/miniray-benchmark --mode thread-pool --slots 6 --tasks 200 --workload mixed
```

完成预热、5 次重复、同构/异构、过载与故障恢复的完整实验：

```sh
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j4
python3 benchmarks/run_suite.py --tasks 400 --repeats 5 --output-dir benchmarks/results/reproduction
```

实测结论见 [benchmark 分析](docs/benchmark-results.md)，完整表格见 [benchmark 报告](benchmarks/results/2026-10-31/report.md)。指标定义、调度比较和扩展性实验见 [docs/performance.md](docs/performance.md)，本机实测及验证范围见 [docs/validation.md](docs/validation.md)。

## 语义与边界

- 重试是有限的 at-least-once 执行尝试，要求任务幂等；不保证外部副作用 exactly-once。
- `max_retries` 指初次执行以外的重试次数；`retryable=false` 禁止所有自动重试。
- `--max-pending` 限制所有未终结任务（等待 + 执行），为执行任务的失败重入预留位置，保证恢复不会突破有界队列。没有 worker 时任务保持等待。
- worker ID 每次启动随机生成；超时后的旧进程不能复活注册，必须重启。心跳只用于活性判断，不能覆盖协调器的槽位计数。
- worker SIGTERM/SIGINT 停止接纳、等待已接纳函数结束并上报，再结束心跳。执行中的任意用户函数无法安全强制中断；挂死函数需要终止 worker 进程恢复。
- 协调器是单点，任务和结果仅存内存；重启会丢失。终态结果、幂等键和 worker 去重记录保留到进程退出，长期服务需要后续引入 TTL/持久化。
- 当前使用明文且无认证的 gRPC，仅用于可信网络实验。未实现 DAG、GPU、动态代码部署、HA 和对象存储。

设计细节见 [docs/architecture.md](docs/architecture.md) 和 [docs/fault_tolerance.md](docs/fault_tolerance.md)。
