# MiniRay 从零构建教程：分布式任务调度与执行系统

本文档是 **MiniRay** 的循序渐进（Step-by-step）开发与理论拆解指南。旨在帮助你在动手写代码之前，先建立对分布式运行时系统的核心认知，然后按照工业级工程实践逐步落地。

---

## 目录
1. [核心认知与理论准备](#1-核心认知与理论准备)
   - 1.1 进程隔离与注册任务模型（Why Registered Tasks?）
   - 1.2 调度架构：Push vs Pull 与中心化权衡
   - 1.3 调度策略：Round-Robin vs Least-Loaded
   - 1.4 分布式容错：心跳探测、At-least-once 与 Stale Result 防护
   - 1.5 资源管理与背压（Backpressure）
2. [数据流与状态机设计](#2-数据流与状态机设计)
   - 2.1 任务生命周期状态机
   - 2.2 核心 RPC 契约（Protobuf 视角）
3. [分阶段实施路线图（Step-by-Step Roadmap）](#3-分阶段实施路线图step-by-step-roadmap)
   - [Phase 0: 项目骨架与 CMake/vcpkg 构建配置](#phase-0-项目骨架与-cmakevcpkg-构建配置)
   - [Phase 1: 核心契约与本地任务注册表](#phase-1-核心契约与本地任务注册表)
   - [Phase 2: Worker 执行器与本地并发槽位](#phase-2-worker-执行器与本地并发槽位)
   - [Phase 3: Coordinator 调度器与 Worker 状态管理](#phase-3-coordinator-调度器与-worker-状态管理)
   - [Phase 4: 心跳检测与故障恢复机制（核心难点）](#phase-4-心跳检测与故障恢复机制核心难点)
   - [Phase 5: Client SDK 封装与端到端链路验证](#phase-5-client-sdk-封装与端到端链路验证)
   - [Phase 6: 性能基准测试与混沌故障注入](#phase-6-性能基准测试与混沌故障注入)
4. [核心代码实现骨架示例](#4-核心代码实现骨架示例)
5. [高频面试题与系统设计防坑指南](#5-高频面试题与系统设计防坑指南)

---

## 1. 核心认知与理论准备

### 1.1 进程隔离与注册任务模型（Why Registered Tasks?）

在单进程中，C++ 线程池可以直接接收 `std::function<void()>` 或任意 lambda 闭包。然而在分布式场景下：
- **内存地址不可跨进程共享**：函数指针指向的是本进程虚拟地址空间的代码段地址（`0x0040...`），网络对端的 Worker 进程如果直接跳转这个地址，会导致段错误（SIGSEGV）或不可预测行为。
- **C++ 闭包无法自动序列化**：Lambda 捕获的堆栈变量、智能指针或包含虚表指针（`vptr`）的对象，无法被二进制反射引擎原样重构。

**MiniRay 的工程解法**：采用 **任务注册制（Registered Task Model）**。
- Coordinator 和 Worker 提前注册好支持的任务名（例如 `"matrix_multiply"`, `"heavy_hash"`）和对应的本地执行函数指针。
- 网络上传输的只是结构化的元数据：`task_name`（任务名）、`task_id`（唯一标识）和 `payload`（二进制序列化后的入参，如 Protobuf/JSON 字节流）。
- Worker 收到请求后，通过名称查表找到本地函数，将 `payload` 反序列化为强类型参数执行。

---

### 1.2 调度架构：Push vs Pull 与中心化权衡

分布式计算系统常见的两种任务分发模式：
1. **Push 模式（MiniRay MVP 采用）**：Coordinator 维护集群所有 Worker 的状态，当任务就绪时，主动选出一个可用 Worker，通过 RPC 调用 Worker 的 `ExecuteTask` 接口。
   - **优点**：Coordinator 拥有全局视角，容易实现精确的全局调度策略（如 Least-Loaded）；任务调度延迟低。
   - **挑战**：Coordinator 必须维护精确的 Worker 活跃状态和负载计数。
2. **Pull 模式（如 Spark/Ray 的某些组件）**：Coordinator 仅维护全局队列，Worker 在本地有空闲槽位（Slot）时向 Coordinator 拉取任务。
   - **优点**：天然自带背压，Worker 不会被压垮。
   - **缺点**：空闲时轮询可能产生额外 RPC 开销或空转等待。

---

### 1.3 调度策略：Round-Robin vs Least-Loaded

- **Round-Robin（轮询调度）**：
  - 维护一个递增的原子计数器：`target_index = counter++ % alive_workers.size()`。
  - **缺陷**：如果任务执行时长不均匀（如有的任务 10ms，有的任务 10s），轮询会导致部分 Worker 积压严重，而另一些 Worker 已经空闲（长尾效应）。
- **Least-Loaded（最小负载优先）**：
  - 定义 Worker 归一化负载：
    $$\text{load}(w) = \frac{\text{active\_tasks}(w)}{\text{capacity}(w)}$$
  - 调度时遍历所有健康的 Worker，选择当前归一化负载最低的节点。若负载相同，则根据任务数或 ID 确定性打破平局。

---

### 1.4 分布式容错：心跳探测、At-least-once 与 Stale Result 防护

在不依赖分布式共识（如 Raft/Paxos）的前提下，中心化集群的容错核心在于**故障推测与安全重试**：

1. **心跳（Heartbeat）与假死**：
   - Worker 周期性向 Coordinator 上报心跳（例如每 500ms 一次）。
   - Coordinator 维护 `last_heartbeat_timestamp`。如果超过阈值（例如 2.5s），Coordinator 将其标记为 `UNHEALTHY` 或 `DEAD`。
   - **注意**：网络分区、GC 停顿或 CPU 抢占都可能导致假死（Worker 没崩，只是消息迟到了）。
2. **At-Least-Once 语义与幂等要求**：
   - 当 Worker A 失去心跳时，Coordinator 将 Worker A 上运行的未完成任务重新塞入待调度队列，并分派给 Worker B 重试。
   - 这意味着同一个任务可能被重复执行（例如 Worker A 其实跑完了，只是回复 RPC 超时）。因此 MiniRay 要求注册的任务逻辑尽量具备幂等性。
3. **Attempt ID 与 Stale Result 防护（核心考点）**：
   - 每次任务被派发时，除了分配全局唯一的 `task_id`，还必须分配单调递增的 `attempt_id`（初次派发为 0，第 1 次重试为 1）。
   - Coordinator 的任务记录中维护 `current_valid_attempt_id`。
   - **竞争场景**：假设 Worker A 只是慢了，它在超时后依然把 Attempt 0 的结果发了回来；此时 Worker B 正在执行 Attempt 1。
   - **防脏写规则**：Coordinator 收到执行结果时，检查请求中的 `attempt_id`。若 `rpc_attempt_id < task.current_valid_attempt_id`，则直接判定为**过期结果（Stale Result）**并无损丢弃，绝不允许覆盖已接受或正在运行的更高代际结果。

---

### 1.5 资源管理与背压（Backpressure）

- **Worker Slot（容量槽位）**：Worker 启动时配置 `capacity`（例如根据 CPU 核心数配置为 4 或 8）。Worker 内部通过信号量（`std::counting_semaphore`）或原子计数器严格限制并行度。
- **Coordinator Bounded Queue（有界队列）**：Coordinator 的任务等待队列必须设置容量上限（`MAX_PENDING_TASKS`）。当队列满载时，直接在 `SubmitTask` RPC 中返回 `RESOURCE_EXHAUSTED` 错误，迫使 Client 进行退避重试（Exponential Backoff），防止内存耗尽崩溃（OOM）。

---

## 2. 数据流与状态机设计

### 2.1 任务生命周期状态机

```text
               Client Submit
                     │
                     ▼
             [ SUBMITTED ]
                     │ (入待调度队列)
                     ▼
              [ PENDING ] ◄──────────────────────┐
                     │                           │
                     │ (调度选中 Worker,          │ 超时/宕机重试
                     │  attempt_id++)            │ (未超最大重试次数)
                     ▼                           │
             [ DISPATCHED ]                      │
                     │                           │
                     │ (Worker 接收并放入本地队列) │
                     ▼                           │
               [ RUNNING ] ──────────────────────┤
                │         │                      │
       成功完成  │         │ 执行抛出异常           │
                ▼         ▼                      ▼
          [ COMPLETED ]  [ FAILED (Terminal) ] ──┘
```

**不可变规则**：
1. 进入 `COMPLETED` 或耗尽重试次数的 `FAILED` 是终态，绝不允许逆向流转。
2. 任何状态转移必须由互斥锁（Mutex）保护，避免并发心跳扫描线程与结果接收线程产生竞态。

---

### 2.2 核心 RPC 契约（Protobuf 视角）

在 `proto/miniray.proto` 中，系统需要定义两组 RPC 服务：
1. `CoordinatorService`：面向 Client 和 Worker 上报。
2. `WorkerService`：面向 Coordinator 派发与控制。

```protobuf
syntax = "proto3";

package miniray.rpc;

enum TaskStatus {
    TASK_STATUS_UNKNOWN = 0;
    TASK_STATUS_SUBMITTED = 1;
    TASK_STATUS_PENDING = 2;
    TASK_STATUS_DISPATCHED = 3;
    TASK_STATUS_RUNNING = 4;
    TASK_STATUS_COMPLETED = 5;
    TASK_STATUS_FAILED = 6;
}

// 任务定义
message TaskSpec {
    string task_id = 1;
    string task_type = 2;
    bytes payload = 3;
    uint32 attempt_id = 4;
}

// Client -> Coordinator
message SubmitTaskRequest {
    string task_type = 1;
    bytes payload = 2;
}

message SubmitTaskResponse {
    string task_id = 1;
    bool accepted = 2;
    string error_message = 3;
}

message GetTaskResultRequest {
    string task_id = 1;
    uint32 timeout_ms = 2;
}

message GetTaskResultResponse {
    string task_id = 1;
    TaskStatus status = 2;
    bytes result = 3;
    string error_message = 4;
}

// Coordinator -> Worker
message DispatchTaskRequest {
    TaskSpec task = 1;
}

message DispatchTaskResponse {
    bool accepted = 1;
    string error_message = 2;
}

// Worker -> Coordinator
message HeartbeatRequest {
    string worker_id = 1;
    string worker_address = 2;
    uint32 capacity = 3;
    uint32 active_tasks = 4;
}

message HeartbeatResponse {
    bool ack = 1;
}

message ReportResultRequest {
    string task_id = 1;
    uint32 attempt_id = 2;
    string worker_id = 3;
    TaskStatus status = 4;
    bytes result = 5;
    string error_message = 6;
}

message ReportResultResponse {
    bool accepted = 1;
}

service CoordinatorService {
    rpc SubmitTask (SubmitTaskRequest) returns (SubmitTaskResponse);
    rpc GetTaskResult (GetTaskResultRequest) returns (GetTaskResultResponse);
    rpc SendHeartbeat (HeartbeatRequest) returns (HeartbeatResponse);
    rpc ReportResult (ReportResultRequest) returns (ReportResultResponse);
}

service WorkerService {
    rpc DispatchTask (DispatchTaskRequest) returns (DispatchTaskResponse);
}
```

---

## 3. 分阶段实施路线图（Step-by-Step Roadmap）

### Phase 0: 项目骨架与 CMake/vcpkg 构建配置
- **目标**：打通现代 C++20 编译管线，引入 Protobuf、gRPC 与 Google Test。
- **关键动作**：
  1. 创建规范的目录拓扑：`include/miniray/`、`src/`、`proto/`、`tests/`。
  2. 编写顶级 `CMakeLists.txt`，配置 `find_package(gRPC CONFIG REQUIRED)` 和 `protobuf_generate_cpp`。
  3. 编写一个最小的 Hello World 测试用例，验证 C++20 概念（如 `<ranges>`, `<concepts>`, `std::jthread`）编译通过。

### Phase 1: 核心契约与本地任务注册表
- **目标**：实现独立于网络的本地任务注册与执行引擎。
- **关键动作**：
  1. 定义 `TaskRegistry` 类：使用 `std::unordered_map<std::string, std::function<std::string(const std::string&)>>` 注册任务。
  2. 编写泛型辅助工具，支持将任意类型的入参反序列化并调用原生函数。
  3. 编写单测 `test_registry.cpp`，测试函数注册、查找、执行以及未知任务名的异常处理。

### Phase 2: Worker 执行器与本地并发槽位
- **目标**：实现 Worker 进程的并发控制与任务执行循环。
- **关键动作**：
  1. 实现 `WorkerThreadPool` 或槽位控制（使用 C++20 `std::counting_semaphore` 限制最大并发）。
  2. 实现 `WorkerServiceImpl`：实现 `DispatchTask` RPC。收到任务后，提交到本地线程池异步执行。
  3. 执行完毕后，调用 Coordinator 的 `ReportResult` RPC 异步上报结果。
  4. 编写单测 `test_worker_execution.cpp`，模拟派发多个计算密集型任务，验证槽位限制与执行结果一致性。

### Phase 3: Coordinator 调度器与 Worker 状态管理
- **目标**：建立中心化状态大盘，实现两种调度策略。
- **关键动作**：
  1. 实现 `WorkerRegistry`：线程安全地增删改查节点信息（`WorkerInfo`）。
  2. 实现 `Scheduler` 接口基类，派生出 `RoundRobinScheduler` 与 `LeastLoadedScheduler`。
  3. 实现 Coordinator 的主调度循环（Dispatcher Thread）：定期或条件唤醒，从 `pending_queue_` 提取任务，调用调度策略分发到 Worker。
  4. 编写单测 `test_scheduler.cpp`：注入模拟的 Worker 负载数据，验证调度选择是否符合算法预期。

### Phase 4: 心跳检测与故障恢复机制（核心难点）
- **目标**：闭环分布式系统的容错能力。
- **关键动作**：
  1. 在 Worker 侧启动后台 `std::jthread`，定时调用 `SendHeartbeat`。
  2. 在 Coordinator 侧启动故障检测循环：每隔 `CHECK_INTERVAL` 扫描所有 Worker。
  3. 若 `now - last_heartbeat > TIMEOUT`，将该 Worker 标为 `DEAD`，获取该 Worker 上正在运行的所有任务。
  4. 任务重试逻辑：递增任务的 `attempt_id`，重新插入 `pending_queue_`。若重试次数超过 `MAX_RETRIES`，转为终态 `FAILED`。
  5. 在 `ReportResult` 中增加校验：拒绝 `attempt_id < task.current_valid_attempt_id` 的结果。
  6. 编写集成测试 `test_worker_failure.cpp`：主动 kill 掉一个正在执行的 Worker，断言任务在另一个 Worker 上被顺利恢复并成功产出结果。

### Phase 5: Client SDK 封装与端到端链路验证
- **目标**：向开发者暴露类似 Ray 的极简 API。
- **关键动作**：
  1. 封装 `MiniRayClient`：提供 `Submit(task_type, payload)` 返回 `TaskFuture`。
  2. 提供 `Get(task_id)` 同步阻塞获取，内部实现轮询重试或流式通知。
  3. 编写端到端示例程序 `examples/distributed_compute.cpp`：模拟集群计算矩阵乘法或大数斐波那契数列。

### Phase 6: 性能基准测试与混沌故障注入
- **目标**：产出简历上能打的量化性能指标与对比数据。
- **关键动作**：
  1. 编写 Benchmark Harness：评估纯 RPC 往返耗时 vs 本地函数调用的调度开销。
  2. 调度策略对比测试：设计长短不一的任务负载（如 5% 的长任务与 95% 的短任务），对比 Round-Robin 和 Least-Loaded 下的 p99 耗时与吞吐量。
  3. 混沌测试：在持续高并发压测过程中随机 SIGKILL 某个 Worker，统计端到端任务成功率与重试延迟。

---

## 4. 核心代码实现骨架示例

### 4.1 任务注册表（Task Registry）
```cpp
// include/miniray/registry.hpp
#pragma once
#include <string>
#include <functional>
#include <unordered_map>
#include <stdexcept>
#include <shared_mutex>

namespace miniray {

using TaskHandler = std::function<std::string(const std::string&)>;

class TaskRegistry {
public:
    void RegisterTask(const std::string& name, TaskHandler handler) {
        std::unique_lock lock(mutex_);
        registry_[name] = std::move(handler);
    }

    std::string Execute(const std::string& name, const std::string& payload) const {
        std::shared_lock lock(mutex_);
        auto it = registry_.find(name);
        if (it == registry_.end()) {
            throw std::runtime_error("Task type not found: " + name);
        }
        return it->second(payload);
    }

    bool HasTask(const std::string& name) const {
        std::shared_lock lock(mutex_);
        return registry_.contains(name);
    }

private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, TaskHandler> registry_;
};

} // namespace miniray
```

### 4.2 调度策略算法接口
```cpp
// include/miniray/scheduler.hpp
#pragma once
#include <vector>
#include <string>
#include <memory>
#include <atomic>
#include <limits>

namespace miniray {

struct WorkerNode {
    std::string id;
    std::string address;
    uint32_t capacity;
    uint32_t active_tasks;
    bool is_healthy;
};

class Scheduler {
public:
    virtual ~Scheduler() = default;
    virtual std::string SelectWorker(const std::vector<WorkerNode>& workers) = 0;
};

class RoundRobinScheduler : public Scheduler {
public:
    std::string SelectWorker(const std::vector<WorkerNode>& workers) override {
        std::vector<const WorkerNode*> available;
        for (const auto& w : workers) {
            if (w.is_healthy && w.active_tasks < w.capacity) {
                available.push_back(&w);
            }
        }
        if (available.empty()) return "";
        size_t idx = counter_.fetch_add(1, std::memory_order_relaxed) % available.size();
        return available[idx]->id;
    }
private:
    std::atomic<size_t> counter_{0};
};

class LeastLoadedScheduler : public Scheduler {
public:
    std::string SelectWorker(const std::vector<WorkerNode>& workers) override {
        const WorkerNode* best_worker = nullptr;
        double min_load = std::numeric_limits<double>::infinity();

        for (const auto& w : workers) {
            if (!w.is_healthy || w.active_tasks >= w.capacity) continue;
            double load = static_cast<double>(w.active_tasks) / static_cast<double>(w.capacity);
            if (load < min_load) {
                min_load = load;
                best_worker = &w;
            }
        }
        return best_worker ? best_worker->id : "";
    }
};

} // namespace miniray
```

---

## 5. 高频面试题与系统设计防坑指南

| 深度考点 | 现场回答话术与底层逻辑 |
|---|---|
| **Q1: 为什么采用 Attempt ID，而不是单靠 Task ID 判重？** | **核心原因在于处理异步网络延迟造成的 Stale Result 竞争**。当 Worker A 因网络卡顿被误判死亡时，Coordinator 会将该任务重分配给 Worker B（Attempt 递增）。若 Worker A 随后苏醒并上报结果，若无 Attempt ID 校验，Worker A 的旧结果可能会脏写覆盖正在由 Worker B 执行的新状态。Attempt ID 提供了单调递增的代际屏障（Generation Epoch）。 |
| **Q2: 调度器 Least-Loaded 为什么比 Round-Robin 更适合生产？** | Round-Robin 假设所有任务时长均质且 Worker 算力同构。但在实际集群中，异构任务（10ms 的轻计算与 10s 的重计算混合）会导致 Round-Robin 将重任务不断堆积到相同节点，产生长尾延迟（Tail Latency）。Least-Loaded 基于实际剩余吞吐容量动态分流，能显著削峰填谷。 |
| **Q3: 为什么说 MiniRay 只能保证 At-least-once，而无法做到 Exactly-once？** | 即使系统内部通过 Attempt ID 过滤了重复结果，但如果 Worker 执行的任务带有外部副作用（例如往数据库插入数据、发送网络请求或写外部磁盘），Worker 在执行完副作用后发生宕机，Coordinator 重新分派该任务后副作用将被再次执行。若要实现 Exactly-once，必须依赖两阶段提交（2PC）或业务本身的幂等性设计。 |
| **Q4: Coordinator 单点故障（SPOF）在生产中怎么扩展？** | 当前 MVP 架构下元数据全在内存中，Coordinator 重启数据丢失。在生产演进中，可以通过引入 Raft 共识协议构建双主高可用集群，或将任务元数据外部化存储到 etcd/Redis 中；调度逻辑则可退化为无状态的 Dispatcher。 |

---

## 下一步行动建议

1. **环境准备**：检查当前系统的 C++ 编译环境、CMake 版本（>= 3.20）以及 gRPC 开发包。
2. **原型初始化**：我们将首先在仓库中建立标准的构建骨架与 `proto/miniray.proto` 文件。
