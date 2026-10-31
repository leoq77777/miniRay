# MiniRay — A Lightweight Distributed Task Execution Runtime
**Project Proposal | C++20 | Distributed Systems | Infrastructure Engineering**

| Attribute | Description |
|---|---|
| Project Name | MiniRay |
| Language | C++20 |
| Target Platform | Linux |
| Development Time | 2 weeks for MVP + 1 week for optimization |
| Target Roles | SDE, Infrastructure Engineer, Distributed Systems Engineer, MLE |
| Core Technologies | C++20, gRPC, Protobuf, CMake, Docker, Google Test |
| Primary Focus | Distributed scheduling, fault tolerance, concurrency, resource management |

## 1. Project Overview

### Objective

Design and implement a lightweight distributed task execution framework inspired by [Ray](https://github.com/ray-project/ray).

MiniRay allows applications to submit computational tasks to a cluster of worker processes, execute them remotely, and retrieve their results asynchronously.

The project focuses on three fundamental distributed systems problems:

1. **Distributed Execution:** How can tasks be executed efficiently across multiple machines?
2. **Scheduling & Resource Management:** How should tasks be assigned to workers with different capacities and workloads?
3. **Fault Tolerance:** How can the system recover when workers fail during task execution?

The objective is to build a functional, testable distributed runtime with clearly defined execution semantics.

### Project Positioning

MiniRay is intended to complement your existing projects:

| Project | Primary Focus |
|---|---|
| zqr-core | Low-latency C++, memory management, CPU performance |
| **MiniRay** | Distributed scheduling, networking, fault tolerance |
| nano-vLLM-MLX | ML inference and serving infrastructure |

MiniRay should emphasize distributed systems architecture rather than low-level lock-free programming, which is already covered by zqr-core.

---

# 2. System Architecture

MiniRay uses a centralized coordinator architecture.

```text
                     Client Application
                            |
                       submit(task)
                            |
                            v
                  +-------------------+
                  |    Coordinator    |
                  |-------------------|
                  | Task Registry     |
                  | Task Scheduler    |
                  | Worker Registry   |
                  | Failure Detector  |
                  | Result Manager    |
                  +-------------------+
                            |
                  gRPC / Protobuf
                            |
             +--------------+--------------+
             |              |              |
             v              v              v
       +-----------+  +-----------+  +-----------+
       | Worker A  |  | Worker B  |  | Worker C  |
       |-----------|  |-----------|  |-----------|
       | Executor  |  | Executor  |  | Executor  |
       | Task Queue|  | Task Queue|  | Task Queue|
       +-----------+  +-----------+  +-----------+
             |              |              |
             +--------------+--------------+
                            |
                      Task Results
                            |
                            v
                         Client
```

### Major Components

| Component | Responsibility |
|---|---|
| Client API | Submit tasks and retrieve results |
| Coordinator | Manage tasks, workers, and scheduling |
| Task Scheduler | Select workers for task execution |
| Worker Registry | Track worker availability and capacity |
| Worker Executor | Execute tasks locally |
| Failure Detector | Detect worker crashes and timeouts |
| Result Manager | Store task results and execution status |
| RPC Layer | Communication between processes |

### Architecture Decisions

For the MVP:

- One centralized coordinator.
- Multiple worker processes.
- gRPC for communication.
- In-memory task metadata.
- Fixed worker capacities.
- Independent tasks without dependencies.
- Graceful shutdown.

**Out of scope:** Distributed consensus, distributed object storage, GPU scheduling, and coordinator high availability.

The coordinator is a single point of failure. This limitation should be documented rather than hidden.

---

# 3. Core Functional Requirements

## 3.1 Remote Task Execution

The system should support submitting tasks to remote workers.

Example API:

```cpp
MiniRay runtime("localhost:50051");

auto task = runtime.submit(
    "matrix_multiply",
    serialize(input)
);

auto result = runtime.get(task);
```

The example is a proposed API, not an existing implementation.

### Design Decision: Task Representation

Unlike a local thread pool, a distributed runtime cannot directly execute arbitrary C++ lambdas across process boundaries.

Function pointers and captured objects cannot generally be serialized into executable code.

For the MVP, use a **registered task model**.

Workers register supported task types:

```cpp
registry.register_task("matrix_multiply", matrix_multiply);
registry.register_task("fibonacci", fibonacci);
```

The client submits:

```text
Task {
    task_id
    task_type
    serialized_arguments
}
```

Workers execute the corresponding registered function.

This avoids the complexity of distributed code deployment.

### Engineering Questions

- How are task arguments serialized?
- How are task IDs generated?
- What happens if the worker does not support a task type?
- How are execution exceptions returned?
- What happens when the RPC succeeds but the client times out?

---

## 3.2 Distributed Task Scheduling

The coordinator maintains worker information:

```cpp
struct WorkerInfo {
    WorkerId id;
    std::string address;
    uint32_t capacity;
    uint32_t active_tasks;
    WorkerStatus status;
};
```

Implement two scheduling policies.

### Policy A: Round Robin

Assign tasks to workers sequentially.

**Advantages:** Simple and low scheduling overhead.

**Disadvantages:** Ignores workload imbalance.

### Policy B: Least Loaded

Assign tasks to the worker with the lowest normalized load.

For example:

\[
\text{load}(w)=\frac{\text{active tasks}(w)}{\text{capacity}(w)}
\]

**Advantages:** Better adaptation to heterogeneous worker capacities.

**Disadvantages:** Requires maintaining reasonably accurate worker state.

### Benchmark

Compare the two policies under:

- Uniform task durations.
- Variable task durations.
- Heterogeneous worker capacities.
- Burst submissions.

Measure throughput, queueing delay, and p99 completion latency.

---

# 4. Fault Tolerance

This is one of the most important parts of the project.

A distributed system must handle partial failures.

## 4.1 Worker Heartbeats

Each worker periodically reports its status:

```text
Worker
   |
   | Heartbeat
   v
Coordinator
```

The coordinator maintains:

```cpp
last_heartbeat
worker_status
active_tasks
```

If a worker stops reporting within a configured timeout, the coordinator marks it unavailable.

A heartbeat timeout indicates a **suspected failure**, not proof that the worker has crashed. Network delays can cause false suspicions.

## 4.2 Task Recovery

Suppose Worker A crashes while executing Task 42.

```text
Task 42
   |
   v
Worker A
   |
   X  Crash
   |
Coordinator detects timeout
   |
   v
Reassign Task 42
   |
   v
Worker B
```

Implement:

- Worker failure detection.
- Task reassignment.
- Configurable retry limits.
- Failed-task reporting.

### Execution Semantics

MiniRay will support **at-least-once execution attempts** for retryable tasks while the coordinator remains alive.

This means a task may execute multiple times.

For example, a worker may complete a task but fail before reporting the result.

The coordinator cannot safely assume the task was never executed.

Therefore:

- Retried tasks should be idempotent.
- Each task has a stable task ID.
- Each execution attempt has a unique attempt ID.
- The coordinator accepts results only from the currently valid attempt.
- Stale results must not overwrite an accepted result.

This does not guarantee exactly-once external side effects.

---

# 5. Resource Management

Each worker has a configured execution capacity.

For example:

```text
Worker A: 4 concurrent tasks
Worker B: 2 concurrent tasks
Worker C: 8 concurrent tasks
```

The scheduler must respect these limits.

### Features

**Concurrency Limits**

Prevent workers from executing more tasks than their configured capacity.

**Bounded Queues**

Limit pending tasks to prevent unbounded memory growth.

**Backpressure**

Reject submissions or return an overload response when the coordinator reaches capacity.

**Worker Availability**

Exclude unhealthy workers from scheduling.

### Engineering Questions

- Should tasks wait at the coordinator or at workers?
- How should worker capacity be represented?
- What happens when all workers are saturated?
- How should scheduling fairness be evaluated?

For the MVP, I recommend maintaining the pending queue at the coordinator.

This simplifies load tracking and task recovery.

---

# 6. Task Lifecycle

Define an explicit task state machine.

```text
                   SUBMITTED
                       |
                       v
                    PENDING
                       |
                       v
                   DISPATCHED
                       |
                       v
                    RUNNING
                    /     \
                   /       \
                  v         v
             COMPLETED    FAILED
                             |
                        Retryable?
                         /      \
                       Yes       No
                        |         |
                        v         v
                     PENDING   TERMINAL
```

Worker loss can also cause a dispatched or running task to return to `PENDING`, provided its retry budget has not been exhausted.

### Correctness Requirements

1. Every accepted task has a unique identifier.
2. Terminal task states cannot transition back to active states.
3. A result must correspond to the correct task and execution attempt.
4. A worker cannot exceed its configured concurrency capacity.
5. Failed tasks cannot retry indefinitely.
6. The coordinator must not silently lose accepted tasks during normal operation.

The last guarantee applies only while the coordinator process remains alive. In-memory state is lost if the coordinator crashes.

These requirements should be tested explicitly.

---

# 7. Technology Stack

| Layer | Technology | Reason |
|---|---|---|
| Language | C++20 | Modern systems programming |
| RPC | gRPC | Production-oriented RPC framework |
| Serialization | Protocol Buffers | Structured, efficient message encoding |
| Build | CMake | Standard C++ build tooling |
| Testing | Google Test | Unit and integration testing |
| Benchmarking | Google Benchmark + custom harness | Component and end-to-end measurements |
| Deployment | Docker Compose | Reproducible multi-process environment |
| Profiling | Linux perf | CPU and system profiling |
| Sanitizers | ASan / TSan | Memory and concurrency correctness |

### Why gRPC?

You already have experience with gRPC from Kuaishou.

Using it lets you focus on scheduling and failure recovery rather than implementing transport protocols.

Building a custom RPC framework would substantially increase scope without necessarily strengthening the project's main engineering narrative.

---

# 8. Performance Evaluation

MiniRay should include a reproducible benchmark suite.

## Benchmark 1: Remote Execution Overhead

Compare:

- Local function execution.
- Local thread-pool execution.
- Remote execution through MiniRay.

Measure task submission-to-completion latency.

Use small tasks to isolate framework overhead.

## Benchmark 2: Scheduling Strategies

Compare round-robin and least-loaded scheduling.

Workloads:

| Workload | Characteristics |
|---|---|
| Uniform | Tasks have similar durations |
| Heterogeneous | Tasks have varying durations |
| Bursty | Many tasks arrive simultaneously |
| Saturated | Submission rate exceeds execution capacity |

Metrics:

- Throughput.
- Queue waiting time.
- p50/p95/p99 completion latency.
- Worker utilization.
- Scheduling overhead.

## Benchmark 3: Scalability

Increase worker count:

```text
1 → 2 → 4 → 8 workers
```

Measure throughput and identify scaling bottlenecks.

A single machine running multiple worker processes is sufficient for initial experiments, but it does not reproduce real network latency or multi-machine resource isolation.

## Benchmark 4: Failure Recovery

Inject worker failures during task execution.

Measure:

- Failure detection latency.
- Task reassignment latency.
- End-to-end recovery time.
- Retry counts.
- Final task completion rate.

The tests should also verify that stale execution attempts cannot overwrite valid results.

---

# 9. Development Roadmap

**Target: 14-day MVP + 7-day engineering validation**

AI-assisted coding can accelerate implementation, but the schedule is intentionally aggressive.

| Stage | Timeline | Deliverable |
|---|---|---|
| M0: Project Setup | Day 1 | CMake, gRPC, Protobuf, tests |
| M1: Remote Execution | Days 2–4 | Client submits tasks to workers |
| M2: Scheduling | Days 5–6 | Round-robin and least-loaded scheduling |
| M3: Fault Tolerance | Days 7–10 | Heartbeats, retries, failure handling |
| M4: Resource Management | Days 11–12 | Capacity limits, bounded queues |
| M5: Integration | Days 13–14 | Working distributed runtime |
| M6: Benchmarking | Days 15–18 | Performance comparison |
| M7: Validation | Days 19–21 | Failure injection, stress tests, documentation |

### MVP Acceptance Criteria

By Day 14, MiniRay should:

- Run one coordinator and multiple worker processes.
- Accept tasks from a client.
- Execute registered task types remotely.
- Return results.
- Support at least two scheduling strategies.
- Respect worker capacity limits.
- Detect worker failures.
- Retry eligible tasks.
- Handle duplicate or stale completion messages.
- Reject tasks when the submission queue is full.

**Do not add more features until these work correctly.**

---

# 10. Proposed Repository Structure

```text
miniray/
├── CMakeLists.txt
├── README.md
├── LICENSE
│
├── proto/
│   └── miniray.proto
│
├── include/miniray/
│   ├── client.hpp
│   ├── coordinator.hpp
│   ├── scheduler.hpp
│   ├── worker.hpp
│   ├── task.hpp
│   └── registry.hpp
│
├── src/
│   ├── client/
│   ├── coordinator/
│   ├── scheduler/
│   └── worker/
│
├── tests/
│   ├── test_scheduler.cpp
│   ├── test_task_lifecycle.cpp
│   ├── test_worker_failure.cpp
│   └── test_retry.cpp
│
├── benchmarks/
│   ├── benchmark_rpc.cpp
│   ├── benchmark_scheduling.cpp
│   └── benchmark_scalability.cpp
│
├── examples/
│   └── distributed_compute.cpp
│
├── docker/
│   └── docker-compose.yml
│
└── docs/
    ├── architecture.md
    ├── fault_tolerance.md
    └── performance.md
```

---

# 11. Resume Positioning

Once implemented and benchmarked, MiniRay could appear on your resume as:

**MiniRay — Distributed Task Execution Framework**  
*C++20, gRPC, Protobuf, Docker, Distributed Systems*

Potential resume bullets:

> Designed and implemented a distributed task execution framework in C++20, supporting asynchronous task submission, centralized scheduling, and remote execution across multiple worker processes.

> Engineered heartbeat-based worker failure detection and retry mechanisms with attempt-level result validation to support fault recovery under partial failures.

> Benchmarked round-robin and load-aware scheduling under heterogeneous workloads, evaluating throughput, tail latency, and scalability across multiple workers.

After profiling and optimization, replace the last bullet with a concrete, verified performance improvement if one is achieved.

---

# 12. Key Engineering Knowledge to Master

To make MiniRay interview-ready, you should be able to answer the following questions.

| Topic | Interview Question |
|---|---|
| RPC | What happens when an RPC times out but the remote operation succeeded? |
| Scheduling | Why might least-loaded scheduling outperform round-robin? |
| Concurrency | How do you protect shared coordinator state? |
| Fault tolerance | How do you distinguish a crashed worker from a slow worker? |
| Retry semantics | Why can't retries guarantee exactly-once execution? |
| Resource management | How does backpressure prevent overload? |
| Scalability | What happens when the coordinator becomes the bottleneck? |
| Consistency | How do you prevent stale task results from being accepted? |
| Performance | How do you separate network latency from scheduling overhead? |
| Architecture | When would a decentralized scheduler be preferable? |

These are the concepts I would prioritize over advanced C++ micro-optimizations.

---

## 13. What Makes MiniRay a Strong Resume Project?

A basic implementation of remote task execution is relatively straightforward with modern RPC frameworks and AI coding tools.

The engineering value comes from demonstrating that the system behaves correctly under difficult conditions.

For example:

**Experiment A — Worker Crash**

Submit 1,000 tasks, terminate one worker during execution, and verify that eligible tasks are reassigned without incorrect result acceptance.

**Experiment B — Scheduler Comparison**

Create a workload with mixed short and long tasks. Compare round-robin and least-loaded scheduling, explaining when each performs better.

**Experiment C — Overload**

Increase the submission rate beyond worker capacity. Verify that queue growth remains bounded and the system applies backpressure.

**Experiment D — Scaling**

Increase worker count and identify where throughput stops scaling. Determine whether the bottleneck is RPC overhead, coordinator synchronization, or task execution.

These experiments create concrete engineering evidence for interviews.

---

## Final Recommendation

For your internship timeline, I would keep MiniRay focused on three pillars:

**1. Distributed Task Scheduling**  
Understand scheduling strategies, worker coordination, and resource management.

**2. Fault Tolerance**  
Understand partial failures, heartbeats, retries, and execution semantics.

**3. Performance Engineering**  
Measure scheduling overhead, throughput, scalability, and tail latency.

Build the MVP quickly with AI assistance, then spend your remaining time validating its behavior and understanding its design decisions.

**Definition of success:** A small distributed runtime that you can explain, test, benchmark, and defend in a technical interview.

That is more valuable for your current goals than an ambitious Ray clone with dozens of unfinished features.
