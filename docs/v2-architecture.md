# MiniRay V2 Architecture Specification

本文档定义 MiniRay 从 V1（中心化无状态 RPC 任务队列）向 V2（高吞吐分布式计算与有状态 Actor 运行时）演进的完整工程规范，涵盖协议扩展、核心状态机不变量、内存安全生命周期与验证矩阵。

---

## 1. 演进目标与统一抽象

### 1.1 V1 实测瓶颈回顾

基于 V1 的 210 批次基准测试（[benchmark-results.md](benchmark-results.md)），当前架构存在四个核心限制：

1. **控制面轮询与同步阻塞**：`CoordinatorService` 使用单派发线程执行 `200ms` 截止时间的同步 `DispatchTask` RPC，空闲时 `sleep_for(10ms)`，导致纯 RPC 仅 `0.119ms` 而 `remote tiny/serial` 端到端延迟达 `14.306ms`，`tiny/burst` 在 1–8 Worker 下吞吐停滞在 `~3,000 tasks/s`。
2. **恢复排队与慢节点长尾**：重试任务通过 `pending_.push_back()` 排入普通队列末尾，导致故障检测仅需 `475ms`，但任务完成恢复高达 `4,216ms`；且缺乏针对慢节点（Straggler）的主动尾延迟抑制机制。
3. **控制面与数据面耦合**：任务输入与返回值完全内联在控制面 Protobuf 消息中（硬上限 `1 MiB`），且不支持跨任务依赖（DAG）与数据局部性感知调度。
4. **仅支持无状态函数与标量槽位**：缺乏常驻内存的有状态实例（Actor）抽象、故障状态重建（Lineage Reconstruction）以及多维异构资源（CPU / Memory / Slots）约束。

### 1.2 核心设计原则

1. **统一 `ObjectRef` 与血缘图（Lineage Graph）底座**：无论是无状态函数（Task）还是有状态方法（Actor Task），其输出均建模为不可变对象（`ObjectId`），输入既支持小数据内联（In-band），也支持引用上游 `ObjectId`（Out-of-band）。动态 DAG 解锁、零拷贝共享内存传输与故障血缘重算共享同一套依赖与位置元数据。
2. **严格向后兼容 V1**：完整保留 V1 的 `MiniRayClient::Submit` 签名、CLI 参数与测试语义。当不启用对象引用、投机执行或多维资源时，全部现有单元测试、进程恢复测试与 210 批基准套件可直接运行，确保 V1 vs. V2 性能对比具备严格的同口径可比性。
3. **不变量优先于吞吐数字**：在引入并发副本竞速、共享内存映射与异步派发后，必须继续保持 **容量上限不突破、有界接纳不溢出、旧代/慢副本结果零污染、共享内存无孤儿泄漏** 四条核心安全不变量。

---

## 2. 协议扩展规范（`proto/miniray.proto`）

在保持现有字段编号与服务方法完全兼容的前提下，按里程碑增量扩展如下消息与 RPC：

```protobuf
// 多维资源向量；milli_cpu 与 mem_bytes 为 0 时退化为 V1 的纯 slots 容量模型。
message Resources {
  uint32 slots = 1;
  uint32 milli_cpu = 2;
  uint64 mem_bytes = 3;
}

// 分布式不可变对象引用与位置元数据。
message ObjectRef {
  string object_id = 1;
  uint64 size_bytes = 2;
  uint32 crc32 = 3;
  string owner_worker_id = 4;
  string host_id = 5;       // 用于同机判定：匹配时直接走 POSIX shm 只读映射
  string shm_name = 6;      // 例如 "/miniray_<cluster>_<object_id>"
}

message TaskArg {
  oneof source {
    bytes inline_data = 1;  // < inline_threshold_bytes (默认 64 KiB) 内联快速路径
    ObjectRef object_ref = 2;
  }
}

// V2.1: 批量派发与协作式取消（用于投机执行胜出后回收慢副本槽位）。
message DispatchBatchRequest {
  repeated TaskSpec tasks = 1;
}
message DispatchBatchResponse {
  repeated DispatchTaskResponse results = 1; // 与请求一一对应
}

message CancelTaskRequest {
  string task_id = 1;
  uint32 attempt_id = 2;
  string reason = 3;
}
message CancelTaskResponse {
  bool cancelled_or_absent = 1;
}

// V2.2: 跨节点分块流式拉取大对象（Worker <-> Worker 直接数据面传输）。
message PullObjectRequest {
  string object_id = 1;
  uint64 offset = 2;
}
message ObjectChunk {
  bytes data = 1;
  uint64 total_size = 2;
  uint32 crc32 = 3;
}

// V2.3: 有状态 Actor 生命周期与顺序方法调用。
message CreateActorRequest {
  string actor_id = 1;      // 可选幂等键
  string actor_class = 2;
  bytes init_payload = 3;
  Resources resources = 4;
  bool spread_placement = 5; // false: Bin-Packing（默认）；true: Spread
  uint32 max_reconstructions = 6;
}
message CreateActorResponse {
  string actor_id = 1;
  bool accepted = 2;
  string error_message = 3;
}

message SubmitActorTaskRequest {
  string actor_id = 1;
  string method = 2;
  repeated TaskArg args = 3;
  string task_id = 4;
  uint64 seq_no = 5;        // 同一客户端会话内单调递增，保证严格按序执行
  bool mutates_state = 6;   // true 时记入 Lineage 重放日志；false 为只读查询
}
```

`WorkerService` 新增接口：
- `rpc DispatchBatch(DispatchBatchRequest) returns (DispatchBatchResponse)`（V2.1）
- `rpc CancelTask(CancelTaskRequest) returns (CancelTaskResponse)`（V2.1）
- `rpc PullObject(PullObjectRequest) returns (stream ObjectChunk)`（V2.2）
- `rpc InstantiateActor(InstantiateActorRequest) returns (InstantiateActorResponse)`（V2.3）
- `rpc DestroyActor(DestroyActorRequest) returns (DestroyActorResponse)`（V2.3）

心跳消息 `HeartbeatRequest` 增量携带：
- `string host_id`（本机唯一标识）
- `repeated string added_objects` / `repeated string evicted_objects`（增量同步 `ObjectLocationDirectory`）
- `HeartbeatResponse.repeated string release_objects`（Coordinator 通知 Worker 回收引用计数归零的共享内存段）

---

## 3. Milestone V2.1：事件驱动控制面、抢占恢复与投机执行

### 3.1 消除轮询与全异步非阻塞派发

1. **事件唤醒机制**：
   - 移除 `CoordinatorService` 中的 `sleep_for(10ms)`。`CoordinatorState` 维护独立条件变量 `dispatch_cv_`。
   - 触发 `dispatch_cv_.notify_one()` 的精确事件集合：
     1. 新任务进入就绪队列（`Submit` 或 DAG 依赖归零）；
     2. Worker 注册、恢复可用或释放槽位（`Heartbeat` 新节点上线、`Report` 释放槽位）；
     3. 派发失败触发回退重试。
   - 定时器唤醒合并：调度线程调用 `dispatch_cv_.wait_until(lock, next_timer_deadline)`，其中 `next_timer_deadline` 取 **最近一次 Worker 心跳过期时间**、**Worker 冷却解除时间（`unavailable_until`）** 与 **最近一次在途任务投机触发时间（`speculate_at`）** 三者的最小值。无活跃任务和节点时零空转开销，有新任务时微秒级唤醒。
2. **gRPC Callback 异步派发与动态批处理（Batching）**：
   - `CoordinatorState::NextDispatchBatch(max_batch_per_worker)` 在单次持锁内按调度策略为每个可用 Worker 打包至多 `B` 个任务（默认 `B = 16`），预留对应槽位后立即释放状态锁。
   - 使用 `stub->async()->DispatchBatch(...)` 发起异步非阻塞 RPC；单个慢 Worker 的网络超时不再阻塞向其他健康 Worker 派发任务。每个 Worker 限制最大并发在途派发批次 `max_inflight_batches = 4`。

### 3.2 EWMA 服务时长估计与三级防饥饿队列

1. **在线统计器 `TaskProfile`**：
   - 按 `task_type` 维护成功完成样本的指数移动平均 \(\widehat{E}\)（\(\alpha = 0.2\)）以及固定大小环形缓冲区（如最近 64 个样本）计算在线 `P95` 执行时长。样本数 `< 8` 时视为冷启动，不触发投机执行。
2. **三级就绪队列与防队头阻塞（HoL Mitigation）**：
   - `recovery_queue_`：存放因 Worker 失联、派发失败或异常重试的任务，按原始 `submitted_at` 升序排列，享有最高派发优先级（直接解决 V1 故障恢复排队 3.5 秒的问题）。
   - `fast_queue_`：存放 \(\widehat{E}(\text{type}) \le \theta_{\text{fast}}\)（默认 `10ms`）的微任务。
   - `normal_queue_`：存放未知类型或长耗时任务。
   - **Aging 防饥饿不变量**：每次出队前检查 `normal_queue_.front()` 的当前排队时长；若超过 `aging_threshold`（默认 `50ms`），本轮强制优先出队 `normal_queue_`，保证长任务在微任务洪峰下存在严格有界的最大等待时间。

### 3.3 并发 Attempt 集合与自适应投机执行（Speculative Execution）

将 V1 的单活跃 Attempt 模型扩展为有界并发副本模型：

```text
PENDING ──► ACTIVE { attempt k on W1 }
                 │
                 ├── (elapsed > 1.5 * P95 && retryable && cluster has idle headroom)
                 ▼
            ACTIVE { attempt k on W1, attempt k+1 on W2 }  (Speculative Race)
                 │
                 ├── First valid COMPLETED (or non-retryable FAILED) arrives from W_win
                 ▼
            COMPLETED (winner recorded; async CancelTask sent to W_lose)
```

**投机执行核心不变量**：
1. **接纳预算守恒**：每个任务实例无论拥有 1 个还是 2 个并发在途副本，在任务层面仍对应同一条逻辑接纳记录，但投机副本仅在 **待调度队列全空（`pending == 0`）且目标健康节点存在空闲槽位** 时允许启动，且选定的 `W2` 必须与已运行副本所在节点 `W1` 不同（Anti-affinity）。系统过载时投机自动静默关闭，绝不与正常排队任务争抢槽位。
2. **多副本结果隔离（Concurrent Attempt Fencing）**：
   - `Task` 维护 `std::map<uint32_t, ActiveAttempt> active_attempts`（硬上限 `max_concurrent_attempts = 2`）。
   - 当某个活跃副本上报 `COMPLETED`：立即释放其占用的 Worker 槽位，将任务状态置为终态 `COMPLETED`，将剩余仍在途的副本移入 `cancelling_attempts` 列表（其占用的远程 Worker 槽位在收到 `CancelTask` 响应、晚到 `ReportResult` 或该 Worker 心跳过期时精确释放，防止协调器提前超发导致 Worker 本地队列溢出）。
   - 当某个活跃副本上报可重试 `FAILED`，而另一并发投机副本仍在健康运行：仅移除失败副本并释放其槽位，**不立即判任务失败也不重复入队**，继续等待在途健康副本完成。
3. **Worker 协作式取消**：`TaskRegistry` 支持注册带 `const CancellationToken&` 的扩展处理函数（兼容旧签名）。对于队列中尚未开始执行的被取消副本，Worker 直接丢弃并立即释放本地槽位；对于执行中的函数，通过原子标志位协作退出。

---

## 4. Milestone V2.2：零拷贝对象存储、动态 DAG 与 EFT 调度

### 4.1 混合对象存储（In-band + POSIX Shared Memory）

1. **双路径阈值**：
   - `size < inline_threshold_bytes`（默认 `64 KiB`）：直接存入 `ReportResultRequest.result`，零额外寻址开销。
   - `size >= inline_threshold_bytes`（默认单对象上限 `256 MiB`）：写入 Worker 本地的 `ObjectStore`，控制面仅传递百字节级的 `ObjectRef` 元数据。
2. **POSIX 共享内存内存布局与无锁只读共享**：
   - 共享内存段命名规范：`/mr_<cluster_nonce>_<object_id>`（满足 macOS POSIX `shm_open` 路径长度 `< 31` 字节限制：`cluster_nonce` 取 4 字节 hex，`object_id` 取 16 字节 hex，总长 25 字节）。
   - 段首固定 64 字节对齐头结构：
     ```cpp
     struct alignas(64) ShmHeader {
       uint32_t magic;                  // 0x4D524159 ("MRAY")
       uint32_t version;                // 1
       uint64_t payload_size;
       uint32_t crc32;
       std::atomic<uint32_t> sealed;    // 0 = WRITING, 1 = SEALED (acquire/release)
       uint32_t creator_pid;
       uint8_t reserved[36];
     };
     ```
   - **Write-Once-Read-Many 不可变契约**：写者完成 `memcpy` 与 `crc32` 计算后执行 `sealed.store(1, std::memory_order_release)`，随后再向 Coordinator 汇报对象就绪。同机读者以 `O_RDONLY` 打开并 `mmap(PROT_READ, MAP_SHARED)`，校验 `magic` 与 `sealed == 1` 后直接暴露 `std::span<const std::byte>` 给算子，**全程零用户态拷贝、零 Protobuf 编解码**。
3. **共享内存防泄漏三层保障**：
   - **引用计数回收**：Coordinator 对每个 `ObjectId` 维护 `ref_count = 未完成下游依赖任务数 + 客户端显式 Pin 计数（带可配 TTL，默认 60s）`。归零时随心跳下发回收指令，持有 Worker 执行 `shm_unlink`。
   - **RAII 优雅退出清理**：`WorkerService` 维护本进程创建的 `shm_name` 集合，析构与 `SIGINT/SIGTERM` 退出路径统一执行 `shm_unlink`。
   - **启动孤儿清扫**：Coordinator 或 Worker 启动时扫描 `/dev/shm`（Linux）或本地注册表文件（macOS，因 Darwin 不支持遍历 `/dev/shm` 目录，在 `/tmp/miniray_shm_manifest_<uid>` 追加记录段名与 `creator_pid`），若 `kill(creator_pid, 0) == -1 && errno == ESRCH`，自动 `shm_unlink` 回收崩溃遗留段。

### 4.2 跨机 `PullObject` 流式传输与 Singleflight

当任务被调度到与依赖对象不同主机（`worker.host_id != ref.host_id`）的节点时：
1. Worker 在正式投递给执行线程前，先进入异步 `DependencyFetchStage`。
2. 对本地缺失的每个 `ObjectRef`，向 `owner_worker_id`（或 Coordinator 返回的任意存活副本节点）发起 `PullObject` 流式读取（分块大小 `1 MiB`），校验 `crc32` 后写入本地 `ObjectStore` 并上报新增副本位置。
3. **Singleflight 并发去重**：若同一 Worker 上同时有多个任务依赖同一个远程 `ObjectId`，本地 `ObjectStore` 通过 `inflight_fetches_[object_id]` 合并为一个共享 `std::shared_future<Status>`，只发生一次跨网传输。

### 4.3 动态 DAG 状态机与 EFT 数据局部性调度

1. **构造期天然无环（Construction-Time Acyclicity）**：任务只能在提交时引用已存在（已获取 `TaskFuture`/`ObjectId`）的上游对象，无法构造反向环路。
2. **新增 `WAITING_DEPS` 状态**：
   - 提交任务含未就绪 `ObjectRef` 时，状态置为 `TASK_STATUS_WAITING_DEPS`（计入 `outstanding_` 预算以保证有界内存，但不进入就绪队列），在每个未完成上游对象上挂载反向唤醒边 `dependents[obj_id].push_back(task_id)`。
   - 上游对象全部 `COMPLETED` 时，`unresolved_deps` 减至 0，任务原子转入 `PENDING` 并唤醒调度器。
   - 任一上游任务终态 `FAILED` 时，级联将所有依赖它的下游任务直接标为终态 `FAILED`，立即归还 `outstanding_` 预算。
3. **EFT（Earliest Finish Time）局部性代价模型**：
   对候选健康且满足资源约束的 Worker \(w\)，评估任务 \(t\) 的期望完成时间：
   \[
   \text{EFT}(t, w) = \widehat{Q}(w) + \sum_{o \in \text{deps}(t)} \text{TransferCost}(o, w) + \widehat{E}(t)
   \]
   其中：
   - \(\widehat{Q}(w)\) 为节点 \(w\) 上当前活跃任务的预期剩余排队等待时间；
   - 若 \(w\) 本地已缓存 \(o\)，或 \(w.\text{host\_id}\) 上存在已封存的同机共享内存段，则 \(\text{TransferCost}(o, w) = 0\)；
   - 否则 \(\text{TransferCost}(o, w) = \frac{\text{size}(o)}{\widehat{B}_{\text{net}}}\)（默认初始带宽估算 `512 MiB/s`，可根据实际 `PullObject` 吞吐在线自适应更新）。
   - 选择 \(\arg\min_w \text{EFT}(t, w)\) 的节点派发，自动实现“小数据负载均衡优先、大数据移动计算优先”的连续权衡。
4. **对象血缘重算（Object Lineage Reconstruction）**：
   若某对象的所有持有 Worker 均失联，且仍有活跃/等待中的下游任务需要读取该对象，Coordinator 查阅 `ProducerMap[object_id]`：若生成该对象的上游任务为幂等任务且未超重算上限，将其从 `COMPLETED` 重新激活为新代 `PENDING` 重算数据并通知等待方。

---

## 5. Milestone V2.3：多维异构资源、有状态 Actor 与状态重建

### 5.1 多维资源调度（Bin-Packing vs. Spread）

1. **资源支配检查**：节点可用资源满足 `avail.slots >= req.slots && avail.milli_cpu >= req.milli_cpu && avail.mem_bytes >= req.mem_bytes` 方可接纳任务或常驻 Actor。
2. **两种放置打分器**：
   - **Bin-Packing（紧凑装箱）**：最大化分配后各维归一化已用率与请求向量的对齐度（余弦相似度 / 最小剩余碎片），保留完整的空闲大节点给高规格任务或大内存 Actor。
   - **Spread（容灾打散）**：最大化分配后节点的最小维度归一化剩余空闲率，避免同一批有状态 Actor 集中在单台物理进程上同时故障。

### 5.2 M:N 有状态 Actor 顺序邮箱模型

1. **零空闲线程占用的 Actor 调度**：
   - Worker 不为每个 Actor 创建独立 OS 线程，而是复用固定大小的 Worker 执行线程池。
   - 每个存活 Actor 对应一个 `ActorMailbox`：内含用户态 `std::unique_ptr<ActorInstance>`、期望执行的下一个序列号 `expected_seq_no`、乱序到达缓冲表 `std::map<uint64_t, ActorCall> pending_calls` 以及排他运行标志 `bool running`。
   - 当 `seq_no == expected_seq_no` 的消息到达且 `running == false` 时，置 `running = true` 并将该 Actor 投入 Worker 线程池执行；线程循环消费连续就绪的 `seq_no` 直至邮箱排空才释放 `running = false`。
   - **核心收益**：支持单 Worker 承载数千个轻量级有状态 Actor，同时严格保证同一 Actor 的方法调用单线程按序执行，用户态业务代码无需加锁。

### 5.3 `(actor_id, epoch, seq_no)` 隔离与 Lineage 状态自动重建

1. **世代隔离（Epoch Fencing）**：每次 Actor 实例化拥有唯一递增 `uint32_t epoch`。所有发往 Worker 的 Actor 调用与结果上报均携带 `(actor_id, epoch, seq_no)`，旧世代的滞留调用或上报一律拒绝。
2. **确定性重放日志与恢复流程**：
   - Coordinator 为存活 Actor 维护构造规格 `CreateActorRequest` 与按 `seq_no` 排序的 `mutates_state = true` 历史调用日志（上限 `max_lineage_length`，默认 `1024` 条；支持 Actor 主动上报序列化状态快照 `Checkpoint` 截断前缀日志）。
   - 当承载该 Actor 的 Worker 心跳过期且 `reconstructions < max_reconstructions`：
     1. Coordinator 将 Actor 状态置为 `RECONSTRUCTING`，递增 `epoch = old_epoch + 1`；
     2. 选取满足多维资源约束的新健康 Worker，先派发 `InstantiateActor(actor_id, epoch, init_spec, checkpoint, lineage_calls)`；
     3. 新 Worker 在本地顺序重放历史状态变更调用（重放期间屏蔽外部结果二次上报），将 `expected_seq_no` 推进至断点处；
     4. Coordinator 将所有尚未终结的在途 Actor 任务重定向至新 Worker 与新 `epoch`，客户端 `TaskFuture::Get()` 全程无感完成。

---

## 6. 代码模块组织与演进计划

在现有 `include/miniray/` 与 `src/` 基础上模块化拆分，保持单文件职责清晰、编译快速：

```text
include/miniray/
├── common.hpp          # 时钟、ID 生成、CRC32、CancellationToken
├── resources.hpp       # [V2.3] 多维资源向量运算与 Pack/Spread 评分器
├── scheduler.hpp       # RoundRobin / LeastLoaded / [V2.2] EFTLocalityScheduler
├── object_store.hpp    # [V2.2] 内联/POSIX shm 零拷贝封装、RAII 段管理、Singleflight 拉取
├── actor.hpp           # [V2.3] ActorInstance 基类、ActorRegistry 与 ActorMailbox
├── state.hpp           # CoordinatorState（三级队列、ActiveAttempt 集合、DAG 图、Actor 表）
├── coordinator.hpp     # CoordinatorService + Async Batch Dispatcher
├── worker.hpp          # WorkerService + DependencyFetcher + PullObject Server
└── client.hpp          # TaskFuture / ObjectHandle / ActorHandle SDK
```

---

## 7. 验证与基准测试矩阵

每个里程碑合并前必须全部通过以下四层验证（包含 `-DMINIRAY_SANITIZER=address` 与 `thread` 独立构建）：

| 阶段 | 新增正确性与故障注入测试 | 新增性能基准与可量化验收目标 |
|---|---|---|
| **V2.1** | 1. 并发投机副本竞速测试：验证先到者胜出、慢副本结果被拦截、远程槽位精确归还无泄漏<br>2. 过载保护测试：验证满载时投机自动关闭、不突破 `max_pending`<br>3. 抢占恢复测试：队列积压下注入 Worker 过期，验证重试任务优先出队 | 1. 复跑原 210 批套件对比：`tiny/serial` P50 `< 0.5ms`（原 `14.3ms`），`tiny/burst` 吞吐 `> 25,000 tasks/s`（原 `3,000`）<br>2. 故障恢复延迟：从 `4.2s` 降至 `< 0.8s`<br>3. Straggler 注入基准（5% 慢执行）：对比开启/关闭 Hedging 的 P99 尾延迟降幅 |
| **V2.2** | 1. POSIX `shm` 读写封存测试、CRC32 损坏检测测试、进程 `SIGKILL` 后孤儿段清扫测试<br>2. 跨节点 `PullObject` 流式传输与 `Singleflight` 并发去重测试<br>3. 多层菱形 DAG 解锁、上游失败级联传播、持有者崩溃触发 Lineage 重算测试 | 1. 64 MiB 载荷同机零拷贝 vs. RPC 传值吞吐与 RSS 内存峰值对比<br>2. 两阶段 MapReduce DAG 在 EFT 局部性调度 vs. Least-Loaded 下的跨节点传输字节数与完成时间对比 |
| **V2.3** | 1. 单 Actor 1,000 并发乱序到达请求的严格 `seq_no` 串行一致性测试（无锁计数器校验）<br>2. 多维资源 Pack vs. Spread 放置确定性测试<br>3. Actor 宿主进程真实 `SIGKILL` 后跨节点 `epoch` 切换与 Lineage 状态无损重建测试 | 1. 单 Worker 承载 1,000 个常驻 Actor 的内存开销与消息吞吐测试<br>2. Actor 故障重建恢复耗时（随 Lineage 日志长度 10 → 1,000 与开启 Checkpoint 的对比曲线） |
