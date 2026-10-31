# Architecture

Client → CoordinatorService → WorkerService → CoordinatorService.ReportResult → Client。

`proto/miniray.proto` 定义任务、RPC 和状态。`CoordinatorState` 是可独立测试的内存状态机；一个互斥锁保护任务、worker、队列及容量预留，保证检查与变更的原子性。`CoordinatorService` 的调度线程在锁外进行有截止时间的 RPC，因此慢 worker 不会阻塞提交、心跳和结果接收。

任务入队后为 PENDING；每次调度先在锁内预留一个 worker 槽位并记录 DISPATCHED，然后释放锁发 RPC，收到确认标记 RUNNING。worker 可能先完成再返回派发确认，确认处理必须重新检查任务状态。COMPLETED 和 FAILED 是终态。运行失败且可以重试时直接变为下一代 PENDING，FAILED 仅表示最终失败。

Worker 列表按 ID 排序。RoundRobin 在完整列表上移动游标并跳过死亡/满载节点；LeastLoaded 使用整数交叉相乘比较 active/capacity，避免浮点平局不稳定，平局按 ID 决定。两者严格尊重容量。

协调器的 `max_pending` 是未终结任务的总接纳预算，运行中的任务占用一个重试预留。这个比仅限制等待队列更保守的规则保证大量 worker 同时故障时也不丢已接纳任务、不突破队列上限。只有终结时才归还预算。终态记录继续保留以支持查询和幂等提交，所以它不是整个进程内存的总上限。

Worker 使用 capacity 个固定线程和大小至多 capacity 的本地接纳集合；含执行中任务的 active 计数严格限额。函数结束后先释放本地槽位再上报，这样协调器收到完成消息后立即派发也不会撞上尚未归还的本地槽位。上报期间线程仍被占用，新任务可以在有界本地队列暂候。心跳线程独立于执行线程，长任务不会阻断正常心跳。

注册表只在查找和复制函数时持有共享锁，执行期间不持锁。RPC 和函数执行均不在协调器状态锁下运行。每个 RPC 客户端调用使用独立 context，可并发共享 stub。

输入验证包括任务类型/ID 长度、1 MiB payload、重试上限 100、worker 容量 1–1024；CLI 对未知参数、重复参数和非正整数报错。内置函数严格解析完整十进制字符串并限制计算范围。
