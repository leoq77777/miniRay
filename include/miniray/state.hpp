#pragma once
#include "miniray.grpc.pb.h"
#include "miniray/scheduler.hpp"
#include <chrono>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <utility>
#include <vector>

namespace miniray {
using Clock = std::chrono::steady_clock;

struct CoordinatorOptions {
  size_t max_pending = 1024; // Includes reservations for running tasks that may need retrying.
  std::chrono::milliseconds heartbeat_timeout{2500};
  bool round_robin = false;
  uint32_t max_batch_size = 16;
  std::chrono::milliseconds fast_task_threshold{10};
  std::chrono::milliseconds aging_threshold{50};
  bool enable_speculation = false;
  double speculation_multiplier = 1.5;
  std::chrono::milliseconds min_speculation_delay{15};
  size_t min_profile_samples = 8;
};

struct Dispatch {
  rpc::TaskSpec task;
  std::string worker_id;
  std::string address;
};

struct DispatchBatchWork {
  std::string worker_id;
  std::string address;
  std::vector<Dispatch> items;
};

struct CancelWork {
  std::string task_id;
  uint32_t attempt_id = 0;
  std::string worker_id;
  std::string address;
  std::string reason;
};

struct CoordinatorWork {
  std::vector<DispatchBatchWork> dispatches;
  std::vector<CancelWork> cancels;
};

class CoordinatorState {
 public:
  explicit CoordinatorState(CoordinatorOptions options = {});
  grpc::Status Submit(const rpc::SubmitTaskRequest&, rpc::SubmitTaskResponse&, Clock::time_point now = Clock::now());
  grpc::Status Get(const rpc::GetTaskResultRequest&, rpc::GetTaskResultResponse&);
  grpc::Status Heartbeat(const rpc::HeartbeatRequest&, rpc::HeartbeatResponse&, Clock::time_point now = Clock::now());
  void Report(const rpc::ReportResultRequest&, rpc::ReportResultResponse&, Clock::time_point now = Clock::now());
  std::optional<Dispatch> NextDispatch(Clock::time_point now = Clock::now());
  void Dispatched(const Dispatch&, bool accepted, Clock::time_point now = Clock::now());
  void DispatchedBatch(const std::vector<Dispatch>& items, const grpc::Status& status,
                       const rpc::DispatchBatchResponse& response, Clock::time_point now = Clock::now());
  CoordinatorWork CollectWork(Clock::time_point now = Clock::now());
  CoordinatorWork WaitNextWork(std::stop_token stop_token);
  std::vector<CancelWork> DrainCancels();
  void CancelAck(const CancelWork& cancel, bool slot_freed);
  void Expire(Clock::time_point now = Clock::now());
  void Stop();

 private:
  struct AttemptInfo {
    uint32_t attempt_id = 0;
    std::string worker_id;
    std::string address;
    rpc::TaskStatus status = rpc::TASK_STATUS_DISPATCHED;
    Clock::time_point started_at;
    bool is_speculative = false;
    bool hedged = false;
  };
  struct Task {
    rpc::SubmitTaskRequest request;
    rpc::GetTaskResultResponse result;
    Clock::time_point queued_at;
    Clock::time_point submitted_at;
    double queue_ms = 0;
    uint32_t pending_attempt_id = 0;
    uint32_t next_attempt_id = 1;
    uint32_t failures_count = 0;
    std::map<uint32_t, AttemptInfo> active_attempts;
  };
  struct TaskProfile {
    double ewma_ms = 0.0;
    std::deque<double> recent_ms;
    size_t count = 0;
    void Record(double ms);
    double P95() const;
  };
  struct Worker {
    WorkerNode node;
    Clock::time_point heartbeat;
    Clock::time_point unavailable_until{};
  };
  using AttemptKey = std::pair<std::string, uint32_t>;

  bool HasReadyTasksLocked() const;
  void EnqueueReadyLocked(const std::string& id, bool urgent);
  std::string PopReadyTaskLocked(Clock::time_point now);
  std::vector<WorkerNode> BuildCandidateNodesLocked(Clock::time_point now,
                                                    const std::map<std::string, size_t>& batch_counts,
                                                    const std::string& exclude_worker = "") const;
  std::optional<Dispatch> SelectReadyDispatchLocked(Clock::time_point now,
                                                    const std::map<std::string, size_t>& batch_counts);
  std::optional<Dispatch> SelectSpeculativeDispatchLocked(Clock::time_point now,
                                                          const std::map<std::string, size_t>& batch_counts);
  CoordinatorWork CollectWorkLocked(Clock::time_point now);
  Clock::time_point NextWakeupLocked(Clock::time_point now) const;
  void ReleaseWorkerSlotLocked(const std::string& worker_id);
  void FailOrRetryLocked(Task& task, const std::string& error, bool worker_expired, Clock::time_point now);
  void DispatchedLocked(const Dispatch& dispatch, bool accepted, Clock::time_point now);
  void ExpireLocked(Clock::time_point now);

  CoordinatorOptions options_;
  std::unique_ptr<Scheduler> scheduler_;
  std::mutex mutex_;
  std::condition_variable changed_;
  std::condition_variable dispatch_cv_;
  std::map<std::string, Task> tasks_;
  std::map<std::string, Worker> workers_;
  std::map<std::string, TaskProfile> profiles_;
  std::deque<std::string> recovery_queue_;
  std::deque<std::string> fast_queue_;
  std::deque<std::string> normal_queue_;
  std::map<AttemptKey, std::string> cancelling_slots_;
  std::vector<CancelWork> pending_cancels_;
  size_t outstanding_ = 0;
  bool stopped_ = false;
};
}  // namespace miniray
