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

namespace miniray {
using Clock = std::chrono::steady_clock;
struct CoordinatorOptions {
  size_t max_pending = 1024; // Includes reservations for running tasks that may need retrying.
  std::chrono::milliseconds heartbeat_timeout{2500};
  bool round_robin = false;
};
struct Dispatch {
  rpc::TaskSpec task;
  std::string worker_id;
  std::string address;
};
class CoordinatorState {
 public:
  explicit CoordinatorState(CoordinatorOptions options = {});
  grpc::Status Submit(const rpc::SubmitTaskRequest&, rpc::SubmitTaskResponse&);
  grpc::Status Get(const rpc::GetTaskResultRequest&, rpc::GetTaskResultResponse&);
  grpc::Status Heartbeat(const rpc::HeartbeatRequest&, rpc::HeartbeatResponse&, Clock::time_point now = Clock::now());
  void Report(const rpc::ReportResultRequest&, rpc::ReportResultResponse&);
  std::optional<Dispatch> NextDispatch();
  void Dispatched(const Dispatch&, bool accepted);
  void Expire(Clock::time_point now = Clock::now());
  void Stop();
 private:
  struct Task {
    rpc::SubmitTaskRequest request;
    rpc::GetTaskResultResponse result;
    Clock::time_point queued_at;
    Clock::time_point submitted_at;
    double queue_ms = 0;
  };
  struct Worker { WorkerNode node; Clock::time_point heartbeat; Clock::time_point unavailable_until{}; };
  void Retry(Task&, const std::string& error);
  void Release(const Task&);
  CoordinatorOptions options_;
  std::unique_ptr<Scheduler> scheduler_;
  std::mutex mutex_;
  std::condition_variable changed_;
  std::map<std::string, Task> tasks_;
  std::map<std::string, Worker> workers_;
  std::deque<std::string> pending_;
  size_t outstanding_ = 0;
  bool stopped_ = false;
};
}  // namespace miniray
