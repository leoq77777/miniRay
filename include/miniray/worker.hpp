#pragma once
#include <grpcpp/grpcpp.h>
#include "miniray.grpc.pb.h"
#include "miniray/registry.hpp"
#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace miniray {
struct WorkerOptions {
  std::string coordinator = "localhost:50051";
  std::string address = "localhost:50052";
  std::string id; // Empty generates a unique process incarnation.
  uint32_t capacity = 2;
  std::chrono::milliseconds heartbeat_interval{500};
};
class WorkerService final : public rpc::WorkerService::Service {
 public:
  WorkerService(WorkerOptions options, const TaskRegistry& registry);
  ~WorkerService() override;
  void Start(const std::string& advertised_address = ""); // Call after the listening socket is ready.
  void Stop(); // Drain accepted work and report results before stopping heartbeats.
  const std::string& id() const { return options_.id; }
  grpc::Status DispatchTask(grpc::ServerContext*, const rpc::DispatchTaskRequest*, rpc::DispatchTaskResponse*) override;
  grpc::Status DispatchBatch(grpc::ServerContext*, const rpc::DispatchBatchRequest*, rpc::DispatchBatchResponse*) override;
  grpc::Status CancelTask(grpc::ServerContext*, const rpc::CancelTaskRequest*, rpc::CancelTaskResponse*) override;
 private:
  static std::string AttemptKey(const std::string& task_id, uint32_t attempt_id);
  static bool ValidTaskSpec(const rpc::TaskSpec& task);
  bool AdmitLocked(const rpc::TaskSpec& task, rpc::DispatchTaskResponse& response);
  void ExecuteLoop();
  void Report(const rpc::ReportResultRequest&);
  WorkerOptions options_;
  const TaskRegistry& registry_;
  std::unique_ptr<rpc::CoordinatorService::Stub> coordinator_;
  std::mutex mutex_;
  std::condition_variable ready_;
  std::deque<rpc::TaskSpec> queue_;
  std::unordered_set<std::string> seen_;
  std::unordered_set<std::string> cancelled_;
  std::unordered_map<std::string, CancellationToken> running_tokens_;
  uint32_t active_ = 0;
  bool stopping_ = false;
  std::atomic<bool> isolated_{false};
  std::vector<std::thread> executors_;
  std::jthread heartbeat_;
};
}  // namespace miniray
