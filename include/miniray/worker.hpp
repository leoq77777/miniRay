#pragma once
#include <grpcpp/grpcpp.h>
#include "miniray.grpc.pb.h"
#include "miniray/registry.hpp"
#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>
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
 private:
  void ExecuteLoop();
  void Report(const rpc::ReportResultRequest&);
  WorkerOptions options_;
  const TaskRegistry& registry_;
  std::unique_ptr<rpc::CoordinatorService::Stub> coordinator_;
  std::mutex mutex_;
  std::condition_variable ready_;
  std::deque<rpc::TaskSpec> queue_;
  std::unordered_set<std::string> seen_;
  uint32_t active_ = 0;
  bool stopping_ = false;
  std::atomic<bool> isolated_{false};
  std::vector<std::thread> executors_;
  std::jthread heartbeat_;
};
}  // namespace miniray
