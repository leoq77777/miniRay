#pragma once
#include <grpcpp/grpcpp.h>
#include "miniray/state.hpp"
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace miniray {
class CoordinatorService final : public rpc::CoordinatorService::Service {
 public:
  explicit CoordinatorService(CoordinatorOptions options = {});
  ~CoordinatorService() override;
  void Stop();
  grpc::Status SubmitTask(grpc::ServerContext*, const rpc::SubmitTaskRequest*, rpc::SubmitTaskResponse*) override;
  grpc::Status GetTaskResult(grpc::ServerContext*, const rpc::GetTaskResultRequest*, rpc::GetTaskResultResponse*) override;
  grpc::Status SendHeartbeat(grpc::ServerContext*, const rpc::HeartbeatRequest*, rpc::HeartbeatResponse*) override;
  grpc::Status ReportResult(grpc::ServerContext*, const rpc::ReportResultRequest*, rpc::ReportResultResponse*) override;

 private:
  std::shared_ptr<rpc::WorkerService::Stub> GetOrCreateStub(const std::string& worker_id, const std::string& address);
  void DispatchBatchAsync(DispatchBatchWork batch);
  void CancelTaskAsync(CancelWork cancel);

  CoordinatorState state_;
  std::mutex rpc_mutex_;
  std::condition_variable rpc_cv_;
  size_t inflight_rpcs_ = 0;
  bool stopping_rpcs_ = false;
  std::unordered_set<grpc::ClientContext*> active_contexts_;
  std::unordered_map<std::string, std::shared_ptr<rpc::WorkerService::Stub>> stubs_;
  std::jthread dispatcher_;
};
}  // namespace miniray
