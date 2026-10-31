#pragma once
#include <grpcpp/grpcpp.h>
#include "miniray/state.hpp"
#include <thread>

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
  CoordinatorState state_;
  std::jthread dispatcher_;
};
}  // namespace miniray
