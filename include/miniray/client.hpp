#pragma once
#include "miniray.grpc.pb.h"
#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>

namespace miniray {
class RpcError : public std::runtime_error {
 public:
  explicit RpcError(const grpc::Status& status) : std::runtime_error(status.error_message()), code(status.error_code()) {}
  grpc::StatusCode code;
};
class TaskFuture {
 public:
  TaskFuture(std::shared_ptr<rpc::CoordinatorService::Stub> stub, std::string id) : stub_(std::move(stub)), id_(std::move(id)) {}
  const std::string& id() const { return id_; }
  rpc::GetTaskResultResponse Wait(std::chrono::milliseconds timeout = std::chrono::seconds(30)) const;
  std::string Get(std::chrono::milliseconds timeout = std::chrono::seconds(30)) const;
 private:
  std::shared_ptr<rpc::CoordinatorService::Stub> stub_;
  std::string id_;
};
class MiniRayClient {
 public:
  explicit MiniRayClient(const std::string& address);
  TaskFuture Submit(const std::string& type, const std::string& payload, uint32_t max_retries = 2,
                    bool retryable = true, std::string task_id = "") const;
  TaskFuture Lookup(const std::string& id) const { return TaskFuture(stub_, id); }
 private:
  std::shared_ptr<rpc::CoordinatorService::Stub> stub_;
};
}  // namespace miniray
