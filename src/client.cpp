#include "miniray/client.hpp"
#include "miniray/common.hpp"
#include <algorithm>

namespace miniray {
MiniRayClient::MiniRayClient(const std::string& address)
    : stub_(rpc::CoordinatorService::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()))) {}

TaskFuture MiniRayClient::Submit(const std::string& type, const std::string& payload, uint32_t retries,
                                 bool retryable, std::string id) const {
  return SubmitDag(type, payload, std::vector<std::string>{}, retries, retryable, std::move(id));
}

TaskFuture MiniRayClient::SubmitDag(const std::string& type, const std::string& payload,
                                    const std::vector<std::string>& dependencies, uint32_t retries,
                                    bool retryable, std::string id) const {
  rpc::SubmitTaskRequest request;
  request.set_task_type(type);
  request.set_payload(payload);
  request.set_max_retries(retries);
  request.set_retryable(retryable);
  request.set_task_id(id.empty() ? UniqueId() : std::move(id));
  for (const auto& dep : dependencies) request.add_dependencies(dep);
  for (int retry = 0;; ++retry) {
    grpc::ClientContext context;
    Deadline(context, std::chrono::seconds(2));
    rpc::SubmitTaskResponse response;
    auto status = stub_->SubmitTask(&context, request, &response);
    if (status.ok()) return TaskFuture(stub_, response.task_id());
    if (retry >= 2 ||
        (status.error_code() != grpc::StatusCode::UNAVAILABLE &&
         status.error_code() != grpc::StatusCode::DEADLINE_EXCEEDED))
      throw RpcError(grpc::Status(status.error_code(), status.error_message() + "; task_id=" + request.task_id()));
  }
}

TaskFuture MiniRayClient::SubmitDag(const std::string& type, const std::string& payload,
                                    const std::vector<TaskFuture>& dependencies, uint32_t retries,
                                    bool retryable, std::string id) const {
  std::vector<std::string> dep_ids;
  dep_ids.reserve(dependencies.size());
  for (const auto& f : dependencies) dep_ids.push_back(f.id());
  return SubmitDag(type, payload, dep_ids, retries, retryable, std::move(id));
}

rpc::GetTaskResultResponse TaskFuture::Wait(std::chrono::milliseconds timeout) const {
  if (timeout.count() <= 0) throw std::invalid_argument("timeout must be positive");
  const auto end = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < end) {
    auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(end - std::chrono::steady_clock::now());
    grpc::ClientContext context;
    Deadline(context, std::max(remaining, std::chrono::milliseconds(1)));
    rpc::GetTaskResultRequest request;
    request.set_task_id(id_);
    request.set_timeout_ms(static_cast<uint32_t>(std::clamp<int64_t>(remaining.count(), 1, 1000)));
    rpc::GetTaskResultResponse response;
    auto status = stub_->GetTaskResult(&context, request, &response);
    if (!status.ok()) throw RpcError(status);
    if (response.status() == rpc::TASK_STATUS_COMPLETED || response.status() == rpc::TASK_STATUS_FAILED)
      return response;
  }
  throw RpcError(grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED, "task wait timed out (task remains active)"));
}

ObjectView TaskFuture::GetView(std::chrono::milliseconds timeout) const {
  const auto start = std::chrono::steady_clock::now();
  auto result = Wait(timeout);
  if (result.status() == rpc::TASK_STATUS_FAILED) throw std::runtime_error(result.error_message());
  if (!result.has_result_ref()) return ObjectView::FromInline(result.result());

  const auto& ref = result.result_ref();
  if (!ref.host_id().empty() && ref.host_id() == LocalHostId() && !ref.shm_name().empty()) {
    try {
      return ObjectStore::MapReadOnly(ref.shm_name(), ref.size_bytes(), ref.crc32(), true);
    } catch (...) {
      // Fall back to streaming PullObject if local segment is unavailable.
    }
  }
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
  auto remaining = std::max(std::chrono::milliseconds(1000), timeout - elapsed);
  return ObjectView::FromInline(ObjectStore::PullFromRemoteWorkers(ref, remaining));
}

std::string TaskFuture::Get(std::chrono::milliseconds timeout) const {
  auto view = GetView(timeout);
  return std::string(view.view());
}
}  // namespace miniray
