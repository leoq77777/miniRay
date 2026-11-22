#include "miniray/coordinator.hpp"
#include "miniray/common.hpp"

namespace miniray {
namespace {
struct AsyncBatchCall {
  grpc::ClientContext context;
  rpc::DispatchBatchRequest request;
  rpc::DispatchBatchResponse response;
  std::vector<Dispatch> items;
};

struct AsyncCancelCall {
  grpc::ClientContext context;
  rpc::CancelTaskRequest request;
  rpc::CancelTaskResponse response;
  CancelWork cancel;
};
}  // namespace

CoordinatorService::CoordinatorService(CoordinatorOptions options)
    : state_(options), dispatcher_([this](std::stop_token stop) {
        while (!stop.stop_requested()) {
          auto work = state_.WaitNextWork(stop);
          if (work.dispatches.empty() && work.cancels.empty()) continue;
          for (auto& cancel : work.cancels) CancelTaskAsync(std::move(cancel));
          for (auto& batch : work.dispatches) DispatchBatchAsync(std::move(batch));
        }
      }) {}

CoordinatorService::~CoordinatorService() { Stop(); }

std::shared_ptr<rpc::WorkerService::Stub> CoordinatorService::GetOrCreateStub(const std::string& worker_id,
                                                                              const std::string& address) {
  std::lock_guard lock(rpc_mutex_);
  auto& stub = stubs_[worker_id];
  if (!stub) {
    stub = std::shared_ptr<rpc::WorkerService::Stub>(
        rpc::WorkerService::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials())));
  }
  return stub;
}

void CoordinatorService::DispatchBatchAsync(DispatchBatchWork batch) {
  auto stub = GetOrCreateStub(batch.worker_id, batch.address);
  auto call = std::make_shared<AsyncBatchCall>();
  Deadline(call->context, std::chrono::milliseconds(200));
  call->items = std::move(batch.items);
  for (const auto& item : call->items) {
    *call->request.add_tasks() = item.task;
  }
  {
    std::lock_guard lock(rpc_mutex_);
    if (stopping_rpcs_) {
      rpc::DispatchBatchResponse empty;
      state_.DispatchedBatch(call->items, grpc::Status(grpc::StatusCode::CANCELLED, "stopping"), empty);
      return;
    }
    ++inflight_rpcs_;
    active_contexts_.insert(&call->context);
  }
  stub->async()->DispatchBatch(&call->context, &call->request, &call->response,
                               [this, stub, call](grpc::Status status) {
                                 state_.DispatchedBatch(call->items, status, call->response);
                                 std::lock_guard lock(rpc_mutex_);
                                 active_contexts_.erase(&call->context);
                                 if (--inflight_rpcs_ == 0) rpc_cv_.notify_all();
                               });
}

void CoordinatorService::CancelTaskAsync(CancelWork cancel) {
  auto stub = GetOrCreateStub(cancel.worker_id, cancel.address);
  auto call = std::make_shared<AsyncCancelCall>();
  Deadline(call->context, std::chrono::milliseconds(200));
  call->request.set_task_id(cancel.task_id);
  call->request.set_attempt_id(cancel.attempt_id);
  call->request.set_reason(cancel.reason);
  call->cancel = std::move(cancel);
  {
    std::lock_guard lock(rpc_mutex_);
    if (stopping_rpcs_) return;
    ++inflight_rpcs_;
    active_contexts_.insert(&call->context);
  }
  stub->async()->CancelTask(&call->context, &call->request, &call->response,
                            [this, stub, call](grpc::Status status) {
                              if (status.ok()) state_.CancelAck(call->cancel, call->response.slot_freed());
                              std::lock_guard lock(rpc_mutex_);
                              active_contexts_.erase(&call->context);
                              if (--inflight_rpcs_ == 0) rpc_cv_.notify_all();
                            });
}

void CoordinatorService::Stop() {
  state_.Stop();
  dispatcher_.request_stop();
  if (dispatcher_.joinable()) dispatcher_.join();
  std::unique_lock lock(rpc_mutex_);
  stopping_rpcs_ = true;
  for (auto* context : active_contexts_) context->TryCancel();
  rpc_cv_.wait(lock, [this] { return inflight_rpcs_ == 0; });
}

grpc::Status CoordinatorService::SubmitTask(grpc::ServerContext*, const rpc::SubmitTaskRequest* r,
                                            rpc::SubmitTaskResponse* s) {
  return state_.Submit(*r, *s);
}

grpc::Status CoordinatorService::GetTaskResult(grpc::ServerContext*, const rpc::GetTaskResultRequest* r,
                                               rpc::GetTaskResultResponse* s) {
  return state_.Get(*r, *s);
}

grpc::Status CoordinatorService::SendHeartbeat(grpc::ServerContext*, const rpc::HeartbeatRequest* r,
                                               rpc::HeartbeatResponse* s) {
  return state_.Heartbeat(*r, *s);
}

grpc::Status CoordinatorService::ReportResult(grpc::ServerContext*, const rpc::ReportResultRequest* r,
                                              rpc::ReportResultResponse* s) {
  state_.Report(*r, *s);
  return grpc::Status::OK;
}
}  // namespace miniray
