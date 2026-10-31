#include "miniray/coordinator.hpp"
#include "miniray/common.hpp"
#include <unordered_map>

namespace miniray {
CoordinatorService::CoordinatorService(CoordinatorOptions options) : state_(options), dispatcher_([this](std::stop_token stop) {
  std::unordered_map<std::string, std::unique_ptr<rpc::WorkerService::Stub>> stubs;
  while (!stop.stop_requested()) {
    state_.Expire();
    auto dispatch = state_.NextDispatch();
    if (!dispatch) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      continue;
    }
    auto& stub = stubs[dispatch->worker_id];
    if (!stub) stub = rpc::WorkerService::NewStub(grpc::CreateChannel(dispatch->address, grpc::InsecureChannelCredentials()));
    grpc::ClientContext context;
    Deadline(context, std::chrono::milliseconds(200));
    rpc::DispatchTaskRequest request;
    *request.mutable_task() = dispatch->task;
    rpc::DispatchTaskResponse response;
    auto status = stub->DispatchTask(&context, request, &response);
    state_.Dispatched(*dispatch, status.ok() && response.accepted());
  }
}) {}
CoordinatorService::~CoordinatorService() { Stop(); }
void CoordinatorService::Stop() {
  dispatcher_.request_stop();
  if (dispatcher_.joinable()) dispatcher_.join();
  state_.Stop();
}
grpc::Status CoordinatorService::SubmitTask(grpc::ServerContext*, const rpc::SubmitTaskRequest* r, rpc::SubmitTaskResponse* s) { return state_.Submit(*r, *s); }
grpc::Status CoordinatorService::GetTaskResult(grpc::ServerContext*, const rpc::GetTaskResultRequest* r, rpc::GetTaskResultResponse* s) { return state_.Get(*r, *s); }
grpc::Status CoordinatorService::SendHeartbeat(grpc::ServerContext*, const rpc::HeartbeatRequest* r, rpc::HeartbeatResponse* s) { return state_.Heartbeat(*r, *s); }
grpc::Status CoordinatorService::ReportResult(grpc::ServerContext*, const rpc::ReportResultRequest* r, rpc::ReportResultResponse* s) {
  state_.Report(*r, *s);
  return grpc::Status::OK;
}
}  // namespace miniray
