#include "miniray/worker.hpp"
#include "miniray/common.hpp"
#include <cstdio>

namespace miniray {
WorkerService::WorkerService(WorkerOptions options, const TaskRegistry& registry)
    : options_(std::move(options)), registry_(registry) {
  if (!options_.capacity || options_.capacity > 1024 || options_.heartbeat_interval.count() <= 0)
    throw std::invalid_argument("invalid worker capacity or heartbeat interval");
  if (options_.id.empty()) options_.id = UniqueId();
  coordinator_ = rpc::CoordinatorService::NewStub(grpc::CreateChannel(options_.coordinator, grpc::InsecureChannelCredentials()));
}
WorkerService::~WorkerService() { Stop(); }
void WorkerService::Start(const std::string& advertised_address) {
  std::lock_guard lock(mutex_);
  if (!executors_.empty() || stopping_) throw std::logic_error("worker already started/stopped");
  if (!advertised_address.empty()) options_.address = advertised_address;
  for (uint32_t i = 0; i < options_.capacity; ++i) executors_.emplace_back([this] { ExecuteLoop(); });
  heartbeat_ = std::jthread([this](std::stop_token stop) {
    while (!stop.stop_requested() && !isolated_) {
      rpc::HeartbeatRequest request;
      request.set_worker_id(options_.id);
      request.set_worker_address(options_.address);
      request.set_capacity(options_.capacity);
      { std::lock_guard guard(mutex_); request.set_active_tasks(active_); }
      rpc::HeartbeatResponse response;
      grpc::ClientContext context;
      Deadline(context, std::chrono::milliseconds(500));
      auto status = coordinator_->SendHeartbeat(&context, request, &response);
      if (status.error_code() == grpc::StatusCode::FAILED_PRECONDITION) {
        isolated_ = true;
        std::fputs("Worker incarnation expired; restart worker to rejoin\n", stderr);
        break;
      }
      auto until = std::chrono::steady_clock::now() + options_.heartbeat_interval;
      while (!stop.stop_requested() && std::chrono::steady_clock::now() < until)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  });
}
void WorkerService::Stop() {
  { std::lock_guard lock(mutex_); stopping_ = true; }
  ready_.notify_all();
  for (auto& thread : executors_) if (thread.joinable()) thread.join();
  heartbeat_.request_stop();
  if (heartbeat_.joinable()) heartbeat_.join();
}
grpc::Status WorkerService::DispatchTask(grpc::ServerContext*, const rpc::DispatchTaskRequest* request, rpc::DispatchTaskResponse* response) {
  const auto& task = request->task();
  if (task.task_id().empty() || task.task_id().size() > 128 || task.task_type().empty() || task.task_type().size() > 256 || task.payload().size() > 1024 * 1024)
    return {grpc::StatusCode::INVALID_ARGUMENT, "invalid task specification"};
  std::lock_guard lock(mutex_);
  const auto key = std::to_string(task.task_id().size()) + ":" + task.task_id() + ":" + std::to_string(task.attempt_id());
  if (seen_.contains(key)) { response->set_accepted(true); return grpc::Status::OK; }
  if (stopping_ || isolated_ || executors_.empty() || active_ >= options_.capacity) {
    response->set_error_message("worker full or unavailable");
    return grpc::Status::OK;
  }
  ++active_;
  seen_.insert(key);
  queue_.push_back(task);
  response->set_accepted(true);
  ready_.notify_one();
  return grpc::Status::OK;
}
void WorkerService::Report(const rpc::ReportResultRequest& request) {
  for (int retry = 0; retry < 10; ++retry) {
    grpc::ClientContext context;
    Deadline(context, std::chrono::milliseconds(500));
    rpc::ReportResultResponse response;
    auto status = coordinator_->ReportResult(&context, request, &response);
    // accepted=false fences a stale attempt; a duplicate terminal report is also harmless.
    if (status.ok()) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  // Stop heartbeats so an unreported completion is recovered rather than stranded.
  isolated_ = true;
  std::fputs("Result reporting failed; worker isolated for recovery\n", stderr);
}
void WorkerService::ExecuteLoop() {
  while (true) {
    rpc::TaskSpec task;
    {
      std::unique_lock lock(mutex_);
      ready_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
      if (queue_.empty()) return;
      task = std::move(queue_.front());
      queue_.pop_front();
    }
    std::fprintf(stderr, "Executing task=%s attempt=%u\n", task.task_id().c_str(), task.attempt_id());
    rpc::ReportResultRequest result;
    result.set_task_id(task.task_id());
    result.set_attempt_id(task.attempt_id());
    result.set_worker_id(options_.id);
    auto execution_start = std::chrono::steady_clock::now();
    try {
      if (!registry_.HasTask(task.task_type())) throw std::invalid_argument("unknown task type: " + task.task_type());
      auto payload = registry_.Execute(task.task_type(), task.payload());
      if (payload.size() > 1024 * 1024) throw std::invalid_argument("result exceeds 1 MiB");
      result.set_result(std::move(payload));
      result.set_status(rpc::TASK_STATUS_COMPLETED);
    } catch (const std::invalid_argument& error) {
      result.set_status(rpc::TASK_STATUS_FAILED);
      result.set_error_message(error.what());
    } catch (const std::exception& error) {
      result.set_status(rpc::TASK_STATUS_FAILED);
      result.set_error_message(error.what());
      result.set_retryable(true);
    } catch (...) {
      result.set_status(rpc::TASK_STATUS_FAILED);
      result.set_error_message("unknown task exception");
      result.set_retryable(true);
    }
    result.set_execution_ms(std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - execution_start).count());
    // Release the local slot before reporting: the coordinator may immediately send more work.
    { std::lock_guard lock(mutex_); --active_; }
    Report(result);
  }
}
}  // namespace miniray
