#include "miniray/state.hpp"
#include "miniray/common.hpp"
#include <algorithm>
#include <stdexcept>

namespace miniray {
namespace {
bool Active(rpc::TaskStatus s) {
  return s == rpc::TASK_STATUS_DISPATCHED || s == rpc::TASK_STATUS_RUNNING;
}
bool Terminal(rpc::TaskStatus s) {
  return s == rpc::TASK_STATUS_COMPLETED || s == rpc::TASK_STATUS_FAILED;
}
}
CoordinatorState::CoordinatorState(CoordinatorOptions options) : options_(options) {
  if (!options.max_pending || options.heartbeat_timeout.count() <= 0) throw std::invalid_argument("invalid coordinator options");
  if (options.round_robin) scheduler_ = std::make_unique<RoundRobinScheduler>();
  else scheduler_ = std::make_unique<LeastLoadedScheduler>();
}
grpc::Status CoordinatorState::Submit(const rpc::SubmitTaskRequest& request, rpc::SubmitTaskResponse& response) {
  if (request.task_type().empty() || request.task_type().size() > 256 || request.task_id().size() > 128 ||
      request.payload().size() > 1024 * 1024 || request.max_retries() > 100)
    return {grpc::StatusCode::INVALID_ARGUMENT, "invalid task type, ID, payload (>1 MiB), or retry limit (>100)"};
  std::lock_guard lock(mutex_);
  if (stopped_) return {grpc::StatusCode::UNAVAILABLE, "coordinator stopping"};
  std::string id = request.task_id();
  if (id.empty()) {
    do { id = UniqueId(); } while (tasks_.contains(id));
  }
  auto existing = tasks_.find(id);
  if (existing != tasks_.end()) {
    const auto& old = existing->second.request;
    if (old.task_type() != request.task_type() || old.payload() != request.payload() ||
        old.retryable() != request.retryable() || old.max_retries() != request.max_retries())
      return {grpc::StatusCode::ALREADY_EXISTS, "task ID belongs to a different submission"};
  } else {
    if (outstanding_ >= options_.max_pending) return {grpc::StatusCode::RESOURCE_EXHAUSTED, "admission queue full"};
    Task task;
    task.request = request;
    task.result.set_task_id(id);
    task.result.set_status(rpc::TASK_STATUS_PENDING);
    task.queued_at = Clock::now();
    task.submitted_at = task.queued_at;
    tasks_.emplace(id, std::move(task));
    pending_.push_back(id);
    ++outstanding_;
  }
  response.set_task_id(id);
  response.set_accepted(true);
  return grpc::Status::OK;
}
grpc::Status CoordinatorState::Get(const rpc::GetTaskResultRequest& request, rpc::GetTaskResultResponse& response) {
  std::unique_lock lock(mutex_);
  auto it = tasks_.find(request.task_id());
  if (it == tasks_.end()) return {grpc::StatusCode::NOT_FOUND, "unknown task ID"};
  if (request.timeout_ms()) {
    changed_.wait_for(lock, std::chrono::milliseconds(std::min(request.timeout_ms(), 1000u)), [&] {
      return stopped_ || Terminal(it->second.result.status());
    });
  }
  response = it->second.result;
  response.set_queue_ms(it->second.queue_ms);
  return grpc::Status::OK;
}
grpc::Status CoordinatorState::Heartbeat(const rpc::HeartbeatRequest& request, rpc::HeartbeatResponse& response, Clock::time_point now) {
  if (request.worker_id().empty() || request.worker_id().size() > 128 || request.worker_address().empty() ||
      request.worker_address().size() > 256 || !request.capacity() || request.capacity() > 1024 || request.active_tasks() > request.capacity())
    return {grpc::StatusCode::INVALID_ARGUMENT, "invalid worker registration"};
  std::lock_guard lock(mutex_);
  if (stopped_) return {grpc::StatusCode::UNAVAILABLE, "coordinator stopping"};
  auto it = workers_.find(request.worker_id());
  if (it == workers_.end()) {
    Worker worker{{request.worker_id(), request.worker_address(), request.capacity(), 0, true}, now};
    workers_.emplace(request.worker_id(), std::move(worker));
  } else {
    auto& worker = it->second;
    // A timed-out process must restart with a new incarnation ID before rejoining.
    if (!worker.node.is_healthy || worker.node.address != request.worker_address() || worker.node.capacity != request.capacity())
      return {grpc::StatusCode::FAILED_PRECONDITION, "worker incarnation expired or registration changed; restart with a new ID"};
    worker.heartbeat = now;
  }
  response.set_ack(true);
  return grpc::Status::OK;
}
void CoordinatorState::Release(const Task& task) {
  auto it = workers_.find(task.result.worker_id());
  if (it != workers_.end() && it->second.node.active_tasks) --it->second.node.active_tasks;
}
void CoordinatorState::Retry(Task& task, const std::string& error) {
  Release(task);
  task.result.set_error_message(error);
  task.result.clear_result();
  task.result.clear_worker_id();
  if (task.request.retryable() && task.result.attempt_id() < task.request.max_retries()) {
    task.result.set_attempt_id(task.result.attempt_id() + 1);
    task.result.set_status(rpc::TASK_STATUS_PENDING);
    task.queued_at = Clock::now();
    pending_.push_back(task.result.task_id());
  } else {
    task.result.set_status(rpc::TASK_STATUS_FAILED);
    task.result.set_completion_ms(std::chrono::duration<double, std::milli>(Clock::now() - task.submitted_at).count());
    --outstanding_;
    changed_.notify_all();
  }
}
void CoordinatorState::Report(const rpc::ReportResultRequest& request, rpc::ReportResultResponse& response) {
  std::lock_guard lock(mutex_);
  response.set_accepted(false);
  auto it = tasks_.find(request.task_id());
  if (it == tasks_.end()) return;
  auto& task = it->second;
  if (!Active(task.result.status()) || task.result.attempt_id() != request.attempt_id() || task.result.worker_id() != request.worker_id()) return;
  if (request.status() == rpc::TASK_STATUS_RUNNING) {
    task.result.set_status(rpc::TASK_STATUS_RUNNING);
    response.set_accepted(true);
    return;
  }
  if (!Terminal(request.status())) return;
  task.result.set_execution_ms(task.result.execution_ms() + std::max(0.0, request.execution_ms()));
  if (request.status() == rpc::TASK_STATUS_FAILED && request.retryable()) {
    Retry(task, request.error_message());
  } else {
    Release(task);
    task.result.set_status(request.status());
    task.result.set_completion_ms(std::chrono::duration<double, std::milli>(Clock::now() - task.submitted_at).count());
    task.result.set_result(request.result());
    task.result.set_error_message(request.error_message());
    --outstanding_;
    changed_.notify_all();
  }
  response.set_accepted(true);
}
std::optional<Dispatch> CoordinatorState::NextDispatch() {
  std::lock_guard lock(mutex_);
  if (stopped_ || pending_.empty()) return std::nullopt;
  std::vector<WorkerNode> nodes;
  for (const auto& [id, worker] : workers_) {
    auto node = worker.node;
    if (Clock::now() < worker.unavailable_until) node.is_healthy = false;
    nodes.push_back(std::move(node));
  }
  auto selection_start = Clock::now();
  auto id = scheduler_->SelectWorker(nodes);
  double scheduling_ms = std::chrono::duration<double, std::milli>(Clock::now() - selection_start).count();
  if (id.empty()) return std::nullopt;
  auto& worker = workers_.at(id).node;
  auto& task = tasks_.at(pending_.front());
  pending_.pop_front();
  task.result.set_scheduling_ms(task.result.scheduling_ms() + scheduling_ms);
  task.queue_ms += std::chrono::duration<double, std::milli>(Clock::now() - task.queued_at).count();
  task.result.set_worker_id(id);
  task.result.set_status(rpc::TASK_STATUS_DISPATCHED);
  ++worker.active_tasks;
  Dispatch dispatch;
  dispatch.worker_id = id;
  dispatch.address = worker.address;
  dispatch.task.set_task_id(task.result.task_id());
  dispatch.task.set_task_type(task.request.task_type());
  dispatch.task.set_payload(task.request.payload());
  dispatch.task.set_attempt_id(task.result.attempt_id());
  return dispatch;
}
void CoordinatorState::Dispatched(const Dispatch& dispatch, bool accepted) {
  std::lock_guard lock(mutex_);
  auto it = tasks_.find(dispatch.task.task_id());
  if (it == tasks_.end()) return;
  auto& task = it->second;
  if (task.result.status() != rpc::TASK_STATUS_DISPATCHED || task.result.worker_id() != dispatch.worker_id ||
      task.result.attempt_id() != dispatch.task.attempt_id()) return;
  // An ambiguous dispatch may still execute remotely. Fence it before retrying.
  if (!accepted) {
    // Avoid burning the entire retry budget on the same unreachable or draining destination.
    workers_.at(dispatch.worker_id).unavailable_until = Clock::now() + options_.heartbeat_timeout;
    Retry(task, "dispatch rejected or RPC failed");
  }
  else task.result.set_status(rpc::TASK_STATUS_RUNNING);
}
void CoordinatorState::Expire(Clock::time_point now) {
  std::lock_guard lock(mutex_);
  for (auto& [id, worker] : workers_) {
    if (!worker.node.is_healthy || now - worker.heartbeat < options_.heartbeat_timeout) continue;
    worker.node.is_healthy = false;
    for (auto& [task_id, task] : tasks_) {
      if (Active(task.result.status()) && task.result.worker_id() == id) Retry(task, "worker heartbeat expired");
    }
  }
}
void CoordinatorState::Stop() {
  std::lock_guard lock(mutex_);
  stopped_ = true;
  for (auto& [id, task] : tasks_) {
    if (!Terminal(task.result.status())) {
      task.result.set_status(rpc::TASK_STATUS_FAILED);
      task.result.set_error_message("coordinator shutting down");
      task.result.set_completion_ms(std::chrono::duration<double, std::milli>(Clock::now() - task.submitted_at).count());
    }
  }
  pending_.clear();
  outstanding_ = 0;
  changed_.notify_all();
}
}  // namespace miniray
