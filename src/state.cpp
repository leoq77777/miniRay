#include "miniray/state.hpp"
#include "miniray/common.hpp"
#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace miniray {
namespace {
bool Terminal(rpc::TaskStatus s) {
  return s == rpc::TASK_STATUS_COMPLETED || s == rpc::TASK_STATUS_FAILED;
}
}  // namespace

void CoordinatorState::TaskProfile::Record(double ms) {
  ms = std::max(0.0, ms);
  ewma_ms = count == 0 ? ms : (0.2 * ms + 0.8 * ewma_ms);
  if (recent_ms.size() >= 64) recent_ms.pop_front();
  recent_ms.push_back(ms);
  ++count;
}

double CoordinatorState::TaskProfile::P95() const {
  if (recent_ms.empty()) return 0.0;
  std::vector<double> sorted(recent_ms.begin(), recent_ms.end());
  std::sort(sorted.begin(), sorted.end());
  size_t idx = std::max<size_t>(1, static_cast<size_t>(std::ceil(0.95 * static_cast<double>(sorted.size())))) - 1;
  return sorted[idx];
}

CoordinatorState::CoordinatorState(CoordinatorOptions options) : options_(options) {
  if (!options.max_pending || options.heartbeat_timeout.count() <= 0 || !options.max_batch_size ||
      options.speculation_multiplier < 1.0)
    throw std::invalid_argument("invalid coordinator options");
  if (options.round_robin) scheduler_ = std::make_unique<RoundRobinScheduler>();
  else scheduler_ = std::make_unique<LeastLoadedScheduler>();
}

bool CoordinatorState::HasReadyTasksLocked() const {
  return !recovery_queue_.empty() || !fast_queue_.empty() || !normal_queue_.empty();
}

void CoordinatorState::EnqueueReadyLocked(const std::string& id, bool urgent) {
  if (urgent) {
    const auto submitted = tasks_.at(id).submitted_at;
    auto pos = std::upper_bound(recovery_queue_.begin(), recovery_queue_.end(), submitted,
                                [this](Clock::time_point t, const std::string& other) {
                                  return t < tasks_.at(other).submitted_at;
                                });
    recovery_queue_.insert(pos, id);
    return;
  }
  const auto& type = tasks_.at(id).request.task_type();
  auto prof = profiles_.find(type);
  if (prof != profiles_.end() && prof->second.count > 0 &&
      prof->second.ewma_ms <= static_cast<double>(options_.fast_task_threshold.count())) {
    fast_queue_.push_back(id);
  } else {
    normal_queue_.push_back(id);
  }
}

std::string CoordinatorState::PopReadyTaskLocked(Clock::time_point now) {
  if (!recovery_queue_.empty()) {
    auto id = std::move(recovery_queue_.front());
    recovery_queue_.pop_front();
    return id;
  }
  if (!normal_queue_.empty() && !fast_queue_.empty()) {
    const auto& oldest_normal = tasks_.at(normal_queue_.front());
    if (now - oldest_normal.queued_at >= options_.aging_threshold) {
      auto id = std::move(normal_queue_.front());
      normal_queue_.pop_front();
      return id;
    }
    auto id = std::move(fast_queue_.front());
    fast_queue_.pop_front();
    return id;
  }
  if (!fast_queue_.empty()) {
    auto id = std::move(fast_queue_.front());
    fast_queue_.pop_front();
    return id;
  }
  auto id = std::move(normal_queue_.front());
  normal_queue_.pop_front();
  return id;
}

grpc::Status CoordinatorState::Submit(const rpc::SubmitTaskRequest& request, rpc::SubmitTaskResponse& response,
                                      Clock::time_point now) {
  if (request.task_type().empty() || request.task_type().size() > 256 || request.task_id().size() > 128 ||
      request.payload().size() > 1024 * 1024 || request.max_retries() > 100)
    return {grpc::StatusCode::INVALID_ARGUMENT, "invalid task type, ID, payload (>1 MiB), or retry limit (>100)"};
  bool notify = false;
  {
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
      task.queued_at = now;
      task.submitted_at = now;
      tasks_.emplace(id, std::move(task));
      EnqueueReadyLocked(id, false);
      ++outstanding_;
      notify = true;
    }
    response.set_task_id(id);
    response.set_accepted(true);
  }
  if (notify) dispatch_cv_.notify_one();
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

grpc::Status CoordinatorState::Heartbeat(const rpc::HeartbeatRequest& request, rpc::HeartbeatResponse& response,
                                         Clock::time_point now) {
  if (request.worker_id().empty() || request.worker_id().size() > 128 || request.worker_address().empty() ||
      request.worker_address().size() > 256 || !request.capacity() || request.capacity() > 1024 ||
      request.active_tasks() > request.capacity())
    return {grpc::StatusCode::INVALID_ARGUMENT, "invalid worker registration"};
  bool wake = false;
  {
    std::lock_guard lock(mutex_);
    if (stopped_) return {grpc::StatusCode::UNAVAILABLE, "coordinator stopping"};
    auto it = workers_.find(request.worker_id());
    if (it == workers_.end()) {
      Worker worker{{request.worker_id(), request.worker_address(), request.capacity(), 0, true}, now};
      workers_.emplace(request.worker_id(), std::move(worker));
      wake = true;
    } else {
      auto& worker = it->second;
      // A timed-out process must restart with a new incarnation ID before rejoining.
      if (!worker.node.is_healthy || worker.node.address != request.worker_address() ||
          worker.node.capacity != request.capacity())
        return {grpc::StatusCode::FAILED_PRECONDITION,
                "worker incarnation expired or registration changed; restart with a new ID"};
      worker.heartbeat = now;
    }
    response.set_ack(true);
  }
  if (wake) dispatch_cv_.notify_one();
  return grpc::Status::OK;
}

void CoordinatorState::ReleaseWorkerSlotLocked(const std::string& worker_id) {
  auto it = workers_.find(worker_id);
  if (it != workers_.end() && it->second.node.active_tasks) --it->second.node.active_tasks;
}

void CoordinatorState::FailOrRetryLocked(Task& task, const std::string& error, bool worker_expired,
                                         Clock::time_point now) {
  task.result.set_error_message(error);
  task.result.clear_result();
  task.result.clear_worker_id();
  ++task.failures_count;
  if (task.request.retryable() && task.failures_count <= task.request.max_retries()) {
    uint32_t next_aid = task.next_attempt_id++;
    task.pending_attempt_id = next_aid;
    task.result.set_attempt_id(next_aid);
    task.result.set_status(rpc::TASK_STATUS_PENDING);
    task.queued_at = now;
    bool urgent = worker_expired || (now - task.submitted_at >= options_.aging_threshold);
    EnqueueReadyLocked(task.result.task_id(), urgent);
  } else {
    task.result.set_status(rpc::TASK_STATUS_FAILED);
    task.result.set_completion_ms(std::chrono::duration<double, std::milli>(now - task.submitted_at).count());
    --outstanding_;
    changed_.notify_all();
  }
}

void CoordinatorState::Report(const rpc::ReportResultRequest& request, rpc::ReportResultResponse& response,
                              Clock::time_point now) {
  bool wake = false;
  {
    std::lock_guard lock(mutex_);
    response.set_accepted(false);
    auto it = tasks_.find(request.task_id());
    if (it == tasks_.end()) return;
    AttemptKey key{request.task_id(), request.attempt_id()};
    auto cancel_it = cancelling_slots_.find(key);
    if (cancel_it != cancelling_slots_.end() && cancel_it->second == request.worker_id()) {
      if (Terminal(request.status())) {
        ReleaseWorkerSlotLocked(request.worker_id());
        cancelling_slots_.erase(cancel_it);
        wake = true;
      }
    }
    auto& task = it->second;
    if (Terminal(task.result.status())) goto done;
    {
      auto att_it = task.active_attempts.find(request.attempt_id());
      if (att_it == task.active_attempts.end() || att_it->second.worker_id != request.worker_id()) goto done;
      if (request.status() == rpc::TASK_STATUS_RUNNING) {
        att_it->second.status = rpc::TASK_STATUS_RUNNING;
        if (task.result.attempt_id() == request.attempt_id()) task.result.set_status(rpc::TASK_STATUS_RUNNING);
        response.set_accepted(true);
        return;
      }
      if (!Terminal(request.status())) goto done;
      double exec_ms = std::max(0.0, request.execution_ms());
      task.result.set_execution_ms(task.result.execution_ms() + exec_ms);
      ReleaseWorkerSlotLocked(request.worker_id());
      task.active_attempts.erase(att_it);
      wake = true;
      if (request.status() == rpc::TASK_STATUS_FAILED && request.retryable()) {
        if (!task.active_attempts.empty()) {
          const auto& remaining = task.active_attempts.begin()->second;
          task.result.set_attempt_id(remaining.attempt_id);
          task.result.set_worker_id(remaining.worker_id);
          task.result.set_status(remaining.status);
        } else {
          FailOrRetryLocked(task, request.error_message(), false, now);
        }
      } else {
        for (const auto& [other_aid, other] : task.active_attempts) {
          cancelling_slots_.emplace(AttemptKey{request.task_id(), other_aid}, other.worker_id);
          pending_cancels_.push_back(
              CancelWork{request.task_id(), other_aid, other.worker_id, other.address, "superseded by completed replica"});
        }
        task.active_attempts.clear();
        task.result.set_attempt_id(request.attempt_id());
        task.result.set_worker_id(request.worker_id());
        task.result.set_status(request.status());
        task.result.set_completion_ms(std::chrono::duration<double, std::milli>(now - task.submitted_at).count());
        task.result.set_result(request.result());
        task.result.set_error_message(request.error_message());
        if (request.status() == rpc::TASK_STATUS_COMPLETED) {
          profiles_[task.request.task_type()].Record(exec_ms);
        }
        --outstanding_;
        changed_.notify_all();
      }
      response.set_accepted(true);
    }
  done:;
  }
  if (wake) dispatch_cv_.notify_one();
}

std::vector<WorkerNode> CoordinatorState::BuildCandidateNodesLocked(
    Clock::time_point now, const std::map<std::string, size_t>& batch_counts, const std::string& exclude_worker) const {
  std::vector<WorkerNode> nodes;
  nodes.reserve(workers_.size());
  for (const auto& [id, worker] : workers_) {
    auto node = worker.node;
    if (now < worker.unavailable_until || id == exclude_worker) node.is_healthy = false;
    auto bc = batch_counts.find(id);
    if (bc != batch_counts.end() && bc->second >= options_.max_batch_size) node.is_healthy = false;
    nodes.push_back(std::move(node));
  }
  return nodes;
}

std::optional<Dispatch> CoordinatorState::SelectReadyDispatchLocked(
    Clock::time_point now, const std::map<std::string, size_t>& batch_counts) {
  if (stopped_ || !HasReadyTasksLocked()) return std::nullopt;
  auto nodes = BuildCandidateNodesLocked(now, batch_counts);
  auto selection_start = Clock::now();
  auto id = scheduler_->SelectWorker(nodes);
  double scheduling_ms = std::chrono::duration<double, std::milli>(Clock::now() - selection_start).count();
  if (id.empty()) return std::nullopt;
  auto& worker = workers_.at(id).node;
  auto task_id = PopReadyTaskLocked(now);
  auto& task = tasks_.at(task_id);
  uint32_t aid = task.pending_attempt_id;
  task.result.set_scheduling_ms(task.result.scheduling_ms() + scheduling_ms);
  task.queue_ms += std::chrono::duration<double, std::milli>(now - task.queued_at).count();
  task.result.set_attempt_id(aid);
  task.result.set_worker_id(id);
  task.result.set_status(rpc::TASK_STATUS_DISPATCHED);
  ++worker.active_tasks;
  task.active_attempts.emplace(
      aid, AttemptInfo{aid, id, worker.address, rpc::TASK_STATUS_DISPATCHED, now, false, false});
  Dispatch dispatch;
  dispatch.worker_id = id;
  dispatch.address = worker.address;
  dispatch.task.set_task_id(task_id);
  dispatch.task.set_task_type(task.request.task_type());
  dispatch.task.set_payload(task.request.payload());
  dispatch.task.set_attempt_id(aid);
  return dispatch;
}

std::optional<Dispatch> CoordinatorState::SelectSpeculativeDispatchLocked(
    Clock::time_point now, const std::map<std::string, size_t>& batch_counts) {
  if (stopped_ || !options_.enable_speculation || HasReadyTasksLocked()) return std::nullopt;
  for (auto& [task_id, task] : tasks_) {
    if (Terminal(task.result.status()) || !task.request.retryable() ||
        task.failures_count >= task.request.max_retries() || task.active_attempts.size() != 1)
      continue;
    auto& orig = task.active_attempts.begin()->second;
    if (orig.hedged) continue;
    auto prof_it = profiles_.find(task.request.task_type());
    if (prof_it == profiles_.end() || prof_it->second.count < options_.min_profile_samples) continue;
    double threshold_ms = std::max(static_cast<double>(options_.min_speculation_delay.count()),
                                   options_.speculation_multiplier * prof_it->second.P95());
    double elapsed_ms = std::chrono::duration<double, std::milli>(now - orig.started_at).count();
    if (elapsed_ms < threshold_ms) continue;
    auto nodes = BuildCandidateNodesLocked(now, batch_counts, orig.worker_id);
    auto selection_start = Clock::now();
    auto w2_id = scheduler_->SelectWorker(nodes);
    double scheduling_ms = std::chrono::duration<double, std::milli>(Clock::now() - selection_start).count();
    if (w2_id.empty()) continue;
    orig.hedged = true;
    uint32_t spec_aid = task.next_attempt_id++;
    auto& w2 = workers_.at(w2_id).node;
    ++w2.active_tasks;
    task.result.set_scheduling_ms(task.result.scheduling_ms() + scheduling_ms);
    task.active_attempts.emplace(
        spec_aid, AttemptInfo{spec_aid, w2_id, w2.address, rpc::TASK_STATUS_DISPATCHED, now, true, true});
    Dispatch dispatch;
    dispatch.worker_id = w2_id;
    dispatch.address = w2.address;
    dispatch.task.set_task_id(task_id);
    dispatch.task.set_task_type(task.request.task_type());
    dispatch.task.set_payload(task.request.payload());
    dispatch.task.set_attempt_id(spec_aid);
    return dispatch;
  }
  return std::nullopt;
}

std::optional<Dispatch> CoordinatorState::NextDispatch(Clock::time_point now) {
  std::lock_guard lock(mutex_);
  std::map<std::string, size_t> empty_counts;
  if (auto ready = SelectReadyDispatchLocked(now, empty_counts)) return ready;
  return SelectSpeculativeDispatchLocked(now, empty_counts);
}

CoordinatorWork CoordinatorState::CollectWorkLocked(Clock::time_point now) {
  CoordinatorWork work;
  if (stopped_) return work;
  work.cancels = std::move(pending_cancels_);
  pending_cancels_.clear();

  std::map<std::string, size_t> batch_index;
  std::map<std::string, size_t> batch_counts;
  auto append = [&](Dispatch d) {
    auto it = batch_index.find(d.worker_id);
    if (it == batch_index.end()) {
      batch_index.emplace(d.worker_id, work.dispatches.size());
      work.dispatches.push_back(DispatchBatchWork{d.worker_id, d.address, {std::move(d)}});
    } else {
      work.dispatches[it->second].items.push_back(std::move(d));
    }
  };

  while (true) {
    if (auto d = SelectReadyDispatchLocked(now, batch_counts)) {
      ++batch_counts[d->worker_id];
      append(std::move(*d));
      continue;
    }
    if (auto spec = SelectSpeculativeDispatchLocked(now, batch_counts)) {
      ++batch_counts[spec->worker_id];
      append(std::move(*spec));
      continue;
    }
    break;
  }
  return work;
}

CoordinatorWork CoordinatorState::CollectWork(Clock::time_point now) {
  std::lock_guard lock(mutex_);
  ExpireLocked(now);
  return CollectWorkLocked(now);
}

Clock::time_point CoordinatorState::NextWakeupLocked(Clock::time_point now) const {
  auto next = now + std::chrono::hours(24);
  for (const auto& [id, worker] : workers_) {
    if (worker.node.is_healthy) {
      next = std::min(next, worker.heartbeat + options_.heartbeat_timeout);
    }
    if (HasReadyTasksLocked() && worker.unavailable_until > now && worker.node.is_healthy) {
      next = std::min(next, worker.unavailable_until);
    }
  }
  if (options_.enable_speculation && !HasReadyTasksLocked()) {
    for (const auto& [task_id, task] : tasks_) {
      if (Terminal(task.result.status()) || !task.request.retryable() ||
          task.failures_count >= task.request.max_retries() || task.active_attempts.size() != 1)
        continue;
      const auto& orig = task.active_attempts.begin()->second;
      if (orig.hedged) continue;
      auto prof_it = profiles_.find(task.request.task_type());
      if (prof_it == profiles_.end() || prof_it->second.count < options_.min_profile_samples) continue;
      double threshold_ms = std::max(static_cast<double>(options_.min_speculation_delay.count()),
                                     options_.speculation_multiplier * prof_it->second.P95());
      auto trigger = orig.started_at +
                     std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double, std::milli>(threshold_ms));
      next = std::min(next, trigger);
    }
  }
  return std::max(now, next);
}

CoordinatorWork CoordinatorState::WaitNextWork(std::stop_token stop_token) {
  std::unique_lock lock(mutex_);
  while (!stopped_ && !stop_token.stop_requested()) {
    auto now = Clock::now();
    ExpireLocked(now);
    auto work = CollectWorkLocked(now);
    if (!work.dispatches.empty() || !work.cancels.empty()) return work;
    auto deadline = NextWakeupLocked(now);
    dispatch_cv_.wait_until(lock, deadline);
  }
  return {};
}

std::vector<CancelWork> CoordinatorState::DrainCancels() {
  std::lock_guard lock(mutex_);
  auto out = std::move(pending_cancels_);
  pending_cancels_.clear();
  return out;
}

void CoordinatorState::CancelAck(const CancelWork& cancel, bool slot_freed) {
  if (!slot_freed) return;
  bool freed = false;
  {
    std::lock_guard lock(mutex_);
    AttemptKey key{cancel.task_id, cancel.attempt_id};
    auto it = cancelling_slots_.find(key);
    if (it != cancelling_slots_.end() && it->second == cancel.worker_id) {
      ReleaseWorkerSlotLocked(cancel.worker_id);
      cancelling_slots_.erase(it);
      freed = true;
    }
  }
  if (freed) dispatch_cv_.notify_one();
}

void CoordinatorState::DispatchedLocked(const Dispatch& dispatch, bool accepted, Clock::time_point now) {
  auto it = tasks_.find(dispatch.task.task_id());
  if (it == tasks_.end()) return;
  AttemptKey key{dispatch.task.task_id(), dispatch.task.attempt_id()};
  auto cancel_it = cancelling_slots_.find(key);
  if (cancel_it != cancelling_slots_.end() && cancel_it->second == dispatch.worker_id) {
    if (!accepted) {
      ReleaseWorkerSlotLocked(dispatch.worker_id);
      cancelling_slots_.erase(cancel_it);
    }
    return;
  }
  auto& task = it->second;
  if (Terminal(task.result.status())) return;
  auto att_it = task.active_attempts.find(dispatch.task.attempt_id());
  if (att_it == task.active_attempts.end() || att_it->second.worker_id != dispatch.worker_id ||
      att_it->second.status != rpc::TASK_STATUS_DISPATCHED)
    return;
  // An ambiguous dispatch may still execute remotely. Fence it before retrying.
  if (!accepted) {
    // Avoid burning the entire retry budget on the same unreachable or draining destination.
    workers_.at(dispatch.worker_id).unavailable_until = now + options_.heartbeat_timeout;
    bool was_speculative = att_it->second.is_speculative;
    ReleaseWorkerSlotLocked(dispatch.worker_id);
    task.active_attempts.erase(att_it);
    if (!task.active_attempts.empty()) {
      const auto& remaining = task.active_attempts.begin()->second;
      task.result.set_attempt_id(remaining.attempt_id);
      task.result.set_worker_id(remaining.worker_id);
      task.result.set_status(remaining.status);
    } else if (!was_speculative) {
      FailOrRetryLocked(task, "dispatch rejected or RPC failed", false, now);
    } else {
      FailOrRetryLocked(task, "speculative dispatch failed", false, now);
    }
  } else {
    att_it->second.status = rpc::TASK_STATUS_RUNNING;
    if (task.result.attempt_id() == dispatch.task.attempt_id()) task.result.set_status(rpc::TASK_STATUS_RUNNING);
  }
}

void CoordinatorState::Dispatched(const Dispatch& dispatch, bool accepted, Clock::time_point now) {
  {
    std::lock_guard lock(mutex_);
    DispatchedLocked(dispatch, accepted, now);
  }
  if (!accepted) dispatch_cv_.notify_one();
}

void CoordinatorState::DispatchedBatch(const std::vector<Dispatch>& items, const grpc::Status& status,
                                       const rpc::DispatchBatchResponse& response, Clock::time_point now) {
  bool any_rejected = false;
  {
    std::lock_guard lock(mutex_);
    bool valid_batch = status.ok() && static_cast<size_t>(response.results_size()) == items.size();
    for (size_t i = 0; i < items.size(); ++i) {
      bool accepted = valid_batch && response.results(static_cast<int>(i)).accepted();
      if (!accepted) any_rejected = true;
      DispatchedLocked(items[i], accepted, now);
    }
  }
  if (any_rejected) dispatch_cv_.notify_one();
}

void CoordinatorState::ExpireLocked(Clock::time_point now) {
  for (auto& [id, worker] : workers_) {
    if (!worker.node.is_healthy || now - worker.heartbeat < options_.heartbeat_timeout) continue;
    worker.node.is_healthy = false;
    for (auto it = cancelling_slots_.begin(); it != cancelling_slots_.end();) {
      if (it->second == id) {
        ReleaseWorkerSlotLocked(id);
        it = cancelling_slots_.erase(it);
      } else {
        ++it;
      }
    }
    for (auto& [task_id, task] : tasks_) {
      if (Terminal(task.result.status())) continue;
      bool removed = false;
      for (auto att_it = task.active_attempts.begin(); att_it != task.active_attempts.end();) {
        if (att_it->second.worker_id == id) {
          ReleaseWorkerSlotLocked(id);
          att_it = task.active_attempts.erase(att_it);
          removed = true;
        } else {
          ++att_it;
        }
      }
      if (!removed) continue;
      if (!task.active_attempts.empty()) {
        const auto& remaining = task.active_attempts.begin()->second;
        task.result.set_attempt_id(remaining.attempt_id);
        task.result.set_worker_id(remaining.worker_id);
        task.result.set_status(remaining.status);
      } else {
        FailOrRetryLocked(task, "worker heartbeat expired", true, now);
      }
    }
  }
}

void CoordinatorState::Expire(Clock::time_point now) {
  {
    std::lock_guard lock(mutex_);
    ExpireLocked(now);
  }
  dispatch_cv_.notify_one();
}

void CoordinatorState::Stop() {
  {
    std::lock_guard lock(mutex_);
    stopped_ = true;
    auto now = Clock::now();
    for (auto& [id, task] : tasks_) {
      if (!Terminal(task.result.status())) {
        task.active_attempts.clear();
        task.result.set_status(rpc::TASK_STATUS_FAILED);
        task.result.set_error_message("coordinator shutting down");
        task.result.set_completion_ms(std::chrono::duration<double, std::milli>(now - task.submitted_at).count());
      }
    }
    recovery_queue_.clear();
    fast_queue_.clear();
    normal_queue_.clear();
    cancelling_slots_.clear();
    pending_cancels_.clear();
    outstanding_ = 0;
  }
  changed_.notify_all();
  dispatch_cv_.notify_all();
}
}  // namespace miniray
