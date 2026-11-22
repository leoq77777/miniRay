#include "miniray/worker.hpp"
#include "miniray/common.hpp"
#include <algorithm>
#include <cstdio>

namespace miniray {
namespace {
constexpr size_t kStreamChunkBytes = 1024 * 1024; // 1 MiB chunks stay well below gRPC's 4 MiB default limit.
}

std::string WorkerService::AttemptKey(const std::string& task_id, uint32_t attempt_id) {
  return std::to_string(task_id.size()) + ":" + task_id + ":" + std::to_string(attempt_id);
}

bool WorkerService::ValidTaskSpec(const rpc::TaskSpec& task) {
  return !task.task_id().empty() && task.task_id().size() <= 128 && !task.task_type().empty() &&
         task.task_type().size() <= 256 && task.payload().size() <= 1024 * 1024 && task.args_size() <= 64;
}

WorkerService::WorkerService(WorkerOptions options, const TaskRegistry& registry)
    : options_(std::move(options)), registry_(registry), store_(options_.host_id) {
  if (!options_.capacity || options_.capacity > 1024 || options_.heartbeat_interval.count() <= 0 ||
      !options_.max_object_bytes)
    throw std::invalid_argument("invalid worker capacity, heartbeat interval, or max object size");
  if (options_.id.empty()) options_.id = UniqueId();
  coordinator_ =
      rpc::CoordinatorService::NewStub(grpc::CreateChannel(options_.coordinator, grpc::InsecureChannelCredentials()));
}

WorkerService::~WorkerService() { Stop(); }

void WorkerService::Start(const std::string& advertised_address) {
  std::lock_guard lock(mutex_);
  if (!executors_.empty() || stopping_) throw std::logic_error("worker already started/stopped");
  if (!advertised_address.empty()) options_.address = advertised_address;
  if (options_.sweep_shm_orphans_on_start) {
    (void)ObjectStore::SweepOrphans();
  }
  for (uint32_t i = 0; i < options_.capacity; ++i) executors_.emplace_back([this] { ExecuteLoop(); });
  heartbeat_ = std::jthread([this](std::stop_token stop) {
    while (!stop.stop_requested() && !isolated_) {
      rpc::HeartbeatRequest request;
      request.set_worker_id(options_.id);
      request.set_worker_address(options_.address);
      request.set_capacity(options_.capacity);
      request.set_host_id(options_.host_id);
      std::vector<rpc::CachedReplica> cached_batch;
      {
        std::lock_guard guard(mutex_);
        request.set_active_tasks(active_);
        cached_batch = std::move(newly_cached_objects_);
        newly_cached_objects_.clear();
      }
      for (auto& c : cached_batch) *request.add_cached_objects() = std::move(c);

      rpc::HeartbeatResponse response;
      grpc::ClientContext context;
      Deadline(context, std::chrono::milliseconds(500));
      auto status = coordinator_->SendHeartbeat(&context, request, &response);
      if (status.error_code() == grpc::StatusCode::FAILED_PRECONDITION) {
        isolated_ = true;
        std::fputs("Worker incarnation expired; restart worker to rejoin\n", stderr);
        break;
      }
      if (status.ok()) {
        for (const auto& oid : response.release_objects()) {
          store_.Evict(oid);
        }
      } else if (!cached_batch.empty()) {
        std::lock_guard guard(mutex_);
        for (auto& c : cached_batch) newly_cached_objects_.push_back(std::move(c));
      }
      auto until = std::chrono::steady_clock::now() + options_.heartbeat_interval;
      while (!stop.stop_requested() && std::chrono::steady_clock::now() < until)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  });
}

void WorkerService::Stop() {
  {
    std::lock_guard lock(mutex_);
    stopping_ = true;
  }
  ready_.notify_all();
  for (auto& thread : executors_)
    if (thread.joinable()) thread.join();
  heartbeat_.request_stop();
  if (heartbeat_.joinable()) heartbeat_.join();
  store_.EvictAll();
}

bool WorkerService::AdmitLocked(const rpc::TaskSpec& task, rpc::DispatchTaskResponse& response) {
  const auto key = AttemptKey(task.task_id(), task.attempt_id());
  if (cancelled_.contains(key) || seen_.contains(key)) {
    response.set_accepted(true);
    return false;
  }
  if (stopping_ || isolated_ || executors_.empty() || active_ >= options_.capacity) {
    response.set_error_message("worker full or unavailable");
    return false;
  }
  ++active_;
  seen_.insert(key);
  queue_.push_back(task);
  response.set_accepted(true);
  return true;
}

grpc::Status WorkerService::DispatchTask(grpc::ServerContext*, const rpc::DispatchTaskRequest* request,
                                         rpc::DispatchTaskResponse* response) {
  const auto& task = request->task();
  if (!ValidTaskSpec(task)) return {grpc::StatusCode::INVALID_ARGUMENT, "invalid task specification"};
  bool queued = false;
  {
    std::lock_guard lock(mutex_);
    queued = AdmitLocked(task, *response);
  }
  if (queued) ready_.notify_one();
  return grpc::Status::OK;
}

grpc::Status WorkerService::DispatchBatch(grpc::ServerContext*, const rpc::DispatchBatchRequest* request,
                                          rpc::DispatchBatchResponse* response) {
  for (const auto& task : request->tasks()) {
    if (!ValidTaskSpec(task)) return {grpc::StatusCode::INVALID_ARGUMENT, "invalid task specification in batch"};
  }
  size_t queued = 0;
  {
    std::lock_guard lock(mutex_);
    for (const auto& task : request->tasks()) {
      auto* item = response->add_results();
      if (AdmitLocked(task, *item)) ++queued;
    }
  }
  if (queued == 1) ready_.notify_one();
  else if (queued > 1) ready_.notify_all();
  return grpc::Status::OK;
}

grpc::Status WorkerService::CancelTask(grpc::ServerContext*, const rpc::CancelTaskRequest* request,
                                       rpc::CancelTaskResponse* response) {
  if (request->task_id().empty() || request->task_id().size() > 128)
    return {grpc::StatusCode::INVALID_ARGUMENT, "invalid cancel task ID"};
  const auto key = AttemptKey(request->task_id(), request->attempt_id());
  std::lock_guard lock(mutex_);
  cancelled_.insert(key);
  seen_.insert(key);
  auto queued_it = std::find_if(queue_.begin(), queue_.end(), [&](const rpc::TaskSpec& spec) {
    return spec.task_id() == request->task_id() && spec.attempt_id() == request->attempt_id();
  });
  if (queued_it != queue_.end()) {
    queue_.erase(queued_it);
    if (active_) --active_;
    response->set_cancelled_or_absent(true);
    response->set_slot_freed(true);
    return grpc::Status::OK;
  }
  auto running_it = running_tokens_.find(key);
  if (running_it != running_tokens_.end()) {
    running_it->second.Cancel();
    response->set_cancelled_or_absent(true);
    response->set_slot_freed(false);
    return grpc::Status::OK;
  }
  response->set_cancelled_or_absent(true);
  response->set_slot_freed(true);
  return grpc::Status::OK;
}

grpc::Status WorkerService::PullObject(grpc::ServerContext* context, const rpc::PullObjectRequest* request,
                                       grpc::ServerWriter<rpc::ObjectChunk>* writer) {
  if (request->object_id().empty() || request->object_id().size() > 128) {
    return {grpc::StatusCode::INVALID_ARGUMENT, "invalid object_id"};
  }
  uint32_t crc = 0;
  auto view_opt = store_.GetLocal(request->object_id(), &crc);
  if (!view_opt) {
    return {grpc::StatusCode::NOT_FOUND, "object not found on worker: " + request->object_id()};
  }
  pull_rpc_count_.fetch_add(1, std::memory_order_relaxed);
  const auto data = view_opt->view();
  if (data.empty()) {
    rpc::ObjectChunk empty_chunk;
    empty_chunk.set_total_size(0);
    empty_chunk.set_crc32(crc);
    if (!writer->Write(empty_chunk)) return {grpc::StatusCode::CANCELLED, "stream write cancelled"};
    return grpc::Status::OK;
  }
  for (size_t offset = 0; offset < data.size(); offset += kStreamChunkBytes) {
    if (context && context->IsCancelled()) return {grpc::StatusCode::CANCELLED, "client cancelled PullObject"};
    size_t n = std::min(kStreamChunkBytes, data.size() - offset);
    rpc::ObjectChunk chunk;
    chunk.set_total_size(data.size());
    chunk.set_crc32(crc);
    chunk.set_data(data.data() + offset, n);
    if (!writer->Write(chunk)) return {grpc::StatusCode::CANCELLED, "stream write failed"};
  }
  return grpc::Status::OK;
}

void WorkerService::Report(const rpc::ReportResultRequest& request) {
  for (int retry = 0; retry < 10; ++retry) {
    grpc::ClientContext context;
    Deadline(context, std::chrono::milliseconds(500));
    rpc::ReportResultResponse response;
    auto status = coordinator_->ReportResult(&context, request, &response);
    // accepted=false fences a stale attempt; if we created an unreferenced shm segment for it, evict immediately.
    if (status.ok()) {
      if (!response.accepted() && request.has_result_ref()) {
        store_.Evict(request.task_id());
      }
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  isolated_ = true;
  std::fputs("Result reporting failed; worker isolated for recovery\n", stderr);
}

void WorkerService::ExecuteLoop() {
  while (true) {
    rpc::TaskSpec task;
    std::string key;
    CancellationToken token;
    {
      std::unique_lock lock(mutex_);
      ready_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
      if (queue_.empty()) return;
      task = std::move(queue_.front());
      queue_.pop_front();
      key = AttemptKey(task.task_id(), task.attempt_id());
      if (cancelled_.contains(key)) token.Cancel();
      running_tokens_.emplace(key, token);
    }
    std::fprintf(stderr, "Executing task=%s attempt=%u\n", task.task_id().c_str(), task.attempt_id());
    rpc::ReportResultRequest result;
    result.set_task_id(task.task_id());
    result.set_attempt_id(task.attempt_id());
    result.set_worker_id(options_.id);
    auto execution_start = std::chrono::steady_clock::now();
    try {
      if (token.IsCancelled()) throw TaskCancelledError();
      if (!registry_.HasTask(task.task_type())) throw std::invalid_argument("unknown task type: " + task.task_type());

      std::vector<ObjectView> resolved_args;
      resolved_args.reserve(static_cast<size_t>(task.args_size()));
      for (const auto& arg : task.args()) {
        if (token.IsCancelled()) throw TaskCancelledError();
        if (arg.has_object_ref()) {
          bool pulled = false;
          auto view = store_.Resolve(
              arg.object_ref(),
              [](const rpc::ObjectRef& ref) { return ObjectStore::PullFromRemoteWorkers(ref); },
              &pulled);
          if (pulled) {
            auto local_opt = store_.GetLocal(arg.object_ref().object_id());
            (void)local_opt;
            rpc::CachedReplica cached;
            cached.set_object_id(arg.object_ref().object_id());
            // Retrieve local shm_name via Put idempotency metadata.
            auto meta = store_.Put(arg.object_ref().object_id(), view.view(), options_.id, options_.address);
            cached.set_shm_name(meta.shm_name());
            std::lock_guard guard(mutex_);
            newly_cached_objects_.push_back(std::move(cached));
          }
          resolved_args.push_back(std::move(view));
        } else {
          resolved_args.push_back(ObjectView::FromInline(arg.inline_data()));
        }
      }

      auto payload = registry_.Execute(task.task_type(), task.payload(), TaskContext{token, resolved_args});
      if (payload.size() > options_.max_object_bytes) throw std::invalid_argument("result exceeds max object size");
      if (payload.size() >= options_.inline_threshold_bytes) {
        auto ref = store_.Put(task.task_id(), payload, options_.id, options_.address);
        *result.mutable_result_ref() = std::move(ref);
      } else {
        result.set_result(std::move(payload));
      }
      result.set_status(rpc::TASK_STATUS_COMPLETED);
    } catch (const TaskCancelledError& error) {
      result.set_status(rpc::TASK_STATUS_FAILED);
      result.set_error_message(error.what());
      result.set_retryable(true);
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
    result.set_execution_ms(
        std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - execution_start).count());
    // Release the local slot before reporting: the coordinator may immediately send more work.
    {
      std::lock_guard lock(mutex_);
      running_tokens_.erase(key);
      --active_;
    }
    Report(result);
  }
}
}  // namespace miniray
