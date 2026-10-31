#include "miniray/client.hpp"
#include "miniray/cli.hpp"
#include "miniray/registry.hpp"
#include "miniray/common.hpp"
#include <algorithm>
#include <condition_variable>
#include <cmath>
#include <csignal>
#include <limits>
#include <unistd.h>
#include <deque>
#include <future>
#include <iostream>
#include <mutex>
#include <vector>

using Clock = std::chrono::steady_clock;
struct Sample { double completion, queue, execution; };
class LocalPool {
 public:
  explicit LocalPool(uint32_t capacity) {
    for (uint32_t i = 0; i < capacity; ++i) threads_.emplace_back([this] {
      while (true) {
        std::packaged_task<Sample()> task;
        {
          std::unique_lock lock(mutex_);
          ready_.wait(lock, [this] { return stopped_ || !tasks_.empty(); });
          if (tasks_.empty()) return;
          task = std::move(tasks_.front()); tasks_.pop_front();
        }
        task();
      }
    });
  }
  ~LocalPool() {
    { std::lock_guard lock(mutex_); stopped_ = true; }
    ready_.notify_all();
    for (auto& thread : threads_) thread.join();
  }
  std::future<Sample> Submit(std::function<Sample()> work) {
    std::packaged_task<Sample()> task(std::move(work));
    auto result = task.get_future();
    { std::lock_guard lock(mutex_); tasks_.push_back(std::move(task)); }
    ready_.notify_one();
    return result;
  }
 private:
  std::mutex mutex_;
  std::condition_variable ready_;
  bool stopped_ = false;
  std::deque<std::packaged_task<Sample()>> tasks_;
  std::vector<std::thread> threads_;
};

struct Recovery { double detection_ms = -1, reassignment_ms = -1, completion_ms = -1; };
miniray::rpc::GetTaskResultResponse Poll(miniray::rpc::CoordinatorService::Stub& stub, const std::string& id) {
  grpc::ClientContext context;
  miniray::Deadline(context, std::chrono::seconds(2));
  miniray::rpc::GetTaskResultRequest request;
  request.set_task_id(id);
  miniray::rpc::GetTaskResultResponse response;
  auto status = stub.GetTaskResult(&context, request, &response);
  if (!status.ok()) throw miniray::RpcError(status);
  return response;
}
Recovery InjectFailure(miniray::rpc::CoordinatorService::Stub& stub, const std::vector<miniray::TaskFuture>& tasks,
                       const std::string& worker, pid_t pid) {
  std::string target;
  uint32_t attempt = 0;
  auto deadline = Clock::now() + std::chrono::seconds(10);
  while (target.empty() && Clock::now() < deadline) {
    for (const auto& task : tasks) {
      auto state = Poll(stub, task.id());
      if (state.worker_id() == worker && state.status() == miniray::rpc::TASK_STATUS_RUNNING) {
        target = task.id(); attempt = state.attempt_id(); break;
      }
    }
    if (target.empty()) std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  if (target.empty()) throw std::runtime_error("no running task on fault-injection worker");
  if (::kill(pid, SIGKILL)) throw std::runtime_error("cannot kill fault-injection worker");
  auto killed = Clock::now();
  deadline = killed + std::chrono::seconds(30);
  Recovery sample;
  while (Clock::now() < deadline) {
    auto state = Poll(stub, target);
    double elapsed = std::chrono::duration<double, std::milli>(Clock::now() - killed).count();
    if (state.attempt_id() > attempt) {
      if (sample.detection_ms < 0) sample.detection_ms = elapsed;
      if (sample.reassignment_ms < 0 && state.status() != miniray::rpc::TASK_STATUS_PENDING)
        sample.reassignment_ms = elapsed;
      if (state.status() == miniray::rpc::TASK_STATUS_COMPLETED) {
        sample.completion_ms = elapsed;
        return sample;
      }
    }
    if (state.status() == miniray::rpc::TASK_STATUS_FAILED) throw std::runtime_error("injected task failed to recover");
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  throw std::runtime_error("failure recovery observation timed out");
}
int main(int argc, char** argv) {
  try {
    miniray::Arguments args(argc, argv, {"coordinator", "tasks", "workload", "mode", "slots", "warmup", "pattern", "kill-worker-id", "kill-worker-pid"});
    auto count = args.Number("tasks", 400);
    auto slots = args.Number("slots", 4);
    auto warmup = args.Number("warmup", 16);
    if (slots > 1024 || count > 100000 || warmup > 1000) throw std::invalid_argument("slots <=1024, tasks <=100000, warmup <=1000 required");
    auto mode = args.Get("mode", "remote");
    auto workload = args.Get("workload", "uniform");
    auto pattern = args.Get("pattern", "burst");
    auto worker = args.Get("kill-worker-id", "");
    uint32_t pid = args.Get("kill-worker-pid", "").empty() ? 0 : args.Number("kill-worker-pid", 1);
    if (mode != "remote" && mode != "local" && mode != "thread-pool" && mode != "rpc") throw std::invalid_argument("mode: remote, local, thread-pool, rpc");
    if (workload != "uniform" && workload != "mixed" && workload != "tiny" && workload != "failure") throw std::invalid_argument("workload: uniform, mixed, tiny, failure");
    if (pattern != "serial" && pattern != "burst") throw std::invalid_argument("pattern: serial, burst");
    if (mode == "rpc" && (workload != "tiny" || pattern != "serial")) throw std::invalid_argument("rpc baseline requires tiny/serial");
    if ((!worker.empty() || pid) && (worker.empty() || pid <= 1 || pid > uint32_t(std::numeric_limits<pid_t>::max()) ||
        mode != "remote" || pattern != "burst" || workload != "failure")) throw std::invalid_argument("fault injection requires worker ID and PID, remote/burst/failure");
    auto address = args.Get("coordinator", "localhost:50051");
    miniray::MiniRayClient client(address);
    auto stub = miniray::rpc::CoordinatorService::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
    miniray::TaskRegistry registry;
    miniray::RegisterBuiltins(registry);
    LocalPool pool(mode == "thread-pool" ? slots : 1);
    auto rpc_roundtrip = [&] {
      grpc::ClientContext context;
      miniray::Deadline(context, std::chrono::seconds(2));
      miniray::rpc::GetTaskResultRequest request;
      request.set_task_id("benchmark-missing-" + miniray::UniqueId());
      miniray::rpc::GetTaskResultResponse response;
      auto status = stub->GetTaskResult(&context, request, &response);
      if (status.error_code() != grpc::StatusCode::NOT_FOUND) throw std::runtime_error("unexpected RPC baseline status");
    };
    std::string type = workload == "tiny" ? "echo" : "sleep";
    auto payload = [&](uint32_t i) -> std::string {
      if (workload == "tiny") return "x";
      if (workload == "failure") return "250";
      return workload == "mixed" && i % 20 == 0 ? "100" : "5";
    };
    uint32_t connection_warmup = mode == "remote" ? std::min(slots * 2, 1024u) : 0;
    if (connection_warmup) {
      std::vector<miniray::TaskFuture> connections;
      for (uint32_t i = 0; i < connection_warmup; ++i) connections.push_back(client.Submit("sleep", "20"));
      for (const auto& task : connections) task.Get();
    }
    for (uint32_t i = 0; i < warmup; ++i) {
      if (mode == "remote") client.Submit(type, payload(i)).Get();
      else if (mode == "rpc") rpc_roundtrip();
      else if (mode == "thread-pool") pool.Submit([&] { registry.Execute(type, payload(i)); return Sample{}; }).get();
      else registry.Execute(type, payload(i));
    }
    std::vector<double> latency, queue, scheduling;
    std::vector<miniray::TaskFuture> remote;
    std::vector<std::future<Sample>> local;
    uint64_t attempts = 0, accepted = 0, rejected = 0, failed = 0, recovered = 0;
    double execution_ms = 0;
    auto record = [&](Sample sample) {
      latency.push_back(sample.completion); queue.push_back(sample.queue); execution_ms += sample.execution;
    };
    auto record_remote = [&](const miniray::rpc::GetTaskResultResponse& result, double client_ms = -1) {
      if (result.status() != miniray::rpc::TASK_STATUS_COMPLETED) { ++failed; return; }
      record({client_ms < 0 ? result.completion_ms() : client_ms, result.queue_ms(), result.execution_ms()});
      scheduling.push_back(result.scheduling_ms());
      attempts += result.attempt_id() + 1;
      if (result.attempt_id()) ++recovered;
    };
    auto begin = Clock::now();
    for (uint32_t i = 0; i < count; ++i) {
      auto submitted = Clock::now();
      auto input = payload(i);
      if (mode == "remote") {
        try {
          auto task = client.Submit(type, input);
          ++accepted;
          if (pattern == "serial") {
            auto result = task.Wait();
            record_remote(result, std::chrono::duration<double, std::milli>(Clock::now() - submitted).count());
          } else remote.push_back(std::move(task));
        } catch (const miniray::RpcError& error) {
          if (error.code != grpc::StatusCode::RESOURCE_EXHAUSTED) throw;
          ++rejected;
        }
      } else if (mode == "rpc") {
        rpc_roundtrip(); ++accepted;
        record({std::chrono::duration<double, std::milli>(Clock::now() - submitted).count(), 0, 0});
      } else {
        auto work = [&, submitted, input] {
          auto started = Clock::now();
          registry.Execute(type, input);
          auto completed = Clock::now();
          return Sample{std::chrono::duration<double, std::milli>(completed - submitted).count(),
                        std::chrono::duration<double, std::milli>(started - submitted).count(),
                        std::chrono::duration<double, std::milli>(completed - started).count()};
        };
        ++accepted;
        if (mode == "thread-pool") {
          auto future = pool.Submit(work);
          if (pattern == "serial") {
            auto sample = future.get();
            sample.completion = std::chrono::duration<double, std::milli>(Clock::now() - submitted).count();
            record(sample);
          }
          else local.push_back(std::move(future));
        } else {
          auto sample = work();
          sample.completion = std::chrono::duration<double, std::milli>(Clock::now() - submitted).count();
          record(sample);
        }
      }
    }
    Recovery recovery;
    if (pid) recovery = InjectFailure(*stub, remote, worker, static_cast<pid_t>(pid));
    for (auto& task : remote) record_remote(task.Wait(std::chrono::seconds(120)));
    for (auto& task : local) record(task.get());
    double seconds = std::chrono::duration<double>(Clock::now() - begin).count();
    for (auto* values : {&latency, &queue, &scheduling}) std::sort(values->begin(), values->end());
    auto percentile = [](const std::vector<double>& samples, double q) {
      return samples.empty() ? 0.0 : samples[std::max<size_t>(1, std::ceil(q * samples.size())) - 1];
    };
    if (accepted + rejected != count || latency.size() + failed != accepted) throw std::runtime_error("benchmark task accounting mismatch");
    auto utilization = execution_ms / (1000 * seconds * (mode == "local" ? 1 : slots));
    std::cout << "mode,workload,pattern,latency_scope,tasks,warmup,connection_warmup,accepted,rejected,completed,failed,elapsed_s,throughput_per_s,p50_ms,p95_ms,p99_ms,queue_p99_ms,scheduling_p99_ms,slot_utilization,attempts,recovered_tasks,detection_observed_ms,reassignment_observed_ms,recovery_observed_ms\n"
      << mode << ',' << workload << ',' << pattern << ',' << (mode == "remote" && pattern == "burst" ? "coordinator" : "client") << ','
      << count << ',' << warmup << ',' << connection_warmup << ',' << accepted << ',' << rejected << ',' << latency.size() << ',' << failed << ',' << seconds << ','
      << latency.size() / seconds << ',' << percentile(latency, .50) << ',' << percentile(latency, .95) << ',' << percentile(latency, .99) << ','
      << percentile(queue, .99) << ',' << percentile(scheduling, .99) << ',' << utilization << ',' << attempts << ',' << recovered << ','
      << recovery.detection_ms << ',' << recovery.reassignment_ms << ',' << recovery.completion_ms << '\n';
    return rejected || failed ? 2 : 0;
  } catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
