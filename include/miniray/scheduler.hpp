#pragma once
#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <unordered_set>
#include <vector>

namespace miniray {
struct WorkerNode {
  std::string id;
  std::string address;
  uint32_t capacity = 0;
  uint32_t active_tasks = 0;
  bool is_healthy = true;
  std::string host_id;
};

struct DependencyLocality {
  uint64_t size_bytes = 0;
  std::unordered_set<std::string> worker_ids;
  std::unordered_set<std::string> host_ids;
};

struct SchedulingContext {
  double expected_exec_ms = 5.0;
  double avg_worker_exec_ms = 5.0;
  double network_bandwidth_bytes_per_ms = 512.0 * 1024.0; // ~512 MiB/s default
  std::vector<DependencyLocality> dependencies;
};

class Scheduler {
 public:
  virtual ~Scheduler() = default;
  virtual std::string SelectWorker(const std::vector<WorkerNode>& workers) = 0;
  virtual std::string SelectWorkerForTask(const std::vector<WorkerNode>& workers, const SchedulingContext& ctx) {
    (void)ctx;
    return SelectWorker(workers);
  }
};

// Called under the coordinator state mutex.
class RoundRobinScheduler : public Scheduler {
 public:
  std::string SelectWorker(const std::vector<WorkerNode>& workers) override {
    if (workers.empty()) return "";
    for (size_t i = 0; i < workers.size(); ++i) {
      size_t index = (next_ + i) % workers.size();
      const auto& w = workers[index];
      if (w.is_healthy && w.active_tasks < w.capacity) {
        next_ = (index + 1) % workers.size();
        return w.id;
      }
    }
    return "";
  }

 private:
  size_t next_ = 0;
};

class LeastLoadedScheduler : public Scheduler {
 public:
  std::string SelectWorker(const std::vector<WorkerNode>& workers) override {
    const WorkerNode* best = nullptr;
    for (const auto& w : workers) {
      if (!w.is_healthy || w.active_tasks >= w.capacity) continue;
      if (!best || uint64_t(w.active_tasks) * best->capacity < uint64_t(best->active_tasks) * w.capacity ||
          (uint64_t(w.active_tasks) * best->capacity == uint64_t(best->active_tasks) * w.capacity && w.id < best->id))
        best = &w;
    }
    return best ? best->id : "";
  }
};

// Earliest Finish Time (EFT) data-locality-aware scheduler.
// Degrades identically to LeastLoadedScheduler when a task has no out-of-band ObjectRef dependencies.
class EftLocalityScheduler : public LeastLoadedScheduler {
 public:
  std::string SelectWorkerForTask(const std::vector<WorkerNode>& workers, const SchedulingContext& ctx) override {
    if (ctx.dependencies.empty()) return SelectWorker(workers);
    const WorkerNode* best = nullptr;
    double best_eft = std::numeric_limits<double>::infinity();
    const double bw = std::max(1.0, ctx.network_bandwidth_bytes_per_ms);
    const double avg_exec = std::max(1.0, ctx.avg_worker_exec_ms);
    for (const auto& w : workers) {
      if (!w.is_healthy || w.active_tasks >= w.capacity) continue;
      double queue_ms = (static_cast<double>(w.active_tasks) / static_cast<double>(w.capacity)) * avg_exec;
      double transfer_ms = 0.0;
      for (const auto& dep : ctx.dependencies) {
        if (dep.worker_ids.contains(w.id)) {
          transfer_ms += 0.0; // Already cached in this worker's ObjectStore.
        } else if (!w.host_id.empty() && dep.host_ids.contains(w.host_id)) {
          transfer_ms += 0.05; // Same-host zero-copy POSIX shm_open + mmap.
        } else {
          transfer_ms += static_cast<double>(dep.size_bytes) / bw;
        }
      }
      double eft = queue_ms + transfer_ms + ctx.expected_exec_ms;
      if (!best || eft + 1e-9 < best_eft) {
        best = &w;
        best_eft = eft;
      } else if (std::abs(eft - best_eft) <= 1e-9) {
        if (uint64_t(w.active_tasks) * best->capacity < uint64_t(best->active_tasks) * w.capacity ||
            (uint64_t(w.active_tasks) * best->capacity == uint64_t(best->active_tasks) * w.capacity && w.id < best->id))
          best = &w;
      }
    }
    return best ? best->id : "";
  }
};
}  // namespace miniray
