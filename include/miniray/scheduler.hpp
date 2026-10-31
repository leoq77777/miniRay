#pragma once
#include <cstdint>
#include <string>
#include <vector>

namespace miniray {
struct WorkerNode {
  std::string id;
  std::string address;
  uint32_t capacity = 0;
  uint32_t active_tasks = 0;
  bool is_healthy = true;
};
class Scheduler {
 public:
  virtual ~Scheduler() = default;
  virtual std::string SelectWorker(const std::vector<WorkerNode>& workers) = 0;
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
          (uint64_t(w.active_tasks) * best->capacity == uint64_t(best->active_tasks) * w.capacity && w.id < best->id)) best = &w;
    }
    return best ? best->id : "";
  }
};
}  // namespace miniray
