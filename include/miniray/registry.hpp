#pragma once
#include <functional>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace miniray {
using TaskHandler = std::function<std::string(const std::string&)>;
class TaskRegistry {
 public:
  void RegisterTask(const std::string& name, TaskHandler handler) {
    if (name.empty() || !handler) throw std::invalid_argument("invalid task registration");
    std::unique_lock lock(mutex_);
    registry_[name] = std::move(handler);
  }
  bool HasTask(const std::string& name) const {
    std::shared_lock lock(mutex_);
    return registry_.contains(name);
  }
  std::string Execute(const std::string& name, const std::string& payload) const {
    TaskHandler handler;
    {
      std::shared_lock lock(mutex_);
      auto it = registry_.find(name);
      if (it == registry_.end()) throw std::invalid_argument("unknown task type: " + name);
      handler = it->second;
    }
    return handler(payload);
  }
 private:
  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, TaskHandler> registry_;
};
void RegisterBuiltins(TaskRegistry& registry);
}  // namespace miniray
