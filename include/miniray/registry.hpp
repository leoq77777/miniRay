#pragma once
#include "miniray/common.hpp"
#include "miniray/object_store.hpp"
#include <functional>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace miniray {
struct TaskContext {
  const CancellationToken& token;
  const std::vector<ObjectView>& args;
};

using TaskHandler = std::function<std::string(const std::string&)>;
using CancellableTaskHandler = std::function<std::string(const std::string&, const CancellationToken&)>;
using DagTaskHandler = std::function<std::string(const std::string&, const TaskContext&)>;

class TaskRegistry {
 public:
  void RegisterTask(const std::string& name, TaskHandler handler) {
    if (name.empty() || !handler) throw std::invalid_argument("invalid task registration");
    RegisterDagTask(name, [fn = std::move(handler)](const std::string& payload, const TaskContext&) {
      return fn(payload);
    });
  }
  void RegisterCancellableTask(const std::string& name, CancellableTaskHandler handler) {
    if (name.empty() || !handler) throw std::invalid_argument("invalid task registration");
    RegisterDagTask(name, [fn = std::move(handler)](const std::string& payload, const TaskContext& ctx) {
      return fn(payload, ctx.token);
    });
  }
  void RegisterDagTask(const std::string& name, DagTaskHandler handler) {
    if (name.empty() || !handler) throw std::invalid_argument("invalid task registration");
    std::unique_lock lock(mutex_);
    registry_[name] = std::move(handler);
  }
  bool HasTask(const std::string& name) const {
    std::shared_lock lock(mutex_);
    return registry_.contains(name);
  }
  std::string Execute(const std::string& name, const std::string& payload) const {
    CancellationToken token;
    std::vector<ObjectView> empty_args;
    return Execute(name, payload, TaskContext{token, empty_args});
  }
  std::string Execute(const std::string& name, const std::string& payload, const CancellationToken& token) const {
    std::vector<ObjectView> empty_args;
    return Execute(name, payload, TaskContext{token, empty_args});
  }
  std::string Execute(const std::string& name, const std::string& payload, const TaskContext& context) const {
    DagTaskHandler handler;
    {
      std::shared_lock lock(mutex_);
      auto it = registry_.find(name);
      if (it == registry_.end()) throw std::invalid_argument("unknown task type: " + name);
      handler = it->second;
    }
    return handler(payload, context);
  }

 private:
  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, DagTaskHandler> registry_;
};

void RegisterBuiltins(TaskRegistry& registry);
}  // namespace miniray
