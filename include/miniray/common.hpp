#pragma once
#include <grpcpp/grpcpp.h>
#include <atomic>
#include <chrono>
#include <iomanip>
#include <memory>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>

namespace miniray {
class CancellationToken {
 public:
  CancellationToken() : cancelled_(std::make_shared<std::atomic<bool>>(false)) {}
  void Cancel() const noexcept { cancelled_->store(true, std::memory_order_relaxed); }
  bool IsCancelled() const noexcept { return cancelled_->load(std::memory_order_relaxed); }

 private:
  std::shared_ptr<std::atomic<bool>> cancelled_;
};

class TaskCancelledError : public std::runtime_error {
 public:
  TaskCancelledError() : std::runtime_error("task attempt cancelled") {}
};
inline std::string UniqueId() {
  thread_local std::mt19937_64 random([] {
    std::random_device device;
    std::seed_seq seed{device(), device(), device(), device(), device(), device(), device(), device()};
    return std::mt19937_64(seed);
  }());
  std::ostringstream out;
  out << std::hex << std::setfill('0') << std::setw(16) << random() << std::setw(16) << random();
  return out.str();
}
inline void Deadline(grpc::ClientContext& context, std::chrono::milliseconds timeout) {
  context.set_deadline(std::chrono::system_clock::now() + timeout);
}
}  // namespace miniray
