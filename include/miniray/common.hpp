#pragma once
#include <grpcpp/grpcpp.h>
#include <unistd.h>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>
#include <random>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>

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

inline std::string LocalHostId() {
  char buf[256] = {};
  std::string host = (::gethostname(buf, sizeof(buf) - 1) == 0) ? std::string(buf) : "localhost";
  return host + ":" + std::to_string(static_cast<uint64_t>(::getuid()));
}

inline uint32_t Crc32(std::span<const std::byte> bytes) noexcept {
  static constexpr auto kTable = [] {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
      uint32_t c = i;
      for (int bit = 0; bit < 8; ++bit) c = (c & 1u) ? (0xEDB88320u ^ (c >> 1u)) : (c >> 1u);
      table[i] = c;
    }
    return table;
  }();
  uint32_t crc = 0xFFFFFFFFu;
  for (std::byte b : bytes) {
    crc = kTable[(crc ^ static_cast<uint8_t>(b)) & 0xFFu] ^ (crc >> 8u);
  }
  return crc ^ 0xFFFFFFFFu;
}

inline uint32_t Crc32(std::string_view text) noexcept {
  return Crc32(std::span<const std::byte>(reinterpret_cast<const std::byte*>(text.data()), text.size()));
}
}  // namespace miniray
