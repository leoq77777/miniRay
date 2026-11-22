#include "miniray/registry.hpp"
#include <charconv>
#include <chrono>
#include <thread>

namespace miniray {
namespace {
uint64_t Number(const std::string& input, uint64_t limit) {
  uint64_t n = 0;
  auto [end, error] = std::from_chars(input.data(), input.data() + input.size(), n);
  if (error != std::errc{} || end != input.data() + input.size() || n > limit)
    throw std::invalid_argument("invalid numeric argument or out of range");
  return n;
}
}
void RegisterBuiltins(TaskRegistry& registry) {
  registry.RegisterTask("echo", [](const std::string& payload) { return payload; });
  registry.RegisterTask("fibonacci", [](const std::string& payload) {
    const auto n = Number(payload, 93);
    uint64_t a = 0, b = 1;
    for (uint64_t i = 0; i < n; ++i) { auto next = a + b; a = b; b = next; }
    return std::to_string(a);
  });
  registry.RegisterCancellableTask("sleep", [](const std::string& payload, const CancellationToken& token) {
    auto duration = Number(payload, 60000);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(duration);
    while (std::chrono::steady_clock::now() < deadline) {
      if (token.IsCancelled()) throw TaskCancelledError();
      auto remaining = deadline - std::chrono::steady_clock::now();
      if (remaining <= std::chrono::steady_clock::duration::zero()) break;
      std::this_thread::sleep_for(std::min<std::chrono::nanoseconds>(remaining, std::chrono::milliseconds(5)));
    }
    if (token.IsCancelled()) throw TaskCancelledError();
    return payload;
  });
}
}  // namespace miniray
