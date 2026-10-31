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
  registry.RegisterTask("sleep", [](const std::string& payload) {
    auto duration = Number(payload, 60000);
    std::this_thread::sleep_for(std::chrono::milliseconds(duration));
    return payload;
  });
}
}  // namespace miniray
