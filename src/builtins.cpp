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
  registry.RegisterTask("blob_produce", [](const std::string& payload) {
    const auto bytes = Number(payload, 128ull * 1024 * 1024);
    std::string out(static_cast<size_t>(bytes), '\0');
    uint64_t state = 0x9E3779B97F4A7C15ull ^ bytes;
    for (size_t offset = 0; offset < out.size(); offset += 4096) {
      state ^= state >> 12;
      state ^= state << 25;
      state ^= state >> 27;
      out[offset] = static_cast<char>((state * 0x2545F4914F6CDD1Dull) & 0xFFu);
    }
    if (!out.empty()) out.back() = static_cast<char>(bytes & 0xFFu);
    return out;
  });
  registry.RegisterDagTask("blob_reduce", [](const std::string& payload, const TaskContext& ctx) {
    uint64_t acc = payload.size();
    uint64_t total_bytes = 0;
    uint32_t zero_copy_views = 0;
    for (const auto& view : ctx.args) {
      if (ctx.token.IsCancelled()) throw TaskCancelledError();
      const auto span = view.view();
      total_bytes += span.size();
      if (view.is_zero_copy_shm()) ++zero_copy_views;
      for (size_t offset = 0; offset < span.size(); offset += 4096) {
        acc = (acc * 1315423911ull) ^ static_cast<uint8_t>(span[offset]);
      }
      if (!span.empty()) acc ^= static_cast<uint8_t>(span.back());
    }
    return std::to_string(total_bytes) + ":" + std::to_string(zero_copy_views) + ":" + std::to_string(acc);
  });
}
}  // namespace miniray
