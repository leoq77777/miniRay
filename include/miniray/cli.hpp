#pragma once
#include <charconv>
#include <csignal>
#include <map>
#include <stdexcept>
#include <string>
#include <thread>
#include <chrono>

namespace miniray {
inline volatile std::sig_atomic_t interrupted = 0;
inline void SignalHandler(int) { interrupted = 1; }
inline void WaitForSignal() {
  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);
  while (!interrupted) std::this_thread::sleep_for(std::chrono::milliseconds(50));
}
class Arguments {
 public:
  Arguments(int argc, char** argv, std::initializer_list<std::string> allowed) {
    for (int i = 1; i < argc; i += 2) {
      std::string key = argv[i];
      bool known = false;
      for (const auto& option : allowed) if (key == "--" + option) known = true;
      if (!known || i + 1 == argc) throw std::invalid_argument("unknown option or missing value: " + key);
      if (!values_.emplace(key.substr(2), argv[i + 1]).second) throw std::invalid_argument("duplicate option: " + key);
    }
  }
  std::string Get(const std::string& key, std::string fallback) const {
    auto it = values_.find(key);
    return it == values_.end() ? fallback : it->second;
  }
  uint32_t Number(const std::string& key, uint32_t fallback) const {
    auto text = Get(key, std::to_string(fallback));
    uint32_t value = 0;
    auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (error != std::errc{} || end != text.data() + text.size() || !value) throw std::invalid_argument("invalid positive integer: " + key);
    return value;
  }
 private:
  std::map<std::string, std::string> values_;
};
}  // namespace miniray
