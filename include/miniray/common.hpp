#pragma once
#include <grpcpp/grpcpp.h>
#include <chrono>
#include <iomanip>
#include <random>
#include <sstream>
#include <string>

namespace miniray {
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
